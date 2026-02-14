// Package flusher provides the ConcurrentFlusher that integrates all flushing components.
//
// ConcurrentFlusher combines:
// - AdaptiveFlusher: Rate-based interval adjustment
// - DirtyPageTracker: Page-level dirty tracking
// - FlushWorkerPool: Parallel background writers
//
// This provides a complete concurrent write solution similar to PostgreSQL's
// background writer architecture, optimized for SyndrDB's document storage model.
package flusher

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"syndrdb/src/internal/domain/models"

	"go.uber.org/zap"
)

// ConcurrentFlusherConfig holds configuration for the concurrent flusher.
type ConcurrentFlusherConfig struct {
	// Adaptive flushing settings
	BaseInterval      time.Duration
	MinInterval       time.Duration
	MaxInterval       time.Duration
	HighLoadThreshold int
	LowLoadThreshold  int

	// Dirty page settings
	WorkerCount     int
	MaxDocsPerPage  int
	MaxBytesPerPage int64
	QueueSize       int

	// Thresholds for immediate flush
	FlushThreshold int   // Document count
	MaxBufferSize  int64 // Total buffer bytes
}

// DefaultConcurrentFlusherConfig provides reasonable defaults.
var DefaultConcurrentFlusherConfig = ConcurrentFlusherConfig{
	// Adaptive settings
	BaseInterval:      500 * time.Millisecond,
	MinInterval:       50 * time.Millisecond,
	MaxInterval:       2000 * time.Millisecond,
	HighLoadThreshold: 10000,
	LowLoadThreshold:  100,

	// Worker settings
	WorkerCount:     4,
	MaxDocsPerPage:  100,
	MaxBytesPerPage: 1 * 1024 * 1024, // 1MB
	QueueSize:       1000,

	// Flush thresholds
	FlushThreshold: 1000,
	MaxBufferSize:  64 * 1024 * 1024, // 64MB
}

// ConcurrentFlusher provides a complete concurrent write solution.
// It coordinates adaptive timing, dirty page tracking, and parallel workers.
type ConcurrentFlusher struct {
	config ConcurrentFlusherConfig
	logger *zap.SugaredLogger

	// Components
	adaptive   *AdaptiveFlusher
	tracker    *DirtyPageTracker
	workerPool *FlushWorkerPool
	writer     PageWriter

	// Bundle-level tracking for flush coordination
	bundleBuffer map[string]*bundleFlushState
	bundleMu     sync.RWMutex

	// Lifecycle
	ctx     context.Context
	cancel  context.CancelFunc
	started atomic.Bool
	stopped atomic.Bool
	wg      sync.WaitGroup
}

// bundleFlushState tracks pending write state for a bundle.
type bundleFlushState struct {
	docCount  int
	byteSize  int64
	lastWrite time.Time
	lastFlush time.Time
}

// NewConcurrentFlusher creates a new concurrent flusher.
func NewConcurrentFlusher(config ConcurrentFlusherConfig, writer PageWriter, logger *zap.SugaredLogger) *ConcurrentFlusher {
	ctx, cancel := context.WithCancel(context.Background())

	cf := &ConcurrentFlusher{
		config:       config,
		logger:       logger,
		writer:       writer,
		bundleBuffer: make(map[string]*bundleFlushState),
		ctx:          ctx,
		cancel:       cancel,
	}

	// Create dirty page tracker
	trackerConfig := DirtyPageTrackerConfig{
		WorkerCount:     config.WorkerCount,
		MaxDocsPerPage:  config.MaxDocsPerPage,
		MaxBytesPerPage: config.MaxBytesPerPage,
		QueueSize:       config.QueueSize,
	}
	cf.tracker = NewDirtyPageTracker(trackerConfig)

	// Create worker pool
	cf.workerPool = NewFlushWorkerPool(config.WorkerCount, cf.tracker, writer, logger)

	// Create adaptive flusher with bundle flush callback
	adaptiveConfig := AdaptiveConfig{
		BaseInterval:      config.BaseInterval,
		MinInterval:       config.MinInterval,
		MaxInterval:       config.MaxInterval,
		HighLoadThreshold: config.HighLoadThreshold,
		LowLoadThreshold:  config.LowLoadThreshold,
		FlushThreshold:    config.FlushThreshold,
		MaxBufferSize:     config.MaxBufferSize,
		WorkerCount:       config.WorkerCount,
	}
	cf.adaptive = NewAdaptiveFlusher(adaptiveConfig, cf.flushBundle, logger)

	return cf
}

// Start begins background flushing operations.
func (cf *ConcurrentFlusher) Start() {
	if !cf.started.CompareAndSwap(false, true) {
		return
	}

	cf.logger.Info("Starting concurrent flusher",
		"workers", cf.config.WorkerCount,
		"minInterval", cf.config.MinInterval,
		"maxInterval", cf.config.MaxInterval)

	cf.workerPool.Start()
	cf.adaptive.Start()

	// Start the bundle coordination goroutine
	cf.wg.Add(1)
	go cf.coordinateFlushes()
}

// Stop gracefully shuts down all flushing operations.
func (cf *ConcurrentFlusher) Stop() {
	if !cf.stopped.CompareAndSwap(false, true) {
		return
	}

	cf.logger.Info("Stopping concurrent flusher...")
	cf.cancel()

	// Stop adaptive flusher first (stops triggering new flushes)
	cf.adaptive.Stop()

	// Wait for coordination to finish
	cf.wg.Wait()

	// Flush any remaining dirty pages
	cf.flushAllBundles()

	// Stop worker pool (drains remaining work)
	cf.workerPool.Stop()

	cf.logger.Info("Concurrent flusher stopped")
}

// MarkDirty records a document write for background flushing.
// Returns true if the page should be flushed immediately.
func (cf *ConcurrentFlusher) MarkDirty(bundleName string, pageID uint32, doc interface{}, estimatedBytes int64) bool {
	// We need to cast to *models.Document, but use interface{} in signature
	// to avoid import cycles. Caller must pass *models.Document.
	// TODO: Consider using a DocumentRef struct to avoid this.

	// Track at bundle level for adaptive flushing
	cf.bundleMu.Lock()
	state, exists := cf.bundleBuffer[bundleName]
	if !exists {
		state = &bundleFlushState{}
		cf.bundleBuffer[bundleName] = state
	}
	state.docCount++
	state.byteSize += estimatedBytes
	state.lastWrite = time.Now()
	bundleDocCount := state.docCount
	bundleByteSize := state.byteSize
	cf.bundleMu.Unlock()

	// Record for adaptive rate tracking
	cf.adaptive.RecordDocuments(bundleName, 1)
	cf.adaptive.RecordBytes(bundleName, estimatedBytes)

	// Check bundle-level thresholds
	if bundleDocCount >= cf.config.FlushThreshold || bundleByteSize >= cf.config.MaxBufferSize {
		cf.adaptive.RequestFlush(bundleName)
		return true
	}

	return false
}

// MarkPageDirty records a document write with page-level tracking.
// schema is required for encoding document Values; pass bundle.DocumentStructure.FieldSchema().
func (cf *ConcurrentFlusher) MarkPageDirty(bundleName string, pageID uint32, doc interface{}, estimatedBytes int64, schema *models.BundleFieldSchema) bool {
	cf.MarkDirty(bundleName, pageID, doc, estimatedBytes)

	var docPtr *models.Document
	if doc != nil {
		if d, ok := doc.(*models.Document); ok {
			docPtr = d
		}
	}

	shouldFlush := cf.tracker.MarkDirty(bundleName, pageID, docPtr, estimatedBytes, schema)

	if shouldFlush {
		cf.tracker.EnqueueForFlush(bundleName, pageID)
	}

	return shouldFlush
}

// RequestFlush requests an immediate flush for a bundle.
func (cf *ConcurrentFlusher) RequestFlush(bundleName string) {
	cf.adaptive.RequestFlush(bundleName)
}

// flushBundle is called by the adaptive flusher when a bundle needs flushing.
func (cf *ConcurrentFlusher) flushBundle(bundleName string) error {
	// Clear bundle-level tracking
	cf.bundleMu.Lock()
	if state, exists := cf.bundleBuffer[bundleName]; exists {
		state.docCount = 0
		state.byteSize = 0
		state.lastFlush = time.Now()
	}
	cf.bundleMu.Unlock()

	// Enqueue all dirty pages for this bundle
	count := cf.tracker.FlushAllForBundle(bundleName)
	if count > 0 {
		cf.logger.Debug("Flushing bundle",
			"bundle", bundleName,
			"pages", count)
	}

	return nil
}

// flushAllBundles flushes all pending bundles during shutdown.
func (cf *ConcurrentFlusher) flushAllBundles() {
	cf.bundleMu.RLock()
	bundles := make([]string, 0, len(cf.bundleBuffer))
	for bundle := range cf.bundleBuffer {
		bundles = append(bundles, bundle)
	}
	cf.bundleMu.RUnlock()

	for _, bundle := range bundles {
		cf.flushBundle(bundle)
	}
}

// coordinateFlushes runs the main coordination loop.
func (cf *ConcurrentFlusher) coordinateFlushes() {
	defer cf.wg.Done()

	ticker := time.NewTicker(cf.adaptive.GetCurrentInterval())
	defer ticker.Stop()

	for {
		select {
		case <-cf.ctx.Done():
			return

		case <-ticker.C:
			// Adjust ticker to match adaptive interval
			interval := cf.adaptive.GetCurrentInterval()
			ticker.Reset(interval)

			// Flush stale bundles
			cf.flushStaleBundles()
		}
	}
}

// flushStaleBundles flushes bundles that haven't been flushed recently.
func (cf *ConcurrentFlusher) flushStaleBundles() {
	cf.bundleMu.RLock()
	now := time.Now()
	interval := cf.adaptive.GetCurrentInterval()
	staleBundles := make([]string, 0)

	for bundle, state := range cf.bundleBuffer {
		if state.docCount > 0 && now.Sub(state.lastWrite) >= interval {
			staleBundles = append(staleBundles, bundle)
		}
	}
	cf.bundleMu.RUnlock()

	for _, bundle := range staleBundles {
		cf.adaptive.RequestFlush(bundle)
	}
}

// ConcurrentFlusherStats contains comprehensive flusher statistics.
type ConcurrentFlusherStats struct {
	// Adaptive flusher stats
	CurrentInterval time.Duration
	CurrentRate     float64

	// Dirty page stats
	DirtyPageCount uint64
	PendingDocs    uint64

	// Worker pool stats
	WorkerStats FlushWorkerPoolStats

	// Bundle stats
	BundleCount int
}

// GetStats returns current flusher statistics.
func (cf *ConcurrentFlusher) GetStats() ConcurrentFlusherStats {
	cf.bundleMu.RLock()
	bundleCount := len(cf.bundleBuffer)
	cf.bundleMu.RUnlock()

	trackerStats := cf.tracker.GetStats()
	workerStats := cf.workerPool.GetAggregateStats()

	return ConcurrentFlusherStats{
		CurrentInterval: cf.adaptive.GetCurrentInterval(),
		CurrentRate:     cf.adaptive.GetCurrentRate(),
		DirtyPageCount:  trackerStats.DirtyPageCount,
		PendingDocs:     trackerStats.PendingDocs,
		WorkerStats:     workerStats,
		BundleCount:     bundleCount,
	}
}

// GetTracker returns the dirty page tracker for direct page-level operations.
func (cf *ConcurrentFlusher) GetTracker() *DirtyPageTracker {
	return cf.tracker
}
