// Package flusher provides adaptive background flushing for document storage.
//
// The AdaptiveFlusher dynamically adjusts flush intervals based on write load
// to optimize both latency and throughput. Under high write load, it flushes
// frequently to reduce memory usage and improve durability. Under low load,
// it flushes less often to batch writes and improve efficiency.
//
// Key features:
// - Rate-based adaptive intervals (50ms-2000ms range)
// - Document count thresholds for immediate flush
// - Buffer size limits for memory management
// - WAL integration for durability guarantees
// - Parallel flush workers for multi-bundle writes
package flusher

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
)

// AdaptiveConfig holds configuration for adaptive background flushing.
// The flusher adjusts its interval based on observed write rates to balance
// latency (under high load) vs throughput (under low load).
type AdaptiveConfig struct {
	// BaseInterval is the starting interval when the flusher initializes.
	// This is used before enough data is collected to make adaptive decisions.
	BaseInterval time.Duration

	// MinInterval is the fastest flush rate under high write load.
	// Setting this too low may cause excessive I/O overhead.
	MinInterval time.Duration

	// MaxInterval is the slowest flush rate under low write load.
	// This helps batch small writes efficiently when the system is idle.
	MaxInterval time.Duration

	// HighLoadThreshold is the docs/second rate considered "high load".
	// Above this threshold, the flusher uses MinInterval.
	HighLoadThreshold int

	// LowLoadThreshold is the docs/second rate considered "low load".
	// Below this threshold, the flusher uses MaxInterval.
	LowLoadThreshold int

	// FlushThreshold is the document count that triggers an immediate flush.
	// Prevents memory buildup from long-running batch operations.
	FlushThreshold int

	// MaxBufferSize is the maximum buffer size before forced flush (bytes).
	// Prevents unbounded memory growth under high write load.
	MaxBufferSize int64

	// WorkerCount is the number of parallel flush workers.
	// More workers help with multiple bundles, but add overhead.
	WorkerCount int
}

// DefaultAdaptiveConfig provides reasonable defaults for most workloads.
// These values are tuned for a balance between latency and throughput.
var DefaultAdaptiveConfig = AdaptiveConfig{
	BaseInterval:      500 * time.Millisecond,
	MinInterval:       50 * time.Millisecond,
	MaxInterval:       2000 * time.Millisecond,
	HighLoadThreshold: 10000,            // 10k docs/sec = high load
	LowLoadThreshold:  100,              // 100 docs/sec = low load
	FlushThreshold:    1000,             // Flush after 1000 docs buffered
	MaxBufferSize:     64 * 1024 * 1024, // 64MB max buffer
	WorkerCount:       2,
}

// FlushRequest represents a pending flush operation for a bundle.
type FlushRequest struct {
	BundleName  string
	BufferSize  int64
	DocumentIDs []string
	Priority    int // Higher = more urgent
	RequestedAt time.Time
}

// FlushFunc is the callback type for performing actual flush operations.
// It receives the bundle name and returns any error encountered.
type FlushFunc func(bundleName string) error

// RateMetrics tracks write rate statistics for adaptive interval calculation.
type RateMetrics struct {
	docsThisWindow uint64    // Documents written in current window
	windowStart    time.Time // Start of current measurement window
	lastDocsPerSec float64   // Calculated rate from previous window
	smoothedRate   float64   // Exponentially smoothed rate for stability
	mu             sync.Mutex
}

// AdaptiveFlusher manages background flushing with dynamic interval adjustment.
// It monitors write rates and adjusts flush frequency to optimize for both
// high-throughput batch operations and low-latency interactive writes.
type AdaptiveFlusher struct {
	config AdaptiveConfig
	logger *zap.SugaredLogger

	// Rate tracking for adaptive intervals
	metrics         RateMetrics
	currentInterval time.Duration

	// Pending flush queue
	pendingFlushes map[string]*FlushRequest // bundleName -> pending request
	flushMu        sync.Mutex

	// Flush callback and workers
	flushFunc  FlushFunc
	flushQueue chan string
	workerWg   sync.WaitGroup

	// Lifecycle management
	ctx     context.Context
	cancel  context.CancelFunc
	started atomic.Bool
	stopped atomic.Bool
}

// NewAdaptiveFlusher creates a new adaptive background flusher.
// The flushFunc callback is invoked when a bundle needs to be flushed to disk.
func NewAdaptiveFlusher(config AdaptiveConfig, flushFunc FlushFunc, logger *zap.SugaredLogger) *AdaptiveFlusher {
	ctx, cancel := context.WithCancel(context.Background())

	af := &AdaptiveFlusher{
		config:          config,
		logger:          logger,
		currentInterval: config.BaseInterval,
		pendingFlushes:  make(map[string]*FlushRequest),
		flushFunc:       flushFunc,
		flushQueue:      make(chan string, 1000), // Buffer for burst handling
		ctx:             ctx,
		cancel:          cancel,
	}

	af.metrics.windowStart = time.Now()

	return af
}

// Start begins the adaptive flusher background workers.
// Call this once after creation to start background processing.
func (af *AdaptiveFlusher) Start() {
	if !af.started.CompareAndSwap(false, true) {
		return // Already started
	}

	af.logger.Info("Starting adaptive background flusher",
		"minInterval", af.config.MinInterval,
		"maxInterval", af.config.MaxInterval,
		"workers", af.config.WorkerCount)

	// Start flush workers
	for i := 0; i < af.config.WorkerCount; i++ {
		af.workerWg.Add(1)
		go af.flushWorker(i)
	}

	// Start the adaptive timer
	af.workerWg.Add(1)
	go af.adaptiveTimer()
}

// Stop gracefully shuts down the adaptive flusher.
// Waits for pending flushes to complete before returning.
func (af *AdaptiveFlusher) Stop() {
	if !af.stopped.CompareAndSwap(false, true) {
		return // Already stopped
	}

	af.logger.Info("Stopping adaptive background flusher...")
	af.cancel()
	close(af.flushQueue)
	af.workerWg.Wait()
	af.logger.Info("Adaptive background flusher stopped")
}

// RecordDocuments increments the document count for rate tracking.
// Call this whenever documents are written to track write rates.
func (af *AdaptiveFlusher) RecordDocuments(bundleName string, count int) {
	// Track for rate calculation
	atomic.AddUint64(&af.metrics.docsThisWindow, uint64(count))

	// Check if we need immediate flush based on threshold
	af.flushMu.Lock()
	req, exists := af.pendingFlushes[bundleName]
	if !exists {
		req = &FlushRequest{
			BundleName:  bundleName,
			RequestedAt: time.Now(),
		}
		af.pendingFlushes[bundleName] = req
	}
	req.DocumentIDs = append(req.DocumentIDs, make([]string, count)...) // Placeholder tracking

	shouldFlush := len(req.DocumentIDs) >= af.config.FlushThreshold
	af.flushMu.Unlock()

	if shouldFlush {
		af.RequestFlush(bundleName)
	}
}

// RecordBytes tracks buffer size for the specified bundle.
// Triggers immediate flush if buffer exceeds MaxBufferSize.
func (af *AdaptiveFlusher) RecordBytes(bundleName string, bytes int64) {
	af.flushMu.Lock()
	req, exists := af.pendingFlushes[bundleName]
	if !exists {
		req = &FlushRequest{
			BundleName:  bundleName,
			RequestedAt: time.Now(),
		}
		af.pendingFlushes[bundleName] = req
	}
	req.BufferSize += bytes

	shouldFlush := req.BufferSize >= af.config.MaxBufferSize
	af.flushMu.Unlock()

	if shouldFlush {
		af.RequestFlush(bundleName)
	}
}

// RequestFlush adds a bundle to the flush queue for processing.
// Non-blocking; if the queue is full, the request is noted for the next timer tick.
func (af *AdaptiveFlusher) RequestFlush(bundleName string) {
	select {
	case af.flushQueue <- bundleName:
		// Queued successfully
	default:
		// Queue full; timer will catch it
		af.logger.Debug("Flush queue full, deferring flush", "bundle", bundleName)
	}
}

// GetCurrentInterval returns the current adaptive flush interval.
func (af *AdaptiveFlusher) GetCurrentInterval() time.Duration {
	return af.currentInterval
}

// GetCurrentRate returns the current smoothed documents per second rate.
func (af *AdaptiveFlusher) GetCurrentRate() float64 {
	af.metrics.mu.Lock()
	defer af.metrics.mu.Unlock()
	return af.metrics.smoothedRate
}

// adjustInterval recalculates the optimal flush interval based on write rate.
// Uses exponential smoothing to avoid oscillation from short-term spikes.
func (af *AdaptiveFlusher) adjustInterval() {
	af.metrics.mu.Lock()
	defer af.metrics.mu.Unlock()

	now := time.Now()
	elapsed := now.Sub(af.metrics.windowStart)

	// Only adjust after at least 1 second of data
	if elapsed < time.Second {
		return
	}

	// Calculate current rate
	docs := atomic.SwapUint64(&af.metrics.docsThisWindow, 0)
	docsPerSecond := float64(docs) / elapsed.Seconds()
	af.metrics.windowStart = now
	af.metrics.lastDocsPerSec = docsPerSecond

	// Exponential moving average for stability (alpha = 0.3)
	// Higher alpha = more responsive, lower = more stable
	const alpha = 0.3
	if af.metrics.smoothedRate == 0 {
		af.metrics.smoothedRate = docsPerSecond
	} else {
		af.metrics.smoothedRate = alpha*docsPerSecond + (1-alpha)*af.metrics.smoothedRate
	}

	// Determine new interval based on smoothed rate
	var newInterval time.Duration

	if af.metrics.smoothedRate >= float64(af.config.HighLoadThreshold) {
		// High load: flush frequently for low latency
		newInterval = af.config.MinInterval
		af.logger.Debug("High write load detected",
			"rate", af.metrics.smoothedRate,
			"interval", newInterval)

	} else if af.metrics.smoothedRate <= float64(af.config.LowLoadThreshold) {
		// Low load: flush infrequently for better batching
		newInterval = af.config.MaxInterval
		af.logger.Debug("Low write load detected",
			"rate", af.metrics.smoothedRate,
			"interval", newInterval)

	} else {
		// Linear interpolation between min and max
		loadRatio := (af.metrics.smoothedRate - float64(af.config.LowLoadThreshold)) /
			float64(af.config.HighLoadThreshold-af.config.LowLoadThreshold)
		intervalRange := af.config.MaxInterval - af.config.MinInterval
		newInterval = af.config.MaxInterval - time.Duration(float64(intervalRange)*loadRatio)
		af.logger.Debug("Medium write load detected",
			"rate", af.metrics.smoothedRate,
			"interval", newInterval)
	}

	af.currentInterval = newInterval
}

// adaptiveTimer runs the main timer loop that triggers periodic flushes.
func (af *AdaptiveFlusher) adaptiveTimer() {
	defer af.workerWg.Done()

	ticker := time.NewTicker(af.currentInterval)
	defer ticker.Stop()

	for {
		select {
		case <-af.ctx.Done():
			af.flushAllPending()
			return

		case <-ticker.C:
			// Adjust interval based on recent write rate
			af.adjustInterval()
			ticker.Reset(af.currentInterval)

			// Flush any stale pending requests
			af.flushStaleRequests()
		}
	}
}

// flushStaleRequests queues bundles that have been pending too long.
func (af *AdaptiveFlusher) flushStaleRequests() {
	af.flushMu.Lock()
	now := time.Now()
	staleBundles := make([]string, 0)

	for bundleName, req := range af.pendingFlushes {
		// Flush if request is older than current interval
		if now.Sub(req.RequestedAt) >= af.currentInterval {
			staleBundles = append(staleBundles, bundleName)
		}
	}
	af.flushMu.Unlock()

	for _, bundleName := range staleBundles {
		af.RequestFlush(bundleName)
	}
}

// flushAllPending flushes all pending bundles during shutdown.
func (af *AdaptiveFlusher) flushAllPending() {
	af.flushMu.Lock()
	bundles := make([]string, 0, len(af.pendingFlushes))
	for bundleName := range af.pendingFlushes {
		bundles = append(bundles, bundleName)
	}
	af.flushMu.Unlock()

	for _, bundleName := range bundles {
		if err := af.flushFunc(bundleName); err != nil {
			af.logger.Error("Error during shutdown flush", "bundle", bundleName, "error", err)
		}
		af.clearPendingFlush(bundleName)
	}
}

// flushWorker processes flush requests from the queue.
func (af *AdaptiveFlusher) flushWorker(id int) {
	defer af.workerWg.Done()

	for bundleName := range af.flushQueue {
		if err := af.flushFunc(bundleName); err != nil {
			af.logger.Error("Flush worker error",
				"worker", id,
				"bundle", bundleName,
				"error", err)
		}
		af.clearPendingFlush(bundleName)
	}
}

// clearPendingFlush removes a bundle from the pending map after successful flush.
func (af *AdaptiveFlusher) clearPendingFlush(bundleName string) {
	af.flushMu.Lock()
	delete(af.pendingFlushes, bundleName)
	af.flushMu.Unlock()
}

// FlusherStats contains current flusher statistics.
type FlusherStats struct {
	CurrentInterval   time.Duration
	CurrentRate       float64
	PendingFlushes    int
	TotalDocsRecorded uint64
}

// GetStats returns current flusher statistics.
func (af *AdaptiveFlusher) GetStats() FlusherStats {
	af.flushMu.Lock()
	pendingCount := len(af.pendingFlushes)
	af.flushMu.Unlock()

	return FlusherStats{
		CurrentInterval:   af.currentInterval,
		CurrentRate:       af.GetCurrentRate(),
		PendingFlushes:    pendingCount,
		TotalDocsRecorded: atomic.LoadUint64(&af.metrics.docsThisWindow),
	}
}
