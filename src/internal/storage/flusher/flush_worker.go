// Package flusher provides concurrent flush workers for background page writes.
//
// FlushWorker processes dirty pages from a queue and writes them to disk
// using batch encoding for efficiency. Multiple workers can run in parallel,
// each handling a partition of pages to avoid contention.
//
// Key features:
// - Batch encoding of documents for efficient I/O
// - Per-page locking to avoid blocking concurrent writes
// - Integration with DirtyPageTracker for coordination
// - Graceful shutdown with pending flush completion
package flusher

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"syndrdb/src/pkg/common/helpers"

	"go.uber.org/zap"
)

// PageWriter is the interface for writing page data to storage.
// This abstraction allows FlushWorker to work with different storage backends.
// Implementers may no-op WriteIndexUpdates if index updates are applied elsewhere.
type PageWriter interface {
	// WritePageBatch writes a batch of encoded documents to a page.
	WritePageBatch(bundleName string, pageID uint32, data []byte) error
	// WriteIndexUpdates writes pending index updates for a page.
	// Called after WritePageBatch when the page has index updates.
	WriteIndexUpdates(bundleName string, pageID uint32, updates []IndexUpdate) error
}

// FlushWorker processes dirty pages and writes them to disk.
type FlushWorker struct {
	id      int
	tracker *DirtyPageTracker
	writer  PageWriter
	logger  *zap.SugaredLogger

	// Lifecycle
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// Statistics
	pagesWritten   atomic.Uint64
	docsWritten    atomic.Uint64
	bytesWritten   atomic.Uint64
	writeErrors    atomic.Uint64
	avgWriteTimeNs atomic.Uint64
}

// NewFlushWorker creates a new flush worker.
func NewFlushWorker(id int, tracker *DirtyPageTracker, writer PageWriter, logger *zap.SugaredLogger) *FlushWorker {
	ctx, cancel := context.WithCancel(context.Background())

	return &FlushWorker{
		id:      id,
		tracker: tracker,
		writer:  writer,
		logger:  logger,
		ctx:     ctx,
		cancel:  cancel,
	}
}

// Start begins processing dirty pages from the queue.
func (fw *FlushWorker) Start() {
	fw.wg.Add(1)
	go fw.run()
}

// Stop gracefully shuts down the worker, waiting for pending writes.
func (fw *FlushWorker) Stop() {
	fw.cancel()
	fw.wg.Wait()
}

// run is the main worker loop.
func (fw *FlushWorker) run() {
	defer fw.wg.Done()

	queue := fw.tracker.GetPagesForWorker(fw.id)
	if queue == nil {
		fw.logger.Error("Invalid worker ID", "id", fw.id)
		return
	}

	fw.logger.Debug("Flush worker started", "id", fw.id)

	for {
		select {
		case <-fw.ctx.Done():
			// Drain remaining pages before exit
			fw.drainQueue(queue)
			fw.logger.Debug("Flush worker stopped", "id", fw.id)
			return

		case page, ok := <-queue:
			if !ok {
				return // Queue closed
			}
			fw.flushPage(page)
		}
	}
}

// drainQueue flushes all remaining pages in the queue during shutdown.
func (fw *FlushWorker) drainQueue(queue <-chan *DirtyPage) {
	for {
		select {
		case page, ok := <-queue:
			if !ok {
				return
			}
			fw.flushPage(page)
		default:
			return // Queue empty
		}
	}
}

// flushPage writes a dirty page to disk using batch encoding.
func (fw *FlushWorker) flushPage(page *DirtyPage) {
	startTime := time.Now()

	page.mu.Lock()
	docCount := len(page.Documents)
	indexUpdates := make([]IndexUpdate, len(page.IndexUpdates))
	copy(indexUpdates, page.IndexUpdates)
	if docCount == 0 && len(indexUpdates) == 0 {
		page.mu.Unlock()
		return // Nothing to flush
	}

	if page.Schema == nil {
		fw.logger.Error("Cannot flush page without schema (Values-based documents require schema)",
			"worker", fw.id,
			"bundle", page.BundleName,
			"page", page.PageID)
		fw.writeErrors.Add(1)
		page.mu.Unlock()
		return
	}

	encoder := helpers.GetBatchEncoder()
	if docCount >= 8 {
		if err := encoder.AddDocumentsParallel(page.Documents, page.Schema, 8, 0); err != nil {
			fw.logger.Error("Failed to encode documents in parallel",
				"worker", fw.id,
				"bundle", page.BundleName,
				"page", page.PageID,
				"docCount", docCount,
				"error", err)
			fw.writeErrors.Add(1)
		}
	} else {
		for _, doc := range page.Documents {
			if err := encoder.AddDocument(doc, page.Schema); err != nil {
				fw.logger.Error("Failed to encode document",
					"worker", fw.id,
					"bundle", page.BundleName,
					"page", page.PageID,
					"docID", doc.DocumentID,
					"error", err)
				fw.writeErrors.Add(1)
			}
		}
	}

	// Get encoded data before unlocking
	data := encoder.Bytes()
	byteCount := len(data)

	// Clear page documents and index updates (we've captured them)
	page.Documents = page.Documents[:0]
	page.IndexUpdates = page.IndexUpdates[:0]
	page.WriteCount = 0
	page.ByteSize = 0
	page.mu.Unlock()

	// Return encoder to pool
	helpers.PutBatchEncoder(encoder)

	// Write to storage (outside page lock)
	if byteCount > 0 {
		if err := fw.writer.WritePageBatch(page.BundleName, page.PageID, data); err != nil {
			fw.logger.Error("Failed to write page",
				"worker", fw.id,
				"bundle", page.BundleName,
				"page", page.PageID,
				"error", err)
			fw.writeErrors.Add(1)
		} else {
			// Update statistics
			fw.pagesWritten.Add(1)
			fw.docsWritten.Add(uint64(docCount))
			fw.bytesWritten.Add(uint64(byteCount))

			// Update average write time (exponential moving average)
			writeTimeNs := uint64(time.Since(startTime).Nanoseconds())
			oldAvg := fw.avgWriteTimeNs.Load()
			if oldAvg == 0 {
				fw.avgWriteTimeNs.Store(writeTimeNs)
			} else {
				// EMA with alpha = 0.1
				newAvg := (oldAvg*9 + writeTimeNs) / 10
				fw.avgWriteTimeNs.Store(newAvg)
			}
		}
	}

	if len(indexUpdates) > 0 {
		if err := fw.writer.WriteIndexUpdates(page.BundleName, page.PageID, indexUpdates); err != nil {
			fw.logger.Error("Failed to write index updates",
				"worker", fw.id,
				"bundle", page.BundleName,
				"page", page.PageID,
				"error", err)
			fw.writeErrors.Add(1)
		}
	}

	// Clear from tracker
	fw.tracker.ClearPage(page)
}

// FlushWorkerStats contains worker statistics.
type FlushWorkerStats struct {
	WorkerID       int
	PagesWritten   uint64
	DocsWritten    uint64
	BytesWritten   uint64
	WriteErrors    uint64
	AvgWriteTimeNs uint64
}

// GetStats returns current worker statistics.
func (fw *FlushWorker) GetStats() FlushWorkerStats {
	return FlushWorkerStats{
		WorkerID:       fw.id,
		PagesWritten:   fw.pagesWritten.Load(),
		DocsWritten:    fw.docsWritten.Load(),
		BytesWritten:   fw.bytesWritten.Load(),
		WriteErrors:    fw.writeErrors.Load(),
		AvgWriteTimeNs: fw.avgWriteTimeNs.Load(),
	}
}

// FlushWorkerPool manages a pool of flush workers.
type FlushWorkerPool struct {
	workers []*FlushWorker
	tracker *DirtyPageTracker
	logger  *zap.SugaredLogger
	started atomic.Bool
	stopped atomic.Bool
}

// NewFlushWorkerPool creates a pool of flush workers.
func NewFlushWorkerPool(workerCount int, tracker *DirtyPageTracker, writer PageWriter, logger *zap.SugaredLogger) *FlushWorkerPool {
	pool := &FlushWorkerPool{
		workers: make([]*FlushWorker, workerCount),
		tracker: tracker,
		logger:  logger,
	}

	for i := 0; i < workerCount; i++ {
		pool.workers[i] = NewFlushWorker(i, tracker, writer, logger)
	}

	return pool
}

// Start begins all workers in the pool.
func (p *FlushWorkerPool) Start() {
	if !p.started.CompareAndSwap(false, true) {
		return
	}

	p.logger.Info("Starting flush worker pool", "workers", len(p.workers))
	for _, worker := range p.workers {
		worker.Start()
	}
}

// Stop gracefully shuts down all workers.
func (p *FlushWorkerPool) Stop() {
	if !p.stopped.CompareAndSwap(false, true) {
		return
	}

	p.logger.Info("Stopping flush worker pool...")

	// Close tracker queues first to signal workers
	p.tracker.Close()

	// Wait for all workers to finish
	for _, worker := range p.workers {
		worker.Stop()
	}

	p.logger.Info("Flush worker pool stopped")
}

// GetStats returns statistics for all workers.
func (p *FlushWorkerPool) GetStats() []FlushWorkerStats {
	stats := make([]FlushWorkerStats, len(p.workers))
	for i, worker := range p.workers {
		stats[i] = worker.GetStats()
	}
	return stats
}

// GetAggregateStats returns combined statistics across all workers.
type FlushWorkerPoolStats struct {
	WorkerCount    int
	TotalPages     uint64
	TotalDocs      uint64
	TotalBytes     uint64
	TotalErrors    uint64
	AvgWriteTimeNs uint64
}

// GetAggregateStats returns combined statistics across all workers.
func (p *FlushWorkerPool) GetAggregateStats() FlushWorkerPoolStats {
	var stats FlushWorkerPoolStats
	stats.WorkerCount = len(p.workers)

	var totalWriteTime uint64
	var writeTimeCount uint64

	for _, worker := range p.workers {
		ws := worker.GetStats()
		stats.TotalPages += ws.PagesWritten
		stats.TotalDocs += ws.DocsWritten
		stats.TotalBytes += ws.BytesWritten
		stats.TotalErrors += ws.WriteErrors
		if ws.AvgWriteTimeNs > 0 {
			totalWriteTime += ws.AvgWriteTimeNs
			writeTimeCount++
		}
	}

	if writeTimeCount > 0 {
		stats.AvgWriteTimeNs = totalWriteTime / writeTimeCount
	}

	return stats
}
