package journal

import (
	"context"
	"fmt"
	"sync"
	"time"

	"syndrdb/src/internal/async"
	"syndrdb/src/pkg/settings"
)

// getSettingsFunc allows dependency injection for testing (can be overridden)
var getSettingsFunc = func() *settings.Arguments {
	return settings.GetSettings()
}

// WALOperation implements the AsyncOperation interface for WAL entries
type WALOperation struct {
	entry      WALEntry
	sequence   uint64
	priority   int
	opType     string
	walManager *WALManager
	resultChan chan error // Channel to send result back
}

// Execute implements AsyncOperation interface
func (wo *WALOperation) Execute(ctx context.Context) error {
	// Use ExecuteWithLogging for transaction management and WAL writing
	err := wo.walManager.ExecuteWithLogging(func(txID string) error {
		// Override the transaction ID in the entry if needed
		entryTxID := wo.entry.TxID
		if entryTxID == "" {
			entryTxID = txID
		}

		// Dispatch to appropriate logging method based on operation type
		switch wo.entry.Operation {
		case OpInsert:
			return wo.walManager.LogDocumentInsert(entryTxID, wo.entry.BundleName, wo.entry.DocumentID, wo.entry.AfterData)
		case OpUpdate:
			return wo.walManager.LogDocumentUpdate(entryTxID, wo.entry.BundleName, wo.entry.DocumentID, wo.entry.BeforeData, wo.entry.AfterData)
		case OpDelete:
			return wo.walManager.LogDocumentDelete(entryTxID, wo.entry.BundleName, wo.entry.DocumentID, wo.entry.BeforeData)
		case OpCreateBundle:
			return wo.walManager.LogBundleCreate(entryTxID, wo.entry.BundleName, wo.entry.Metadata)
		case OpDeleteBundle:
			return wo.walManager.LogBundleDelete(entryTxID, wo.entry.BundleName, wo.entry.Metadata)
		case OpCreateIndex:
			return wo.walManager.LogIndexCreate(entryTxID, wo.entry.BundleName, wo.entry.DocumentID, wo.entry.AfterData)
		case OpDropIndex:
			return wo.walManager.LogIndexDrop(entryTxID, wo.entry.BundleName, wo.entry.DocumentID, wo.entry.BeforeData)
		default:
			return fmt.Errorf("unsupported async WAL operation: %d", wo.entry.Operation)
		}
	})

	// HIGH-008: Ensure result channel always receives error (even on context cancellation)
	// This prevents callers from hanging waiting for a result
	select {
	case wo.resultChan <- err:
		// Successfully sent error to channel
	case <-ctx.Done():
		// Context cancelled - still send the error to channel if possible
		// This ensures the caller doesn't hang waiting for a result
		select {
		case wo.resultChan <- ctx.Err():
		default:
			// Channel is full or closed - error will be lost but context error is returned
		}
		return ctx.Err()
	}

	return err
}

// GetSequence implements AsyncOperation interface
func (wo *WALOperation) GetSequence() uint64 {
	return wo.sequence
}

// GetPriority implements AsyncOperation interface
func (wo *WALOperation) GetPriority() int {
	return wo.priority
}

// GetType implements AsyncOperation interface
func (wo *WALOperation) GetType() string {
	return wo.opType
}

// WALBatchOperation implements the AsyncOperation interface for batch WAL entries
type WALBatchOperation struct {
	entries    []WALEntry
	sequence   uint64
	priority   int
	walManager *WALManager
	resultChan chan error // Channel to send result back
}

// Execute implements AsyncOperation interface for batch operations
func (wbo *WALBatchOperation) Execute(ctx context.Context) error {
	// Process batch within a single transaction for consistency
	err := wbo.walManager.ExecuteWithLogging(func(txID string) error {
		for i, entry := range wbo.entries {
			// Override transaction ID for consistency in batch
			entryTxID := entry.TxID
			if entryTxID == "" {
				entryTxID = txID
			}

			var entryErr error
			switch entry.Operation {
			case OpInsert:
				entryErr = wbo.walManager.LogDocumentInsert(entryTxID, entry.BundleName, entry.DocumentID, entry.AfterData)
			case OpUpdate:
				entryErr = wbo.walManager.LogDocumentUpdate(entryTxID, entry.BundleName, entry.DocumentID, entry.BeforeData, entry.AfterData)
			case OpDelete:
				entryErr = wbo.walManager.LogDocumentDelete(entryTxID, entry.BundleName, entry.DocumentID, entry.BeforeData)
			case OpCreateBundle:
				entryErr = wbo.walManager.LogBundleCreate(entryTxID, entry.BundleName, entry.Metadata)
			case OpDeleteBundle:
				entryErr = wbo.walManager.LogBundleDelete(entryTxID, entry.BundleName, entry.Metadata)
			case OpCreateIndex:
				entryErr = wbo.walManager.LogIndexCreate(entryTxID, entry.BundleName, entry.DocumentID, entry.AfterData)
			case OpDropIndex:
				entryErr = wbo.walManager.LogIndexDrop(entryTxID, entry.BundleName, entry.DocumentID, entry.BeforeData)
			default:
				entryErr = fmt.Errorf("unsupported batch WAL operation: %d", entry.Operation)
			}

			if entryErr != nil {
				return fmt.Errorf("failed to process batch entry %d: %w", i, entryErr)
			}
		}
		return nil
	})

	// Send result back through channel
	select {
	case wbo.resultChan <- err:
	case <-ctx.Done():
		return ctx.Err()
	}

	return err
}

// GetSequence implements AsyncOperation interface
func (wbo *WALBatchOperation) GetSequence() uint64 {
	return wbo.sequence
}

// GetPriority implements AsyncOperation interface
func (wbo *WALBatchOperation) GetPriority() int {
	return wbo.priority
}

// GetType implements AsyncOperation interface
func (wbo *WALBatchOperation) GetType() string {
	return "wal-batch"
}

// WALFlushOperation implements the AsyncOperation interface for flush operations
type WALFlushOperation struct {
	sequence   uint64
	walManager *WALManager
	resultChan chan error // Channel to send result back
}

// Execute implements AsyncOperation interface for flush operations
func (wfo *WALFlushOperation) Execute(ctx context.Context) error {
	err := wfo.walManager.Flush()

	// Send result back through channel
	select {
	case wfo.resultChan <- err:
	case <-ctx.Done():
		return ctx.Err()
	}

	return err
}

// GetSequence implements AsyncOperation interface
func (wfo *WALFlushOperation) GetSequence() uint64 {
	return wfo.sequence
}

// GetPriority implements AsyncOperation interface (flush has high priority)
func (wfo *WALFlushOperation) GetPriority() int {
	return 10 // High priority for flush operations
}

// GetType implements AsyncOperation interface
func (wfo *WALFlushOperation) GetType() string {
	return "wal-flush"
}

// AsyncWALAdapter provides asynchronous WAL operations using the async infrastructure
// while still using the existing WAL storage format for compatibility
type AsyncWALAdapter struct {
	sequenceGen *async.SequenceGenerator
	workerPool  *async.WorkerPool
	walManager  *WALManager
	mode        string
	ctx         context.Context
	cancel      context.CancelFunc
	mu          sync.RWMutex
	closed      bool
	started     bool
}

// NewAsyncWALAdapter creates a new async WAL adapter
func NewAsyncWALAdapter(walManager *WALManager, workers int, queueSize int) (*AsyncWALAdapter, error) {
	ctx, cancel := context.WithCancel(context.Background())

	// Create sequence generator
	sequenceGen := async.NewSequenceGenerator()

	// Create worker pool configuration
	config := async.WorkerPoolConfig{
		WorkerCount: workers,
		QueueSize:   queueSize,
	}

	// Create worker pool
	workerPool := async.NewWorkerPool(config)

	adapter := &AsyncWALAdapter{
		sequenceGen: sequenceGen,
		workerPool:  workerPool,
		walManager:  walManager,
		mode:        "async",
		ctx:         ctx,
		cancel:      cancel,
		closed:      false,
		started:     false,
	}

	return adapter, nil
}

// WriteEntry implements WALModeInterface for async WAL
func (awa *AsyncWALAdapter) WriteEntry(entry WALEntry) error {
	awa.mu.RLock()
	if awa.closed {
		awa.mu.RUnlock()
		return fmt.Errorf("async WAL adapter is closed")
	}
	if !awa.started {
		awa.mu.RUnlock()
		return fmt.Errorf("async WAL adapter is not started")
	}
	awa.mu.RUnlock()

	// Get sequence number for ordering
	sequence := awa.sequenceGen.Next()

	// Create result channel for synchronous waiting
	resultChan := make(chan error, 1)

	// Create WAL operation
	operation := &WALOperation{
		entry:      entry,
		sequence:   sequence,
		priority:   5, // Normal priority
		opType:     "wal-entry",
		walManager: awa.walManager,
		resultChan: resultChan,
	}

	// Submit to worker pool
	err := awa.workerPool.Submit(operation)
	if err != nil {
		return fmt.Errorf("failed to submit WAL entry: %w", err)
	}

	// Wait for completion
	select {
	case err := <-resultChan:
		if err != nil {
			return fmt.Errorf("async WAL write failed: %w", err)
		}
		return nil
	case <-awa.ctx.Done():
		return fmt.Errorf("async WAL write cancelled: %w", awa.ctx.Err())
	}
}

// WriteEntries implements WALModeInterface for async WAL batch operations
func (awa *AsyncWALAdapter) WriteEntries(entries []WALEntry) error {
	if len(entries) == 0 {
		return nil
	}

	awa.mu.RLock()
	if awa.closed {
		awa.mu.RUnlock()
		return fmt.Errorf("async WAL adapter is closed")
	}
	if !awa.started {
		awa.mu.RUnlock()
		return fmt.Errorf("async WAL adapter is not started")
	}
	awa.mu.RUnlock()

	// Get sequence number for batch ordering
	sequence := awa.sequenceGen.Next()

	// Create result channel for synchronous waiting
	resultChan := make(chan error, 1)

	// Create batch operation
	batchOp := &WALBatchOperation{
		entries:    entries,
		sequence:   sequence,
		priority:   5, // Normal priority
		walManager: awa.walManager,
		resultChan: resultChan,
	}

	// Submit batch to worker pool
	err := awa.workerPool.Submit(batchOp)
	if err != nil {
		return fmt.Errorf("failed to submit WAL batch: %w", err)
	}

	// Wait for batch completion
	select {
	case err := <-resultChan:
		if err != nil {
			return fmt.Errorf("async WAL batch write failed: %w", err)
		}
		return nil
	case <-awa.ctx.Done():
		return fmt.Errorf("async WAL batch write cancelled: %w", awa.ctx.Err())
	}
}

// Flush implements WALModeInterface for async WAL
func (awa *AsyncWALAdapter) Flush() error {
	awa.mu.RLock()
	if awa.closed {
		awa.mu.RUnlock()
		return fmt.Errorf("async WAL adapter is closed")
	}
	if !awa.started {
		awa.mu.RUnlock()
		return fmt.Errorf("async WAL adapter is not started")
	}
	awa.mu.RUnlock()

	// Get sequence number for flush ordering
	sequence := awa.sequenceGen.Next()

	// Create result channel for synchronous waiting
	resultChan := make(chan error, 1)

	// Create flush operation
	flushOp := &WALFlushOperation{
		sequence:   sequence,
		walManager: awa.walManager,
		resultChan: resultChan,
	}

	// Submit flush to worker pool
	err := awa.workerPool.Submit(flushOp)
	if err != nil {
		return fmt.Errorf("failed to submit WAL flush: %w", err)
	}

	// Wait for flush completion
	select {
	case err := <-resultChan:
		if err != nil {
			return fmt.Errorf("async WAL flush failed: %w", err)
		}
		return nil
	case <-awa.ctx.Done():
		return fmt.Errorf("async WAL flush cancelled: %w", awa.ctx.Err())
	}
}

// IsAsync implements WALModeInterface
func (awa *AsyncWALAdapter) IsAsync() bool {
	return true
}

// GetMode implements WALModeInterface
func (awa *AsyncWALAdapter) GetMode() string {
	return awa.mode
}

// Close implements WALModeInterface for async WAL
func (awa *AsyncWALAdapter) Close() error {
	awa.mu.Lock()
	if awa.closed {
		awa.mu.Unlock()
		return nil
	}
	awa.closed = true
	awa.mu.Unlock()

	// Cancel context to stop all operations
	awa.cancel()

	// HIGH-007: Use configurable timeout for stopping worker pool
	stopTimeout := 30 * time.Second // Default fallback
	if settings := getSettingsFunc(); settings != nil {
		stopTimeout = time.Duration(settings.WorkerPoolStopTimeoutSeconds) * time.Second
	}
	
	if err := awa.workerPool.Stop(stopTimeout); err != nil {
		return fmt.Errorf("failed to stop worker pool: %w", err)
	}

	// Close underlying WAL manager
	return awa.walManager.Close()
}

// Start initializes the async WAL processing
func (awa *AsyncWALAdapter) Start() error {
	awa.mu.Lock()
	defer awa.mu.Unlock()

	if awa.closed {
		return fmt.Errorf("cannot start closed async WAL adapter")
	}

	if awa.started {
		return fmt.Errorf("async WAL adapter already started")
	}

	// Start the worker pool
	awa.workerPool.Start()
	awa.started = true

	return nil
}
