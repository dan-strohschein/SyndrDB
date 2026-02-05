package async

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"
)

// AsyncOperation represents a unit of work that can be executed asynchronously
type AsyncOperation interface {
	// Execute performs the actual work and returns an error if failed
	Execute(ctx context.Context) error

	// GetSequence returns the sequence number for ordering
	GetSequence() uint64

	// GetPriority returns priority (higher numbers = higher priority)
	GetPriority() int

	// GetType returns operation type for metrics and debugging
	GetType() string
}

// WorkerPool manages a pool of goroutines that execute async operations
// Each worker processes operations from the queue in sequence order
type WorkerPool struct {
	workers      int
	queue        *OrderedQueue
	workerWg     sync.WaitGroup
	ctx          context.Context
	cancel       context.CancelFunc
	metrics      *WorkerMetrics
	errorHandler func(error, AsyncOperation)
}

// WorkerMetrics tracks performance and health of the worker pool
type WorkerMetrics struct {
	mu               sync.RWMutex
	operationsTotal  uint64
	operationsFailed uint64
	queueDepth       int64
	avgProcessTime   time.Duration
	lastProcessTime  time.Time
}

// WorkerPoolConfig contains configuration for creating a worker pool
type WorkerPoolConfig struct {
	WorkerCount  int
	QueueSize    int
	ErrorHandler func(error, AsyncOperation)
}

// NewWorkerPool creates a new worker pool with the specified configuration
func NewWorkerPool(config WorkerPoolConfig) *WorkerPool {
	ctx, cancel := context.WithCancel(context.Background())

	// Default error handler logs so errors are not dropped silently
	errorHandler := config.ErrorHandler
	if errorHandler == nil {
		errorHandler = func(err error, op AsyncOperation) {
			log.Printf("worker pool error: %v (operation: %s, sequence: %d)", err, op.GetType(), op.GetSequence())
		}
	}

	return &WorkerPool{
		workers:      config.WorkerCount,
		queue:        NewOrderedQueue(config.QueueSize),
		ctx:          ctx,
		cancel:       cancel,
		metrics:      &WorkerMetrics{},
		errorHandler: errorHandler,
	}
}

// Start begins processing operations with the configured number of workers
func (wp *WorkerPool) Start() {
	for i := 0; i < wp.workers; i++ {
		wp.workerWg.Add(1)
		go wp.worker()
	}
}

// Stop gracefully shuts down the worker pool, waiting for current operations to complete
func (wp *WorkerPool) Stop(timeout time.Duration) error {
	// Signal workers to stop
	wp.cancel()

	// Wait for workers to finish with timeout
	done := make(chan struct{})
	go func() {
		wp.workerWg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-time.After(timeout):
		return fmt.Errorf("worker pool shutdown timed out after %v", timeout)
	}
}

// Submit adds an operation to the queue for async execution
func (wp *WorkerPool) Submit(op AsyncOperation) error {
	return wp.queue.Enqueue(op)
}

// GetMetrics returns a copy of current worker pool metrics
func (wp *WorkerPool) GetMetrics() WorkerMetrics {
	wp.metrics.mu.RLock()
	defer wp.metrics.mu.RUnlock()

	return WorkerMetrics{
		operationsTotal:  wp.metrics.operationsTotal,
		operationsFailed: wp.metrics.operationsFailed,
		queueDepth:       wp.metrics.queueDepth,
		avgProcessTime:   wp.metrics.avgProcessTime,
		lastProcessTime:  wp.metrics.lastProcessTime,
	}
}

// worker is the main worker goroutine that processes operations from the queue
func (wp *WorkerPool) worker() {
	defer wp.workerWg.Done()

	for {
		select {
		case <-wp.ctx.Done():
			return
		default:
			// Try to get an operation from the queue
			op, err := wp.queue.Dequeue(wp.ctx)
			if err != nil {
				if err == context.Canceled {
					return
				}
				// TODO: I need to add proper error handling for queue errors
				continue
			}

			// Execute the operation and track metrics
			wp.executeOperation(op)
		}
	}
}

// executeOperation executes a single operation and updates metrics
func (wp *WorkerPool) executeOperation(op AsyncOperation) {
	startTime := time.Now()

	// Execute the operation
	err := op.Execute(wp.ctx)

	// Update metrics
	wp.updateMetrics(startTime, err != nil)

	// Handle errors
	if err != nil {
		wp.errorHandler(err, op)
	}
}

// updateMetrics updates worker pool metrics after operation execution
func (wp *WorkerPool) updateMetrics(startTime time.Time, failed bool) {
	wp.metrics.mu.Lock()
	defer wp.metrics.mu.Unlock()

	wp.metrics.operationsTotal++
	if failed {
		wp.metrics.operationsFailed++
	}

	processTime := time.Since(startTime)
	wp.metrics.lastProcessTime = time.Now()

	// Simple moving average for process time
	if wp.metrics.avgProcessTime == 0 {
		wp.metrics.avgProcessTime = processTime
	} else {
		wp.metrics.avgProcessTime = (wp.metrics.avgProcessTime + processTime) / 2
	}

	wp.metrics.queueDepth = int64(wp.queue.Size())
}
