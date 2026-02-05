package async

import (
	"container/heap"
	"context"
	"fmt"
	"sync"
	"time"
)

// OrderedQueue maintains operations in sequence order with backpressure support
// Operations are processed in sequence number order to maintain consistency
type OrderedQueue struct {
	mu           sync.Mutex
	heap         *operationHeap
	maxSize      int
	notEmpty     *sync.Cond
	notFull      *sync.Cond
	closed       bool
	backpressure *BackpressureManager
}

// BackpressureManager handles queue overflow situations
type BackpressureManager struct {
	maxWaitTime        time.Duration
	rejectedCount      uint64
	backpressureActive bool
}

// operationHeap implements heap.Interface for priority queue based on sequence numbers
type operationHeap []AsyncOperation

func (h operationHeap) Len() int { return len(h) }

func (h operationHeap) Less(i, j int) bool {
	// Lower sequence numbers have higher priority (processed first)
	seqI := h[i].GetSequence()
	seqJ := h[j].GetSequence()

	if seqI == seqJ {
		// If sequences are equal, use priority as tiebreaker
		return h[i].GetPriority() > h[j].GetPriority()
	}

	return seqI < seqJ
}

func (h operationHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *operationHeap) Push(x interface{}) {
	*h = append(*h, x.(AsyncOperation))
}

func (h *operationHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[0 : n-1]
	return item
}

// NewOrderedQueue creates a new ordered queue with the specified maximum size
func NewOrderedQueue(maxSize int) *OrderedQueue {
	if maxSize <= 0 {
		maxSize = 1000 // Default reasonable size
	}

	oq := &OrderedQueue{
		heap:    &operationHeap{},
		maxSize: maxSize,
		backpressure: &BackpressureManager{
			maxWaitTime: 5 * time.Second, // Default 5 second wait
		},
	}

	oq.notEmpty = sync.NewCond(&oq.mu)
	oq.notFull = sync.NewCond(&oq.mu)

	heap.Init(oq.heap)

	return oq
}

// Enqueue adds an operation to the queue, blocking if queue is full.
// Uses Cond.Wait() with a timeout (maxWaitTime) so we wake as soon as there is space
// or when the wait time is exceeded, avoiding indefinite block and avoiding polling.
func (oq *OrderedQueue) Enqueue(op AsyncOperation) error {
	waitStart := time.Now()

	oq.mu.Lock()
	defer oq.mu.Unlock()

	for {
		if oq.closed {
			return fmt.Errorf("queue is closed")
		}

		if oq.heap.Len() < oq.maxSize {
			oq.backpressure.backpressureActive = false
			heap.Push(oq.heap, op)
			oq.notEmpty.Signal()
			return nil
		}

		oq.backpressure.backpressureActive = true
		remaining := oq.backpressure.maxWaitTime - time.Since(waitStart)
		if remaining <= 0 {
			oq.backpressure.rejectedCount++
			return fmt.Errorf("queue full: rejected after waiting %v", oq.backpressure.maxWaitTime)
		}

		// Wake waiters after remaining time so we don't block forever
		timer := time.AfterFunc(remaining, func() {
			oq.mu.Lock()
			oq.notFull.Broadcast()
			oq.mu.Unlock()
		})
		oq.notFull.Wait()
		timer.Stop()

		// Re-check after Wait(); loop will re-evaluate condition and timeout
	}
}

// dequeueWakeInterval is how long Dequeue waits on notEmpty before re-checking context.
// Keeps workers responsive to context cancel without busy polling.
const dequeueWakeInterval = 100 * time.Millisecond

// Dequeue removes and returns the next operation in sequence order.
// Uses Cond.Wait() with a periodic wake (dequeueWakeInterval) so we re-check context
// and don't block forever, while waking immediately when an item is enqueued.
func (oq *OrderedQueue) Dequeue(ctx context.Context) (AsyncOperation, error) {
	oq.mu.Lock()
	defer oq.mu.Unlock()

	for oq.heap.Len() == 0 && !oq.closed {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		timer := time.AfterFunc(dequeueWakeInterval, func() {
			oq.mu.Lock()
			oq.notEmpty.Broadcast()
			oq.mu.Unlock()
		})
		oq.notEmpty.Wait()
		timer.Stop()
	}

	if oq.closed && oq.heap.Len() == 0 {
		return nil, fmt.Errorf("queue is closed")
	}

	op := heap.Pop(oq.heap).(AsyncOperation)
	oq.notFull.Signal()
	return op, nil
}

// Size returns the current number of operations in the queue
func (oq *OrderedQueue) Size() int {
	oq.mu.Lock()
	defer oq.mu.Unlock()

	return oq.heap.Len()
}

// Close closes the queue and wakes up any waiting goroutines
func (oq *OrderedQueue) Close() {
	oq.mu.Lock()
	defer oq.mu.Unlock()

	oq.closed = true
	oq.notEmpty.Broadcast()
	oq.notFull.Broadcast()
}

// GetBackpressureMetrics returns current backpressure statistics
func (oq *OrderedQueue) GetBackpressureMetrics() (bool, uint64) {
	oq.mu.Lock()
	defer oq.mu.Unlock()

	return oq.backpressure.backpressureActive, oq.backpressure.rejectedCount
}

// SetMaxWaitTime configures how long enqueue operations will wait when queue is full
func (oq *OrderedQueue) SetMaxWaitTime(duration time.Duration) {
	oq.mu.Lock()
	defer oq.mu.Unlock()

	oq.backpressure.maxWaitTime = duration
}

// Peek returns the next operation without removing it (for debugging/monitoring)
func (oq *OrderedQueue) Peek() (AsyncOperation, error) {
	oq.mu.Lock()
	defer oq.mu.Unlock()

	if oq.heap.Len() == 0 {
		return nil, fmt.Errorf("queue is empty")
	}

	// Return copy of the first element without removing it
	return (*oq.heap)[0], nil
}
