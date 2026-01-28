// bundle_operation_lock.go
//
// This file implements a fine-grained locking mechanism for bundle operations to ensure
// thread safety during concurrent read/write operations and administrative tasks like
// bundle renaming. The BundleOperationLock uses atomic operations to track active readers
// and writers without blocking read operations from each other, while ensuring exclusive
// access for write operations and administrative tasks.
//
// Key Features:
// - Multiple concurrent readers allowed
// - Exclusive access for writers (blocks readers and other writers)
// - Administrative operations (like rename) can wait for all active operations to complete
// - Atomic counters for lock-free reader tracking in common cases
// - Timeout support to prevent indefinite waits
//
// Design Principles:
// - Single Responsibility: Manages operation-level locking for a single bundle
// - Open/Closed: Can be extended with additional lock types (e.g., schema locks)
// - DRY: Reusable across all bundle operations

package bundle

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
)

// BundleOperationLock manages concurrent access to a bundle, tracking active
// read and write operations to ensure data consistency during administrative
// operations like renaming.
type BundleOperationLock struct {
	// activeReaders counts the number of active read operations
	// Using atomic operations for lock-free increment/decrement
	activeReaders int64

	// activeWriters counts the number of active write operations
	// Using atomic operations for lock-free increment/decrement
	activeWriters int64

	// renameInProgress indicates whether a rename operation is waiting or in progress
	// When true, new operations should be blocked
	renameInProgress atomic.Bool

	// mutex protects the condition variable and coordinates between operations
	mutex sync.Mutex

	// cond allows operations to wait for specific conditions (e.g., zero active operations)
	cond *sync.Cond

	// bundleName is stored for better error messages and debugging
	bundleName string

	// logger is optional for error reporting when negative counters are detected
	// Nil-safe: if nil, no logging occurs (maintains backward compatibility)
	logger *zap.SugaredLogger
}

// NewBundleOperationLock creates a new operation lock for a bundle
// Logger is optional - if nil, no logging will occur (maintains backward compatibility)
func NewBundleOperationLock(bundleName string, logger ...*zap.SugaredLogger) *BundleOperationLock {
	lock := &BundleOperationLock{
		bundleName: bundleName,
	}
	if len(logger) > 0 && logger[0] != nil {
		lock.logger = logger[0]
	}
	lock.cond = sync.NewCond(&lock.mutex)
	return lock
}

// SetLogger sets the logger for this lock (useful when logger isn't available at construction)
func (bol *BundleOperationLock) SetLogger(logger *zap.SugaredLogger) {
	bol.logger = logger
}

// captureStackTrace captures a stack trace for error reporting
// Returns a formatted string with up to 10 stack frames
func (bol *BundleOperationLock) captureStackTrace() string {
	// Capture up to 10 stack frames
	pc := make([]uintptr, 10)
	n := runtime.Callers(3, pc) // Skip captureStackTrace, release method, and runtime
	frames := runtime.CallersFrames(pc[:n])

	var trace []string
	for {
		frame, more := frames.Next()
		if !more {
			break
		}
		// Format as "file:line function"
		trace = append(trace, fmt.Sprintf("%s:%d %s", frame.File, frame.Line, frame.Function))
	}

	if len(trace) == 0 {
		return "no stack trace available"
	}

	// Join with " -> " for readability
	result := ""
	for i, frame := range trace {
		if i > 0 {
			result += " -> "
		}
		result += frame
	}
	return result
}

// AcquireReadLock attempts to acquire a read lock on the bundle.
// Returns an error if a rename operation is in progress.
// Multiple readers can hold the lock simultaneously.
func (bol *BundleOperationLock) AcquireReadLock() error {
	bol.mutex.Lock()
	defer bol.mutex.Unlock()

	if bol.renameInProgress.Load() {
		return fmt.Errorf("bundle '%s' is being renamed, operation blocked", bol.bundleName)
	}

	atomic.AddInt64(&bol.activeReaders, 1)
	return nil
}

// ReleaseReadLock releases a previously acquired read lock and signals
// any waiting operations (like rename) that might be waiting for all
// operations to complete.
func (bol *BundleOperationLock) ReleaseReadLock() {
	newCount := atomic.AddInt64(&bol.activeReaders, -1)
	if newCount < 0 {
		// Negative counter indicates serious bug: double-release or missing acquire
		// Log error with stack trace for debugging
		if bol.logger != nil {
			stackTrace := bol.captureStackTrace()
			bol.logger.Errorw("Negative counter detected in BundleOperationLock (CRITICAL BUG)",
				"bundle", bol.bundleName,
				"counter_type", "readers",
				"detected_value", newCount,
				"stack_trace", stackTrace,
				"description", "Double-release or missing acquire detected - indicates lock usage bug")
		}
		// Reset to 0 to prevent cascading failures
		atomic.StoreInt64(&bol.activeReaders, 0)
	}

	// Signal waiting operations if we're the last one
	if newCount == 0 {
		bol.cond.Broadcast()
	}
}

// AcquireWriteLock attempts to acquire a write lock on the bundle.
// Returns an error if a rename operation is in progress.
// Only one writer can hold the lock at a time, and no readers can be active.
func (bol *BundleOperationLock) AcquireWriteLock() error {
	bol.mutex.Lock()
	defer bol.mutex.Unlock()

	if bol.renameInProgress.Load() {
		return fmt.Errorf("bundle '%s' is being renamed, operation blocked", bol.bundleName)
	}

	atomic.AddInt64(&bol.activeWriters, 1)
	return nil
}

// ReleaseWriteLock releases a previously acquired write lock and signals
// any waiting operations (like rename) that might be waiting for all
// operations to complete.
func (bol *BundleOperationLock) ReleaseWriteLock() {
	newCount := atomic.AddInt64(&bol.activeWriters, -1)
	if newCount < 0 {
		// Negative counter indicates serious bug: double-release or missing acquire
		// Log error with stack trace for debugging
		if bol.logger != nil {
			stackTrace := bol.captureStackTrace()
			bol.logger.Errorw("Negative counter detected in BundleOperationLock (CRITICAL BUG)",
				"bundle", bol.bundleName,
				"counter_type", "writers",
				"detected_value", newCount,
				"stack_trace", stackTrace,
				"description", "Double-release or missing acquire detected - indicates lock usage bug")
		}
		// Reset to 0 to prevent cascading failures
		atomic.StoreInt64(&bol.activeWriters, 0)
	}

	// Signal waiting operations if we're the last one
	if newCount == 0 {
		bol.cond.Broadcast()
	}
}

// WaitForActiveOperations blocks until all active read and write operations
// complete or the timeout expires. This should be called before administrative
// operations like renaming to ensure data consistency.
//
// The timeout prevents indefinite waits in case of stuck operations.
// A timeout of 0 means wait indefinitely (not recommended).
// CRITICAL FIX: Uses polling instead of goroutine+cond.Wait to prevent goroutine leaks
func (bol *BundleOperationLock) WaitForActiveOperations(timeout time.Duration) error {
	// Mark that a rename is in progress to block new operations
	bol.renameInProgress.Store(true)

	// CRITICAL FIX: Use polling with short sleeps instead of spawning a goroutine
	// The previous implementation spawned a goroutine that would:
	// 1. Block on cond.Wait() holding the mutex
	// 2. If timeout occurred, the goroutine would leak AND hold the mutex forever
	// This polling approach avoids both issues
	startTime := time.Now()
	pollInterval := 5 * time.Millisecond

	for {
		// Check timeout first
		if timeout > 0 && time.Since(startTime) >= timeout {
			// Timeout occurred - clear the rename flag so operations can resume
			bol.renameInProgress.Store(false)
			readers := atomic.LoadInt64(&bol.activeReaders)
			writers := atomic.LoadInt64(&bol.activeWriters)
			return fmt.Errorf(
				"timeout waiting for active operations on bundle '%s' (readers: %d, writers: %d)",
				bol.bundleName, readers, writers,
			)
		}

		// Check if all operations are complete
		readers := atomic.LoadInt64(&bol.activeReaders)
		writers := atomic.LoadInt64(&bol.activeWriters)
		if readers == 0 && writers == 0 {
			return nil // Success - all operations completed
		}

		// Sleep briefly before checking again
		time.Sleep(pollInterval)
	}
}

// CompleteAdministrativeOperation should be called after an administrative
// operation (like rename) completes to allow normal operations to resume.
func (bol *BundleOperationLock) CompleteAdministrativeOperation() {
	bol.renameInProgress.Store(false)
}

// GetActiveOperationCounts returns the current number of active readers and writers.
// Useful for monitoring and debugging.
func (bol *BundleOperationLock) GetActiveOperationCounts() (readers int64, writers int64) {
	return atomic.LoadInt64(&bol.activeReaders), atomic.LoadInt64(&bol.activeWriters)
}

// IsRenameInProgress returns whether a rename operation is currently in progress
func (bol *BundleOperationLock) IsRenameInProgress() bool {
	return bol.renameInProgress.Load()
}
