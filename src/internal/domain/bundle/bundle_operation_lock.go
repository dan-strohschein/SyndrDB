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
	"sync"
	"sync/atomic"
	"time"
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
}

// NewBundleOperationLock creates a new operation lock for a bundle
func NewBundleOperationLock(bundleName string) *BundleOperationLock {
	lock := &BundleOperationLock{
		bundleName: bundleName,
	}
	lock.cond = sync.NewCond(&lock.mutex)
	return lock
}

// AcquireReadLock attempts to acquire a read lock on the bundle.
// Returns an error if a rename operation is in progress.
// Multiple readers can hold the lock simultaneously.
func (bol *BundleOperationLock) AcquireReadLock() error {
	if bol.renameInProgress.Load() {
		return fmt.Errorf("bundle '%s' is being renamed, operation blocked", bol.bundleName)
	}

	atomic.AddInt64(&bol.activeReaders, 1)

	// Double-check after incrementing (rare race condition)
	if bol.renameInProgress.Load() {
		atomic.AddInt64(&bol.activeReaders, -1)
		return fmt.Errorf("bundle '%s' is being renamed, operation blocked", bol.bundleName)
	}

	return nil
}

// ReleaseReadLock releases a previously acquired read lock and signals
// any waiting operations (like rename) that might be waiting for all
// operations to complete.
func (bol *BundleOperationLock) ReleaseReadLock() {
	newCount := atomic.AddInt64(&bol.activeReaders, -1)
	if newCount < 0 {
		// This should never happen but log if it does
		// TODO: Add logger parameter or use a package-level logger
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
	if bol.renameInProgress.Load() {
		return fmt.Errorf("bundle '%s' is being renamed, operation blocked", bol.bundleName)
	}

	atomic.AddInt64(&bol.activeWriters, 1)

	// Double-check after incrementing (rare race condition)
	if bol.renameInProgress.Load() {
		atomic.AddInt64(&bol.activeWriters, -1)
		return fmt.Errorf("bundle '%s' is being renamed, operation blocked", bol.bundleName)
	}

	return nil
}

// ReleaseWriteLock releases a previously acquired write lock and signals
// any waiting operations (like rename) that might be waiting for all
// operations to complete.
func (bol *BundleOperationLock) ReleaseWriteLock() {
	newCount := atomic.AddInt64(&bol.activeWriters, -1)
	if newCount < 0 {
		// This should never happen but log if it does
		// TODO: Add logger parameter or use a package-level logger
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
func (bol *BundleOperationLock) WaitForActiveOperations(timeout time.Duration) error {
	// Mark that a rename is in progress to block new operations
	bol.renameInProgress.Store(true)

	// Create a channel for timeout
	done := make(chan bool, 1)
	
	go func() {
		bol.mutex.Lock()
		defer bol.mutex.Unlock()

		// Wait until both readers and writers reach zero
		for atomic.LoadInt64(&bol.activeReaders) > 0 || atomic.LoadInt64(&bol.activeWriters) > 0 {
			bol.cond.Wait()
		}
		done <- true
	}()

	// Wait with timeout if specified
	if timeout > 0 {
		select {
		case <-done:
			return nil
		case <-time.After(timeout):
			// Timeout occurred - clear the rename flag so operations can resume
			bol.renameInProgress.Store(false)
			readers := atomic.LoadInt64(&bol.activeReaders)
			writers := atomic.LoadInt64(&bol.activeWriters)
			return fmt.Errorf(
				"timeout waiting for active operations on bundle '%s' (readers: %d, writers: %d)",
				bol.bundleName, readers, writers,
			)
		}
	}

	// No timeout - wait indefinitely
	<-done
	return nil
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
