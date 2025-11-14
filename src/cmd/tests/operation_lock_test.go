package main
// operation_lock_test.go
//
// This file contains unit tests for the BundleOperationLock functionality.
// Tests verify that concurrent operations are properly coordinated and that
// administrative operations (like rename) can safely wait for active operations.
//
// Test Coverage:
// - Basic lock acquisition and release for readers
// - Basic lock acquisition and release for writers
// - Multiple concurrent readers (allowed)
// - Blocking writers when readers are active
// - Blocking readers when rename is in progress
// - Timeout behavior when operations don't complete
// - Proper cleanup after administrative operations


import (
	"syndrdb/src/internal/domain/bundle"
	"sync"
	"testing"
	"time"
)

// TestBundleOperationLock_BasicReadLock tests basic read lock acquisition and release
func TestBundleOperationLock_BasicReadLock(t *testing.T) {
	lock := bundle.NewBundleOperationLock("test_bundle")

	// Acquire read lock
	if err := lock.AcquireReadLock(); err != nil {
		t.Fatalf("Failed to acquire read lock: %v", err)
	}

	// Verify active readers count
	readers, writers := lock.GetActiveOperationCounts()
	if readers != 1 {
		t.Errorf("Expected 1 active reader, got %d", readers)
	}
	if writers != 0 {
		t.Errorf("Expected 0 active writers, got %d", writers)
	}

	// Release read lock
	lock.ReleaseReadLock()

	// Verify counts are zero
	readers, writers = lock.GetActiveOperationCounts()
	if readers != 0 {
		t.Errorf("Expected 0 active readers after release, got %d", readers)
	}
	if writers != 0 {
		t.Errorf("Expected 0 active writers after release, got %d", writers)
	}
}

// TestBundleOperationLock_BasicWriteLock tests basic write lock acquisition and release
func TestBundleOperationLock_BasicWriteLock(t *testing.T) {
	lock := bundle.NewBundleOperationLock("test_bundle")

	// Acquire write lock
	if err := lock.AcquireWriteLock(); err != nil {
		t.Fatalf("Failed to acquire write lock: %v", err)
	}

	// Verify active writers count
	readers, writers := lock.GetActiveOperationCounts()
	if readers != 0 {
		t.Errorf("Expected 0 active readers, got %d", readers)
	}
	if writers != 1 {
		t.Errorf("Expected 1 active writer, got %d", writers)
	}

	// Release write lock
	lock.ReleaseWriteLock()

	// Verify counts are zero
	readers, writers = lock.GetActiveOperationCounts()
	if readers != 0 {
		t.Errorf("Expected 0 active readers after release, got %d", readers)
	}
	if writers != 0 {
		t.Errorf("Expected 0 active writers after release, got %d", writers)
	}
}

// TestBundleOperationLock_MultipleConcurrentReaders tests that multiple readers can acquire locks simultaneously
func TestBundleOperationLock_MultipleConcurrentReaders(t *testing.T) {
	lock := bundle.NewBundleOperationLock("test_bundle")
	const numReaders = 10

	var wg sync.WaitGroup
	wg.Add(numReaders)

	// Acquire multiple read locks concurrently
	for i := 0; i < numReaders; i++ {
		go func() {
			defer wg.Done()
			if err := lock.AcquireReadLock(); err != nil {
				t.Errorf("Failed to acquire read lock: %v", err)
			}
			time.Sleep(100 * time.Millisecond) // Hold lock briefly
			lock.ReleaseReadLock()
		}()
	}

	// Wait briefly for all readers to acquire locks
	time.Sleep(50 * time.Millisecond)

	// Verify all readers acquired locks
	readers, writers := lock.GetActiveOperationCounts()
	if readers != numReaders {
		t.Errorf("Expected %d active readers, got %d", numReaders, readers)
	}
	if writers != 0 {
		t.Errorf("Expected 0 active writers, got %d", writers)
	}

	// Wait for all to complete
	wg.Wait()

	// Verify all locks released
	readers, writers = lock.GetActiveOperationCounts()
	if readers != 0 {
		t.Errorf("Expected 0 active readers after release, got %d", readers)
	}
	if writers != 0 {
		t.Errorf("Expected 0 active writers after release, got %d", writers)
	}
}

// TestBundleOperationLock_RenameBlocksNewOperations tests that setting rename flag blocks new operations
func TestBundleOperationLock_RenameBlocksNewOperations(t *testing.T) {
	lock := bundle.NewBundleOperationLock("test_bundle")

	// Start a rename operation (this sets the flag without waiting)
	go func() {
		if err := lock.WaitForActiveOperations(5 * time.Second); err != nil {
			t.Errorf("WaitForActiveOperations failed: %v", err)
		}
	}()

	// Give the rename flag time to be set
	time.Sleep(100 * time.Millisecond)

	// Try to acquire read lock - should be blocked
	if err := lock.AcquireReadLock(); err == nil {
		t.Error("Expected read lock to be blocked during rename, but it succeeded")
		lock.ReleaseReadLock()
	}

	// Try to acquire write lock - should be blocked
	if err := lock.AcquireWriteLock(); err == nil {
		t.Error("Expected write lock to be blocked during rename, but it succeeded")
		lock.ReleaseWriteLock()
	}

	// Complete the rename
	lock.CompleteAdministrativeOperation()

	// Now operations should succeed
	if err := lock.AcquireReadLock(); err != nil {
		t.Errorf("Expected read lock to succeed after rename complete: %v", err)
	}
	lock.ReleaseReadLock()
}

// TestBundleOperationLock_WaitForActiveOperations tests waiting for operations to complete
func TestBundleOperationLock_WaitForActiveOperations(t *testing.T) {
	lock := bundle.NewBundleOperationLock("test_bundle")

	// Start some read operations
	const numReaders = 5
	var wg sync.WaitGroup
	wg.Add(numReaders)

	for i := 0; i < numReaders; i++ {
		go func() {
			defer wg.Done()
			if err := lock.AcquireReadLock(); err != nil {
				t.Errorf("Failed to acquire read lock: %v", err)
				return
			}
			time.Sleep(200 * time.Millisecond) // Hold lock for 200ms
			lock.ReleaseReadLock()
		}()
	}

	// Wait for readers to acquire locks
	time.Sleep(50 * time.Millisecond)

	// Start waiting for operations (should complete after readers finish)
	start := time.Now()
	if err := lock.WaitForActiveOperations(5 * time.Second); err != nil {
		t.Fatalf("WaitForActiveOperations failed: %v", err)
	}
	duration := time.Since(start)

	// Should have waited approximately 200ms for readers to complete
	if duration < 150*time.Millisecond || duration > 400*time.Millisecond {
		t.Errorf("Expected wait duration around 200ms, got %v", duration)
	}

	// Verify no active operations
	readers, writers := lock.GetActiveOperationCounts()
	if readers != 0 || writers != 0 {
		t.Errorf("Expected 0 active operations after wait, got readers=%d, writers=%d", readers, writers)
	}

	// Clean up
	lock.CompleteAdministrativeOperation()
	wg.Wait()
}

// TestBundleOperationLock_TimeoutWaiting tests that wait times out if operations don't complete
func TestBundleOperationLock_TimeoutWaiting(t *testing.T) {
	lock := bundle.NewBundleOperationLock("test_bundle")

	// Start a long-running read operation
	if err := lock.AcquireReadLock(); err != nil {
		t.Fatalf("Failed to acquire read lock: %v", err)
	}
	defer lock.ReleaseReadLock()

	// Try to wait with short timeout - should timeout
	start := time.Now()
	err := lock.WaitForActiveOperations(100 * time.Millisecond)
	duration := time.Since(start)

	if err == nil {
		t.Error("Expected timeout error but got nil")
	}

	// Should have waited approximately the timeout duration
	if duration < 90*time.Millisecond || duration > 200*time.Millisecond {
		t.Errorf("Expected timeout duration around 100ms, got %v", duration)
	}

	// Verify rename flag was cleared after timeout
	if lock.IsRenameInProgress() {
		t.Error("Expected rename flag to be cleared after timeout")
	}

	// Verify operations can resume after timeout
	if err := lock.AcquireReadLock(); err != nil {
		t.Errorf("Expected to acquire read lock after timeout: %v", err)
	}
	lock.ReleaseReadLock()
}

// TestBundleOperationLock_CompleteAdministrativeOperation tests cleanup after admin operation
func TestBundleOperationLock_CompleteAdministrativeOperation(t *testing.T) {
	lock := bundle.NewBundleOperationLock("test_bundle")

	// Start wait (sets rename flag)
	go func() {
		_ = lock.WaitForActiveOperations(5 * time.Second)
	}()

	// Wait for flag to be set
	time.Sleep(50 * time.Millisecond)

	// Verify rename is in progress
	if !lock.IsRenameInProgress() {
		t.Error("Expected rename to be in progress")
	}

	// Complete administrative operation
	lock.CompleteAdministrativeOperation()

	// Verify rename flag cleared
	if lock.IsRenameInProgress() {
		t.Error("Expected rename flag to be cleared")
	}

	// Verify operations can now proceed
	if err := lock.AcquireReadLock(); err != nil {
		t.Errorf("Expected read lock to succeed after admin operation complete: %v", err)
	}
	lock.ReleaseReadLock()
}

// TestBundleOperationLock_ConcurrentReadersAndWait tests complex scenario with concurrent operations
func TestBundleOperationLock_ConcurrentReadersAndWait(t *testing.T) {
	lock := bundle.NewBundleOperationLock("test_bundle")

	// Start multiple readers
	const numReaders = 10
	var readersWg sync.WaitGroup
	readersWg.Add(numReaders)

	for i := 0; i < numReaders; i++ {
		go func(id int) {
			defer readersWg.Done()
			if err := lock.AcquireReadLock(); err != nil {
				// It's ok if some readers fail to acquire (rename might have started)
				return
			}
			time.Sleep(100 * time.Millisecond)
			lock.ReleaseReadLock()
		}(i)
	}

	// Wait for readers to start
	time.Sleep(50 * time.Millisecond)

	// Start wait for active operations
	waitComplete := make(chan bool)
	go func() {
		if err := lock.WaitForActiveOperations(5 * time.Second); err != nil {
			t.Errorf("WaitForActiveOperations failed: %v", err)
		}
		close(waitComplete)
	}()

	// Wait for everything to complete
	readersWg.Wait()
	<-waitComplete

	// Verify clean state
	readers, writers := lock.GetActiveOperationCounts()
	if readers != 0 || writers != 0 {
		t.Errorf("Expected clean state, got readers=%d, writers=%d", readers, writers)
	}

	// Clean up
	lock.CompleteAdministrativeOperation()
}
