package main

// lock_free_select_e2e_test.go
//
// This file contains E2E tests for the Phase 1 lock contention fix: Lock-Free SELECT queries.
// Tests verify that concurrent read and write operations work correctly without bundle-level
// read locks, relying instead on MVCC visibility checks and the lock-free page cache.
//
// Test Coverage:
// - Lock-free AcquireReadLock behavior (no mutex serialization)
// - Concurrent readers can all proceed without blocking
// - MVCC visibility filtering via IsVisibleReadCommitted()
// - Rename-in-progress still blocks new readers (safety check)
//
// These tests focus on the BundleOperationLock behavior since that's where the lock-free
// optimization was implemented. Full integration tests with BundleService require the
// homegrown test infrastructure.

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"syndrdb/src/internal/domain/bundle"
	"syndrdb/src/internal/domain/models"

	"github.com/stretchr/testify/assert"
)

// TestLockFreeAcquireReadLock_NoMutexSerialization tests that the lock-free
// AcquireReadLock implementation doesn't serialize readers through a mutex.
// Previously, all readers would block on mutex.Lock() just to check renameInProgress.
// Now they use pure atomic operations.
func TestLockFreeAcquireReadLock_NoMutexSerialization(t *testing.T) {
	EnsureTestIsolation(t)

	lock := bundle.NewBundleOperationLock("test_lock_free_bundle")

	const numReaders = 100
	const holdDuration = 50 * time.Millisecond

	var wg sync.WaitGroup
	var successfulAcquires int64
	var acquireErrors int64

	startTime := time.Now()
	startBarrier := make(chan struct{})

	// Launch many concurrent readers
	for i := 0; i < numReaders; i++ {
		wg.Add(1)
		go func(readerID int) {
			defer wg.Done()

			// Wait for all goroutines to be ready
			<-startBarrier

			// Try to acquire read lock
			if err := lock.AcquireReadLock(); err != nil {
				atomic.AddInt64(&acquireErrors, 1)
				return
			}

			atomic.AddInt64(&successfulAcquires, 1)

			// Hold lock briefly to simulate read operation
			time.Sleep(holdDuration)

			lock.ReleaseReadLock()
		}(i)
	}

	// Release all goroutines at once
	close(startBarrier)

	// Wait for all to complete
	wg.Wait()

	elapsed := time.Since(startTime)

	// All readers should have acquired locks successfully
	assert.Equal(t, int64(numReaders), atomic.LoadInt64(&successfulAcquires),
		"All readers should successfully acquire read lock")
	assert.Zero(t, atomic.LoadInt64(&acquireErrors),
		"No readers should fail to acquire lock")

	// With lock-free implementation, all readers can acquire simultaneously.
	// With mutex-based implementation, they would serialize and take ~numReaders * small_time.
	// We expect completion in roughly holdDuration + small overhead, not numReaders * overhead.
	expectedMaxTime := holdDuration + 500*time.Millisecond // Allow generous overhead
	assert.Less(t, elapsed, expectedMaxTime,
		"Lock-free readers should complete quickly (got %v, expected < %v)", elapsed, expectedMaxTime)

	t.Logf("Lock-free read lock test: %d readers completed in %v", numReaders, elapsed)
}

// TestLockFreeReadLock_RaceWithRename tests the race mitigation in AcquireReadLock.
// After Phase 1 changes, AcquireReadLock checks renameInProgress, increments readers,
// then re-checks to handle the race window.
func TestLockFreeReadLock_RaceWithRename(t *testing.T) {
	EnsureTestIsolation(t)

	lock := bundle.NewBundleOperationLock("test_race_bundle")

	// Start a goroutine that will set renameInProgress
	renameStarted := make(chan struct{})
	renameDone := make(chan struct{})

	go func() {
		close(renameStarted)
		// WaitForActiveOperations sets renameInProgress = true
		lock.WaitForActiveOperations(5 * time.Second)
		close(renameDone)
	}()

	// Wait for rename to start
	<-renameStarted
	time.Sleep(50 * time.Millisecond) // Give time for renameInProgress to be set

	// Try to acquire read lock - should fail because rename is in progress
	err := lock.AcquireReadLock()
	assert.Error(t, err, "Should fail to acquire read lock during rename")
	if err != nil {
		assert.Contains(t, err.Error(), "being renamed",
			"Error should indicate rename is in progress")
	}

	// Complete the rename
	lock.CompleteAdministrativeOperation()
	<-renameDone

	// Now read lock should work
	err = lock.AcquireReadLock()
	assert.NoError(t, err, "Should acquire read lock after rename completes")
	if err == nil {
		lock.ReleaseReadLock()
	}
}

// TestMVCCVisibility_IsVisibleReadCommitted tests that IsVisibleReadCommitted()
// correctly filters documents for lock-free reads.
func TestMVCCVisibility_IsVisibleReadCommitted(t *testing.T) {
	EnsureTestIsolation(t)

	now := time.Now()

	testCases := []struct {
		name     string
		doc      models.Document
		expected bool
	}{
		{
			name: "Committed current document is visible",
			doc: models.Document{
				DocumentID:     "doc1",
				CommitSequence: 1,
				SupersededAt:   time.Time{}, // Zero = current
				DeletedByTxID:  0,
			},
			expected: true,
		},
		{
			name: "Legacy document (no MVCC fields) is visible",
			doc: models.Document{
				DocumentID:     "doc2",
				CommitSequence: 0, // Legacy
				CreatedByTxID:  0, // Legacy
				SupersededAt:   time.Time{},
				DeletedByTxID:  0,
			},
			expected: true,
		},
		{
			name: "Uncommitted document is not visible",
			doc: models.Document{
				DocumentID:     "doc3",
				CommitSequence: 0,   // Uncommitted
				CreatedByTxID:  123, // Has active transaction
				SupersededAt:   time.Time{},
				DeletedByTxID:  0,
			},
			expected: false,
		},
		{
			name: "Superseded document is not visible",
			doc: models.Document{
				DocumentID:     "doc4",
				CommitSequence: 1,
				SupersededAt:   now.Add(-time.Hour), // Superseded
				DeletedByTxID:  0,
			},
			expected: false,
		},
		{
			name: "Deleted document is not visible",
			doc: models.Document{
				DocumentID:     "doc5",
				CommitSequence: 1,
				SupersededAt:   time.Time{},
				DeletedByTxID:  456, // Deleted
			},
			expected: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := tc.doc.IsVisibleReadCommitted()
			assert.Equal(t, tc.expected, result,
				"IsVisibleReadCommitted() should return %v for %s", tc.expected, tc.name)
		})
	}
}

// TestConcurrentReaders_AllComplete tests that many concurrent readers
// can all complete their operations without blocking each other.
func TestConcurrentReaders_AllComplete(t *testing.T) {
	EnsureTestIsolation(t)

	lock := bundle.NewBundleOperationLock("concurrent_readers_bundle")

	const numReaders = 50
	const iterations = 100

	var wg sync.WaitGroup
	var totalOps int64
	var errors int64

	// Launch concurrent readers that repeatedly acquire and release
	for i := 0; i < numReaders; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for j := 0; j < iterations; j++ {
				if err := lock.AcquireReadLock(); err != nil {
					atomic.AddInt64(&errors, 1)
					continue
				}

				// Verify we have the lock
				readers, _ := lock.GetActiveOperationCounts()
				if readers < 1 {
					atomic.AddInt64(&errors, 1)
				}

				atomic.AddInt64(&totalOps, 1)
				lock.ReleaseReadLock()
			}
		}()
	}

	wg.Wait()

	expectedOps := int64(numReaders * iterations)
	assert.Equal(t, expectedOps, atomic.LoadInt64(&totalOps),
		"All read operations should complete")
	assert.Zero(t, atomic.LoadInt64(&errors),
		"No errors should occur during concurrent reads")

	// Verify final state is clean
	readers, writers := lock.GetActiveOperationCounts()
	assert.Zero(t, readers, "No readers should remain after test")
	assert.Zero(t, writers, "No writers should remain after test")

	t.Logf("Completed %d total read lock operations across %d concurrent readers",
		atomic.LoadInt64(&totalOps), numReaders)
}

// TestLockFreeReadLock_ReaderCounterAccuracy tests that the atomic reader counter
// is accurate under high concurrency.
func TestLockFreeReadLock_ReaderCounterAccuracy(t *testing.T) {
	EnsureTestIsolation(t)

	lock := bundle.NewBundleOperationLock("counter_accuracy_bundle")

	const numReaders = 30

	var wg sync.WaitGroup
	allAcquired := make(chan struct{})
	releaseSignal := make(chan struct{})

	// Launch readers that all hold their locks until signaled
	for i := 0; i < numReaders; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			if err := lock.AcquireReadLock(); err != nil {
				t.Errorf("Failed to acquire read lock: %v", err)
				return
			}

			// Signal that we've acquired
			select {
			case <-allAcquired:
			default:
			}

			// Wait for release signal
			<-releaseSignal

			lock.ReleaseReadLock()
		}()
	}

	// Give time for all readers to acquire
	time.Sleep(100 * time.Millisecond)
	close(allAcquired)

	// Check reader count matches expected
	readers, writers := lock.GetActiveOperationCounts()
	assert.Equal(t, int64(numReaders), readers,
		"Reader count should match number of concurrent readers")
	assert.Zero(t, writers, "No writers should be active")

	// Signal all to release
	close(releaseSignal)
	wg.Wait()

	// Verify counters are zero
	readers, writers = lock.GetActiveOperationCounts()
	assert.Zero(t, readers, "Reader count should be zero after release")
	assert.Zero(t, writers, "Writer count should be zero")
}
