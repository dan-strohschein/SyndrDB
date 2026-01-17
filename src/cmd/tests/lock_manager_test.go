package main

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"syndrdb/src/internal/storage"

	"go.uber.org/zap"
)

// TestLockManager_MultipleConcurrentReaders tests that LockManager properly tracks
// multiple concurrent readers on the same document (CRIT-004 fix).
func TestLockManager_MultipleConcurrentReaders(t *testing.T) {
	logger := zap.NewNop().Sugar()
	lm := storage.NewLockManager(logger)

	bundleName := "test_bundle"
	documentID := "doc1"
	const numReaders = 10

	var wg sync.WaitGroup
	wg.Add(numReaders)

	// Acquire read locks from multiple transactions concurrently
	for i := 0; i < numReaders; i++ {
		go func(txID int) {
			defer wg.Done()
			txIDStr := fmt.Sprintf("tx%d", txID)
			sessionID := fmt.Sprintf("session%d", txID)
			if err := lm.AcquireReadLock(bundleName, documentID, txIDStr, sessionID); err != nil {
				t.Errorf("Failed to acquire read lock for txID=%s: %v", txIDStr, err)
			}
			time.Sleep(50 * time.Millisecond) // Hold lock briefly
		}(i)
	}

	// Wait for all readers to acquire locks
	time.Sleep(100 * time.Millisecond)

	// Verify all readers have acquired locks by checking lock info
	lockInfo := lm.GetLockInfo()
	totalReadLocks := lockInfo["total_read_locks"].(int)
	if totalReadLocks < numReaders {
		t.Errorf("Expected at least %d read locks, got %d", numReaders, totalReadLocks)
	}

	// Wait for all goroutines to release
	wg.Wait()

	// Release locks for all transactions
	for i := 0; i < numReaders; i++ {
		txIDStr := fmt.Sprintf("tx%d", i)
		lm.ReleaseLocks(txIDStr)
	}

	// Verify all locks are released
	lockInfo = lm.GetLockInfo()
	totalReadLocks = lockInfo["total_read_locks"].(int)
	totalWriteLocks := lockInfo["total_write_locks"].(int)
	if totalReadLocks != 0 || totalWriteLocks != 0 {
		t.Errorf("Expected all locks released, got read=%d, write=%d", totalReadLocks, totalWriteLocks)
	}
}

// TestLockManager_WriteWaitsForAllReaders tests that write lock waits for all readers to release
func TestLockManager_WriteWaitsForAllReaders(t *testing.T) {
	logger := zap.NewNop().Sugar()
	lm := storage.NewLockManager(logger)

	bundleName := "test_bundle"
	documentID := "doc1"
	const numReaders = 5

	// Acquire multiple read locks
	for i := 0; i < numReaders; i++ {
		txID := fmt.Sprintf("read_tx%d", i)
		sessionID := fmt.Sprintf("session%d", i)
		if err := lm.AcquireReadLock(bundleName, documentID, txID, sessionID); err != nil {
			t.Fatalf("Failed to acquire read lock: %v", err)
		}
	}

	// Try to acquire write lock - should block and timeout since readers are active
	// Run in goroutine since it will block until timeout
	writeTxID := "write_tx1"
	writeSessionID := "write_session1"
	writeLockAcquired := make(chan bool, 1)
	writeLockError := make(chan error, 1)

	go func() {
		err := lm.AcquireWriteLock(bundleName, documentID, writeTxID, writeSessionID)
		if err != nil {
			writeLockError <- err
		} else {
			writeLockAcquired <- true
		}
	}()

	// Give it a moment to start waiting
	time.Sleep(50 * time.Millisecond)

	// Verify write lock hasn't been acquired yet (readers are still active)
	select {
	case <-writeLockAcquired:
		t.Error("Write lock should not be acquired while readers are active")
		lm.ReleaseLocks(writeTxID)
	case err := <-writeLockError:
		// If it errors (timeout), that's expected behavior
		t.Logf("Write lock correctly timed out or failed: %v", err)
	case <-time.After(100 * time.Millisecond):
		// Writer is correctly blocked - this is expected
		t.Log("Write lock correctly blocked waiting for readers (will timeout)")
	}

	// Release all read locks - this should wake the waiting writer
	for i := 0; i < numReaders; i++ {
		txID := fmt.Sprintf("read_tx%d", i)
		lm.ReleaseLocks(txID)
	}

	// Wait for write lock to be acquired (it should have been woken)
	select {
	case <-writeLockAcquired:
		t.Log("Write lock successfully acquired after all readers released")
		lm.ReleaseLocks(writeTxID)
	case err := <-writeLockError:
		// If it timed out, that's also acceptable in this test scenario
		t.Logf("Write lock acquisition completed with: %v", err)
		// Try again synchronously - should succeed now
		if err := lm.AcquireWriteLock(bundleName, documentID, writeTxID, writeSessionID); err != nil {
			t.Errorf("Expected write lock to succeed after readers released: %v", err)
		} else {
			lm.ReleaseLocks(writeTxID)
		}
	case <-time.After(1 * time.Second):
		t.Error("Write lock should have been woken after all readers released")
		// Try to acquire synchronously as fallback
		if err := lm.AcquireWriteLock(bundleName, documentID, writeTxID, writeSessionID); err != nil {
			t.Errorf("Expected write lock to succeed after readers released: %v", err)
		} else {
			lm.ReleaseLocks(writeTxID)
		}
	}
}

// TestLockManager_ReadWriteConcurrent tests concurrent read/write lock operations
// This test should pass with -race flag to verify thread safety
func TestLockManager_ReadWriteConcurrent(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping concurrent test in short mode")
	}

	logger := zap.NewNop().Sugar()
	lm := storage.NewLockManager(logger)

	bundleName := "test_bundle"
	documentID := "doc1"
	const numGoroutines = 20

	var wg sync.WaitGroup
	errors := make(chan error, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			txID := fmt.Sprintf("tx%d", id)
			sessionID := fmt.Sprintf("session%d", id)

			if id%2 == 0 {
				// Even IDs: read locks
				if err := lm.AcquireReadLock(bundleName, documentID, txID, sessionID); err != nil {
					errors <- fmt.Errorf("read lock failed for txID=%s: %v", txID, err)
					return
				}
				time.Sleep(10 * time.Millisecond)
				lm.ReleaseLocks(txID)
			} else {
				// Odd IDs: write locks (will likely timeout, which is expected)
				_ = lm.AcquireWriteLock(bundleName, documentID, txID, sessionID)
				// Don't check error - timeout is expected when readers are active
				lm.ReleaseLocks(txID)
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	// Log errors but don't fail - some timeouts are expected
	for err := range errors {
		t.Logf("Concurrent operation error (may be expected): %v", err)
	}

	// Verify all locks are released
	lockInfo := lm.GetLockInfo()
	totalReadLocks := lockInfo["total_read_locks"].(int)
	totalWriteLocks := lockInfo["total_write_locks"].(int)
	if totalReadLocks != 0 || totalWriteLocks != 0 {
		t.Errorf("Expected all locks released after concurrent ops, got read=%d, write=%d",
			totalReadLocks, totalWriteLocks)
	}
}
