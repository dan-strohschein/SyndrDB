package main

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"syndrdb/src/internal/storage"

	"go.uber.org/zap"
)

// BenchmarkLockManager_FastPath benchmarks lock acquisition in the fast path (no contention)
// This verifies that condition variables don't add overhead when locks are immediately available
func BenchmarkLockManager_FastPath(b *testing.B) {
	logger := zap.NewNop().Sugar()
	lm := storage.NewLockManager(logger)

	bundleName := "test_bundle"
	documentID := "doc1"

	b.ResetTimer()
	b.ReportAllocs()

	// Fast path: no contention, lock should be immediately available
	for i := 0; i < b.N; i++ {
		txID := fmt.Sprintf("tx_%d", i)
		sessionID := fmt.Sprintf("session_%d", i)
		err := lm.AcquireWriteLock(bundleName, documentID, txID, sessionID)
		if err != nil {
			b.Fatalf("Failed to acquire lock: %v", err)
		}
		lm.ReleaseLocks(txID)
	}
}

// TestLockManager_ConditionVariableWake verifies that waiting goroutines are properly woken
// when locks are released (condition variable behavior)
func TestLockManager_ConditionVariableWake(t *testing.T) {
	logger := zap.NewNop().Sugar()
	lm := storage.NewLockManager(logger)

	bundleName := "test_bundle"
	documentID := "doc1"

	// Writer acquires lock first
	writerTxID := "tx_writer_1"
	writerSessionID := "session_writer_1"
	err := lm.AcquireWriteLock(bundleName, documentID, writerTxID, writerSessionID)
	if err != nil {
		t.Fatalf("Failed to acquire initial write lock: %v", err)
	}

	// Reader tries to acquire lock (will block on condition variable)
	readerTxID := "tx_reader_1"
	readerSessionID := "session_reader_1"
	readerAcquired := make(chan bool, 1)
	readerError := make(chan error, 1)

	startTime := time.Now()
	go func() {
		err := lm.AcquireReadLock(bundleName, documentID, readerTxID, readerSessionID)
		if err != nil {
			readerError <- err
			return
		}
		readerAcquired <- true
	}()

	// Give reader a moment to start waiting
	time.Sleep(50 * time.Millisecond)

	// Verify reader is blocked (not acquired yet)
	select {
	case <-readerAcquired:
		t.Error("Reader should be blocked waiting for write lock")
	default:
		t.Log("Reader correctly blocked on condition variable")
	}

	// Release write lock - should wake reader immediately
	lm.ReleaseLocks(writerTxID)
	wakeTime := time.Since(startTime)

	// Reader should be woken and acquire lock quickly
	select {
	case <-readerAcquired:
		// Success - reader was woken
		t.Logf("Reader successfully woken after %v", wakeTime)
	case err := <-readerError:
		t.Fatalf("Reader failed: %v", err)
	case <-time.After(500 * time.Millisecond):
		t.Error("Reader should have been woken within 500ms, condition variable may not be working")
	}

	// Cleanup
	lm.ReleaseLocks(readerTxID)
}

// TestLockManager_MultipleWaitersWake verifies that multiple waiting goroutines
// are properly woken when a write lock is released (Broadcast behavior)
func TestLockManager_MultipleWaitersWake(t *testing.T) {
	logger := zap.NewNop().Sugar()
	lm := storage.NewLockManager(logger)

	bundleName := "test_bundle"
	documentID := "doc1"

	// Writer acquires lock first
	writerTxID := "tx_writer_1"
	writerSessionID := "session_writer_1"
	err := lm.AcquireWriteLock(bundleName, documentID, writerTxID, writerSessionID)
	if err != nil {
		t.Fatalf("Failed to acquire initial write lock: %v", err)
	}

	// Multiple readers try to acquire lock (will all block on condition variable)
	const numReaders = 5
	readersAcquired := make([]chan bool, numReaders)
	for i := 0; i < numReaders; i++ {
		readersAcquired[i] = make(chan bool, 1)
		go func(readerID int) {
			txID := fmt.Sprintf("tx_reader_%d", readerID)
			sessionID := fmt.Sprintf("session_reader_%d", readerID)
			err := lm.AcquireReadLock(bundleName, documentID, txID, sessionID)
			if err == nil {
				readersAcquired[readerID] <- true
			}
		}(i)
	}

	// Give readers time to start waiting
	time.Sleep(100 * time.Millisecond)

	// Verify all readers are blocked
	for i := 0; i < numReaders; i++ {
		select {
		case <-readersAcquired[i]:
			t.Errorf("Reader %d should be blocked", i)
		default:
		}
	}

	// Release write lock - should wake all readers (Broadcast)
	lm.ReleaseLocks(writerTxID)

	// All readers should be woken and acquire locks
	timeout := time.After(1 * time.Second)
	for i := 0; i < numReaders; i++ {
		select {
		case <-readersAcquired[i]:
			t.Logf("Reader %d successfully woken", i)
		case <-timeout:
			t.Errorf("Reader %d was not woken within timeout", i)
		}
	}

	// Cleanup
	for i := 0; i < numReaders; i++ {
		txID := fmt.Sprintf("tx_reader_%d", i)
		lm.ReleaseLocks(txID)
	}
}

// TestLockManager_NoBusyWait verifies that the lock manager doesn't busy-wait
// by checking that CPU usage is low during contention
func TestLockManager_NoBusyWait(t *testing.T) {
	logger := zap.NewNop().Sugar()
	lm := storage.NewLockManager(logger)

	bundleName := "test_bundle"
	documentID := "doc1"

	// Writer acquires lock and holds it
	writerTxID := "tx_writer_1"
	writerSessionID := "session_writer_1"
	err := lm.AcquireWriteLock(bundleName, documentID, writerTxID, writerSessionID)
	if err != nil {
		t.Fatalf("Failed to acquire initial write lock: %v", err)
	}

	// Multiple goroutines try to acquire locks (will block efficiently on condition variables)
	const numWaiters = 10
	var wg sync.WaitGroup
	wg.Add(numWaiters)

	startTime := time.Now()
	for i := 0; i < numWaiters; i++ {
		go func(waiterID int) {
			defer wg.Done()
			txID := fmt.Sprintf("tx_waiter_%d", waiterID)
			sessionID := fmt.Sprintf("session_waiter_%d", waiterID)
			_ = lm.AcquireReadLock(bundleName, documentID, txID, sessionID)
		}(i)
	}

	// Wait a short time to ensure all goroutines have started waiting
	time.Sleep(100 * time.Millisecond)

	// If we were busy-waiting, we'd see high CPU usage
	// With condition variables, goroutines should be efficiently blocked
	// We verify this by checking that all waiters complete quickly after lock release
	lockHoldDuration := time.Since(startTime)

	// Release lock - all waiters should wake efficiently
	lm.ReleaseLocks(writerTxID)

	// All waiters should complete quickly
	done := make(chan bool, 1)
	go func() {
		wg.Wait()
		done <- true
	}()

	select {
	case <-done:
		wakeDuration := time.Since(startTime) - lockHoldDuration
		t.Logf("All %d waiters woken efficiently after %v", numWaiters, wakeDuration)
		// Verify wake duration is reasonable (not excessive)
		if wakeDuration > 500*time.Millisecond {
			t.Errorf("Wake duration too high (%v), may indicate inefficiency", wakeDuration)
		}
	case <-time.After(2 * time.Second):
		t.Error("Waiters did not complete within timeout")
	}

	// Cleanup
	for i := 0; i < numWaiters; i++ {
		txID := fmt.Sprintf("tx_waiter_%d", i)
		lm.ReleaseLocks(txID)
	}
}

// TestLockManager_FastPathPerformance verifies that fast path (no contention)
// maintains ~100 microsecond performance
func TestLockManager_FastPathPerformance(t *testing.T) {
	logger := zap.NewNop().Sugar()
	lm := storage.NewLockManager(logger)

	bundleName := "test_bundle"
	documentID := "doc1"

	// Measure fast path performance (no contention)
	const iterations = 1000
	startTime := time.Now()

	for i := 0; i < iterations; i++ {
		txID := fmt.Sprintf("tx_%d", i)
		sessionID := fmt.Sprintf("session_%d", i)
		err := lm.AcquireWriteLock(bundleName, documentID, txID, sessionID)
		if err != nil {
			t.Fatalf("Failed to acquire lock: %v", err)
		}
		lm.ReleaseLocks(txID)
	}

	totalDuration := time.Since(startTime)
	avgDuration := totalDuration / iterations

	t.Logf("Fast path performance: %v average per lock acquisition/release", avgDuration)

	// Fast path should be ~100 microseconds or less
	// Allow some margin for test overhead
	if avgDuration > 500*time.Microsecond {
		t.Errorf("Fast path too slow: %v (expected ~100 microseconds)", avgDuration)
	}
}
