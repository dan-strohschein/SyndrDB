package storage

import (
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"syndrdb/src/pkg/settings"

	"go.uber.org/zap"
)

func init() {
	// Override settings function for tests to avoid requiring a full settings singleton
	getSettingsFunc = func() *settings.Arguments {
		return &settings.Arguments{LockTimeoutSeconds: 30}
	}
}

func TestDeadlockDetection_WriteWrite(t *testing.T) {
	// Classic deadlock: T1 locks A, T2 locks B, T1 waits for B, T2 waits for A
	logger := zap.NewNop().Sugar()
	lm := NewLockManager(logger, 5*time.Second)

	// T1 acquires write lock on A
	if err := lm.AcquireWriteLock("bundle", "docA", "tx1", "sess1"); err != nil {
		t.Fatalf("T1 lock A: %v", err)
	}

	// T2 acquires write lock on B
	if err := lm.AcquireWriteLock("bundle", "docB", "tx2", "sess2"); err != nil {
		t.Fatalf("T2 lock B: %v", err)
	}

	var wg sync.WaitGroup
	var t1Err, t2Err error

	// T1 tries to lock B (blocked by T2)
	wg.Add(1)
	go func() {
		defer wg.Done()
		t1Err = lm.AcquireWriteLock("bundle", "docB", "tx1", "sess1")
	}()

	// Give T1 a moment to register in wait-for graph
	time.Sleep(50 * time.Millisecond)

	// T2 tries to lock A (blocked by T1) — deadlock!
	wg.Add(1)
	go func() {
		defer wg.Done()
		t2Err = lm.AcquireWriteLock("bundle", "docA", "tx2", "sess2")
		// Simulate rollback: if T2 is the deadlock victim, release its locks
		if errors.Is(t2Err, ErrDeadlockDetected) {
			lm.ReleaseLocks("tx2")
		}
	}()

	wg.Wait()

	// At least one should get a deadlock error
	deadlockCount := 0
	if t1Err != nil && errors.Is(t1Err, ErrDeadlockDetected) {
		deadlockCount++
	}
	if t2Err != nil && errors.Is(t2Err, ErrDeadlockDetected) {
		deadlockCount++
	}

	if deadlockCount == 0 {
		t.Errorf("expected at least one deadlock error, got t1Err=%v, t2Err=%v", t1Err, t2Err)
	}
}

func TestNoDeadlock_SequentialRelease(t *testing.T) {
	// T1 locks A, T2 waits for A, T1 releases A → T2 acquires
	logger := zap.NewNop().Sugar()
	lm := NewLockManager(logger, 5*time.Second)

	// T1 acquires write lock on A
	if err := lm.AcquireWriteLock("bundle", "docA", "tx1", "sess1"); err != nil {
		t.Fatalf("T1 lock A: %v", err)
	}

	var t2Err error
	done := make(chan struct{})

	// T2 tries to lock A (blocked by T1)
	go func() {
		t2Err = lm.AcquireWriteLock("bundle", "docA", "tx2", "sess2")
		close(done)
	}()

	// Give T2 time to start waiting
	time.Sleep(50 * time.Millisecond)

	// T1 releases A — T2 should wake up and acquire
	lm.ReleaseLocks("tx1")

	select {
	case <-done:
		if t2Err != nil {
			t.Fatalf("T2 should have acquired lock after T1 release, got error: %v", t2Err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("T2 timed out waiting for lock — channel notification may not be working")
	}
}

func TestDeadlockDetection_ReadWrite(t *testing.T) {
	// T1 holds read on A, T2 holds write on B, T1 wants write on B, T2 wants write on A
	logger := zap.NewNop().Sugar()
	lm := NewLockManager(logger, 5*time.Second)

	if err := lm.AcquireReadLock("bundle", "docA", "tx1", "sess1"); err != nil {
		t.Fatalf("T1 read A: %v", err)
	}
	if err := lm.AcquireWriteLock("bundle", "docB", "tx2", "sess2"); err != nil {
		t.Fatalf("T2 write B: %v", err)
	}

	var wg sync.WaitGroup
	var t1Err, t2Err error

	wg.Add(1)
	go func() {
		defer wg.Done()
		t1Err = lm.AcquireWriteLock("bundle", "docB", "tx1", "sess1")
	}()

	time.Sleep(50 * time.Millisecond)

	wg.Add(1)
	go func() {
		defer wg.Done()
		t2Err = lm.AcquireWriteLock("bundle", "docA", "tx2", "sess2")
		// Simulate rollback: if T2 is the deadlock victim, release its locks
		if errors.Is(t2Err, ErrDeadlockDetected) {
			lm.ReleaseLocks("tx2")
		}
	}()

	wg.Wait()

	deadlockCount := 0
	if t1Err != nil && errors.Is(t1Err, ErrDeadlockDetected) {
		deadlockCount++
	}
	if t2Err != nil && errors.Is(t2Err, ErrDeadlockDetected) {
		deadlockCount++
	}

	if deadlockCount == 0 {
		t.Errorf("expected deadlock, got t1Err=%v, t2Err=%v", t1Err, t2Err)
	}
}

func TestTimeoutStillWorks(t *testing.T) {
	// Even with deadlock detection, timeout should still be the fallback
	logger := zap.NewNop().Sugar()
	lm := NewLockManager(logger, 500*time.Millisecond) // Short timeout

	// T1 locks A and never releases
	if err := lm.AcquireWriteLock("bundle", "docA", "tx1", "sess1"); err != nil {
		t.Fatalf("T1 lock A: %v", err)
	}

	// T2 waits for A — no deadlock, just timeout
	start := time.Now()
	err := lm.AcquireWriteLock("bundle", "docA", "tx2", "sess2")
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected timeout error")
	}
	if errors.Is(err, ErrDeadlockDetected) {
		t.Fatal("expected timeout, not deadlock")
	}
	if !strings.Contains(err.Error(), "timeout") {
		t.Fatalf("expected timeout error, got: %v", err)
	}
	if elapsed < 400*time.Millisecond {
		t.Errorf("timeout happened too early: %v", elapsed)
	}
}

func TestWaitForGraph_CycleDetection(t *testing.T) {
	g := NewWaitForGraph()

	// No cycle: A → B → C
	g.AddEdge("A", "B")
	g.AddEdge("B", "C")
	if cycle := g.DetectCycle("A"); cycle != nil {
		t.Errorf("expected no cycle, got %v", cycle)
	}

	// Add cycle: C → A
	g.AddEdge("C", "A")
	cycle := g.DetectCycle("A")
	if cycle == nil {
		t.Fatal("expected cycle to be detected")
	}
	// Cycle should start and end with "A"
	if cycle[0] != "A" || cycle[len(cycle)-1] != "A" {
		t.Errorf("cycle should start and end with A, got %v", cycle)
	}
}

func TestWaitForGraph_RemoveWaiter(t *testing.T) {
	g := NewWaitForGraph()
	g.AddEdge("A", "B")
	g.AddEdge("B", "A")

	// Remove A as waiter — no more cycle from A
	g.RemoveWaiter("A")
	if cycle := g.DetectCycle("B"); cycle != nil {
		// B→A edge was there, but A→B is removed, so no cycle
		t.Errorf("expected no cycle after removing waiter A, got %v", cycle)
	}
}

func TestWaitForGraph_RemoveHolder(t *testing.T) {
	g := NewWaitForGraph()
	g.AddEdge("A", "B")
	g.AddEdge("B", "A")

	// Remove B as holder (someone waiting on B releases)
	g.RemoveHolder("B")
	if cycle := g.DetectCycle("A"); cycle != nil {
		t.Errorf("expected no cycle after removing holder B, got %v", cycle)
	}
}
