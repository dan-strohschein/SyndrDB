package server

import (
	"sync"
	"testing"
)

func TestSIReadTracker_BasicReadWrite(t *testing.T) {
	tracker := NewSIReadTracker()

	// T1 reads docA
	tracker.RecordRead(1, []string{"bundle:docA"})

	// T2 writes docA — should create rw-antidependency: T1 →rw T2
	tracker.RecordWrite(2, "bundle:docA")

	tracker.mu.RLock()
	_, hasOut := tracker.outConflicts[1][2]
	_, hasIn := tracker.inConflicts[2][1]
	tracker.mu.RUnlock()

	if !hasOut {
		t.Error("expected outConflicts[T1] to contain T2")
	}
	if !hasIn {
		t.Error("expected inConflicts[T2] to contain T1")
	}
}

func TestSIReadTracker_NoConflictSameTx(t *testing.T) {
	tracker := NewSIReadTracker()

	// T1 reads and writes the same doc — no self-conflict
	tracker.RecordRead(1, []string{"bundle:docA"})
	tracker.RecordWrite(1, "bundle:docA")

	tracker.mu.RLock()
	outSet := tracker.outConflicts[1]
	inSet := tracker.inConflicts[1]
	tracker.mu.RUnlock()

	if len(outSet) > 0 {
		t.Errorf("expected no outConflicts for T1, got %v", outSet)
	}
	if len(inSet) > 0 {
		t.Errorf("expected no inConflicts for T1, got %v", inSet)
	}
}

func TestSIReadTracker_DangerousStructure(t *testing.T) {
	// Classic write-skew scenario:
	// T1 reads docA (SIREAD on docA for T1)
	// T2 reads docB (SIREAD on docB for T2)
	// T1 writes docB → rw-antidependency: T2 →rw T1 (T2 read docB, T1 wrote docB)
	// T2 writes docA → rw-antidependency: T1 →rw T2 (T1 read docA, T2 wrote docA)
	// T1 commits first
	// T2 tries to commit → dangerous structure: T1(committed) →rw T2 →rw T1

	tracker := NewSIReadTracker()

	// Both transactions read
	tracker.RecordRead(1, []string{"bundle:docA"})
	tracker.RecordRead(2, []string{"bundle:docB"})

	// T1 writes docB (creates: T2 →rw T1)
	tracker.RecordWrite(1, "bundle:docB")

	// T2 writes docA (creates: T1 →rw T2)
	tracker.RecordWrite(2, "bundle:docA")

	// T1 commits first — should succeed
	if err := tracker.CheckCommit(1); err != nil {
		t.Fatalf("T1 commit should succeed, got: %v", err)
	}
	tracker.TransactionCommitted(1)

	// T2 tries to commit — dangerous structure exists: T1(committed) ∈ inConflicts[T2]
	// AND outConflicts[T2] is non-empty
	if err := tracker.CheckCommit(2); err == nil {
		t.Fatal("T2 commit should fail due to dangerous structure (write skew)")
	}
}

func TestSIReadTracker_NonConflictingCommits(t *testing.T) {
	tracker := NewSIReadTracker()

	// T1 reads docA, T2 reads docB — different documents, no conflict
	tracker.RecordRead(1, []string{"bundle:docA"})
	tracker.RecordRead(2, []string{"bundle:docB"})

	// T1 writes docC, T2 writes docD — no rw-antidependencies
	tracker.RecordWrite(1, "bundle:docC")
	tracker.RecordWrite(2, "bundle:docD")

	// Both should commit successfully
	if err := tracker.CheckCommit(1); err != nil {
		t.Errorf("T1 should commit, got: %v", err)
	}
	tracker.TransactionCommitted(1)

	if err := tracker.CheckCommit(2); err != nil {
		t.Errorf("T2 should commit, got: %v", err)
	}
}

func TestSIReadTracker_ReadOnlyNeverAborted(t *testing.T) {
	tracker := NewSIReadTracker()

	// T1 is read-only: reads docA and docB
	tracker.RecordRead(1, []string{"bundle:docA", "bundle:docB"})

	// T2 writes docA (creates rw-antidependency: T1 →rw T2)
	tracker.RecordWrite(2, "bundle:docA")

	// T2 commits
	if err := tracker.CheckCommit(2); err != nil {
		t.Fatalf("T2 should commit, got: %v", err)
	}
	tracker.TransactionCommitted(2)

	// T1 is read-only: no writes → no inConflicts → CheckCommit should pass
	if err := tracker.CheckCommit(1); err != nil {
		t.Errorf("read-only T1 should never be aborted, got: %v", err)
	}
}

func TestSIReadTracker_TransactionFinishedCleanup(t *testing.T) {
	tracker := NewSIReadTracker()

	tracker.RecordRead(1, []string{"bundle:docA"})
	tracker.RecordWrite(2, "bundle:docA")
	tracker.TransactionCommitted(1)

	// Clean up T1
	tracker.TransactionFinished(1)

	// After cleanup, T2 should be able to commit (T1 no longer in committedTxs)
	if err := tracker.CheckCommit(2); err != nil {
		t.Errorf("T2 should commit after T1 cleanup, got: %v", err)
	}

	// Verify stats are clean
	stats := tracker.GetStats()
	if stats["committed_tracking"] != 0 {
		t.Errorf("expected 0 committed_tracking after cleanup, got %d", stats["committed_tracking"])
	}
}

func TestSIReadTracker_ConcurrentReadRecording(t *testing.T) {
	tracker := NewSIReadTracker()

	var wg sync.WaitGroup
	// 100 transactions concurrently recording reads
	for i := uint64(1); i <= 100; i++ {
		wg.Add(1)
		go func(txID uint64) {
			defer wg.Done()
			tracker.RecordRead(txID, []string{"bundle:docA"})
		}(i)
	}
	wg.Wait()

	// Verify all 100 txIDs recorded
	val, ok := tracker.docReaders.Load("bundle:docA")
	if !ok {
		t.Fatal("expected SIREAD set for bundle:docA")
	}
	set := val.(*sireadTxSet)
	set.mu.RLock()
	count := len(set.txIDs)
	set.mu.RUnlock()
	if count != 100 {
		t.Errorf("expected 100 SIREAD locks, got %d", count)
	}
}

func TestSIReadTracker_OneInOneOut_NoCommitted(t *testing.T) {
	// If T0 is NOT committed, there's no dangerous structure even with
	// both inConflicts and outConflicts present.
	tracker := NewSIReadTracker()

	// T0 reads docA → T1 writes docA → T0 →rw T1 (inConflicts[T1] = {T0})
	tracker.RecordRead(10, []string{"bundle:docA"})
	tracker.RecordWrite(11, "bundle:docA")

	// T1 reads docB → T2 writes docB → T1 →rw T2 (outConflicts[T1] = {T2})
	tracker.RecordRead(11, []string{"bundle:docB"})
	tracker.RecordWrite(12, "bundle:docB")

	// T1 has inConflicts (from T0) and outConflicts (to T2), but T0 is NOT committed
	// So T1 should be allowed to commit
	if err := tracker.CheckCommit(11); err != nil {
		t.Errorf("T1 should commit (T0 not committed), got: %v", err)
	}
}
