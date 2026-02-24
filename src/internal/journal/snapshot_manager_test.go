package journal

import (
	"testing"
)

func TestCreateStatementSnapshotReturnsFreshSequence(t *testing.T) {
	sm := NewSnapshotManager()
	sm.SetGlobalSequence(100)

	// Register a transaction
	txID := uint64(1)
	sm.RegisterActiveTransaction(txID)

	// Create transaction snapshot at sequence 100
	txSnapshot, err := sm.CreateSnapshot(txID)
	if err != nil {
		t.Fatalf("CreateSnapshot error: %v", err)
	}
	if txSnapshot.SnapshotSequence != 100 {
		t.Errorf("transaction snapshot seq = %d, want 100", txSnapshot.SnapshotSequence)
	}

	// Simulate some commits advancing the global sequence
	sm.GetNextCommitSequence() // 101
	sm.GetNextCommitSequence() // 102
	sm.GetNextCommitSequence() // 103

	// Create statement snapshot - should see fresh sequence
	stmtSnapshot := sm.CreateStatementSnapshot(txID)
	if stmtSnapshot.SnapshotSequence != 103 {
		t.Errorf("statement snapshot seq = %d, want 103", stmtSnapshot.SnapshotSequence)
	}

	// Original transaction snapshot should be unchanged
	if txSnapshot.SnapshotSequence != 100 {
		t.Errorf("transaction snapshot was mutated: seq = %d, want 100", txSnapshot.SnapshotSequence)
	}
}

func TestCreateStatementSnapshotExcludesOwnTxID(t *testing.T) {
	sm := NewSnapshotManager()
	sm.SetGlobalSequence(50)

	txID := uint64(1)
	otherTxID := uint64(2)

	sm.RegisterActiveTransaction(txID)
	sm.RegisterActiveTransaction(otherTxID)

	stmtSnapshot := sm.CreateStatementSnapshot(txID)

	// Should NOT contain own txID
	if stmtSnapshot.ActiveTxIDs[txID] {
		t.Error("statement snapshot should not contain own txID")
	}

	// Should contain other active txID
	if !stmtSnapshot.ActiveTxIDs[otherTxID] {
		t.Error("statement snapshot should contain other active txID")
	}
}

func TestCreateStatementSnapshotDoesNotAffectOldestSnapshot(t *testing.T) {
	sm := NewSnapshotManager()
	sm.SetGlobalSequence(50)

	txID := uint64(1)
	sm.RegisterActiveTransaction(txID)

	// Create transaction snapshot (anchors oldest at 50)
	_, err := sm.CreateSnapshot(txID)
	if err != nil {
		t.Fatalf("CreateSnapshot error: %v", err)
	}

	oldestBefore := sm.GetOldestActiveSnapshot()

	// Advance sequence
	sm.GetNextCommitSequence() // 51
	sm.GetNextCommitSequence() // 52

	// Statement snapshot should NOT affect oldest snapshot
	_ = sm.CreateStatementSnapshot(txID)

	oldestAfter := sm.GetOldestActiveSnapshot()
	if oldestAfter != oldestBefore {
		t.Errorf("oldest snapshot changed: before=%d, after=%d", oldestBefore, oldestAfter)
	}
}

func TestCreateStatementSnapshotDoesNotStoreInActiveSnapshots(t *testing.T) {
	sm := NewSnapshotManager()
	sm.SetGlobalSequence(50)

	txID := uint64(1)
	sm.RegisterActiveTransaction(txID)

	// Create transaction snapshot
	_, err := sm.CreateSnapshot(txID)
	if err != nil {
		t.Fatalf("CreateSnapshot error: %v", err)
	}

	// Create statement snapshot
	_ = sm.CreateStatementSnapshot(txID)

	// activeSnapshots should still only have one entry (the transaction snapshot)
	sm.mu.RLock()
	count := len(sm.activeSnapshots)
	sm.mu.RUnlock()

	if count != 1 {
		t.Errorf("activeSnapshots count = %d, want 1 (statement snapshot should not be stored)", count)
	}
}

func TestCreateStatementSnapshotTransactionID(t *testing.T) {
	sm := NewSnapshotManager()
	sm.SetGlobalSequence(50)

	txID := uint64(42)
	sm.RegisterActiveTransaction(txID)

	stmtSnapshot := sm.CreateStatementSnapshot(txID)
	if stmtSnapshot.TransactionID != txID {
		t.Errorf("statement snapshot txID = %d, want %d", stmtSnapshot.TransactionID, txID)
	}
}
