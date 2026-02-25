package journal

import (
	"os"
	"testing"

	"go.uber.org/zap"
)

// createTestWALManager creates a WALManager with a temporary directory for testing.
func createTestWALManager(t *testing.T) (*WALManager, string) {
	t.Helper()
	tmpDir, err := os.MkdirTemp("", "syndrdb-wal-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}

	logger := zap.NewNop().Sugar()
	wal, err := NewWriteAheadLog(WALConfig{
		LogDir:         tmpDir,
		MaxFileSize:    100 * 1024 * 1024, // 100MB
		DurabilityMode: "performance",
	}, logger)
	if err != nil {
		os.RemoveAll(tmpDir)
		t.Fatalf("failed to create WAL: %v", err)
	}

	wm := &WALManager{
		wal:                wal,
		logger:             logger,
		activeTxs:          make(map[string]*Transaction),
		transactionCounter: NewTransactionCounter(),
		snapshotManager:    NewSnapshotManager(),
	}

	return wm, tmpDir
}

func TestRecoverOnStartup_CleanShutdown(t *testing.T) {
	wm, tmpDir := createTestWALManager(t)
	defer os.RemoveAll(tmpDir)

	// Simulate a clean session: begin tx → insert → commit
	txID, err := wm.BeginTransaction()
	if err != nil {
		t.Fatalf("BeginTransaction: %v", err)
	}
	if err := wm.LogDocumentInsert(txID, "testbundle", "doc1", map[string]interface{}{"x": 1}); err != nil {
		t.Fatalf("LogDocumentInsert: %v", err)
	}
	if err := wm.CommitTransaction(txID, nil); err != nil {
		t.Fatalf("CommitTransaction: %v", err)
	}
	wm.Flush()

	// Run recovery — should find the committed transaction
	result, err := wm.RecoverOnStartup(nil)
	if err != nil {
		t.Fatalf("RecoverOnStartup: %v", err)
	}

	if result.RedoneOps != 1 {
		t.Errorf("RedoneOps = %d, want 1", result.RedoneOps)
	}
	if result.UncommittedTxCount != 0 {
		t.Errorf("UncommittedTxCount = %d, want 0", result.UncommittedTxCount)
	}
}

func TestRecoverOnStartup_UncommittedTransaction(t *testing.T) {
	wm, tmpDir := createTestWALManager(t)
	defer os.RemoveAll(tmpDir)

	// Simulate a crash: begin tx → insert → NO commit
	txID, err := wm.BeginTransaction()
	if err != nil {
		t.Fatalf("BeginTransaction: %v", err)
	}
	if err := wm.LogDocumentInsert(txID, "testbundle", "doc1", map[string]interface{}{"x": 1}); err != nil {
		t.Fatalf("LogDocumentInsert: %v", err)
	}
	wm.Flush()

	// Run recovery with undo function
	undoneCount := 0
	undoFunc := func(entry WALEntry) error {
		undoneCount++
		return nil
	}

	result, err := wm.RecoverOnStartup(undoFunc)
	if err != nil {
		t.Fatalf("RecoverOnStartup: %v", err)
	}

	if result.UncommittedTxCount != 1 {
		t.Errorf("UncommittedTxCount = %d, want 1", result.UncommittedTxCount)
	}
	if result.UndoneOps != 1 {
		t.Errorf("UndoneOps = %d, want 1", result.UndoneOps)
	}
	if undoneCount != 1 {
		t.Errorf("undoFunc called %d times, want 1", undoneCount)
	}
}

func TestRecoverOnStartup_RestoresTransactionCounter(t *testing.T) {
	wm, tmpDir := createTestWALManager(t)
	defer os.RemoveAll(tmpDir)

	// Create several transactions
	for i := 0; i < 5; i++ {
		txID, err := wm.BeginTransaction()
		if err != nil {
			t.Fatalf("BeginTransaction %d: %v", i, err)
		}
		if err := wm.CommitTransaction(txID, nil); err != nil {
			t.Fatalf("CommitTransaction %d: %v", i, err)
		}
	}
	wm.Flush()

	// Reset the counter to simulate a restart
	wm.transactionCounter.SetValue(0)

	result, err := wm.RecoverOnStartup(nil)
	if err != nil {
		t.Fatalf("RecoverOnStartup: %v", err)
	}

	if result.HighestTxID == 0 {
		t.Error("HighestTxID should be > 0 after recovery")
	}

	// The transaction counter should be restored to highest + 1
	currentCounter := wm.transactionCounter.GetCurrent()
	if currentCounter <= result.HighestTxID {
		t.Errorf("transaction counter %d should be > highest recovered txID %d",
			currentCounter, result.HighestTxID)
	}
}

func TestRecoverOnStartup_Idempotent(t *testing.T) {
	wm, tmpDir := createTestWALManager(t)
	defer os.RemoveAll(tmpDir)

	// Create a committed transaction
	txID, err := wm.BeginTransaction()
	if err != nil {
		t.Fatalf("BeginTransaction: %v", err)
	}
	if err := wm.LogDocumentInsert(txID, "testbundle", "doc1", map[string]interface{}{"x": 1}); err != nil {
		t.Fatalf("LogDocumentInsert: %v", err)
	}
	if err := wm.CommitTransaction(txID, nil); err != nil {
		t.Fatalf("CommitTransaction: %v", err)
	}
	wm.Flush()

	// Run recovery twice — should produce the same result
	result1, err := wm.RecoverOnStartup(nil)
	if err != nil {
		t.Fatalf("RecoverOnStartup (1st): %v", err)
	}

	result2, err := wm.RecoverOnStartup(nil)
	if err != nil {
		t.Fatalf("RecoverOnStartup (2nd): %v", err)
	}

	if result1.RedoneOps != result2.RedoneOps {
		t.Errorf("recovery not idempotent: RedoneOps %d vs %d", result1.RedoneOps, result2.RedoneOps)
	}
	if result1.UncommittedTxCount != result2.UncommittedTxCount {
		t.Errorf("recovery not idempotent: UncommittedTxCount %d vs %d",
			result1.UncommittedTxCount, result2.UncommittedTxCount)
	}
}

func TestFindLastCheckpoint_NoCheckpoints(t *testing.T) {
	wm, tmpDir := createTestWALManager(t)
	defer os.RemoveAll(tmpDir)

	// No checkpoints logged — should return 0
	lsn, err := wm.FindLastCheckpoint()
	if err != nil {
		t.Fatalf("FindLastCheckpoint: %v", err)
	}
	if lsn != 0 {
		t.Errorf("expected LSN 0 with no checkpoints, got %d", lsn)
	}
}

func TestFindLastCheckpoint_WithCheckpoint(t *testing.T) {
	wm, tmpDir := createTestWALManager(t)
	defer os.RemoveAll(tmpDir)

	// Log a checkpoint complete marker
	metadata := "startLSN=42,pagesSynced=10"
	if err := wm.wal.LogOperation("", OpCheckpointComplete, "", "", "", "", metadata); err != nil {
		t.Fatalf("LogOperation: %v", err)
	}
	wm.Flush()

	lsn, err := wm.FindLastCheckpoint()
	if err != nil {
		t.Fatalf("FindLastCheckpoint: %v", err)
	}
	if lsn != 42 {
		t.Errorf("expected LSN 42, got %d", lsn)
	}
}
