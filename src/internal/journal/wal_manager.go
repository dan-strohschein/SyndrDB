package journal

/*
WAL MANAGER

This file provides a high-level interface for integrating Write Ahead Logging
with SyndrDB operations. It handles transaction management, automatic logging
of operations, and provides convenient methods for the command director to use.

The WAL manager ensures that all database operations are properly logged
before execution, maintaining ACID properties and enabling recovery.
*/

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"syndrdb/src/pkg/settings"
	"time"

	"github.com/dan-strohschein/HVJson/hvjson"
	"go.uber.org/zap"
)

// WALManager provides a high-level interface for WAL operations
type WALManager struct {
	wal                  *WriteAheadLog
	logger               *zap.SugaredLogger
	activeTxs            map[string]*Transaction
	activeTxsMu          sync.Mutex          // protects activeTxs from concurrent access by multiple connection goroutines
	transactionCounter   *TransactionCounter // Monotonic transaction ID generator for MVCC
	snapshotManager      *SnapshotManager    // MVCC snapshot management
	restorePointManager  *RestorePointManager // Named restore points for PITR
}

// Transaction represents an active database transaction
type Transaction struct {
	ID        string
	StartTime time.Time
}

// NewWALManager creates a new WAL manager instance
func NewWALManager(logger *zap.SugaredLogger) (*WALManager, error) {
	if logger == nil {
		return nil, fmt.Errorf("logger cannot be nil")
	}

	// Get settings for WAL configuration
	settings := settings.GetSettings()

	// Configure WAL based on settings
	config := WALConfig{
		LogDir:             fmt.Sprintf("%s/%s", settings.LogDir, "wal"),
		MaxFileSize:        settings.MaxJournalFileSize,
		FlushInterval:      1 * time.Second,
		RetentionDays:      30,
		FsyncOnCommit:      settings.FsyncOnCommit,
		CompressionEnabled: false,
		EncryptionEnabled:  false,
		AutoFlush:          true,
		DurabilityMode:     settings.DurabilityMode,
		UseGroupCommit:     settings.UseGroupCommit,
		WALBatchSize:       settings.WALBatchSize,
		WALMaxFlushDelay:   time.Duration(settings.WALMaxFlushDelay) * time.Millisecond,
	}

	// Use default log directory if not set
	if config.LogDir == "" {
		config.LogDir = "./log_files/wal"
	}

	// Use default max file size if not set
	if config.MaxFileSize <= 0 {
		config.MaxFileSize = 100 * 1024 * 1024 // 100MB
	}

	wal, err := NewWriteAheadLog(config, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create WAL: %w", err)
	}

	// Initialize restore point manager using WAL directory
	restorePointPath := fmt.Sprintf("%s/restore_points.json", config.LogDir)
	restorePointMgr := NewRestorePointManager(restorePointPath)

	manager := &WALManager{
		wal:                  wal,
		logger:               logger,
		activeTxs:            make(map[string]*Transaction),
		transactionCounter:   NewTransactionCounter(),
		snapshotManager:      NewSnapshotManager(),
		restorePointManager:  restorePointMgr,
	}

	logger.Info("WAL Manager initialized successfully")
	return manager, nil
}

// generateTxID generates a unique transaction ID using monotonic counter
func (wm *WALManager) generateTxID() (string, error) {
	txID, err := wm.transactionCounter.Next()
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%016x", txID), nil
}

// generateTxIDUint64 generates a transaction ID as uint64 (for MVCC)
func (wm *WALManager) generateTxIDUint64() (uint64, error) {
	return wm.transactionCounter.Next()
}

// BeginTransaction starts a new transaction and returns the transaction ID
func (wm *WALManager) BeginTransaction() (string, error) {
	txID, err := wm.generateTxID()
	if err != nil {
		return "", fmt.Errorf("transaction counter: %w", err)
	}

	tx := &Transaction{
		ID:        txID,
		StartTime: time.Now(),
	}

	wm.activeTxsMu.Lock()
	wm.activeTxs[txID] = tx
	err = wm.wal.LogOperation(txID, OpBeginTx, "", "", "", "", "")
	if err != nil {
		delete(wm.activeTxs, txID)
		wm.activeTxsMu.Unlock()
		return "", fmt.Errorf("failed to log transaction begin: %w", err)
	}
	wm.activeTxsMu.Unlock()

	wm.logger.Debugf("Started transaction: %s", txID)
	return txID, nil
}

// CommitTransaction commits a transaction.
// If preallocatedCommitSeq is non-nil, that sequence is used (avoids double GetNextCommitSequence when caller already allocated).
func (wm *WALManager) CommitTransaction(txID string, preallocatedCommitSeq *uint64) error {
	wm.activeTxsMu.Lock()
	tx, exists := wm.activeTxs[txID]
	if !exists {
		wm.activeTxsMu.Unlock()
		return fmt.Errorf("transaction %s not found", txID)
	}
	delete(wm.activeTxs, txID)
	wm.activeTxsMu.Unlock()

	var commitSequence uint64
	if preallocatedCommitSeq != nil {
		commitSequence = *preallocatedCommitSeq
	} else {
		commitSequence = wm.snapshotManager.GetNextCommitSequence()
	}

	err := wm.wal.LogOperation(txID, OpCommitTx, "", "", "", "", "")
	if err != nil {
		return fmt.Errorf("failed to log transaction commit: %w", err)
	}

	// Convert txID string to uint64 for snapshot cleanup
	// txID is hex string, parse it
	var txIDUint64 uint64
	_, err = fmt.Sscanf(txID, "%016x", &txIDUint64)
	if err == nil {
		// Unregister from active transactions
		wm.snapshotManager.UnregisterActiveTransaction(txIDUint64)
		// Remove snapshot
		wm.snapshotManager.RemoveSnapshot(txIDUint64)
	}

	duration := time.Since(tx.StartTime)
	wm.logger.Debugf("Committed transaction: %s (duration: %v, commitSeq: %d)",
		txID, duration, commitSequence)
	return nil
}

// RollbackTransaction rolls back a transaction
func (wm *WALManager) RollbackTransaction(txID string) error {
	wm.activeTxsMu.Lock()
	tx, exists := wm.activeTxs[txID]
	if !exists {
		wm.activeTxsMu.Unlock()
		return fmt.Errorf("transaction %s not found", txID)
	}
	delete(wm.activeTxs, txID)
	wm.activeTxsMu.Unlock()

	// Log the transaction rollback
	err := wm.wal.LogOperation(txID, OpRollbackTx, "", "", "", "", "")
	if err != nil {
		return fmt.Errorf("failed to log transaction rollback: %w", err)
	}

	// Convert txID string to uint64 for snapshot cleanup
	var txIDUint64 uint64
	_, err = fmt.Sscanf(txID, "%016x", &txIDUint64)
	if err == nil {
		// Unregister from active transactions
		wm.snapshotManager.UnregisterActiveTransaction(txIDUint64)
		// Remove snapshot
		wm.snapshotManager.RemoveSnapshot(txIDUint64)
	}

	duration := time.Since(tx.StartTime)
	wm.logger.Debugf("Rolled back transaction: %s (duration: %v)", txID, duration)
	return nil
}

// LogDocumentInsert logs a document insertion operation
func (wm *WALManager) LogDocumentInsert(txID, bundleName, documentID string, documentData interface{}) error {
	// PERF: hvjson is 27-42% faster than encoding/json
	afterData, err := hvjson.Marshal(&documentData)
	if err != nil {
		return fmt.Errorf("failed to marshal document data: %w", err)
	}

	metadata := fmt.Sprintf(`{"bundle_name":"%s","document_id":"%s"}`, bundleName, documentID)

	err = wm.wal.LogOperation(txID, OpInsert, bundleName, documentID, "", string(afterData), metadata)
	if err != nil {
		return fmt.Errorf("failed to log document insert: %w", err)
	}

	wm.logger.Debugf("Logged document insert: bundle=%s, doc=%s, tx=%s", bundleName, documentID, txID)
	return nil
}

// LogBatchInsert logs a bulk document insertion operation as a single WAL entry
func (wm *WALManager) LogBatchInsert(txID, bundleName string, documentIDs []string, documentsData interface{}) error {
	afterData, err := hvjson.Marshal(&documentsData)
	if err != nil {
		return fmt.Errorf("failed to marshal batch documents data: %w", err)
	}

	metadata := fmt.Sprintf(`{"bundle_name":"%s","document_count":%d}`, bundleName, len(documentIDs))

	// Use first document ID as the representative; full list is in afterData
	representativeID := "batch"
	if len(documentIDs) > 0 {
		representativeID = documentIDs[0]
	}

	err = wm.wal.LogOperation(txID, OpBatchInsert, bundleName, representativeID, "", string(afterData), metadata)
	if err != nil {
		return fmt.Errorf("failed to log batch insert: %w", err)
	}

	wm.logger.Debugf("Logged batch insert: bundle=%s, docs=%d, tx=%s", bundleName, len(documentIDs), txID)
	return nil
}

// LogDocumentUpdate logs a document update operation
func (wm *WALManager) LogDocumentUpdate(txID, bundleName, documentID string, beforeData, afterData interface{}) error {
	// PERF: hvjson is 27-42% faster than encoding/json
	beforeJSON, err := hvjson.Marshal(&beforeData)
	if err != nil {
		return fmt.Errorf("failed to marshal before data: %w", err)
	}

	afterJSON, err := hvjson.Marshal(&afterData)
	if err != nil {
		return fmt.Errorf("failed to marshal after data: %w", err)
	}

	metadata := fmt.Sprintf(`{"bundle_name":"%s","document_id":"%s"}`, bundleName, documentID)

	err = wm.wal.LogOperation(txID, OpUpdate, bundleName, documentID, string(beforeJSON), string(afterJSON), metadata)
	if err != nil {
		return fmt.Errorf("failed to log document update: %w", err)
	}

	wm.logger.Debugf("Logged document update: bundle=%s, doc=%s, tx=%s", bundleName, documentID, txID)
	return nil
}

// LogDocumentDelete logs a document deletion operation
func (wm *WALManager) LogDocumentDelete(txID, bundleName, documentID string, documentData interface{}) error {
	// PERF: hvjson is 27-42% faster than encoding/json
	beforeData, err := hvjson.Marshal(&documentData)
	if err != nil {
		return fmt.Errorf("failed to marshal document data: %w", err)
	}

	metadata := fmt.Sprintf(`{"bundle_name":"%s","document_id":"%s"}`, bundleName, documentID)

	err = wm.wal.LogOperation(txID, OpDelete, bundleName, documentID, string(beforeData), "", metadata)
	if err != nil {
		return fmt.Errorf("failed to log document delete: %w", err)
	}

	wm.logger.Debugf("Logged document delete: bundle=%s, doc=%s, tx=%s", bundleName, documentID, txID)
	return nil
}

// LogBundleCreate logs a bundle creation operation
func (wm *WALManager) LogBundleCreate(txID, bundleName string, bundleData interface{}) error {
	// PERF: hvjson is 27-42% faster than encoding/json
	afterData, err := hvjson.Marshal(&bundleData)
	if err != nil {
		return fmt.Errorf("failed to marshal bundle data: %w", err)
	}

	metadata := fmt.Sprintf(`{"bundle_name":"%s"}`, bundleName)

	err = wm.wal.LogOperation(txID, OpCreateBundle, bundleName, "", "", string(afterData), metadata)
	if err != nil {
		return fmt.Errorf("failed to log bundle create: %w", err)
	}

	wm.logger.Debugf("Logged bundle create: bundle=%s, tx=%s", bundleName, txID)
	return nil
}

// LogBundleDelete logs a bundle deletion operation
func (wm *WALManager) LogBundleDelete(txID, bundleName string, bundleData interface{}) error {
	// PERF: hvjson is 27-42% faster than encoding/json
	beforeData, err := hvjson.Marshal(&bundleData)
	if err != nil {
		return fmt.Errorf("failed to marshal bundle data: %w", err)
	}

	metadata := fmt.Sprintf(`{"bundle_name":"%s"}`, bundleName)

	err = wm.wal.LogOperation(txID, OpDeleteBundle, bundleName, "", string(beforeData), "", metadata)
	if err != nil {
		return fmt.Errorf("failed to log bundle delete: %w", err)
	}

	wm.logger.Debugf("Logged bundle delete: bundle=%s, tx=%s", bundleName, txID)
	return nil
}

// LogIndexCreate logs an index creation operation
func (wm *WALManager) LogIndexCreate(txID, bundleName, indexName string, indexData interface{}) error {
	// PERF: hvjson is 27-42% faster than encoding/json
	afterData, err := hvjson.Marshal(&indexData)
	if err != nil {
		return fmt.Errorf("failed to marshal index data: %w", err)
	}

	metadata := fmt.Sprintf(`{"bundle_name":"%s","index_name":"%s"}`, bundleName, indexName)

	err = wm.wal.LogOperation(txID, OpCreateIndex, bundleName, indexName, "", string(afterData), metadata)
	if err != nil {
		return fmt.Errorf("failed to log index create: %w", err)
	}

	wm.logger.Debugf("Logged index create: bundle=%s, index=%s, tx=%s", bundleName, indexName, txID)
	return nil
}

// LogIndexDrop logs an index drop operation
func (wm *WALManager) LogIndexDrop(txID, bundleName, indexName string, indexData interface{}) error {
	// PERF: hvjson is 27-42% faster than encoding/json
	beforeData, err := hvjson.Marshal(&indexData)
	if err != nil {
		return fmt.Errorf("failed to marshal index data: %w", err)
	}

	metadata := fmt.Sprintf(`{"bundle_name":"%s","index_name":"%s"}`, bundleName, indexName)

	err = wm.wal.LogOperation(txID, OpDropIndex, bundleName, indexName, string(beforeData), "", metadata)
	if err != nil {
		return fmt.Errorf("failed to log index drop: %w", err)
	}

	wm.logger.Debugf("Logged index drop: bundle=%s, index=%s, tx=%s", bundleName, indexName, txID)
	return nil
}

// LogDatabaseCreate logs a database creation operation to the WAL.
// This records the creation of a new database, preserving the transaction
// history for future crash recovery and audit purposes.
// Parameters:
//   - txID: The transaction ID this operation belongs to
//   - databaseName: The name of the database being created
//   - databaseData: The database metadata and configuration
//
// Returns: error if marshaling fails or WAL logging fails
func (wm *WALManager) LogDatabaseCreate(txID, databaseName string, databaseData interface{}) error {
	// PERF: hvjson is 27-42% faster than encoding/json
	afterData, err := hvjson.Marshal(&databaseData)
	if err != nil {
		return fmt.Errorf("failed to marshal database data: %w", err)
	}

	metadata := fmt.Sprintf(`{"database_name":"%s","operation":"CREATE_DATABASE"}`, databaseName)

	err = wm.wal.LogOperation(txID, OpCreateDatabase, databaseName, "", "", string(afterData), metadata)
	if err != nil {
		return fmt.Errorf("failed to log database create: %w", err)
	}

	wm.logger.Debugf("Logged database create: database=%s, tx=%s", databaseName, txID)
	return nil
}

// LogDatabaseAttach logs a database attach operation to the WAL.
// This records the attachment of an external database for crash recovery.
func (wm *WALManager) LogDatabaseAttach(txID, databaseName string, attachData interface{}) error {
	afterData, err := hvjson.Marshal(&attachData)
	if err != nil {
		return fmt.Errorf("failed to marshal attach data: %w", err)
	}

	metadata := fmt.Sprintf(`{"database_name":"%s","operation":"ATTACH_DATABASE"}`, databaseName)

	err = wm.wal.LogOperation(txID, OpAttachDatabase, databaseName, "", "", string(afterData), metadata)
	if err != nil {
		return fmt.Errorf("failed to log database attach: %w", err)
	}

	wm.logger.Debugf("Logged database attach: database=%s, tx=%s", databaseName, txID)
	return nil
}

// LogDatabaseDetach logs a database detach operation to the WAL.
// This records the detachment of a database for crash recovery.
func (wm *WALManager) LogDatabaseDetach(txID, databaseName string, detachData interface{}) error {
	afterData, err := hvjson.Marshal(&detachData)
	if err != nil {
		return fmt.Errorf("failed to marshal detach data: %w", err)
	}

	metadata := fmt.Sprintf(`{"database_name":"%s","operation":"DETACH_DATABASE"}`, databaseName)

	err = wm.wal.LogOperation(txID, OpDetachDatabase, databaseName, "", "", string(afterData), metadata)
	if err != nil {
		return fmt.Errorf("failed to log database detach: %w", err)
	}

	wm.logger.Debugf("Logged database detach: database=%s, tx=%s", databaseName, txID)
	return nil
}

// LogRelationshipDrop logs a relationship drop operation to the WAL.
// This records the removal of a relationship metadata from a bundle, preserving the transaction
// history for future crash recovery and audit purposes.
// Parameters:
//   - txID: The transaction ID this operation belongs to
//   - bundleName: The name of the source bundle containing the relationship
//   - relationshipName: The name of the relationship being dropped (e.g., "Authors_Books_1")
//
// Returns: error if marshaling fails or WAL logging fails
// TODO: Implement replay logic for DROP_RELATIONSHIP operations in WAL recovery process to restore database state after crash
func (wm *WALManager) LogRelationshipDrop(txID, bundleName, relationshipName string) error {
	metadata := fmt.Sprintf(`{"bundle_name":"%s","relationship_name":"%s","operation":"DROP_RELATIONSHIP"}`, bundleName, relationshipName)

	// Log the relationship drop operation
	// We use empty beforeData and afterData as this is a metadata-only operation
	err := wm.wal.LogOperation(txID, OpDropRelationship, bundleName, relationshipName, "", "", metadata)
	if err != nil {
		return fmt.Errorf("failed to log relationship drop: %w", err)
	}

	wm.logger.Debugf("Logged relationship drop: bundle=%s, relationship=%s, tx=%s", bundleName, relationshipName, txID)
	return nil
}

// ExecuteWithLogging executes a function within a transaction with automatic logging
// PHASE 2: MVCC - Autocommit mode: Creates per-statement snapshot for MVCC isolation
func (wm *WALManager) ExecuteWithLogging(operation func(txID string) error) error {
	// PHASE 2: MVCC - Use BeginTransactionUint64 for autocommit to get snapshot
	// This ensures each autocommit statement has its own snapshot for MVCC isolation
	txIDUint64, err := wm.BeginTransactionUint64()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	// Convert to string for compatibility with existing operation function
	txID := fmt.Sprintf("%016x", txIDUint64)

	// Execute the operation
	if err := operation(txID); err != nil {
		// Rollback on error
		if rollbackErr := wm.RollbackTransaction(txID); rollbackErr != nil {
			wm.logger.Errorf("Failed to rollback transaction %s: %v", txID, rollbackErr)
		}
		return err
	}

	// Commit on success (nil = allocate commit sequence here; explicit-tx path passes preallocated)
	if err := wm.CommitTransaction(txID, nil); err != nil {
		// Try to rollback if commit fails
		if rollbackErr := wm.RollbackTransaction(txID); rollbackErr != nil {
			wm.logger.Errorf("Failed to rollback transaction %s after commit failure: %v", txID, rollbackErr)
		}
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	// PHASE 2: MVCC - Snapshot is automatically cleaned up in CommitTransaction
	// via snapshotManager.RemoveSnapshot() and UnregisterActiveTransaction()

	return nil
}

// Flush forces all pending WAL entries to be written to disk
func (wm *WALManager) Flush() error {
	return wm.wal.Flush()
}

// Close gracefully shuts down the WAL manager
func (wm *WALManager) Close() error {
	// Collect active txIDs under lock to avoid concurrent map iteration
	wm.activeTxsMu.Lock()
	ids := make([]string, 0, len(wm.activeTxs))
	for txID := range wm.activeTxs {
		ids = append(ids, txID)
	}
	wm.activeTxsMu.Unlock()

	for _, txID := range ids {
		if err := wm.RollbackTransaction(txID); err != nil {
			wm.logger.Errorf("Failed to rollback active transaction %s during shutdown: %v", txID, err)
		}
	}

	// Close the WAL
	if err := wm.wal.Close(); err != nil {
		return fmt.Errorf("failed to close WAL: %w", err)
	}

	wm.logger.Info("WAL Manager closed successfully")
	return nil
}

// GetCurrentLSN returns the current Log Sequence Number
func (wm *WALManager) GetCurrentLSN() uint64 {
	return wm.wal.GetCurrentLSN()
}

// CleanupOldFiles removes old WAL files based on retention policy
func (wm *WALManager) CleanupOldFiles() error {
	return wm.wal.CleanupOldFiles()
}

// ReplayOperations replays WAL operations for recovery
func (wm *WALManager) ReplayOperations(fromLSN uint64, replayFunc func(WALEntry) error) error {
	return wm.wal.ReplayOperations(fromLSN, replayFunc)
}

// UndoToLSN performs a rollback by applying before-images from current LSN to target LSN
// This is used for transaction rollback and savepoint rollback
// bundleManager is the interface to apply undo operations to actual data
// lockManager is the lock manager to release locks after undo (can be nil if locks handled separately)
func (wm *WALManager) UndoToLSN(targetLSN uint64, txID string, undoFunc func(WALEntry) error, lockManager interface{}) error {
	if undoFunc == nil {
		return fmt.Errorf("undoFunc cannot be nil")
	}

	currentLSN := wm.wal.GetCurrentLSN()
	if targetLSN >= currentLSN {
		wm.logger.Debugf("No undo needed: targetLSN=%d >= currentLSN=%d", targetLSN, currentLSN)
		return nil
	}

	wm.logger.Debugf("Starting undo from LSN %d to LSN %d for txID=%s", currentLSN, targetLSN, txID)

	// Collect entries to undo (we need to read forward then apply backward)
	entriesToUndo := make([]WALEntry, 0)

	// Debug: track all entries seen during replay
	totalEntriesSeen := 0
	matchingTxID := 0
	matchingLSN := 0

	// Read entries from target LSN to current LSN
	err := wm.wal.ReplayOperations(targetLSN, func(entry WALEntry) error {
		totalEntriesSeen++
		wm.logger.Debugf("REPLAY: LSN=%d, TxID=%s, Op=%d, targetLSN=%d, checkTxID=%s",
			entry.LSN, entry.TxID, entry.Operation, targetLSN, txID)

		// Check if txID matches
		if entry.TxID == txID {
			matchingTxID++
			wm.logger.Debugf("REPLAY: TxID matches!")

			// Check if LSN is after target
			if entry.LSN > targetLSN {
				matchingLSN++
				entriesToUndo = append(entriesToUndo, entry)
				wm.logger.Debugf("REPLAY: Entry added to undo list! Total: %d", len(entriesToUndo))
			} else {
				wm.logger.Debugf("REPLAY: LSN %d <= targetLSN %d, skipping", entry.LSN, targetLSN)
			}
		} else {
			wm.logger.Debugf("REPLAY: TxID mismatch (entry=%s, target=%s)", entry.TxID, txID)
		}

		return nil
	})

	wm.logger.Debugf("REPLAY SUMMARY: totalSeen=%d, matchingTxID=%d, matchingLSN=%d, toUndo=%d",
		totalEntriesSeen, matchingTxID, matchingLSN, len(entriesToUndo))

	if err != nil {
		return fmt.Errorf("failed to read WAL entries for undo: %w", err)
	}

	// Apply undo operations in reverse order (most recent first)
	undoCount := 0
	for i := len(entriesToUndo) - 1; i >= 0; i-- {
		entry := entriesToUndo[i]

		// Skip transaction control operations (they don't need undo)
		if entry.Operation == OpBeginTx || entry.Operation == OpCommitTx || entry.Operation == OpRollbackTx {
			continue
		}

		// Apply the undo operation using the before-image
		if err := undoFunc(entry); err != nil {
			return fmt.Errorf("failed to undo operation at LSN %d: %w", entry.LSN, err)
		}

		undoCount++
		wm.logger.Debugf("Undid operation: LSN=%d, Op=%d, Bundle=%s, Doc=%s",
			entry.LSN, entry.Operation, entry.BundleName, entry.DocumentID)
	}

	// Release locks if lock manager provided
	if lockManager != nil {
		// Type assert to get the ReleaseLocks method
		// This allows us to pass the lock manager interface without importing it
		type lockReleaser interface {
			ReleaseLocks(txID string) int
		}

		if lr, ok := lockManager.(lockReleaser); ok {
			releaseCount := lr.ReleaseLocks(txID)
			wm.logger.Debugf("Released %d locks for txID=%s during undo", releaseCount, txID)
		}
	}

	wm.logger.Debugf("Undo complete: %d operations reversed for txID=%s", undoCount, txID)
	return nil
}

// GetTransactionCounter returns the transaction counter for persistence/recovery
func (wm *WALManager) GetTransactionCounter() *TransactionCounter {
	return wm.transactionCounter
}

// InitializeTransactionCounter sets the counter value during recovery
// Should be called after loading persisted counter value or scanning WAL
func (wm *WALManager) InitializeTransactionCounter(value uint64) {
	wm.transactionCounter.SetValue(value)
	wm.logger.Debugf("Initialized transaction counter to %d", value)
}

// BeginTransactionUint64 starts a new transaction and returns the transaction ID as uint64
// This is the MVCC-compatible version that returns numeric IDs
func (wm *WALManager) BeginTransactionUint64() (uint64, error) {
	txID, err := wm.generateTxIDUint64()
	if err != nil {
		return 0, fmt.Errorf("transaction counter: %w", err)
	}
	txIDStr := fmt.Sprintf("%016x", txID)

	tx := &Transaction{
		ID:        txIDStr,
		StartTime: time.Now(),
	}

	wm.activeTxsMu.Lock()
	wm.activeTxs[txIDStr] = tx
	wm.activeTxsMu.Unlock()

	// Register transaction as active in snapshot manager
	wm.snapshotManager.RegisterActiveTransaction(txID)

	// Create snapshot for this transaction
	snapshot, err := wm.snapshotManager.CreateSnapshot(txID)
	if err != nil {
		wm.activeTxsMu.Lock()
		delete(wm.activeTxs, txIDStr)
		wm.activeTxsMu.Unlock()
		wm.snapshotManager.UnregisterActiveTransaction(txID)
		return 0, fmt.Errorf("failed to create snapshot: %w", err)
	}

	// Log the transaction begin
	err = wm.wal.LogOperation(txIDStr, OpBeginTx, "", "", "", "", "")
	if err != nil {
		wm.activeTxsMu.Lock()
		delete(wm.activeTxs, txIDStr)
		wm.activeTxsMu.Unlock()
		wm.snapshotManager.UnregisterActiveTransaction(txID)
		wm.snapshotManager.RemoveSnapshot(txID)
		return 0, fmt.Errorf("failed to log transaction begin: %w", err)
	}

	wm.logger.Debugf("Started transaction: %d (hex: %s) with snapshot sequence %d", txID, txIDStr, snapshot.SnapshotSequence)
	return txID, nil
}

// GetSnapshotManager returns the snapshot manager for external access
func (wm *WALManager) GetSnapshotManager() *SnapshotManager {
	return wm.snapshotManager
}

// LogCommitSequenceAssign logs a batch commit sequence assignment operation
// PHASE 2: MVCC - Used for async batch commit sequence updates
func (wm *WALManager) LogCommitSequenceAssign(txID string, commitSequence uint64, documentLocationsJSON string) error {
	metadata := fmt.Sprintf("commitSequence=%d", commitSequence)
	err := wm.wal.LogOperation(txID, OpCommitSequenceAssign, "", "", "", documentLocationsJSON, metadata)
	if err != nil {
		return fmt.Errorf("failed to log commit sequence assignment: %w", err)
	}
	wm.logger.Debugf("Logged commit sequence assignment: tx=%s, commitSeq=%d", txID, commitSequence)
	return nil
}

// RecoverCommitSequenceAssignments replays COMMIT_SEQUENCE_ASSIGN entries during recovery
// PHASE 2: MVCC - WAL Recovery
// This function should be called during server startup to replay any unprocessed commit sequence assignments
// Returns the highest commit sequence found, which should be used to initialize the snapshot manager
func (wm *WALManager) RecoverCommitSequenceAssignments() (uint64, error) {
	highestCommitSeq := uint64(0)
	recoveredCount := 0

	err := wm.wal.ReplayOperations(0, func(entry WALEntry) error {
		if entry.Operation == OpCommitSequenceAssign {
			// Parse commit sequence from metadata
			var commitSeq uint64
			_, err := fmt.Sscanf(entry.Metadata, "commitSequence=%d", &commitSeq)
			if err != nil {
				wm.logger.Warnf("Failed to parse commit sequence from WAL entry LSN=%d: %v", entry.LSN, err)
				return nil // Continue recovery even if one entry fails
			}

			// Track highest commit sequence
			if commitSeq > highestCommitSeq {
				highestCommitSeq = commitSeq
			}

			// Parse document locations from AfterData (JSON)
			var documentLocations map[string]interface{}
			if entry.AfterData != "" {
				if err := hvjson.Unmarshal([]byte(entry.AfterData), &documentLocations); err != nil {
					wm.logger.Warnf("Failed to parse document locations from WAL entry LSN=%d: %v", entry.LSN, err)
					// Continue recovery - document locations parsing failure doesn't block recovery
				} else {
					// TODO: Replay document commit sequence updates
					// For now, we just track the highest commit sequence
					// Full implementation will update documents with commit sequences
					wm.logger.Debugf("Recovered COMMIT_SEQUENCE_ASSIGN: txID=%s, commitSeq=%d, documents=%d",
						entry.TxID, commitSeq, len(documentLocations))
					recoveredCount++
				}
			}
		}
		return nil
	})

	if err != nil {
		return highestCommitSeq, fmt.Errorf("failed to recover commit sequence assignments: %w", err)
	}

	if highestCommitSeq > 0 {
		// Initialize snapshot manager with recovered commit sequence
		wm.snapshotManager.SetGlobalSequence(highestCommitSeq)
		wm.logger.Debugf("Recovered %d COMMIT_SEQUENCE_ASSIGN entries, highest commit sequence: %d",
			recoveredCount, highestCommitSeq)
	}

	return highestCommitSeq, nil
}

// RecoveryResult holds statistics from a crash recovery run.
type RecoveryResult struct {
	RedoneOps             int    // Committed operations replayed
	UndoneOps             int    // Uncommitted operations rolled back
	UncommittedTxCount    int    // Transactions that were rolled back
	HighestCommitSequence uint64 // Restored global commit sequence
	HighestTxID           uint64 // Restored transaction counter
	CheckpointLSN         uint64 // LSN we recovered from
	Errors                int    // Non-fatal errors encountered
}

// FindLastCheckpoint scans the WAL to find the most recent OpCheckpointComplete entry.
// Returns the startLSN recorded in that checkpoint's metadata, or 0 if no checkpoint found.
func (wm *WALManager) FindLastCheckpoint() (uint64, error) {
	var lastCheckpointLSN uint64

	err := wm.wal.ReplayOperations(0, func(entry WALEntry) error {
		if entry.Operation == OpCheckpointComplete {
			// Parse startLSN from metadata: "startLSN=123,pagesSynced=45"
			if strings.HasPrefix(entry.Metadata, "startLSN=") {
				parts := strings.SplitN(entry.Metadata, ",", 2)
				if lsn, err := strconv.ParseUint(strings.TrimPrefix(parts[0], "startLSN="), 10, 64); err == nil {
					lastCheckpointLSN = lsn
				}
			}
		}
		return nil
	})

	return lastCheckpointLSN, err
}

// RecoverOnStartup performs automatic crash recovery by replaying the WAL from the last
// checkpoint. It identifies committed and uncommitted transactions, redoes committed
// operations, and undoes uncommitted ones. This should be called during server startup
// before accepting client connections.
//
// undoFunc is called for each WAL entry that needs to be undone (uncommitted transactions).
// It receives the WAL entry and should apply the before-image to reverse the operation.
// If undoFunc is nil, uncommitted transactions are logged but their effects remain
// (best-effort recovery mode for when the undo infrastructure isn't available).
func (wm *WALManager) RecoverOnStartup(undoFunc func(WALEntry) error) (*RecoveryResult, error) {
	startTime := time.Now()
	result := &RecoveryResult{}

	wm.logger.Info("Starting WAL crash recovery...")

	// Phase 1: Find last checkpoint
	checkpointLSN, err := wm.FindLastCheckpoint()
	if err != nil {
		wm.logger.Warnf("Failed to find last checkpoint, recovering from LSN 0: %v", err)
		checkpointLSN = 0
	}
	result.CheckpointLSN = checkpointLSN
	wm.logger.Infof("Recovery starting from checkpoint LSN %d", checkpointLSN)

	// Phase 2: Replay WAL and classify transactions
	// Track transaction state: txID → committed (true) or still open (false)
	txState := make(map[string]bool)     // txID → true=committed, false=open
	txEntries := make(map[string][]WALEntry) // txID → entries for undo if uncommitted
	var highestCommitSeq uint64
	var highestTxID uint64

	err = wm.wal.ReplayOperations(checkpointLSN, func(entry WALEntry) error {
		// Track highest txID seen (for transaction counter recovery)
		if entry.TxID != "" {
			if txIDVal, parseErr := strconv.ParseUint(entry.TxID, 16, 64); parseErr == nil {
				if txIDVal > highestTxID {
					highestTxID = txIDVal
				}
			}
		}

		switch entry.Operation {
		case OpBeginTx:
			txState[entry.TxID] = false // open
		case OpCommitTx:
			txState[entry.TxID] = true // committed
			// Extract commit sequence from metadata if present
			if strings.Contains(entry.Metadata, "commitSequence=") {
				var seq uint64
				if _, scanErr := fmt.Sscanf(entry.Metadata, "commitSequence=%d", &seq); scanErr == nil {
					if seq > highestCommitSeq {
						highestCommitSeq = seq
					}
				}
			}
			// Remove undo entries for committed transactions (save memory)
			delete(txEntries, entry.TxID)
			result.RedoneOps++
		case OpRollbackTx:
			txState[entry.TxID] = true // treated as resolved (already rolled back)
			delete(txEntries, entry.TxID)
		case OpCommitSequenceAssign:
			// Track highest commit sequence from explicit assignments
			var seq uint64
			if _, scanErr := fmt.Sscanf(entry.Metadata, "commitSequence=%d", &seq); scanErr == nil {
				if seq > highestCommitSeq {
					highestCommitSeq = seq
				}
			}
		case OpInsert, OpUpdate, OpDelete, OpBatchInsert:
			// Track data operations for potential undo
			if entry.TxID != "" {
				if committed, known := txState[entry.TxID]; known && !committed {
					txEntries[entry.TxID] = append(txEntries[entry.TxID], entry)
				} else if !known {
					// Transaction began before our replay window — mark as open
					txState[entry.TxID] = false
					txEntries[entry.TxID] = append(txEntries[entry.TxID], entry)
				}
			}
		}
		return nil
	})
	if err != nil {
		return result, fmt.Errorf("WAL replay failed: %w", err)
	}

	// Phase 3: Undo uncommitted transactions
	if undoFunc != nil {
		for txID, committed := range txState {
			if !committed {
				entries := txEntries[txID]
				if len(entries) == 0 {
					continue
				}
				result.UncommittedTxCount++
				wm.logger.Infof("Rolling back uncommitted transaction %s (%d operations)", txID, len(entries))

				// Undo in reverse order (last operation first)
				for i := len(entries) - 1; i >= 0; i-- {
					if err := undoFunc(entries[i]); err != nil {
						wm.logger.Warnf("Failed to undo entry LSN=%d for tx %s: %v", entries[i].LSN, txID, err)
						result.Errors++
					} else {
						result.UndoneOps++
					}
				}
			}
		}
	} else {
		// Count uncommitted but don't undo
		for _, committed := range txState {
			if !committed {
				result.UncommittedTxCount++
			}
		}
		if result.UncommittedTxCount > 0 {
			wm.logger.Warnf("Found %d uncommitted transactions but no undo function provided (best-effort recovery)",
				result.UncommittedTxCount)
		}
	}

	// Phase 4: Restore global state
	result.HighestCommitSequence = highestCommitSeq
	result.HighestTxID = highestTxID

	if highestCommitSeq > 0 {
		wm.snapshotManager.SetGlobalSequence(highestCommitSeq)
		wm.logger.Infof("Restored global commit sequence to %d", highestCommitSeq)
	}

	if highestTxID > 0 {
		wm.transactionCounter.SetValue(highestTxID + 1)
		wm.logger.Infof("Restored transaction counter to %d", highestTxID+1)
	}

	duration := time.Since(startTime)
	wm.logger.Infof("WAL crash recovery complete in %v: checkpoint=%d, redone=%d, undone=%d, uncommitted_txs=%d, errors=%d, commitSeq=%d, txID=%d",
		duration, checkpointLSN, result.RedoneOps, result.UndoneOps, result.UncommittedTxCount,
		result.Errors, highestCommitSeq, highestTxID)

	return result, nil
}

// CreateRestorePoint creates a named restore point with the current LSN and commit sequence.
// The restore point is recorded both in the WAL (as OpRestorePoint) and in the
// RestorePointManager's persistent storage.
func (wm *WALManager) CreateRestorePoint(name string) (*RestorePoint, error) {
	if name == "" {
		return nil, fmt.Errorf("restore point name cannot be empty")
	}

	lsn := wm.wal.GetCurrentLSN()
	commitSeq := wm.snapshotManager.GetCurrentSequence()

	// Log the restore point to WAL (name stored in Metadata field)
	if err := wm.wal.LogOperation("", OpRestorePoint, "", "", "", "", name); err != nil {
		return nil, fmt.Errorf("failed to log restore point to WAL: %w", err)
	}

	// Create in the restore point manager
	rp, err := wm.restorePointManager.Create(name, lsn, commitSeq)
	if err != nil {
		return nil, fmt.Errorf("failed to create restore point: %w", err)
	}

	wm.logger.Infow("Created restore point",
		"name", name,
		"lsn", lsn,
		"commit_seq", commitSeq,
	)

	return rp, nil
}

// GetRestorePointManager returns the restore point manager.
func (wm *WALManager) GetRestorePointManager() *RestorePointManager {
	return wm.restorePointManager
}

// GetWALDir returns the WAL directory path.
func (wm *WALManager) GetWALDir() string {
	return wm.wal.baseFilePath
}

// GetWAL returns the underlying WriteAheadLog instance.
func (wm *WALManager) GetWAL() *WriteAheadLog {
	return wm.wal
}
