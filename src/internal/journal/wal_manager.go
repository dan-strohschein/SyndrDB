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
	"encoding/json"
	"fmt"
	"syndrdb/src/pkg/settings"
	"time"

	"go.uber.org/zap"
)

// WALManager provides a high-level interface for WAL operations
type WALManager struct {
	wal                *WriteAheadLog
	logger             *zap.SugaredLogger
	activeTxs          map[string]*Transaction
	transactionCounter *TransactionCounter // Monotonic transaction ID generator for MVCC
	snapshotManager    *SnapshotManager    // MVCC snapshot management
}

// Transaction represents an active database transaction
type Transaction struct {
	ID         string
	StartTime  time.Time
	Operations []WALEntry
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

	manager := &WALManager{
		wal:                wal,
		logger:             logger,
		activeTxs:          make(map[string]*Transaction),
		transactionCounter: NewTransactionCounter(),
		snapshotManager:    NewSnapshotManager(),
	}

	logger.Info("WAL Manager initialized successfully")
	return manager, nil
}

// generateTxID generates a unique transaction ID using monotonic counter
// Returns string for backward compatibility, but internally uses uint64
func (wm *WALManager) generateTxID() string {
	txID := wm.transactionCounter.Next()
	// Convert uint64 to hex string for backward compatibility
	return fmt.Sprintf("%016x", txID)
}

// generateTxIDUint64 generates a transaction ID as uint64 (for MVCC)
func (wm *WALManager) generateTxIDUint64() uint64 {
	return wm.transactionCounter.Next()
}

// BeginTransaction starts a new transaction and returns the transaction ID
func (wm *WALManager) BeginTransaction() (string, error) {
	txID := wm.generateTxID()

	tx := &Transaction{
		ID:         txID,
		StartTime:  time.Now(),
		Operations: make([]WALEntry, 0),
	}

	wm.activeTxs[txID] = tx

	// Log the transaction begin
	err := wm.wal.LogOperation(txID, OpBeginTx, "", "", "", "", "")
	if err != nil {
		delete(wm.activeTxs, txID)
		return "", fmt.Errorf("failed to log transaction begin: %w", err)
	}

	wm.logger.Debugf("Started transaction: %s", txID)
	return txID, nil
}

// CommitTransaction commits a transaction
func (wm *WALManager) CommitTransaction(txID string) error {
	tx, exists := wm.activeTxs[txID]
	if !exists {
		return fmt.Errorf("transaction %s not found", txID)
	}

	// Get commit sequence for this transaction
	commitSequence := wm.snapshotManager.GetNextCommitSequence()

	// Log the transaction commit
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

	// Remove from active transactions
	delete(wm.activeTxs, txID)

	duration := time.Since(tx.StartTime)
	wm.logger.Debugf("Committed transaction: %s (duration: %v, operations: %d, commitSeq: %d)",
		txID, duration, len(tx.Operations), commitSequence)
	return nil
}

// RollbackTransaction rolls back a transaction
func (wm *WALManager) RollbackTransaction(txID string) error {
	tx, exists := wm.activeTxs[txID]
	if !exists {
		return fmt.Errorf("transaction %s not found", txID)
	}

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

	// Remove from active transactions
	delete(wm.activeTxs, txID)

	duration := time.Since(tx.StartTime)
	wm.logger.Debugf("Rolled back transaction: %s (duration: %v, operations: %d)",
		txID, duration, len(tx.Operations))
	return nil
}

// LogDocumentInsert logs a document insertion operation
func (wm *WALManager) LogDocumentInsert(txID, bundleName, documentID string, documentData interface{}) error {
	afterData, err := json.Marshal(documentData)
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

// LogDocumentUpdate logs a document update operation
func (wm *WALManager) LogDocumentUpdate(txID, bundleName, documentID string, beforeData, afterData interface{}) error {
	beforeJSON, err := json.Marshal(beforeData)
	if err != nil {
		return fmt.Errorf("failed to marshal before data: %w", err)
	}

	afterJSON, err := json.Marshal(afterData)
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
	beforeData, err := json.Marshal(documentData)
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
	afterData, err := json.Marshal(bundleData)
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
	beforeData, err := json.Marshal(bundleData)
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
	afterData, err := json.Marshal(indexData)
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
	beforeData, err := json.Marshal(indexData)
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
	afterData, err := json.Marshal(databaseData)
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
func (wm *WALManager) ExecuteWithLogging(operation func(txID string) error) error {
	txID, err := wm.BeginTransaction()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	// Execute the operation
	if err := operation(txID); err != nil {
		// Rollback on error
		if rollbackErr := wm.RollbackTransaction(txID); rollbackErr != nil {
			wm.logger.Errorf("Failed to rollback transaction %s: %v", txID, rollbackErr)
		}
		return err
	}

	// Commit on success
	if err := wm.CommitTransaction(txID); err != nil {
		// Try to rollback if commit fails
		if rollbackErr := wm.RollbackTransaction(txID); rollbackErr != nil {
			wm.logger.Errorf("Failed to rollback transaction %s after commit failure: %v", txID, rollbackErr)
		}
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// Flush forces all pending WAL entries to be written to disk
func (wm *WALManager) Flush() error {
	return wm.wal.Flush()
}

// Close gracefully shuts down the WAL manager
func (wm *WALManager) Close() error {
	// Rollback any active transactions
	for txID := range wm.activeTxs {
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

	wm.logger.Infof("Starting undo from LSN %d to LSN %d for txID=%s", currentLSN, targetLSN, txID)

	// Collect entries to undo (we need to read forward then apply backward)
	entriesToUndo := make([]WALEntry, 0)

	// Debug: track all entries seen during replay
	totalEntriesSeen := 0
	matchingTxID := 0
	matchingLSN := 0

	// Read entries from target LSN to current LSN
	err := wm.wal.ReplayOperations(targetLSN, func(entry WALEntry) error {
		totalEntriesSeen++
		wm.logger.Infof("REPLAY: LSN=%d, TxID=%s, Op=%d, targetLSN=%d, checkTxID=%s",
			entry.LSN, entry.TxID, entry.Operation, targetLSN, txID)

		// Check if txID matches
		if entry.TxID == txID {
			matchingTxID++
			wm.logger.Infof("REPLAY: TxID matches!")

			// Check if LSN is after target
			if entry.LSN > targetLSN {
				matchingLSN++
				entriesToUndo = append(entriesToUndo, entry)
				wm.logger.Infof("REPLAY: Entry added to undo list! Total: %d", len(entriesToUndo))
			} else {
				wm.logger.Infof("REPLAY: LSN %d <= targetLSN %d, skipping", entry.LSN, targetLSN)
			}
		} else {
			wm.logger.Infof("REPLAY: TxID mismatch (entry=%s, target=%s)", entry.TxID, txID)
		}

		return nil
	})

	wm.logger.Infof("REPLAY SUMMARY: totalSeen=%d, matchingTxID=%d, matchingLSN=%d, toUndo=%d",
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

	wm.logger.Infof("Undo complete: %d operations reversed for txID=%s", undoCount, txID)
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
	wm.logger.Infof("Initialized transaction counter to %d", value)
}

// BeginTransactionUint64 starts a new transaction and returns the transaction ID as uint64
// This is the MVCC-compatible version that returns numeric IDs
func (wm *WALManager) BeginTransactionUint64() (uint64, error) {
	txID := wm.generateTxIDUint64()
	txIDStr := fmt.Sprintf("%016x", txID)

	tx := &Transaction{
		ID:         txIDStr,
		StartTime:  time.Now(),
		Operations: make([]WALEntry, 0),
	}

	wm.activeTxs[txIDStr] = tx

	// Register transaction as active in snapshot manager
	wm.snapshotManager.RegisterActiveTransaction(txID)

	// Create snapshot for this transaction
	snapshot, err := wm.snapshotManager.CreateSnapshot(txID)
	if err != nil {
		delete(wm.activeTxs, txIDStr)
		wm.snapshotManager.UnregisterActiveTransaction(txID)
		return 0, fmt.Errorf("failed to create snapshot: %w", err)
	}

	// Log the transaction begin
	err = wm.wal.LogOperation(txIDStr, OpBeginTx, "", "", "", "", "")
	if err != nil {
		delete(wm.activeTxs, txIDStr)
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
