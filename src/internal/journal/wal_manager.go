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
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"syndrdb/src/pkg/settings"
	"time"

	"go.uber.org/zap"
)

// WALManager provides a high-level interface for WAL operations
type WALManager struct {
	wal       *WriteAheadLog
	logger    *zap.SugaredLogger
	activeTxs map[string]*Transaction
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
		LogDir:             settings.LogDir,
		MaxFileSize:        settings.MaxJournalFileSize,
		FlushInterval:      1 * time.Second,
		RetentionDays:      30,
		FsyncOnCommit:      true,
		CompressionEnabled: false,
		EncryptionEnabled:  false,
		AutoFlush:          true,
	}

	// Use default log directory if not set
	if config.LogDir == "" {
		config.LogDir = "./log_files"
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
		wal:       wal,
		logger:    logger,
		activeTxs: make(map[string]*Transaction),
	}

	logger.Info("WAL Manager initialized successfully")
	return manager, nil
}

// generateTxID generates a unique transaction ID
func (wm *WALManager) generateTxID() string {
	bytes := make([]byte, 8)
	rand.Read(bytes)
	return hex.EncodeToString(bytes)
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

	// Log the transaction commit
	err := wm.wal.LogOperation(txID, OpCommitTx, "", "", "", "", "")
	if err != nil {
		return fmt.Errorf("failed to log transaction commit: %w", err)
	}

	// Remove from active transactions
	delete(wm.activeTxs, txID)

	duration := time.Since(tx.StartTime)
	wm.logger.Debugf("Committed transaction: %s (duration: %v, operations: %d)",
		txID, duration, len(tx.Operations))
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
