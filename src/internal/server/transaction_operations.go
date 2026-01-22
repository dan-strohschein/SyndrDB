package server

/*
transaction_operations.go

This file implements transaction command handlers for SyndrDB's multi-statement transaction support.
It provides handlers for BEGIN TRANSACTION, COMMIT, ROLLBACK, SAVEPOINT, and ROLLBACK TO SAVEPOINT
commands, integrating with the WAL manager for durability and the lock manager for isolation.

Key Features:
- BEGIN TRANSACTION: Initiates a new transaction for the session
- COMMIT: Commits active transaction and releases all locks
- ROLLBACK: Aborts transaction, undoes changes via WAL, releases locks
- SAVEPOINT: Creates a named savepoint at current LSN (single-level only)
- ROLLBACK TO SAVEPOINT: Rolls back to savepoint, undoes changes since savepoint
- Auto-rollback on errors: Automatically aborts transaction on any command error
- Idle timeout detection: Aborts transactions that exceed configured idle timeout
- Debug-aware logging: Detailed logs in debug mode, rollback-only logs in production

Architecture:
- Uses new SyndrQL tokenizer and ParseTransactionCommand parser
- Integrates with session transaction state (TransactionStatus, Savepoint)
- Coordinates WAL manager for transaction lifecycle (BeginTransaction, CommitTransaction, UndoToLSN)
- Coordinates lock manager for document-level locking via WAL undo
- Enforces DML-only operations within transactions (checked in CommandDirector)
- Single-level savepoints (nested savepoints return error)

Transaction Isolation:
- Lock-based isolation (not MVCC for community edition)
- Document-level READ and WRITE locks
- Auto-upgrade from READ to WRITE for same transaction
- 30-second hardcoded deadlock timeout
- First-wins conflict resolution

Error Handling:
- Returns descriptive errors for invalid transaction states
- Returns error for DDL operations in transactions
- Returns error for nested savepoints
- Auto-rollback on idle timeout
- Auto-rollback on any operation error (handled in CommandDirector)

Design Principles:
- DRY: Common error patterns use helper functions
- Single Responsibility: Each function handles one transaction command type
- Open/Close: Extensible for future transaction features (e.g., isolation levels)
*/

import (
	encodingjson "encoding/json"
	"fmt"
	"strings"
	"sync"
	"syndrdb/src/internal/domain/models"
	"syndrdb/src/internal/journal"
	"syndrdb/src/internal/syndrQL"
	"syndrdb/src/pkg/errors"
	"syndrdb/src/pkg/settings"
	"time"

	"go.uber.org/zap"
)

// PHASE 2: MVCC - TransactionBuffer tracks document locations for commit sequence assignment
// DocumentLocation stores where a document was written so we can update its CommitSequence on commit
type DocumentLocation struct {
	BundleName string
	PageID     uint32
	FileID     uint32
	Offset     int64 // Optional: for precise updates (future optimization)
}

// TransactionBuffer tracks all documents written in a transaction
// Used to update CommitSequence on commit
type TransactionBuffer struct {
	DocumentLocations map[string]DocumentLocation // docID -> location
	mu                sync.RWMutex
}

// NewTransactionBuffer creates a new transaction buffer
func NewTransactionBuffer() *TransactionBuffer {
	return &TransactionBuffer{
		DocumentLocations: make(map[string]DocumentLocation),
	}
}

// AddDocumentLocation adds a document location to the buffer
func (tb *TransactionBuffer) AddDocumentLocation(documentID string, location DocumentLocation) {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	tb.DocumentLocations[documentID] = location
}

// GetDocumentLocations returns all document locations in the buffer
func (tb *TransactionBuffer) GetDocumentLocations() map[string]DocumentLocation {
	tb.mu.RLock()
	defer tb.mu.RUnlock()
	// Return a copy to prevent external modification
	result := make(map[string]DocumentLocation, len(tb.DocumentLocations))
	for k, v := range tb.DocumentLocations {
		result[k] = v
	}
	return result
}

// Clear clears all document locations from the buffer
func (tb *TransactionBuffer) Clear() {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	tb.DocumentLocations = make(map[string]DocumentLocation)
}

// HandleBeginTransaction handles BEGIN TRANSACTION command
func HandleBeginTransaction(session *Session, serviceManager ServiceManager, logger *zap.SugaredLogger) (*CommandResponse, error) {
	// Check if already in a transaction
	if session.IsInTransaction() {
		return nil, errors.New(errors.ERR_TRANSACTION_CONFLICT,
			fmt.Sprintf("transaction already active (transaction ID: %s)", session.ActiveTransactionID),
			errors.LayerTransaction).WithContext("tx_id", session.ActiveTransactionID)
	}

	// Check WAL availability
	if serviceManager.WALManager == nil {
		return nil, errors.New(errors.ERR_SYSTEM_CONFIG,
			"transactions not available: WAL manager not initialized",
			errors.LayerAPI)
	}

	// Begin transaction in WAL
	txID, err := serviceManager.WALManager.BeginTransaction()
	if err != nil {
		return nil, errors.WrapWithMessage(err, errors.ERR_INTERNAL_TRANSACTION,
			"failed to begin transaction", errors.LayerTransaction)
	}

	// Get current LSN for transaction start
	startLSN := serviceManager.WALManager.GetCurrentLSN()

	// Initialize transaction state in session
	session.BeginTransaction(txID, startLSN)

	// Debug-aware logging
	debugMode := settings.GetSettings().Debug
	if debugMode {
		logger.Infof("BEGIN TRANSACTION: txID=%s, startLSN=%d, session=%s", txID, startLSN, session.SessionID)
	}

	return &CommandResponse{
		Result:      fmt.Sprintf("Transaction started: %s", txID),
		ResultCount: 0,
	}, nil
}

// HandleCommit handles COMMIT command
// PHASE 2: MVCC - Implements async batch commit sequence assignment
func HandleCommit(session *Session, serviceManager ServiceManager, logger *zap.SugaredLogger) (*CommandResponse, error) {
	// Check if in a transaction
	if !session.IsInTransaction() {
		return nil, errors.New(errors.ERR_TRANSACTION_NOT_STARTED,
			"no active transaction to commit",
			errors.LayerTransaction)
	}

	txID := session.ActiveTransactionID

	// PHASE 2: MVCC - Get commit sequence from snapshot manager (via WALManager)
	var commitSequence uint64
	if serviceManager.WALManager != nil {
		// WALManager.CommitTransaction already gets commit sequence internally
		// We need to get it before calling CommitTransaction to use it for document updates
		// Access snapshot manager through WALManager
		snapshotMgr := serviceManager.WALManager.GetSnapshotManager()
		if snapshotMgr != nil {
			commitSequence = snapshotMgr.GetNextCommitSequence()
		}
	}

	// PHASE 2: MVCC - Get document locations from transaction buffer
	var documentLocations map[string]DocumentLocation
	if session.TransactionBuffer != nil {
		documentLocations = session.TransactionBuffer.GetDocumentLocations()
	}

	// PHASE 2: MVCC - Write WAL entry for commit sequence assignment (batch)
	// This marks the transaction as committing immediately
	if len(documentLocations) > 0 && serviceManager.WALManager != nil {
		// Serialize document locations for WAL entry metadata
		// Format: JSON array of {documentID, bundleName, pageID, fileID}
		locationsJSON, err := encodingjson.Marshal(documentLocations)
		if err != nil {
			logger.Warnf("Failed to serialize document locations for WAL: %v", err)
		} else {
			// Log COMMIT_SEQUENCE_ASSIGN operation
			err = serviceManager.WALManager.LogCommitSequenceAssign(txID, commitSequence, string(locationsJSON))
			if err != nil {
				logger.Warnf("Failed to log commit sequence assignment: %v", err)
				// Don't fail commit if WAL logging fails - transaction is still committed
			}
		}
	}

	// Commit transaction in WAL (existing logic)
	err := serviceManager.WALManager.CommitTransaction(txID)
	if err != nil {
		// Abort transaction on commit failure
		session.AbortTransaction()
		if serviceManager.LockManager != nil {
			serviceManager.LockManager.ReleaseLocks(txID)
		}
		return nil, errors.WrapWithMessage(err, errors.ERR_INTERNAL_TRANSACTION,
			"failed to commit transaction", errors.LayerTransaction).WithContext("tx_id", txID)
	}

	// PHASE 2: MVCC - Start async background update of documents with commit sequence
	// This doesn't block the commit response
	if len(documentLocations) > 0 && commitSequence > 0 {
		go updateDocumentsCommitSequenceAsync(
			serviceManager,
			txID,
			commitSequence,
			documentLocations,
			logger,
		)
	}

	// Release all locks for this transaction
	if serviceManager.LockManager != nil {
		serviceManager.LockManager.ReleaseLocks(txID)
	}

	// Mark transaction as committed in session
	session.CommitTransaction()

	// Debug-aware logging
	debugMode := settings.GetSettings().Debug
	if debugMode {
		logger.Infof("COMMIT: txID=%s, operations=%d, commitSeq=%d, documents=%d, session=%s",
			txID, len(session.PendingOperations), commitSequence, len(documentLocations), session.SessionID)
	}

	return &CommandResponse{
		Result:      fmt.Sprintf("Transaction committed: %s (commitSeq: %d)", txID, commitSequence),
		ResultCount: 0,
	}, nil
}

// updateDocumentsCommitSequenceAsync updates documents with commit sequence in background
// PHASE 2: MVCC - Async batch commit sequence assignment
func updateDocumentsCommitSequenceAsync(
	serviceManager ServiceManager,
	txID string,
	commitSequence uint64,
	documentLocations map[string]DocumentLocation,
	logger *zap.SugaredLogger,
) {
	// TODO: Implement document update logic
	// For each document location:
	// 1. Load the document from bundle file
	// 2. Update CommitSequence field
	// 3. Write back to bundle file (or update in-place if possible)
	// This is a placeholder - full implementation requires document update infrastructure
	logger.Debugf("PHASE 2: Async commit sequence update started: txID=%s, commitSeq=%d, documents=%d",
		txID, commitSequence, len(documentLocations))
}

// createUndoFunction creates a function that can undo WAL operations
// This function applies the reverse of each operation using the before-image data
func createUndoFunction(serviceManager ServiceManager, database *models.Database, logger *zap.SugaredLogger) func(journal.WALEntry) error {
	return func(entry journal.WALEntry) error {
		switch entry.Operation {
		case journal.OpInsert:
			// For INSERT, undo means DELETE the document
			// First check if document is still in buffer (not yet flushed)
			isBuffered := serviceManager.BundleService.IsDocumentBuffered(entry.BundleName, entry.DocumentID)

			if isBuffered {
				// Document never made it to disk - just discard from buffer
				logger.Infof("UNDO INSERT: Discarding buffered document %s from bundle %s", entry.DocumentID, entry.BundleName)
				err := serviceManager.BundleService.MarkDocumentDiscarded(entry.BundleName, entry.DocumentID)
				if err != nil {
					logger.Errorf("UNDO INSERT: Failed to discard buffered document: %v", err)
					return errors.WrapWithMessage(err, errors.ERR_INTERNAL_TRANSACTION,
						fmt.Sprintf("failed to discard buffered document %s", entry.DocumentID),
						errors.LayerTransaction).WithContext("document_id", entry.DocumentID).WithContext("bundle", entry.BundleName)
				}
				logger.Infof("UNDO INSERT SUCCESS: Discarded buffered document %s from bundle %s", entry.DocumentID, entry.BundleName)
				return nil
			}

			// Document was flushed to disk - need physical delete
			logger.Infof("UNDO INSERT: Document %s was flushed, performing physical delete from bundle %s", entry.DocumentID, entry.BundleName)

			// Execute the delete within an auto-commit transaction to ensure it's durable
			err := serviceManager.WALManager.ExecuteWithLogging(func(txID string) error {
				// Get the bundle
				bundle, err := serviceManager.BundleService.GetBundleByName(database, entry.BundleName)
				if err != nil {
					return errors.WrapWithMessage(err, errors.ERR_INTERNAL_STORAGE,
						fmt.Sprintf("failed to get bundle %s", entry.BundleName),
						errors.LayerStorage).WithContext("bundle", entry.BundleName)
				}

				// Create delete command with specific document ID (no WHERE clause needed)
				deleteCmd := &models.DocumentDeleteCommand{
					BundleName: entry.BundleName,
				}

				// Call DeleteDocumentFromBundle with explicit docID
				return serviceManager.BundleService.DeleteDocumentFromBundle(bundle, deleteCmd, []string{entry.DocumentID})
			})

			if err != nil {
				logger.Errorf("UNDO INSERT FAILED: %v", err)
				return errors.WrapWithMessage(err, errors.ERR_INTERNAL_TRANSACTION,
					fmt.Sprintf("failed to undo insert of document %s", entry.DocumentID),
					errors.LayerTransaction).WithContext("document_id", entry.DocumentID).WithContext("bundle", entry.BundleName)
			}
			logger.Infof("UNDO INSERT SUCCESS: Physically deleted flushed document %s from bundle %s", entry.DocumentID, entry.BundleName)

		case journal.OpUpdate:
			// For UPDATE, restore the before-image
			if entry.BeforeData == "" {
				return errors.New(errors.ERR_INTERNAL_WAL,
					fmt.Sprintf("no before-data available for update undo: doc=%s", entry.DocumentID),
					errors.LayerWAL).WithContext("document_id", entry.DocumentID).WithContext("operation", "update")
			}

			var beforeDoc map[string]interface{}
			if err := encodingjson.Unmarshal([]byte(entry.BeforeData), &beforeDoc); err != nil {
				return errors.WrapWithMessage(err, errors.ERR_INTERNAL,
					"failed to unmarshal before-data for undo", errors.LayerTransaction).WithContext("document_id", entry.DocumentID)
			}

			bundle, err := serviceManager.BundleService.GetBundleByName(database, entry.BundleName)
			if err != nil {
				return errors.WrapWithMessage(err, errors.ERR_INTERNAL_STORAGE,
					fmt.Sprintf("failed to get bundle %s for undo", entry.BundleName),
					errors.LayerStorage).WithContext("bundle", entry.BundleName)
			}

			// Convert the map to KeyValue pairs for update command
			fields := make([]models.KeyValue, 0, len(beforeDoc))
			for key, value := range beforeDoc {
				// Skip internal fields like DocumentID, CreatedAt, UpdatedAt
				if key != "DocumentID" && key != "CreatedAt" && key != "UpdatedAt" {
					fields = append(fields, models.KeyValue{Key: key, Value: value})
				}
			}

			// Create update command with WHERE clause to target specific document
			updateCmd := &models.DocumentUpdateCommand{
				BundleName:  entry.BundleName,
				Fields:      fields,
				WhereClause: fmt.Sprintf("DocumentID = '%s'", entry.DocumentID),
			}

			err = serviceManager.BundleService.UpdateDocumentInBundle(database, bundle, updateCmd)
			if err != nil {
				return errors.WrapWithMessage(err, errors.ERR_INTERNAL_TRANSACTION,
					fmt.Sprintf("failed to undo update of document %s", entry.DocumentID),
					errors.LayerTransaction).WithContext("document_id", entry.DocumentID).WithContext("bundle", entry.BundleName)
			}
			logger.Debugf("Undid UPDATE: restored document %s in bundle %s", entry.DocumentID, entry.BundleName)

		case journal.OpDelete:
			// For DELETE, restore the document from before-image
			if entry.BeforeData == "" {
				return errors.New(errors.ERR_INTERNAL_WAL,
					fmt.Sprintf("no before-data available for delete undo: doc=%s", entry.DocumentID),
					errors.LayerWAL).WithContext("document_id", entry.DocumentID).WithContext("operation", "delete")
			}

			var beforeDoc map[string]interface{}
			if err := encodingjson.Unmarshal([]byte(entry.BeforeData), &beforeDoc); err != nil {
				return errors.WrapWithMessage(err, errors.ERR_INTERNAL,
					"failed to unmarshal before-data for undo", errors.LayerTransaction).WithContext("document_id", entry.DocumentID)
			}

			bundle, err := serviceManager.BundleService.GetBundleByName(database, entry.BundleName)
			if err != nil {
				return errors.WrapWithMessage(err, errors.ERR_INTERNAL_STORAGE,
					fmt.Sprintf("failed to get bundle %s for undo", entry.BundleName),
					errors.LayerStorage).WithContext("bundle", entry.BundleName)
			}

			// Convert the map to KeyValue pairs for document command
			fields := make([]models.KeyValue, 0, len(beforeDoc))
			for key, value := range beforeDoc {
				// Skip internal fields like DocumentID, CreatedAt, UpdatedAt
				if key != "DocumentID" && key != "CreatedAt" && key != "UpdatedAt" {
					fields = append(fields, models.KeyValue{Key: key, Value: value})
				}
			}

			// Create a DocumentCommand from the before-image
			docCommand := &models.DocumentCommand{
				BundleName: entry.BundleName,
				Fields:     fields,
			}

			// Re-insert the document (DocumentID will be regenerated, but that's a known limitation)
			// TODO: Implement RestoreDocumentWithID method to preserve original DocumentID
			_, err = serviceManager.BundleService.AddDocumentToBundle(database, bundle, docCommand)
			if err != nil {
				return errors.WrapWithMessage(err, errors.ERR_INTERNAL_TRANSACTION,
					fmt.Sprintf("failed to undo delete of document %s", entry.DocumentID),
					errors.LayerTransaction).WithContext("document_id", entry.DocumentID).WithContext("bundle", entry.BundleName)
			}
			logger.Debugf("Undid DELETE: restored document to bundle %s (new DocumentID generated)", entry.BundleName)

		default:
			// Skip transaction control operations
			logger.Debugf("Skipping undo for operation type %d", entry.Operation)
		}

		return nil
	}
}

// HandleRollback handles ROLLBACK command (full rollback, not to savepoint)
func HandleRollback(session *Session, serviceManager ServiceManager, database *models.Database, logger *zap.SugaredLogger) (*CommandResponse, error) {
	// Check if in a transaction
	if !session.IsInTransaction() {
		return nil, errors.New(errors.ERR_TRANSACTION_NOT_STARTED,
			"no active transaction to rollback",
			errors.LayerTransaction)
	}

	txID := session.ActiveTransactionID
	startLSN := session.TransactionStartLSN
	currentLSN := serviceManager.WALManager.GetCurrentLSN()

	// Undo all changes since transaction start
	if currentLSN > startLSN {
		// CRITICAL: Flush WAL to ensure all buffered entries are written before replay
		// The WAL uses batched writes for performance, so we must flush before reading
		if err := serviceManager.WALManager.Flush(); err != nil {
			logger.Errorf("Failed to flush WAL before undo: %v", err)
		}

		undoFunc := createUndoFunction(serviceManager, database, logger)
		err := serviceManager.WALManager.UndoToLSN(startLSN, txID, undoFunc, serviceManager.LockManager)
		if err != nil {
			logger.Errorf("Failed to undo transaction changes: %v", err)
			// Continue with rollback even if undo fails
		}
	}

	// Rollback transaction in WAL
	err := serviceManager.WALManager.RollbackTransaction(txID)
	if err != nil {
		logger.Errorf("Failed to log rollback for transaction %s: %v", txID, err)
		// Continue with session cleanup
	}

	// Release all locks for this transaction
	if serviceManager.LockManager != nil {
		serviceManager.LockManager.ReleaseLocks(txID)
	}

	// Mark transaction as aborted in session
	session.AbortTransaction()

	// Flush buffered writes to disk after rollback, then physically delete discarded documents
	// Process: 1) Collect discarded docIDs, 2) Flush buffer, 3) Delete flushed discarded docs
	if serviceManager.BundleService != nil {
		// Step 1: Collect all discarded document IDs from all bundles BEFORE flushing
		discardedByBundle := make(map[string][]string)
		for bundleName := range database.Bundles {
			discarded := serviceManager.BundleService.GetDiscardedDocuments(bundleName)
			if len(discarded) > 0 {
				discardedByBundle[bundleName] = discarded
				logger.Debugf("Found %d discarded documents in bundle %s", len(discarded), bundleName)
			}
		}

		// Step 2: Flush all buffers (writes both discarded and non-discarded documents to disk)
		if err := serviceManager.BundleService.FlushAllBuffers(); err != nil {
			logger.Warnf("Failed to flush buffers after rollback (txID=%s): %v", txID, err)
		} else {
			logger.Debugf("Flushed buffers after rollback (txID=%s)", txID)
		}

		// Step 3: Physically delete the discarded documents that were just flushed
		for bundleName, docIDs := range discardedByBundle {
			err := serviceManager.BundleService.DeleteDiscardedDocuments(database, bundleName, docIDs)
			if err != nil {
				logger.Warnf("Failed to delete discarded documents from bundle %s: %v", bundleName, err)
			} else {
				logger.Infof("Physically deleted %d flushed-then-discarded documents from bundle %s", len(docIDs), bundleName)
				// Clear the discarded flags now that documents are deleted
				serviceManager.BundleService.ClearDiscardedDocuments(bundleName, docIDs)
			}
		}
	}

	// Always log rollbacks (both debug and production)
	logger.Infof("ROLLBACK: txID=%s, operations=%d, session=%s",
		txID, len(session.PendingOperations), session.SessionID)

	return &CommandResponse{
		Result:      fmt.Sprintf("Transaction rolled back: %s", txID),
		ResultCount: 0,
	}, nil
}

// HandleSavepoint handles SAVEPOINT "name" command
func HandleSavepoint(savepointName string, session *Session, serviceManager ServiceManager, logger *zap.SugaredLogger) (*CommandResponse, error) {
	// Check if in a transaction
	if !session.IsInTransaction() {
		return nil, errors.New(errors.ERR_TRANSACTION_NOT_STARTED,
			"no active transaction: savepoints can only be created within a transaction",
			errors.LayerTransaction)
	}

	// Check if savepoint already exists (single-level savepoints only)
	if session.CurrentSavepoint != nil {
		return nil, errors.New(errors.ERR_VALIDATION_CONSTRAINT,
			fmt.Sprintf("savepoint already exists: '%s' (nested savepoints not supported)", session.CurrentSavepoint.Name),
			errors.LayerTransaction).WithContext("savepoint", session.CurrentSavepoint.Name)
	}

	// Get current LSN for savepoint
	currentLSN := serviceManager.WALManager.GetCurrentLSN()

	// Create savepoint in session
	session.SetSavepoint(savepointName, currentLSN)

	// Debug-aware logging
	debugMode := settings.GetSettings().Debug
	if debugMode {
		logger.Infof("SAVEPOINT: name=%s, lsn=%d, txID=%s, session=%s",
			savepointName, currentLSN, session.ActiveTransactionID, session.SessionID)
	}

	return &CommandResponse{
		Result:      fmt.Sprintf("Savepoint created: %s", savepointName),
		ResultCount: 0,
	}, nil
}

// HandleRollbackToSavepoint handles ROLLBACK TO SAVEPOINT "name" command
func HandleRollbackToSavepoint(savepointName string, session *Session, serviceManager ServiceManager, database *models.Database, logger *zap.SugaredLogger) (*CommandResponse, error) {
	// Check if in a transaction
	if !session.IsInTransaction() {
		return nil, errors.New(errors.ERR_TRANSACTION_NOT_STARTED,
			"no active transaction: cannot rollback to savepoint outside transaction",
			errors.LayerTransaction)
	}

	// Check if savepoint exists
	if session.CurrentSavepoint == nil {
		return nil, errors.New(errors.ERR_NOT_FOUND_INDEX,
			fmt.Sprintf("savepoint not found: '%s'", savepointName),
			errors.LayerTransaction).WithContext("savepoint", savepointName)
	}

	// Verify savepoint name matches (single-level savepoints only)
	if session.CurrentSavepoint.Name != savepointName {
		return nil, errors.New(errors.ERR_NOT_FOUND_INDEX,
			fmt.Sprintf("savepoint not found: '%s' (active savepoint: '%s')",
				savepointName, session.CurrentSavepoint.Name),
			errors.LayerTransaction).WithContext("savepoint", savepointName).WithContext("active_savepoint", session.CurrentSavepoint.Name)
	}

	txID := session.ActiveTransactionID
	savepointLSN := session.CurrentSavepoint.LSN
	currentLSN := serviceManager.WALManager.GetCurrentLSN()

	// Undo changes since savepoint
	if currentLSN > savepointLSN {
		// CRITICAL: Flush WAL to ensure all buffered entries are written before replay
		if err := serviceManager.WALManager.Flush(); err != nil {
			logger.Errorf("Failed to flush WAL before undo: %v", err)
		}

		undoFunc := createUndoFunction(serviceManager, database, logger)
		err := serviceManager.WALManager.UndoToLSN(savepointLSN, txID, undoFunc, serviceManager.LockManager)
		if err != nil {
			return nil, errors.WrapWithMessage(err, errors.ERR_INTERNAL_TRANSACTION,
				fmt.Sprintf("failed to rollback to savepoint '%s'", savepointName),
				errors.LayerTransaction).WithContext("savepoint", savepointName).WithContext("tx_id", txID)
		}
	}

	// Clear the savepoint (transaction continues)
	session.ClearSavepoint()

	// Always log savepoint rollbacks (both debug and production)
	logger.Infof("ROLLBACK TO SAVEPOINT: name=%s, lsn=%d, txID=%s, session=%s",
		savepointName, savepointLSN, txID, session.SessionID)

	return &CommandResponse{
		Result:      fmt.Sprintf("Rolled back to savepoint: %s", savepointName),
		ResultCount: 0,
	}, nil
}

// ParseAndExecuteTransactionCommand tokenizes, parses, and executes a transaction command
func ParseAndExecuteTransactionCommand(command string, session *Session, serviceManager ServiceManager, database *models.Database, logger *zap.SugaredLogger) (*CommandResponse, error) {
	// Tokenize the command
	tokenizer := syndrQL.NewTokenizer(command)
	tokens, err := tokenizer.Tokenize()
	if err != nil {
		return nil, errors.WrapWithMessage(err, errors.ERR_VALIDATION_SYNTAX,
			"failed to tokenize transaction command", errors.LayerParser).WithContext("command", command)
	}

	// Remove EOF token if present
	if len(tokens) > 0 && tokens[len(tokens)-1].Type == syndrQL.TOKEN_EOF {
		tokens = tokens[:len(tokens)-1]
	}

	// Parse transaction command
	txNode, err := syndrQL.ParseTransactionCommand(tokens)
	if err != nil {
		return nil, errors.WrapWithMessage(err, errors.ERR_VALIDATION_SYNTAX,
			"failed to parse transaction command", errors.LayerParser).WithContext("command", command)
	}

	// Execute based on transaction type
	switch txNode.Type {
	case syndrQL.TransactionBegin:
		return HandleBeginTransaction(session, serviceManager, logger)

	case syndrQL.TransactionCommit:
		return HandleCommit(session, serviceManager, logger)

	case syndrQL.TransactionRollback:
		return HandleRollback(session, serviceManager, database, logger)

	case syndrQL.TransactionSavepoint:
		return HandleSavepoint(txNode.SavepointName, session, serviceManager, logger)

	case syndrQL.TransactionRollbackToSavepoint:
		return HandleRollbackToSavepoint(txNode.SavepointName, session, serviceManager, database, logger)

	default:
		return nil, errors.New(errors.ERR_VALIDATION_FIELD,
			fmt.Sprintf("unknown transaction type: %s", txNode.Type),
			errors.LayerParser).WithContext("type", fmt.Sprintf("%s", txNode.Type))
	}
}

// CheckTransactionIdleTimeout checks if the active transaction has exceeded idle timeout
// Returns error if timeout exceeded, nil otherwise
func CheckTransactionIdleTimeout(session *Session, logger *zap.SugaredLogger) error {
	if !session.IsInTransaction() {
		return nil
	}

	// Get timeout from settings
	timeout := settings.GetSettings().GetTransactionIdleTimeout()

	if session.IsIdleExpired(timeout) {
		// Get transaction details for logging
		txInfo := session.GetTransactionInfo()
		txID := txInfo["transaction_id"].(string)
		startTime := txInfo["start_time"].(time.Time)
		idleDuration := time.Since(startTime)

		logger.Warnf("Transaction idle timeout exceeded: txID=%s, idle=%v, session=%s",
			txID, idleDuration, session.SessionID)

		return errors.New(errors.ERR_RESOURCE_TIMEOUT,
			fmt.Sprintf("transaction idle timeout exceeded (txID: %s, idle: %v)", txID, idleDuration),
			errors.LayerTransaction).WithContext("tx_id", txID).WithContext("idle_duration", idleDuration.String())
	}

	return nil
}

// AutoRollbackOnError performs automatic rollback when an error occurs during a transaction
func AutoRollbackOnError(session *Session, serviceManager ServiceManager, database *models.Database, commandError error, logger *zap.SugaredLogger) {
	if !session.IsInTransaction() {
		return
	}

	txID := session.ActiveTransactionID
	startLSN := session.TransactionStartLSN
	currentLSN := serviceManager.WALManager.GetCurrentLSN()

	// Log the auto-rollback (always logged, both debug and production)
	logger.Warnf("Auto-rollback triggered: txID=%s, error=%v, session=%s",
		txID, commandError, session.SessionID)

	// Undo all changes since transaction start
	if currentLSN > startLSN {
		// CRITICAL: Flush WAL to ensure all buffered entries are written before replay
		if err := serviceManager.WALManager.Flush(); err != nil {
			logger.Errorf("Failed to flush WAL before undo: %v", err)
		}

		undoFunc := createUndoFunction(serviceManager, database, logger)
		err := serviceManager.WALManager.UndoToLSN(startLSN, txID, undoFunc, serviceManager.LockManager)
		if err != nil {
			logger.Errorf("Failed to undo transaction changes during auto-rollback: %v", err)
		}
	}

	// Rollback transaction in WAL
	err := serviceManager.WALManager.RollbackTransaction(txID)
	if err != nil {
		logger.Errorf("Failed to log rollback during auto-rollback: %v", err)
	}

	// Release all locks
	if serviceManager.LockManager != nil {
		serviceManager.LockManager.ReleaseLocks(txID)
	}

	// Mark transaction as aborted
	session.AbortTransaction()
}

// IsDMLCommand checks if a command is a DML command (allowed in transactions)
// DML commands: SELECT, INSERT/ADD, UPDATE, DELETE
func IsDMLCommand(command string) bool {
	commandLower := strings.ToLower(strings.TrimSpace(command))
	return strings.HasPrefix(commandLower, "select") ||
		strings.HasPrefix(commandLower, "insert") ||
		strings.HasPrefix(commandLower, "add") ||
		strings.HasPrefix(commandLower, "update") ||
		strings.HasPrefix(commandLower, "delete")
}

// IsDDLCommand checks if a command is a DDL command (not allowed in transactions)
// DDL commands: CREATE, DROP, ALTER, TRUNCATE, RENAME
func IsDDLCommand(command string) bool {
	commandLower := strings.ToLower(strings.TrimSpace(command))
	return strings.HasPrefix(commandLower, "create") ||
		strings.HasPrefix(commandLower, "drop") ||
		strings.HasPrefix(commandLower, "alter") ||
		strings.HasPrefix(commandLower, "truncate") ||
		strings.HasPrefix(commandLower, "rename")
}

// IsTransactionCommand checks if a command is a transaction control command
func IsTransactionCommand(command string) bool {
	commandLower := strings.ToLower(strings.TrimSpace(command))
	return strings.HasPrefix(commandLower, "begin") ||
		strings.HasPrefix(commandLower, "commit") ||
		strings.HasPrefix(commandLower, "rollback") ||
		strings.HasPrefix(commandLower, "savepoint")
}
