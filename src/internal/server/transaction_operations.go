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
- MVCC snapshot isolation for reads (no read locks on SELECT)
- Write locks (document-level) for mutations (UPDATE, DELETE)
- Auto-upgrade from READ to WRITE for same transaction (SELECT FOR UPDATE)
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
	"context"
	encodingjson "encoding/json"
	"fmt"
	"strings"
	"sync"
	"syndrdb/src/internal/domain/database"
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
func HandleBeginTransaction(session *Session, serviceManager ServiceManager, logger *zap.SugaredLogger, isolationLevel ...syndrQL.IsolationLevel) (*CommandResponse, error) {
	// Check if already in a transaction
	if session.IsInTransaction() {
		return nil, errors.New(errors.ERR_TRANSACTION_CONFLICT,
			fmt.Sprintf("transaction already active (transaction ID: %s)", session.ActiveTransactionID),
			errors.LayerTransaction).WithContext("tx_id", session.ActiveTransactionID)
	}

	// Determine effective isolation level
	var level syndrQL.IsolationLevel
	if len(isolationLevel) > 0 {
		level = isolationLevel[0]
	}

	// Check WAL availability
	if serviceManager.WALManager == nil {
		return nil, errors.New(errors.ERR_SYSTEM_CONFIG,
			"transactions not available: WAL manager not initialized",
			errors.LayerAPI)
	}

	// PHASE 1: MVCC - Use BeginTransactionUint64 to create snapshot; store in session
	txIDUint64, err := serviceManager.WALManager.BeginTransactionUint64()
	if err != nil {
		return nil, errors.WrapWithMessage(err, errors.ERR_INTERNAL_TRANSACTION,
			"failed to begin transaction", errors.LayerTransaction)
	}
	txIDStr := fmt.Sprintf("%016x", txIDUint64)

	// Get current LSN for transaction start (after begin logged)
	startLSN := serviceManager.WALManager.GetCurrentLSN()

	// Get MVCC snapshot created by BeginTransactionUint64; store in session
	var snapshot *journal.Snapshot
	if sm := serviceManager.WALManager.GetSnapshotManager(); sm != nil {
		if snap, ok := sm.GetSnapshot(txIDUint64); ok && snap != nil {
			snapshot = snap
		}
	}

	// Initialize transaction state in session (including snapshot for read-path visibility)
	if level != syndrQL.IsolationDefault {
		session.BeginTransaction(txIDStr, startLSN, snapshot, level)
	} else {
		session.BeginTransaction(txIDStr, startLSN, snapshot)
	}

	// Debug-aware logging
	debugMode := settings.GetSettings().Debug
	if debugMode {
		snapSeq := uint64(0)
		if snapshot != nil {
			snapSeq = snapshot.SnapshotSequence
		}
		logger.Infof("BEGIN TRANSACTION: txID=%s, startLSN=%d, session=%s, snapshotSeq=%d, isolation=%s",
			txIDStr, startLSN, session.SessionID, snapSeq, session.GetEffectiveIsolationLevel())
	}

	return &CommandResponse{
		Result:      fmt.Sprintf("Transaction started: %s", txIDStr),
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

	// PHASE 3: MVCC - Get snapshot for conflict checking
	// Convert txID string to uint64 for snapshot lookup
	var txIDUint64 uint64
	var snapshotSequence uint64
	if serviceManager.WALManager != nil {
		snapshotMgr := serviceManager.WALManager.GetSnapshotManager()
		if snapshotMgr != nil {
			// Parse txID hex string to uint64
			_, err := fmt.Sscanf(txID, "%016x", &txIDUint64)
			if err == nil {
				// Get snapshot to retrieve snapshot sequence
				if snapshot, exists := snapshotMgr.GetSnapshot(txIDUint64); exists {
					snapshotSequence = snapshot.SnapshotSequence
				}
			}
		}
	}

	// PHASE 2: MVCC - Get document locations from transaction buffer
	var documentLocations map[string]DocumentLocation
	if session.TransactionBuffer != nil {
		documentLocations = session.TransactionBuffer.GetDocumentLocations()
	}

	// PHASE 3: MVCC - Check for write-write conflicts before commit
	// If any document was modified after snapshot, abort transaction
	if serviceManager.ConflictTracker != nil && len(documentLocations) > 0 && snapshotSequence > 0 {
		conflictingDocs := make([]string, 0)
		for docID := range documentLocations {
			if serviceManager.ConflictTracker.CheckConflict(docID, snapshotSequence) {
				conflictingDocs = append(conflictingDocs, docID)
			}
		}

		if len(conflictingDocs) > 0 {
			// Conflict detected - abort transaction
			session.AbortTransaction()
			if serviceManager.LockManager != nil {
				serviceManager.LockManager.ReleaseLocks(txID)
			}
			// Convert slice to comma-separated string for context
			conflictingDocsStr := strings.Join(conflictingDocs, ", ")
			return nil, errors.New(errors.ERR_TRANSACTION_CONFLICT,
				fmt.Sprintf("write conflict detected: documents [%s] were modified after transaction snapshot (snapshotSeq: %d)",
					conflictingDocsStr, snapshotSequence),
				errors.LayerTransaction).WithContext("tx_id", txID).WithContext("conflicting_documents", conflictingDocsStr)
		}
	}

	// SSI: Record rw-antidependencies for all documents written by this SERIALIZABLE transaction,
	// then check for dangerous structure (two consecutive rw-antidependencies).
	if serviceManager.SIReadTracker != nil && session.GetEffectiveIsolationLevel() == syndrQL.IsolationSerializable && txIDUint64 > 0 {
		// Record writes: for each written doc, check if another SERIALIZABLE tx holds a SIREAD lock
		for docID, loc := range documentLocations {
			docKey := loc.BundleName + ":" + docID
			serviceManager.SIReadTracker.RecordWrite(txIDUint64, docKey)
		}
		if ssiErr := serviceManager.SIReadTracker.CheckCommit(txIDUint64); ssiErr != nil {
			// SSI violation detected — abort transaction
			session.AbortTransaction()
			if serviceManager.LockManager != nil {
				serviceManager.LockManager.ReleaseLocks(txID)
			}
			serviceManager.SIReadTracker.TransactionFinished(txIDUint64)
			return nil, errors.New(errors.ERR_TRANSACTION_CONFLICT,
				ssiErr.Error(),
				errors.LayerTransaction).WithContext("tx_id", txID)
		}
	}

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

	var commitSeqArg *uint64
	if commitSequence > 0 {
		commitSeqArg = &commitSequence
	}
	err := serviceManager.WALManager.CommitTransaction(txID, commitSeqArg)
	if err != nil {
		session.AbortTransaction()
		if serviceManager.LockManager != nil {
			serviceManager.LockManager.ReleaseLocks(txID)
		}
		return nil, errors.WrapWithMessage(err, errors.ERR_INTERNAL_TRANSACTION,
			"failed to commit transaction", errors.LayerTransaction).WithContext("tx_id", txID)
	}

	// PHASE 2a: Update ConflictTracker so future conflict checks see this commit
	if serviceManager.ConflictTracker != nil && len(documentLocations) > 0 && commitSequence > 0 {
		docIDs := make([]string, 0, len(documentLocations))
		for id := range documentLocations {
			docIDs = append(docIDs, id)
		}
		serviceManager.ConflictTracker.UpdateCommitSequencesBatch(docIDs, commitSequence)
	}

	// PHASE 2: MVCC - Start async background update of documents with commit sequence
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

	// SSI: Mark transaction as committed in SSI tracker, then schedule cleanup
	if serviceManager.SIReadTracker != nil && session.GetEffectiveIsolationLevel() == syndrQL.IsolationSerializable && txIDUint64 > 0 {
		serviceManager.SIReadTracker.TransactionCommitted(txIDUint64)
		// Deferred cleanup: keep committed state briefly for other transactions' commit checks
		go func(tracker *SIReadTracker, id uint64) {
			tracker.TransactionFinished(id)
		}(serviceManager.SIReadTracker, txIDUint64)
	}

	// Flush ALL index updates after commit including B-tree disk flush (full durability)
	if serviceManager.BundleService != nil {
		serviceManager.BundleService.ForceFlushIndexUpdatesFull()
	}

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

// updateDocumentsCommitSequenceAsync updates documents with commit sequence in background.
// For each document written in the transaction, it stamps CommitSequence on the in-memory
// page cache entry so MVCC visibility rules work without the legacy CommitSequence==0 fallback.
// Failures are logged but not fatal — the WAL has the assignment recorded for crash recovery.
func updateDocumentsCommitSequenceAsync(
	serviceManager ServiceManager,
	txID string,
	commitSequence uint64,
	documentLocations map[string]DocumentLocation,
	logger *zap.SugaredLogger,
) {
	if serviceManager.BundleService == nil {
		logger.Warnf("BundleService is nil, cannot update commit sequences for txID=%s", txID)
		return
	}

	var errCount int
	for docID, loc := range documentLocations {
		err := serviceManager.BundleService.UpdateDocumentCommitSequence(
			loc.BundleName, docID, commitSequence,
		)
		if err != nil {
			errCount++
			logger.Debugf("Failed to update commit sequence for doc %s in bundle %s: %v",
				docID, loc.BundleName, err)
			// Not fatal — WAL has the assignment logged for crash recovery
		}
	}

	logger.Debugf("Async commit sequence update complete: txID=%s, commitSeq=%d, docs=%d, errors=%d",
		txID, commitSequence, len(documentLocations), errCount)
}

// createUndoFunction creates a function that can undo WAL operations.
// This function applies the reverse of each operation using the before-image data.
// When database is nil (e.g. during crash recovery), dbService is used to resolve
// the database dynamically by searching all databases for the target bundle.
func createUndoFunction(serviceManager ServiceManager, db *models.Database, dbService *database.DatabaseService, logger *zap.SugaredLogger) func(journal.WALEntry) error {
	// Cache resolved databases per bundle name (avoids repeated lookups during recovery)
	dbCache := make(map[string]*models.Database)

	resolveDB := func(bundleName string) *models.Database {
		if db != nil {
			return db
		}
		if cached, ok := dbCache[bundleName]; ok {
			return cached
		}
		if dbService != nil {
			for _, candidate := range dbService.ListDatabases() {
				if _, err := serviceManager.BundleService.GetBundleByName(candidate, bundleName); err == nil {
					dbCache[bundleName] = candidate
					return candidate
				}
			}
		}
		logger.Warnf("Could not resolve database for bundle '%s' during undo", bundleName)
		return nil
	}

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

			resolvedDB := resolveDB(entry.BundleName)
			if resolvedDB == nil {
				return errors.New(errors.ERR_INTERNAL_STORAGE,
					fmt.Sprintf("cannot resolve database for bundle '%s' during undo insert", entry.BundleName),
					errors.LayerStorage).WithContext("bundle", entry.BundleName)
			}

			// Execute the delete within an auto-commit transaction to ensure it's durable
			err := serviceManager.WALManager.ExecuteWithLogging(func(txID string) error {
				// Get the bundle
				bundle, err := serviceManager.BundleService.GetBundleByName(resolvedDB, entry.BundleName)
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
				return serviceManager.BundleService.DeleteDocumentFromBundle(bundle, deleteCmd, []string{entry.DocumentID}, nil)
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

			resolvedDB := resolveDB(entry.BundleName)
			if resolvedDB == nil {
				return errors.New(errors.ERR_INTERNAL_STORAGE,
					fmt.Sprintf("cannot resolve database for bundle '%s' during undo update", entry.BundleName),
					errors.LayerStorage).WithContext("bundle", entry.BundleName)
			}

			bundle, err := serviceManager.BundleService.GetBundleByName(resolvedDB, entry.BundleName)
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

			err = serviceManager.BundleService.UpdateDocumentInBundle(context.Background(), resolvedDB, bundle, updateCmd)
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

			resolvedDB := resolveDB(entry.BundleName)
			if resolvedDB == nil {
				return errors.New(errors.ERR_INTERNAL_STORAGE,
					fmt.Sprintf("cannot resolve database for bundle '%s' during undo delete", entry.BundleName),
					errors.LayerStorage).WithContext("bundle", entry.BundleName)
			}

			bundle, err := serviceManager.BundleService.GetBundleByName(resolvedDB, entry.BundleName)
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
			_, err = serviceManager.BundleService.AddDocumentToBundle(resolvedDB, bundle, docCommand)
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

		undoFunc := createUndoFunction(serviceManager, database, nil, logger)
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

	// SSI: Clean up SIREAD locks and conflict state for this transaction
	if serviceManager.SIReadTracker != nil {
		var txIDUint64 uint64
		if _, err := fmt.Sscanf(txID, "%016x", &txIDUint64); err == nil && txIDUint64 > 0 {
			serviceManager.SIReadTracker.TransactionFinished(txIDUint64)
		}
	}

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

		undoFunc := createUndoFunction(serviceManager, database, nil, logger)
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
		return HandleBeginTransaction(session, serviceManager, logger, txNode.IsolationLevel)

	case syndrQL.TransactionCommit:
		return HandleCommit(session, serviceManager, logger)

	case syndrQL.TransactionRollback:
		return HandleRollback(session, serviceManager, database, logger)

	case syndrQL.TransactionSavepoint:
		return HandleSavepoint(txNode.SavepointName, session, serviceManager, logger)

	case syndrQL.TransactionRollbackToSavepoint:
		return HandleRollbackToSavepoint(txNode.SavepointName, session, serviceManager, database, logger)

	case syndrQL.TransactionSetIsolation:
		return HandleSetTransactionIsolation(txNode.IsolationLevel, session, logger)

	case syndrQL.TransactionSetSessionIsolation:
		return HandleSetSessionIsolation(txNode.IsolationLevel, session, logger)

	case syndrQL.TransactionShowIsolation:
		return HandleShowIsolationLevel(session, logger)

	default:
		return nil, errors.New(errors.ERR_VALIDATION_FIELD,
			fmt.Sprintf("unknown transaction type: %s", txNode.Type),
			errors.LayerParser).WithContext("type", fmt.Sprintf("%s", txNode.Type))
	}
}

// HandleSetTransactionIsolation handles SET TRANSACTION ISOLATION LEVEL command.
// Sets a pending isolation level that takes effect on the next BEGIN TRANSACTION.
func HandleSetTransactionIsolation(level syndrQL.IsolationLevel, session *Session, logger *zap.SugaredLogger) (*CommandResponse, error) {
	// Cannot SET TRANSACTION while in a transaction
	if session.IsInTransaction() {
		return nil, errors.New(errors.ERR_TRANSACTION_CONFLICT,
			"SET TRANSACTION ISOLATION LEVEL must be called before BEGIN TRANSACTION",
			errors.LayerTransaction)
	}

	session.SetTransactionIsolation(level)

	effectiveLevel := level
	if effectiveLevel == syndrQL.IsolationReadUncommitted {
		effectiveLevel = syndrQL.IsolationReadCommitted
	}

	logger.Debugf("SET TRANSACTION ISOLATION LEVEL %s (session=%s)", effectiveLevel, session.SessionID)

	return &CommandResponse{
		Result:      fmt.Sprintf("SET TRANSACTION ISOLATION LEVEL %s", effectiveLevel),
		ResultCount: 0,
	}, nil
}

// HandleSetSessionIsolation handles SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL command.
// Sets the session-level default isolation level that persists across transactions.
func HandleSetSessionIsolation(level syndrQL.IsolationLevel, session *Session, logger *zap.SugaredLogger) (*CommandResponse, error) {
	session.SetDefaultIsolationLevel(level)

	effectiveLevel := level
	if effectiveLevel == syndrQL.IsolationReadUncommitted {
		effectiveLevel = syndrQL.IsolationReadCommitted
	}

	logger.Debugf("SET SESSION default isolation level to %s (session=%s)", effectiveLevel, session.SessionID)

	return &CommandResponse{
		Result:      fmt.Sprintf("SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL %s", effectiveLevel),
		ResultCount: 0,
	}, nil
}

// HandleShowIsolationLevel handles SHOW TRANSACTION ISOLATION LEVEL command.
// Returns the effective isolation level for the current session/transaction.
func HandleShowIsolationLevel(session *Session, logger *zap.SugaredLogger) (*CommandResponse, error) {
	level := session.GetEffectiveIsolationLevel()

	return &CommandResponse{
		Result:      level.String(),
		ResultCount: 1,
	}, nil
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

		undoFunc := createUndoFunction(serviceManager, database, nil, logger)
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
// DML commands: SELECT, INSERT/ADD, BULK ADD/INSERT, UPDATE, DELETE
func IsDMLCommand(command string) bool {
	commandLower := strings.ToLower(strings.TrimSpace(command))
	return strings.HasPrefix(commandLower, "select") ||
		strings.HasPrefix(commandLower, "insert") ||
		strings.HasPrefix(commandLower, "add") ||
		strings.HasPrefix(commandLower, "bulk") ||
		strings.HasPrefix(commandLower, "update") ||
		strings.HasPrefix(commandLower, "delete")
}

// IsDDLCommand checks if a command is a DDL command (not allowed in transactions)
// DDL commands: CREATE, DROP, ALTER, TRUNCATE, RENAME, ATTACH, DETACH
func IsDDLCommand(command string) bool {
	commandLower := strings.ToLower(strings.TrimSpace(command))
	return strings.HasPrefix(commandLower, "create") ||
		strings.HasPrefix(commandLower, "drop") ||
		strings.HasPrefix(commandLower, "alter") ||
		strings.HasPrefix(commandLower, "truncate") ||
		strings.HasPrefix(commandLower, "rename") ||
		strings.HasPrefix(commandLower, "attach") ||
		strings.HasPrefix(commandLower, "detach")
}

// IsTransactionCommand checks if a command is a transaction control command
func IsTransactionCommand(command string) bool {
	commandLower := strings.ToLower(strings.TrimSpace(command))
	return strings.HasPrefix(commandLower, "begin") ||
		strings.HasPrefix(commandLower, "commit") ||
		strings.HasPrefix(commandLower, "rollback") ||
		strings.HasPrefix(commandLower, "savepoint") ||
		strings.HasPrefix(commandLower, "set transaction") ||
		strings.HasPrefix(commandLower, "set session") ||
		strings.HasPrefix(commandLower, "show transaction")
}
