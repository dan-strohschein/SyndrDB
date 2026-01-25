// occ_update.go provides Optimistic Concurrency Control (OCC) for UPDATE operations.
// OCC avoids upfront lock acquisition by reading documents without locks, computing changes,
// then validating at commit that no concurrent modifications occurred. This significantly
// improves throughput when multiple transactions update overlapping document sets.
//
// The hybrid approach tries OCC first, then falls back to pessimistic locking after
// repeated conflicts, providing the best of both worlds:
// - Low contention: OCC wins (no lock waiting)
// - High contention: Pessimistic prevents livelock
package server

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"time"

	bndle "syndrdb/src/internal/domain/bundle"
	"syndrdb/src/internal/domain/models"
	"syndrdb/src/internal/query/planner"
	"syndrdb/src/pkg/errors"

	"go.uber.org/zap"
)

// OCC configuration constants
const (
	// occRetryLimit is the maximum number of OCC attempts before escalating to pessimistic locking
	occRetryLimit = 3

	// occBackoffBase is the initial backoff duration between OCC retries
	occBackoffBase = 5 * time.Millisecond

	// occBackoffMax is the maximum backoff duration between OCC retries
	occBackoffMax = 100 * time.Millisecond
)

// documentVersion captures the version info of a document at read time for OCC validation
type documentVersion struct {
	DocumentID      string
	VersionSequence uint64
	CommitSequence  uint64
	CreatedByTxID   uint64
}

// occReadResult holds the documents and their versions from the OCC read phase
type occReadResult struct {
	Documents []*models.Document
	Versions  map[string]documentVersion // docID -> version at read time
}

// executeUpdateWithOCC attempts an optimistic update first, falling back to pessimistic on conflicts.
// This is the main entry point for hybrid OCC updates.
func executeUpdateWithOCC(
	ctx context.Context,
	serviceManager ServiceManager,
	database *models.Database,
	bundle *models.Bundle,
	docCommand *models.DocumentUpdateCommand,
	session *Session,
	logger *zap.SugaredLogger,
) error {
	bundleName := bundle.Name

	// Try OCC path first (no upfront locks)
	for attempt := 0; attempt < occRetryLimit; attempt++ {
		err := executeOptimisticUpdate(ctx, serviceManager, database, bundle, docCommand, session, logger)
		if err == nil {
			if attempt > 0 {
				logger.Debugf("OCC update succeeded on attempt %d for bundle '%s'", attempt+1, bundleName)
			}
			return nil
		}

		// Check if it's a version conflict (retryable)
		if !isVersionConflictError(err) {
			// Non-retryable error, return immediately
			return err
		}

		logger.Debugf("OCC version conflict on attempt %d for bundle '%s', will retry", attempt+1, bundleName)

		// Exponential backoff with jitter before retry
		if attempt < occRetryLimit-1 {
			backoff := occBackoffBase * time.Duration(1<<attempt)
			if backoff > occBackoffMax {
				backoff = occBackoffMax
			}
			// Add jitter (0-50% of backoff)
			jitter := time.Duration(rand.Int63n(int64(backoff / 2)))
			time.Sleep(backoff + jitter)
		}
	}

	// OCC failed after all retries, escalate to pessimistic locking
	logger.Debugf("OCC failed after %d attempts for bundle '%s', escalating to pessimistic locking", occRetryLimit, bundleName)
	return executePessimisticUpdate(ctx, serviceManager, database, bundle, docCommand, session, logger)
}

// executeOptimisticUpdate performs an update using optimistic concurrency control.
// Phase 1: Read documents without locks, record versions
// Phase 2: Compute changes in memory
// Phase 3: Acquire brief validation lock, verify versions, apply changes
func executeOptimisticUpdate(
	ctx context.Context,
	serviceManager ServiceManager,
	database *models.Database,
	bundle *models.Bundle,
	docCommand *models.DocumentUpdateCommand,
	session *Session,
	logger *zap.SugaredLogger,
) error {
	// PHASE 1: READ - Get documents without locks, capture versions
	readResult, err := occReadPhase(serviceManager, bundle, docCommand, logger)
	if err != nil {
		return err
	}

	if len(readResult.Documents) == 0 {
		// No documents to update
		return nil
	}

	// PHASE 2: COMPUTE - Changes are computed by UpdateDocumentInBundle
	// We just need to pass the pre-fetched docs and their versions for validation

	// PHASE 3: VALIDATE & WRITE - Brief lock, verify versions, apply changes
	return occValidateAndWrite(ctx, serviceManager, database, bundle, docCommand, readResult, session, logger)
}

// occReadPhase reads documents matching the WHERE clause without acquiring locks.
// Returns the documents and their version info for later validation.
func occReadPhase(
	serviceManager ServiceManager,
	bundle *models.Bundle,
	docCommand *models.DocumentUpdateCommand,
	logger *zap.SugaredLogger,
) (*occReadResult, error) {
	bundleName := bundle.Name
	result := &occReadResult{
		Versions: make(map[string]documentVersion),
	}

	// Use read lock only (allows concurrent reads)
	if err := serviceManager.BundleService.AcquireBundleReadLock(bundleName); err != nil {
		return nil, errors.WrapWithMessage(err, errors.ERR_INTERNAL_LOCK,
			"OCC read phase: failed to acquire read lock", errors.LayerTransaction)
	}
	defer serviceManager.BundleService.ReleaseBundleReadLock(bundleName)

	// Get documents matching WHERE clause
	var docs []*models.Document
	var err error

	if docCommand.WhereClause != "" && strings.TrimSpace(docCommand.WhereClause) != "" {
		docs, err = serviceManager.BundleService.GetDocumentsByFilter(bundle, docCommand.WhereClause, nil)
	} else {
		// Bulk update without WHERE - get all documents
		docs, err = serviceManager.BundleService.GetDocumentsByFilter(bundle, "", nil)
	}

	if err != nil {
		return nil, errors.WrapWithMessage(err, errors.ERR_INTERNAL_QUERY,
			"OCC read phase: failed to filter documents", errors.LayerQuery)
	}

	// Capture version info for each document
	for _, doc := range docs {
		if doc == nil {
			continue
		}
		result.Versions[doc.DocumentID] = documentVersion{
			DocumentID:      doc.DocumentID,
			VersionSequence: doc.VersionSequence,
			CommitSequence:  doc.CommitSequence,
			CreatedByTxID:   doc.CreatedByTxID,
		}
	}

	result.Documents = docs
	logger.Debugf("OCC read phase: captured %d documents with versions for bundle '%s'", len(docs), bundleName)

	return result, nil
}

// occValidateAndWrite validates that document versions haven't changed, then applies the update.
// This is the critical section where we hold a brief write lock.
func occValidateAndWrite(
	ctx context.Context,
	serviceManager ServiceManager,
	database *models.Database,
	bundle *models.Bundle,
	docCommand *models.DocumentUpdateCommand,
	readResult *occReadResult,
	session *Session,
	logger *zap.SugaredLogger,
) error {
	bundleName := bundle.Name

	// Acquire write lock for validation and write
	if err := serviceManager.BundleService.AcquireBundleWriteLock(bundleName); err != nil {
		return errors.WrapWithMessage(err, errors.ERR_INTERNAL_LOCK,
			"OCC validate phase: failed to acquire write lock", errors.LayerTransaction)
	}
	defer serviceManager.BundleService.ReleaseBundleWriteLock(bundleName)

	// VALIDATION: Check that no document versions have changed
	for docID, readVersion := range readResult.Versions {
		currentDoc, err := serviceManager.BundleService.GetDocumentByID(bundle, docID)
		if err != nil {
			// Document was deleted - version conflict
			return errors.New(errors.ERR_TRANSACTION_VERSION_CONFLICT,
				"OCC validation failed: document was deleted during transaction",
				errors.LayerTransaction).WithContext("document_id", docID)
		}

		if currentDoc == nil {
			return errors.New(errors.ERR_TRANSACTION_VERSION_CONFLICT,
				"OCC validation failed: document was deleted during transaction",
				errors.LayerTransaction).WithContext("document_id", docID)
		}

		// Version check: VersionSequence or CommitSequence changed = conflict
		if currentDoc.VersionSequence != readVersion.VersionSequence {
			return errors.New(errors.ERR_TRANSACTION_VERSION_CONFLICT,
				fmt.Sprintf("OCC validation failed: document version changed during transaction (read: %d, current: %d)",
					readVersion.VersionSequence, currentDoc.VersionSequence),
				errors.LayerTransaction).WithContext("document_id", docID)
		}

		// Also check CommitSequence - if it changed, another transaction committed
		if currentDoc.CommitSequence != readVersion.CommitSequence {
			return errors.New(errors.ERR_TRANSACTION_VERSION_CONFLICT,
				"OCC validation failed: document was committed by another transaction",
				errors.LayerTransaction).WithContext("document_id", docID)
		}
	}

	logger.Debugf("OCC validation passed for %d documents in bundle '%s'", len(readResult.Versions), bundleName)

	// WRITE: Apply the update (we already hold the write lock)
	// Create lock info with pre-fetched docs to skip re-fetching
	lockInfo := &bndle.DocumentLockInfo{
		PreFetchedDocs: readResult.Documents,
		// No LockManager - we're using bundle-level lock for the OCC write phase
	}

	// Build context with MVCC snapshot if in transaction
	if session != nil && session.IsInTransaction() {
		if snap := session.GetMVCCSnapshot(); snap != nil {
			snapshotInfo := &planner.SnapshotInfo{
				SnapshotSequence: snap.SnapshotSequence,
				TransactionID:    snap.TransactionID,
				ActiveTxIDs:      snap.ActiveTxIDs,
			}
			ctx = planner.WithSnapshotInfo(ctx, snapshotInfo)
		}
	}

	// Execute with WAL logging if available
	if serviceManager.WALManager != nil {
		return serviceManager.WALManager.ExecuteWithLogging(func(txID string) error {
			if err := serviceManager.WALManager.LogDocumentUpdate(txID, bundleName, "multiple", nil, docCommand.Fields); err != nil {
				return errors.WrapWithMessage(err, errors.ERR_INTERNAL_WAL,
					"failed to log document update", errors.LayerWAL).WithContext("bundle", bundleName)
			}
			lockInfo.VersionTxID = txID
			return serviceManager.BundleService.UpdateDocumentInBundle(ctx, database, bundle, docCommand, lockInfo)
		})
	}

	// No WAL - direct update
	return serviceManager.BundleService.UpdateDocumentInBundle(ctx, database, bundle, docCommand, lockInfo)
}

// executePessimisticUpdate performs a traditional pessimistic update with document-level locks.
// This is the fallback when OCC fails due to high contention.
func executePessimisticUpdate(
	ctx context.Context,
	serviceManager ServiceManager,
	database *models.Database,
	bundle *models.Bundle,
	docCommand *models.DocumentUpdateCommand,
	session *Session,
	logger *zap.SugaredLogger,
) error {
	// This delegates to the existing pessimistic update logic
	// We need to build the lock info and call UpdateDocumentInBundle

	bundleName := bundle.Name

	// Build context with MVCC snapshot if in transaction
	if session != nil && session.IsInTransaction() {
		if snap := session.GetMVCCSnapshot(); snap != nil {
			snapshotInfo := &planner.SnapshotInfo{
				SnapshotSequence: snap.SnapshotSequence,
				TransactionID:    snap.TransactionID,
				ActiveTxIDs:      snap.ActiveTxIDs,
			}
			ctx = planner.WithSnapshotInfo(ctx, snapshotInfo)
		}
	}

	// Use bundle-level lock for pessimistic fallback (simpler, no document-level lock convoy)
	lockInfo := &bndle.DocumentLockInfo{}

	// Execute with WAL logging if available
	if serviceManager.WALManager != nil {
		return serviceManager.WALManager.ExecuteWithLogging(func(txID string) error {
			if err := serviceManager.WALManager.LogDocumentUpdate(txID, bundleName, "multiple", nil, docCommand.Fields); err != nil {
				return errors.WrapWithMessage(err, errors.ERR_INTERNAL_WAL,
					"failed to log document update", errors.LayerWAL).WithContext("bundle", bundleName)
			}
			lockInfo.TxID = txID
			lockInfo.VersionTxID = txID
			return serviceManager.BundleService.UpdateDocumentInBundle(ctx, database, bundle, docCommand, lockInfo)
		})
	}

	// No WAL - direct update
	return serviceManager.BundleService.UpdateDocumentInBundle(ctx, database, bundle, docCommand, lockInfo)
}

// isVersionConflictError returns true if the error is a version conflict that should trigger a retry
func isVersionConflictError(err error) bool {
	if err == nil {
		return false
	}

	// Check if it's our custom SyndrDBError with version conflict code
	if syndrErr, ok := err.(errors.SyndrDBError); ok {
		return syndrErr.Code() == errors.ERR_TRANSACTION_VERSION_CONFLICT
	}

	// Also check string content as fallback
	errStr := err.Error()
	return strings.Contains(errStr, "version conflict") ||
		strings.Contains(errStr, "ERR_TRANSACTION_VERSION_CONFLICT")
}
