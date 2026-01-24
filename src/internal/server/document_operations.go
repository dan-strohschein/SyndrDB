package server

import (
	"context"
	"fmt"
	"sort"
	"strings"
	bndle "syndrdb/src/internal/domain/bundle"
	"syndrdb/src/internal/domain/models"
	"syndrdb/src/internal/query/planner"
	"syndrdb/src/pkg/common/helpers"
	"syndrdb/src/pkg/errors"
	"time"

	"go.uber.org/zap"
)

// UpdateDocument handles UPDATE DOCUMENTS commands
// Syntax: UPDATE DOCUMENTS IN BUNDLE "<BUNDLE_NAME>" (<FIELD_NAME> = <VALUE>) WHERE <WHERE_CLAUSE>;
func UpdateDocument(commandParts []string, serviceManager ServiceManager, database *models.Database, command string, logger *zap.SugaredLogger, session *Session) (*CommandResponse, error) {
	// Enhanced bundle name parsing following SyndrDB comprehensive error handling
	// This replaces the fragile index-based parsing with robust string extraction
		bundleName, err := parseBundleNameFromCommand(command, "IN")
	if err != nil {
		logger.Errorf("Failed to parse bundle name from UPDATE command: %v", err)
		logger.Debugf("Command was: %s", command)
		return nil, errors.WrapWithMessage(err, errors.ERR_VALIDATION_SYNTAX,
			"UPDATE DOCUMENTS command parsing failed", errors.LayerCommand).WithContext("command", command)
	}

	// Additional validation following SyndrDB defensive programming practices
	if bundleName == "" {
		return nil, errors.New(errors.ERR_VALIDATION_REQUIRED,
			"bundle name cannot be empty in UPDATE DOCUMENTS command", errors.LayerCommand)
	}

	// Parse the document command using new parser with feature flag support
	// This will attempt new parser if enabled, fallback to legacy parser on failure
	docCommand, err := parseUpdateDocument(command, logger)
	if err != nil {
		return nil, errors.ConvertError(err, errors.LayerCommand).WithContext("command", command)
	}

	// SAFETY: Validate CONFIRMED keyword requirement for bulk updates without WHERE clause
	if docCommand.WhereClause == "" && !docCommand.Confirmed {
		return nil, errors.New(errors.ERR_VALIDATION_REQUIRED,
			fmt.Sprintf("bulk UPDATE without WHERE clause requires CONFIRMED keyword. Use: UPDATE DOCUMENTS IN BUNDLE \"%s\" (...) CONFIRMED", bundleName),
			errors.LayerCommand).WithContext("bundle", bundleName)
	}

	// Get the bundle by name
	bundle, err := serviceManager.BundleService.GetBundleByName(database, bundleName)
	if err != nil {
		return nil, errors.ConvertError(err, errors.LayerCommand).WithContext("bundle", bundleName)
	}

	// TASK 2: Document-level locking - Get document IDs and acquire locks before execution
	// This allows UpdateDocumentInBundle to skip bundle write lock and use document locks instead
	var lockInfo *bndle.DocumentLockInfo
	var docIDs []string
	
	// OPTIMIZATION: Skip pre-fetch for large bundles where we'd use bundle-level locking anyway
	// Document-level locking only helps when count <= threshold. For large bundles, the pre-fetch
	// is wasted work because UpdateDocumentInBundle will re-scan anyway.
	// Threshold aligns with lockEscalationThreshold (Phase 5: significantly increased).
	preFetchThreshold := int64(200_000)
	shouldPreFetch := bundle.TotalDocuments <= preFetchThreshold
	
	// Get document IDs using query planner (same fast path as SELECT)
	if shouldPreFetch && docCommand.WhereClause != "" && strings.TrimSpace(docCommand.WhereClause) != "" {
		// Use query planner to get document IDs (Task 1 implementation)
		// For now, use WhereFilterService as fallback until query planner integration is complete
		whereService := bndle.NewWhereFilterService(serviceManager.BundleService, logger)
		docIDs, err = whereService.GetDocumentIDsByFilter(bundle, docCommand.WhereClause)
		if err != nil {
			return nil, errors.WrapWithMessage(err, errors.ERR_INTERNAL_QUERY,
				"failed to filter documents by WHERE clause", errors.LayerQuery).WithContext("bundle", bundleName)
		}
		logger.Debugf("Pre-fetched %d document IDs for potential document-level locking", len(docIDs))
	} else if !shouldPreFetch {
		// Large bundle - skip pre-fetch, use bundle-level locking
		// UpdateDocumentInBundle will do a single scan with bundle lock (more efficient)
		logger.Debugf("Skipping pre-fetch for large bundle (%d docs > threshold %d), using bundle-level locking",
			bundle.TotalDocuments, preFetchThreshold)
		docIDs = []string{} // Empty - signals to use bundle lock path
	} else {
		// Empty WHERE clause - will need bundle lock (bulk update)
		docIDs = []string{}
	}

	// TASK 2: Acquire document-level locks if we have document IDs
	// DOCUMENT-LEVEL LOCKING: Extended to autocommit operations for better concurrent write throughput
	// For explicit transactions, locks are held until commit/rollback
	// For autocommit, locks are released after the operation completes.
	// Phase 5: significantly increased (was 1000) so document-level locks used for almost all updates.
	const lockEscalationThreshold = 100_000
	useDocumentLocks := len(docIDs) > 0 && len(docIDs) <= lockEscalationThreshold
	
	logger.Debugf("UPDATE document locking decision: docIDs=%d, threshold=%d, useDocumentLocks=%v",
		len(docIDs), lockEscalationThreshold, useDocumentLocks)
	
	// Determine txID and sessionID for locking
	var lockTxID, lockSessionID string
	isAutocommit := false
	if session != nil && session.IsInTransaction() {
		// Explicit transaction - use existing IDs
		lockTxID = session.ActiveTransactionID
		lockSessionID = session.SessionID
	} else if useDocumentLocks {
		// Autocommit - generate temporary IDs for document locks
		// These locks will be released after the operation completes
		lockTxID = helpers.GenerateFastUUID()
		lockSessionID = "autocommit-" + lockTxID[:8]
		isAutocommit = true
	}

	if useDocumentLocks && lockTxID != "" {
		// Sort document IDs to prevent deadlocks (always acquire in same order)
		sort.Strings(docIDs)
		
		// Acquire write locks for all matching documents
		lockedDocIDs := make([]string, 0, len(docIDs))
		for _, docID := range docIDs {
			if err := serviceManager.LockManager.AcquireWriteLock(bundleName, docID, lockTxID, lockSessionID); err != nil {
				// Release all locks for this transaction (ReleaseLocks releases all locks for txID)
				serviceManager.LockManager.ReleaseLocks(lockTxID)
				return nil, errors.WrapWithMessage(err, errors.ERR_INTERNAL_LOCK,
					fmt.Sprintf("failed to acquire write lock for document %s", docID), errors.LayerTransaction).WithContext("document_id", docID)
			}
			lockedDocIDs = append(lockedDocIDs, docID)
		}
		
		// Create lock info to pass to UpdateDocumentInBundle
		lockInfo = &bndle.DocumentLockInfo{
			LockManager:  serviceManager.LockManager,
			TxID:         lockTxID,
			SessionID:    lockSessionID,
			LockedDocIDs: lockedDocIDs,
		}
		
		// TASK 2: Release locks on error or for autocommit
		// For explicit transactions: locks are held until transaction commits/rolls back (unless error)
		// For autocommit: always release locks after operation completes
		defer func() {
			if lockInfo != nil && lockInfo.LockManager != nil && (err != nil || isAutocommit) {
				serviceManager.LockManager.ReleaseLocks(lockTxID)
				if isAutocommit {
					logger.Debugf("Released document locks after autocommit operation")
				} else {
					logger.Debugf("Released document locks due to update error")
				}
			}
		}()
		
		if isAutocommit {
			logger.Debugf("Acquired document-level write locks for %d documents (autocommit txID=%s)", len(lockedDocIDs), lockTxID[:8])
		} else {
			logger.Debugf("Acquired document-level write locks for %d documents in transaction %s", len(lockedDocIDs), lockTxID)
		}
	}
	// NOTE: When count > threshold, we don't pass docIDs because without locks, concurrent
	// transactions could delete/modify those documents, making the IDs stale.
	// UpdateDocumentInBundle will re-run the WHERE query which is safer.

	// PHASE 1: Build context with MVCC snapshot when session in transaction (for getDocumentsByQueryPlanner)
	ctx := context.Background()
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
		globalMetrics := GetGlobalServerMetrics()
		globalMetrics.TransactionsBegun.Add(1)

		err = serviceManager.WALManager.ExecuteWithLogging(func(txID string) error {
			err := serviceManager.WALManager.LogDocumentUpdate(txID, bundleName, "multiple", nil, docCommand.Fields)
			if err != nil {
				return errors.WrapWithMessage(err, errors.ERR_INTERNAL_WAL,
					"failed to log document update", errors.LayerWAL).WithContext("bundle", bundleName)
			}
			li := lockInfo
			if li == nil {
				li = &bndle.DocumentLockInfo{TxID: txID, VersionTxID: txID}
			} else {
				li = &bndle.DocumentLockInfo{
					LockManager:  li.LockManager,
					TxID:         li.TxID,
					SessionID:    li.SessionID,
					LockedDocIDs: li.LockedDocIDs,
					VersionTxID:  txID,
				}
			}
			return serviceManager.BundleService.UpdateDocumentInBundle(ctx, database, bundle, docCommand, li)
		})

		if err != nil {
			globalMetrics.TransactionsRolledBack.Add(1)
		} else {
			globalMetrics.TransactionsCommitted.Add(1)
		}
	} else {
		logger.Warn("WAL Manager not available, executing without transaction logging")
		if lockInfo != nil {
			err = serviceManager.BundleService.UpdateDocumentInBundle(ctx, database, bundle, docCommand, lockInfo)
		} else {
			err = serviceManager.BundleService.UpdateDocumentInBundle(ctx, database, bundle, docCommand)
		}
		// Note: without WAL we have no txID; Phase 2b version fields use 0
	}

	if err != nil {
		return nil, errors.ConvertError(err, errors.LayerCommand).WithContext("bundle", bundleName)
	}

	// METRICS: Track document update
	globalMetrics := GetGlobalServerMetrics()
	globalMetrics.DocumentUpdatesTotal.Add(1)
	dbMetrics := GetDatabaseMetrics(database.Name)
	dbMetrics.DBDocumentUpdatesTotal.Add(1)
	bundleMetrics := GetBundleMetrics(database.Name, bundleName)
	bundleMetrics.BundleDocumentsUpdated.Add(1)

	// Track write for plan cache invalidation (MongoDB-style write-threshold)
	if serviceManager.UnifiedPlanner != nil {
		invalidationMgr := serviceManager.UnifiedPlanner.GetInvalidationManager()
		if invalidationMgr != nil {
			invalidationMgr.OnWrite(bundleName, 1)
		}
	}

	cmdResponse := &CommandResponse{
		ResultCount: 1,
		Result:      "Document updated successfully in bundle '" + bundleName + "'.",
	}
	return cmdResponse, nil
}

// AddDocument handles ADD DOCUMENT commands
// Syntax: ADD DOCUMENT TO "<BUNDLE_NAME>" (<FIELD_NAME>: <VALUE>, ...);
func AddDocument(commandParts []string, command string, logger *zap.SugaredLogger, serviceManager ServiceManager, database *models.Database, session *Session) (*CommandResponse, error) {
	startingTime := time.Now()

	// TRACE: Start comprehensive tracing
	tr := StartRegion("AddDocument.TOTAL", logger)
	defer tr.End()

	logger.Debugf("Trying to add document to %s.%s", database.Name, commandParts[3])

	if len(commandParts) < 4 {
		return nil, errors.New(errors.ERR_VALIDATION_SYNTAX,
			"ADD DOCUMENT requires the spec 'TO <bundle_name>'", errors.LayerCommand)
	}

	// Parse the document command using new parser with fallback
	// This uses the same feature flag and fallback pattern as SELECT queries
	parseRegion := StartRegion("AddDocument.ParseCommand", logger)
	docCommand, err := parseAddDocument(command, logger)
	parseRegion.End()
	if err != nil {
		return nil, errors.ConvertError(err, errors.LayerCommand).WithContext("command", command)
	}

	bundleName := docCommand.BundleName
	// Get the bundle by name
	bundleLookupRegion := StartRegion("AddDocument.GetBundleByName", logger)
	bundle, err := serviceManager.BundleService.GetBundleByName(database, docCommand.BundleName)
	bundleLookupRegion.End()
	if err != nil {
		return nil, errors.ConvertError(err, errors.LayerCommand).WithContext("bundle", bundleName)
	}

	docID := ""

	// Execute with WAL logging
	if serviceManager.WALManager != nil {
		if session != nil && session.IsInTransaction() {
			// PHASE 3: Document-level lock for ADD; no bundle lock. Generate ID, lock, then add. Hold until COMMIT (strict 2PL).
			txID := session.ActiveTransactionID
			preallocID := helpers.GenerateFastUUID()
			if serviceManager.LockManager != nil {
				if err := serviceManager.LockManager.AcquireWriteLock(bundleName, preallocID, txID, session.SessionID); err != nil {
					return nil, errors.WrapWithMessage(err, errors.ERR_INTERNAL_LOCK,
						"failed to acquire document lock for insert", errors.LayerTransaction).WithContext("bundle", bundleName)
				}
				logger.Debugf("Acquired document lock for insert in transaction %s (docID=%s)", txID, preallocID)
			}

			logger.Infof("TRANSACTION INSERT: txID=%s, bundle=%s, session=%s", txID, bundleName, session.SessionID)

			docID, err = serviceManager.BundleService.AddDocumentToBundleWithTxID(database, bundle, docCommand, txID, preallocID)
			if err != nil {
				if serviceManager.LockManager != nil {
					serviceManager.LockManager.ReleaseLocks(txID)
				}
				return nil, errors.ConvertError(err, errors.LayerCommand).WithContext("bundle", bundleName)
			}

			// PHASE 2: MVCC - Track document location in transaction buffer for commit sequence assignment
			// Get pageID and fileID from bundle storage (we'll need to enhance this to return location info)
			// For now, we'll track what we can - the full implementation will get fileID from manifest
			if session.TransactionBuffer != nil {
				// Get fileID from manifest (simplified - full implementation will get from AppendDocumentToBundleFileWithTxID return)
				// TODO: Enhance AppendDocumentToBundleFileWithTxID to return location struct with pageID, fileID, offset
				location := DocumentLocation{
					BundleName: bundleName,
					PageID:     0, // Will be updated when we enhance the return value
					FileID:     0, // Will be updated when we enhance the return value
					Offset:     0, // Optional for now
				}
				session.TransactionBuffer.AddDocumentLocation(docID, location)
			}

			// Log the insertion to the WAL
			err = serviceManager.WALManager.LogDocumentInsert(txID, bundleName, docID, docCommand.Fields)
			if err != nil {
				return nil, errors.WrapWithMessage(err, errors.ERR_INTERNAL_WAL,
					"failed to log document insert", errors.LayerWAL).WithContext("bundle", bundleName).WithContext("document_id", docID)
			}

			logger.Infof("TRANSACTION INSERT LOGGED: txID=%s, bundle=%s, docID=%s", txID, bundleName, docID)
		} else {
			// Auto-commit transaction: use ExecuteWithLogging wrapper
			globalMetrics := GetGlobalServerMetrics()
			globalMetrics.TransactionsBegun.Add(1)

			// WAL logging with execution
			walRegion := StartRegion("AddDocument.WAL.ExecuteWithLogging", logger)
			err = serviceManager.WALManager.ExecuteWithLogging(func(txID string) error {
				// Log the document insertion before execution
				walLogRegion := StartRegion("AddDocument.WAL.LogInsert", logger)
				err := serviceManager.WALManager.LogDocumentInsert(txID, bundleName, "pending", docCommand.Fields)
				walLogRegion.End()
				if err != nil {
					return errors.WrapWithMessage(err, errors.ERR_INTERNAL_WAL,
						"failed to log document insert", errors.LayerWAL).WithContext("bundle", bundleName)
				}

				// Add the document to the bundle
				addDocRegion := StartRegion("AddDocument.BundleService.AddDocumentToBundle", logger)
				docID, err = serviceManager.BundleService.AddDocumentToBundle(database, bundle, docCommand)
				addDocRegion.EndWithData(map[string]interface{}{"docID": docID})
				return err
			})
			walRegion.End()

			// METRICS: Track transaction outcome
			if err != nil {
				globalMetrics.TransactionsRolledBack.Add(1)
			} else {
				globalMetrics.TransactionsCommitted.Add(1)
			}
		}
	} else {
		// Fallback to direct execution if WAL is not available
		logger.Warn("WAL Manager not available, executing without transaction logging")
		docID, err = serviceManager.BundleService.AddDocumentToBundle(database, bundle, docCommand)
	}

	if err != nil {
		return nil, errors.ConvertError(err, errors.LayerCommand).WithContext("bundle", bundleName)
	}

	// METRICS: Track document insert
	globalMetrics := GetGlobalServerMetrics()
	globalMetrics.DocumentInsertsTotal.Add(1)
	dbMetrics := GetDatabaseMetrics(database.Name)
	dbMetrics.DBDocumentInsertsTotal.Add(1)
	bundleMetrics := GetBundleMetrics(database.Name, bundleName)
	bundleMetrics.BundleDocumentsInserted.Add(1)
	bundleMetrics.BundleCurrentDocCount.Add(1)

	// Track write for plan cache invalidation (MongoDB-style write-threshold)
	if serviceManager.UnifiedPlanner != nil {
		invalidationMgr := serviceManager.UnifiedPlanner.GetInvalidationManager()
		if invalidationMgr != nil {
			invalidationMgr.OnWrite(bundleName, 1)
		}
	}

	result := fmt.Sprintf("{\"DocumentID\": \"%s\", \"message\": \"Document added successfully to bundle '%s'.\"}", docID, bundleName)
	cmdResponse := &CommandResponse{
		ResultCount: 1,
		Result:      result,
	}
	endingTime := time.Since(startingTime)
	logger.Infof("DEBUG DEBUG :: AddDocument total time: %s", endingTime.String())
	return cmdResponse, nil
}
