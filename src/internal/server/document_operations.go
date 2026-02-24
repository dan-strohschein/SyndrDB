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
	"syndrdb/src/pkg/settings"
	"time"

	"go.uber.org/zap"
)

// UpdateDocument handles UPDATE DOCUMENTS commands
// Syntax: UPDATE DOCUMENTS IN BUNDLE "<BUNDLE_NAME>" (<FIELD_NAME> = <VALUE>) WHERE <WHERE_CLAUSE>;
//
// This function uses a Hybrid OCC (Optimistic Concurrency Control) approach:
// 1. First tries optimistic update (no upfront locks, validate at commit)
// 2. Falls back to pessimistic locking after repeated conflicts
// This provides best performance under both low and high contention scenarios.
func UpdateDocument(commandParts []string, serviceManager ServiceManager, database *models.Database, command string, logger *zap.SugaredLogger, session *Session, startTime time.Time) (*CommandResponse, error) {
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

	// HYBRID OCC: For autocommit operations (non-transactional), use OCC with pessimistic fallback.
	// This provides better throughput under contention by avoiding sequential lock acquisition.
	// For explicit transactions, continue using document-level locks for strict 2PL semantics.
	useHybridOCC := session == nil || !session.IsInTransaction()

	if useHybridOCC {
		ctx := context.Background()
		globalMetrics := GetGlobalServerMetrics()
		globalMetrics.TransactionsBegun.Add(1)

		args := settings.GetSettings()
		if args.EnableRCUWrites {
			logger.Debugf("Using RCU-direct UPDATE on bundle '%s'", bundleName)
			err = executeUpdateRCUDirect(ctx, serviceManager, database, bundle, docCommand, session, logger)
		} else {
			logger.Debugf("Using hybrid OCC for UPDATE on bundle '%s'", bundleName)
			err = executeUpdateWithOCC(ctx, serviceManager, database, bundle, docCommand, session, logger)
		}

		if err != nil {
			globalMetrics.TransactionsRolledBack.Add(1)
			return nil, errors.ConvertError(err, errors.LayerCommand).WithContext("bundle", bundleName)
		}

		globalMetrics.TransactionsCommitted.Add(1)

		// METRICS: Track document update
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
			ResultCount:     1,
			Result:          "Document updated successfully in bundle '" + bundleName + "'.",
			ExecutionTimeMS: float64(time.Since(startTime).Nanoseconds()) / 1e6,
		}
		return cmdResponse, nil
	}

	// EXPLICIT TRANSACTION PATH: Use document-level locking for strict 2PL semantics
	return updateDocumentPessimistic(commandParts, serviceManager, database, bundle, docCommand, session, logger, startTime)
}

// updateDocumentPessimistic handles UPDATE using traditional pessimistic document-level locking.
// Used for explicit transactions where strict 2PL semantics are required.
func updateDocumentPessimistic(commandParts []string, serviceManager ServiceManager, database *models.Database, bundle *models.Bundle, docCommand *models.DocumentUpdateCommand, session *Session, logger *zap.SugaredLogger, startTime time.Time) (*CommandResponse, error) {
	bundleName := bundle.Name

	// EXPLICIT TRANSACTION: Document-level locking for strict 2PL semantics
	// Locks are held until transaction commits/rolls back
	var lockInfo *bndle.DocumentLockInfo
	var docIDs []string
	var preFetchedDocs []*models.Document
	var err error

	preFetchThreshold := int64(200_000)
	shouldPreFetch := bundle.TotalDocuments <= preFetchThreshold

	if shouldPreFetch && docCommand.WhereClause != "" && strings.TrimSpace(docCommand.WhereClause) != "" {
		whereService := bndle.NewWhereFilterService(serviceManager.BundleService, logger)
		docIDs, preFetchedDocs, err = whereService.GetDocumentsAndIDsByFilter(bundle, docCommand.WhereClause)
		if err != nil {
			return nil, errors.WrapWithMessage(err, errors.ERR_INTERNAL_QUERY,
				"failed to filter documents by WHERE clause", errors.LayerQuery).WithContext("bundle", bundleName)
		}
		logger.Debugf("Pre-fetched %d document IDs and docs for document-level locking in transaction", len(docIDs))
	} else if !shouldPreFetch {
		logger.Debugf("Skipping pre-fetch for large bundle (%d docs > threshold %d), using bundle-level locking",
			bundle.TotalDocuments, preFetchThreshold)
		docIDs = []string{}
	} else {
		docIDs = []string{}
	}

	// Document-level locking for explicit transactions
	const lockEscalationThreshold = 100_000
	useDocumentLocks := len(docIDs) > 0 && len(docIDs) <= lockEscalationThreshold

	// For explicit transactions, use session's transaction ID
	lockTxID := session.ActiveTransactionID
	lockSessionID := session.SessionID

	if useDocumentLocks && lockTxID != "" {
		// Sort document IDs to prevent deadlocks (always acquire in same order)
		sort.Strings(docIDs)

		// SEQUENTIAL lock acquisition - guarantees global ordering for deadlock prevention
		lockedDocIDs := make([]string, 0, len(docIDs))
		var firstErr error
		var failedDocID string

		for _, docID := range docIDs {
			if err := serviceManager.LockManager.AcquireWriteLock(bundleName, docID, lockTxID, lockSessionID); err != nil {
				firstErr = err
				failedDocID = docID
				break
			}
			lockedDocIDs = append(lockedDocIDs, docID)
		}

		if firstErr != nil {
			serviceManager.LockManager.ReleaseLocks(lockTxID)
			return nil, errors.WrapWithMessage(firstErr, errors.ERR_INTERNAL_LOCK,
				fmt.Sprintf("failed to acquire write lock for document %s", failedDocID), errors.LayerTransaction).WithContext("document_id", failedDocID)
		}

		// Create lock info to pass to UpdateDocumentInBundle
		lockInfo = &bndle.DocumentLockInfo{
			LockManager:    serviceManager.LockManager,
			TxID:           lockTxID,
			SessionID:      lockSessionID,
			LockedDocIDs:   lockedDocIDs,
			PreFetchedDocs: preFetchedDocs,
		}

		// For explicit transactions: release locks only on error (locks held until commit/rollback)
		defer func() {
			if lockInfo != nil && lockInfo.LockManager != nil && err != nil {
				serviceManager.LockManager.ReleaseLocks(lockTxID)
				logger.Debugf("Released document locks due to update error")
			}
		}()

		logger.Debugf("Acquired document-level write locks for %d documents in transaction %s", len(lockedDocIDs), lockTxID)
	}

	// Build context with MVCC snapshot for transaction
	ctx := context.Background()
	if snap := session.GetMVCCSnapshot(); snap != nil {
		snapshotInfo := &planner.SnapshotInfo{
			SnapshotSequence: snap.SnapshotSequence,
			TransactionID:    snap.TransactionID,
			ActiveTxIDs:      snap.ActiveTxIDs,
		}
		ctx = planner.WithSnapshotInfo(ctx, snapshotInfo)
	}

	// Execute with WAL logging if available
	if serviceManager.WALManager != nil {
		err = serviceManager.WALManager.ExecuteWithLogging(func(txID string) error {
			walErr := serviceManager.WALManager.LogDocumentUpdate(txID, bundleName, "multiple", nil, docCommand.Fields)
			if walErr != nil {
				return errors.WrapWithMessage(walErr, errors.ERR_INTERNAL_WAL,
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
	} else {
		logger.Warn("WAL Manager not available, executing without transaction logging")
		if lockInfo != nil {
			err = serviceManager.BundleService.UpdateDocumentInBundle(ctx, database, bundle, docCommand, lockInfo)
		} else {
			err = serviceManager.BundleService.UpdateDocumentInBundle(ctx, database, bundle, docCommand)
		}
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

	// Track write for plan cache invalidation
	if serviceManager.UnifiedPlanner != nil {
		invalidationMgr := serviceManager.UnifiedPlanner.GetInvalidationManager()
		if invalidationMgr != nil {
			invalidationMgr.OnWrite(bundleName, 1)
		}
	}

	cmdResponse := &CommandResponse{
		ResultCount:     1,
		Result:          "Document updated successfully in bundle '" + bundleName + "'.",
		ExecutionTimeMS: float64(time.Since(startTime).Nanoseconds()) / 1e6,
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
	endingTime := time.Since(startingTime)
	cmdResponse := &CommandResponse{
		ResultCount:     1,
		Result:          result,
		ExecutionTimeMS: float64(endingTime.Nanoseconds()) / 1e6,
	}
	logger.Debugf("DEBUG :: AddDocument total time: %s", endingTime.String())
	return cmdResponse, nil
}

// BulkAddDocuments handles BULK ADD DOCUMENTS and BULK INSERT INTO commands.
// All-or-nothing semantics: all documents are validated upfront; if any fails, the entire batch is rejected.
func BulkAddDocuments(commandParts []string, command string, logger *zap.SugaredLogger, serviceManager ServiceManager, database *models.Database, session *Session) (*CommandResponse, error) {
	startingTime := time.Now()

	// Parse the bulk command
	bulkCmd, err := parseBulkCommand(command, logger)
	if err != nil {
		return nil, errors.ConvertError(err, errors.LayerCommand).WithContext("command", command)
	}

	bundleName := bulkCmd.BundleName
	docCount := len(bulkCmd.Documents)

	// Look up bundle (once)
	bundle, err := serviceManager.BundleService.GetBundleByName(database, bundleName)
	if err != nil {
		return nil, errors.ConvertError(err, errors.LayerCommand).WithContext("bundle", bundleName)
	}

	var docIDs []string

	if serviceManager.WALManager != nil {
		if session != nil && session.IsInTransaction() {
			// TRANSACTION PATH: Pre-allocate doc IDs, acquire write locks, call WithTxID variant
			txID := session.ActiveTransactionID

			preallocIDs := make([]string, docCount)
			for i := range preallocIDs {
				preallocIDs[i] = helpers.GenerateFastUUID()
			}

			// Acquire document-level write locks
			if serviceManager.LockManager != nil {
				sort.Strings(preallocIDs) // prevent deadlocks
				for _, docID := range preallocIDs {
					if err := serviceManager.LockManager.AcquireWriteLock(bundleName, docID, txID, session.SessionID); err != nil {
						serviceManager.LockManager.ReleaseLocks(txID)
						return nil, errors.WrapWithMessage(err, errors.ERR_INTERNAL_LOCK,
							"failed to acquire document lock for bulk insert", errors.LayerTransaction).WithContext("bundle", bundleName)
					}
				}
			}

			docIDs, err = serviceManager.BundleService.AddDocumentsToBundleWithTxID(database, bundle, bulkCmd, txID, preallocIDs)
			if err != nil {
				if serviceManager.LockManager != nil {
					serviceManager.LockManager.ReleaseLocks(txID)
				}
				return nil, errors.ConvertError(err, errors.LayerCommand).WithContext("bundle", bundleName)
			}

			// Track document locations in transaction buffer
			if session.TransactionBuffer != nil {
				for _, docID := range docIDs {
					location := DocumentLocation{
						BundleName: bundleName,
					}
					session.TransactionBuffer.AddDocumentLocation(docID, location)
				}
			}

			// Log the batch insertion to the WAL
			err = serviceManager.WALManager.LogBatchInsert(txID, bundleName, docIDs, bulkCmd.Documents)
			if err != nil {
				return nil, errors.WrapWithMessage(err, errors.ERR_INTERNAL_WAL,
					"failed to log batch insert", errors.LayerWAL).WithContext("bundle", bundleName)
			}
		} else {
			// AUTO-COMMIT PATH: ExecuteWithLogging wrapper
			globalMetrics := GetGlobalServerMetrics()
			globalMetrics.TransactionsBegun.Add(1)

			err = serviceManager.WALManager.ExecuteWithLogging(func(txID string) error {
				walErr := serviceManager.WALManager.LogBatchInsert(txID, bundleName, []string{"pending"}, bulkCmd.Documents)
				if walErr != nil {
					return errors.WrapWithMessage(walErr, errors.ERR_INTERNAL_WAL,
						"failed to log batch insert", errors.LayerWAL).WithContext("bundle", bundleName)
				}
				docIDs, err = serviceManager.BundleService.AddDocumentsToBundle(database, bundle, bulkCmd)
				return err
			})

			if err != nil {
				globalMetrics.TransactionsRolledBack.Add(1)
			} else {
				globalMetrics.TransactionsCommitted.Add(1)
			}
		}
	} else {
		// No-WAL fallback
		logger.Warn("WAL Manager not available, executing bulk insert without transaction logging")
		docIDs, err = serviceManager.BundleService.AddDocumentsToBundle(database, bundle, bulkCmd)
	}

	if err != nil {
		return nil, errors.ConvertError(err, errors.LayerCommand).WithContext("bundle", bundleName)
	}

	// METRICS: Track document inserts
	globalMetrics := GetGlobalServerMetrics()
	globalMetrics.DocumentInsertsTotal.Add(uint64(docCount))
	dbMetrics := GetDatabaseMetrics(database.Name)
	dbMetrics.DBDocumentInsertsTotal.Add(uint64(docCount))
	bundleMetrics := GetBundleMetrics(database.Name, bundleName)
	bundleMetrics.BundleDocumentsInserted.Add(uint64(docCount))
	bundleMetrics.BundleCurrentDocCount.Add(uint64(docCount))

	// Track write for plan cache invalidation
	if serviceManager.UnifiedPlanner != nil {
		invalidationMgr := serviceManager.UnifiedPlanner.GetInvalidationManager()
		if invalidationMgr != nil {
			invalidationMgr.OnWrite(bundleName, docCount)
		}
	}

	result := fmt.Sprintf("{\"documents_inserted\": %d, \"bundle\": \"%s\", \"document_ids\": %v}", docCount, bundleName, docIDs)
	endingTime := time.Since(startingTime)
	cmdResponse := &CommandResponse{
		ResultCount:     docCount,
		Result:          result,
		ExecutionTimeMS: float64(endingTime.Nanoseconds()) / 1e6,
	}
	return cmdResponse, nil
}
