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
	"syndrdb/src/pkg/extension"
	"syndrdb/src/pkg/settings"
	"time"

	"go.uber.org/zap"
)

// notifyCDCAndMatView fires CDC and MatView change hooks after a mutation.
func notifyCDCAndMatView(ctx context.Context, operation, bundleName, docID string, before, after map[string]interface{}, logger *zap.SugaredLogger) {
	reg := extension.GetRegistry()

	// CDC hook: notify all CDC listeners
	if reg.HasCDCExtension() {
		event := extension.DataChangeEvent{
			Operation:  operation,
			BundleName: bundleName,
			DocumentID: docID,
			Before:     before,
			After:      after,
			Timestamp:  time.Now(),
		}
		for _, cdc := range reg.GetCDCExtensions() {
			cdc.OnDataChange(ctx, event)
		}
	}

	// MatView refresh hook: notify if this bundle is a source for any materialized view
	if reg.HasMatViewRefreshExtension() {
		mvExt := reg.GetMatViewRefreshExtension()
		if mvExt.IsSourceBundle(bundleName) {
			if err := mvExt.OnSourceChange(ctx, bundleName, docID, operation, after); err != nil {
				logger.Warnf("MatView OnSourceChange error (%s): %v", operation, err)
			}
		}
	}
}

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

	// SAFETY: Reject updates to DocumentID (immutable system field)
	for _, field := range docCommand.Fields {
		if strings.EqualFold(field.Key, "DocumentID") {
			return nil, errors.New(errors.ERR_VALIDATION_REQUIRED,
				"cannot update DocumentID: this field is immutable and managed by the system",
				errors.LayerCommand).WithContext("bundle", bundleName)
		}
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

	// Enterprise sharding hook: reject updates to the shard key field
	if reg := extension.GetRegistry(); reg.HasShardingExtension() {
		shardExt := reg.GetShardingExtension()
		if shardExt.IsShardedBundle(bundleName) {
			policy := shardExt.GetShardPolicy(bundleName)
			if policy != nil {
				for _, field := range docCommand.Fields {
					if strings.EqualFold(field.Key, policy.ShardKey) {
						return nil, errors.New(errors.ERR_VALIDATION_REQUIRED,
							fmt.Sprintf("cannot update shard key field '%s': updates to the shard key are not supported on sharded bundles", policy.ShardKey),
							errors.LayerCommand).WithContext("bundle", bundleName)
					}
				}
			}
			// Route UPDATE to correct shard(s) based on WHERE clause
			shardBundles := shardExt.ResolveReadShards(bundleName, docCommand.WhereClause)
			if len(shardBundles) == 1 {
				// Single shard — rewrite bundle target
				bundle, err = serviceManager.BundleService.GetBundleByName(database, shardBundles[0])
				if err != nil {
					return nil, errors.ConvertError(err, errors.LayerCommand).WithContext("bundle", shardBundles[0])
				}
				bundleName = shardBundles[0]
				logger.Debugf("Sharding: routed UPDATE to shard bundle '%s'", shardBundles[0])
			} else if len(shardBundles) > 1 && reg.HasDistributedTxExtension() && serviceManager.WALManager != nil {
				// Multi-shard UPDATE with 2PC: execute UPDATE on each shard under shared WAL txID
				dtxExt := reg.GetDistributedTxExtension()
				sort.Strings(shardBundles)

				sessionID := ""
				if session != nil {
					sessionID = session.SessionID
				}
				dtxID := dtxExt.TrackBegin(sessionID, shardBundles)

				txIDUint64, txErr := serviceManager.WALManager.BeginTransactionUint64()
				if txErr != nil {
					dtxExt.TrackAbort(dtxID)
					return nil, errors.WrapWithMessage(txErr, errors.ERR_INTERNAL_QUERY,
						"failed to begin distributed transaction for UPDATE", errors.LayerCommand).WithContext("bundle", bundleName)
				}
				txIDStr := fmt.Sprintf("%016x", txIDUint64)
				_ = txIDStr // WAL txID used for shared transaction scope

				var totalUpdated int
				var firstErr error
				for _, shardName := range shardBundles {
					shardBundle, shardErr := serviceManager.BundleService.GetBundleByName(database, shardName)
					if shardErr != nil {
						firstErr = shardErr
						break
					}
					shardCmd := &models.DocumentUpdateCommand{
						BundleName:  shardName,
						Fields:      docCommand.Fields,
						WhereClause: docCommand.WhereClause,
						Confirmed:   docCommand.Confirmed,
					}
					whereService := bndle.NewWhereFilterService(serviceManager.BundleService, logger)
					shardDocIDs, _, filterErr := whereService.GetDocumentsAndIDsByFilter(shardBundle, shardCmd.WhereClause)
					if filterErr != nil {
						firstErr = filterErr
						break
					}
					if len(shardDocIDs) == 0 {
						continue
					}
					updErr := serviceManager.BundleService.UpdateDocumentInBundle(context.Background(), database, shardBundle, shardCmd)
					if updErr != nil {
						firstErr = updErr
						break
					}
					totalUpdated += len(shardDocIDs)
				}

				if firstErr != nil {
					_ = serviceManager.WALManager.RollbackTransaction(txIDStr)
					dtxExt.TrackAbort(dtxID)
					return nil, errors.WrapWithMessage(firstErr, errors.ERR_INTERNAL_QUERY,
						"distributed transaction aborted for UPDATE", errors.LayerCommand).WithContext("bundle", bundleName)
				}

				dtxExt.TrackPrepared(dtxID)
				if commitErr := serviceManager.WALManager.CommitTransaction(txIDStr, nil); commitErr != nil {
					_ = serviceManager.WALManager.RollbackTransaction(txIDStr)
					dtxExt.TrackAbort(dtxID)
					return nil, errors.WrapWithMessage(commitErr, errors.ERR_INTERNAL_QUERY,
						"distributed transaction commit failed for UPDATE", errors.LayerCommand).WithContext("bundle", bundleName)
				}
				dtxExt.TrackCommit(dtxID)

				logger.Debugf("Sharding: distributed UPDATE across %d shards, %d docs updated", len(shardBundles), totalUpdated)
				return &CommandResponse{
					ResultCount:     totalUpdated,
					Result:          fmt.Sprintf("{\"documents_updated\": %d, \"bundle\": \"%s\", \"shards_used\": %d}", totalUpdated, bundleName, len(shardBundles)),
					ExecutionTimeMS: float64(time.Since(startTime).Nanoseconds()) / 1e6,
				}, nil
			}
			// Multi-shard updates without dtx extension fall through to scatter via normal path
			// (WHERE filtering will handle correctness)
		}
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

		// Enterprise index extension hook: notify indexes of updated document
		if reg := extension.GetRegistry(); len(reg.GetIndexExtensions()) > 0 {
			docMap := keyValuesToMap(docCommand.Fields)
			for _, idx := range reg.GetIndexExtensions() {
				if notifyErr := idx.OnDocumentChange(context.Background(), bundleName, "", "UPDATE", docMap); notifyErr != nil {
					logger.Warnf("Index extension OnDocumentChange error (UPDATE): %v", notifyErr)
				}
			}
		}

		// Enterprise CDC + MatView hooks
		notifyCDCAndMatView(context.Background(), "UPDATE", bundleName, "", nil, keyValuesToMap(docCommand.Fields), logger)

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
	// For READ COMMITTED, create a fresh snapshot so WHERE clause sees latest committed data
	ctx := context.Background()
	if session.IsReadCommitted() {
		ctx = createReadCommittedSnapshot(ctx, session, serviceManager, logger)
	} else if snap := session.GetMVCCSnapshot(); snap != nil {
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

	// Enterprise index extension hook: notify indexes of updated document (pessimistic path)
	if reg := extension.GetRegistry(); len(reg.GetIndexExtensions()) > 0 {
		docMap := keyValuesToMap(docCommand.Fields)
		for _, idx := range reg.GetIndexExtensions() {
			if notifyErr := idx.OnDocumentChange(context.Background(), bundleName, "", "UPDATE", docMap); notifyErr != nil {
				logger.Warnf("Index extension OnDocumentChange error (UPDATE): %v", notifyErr)
			}
		}
	}

	// Enterprise CDC + MatView hooks (pessimistic path)
	notifyCDCAndMatView(context.Background(), "UPDATE", bundleName, "", nil, keyValuesToMap(docCommand.Fields), logger)

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

	// Enterprise sharding hook: route INSERT to correct shard sub-bundle
	if reg := extension.GetRegistry(); reg.HasShardingExtension() {
		shardExt := reg.GetShardingExtension()
		if shardExt.IsShardedBundle(bundleName) {
			docMap := keyValuesToMap(docCommand.Fields)
			shardBundleName, shardErr := shardExt.ResolveWriteShard(bundleName, docMap)
			if shardErr != nil {
				return nil, errors.WrapWithMessage(shardErr, errors.ERR_INTERNAL_QUERY,
					"failed to resolve shard for insert", errors.LayerCommand).WithContext("bundle", bundleName)
			}
			bundle, err = serviceManager.BundleService.GetBundleByName(database, shardBundleName)
			if err != nil {
				return nil, errors.ConvertError(err, errors.LayerCommand).WithContext("bundle", shardBundleName)
			}
			bundleName = shardBundleName
			logger.Debugf("Sharding: routed INSERT to shard bundle '%s'", shardBundleName)
		}
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

	// Enterprise index extension hook: notify indexes of new document
	if reg := extension.GetRegistry(); len(reg.GetIndexExtensions()) > 0 {
		docMap := keyValuesToMap(docCommand.Fields)
		for _, idx := range reg.GetIndexExtensions() {
			if notifyErr := idx.OnDocumentChange(context.Background(), bundleName, docID, "INSERT", docMap); notifyErr != nil {
				logger.Warnf("Index extension OnDocumentChange error (INSERT): %v", notifyErr)
			}
		}
	}

	// Enterprise CDC + MatView hooks
	notifyCDCAndMatView(context.Background(), "INSERT", bundleName, docID, nil, keyValuesToMap(docCommand.Fields), logger)

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

	// Enterprise sharding hook: for sharded bundles, group docs by shard and insert per-shard
	if reg := extension.GetRegistry(); reg.HasShardingExtension() {
		shardExt := reg.GetShardingExtension()
		if shardExt.IsShardedBundle(bundleName) {
			// Group documents by target shard
			shardGroups := make(map[string][][]models.KeyValue)
			for _, docFields := range bulkCmd.Documents {
				docMap := keyValuesToMap(docFields)
				shardName, shardErr := shardExt.ResolveWriteShard(bundleName, docMap)
				if shardErr != nil {
					return nil, errors.WrapWithMessage(shardErr, errors.ERR_INTERNAL_QUERY,
						"failed to resolve shard for bulk insert", errors.LayerCommand).WithContext("bundle", bundleName)
				}
				shardGroups[shardName] = append(shardGroups[shardName], docFields)
			}

			// Execute per-shard bulk inserts
			var allDocIDs []string

			// Enterprise 2PC hook: when multiple shards are involved and distributed
			// transaction extension is registered, wrap all shard writes in a single
			// shared WAL transaction for cross-shard atomicity.
			if len(shardGroups) > 1 && reg.HasDistributedTxExtension() && serviceManager.WALManager != nil {
				dtxExt := reg.GetDistributedTxExtension()
				shardNames := make([]string, 0, len(shardGroups))
				for name := range shardGroups {
					shardNames = append(shardNames, name)
				}
				sort.Strings(shardNames)

				sessionID := ""
				if session != nil {
					sessionID = session.SessionID
				}
				dtxID := dtxExt.TrackBegin(sessionID, shardNames)

				// Begin shared WAL transaction
				txIDUint64, txErr := serviceManager.WALManager.BeginTransactionUint64()
				if txErr != nil {
					dtxExt.TrackAbort(dtxID)
					return nil, errors.WrapWithMessage(txErr, errors.ERR_INTERNAL_QUERY,
						"failed to begin distributed transaction", errors.LayerCommand).WithContext("bundle", bundleName)
				}
				txIDStr := fmt.Sprintf("%016x", txIDUint64)

				var firstErr error
				for _, shardName := range shardNames {
					shardDocs := shardGroups[shardName]
					shardBundle, shardErr := serviceManager.BundleService.GetBundleByName(database, shardName)
					if shardErr != nil {
						firstErr = shardErr
						break
					}
					shardBulkCmd := &models.BulkDocumentCommand{
						CommandType: bulkCmd.CommandType,
						BundleName:  shardName,
						Documents:   shardDocs,
					}
					logErr := serviceManager.WALManager.LogBatchInsert(txIDStr, shardName, []string{"pending"}, shardDocs)
					if logErr != nil {
						firstErr = logErr
						break
					}
					shardDocIDs, insertErr := serviceManager.BundleService.AddDocumentsToBundle(database, shardBundle, shardBulkCmd)
					if insertErr != nil {
						firstErr = insertErr
						break
					}
					allDocIDs = append(allDocIDs, shardDocIDs...)
				}

				if firstErr != nil {
					_ = serviceManager.WALManager.RollbackTransaction(txIDStr)
					dtxExt.TrackAbort(dtxID)
					return nil, errors.WrapWithMessage(firstErr, errors.ERR_INTERNAL_QUERY,
						"distributed transaction aborted", errors.LayerCommand).WithContext("bundle", bundleName)
				}

				dtxExt.TrackPrepared(dtxID)
				if commitErr := serviceManager.WALManager.CommitTransaction(txIDStr, nil); commitErr != nil {
					_ = serviceManager.WALManager.RollbackTransaction(txIDStr)
					dtxExt.TrackAbort(dtxID)
					return nil, errors.WrapWithMessage(commitErr, errors.ERR_INTERNAL_QUERY,
						"distributed transaction commit failed", errors.LayerCommand).WithContext("bundle", bundleName)
				}
				dtxExt.TrackCommit(dtxID)
			} else {
				// Original per-shard independent path (single shard or no dtx extension)
				for shardName, shardDocs := range shardGroups {
					shardBundle, shardErr := serviceManager.BundleService.GetBundleByName(database, shardName)
					if shardErr != nil {
						return nil, errors.ConvertError(shardErr, errors.LayerCommand).WithContext("bundle", shardName)
					}
					shardBulkCmd := &models.BulkDocumentCommand{
						CommandType: bulkCmd.CommandType,
						BundleName:  shardName,
						Documents:   shardDocs,
					}

					var shardDocIDs []string
					if serviceManager.WALManager != nil {
						walErr := serviceManager.WALManager.ExecuteWithLogging(func(txID string) error {
							logErr := serviceManager.WALManager.LogBatchInsert(txID, shardName, []string{"pending"}, shardDocs)
							if logErr != nil {
								return logErr
							}
							shardDocIDs, shardErr = serviceManager.BundleService.AddDocumentsToBundle(database, shardBundle, shardBulkCmd)
							return shardErr
						})
						if walErr != nil {
							return nil, errors.ConvertError(walErr, errors.LayerCommand).WithContext("bundle", shardName)
						}
					} else {
						shardDocIDs, shardErr = serviceManager.BundleService.AddDocumentsToBundle(database, shardBundle, shardBulkCmd)
						if shardErr != nil {
							return nil, errors.ConvertError(shardErr, errors.LayerCommand).WithContext("bundle", shardName)
						}
					}
					allDocIDs = append(allDocIDs, shardDocIDs...)
				}
			}

			logger.Debugf("Sharding: bulk insert routed %d docs across %d shards", docCount, len(shardGroups))

			// METRICS
			globalMetrics := GetGlobalServerMetrics()
			globalMetrics.DocumentInsertsTotal.Add(uint64(docCount))
			dbMetrics := GetDatabaseMetrics(database.Name)
			dbMetrics.DBDocumentInsertsTotal.Add(uint64(docCount))

			result := fmt.Sprintf("{\"documents_inserted\": %d, \"bundle\": \"%s\", \"shards_used\": %d, \"document_ids\": %v}",
				docCount, bundleName, len(shardGroups), allDocIDs)
			return &CommandResponse{
				ResultCount:     docCount,
				Result:          result,
				ExecutionTimeMS: float64(time.Since(startingTime).Nanoseconds()) / 1e6,
			}, nil
		}
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

// keyValuesToMap converts []models.KeyValue to map[string]interface{} for extension hooks.
func keyValuesToMap(kvs []models.KeyValue) map[string]interface{} {
	m := make(map[string]interface{}, len(kvs))
	for _, kv := range kvs {
		m[kv.Key] = kv.Value
	}
	return m
}
