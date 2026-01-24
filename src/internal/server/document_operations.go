package server

import (
	"fmt"
	"sort"
	"strings"
	bndle "syndrdb/src/internal/domain/bundle"
	"syndrdb/src/internal/domain/models"
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
	
	// Get document IDs using query planner (same fast path as SELECT)
	if docCommand.WhereClause != "" && strings.TrimSpace(docCommand.WhereClause) != "" {
		// Use query planner to get document IDs (Task 1 implementation)
		// For now, use WhereFilterService as fallback until query planner integration is complete
		whereService := bndle.NewWhereFilterService(serviceManager.BundleService, logger)
		docIDs, err = whereService.GetDocumentIDsByFilter(bundle, docCommand.WhereClause)
		if err != nil {
			return nil, errors.WrapWithMessage(err, errors.ERR_INTERNAL_QUERY,
				"failed to filter documents by WHERE clause", errors.LayerQuery).WithContext("bundle", bundleName)
		}
	} else {
		// Empty WHERE clause - will need bundle lock (bulk update)
		docIDs = []string{}
	}

	// TASK 2: Acquire document-level locks if we have document IDs
	// DOCUMENT-LEVEL LOCKING: Extended to autocommit operations for better concurrent write throughput
	// For explicit transactions, locks are held until commit/rollback
	// For autocommit, locks are released after the operation completes
	const lockEscalationThreshold = 1000 // Increased from 100 to handle larger category queries
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
	} else if len(docIDs) > 0 {
		// OPTIMIZATION: Even without document locks, pass docIDs to avoid double WHERE scan
		// This allows UpdateDocumentInBundle to fetch documents by ID instead of re-running WHERE
		lockInfo = &bndle.DocumentLockInfo{
			LockManager:  nil, // No actual locks acquired
			TxID:         "",
			SessionID:    "",
			LockedDocIDs: docIDs, // Pass IDs for direct document fetch
		}
		logger.Debugf("Passing %d document IDs to UpdateDocumentInBundle (no locks, count > threshold)", len(docIDs))
	}

	// Execute with WAL logging if available
	if serviceManager.WALManager != nil {
		// METRICS: Track transaction begin
		globalMetrics := GetGlobalServerMetrics()
		globalMetrics.TransactionsBegun.Add(1)

		err = serviceManager.WALManager.ExecuteWithLogging(func(txID string) error {
			// Log the document update before execution
			// Note: We'll log the fields being updated, actual before/after data is captured by bundle service
			err := serviceManager.WALManager.LogDocumentUpdate(txID, bundleName, "multiple", nil, docCommand.Fields)
			if err != nil {
				return errors.WrapWithMessage(err, errors.ERR_INTERNAL_WAL,
					"failed to log document update", errors.LayerWAL).WithContext("bundle", bundleName)
			}

			// Update the document in the bundle
			// TASK 2: Pass lock info if document locks were acquired
			if lockInfo != nil {
				return serviceManager.BundleService.UpdateDocumentInBundle(database, bundle, docCommand, lockInfo)
			}
			return serviceManager.BundleService.UpdateDocumentInBundle(database, bundle, docCommand)
		})

		// METRICS: Track transaction outcome
		if err != nil {
			globalMetrics.TransactionsRolledBack.Add(1)
		} else {
			globalMetrics.TransactionsCommitted.Add(1)
		}
	} else {
		// Fallback to direct execution if WAL is not available
		logger.Warn("WAL Manager not available, executing without transaction logging")
		if lockInfo != nil {
			err = serviceManager.BundleService.UpdateDocumentInBundle(database, bundle, docCommand, lockInfo)
		} else {
			err = serviceManager.BundleService.UpdateDocumentInBundle(database, bundle, docCommand)
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

	// TRANSACTION SUPPORT: For INSERT operations in transactions, we need to handle locking differently
	// Since the document ID doesn't exist yet, we'll acquire a bundle-level write lock
	// to prevent concurrent inserts from conflicting. The actual document lock will be
	// acquired after the ID is generated during bundle service execution.
	if session != nil && session.IsInTransaction() {
		// Acquire a bundle-level lock using a special convention: "bundle:<bundle_name>"
		bundleLockID := fmt.Sprintf("bundle:%s", bundleName)
		if err := serviceManager.LockManager.AcquireWriteLock(bundleName, bundleLockID, session.ActiveTransactionID, session.SessionID); err != nil {
			return nil, errors.WrapWithMessage(err, errors.ERR_INTERNAL_LOCK,
				"failed to acquire bundle lock for insert", errors.LayerTransaction).WithContext("bundle", bundleName)
		}
		logger.Debugf("Acquired bundle lock for insert in transaction %s", session.ActiveTransactionID)
	}

	// Execute with WAL logging
	if serviceManager.WALManager != nil {
		// Check if we're in an explicit transaction
		if session != nil && session.IsInTransaction() {
			// Within explicit transaction: log to existing transaction
			txID := session.ActiveTransactionID

			logger.Infof("TRANSACTION INSERT: txID=%s, bundle=%s, session=%s", txID, bundleName, session.SessionID)

			// Add document with transaction ID tracking (buffer-aware)
			docID, err = serviceManager.BundleService.AddDocumentToBundleWithTxID(database, bundle, docCommand, txID)
			if err != nil {
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
