package server

import (
	"fmt"
	bndle "syndrdb/src/internal/domain/bundle"
	"syndrdb/src/internal/domain/models"
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

	// TRANSACTION SUPPORT: Acquire write locks for documents being updated when in a transaction
	// This ensures proper isolation and prevents concurrent modifications
	if session != nil && session.IsInTransaction() {
		// OPTIMIZATION: Get document IDs that match the WHERE clause before acquiring locks
		// This prevents holding locks while parsing/filtering documents
		whereService := bndle.NewWhereFilterService(serviceManager.BundleService, logger)
		docIDs, err := whereService.GetDocumentIDsByFilter(bundle, docCommand.WhereClause)
		if err != nil {
			return nil, errors.WrapWithMessage(err, errors.ERR_INTERNAL_QUERY,
				"failed to filter documents by WHERE clause", errors.LayerQuery).WithContext("bundle", bundleName)
		}

		// Acquire write locks for all matching documents
		for _, docID := range docIDs {
			if err := serviceManager.LockManager.AcquireWriteLock(bundleName, docID, session.ActiveTransactionID, session.SessionID); err != nil {
				return nil, errors.WrapWithMessage(err, errors.ERR_INTERNAL_LOCK,
					fmt.Sprintf("failed to acquire write lock for document %s", docID), errors.LayerTransaction).WithContext("document_id", docID)
			}
		}
		logger.Debugf("Acquired write locks for %d documents in transaction %s", len(docIDs), session.ActiveTransactionID)
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
		err = serviceManager.BundleService.UpdateDocumentInBundle(database, bundle, docCommand)
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
	// #region agent log (commented out - performance debugging)
	// logEntry = map[string]interface{}{
	// 	"sessionId":    "debug-session",
	// 	"runId":        "run1",
	// 	"hypothesisId": "B",
	// 	"location":     "document_operations.go:138",
	// 	"message":      "After parse",
	// 	"timestamp":    time.Now().UnixNano() / 1e6,
	// 	"data":         map[string]interface{}{"parseTimeMs": parseTime.Nanoseconds() / 1e6},
	// }
	// if logBytes, err := json.Marshal(logEntry); err == nil {
	// 	if f, err := os.OpenFile("/Users/danstrohschein/Documents/CodeProjects/golang/SyndrDB/.cursor/debug.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644); err == nil {
	// 		f.Write(append(logBytes, '\n'))
	// 		f.Close()
	// 	}
	// }
	// #endregion
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
			// #region agent log (commented out - performance debugging)
			// logEntry := map[string]interface{}{
			// 	"sessionId":    "debug-session",
			// 	"runId":        "run1",
			// 	"hypothesisId": "F",
			// 	"location":     "document_operations.go:199",
			// 	"message":      "After ExecuteWithLogging",
			// 	"timestamp":    time.Now().UnixNano() / 1e6,
			// 	"data":         map[string]interface{}{"walLogTotalTimeMs": walLogTotalTime.Nanoseconds() / 1e6},
			// }
			// if logBytes, err := json.Marshal(logEntry); err == nil {
			// 	if f, err := os.OpenFile("/Users/danstrohschein/Documents/CodeProjects/golang/SyndrDB/.cursor/debug.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644); err == nil {
			// 		f.Write(append(logBytes, '\n'))
			// 		f.Close()
			// 	}
			// }
			// #endregion

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
	// totalTime := time.Since(startTime)
	// #region agent log (commented out - performance debugging)
	// logEntry = map[string]interface{}{
	// 	"sessionId":    "debug-session",
	// 	"runId":        "run1",
	// 	"hypothesisId": "G",
	// 	"location":     "document_operations.go:240",
	// 	"message":      "AddDocument end",
	// 	"timestamp":    time.Now().UnixNano() / 1e6,
	// 	"data":         map[string]interface{}{"totalTimeMs": totalTime.Nanoseconds() / 1e6},
	// }
	// if logBytes, err := json.Marshal(logEntry); err == nil {
	// 	if f, err := os.OpenFile("/Users/danstrohschein/Documents/CodeProjects/golang/SyndrDB/.cursor/debug.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644); err == nil {
	// 		f.Write(append(logBytes, '\n'))
	// 		f.Close()
	// 	}
	// }
	// #endregion
	cmdResponse := &CommandResponse{
		ResultCount: 1,
		Result:      result,
	}
	endingTime := time.Since(startingTime)
	logger.Infof("DEBUG DEBUG :: AddDocument total time: %s", endingTime.String())
	return cmdResponse, nil
}
