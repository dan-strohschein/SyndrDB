package server

import (
	"fmt"
	bndle "syndrdb/src/internal/domain/bundle"
	"syndrdb/src/internal/domain/models"

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
		return nil, fmt.Errorf("UPDATE DOCUMENTS command parsing failed: %w", err)
	}

	// Additional validation following SyndrDB defensive programming practices
	if bundleName == "" {
		return nil, fmt.Errorf("bundle name cannot be empty in UPDATE DOCUMENTS command")
	}

	// Parse the document command using new parser with feature flag support
	// This will attempt new parser if enabled, fallback to legacy parser on failure
	docCommand, err := parseUpdateDocument(command, logger)
	if err != nil {
		return nil, fmt.Errorf("error parsing update document command: %v", err)
	}

	// SAFETY: Validate CONFIRMED keyword requirement for bulk updates without WHERE clause
	if docCommand.WhereClause == "" && !docCommand.Confirmed {
		return nil, fmt.Errorf("bulk UPDATE without WHERE clause requires CONFIRMED keyword. Use: UPDATE DOCUMENTS IN BUNDLE \"%s\" (...) CONFIRMED", bundleName)
	}

	// Get the bundle by name
	bundle, err := serviceManager.BundleService.GetBundleByName(database, bundleName)
	if err != nil {
		return nil, fmt.Errorf("error retrieving bundle '%s': %v", bundleName, err)
	}

	// TRANSACTION SUPPORT: Acquire write locks for documents being updated when in a transaction
	// This ensures proper isolation and prevents concurrent modifications
	if session != nil && session.IsInTransaction() {
		// OPTIMIZATION: Get document IDs that match the WHERE clause before acquiring locks
		// This prevents holding locks while parsing/filtering documents
		whereService := bndle.NewWhereFilterService(serviceManager.BundleService, logger)
		docIDs, err := whereService.GetDocumentIDsByFilter(bundle, docCommand.WhereClause)
		if err != nil {
			return nil, fmt.Errorf("failed to filter documents by WHERE clause: %w", err)
		}

		// Acquire write locks for all matching documents
		for _, docID := range docIDs {
			if err := serviceManager.LockManager.AcquireWriteLock(bundleName, docID, session.ActiveTransactionID, session.SessionID); err != nil {
				return nil, fmt.Errorf("failed to acquire write lock for document %s: %w", docID, err)
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
				return fmt.Errorf("failed to log document update: %w", err)
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
		return nil, fmt.Errorf("error updating document in bundle '%s': %v", bundleName, err)
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
	logger.Debugf("Trying to add document to %s.%s", database.Name, commandParts[3])

	if len(commandParts) < 4 {
		return nil, fmt.Errorf("ADD DOCUMENT requires the spec 'TO <bundle_name>'")
	}

	// Parse the document command using new parser with fallback
	// This uses the same feature flag and fallback pattern as SELECT queries
	docCommand, err := parseAddDocument(command, logger)
	if err != nil {
		return nil, fmt.Errorf("error parsing add document command: %v", err)
	}

	bundleName := docCommand.BundleName
	// Get the bundle by name
	bundle, err := serviceManager.BundleService.GetBundleByName(database, docCommand.BundleName)
	if err != nil {
		return nil, fmt.Errorf("error retrieving bundle '%s': %v", bundleName, err)
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
			return nil, fmt.Errorf("failed to acquire bundle lock for insert: %w", err)
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
				return nil, fmt.Errorf("error adding document to bundle '%s': %v", bundleName, err)
			}

			// Log the insertion to the WAL
			err = serviceManager.WALManager.LogDocumentInsert(txID, bundleName, docID, docCommand.Fields)
			if err != nil {
				return nil, fmt.Errorf("failed to log document insert: %w", err)
			}

			logger.Infof("TRANSACTION INSERT LOGGED: txID=%s, bundle=%s, docID=%s", txID, bundleName, docID)
		} else {
			// Auto-commit transaction: use ExecuteWithLogging wrapper
			globalMetrics := GetGlobalServerMetrics()
			globalMetrics.TransactionsBegun.Add(1)

			err = serviceManager.WALManager.ExecuteWithLogging(func(txID string) error {
				// Log the document insertion before execution
				// Note: Document ID will be generated during bundle service execution
				err := serviceManager.WALManager.LogDocumentInsert(txID, bundleName, "pending", docCommand.Fields)
				if err != nil {
					return fmt.Errorf("failed to log document insert: %w", err)
				}

				// Add the document to the bundle
				docID, err = serviceManager.BundleService.AddDocumentToBundle(database, bundle, docCommand)
				return err
			})

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
		return nil, fmt.Errorf("error adding document to bundle '%s': %v", bundleName, err)
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
	return cmdResponse, nil
}
