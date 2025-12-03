package server

import (
	"context"
	"fmt"
	"strings"
	bndle "syndrdb/src/internal/domain/bundle"
	db "syndrdb/src/internal/domain/database"
	"syndrdb/src/internal/domain/document"
	"syndrdb/src/internal/domain/models"
	"syndrdb/src/internal/query/planner"
	"syndrdb/src/pkg/common/helpers"
	"syndrdb/src/pkg/settings"
	"time"

	"go.uber.org/zap"
)

// Helper function for debugging
func getKeys(m map[string]interface{}) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

func CommandDirector(ctx context.Context, database *models.Database, serviceManager ServiceManager, command string, logger *zap.SugaredLogger, startTime time.Time, session *Session, clientIP string) (interface{}, error) {
	if database == nil {
		// Get the database from the session.
	}

	// Check if this is a GraphQL command first
	if strings.HasPrefix(command, "GRAPHQL::") {
		if serviceManager.GraphQLProcessor == nil {
			return nil, fmt.Errorf("GraphQL is not enabled on this server")
		}
		return serviceManager.GraphQLProcessor.ProcessGraphQLCommand(command, session, clientIP)
	}

	// Input validation for security
	securityConfig := DefaultSecurityConfig()
	if err := ValidateInput(command, "command", securityConfig); err != nil {
		logger.Warnf("Command validation failed: %v", err)
		return nil, fmt.Errorf("invalid command: %v", err)
	}

	// Sanitize input
	command = SanitizeInput(command)
	command = strings.TrimSpace(command)
	command = strings.TrimSuffix(command, ";") // Remove trailing semicolon if present
	commandParts := strings.Split(command, " ")
	result := ""

	// OPTIMIZATION: Compute lowercase version once to avoid 40+ allocations
	commandLower := strings.ToLower(command)

	// EXPLAIN command - must be checked before SELECT to intercept EXPLAIN SELECT
	if strings.HasPrefix(commandLower, "explain") {
		return HandleExplainCommand(ctx, command, database, serviceManager, logger, startTime)
	}

	if strings.HasPrefix(commandLower, "select") {
		// Parse SELECT command
		// Check for SELECT DATABASES (special case for system catalog)
		if len(commandParts) >= 2 && strings.ToLower(commandParts[1]) == "databases" {
			result1, err, shouldReturn := SelectDatabases(commandParts, serviceManager)
			if shouldReturn {
				return result1, err
			}
			return nil, nil
		}

		// All other SELECT queries (including field lists) go through unified parser
		// This handles things like: SELECT field1, field2 FROM bundle
		//               SELECT DOCUMENTS FROM bundle
		//               SELECT COUNT(*) FROM bundle
		//               SELECT * FROM bundle JOIN...
		//               SELECT field1, COUNT(*) FROM bundle GROUP BY field1
		return SelectDocuments(ctx, commandParts, serviceManager, database, logger, startTime, session)
	}

	if strings.HasPrefix(commandLower, "show") {
		// Parse SHOW command
		switch strings.ToLower(commandParts[1]) {
		case "databases":
			return ShowDatabases(command, logger, serviceManager)
		case "bundles":

			return ShowBundles(command, database, logger, serviceManager)
		case "bundle":
			return ShowBundle(command, database, logger, serviceManager)
		case "sessions":
			return ShowSessions(command, logger, serviceManager)
		case "session":
			return ShowSession(command, logger, serviceManager)
		case "users":
			return ShowUsers(command, database, logger, serviceManager)
		case "migrations":
			// SHOW MIGRATIONS FOR "database_name"
			return ShowMigrationsCommand(command, database, logger, serviceManager)
		case "rate":
			if len(commandParts) > 2 && strings.ToLower(commandParts[2]) == "limit" {
				return ShowRateLimit(command, logger, serviceManager)
			}
			return nil, fmt.Errorf("unknown SHOW RATE command: %s", command)
		}
		return nil, fmt.Errorf("unknown SHOW command: %s", command)
	}

	if strings.HasPrefix(commandLower, "invalidate") {
		// Parse INVALIDATE command
		switch strings.ToLower(commandParts[1]) {
		case "session":
			return InvalidateSession(command, logger, serviceManager)
		}
		return nil, fmt.Errorf("unknown INVALIDATE command: %s", command)
	}

	if strings.HasPrefix(commandLower, "create") {

		switch strings.ToLower(commandParts[1]) {
		case "database":
			return CreateDatabase(command, logger, serviceManager, result)
		case "bundle":

			return CreateBundleCommand(command, logger, serviceManager, database, result)
		case "b-index":
			result1, err := CreateBTreeIndex(command, logger, serviceManager, database)
			if err != nil {
				// If there is an error, return it
				return result1, err
			} else {
				return result1, nil
			}
		case "h-index":
			result1, err, shouldReturn := CreateHashIndex(command, logger, serviceManager, database)
			if shouldReturn {
				return result1, err
			}
		case "user":
			// CREATE USER "username" WITH PASSWORD 'password';
			// TODO: Determine debug mode from server configuration
			debugMode := false
			return CreateUserCommand(command, logger, serviceManager, database, debugMode)
		case "role":
			// CREATE ROLE "role_name" [WITH DESCRIPTION "description"];
			return CreateRole(command, logger, serviceManager)
		default:

			return &result, fmt.Errorf("unknown command format: %s", command)
		}
		return &result, nil
	}

	// Parse Add Document command
	if strings.HasPrefix(commandLower, "add") {
		switch strings.ToLower(commandParts[1]) {
		case "document":

			return AddDocument(commandParts, command, logger, serviceManager, database)
		case "user":
			return AddUser(command, logger, serviceManager)
		}
	}

	// Parse UPDATE  command
	if strings.HasPrefix(commandLower, "update") {
		switch strings.ToLower(commandParts[1]) {
		case "database":
			dbCommand, err := db.ParseUpdateDatabaseCommand(command)
			if err != nil {
				return &result, err
			}
			// Execute the database command
			serviceManager.DatabaseService.UpdateDatabase(*dbCommand)
		case "bundle":
			normalizedCommand := helpers.NormalizeCommand(command) // Normalize once
			//logger.Infof("DEBUG COMMAND IS :: %s", normalizedCommand)

			// Check if this is a DROP RELATIONSHIP command - handle it separately without going through new parser
			if strings.Contains(strings.ToUpper(normalizedCommand), "DROP RELATIONSHIP") {
				// Parse DROP RELATIONSHIP command using old parser
				RelationshipCommand, err := bndle.ParseDropRelationshipCommand(normalizedCommand)
				if err != nil {
					return &result, err
				}

				// Get the bundle
				bundle, err := serviceManager.BundleService.GetBundleByName(database, RelationshipCommand.BundleName)
				if err != nil {
					return &result, fmt.Errorf("bundle '%s' not found: %w", RelationshipCommand.BundleName, err)
				}

				// Execute DROP RELATIONSHIP with WAL logging if available
				if serviceManager.WALManager != nil {
					// METRICS: Track transaction begin
					globalMetrics := GetGlobalServerMetrics()
					globalMetrics.TransactionsBegun.Add(1)

					err = serviceManager.WALManager.ExecuteWithLogging(func(txID string) error {
						// Log the relationship drop before execution
						// TODO: Implement WAL replay logic for relationship drops in LogRelationshipDrop method for crash recovery
						err := serviceManager.WALManager.LogRelationshipDrop(txID, RelationshipCommand.BundleName, RelationshipCommand.Name)
						if err != nil {
							return fmt.Errorf("failed to log relationship drop: %w", err)
						}

						// Remove the relationship from the bundle
						return serviceManager.BundleService.RemoveRelationshipFromBundle(bundle, RelationshipCommand.Name)
					})

					// METRICS: Track transaction outcome
					if err != nil {
						globalMetrics.TransactionsRolledBack.Add(1)
						return &result, fmt.Errorf("failed to drop relationship: %w", err)
					} else {
						globalMetrics.TransactionsCommitted.Add(1)
					}
				} else {
					// Fallback to direct execution if WAL is not available
					logger.Warn("WAL Manager not available, executing without transaction logging")
					err = serviceManager.BundleService.RemoveRelationshipFromBundle(bundle, RelationshipCommand.Name)
					if err != nil {
						return &result, fmt.Errorf("failed to drop relationship: %w", err)
					}
				}

				// Build success message
				result = fmt.Sprintf("Relationship '%s' removed from bundle '%s'.", RelationshipCommand.Name, bundle.Name)
				return &result, nil
			}

			// Parse UPDATE BUNDLE using new parser (for field changes, renames, etc.)
			// bndleCommand, err := bndle.ParseUpdateBundleCommand(normalizedCommand, logger)
			// if err != nil {
			// 	return &result, err
			// }

			bndleCommand, err := parseUpdateBundleWithNewParser(normalizedCommand, logger)
			if err != nil {
				return &result, err
			}

			// Get the bundle first if we need to apply changes
			var bundle *models.Bundle
			if bndleCommand.NewBundleName != "" || len(bndleCommand.Changes) > 0 || bndleCommand.HasRelationshipCommands {
				bundle, err = serviceManager.BundleService.GetBundleByName(database, bndleCommand.BundleName)
				if err != nil {
					return &result, fmt.Errorf("bundle '%s' not found: %w", bndleCommand.BundleName, err)
				}
			}

			// Rename bundle if new name is provided
			if bndleCommand.NewBundleName != "" {
				logger.Infof("Renaming bundle '%s' to '%s'", bndleCommand.BundleName, bndleCommand.NewBundleName)
				err := serviceManager.BundleService.RenameBundle(database, bundle, bndleCommand.NewBundleName)
				if err != nil {
					return &result, fmt.Errorf("failed to rename bundle: %w", err)
				}
				logger.Infof("Successfully renamed bundle '%s' to '%s'", bndleCommand.BundleName, bndleCommand.NewBundleName)

				// Update the bundle reference after rename
				bundle, err = serviceManager.BundleService.GetBundleByName(database, bndleCommand.NewBundleName)
				if err != nil {
					return &result, fmt.Errorf("failed to get renamed bundle: %w", err)
				}
				result = fmt.Sprintf("Bundle renamed to '%s' successfully.", bndleCommand.NewBundleName)
			}

			// Apply field changes if present
			if len(bndleCommand.Changes) > 0 {
				logger.Infof("Applying %d field changes to bundle '%s'", len(bndleCommand.Changes), bundle.Name)
				err := serviceManager.BundleService.ApplyFieldChanges(database, bundle, bndleCommand.Changes)
				if err != nil {
					return &result, fmt.Errorf("failed to apply field changes: %w", err)
				}
				logger.Infof("Successfully applied field changes to bundle '%s'", bundle.Name)

				if len(result) > 0 {
					result = fmt.Sprintf("%s Bundle '%s' updated successfully.", result, bundle.Name)
				} else {
					result = fmt.Sprintf("Bundle '%s' updated successfully.", bundle.Name)
				}
			}

			// Handle relationship changes if present (ADD RELATIONSHIP or CREATE RELATIONSHIP)
			if bndleCommand.HasRelationshipCommands {
				var RelationshipCommand *models.RelationshipCommand
				var err error

				if strings.Contains(strings.ToUpper(normalizedCommand), "ADD RELATIONSHIP") {
					// Check if it's the new ADD RELATIONSHIP syntax
					RelationshipCommand, err = bndle.ParseAddRelationshipCommand(normalizedCommand)
					if err != nil {
						return &result, err
					}
					AddRelationshipToBundle(serviceManager, database, bndleCommand.BundleName, RelationshipCommand)
				} else {
					// Use the old CREATE RELATIONSHIP syntax
					RelationshipCommand, err = bndle.ParseCreateRelationshipCommand(normalizedCommand)
					if err != nil {
						return &result, err
					}
					AddRelationshipToBundle(serviceManager, database, bndleCommand.BundleName, RelationshipCommand)
				}
			}

		case "documents":
			/*
				UPDATE DOCUMENTS IN BUNDLE "BUNDLE_NAME"
				(<FIELDNAME> = <VALUE>, <FIELDNAME> = <VALUE>, ... )
			*/

			result1, err := UpdateDocument(commandParts, serviceManager, database, command, logger)

			return result1, err

		case "user":
			// UPDATE USER "username" SET PASSWORD = "new_password" [FORCE];
			return UpdateUser(command, logger, serviceManager)
		case "role":
			// UPDATE ROLE "role_name" SET DESCRIPTION = "new_description" [FORCE];
			return UpdateRole(command, logger, serviceManager)
		default:
			return &result, fmt.Errorf("unknown command format: %s", command)
		}
		return &result, nil
	}

	// Handle ALTER as an alias for UPDATE
	if strings.HasPrefix(commandLower, "alter") {
		if len(commandParts) >= 2 && strings.ToLower(commandParts[1]) == "role" {
			// ALTER ROLE "role_name" SET DESCRIPTION = "new_description" [FORCE];
			return UpdateRole(command, logger, serviceManager)
		}
	}

	// Parse GRANT command
	if strings.HasPrefix(commandLower, "grant") {
		return GrantPermission(command, logger, serviceManager)
	}

	// Parse REVOKE command
	if strings.HasPrefix(commandLower, "revoke") {
		return RevokePermission(command, logger, serviceManager)
	}

	// Parse ATTACH command
	if strings.HasPrefix(commandLower, "attach") {
		// Check if this is ATTACH DATABASE syntax by looking for DATABASE keyword
		if strings.Contains(commandLower, "attach database") {
			return AttachDatabase(command, logger, serviceManager)
		}
		// Default to user attachment for other ATTACH commands
		return AttachUserToDatabase(command, logger, serviceManager)
	}

	// Parse START MIGRATION command
	if strings.HasPrefix(commandLower, "start") {
		if len(commandParts) >= 2 && strings.ToLower(commandParts[1]) == "migration" {
			// START MIGRATION [WITH DESCRIPTION "..."] <commands> COMMIT
			return StartMigrationCommand(command, database, logger, serviceManager)
		}
		return nil, fmt.Errorf("unknown START command: %s", command)
	}

	// Parse APPLY command
	if strings.HasPrefix(commandLower, "apply") {
		if len(commandParts) >= 2 && strings.ToLower(commandParts[1]) == "migration" {
			// APPLY MIGRATION WITH VERSION <number> [FORCE]
			return ApplyMigrationCommand(command, database, logger, serviceManager)
		}
		if len(commandParts) >= 2 && strings.ToLower(commandParts[1]) == "rollback" {
			// APPLY ROLLBACK TO VERSION <number>
			return ApplyRollbackCommand(command, database, logger, serviceManager)
		}
		return nil, fmt.Errorf("unknown APPLY command: %s", command)
	}

	// Parse VALIDATE command
	if strings.HasPrefix(commandLower, "validate") {
		if len(commandParts) >= 2 && strings.ToLower(commandParts[1]) == "migration" {
			// VALIDATE MIGRATION WITH VERSION <number>
			return ValidateMigrationCommand(command, database, logger, serviceManager)
		}
		if len(commandParts) >= 2 && strings.ToLower(commandParts[1]) == "rollback" {
			// VALIDATE ROLLBACK TO VERSION <number>
			return ValidateRollbackCommand(command, database, logger, serviceManager)
		}
		return nil, fmt.Errorf("unknown VALIDATE command: %s", command)
	}

	if strings.HasPrefix(commandLower, "drop") {
		switch strings.ToLower(commandParts[1]) {
		case "database":
			break
			//return DropDatabase(command, logger, serviceManager)
		case "bundle":
			bundleCmd, err := parseDropBundle(command, logger)
			if err != nil {
				return &result, fmt.Errorf("error parsing DROP BUNDLE command: %v", err)
			}

			// Validate bundle name
			bundleName := bundleCmd.BundleName
			if bundleName == "" {
				return &result, fmt.Errorf("bundle name cannot be empty in DROP BUNDLE command")
			}

			// Validate that the bundle exists
			bundle, err := serviceManager.BundleService.GetBundleByName(database, bundleName)
			if err != nil {
				return &result, fmt.Errorf("error retrieving bundle '%s': %v", bundleName, err)
			}

			// Validate that there are no documents in the bundle
			// We will eventually need to add a force option to make this work even with documents
			// but that will also require a more granular permission setup and careful handling
			if bundle.Documents != nil && len(*bundle.Documents) > 0 {
				return &result, fmt.Errorf("bundle '%s' is not empty and cannot be deleted", bundleName)
			}

			return DeleteBundleCommand(bundleCmd, logger, serviceManager, database)
		case "user":
			// DROP USER "username" [FORCE]; (same as DELETE USER)
			return DeleteUser(command, logger, serviceManager)
		case "role":
			// DROP ROLE "role_name" [FORCE]; (same as DELETE ROLE)
			return DeleteRole(command, logger, serviceManager)
		}
	}

	// Parse DELETE  command
	if strings.HasPrefix(commandLower, "delete") {

		switch strings.ToLower(commandParts[1]) {
		case "database":
			dbCommand, err := db.ParseDeleteDatabaseCommand(command)
			if err != nil {
				return &result, err
			}
			// Execute the database command
			serviceManager.DatabaseService.DeleteDatabase(dbCommand.DatabaseName)
		case "bundle":
			// DROP BUNDLE "<BUNDLE_NAME>"
			// Use new parser if feature flag is enabled, fallback to legacy on error

			// TODO This is repeated code, but I want both delete and drop to work
			// the same. This should get wrapped up in a common function
			bundleCmd, err := parseDropBundle(command, logger)
			if err != nil {
				return &result, fmt.Errorf("error parsing DROP BUNDLE command: %v", err)
			}

			// Validate bundle name
			bundleName := bundleCmd.BundleName
			if bundleName == "" {
				return &result, fmt.Errorf("bundle name cannot be empty in DROP BUNDLE command")
			}

			// Validate that the bundle exists
			bundle, err := serviceManager.BundleService.GetBundleByName(database, bundleName)
			if err != nil {
				return &result, fmt.Errorf("error retrieving bundle '%s': %v", bundleName, err)
			}

			// Validate that there are no documents in the bundle
			// We will eventually need to add a force option to make this work even with documents
			// but that will also require a more granular permission setup and careful handling
			if bundle.Documents != nil && len(*bundle.Documents) > 0 {
				return &result, fmt.Errorf("bundle '%s' is not empty and cannot be deleted", bundleName)
			}

			return DeleteBundleCommand(bundleCmd, logger, serviceManager, database)
		case "documents":
			// DELETE DOCUMENTS FROM "<BUNDLE_NAME>" WHERE <WHERE_CLAUSE>
			// Parse the document command first to get bundle name and WHERE clause
			// Use new parser if feature flag is enabled, fallback to legacy on error
			docCommand, err := parseDeleteDocument(command, logger)
			if err != nil {
				return nil, fmt.Errorf("error parsing delete document command: %v", err)
			}

			// Additional validation following SyndrDB defensive programming practices
			if docCommand.BundleName == "" {
				return nil, fmt.Errorf("bundle name cannot be empty in DELETE DOCUMENTS command")
			}

			bundleName := docCommand.BundleName
			bundle, err := serviceManager.BundleService.GetBundleByName(database, bundleName)
			if err != nil {
				return nil, fmt.Errorf("error retrieving bundle '%s': %v", bundleName, err)
			}

			// OPTIMIZATION: Use dedicated WHERE filter service for efficient ID extraction
			// This replaces the previous hacky approach of constructing a SELECT query
			// and extracting IDs from full document results
			whereService := bndle.NewWhereFilterService(serviceManager.BundleService, logger)
			docIDs, err := whereService.GetDocumentIDsByFilter(bundle, docCommand.WhereClause)
			if err != nil {
				return nil, fmt.Errorf("failed to filter documents by WHERE clause: %w", err)
			}

			logger.Infof("WHERE clause filter matched %d documents for deletion", len(docIDs)) // Execute with WAL logging if available
			if serviceManager.WALManager != nil {
				// METRICS: Track transaction begin
				globalMetrics := GetGlobalServerMetrics()
				globalMetrics.TransactionsBegun.Add(1)

				err = serviceManager.WALManager.ExecuteWithLogging(func(txID string) error {
					// Log the document deletion before execution
					// Note: We'll log the where clause as metadata for the deletion
					err := serviceManager.WALManager.LogDocumentDelete(txID, bundleName, "multiple", docCommand.WhereClause)
					if err != nil {
						return fmt.Errorf("failed to log document delete: %w", err)
					}

					// Delete the document from the bundle
					return serviceManager.BundleService.DeleteDocumentFromBundle(bundle, docCommand, docIDs)
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
				err = serviceManager.BundleService.DeleteDocumentFromBundle(bundle, docCommand, docIDs)
			}

			if err != nil {
				return nil, fmt.Errorf("error deleting document from bundle '%s': %v", bundleName, err)
			}

			// METRICS: Track document deletes
			globalMetrics := GetGlobalServerMetrics()
			globalMetrics.DocumentDeletesTotal.Add(uint64(len(docIDs)))
			dbMetrics := GetDatabaseMetrics(database.Name)
			dbMetrics.DBDocumentDeletesTotal.Add(uint64(len(docIDs)))
			bundleMetrics := GetBundleMetrics(database.Name, bundleName)
			bundleMetrics.BundleDocumentsDeleted.Add(uint64(len(docIDs)))
			bundleMetrics.BundleCurrentDocCount.Add(^uint64(len(docIDs) - 1)) // Atomic subtract

			// Track write for plan cache invalidation (MongoDB-style write-threshold)
			if serviceManager.UnifiedPlanner != nil {
				invalidationMgr := serviceManager.UnifiedPlanner.GetInvalidationManager()
				if invalidationMgr != nil {
					invalidationMgr.OnWrite(bundleName, len(docIDs))
				}
			}

			// STEP 6: Format success response with deleted document IDs
			deletedCount := len(docCommand.DeletedDocumentIDs)
			if deletedCount == 0 {
				result = fmt.Sprintf("{\"message\": \"No documents matched the WHERE clause in bundle '%s'\"}", bundleName)
			} else if deletedCount == 1 {
				result = fmt.Sprintf("{\"message\": \"Successfully deleted 1 document from bundle '%s'\", \"deleted_ids\": [\"%s\"]}",
					bundleName, docCommand.DeletedDocumentIDs[0])
			} else {
				// Build JSON array of deleted IDs
				idsJSON := "["
				for i, id := range docCommand.DeletedDocumentIDs {
					if i > 0 {
						idsJSON += ", "
					}
					idsJSON += fmt.Sprintf("\"%s\"", id)
				}
				idsJSON += "]"
				result = fmt.Sprintf("{\"message\": \"Successfully deleted %d documents from bundle '%s'\", \"deleted_ids\": %s}",
					deletedCount, bundleName, idsJSON)
			}
		case "user":
			// DELETE USER "username" [FORCE];
			return DeleteUser(command, logger, serviceManager)
		case "role":
			// DELETE ROLE "role_name" [FORCE];
			return DeleteRole(command, logger, serviceManager)
		default:
			return &result, fmt.Errorf("unknown command format: %s", command)
		}
		return &result, nil
	}

	// Parse CHECKPOINT command
	if strings.HasPrefix(commandLower, "checkpoint") {
		return Checkpoint(command, logger, &serviceManager)
	}

	// Parse BACKUP command
	if strings.HasPrefix(commandLower, "backup") {
		return BackupDatabase(command, logger, &serviceManager)
	}

	// Parse RESTORE command
	if strings.HasPrefix(commandLower, "restore") {
		return RestoreDatabase(command, logger, &serviceManager)
	}

	// Parse LOCK command
	if strings.HasPrefix(commandLower, "lock") {
		return LockDatabaseCommand(command, logger, &serviceManager)
	}

	// Parse UNLOCK command
	if strings.HasPrefix(commandLower, "unlock") {
		return UnlockDatabaseCommand(command, logger, &serviceManager)
	}

	if strings.HasPrefix(commandLower, "use") {
		return UseDatabase(command, logger, serviceManager)
	}

	// Parse RENAME command
	if strings.HasPrefix(commandLower, "rename") {
		if len(commandParts) >= 2 && strings.ToLower(commandParts[1]) == "database" {
			// RENAME DATABASE "old_name" TO "new_name" [FORCE]
			return RenameDatabase(command, logger, serviceManager, session)
		}
		return nil, fmt.Errorf("unknown RENAME command: %s", command)
	}

	return &result, nil
}

// Session management commands

// ShowSessions shows all active sessions
// Syntax: SHOW SESSIONS
// filterDocumentFields filters documents to only include specified fields
// Returns a new map with documents containing only the selected fields
func filterDocumentFields(documents map[string]*models.Document, selectedFields []string, logger *zap.SugaredLogger) map[string]*models.Document {
	if len(selectedFields) == 0 {
		return documents // Return all fields if no specific fields requested
	}

	filteredDocuments := make(map[string]*models.Document)

	for docID, doc := range documents {
		// Create a new document with only the selected fields
		filteredFields := make(map[string]models.Field)

		// Add selected fields if they exist in the original document
		for _, fieldName := range selectedFields {
			if field, exists := doc.Fields[fieldName]; exists {
				filteredFields[fieldName] = field
				logger.Debugf("Including field '%s' for document %s", fieldName, docID)
			} else {
				logger.Debugf("Field '%s' not found in document %s", fieldName, docID)
			}
		}

		// STEP 1: Use document pool to reduce allocations
		// TODO: Option C - Implement reference counting for automatic pool return
		// Create new document with filtered fields
		filteredDoc := document.GetPooledDocument()
		filteredDoc.DocumentID = doc.DocumentID
		filteredDoc.Fields = filteredFields
		filteredDoc.CreatedAt = doc.CreatedAt
		filteredDoc.UpdatedAt = doc.UpdatedAt

		filteredDocuments[docID] = filteredDoc
	}

	logger.Infof("Filtered %d documents to include only fields: %v", len(filteredDocuments), selectedFields)
	return filteredDocuments
}

func SelectDocuments(ctx context.Context, commandParts []string, serviceManager ServiceManager, database *models.Database, logger *zap.SugaredLogger, startTime time.Time, session *Session) (interface{}, error) {

	fullCommand := strings.Join(commandParts, " ")

	logger.Debugf("Processing SELECT query: %s", fullCommand)

	// METRICS: Track query execution
	metrics := GetGlobalServerMetrics()
	metrics.QueryExecutionsTotal.Add(1)

	// STEP 1: Parse the query using parseQuery (respects feature flag, has fallback)
	query, err := ParseQuery(fullCommand, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to parse query: %w", err)
	}

	logger.Debugf("Parsed unified query: WHERE:%v, Type=%s, HasJoin=%v, HasGroupBy=%v, HasOrderBy=%v, HasLimit=%v",
		query.WhereExpression, query.QueryType, query.HasJoin(), query.HasGroupBy(), query.HasOrderBy(), query.HasLimit())

	// STEP 2: Use unified query planner from ServiceManager (with plan caching)
	unifiedPlanner := serviceManager.UnifiedPlanner

	// STEP 3: Create execution plan
	plan, err := unifiedPlanner.CreatePlan(query, database)
	if err != nil {
		return nil, fmt.Errorf("failed to create execution plan: %w", err)
	}

	// METRICS: Track query plan cache performance (cache hit/miss tracking happens in planner)
	// Track query features
	if query.HasJoin() {
		metrics.QueryJoinsTotal.Add(1)
	}
	if query.HasGroupBy() {
		metrics.QueryGroupBysTotal.Add(1)
	}
	if query.HasOrderBy() {
		metrics.QueryOrderBysTotal.Add(1)
	}

	logger.Debugf("Execution plan created: Cost=%.2f, EstimatedRows=%d, IndexesUsed=%v",
		plan.Cost, plan.EstimatedRows, plan.IndexesUsed)

	// STEP 4: Create timeout context and execute the plan
	args := settings.GetSettings()
	isAdmin := false
	if session != nil {
		isAdmin = session.GetIsAdmin()
	}

	timeout := args.GetQueryTimeout(isAdmin)
	var cancel context.CancelFunc
	var timeoutOccurred bool
	var timeoutError error

	// STEP 4.5: Create memory tracker for per-query memory limit (DoS protection)
	memoryLimit := args.GetQueryMemoryLimit(isAdmin)
	memoryTracker := NewMemoryTracker(memoryLimit)
	ctx = WithMemoryTracker(ctx, memoryTracker)
	if timeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()

		// Launch warning goroutine at 80% threshold
		// TODO: Make warning threshold configurable for different deployment scenarios
		go func() {
			warningTime := time.Duration(float64(timeout) * 0.8)
			select {
			case <-time.After(warningTime):
				username := "anonymous"
				if session != nil {
					username = session.Username
				}
				logger.Warnw("Query approaching timeout",
					"query", query.FromBundle,
					"timeout", timeout,
					"elapsed", warningTime,
					"username", username,
				)
			case <-ctx.Done():
				return
			}
		}()
	}

	// Execute the plan with context
	documents, err := plan.RootNode.Execute(ctx)
	if err != nil {
		// Check if error is due to timeout
		if ctx.Err() == context.DeadlineExceeded {
			// METRICS: Track query timeout
			metrics.QueryTimeoutsTotal.Add(1)
			timeoutOccurred = true
			timeoutError = fmt.Errorf("query execution timeout exceeded (%v)", timeout)
			logger.Warnw("Query timed out, returning partial results",
				"timeout", timeout,
				"partial_results", len(documents),
				"query", query.FromBundle,
			)
			// Continue with partial results
		} else if err == planner.ErrMemoryLimitExceeded {
			// METRICS: Track memory limit exceeded
			metrics.QueryMemoryLimitExceeded.Add(1)
			// Memory limit exceeded - record metrics and return error immediately
			// CLEANUP VERIFICATION: When this error occurs, the execution nodes return nil documents
			// and all allocated maps go out of scope for GC cleanup. Document pointers reference
			// bundle cache (not pooled allocations), so no explicit cleanup needed. The empty
			// CommandResponse below has no PooledMaps or PooledDocuments, ensuring sendResult's
			// defer cleanup is safe.
			memoryTracker.RecordMetrics(plan.EstimatedRows)
			errorMsg := memoryTracker.FormatErrorMessage(plan.EstimatedRows)
			logger.Warnw("Query memory limit exceeded",
				"error", errorMsg,
				"query", query.FromBundle,
				"estimated_rows", plan.EstimatedRows,
			)

			// TODO: I could implement graceful degradation with partial results instead of hard error
			return &CommandResponse{
				ResultCount:     0,
				Result:          nil,
				ExecutionTimeMS: float64(time.Since(startTime).Nanoseconds()) / 1e6,
				Error:           &errorMsg,
			}, nil
		} else {
			return nil, fmt.Errorf("failed to execute query plan: %w", err)
		}
	}

	logger.Infof("Query executed successfully: Retrieved %d documents", len(documents))

	// EXPRESSION-ONLY SELECT: Handle queries without FROM clause
	// These return a single synthetic document with expression results in Data map
	// Skip document metadata (DocumentID, CreatedAt, UpdatedAt) and return only expression values
	if query.FromBundle == "" && len(documents) == 1 {
		logger.Infof("EXPRESSION-ONLY SELECT detected: FromBundle='%s', document count=%d", query.FromBundle, len(documents))
		for docID, doc := range documents {
			// Expression results are in doc.Data map - return directly
			logger.Infof("Expression result document ID: %s, Data keys: %v, Data values: %+v", docID, getKeys(doc.Data), doc.Data)
			results := make([]map[string]interface{}, 1)
			results[0] = doc.Data

			executionTime := float64(time.Since(startTime).Nanoseconds()) / 1e6

			return &CommandResponse{
				ResultCount:     1,
				Result:          results,
				ExecutionTimeMS: executionTime,
			}, nil
		}
	}

	// PHASE H: For simple SELECT queries, stream documents directly without intermediate transform
	// Only use streaming when:
	// 1. Not a COUNT query (needs aggregation)
	// 2. No ORDER BY (would need sorting which requires materialized slice)
	// Streaming eliminates ~300 allocations by skipping the map[string]interface{} intermediate layer
	useStreaming := !query.IsCountOnly && !query.HasOrderBy()

	// CRITICAL FIX: For GROUP BY queries, merge SelectFields with aggregate field names
	// Aggregate fields (MIN, MAX, COUNT, SUM, AVG) are not in SelectFields but need to be included in results
	selectedFields := query.SelectFields
	if query.HasGroupBy() && len(query.AggregateFields) > 0 {
		// Build list of all fields to include: GROUP BY fields + aggregate result fields
		fieldsToInclude := make([]string, 0, len(query.SelectFields)+len(query.AggregateFields))
		fieldsToInclude = append(fieldsToInclude, query.SelectFields...)

		// Add aggregate field names (use Alias if present, otherwise generate from Function_Field)
		// CRITICAL: Use same logic as aggregation_node.go:getAggregateKey()
		for _, aggField := range query.AggregateFields {
			aggFieldName := aggField.Alias
			if aggFieldName == "" {
				// Generate field name matching aggregation_node.go logic
				if aggField.Field == "*" {
					// COUNT(*) -> count_all
					aggFieldName = strings.ToLower(aggField.Function) + "_all"
				} else {
					// MIN(field) -> min_field, MAX(field) -> max_field, etc.
					aggFieldName = strings.ToLower(aggField.Function) + "_" + aggField.Field
				}
			}
			fieldsToInclude = append(fieldsToInclude, aggFieldName)
		}

		selectedFields = fieldsToInclude
		logger.Debugf("GROUP BY: Including %d fields in projection: %v", len(selectedFields), selectedFields)
	}

	var flattenedDocs []map[string]interface{}

	if !useStreaming {
		// BUSINESS RULE: Every query has a SortNode (user ORDER BY or default CreatedAt ASC)
		// Get sorted documents from SortNode to preserve sort order
		var sortedDocs []*models.Document

		// Only extract sorted docs if RootNode is DIRECTLY a SortNode (no LimitNode wrapper)
		// When LimitNode wraps SortNode, don't extract - would bypass the LIMIT!
		if sn, ok := plan.RootNode.(*planner.SortNode); ok {
			sortedDocs = sn.GetSortedDocuments()
		}

		if sortedDocs != nil && len(sortedDocs) > 0 {
			// Use order-preserving transform for sorted documents
			// This respects the sort order from the planner (user ORDER BY or default CreatedAt)
			flattenedDocs = helpers.TransformSortedDocumentsToFlatFormatWithProjection(sortedDocs, selectedFields)
		} else {
			// Fallback: SortNode not found or didn't populate sortedDocs
			// This shouldn't happen with the new planner design, but keep for safety
			logger.Warn("SortNode not found in plan tree or sortedDocs empty, using fallback transform")
			flattenedDocs = helpers.TransformDocumentsToFlatFormatWithProjection(documents, selectedFields)
		}
	} // NOTE: Sorting is handled by the SortNode in the unified query planner execution tree.
	// This legacy sort code was causing a bug where documents were re-sorted AFTER the
	// SortNode had already correctly ordered them, destroying the proper sort order.
	// The SortNode uses FieldValue type-aware comparisons, which is more accurate than
	// this interface{} comparison approach. Do not re-enable this code.

	var results interface{}
	var resultCount int

	if useStreaming {
		// Streaming path: no intermediate results, will encode directly
		results = nil
		resultCount = len(documents)
	} else if query.IsCountOnly {
		// Extract the actual COUNT(*) value from the aggregate result
		// The GROUP BY executor returns 1 document with the count_all field
		countValue := 0
		if len(flattenedDocs) > 0 {
			// Get the first (and only) result document
			firstDoc := flattenedDocs[0]
			// Look for count_all field (created by GROUP BY executor for COUNT(*))
			if count, exists := firstDoc["count_all"]; exists {
				switch v := count.(type) {
				case int64:
					countValue = int(v)
				case int:
					countValue = v
				case float64:
					countValue = int(v)
				case models.FieldValue:
					// ✅ Handle FieldValue from response formatter
					if intVal, ok := v.AsInt(); ok {
						countValue = int(intVal)
					} else if floatVal, ok := v.AsFloat(); ok {
						countValue = int(floatVal)
					}
				}
			}
		}
		results = map[string]int{"Count": countValue}
		resultCount = 1
	} else {
		results = flattenedDocs
		resultCount = len(flattenedDocs)
	}

	// Calculate execution time
	executionTime := float64(time.Since(startTime).Nanoseconds()) / 1e6

	// METRICS: Record query latency histogram
	latencyMs := executionTime
	if latencyMs < 1 {
		metrics.QueryLatencyLt1ms.Add(1)
	} else if latencyMs < 10 {
		metrics.QueryLatencyLt10ms.Add(1)
	} else if latencyMs < 100 {
		metrics.QueryLatencyLt100ms.Add(1)
	} else if latencyMs < 1000 {
		metrics.QueryLatencyLt1s.Add(1)
	} else {
		metrics.QueryLatencyGte1s.Add(1)
	}

	// Create response
	cmdResponse := &CommandResponse{
		ResultCount:     resultCount,
		Result:          results,
		ExecutionTimeMS: executionTime,
	}

	// Add timeout information if timeout occurred
	if timeoutOccurred {
		cmdResponse.TimeoutOccurred = true
		errorMsg := timeoutError.Error()
		cmdResponse.Error = &errorMsg
	}

	// PHASE H: For streaming path, store documents for direct encoding
	if useStreaming {
		cmdResponse.StreamDocuments = documents
		cmdResponse.StreamFields = selectedFields // Use merged fields (includes aggregates)
		cmdResponse.ResultCount = len(documents)

		// CRITICAL FIX: For direct function calls (E2E tests), convert StreamDocuments to Result
		// HTTP responses use StreamDocuments for efficient streaming, but direct callers expect Result
		// This ensures backward compatibility with existing tests while maintaining streaming optimization
		cmdResponse.Result = helpers.TransformDocumentsToFlatFormatWithProjection(documents, selectedFields)
	} else {
		// PHASE A: Store pooled maps for cleanup after JSON marshaling (avoids closure allocation)
		if flattenedDocs, ok := results.([]map[string]interface{}); ok {
			cmdResponse.PooledMaps = flattenedDocs
		}
	}

	logger.Debugf("Returning %d documents (execution time: %.2fms)",
		cmdResponse.ResultCount, cmdResponse.ExecutionTimeMS)

	// Record memory tracking metrics for successful queries
	memoryTracker.RecordMetrics(cmdResponse.ResultCount)

	return cmdResponse, nil

}

func SelectDatabases(commandParts []string, serviceManager ServiceManager) (*CommandResponse, error, bool) {
	if len(commandParts) < 3 {
		return nil, fmt.Errorf("SELECT DATABASES requires the spec 'FROM Default'"), false
	}
	if strings.EqualFold(commandParts[3], "DEFAULT") {
		databases := serviceManager.DatabaseService.ListDatabases()

		if len(databases) == 0 {
			//fmt.Print("No databases found.\n")
			databases = make([]*models.Database, 0, 10)
		}

		cmdResponse := &CommandResponse{
			ResultCount: len(databases),
			Result:      databases,
		}
		return cmdResponse, nil, true
	}
	return nil, nil, false
}
