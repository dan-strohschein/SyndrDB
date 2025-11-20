package server

import (
	"fmt"
	"sort"
	"strings"
	bndle "syndrdb/src/internal/domain/bundle"
	db "syndrdb/src/internal/domain/database"
	"syndrdb/src/internal/domain/document"
	"syndrdb/src/internal/domain/models"
	"syndrdb/src/internal/query/queryparser"
	"syndrdb/src/pkg/common/helpers"
	"time"

	"go.uber.org/zap"
)

func CommandDirector(database *models.Database, serviceManager ServiceManager, command string, logger *zap.SugaredLogger, startTime time.Time, session *Session, clientIP string) (interface{}, error) {
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
		return SelectDocuments(commandParts, serviceManager, database, logger, startTime)
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
				err := serviceManager.BundleService.ApplyFieldChanges(bundle, bndleCommand.Changes)
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

			// Handle relationship changes if present
			if bndleCommand.HasRelationshipCommands {
				//TODO : Don't assume its always a create, it could be to update a relationship
				var RelationshipCommand *models.RelationshipCommand
				var err error

				// Check if it's the new ADD RELATIONSHIP syntax
				if strings.Contains(strings.ToUpper(normalizedCommand), "ADD RELATIONSHIP") {
					RelationshipCommand, err = bndle.ParseAddRelationshipCommand(normalizedCommand)
				} else {
					// Use the old CREATE RELATIONSHIP syntax
					RelationshipCommand, err = bndle.ParseCreateRelationshipCommand(normalizedCommand)
				}

				if err != nil {
					return &result, err
				}
				// CREATE RELATIONSHIP "RELATIONSHIP_NAME"
				// FROM BUNDLE "BUNDLE_NAME"
				// WITH FIELD "<FIELDNAME>"
				// TO BUNDLE "BUNDLE_NAME"
				// WITH FIELD "<FIELDNAME>"
				AddRelationshipToBundle(serviceManager, database, bndleCommand.BundleName, RelationshipCommand)
			}

		case "documents":
			/*
				UPDATE DOCUMENTS IN BUNDLE "BUNDLE_NAME"
				(<FIELDNAME> = <VALUE>, <FIELDNAME> = <VALUE>, ... )
			*/

			result1, err := UpdateDocument(commandParts, serviceManager, database, command, logger)

			return result1, err

		case "user":
			// ParseCreateRelationshipCommand(command)
		default:
			return &result, fmt.Errorf("unknown command format: %s", command)
		}
		return &result, nil
	}

	// Parse GRANT command
	if strings.HasPrefix(commandLower, "grant") {
		return GrantPermission(command, logger, serviceManager)
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

			// PLACEHOLDER: This is where the actual deletion will happen
			// serviceManager.BundleService.RemoveBundle(database, bundleName)
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
			} else {
				// Fallback to direct execution if WAL is not available
				logger.Warn("WAL Manager not available, executing without transaction logging")
				err = serviceManager.BundleService.DeleteDocumentFromBundle(bundle, docCommand, docIDs)
			}

			if err != nil {
				return nil, fmt.Errorf("error deleting document from bundle '%s': %v", bundleName, err)
			}

			// STEP 2: Invalidate query plan cache after data mutation
			if serviceManager.UnifiedPlanner != nil {
				serviceManager.UnifiedPlanner.InvalidatePlanCache()
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
			// ParseCreateRelationshipCommand(command)
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

func SelectDocuments(commandParts []string, serviceManager ServiceManager, database *models.Database, logger *zap.SugaredLogger, startTime time.Time) (interface{}, error) {

	fullCommand := strings.Join(commandParts, " ")

	logger.Debugf("Processing SELECT query: %s", fullCommand)

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

	logger.Debugf("Execution plan created: Cost=%.2f, EstimatedRows=%d, IndexesUsed=%v",
		plan.Cost, plan.EstimatedRows, plan.IndexesUsed)

	// STEP 4: Execute the plan
	documents, err := plan.RootNode.Execute()
	if err != nil {
		return nil, fmt.Errorf("failed to execute query plan: %w", err)
	}

	logger.Debugf("Query executed successfully: Retrieved %d documents", len(documents))

	// PHASE H: For simple SELECT queries, stream documents directly without intermediate transform
	// Only use streaming when:
	// 1. Not a COUNT query (needs aggregation)
	// 2. No ORDER BY (would need sorting which requires materialized slice)
	// Streaming eliminates ~300 allocations by skipping the map[string]interface{} intermediate layer
	useStreaming := !query.IsCountOnly && !query.HasOrderBy()

	var flattenedDocs []map[string]interface{}

	if !useStreaming {
		// Legacy path: Transform to maps for sorting/aggregation
		flattenedDocs = helpers.TransformDocumentsToFlatFormatWithProjection(documents, query.SelectFields)
	}

	// TODO Update the sorting to use a more powerful sorter that can handle different data types
	// If the dev decided to put an order by on a countOnly query, ignore it
	if query.HasOrderBy() && len(flattenedDocs) > 0 && !query.IsCountOnly {
		sort.SliceStable(flattenedDocs, func(i, j int) bool {
			for _, field := range query.OrderBy.Fields {
				val1, exists1 := flattenedDocs[i][field.FieldName]
				val2, exists2 := flattenedDocs[j][field.FieldName]

				// Handle missing fields
				if !exists1 && !exists2 {
					continue
				}
				if !exists1 {
					return field.Direction == queryparser.SortDesc
				}
				if !exists2 {
					return field.Direction == queryparser.SortAsc
				}

				// Compare values
				cmp := compareValuesForSort(val1, val2)
				if cmp != 0 {
					if field.Direction == queryparser.SortAsc {
						return cmp < 0
					}
					return cmp > 0
				}
			}
			return false
		})
	}

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

	// Create response
	cmdResponse := &CommandResponse{
		ResultCount:     resultCount,
		Result:          results,
		ExecutionTimeMS: executionTime,
	}

	// PHASE H: For streaming path, store documents for direct encoding
	if useStreaming {
		cmdResponse.StreamDocuments = documents
		cmdResponse.StreamFields = query.SelectFields
		cmdResponse.ResultCount = len(documents)
	} else {
		// PHASE A: Store pooled maps for cleanup after JSON marshaling (avoids closure allocation)
		if flattenedDocs, ok := results.([]map[string]interface{}); ok {
			cmdResponse.PooledMaps = flattenedDocs
		}
	}

	logger.Debugf("Returning %d documents (execution time: %.2fms)",
		cmdResponse.ResultCount, cmdResponse.ExecutionTimeMS)

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
