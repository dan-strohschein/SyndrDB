package server

import (
	"fmt"
	"path/filepath"
	"strings"
	"syndrdb/src/internal/domain/bundle"
	bndle "syndrdb/src/internal/domain/bundle"
	db "syndrdb/src/internal/domain/database"
	"syndrdb/src/internal/domain/index"
	"syndrdb/src/internal/domain/models"
	"syndrdb/src/internal/query/planner"
	"syndrdb/src/internal/query/queryparser"
	"syndrdb/src/pkg/common/helpers"
	"syndrdb/src/pkg/settings"

	"go.uber.org/zap"
)

func CommandDirector(database *models.Database, serviceManager ServiceManager, command string, logger *zap.SugaredLogger) (interface{}, error) {
	command = strings.TrimSpace(command)
	command = strings.TrimSuffix(command, ";") // Remove trailing semicolon if present
	commandParts := strings.Split(command, " ")
	result := ""

	if strings.HasPrefix(strings.ToLower(command), "select") {
		// Parse SELECT command
		//dbCommand, err := engine.ParseSelectCommand(command)
		switch strings.ToLower(commandParts[1]) {
		case "databases":
			result1, err, shouldReturn := SelectDatabases(commandParts, serviceManager)
			if shouldReturn {
				return result1, err
			}

		case "documents":

			return SelectDocuments(commandParts, serviceManager, database, logger)
		}
		return nil, nil
	}

	if strings.HasPrefix(strings.ToLower(command), "create") {

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
			// ParseCreateRelationshipCommand(command)
		default:

			return &result, fmt.Errorf("unknown command format: %s", command)
		}
		return &result, nil
	}

	// Parse Add Document command
	if strings.HasPrefix(strings.ToLower(command), "add") {
		switch strings.ToLower(commandParts[1]) {
		case "document":

			return AddDocument(commandParts, command, logger, serviceManager, database)
		}
	}

	// Parse UPDATE  command
	if strings.HasPrefix(strings.ToLower(command), "update") {
		switch strings.ToLower(commandParts[1]) {
		case "database":
			dbCommand, err := db.ParseUpdateDatabaseCommand(command)
			if err != nil {
				return &result, err
			}
			// Execute the database command
			serviceManager.DatabaseService.UpdateDatabase(*dbCommand)
		case "bundle":
			bndleCommand, err := bndle.ParseUpdateBundleCommand(command)
			if err != nil {
				return &result, err
			}
			if bndleCommand.HasRelationshipCommands {
				//TODO : Don't assume its always a create, it could be to update a relationship
				var RelationshipCommand *models.RelationshipCommand
				var err error

				// Check if it's the new ADD RELATIONSHIP syntax
				if strings.Contains(strings.ToUpper(command), "ADD RELATIONSHIP") {
					RelationshipCommand, err = bndle.ParseAddRelationshipCommand(command)
				} else {
					// Use the old CREATE RELATIONSHIP syntax
					RelationshipCommand, err = bndle.ParseCreateRelationshipCommand(command)
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

	// Parse DELETE  command
	if strings.HasPrefix(strings.ToLower(command), "delete") {

		switch strings.ToLower(commandParts[1]) {
		case "database":
			dbCommand, err := db.ParseDeleteDatabaseCommand(command)
			if err != nil {
				return &result, err
			}
			// Execute the database command
			serviceManager.DatabaseService.DeleteDatabase(dbCommand.DatabaseName)
		case "bundle":
			bundleName, err := parseBundleNameFromCommand(command, "DELETE")
			if err != nil {
				return &result, err
			}

			//Validate that there are no documents in the bundle
			bundle, err := serviceManager.BundleService.GetBundleByName(database, bundleName)
			if err != nil {
				return &result, err
			}

			if bundle.Documents != nil && len(*bundle.Documents) > 0 {
				return &result, fmt.Errorf("bundle '%s' is not empty and cannot be deleted", bundleName)
			}

			serviceManager.BundleService.RemoveBundle(database, bundleName)
		case "documents":
			//DELETE DOCUMENTS FROM BUNDLE "BUNDLE_NAME"
			//WHERE <FIELDNAME> = <VALUE>
			bundleName, err := parseBundleNameFromCommand(command, "FROM")
			if err != nil {
				logger.Errorf("Failed to parse bundle name from SELECT command: %v", err)
				logger.Debugf("Command was: %s", command)
				return nil, fmt.Errorf("SELECT DOCUMENTS command parsing failed: %w", err)
			}

			// Additional validation following SyndrDB defensive programming practices
			if bundleName == "" {
				return nil, fmt.Errorf("bundle name cannot be empty in SELECT DOCUMENTS command")
			}

			// Additional validation following SyndrDB defensive programming practices
			if bundleName == "" {
				return nil, fmt.Errorf("bundle name cannot be empty in UPDATE DOCUMENTS command")
			}
			// Parse the document command
			docCommand, err := bndle.ParseDeleteDocumentCommand(command, logger)
			if err != nil {
				return nil, fmt.Errorf("error parsing delete document command: %v", err)
			}
			bundle, err := serviceManager.BundleService.GetBundleByName(database, bundleName)
			if err != nil {
				return nil, fmt.Errorf("error retrieving bundle '%s': %v", bundleName, err)
			}

			// Delete the document from the bundle
			err = serviceManager.BundleService.DeleteDocumentFromBundle(bundle, docCommand)
			if err != nil {
				return nil, fmt.Errorf("error deleting document from bundle '%s': %v", bundleName, err)
			}
		case "user":
			// ParseCreateRelationshipCommand(command)
		default:
			return &result, fmt.Errorf("unknown command format: %s", command)
		}
		return &result, nil
	}

	return &result, nil
}

func AddRelationshipToBundle(serviceManager ServiceManager, database *models.Database, bundleName string, relationshipCommand *models.RelationshipCommand) (*CommandResponse, error) {
	bundle, err := serviceManager.BundleService.GetBundleByName(database, bundleName)
	if err != nil {
		return nil, fmt.Errorf("error retrieving bundle '%s': %v", bundleName, err)
	}

	err = serviceManager.BundleService.AddRelationshipToBundle(bundle, relationshipCommand)
	if err != nil {
		return nil, fmt.Errorf("error adding relationship to bundle '%s': %v", bundleName, err)
	}

	cmdResponse := &CommandResponse{
		ResultCount: 1,
		Result:      "Relationship successfully added to bundle '" + bundleName + "'.",
	}
	return cmdResponse, nil
}

func UpdateDocument(commandParts []string, serviceManager ServiceManager, database *models.Database, command string, logger *zap.SugaredLogger) (*CommandResponse, error) {
	// if len(commandParts) < 5 || !strings.EqualFold(commandParts[2], "IN") {
	// 	return nil, fmt.Errorf("UPDATE DOCUMENTS requires the spec 'IN <Bundle_name>'")
	// }
	// bundleName := strings.Trim(commandParts[4], "\"'")
	// bundleName = strings.ReplaceAll(bundleName, "\"", "")
	// bundleName = strings.ReplaceAll(bundleName, "'", "")
	// bundleName = strings.ReplaceAll(bundleName, "”", "") // A very odd type of quote that can appear in text

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

	// Get the bundle by name
	bundle, err := serviceManager.BundleService.GetBundleByName(database, bundleName)
	if err != nil {
		return nil, fmt.Errorf("error retrieving bundle '%s': %v", bundleName, err)
	}

	// Parse the document command
	docCommand, err := bndle.ParseUpdateDocumentCommand(command, logger)
	if err != nil {
		return nil, fmt.Errorf("error parsing update document command: %v", err)
	}

	// Delete the document from the bundle
	err = serviceManager.BundleService.UpdateDocumentInBundle(bundle, docCommand)
	if err != nil {
		return nil, fmt.Errorf("error updating document in bundle '%s': %v", bundleName, err)
	}

	cmdResponse := &CommandResponse{
		ResultCount: 1,
		Result:      "Document updated successfully in bundle '" + bundleName + "'.",
	}
	return cmdResponse, nil
}

func AddDocument(commandParts []string, command string, logger *zap.SugaredLogger, serviceManager ServiceManager, database *models.Database) (*CommandResponse, error) {
	if len(commandParts) < 4 {
		return nil, fmt.Errorf("ADD DOCUMENT requires the spec 'TO <bundle_name>'")
	}

	// Parse the document command
	docCommand, err := bndle.ParseAddDocumentCommand(command, logger)
	if err != nil {
		return nil, fmt.Errorf("error parsing add document command: %v", err)
	}

	bundleName := docCommand.BundleName
	// Get the bundle by name
	bundle, err := serviceManager.BundleService.GetBundleByName(database, docCommand.BundleName)
	if err != nil {
		return nil, fmt.Errorf("error retrieving bundle '%s': %v", bundleName, err)
	}
	// Add the document to the bundle
	err = serviceManager.BundleService.AddDocumentToBundle(database, bundle, docCommand)
	if err != nil {
		return nil, fmt.Errorf("error adding document to bundle '%s': %v", bundleName, err)
	}
	result := fmt.Sprintf("Document added successfully to bundle '%s'.", bundleName)
	cmdResponse := &CommandResponse{
		ResultCount: 1,
		Result:      result,
	}
	return cmdResponse, nil
}

func CreateHashIndex(command string, logger *zap.SugaredLogger, serviceManager ServiceManager, database *models.Database) (*CommandResponse, error, bool) {
	hashIndexCommand, err := index.ParseCreateHashIndexCommand(command, logger)
	if err != nil {
		return nil, fmt.Errorf("error parsing hash index command: %v", err), false
	}
	logger.Infof("Parsed Hash index command: %+v", hashIndexCommand)

	// Get the bundle by name
	bundle, err := serviceManager.BundleService.GetBundleByName(database, hashIndexCommand.BundleName)
	if err == nil {
		return nil, fmt.Errorf("bundle '%s' cannot be found", bundle.Name), false
	}

	err = serviceManager.BundleService.AddIndexToBundle(database, bundle, hashIndexCommand)
	if err != nil {
		return nil, fmt.Errorf("error adding hash index to bundle '%s': %v", hashIndexCommand.BundleName, err), false
	}

	cmdResponse := &CommandResponse{
		ResultCount: 1,
		Result:      "Index created successfully",
	}
	return cmdResponse, nil, true
}

func CreateBTreeIndex(command string, logger *zap.SugaredLogger, serviceManager ServiceManager, database *models.Database) (*CommandResponse, error) {
	btreeIndexCommand, err := index.ParseCreateBTreeIndexCommand(command, logger)
	if err != nil {
		return nil, fmt.Errorf("error parsing B-Tree index command: %v", err)
	}
	logger.Infof("Parsed B-Tree index command: %+v", btreeIndexCommand)

	// Get the bundle by name
	bundle, err := serviceManager.BundleService.GetBundleByName(database, btreeIndexCommand.BundleName)
	if err == nil && bundle == nil {
		return nil, fmt.Errorf("bundle '%s' cannot be found", btreeIndexCommand.BundleName)
	}

	// TODO Validate the index name
	err = serviceManager.BundleService.AddIndexToBundle(database, bundle, btreeIndexCommand)
	if err != nil {
		return nil, fmt.Errorf("error adding B-Tree index to bundle '%s': %v", btreeIndexCommand.BundleName, err)
	}

	cmdResponse := &CommandResponse{
		ResultCount: 1,
		Result:      "Index created successfully",
	}
	return cmdResponse, nil
}

func CreateBundleCommand(command string, logger *zap.SugaredLogger, serviceManager ServiceManager, database *models.Database, result string) (*CommandResponse, error) {
	args := settings.GetSettings()
	bundleCmd, err := bndle.ParseCreateBundleCommand(command, logger)
	if err != nil {
		return nil, fmt.Errorf("error parsing bundle command: %v", err)
	}

	//Check if the bundle already exists
	filePath := filepath.Join(args.DataDir, fmt.Sprintf("%s.bnd", bundleCmd.BundleName))
	existingBundle := helpers.FileExists(filePath, *logger)
	if existingBundle {
		return nil, fmt.Errorf("bundle '%s' already exists", bundleCmd.BundleName)
	}

	// Validate the bundle name with a regex
	if !bundle.IsValidBundleName(bundleCmd.BundleName) {
		return nil, fmt.Errorf("invalid bundle name: %s. Bundle names must start with a letter, can be alphanumeric, with underscores and hyphens", bundleCmd.BundleName)
	}

	// Get database object by name
	// database, err1 := serviceManager.DatabaseService.GetDatabaseByName(database.Name)
	// if err1 != nil {
	// 	return nil, fmt.Errorf("error retrieving database '%s': %v", database.Name, err)
	// }
	logger.Infof("Creating bundle '%s' in database '%s'", bundleCmd.BundleName, database.Name)
	// Add the bundle to the database
	err = serviceManager.BundleService.AddBundle(serviceManager.DatabaseService, database, bundleCmd)
	if err != nil {
		return nil, fmt.Errorf("error creating bundle: %v", err)
	}

	// Return the response
	result = fmt.Sprintf("Bundle '%s' created successfully in database '%s'.", bundleCmd.BundleName, database.Name)
	cmdResponse := &CommandResponse{
		ResultCount: 1,
		Result:      result,
	}
	return cmdResponse, nil
}

func CreateDatabase(command string, logger *zap.SugaredLogger, serviceManager ServiceManager, result string) (interface{}, error) {
	dbCommand, err := db.ParseCreateDatabaseCommand(command, logger)
	if err != nil {
		return nil, err
	}

	// Check if the database already exists
	existingDB, err := serviceManager.DatabaseService.GetDatabaseByName(dbCommand.DatabaseName)
	if err == nil {
		return nil, fmt.Errorf("database '%s' already exists", existingDB.Name)
	}

	//Validate the database name with a regex
	if !db.IsValidDatabaseName(dbCommand.DatabaseName) {
		return nil, fmt.Errorf("invalid database name: %s. Database names must start with a letter, can be alphanumeric, with underscores and hyphens", dbCommand.DatabaseName)
	}
	// Execute the database command
	err = serviceManager.DatabaseService.AddDatabase(*dbCommand)
	if err != nil {
		return nil, fmt.Errorf("error creating database: %v", err)
	}
	result = fmt.Sprintf("Database '%s' created successfully.", dbCommand.DatabaseName)
	cmdResponse := &CommandResponse{
		ResultCount: 1,
		Result:      result,
	}
	return cmdResponse, nil
}

func SelectDocuments(commandParts []string, serviceManager ServiceManager, database *models.Database, logger *zap.SugaredLogger) (interface{}, error) {
	// First, check if this is a JOIN query by examining the full command
	fullCommand := strings.Join(commandParts, " ")

	// Detect JOIN queries
	if strings.Contains(strings.ToUpper(fullCommand), "JOIN") {
		return SelectDocumentsWithJoin(fullCommand, serviceManager, database, logger)
	}

	// Detect ORDER BY queries
	if strings.Contains(strings.ToUpper(fullCommand), "ORDER BY") {
		return SelectDocumentsWithOrderBy(fullCommand, serviceManager, database, logger)
	}

	// Handle regular SELECT without JOIN
	if len(commandParts) < 4 || !strings.EqualFold(commandParts[2], "FROM") {
		return nil, fmt.Errorf("SELECT DOCUMENTS requires the spec 'FROM <Bundle_name>'")
	}

	bundleName := strings.Trim(commandParts[3], "\"'")

	bundleName = strings.ReplaceAll(bundleName, "\"", "")
	bundleName = strings.ReplaceAll(bundleName, "'", "")
	bundleName = strings.ReplaceAll(bundleName, "”", "") // A very odd type of quote that can appear in text

	// TODO : Change the code after this to create an Execution Plan
	// and then use the execution plan to execute the command. The execution plan
	// should use the buffer pool to get the bundle and documents, and also
	// use the indexes if available.

	if !bundle.IsValidBundleName(bundleName) {
		return nil, fmt.Errorf("invalid bundle name: %s. Bundle names can only contain letters, numbers, underscores, and hyphens", bundleName)
	}

	// Get the bundle by name
	bundle, err := serviceManager.BundleService.GetBundleByName(database, bundleName)
	if err != nil {
		return nil, fmt.Errorf("error retrieving bundle '%s': %v", bundleName, err)
	}

	var documents map[string]*models.Document

	if len(commandParts) > 4 && strings.EqualFold(commandParts[4], "WHERE") {
		whereClause := strings.Join(commandParts[5:], " ")

		// Create execution planner
		planner := planner.NewQueryPlannerWithService(logger, serviceManager.BundleService)

		// Create execution plan
		plan, err := planner.CreateExecutionPlan(bundle, whereClause)
		if err != nil {
			logger.Warnf("Failed to create execution plan, falling back to full scan: %v", err)
			// Fallback to existing filter logic
			filteredDocs, err := queryparser.FilterDocuments(bundle, whereClause, logger)
			if err != nil {
				return nil, fmt.Errorf("error filtering documents: %v", err)
			}
			documents = make(map[string]*models.Document)
			for _, v := range filteredDocs {
				documents[v.DocumentID] = v
			}
		} else {
			// Execute the plan
			logger.Infof("Executing plan with indexes: %v", plan.IndexesUsed)
			documents, err = plan.RootNode.Execute()
			if err != nil {
				return nil, fmt.Errorf("error executing query plan: %v", err)
			}
		}
	} else {
		// No WHERE clause - return all documents
		documents = make(map[string]*models.Document)
		for k, v := range *bundle.Documents {
			docCopy := v
			documents[k] = &docCopy
		}
	}
	// var documents map[string]*models.Document
	// if len(commandParts) > 4 && strings.EqualFold(commandParts[4], "WHERE") {
	// 	//logger.Infof("Filtering documents in bundle '%s' with WHERE clause: %s", bundleName, strings.Join(commandParts[5:], " "))
	// 	whereClause := strings.Join(commandParts[5:], " ")
	// 	filteredDocs, err := queryparser.FilterDocuments(bundle, whereClause, logger)
	// 	if err != nil {
	// 		return nil, fmt.Errorf("error filtering documents: %v", err)
	// 	}

	// 	// if len(filteredDocs) > 0 {
	// 	// 	prettyJSON, err := json.MarshalIndent(filteredDocs, "", "  ")
	// 	// 	if err != nil {
	// 	// 		logger.Warnf("Failed to convert documents to JSON: %v", err)
	// 	// 	} else {
	// 	// 		logger.Infof("Found %d documents: \n%s", len(filteredDocs), string(prettyJSON))
	// 	// 	}
	// 	// } else {
	// 	// 	logger.Infof("No documents found matching the filter")
	// 	// }

	// 	documents = make(map[string]*models.Document)
	// 	for _, v := range filteredDocs {
	// 		docCopy := v
	// 		documents[docCopy.DocumentID] = v
	// 	}
	// } else {
	// 	// Get documents from the bundle
	// 	documents = make(map[string]*models.Document)
	// 	for k, v := range *bundle.Documents {
	// 		docCopy := v
	// 		documents[k] = &docCopy
	// 	}
	// }

	// if len(documents) == 0 {
	// 	result = fmt.Sprintf("No documents found in bundle '%s'.", bundleName)
	// } else {
	// 	result = fmt.Sprintf("Found %d documents in bundle '%s'.", len(documents), bundleName)
	// }
	// logger.Infof(result)

	cmdResponse := &CommandResponse{
		ResultCount: len(documents),
		Result:      documents,
	}
	return cmdResponse, nil
}

// SelectDocumentsWithJoin handles SELECT queries with JOIN clauses
func SelectDocumentsWithJoin(query string, serviceManager ServiceManager, database *models.Database, logger *zap.SugaredLogger) (interface{}, error) {
	logger.Infof("Processing JOIN query: %s", query)

	// Parse the JOIN query
	joinQuery, err := queryparser.ParseSelectJoinQuery(query, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to parse JOIN query: %w", err)
	}

	// Create join-capable query planner
	joinPlanner := planner.NewJoinQueryPlanner(logger, serviceManager.BundleService)

	// Create execution plan for the JOIN query
	plan, err := joinPlanner.CreateJoinExecutionPlan(joinQuery, database)
	if err != nil {
		return nil, fmt.Errorf("failed to create JOIN execution plan: %w", err)
	}

	// Execute the plan
	logger.Infof("Executing JOIN plan with cost %.2f, estimated rows: %d", plan.Cost, plan.EstimatedRows)
	documents, err := plan.RootNode.Execute()
	if err != nil {
		return nil, fmt.Errorf("error executing JOIN query plan: %w", err)
	}

	logger.Infof("JOIN query executed successfully, returned %d documents", len(documents))

	cmdResponse := &CommandResponse{
		ResultCount: len(documents),
		Result:      documents,
	}
	return cmdResponse, nil
}

// SelectDocumentsWithOrderBy handles SELECT queries with ORDER BY clauses
func SelectDocumentsWithOrderBy(query string, serviceManager ServiceManager, database *models.Database, logger *zap.SugaredLogger) (interface{}, error) {
	logger.Infof("Processing ORDER BY query: %s", query)

	// Parse the ORDER BY query
	selectQuery, err := queryparser.ParseSelectQueryWithOrder(query, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to parse ORDER BY query: %w", err)
	}

	// Extract bundle name
	bundleName := selectQuery.FromBundle
	if !bundle.IsValidBundleName(bundleName) {
		return nil, fmt.Errorf("invalid bundle name: %s. Bundle names can only contain letters, numbers, underscores, and hyphens", bundleName)
	}

	// Get the bundle by name
	bundle, err := serviceManager.BundleService.GetBundleByName(database, bundleName)
	if err != nil {
		return nil, fmt.Errorf("error retrieving bundle '%s': %v", bundleName, err)
	}

	var documents map[string]*models.Document

	// Handle WHERE clause if present
	if selectQuery.WhereClause != nil {
		// For now, we'll use the original query parsing since the WHERE structure is complex
		// Extract WHERE clause from the original query string
		whereStart := strings.Index(strings.ToUpper(query), "WHERE")
		orderByStart := strings.Index(strings.ToUpper(query), "ORDER BY")

		var whereClause string
		if whereStart >= 0 {
			whereEnd := len(query)
			if orderByStart > whereStart {
				whereEnd = orderByStart
			}
			whereClause = strings.TrimSpace(query[whereStart+5 : whereEnd])
		}

		if whereClause != "" {
			// Create execution planner
			planner := planner.NewQueryPlannerWithService(logger, serviceManager.BundleService)

			// Create execution plan
			plan, err := planner.CreateExecutionPlan(bundle, whereClause)
			if err != nil {
				logger.Warnf("Failed to create execution plan, falling back to full scan: %v", err)
				// Fallback to existing filter logic
				filteredDocs, err := queryparser.FilterDocuments(bundle, whereClause, logger)
				if err != nil {
					return nil, fmt.Errorf("error filtering documents: %v", err)
				}
				documents = make(map[string]*models.Document)
				for _, v := range filteredDocs {
					documents[v.DocumentID] = v
				}
			} else {
				// Execute the plan
				logger.Infof("Executing plan with indexes: %v", plan.IndexesUsed)
				documents, err = plan.RootNode.Execute()
				if err != nil {
					return nil, fmt.Errorf("error executing query plan: %v", err)
				}
			}
		}
	} else {
		// No WHERE clause - return all documents
		documents = make(map[string]*models.Document)
		for k, v := range *bundle.Documents {
			docCopy := v
			documents[k] = &docCopy
		}
	}

	// Sort the documents according to the ORDER BY clause
	sorter := queryparser.NewDocumentSorter(selectQuery.OrderBy, logger)
	sortedDocuments, err := sorter.SortDocumentMap(documents)
	if err != nil {
		return nil, fmt.Errorf("error sorting documents: %v", err)
	}

	logger.Infof("ORDER BY query executed successfully, returned %d sorted documents", len(sortedDocuments))

	// Convert sorted slice back to map for response consistency
	resultMap := make(map[string]*models.Document)
	for _, doc := range sortedDocuments {
		resultMap[doc.DocumentID] = doc
	}

	cmdResponse := &CommandResponse{
		ResultCount: len(resultMap),
		Result:      resultMap,
	}
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
			databases = make([]*models.Database, 0)
		}

		cmdResponse := &CommandResponse{
			ResultCount: len(databases),
			Result:      databases,
		}
		return cmdResponse, nil, true
	}
	return nil, nil, false
}

// parseBundleNameFromCommand extracts the bundle name from UPDATE/DELETE commands
// This function follows the Single Responsibility Principle by handling only bundle name extraction
// Following SyndrDB comprehensive error handling, it properly handles quoted strings and multi-line commands
// Parameters:
//   - command: The full command string to parse
//   - keyword: The keyword to look for ("IN" for UPDATE, "FROM" for DELETE)
//
// Returns:
//   - string: The extracted bundle name without quotes
//   - error: Any error that occurred during parsing
func parseBundleNameFromCommand(command, keyword string) (string, error) {
	// Normalize the command by removing extra whitespace and newlines
	// Following SyndrDB data integrity requirements, ensure consistent parsing
	normalizedCommand := strings.ReplaceAll(command, "\n", " ")
	normalizedCommand = strings.ReplaceAll(normalizedCommand, "\r", " ")
	normalizedCommand = strings.ReplaceAll(normalizedCommand, "\t", " ")

	// Collapse multiple spaces into single spaces
	for strings.Contains(normalizedCommand, "  ") {
		normalizedCommand = strings.ReplaceAll(normalizedCommand, "  ", " ")
	}
	normalizedCommand = strings.TrimSpace(normalizedCommand)

	// Find the position of the keyword (case-insensitive)
	keywordUpper := strings.ToUpper(keyword)
	commandUpper := strings.ToUpper(normalizedCommand)
	keywordPos := strings.Index(commandUpper, keywordUpper)

	if keywordPos == -1 {
		return "", fmt.Errorf("keyword '%s' not found in command", keyword)
	}

	// Extract the part after the keyword
	afterKeyword := normalizedCommand[keywordPos+len(keyword):]
	afterKeyword = strings.TrimSpace(afterKeyword)

	// Look for "BUNDLE" keyword after the main keyword
	bundleUpper := "BUNDLE"
	bundlePos := strings.Index(strings.ToUpper(afterKeyword), bundleUpper)

	if bundlePos == -1 {
		return "", fmt.Errorf("'BUNDLE' keyword not found after '%s'", keyword)
	}

	// Extract the part after "BUNDLE"
	afterBundle := afterKeyword[bundlePos+len(bundleUpper):]
	afterBundle = strings.TrimSpace(afterBundle)

	// Find the quoted bundle name
	bundleName, err := extractQuotedString(afterBundle)
	if err != nil {
		return "", fmt.Errorf("failed to extract bundle name: %w", err)
	}

	return bundleName, nil
}

// extractQuotedString extracts a quoted string from the beginning of a text
// This function follows the Single Responsibility Principle by handling only quoted string extraction
// Following SyndrDB comprehensive error handling, it supports multiple quote types
// Parameters:
//   - text: The text to extract the quoted string from
//
// Returns:
//   - string: The extracted string without quotes
//   - error: Any error that occurred during extraction
func extractQuotedString(text string) (string, error) {
	text = strings.TrimSpace(text)

	if len(text) == 0 {
		return "", fmt.Errorf("empty text provided for quote extraction")
	}

	// Check for different quote types
	quoteChars := []rune{'"', '\'', '"', '"'} // Regular quotes and smart quotes

	for _, quoteChar := range quoteChars {
		if rune(text[0]) == quoteChar {
			// Find the closing quote
			for i := 1; i < len(text); i++ {
				if rune(text[i]) == quoteChar {
					// Found closing quote
					return text[1:i], nil
				}
			}
			return "", fmt.Errorf("unterminated quoted string starting with %c", quoteChar)
		}
	}

	// If no quotes found, look for the first word (until space or special character)
	words := strings.Fields(text)
	if len(words) > 0 {
		// Find where the first word ends (before parentheses, WHERE, etc.)
		firstWord := words[0]
		stopChars := []string{"(", "WHERE", "SET"}

		for _, stopChar := range stopChars {
			if idx := strings.Index(strings.ToUpper(text), stopChar); idx != -1 {
				beforeStop := strings.TrimSpace(text[:idx])
				if beforeStop != "" {
					return beforeStop, nil
				}
			}
		}

		return firstWord, nil
	}

	return "", fmt.Errorf("no quoted string or valid identifier found in text: %s", text)
}
