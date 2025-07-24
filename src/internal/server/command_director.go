package server

import (
	"fmt"
	"path/filepath"
	"strings"
	bndle "syndrdb/src/internal/domain/bundle"
	db "syndrdb/src/internal/domain/database"
	"syndrdb/src/internal/domain/index"
	"syndrdb/src/internal/domain/models"
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
			result1, err, shouldReturn := CreateBTreeIndex(command, logger, serviceManager, database)
			if shouldReturn {
				return result1, err
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
				RelationshipCommand, err := bndle.ParseCreateRelationshipCommand(command)
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
			bndle.ParseDeleteBundleCommand(command)
		case "documents":
			//DELETE DOCUMENTS FROM BUNDLE "BUNDLE_NAME"
			//WHERE <FIELDNAME> = <VALUE>
			if len(commandParts) < 5 || !strings.EqualFold(commandParts[2], "FROM") {
				return nil, fmt.Errorf("DELETE DOCUMENTS requires the spec 'FROM <Bundle_name>'")
			}
			bundleName := strings.Trim(commandParts[4], "\"'")
			bundleName = strings.ReplaceAll(bundleName, "\"", "")
			bundleName = strings.ReplaceAll(bundleName, "'", "")
			bundleName = strings.ReplaceAll(bundleName, "”", "") // A very odd type of quote that can appear in text
			// Get the bundle by name

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
	if len(commandParts) < 5 || !strings.EqualFold(commandParts[2], "IN") {
		return nil, fmt.Errorf("UPDATE DOCUMENTS requires the spec 'IN <Bundle_name>'")
	}
	bundleName := strings.Trim(commandParts[4], "\"'")
	bundleName = strings.ReplaceAll(bundleName, "\"", "")
	bundleName = strings.ReplaceAll(bundleName, "'", "")
	bundleName = strings.ReplaceAll(bundleName, "”", "") // A very odd type of quote that can appear in text
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

func CreateBTreeIndex(command string, logger *zap.SugaredLogger, serviceManager ServiceManager, database *models.Database) (*CommandResponse, error, bool) {
	btreeIndexCommand, err := index.ParseCreateBTreeIndexCommand(command, logger)
	if err != nil {
		return nil, fmt.Errorf("error parsing B-Tree index command: %v", err), true
	}
	logger.Infof("Parsed B-Tree index command: %+v", btreeIndexCommand)

	// Get the bundle by name
	bundle, err := serviceManager.BundleService.GetBundleByName(database, btreeIndexCommand.BundleName)
	if err == nil {
		return nil, fmt.Errorf("bundle '%s' cannot be found", bundle.Name), true
	}

	// TODO Validate the index name
	err = serviceManager.BundleService.AddIndexToBundle(database, bundle, btreeIndexCommand)
	if err != nil {
		return nil, fmt.Errorf("error adding B-Tree index to bundle '%s': %v", btreeIndexCommand.BundleName, err), true
	}

	cmdResponse := &CommandResponse{
		ResultCount: 1,
		Result:      "Index created successfully",
	}
	return cmdResponse, nil, true
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

	//TODO The database pointer is null for some reason here. So we need to fix that shit.
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
	if len(commandParts) < 4 || !strings.EqualFold(commandParts[2], "FROM") {
		return nil, fmt.Errorf("SELECT DOCUMENTS requires the spec 'FROM <Bundle_name>'")
	}

	bundleName := strings.Trim(commandParts[3], "\"'")

	bundleName = strings.ReplaceAll(bundleName, "\"", "")
	bundleName = strings.ReplaceAll(bundleName, "'", "")
	bundleName = strings.ReplaceAll(bundleName, "”", "") // A very odd type of quote that can appear in text

	// Get the bundle by name
	bundle, err := serviceManager.BundleService.GetBundleByName(database, bundleName)
	if err != nil {
		return nil, fmt.Errorf("error retrieving bundle '%s': %v", bundleName, err)
	}

	var documents map[string]*models.Document
	if len(commandParts) > 4 && strings.EqualFold(commandParts[4], "WHERE") {
		//logger.Infof("Filtering documents in bundle '%s' with WHERE clause: %s", bundleName, strings.Join(commandParts[5:], " "))
		whereClause := strings.Join(commandParts[5:], " ")
		filteredDocs, err := queryparser.FilterDocuments(bundle, whereClause, logger)
		if err != nil {
			return nil, fmt.Errorf("error filtering documents: %v", err)
		}

		// if len(filteredDocs) > 0 {
		// 	prettyJSON, err := json.MarshalIndent(filteredDocs, "", "  ")
		// 	if err != nil {
		// 		logger.Warnf("Failed to convert documents to JSON: %v", err)
		// 	} else {
		// 		logger.Infof("Found %d documents: \n%s", len(filteredDocs), string(prettyJSON))
		// 	}
		// } else {
		// 	logger.Infof("No documents found matching the filter")
		// }

		documents = make(map[string]*models.Document)
		for _, v := range filteredDocs {
			docCopy := v
			documents[docCopy.DocumentID] = v
		}
	} else {
		// Get documents from the bundle
		documents = make(map[string]*models.Document)
		for k, v := range bundle.Documents {
			docCopy := v
			documents[k] = &docCopy
		}
	}

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
