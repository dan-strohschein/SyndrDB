package server

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	bndle "syndrdb/src/internal/domain/bundle"
	db "syndrdb/src/internal/domain/database"
	"syndrdb/src/internal/domain/index"
	"syndrdb/src/internal/domain/models"
	"syndrdb/src/internal/query/executor"
	"syndrdb/src/internal/query/planner"
	"syndrdb/src/internal/query/queryparser"
	"syndrdb/src/pkg/common/helpers"
	"time"

	"go.uber.org/zap"
)

func CommandDirector(database *models.Database, serviceManager ServiceManager, command string, logger *zap.SugaredLogger, startTime time.Time) (interface{}, error) {
	// Check if this is a GraphQL command first
	if strings.HasPrefix(command, "GRAPHQL::") {
		if serviceManager.GraphQLProcessor == nil {
			return nil, fmt.Errorf("GraphQL is not enabled on this server")
		}
		return serviceManager.GraphQLProcessor.ProcessGraphQLCommand(command)
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

	if strings.HasPrefix(strings.ToLower(command), "select") {
		// Parse SELECT command
		//dbCommand, err := engine.ParseSelectCommand(command)
		switch strings.ToLower(commandParts[1]) {
		case "databases":
			result1, err, shouldReturn := SelectDatabases(commandParts, serviceManager)
			if shouldReturn {
				return result1, err
			}

		case "top":
			return SelectTopDocuments(commandParts, serviceManager, database, logger, startTime)

		case "documents":
			return SelectDocuments(commandParts, serviceManager, database, logger, startTime)
		case "count":
			return SelectDocumentCount(commandParts, serviceManager, database, logger, startTime)
		}
		return nil, nil
	}

	if strings.HasPrefix(strings.ToLower(command), "show") {
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
		case "rate":
			if len(commandParts) > 2 && strings.ToLower(commandParts[2]) == "limit" {
				return ShowRateLimit(command, logger, serviceManager)
			}
			return nil, fmt.Errorf("unknown SHOW RATE command: %s", command)
		}
		return nil, fmt.Errorf("unknown SHOW command: %s", command)
	}

	if strings.HasPrefix(strings.ToLower(command), "invalidate") {
		// Parse INVALIDATE command
		switch strings.ToLower(commandParts[1]) {
		case "session":
			return InvalidateSession(command, logger, serviceManager)
		}
		return nil, fmt.Errorf("unknown INVALIDATE command: %s", command)
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
		case "user":
			return AddUser(command, logger, serviceManager)
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

	// Parse GRANT command
	if strings.HasPrefix(strings.ToLower(command), "grant") {
		return GrantPermission(command, logger, serviceManager)
	}

	// Parse ATTACH command
	if strings.HasPrefix(strings.ToLower(command), "attach") {
		// Check if this is ATTACH DATABASE syntax by looking for DATABASE keyword
		commandLower := strings.ToLower(command)
		if strings.Contains(commandLower, "attach database") {
			return AttachDatabase(command, logger, serviceManager)
		}
		// Default to user attachment for other ATTACH commands
		return AttachUserToDatabase(command, logger, serviceManager)
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

			// Execute with WAL logging if available
			if serviceManager.WALManager != nil {
				err = serviceManager.WALManager.ExecuteWithLogging(func(txID string) error {
					// Log the document deletion before execution
					// Note: We'll log the where clause as metadata for the deletion
					err := serviceManager.WALManager.LogDocumentDelete(txID, bundleName, "multiple", docCommand.WhereClause)
					if err != nil {
						return fmt.Errorf("failed to log document delete: %w", err)
					}

					// Delete the document from the bundle
					return serviceManager.BundleService.DeleteDocumentFromBundle(bundle, docCommand)
				})
			} else {
				// Fallback to direct execution if WAL is not available
				logger.Warn("WAL Manager not available, executing without transaction logging")
				err = serviceManager.BundleService.DeleteDocumentFromBundle(bundle, docCommand)
			}

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

	if strings.HasPrefix(strings.ToLower(command), "use") {
		return UseDatabase(command, logger, serviceManager)
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

// AddUser processes an ADD USER command
// Syntax: ADD USER username WITH PASSWORD 'password'
func AddUser(command string, logger *zap.SugaredLogger, serviceManager ServiceManager) (*CommandResponse, error) {
	logger.Infof("Processing ADD USER command: %s", command)

	// Parse the command: ADD USER username WITH PASSWORD 'password'
	parts := strings.Fields(command)
	if len(parts) < 6 {
		return nil, fmt.Errorf("invalid ADD USER syntax: expected 'ADD USER username WITH PASSWORD password'")
	}

	if !strings.EqualFold(parts[0], "ADD") || !strings.EqualFold(parts[1], "USER") {
		return nil, fmt.Errorf("invalid ADD USER command format")
	}

	if !strings.EqualFold(parts[3], "WITH") || !strings.EqualFold(parts[4], "PASSWORD") {
		return nil, fmt.Errorf("invalid ADD USER syntax: expected 'WITH PASSWORD'")
	}

	username := parts[2]
	password := strings.Trim(parts[5], "'\"") // Remove quotes from password

	// Get the Primary database
	primaryDB, err := serviceManager.DatabaseService.GetDatabaseByName("primary")
	if err != nil {
		return nil, fmt.Errorf("primary database not found: %w", err)
	}

	// Find the Users bundle
	usersBundle, exists := primaryDB.Bundles["Users"]
	if !exists {
		return nil, fmt.Errorf("Users bundle not found in Primary database")
	}

	// Check if user already exists
	if usersBundle.Documents != nil {
		for _, doc := range *usersBundle.Documents {
			if usernameField, ok := doc.Fields["Username"]; ok {
				if usernameField.Value == username {
					return nil, fmt.Errorf("user '%s' already exists", username)
				}
			}
		}
	}

	// Generate a unique UserID
	userID := fmt.Sprintf("user_%s_%d", username, time.Now().Unix())

	// Hash the password (simplified - in production use proper hashing like bcrypt)
	hashedPassword := fmt.Sprintf("hashed_%s", password) // TODO: Implement proper password hashing

	// Create the user document
	userDoc := models.Document{
		DocumentID: userID,
		Fields: map[string]models.Field{
			"DocumentID": {Name: "DocumentID", Value: userID},
			"UserID":     {Name: "UserID", Value: userID},
			"Username":   {Name: "Username", Value: username},
			"Password":   {Name: "Password", Value: hashedPassword},
			"CreatedAt":  {Name: "CreatedAt", Value: time.Now().Format(time.RFC3339)},
		},
	}

	// Add the user document to the Users bundle
	if usersBundle.Documents == nil {
		documentsMap := make(map[string]models.Document)
		usersBundle.Documents = &documentsMap
	}

	(*usersBundle.Documents)[userDoc.DocumentID] = userDoc

	// Update the bundle back in the database
	primaryDB.Bundles["Users"] = usersBundle

	logger.Infof("User '%s' created successfully with ID: %s", username, userID)

	response := &CommandResponse{
		ResultCount: 1,
		Result:      fmt.Sprintf("User '%s' created successfully with ID: %s", username, userID),
	}

	return response, nil
}

// GrantPermission processes a GRANT command
// Syntax: GRANT permission TO USER username
func GrantPermission(command string, logger *zap.SugaredLogger, serviceManager ServiceManager) (*CommandResponse, error) {
	logger.Infof("Processing GRANT command: %s", command)

	// Parse the command: GRANT permission TO USER username
	parts := strings.Fields(command)
	if len(parts) < 5 {
		return nil, fmt.Errorf("invalid GRANT syntax: expected 'GRANT permission TO USER username'")
	}

	if !strings.EqualFold(parts[0], "GRANT") || !strings.EqualFold(parts[2], "TO") || !strings.EqualFold(parts[3], "USER") {
		return nil, fmt.Errorf("invalid GRANT command format")
	}

	permission := parts[1]
	username := parts[4]

	// Get the Primary database
	primaryDB, err := serviceManager.DatabaseService.GetDatabaseByName("primary")
	if err != nil {
		return nil, fmt.Errorf("primary database not found: %w", err)
	}

	// Find the user first
	usersBundle, exists := primaryDB.Bundles["Users"]
	if !exists {
		return nil, fmt.Errorf("Users bundle not found in Primary database")
	}

	var userID string
	if usersBundle.Documents != nil {
		found := false
		for _, doc := range *usersBundle.Documents {
			if usernameField, ok := doc.Fields["Username"]; ok {
				if usernameField.Value == username {
					if userIDField, ok := doc.Fields["UserID"]; ok {
						userID = userIDField.Value.(string)
						found = true
						break
					}
				}
			}
		}
		if !found {
			return nil, fmt.Errorf("user '%s' not found", username)
		}
	} else {
		return nil, fmt.Errorf("user '%s' not found", username)
	}

	// Find or create the permission in Permissions bundle
	permissionsBundle, exists := primaryDB.Bundles["Permissions"]
	if !exists {
		return nil, fmt.Errorf("Permissions bundle not found in Primary database")
	}

	var permissionID string
	if permissionsBundle.Documents != nil {
		found := false
		for _, doc := range *permissionsBundle.Documents {
			if nameField, ok := doc.Fields["PermissionName"]; ok {
				if nameField.Value == permission {
					if idField, ok := doc.Fields["PermissionID"]; ok {
						permissionID = idField.Value.(string)
						found = true
						break
					}
				}
			}
		}
		if !found {
			// Create the permission if it doesn't exist
			permissionID = fmt.Sprintf("perm_%s_%d", permission, time.Now().Unix())
			permissionDoc := models.Document{
				DocumentID: permissionID,
				Fields: map[string]models.Field{
					"DocumentID":     {Name: "DocumentID", Value: permissionID},
					"PermissionID":   {Name: "PermissionID", Value: permissionID},
					"PermissionName": {Name: "PermissionName", Value: permission},
					"Description":    {Name: "Description", Value: fmt.Sprintf("Permission for %s", permission)},
				},
			}
			(*permissionsBundle.Documents)[permissionDoc.DocumentID] = permissionDoc
			primaryDB.Bundles["Permissions"] = permissionsBundle
		}
	} else {
		// Create the permission since bundle is empty
		documentsMap := make(map[string]models.Document)
		permissionsBundle.Documents = &documentsMap

		permissionID = fmt.Sprintf("perm_%s_%d", permission, time.Now().Unix())
		permissionDoc := models.Document{
			DocumentID: permissionID,
			Fields: map[string]models.Field{
				"DocumentID":     {Name: "DocumentID", Value: permissionID},
				"PermissionID":   {Name: "PermissionID", Value: permissionID},
				"PermissionName": {Name: "PermissionName", Value: permission},
				"Description":    {Name: "Description", Value: fmt.Sprintf("Permission for %s", permission)},
			},
		}
		(*permissionsBundle.Documents)[permissionDoc.DocumentID] = permissionDoc
		primaryDB.Bundles["Permissions"] = permissionsBundle
	}

	// Add the user-permission relationship to UserPermissions bundle
	userPermissionsBundle, exists := primaryDB.Bundles["UserPermissions"]
	if !exists {
		return nil, fmt.Errorf("UserPermissions bundle not found in Primary database")
	}

	// Check if the relationship already exists
	if userPermissionsBundle.Documents != nil {
		for _, doc := range *userPermissionsBundle.Documents {
			if userIDField, ok := doc.Fields["UserID"]; ok {
				if permIDField, ok := doc.Fields["PermissionID"]; ok {
					if userIDField.Value == userID && permIDField.Value == permissionID {
						return nil, fmt.Errorf("user '%s' already has permission '%s'", username, permission)
					}
				}
			}
		}
	}

	// Create the user-permission relationship
	relationshipID := fmt.Sprintf("userperm_%s_%s_%d", userID, permissionID, time.Now().Unix())
	relationshipDoc := models.Document{
		DocumentID: relationshipID,
		Fields: map[string]models.Field{
			"DocumentID":   {Name: "DocumentID", Value: relationshipID},
			"UserID":       {Name: "UserID", Value: userID},
			"PermissionID": {Name: "PermissionID", Value: permissionID},
			"GrantedAt":    {Name: "GrantedAt", Value: time.Now().Format(time.RFC3339)},
		},
	}

	if userPermissionsBundle.Documents == nil {
		documentsMap := make(map[string]models.Document)
		userPermissionsBundle.Documents = &documentsMap
	}

	(*userPermissionsBundle.Documents)[relationshipDoc.DocumentID] = relationshipDoc
	primaryDB.Bundles["UserPermissions"] = userPermissionsBundle

	logger.Infof("Permission '%s' granted to user '%s'", permission, username)

	response := &CommandResponse{
		ResultCount: 1,
		Result:      fmt.Sprintf("Permission '%s' granted to user '%s'", permission, username),
	}

	return response, nil
}

// AttachUserToDatabase processes an ATTACH command
// Syntax: ATTACH USER username TO DATABASE database_name
func AttachUserToDatabase(command string, logger *zap.SugaredLogger, serviceManager ServiceManager) (*CommandResponse, error) {
	logger.Infof("Processing ATTACH command: %s", command)

	// Parse the command: ATTACH USER username TO DATABASE database_name
	parts := strings.Fields(command)
	if len(parts) < 6 {
		return nil, fmt.Errorf("invalid ATTACH syntax: expected 'ATTACH USER username TO DATABASE database_name'")
	}

	if !strings.EqualFold(parts[0], "ATTACH") || !strings.EqualFold(parts[1], "USER") ||
		!strings.EqualFold(parts[3], "TO") || !strings.EqualFold(parts[4], "DATABASE") {
		return nil, fmt.Errorf("invalid ATTACH command format")
	}

	username := parts[2]
	databaseName := parts[5]

	// Get the Primary database
	primaryDB, err := serviceManager.DatabaseService.GetDatabaseByName("primary")
	if err != nil {
		return nil, fmt.Errorf("primary database not found: %w", err)
	}

	// Find the user
	usersBundle, exists := primaryDB.Bundles["Users"]
	if !exists {
		return nil, fmt.Errorf("Users bundle not found in Primary database")
	}

	var userID string
	if usersBundle.Documents != nil {
		found := false
		for _, doc := range *usersBundle.Documents {
			if usernameField, ok := doc.Fields["Username"]; ok {
				if usernameField.Value == username {
					if userIDField, ok := doc.Fields["UserID"]; ok {
						userID = userIDField.Value.(string)
						found = true
						break
					}
				}
			}
		}
		if !found {
			return nil, fmt.Errorf("user '%s' not found", username)
		}
	} else {
		return nil, fmt.Errorf("user '%s' not found", username)
	}

	// Find the database
	databasesBundle, exists := primaryDB.Bundles["Databases"]
	if !exists {
		return nil, fmt.Errorf("Databases bundle not found in Primary database")
	}

	var databaseID string
	if databasesBundle.Documents != nil {
		found := false
		for _, doc := range *databasesBundle.Documents {
			if nameField, ok := doc.Fields["Name"]; ok {
				if nameField.Value == databaseName {
					if dbIDField, ok := doc.Fields["DatabaseID"]; ok {
						databaseID = dbIDField.Value.(string)
						found = true
						break
					}
				}
			}
		}
		if !found {
			return nil, fmt.Errorf("database '%s' not found", databaseName)
		}
	} else {
		return nil, fmt.Errorf("database '%s' not found", databaseName)
	}

	// Add the user-database relationship to DatabaseUsers bundle
	databaseUsersBundle, exists := primaryDB.Bundles["DatabaseUsers"]
	if !exists {
		return nil, fmt.Errorf("DatabaseUsers bundle not found in Primary database")
	}

	// Check if the relationship already exists
	if databaseUsersBundle.Documents != nil {
		for _, doc := range *databaseUsersBundle.Documents {
			if userIDField, ok := doc.Fields["UserID"]; ok {
				if dbIDField, ok := doc.Fields["DatabaseID"]; ok {
					if userIDField.Value == userID && dbIDField.Value == databaseID {
						return nil, fmt.Errorf("user '%s' is already attached to database '%s'", username, databaseName)
					}
				}
			}
		}
	}

	// Create the user-database relationship
	relationshipID := fmt.Sprintf("dbuser_%s_%s_%d", userID, databaseID, time.Now().Unix())
	relationshipDoc := models.Document{
		DocumentID: relationshipID,
		Fields: map[string]models.Field{
			"DocumentID": {Name: "DocumentID", Value: relationshipID},
			"UserID":     {Name: "UserID", Value: userID},
			"DatabaseID": {Name: "DatabaseID", Value: databaseID},
			"AttachedAt": {Name: "AttachedAt", Value: time.Now().Format(time.RFC3339)},
		},
	}

	if databaseUsersBundle.Documents == nil {
		documentsMap := make(map[string]models.Document)
		databaseUsersBundle.Documents = &documentsMap
	}

	(*databaseUsersBundle.Documents)[relationshipDoc.DocumentID] = relationshipDoc
	primaryDB.Bundles["DatabaseUsers"] = databaseUsersBundle

	logger.Infof("User '%s' attached to database '%s'", username, databaseName)

	response := &CommandResponse{
		ResultCount: 1,
		Result:      fmt.Sprintf("User '%s' attached to database '%s'", username, databaseName),
	}

	return response, nil
}

// CheckUserHasPermission checks if a user has a specific permission
func CheckUserHasPermission(username, permission string, serviceManager ServiceManager) (bool, error) {
	// Get the Primary database
	primaryDB, err := serviceManager.DatabaseService.GetDatabaseByName("primary")
	if err != nil {
		return false, fmt.Errorf("primary database not found: %w", err)
	}

	// Find the user
	usersBundle, exists := primaryDB.Bundles["Users"]
	if !exists {
		return false, fmt.Errorf("Users bundle not found in Primary database")
	}

	var userID string
	if usersBundle.Documents != nil {
		found := false
		for _, doc := range *usersBundle.Documents {
			if usernameField, ok := doc.Fields["Username"]; ok {
				if usernameField.Value == username {
					if userIDField, ok := doc.Fields["UserID"]; ok {
						userID = userIDField.Value.(string)
						found = true
						break
					}
				}
			}
		}
		if !found {
			return false, fmt.Errorf("user '%s' not found", username)
		}
	} else {
		return false, fmt.Errorf("user '%s' not found", username)
	}

	// Find the permission
	permissionsBundle, exists := primaryDB.Bundles["Permissions"]
	if !exists {
		return false, fmt.Errorf("Permissions bundle not found in Primary database")
	}

	var permissionID string
	if permissionsBundle.Documents != nil {
		found := false
		for _, doc := range *permissionsBundle.Documents {
			if nameField, ok := doc.Fields["PermissionName"]; ok {
				if nameField.Value == permission {
					if idField, ok := doc.Fields["PermissionID"]; ok {
						permissionID = idField.Value.(string)
						found = true
						break
					}
				}
			}
		}
		if !found {
			return false, nil // Permission doesn't exist, user doesn't have it
		}
	} else {
		return false, nil // No permissions exist, user doesn't have it
	}

	// Check if the user has this permission in UserPermissions bundle
	userPermissionsBundle, exists := primaryDB.Bundles["UserPermissions"]
	if !exists {
		return false, fmt.Errorf("UserPermissions bundle not found in Primary database")
	}

	if userPermissionsBundle.Documents != nil {
		for _, doc := range *userPermissionsBundle.Documents {
			if userIDField, ok := doc.Fields["UserID"]; ok {
				if permIDField, ok := doc.Fields["PermissionID"]; ok {
					if userIDField.Value == userID && permIDField.Value == permissionID {
						return true, nil
					}
				}
			}
		}
	}

	return false, nil
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

	// Execute with WAL logging if available
	if serviceManager.WALManager != nil {
		err = serviceManager.WALManager.ExecuteWithLogging(func(txID string) error {
			// Log the document update before execution
			// Note: We'll log the fields being updated, actual before/after data is captured by bundle service
			err := serviceManager.WALManager.LogDocumentUpdate(txID, bundleName, "multiple", nil, docCommand.Fields)
			if err != nil {
				return fmt.Errorf("failed to log document update: %w", err)
			}

			// Update the document in the bundle
			return serviceManager.BundleService.UpdateDocumentInBundle(bundle, docCommand)
		})
	} else {
		// Fallback to direct execution if WAL is not available
		logger.Warn("WAL Manager not available, executing without transaction logging")
		err = serviceManager.BundleService.UpdateDocumentInBundle(bundle, docCommand)
	}

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

	docID := ""
	// Execute with WAL logging if available
	if serviceManager.WALManager != nil {
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
	} else {
		// Fallback to direct execution if WAL is not available
		logger.Warn("WAL Manager not available, executing without transaction logging")
		docID, err = serviceManager.BundleService.AddDocumentToBundle(database, bundle, docCommand)
	}

	if err != nil {
		return nil, fmt.Errorf("error adding document to bundle '%s': %v", bundleName, err)
	}

	result := fmt.Sprintf("{\"DocumentID\": \"%s\", \"message\": \"Document added successfully to bundle '%s'.\"}", docID, bundleName)
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
	//args := settings.GetSettings()
	bundleCmd, err := bndle.ParseCreateBundleCommand(command, logger)
	if err != nil {
		return nil, fmt.Errorf("error parsing bundle command: %v", err)
	}

	//Check if the bundle already exists
	databasePath := helpers.GetDatabaseFolderPath(database.Name)
	filePath := filepath.Join(databasePath, fmt.Sprintf("%s_%s.bnd", database.Name, bundleCmd.BundleName))
	existingBundle := helpers.FileExists(filePath, *logger)
	if existingBundle {
		return nil, fmt.Errorf("bundle '%s' already exists", bundleCmd.BundleName)
	}

	// Validate the bundle name with a regex
	if !bndle.IsValidBundleName(bundleCmd.BundleName) {
		return nil, fmt.Errorf("invalid bundle name: %s. Bundle names must start with a letter, can be alphanumeric, with underscores and hyphens", bundleCmd.BundleName)
	}

	// Get database object by name
	// database, err1 := serviceManager.DatabaseService.GetDatabaseByName(database.Name)
	// if err1 != nil {
	// 	return nil, fmt.Errorf("error retrieving database '%s': %v", database.Name, err)
	// }
	logger.Infof("Creating bundle '%s' in database '%s'", bundleCmd.BundleName, database.Name)

	// Execute with WAL logging if available
	if serviceManager.WALManager != nil {
		err = serviceManager.WALManager.ExecuteWithLogging(func(txID string) error {
			// Log the bundle creation before execution
			err := serviceManager.WALManager.LogBundleCreate(txID, bundleCmd.BundleName, bundleCmd)
			if err != nil {
				return fmt.Errorf("failed to log bundle create: %w", err)
			}

			// Add the bundle to the database
			bundle, err := serviceManager.BundleService.AddBundle(serviceManager.DatabaseService, database, bundleCmd)
			if err != nil {
				return fmt.Errorf("error creating bundle: %v", err)
			}

			// CRITICAL FIX: Check for errors when adding bundle to catalog
			// Without this, bundles may be created but not registered in the system catalog
			err = serviceManager.InternalCatalogService.RegisterBundleInCatalog(bundle)
			if err != nil {
				// Bundle was created but catalog registration failed
				logger.Errorf("Bundle '%s' created but failed to register in catalog: %v", bundle.Name, err)
				return fmt.Errorf("bundle created but catalog registration failed: %v", err)
			}

			return err
		})
	} else {
		// Fallback to direct execution if WAL is not available
		logger.Warn("WAL Manager not available, executing without transaction logging")
		bundle, err := serviceManager.BundleService.AddBundle(serviceManager.DatabaseService, database, bundleCmd)

		if err != nil {
			return nil, fmt.Errorf("error creating bundle: %v", err)
		}

		// CRITICAL FIX: Check for errors when adding bundle to catalog
		// Without this, bundles may be created but not registered in the system catalog
		err = serviceManager.InternalCatalogService.RegisterBundleInCatalog(bundle)
		if err != nil {
			// Bundle was created but catalog registration failed
			logger.Errorf("Bundle '%s' created but failed to register in catalog: %v", bundle.Name, err)
			return nil, fmt.Errorf("bundle created but catalog registration failed: %v", err)
		}
	}

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
	// TODO: Add user authentication context to check Admin permissions
	// When user sessions are implemented, uncomment the following lines:
	//
	// hasAdminPermission, err := CheckUserHasPermission(currentUser, "Admin", serviceManager)
	// if err != nil {
	//     return nil, fmt.Errorf("error checking admin permissions: %w", err)
	// }
	// if !hasAdminPermission {
	//     return nil, fmt.Errorf("access denied: only users with Admin permissions can create databases")
	// }

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
	newDb, err := serviceManager.DatabaseService.AddDatabase(*dbCommand)
	if err != nil {
		return nil, fmt.Errorf("error creating database: %v", err)
	}

	// CRITICAL FIX: Check for errors when adding database to catalog
	// Without this, databases may be created but not registered in the system catalog
	err = serviceManager.InternalCatalogService.AddDatabaseToCatalog(newDb)
	if err != nil {
		// Database was created but catalog registration failed
		// This is a critical issue as the database won't be discoverable
		logger.Errorf("Database '%s' created but failed to register in catalog: %v", newDb.Name, err)
		return nil, fmt.Errorf("database created but catalog registration failed: %v", err)
	}

	result = fmt.Sprintf("Database '%s' created successfully.", dbCommand.DatabaseName)
	cmdResponse := &CommandResponse{
		ResultCount: 1,
		Result:      result,
	}
	return cmdResponse, nil
}

// Session management commands

// ShowSessions shows all active sessions
// Syntax: SHOW SESSIONS
func ShowSessions(command string, logger *zap.SugaredLogger, serviceManager ServiceManager) (*CommandResponse, error) {
	logger.Infof("Processing SHOW SESSIONS command: %s", command)

	// This would need access to the SessionManager, which is not currently available in the CommandDirector
	// For now, return a placeholder response
	response := &CommandResponse{
		ResultCount: 1,
		Result:      "Session management requires server context - use server.SessionManager.GetSessionStats()",
	}

	return response, nil
}

// ShowSession shows information about a specific session
// Syntax: SHOW SESSION session_id
func ShowSession(command string, logger *zap.SugaredLogger, serviceManager ServiceManager) (*CommandResponse, error) {
	logger.Infof("Processing SHOW SESSION command: %s", command)

	parts := strings.Fields(command)
	if len(parts) < 3 {
		return nil, fmt.Errorf("invalid SHOW SESSION syntax: expected 'SHOW SESSION session_id'")
	}

	sessionID := parts[2]

	// This would need access to the SessionManager, which is not currently available in the CommandDirector
	// For now, return a placeholder response
	response := &CommandResponse{
		ResultCount: 1,
		Result:      fmt.Sprintf("Session info for %s requires server context - use server.SessionManager.GetSession()", sessionID),
	}

	return response, nil
}

// InvalidateSession invalidates a specific session
// Syntax: INVALIDATE SESSION session_id
func InvalidateSession(command string, logger *zap.SugaredLogger, serviceManager ServiceManager) (*CommandResponse, error) {
	logger.Infof("Processing INVALIDATE SESSION command: %s", command)

	parts := strings.Fields(command)
	if len(parts) < 3 {
		return nil, fmt.Errorf("invalid INVALIDATE SESSION syntax: expected 'INVALIDATE SESSION session_id'")
	}

	sessionID := parts[2]

	// This would need access to the SessionManager, which is not currently available in the CommandDirector
	// For now, return a placeholder response
	response := &CommandResponse{
		ResultCount: 1,
		Result:      fmt.Sprintf("Session invalidation for %s requires server context - use server.SessionManager.InvalidateSession()", sessionID),
	}

	return response, nil
}

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

		// Create new document with filtered fields
		filteredDoc := &models.Document{
			DocumentID: doc.DocumentID,
			Fields:     filteredFields,
			CreatedAt:  doc.CreatedAt,
			UpdatedAt:  doc.UpdatedAt,
		}

		filteredDocuments[docID] = filteredDoc
	}

	logger.Infof("Filtered %d documents to include only fields: %v", len(filteredDocuments), selectedFields)
	return filteredDocuments
}

// SelectTopDocuments handles SELECT TOP N DOCUMENTS FROM "bundle" queries
func SelectTopDocuments(commandParts []string, serviceManager ServiceManager, database *models.Database, logger *zap.SugaredLogger, startTime time.Time) (interface{}, error) {
	logger.Infof("Processing SELECT TOP command with %d parts: %v", len(commandParts), commandParts)

	// Expected syntax: SELECT TOP <number> DOCUMENTS FROM "<bundle_name>" [WHERE conditions] [ORDER BY field]
	if len(commandParts) < 5 || !strings.EqualFold(commandParts[3], "DOCUMENTS") || !strings.EqualFold(commandParts[4], "FROM") {
		return nil, fmt.Errorf("SELECT TOP requires the syntax 'SELECT TOP <number> DOCUMENTS FROM \"<bundle_name>\"'")
	}

	// Parse the number parameter
	topCount, err := strconv.Atoi(commandParts[2])
	if err != nil {
		return nil, fmt.Errorf("SELECT TOP requires a valid number, got: %s", commandParts[2])
	}
	if topCount <= 0 {
		return nil, fmt.Errorf("SELECT TOP number must be positive, got: %d", topCount)
	}

	logger.Infof("TOP count parsed as: %d", topCount)

	// Get the full command for parsing additional clauses
	fullCommand := strings.Join(commandParts, " ")

	// Check for ORDER BY or other clauses and delegate to appropriate handlers
	if strings.Contains(strings.ToUpper(fullCommand), "ORDER BY") {
		return SelectTopDocumentsWithOrderBy(fullCommand, topCount, serviceManager, database, logger, startTime)
	}

	// Handle basic TOP query without ORDER BY
	bundleName := strings.Trim(commandParts[5], "\"'")
	bundleName = strings.ReplaceAll(bundleName, "\"", "")
	bundleName = strings.ReplaceAll(bundleName, "'", "")

	logger.Infof("Bundle name parsed as: '%s'", bundleName)

	if !bndle.IsValidBundleName(bundleName) {
		return nil, fmt.Errorf("invalid bundle name: %s. Bundle names can only contain letters, numbers, underscores, and hyphens", bundleName)
	}

	// Get the bundle by name
	bundle, err := serviceManager.BundleService.GetBundleByName(database, bundleName)
	if err != nil {
		return nil, fmt.Errorf("error retrieving bundle '%s': %v", bundleName, err)
	}

	logger.Infof("Bundle retrieved successfully: %s", bundleName)
	if bundle.Documents != nil {
		logger.Infof("Bundle contains %d documents", len(*bundle.Documents))
	} else {
		logger.Warnf("Bundle.Documents is nil")
	}

	var documents map[string]*models.Document

	// Check for WHERE clause
	if len(commandParts) > 6 && strings.EqualFold(commandParts[6], "WHERE") {
		whereClause := strings.Join(commandParts[7:], " ")

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
		// No WHERE clause - load all documents using new page-based architecture
		logger.Infof("Loading all documents using page-based document loading")

		// Use GetDocumentsByFilter with empty filter to get all documents
		allDocs, err := serviceManager.BundleService.GetDocumentsByFilter(bundle, "")
		if err != nil {
			return nil, fmt.Errorf("error loading all documents: %v", err)
		}

		logger.Infof("Loaded %d documents from bundle using page-based loading", len(allDocs))

		// Convert slice to map for consistency
		documents = make(map[string]*models.Document)
		for _, doc := range allDocs {
			documents[doc.DocumentID] = doc
		}

		logger.Infof("Converted %d documents to result map", len(documents))
	}

	logger.Infof("Total documents before TOP limit: %d", len(documents))

	// Convert to slice and limit the results
	var documentSlice []*models.Document
	for _, doc := range documents {
		documentSlice = append(documentSlice, doc)
	}

	logger.Infof("Converted to slice - length: %d", len(documentSlice))

	// Apply TOP limit
	if topCount < len(documentSlice) {
		documentSlice = documentSlice[:topCount]
		logger.Infof("Applied TOP %d limit - result length: %d", topCount, len(documentSlice))
	} else {
		logger.Infof("TOP %d limit not applied - document count (%d) is less than or equal to limit", topCount, len(documentSlice))
	}

	// Convert back to map for consistent response format
	limitedDocuments := make(map[string]*models.Document)
	for _, doc := range documentSlice {
		limitedDocuments[doc.DocumentID] = doc
	}

	logger.Infof("Final result: SELECT TOP %d returned %d documents from bundle '%s'", topCount, len(limitedDocuments), bundleName)

	// Calculate execution time
	executionTime := float64(time.Since(startTime).Nanoseconds()) / 1e6 // Convert to milliseconds

	cmdResponse := &CommandResponse{
		ResultCount:     len(limitedDocuments),
		Result:          limitedDocuments,
		ExecutionTimeMS: executionTime,
	}

	logger.Infof("Returning CommandResponse with ResultCount: %d", cmdResponse.ResultCount)
	return cmdResponse, nil
}

// SelectTopDocumentsWithOrderBy handles SELECT TOP N DOCUMENTS FROM "bundle" ORDER BY queries
func SelectTopDocumentsWithOrderBy(fullCommand string, topCount int, serviceManager ServiceManager, database *models.Database, logger *zap.SugaredLogger, startTime time.Time) (interface{}, error) {
	logger.Infof("Processing SELECT TOP %d with ORDER BY query: %s", topCount, fullCommand)

	// Parse the ORDER BY query using existing parser
	selectQuery, err := queryparser.ParseSelectQueryWithOrder(fullCommand, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to parse SELECT TOP ORDER BY query: %w", err)
	}

	// Extract bundle name
	bundleName := selectQuery.FromBundle
	if !bndle.IsValidBundleName(bundleName) {
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
		// Extract WHERE clause from the original query string
		whereStart := strings.Index(strings.ToUpper(fullCommand), "WHERE")
		orderByStart := strings.Index(strings.ToUpper(fullCommand), "ORDER BY")

		var whereClause string
		if whereStart >= 0 {
			whereEnd := len(fullCommand)
			if orderByStart > whereStart {
				whereEnd = orderByStart
			}
			whereClause = strings.TrimSpace(fullCommand[whereStart+5 : whereEnd])
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
		if bundle.Documents != nil {
			for k, v := range *bundle.Documents {
				docCopy := v
				documents[k] = &docCopy
			}
		}
	}

	// Sort the documents according to the ORDER BY clause
	sorter := queryparser.NewDocumentSorter(selectQuery.OrderBy, logger)
	sortedDocuments, err := sorter.SortDocumentMap(documents)
	if err != nil {
		return nil, fmt.Errorf("error sorting documents: %v", err)
	}

	// Apply TOP limit to sorted documents
	if topCount < len(sortedDocuments) {
		sortedDocuments = sortedDocuments[:topCount]
	}

	// Convert sorted slice back to map for response consistency
	limitedDocuments := make(map[string]*models.Document)
	for _, doc := range sortedDocuments {
		limitedDocuments[doc.DocumentID] = doc
	}

	// Apply field selection if specific fields were requested
	if len(selectQuery.SelectFields) > 0 {
		limitedDocuments = filterDocumentFields(limitedDocuments, selectQuery.SelectFields, logger)
	}

	logger.Infof("SELECT TOP %d ORDER BY query executed successfully, returned %d sorted documents", topCount, len(limitedDocuments))

	cmdResponse := &CommandResponse{
		ResultCount: len(limitedDocuments),
		Result:      limitedDocuments,
	}
	return cmdResponse, nil
}

func SelectDocuments(commandParts []string, serviceManager ServiceManager, database *models.Database, logger *zap.SugaredLogger, startTime time.Time) (interface{}, error) {
	// First, check if this is a JOIN query by examining the full command
	fullCommand := strings.Join(commandParts, " ")

	// Detect JOIN queries
	if strings.Contains(strings.ToUpper(fullCommand), "JOIN") {
		return SelectDocumentsWithJoin(fullCommand, serviceManager, database, logger, startTime)
	}

	// Detect GROUP BY queries
	if strings.Contains(strings.ToUpper(fullCommand), "GROUP BY") {
		return SelectDocumentsWithGroupBy(fullCommand, serviceManager, database, logger, startTime)
	}

	// Detect ORDER BY queries
	if strings.Contains(strings.ToUpper(fullCommand), "ORDER BY") {
		return SelectDocumentsWithOrderBy(fullCommand, serviceManager, database, logger, startTime)
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

	if !bndle.IsValidBundleName(bundleName) {
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
		// No WHERE clause - load all documents using new page-based architecture
		logger.Infof("Loading all documents using page-based document loading")

		// Use GetDocumentsByFilter with empty filter to get all documents
		allDocs, err := serviceManager.BundleService.GetDocumentsByFilter(bundle, "")
		if err != nil {
			return nil, fmt.Errorf("error loading all documents: %v", err)
		}

		logger.Infof("Loaded %d documents from bundle using page-based loading", len(allDocs))

		// Convert slice to map for consistency
		documents = make(map[string]*models.Document)
		for _, doc := range allDocs {
			documents[doc.DocumentID] = doc
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

	// Calculate execution time
	executionTime := float64(time.Since(startTime).Nanoseconds()) / 1e6 // Convert to milliseconds

	cmdResponse := &CommandResponse{
		ResultCount:     len(documents),
		Result:          documents,
		ExecutionTimeMS: executionTime,
	}
	return cmdResponse, nil
}

// SelectDocumentCount handles SELECT COUNT FROM "bundle" queries with optional WHERE, ORDER BY, and GROUP BY clauses
// This function follows SyndrDB comprehensive error handling practices and implements efficient document counting
// without needing to load full document content when possible. It supports the same filtering and grouping
// capabilities as other SELECT operations but returns only the count of matching documents.
//
// Supported syntax:
//   - SELECT COUNT FROM "bundle"
//   - SELECT COUNT FROM "bundle" WHERE conditions
//   - SELECT COUNT FROM "bundle" ORDER BY field
//   - SELECT COUNT FROM "bundle" GROUP BY field
//   - SELECT COUNT FROM "bundle" WHERE conditions ORDER BY field
//   - SELECT COUNT FROM "bundle" WHERE conditions GROUP BY field
//
// Future enhancement: Will support mixed field lists like "SELECT field1, COUNT FROM bundle GROUP BY field1"
// when accompanied by appropriate GROUP BY clauses.
//
// Parameters:
//   - commandParts: The parsed command parts from the original query
//   - serviceManager: Service manager containing all database services
//   - database: Target database context
//   - logger: Logger for debugging and error reporting
//
// Returns:
//   - interface{}: CommandResponse containing the count result
//   - error: Any error that occurred during processing
func SelectDocumentCount(commandParts []string, serviceManager ServiceManager, database *models.Database, logger *zap.SugaredLogger, startTime time.Time) (interface{}, error) {
	logger.Infof("Processing SELECT COUNT query: %v", commandParts)

	// Reconstruct full command for advanced parsing
	fullCommand := strings.Join(commandParts, " ")

	// Validate basic syntax: SELECT COUNT FROM "bundle"
	if len(commandParts) < 4 || !strings.EqualFold(commandParts[2], "FROM") {
		return nil, fmt.Errorf("SELECT COUNT requires the syntax 'SELECT COUNT FROM \"<bundle_name>\"'")
	}

	// Detect and delegate to specialized handlers for complex queries
	upperCommand := strings.ToUpper(fullCommand)

	// Handle GROUP BY COUNT queries using existing GROUP BY infrastructure
	if strings.Contains(upperCommand, "GROUP BY") {
		logger.Infof("Delegating COUNT with GROUP BY to GROUP BY handler")
		return SelectDocumentsWithGroupBy(fullCommand, serviceManager, database, logger, startTime)
	}

	// Handle ORDER BY COUNT queries using existing ORDER BY infrastructure
	if strings.Contains(upperCommand, "ORDER BY") {
		logger.Infof("Delegating COUNT with ORDER BY to ORDER BY handler (will return count only)")
		// For ORDER BY, we'll process normally but return only count
		result, err := SelectDocumentsWithOrderBy(fullCommand, serviceManager, database, logger, startTime)
		if err != nil {
			return nil, err
		}

		// Extract count from the result and return count-only response
		if cmdResp, ok := result.(*CommandResponse); ok {
			countResponse := &CommandResponse{
				ResultCount: 1,
				Result:      cmdResp.ResultCount,
			}
			logger.Infof("ORDER BY COUNT query returned count: %d", cmdResp.ResultCount)
			return countResponse, nil
		}
		return nil, fmt.Errorf("unexpected result type from ORDER BY handler")
	}

	// Handle basic COUNT queries (with optional WHERE clause)
	bundleName := strings.Trim(commandParts[3], "\"'")
	bundleName = strings.ReplaceAll(bundleName, "\"", "")
	bundleName = strings.ReplaceAll(bundleName, "'", "")
	bundleName = strings.ReplaceAll(bundleName, "\u201C", "") // Handle unicode left double quote
	bundleName = strings.ReplaceAll(bundleName, "\u201D", "") // Handle unicode right double quote

	if !bndle.IsValidBundleName(bundleName) {
		return nil, fmt.Errorf("invalid bundle name: %s. Bundle names can only contain letters, numbers, underscores, and hyphens", bundleName)
	}

	logger.Infof("Processing COUNT query for bundle: %s", bundleName)

	// Get the bundle by name
	bundle, err := serviceManager.BundleService.GetBundleByName(database, bundleName)
	if err != nil {
		return nil, fmt.Errorf("error retrieving bundle '%s': %v", bundleName, err)
	}

	var documentCount int

	// Check for WHERE clause
	if len(commandParts) > 4 && strings.EqualFold(commandParts[4], "WHERE") {
		whereClause := strings.Join(commandParts[5:], " ")
		logger.Infof("Processing COUNT query with WHERE clause: %s", whereClause)

		// Create execution planner for optimized counting
		planner := planner.NewQueryPlannerWithService(logger, serviceManager.BundleService)

		// Create execution plan
		plan, err := planner.CreateExecutionPlan(bundle, whereClause)
		if err != nil {
			logger.Warnf("Failed to create execution plan for COUNT, falling back to full scan: %v", err)
			// Fallback to filtering and counting
			filteredDocs, err := queryparser.FilterDocuments(bundle, whereClause, logger)
			if err != nil {
				return nil, fmt.Errorf("error filtering documents for COUNT: %v", err)
			}
			documentCount = len(filteredDocs)
		} else {
			// Execute the plan and count results
			logger.Infof("Executing COUNT plan with indexes: %v", plan.IndexesUsed)
			documents, err := plan.RootNode.Execute()
			if err != nil {
				return nil, fmt.Errorf("error executing COUNT query plan: %v", err)
			}
			documentCount = len(documents)
		}
	} else {
		// No WHERE clause - count all documents efficiently
		logger.Infof("Counting all documents in bundle using optimized method")

		// Use efficient document counting without loading full content
		allDocs, err := serviceManager.BundleService.GetDocumentsByFilter(bundle, "")
		if err != nil {
			return nil, fmt.Errorf("error counting all documents: %v", err)
		}
		documentCount = len(allDocs)
	}

	logger.Infof("COUNT query completed: %d documents found in bundle '%s'", documentCount, bundleName)

	// Calculate execution time
	executionTime := float64(time.Since(startTime).Nanoseconds()) / 1e6 // Convert to milliseconds

	// Return count-only response
	cmdResponse := &CommandResponse{
		ResultCount:     1,
		Result:          documentCount,
		ExecutionTimeMS: executionTime,
	}
	return cmdResponse, nil
}

// SelectDocumentsWithJoin handles SELECT queries with JOIN clauses
func SelectDocumentsWithJoin(query string, serviceManager ServiceManager, database *models.Database, logger *zap.SugaredLogger, startTime time.Time) (interface{}, error) {
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

	// Apply field selection if specific fields were requested
	if len(joinQuery.SelectFields) > 0 {
		documents = filterDocumentFields(documents, joinQuery.SelectFields, logger)
	}

	logger.Infof("JOIN query executed successfully, returned %d documents", len(documents))

	// Calculate execution time
	executionTime := float64(time.Since(startTime).Nanoseconds()) / 1e6 // Convert to milliseconds

	cmdResponse := &CommandResponse{
		ResultCount:     len(documents),
		Result:          documents,
		ExecutionTimeMS: executionTime,
	}
	return cmdResponse, nil
}

// SelectDocumentsWithOrderBy handles SELECT queries with ORDER BY clauses
func SelectDocumentsWithOrderBy(query string, serviceManager ServiceManager, database *models.Database, logger *zap.SugaredLogger, startTime time.Time) (interface{}, error) {
	logger.Infof("Processing ORDER BY query: %s", query)

	// Parse the ORDER BY query
	selectQuery, err := queryparser.ParseSelectQueryWithOrder(query, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to parse ORDER BY query: %w", err)
	}

	// Extract bundle name
	bundleName := selectQuery.FromBundle
	if !bndle.IsValidBundleName(bundleName) {
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
		if bundle.Documents != nil {
			for k, v := range *bundle.Documents {
				docCopy := v
				documents[k] = &docCopy
			}
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

	// Apply field selection if specific fields were requested
	if len(selectQuery.SelectFields) > 0 {
		resultMap = filterDocumentFields(resultMap, selectQuery.SelectFields, logger)
	}

	// Calculate execution time
	executionTime := float64(time.Since(startTime).Nanoseconds()) / 1e6 // Convert to milliseconds

	cmdResponse := &CommandResponse{
		ResultCount:     len(resultMap),
		Result:          resultMap,
		ExecutionTimeMS: executionTime,
	}
	return cmdResponse, nil
}

// SelectDocumentsWithGroupBy handles SELECT queries with GROUP BY clauses
func SelectDocumentsWithGroupBy(query string, serviceManager ServiceManager, database *models.Database, logger *zap.SugaredLogger, startTime time.Time) (interface{}, error) {
	logger.Infof("Processing GROUP BY query: %s", query)

	// Parse the GROUP BY query
	groupByQuery, err := queryparser.ParseSelectQueryWithGroupBy(query, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to parse GROUP BY query: %w", err)
	}

	// Extract bundle name
	bundleName := groupByQuery.FromBundle
	if !bndle.IsValidBundleName(bundleName) {
		return nil, fmt.Errorf("invalid bundle name: %s. Bundle names can only contain letters, numbers, underscores, and hyphens", bundleName)
	}

	// Get the bundle by name
	bundleObj, err := serviceManager.BundleService.GetBundleByName(database, bundleName)
	if err != nil {
		return nil, fmt.Errorf("error retrieving bundle '%s': %v", bundleName, err)
	}

	// Get all documents from the bundle
	allDocuments := make(map[string]*models.Document)
	for k, v := range *bundleObj.Documents {
		docCopy := v
		allDocuments[k] = &docCopy
	}

	// Create and execute GROUP BY executor
	groupByExecutor := executor.NewGroupByExecutor(groupByQuery, logger)

	// Execute the GROUP BY query
	resultMap, err := groupByExecutor.Execute(allDocuments)
	if err != nil {
		return nil, fmt.Errorf("error executing GROUP BY query: %v", err)
	}

	logger.Infof("GROUP BY query returned %d groups", len(resultMap))

	// Calculate execution time
	executionTime := float64(time.Since(startTime).Nanoseconds()) / 1e6 // Convert to milliseconds

	cmdResponse := &CommandResponse{
		ResultCount:     len(resultMap),
		Result:          resultMap,
		ExecutionTimeMS: executionTime,
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

// ShowRateLimit displays current rate limiting statistics
func ShowRateLimit(command string, logger *zap.SugaredLogger, serviceManager ServiceManager) (interface{}, error) {
	response := map[string]interface{}{
		"command": "SHOW RATE LIMIT",
		"message": "Rate limiting statistics",
		"status":  "success",
		"data": map[string]interface{}{
			"note":        "Rate limiting is active. Use server logs for detailed statistics.",
			"description": "This command shows rate limiting is enabled and protecting the server from abuse.",
		},
	}

	return response, nil
}

// ShowDatabases shows all available databases
// Syntax: SHOW DATABASES
func ShowDatabases(command string, logger *zap.SugaredLogger, serviceManager ServiceManager) (*CommandResponse, error) {
	logger.Infof("Processing SHOW DATABASES command: %s", command)

	// Get the list of databases from the database service (loaded in memory)
	databases := serviceManager.DatabaseService.ListDatabases()

	// Extract database names for the response
	databaseNames := make([]string, len(databases))
	for i, db := range databases {
		databaseNames[i] = db.Name
	}

	// Optional: Check catalog consistency for debugging (if catalog service is available)
	if serviceManager.InternalCatalogService != nil {
		catalogDatabases, err := serviceManager.InternalCatalogService.ListAllDatabasesInCatalog()
		if err != nil {
			logger.Warnf("Warning: Could not verify catalog consistency: %v", err)
		} else {
			catalogNames := make([]string, 0, len(catalogDatabases))
			for _, dbInfo := range catalogDatabases {
				if dbName, ok := dbInfo["Name"].(string); ok {
					catalogNames = append(catalogNames, dbName)
				}
			}

			// Check for inconsistencies
			loadedSet := make(map[string]bool)
			for _, name := range databaseNames {
				loadedSet[name] = true
			}

			catalogSet := make(map[string]bool)
			for _, name := range catalogNames {
				catalogSet[name] = true
			}

			// Find databases in catalog but not loaded
			for _, name := range catalogNames {
				if !loadedSet[name] {
					logger.Warnf("Database '%s' is in catalog but not loaded in memory", name)
				}
			}

			// Find databases loaded but not in catalog
			for _, name := range databaseNames {
				if !catalogSet[name] {
					logger.Warnf("Database '%s' is loaded but not found in catalog", name)
				}
			}

			logger.Debugf("Catalog consistency check: %d loaded, %d in catalog", len(databaseNames), len(catalogNames))
		}
	}

	response := &CommandResponse{
		ResultCount: len(databaseNames),
		Result:      databaseNames,
	}

	logger.Infof("Found %d databases", len(databaseNames))
	return response, nil
}

// ShowBundles shows all bundles in a specific database
// Syntax: SHOW BUNDLES [FOR "<DATABASE_NAME>"]
func ShowBundles(command string, database *models.Database, logger *zap.SugaredLogger, serviceManager ServiceManager) (*CommandResponse, error) {
	logger.Infof("Processing SHOW BUNDLES command: %s", command)

	var targetDatabase *models.Database
	var targetDatabaseID string
	var targetDatabaseName string

	// Parse command to check for "FOR <DATABASE_NAME>" syntax
	commandLower := strings.ToLower(command)
	if strings.Contains(commandLower, " for ") {
		// Extract database name from "SHOW BUNDLES FOR "<DATABASE_NAME>""
		databaseName, err := parseDatabaseNameFromShowBundlesFor(command)
		if err != nil {
			return nil, fmt.Errorf("failed to parse database name from command: %w", err)
		}

		// First check if the database exists in the loaded databases (consistent with USE DATABASE)
		targetDatabase, err = serviceManager.DatabaseService.GetDatabaseByName(databaseName)
		if err != nil {
			return nil, fmt.Errorf("database '%s' not found in system: %w", databaseName, err)
		}

		// Verify database is also in catalog (for additional validation, similar to UseDatabase)
		if serviceManager.InternalCatalogService != nil {
			dbDocument, dbErr := serviceManager.InternalCatalogService.GetDatabaseFromCatalogByName(databaseName)
			if dbErr != nil {
				return nil, fmt.Errorf("database '%s' not found in catalog: %w", databaseName, dbErr)
			}
			if dbDocument == nil {
				logger.Warnf("Warning: Database '%s' is loaded but not found in catalog", databaseName)
			}

			// allDatabases, catalogErr := serviceManager.InternalCatalogService.ListAllDatabasesInCatalog()
			// if catalogErr != nil {
			// 	logger.Warnf("Warning: Failed to verify database in catalog: %v", catalogErr)
			// } else {
			// 	// Check if the requested database exists in catalog
			// 	var foundInCatalog bool
			// 	for _, dbInfo := range allDatabases {
			// 		if dbName, ok := dbInfo["Name"].(string); ok && dbName == databaseName {
			// 			foundInCatalog = true
			// 			break
			// 		}
			// 	}

			// 	if !foundInCatalog {
			// 		logger.Warnf("Warning: Database '%s' is loaded but not found in catalog", databaseName)
			// 	}
			// }
		}

		targetDatabaseID = targetDatabase.DatabaseID
		targetDatabaseName = targetDatabase.Name
		logger.Infof("Found database '%s' with ID: %s", databaseName, targetDatabaseID)
	} else {
		// Original syntax - use current database and show bundles from catalog for that database
		if database == nil {
			return nil, fmt.Errorf("no database selected: use 'USE database_name' to select a database first")
		}

		targetDatabaseID = database.DatabaseID
		targetDatabaseName = database.Name
		logger.Infof("Using current database '%s' with ID: %s", database.Name, targetDatabaseID)
	}

	// For "SHOW BUNDLES FOR database" syntax, use catalog approach
	// Get bundles from the catalog for the target database ID
	if serviceManager.InternalCatalogService == nil {
		return nil, fmt.Errorf("internal catalog service is not available")
	}

	// allBundles, err := serviceManager.InternalCatalogService.ListAllBundlesInCatalog()
	// if err != nil {
	// 	return nil, fmt.Errorf("failed to retrieve bundles from catalog: %w", err)
	// }
	allBundles, err := serviceManager.InternalCatalogService.GetBundlesFromCatalogByDatabaseName(targetDatabaseName)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve bundles for database '%s' from catalog: %w", targetDatabaseName, err)
	}
	//logger.Debugf("DEBUG DEBUG DEBUG Retrieved %d bundles from catalog for database '%s'", len(*allBundles), targetDatabaseName)
	// Filter bundles for the target database
	// var bundleInfos []models.Document
	// for _, bundleInfo := range *allBundles {
	// 	// Type assert bundleInfo to map[string]interface{}

	// 	if bundleMap, ok := bundleInfo.(map[string]interface{}); ok {
	// 		//if dbID, ok := bundleMap["DatabaseID"].(string); ok && dbID == targetDatabaseID {
	// 		// Create a clean response with relevant fields
	// 		cleanBundleInfo := make(map[string]interface{})
	// 		cleanBundleInfo["Name"] = bundleMap["Name"]
	// 		cleanBundleInfo["BundleID"] = bundleMap["BundleID"]
	// 		cleanBundleInfo["DatabaseID"] = bundleMap["DatabaseID"]
	// 		cleanBundleInfo["DatabaseName"] = bundleMap["DatabaseName"]
	// 		bundleInfos = append(bundleInfos, cleanBundleInfo)
	// 		//}
	// 	}
	// }

	response := &CommandResponse{
		ResultCount: len(*allBundles),
		Result:      *allBundles,
	}

	if targetDatabase != nil {
		logger.Infof("Found %d bundles in database %s from system catalog", len(*allBundles), targetDatabase.Name)
	} else {
		logger.Infof("Found %d bundles for database ID %s from system catalog", len(*allBundles), targetDatabaseID)
	}
	return response, nil
}

// ShowBundle handles the "SHOW BUNDLE "<BUNDLE_NAME>";" command
func ShowBundle(command string, database *models.Database, logger *zap.SugaredLogger, serviceManager ServiceManager) (*CommandResponse, error) {
	logger.Infof("Processing SHOW BUNDLE command: %s", command)

	if database == nil {
		return nil, fmt.Errorf("no database selected: use 'USE database_name' to select a database first")
	}

	// Parse the bundle name from the command
	// Expected format: SHOW BUNDLE "<BUNDLE_NAME>";
	bundleName, err := parseBundleNameFromShowCommand(command)
	if err != nil {
		return nil, fmt.Errorf("failed to parse bundle name from command: %w", err)
	}

	// Get the bundle metadata (without documents) from the bundle service
	bundle, err := serviceManager.BundleService.GetBundleMetadata(database, bundleName)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve bundle '%s': %w", bundleName, err)
	}

	// Convert bundle to a response-friendly format
	bundleInfo := map[string]interface{}{
		"Name":           bundle.Name,
		"BundleID":       bundle.BundleID,
		"Description":    bundle.Description,
		"CreatedBy":      bundle.CreatedBy,
		"CreatedAt":      bundle.CreatedAt,
		"UpdatedAt":      bundle.UpdatedAt,
		"Permissions":    bundle.Permissions,
		"PageCount":      bundle.PageCount,
		"TotalDocuments": bundle.TotalDocuments,
	}

	// Always include document structure information (even if empty)
	bundleInfo["DocumentStructure"] = bundle.DocumentStructure

	// Debug logging to see what we have
	logger.Debugf("Bundle DocumentStructure: %+v", bundle.DocumentStructure)
	logger.Debugf("FieldDefinitions count: %d", len(bundle.DocumentStructure.FieldDefinitions))

	// Include indexes information if available
	if len(bundle.Indexes) > 0 {
		bundleInfo["Indexes"] = bundle.Indexes
	}

	// Include relationships information if available
	if len(bundle.Relationships) > 0 {
		bundleInfo["Relationships"] = bundle.Relationships
	}

	// Include constraints information if available
	if len(bundle.Constraints) > 0 {
		bundleInfo["Constraints"] = bundle.Constraints
	}

	response := &CommandResponse{
		ResultCount: 1,
		Result:      bundleInfo,
	}

	logger.Infof("Retrieved metadata for bundle '%s' in database %s", bundleName, database.Name)
	return response, nil
}

// ShowUsers shows all documents in the Users bundle from the primary database
// Syntax: SHOW USERS;
func ShowUsers(command string, database *models.Database, logger *zap.SugaredLogger, serviceManager ServiceManager) (*CommandResponse, error) {
	logger.Infof("Processing SHOW USERS command: %s", command)

	// Always use the primary database for system catalogs like Users
	primaryDB := serviceManager.DatabaseService.Databases["primary"]
	if primaryDB == nil {
		return nil, fmt.Errorf("primary database not found - system catalogs unavailable")
	}

	// Get the Users bundle from the primary database
	usersBundle, err := serviceManager.BundleService.GetBundleByName(primaryDB, "Users")
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve Users bundle: %w", err)
	}

	// Get all documents from the Users bundle (empty WHERE clause returns all)
	userDocs, err := serviceManager.BundleService.GetDocumentsByFilter(usersBundle, "")
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve user documents: %w", err)
	}

	// Convert documents to response format
	var users []map[string]interface{}
	for _, doc := range userDocs {
		user := make(map[string]interface{})

		// Extract relevant user fields from the document
		for fieldName, field := range doc.Fields {
			// Skip sensitive fields like password hash
			if fieldName != "PasswordHash" {
				user[fieldName] = field.Value
			}
		}

		users = append(users, user)
	}

	response := &CommandResponse{
		ResultCount: len(users),
		Result:      users,
	}

	logger.Infof("Retrieved %d users from Users bundle", len(users))

	return response, nil
}

// parseBundleNameFromShowCommand extracts the bundle name from SHOW BUNDLE "<NAME>" command
func parseBundleNameFromShowCommand(command string) (string, error) {
	// Expected format: SHOW BUNDLE "<BUNDLE_NAME>";
	// Find the quoted bundle name
	re := regexp.MustCompile(`(?i)show\s+bundle\s+"([^"]+)"`)
	matches := re.FindStringSubmatch(command)

	if len(matches) < 2 {
		return "", fmt.Errorf("invalid SHOW BUNDLE command format. Expected: SHOW BUNDLE \"<bundle_name>\";")
	}

	return matches[1], nil
}

// parseDatabaseNameFromShowBundlesFor extracts the database name from SHOW BUNDLES FOR "<NAME>" command
func parseDatabaseNameFromShowBundlesFor(command string) (string, error) {
	// Expected format: SHOW BUNDLES FOR "<DATABASE_NAME>";
	// Find the quoted database name
	re := regexp.MustCompile(`(?i)show\s+bundles\s+for\s+"([^"]+)"`)
	matches := re.FindStringSubmatch(command)

	if len(matches) < 2 {
		return "", fmt.Errorf("invalid SHOW BUNDLES FOR command format. Expected: SHOW BUNDLES FOR \"<database_name>\";")
	}

	return matches[1], nil
}

// UseDatabase handles the USE "<DATABASE_NAME>"; command to switch the current database context
func UseDatabase(command string, logger *zap.SugaredLogger, serviceManager ServiceManager) (*CommandResponse, error) {
	logger.Infof("Processing USE command: %s", command)

	// Parse the database name from the USE command
	databaseName, err := parseDatabaseNameFromUse(command)
	if err != nil {
		return nil, fmt.Errorf("failed to parse database name from USE command: %w", err)
	}

	// Check if the database exists in the loaded databases (consistent with SHOW DATABASES)
	database, err := serviceManager.DatabaseService.GetDatabaseByName(databaseName)
	if err != nil {
		return nil, fmt.Errorf("database '%s' not found in system: %w", databaseName, err)
	}

	// Verify database is also in catalog (for additional validation)
	if serviceManager.InternalCatalogService != nil {
		allDatabases, catalogErr := serviceManager.InternalCatalogService.ListAllDatabasesInCatalog()
		if catalogErr != nil {
			logger.Warnf("Warning: Failed to verify database in catalog: %v", catalogErr)
		} else {
			// Check if the requested database exists in catalog
			var foundInCatalog bool
			for _, dbInfo := range allDatabases {
				if dbName, ok := dbInfo["Name"].(string); ok && dbName == databaseName {
					foundInCatalog = true
					break
				}
			}

			if !foundInCatalog {
				logger.Warnf("Warning: Database '%s' is loaded but not found in catalog", databaseName)
			}
		}
	}

	logger.Infof("Successfully validated database '%s' exists (ID: %s)", databaseName, database.DatabaseID)

	response := &CommandResponse{
		ResultCount: 1,
		Result:      fmt.Sprintf("Database context switched to '%s'.", databaseName),
	}
	return response, nil
}

// AttachDatabase handles the ATTACH "<file_path>" "<database_name>" command
func AttachDatabase(command string, logger *zap.SugaredLogger, serviceManager ServiceManager) (*CommandResponse, error) {
	logger.Infof("Processing ATTACH DATABASE command: %s", command)

	// Parse the file path and database name from the command
	// Expected format: ATTACH "<file_path>" "<database_name>";
	filePath, databaseName, err := parseAttachDatabaseCommand(command)
	if err != nil {
		return nil, fmt.Errorf("failed to parse ATTACH command: %w", err)
	}

	// Check if the database file exists
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		return nil, fmt.Errorf("database file does not exist: %s", filePath)
	}

	// Check if database already exists in catalog (optional - continue if catalog is corrupted)
	catalogAvailable := false
	if serviceManager.InternalCatalogService != nil {
		allDatabases, catalogErr := serviceManager.InternalCatalogService.ListAllDatabasesInCatalog()
		if catalogErr != nil {
			logger.Warnf("Warning: Failed to check existing databases in catalog (continuing without catalog check): %v", catalogErr)
		} else {
			catalogAvailable = true
			// Check if the database already exists in catalog
			for _, dbInfo := range allDatabases {
				if dbName, ok := dbInfo["Name"].(string); ok && dbName == databaseName {
					return nil, fmt.Errorf("database '%s' already exists in system catalog", databaseName)
				}
			}
		}
	}

	// Generate a new database ID
	databaseID := generateDatabaseID()

	// Create database entry for catalog
	newDatabase := &models.Database{
		DatabaseID:    databaseID,
		Name:          databaseName,
		Description:   fmt.Sprintf("Database attached from file: %s", filePath),
		Bundles:       make(map[string]models.Bundle),
		DataDirectory: filepath.Dir(filePath),
	}

	// Add database to catalog (only if catalog is available)
	if catalogAvailable && serviceManager.InternalCatalogService != nil {
		err = serviceManager.InternalCatalogService.AddDatabaseToCatalog(newDatabase)
		if err != nil {
			logger.Warnf("Warning: Failed to add database to catalog (continuing with in-memory only): %v", err)
		} else {
			logger.Infof("Successfully added database '%s' to catalog", databaseName)
		}
	}

	// Add the database to the in-memory service without creating files
	// since the files already exist
	serviceManager.DatabaseService.Databases[databaseName] = newDatabase

	// Discover and attach bundles from the same directory
	bundlesAdded := 0
	dbDir := filepath.Dir(filePath)

	// Look for .bnd files in the same directory
	bundleFiles, err := filepath.Glob(filepath.Join(dbDir, "*.bnd"))
	if err != nil {
		logger.Warnf("Failed to scan for bundle files: %v", err)
	} else {
		for _, bundleFile := range bundleFiles {
			bundleName := strings.TrimSuffix(filepath.Base(bundleFile), ".bnd")

			// Skip system bundles that might conflict
			if bundleName == "Databases" || bundleName == "Users" || bundleName == "Bundles" {
				continue
			}

			// Check if bundle already exists in catalog (optional check)
			bundleExists := false
			if catalogAvailable && serviceManager.InternalCatalogService != nil {
				allBundles, bundleErr := serviceManager.InternalCatalogService.ListAllBundlesInCatalog()
				if bundleErr == nil {
					for _, bundleInfo := range allBundles {
						if bundleName2, ok := bundleInfo["Name"].(string); ok && bundleName2 == bundleName {
							// Check if it's for this database (use a database-scoped check)
							bundleExists = true
							break
						}
					}
				} else {
					logger.Warnf("Warning: Failed to check existing bundles in catalog for '%s': %v", bundleName, bundleErr)
				}
			}

			if !bundleExists {
				// Create bundle entry for catalog - referencing existing files
				bundleID := generateBundleID()
				newBundle := &models.Bundle{
					BundleID:    bundleID,
					Name:        bundleName,
					Description: fmt.Sprintf("Bundle discovered from file: %s", bundleFile),
					CreatedAt:   time.Now(),
					UpdatedAt:   time.Now(),
					Database:    newDatabase, // Set the database reference
				}

				// Add bundle to the database's bundles map
				newDatabase.Bundles[bundleName] = *newBundle

				// Add bundle to catalog (only if catalog is available)
				if catalogAvailable && serviceManager.InternalCatalogService != nil {
					bundleErr := serviceManager.InternalCatalogService.RegisterBundleInCatalog(newBundle)
					if bundleErr != nil {
						logger.Warnf("Warning: Failed to add bundle '%s' to catalog (continuing with in-memory only): %v", bundleName, bundleErr)
					} else {
						logger.Infof("Added bundle '%s' to catalog", bundleName)
					}
				}
				bundlesAdded++
				logger.Infof("Registered bundle '%s' from file '%s'", bundleName, bundleFile)
			}
		}
	}

	response := &CommandResponse{
		ResultCount: 1,
		Result: map[string]interface{}{
			"DatabaseName": databaseName,
			"DatabaseID":   databaseID,
			"FilePath":     filePath,
			"BundlesAdded": bundlesAdded,
			"Status":       "Database attached successfully",
		},
	}

	logger.Infof("Successfully attached database '%s' from file '%s' with %d bundles", databaseName, filePath, bundlesAdded)
	return response, nil
}

// parseAttachDatabaseCommand parses the ATTACH DATABASE command to extract file path and database name
func parseAttachDatabaseCommand(command string) (string, string, error) {
	// Expected format: ATTACH DATABASE "<file_path>" "<database_name>";
	// Use regex to extract both quoted strings after DATABASE keyword
	re := regexp.MustCompile(`(?i)attach\s+database\s+"([^"]+)"\s+"([^"]+)"`)
	matches := re.FindStringSubmatch(command)

	if len(matches) < 3 {
		return "", "", fmt.Errorf("invalid ATTACH DATABASE command format. Expected: ATTACH DATABASE \"<file_path>\" \"<database_name>\";")
	}

	filePath := matches[1]
	databaseName := matches[2]

	return filePath, databaseName, nil
}

// generateDatabaseID generates a unique database ID
func generateDatabaseID() string {
	return fmt.Sprintf("db_%d", time.Now().UnixNano())
}

// generateBundleID generates a unique bundle ID
func generateBundleID() string {
	return fmt.Sprintf("bnd_%d", time.Now().UnixNano())
}

// parseDatabaseNameFromUse extracts the database name from USE "<DATABASE_NAME>"; command
func parseDatabaseNameFromUse(command string) (string, error) {
	// Expected format: USE "<DATABASE_NAME>";
	// Find the quoted database name
	re := regexp.MustCompile(`(?i)use\s+"([^"]+)"`)
	matches := re.FindStringSubmatch(command)

	if len(matches) < 2 {
		return "", fmt.Errorf("invalid USE command format. Expected: USE \"<database_name>\";")
	}

	return matches[1], nil
}
