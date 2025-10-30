package server

import (
	"fmt"
	"strings"
	"syndrdb/src/internal/domain/models"
	"time"

	"go.uber.org/zap"
)

// AddUser processes the ADD USER command
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

// To be populated during refactoring
