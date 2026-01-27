package server

import (
	"fmt"
	"strings"
	"syndrdb/src/internal/domain/models"
	"syndrdb/src/pkg/errors"
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
		return nil, errors.New(errors.ERR_VALIDATION_SYNTAX,
			"invalid ADD USER syntax: expected 'ADD USER username WITH PASSWORD password'",
			errors.LayerCommand)
	}

	if !strings.EqualFold(parts[0], "ADD") || !strings.EqualFold(parts[1], "USER") {
		return nil, errors.New(errors.ERR_VALIDATION_SYNTAX,
			"invalid ADD USER command format", errors.LayerCommand)
	}

	if !strings.EqualFold(parts[3], "WITH") || !strings.EqualFold(parts[4], "PASSWORD") {
		return nil, errors.New(errors.ERR_VALIDATION_SYNTAX,
			"invalid ADD USER syntax: expected 'WITH PASSWORD'", errors.LayerCommand)
	}

	username := parts[2]
	password := strings.Trim(parts[5], "'\"") // Remove quotes from password

	// Get the Primary database
	primaryDB, err := serviceManager.DatabaseService.GetDatabaseByName("primary")
	if err != nil {
		return nil, errors.WrapWithMessage(err, errors.ERR_NOT_FOUND_DATABASE,
			"primary database not found", errors.LayerCommand)
	}

	// Find the Users bundle
	usersBundle, err := serviceManager.BundleService.GetBundleByName(primaryDB, "Users")
	if err != nil {
		return nil, errors.WrapWithMessage(err, errors.ERR_NOT_FOUND_BUNDLE,
			"Users bundle not found in Primary database", errors.LayerCommand)
	}

	// Check if user already exists
	existingDocs, err := serviceManager.BundleService.GetDocumentsByFilter(usersBundle, "", nil)
	if err != nil {
		return nil, errors.WrapWithMessage(err, errors.ERR_INTERNAL_QUERY,
			"failed to retrieve user documents", errors.LayerCommand)
	}
	for _, doc := range existingDocs {
		if usernameField, ok := doc.Fields["Username"]; ok {
			if str, ok := usernameField.Value.AsString(); ok && str == username {
				return nil, errors.New(errors.ERR_VALIDATION_CONSTRAINT,
					fmt.Sprintf("user '%s' already exists", username),
					errors.LayerCommand).WithContext("username", username)
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
			"DocumentID": {Name: "DocumentID", Value: models.NewStringValue(userID)},
			"UserID":     {Name: "UserID", Value: models.NewStringValue(userID)},
			"Username":   {Name: "Username", Value: models.NewStringValue(username)},
			"Password":   {Name: "Password", Value: models.NewStringValue(hashedPassword)},
			"CreatedAt":  {Name: "CreatedAt", Value: models.NewStringValue(time.Now().Format(time.RFC3339))},
		},
	}

	// Add the user document to the Users bundle
	err = serviceManager.BundleService.AddDocumentToBundleByStruct(primaryDB, usersBundle, &userDoc)
	if err != nil {
		return nil, errors.WrapWithMessage(err, errors.ERR_INTERNAL_STORAGE,
			"failed to add user document to Users bundle", errors.LayerCommand)
	}

	logger.Infof("User '%s' created successfully with ID: %s", username, userID)

	response := &CommandResponse{
		ResultCount: 1,
		Result:      fmt.Sprintf("User '%s' created successfully with ID: %s", username, userID),
	}

	return response, nil
}

// GrantPermission processes a GRANT command using the new parser-based implementation
// Syntax:
//
//	GRANT "permission" TO USER "username";
//	GRANT ROLE "role" TO USER "username";
func GrantPermission(command string, logger *zap.SugaredLogger, serviceManager ServiceManager) (*CommandResponse, error) {
	logger.Infof("Processing GRANT command: %s", command)

	// Get the Primary database for context
	primaryDB, err := serviceManager.DatabaseService.GetDatabaseByName("primary")
	if err != nil {
		return nil, errors.WrapWithMessage(err, errors.ERR_NOT_FOUND_DATABASE,
			"primary database not found", errors.LayerCommand)
	}

	// Use the new parser-based handler with debug mode disabled (secure errors)
	return GrantPermissionOrRoleCommand(command, logger, serviceManager, primaryDB, false)
}

// RevokePermission processes a REVOKE command using the new parser-based implementation with optional FORCE
// Syntax:
//
//	REVOKE "permission" FROM USER "username";
//	REVOKE "permission" FROM USER "username" FORCE;
//	REVOKE ROLE "role" FROM USER "username";
//	REVOKE ROLE "role" FROM USER "username" FORCE;
//
// TODO: I can add support for revoking permissions on specific databases/bundles
func RevokePermission(command string, logger *zap.SugaredLogger, serviceManager ServiceManager) (*CommandResponse, error) {
	logger.Infof("Processing REVOKE command: %s", command)

	// Get the Primary database for context
	primaryDB, err := serviceManager.DatabaseService.GetDatabaseByName("primary")
	if err != nil {
		return nil, errors.WrapWithMessage(err, errors.ERR_NOT_FOUND_DATABASE,
			"primary database not found", errors.LayerCommand)
	}

	// Use the new parser-based handler with debug mode disabled (secure errors)
	return RevokePermissionOrRoleCommand(command, logger, serviceManager, primaryDB, serviceManager.SessionManager, serviceManager.ActiveConnections, false)
}

// AttachUserToDatabase processes an ATTACH command
// Syntax: ATTACH USER username TO DATABASE database_name
func AttachUserToDatabase(command string, logger *zap.SugaredLogger, serviceManager ServiceManager) (*CommandResponse, error) {
	logger.Infof("Processing ATTACH command: %s", command)

	// Parse the command: ATTACH USER username TO DATABASE database_name
	parts := strings.Fields(command)
	if len(parts) < 6 {
		return nil, errors.New(errors.ERR_VALIDATION_SYNTAX,
			"invalid ATTACH syntax: expected 'ATTACH USER username TO DATABASE database_name'",
			errors.LayerCommand)
	}

	if !strings.EqualFold(parts[0], "ATTACH") || !strings.EqualFold(parts[1], "USER") ||
		!strings.EqualFold(parts[3], "TO") || !strings.EqualFold(parts[4], "DATABASE") {
		return nil, errors.New(errors.ERR_VALIDATION_SYNTAX,
			"invalid ATTACH command format", errors.LayerCommand)
	}

	username := parts[2]
	databaseName := parts[5]

	// Get the Primary database
	primaryDB, err := serviceManager.DatabaseService.GetDatabaseByName("primary")
	if err != nil {
		return nil, errors.WrapWithMessage(err, errors.ERR_NOT_FOUND_DATABASE,
			"primary database not found", errors.LayerCommand)
	}

	// Find the user
	usersBundle, err := serviceManager.BundleService.GetBundleByName(primaryDB, "Users")
	if err != nil {
		return nil, errors.WrapWithMessage(err, errors.ERR_NOT_FOUND_BUNDLE,
			"Users bundle not found in Primary database", errors.LayerCommand)
	}

	var userID string
	userDocs, err := serviceManager.BundleService.GetDocumentsByFilter(usersBundle, "", nil)
	if err != nil {
		return nil, errors.WrapWithMessage(err, errors.ERR_INTERNAL_QUERY,
			"failed to retrieve user documents", errors.LayerCommand)
	}
	found := false
	for _, doc := range userDocs {
		if usernameField, ok := doc.Fields["Username"]; ok {
			if str, ok := usernameField.Value.AsString(); ok && str == username {
				if userIDField, ok := doc.Fields["UserID"]; ok {
					userID, _ = userIDField.Value.AsString()
					found = true
					break
				}
			}
		}
	}
	if !found {
		return nil, errors.New(errors.ERR_NOT_FOUND_USER,
			fmt.Sprintf("user '%s' not found", username),
			errors.LayerCommand).WithContext("username", username)
	}

	// Find the database
	databasesBundle, err := serviceManager.BundleService.GetBundleByName(primaryDB, "Databases")
	if err != nil {
		return nil, errors.WrapWithMessage(err, errors.ERR_NOT_FOUND_BUNDLE,
			"Databases bundle not found in Primary database", errors.LayerCommand)
	}

	var databaseID string
	dbDocs, err := serviceManager.BundleService.GetDocumentsByFilter(databasesBundle, "", nil)
	if err != nil {
		return nil, errors.WrapWithMessage(err, errors.ERR_INTERNAL_QUERY,
			"failed to retrieve database documents", errors.LayerCommand)
	}
	dbFound := false
	for _, doc := range dbDocs {
		if nameField, ok := doc.Fields["Name"]; ok {
			if str, ok := nameField.Value.AsString(); ok && str == databaseName {
				if dbIDField, ok := doc.Fields["DatabaseID"]; ok {
					databaseID, _ = dbIDField.Value.AsString()
					dbFound = true
					break
				}
			}
		}
	}
	if !dbFound {
		return nil, errors.New(errors.ERR_NOT_FOUND_DATABASE,
			fmt.Sprintf("database '%s' not found", databaseName),
			errors.LayerCommand).WithContext("database", databaseName)
	}

	// Add the user-database relationship to DatabaseUsers bundle
	databaseUsersBundle, err := serviceManager.BundleService.GetBundleByName(primaryDB, "DatabaseUsers")
	if err != nil {
		return nil, errors.WrapWithMessage(err, errors.ERR_NOT_FOUND_BUNDLE,
			"DatabaseUsers bundle not found in Primary database", errors.LayerCommand)
	}

	// Check if the relationship already exists
	dbUserDocs, err := serviceManager.BundleService.GetDocumentsByFilter(databaseUsersBundle, "", nil)
	if err != nil {
		return nil, errors.WrapWithMessage(err, errors.ERR_INTERNAL_QUERY,
			"failed to retrieve database user documents", errors.LayerCommand)
	}
	for _, doc := range dbUserDocs {
		if userIDField, ok := doc.Fields["UserID"]; ok {
			if dbIDField, ok := doc.Fields["DatabaseID"]; ok {
				str1, ok1 := userIDField.Value.AsString()
				str2, ok2 := dbIDField.Value.AsString()
				if ok1 && ok2 && str1 == userID && str2 == databaseID {
					return nil, errors.New(errors.ERR_VALIDATION_CONSTRAINT,
						fmt.Sprintf("user '%s' is already attached to database '%s'", username, databaseName),
						errors.LayerCommand).WithContext("username", username).WithContext("database", databaseName)
				}
			}
		}
	}

	// Create the user-database relationship
	relationshipID := fmt.Sprintf("dbuser_%s_%s_%d", userID, databaseID, time.Now().Unix())
	relationshipDoc := models.Document{
		DocumentID: relationshipID,
		Fields: map[string]models.Field{
			"DocumentID": {Name: "DocumentID", Value: models.NewStringValue(relationshipID)},
			"UserID":     {Name: "UserID", Value: models.NewStringValue(userID)},
			"DatabaseID": {Name: "DatabaseID", Value: models.NewStringValue(databaseID)},
			"AttachedAt": {Name: "AttachedAt", Value: models.NewStringValue(time.Now().Format(time.RFC3339))},
		},
	}

	err = serviceManager.BundleService.AddDocumentToBundleByStruct(primaryDB, databaseUsersBundle, &relationshipDoc)
	if err != nil {
		return nil, errors.WrapWithMessage(err, errors.ERR_INTERNAL_STORAGE,
			"failed to add relationship document to DatabaseUsers bundle", errors.LayerCommand)
	}

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
		return false, errors.WrapWithMessage(err, errors.ERR_NOT_FOUND_DATABASE,
			"primary database not found", errors.LayerCommand)
	}

	// Find the user
	usersBundle, err := serviceManager.BundleService.GetBundleByName(primaryDB, "Users")
	if err != nil {
		return false, errors.WrapWithMessage(err, errors.ERR_NOT_FOUND_BUNDLE,
			"Users bundle not found in Primary database", errors.LayerCommand)
	}

	var userID string
	userDocs, err := serviceManager.BundleService.GetDocumentsByFilter(usersBundle, "", nil)
	if err != nil {
		return false, errors.WrapWithMessage(err, errors.ERR_INTERNAL_QUERY,
			"failed to retrieve user documents", errors.LayerCommand)
	}
	found := false
	for _, doc := range userDocs {
		if usernameField, ok := doc.Fields["Username"]; ok {
			if str, ok := usernameField.Value.AsString(); ok && str == username {
				if userIDField, ok := doc.Fields["UserID"]; ok {
					userID, _ = userIDField.Value.AsString()
					found = true
					break
				}
			}
		}
	}
	if !found {
		return false, errors.New(errors.ERR_NOT_FOUND_USER,
			fmt.Sprintf("user '%s' not found", username),
			errors.LayerCommand).WithContext("username", username)
	}

	// Find the permission
	permissionsBundle, err := serviceManager.BundleService.GetBundleByName(primaryDB, "Permissions")
	if err != nil {
		return false, errors.WrapWithMessage(err, errors.ERR_NOT_FOUND_BUNDLE,
			"Permissions bundle not found in Primary database", errors.LayerCommand)
	}

	var permissionID string
	permDocs, err := serviceManager.BundleService.GetDocumentsByFilter(permissionsBundle, "", nil)
	if err != nil {
		return false, errors.WrapWithMessage(err, errors.ERR_INTERNAL_QUERY,
			"failed to retrieve permission documents", errors.LayerCommand)
	}
	permFound := false
	for _, doc := range permDocs {
		if nameField, ok := doc.Fields["PermissionName"]; ok {
			if str, ok := nameField.Value.AsString(); ok && str == permission {
				if idField, ok := doc.Fields["PermissionID"]; ok {
					permissionID, _ = idField.Value.AsString()
					permFound = true
					break
				}
			}
		}
	}
	if !permFound {
		return false, nil // Permission doesn't exist, user doesn't have it
	}

	// Check if the user has this permission in UserPermissions bundle
	userPermissionsBundle, err := serviceManager.BundleService.GetBundleByName(primaryDB, "UserPermissions")
	if err != nil {
		return false, errors.WrapWithMessage(err, errors.ERR_NOT_FOUND_BUNDLE,
			"UserPermissions bundle not found in Primary database", errors.LayerCommand)
	}

	userPermDocs, err := serviceManager.BundleService.GetDocumentsByFilter(userPermissionsBundle, "", nil)
	if err != nil {
		return false, errors.WrapWithMessage(err, errors.ERR_INTERNAL_QUERY,
			"failed to retrieve user permission documents", errors.LayerCommand)
	}
	for _, doc := range userPermDocs {
		if userIDField, ok := doc.Fields["UserID"]; ok {
			if permIDField, ok := doc.Fields["PermissionID"]; ok {
				str1, ok1 := userIDField.Value.AsString()
				str2, ok2 := permIDField.Value.AsString()
				if ok1 && ok2 && str1 == userID && str2 == permissionID {
					return true, nil
				}
			}
		}
	}

	return false, nil
}

// UpdateUser processes UPDATE USER commands using the parser-based implementation
// Syntax: UPDATE USER "username" SET PASSWORD = "new_password" [FORCE];
func UpdateUser(command string, logger *zap.SugaredLogger, serviceManager ServiceManager) (*CommandResponse, error) {
	logger.Infof("Processing UPDATE USER command")

	// Get the primary database for RBAC operations
	primaryDB, err := serviceManager.DatabaseService.GetDatabaseByName("primary")
	if err != nil {
		return nil, errors.WrapWithMessage(err, errors.ERR_NOT_FOUND_DATABASE,
			"primary database not found", errors.LayerCommand)
	}

	// Delegate to UpdateUserCommand with debugMode=false for production
	return UpdateUserCommand(command, logger, serviceManager, primaryDB, false)
}

// DeleteUser processes DELETE USER and DROP USER commands using the parser-based implementation
// Syntax:
//   - DELETE USER "username" [FORCE];
//   - DROP USER "username" [FORCE];
func DeleteUser(command string, logger *zap.SugaredLogger, serviceManager ServiceManager) (*CommandResponse, error) {
	logger.Infof("Processing DELETE/DROP USER command")

	// Get the primary database for RBAC operations
	primaryDB, err := serviceManager.DatabaseService.GetDatabaseByName("primary")
	if err != nil {
		return nil, errors.WrapWithMessage(err, errors.ERR_NOT_FOUND_DATABASE,
			"primary database not found", errors.LayerCommand)
	}

	// Delegate to DeleteUserCommand with debugMode=false for production
	return DeleteUserCommand(command, logger, serviceManager, primaryDB, false)
}

// CreateRole processes CREATE ROLE commands using the parser-based implementation
// Syntax: CREATE ROLE "role_name" [WITH DESCRIPTION "description"];
func CreateRole(command string, logger *zap.SugaredLogger, serviceManager ServiceManager) (*CommandResponse, error) {
	logger.Infof("Processing CREATE ROLE command")

	// Get the primary database for RBAC operations
	primaryDB, err := serviceManager.DatabaseService.GetDatabaseByName("primary")
	if err != nil {
		return nil, errors.WrapWithMessage(err, errors.ERR_NOT_FOUND_DATABASE,
			"primary database not found", errors.LayerCommand)
	}

	// Delegate to CreateRoleCommand with debugMode=false for production
	return CreateRoleCommand(command, logger, serviceManager, primaryDB, false)
}

// UpdateRole processes UPDATE ROLE and ALTER ROLE commands using the parser-based implementation
// Syntax:
//   - UPDATE ROLE "role_name" SET DESCRIPTION = "new_description" [FORCE];
//   - ALTER ROLE "role_name" SET DESCRIPTION = "new_description" [FORCE];
func UpdateRole(command string, logger *zap.SugaredLogger, serviceManager ServiceManager) (*CommandResponse, error) {
	logger.Infof("Processing UPDATE/ALTER ROLE command")

	// Get the primary database for RBAC operations
	primaryDB, err := serviceManager.DatabaseService.GetDatabaseByName("primary")
	if err != nil {
		return nil, errors.WrapWithMessage(err, errors.ERR_NOT_FOUND_DATABASE,
			"primary database not found", errors.LayerCommand)
	}

	// Delegate to UpdateRoleCommand with debugMode=false for production
	return UpdateRoleCommand(command, logger, serviceManager, primaryDB, false)
}

// DeleteRole processes DELETE ROLE and DROP ROLE commands using the parser-based implementation
// Syntax:
//   - DELETE ROLE "role_name" [FORCE];
//   - DROP ROLE "role_name" [FORCE];
func DeleteRole(command string, logger *zap.SugaredLogger, serviceManager ServiceManager) (*CommandResponse, error) {
	logger.Infof("Processing DELETE/DROP ROLE command")

	// Get the primary database for RBAC operations
	primaryDB, err := serviceManager.DatabaseService.GetDatabaseByName("primary")
	if err != nil {
		return nil, errors.WrapWithMessage(err, errors.ERR_NOT_FOUND_DATABASE,
			"primary database not found", errors.LayerCommand)
	}

	// Delegate to DeleteRoleCommand with debugMode=false for production
	return DeleteRoleCommand(command, logger, serviceManager, primaryDB, false)
}

// To be populated during refactoring
