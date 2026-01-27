package server

import (
	"context"
	"fmt"
	"strings"
	"syndrdb/src/internal/auth"
	"syndrdb/src/internal/domain/bundle"
	"syndrdb/src/internal/domain/database"
	"syndrdb/src/internal/domain/models"
	"syndrdb/src/pkg/common/helpers"
	"syndrdb/src/pkg/errors"
	"time"

	"go.uber.org/zap"
)

/*
user_service.go

This file implements the UserService, which provides centralized user management
functionality for the SyndrDB primary database. It follows the Single Responsibility
Principle by handling only user-related operations.

Key responsibilities:
- Create users in the primary database Users bundle
- Retrieve users by username or UserID
- Validate user credentials using Argon2id hashing
- Integrate with BundleService for persistent storage
- Integrate with auth.UserStore for password hashing

Design Principles:
- Single Responsibility: Only manages user CRUD operations
- DRY: Centralizes user lookup logic
- Open/Closed: Extensible for additional user fields without modification

Performance Targets:
- CreateUser: < 10ms
- GetUserByUsername: < 5ms
- ValidateUserCredentials: < 15ms (includes Argon2 verification)

TODO: I will add support for user profile fields (email, phone, metadata)
TODO: I will implement user deactivation/soft delete functionality
TODO: I will add password expiration and rotation policies
TODO: I will add user session management integration
TODO: I will implement user audit logging for RBAC compliance
*/

// UserService provides centralized user management functionality
type UserService struct {
	bundleService   *bundle.BundleService
	databaseService *database.DatabaseService
	userStore       *auth.UserStore
	logger          *zap.SugaredLogger
	debugMode       bool
}

// NewUserService creates a new UserService instance
func NewUserService(
	bundleService *bundle.BundleService,
	databaseService *database.DatabaseService,
	userStore *auth.UserStore,
	logger *zap.SugaredLogger,
	debugMode bool,
) *UserService {
	return &UserService{
		bundleService:   bundleService,
		databaseService: databaseService,
		userStore:       userStore,
		logger:          logger,
		debugMode:       debugMode,
	}
}

// isSystemUser checks if a user is a system user that cannot be modified or deleted
// System users are created during database initialization with IsSystem=true
// Returns error if user is a system user, nil otherwise
// TODO: I can add support for super-admin override to modify system users
func (us *UserService) isSystemUser(username string) error {
	// Get primary database
	primaryDB, err := us.databaseService.GetDatabaseByName("primary")
	if err != nil {
		return errors.ConvertError(err, errors.LayerCommand).WithContext("database", "primary")
	}

	// Get Users bundle
	usersBundle, err := us.bundleService.GetBundleByName(primaryDB, "Users")
	if err != nil {
		return errors.ConvertError(err, errors.LayerCommand).WithContext("bundle", "Users")
	}

	// Find user document by username (case-insensitive) using BundleService
	usernameLower := strings.ToLower(username)
	docs, err := us.bundleService.GetDocumentsByFilter(usersBundle, "", nil)
	if err != nil {
		return errors.ConvertError(err, errors.LayerCommand).WithContext("bundle", "Users")
	}
	for _, doc := range docs {
		if nameField, ok := doc.Fields["Username"]; ok {
			if nameValue, ok := nameField.Value.AsString(); ok {
				if strings.ToLower(nameValue) == usernameLower {
					// Check IsSystem field
					if isSystemField, ok := doc.Fields["IsSystem"]; ok {
						if isSystem, ok := isSystemField.Value.AsBool(); ok && isSystem {
							return errors.New(errors.ERR_PERMISSION_DENIED,
								fmt.Sprintf("Cannot modify system user '%s'", username),
								errors.LayerAuth).WithContext("username", username)
						}
					}
					// User found but not a system user
					return nil
				}
			}
		}
	}

	// User not found - that's ok, they might not exist yet
	return nil
}

// CreateUser creates a new user in the primary database Users bundle
// Parameters:
//   - username: User's username (alphanumeric, dash, underscore only, case-insensitive unique)
//   - password: User's password (will be hashed with Argon2id)
//
// Returns:
//   - userID: The generated UUID for the new user
//   - error: Any error that occurred during creation
//
// TODO: I will add support for setting initial user roles/permissions during creation
// TODO: I will add email verification workflow
// TODO: I will add password complexity enforcement options
func (us *UserService) CreateUser(username, password string) (string, error) {
	us.logger.Infof("Creating new user: %s", username)

	// Validate username format
	securityConfig := DefaultSecurityConfig()
	if err := ValidateInput(username, "username", securityConfig); err != nil {
		return "", errors.WrapWithMessage(err, errors.ERR_VALIDATION_FIELD,
			"invalid username", errors.LayerCommand).WithContext("username", username)
	}

	// Validate password strength
	if err := ValidateInput(password, "password", securityConfig); err != nil {
		return "", errors.WrapWithMessage(err, errors.ERR_VALIDATION_FIELD,
			"invalid password", errors.LayerCommand)
	}

	// Get the primary database
	primaryDB, err := us.databaseService.GetDatabaseByName("primary")
	if err != nil {
		return "", errors.ConvertError(err, errors.LayerCommand).WithContext("database", "primary")
	}

	// Get the Users bundle
	usersBundle, err := us.bundleService.GetBundleByName(primaryDB, "Users")
	if err != nil {
		return "", errors.ConvertError(err, errors.LayerCommand).WithContext("bundle", "Users")
	}

	// Check if username already exists (case-insensitive) using BundleService
	existingDocs, err := us.bundleService.GetDocumentsByFilter(usersBundle, "", nil)
	if err != nil {
		return "", errors.ConvertError(err, errors.LayerCommand).WithContext("bundle", "Users")
	}
	for _, doc := range existingDocs {
		if nameField, ok := doc.Fields["Name"]; ok {
			if str, ok := nameField.Value.AsString(); ok && strings.EqualFold(str, username) {
				return "", errors.New(errors.ERR_VALIDATION_CONSTRAINT,
					fmt.Sprintf("user '%s' already exists", username),
					errors.LayerCommand).WithContext("username", username)
			}
		}
	}

	// Generate UserID
	userID := helpers.GenerateUUID()

	// Hash password using Argon2id via auth.UserStore
	// This ensures consistent password hashing across the system
	newUser := auth.NewUser{
		UserID:   userID,
		Username: username,
		Password: password,
	}

	storedUser, err := us.userStore.AddUser(newUser)
	if err != nil {
		return "", errors.ConvertError(err, errors.LayerCommand).WithContext("username", username)
	}

	// TODO (STEP 1 - Future): Replace with document.GetPooledDocument() to reduce allocations
	// This is a user-facing operation (lower frequency than query hot-path)
	// Create user document for the primary database Users bundle
	userDoc := &models.Document{
		DocumentID: helpers.GenerateFastUUID(),
		Fields: map[string]models.Field{
			"DocumentID": {
				Name:  "DocumentID",
				Value: models.NewStringValue(helpers.GenerateFastUUID()),
			},
			"UserID": {
				Name:  "UserID",
				Value: models.NewStringValue(userID),
			},
			"PasswordHash": {
				Name:  "PasswordHash",
				Value: models.NewStringValue(string(storedUser.PasswordHash.Hash)), // Store the hash
			},
			"Name": {
				Name:  "Name",
				Value: models.NewStringValue(username),
			},
			"IsActive": {
				Name:  "IsActive",
				Value: models.NewBoolValue(true),
			},
			"IsLockedOut": {
				Name:  "IsLockedOut",
				Value: models.NewBoolValue(false),
			},
			"FailedLoginAttempts": {
				Name:  "FailedLoginAttempts",
				Value: models.NewIntValue(0),
			},
			"LockoutExpiresOn": {
				Name:  "LockoutExpiresOn",
				Value: models.NewStringValue(time.Now().Format(time.RFC3339)),
			},
			"IsSystem": {
				Name:  "IsSystem",
				Value: models.NewBoolValue(false), // User-created users are not system users
			},
		},
	}

	// Add user document to Users bundle using BundleService
	err = us.bundleService.AddDocumentToBundleByStruct(primaryDB, usersBundle, userDoc)
	if err != nil {
		// Rollback: Remove from UserStore if bundle insertion fails
		// TODO: I will implement proper transaction support for atomic operations
		return "", errors.ConvertError(err, errors.LayerCommand).WithContext("username", username).WithContext("bundle", "Users")
	}

	us.logger.Infof("User '%s' created successfully with ID: %s", username, userID)
	return userID, nil
}

// GetUserByUsername retrieves a user document by username (case-insensitive)
// Parameters:
//   - username: The username to search for
//
// Returns:
//   - *models.Document: The user document if found
//   - error: Any error that occurred, or nil if user not found
//
// TODO: I will add caching for frequently accessed users
// TODO: I will add user profile expansion (join with roles/permissions)
func (us *UserService) GetUserByUsername(username string) (*models.Document, error) {
	// Get the primary database
	primaryDB, err := us.databaseService.GetDatabaseByName("primary")
	if err != nil {
		return nil, errors.ConvertError(err, errors.LayerCommand).WithContext("database", "primary")
	}

	// Get the Users bundle
	usersBundle, err := us.bundleService.GetBundleByName(primaryDB, "Users")
	if err != nil {
		return nil, errors.ConvertError(err, errors.LayerCommand).WithContext("bundle", "Users")
	}

	// Search for user (case-insensitive) using BundleService
	docs, err := us.bundleService.GetDocumentsByFilter(usersBundle, "", nil)
	if err != nil {
		return nil, errors.ConvertError(err, errors.LayerCommand).WithContext("bundle", "Users")
	}
	for _, doc := range docs {
		if nameField, ok := doc.Fields["Name"]; ok {
			if str, ok := nameField.Value.AsString(); ok && strings.EqualFold(str, username) {
				return doc, nil
			}
		}
	}

	// User not found
	return nil, errors.New(errors.ERR_NOT_FOUND_USER,
		fmt.Sprintf("user '%s' not found", username),
		errors.LayerAuth).WithContext("username", username)
}

// GetUserByID retrieves a user document by UserID
// Parameters:
//   - userID: The UUID of the user
//
// Returns:
//   - *models.Document: The user document if found
//   - error: Any error that occurred, or nil if user not found
func (us *UserService) GetUserByID(userID string) (*models.Document, error) {
	// Get the primary database
	primaryDB, err := us.databaseService.GetDatabaseByName("primary")
	if err != nil {
		return nil, errors.ConvertError(err, errors.LayerCommand).WithContext("database", "primary")
	}

	// Get the Users bundle
	usersBundle, err := us.bundleService.GetBundleByName(primaryDB, "Users")
	if err != nil {
		return nil, errors.ConvertError(err, errors.LayerCommand).WithContext("bundle", "Users")
	}

	// Search for user by UserID using BundleService
	docs, err := us.bundleService.GetDocumentsByFilter(usersBundle, "", nil)
	if err != nil {
		return nil, errors.ConvertError(err, errors.LayerCommand).WithContext("bundle", "Users")
	}
	for _, doc := range docs {
		if idField, ok := doc.Fields["UserID"]; ok {
			if str, ok := idField.Value.AsString(); ok && str == userID {
				return doc, nil
			}
		}
	}

	// User not found
	return nil, errors.New(errors.ERR_NOT_FOUND_USER,
		fmt.Sprintf("user with ID '%s' not found", userID),
		errors.LayerAuth).WithContext("user_id", userID)
}

// ValidateUserCredentials checks if the provided username/password combination is valid
// This uses the auth.UserStore for Argon2id password verification
// Parameters:
//   - username: The username to validate
//   - password: The password to verify
//
// Returns:
//   - bool: true if credentials are valid, false otherwise
//   - error: Any error that occurred during validation
//
// TODO: I will add brute force protection integration
// TODO: I will add account lockout on failed attempts
// TODO: I will add password expiration checking
func (us *UserService) ValidateUserCredentials(username, password string) (bool, error) {
	// Use UserStore for credential verification (includes Argon2id verification)
	isValid, _, err := us.userStore.VerifyCredentials(username, password)
	if err != nil {
		return false, errors.ConvertError(err, errors.LayerAuth).WithContext("username", username)
	}

	return isValid, nil
}

// UpdateUser updates user fields (currently supports PASSWORD only)
// Parameters:
//   - username: The username to update
//   - updates: Map of field names to new values (e.g., "PASSWORD" -> "new_password")
//   - force: Whether to forcefully terminate active sessions
//
// Returns:
//   - error: Any error that occurred during update
//
// TODO: I will add support for updating additional user fields (email, phone, metadata)
// TODO: I will add field-level audit logging for compliance
// TODO: I will add support for conditional updates (optimistic locking)
// TODO: I will add password history checking to prevent reuse
func (us *UserService) UpdateUser(username string, updates map[string]string, force bool) error {
	us.logger.Infof("Updating user: %s (force=%v)", username, force)

	// Check if user is a system user
	if err := us.isSystemUser(username); err != nil {
		return err
	}

	// Get primary database
	primaryDB, err := us.databaseService.GetDatabaseByName("primary")
	if err != nil {
		if us.debugMode {
			return errors.WrapWithMessage(err, errors.ERR_NOT_FOUND_DATABASE, "primary database not found", errors.LayerCommand)
		}
		return errors.New(errors.ERR_INTERNAL, "internal error: database access failed", errors.LayerCommand)
	}

	// Get Users bundle
	usersBundle, err := us.bundleService.GetBundleByName(primaryDB, "Users")
	if err != nil {
		if us.debugMode {
			return errors.WrapWithMessage(err, errors.ERR_NOT_FOUND_BUNDLE, "Users bundle not found", errors.LayerCommand)
		}
		return errors.New(errors.ERR_INTERNAL, "internal error: user storage not available", errors.LayerCommand)
	}

	// Find user document (case-insensitive)
	// WRITE-THROUGH CACHE: Use GetDocumentsByFilter instead of bundle.Documents
	usernameLower := strings.ToLower(username)
	var targetDocID string
	docs, err := us.bundleService.GetDocumentsByFilter(usersBundle, "", nil)
	if err != nil {
		return errors.WrapWithMessage(err, errors.ERR_INTERNAL, "failed to retrieve users", errors.LayerCommand)
	}

	for _, doc := range docs {
		if nameField, ok := doc.Fields["Username"]; ok {
			if nameValue, ok := nameField.Value.AsString(); ok {
				if strings.ToLower(nameValue) == usernameLower {
					targetDocID = doc.DocumentID
					break
				}
			}
		}
	}

	if targetDocID == "" {
		return errors.New(errors.ERR_NOT_FOUND_USER,
			fmt.Sprintf("user '%s' not found", username),
			errors.LayerAuth).WithContext("username", username)
	}

	// Check for active sessions if not forcing
	serviceManager := GetServiceManager()
	if !force && serviceManager.SessionManager != nil {
		sessions := serviceManager.SessionManager.GetUserSessions(username)
		if len(sessions) > 0 {
			return errors.New(errors.ERR_VALIDATION_CONSTRAINT,
				fmt.Sprintf("user '%s' has %d active session(s). Use FORCE to terminate sessions and proceed",
					username, len(sessions)),
				errors.LayerCommand).WithContext("username", username).WithContext("session_count", fmt.Sprintf("%d", len(sessions)))
		}
	}

	// If forcing and sessions exist, terminate them
	if force && serviceManager.SessionManager != nil && serviceManager.ActiveConnections != nil {
		sessions := serviceManager.SessionManager.GetUserSessions(username)
		if len(sessions) > 0 {
			terminated, _ := serviceManager.SessionManager.TerminateUserSessions(username, serviceManager.ActiveConnections)
			us.logger.Warnw("FORCED UPDATE USER - TERMINATED USER SESSIONS",
				"username", username,
				"sessionsTerminated", terminated,
				"operator", "SYSTEM", // TODO: Replace with actual operator username
			)
		}
	}

	// Find the target document from the slice
	var targetDoc *models.Document
	for _, doc := range docs {
		if doc.DocumentID == targetDocID {
			targetDoc = doc
			break
		}
	}
	if targetDoc == nil {
		return errors.New(errors.ERR_NOT_FOUND_USER,
			fmt.Sprintf("user '%s' document not found", username),
			errors.LayerAuth).WithContext("username", username)
	}

	// Apply updates - build fields for UpdateDocumentInBundle
	var updateFields []models.KeyValue
	for field, value := range updates {
		switch strings.ToUpper(field) {
		case "PASSWORD":
			// Hash the new password using UserStore
			if userIDField, ok := targetDoc.Fields["UserID"]; ok {
				if userID, ok := userIDField.Value.AsString(); ok {
					newUser := auth.NewUser{
						UserID:   userID,
						Username: username,
						Password: value,
					}
					storedUser, err := us.userStore.AddUser(newUser) // This updates if exists
					if err != nil {
						return errors.ConvertError(err, errors.LayerCommand).WithContext("username", username)
					}

					// Add PasswordHash to update fields
					updateFields = append(updateFields, models.KeyValue{
						Key:   "PasswordHash",
						Value: string(storedUser.PasswordHash.Hash),
					})
				}
			}
		default:
			if us.debugMode {
				us.logger.Warnf("Ignoring unsupported update field: %s", field)
			}
		}
	}

	// Save updated document back to bundle using UpdateDocumentInBundle
	updateCmd := &models.DocumentUpdateCommand{
		BundleName:  "Users",
		Fields:      updateFields,
		WhereClause: fmt.Sprintf("DocumentID = '%s'", targetDoc.DocumentID),
	}
	updateErr := us.bundleService.UpdateDocumentInBundle(context.Background(), primaryDB, usersBundle, updateCmd)
	if updateErr != nil {
		return errors.WrapWithMessage(updateErr, errors.ERR_INTERNAL, "failed to save user update", errors.LayerCommand)
	}

	us.logger.Infof("User '%s' updated successfully", username)
	return nil
}

// DeleteUser removes a user from the system and cleans up related data
// Parameters:
//   - username: The username to delete
//   - force: Whether to forcefully terminate active sessions
//
// Returns:
//   - error: Any error that occurred during deletion
//
// TODO: I will add CASCADE option to delete user-created databases and objects
// TODO: I will add soft delete with recovery period (retention policy)
// TODO: I will add archival of user data before deletion (compliance)
// TODO: I will add support for transferring ownership before deletion
func (us *UserService) DeleteUser(username string, force bool) error {
	us.logger.Infof("Deleting user: %s (force=%v)", username, force)

	// Check if user is a system user
	if err := us.isSystemUser(username); err != nil {
		return err
	}

	// Get primary database
	primaryDB, err := us.databaseService.GetDatabaseByName("primary")
	if err != nil {
		if us.debugMode {
			return errors.WrapWithMessage(err, errors.ERR_NOT_FOUND_DATABASE, "primary database not found", errors.LayerCommand)
		}
		return errors.New(errors.ERR_INTERNAL, "internal error: database access failed", errors.LayerCommand)
	}

	// Get Users bundle
	usersBundle, err := us.bundleService.GetBundleByName(primaryDB, "Users")
	if err != nil {
		if us.debugMode {
			return errors.WrapWithMessage(err, errors.ERR_NOT_FOUND_BUNDLE, "Users bundle not found", errors.LayerCommand)
		}
		return errors.New(errors.ERR_INTERNAL, "internal error: user storage not available", errors.LayerCommand)
	}

	// Find user document (case-insensitive)
	// WRITE-THROUGH CACHE: Use GetDocumentsByFilter instead of bundle.Documents
	usernameLower := strings.ToLower(username)
	var userID string
	var targetDocID string
	docs, err := us.bundleService.GetDocumentsByFilter(usersBundle, "", nil)
	if err != nil {
		return errors.WrapWithMessage(err, errors.ERR_INTERNAL, "failed to retrieve users", errors.LayerCommand)
	}

	for _, doc := range docs {
		if nameField, ok := doc.Fields["Username"]; ok {
			if nameValue, ok := nameField.Value.AsString(); ok {
				if strings.ToLower(nameValue) == usernameLower {
					// Get UserID for junction table cleanup
					if idField, ok := doc.Fields["UserID"]; ok {
						if id, ok := idField.Value.AsString(); ok {
							userID = id
						}
					}
					targetDocID = doc.DocumentID
					break
				}
			}
		}
	}

	if targetDocID == "" {
		return errors.New(errors.ERR_NOT_FOUND_USER,
			fmt.Sprintf("user '%s' not found", username),
			errors.LayerAuth).WithContext("username", username)
	}

	// Check for active sessions if not forcing
	serviceManager := GetServiceManager()
	if !force && serviceManager.SessionManager != nil {
		sessions := serviceManager.SessionManager.GetUserSessions(username)
		if len(sessions) > 0 {
			return errors.New(errors.ERR_VALIDATION_CONSTRAINT,
				fmt.Sprintf("user '%s' has %d active session(s). Use FORCE to terminate sessions and proceed",
					username, len(sessions)),
				errors.LayerCommand).WithContext("username", username).WithContext("session_count", fmt.Sprintf("%d", len(sessions)))
		}
	}

	// If forcing and sessions exist, terminate them
	if force && serviceManager.SessionManager != nil && serviceManager.ActiveConnections != nil {
		sessions := serviceManager.SessionManager.GetUserSessions(username)
		if len(sessions) > 0 {
			terminated, _ := serviceManager.SessionManager.TerminateUserSessions(username, serviceManager.ActiveConnections)
			us.logger.Warnw("FORCED DELETE USER - TERMINATED USER SESSIONS",
				"username", username,
				"sessionsTerminated", terminated,
				"operator", "SYSTEM", // TODO: Replace with actual operator username
			)
		}
	}

	// Auto-cleanup: Remove user from junction tables (UserRoles, UserPermissions)
	if userID != "" {
		// Clean up UserRoles
		if userRolesBundle, err := us.bundleService.GetBundleByName(primaryDB, "UserRoles"); err == nil {
			us.cleanupJunctionTable(userRolesBundle, "UserID", userID, "UserRoles")
		}

		// Clean up UserPermissions
		if userPermsBundle, err := us.bundleService.GetBundleByName(primaryDB, "UserPermissions"); err == nil {
			us.cleanupJunctionTable(userPermsBundle, "UserID", userID, "UserPermissions")
		}
	}

	// Remove user document from Users bundle using DeleteDocumentFromBundle
	// WRITE-THROUGH CACHE: Use bundleService.DeleteDocumentFromBundle
	deleteCmd := &models.DocumentDeleteCommand{
		BundleName:  "Users",
		WhereClause: fmt.Sprintf("DocumentID = '%s'", targetDocID),
	}
	deleteErr := us.bundleService.DeleteDocumentFromBundle(usersBundle, deleteCmd, []string{targetDocID}, nil)
	if deleteErr != nil {
		return errors.WrapWithMessage(deleteErr, errors.ERR_INTERNAL, "failed to delete user document", errors.LayerCommand)
	}

	us.logger.Infof("User '%s' deleted successfully", username)
	return nil
}

// cleanupJunctionTable removes all records matching a specific field value
// This is used for cascade deletion of junction table records
// WRITE-THROUGH CACHE: Use GetDocumentsByFilter and DeleteDocumentFromBundle
func (us *UserService) cleanupJunctionTable(junctionBundle *models.Bundle, fieldName, fieldValue, tableName string) {
	docs, err := us.bundleService.GetDocumentsByFilter(junctionBundle, "", nil)
	if err != nil {
		us.logger.Warnf("Failed to get documents from %s: %v", tableName, err)
		return
	}

	removedCount := 0

	// Collect matching document IDs for batch delete
	var toDeleteIDs []string
	var toDeleteDocs []*models.Document
	for _, doc := range docs {
		if field, ok := doc.Fields[fieldName]; ok {
			if value, ok := field.Value.AsString(); ok && value == fieldValue {
				toDeleteIDs = append(toDeleteIDs, doc.DocumentID)
				toDeleteDocs = append(toDeleteDocs, doc)
			}
		}
	}

	// Delete matching documents using DeleteDocumentFromBundle
	if len(toDeleteIDs) > 0 {
		deleteCmd := &models.DocumentDeleteCommand{
			BundleName:  tableName,
			WhereClause: fmt.Sprintf("%s = '%s'", fieldName, fieldValue),
		}
		if delErr := us.bundleService.DeleteDocumentFromBundle(junctionBundle, deleteCmd, toDeleteIDs, toDeleteDocs); delErr != nil {
			us.logger.Warnf("Failed to delete documents from %s: %v", tableName, delErr)
		} else {
			removedCount = len(toDeleteIDs)
		}
	}

	if removedCount > 0 {
		us.logger.Infof("Removed %d records from %s for %s=%s", removedCount, tableName, fieldName, fieldValue)
	}
}

// TODO: I will implement ListUsers with pagination and filtering
// TODO: I will implement LockoutUser and UnlockUser for security management
