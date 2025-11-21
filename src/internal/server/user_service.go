package server

import (
	"fmt"
	"strings"
	"syndrdb/src/internal/auth"
	"syndrdb/src/internal/domain/bundle"
	"syndrdb/src/internal/domain/database"
	"syndrdb/src/internal/domain/models"
	"syndrdb/src/pkg/common/helpers"
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
		return fmt.Errorf("primary database not found: %w", err)
	}

	// Get Users bundle
	usersBundle, err := us.bundleService.GetBundleByName(primaryDB, "Users")
	if err != nil {
		return fmt.Errorf("Users bundle not found: %w", err)
	}

	// Find user document by username (case-insensitive)
	usernameLower := strings.ToLower(username)
	docs := *usersBundle.Documents
	for _, doc := range docs {
		if nameField, ok := doc.Fields["Username"]; ok {
			if nameValue, ok := nameField.Value.AsString(); ok {
				if strings.ToLower(nameValue) == usernameLower {
					// Check IsSystem field
					if isSystemField, ok := doc.Fields["IsSystem"]; ok {
						if isSystem, ok := isSystemField.Value.AsBool(); ok && isSystem {
							return fmt.Errorf("Cannot modify system user '%s'", username)
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
		return "", fmt.Errorf("invalid username: %w", err)
	}

	// Validate password strength
	if err := ValidateInput(password, "password", securityConfig); err != nil {
		return "", fmt.Errorf("invalid password: %w", err)
	}

	// Get the primary database
	primaryDB, err := us.databaseService.GetDatabaseByName("primary")
	if err != nil {
		if us.debugMode {
			return "", fmt.Errorf("primary database not found: %w", err)
		}
		return "", fmt.Errorf("internal error: database access failed")
	}

	// Get the Users bundle
	usersBundle, err := us.bundleService.GetBundleByName(primaryDB, "Users")
	if err != nil {
		if us.debugMode {
			return "", fmt.Errorf("Users bundle not found: %w", err)
		}
		return "", fmt.Errorf("internal error: user storage not available")
	}

	// Check if username already exists (case-insensitive)
	if usersBundle.Documents != nil {
		for _, doc := range *usersBundle.Documents {
			if nameField, ok := doc.Fields["Name"]; ok {
				if str, ok := nameField.Value.AsString(); ok && strings.EqualFold(str, username) {
					if us.debugMode {
						return "", fmt.Errorf("user '%s' already exists", username)
					}
					return "", fmt.Errorf("username already taken")
				}
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
		if us.debugMode {
			return "", fmt.Errorf("failed to hash password: %w", err)
		}
		return "", fmt.Errorf("internal error: user creation failed")
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
		if us.debugMode {
			return "", fmt.Errorf("failed to add user document to bundle: %w", err)
		}
		return "", fmt.Errorf("internal error: user creation failed")
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
		if us.debugMode {
			return nil, fmt.Errorf("primary database not found: %w", err)
		}
		return nil, fmt.Errorf("internal error: database access failed")
	}

	// Get the Users bundle
	usersBundle, err := us.bundleService.GetBundleByName(primaryDB, "Users")
	if err != nil {
		if us.debugMode {
			return nil, fmt.Errorf("Users bundle not found: %w", err)
		}
		return nil, fmt.Errorf("internal error: user storage not available")
	}

	// Search for user (case-insensitive)
	if usersBundle.Documents != nil {
		for _, doc := range *usersBundle.Documents {
			if nameField, ok := doc.Fields["Name"]; ok {
				if str, ok := nameField.Value.AsString(); ok && strings.EqualFold(str, username) {
					docCopy := doc
					return &docCopy, nil
				}
			}
		}
	}

	// User not found
	if us.debugMode {
		return nil, fmt.Errorf("user '%s' not found", username)
	}
	return nil, fmt.Errorf("invalid credentials")
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
		if us.debugMode {
			return nil, fmt.Errorf("primary database not found: %w", err)
		}
		return nil, fmt.Errorf("internal error: database access failed")
	}

	// Get the Users bundle
	usersBundle, err := us.bundleService.GetBundleByName(primaryDB, "Users")
	if err != nil {
		if us.debugMode {
			return nil, fmt.Errorf("Users bundle not found: %w", err)
		}
		return nil, fmt.Errorf("internal error: user storage not available")
	}

	// Search for user by UserID
	if usersBundle.Documents != nil {
		for _, doc := range *usersBundle.Documents {
			if idField, ok := doc.Fields["UserID"]; ok {
				if str, ok := idField.Value.AsString(); ok && str == userID {
					docCopy := doc
					return &docCopy, nil
				}
			}
		}
	}

	// User not found
	if us.debugMode {
		return nil, fmt.Errorf("user with ID '%s' not found", userID)
	}
	return nil, fmt.Errorf("user not found")
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
		if us.debugMode {
			return false, fmt.Errorf("credential verification error: %w", err)
		}
		return false, fmt.Errorf("authentication failed")
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
			return fmt.Errorf("primary database not found: %w", err)
		}
		return fmt.Errorf("internal error: database access failed")
	}

	// Get Users bundle
	usersBundle, err := us.bundleService.GetBundleByName(primaryDB, "Users")
	if err != nil {
		if us.debugMode {
			return fmt.Errorf("Users bundle not found: %w", err)
		}
		return fmt.Errorf("internal error: user storage not available")
	}

	// Find user document (case-insensitive)
	usernameLower := strings.ToLower(username)
	var targetDocID string
	docs := *usersBundle.Documents

	for docID, doc := range docs {
		if nameField, ok := doc.Fields["Username"]; ok {
			if nameValue, ok := nameField.Value.AsString(); ok {
				if strings.ToLower(nameValue) == usernameLower {
					targetDocID = docID
					break
				}
			}
		}
	}

	if targetDocID == "" {
		if us.debugMode {
			return fmt.Errorf("user '%s' not found", username)
		}
		return fmt.Errorf("user not found")
	}

	// Check for active sessions if not forcing
	serviceManager := GetServiceManager()
	if !force && serviceManager.SessionManager != nil {
		sessions := serviceManager.SessionManager.GetUserSessions(username)
		if len(sessions) > 0 {
			return fmt.Errorf("user '%s' has %d active session(s). Use FORCE to terminate sessions and proceed",
				username, len(sessions))
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

	// Get target document for modification
	targetDoc := docs[targetDocID]

	// Apply updates
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
						if us.debugMode {
							return fmt.Errorf("failed to hash new password: %w", err)
						}
						return fmt.Errorf("internal error: password update failed")
					}

					// Update PasswordHash field in document
					targetDoc.Fields["PasswordHash"] = models.Field{
						Name:  "PasswordHash",
						Value: models.NewStringValue(string(storedUser.PasswordHash.Hash)),
					}
				}
			}
		default:
			if us.debugMode {
				us.logger.Warnf("Ignoring unsupported update field: %s", field)
			}
		}
	}

	// Save updated document back to bundle
	(*usersBundle.Documents)[targetDocID] = targetDoc

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
			return fmt.Errorf("primary database not found: %w", err)
		}
		return fmt.Errorf("internal error: database access failed")
	}

	// Get Users bundle
	usersBundle, err := us.bundleService.GetBundleByName(primaryDB, "Users")
	if err != nil {
		if us.debugMode {
			return fmt.Errorf("Users bundle not found: %w", err)
		}
		return fmt.Errorf("internal error: user storage not available")
	}

	// Find user document (case-insensitive)
	usernameLower := strings.ToLower(username)
	var userID string
	var targetDocID string
	docs := *usersBundle.Documents

	for docID, doc := range docs {
		if nameField, ok := doc.Fields["Username"]; ok {
			if nameValue, ok := nameField.Value.AsString(); ok {
				if strings.ToLower(nameValue) == usernameLower {
					// Get UserID for junction table cleanup
					if idField, ok := doc.Fields["UserID"]; ok {
						if id, ok := idField.Value.AsString(); ok {
							userID = id
						}
					}
					targetDocID = docID
					break
				}
			}
		}
	}

	if targetDocID == "" {
		if us.debugMode {
			return fmt.Errorf("user '%s' not found", username)
		}
		return fmt.Errorf("user not found")
	}

	// Check for active sessions if not forcing
	serviceManager := GetServiceManager()
	if !force && serviceManager.SessionManager != nil {
		sessions := serviceManager.SessionManager.GetUserSessions(username)
		if len(sessions) > 0 {
			return fmt.Errorf("user '%s' has %d active session(s). Use FORCE to terminate sessions and proceed",
				username, len(sessions))
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

	// Remove user document from Users bundle (delete from map)
	delete(*usersBundle.Documents, targetDocID)

	us.logger.Infof("User '%s' deleted successfully", username)
	return nil
}

// cleanupJunctionTable removes all records matching a specific field value
// This is used for cascade deletion of junction table records
func (us *UserService) cleanupJunctionTable(junctionBundle *models.Bundle, fieldName, fieldValue, tableName string) {
	if junctionBundle.Documents == nil {
		return
	}

	docs := *junctionBundle.Documents
	removedCount := 0

	// Collect docIDs to delete
	toDelete := make([]string, 0)
	for docID, doc := range docs {
		if field, ok := doc.Fields[fieldName]; ok {
			if value, ok := field.Value.AsString(); ok && value == fieldValue {
				toDelete = append(toDelete, docID)
			}
		}
	}

	// Delete collected documents
	for _, docID := range toDelete {
		delete(*junctionBundle.Documents, docID)
		removedCount++
	}

	if removedCount > 0 {
		us.logger.Infof("Removed %d records from %s for %s=%s", removedCount, tableName, fieldName, fieldValue)
	}
}

// TODO: I will implement ListUsers with pagination and filtering
// TODO: I will implement LockoutUser and UnlockUser for security management
