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

// TODO: I will implement UpdateUser for password changes and profile updates
// TODO: I will implement DeleteUser for user removal (with cascade options)
// TODO: I will implement ListUsers with pagination and filtering
// TODO: I will implement LockoutUser and UnlockUser for security management
