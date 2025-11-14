package homegrown

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"

	"syndrdb/src/internal/auth"
	"syndrdb/src/internal/domain/models"
	"syndrdb/src/internal/server"
	"syndrdb/src/pkg/common/helpers"
)

/*
user_service_test.go

This file contains comprehensive unit tests for the UserService RBAC functionality.
It validates user creation, lookup, authentication, and error handling scenarios.

Test Coverage:
- User creation with valid credentials
- Duplicate username detection (case-insensitive)
- Invalid username validation (special characters, empty, etc.)
- Password hashing and verification
- User lookup by username and ID
- Edge cases and error scenarios

Design Principles:
- Single Responsibility: Each test validates one specific behavior
- DRY: Reuses setupUserServiceTest() helper for common initialization
- Open/Closed: Easy to extend with new test cases without modifying existing tests

TODO: I will add tests for concurrent user creation scenarios
TODO: I will add tests for user update functionality when implemented
TODO: I will add performance benchmarks for user creation at scale
*/

// setupUserServiceTest initializes a clean test environment for UserService testing
func setupUserServiceTest(t *testing.T) (*server.UserService, *models.Database, func()) {
	logger := zaptest.NewLogger(t).Sugar()

	// Create temporary directory for test database
	tempDir, err := os.MkdirTemp("", "syndrdb_userservice_test_*")
	require.NoError(t, err, "Failed to create temp directory")

	// Use the standard test database service
	databaseService, _, err := StandupTestDatabaseService()
	require.NoError(t, err, "Failed to setup database service")

	// Get primary database
	primaryDB, err := databaseService.GetDatabaseByName("primary")
	require.NoError(t, err, "Failed to get primary database")
	require.NotNil(t, primaryDB, "Primary database should exist")

	// Get bundle service from service manager
	serviceManager := server.GetServiceManager()
	require.NotNil(t, serviceManager, "ServiceManager should exist")
	require.NotNil(t, serviceManager.BundleService, "BundleService should exist")

	// Create UserStore for password hashing
	userStorePath := filepath.Join(tempDir, "users.dat")
	encryptionKey := "test-encryption-key-2025"
	userStore, err := auth.NewUserStore(userStorePath, encryptionKey)
	require.NoError(t, err, "Failed to create UserStore")

	// Initialize UserService
	debugMode := true // Enable debug mode for detailed error messages in tests
	userService := server.NewUserService(
		serviceManager.BundleService,
		databaseService,
		userStore,
		logger,
		debugMode,
	)

	// Cleanup function
	cleanup := func() {
		os.RemoveAll(tempDir)
	}

	return userService, primaryDB, cleanup
}

// TestCreateUser_ValidUser tests successful user creation with valid credentials
func TestCreateUser_ValidUser(t *testing.T) {
	userService, _, cleanup := setupUserServiceTest(t)
	defer cleanup()

	username := "alice"
	password := "SecurePassword123!"

	userID, err := userService.CreateUser(username, password)

	assert.NoError(t, err, "CreateUser should succeed with valid credentials")
	assert.NotEmpty(t, userID, "UserID should be generated")
	assert.Len(t, userID, 36, "UserID should be a valid UUID (36 chars)")
}

// TestCreateUser_DuplicateUsername tests case-insensitive duplicate username detection
func TestCreateUser_DuplicateUsername(t *testing.T) {
	userService, _, cleanup := setupUserServiceTest(t)
	defer cleanup()

	username := "bob"
	password := "SecurePassword123!"

	// Create first user
	_, err := userService.CreateUser(username, password)
	require.NoError(t, err, "First user creation should succeed")

	// Attempt to create duplicate with same case
	_, err = userService.CreateUser(username, password)
	assert.Error(t, err, "Should fail with duplicate username")
	assert.Contains(t, err.Error(), "already exists", "Error should indicate duplicate")

	// Attempt to create duplicate with different case (case-insensitive check)
	_, err = userService.CreateUser("BOB", password)
	assert.Error(t, err, "Should fail with case-insensitive duplicate")
	assert.Contains(t, err.Error(), "already exists", "Error should indicate duplicate")

	_, err = userService.CreateUser("Bob", password)
	assert.Error(t, err, "Should fail with mixed-case duplicate")
}

// TestCreateUser_InvalidUsername tests username validation rules
func TestCreateUser_InvalidUsername(t *testing.T) {
	userService, _, cleanup := setupUserServiceTest(t)
	defer cleanup()

	testCases := []struct {
		name        string
		username    string
		password    string
		expectError bool
		errorMsg    string
	}{
		{
			name:        "Empty username",
			username:    "",
			password:    "SecurePassword123!",
			expectError: true,
			errorMsg:    "username cannot be empty",
		},
		{
			name:        "Username with spaces",
			username:    "user name",
			password:    "SecurePassword123!",
			expectError: true,
			errorMsg:    "invalid username",
		},
		{
			name:        "Username with special chars",
			username:    "user@domain",
			password:    "SecurePassword123!",
			expectError: true,
			errorMsg:    "invalid username",
		},
		{
			name:        "Username starting with number",
			username:    "1user",
			password:    "SecurePassword123!",
			expectError: true,
			errorMsg:    "must start with a letter",
		},
		{
			name:        "Valid username with dash",
			username:    "user-name",
			password:    "SecurePassword123!",
			expectError: false,
		},
		{
			name:        "Valid username with underscore",
			username:    "user_name",
			password:    "SecurePassword123!",
			expectError: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := userService.CreateUser(tc.username, tc.password)

			if tc.expectError {
				assert.Error(t, err, "Should fail with invalid username: %s", tc.username)
				if tc.errorMsg != "" {
					assert.Contains(t, err.Error(), tc.errorMsg, "Error message should match")
				}
			} else {
				assert.NoError(t, err, "Should succeed with valid username: %s", tc.username)
			}
		})
	}
}

// TestCreateUser_PasswordHashing tests that passwords are properly hashed
func TestCreateUser_PasswordHashing(t *testing.T) {
	userService, primaryDB, cleanup := setupUserServiceTest(t)
	defer cleanup()

	username := "charlie"
	password := "SecurePassword123!"

	userID, err := userService.CreateUser(username, password)
	require.NoError(t, err, "User creation should succeed")

	// Retrieve user from Users bundle and verify password is hashed
	usersBundle, exists := primaryDB.Bundles["Users"]
	require.True(t, exists, "Users bundle should exist")
	require.NotNil(t, usersBundle.Documents, "Users bundle should have documents")

	userDoc, exists := (*usersBundle.Documents)[userID]
	require.True(t, exists, "User document should exist")

	passwordHashField, exists := userDoc.Fields["PasswordHash"]
	require.True(t, exists, "PasswordHash field should exist")

	passwordHash := passwordHashField.Value.(string)

	// Verify password is hashed (should start with Argon2id prefix)
	assert.NotEqual(t, password, passwordHash, "Password should be hashed, not stored in plaintext")
	assert.Greater(t, len(passwordHash), 50, "Hashed password should be significantly longer")

	// TODO: I will add verification that hash follows Argon2id format specification
}

// TestGetUserByUsername_Success tests successful user lookup by username
func TestGetUserByUsername_Success(t *testing.T) {
	userService, _, cleanup := setupUserServiceTest(t)
	defer cleanup()

	username := "diana"
	password := "SecurePassword123!"

	createdUserID, err := userService.CreateUser(username, password)
	require.NoError(t, err, "User creation should succeed")

	// Lookup by exact case
	user, err := userService.GetUserByUsername(username)
	assert.NoError(t, err, "GetUserByUsername should succeed")
	assert.NotNil(t, user, "User should be found")
	assert.Equal(t, createdUserID, user.Fields["UserID"].Value, "UserID should match")

	// Lookup by different case (case-insensitive)
	user, err = userService.GetUserByUsername("DIANA")
	assert.NoError(t, err, "GetUserByUsername should be case-insensitive")
	assert.NotNil(t, user, "User should be found with different case")

	user2, err2 := userService.GetUserByUsername("Diana")
	assert.NoError(t, err2, "GetUserByUsername should work with mixed case")
	assert.NotNil(t, user2, "User should be found with mixed case")
}

// TestGetUserByUsername_NotFound tests lookup for non-existent user
func TestGetUserByUsername_NotFound(t *testing.T) {
	userService, _, cleanup := setupUserServiceTest(t)
	defer cleanup()

	user, err := userService.GetUserByUsername("nonexistent")
	assert.Error(t, err, "Should fail for non-existent user")
	assert.Nil(t, user, "User should be nil")
	assert.Contains(t, err.Error(), "not found", "Error should indicate user not found")
}

// TestGetUserByID_Success tests successful user lookup by ID
func TestGetUserByID_Success(t *testing.T) {
	userService, _, cleanup := setupUserServiceTest(t)
	defer cleanup()

	username := "eve"
	password := "SecurePassword123!"

	userID, err := userService.CreateUser(username, password)
	require.NoError(t, err, "User creation should succeed")

	user, err := userService.GetUserByID(userID)
	assert.NoError(t, err, "GetUserByID should succeed")
	assert.NotNil(t, user, "User should be found")
	assert.Equal(t, username, user.Fields["Username"].Value, "Username should match")
}

// TestGetUserByID_NotFound tests lookup for non-existent user ID
func TestGetUserByID_NotFound(t *testing.T) {
	userService, _, cleanup := setupUserServiceTest(t)
	defer cleanup()

	fakeUserID := helpers.GenerateUUID()
	user, err := userService.GetUserByID(fakeUserID)
	assert.Error(t, err, "Should fail for non-existent user ID")
	assert.Nil(t, user, "User should be nil")
}

// TestValidateUserCredentials_Success tests successful password verification
func TestValidateUserCredentials_Success(t *testing.T) {
	userService, _, cleanup := setupUserServiceTest(t)
	defer cleanup()

	username := "frank"
	password := "SecurePassword123!"

	_, err := userService.CreateUser(username, password)
	require.NoError(t, err, "User creation should succeed")

	// Validate with correct password
	valid, err := userService.ValidateUserCredentials(username, password)
	assert.NoError(t, err, "Validation should succeed")
	assert.True(t, valid, "Credentials should be valid")

	// Validate with case-insensitive username
	valid, err = userService.ValidateUserCredentials("FRANK", password)
	assert.NoError(t, err, "Validation should work with different case username")
	assert.True(t, valid, "Credentials should be valid with case-insensitive username")
}

// TestValidateUserCredentials_InvalidPassword tests password verification with wrong password
func TestValidateUserCredentials_InvalidPassword(t *testing.T) {
	userService, _, cleanup := setupUserServiceTest(t)
	defer cleanup()

	username := "grace"
	password := "SecurePassword123!"

	_, err := userService.CreateUser(username, password)
	require.NoError(t, err, "User creation should succeed")

	// Validate with incorrect password
	valid, err := userService.ValidateUserCredentials(username, "WrongPassword!")
	assert.NoError(t, err, "Validation function should not error on wrong password")
	assert.False(t, valid, "Credentials should be invalid")
}

// TestValidateUserCredentials_NonExistentUser tests validation for non-existent user
func TestValidateUserCredentials_NonExistentUser(t *testing.T) {
	userService, _, cleanup := setupUserServiceTest(t)
	defer cleanup()

	valid, err := userService.ValidateUserCredentials("nonexistent", "SomePassword!")
	assert.Error(t, err, "Should fail for non-existent user")
	assert.False(t, valid, "Credentials should be invalid")
}

// TestCreateUser_EmptyPassword tests validation of empty password
func TestCreateUser_EmptyPassword(t *testing.T) {
	userService, _, cleanup := setupUserServiceTest(t)
	defer cleanup()

	_, err := userService.CreateUser("testuser", "")
	assert.Error(t, err, "Should fail with empty password")
	// TODO: I will add specific error message validation when password policy is enhanced
}

// TestCreateUser_ShortPassword tests password minimum length validation
func TestCreateUser_ShortPassword(t *testing.T) {
	userService, _, cleanup := setupUserServiceTest(t)
	defer cleanup()

	_, err := userService.CreateUser("testuser", "short")
	assert.Error(t, err, "Should fail with password shorter than 8 characters")
	// TODO: I will add configurable password policies (min length, complexity requirements)
}

// TestUserService_ConcurrentCreation tests concurrent user creation safety
func TestUserService_ConcurrentCreation(t *testing.T) {
	_, _, cleanup := setupUserServiceTest(t)
	defer cleanup()

	// TODO: I will implement comprehensive concurrent user creation tests
	// This will verify thread-safety of the UserService when multiple
	// goroutines attempt to create users simultaneously

	t.Skip("Concurrent creation tests not yet implemented")
}
