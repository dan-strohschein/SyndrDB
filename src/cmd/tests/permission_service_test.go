package main

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
)

/*
permission_service_test.go

This file contains comprehensive unit tests for the PermissionService RBAC functionality.
It validates permission grants, role grants, revocations, permission inheritance through roles,
and duplicate prevention.

Test Coverage:
- Direct permission grants to users
- Role grants to users
- Permission revocation from users
- Role revocation from users
- Permission inheritance through roles (Many-to-Many)
- Duplicate grant prevention
- Non-existent user/role/permission handling
- Permission resolution (UserHasPermission with role traversal)

Design Principles:
- Single Responsibility: Each test validates one specific behavior
- DRY: Reuses setupPermissionServiceTest() helper for common initialization
- Open/Closed: Easy to extend with new test cases without modifying existing tests

TODO: I will add tests for nested role hierarchies when implemented
TODO: I will add tests for permission wildcards (e.g., "Database.*")
TODO: I will add performance benchmarks for permission resolution at scale
*/

// setupPermissionServiceTest initializes a clean test environment for PermissionService testing
func setupPermissionServiceTest(t *testing.T) (*server.PermissionService, *server.UserService, *models.Database, func()) {
	logger := zaptest.NewLogger(t).Sugar()

	// Create temporary directory for test database
	tempDir, err := os.MkdirTemp("", "syndrdb_permservice_test_*")
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

	// Initialize services
	debugMode := true // Enable debug mode for detailed error messages in tests
	userService := server.NewUserService(
		serviceManager.BundleService,
		databaseService,
		userStore,
		logger,
		debugMode,
	)
	permissionService := server.NewPermissionService(
		serviceManager.BundleService,
		databaseService,
		logger,
		debugMode,
	)

	// Cleanup function
	cleanup := func() {
		os.RemoveAll(tempDir)
	}

	return permissionService, userService, primaryDB, cleanup
}

// TestGrantPermissionToUser_Success tests successful permission grant
func TestGrantPermissionToUser_Success(t *testing.T) {
	permService, userService, _, cleanup := setupPermissionServiceTest(t)
	defer cleanup()

	// Create a test user
	username := "alice"
	password := "SecurePassword123!"
	_, err := userService.CreateUser(username, password)
	require.NoError(t, err, "User creation should succeed")

	// Grant permission
	err = permService.GrantPermissionToUser(username, "Read")
	assert.NoError(t, err, "GrantPermissionToUser should succeed")

	// Verify permission was granted
	hasPermission, err := permService.UserHasPermission(username, "Read")
	assert.NoError(t, err, "UserHasPermission check should succeed")
	assert.True(t, hasPermission, "User should have the granted permission")
}

// TestGrantPermissionToUser_DuplicatePrevention tests that duplicate grants are prevented
func TestGrantPermissionToUser_DuplicatePrevention(t *testing.T) {
	permService, userService, _, cleanup := setupPermissionServiceTest(t)
	defer cleanup()

	// Create a test user
	username := "bob"
	password := "SecurePassword123!"
	_, err := userService.CreateUser(username, password)
	require.NoError(t, err, "User creation should succeed")

	// Grant permission first time
	err = permService.GrantPermissionToUser(username, "Write")
	require.NoError(t, err, "First grant should succeed")

	// Attempt duplicate grant
	err = permService.GrantPermissionToUser(username, "Write")
	assert.Error(t, err, "Duplicate grant should fail")
	assert.Contains(t, err.Error(), "already has permission", "Error should indicate duplicate")
}

// TestGrantPermissionToUser_NonExistentUser tests grant to non-existent user
func TestGrantPermissionToUser_NonExistentUser(t *testing.T) {
	permService, _, _, cleanup := setupPermissionServiceTest(t)
	defer cleanup()

	err := permService.GrantPermissionToUser("nonexistent", "Read")
	assert.Error(t, err, "Should fail for non-existent user")
	assert.Contains(t, err.Error(), "not found", "Error should indicate user not found")
}

// TestGrantPermissionToUser_CreatesMissingPermission tests automatic permission creation
func TestGrantPermissionToUser_CreatesMissingPermission(t *testing.T) {
	permService, userService, primaryDB, cleanup := setupPermissionServiceTest(t)
	defer cleanup()

	// Create a test user
	username := "charlie"
	password := "SecurePassword123!"
	_, err := userService.CreateUser(username, password)
	require.NoError(t, err, "User creation should succeed")

	// Grant a custom permission that doesn't exist yet
	customPermission := "CustomPermission"
	err = permService.GrantPermissionToUser(username, customPermission)
	assert.NoError(t, err, "Grant should succeed and create permission")

	// Verify permission was created in Permissions bundle
	permissionsBundle, exists := primaryDB.Bundles["Permissions"]
	require.True(t, exists, "Permissions bundle should exist")
	require.NotNil(t, permissionsBundle.Documents, "Permissions bundle should have documents")

	found := false
	for _, doc := range *permissionsBundle.Documents {
		if nameField, ok := doc.Fields["PermissionName"]; ok {
			if nameField.Value == customPermission {
				found = true
				break
			}
		}
	}
	assert.True(t, found, "Custom permission should be created in Permissions bundle")
}

// TestGrantRoleToUser_Success tests successful role grant
func TestGrantRoleToUser_Success(t *testing.T) {
	permService, userService, _, cleanup := setupPermissionServiceTest(t)
	defer cleanup()

	// Create a test user
	username := "diana"
	password := "SecurePassword123!"
	_, err := userService.CreateUser(username, password)
	require.NoError(t, err, "User creation should succeed")

	// Grant role (Data-Writer exists from hydration)
	err = permService.GrantRoleToUser(username, "Data-Writer")
	assert.NoError(t, err, "GrantRoleToUser should succeed")

	// Verify user has permissions from the role
	// Data-Writer role should have Write and Read-Write permissions
	hasWrite, err := permService.UserHasPermission(username, "Write")
	assert.NoError(t, err, "UserHasPermission check should succeed")
	assert.True(t, hasWrite, "User should have Write permission via Data-Writer role")

	hasReadWrite, err := permService.UserHasPermission(username, "Read-Write")
	assert.NoError(t, err, "UserHasPermission check should succeed")
	assert.True(t, hasReadWrite, "User should have Read-Write permission via Data-Writer role")
}

// TestGrantRoleToUser_DuplicatePrevention tests that duplicate role grants are prevented
func TestGrantRoleToUser_DuplicatePrevention(t *testing.T) {
	permService, userService, _, cleanup := setupPermissionServiceTest(t)
	defer cleanup()

	// Create a test user
	username := "eve"
	password := "SecurePassword123!"
	_, err := userService.CreateUser(username, password)
	require.NoError(t, err, "User creation should succeed")

	// Grant role first time
	err = permService.GrantRoleToUser(username, "Data-Reader")
	require.NoError(t, err, "First role grant should succeed")

	// Attempt duplicate grant
	err = permService.GrantRoleToUser(username, "Data-Reader")
	assert.Error(t, err, "Duplicate role grant should fail")
	assert.Contains(t, err.Error(), "already has role", "Error should indicate duplicate")
}

// TestGrantRoleToUser_NonExistentRole tests grant of non-existent role
func TestGrantRoleToUser_NonExistentRole(t *testing.T) {
	permService, userService, _, cleanup := setupPermissionServiceTest(t)
	defer cleanup()

	// Create a test user
	username := "frank"
	password := "SecurePassword123!"
	_, err := userService.CreateUser(username, password)
	require.NoError(t, err, "User creation should succeed")

	// Attempt to grant non-existent role
	err = permService.GrantRoleToUser(username, "NonExistentRole")
	assert.Error(t, err, "Should fail for non-existent role")
	assert.Contains(t, err.Error(), "not found", "Error should indicate role not found")
}

// TestRevokePermissionFromUser_Success tests successful permission revocation
func TestRevokePermissionFromUser_Success(t *testing.T) {
	permService, userService, _, cleanup := setupPermissionServiceTest(t)
	defer cleanup()

	// Create a test user
	username := "grace"
	password := "SecurePassword123!"
	_, err := userService.CreateUser(username, password)
	require.NoError(t, err, "User creation should succeed")

	// Grant permission
	err = permService.GrantPermissionToUser(username, "Read")
	require.NoError(t, err, "Grant should succeed")

	// Verify permission exists
	hasPermission, err := permService.UserHasPermission(username, "Read")
	require.NoError(t, err)
	require.True(t, hasPermission, "User should have permission before revocation")

	// Revoke permission
	err = permService.RevokePermissionFromUser(username, "Read")
	assert.NoError(t, err, "RevokePermissionFromUser should succeed")

	// Verify permission was revoked
	hasPermission, err = permService.UserHasPermission(username, "Read")
	assert.NoError(t, err, "UserHasPermission check should succeed")
	assert.False(t, hasPermission, "User should not have permission after revocation")
}

// TestRevokePermissionFromUser_NotGranted tests revocation of permission that wasn't granted
func TestRevokePermissionFromUser_NotGranted(t *testing.T) {
	permService, userService, _, cleanup := setupPermissionServiceTest(t)
	defer cleanup()

	// Create a test user
	username := "henry"
	password := "SecurePassword123!"
	_, err := userService.CreateUser(username, password)
	require.NoError(t, err, "User creation should succeed")

	// Attempt to revoke permission that was never granted
	err = permService.RevokePermissionFromUser(username, "Admin")
	assert.Error(t, err, "Should fail when revoking non-granted permission")
	assert.Contains(t, err.Error(), "does not have permission", "Error should indicate permission not granted")
}

// TestRevokeRoleFromUser_Success tests successful role revocation
func TestRevokeRoleFromUser_Success(t *testing.T) {
	permService, userService, _, cleanup := setupPermissionServiceTest(t)
	defer cleanup()

	// Create a test user
	username := "iris"
	password := "SecurePassword123!"
	_, err := userService.CreateUser(username, password)
	require.NoError(t, err, "User creation should succeed")

	// Grant role
	err = permService.GrantRoleToUser(username, "Data-Writer")
	require.NoError(t, err, "Grant role should succeed")

	// Verify user has permission from role
	hasPermission, err := permService.UserHasPermission(username, "Write")
	require.NoError(t, err)
	require.True(t, hasPermission, "User should have permission before role revocation")

	// Revoke role
	err = permService.RevokeRoleFromUser(username, "Data-Writer")
	assert.NoError(t, err, "RevokeRoleFromUser should succeed")

	// Verify user no longer has permission from role
	hasPermission, err = permService.UserHasPermission(username, "Write")
	assert.NoError(t, err, "UserHasPermission check should succeed")
	assert.False(t, hasPermission, "User should not have permission after role revocation")
}

// TestUserHasPermission_DirectAndViaRole tests permission resolution through both direct grant and role
func TestUserHasPermission_DirectAndViaRole(t *testing.T) {
	permService, userService, _, cleanup := setupPermissionServiceTest(t)
	defer cleanup()

	// Create a test user
	username := "jack"
	password := "SecurePassword123!"
	_, err := userService.CreateUser(username, password)
	require.NoError(t, err, "User creation should succeed")

	// Grant direct permission
	err = permService.GrantPermissionToUser(username, "Admin")
	require.NoError(t, err)

	// Grant role with different permissions
	err = permService.GrantRoleToUser(username, "Data-Reader")
	require.NoError(t, err)

	// Check direct permission
	hasAdmin, err := permService.UserHasPermission(username, "Admin")
	assert.NoError(t, err)
	assert.True(t, hasAdmin, "User should have direct permission")

	// Check permission from role
	hasRead, err := permService.UserHasPermission(username, "Read")
	assert.NoError(t, err)
	assert.True(t, hasRead, "User should have permission from role")

	// Check permission user doesn't have
	hasWrite, err := permService.UserHasPermission(username, "Write")
	assert.NoError(t, err)
	assert.False(t, hasWrite, "User should not have permission they weren't granted")
}

// TestUserHasPermission_MultipleRoles tests permission resolution with multiple roles
func TestUserHasPermission_MultipleRoles(t *testing.T) {
	permService, userService, _, cleanup := setupPermissionServiceTest(t)
	defer cleanup()

	// Create a test user
	username := "karen"
	password := "SecurePassword123!"
	_, err := userService.CreateUser(username, password)
	require.NoError(t, err, "User creation should succeed")

	// Grant multiple roles
	err = permService.GrantRoleToUser(username, "Data-Reader")
	require.NoError(t, err)
	err = permService.GrantRoleToUser(username, "Data-Writer")
	require.NoError(t, err)

	// User should have permissions from both roles
	hasRead, err := permService.UserHasPermission(username, "Read")
	assert.NoError(t, err)
	assert.True(t, hasRead, "User should have Read from Data-Reader role")

	hasWrite, err := permService.UserHasPermission(username, "Write")
	assert.NoError(t, err)
	assert.True(t, hasWrite, "User should have Write from Data-Writer role")

	hasReadWrite, err := permService.UserHasPermission(username, "Read-Write")
	assert.NoError(t, err)
	assert.True(t, hasReadWrite, "User should have Read-Write from Data-Writer role")
}

// TestUserHasPermission_CaseSensitive tests that permission names are case-sensitive
func TestUserHasPermission_CaseSensitive(t *testing.T) {
	permService, userService, _, cleanup := setupPermissionServiceTest(t)
	defer cleanup()

	// Create a test user
	username := "larry"
	password := "SecurePassword123!"
	_, err := userService.CreateUser(username, password)
	require.NoError(t, err, "User creation should succeed")

	// Grant permission with specific case
	err = permService.GrantPermissionToUser(username, "Read")
	require.NoError(t, err)

	// Check with exact case - should work
	hasPermission, err := permService.UserHasPermission(username, "Read")
	assert.NoError(t, err)
	assert.True(t, hasPermission, "Should find permission with exact case")

	// Check with different case - should fail (case-sensitive)
	hasPermission, err = permService.UserHasPermission(username, "READ")
	assert.NoError(t, err)
	assert.False(t, hasPermission, "Should not find permission with different case")

	hasPermission, err = permService.UserHasPermission(username, "read")
	assert.NoError(t, err)
	assert.False(t, hasPermission, "Should not find permission with lowercase")
}

// TestPermissionService_DefaultRolePermissions tests that default roles have correct permissions
func TestPermissionService_DefaultRolePermissions(t *testing.T) {
	permService, userService, _, cleanup := setupPermissionServiceTest(t)
	defer cleanup()

	testCases := []struct {
		username      string
		role          string
		shouldHave    []string
		shouldNotHave []string
	}{
		{
			username:      "dbo_user",
			role:          "Dbo",
			shouldHave:    []string{"Read", "Write", "Admin", "Read-Write"},
			shouldNotHave: []string{},
		},
		{
			username:      "reader_user",
			role:          "Data-Reader",
			shouldHave:    []string{"Read"},
			shouldNotHave: []string{"Write", "Admin", "Read-Write"},
		},
		{
			username:      "writer_user",
			role:          "Data-Writer",
			shouldHave:    []string{"Write", "Read-Write"},
			shouldNotHave: []string{"Read", "Admin"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.role, func(t *testing.T) {
			// Create user
			_, err := userService.CreateUser(tc.username, "SecurePassword123!")
			require.NoError(t, err)

			// Grant role
			err = permService.GrantRoleToUser(tc.username, tc.role)
			require.NoError(t, err)

			// Check expected permissions
			for _, perm := range tc.shouldHave {
				has, err := permService.UserHasPermission(tc.username, perm)
				assert.NoError(t, err)
				assert.True(t, has, "User with %s role should have %s permission", tc.role, perm)
			}

			// Check permissions user should not have
			for _, perm := range tc.shouldNotHave {
				has, err := permService.UserHasPermission(tc.username, perm)
				assert.NoError(t, err)
				assert.False(t, has, "User with %s role should not have %s permission", tc.role, perm)
			}
		})
	}
}

// TestPermissionService_ConcurrentGrants tests concurrent permission grants
func TestPermissionService_ConcurrentGrants(t *testing.T) {
	_, _, _, cleanup := setupPermissionServiceTest(t)
	defer cleanup()

	// TODO: I will implement comprehensive concurrent grant tests
	// This will verify thread-safety of the PermissionService when multiple
	// goroutines attempt to grant permissions/roles simultaneously

	t.Skip("Concurrent grant tests not yet implemented")
}
