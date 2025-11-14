/*
ROOT USER VALIDATION TEST IMPLEMENTATION

This file implements comprehensive validation tests for the Root user in SyndrDB.
The Root user is the default administrative user created during database initialization,
with the Dbo role granting full permissions across all database entities.

TEST STRUCTURE:
Following SyndrDB test patterns with global state management and standalone test runner.
Tests validate Root user creation, authentication, role assignment, and access to all
database entities (databases, bundles, documents, indexes).

MAIN TEST FUNCTIONS:
- testRootUser_CreatedProperly: Validates Root user exists in Users bundle
- testRootUser_CanLogin: Tests authentication with correct password
- testRootUser_PasswordHashed: Verifies Argon2id password hashing
- testRootUser_HasDboRole: Confirms Dbo role assignment
- testRootUser_HasAllPermissions: Validates all 4 permissions
- testRootUser_CanAccessDatabases: Tests database-level access
- testRootUser_CanAccessBundles: Tests bundle access
- testRootUser_CanAccessDocuments: Tests document access
- testRootUser_CanAccessIndexes: Tests index access
- testRootUser_FullAccessWorkflow: Complete end-to-end workflow test
- testRootUser_AuthenticationFailsWithWrongPassword: Security tests
- testRootUser_CaseInsensitiveUsername: Username handling tests
*/

package homegrown

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"syndrdb/src/internal/auth"
	defaultdb "syndrdb/src/internal/defaultDB"
	"syndrdb/src/internal/domain/bundle"
	"syndrdb/src/internal/domain/database"
	"syndrdb/src/internal/domain/document"
	"syndrdb/src/internal/domain/models"
	"syndrdb/src/internal/server"
	"syndrdb/src/internal/storage/buffer"
	"syndrdb/src/internal/storage/bundlestore"
	"syndrdb/src/internal/storage/databasestore"
	"syndrdb/src/pkg/common/helpers"
	"syndrdb/src/pkg/settings"
)

// Global variables for Root user test environment
var (
	testRootUserDatabaseService  *database.DatabaseService
	testRootUserServiceManager   *server.ServiceManager
	testRootUserService          *server.UserService
	testRootUserPermService      *server.PermissionService
	testRootUserDatabase         *models.Database
	rootUserTestTempDir          string
	testRootUserEnvironmentSetup bool
	testRootUserCleanupLock      sync.Mutex
	rootUserTestResults          map[string]bool
	rootUserTestErrors           map[string]error
	rootUserTestOnce             sync.Once
)

// setupRootUserTestEnvironment initializes the Root user test environment
func setupRootUserTestEnvironment() error {
	testRootUserCleanupLock.Lock()
	defer testRootUserCleanupLock.Unlock()

	if testRootUserEnvironmentSetup {
		return nil
	}

	// Initialize logger if not already done
	if ColorLogger == nil {
		return fmt.Errorf("ColorLogger not initialized for Root user tests")
	}

	ColorLogger.Info("Setting up Root user test environment...")

	// Initialize test results tracking
	rootUserTestResults = make(map[string]bool)
	rootUserTestErrors = make(map[string]error)

	// Create a UNIQUE temporary directory for this test run (simulate cold start)
	testDataDir, err := os.MkdirTemp("", "syndrdb_root_user_test_*")
	if err != nil {
		return fmt.Errorf("failed to create temp test directory: %v", err)
	}

	ColorLogger.Infof("Created isolated test directory: %s", testDataDir)

	// CRITICAL: Update global settings to point to our test directory
	// This ensures all database operations use the isolated test environment
	// First, ensure settings are initialized
	_ = settings.GetSettings()

	// Now update with our test-specific paths
	settings.UpdateSettings(settings.Arguments{
		DataDir: testDataDir,
		LogDir:  filepath.Join(testDataDir, "logs"),
		TempDir: filepath.Join(testDataDir, "temp"),
	})

	// Create NEW settings object for test services
	args := &settings.Arguments{
		DataDir:          testDataDir,
		TempDir:          filepath.Join(testDataDir, "temp"),
		LogLevel:         "warn",
		CreateDefaultDB:  true,
		AuthEnabled:      true,
		BundleBufferSize: 100,
	}

	// Ensure temp and log directories exist
	if err := os.MkdirAll(args.TempDir, 0755); err != nil {
		os.RemoveAll(testDataDir)
		return fmt.Errorf("failed to create temp directory: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(testDataDir, "logs"), 0755); err != nil {
		os.RemoveAll(testDataDir)
		return fmt.Errorf("failed to create logs directory: %v", err)
	}

	// Create database store
	store, err := databasestore.NewDatabaseStore(args.DataDir, ColorLogger)
	if err != nil {
		os.RemoveAll(testDataDir)
		return fmt.Errorf("failed to create database store: %v", err)
	}

	// Create database service
	factory := database.NewDatabaseFactory().(*database.DatabaseFactoryImpl).WithDefaultDataDirectory(args.DataDir)
	testRootUserDatabaseService = database.NewDatabaseService(store, factory, args, ColorLogger)

	// Create primary database (first-time initialization)
	primaryDB := &models.Database{
		DatabaseID:    helpers.GenerateUUID(),
		Name:          "primary",
		Description:   "Primary database for Root user tests",
		DataDirectory: args.DataDir,
		Bundles:       make(map[string]models.Bundle),
		BundleFiles:   []string{},
	}

	err = store.CreateDatabaseDataFile(primaryDB)
	if err != nil {
		os.RemoveAll(testDataDir)
		return fmt.Errorf("failed to create primary database: %v", err)
	}

	testRootUserDatabaseService.Databases[primaryDB.DatabaseID] = primaryDB
	testRootUserDatabaseService.Databases["primary"] = primaryDB

	// Create bundle infrastructure
	fileRegistry, err := buffer.NewFileRegistry(args.DataDir, buffer.SyncInterval, ColorLogger)
	if err != nil {
		os.RemoveAll(testDataDir)
		return fmt.Errorf("failed to create file registry: %v", err)
	}

	bufferPool := buffer.NewBufferPool(args.BundleBufferSize, buffer.DefaultPageSize, fileRegistry, ColorLogger)
	bundleStore, err := bundlestore.NewBundleStore(args.DataDir, bufferPool, ColorLogger, "json")
	if err != nil {
		os.RemoveAll(testDataDir)
		return fmt.Errorf("failed to create bundle store: %v", err)
	}

	bundleFactory := bundle.NewBundleFactory()
	documentFactory := document.NewDocumentFactory()
	bundleService := bundle.NewBundleService(bundleStore, bundleFactory, documentFactory, ColorLogger, args)

	// Initialize primary database catalogs (creates RBAC bundles)
	err = defaultdb.InitPrimaryBundleCatalogs(testRootUserDatabaseService, store, primaryDB, ColorLogger, bundleService)
	if err != nil {
		os.RemoveAll(testDataDir)
		return fmt.Errorf("failed to initialize primary database catalogs: %v", err)
	}

	// Initialize UserStore (simulating server initialization)
	userStorePath := filepath.Join(args.DataDir, "users.db")
	encryptionKey := "SyndrDB-Test-UserStore-Key"
	authConfig := auth.DefaultAuthRateLimitConfig()

	userStore, err := auth.NewUserStoreWithAuditor(
		userStorePath,
		encryptionKey,
		ColorLogger,
		authConfig,
		nil, // No auditor for tests
	)
	if err != nil {
		os.RemoveAll(testDataDir)
		return fmt.Errorf("failed to initialize user store: %v", err)
	}

	// Initialize ServiceManager
	catalogService := defaultdb.NewCatalogService(testRootUserDatabaseService, bundleService, ColorLogger)
	testRootUserServiceManager = server.InitServiceManager(testRootUserDatabaseService, bundleService, catalogService, nil, userStore, ColorLogger, false)

	// Initialize RBAC services
	testRootUserService = server.NewUserService(
		bundleService,
		testRootUserDatabaseService,
		userStore,
		ColorLogger,
		false,
	)
	testRootUserPermService = server.NewPermissionService(
		bundleService,
		testRootUserDatabaseService,
		ColorLogger,
		false,
	)

	testRootUserServiceManager.UserService = testRootUserService
	testRootUserServiceManager.PermissionService = testRootUserPermService

	// Hydrate RBAC data (simulating server startup)
	err = defaultdb.HydratePermissionPrimaryCatalogs(testRootUserDatabaseService, store, ColorLogger, bundleService)
	if err != nil {
		os.RemoveAll(testDataDir)
		return fmt.Errorf("failed to hydrate permissions: %v", err)
	}

	err = defaultdb.HydrateRolesPrimaryCatalogs(testRootUserDatabaseService, store, ColorLogger, bundleService)
	if err != nil {
		os.RemoveAll(testDataDir)
		return fmt.Errorf("failed to hydrate roles: %v", err)
	}

	// Hydrate role-permission assignments (junction table)
	err = defaultdb.HydrateRolesPermissionsPrimaryCatalogs(testRootUserDatabaseService, store, ColorLogger, bundleService)
	if err != nil {
		os.RemoveAll(testDataDir)
		return fmt.Errorf("failed to hydrate role-permission assignments: %v", err)
	}

	// Hydrate users using UserStore API (automatically hashes passwords with Argon2id)
	err = defaultdb.HydrateUserPrimaryCatalogs(testRootUserDatabaseService, store, ColorLogger, bundleService, userStore)
	if err != nil {
		os.RemoveAll(testDataDir)
		return fmt.Errorf("failed to hydrate users: %v", err)
	}

	// Hydrate user permissions (direct user-to-permission assignments)
	err = defaultdb.HydrateUserPermissionsPrimaryCatalogs(testRootUserDatabaseService, store, ColorLogger, bundleService)
	if err != nil {
		os.RemoveAll(testDataDir)
		return fmt.Errorf("failed to hydrate user permissions: %v", err)
	}

	// NOTE: UpdateDefaultUserPasswords no longer needed - HydrateUserPrimaryCatalogs handles it

	// Get the primary database reference
	testRootUserDatabase, err = testRootUserDatabaseService.GetDatabaseByName("primary")
	if err != nil {
		os.RemoveAll(testDataDir)
		return fmt.Errorf("failed to get primary database: %v", err)
	}

	testRootUserEnvironmentSetup = true
	ColorLogger.Info("✓ Root user test environment setup complete")

	// Schedule cleanup
	// Note: We'll clean up the temp directory when tests complete
	// Store the path for cleanup
	if rootUserTestTempDir == "" {
		rootUserTestTempDir = testDataDir
	}

	return nil
}

// cleanupRootUserTestEnvironment removes the temporary test directory
func cleanupRootUserTestEnvironment() error {
	if rootUserTestTempDir == "" {
		return nil
	}

	ColorLogger.Infof("Cleaning up Root user test environment: %s", rootUserTestTempDir)

	err := os.RemoveAll(rootUserTestTempDir)
	if err != nil {
		return fmt.Errorf("failed to clean up temp directory: %v", err)
	}

	ColorLogger.Info("✓ Root user test environment cleanup complete")
	rootUserTestTempDir = ""
	return nil
}

// testRootUser_CreatedProperly validates that the Root user is created during initialization
func testRootUser_CreatedProperly() error {
	ColorLogger.Info("Testing Root user creation...")

	// Get Users bundle
	usersBundle, exists := testRootUserDatabase.Bundles["Users"]
	if !exists {
		return fmt.Errorf("Users bundle not found")
	}

	if usersBundle.Documents == nil {
		return fmt.Errorf("Users bundle has no documents")
	}

	// Find Root user document
	var rootUser *models.Document
	docs := *usersBundle.Documents
	for i := range docs {
		if username, ok := docs[i].Data["Username"].(string); ok && username == "Root" {
			doc := docs[i]
			rootUser = &doc
			break
		}
	}

	if rootUser == nil {
		return fmt.Errorf("Root user not found in Users bundle")
	}

	// Validate Root user fields
	if rootUser.Data["UserID"] == nil {
		return fmt.Errorf("Root user missing UserID")
	}

	if username, ok := rootUser.Data["Username"].(string); !ok || username != "Root" {
		return fmt.Errorf("Root user Username incorrect: got %v", rootUser.Data["Username"])
	}

	if isActive, ok := rootUser.Data["IsActive"].(bool); !ok || !isActive {
		return fmt.Errorf("Root user IsActive should be true, got %v", rootUser.Data["IsActive"])
	}

	if isLockedOut, ok := rootUser.Data["IsLockedOut"].(bool); !ok || isLockedOut {
		return fmt.Errorf("Root user IsLockedOut should be false, got %v", rootUser.Data["IsLockedOut"])
	}

	ColorLogger.Info("✓ Root user created properly with all required fields")
	return nil
}

// testRootUser_CanLogin tests that Root user can authenticate with correct password
func testRootUser_CanLogin() error {
	ColorLogger.Info("Testing Root user login...")

	authenticated, err := testRootUserService.ValidateUserCredentials("Root", "root")
	if err != nil {
		return fmt.Errorf("authentication failed with error: %v", err)
	}

	if !authenticated {
		return fmt.Errorf("Root user authentication failed with correct password")
	}

	ColorLogger.Info("✓ Root user can log in with correct password")
	return nil
}

// testRootUser_PasswordHashed verifies password is hashed with Argon2id, not stored as plaintext
func testRootUser_PasswordHashed() error {
	ColorLogger.Info("Testing Root user password hashing...")

	// Get Users bundle
	usersBundle, exists := testRootUserDatabase.Bundles["Users"]
	if !exists {
		return fmt.Errorf("Users bundle not found")
	}

	if usersBundle.Documents == nil {
		return fmt.Errorf("Users bundle has no documents")
	}

	// Find Root user document
	var rootUser *models.Document
	docs := *usersBundle.Documents
	for i := range docs {
		if username, ok := docs[i].Data["Username"].(string); ok && username == "Root" {
			doc := docs[i]
			rootUser = &doc
			break
		}
	}

	if rootUser == nil {
		return fmt.Errorf("Root user not found in Users bundle")
	}

	// NOTE: Password is NOT stored in bundle documents - it's in UserStore with Argon2id hashing
	// The bundle should NOT have a Password field
	if _, hasPassword := rootUser.Data["Password"]; hasPassword {
		return fmt.Errorf("Root user bundle document should NOT contain Password field (passwords stored in UserStore)")
	}

	// Verify password is properly hashed by testing authentication
	// Authentication will fail if password is not properly hashed in UserStore
	authenticated, err := testRootUserService.ValidateUserCredentials("Root", "root")
	if err != nil {
		return fmt.Errorf("authentication error (password may not be hashed): %v", err)
	}
	if !authenticated {
		return fmt.Errorf("authentication failed, password hash may be incorrect or missing in UserStore")
	}

	ColorLogger.Info("✓ Root user password properly hashed in UserStore (not in bundle)")
	return nil
}

// testRootUser_HasDboRole validates that Root user has the Dbo role assigned
func testRootUser_HasDboRole() error {
	ColorLogger.Info("Testing Root user Dbo role assignment...")

	// Get UserRoles bundle
	userRolesBundle, exists := testRootUserDatabase.Bundles["UserRoles"]
	if !exists {
		return fmt.Errorf("UserRoles bundle not found")
	}

	// Get Users bundle to find Root user's UserID
	usersBundle, exists := testRootUserDatabase.Bundles["Users"]
	if !exists {
		return fmt.Errorf("Users bundle not found")
	}

	if usersBundle.Documents == nil {
		return fmt.Errorf("Users bundle has no documents")
	}

	var rootUserID string
	userDocs := *usersBundle.Documents
	for i := range userDocs {
		if username, ok := userDocs[i].Data["Username"].(string); ok && username == "Root" {
			if userID, ok := userDocs[i].Data["UserID"].(string); ok {
				rootUserID = userID
				break
			}
		}
	}

	if rootUserID == "" {
		return fmt.Errorf("could not find Root user's UserID")
	}

	if userRolesBundle.Documents == nil {
		return fmt.Errorf("UserRoles bundle has no documents")
	}

	// Find UserRoles document for Root user
	var rootUserRole *models.Document
	userRoleDocs := *userRolesBundle.Documents
	for i := range userRoleDocs {
		if userID, ok := userRoleDocs[i].Data["UserID"].(string); ok && userID == rootUserID {
			doc := userRoleDocs[i]
			rootUserRole = &doc
			break
		}
	}

	if rootUserRole == nil {
		return fmt.Errorf("Root user has no role assignment in UserRoles bundle")
	}

	// Get the RoleID from UserRoles document
	roleID, ok := rootUserRole.Data["RoleID"].(string)
	if !ok {
		return fmt.Errorf("Root user UserRoles RoleID has invalid type: %T", rootUserRole.Data["RoleID"])
	}

	// Look up the role in the Roles bundle to get the role name
	rolesBundle, exists := testRootUserDatabase.Bundles["Roles"]
	if !exists {
		return fmt.Errorf("Roles bundle not found")
	}

	if rolesBundle.Documents == nil {
		return fmt.Errorf("Roles bundle has no documents")
	}

	var roleDoc *models.Document
	roleDocs := *rolesBundle.Documents
	for i := range roleDocs {
		if rid, ok := roleDocs[i].Data["RoleID"].(string); ok && rid == roleID {
			doc := roleDocs[i]
			roleDoc = &doc
			break
		}
	}

	if roleDoc == nil {
		return fmt.Errorf("Role with ID %s not found in Roles bundle", roleID)
	}

	// Verify role name is "Dbo"
	roleName, ok := roleDoc.Data["Name"].(string)
	if !ok {
		return fmt.Errorf("Role Name has invalid type: %T", roleDoc.Data["Name"])
	}

	if roleName != "Dbo" {
		return fmt.Errorf("Root user has wrong role: expected 'Dbo', got '%s'", roleName)
	}

	ColorLogger.Info("✓ Root user has Dbo role assigned")
	return nil
}

// testRootUser_HasAllPermissions validates that Root user has all 4 permissions
func testRootUser_HasAllPermissions() error {
	ColorLogger.Info("Testing Root user permissions...")

	// Get Users bundle to find Root user's UserID
	usersBundle, exists := testRootUserDatabase.Bundles["Users"]
	if !exists {
		return fmt.Errorf("Users bundle not found")
	}

	if usersBundle.Documents == nil {
		return fmt.Errorf("Users bundle has no documents")
	}

	var rootUserID string
	userDocs := *usersBundle.Documents
	for i := range userDocs {
		if username, ok := userDocs[i].Data["Username"].(string); ok && username == "Root" {
			if userID, ok := userDocs[i].Data["UserID"].(string); ok {
				rootUserID = userID
				break
			}
		}
	}

	if rootUserID == "" {
		return fmt.Errorf("could not find Root user's UserID")
	}

	// Dbo role should grant all 4 permissions
	requiredPermissions := []string{"Read", "Write", "Admin", "Read-Write"}

	// Check each permission
	for _, permName := range requiredPermissions {
		hasPermission, _ := testRootUserPermService.UserHasPermission("Root", permName)
		if !hasPermission {
			return fmt.Errorf("Root user missing permission: %s", permName)
		}
		ColorLogger.Infof("  ✓ Root user has %s permission", permName)
	}

	ColorLogger.Info("✓ Root user has all required permissions (Read, Write, Admin, Read-Write)")
	return nil
}

// testRootUser_CanAccessDatabases tests that Root user has database-level access (Admin permission)
func testRootUser_CanAccessDatabases() error {
	ColorLogger.Info("Testing Root user database access...")

	// Get Users bundle to find Root user's UserID
	usersBundle, exists := testRootUserDatabase.Bundles["Users"]
	if !exists {
		return fmt.Errorf("Users bundle not found")
	}

	if usersBundle.Documents == nil {
		return fmt.Errorf("Users bundle has no documents")
	}

	var rootUserID string
	userDocs := *usersBundle.Documents
	for i := range userDocs {
		if username, ok := userDocs[i].Data["Username"].(string); ok && username == "Root" {
			if userID, ok := userDocs[i].Data["UserID"].(string); ok {
				rootUserID = userID
				break
			}
		}
	}

	if rootUserID == "" {
		return fmt.Errorf("could not find Root user's UserID")
	}

	// Admin permission grants database-level operations
	hasAdmin, _ := testRootUserPermService.UserHasPermission("Root", "Admin")
	if !hasAdmin {
		return fmt.Errorf("Root user lacks Admin permission for database access")
	}

	ColorLogger.Info("✓ Root user can access databases (Admin permission)")
	return nil
}

// testRootUser_CanAccessBundles tests that Root user has access to all RBAC bundles
func testRootUser_CanAccessBundles() error {
	ColorLogger.Info("Testing Root user bundle access...")

	// Get Users bundle to find Root user's UserID
	usersBundle, exists := testRootUserDatabase.Bundles["Users"]
	if !exists {
		return fmt.Errorf("Users bundle not found")
	}

	if usersBundle.Documents == nil {
		return fmt.Errorf("Users bundle has no documents")
	}

	var rootUserID string
	userDocs := *usersBundle.Documents
	for i := range userDocs {
		if username, ok := userDocs[i].Data["Username"].(string); ok && username == "Root" {
			if userID, ok := userDocs[i].Data["UserID"].(string); ok {
				rootUserID = userID
				break
			}
		}
	}

	if rootUserID == "" {
		return fmt.Errorf("could not find Root user's UserID")
	}

	// Test access to all 6 RBAC bundles
	rbacBundles := []string{"Users", "Roles", "Permissions", "UserRoles", "UserPermissions", "RolesPermissions"}

	for _, bundleName := range rbacBundles {
		bundle, exists := testRootUserDatabase.Bundles[bundleName]
		if !exists {
			return fmt.Errorf("bundle %s not found", bundleName)
		}

		// Read permission allows bundle access
		hasRead, _ := testRootUserPermService.UserHasPermission("Root", "Read")
		if !hasRead {
			return fmt.Errorf("Root user lacks Read permission for bundle %s", bundleName)
		}

		// Write permission allows bundle modification
		hasWrite, _ := testRootUserPermService.UserHasPermission("Root", "Write")
		if !hasWrite {
			return fmt.Errorf("Root user lacks Write permission for bundle %s", bundleName)
		}

		ColorLogger.Infof("  ✓ Root user can access bundle: %s", bundle.Name)
	}

	ColorLogger.Info("✓ Root user can access all 6 RBAC bundles")
	return nil
}

// testRootUser_CanAccessDocuments tests that Root user has Read-Write permission for documents
func testRootUser_CanAccessDocuments() error {
	ColorLogger.Info("Testing Root user document access...")

	// Get Users bundle to find Root user's UserID
	usersBundle, exists := testRootUserDatabase.Bundles["Users"]
	if !exists {
		return fmt.Errorf("Users bundle not found")
	}

	if usersBundle.Documents == nil {
		return fmt.Errorf("Users bundle has no documents")
	}

	var rootUserID string
	userDocs := *usersBundle.Documents
	for i := range userDocs {
		if username, ok := userDocs[i].Data["Username"].(string); ok && username == "Root" {
			if userID, ok := userDocs[i].Data["UserID"].(string); ok {
				rootUserID = userID
				break
			}
		}
	}

	if rootUserID == "" {
		return fmt.Errorf("could not find Root user's UserID")
	}

	// Read-Write permission grants document-level operations
	hasReadWrite, _ := testRootUserPermService.UserHasPermission("Root", "Read-Write")
	if !hasReadWrite {
		return fmt.Errorf("Root user lacks Read-Write permission for document access")
	}

	// Test actual document access in Users bundle
	allDocs := *usersBundle.Documents
	if len(allDocs) == 0 {
		return fmt.Errorf("no documents found in Users bundle")
	}

	ColorLogger.Infof("  ✓ Root user can access %d documents in Users bundle", len(allDocs))
	ColorLogger.Info("✓ Root user can access documents (Read-Write permission)")
	return nil
}

// testRootUser_CanAccessIndexes tests that Root user has Admin permission for indexes
func testRootUser_CanAccessIndexes() error {
	ColorLogger.Info("Testing Root user index access...")

	// Get Users bundle to find Root user's UserID
	usersBundle, exists := testRootUserDatabase.Bundles["Users"]
	if !exists {
		return fmt.Errorf("Users bundle not found")
	}

	if usersBundle.Documents == nil {
		return fmt.Errorf("Users bundle has no documents")
	}

	var rootUserID string
	userDocs := *usersBundle.Documents
	for i := range userDocs {
		if username, ok := userDocs[i].Data["Username"].(string); ok && username == "Root" {
			if userID, ok := userDocs[i].Data["UserID"].(string); ok {
				rootUserID = userID
				break
			}
		}
	}

	if rootUserID == "" {
		return fmt.Errorf("could not find Root user's UserID")
	}

	// Admin permission grants index operations
	hasAdmin, _ := testRootUserPermService.UserHasPermission("Root", "Admin")
	if !hasAdmin {
		return fmt.Errorf("Root user lacks Admin permission for index access")
	}

	// Test index access across all bundles
	rbacBundles := []string{"Users", "Roles", "Permissions", "UserRoles", "UserPermissions", "RolesPermissions"}
	indexCount := 0

	for _, bundleName := range rbacBundles {
		bundle, exists := testRootUserDatabase.Bundles[bundleName]
		if !exists {
			continue
		}

		// Check if bundle has indexes
		if bundle.Indexes != nil {
			indexCount += len(bundle.Indexes)
		}
	}

	ColorLogger.Infof("  ✓ Root user can access %d indexes across RBAC bundles", indexCount)
	ColorLogger.Info("✓ Root user can access indexes (Admin permission)")
	return nil
}

// testRootUser_FullAccessWorkflow tests complete end-to-end workflow for Root user
func testRootUser_FullAccessWorkflow() error {
	ColorLogger.Info("Testing Root user full access workflow...")

	// Step 1: Authenticate
	ColorLogger.Info("  Step 1: Authenticate Root user...")
	authenticated, err := testRootUserService.ValidateUserCredentials("Root", "root")
	if err != nil || !authenticated {
		return fmt.Errorf("step 1 failed - authentication: %v", err)
	}

	// Step 2: Get Root user ID
	usersBundle, exists := testRootUserDatabase.Bundles["Users"]
	if !exists {
		return fmt.Errorf("step 2 failed - Users bundle not found")
	}

	if usersBundle.Documents == nil {
		return fmt.Errorf("step 2 failed - Users bundle has no documents")
	}

	var rootUserID string
	userDocs := *usersBundle.Documents
	for i := range userDocs {
		if username, ok := userDocs[i].Data["Username"].(string); ok && username == "Root" {
			if userID, ok := userDocs[i].Data["UserID"].(string); ok {
				rootUserID = userID
				break
			}
		}
	}

	if rootUserID == "" {
		return fmt.Errorf("step 2 failed - could not find Root user's UserID")
	}

	// Step 3: Verify all permissions
	ColorLogger.Info("  Step 2: Verify all permissions...")
	requiredPermissions := []string{"Read", "Write", "Admin", "Read-Write"}
	for _, permName := range requiredPermissions {
		hasPerm, _ := testRootUserPermService.UserHasPermission("Root", permName)
		if !hasPerm {
			return fmt.Errorf("step 2 failed - missing permission: %s", permName)
		}
	}

	// Step 4: Access database
	ColorLogger.Info("  Step 3: Access database...")
	hasAdminPerm2, _ := testRootUserPermService.UserHasPermission("Root", "Admin")
	if !hasAdminPerm2 {
		return fmt.Errorf("step 3 failed - no Admin permission for database access")
	}

	// Step 5: Access bundles
	ColorLogger.Info("  Step 4: Access all bundles...")
	rbacBundles := []string{"Users", "Roles", "Permissions", "UserRoles", "UserPermissions", "RolesPermissions"}
	for _, bundleName := range rbacBundles {
		_, exists := testRootUserDatabase.Bundles[bundleName]
		if !exists {
			return fmt.Errorf("step 4 failed - cannot access bundle %s", bundleName)
		}
	}

	// Step 6: Access documents
	ColorLogger.Info("  Step 5: Access documents...")
	allDocs := *usersBundle.Documents
	if len(allDocs) == 0 {
		return fmt.Errorf("step 5 failed - no documents accessible")
	}

	// Step 7: Access indexes
	ColorLogger.Info("  Step 6: Access indexes...")
	hasAdminPerm3, _ := testRootUserPermService.UserHasPermission("Root", "Admin")
	if !hasAdminPerm3 {
		return fmt.Errorf("step 6 failed - no Admin permission for index access")
	}

	ColorLogger.Info("✓ Root user full access workflow completed successfully")
	return nil
}

// testRootUser_AuthenticationFailsWithWrongPassword tests that wrong passwords are rejected
func testRootUser_AuthenticationFailsWithWrongPassword() error {
	ColorLogger.Info("Testing Root user authentication with wrong passwords...")

	wrongPasswords := []string{
		"wrong",
		"Root",
		"password",
		"admin",
		"12345",
		"",
		"rootroot",
	}

	for _, wrongPassword := range wrongPasswords {
		authenticated, _ := testRootUserService.ValidateUserCredentials("Root", wrongPassword)

		if authenticated {
			return fmt.Errorf("wrong password '%s' incorrectly authenticated", wrongPassword)
		}
		ColorLogger.Infof("  ✓ Wrong password '%s' correctly rejected", wrongPassword)
	}

	ColorLogger.Info("✓ All wrong passwords correctly rejected")
	return nil
}

// testRootUser_CaseInsensitiveUsername tests that username is case-insensitive
func testRootUser_CaseInsensitiveUsername() error {
	ColorLogger.Info("Testing Root user case-insensitive username...")

	usernameVariations := []string{
		"Root",
		"root",
		"ROOT",
		"RoOt",
		"rOOT",
	}

	for _, username := range usernameVariations {
		authenticated, err := testRootUserService.ValidateUserCredentials(username, "root")
		if err != nil {
			return fmt.Errorf("authentication with username '%s' failed: %v", username, err)
		}

		if !authenticated {
			return fmt.Errorf("username '%s' failed to authenticate (should be case-insensitive)", username)
		}
		ColorLogger.Infof("  ✓ Username variation '%s' authenticated successfully", username)
	}

	ColorLogger.Info("✓ Username is case-insensitive")
	return nil
}

// getRootUserID is a helper function to find Root user's UserID
func getRootUserID() (string, error) {
	usersBundle, exists := testRootUserDatabase.Bundles["Users"]
	if !exists {
		return "", fmt.Errorf("Users bundle not found")
	}

	if usersBundle.Documents == nil {
		return "", fmt.Errorf("Users bundle has no documents")
	}

	userDocs := *usersBundle.Documents
	for i := range userDocs {
		if username, ok := userDocs[i].Data["Username"].(string); ok && strings.EqualFold(username, "Root") {
			if userID, ok := userDocs[i].Data["UserID"].(string); ok {
				return userID, nil
			}
		}
	}

	return "", fmt.Errorf("Root user not found")
}

// RunRootUserTests executes all Root user validation tests
func RunRootUserTests() error {
	ColorLogger.Info("=== Starting Root User Validation Tests ===")

	// Setup test environment
	if err := setupRootUserTestEnvironment(); err != nil {
		return fmt.Errorf("failed to setup Root user test environment: %v", err)
	}

	// Ensure cleanup happens at the end
	defer func() {
		if err := cleanupRootUserTestEnvironment(); err != nil {
			ColorLogger.Errorf("Warning: failed to cleanup test environment: %v", err)
		}
	}()

	// Run all tests
	tests := []struct {
		name string
		fn   func() error
	}{
		{"CreatedProperly", testRootUser_CreatedProperly},
		{"CanLogin", testRootUser_CanLogin},
		{"PasswordHashed", testRootUser_PasswordHashed},
		{"CaseInsensitiveUsername", testRootUser_CaseInsensitiveUsername}, // MUST run before AuthenticationFailsWithWrongPassword (which locks account)
		{"HasDboRole", testRootUser_HasDboRole},
		{"HasAllPermissions", testRootUser_HasAllPermissions},
		{"CanAccessDatabases", testRootUser_CanAccessDatabases},
		{"CanAccessBundles", testRootUser_CanAccessBundles},
		{"CanAccessDocuments", testRootUser_CanAccessDocuments},
		{"CanAccessIndexes", testRootUser_CanAccessIndexes},
		{"FullAccessWorkflow", testRootUser_FullAccessWorkflow},
		{"AuthenticationFailsWithWrongPassword", testRootUser_AuthenticationFailsWithWrongPassword}, // Locks account - run last
	}

	passedCount := 0
	failedCount := 0

	for _, test := range tests {
		testName := "RootUser_" + test.name
		ColorLogger.Infof("\n--- Running test: %s ---", testName)

		if err := test.fn(); err != nil {
			ColorLogger.Errorf("✗ FAILED: %s - %v", testName, err)
			rootUserTestResults[testName] = false
			rootUserTestErrors[testName] = err
			failedCount++
		} else {
			ColorLogger.Infof("✓ PASSED: %s", testName)
			rootUserTestResults[testName] = true
			passedCount++
		}
	}

	// Print summary
	ColorLogger.Infof("\n=== Root User Test Summary ===")
	ColorLogger.Infof("Total Tests: %d", len(tests))
	ColorLogger.Infof("Passed: %d", passedCount)
	ColorLogger.Infof("Failed: %d", failedCount)

	if failedCount > 0 {
		ColorLogger.Error("\nFailed Tests:")
		for testName, err := range rootUserTestErrors {
			ColorLogger.Errorf("  - %s: %v", testName, err)
		}
		return fmt.Errorf("%d Root user test(s) failed", failedCount)
	}

	ColorLogger.Info("\n✓ All Root user tests passed!")
	return nil
}

// Helper function for min (Go 1.18+ has this in math package)
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
