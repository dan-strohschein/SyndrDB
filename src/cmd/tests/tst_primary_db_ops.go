/*
PRIMARY DATABASE INITIALIZATION TESTS

This file implements simple unit tests for primary database initialization
focusing on the InitPrimaryBundleCatalogs function and related operations.
Tests follow the same pattern as other SyndrDB test files with proper
setup, execution, validation, and cleanup phases.
*/

package main

import (
	"fmt"
	"os"
	"path/filepath"

	defaultdb "syndrdb/src/internal/defaultDB"
	"syndrdb/src/internal/domain/bundle"
	"syndrdb/src/internal/domain/database"
	"syndrdb/src/internal/domain/document"
	"syndrdb/src/internal/domain/models"
	"syndrdb/src/internal/storage/buffer"
	"syndrdb/src/internal/storage/bundlestore"
	"syndrdb/src/internal/storage/databasestore"
	"syndrdb/src/pkg/common/helpers"
	"syndrdb/src/pkg/settings"

	"go.uber.org/zap"
)

// Global test state for primary database testing
var (
	testPrimaryDatabase *models.Database
	testBundleService   *bundle.BundleService
	testDatabaseService *database.DatabaseService
	testStorageEngine   *databasestore.DatabaseStorageEngine
	testPrimaryLogger   *zap.SugaredLogger
)

// GetPrimaryDatabaseUseCases returns simple test cases for primary database initialization
func GetPrimaryDatabaseUseCases() []DatabaseCreationUseCase {
	return []DatabaseCreationUseCase{
		// Basic Initialization Tests
		{
			Name:          "InitializePrimaryDatabase",
			Description:   "Create test primary database and call InitPrimaryBundleCatalogs",
			Category:      "Initialization",
			SetupFunc:     setupPrimaryDatabaseTest,
			ExecuteFunc:   testInitPrimaryBundleCatalogs,
			ValidateFunc:  validateSystemBundlesCreated,
			CleanupFunc:   cleanupPrimaryDatabaseTest,
			ExpectSuccess: true,
		},
		{
			Name:          "ValidateSystemBundles",
			Description:   "Verify all expected system bundles are created with correct schemas",
			Category:      "Validation",
			SetupFunc:     setupPrimaryDatabaseTest,
			ExecuteFunc:   testInitPrimaryBundleCatalogs,
			ValidateFunc:  validateBundleSchemas,
			CleanupFunc:   cleanupPrimaryDatabaseTest,
			ExpectSuccess: true,
		},
		{
			Name:          "TestBundleRelationships",
			Description:   "Verify relationships between system bundles are properly established",
			Category:      "Validation",
			SetupFunc:     setupPrimaryDatabaseTest,
			ExecuteFunc:   testInitPrimaryBundleCatalogs,
			ValidateFunc:  validateBundleRelationships,
			CleanupFunc:   cleanupPrimaryDatabaseTest,
			ExpectSuccess: true,
		},
		{
			Name:          "HydratePermissionsCatalog",
			Description:   "Test HydratePermissionPrimaryCatalogs function adds default permissions",
			Category:      "DataHydration",
			SetupFunc:     setupPrimaryDatabaseTest,
			ExecuteFunc:   testHydratePermissionsCatalog,
			ValidateFunc:  validatePermissionsHydration,
			CleanupFunc:   cleanupPrimaryDatabaseTest,
			ExpectSuccess: true,
		},
		{
			Name:          "HydrateRolesCatalog",
			Description:   "Test HydrateRolesPrimaryCatalogs function adds default roles",
			Category:      "DataHydration",
			SetupFunc:     setupPrimaryDatabaseTest,
			ExecuteFunc:   testHydrateRolesCatalog,
			ValidateFunc:  validateRolesHydration,
			CleanupFunc:   cleanupPrimaryDatabaseTest,
			ExpectSuccess: true,
		},
		{
			Name:          "HydrateUsersCatalog",
			Description:   "Test HydrateUserPrimaryCatalogs function adds default users",
			Category:      "DataHydration",
			SetupFunc:     setupPrimaryDatabaseTest,
			ExecuteFunc:   testHydrateUsersCatalog,
			ValidateFunc:  validateUsersHydration,
			CleanupFunc:   cleanupPrimaryDatabaseTest,
			ExpectSuccess: true,
		},
		{
			Name:          "HydrateUserPermissionsCatalog",
			Description:   "Test HydrateUserPermissionsPrimaryCatalogs function links users to permissions",
			Category:      "DataHydration",
			SetupFunc:     setupPrimaryDatabaseTest,
			ExecuteFunc:   testHydrateUserPermissionsCatalog,
			ValidateFunc:  validateUserPermissionsHydration,
			CleanupFunc:   cleanupPrimaryDatabaseTest,
			ExpectSuccess: true,
		},
		{
			Name:          "HydrateDatabaseUsersCatalog",
			Description:   "Test HydrateDatabaseUsersPrimaryCatalogs function links users to primary database",
			Category:      "DataHydration",
			SetupFunc:     setupPrimaryDatabaseTest,
			ExecuteFunc:   testHydrateDatabaseUsersCatalog,
			ValidateFunc:  validateDatabaseUsersHydration,
			CleanupFunc:   cleanupPrimaryDatabaseTest,
			ExpectSuccess: true,
		},
	}
}

// Setup function to prepare test environment
func setupPrimaryDatabaseTest() error {
	helpers.Init()
	testPrimaryLogger = helpers.SetupLogger().Sugar()

	args := settings.GetSettings()

	// Create unique test directory for each test run to avoid conflicts
	testID := helpers.GenerateUUID()[:8]
	testDataDir := filepath.Join("bin", "tests", "data_files", "primary_test_"+testID)

	// Ensure clean directory - remove any existing test directory
	os.RemoveAll(testDataDir)
	err := os.MkdirAll(testDataDir, 0755)
	if err != nil {
		return fmt.Errorf("failed to create test data directory: %w", err)
	}

	args.DataDir = testDataDir
	args.TempDir = "./temp_files"
	args.LogLevel = "warn"

	// Create storage engine
	store, err := databasestore.NewDatabaseStore(args.DataDir, testPrimaryLogger)
	if err != nil {
		return fmt.Errorf("failed to create database store: %w", err)
	}
	testStorageEngine = store

	// Create database service
	factory := database.NewDatabaseFactory().(*database.DatabaseFactoryImpl).WithDefaultDataDirectory(args.DataDir)
	testDatabaseService = database.NewDatabaseService(store, factory, args, testPrimaryLogger)

	// Create buffer pool for bundle store
	fileRegistry, err := buffer.NewFileRegistry(args.DataDir, buffer.SyncAlways, testPrimaryLogger)
	if err != nil {
		return fmt.Errorf("failed to create file registry: %w", err)
	}
	bufferPool := buffer.NewBufferPool(1024, 100, fileRegistry, testPrimaryLogger)

	// Create bundle service
	bundleStore, err := bundlestore.NewBundleStore(args.DataDir, bufferPool, testPrimaryLogger, "json")
	if err != nil {
		return fmt.Errorf("failed to create bundle store: %w", err)
	}
	bundleFactory := bundle.NewBundleFactory()
	documentFactory := document.NewDocumentFactory()
	testBundleService = bundle.NewBundleService(bundleStore, bundleFactory, documentFactory, testPrimaryLogger, args)

	// Create a test primary database with a unique name each time
	testDbName := fmt.Sprintf("test_primary_%s", helpers.GenerateUUID()[:8])
	testPrimaryDatabase = &models.Database{
		DatabaseID:    helpers.GenerateUUID(),
		Name:          testDbName,
		Description:   "Test primary database for initialization testing",
		DataDirectory: testDatabaseService.Settings.DataDir,
		Bundles:       make(map[string]models.Bundle),
		BundleFiles:   []string{},
	}

	// Save the test database
	err = testStorageEngine.CreateDatabaseDataFile(testPrimaryDatabase)
	if err != nil {
		return fmt.Errorf("failed to create test primary database file: %w", err)
	}

	//Add to service
	testDatabaseService.Databases[testDbName] = testPrimaryDatabase
	// Also register under "primary" key for hydration functions
	testDatabaseService.Databases["primary"] = testPrimaryDatabase

	return nil
}

// Test function that calls InitPrimaryBundleCatalogs
func testInitPrimaryBundleCatalogs() error {
	if testPrimaryDatabase == nil {
		return fmt.Errorf("test primary database not initialized")
	}

	// Call the actual InitPrimaryBundleCatalogs function
	err := defaultdb.InitPrimaryBundleCatalogs(
		testDatabaseService,
		testStorageEngine,
		testPrimaryDatabase,
		testPrimaryLogger,
		testBundleService,
	)

	if err != nil {
		return fmt.Errorf("InitPrimaryBundleCatalogs failed: %w", err)
	}

	return nil
}

// Validation function to check system bundles were created
func validateSystemBundlesCreated() error {
	expectedBundles := []string{
		"Databases", "Users", "Permissions", "UserPermissions",
		"DatabaseUsers", "Roles", "UserRoles",
	}

	for _, bundleName := range expectedBundles {
		bundle, err := testBundleService.GetBundleByName(testPrimaryDatabase, bundleName)
		if err != nil {
			return fmt.Errorf("system bundle '%s' not found: %w", bundleName, err)
		}
		if bundle == nil {
			return fmt.Errorf("system bundle '%s' is nil", bundleName)
		}
	}

	return nil
}

// Validation function to check bundle schemas
func validateBundleSchemas() error {
	// Test Databases bundle schema
	dbBundle, err := testBundleService.GetBundleByName(testPrimaryDatabase, "Databases")
	if err != nil {
		return fmt.Errorf("failed to get Databases bundle: %w", err)
	}

	expectedFields := []string{"DocumentID", "DatabaseID", "Name", "FilePath"}
	for _, field := range expectedFields {
		if _, exists := dbBundle.DocumentStructure.FieldDefinitions[field]; !exists {
			return fmt.Errorf("databases bundle missing field: %s", field)
		}
	}

	// Test Users bundle schema
	usersBundle, err := testBundleService.GetBundleByName(testPrimaryDatabase, "Users")
	if err != nil {
		return fmt.Errorf("failed to get Users bundle: %w", err)
	}

	expectedUserFields := []string{"DocumentID", "UserID", "PasswordHash", "Name", "IsActive"}
	for _, field := range expectedUserFields {
		if _, exists := usersBundle.DocumentStructure.FieldDefinitions[field]; !exists {
			return fmt.Errorf("users bundle missing field: %s", field)
		}
	}

	return nil
}

// Validation function to check bundle relationships
func validateBundleRelationships() error {
	// Check if Users bundle has relationships
	usersBundle, err := testBundleService.GetBundleByName(testPrimaryDatabase, "Users")
	if err != nil {
		return fmt.Errorf("failed to get Users bundle: %w", err)
	}

	if len(usersBundle.Relationships) == 0 {
		return fmt.Errorf("users bundle has no relationships defined")
	}

	return nil
}

// Cleanup function
func cleanupPrimaryDatabaseTest() error {
	// Remove test database from service if it exists
	if testPrimaryDatabase != nil && testDatabaseService != nil {
		delete(testDatabaseService.Databases, testPrimaryDatabase.Name)

		// Remove all bundle files for this test database
		if testBundleService != nil {
			for bundleName := range testPrimaryDatabase.Bundles {
				bundleFilePath := filepath.Join(testDatabaseService.Settings.DataDir,
					bundleName+".bnd")
				os.Remove(bundleFilePath) // Remove bundle file
			}
		}

		// Remove the entire test database directory
		testDbDir := testDatabaseService.Settings.DataDir
		os.RemoveAll(testDbDir)
	}

	// Reset test state
	testPrimaryDatabase = nil
	testBundleService = nil
	testDatabaseService = nil
	testStorageEngine = nil
	return nil
}

// Test function that calls HydratePermissionPrimaryCatalogs
func testHydratePermissionsCatalog() error {
	if testPrimaryDatabase == nil {
		return fmt.Errorf("test primary database not initialized")
	}

	// First, call InitPrimaryBundleCatalogs to create the bundles
	err := defaultdb.InitPrimaryBundleCatalogs(
		testDatabaseService,
		testStorageEngine,
		testPrimaryDatabase,
		testPrimaryLogger,
		testBundleService,
	)
	if err != nil {
		return fmt.Errorf("InitPrimaryBundleCatalogs failed: %w", err)
	}

	// Now call HydratePermissionPrimaryCatalogs to add default permissions
	err = defaultdb.HydratePermissionPrimaryCatalogs(
		testDatabaseService,
		testStorageEngine,
		testPrimaryLogger,
		testBundleService,
	)
	if err != nil {
		return fmt.Errorf("HydratePermissionPrimaryCatalogs failed: %w", err)
	}

	return nil
}

// Validation function to check that permissions were properly hydrated
func validatePermissionsHydration() error {
	// Get the Permissions bundle
	permissionsBundle, err := testBundleService.GetBundleByName(testPrimaryDatabase, "Permissions")
	if err != nil {
		return fmt.Errorf("failed to get Permissions bundle: %w", err)
	}

	// Check that the bundle has documents
	if permissionsBundle.Documents == nil || len(*permissionsBundle.Documents) == 0 {
		return fmt.Errorf("permissions bundle has no documents")
	}

	// Expected permissions that should be added
	expectedPermissions := []string{"Read", "Write", "Admin", "Read-Write"}
	foundPermissions := make(map[string]bool)

	// Check each document in the permissions bundle
	for _, doc := range *permissionsBundle.Documents {
		if nameField, exists := doc.Fields["Name"]; exists {
			if nameValue, ok := nameField.Value.(string); ok {
				foundPermissions[nameValue] = true
			}
		}
	}

	// Verify all expected permissions were found
	for _, expectedPerm := range expectedPermissions {
		if !foundPermissions[expectedPerm] {
			return fmt.Errorf("expected permission '%s' not found in permissions bundle", expectedPerm)
		}
	}

	// Verify the count matches
	if len(foundPermissions) != len(expectedPermissions) {
		return fmt.Errorf("expected %d permissions, found %d", len(expectedPermissions), len(foundPermissions))
	}

	// Verify each permission document has the required fields
	for _, doc := range *permissionsBundle.Documents {
		// Check PermissionID field
		if permIdField, exists := doc.Fields["PermissionID"]; !exists {
			return fmt.Errorf("permission document missing PermissionID field")
		} else if permIdValue, ok := permIdField.Value.(string); !ok || permIdValue == "" {
			return fmt.Errorf("permission document has invalid PermissionID value")
		}

		// Check Name field
		if nameField, exists := doc.Fields["Name"]; !exists {
			return fmt.Errorf("permission document missing Name field")
		} else if nameValue, ok := nameField.Value.(string); !ok || nameValue == "" {
			return fmt.Errorf("permission document has invalid Name value")
		}

		// Check DocumentID exists
		if doc.DocumentID == "" {
			return fmt.Errorf("permission document missing DocumentID")
		}
	}

	return nil
}

// Test function that calls HydrateRolesPrimaryCatalogs
func testHydrateRolesCatalog() error {
	if testPrimaryDatabase == nil {
		return fmt.Errorf("test primary database not initialized")
	}

	// First, call InitPrimaryBundleCatalogs to create the bundles
	err := defaultdb.InitPrimaryBundleCatalogs(
		testDatabaseService,
		testStorageEngine,
		testPrimaryDatabase,
		testPrimaryLogger,
		testBundleService,
	)
	if err != nil {
		return fmt.Errorf("InitPrimaryBundleCatalogs failed: %w", err)
	}

	// Now call HydrateRolesPrimaryCatalogs to add default roles
	err = defaultdb.HydrateRolesPrimaryCatalogs(
		testDatabaseService,
		testStorageEngine,
		testPrimaryLogger,
		testBundleService,
	)
	if err != nil {
		return fmt.Errorf("HydrateRolesPrimaryCatalogs failed: %w", err)
	}

	return nil
}

// Validation function to check that roles were properly hydrated
func validateRolesHydration() error {
	// Get the Roles bundle
	rolesBundle, err := testBundleService.GetBundleByName(testPrimaryDatabase, "Roles")
	if err != nil {
		return fmt.Errorf("failed to get Roles bundle: %w", err)
	}

	// Check that the bundle has documents
	if rolesBundle.Documents == nil || len(*rolesBundle.Documents) == 0 {
		return fmt.Errorf("roles bundle has no documents")
	}

	// Expected roles that should be added
	expectedRoles := []string{"Dbo", "Data-Reader", "Data-Writer"}
	foundRoles := make(map[string]bool)

	// Check each document in the roles bundle
	for _, doc := range *rolesBundle.Documents {
		if nameField, exists := doc.Fields["Name"]; exists {
			if nameValue, ok := nameField.Value.(string); ok {
				foundRoles[nameValue] = true
			}
		}
	}

	// Verify all expected roles were found
	for _, expectedRole := range expectedRoles {
		if !foundRoles[expectedRole] {
			return fmt.Errorf("expected role '%s' not found in roles bundle", expectedRole)
		}
	}

	// Verify the count matches
	if len(foundRoles) != len(expectedRoles) {
		return fmt.Errorf("expected %d roles, found %d", len(expectedRoles), len(foundRoles))
	}

	// Verify each role document has the required fields
	for _, doc := range *rolesBundle.Documents {
		// Check RoleID field
		if roleIdField, exists := doc.Fields["RoleID"]; !exists {
			return fmt.Errorf("role document missing RoleID field")
		} else if roleIdValue, ok := roleIdField.Value.(string); !ok || roleIdValue == "" {
			return fmt.Errorf("role document has invalid RoleID value")
		}

		// Check Name field
		if nameField, exists := doc.Fields["Name"]; !exists {
			return fmt.Errorf("role document missing Name field")
		} else if nameValue, ok := nameField.Value.(string); !ok || nameValue == "" {
			return fmt.Errorf("role document has invalid Name value")
		}

		// Check DocumentID exists
		if doc.DocumentID == "" {
			return fmt.Errorf("role document missing DocumentID")
		}
	}

	return nil
}

// Test function to hydrate users catalog
func testHydrateUsersCatalog() error {
	// First ensure primary bundle catalogs are initialized
	err := defaultdb.InitPrimaryBundleCatalogs(testDatabaseService, testStorageEngine, testPrimaryDatabase, testPrimaryLogger, testBundleService)
	if err != nil {
		return fmt.Errorf("failed to initialize primary bundle catalogs: %v", err)
	}

	// Now hydrate the users catalog
	err = defaultdb.HydrateUserPrimaryCatalogs(testDatabaseService, testStorageEngine, testPrimaryLogger, testBundleService)
	if err != nil {
		return fmt.Errorf("failed to hydrate users catalog: %v", err)
	}

	return nil
}

// Validation function for users hydration
func validateUsersHydration() error {
	// Get the Users bundle
	usersBundle, err := testBundleService.GetBundleByName(testPrimaryDatabase, "Users")
	if err != nil {
		return fmt.Errorf("failed to get Users bundle: %v", err)
	}

	// Check that we have the expected number of user documents
	expectedUsers := []string{"Admin", "Reader", "Writer"}
	if len(*usersBundle.Documents) != len(expectedUsers) {
		return fmt.Errorf("expected %d user documents, got %d", len(expectedUsers), len(*usersBundle.Documents))
	}

	// Verify each expected user exists
	foundUsers := make(map[string]bool)
	for _, doc := range *usersBundle.Documents {
		if nameField, exists := doc.Fields["Name"]; exists {
			if userName, ok := nameField.Value.(string); ok {
				foundUsers[userName] = true
			}
		}
	}

	for _, expectedUser := range expectedUsers {
		if !foundUsers[expectedUser] {
			return fmt.Errorf("expected user '%s' not found in Users bundle", expectedUser)
		}
	}

	// Verify each user document has the required fields
	for _, doc := range *usersBundle.Documents {
		// Check UserID field
		if userIdField, exists := doc.Fields["UserID"]; !exists {
			return fmt.Errorf("user document missing UserID field")
		} else if userIdValue, ok := userIdField.Value.(string); !ok || userIdValue == "" {
			return fmt.Errorf("user document has invalid UserID value")
		}

		// Check Name field
		if nameField, exists := doc.Fields["Name"]; !exists {
			return fmt.Errorf("user document missing Name field")
		} else if nameValue, ok := nameField.Value.(string); !ok || nameValue == "" {
			return fmt.Errorf("user document has invalid Name value")
		}

		// Check DocumentID exists
		if doc.DocumentID == "" {
			return fmt.Errorf("user document missing DocumentID")
		}
	}

	return nil
}

// Test function to hydrate user permissions catalog
func testHydrateUserPermissionsCatalog() error {
	// First ensure primary bundle catalogs are initialized
	err := defaultdb.InitPrimaryBundleCatalogs(testDatabaseService, testStorageEngine, testPrimaryDatabase, testPrimaryLogger, testBundleService)
	if err != nil {
		return fmt.Errorf("failed to initialize primary bundle catalogs: %v", err)
	}

	// Hydrate permissions (prerequisite)
	err = defaultdb.HydratePermissionPrimaryCatalogs(testDatabaseService, testStorageEngine, testPrimaryLogger, testBundleService)
	if err != nil {
		return fmt.Errorf("failed to hydrate permissions catalog: %v", err)
	}

	// Hydrate users (prerequisite)
	err = defaultdb.HydrateUserPrimaryCatalogs(testDatabaseService, testStorageEngine, testPrimaryLogger, testBundleService)
	if err != nil {
		return fmt.Errorf("failed to hydrate users catalog: %v", err)
	}

	// Now hydrate the user permissions catalog
	err = defaultdb.HydrateUserPermissionsPrimaryCatalogs(testDatabaseService, testStorageEngine, testPrimaryLogger, testBundleService)
	if err != nil {
		return fmt.Errorf("failed to hydrate user permissions catalog: %v", err)
	}

	return nil
}

// Validation function for user permissions hydration
func validateUserPermissionsHydration() error {
	// Get the UserPermissions bundle
	userPermissionsBundle, err := testBundleService.GetBundleByName(testPrimaryDatabase, "UserPermissions")
	if err != nil {
		return fmt.Errorf("failed to get UserPermissions bundle: %v", err)
	}

	// Expected relationships:
	// Admin -> Read, Write, Admin, Read-Write (4 links)
	// Reader -> Read (1 link)
	// Writer -> Write (1 link)
	// Total: 6 UserPermission documents
	expectedLinkCount := 6
	if len(*userPermissionsBundle.Documents) != expectedLinkCount {
		return fmt.Errorf("expected %d user permission documents, got %d", expectedLinkCount, len(*userPermissionsBundle.Documents))
	}

	// Get the Users and Permissions bundles for validation
	usersBundle, err := testBundleService.GetBundleByName(testPrimaryDatabase, "Users")
	if err != nil {
		return fmt.Errorf("failed to get Users bundle: %v", err)
	}

	permissionsBundle, err := testBundleService.GetBundleByName(testPrimaryDatabase, "Permissions")
	if err != nil {
		return fmt.Errorf("failed to get Permissions bundle: %v", err)
	}

	// Create maps for lookup
	usersByID := make(map[string]string)
	for _, userDoc := range *usersBundle.Documents {
		userID := userDoc.Fields["UserID"].Value.(string)
		userName := userDoc.Fields["Name"].Value.(string)
		usersByID[userID] = userName
	}

	permissionsByID := make(map[string]string)
	for _, permDoc := range *permissionsBundle.Documents {
		permID := permDoc.Fields["PermissionID"].Value.(string)
		permName := permDoc.Fields["Name"].Value.(string)
		permissionsByID[permID] = permName
	}

	// Count permissions by user
	userPermissionCounts := make(map[string]int)
	userPermissions := make(map[string][]string)

	// Verify each user permission document has the required fields
	for _, doc := range *userPermissionsBundle.Documents {
		// Check UserPermissionID field
		if userPermIdField, exists := doc.Fields["UserPermissionID"]; !exists {
			return fmt.Errorf("user permission document missing UserPermissionID field")
		} else if userPermIdValue, ok := userPermIdField.Value.(string); !ok || userPermIdValue == "" {
			return fmt.Errorf("user permission document has invalid UserPermissionID value")
		}

		// Check UserID field
		if userIdField, exists := doc.Fields["UserID"]; !exists {
			return fmt.Errorf("user permission document missing UserID field")
		} else if userIdValue, ok := userIdField.Value.(string); !ok || userIdValue == "" {
			return fmt.Errorf("user permission document has invalid UserID value")
		} else {
			userName := usersByID[userIdValue]
			userPermissionCounts[userName]++

			// Check PermissionID field
			if permIdField, exists := doc.Fields["PermissionID"]; !exists {
				return fmt.Errorf("user permission document missing PermissionID field")
			} else if permIdValue, ok := permIdField.Value.(string); !ok || permIdValue == "" {
				return fmt.Errorf("user permission document has invalid PermissionID value")
			} else {
				permissionName := permissionsByID[permIdValue]
				userPermissions[userName] = append(userPermissions[userName], permissionName)
			}
		}

		// Check DocumentID exists
		if doc.DocumentID == "" {
			return fmt.Errorf("user permission document missing DocumentID")
		}
	}

	// Verify expected permission counts
	expectedCounts := map[string]int{
		"Admin":  4, // Read, Write, Admin, Read-Write
		"Reader": 1, // Read
		"Writer": 1, // Write
	}

	for user, expectedCount := range expectedCounts {
		if actualCount, exists := userPermissionCounts[user]; !exists {
			return fmt.Errorf("no permissions found for user '%s'", user)
		} else if actualCount != expectedCount {
			return fmt.Errorf("user '%s' expected %d permissions, got %d", user, expectedCount, actualCount)
		}
	}

	// Verify specific permission assignments
	expectedPermissions := map[string][]string{
		"Admin":  {"Read", "Write", "Admin", "Read-Write"},
		"Reader": {"Read"},
		"Writer": {"Write"},
	}

	for user, expectedPerms := range expectedPermissions {
		actualPerms := userPermissions[user]
		if len(actualPerms) != len(expectedPerms) {
			return fmt.Errorf("user '%s' expected %d permissions, got %d", user, len(expectedPerms), len(actualPerms))
		}

		// Convert to maps for easier comparison
		expectedPermMap := make(map[string]bool)
		for _, perm := range expectedPerms {
			expectedPermMap[perm] = true
		}

		for _, actualPerm := range actualPerms {
			if !expectedPermMap[actualPerm] {
				return fmt.Errorf("user '%s' has unexpected permission '%s'", user, actualPerm)
			}
		}
	}

	return nil
}

// Test function to hydrate database users catalog
func testHydrateDatabaseUsersCatalog() error {
	// First ensure primary bundle catalogs are initialized
	err := defaultdb.InitPrimaryBundleCatalogs(testDatabaseService, testStorageEngine, testPrimaryDatabase, testPrimaryLogger, testBundleService)
	if err != nil {
		return fmt.Errorf("failed to initialize primary bundle catalogs: %v", err)
	}

	// Hydrate users (prerequisite)
	err = defaultdb.HydrateUserPrimaryCatalogs(testDatabaseService, testStorageEngine, testPrimaryLogger, testBundleService)
	if err != nil {
		return fmt.Errorf("failed to hydrate users catalog: %v", err)
	}

	// Now hydrate the database users catalog
	err = defaultdb.HydrateDatabaseUsersPrimaryCatalogs(testDatabaseService, testStorageEngine, testPrimaryLogger, testBundleService)
	if err != nil {
		return fmt.Errorf("failed to hydrate database users catalog: %v", err)
	}

	return nil
}

// Validation function for database users hydration
func validateDatabaseUsersHydration() error {
	// Get the DatabaseUsers bundle
	databaseUsersBundle, err := testBundleService.GetBundleByName(testPrimaryDatabase, "DatabaseUsers")
	if err != nil {
		return fmt.Errorf("failed to get DatabaseUsers bundle: %v", err)
	}

	// Expected: All 3 users (Admin, Reader, Writer) linked to primary database
	// Total: 3 DatabaseUser documents
	expectedLinkCount := 3
	if len(*databaseUsersBundle.Documents) != expectedLinkCount {
		return fmt.Errorf("expected %d database user documents, got %d", expectedLinkCount, len(*databaseUsersBundle.Documents))
	}

	// Get the Users bundle for validation
	usersBundle, err := testBundleService.GetBundleByName(testPrimaryDatabase, "Users")
	if err != nil {
		return fmt.Errorf("failed to get Users bundle: %v", err)
	}

	// Create map for user lookup
	usersByID := make(map[string]string)
	for _, userDoc := range *usersBundle.Documents {
		userID := userDoc.Fields["UserID"].Value.(string)
		userName := userDoc.Fields["Name"].Value.(string)
		usersByID[userID] = userName
	}

	// Get the primary database ID for validation
	primaryDBID := testPrimaryDatabase.DatabaseID

	// Count database access by user
	userDatabaseCounts := make(map[string]int)
	linkedUsers := make(map[string]bool)

	// Verify each database user document has the required fields
	for _, doc := range *databaseUsersBundle.Documents {
		// Check DatabaseUserID field (note: this field name might be different, let me check)
		// Looking at the code, it should be "DatabaseUserID"
		if dbUserIdField, exists := doc.Fields["DatabaseUserID"]; !exists {
			return fmt.Errorf("database user document missing DatabaseUserID field")
		} else if dbUserIdValue, ok := dbUserIdField.Value.(string); !ok || dbUserIdValue == "" {
			return fmt.Errorf("database user document has invalid DatabaseUserID value")
		}

		// Check UserID field
		if userIdField, exists := doc.Fields["UserID"]; !exists {
			return fmt.Errorf("database user document missing UserID field")
		} else if userIdValue, ok := userIdField.Value.(string); !ok || userIdValue == "" {
			return fmt.Errorf("database user document has invalid UserID value")
		} else {
			userName := usersByID[userIdValue]
			userDatabaseCounts[userName]++
			linkedUsers[userName] = true
		}

		// Check DatabaseID field
		if dbIdField, exists := doc.Fields["DatabaseID"]; !exists {
			return fmt.Errorf("database user document missing DatabaseID field")
		} else if dbIdValue, ok := dbIdField.Value.(string); !ok || dbIdValue == "" {
			return fmt.Errorf("database user document has invalid DatabaseID value")
		} else if dbIdValue != primaryDBID {
			return fmt.Errorf("database user document has incorrect DatabaseID, expected %s, got %s", primaryDBID, dbIdValue)
		}

		// Check DocumentID exists
		if doc.DocumentID == "" {
			return fmt.Errorf("database user document missing DocumentID")
		}
	}

	// Verify all expected users are linked to the database
	expectedUsers := []string{"Admin", "Reader", "Writer"}
	for _, expectedUser := range expectedUsers {
		if !linkedUsers[expectedUser] {
			return fmt.Errorf("expected user '%s' not linked to primary database", expectedUser)
		}
		if userDatabaseCounts[expectedUser] != 1 {
			return fmt.Errorf("user '%s' expected 1 database link, got %d", expectedUser, userDatabaseCounts[expectedUser])
		}
	}

	// Verify no unexpected users are linked
	if len(linkedUsers) != len(expectedUsers) {
		return fmt.Errorf("expected %d users linked to database, got %d", len(expectedUsers), len(linkedUsers))
	}

	return nil
}
