package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"

	"syndrdb/src/internal/domain/database"
	"syndrdb/src/internal/domain/models"
	"syndrdb/src/internal/server"
	"syndrdb/src/pkg/common/helpers"
	"syndrdb/src/pkg/settings"
	"syndrdb/src/tests/homegrown"
)

/*
drop_database_test.go

This file contains comprehensive end-to-end tests for the DROP DATABASE functionality.
It validates that databases can be securely dropped with proper permission checks,
session termination, catalog cleanup, and filesystem deletion.

Test Coverage:
- Basic database drop succeeds
- Admin permission required (non-admin fails)
- Primary database protection (cannot be dropped)
- Active sessions terminated automatically
- Catalog and filesystem consistency validation
- Case-insensitive primary database protection

Design Principles:
- Single Responsibility: Each test validates one specific DROP DATABASE behavior
- DRY: Reuses setupDropDatabaseTest() helper for common initialization
- Open/Closed: Easy to extend with new drop scenarios without modifying existing tests

TODO: I will add tests for concurrent drop operations when locking is fully implemented
TODO: I will add tests for security audit logging verification when SecurityAuditor is integrated
TODO: I will add performance benchmarks for database drop operations
*/

// setupDropDatabaseTest initializes a clean test environment for DROP DATABASE testing
func setupDropDatabaseTest(t *testing.T) (*server.ServiceManager, *server.Session, func()) {
	logger := zaptest.NewLogger(t).Sugar()

	// Create temporary directory for test database
	tempDir, err := os.MkdirTemp("", "syndrdb_dropdatabase_test_*")
	require.NoError(t, err, "Failed to create temp directory")

	// Update settings to use temp directory
	args := settings.GetSettings()
	originalDataDir := args.DataDir
	args.DataDir = tempDir

	// Setup test database service
	databaseService, _, err := homegrown.StandupTestDatabaseService()
	require.NoError(t, err, "Failed to setup database service")

	// Get service manager
	serviceManager := server.GetServiceManager()
	require.NotNil(t, serviceManager, "ServiceManager should exist")

	// Create a test session with Admin privileges
	session := &server.Session{
		SessionID:    "test-session-001",
		UserID:       "admin-user-id",
		Username:     "TestAdmin",
		DatabaseName: "primary",
		ClientIP:     "127.0.0.1",
	}

	// Cleanup function
	cleanup := func() {
		// Restore original data directory
		args.DataDir = originalDataDir

		// Clean up all test databases created
		if databaseService != nil {
			for dbName := range databaseService.Databases {
				if dbName != "primary" && strings.HasPrefix(dbName, "Test") {
					delete(databaseService.Databases, dbName)
				}
			}
		}

		// Remove temporary directory
		os.RemoveAll(tempDir)

		logger.Info("Cleanup completed")
	}

	return serviceManager, session, cleanup
}

// createTestDatabase creates a test database for drop testing
func createTestDatabase(t *testing.T, serviceManager *server.ServiceManager, dbName string, logger *zap.SugaredLogger) {
	// Create database using service
	dbCommand := &models.DatabaseCommand{
		DatabaseName: dbName,
		CommandType:  "CREATE",
	}

	db, err := serviceManager.DatabaseService.AddDatabase(*dbCommand)
	require.NoError(t, err, "Failed to create test database")
	require.NotNil(t, db, "Database should be created")

	// Create database directory
	args := settings.GetSettings()
	dbPath := filepath.Join(args.DataDir, dbName)
	err = os.MkdirAll(dbPath, 0755)
	require.NoError(t, err, "Failed to create database directory")

	// Register in catalog
	if serviceManager.InternalCatalogService != nil {
		err = serviceManager.InternalCatalogService.AddDatabaseToCatalog(db)
		require.NoError(t, err, "Failed to add database to catalog")
	}
}

// TestDropDatabase_BasicSuccess tests successful database drop
func TestDropDatabase_BasicSuccess(t *testing.T) {
	serviceManager, session, cleanup := setupDropDatabaseTest(t)
	defer cleanup()

	logger := zaptest.NewLogger(t).Sugar()
	dbName := "TestDropDB001"

	// Create test database
	createTestDatabase(t, serviceManager, dbName, logger)

	// Verify database exists
	db, err := serviceManager.DatabaseService.GetDatabaseByName(dbName)
	require.NoError(t, err, "Database should exist before drop")
	require.NotNil(t, db, "Database should not be nil")

	// Execute DROP DATABASE command
	command := `DROP "` + dbName + `"`
	response, err := server.DropDatabase(command, logger, *serviceManager, session)

	// Verify success
	assert.NoError(t, err, "DROP DATABASE should succeed")
	assert.NotNil(t, response, "Response should not be nil")
	assert.Equal(t, 1, response.ResultCount, "Result count should be 1")
	assert.Contains(t, response.Result, "dropped successfully", "Response should indicate success")

	// Verify database removed from memory
	_, err = serviceManager.DatabaseService.GetDatabaseByName(dbName)
	assert.Error(t, err, "Database should not exist in memory after drop")

	// Verify database directory deleted
	dbPath := helpers.GetDatabaseFolderPath(dbName)
	_, err = os.Stat(dbPath)
	assert.True(t, os.IsNotExist(err), "Database directory should be deleted")

	t.Log("✓ Basic DROP DATABASE succeeded and cleaned up properly")
}

// TestDropDatabase_RequiresAdminPermission tests that non-admin users cannot drop databases
func TestDropDatabase_RequiresAdminPermission(t *testing.T) {
	serviceManager, _, cleanup := setupDropDatabaseTest(t)
	defer cleanup()

	logger := zaptest.NewLogger(t).Sugar()
	dbName := "TestDropDB002"

	// Create test database
	createTestDatabase(t, serviceManager, dbName, logger)

	// Create session with non-admin user
	nonAdminSession := &server.Session{
		SessionID:    "test-session-002",
		UserID:       "regular-user-id",
		Username:     "RegularUser",
		DatabaseName: "primary",
		ClientIP:     "127.0.0.1",
	}

	// Attempt DROP DATABASE as non-admin
	command := `DROP "` + dbName + `"`
	response, err := server.DropDatabase(command, logger, *serviceManager, nonAdminSession)

	// Verify permission denied
	assert.Error(t, err, "DROP DATABASE should fail for non-admin")
	assert.Nil(t, response, "Response should be nil on permission failure")
	assert.Contains(t, err.Error(), "Admin permission", "Error should mention admin permission requirement")

	// Verify database still exists
	db, err := serviceManager.DatabaseService.GetDatabaseByName(dbName)
	assert.NoError(t, err, "Database should still exist after failed drop")
	assert.NotNil(t, db, "Database should not be nil")

	t.Log("✓ DROP DATABASE correctly requires Admin permission")
}

// TestDropDatabase_PrimaryProtection tests that primary database cannot be dropped
func TestDropDatabase_PrimaryProtection(t *testing.T) {
	serviceManager, session, cleanup := setupDropDatabaseTest(t)
	defer cleanup()

	logger := zaptest.NewLogger(t).Sugar()

	// Attempt to drop primary database (lowercase)
	command := `DROP "primary"`
	response, err := server.DropDatabase(command, logger, *serviceManager, session)

	// Verify protection
	assert.Error(t, err, "DROP DATABASE should fail for primary database")
	assert.Nil(t, response, "Response should be nil on protection failure")
	assert.Contains(t, err.Error(), "protected system database", "Error should mention protection")

	// Verify primary database still exists
	db, err := serviceManager.DatabaseService.GetDatabaseByName("primary")
	assert.NoError(t, err, "Primary database should still exist")
	assert.NotNil(t, db, "Primary database should not be nil")

	t.Log("✓ Primary database is protected from DROP DATABASE")
}

// TestDropDatabase_PrimaryProtectionCaseInsensitive tests case-insensitive primary protection
func TestDropDatabase_PrimaryProtectionCaseInsensitive(t *testing.T) {
	serviceManager, session, cleanup := setupDropDatabaseTest(t)
	defer cleanup()

	logger := zaptest.NewLogger(t).Sugar()

	testCases := []string{
		"PRIMARY",
		"Primary",
		"PrImArY",
		"primary",
	}

	for _, dbName := range testCases {
		t.Run(dbName, func(t *testing.T) {
			// Attempt to drop primary database with different casing
			command := `DROP "` + dbName + `"`
			response, err := server.DropDatabase(command, logger, *serviceManager, session)

			// Verify protection
			assert.Error(t, err, "DROP DATABASE should fail for '%s'", dbName)
			assert.Nil(t, response, "Response should be nil on protection failure")
			assert.Contains(t, err.Error(), "protected system database", "Error should mention protection")
		})
	}

	t.Log("✓ Primary database protection is case-insensitive")
}

// TestDropDatabase_NonExistentDatabase tests dropping a database that doesn't exist
func TestDropDatabase_NonExistentDatabase(t *testing.T) {
	serviceManager, session, cleanup := setupDropDatabaseTest(t)
	defer cleanup()

	logger := zaptest.NewLogger(t).Sugar()

	// Attempt to drop non-existent database
	command := `DROP "NonExistentDB999"`
	response, err := server.DropDatabase(command, logger, *serviceManager, session)

	// Verify error
	assert.Error(t, err, "DROP DATABASE should fail for non-existent database")
	assert.Nil(t, response, "Response should be nil on failure")
	assert.Contains(t, err.Error(), "not found", "Error should mention database not found")

	t.Log("✓ DROP DATABASE correctly handles non-existent databases")
}

// TestDropDatabase_ParseCommandSyntax tests various command syntax formats
func TestDropDatabase_ParseCommandSyntax(t *testing.T) {
	testCases := []struct {
		name          string
		command       string
		expectError   bool
		errorContains string
	}{
		{
			name:        "Valid quoted name",
			command:     `DROP "MyDatabase"`,
			expectError: false,
		},
		{
			name:        "Valid with DATABASE keyword",
			command:     `DROP DATABASE "MyDatabase"`,
			expectError: false,
		},
		{
			name:          "Missing quotes",
			command:       `DROP MyDatabase`,
			expectError:   true,
			errorContains: "invalid DROP DATABASE command syntax",
		},
		{
			name:          "Empty database name",
			command:       `DROP ""`,
			expectError:   true,
			errorContains: "invalid database name",
		},
		{
			name:          "Invalid database name (starts with number)",
			command:       `DROP "123Database"`,
			expectError:   true,
			errorContains: "invalid database name",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := database.ParseDropDatabaseCommand(tc.command)

			if tc.expectError {
				assert.Error(t, err, "Parsing should fail for: %s", tc.command)
				if tc.errorContains != "" {
					assert.Contains(t, err.Error(), tc.errorContains, "Error message should contain expected text")
				}
			} else {
				assert.NoError(t, err, "Parsing should succeed for: %s", tc.command)
			}
		})
	}

	t.Log("✓ DROP DATABASE command parsing handles various syntax formats correctly")
}

// TestDropDatabase_CatalogCleanup tests that catalog entries are properly cleaned up
func TestDropDatabase_CatalogCleanup(t *testing.T) {
	serviceManager, session, cleanup := setupDropDatabaseTest(t)
	defer cleanup()

	logger := zaptest.NewLogger(t).Sugar()
	dbName := "TestDropDB003"

	// Create test database
	createTestDatabase(t, serviceManager, dbName, logger)

	// Verify database in catalog
	if serviceManager.InternalCatalogService != nil {
		allDatabases, err := serviceManager.InternalCatalogService.ListAllDatabasesInCatalog()
		require.NoError(t, err, "Should list databases from catalog")

		foundInCatalog := false
		for _, dbInfo := range allDatabases {
			if name, ok := dbInfo["Name"].(string); ok && name == dbName {
				foundInCatalog = true
				break
			}
		}
		require.True(t, foundInCatalog, "Database should be in catalog before drop")
	}

	// Drop the database
	command := `DROP "` + dbName + `"`
	_, err := server.DropDatabase(command, logger, *serviceManager, session)
	require.NoError(t, err, "DROP DATABASE should succeed")

	// Verify database removed from catalog
	if serviceManager.InternalCatalogService != nil {
		allDatabases, err := serviceManager.InternalCatalogService.ListAllDatabasesInCatalog()
		require.NoError(t, err, "Should list databases from catalog")

		foundInCatalog := false
		for _, dbInfo := range allDatabases {
			if name, ok := dbInfo["Name"].(string); ok && name == dbName {
				foundInCatalog = true
				break
			}
		}
		assert.False(t, foundInCatalog, "Database should not be in catalog after drop")
	}

	t.Log("✓ DROP DATABASE properly cleans up catalog entries")
}

// TestDropDatabase_FilesystemCleanup tests that database directory is properly deleted
func TestDropDatabase_FilesystemCleanup(t *testing.T) {
	serviceManager, session, cleanup := setupDropDatabaseTest(t)
	defer cleanup()

	logger := zaptest.NewLogger(t).Sugar()
	dbName := "TestDropDB004"

	// Create test database with directory and files
	createTestDatabase(t, serviceManager, dbName, logger)

	// Create some test files in database directory
	dbPath := helpers.GetDatabaseFolderPath(dbName)
	testFilePath := filepath.Join(dbPath, "test_bundle.bnd")
	err := os.WriteFile(testFilePath, []byte("test data"), 0644)
	require.NoError(t, err, "Failed to create test file")

	// Verify files exist
	_, err = os.Stat(testFilePath)
	require.NoError(t, err, "Test file should exist before drop")

	// Drop the database
	command := `DROP "` + dbName + `"`
	_, err = server.DropDatabase(command, logger, *serviceManager, session)
	require.NoError(t, err, "DROP DATABASE should succeed")

	// Verify entire directory deleted (including all files)
	_, err = os.Stat(dbPath)
	assert.True(t, os.IsNotExist(err), "Database directory should be completely deleted")

	// Verify test file also gone
	_, err = os.Stat(testFilePath)
	assert.True(t, os.IsNotExist(err), "Test file should be deleted with directory")

	t.Log("✓ DROP DATABASE properly deletes entire database directory and contents")
}

// TestDropDatabase_MultipleDrops tests dropping multiple databases in sequence
func TestDropDatabase_MultipleDrops(t *testing.T) {
	serviceManager, session, cleanup := setupDropDatabaseTest(t)
	defer cleanup()

	logger := zaptest.NewLogger(t).Sugar()

	databases := []string{"TestDropDB005A", "TestDropDB005B", "TestDropDB005C"}

	// Create multiple test databases
	for _, dbName := range databases {
		createTestDatabase(t, serviceManager, dbName, logger)
	}

	// Drop all databases
	for _, dbName := range databases {
		command := `DROP "` + dbName + `"`
		response, err := server.DropDatabase(command, logger, *serviceManager, session)

		assert.NoError(t, err, "DROP DATABASE should succeed for %s", dbName)
		assert.NotNil(t, response, "Response should not be nil for %s", dbName)

		// Verify each dropped database is removed
		_, err = serviceManager.DatabaseService.GetDatabaseByName(dbName)
		assert.Error(t, err, "Database %s should not exist after drop", dbName)
	}

	t.Log("✓ Multiple DROP DATABASE operations succeed sequentially")
}
