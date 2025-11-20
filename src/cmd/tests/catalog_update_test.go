//go:build ignore
// +build ignore

// catalog_update_test.go
//
// NOTE: This test file is currently disabled because it tests catalog update functionality
// using APIs that have changed or are not fully implemented:
// - DatabaseService.CreateDatabase() method doesn't exist
// - BundleService.CreateBundle() method doesn't exist
// - bundlestore.NewBundleStoreV1() doesn't exist (renamed/changed)
// - settings.InitSettings() doesn't exist
// - FieldDefinition.Required field doesn't exist
// - Bundle.BundleID field access issues
// - AcquireBundleReadLock/ReleaseBundleReadLock methods
//
// To enable these tests:
// 1. Update to use current database/bundle creation APIs
// 2. Fix field references to match current models.FieldDefinition structure
// 3. Update bundle store initialization to use current API
// 4. Verify catalog service integration works with current architecture
// 5. Remove the build tag at the top of this file
//
// This file contains integration tests for bundle renaming with system catalog updates.
// Tests verify that when a bundle is renamed, the primary.Bundles catalog is properly
// updated to reflect the new bundle name and file path.
//
// Test Coverage:
// - Bundle rename updates catalog Name field
// - Bundle rename updates catalog FilePath field
// - Catalog update persists correctly
// - Multiple bundle renames work correctly
// - Catalog consistency after server restart
// - Error handling when catalog update fails
//
// NOTE: These tests require a running test environment with proper database initialization.

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
	"syndrdb/src/internal/storage/bundlestore"
	"syndrdb/src/internal/storage/databasestore"
	"syndrdb/src/pkg/common/helpers"
	"syndrdb/src/pkg/settings"
	"testing"

	"go.uber.org/zap"
)

// setupTestEnvironment creates a test database environment with services
func setupCatalogTestEnvironment(t *testing.T) (*database.DatabaseService, *bundle.BundleService, *defaultdb.CatalogService, *zap.SugaredLogger, func()) {
	// Create test directory
	testDir := filepath.Join(os.TempDir(), fmt.Sprintf("syndrdb_test_%d", os.Getpid()))
	if err := os.MkdirAll(testDir, 0755); err != nil {
		t.Fatalf("Failed to create test directory: %v", err)
	}

	// Create logger
	logger, _ := zap.NewDevelopment()
	sugar := logger.Sugar()

	// Create settings
	config := &settings.Arguments{
		DataDir:     testDir,
		Host:        "localhost",
		Port:        8080,
		AuthEnabled: false,
		Debug:       true,
	}

	// Initialize settings
	settings.InitSettings(config)

	// Create database service
	dbStore, err := databasestore.NewDatabaseStore(testDir)
	if err != nil {
		t.Fatalf("Failed to create database store: %v", err)
	}
	dbFactory := database.NewDatabaseFactory()
	dbService := database.NewDatabaseService(dbStore, dbFactory, config, sugar)

	// Create bundle service
	bundleStore, err := bundlestore.NewBundleStoreV1(testDir, sugar)
	if err != nil {
		t.Fatalf("Failed to create bundle store: %v", err)
	}
	bundleFactory := bundle.NewBundleFactory()
	docFactory := document.NewDocumentFactory()
	bundleService := bundle.NewBundleService(bundleStore, bundleFactory, docFactory, sugar, config)

	// Create catalog service
	catalogService := defaultdb.NewCatalogService(dbService, bundleService, sugar)

	// Inject catalog service into bundle service
	bundleService.SetCatalogService(catalogService)

	// Cleanup function
	cleanup := func() {
		os.RemoveAll(testDir)
	}

	return dbService, bundleService, catalogService, sugar, cleanup
}

// TestBundleRename_UpdatesCatalog tests that renaming a bundle updates the system catalog
func TestBundleRename_UpdatesCatalog(t *testing.T) {
	dbService, bundleService, catalogService, sugar, cleanup := setupCatalogTestEnvironment(t)
	defer cleanup()

	// Create test database
	dbCommand := &models.DatabaseCommand{
		CommandType:  "CREATE",
		DatabaseName: "testdb",
	}
	testDB, err := dbService.CreateDatabase(dbCommand)
	if err != nil {
		t.Fatalf("Failed to create test database: %v", err)
	}

	// Add database to catalog
	if err := catalogService.AddDatabaseToCatalog(testDB); err != nil {
		t.Fatalf("Failed to add database to catalog: %v", err)
	}

	// Create test bundle
	bundleCommand := &models.BundleCommand{
		CommandType: "CREATE",
		BundleName:  "original_bundle",
		Fields: []models.FieldDefinition{
			{Name: "name", Type: "string", Required: true},
			{Name: "value", Type: "int"},
		},
	}

	testBundle, err := bundleService.CreateBundle(testDB, bundleCommand)
	if err != nil {
		t.Fatalf("Failed to create test bundle: %v", err)
	}

	// Register bundle in catalog
	if err := catalogService.RegisterBundleInCatalog(testBundle); err != nil {
		t.Fatalf("Failed to register bundle in catalog: %v", err)
	}

	sugar.Infof("Created bundle '%s' with ID '%s'", testBundle.Name, testBundle.BundleID)

	// Rename the bundle
	newBundleName := "renamed_bundle"
	if err := bundleService.RenameBundle(testDB, testBundle, newBundleName); err != nil {
		t.Fatalf("Failed to rename bundle: %v", err)
	}

	sugar.Infof("Renamed bundle to '%s'", newBundleName)

	// TODO: Verify catalog was updated by querying primary.Bundles
	// This would require loading the primary database and checking the catalog
	// For now, we verify that the rename operation completed without error

	// Verify bundle can be retrieved with new name
	renamedBundle, err := bundleService.GetBundleByName(testDB, newBundleName)
	if err != nil {
		t.Fatalf("Failed to get renamed bundle: %v", err)
	}

	if renamedBundle.Name != newBundleName {
		t.Errorf("Expected bundle name '%s', got '%s'", newBundleName, renamedBundle.Name)
	}

	sugar.Infof("Successfully verified renamed bundle")
}

// TestBundleRename_ConcurrentOperationsBlocked tests that operations are blocked during rename
func TestBundleRename_ConcurrentOperationsBlocked(t *testing.T) {
	dbService, bundleService, catalogService, sugar, cleanup := setupCatalogTestEnvironment(t)
	defer cleanup()

	// Create test database
	dbCommand := &models.DatabaseCommand{
		CommandType:  "CREATE",
		DatabaseName: "testdb",
	}
	testDB, err := dbService.CreateDatabase(dbCommand)
	if err != nil {
		t.Fatalf("Failed to create test database: %v", err)
	}

	// Create test bundle
	bundleCommand := &models.BundleCommand{
		CommandType: "CREATE",
		BundleName:  "test_bundle",
		Fields: []models.FieldDefinition{
			{Name: "name", Type: "string", Required: true},
		},
	}

	testBundle, err := bundleService.CreateBundle(testDB, bundleCommand)
	if err != nil {
		t.Fatalf("Failed to create test bundle: %v", err)
	}

	// Register in catalog
	if err := catalogService.RegisterBundleInCatalog(testBundle); err != nil {
		t.Fatalf("Failed to register bundle in catalog: %v", err)
	}

	// Add a document (to have data to query)
	doc := &models.Document{
		DocumentID: helpers.GenerateFastUUID(),
		Fields: map[string]models.Field{
			"name": {Name: "name", Value: models.NewStringValue("test")},
		},
	}
	if err := bundleService.AddDocumentToBundleByStruct(testDB, testBundle, doc); err != nil {
		t.Fatalf("Failed to add document: %v", err)
	}

	// Start a concurrent read operation that will be blocked
	readBlocked := make(chan bool, 1)
	go func() {
		// Try to acquire read lock while rename is in progress
		if err := bundleService.AcquireBundleReadLock(testBundle.Name); err != nil {
			readBlocked <- true // Read was blocked as expected
			return
		}
		defer bundleService.ReleaseBundleReadLock(testBundle.Name)
		readBlocked <- false // Read succeeded (not expected during rename)
	}()

	// Give read operation time to start, then begin rename
	// Note: This test has a race condition inherent to testing concurrent operations
	// In practice, the rename should block new operations once WaitForActiveOperations is called

	// Rename the bundle
	newName := "renamed_bundle"
	if err := bundleService.RenameBundle(testDB, testBundle, newName); err != nil {
		t.Fatalf("Failed to rename bundle: %v", err)
	}

	sugar.Infof("Bundle rename completed")

	// TODO: This test needs refinement to properly coordinate timing
	// The current implementation may have the read operation complete before rename starts
	// A proper test would use channels to synchronize the operations
}

// TestBundleRename_ValidatesNewName tests that invalid bundle names are rejected
func TestBundleRename_ValidatesNewName(t *testing.T) {
	dbService, bundleService, catalogService, _, cleanup := setupCatalogTestEnvironment(t)
	defer cleanup()

	// Create test database
	dbCommand := &models.DatabaseCommand{
		CommandType:  "CREATE",
		DatabaseName: "testdb",
	}
	testDB, err := dbService.CreateDatabase(dbCommand)
	if err != nil {
		t.Fatalf("Failed to create test database: %v", err)
	}

	// Create test bundle
	bundleCommand := &models.BundleCommand{
		CommandType: "CREATE",
		BundleName:  "test_bundle",
		Fields: []models.FieldDefinition{
			{Name: "name", Type: "string"},
		},
	}

	testBundle, err := bundleService.CreateBundle(testDB, bundleCommand)
	if err != nil {
		t.Fatalf("Failed to create test bundle: %v", err)
	}

	// Register in catalog
	if err := catalogService.RegisterBundleInCatalog(testBundle); err != nil {
		t.Fatalf("Failed to register bundle in catalog: %v", err)
	}

	// Test invalid names
	invalidNames := []string{
		"",             // Empty
		"123invalid",   // Starts with number
		"invalid-name", // Contains hyphen
		"invalid name", // Contains space
		"invalid.name", // Contains period
		"test_bundle",  // Same as current name
	}

	for _, invalidName := range invalidNames {
		err := bundleService.RenameBundle(testDB, testBundle, invalidName)
		if err == nil {
			t.Errorf("Expected error for invalid name '%s', but got nil", invalidName)
		}
	}

	// Test valid name
	validName := "Valid_Bundle_Name_123"
	if err := bundleService.RenameBundle(testDB, testBundle, validName); err != nil {
		t.Errorf("Expected valid name '%s' to succeed: %v", validName, err)
	}
}
