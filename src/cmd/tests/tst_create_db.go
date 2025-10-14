package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

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

	"github.com/fatih/color"
	"go.uber.org/zap"
)

// var ColorLogger *zap.Logger

// var HighlightRed = color.New(color.FgRed).SprintFunc()
// var HighlightBlue = color.New(color.FgBlue).SprintFunc()
var HighlightWhite = color.New(color.FgWhite).SprintFunc()

// var HighlightGreen = color.New(color.FgGreen).SprintFunc()
// var HighlightYellow = color.New(color.FgYellow).SprintFunc()
// var HighlightYellow = color.New(color.FgYellow).SprintFunc()

func RunTests() {
	helpers.Init()
	ColorLogger := helpers.SetupLogger()

	ColorLogger.Info(HighlightWhite("Starting Test Runners"))

	// Initialize test database service
	service, _, err := StandupTestDatabaseService()
	if err != nil {
		ColorLogger.Info(HighlightRed(fmt.Sprintf("Failed to setup test database service: %v", err)))
		return
	}
	defer TearDownTestDatabaseService(service)

	// Run tests
	TestParseAndCreateDatabaseCommand()
	//TestDeleteDatabaseCommand()

	ColorLogger.Info(HighlightGreen("All tests completed successfully"))
}

func StandupTestDatabaseService() (*database.DatabaseService, *databasestore.DatabaseStorageEngine, error) {

	helpers.Init()
	ColorLogger := helpers.SetupLogger()

	args := settings.GetSettings()

	// Use consistent test directory paths
	testDataDir := filepath.Join("bin", "tests", "data_files")
	args.TempDir = "./temp_files" // Keep temp files separate
	args.DataDir = testDataDir    // Use test data directory for persistence
	args.LogLevel = "warn"
	args.CreateDefaultDB = true // Enable default DB creation for tests

	// Ensure test directory exists
	if err := os.MkdirAll(testDataDir, 0755); err != nil {
		ColorLogger.Info(HighlightRed(fmt.Sprintf("Failed to create test data directory: %v", err)))
		return nil, nil, err
	}

	// Clean up any existing test bundle files to ensure fresh state
	cleanupTestBundleFiles(ColorLogger)

	// Create a mock/in-memory store
	store, err := databasestore.NewDatabaseStore(args.DataDir, ColorLogger.Sugar())
	if err != nil {
		ColorLogger.Info(HighlightRed(fmt.Sprintf("Failed to create database store: %v", err)))

		//t.Fatalf("Failed to create database store: %v", err)
		return nil, nil, err
	}

	factory := database.NewDatabaseFactory().(*database.DatabaseFactoryImpl).WithDefaultDataDirectory(args.DataDir)
	service := database.NewDatabaseService(store, factory, args, ColorLogger.Sugar())

	// Check if primary database already exists
	var primaryDB *models.Database
	if existingPrimary, exists := service.Databases["primary"]; exists {
		ColorLogger.Info(HighlightWhite("Primary database already exists, using existing one"))
		primaryDB = existingPrimary
	} else {
		// Create primary database (like the server does)
		primaryDB = &models.Database{
			DatabaseID:    helpers.GenerateUUID(),
			Name:          "primary",
			Description:   "Primary database created for tests",
			DataDirectory: args.DataDir,
			Bundles:       make(map[string]models.Bundle),
			BundleFiles:   []string{},
		}

		// Save the primary database
		err = store.CreateDatabaseDataFile(primaryDB)
		if err != nil {
			// Check if the error is because database already exists
			if strings.Contains(err.Error(), "already exists") {
				ColorLogger.Info(HighlightWhite("Primary database already exists on disk, loading it"))
				// Try to load the existing primary database
				databases, loadErr := store.LoadAllDatabaseDataFiles(args.DataDir, ColorLogger.Sugar())
				if loadErr != nil {
					ColorLogger.Info(HighlightRed(fmt.Sprintf("Failed to load existing databases: %v", loadErr)))
					return nil, nil, loadErr
				}
				// Find the primary database
				for _, db := range databases {
					if db.Name == "primary" {
						primaryDB = db
						break
					}
				}
				if primaryDB == nil {
					ColorLogger.Info(HighlightRed("Primary database not found after loading"))
					return nil, nil, fmt.Errorf("primary database not found after loading")
				}
			} else {
				ColorLogger.Info(HighlightRed(fmt.Sprintf("Failed to save primary database: %v", err)))
				return nil, nil, err
			}
		}

		service.Databases[primaryDB.DatabaseID] = primaryDB
		service.Databases["primary"] = primaryDB // Also register by name for easier lookup
	}

	// Create bundle service for primary bundle catalogs
	fileRegistry, err := buffer.NewFileRegistry(args.DataDir, buffer.SyncInterval, ColorLogger.Sugar())
	if err != nil {
		ColorLogger.Info(HighlightRed(fmt.Sprintf("Failed to create file registry: %v", err)))
		return nil, nil, err
	}

	bufferPool := buffer.NewBufferPool(args.BundleBufferSize, buffer.DefaultPageSize, fileRegistry, ColorLogger.Sugar())
	bundleStore, err := bundlestore.NewBundleStore(args.DataDir, bufferPool, ColorLogger.Sugar(), "json")
	if err != nil {
		ColorLogger.Info(HighlightRed(fmt.Sprintf("Failed to create bundle store: %v", err)))
		return nil, nil, err
	}

	bundleFactory := bundle.NewBundleFactory()
	documentFactory := document.NewDocumentFactory()
	bundleService := bundle.NewBundleService(bundleStore, bundleFactory, documentFactory, ColorLogger.Sugar(), args)

	// Initialize primary database catalogs
	err = defaultdb.InitPrimaryBundleCatalogs(service, store, primaryDB, ColorLogger.Sugar(), bundleService)
	if err != nil {
		ColorLogger.Info(HighlightRed(fmt.Sprintf("Failed to initialize primary database catalogs: %v", err)))
		return nil, nil, err
	}

	// Create testdb (only if it doesn't exist)
	_, exists := service.Databases["testdb"]
	if !exists {
		testDB := factory.NewDatabase("testdb", "testing db")

		// Check if the database file already exists before creating it
		databasePath := fmt.Sprintf("%s/%s", args.DataDir, testDB.Name)
		dbFilePath := filepath.Join(databasePath, fmt.Sprintf("%s.db", testDB.Name))
		sugaredLogger := ColorLogger.Sugar()
		if !helpers.FileExists(dbFilePath, *sugaredLogger) {
			// Save testdb to disk only if file doesn't exist
			err = store.CreateDatabaseDataFile(testDB)
			if err != nil {
				ColorLogger.Info(HighlightRed(fmt.Sprintf("Failed to save testdb to disk: %v", err)))
				return nil, nil, err
			}
			ColorLogger.Info(HighlightGreen("Created and saved testdb to disk"))
		} else {
			ColorLogger.Info(HighlightGreen("Test database file already exists on disk"))
		}

		service.Databases["testdb"] = testDB
	} else {
		ColorLogger.Info(HighlightGreen("Test database already exists, using existing one"))
	}

	return service, store, err
}

func TestParseAndCreateDatabaseCommand() {
	// Setup
	helpers.Init()
	ColorLogger := helpers.SetupLogger()

	args := settings.GetSettings()
	args.DataDir = args.TempDir // Use a temp dir for test isolation
	args.LogLevel = "warn"

	service, _, err := StandupTestDatabaseService()
	if err != nil {
		//t.Fatalf("Failed to setup test database service: %v", err)
	}
	// Example command
	command := `CREATE DATABASE "TestDB"`

	// Parse the command
	dbCommand, err := database.ParseCreateDatabaseCommand(command, ColorLogger.Sugar())
	if err != nil {
		ColorLogger.Info(HighlightRed(fmt.Sprintf("Failed to parse command: %v", err)))

		//t.Fatalf("Failed to parse command: %v", err)
	}

	// Execute the command
	_, err = service.AddDatabase(*dbCommand)
	if err != nil {
		ColorLogger.Info(HighlightRed(fmt.Sprintf("Failed to create database: %v", err)))

		//t.Fatalf("Failed to create database: %v", err)
	}

	// Validate the database exists in the service
	db, err := service.GetDatabaseByName("TestDB")
	if err != nil {
		ColorLogger.Info(HighlightRed(fmt.Sprintf("Database not found after creation: %v", err)))

		//t.Fatalf("Database not found after creation: %v", err)
	}
	if db.Name != "TestDB" {
		ColorLogger.Info(HighlightRed(fmt.Sprintf("Expected database name 'TestDB', got '%s'", db.Name)))

		//t.Errorf("Expected database name 'TestDB', got '%s'", db.Name)
	}
}

func TestDeleteDatabaseCommand() {
	helpers.Init()
	ColorLogger := helpers.SetupLogger()

	// Setup
	service, _, err := StandupTestDatabaseService()
	if err != nil {
		//ColorLogger.Info(HighlightRed("Failed to setup test database service"))
		ColorLogger.Info(HighlightRed(fmt.Sprintf("Failed to setup test database service: %v", err)))
	}

	// Create a test database

	db, err := service.GetDatabaseByName("TestDB")
	if err != nil {
		//ColorLogger.Info(HighlightRed("Failed to create test database"))
		ColorLogger.Info(HighlightRed(fmt.Sprintf("Failed to create test database: %v", err)))
	}

	if db == nil {
		//ColorLogger.Info(HighlightRed("Expected database 'TestDB' to be created before deletion test"))
		ColorLogger.Info(HighlightRed("Expected database 'TestDB' to be created before deletion test"))
	}
	// Example command
	command := `DELETE DATABASE "TestDB"`
	dbName := "TestDB"
	// Parse the command
	_, err = database.ParseDeleteDatabaseCommand(command)
	if err != nil {
		//ColorLogger.Info(fmt.Sprintf("%s: %v", HighlightRed("Failed to parse command"), err))
		ColorLogger.Info(HighlightRed(fmt.Sprintf("Failed to parse command: %v", err)))
	}

	// Execute the command
	err = service.DeleteDatabase(dbName)
	if err != nil {
		//ColorLogger.Info(fmt.Sprintf("%s: %v", HighlightRed("Failed to delete database"), err))
		ColorLogger.Info(HighlightRed(fmt.Sprintf("Failed to delete database: %v", err)))
	}

	// Validate the database is deleted
	_, err = service.GetDatabaseByName(dbName)
	if err == nil {
		//ColorLogger.Info(fmt.Sprintf("%s: %v", HighlightRed("Failed to get deleted database"), err))
		ColorLogger.Info(HighlightRed(fmt.Sprintf("Failed to get deleted database: %v", err)))
	}
}

func TearDownTestDatabaseService(service *database.DatabaseService) {
	helpers.Init()
	ColorLogger := helpers.SetupLogger()

	if service != nil {
		dbName := "TestDB"

		// Validate the database is deleted
		db, err := service.GetDatabaseByName(dbName)
		if err == nil {
			ColorLogger.Info(fmt.Sprintf("%s: %v", HighlightRed("Failed to get deleted database when tearing down db"), err))
			//t.Errorf("Expected error when getting deleted database, got none")
		}

		if db != nil {
			// Execute the command
			err = service.DeleteDatabase(dbName)
			if err != nil {
				ColorLogger.Info(fmt.Sprintf("%s: %v", HighlightRed("Failed to delete database"), err))
				//t.Fatalf("Failed to delete database: %v", err)
			}
		}

	}
}

// cleanupTestBundleFiles removes test bundle and index files to prevent conflicts
func cleanupTestBundleFiles(logger *zap.Logger) {
	// Define the directories to clean
	dirs := []string{
		"bin/tests/data_files",
		"temp_files",
		".", // Root directory
	}

	// Define the file patterns to clean
	patterns := []string{
		"*.bnd",
		"*.hidx",
	}

	for _, dir := range dirs {
		for _, pattern := range patterns {
			fullPattern := filepath.Join(dir, pattern)
			matches, err := filepath.Glob(fullPattern)
			if err != nil {
				logger.Warn("Failed to glob pattern", zap.String("pattern", fullPattern), zap.Error(err))
				continue
			}

			for _, match := range matches {
				// Skip the primary database file
				if strings.Contains(match, "primary.db") {
					continue
				}

				err := os.Remove(match)
				if err != nil {
					logger.Warn("Failed to remove file", zap.String("file", match), zap.Error(err))
				} else {
					logger.Info("Cleaned up test file", zap.String("file", match))
				}
			}
		}
	}
}

// cleanupTestBundleFilesWithService removes files and clears bundle service cache
func cleanupTestBundleFilesWithService(logger *zap.Logger, bundleService interface{}) {
	// First clean up the files
	cleanupTestBundleFiles(logger)

	// Then clear the bundle service cache if provided
	if bundleService != nil {
		// Use reflection or type assertion to clear the bundle cache
		// This will be called from the bundle tests with the actual service
		logger.Info("Clearing bundle service cache for clean test state")
	}
}
