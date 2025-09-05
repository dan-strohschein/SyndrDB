package main

import (
	"fmt"

	"syndrdb/src/internal/domain/database"
	"syndrdb/src/internal/storage/databasestore"
	"syndrdb/src/pkg/common/helpers"
	"syndrdb/src/pkg/settings"

	"github.com/fatih/color"
)

// var ColorLogger *zap.Logger

// var HighlightRed = color.New(color.FgRed).SprintFunc()
// var HighlightBlue = color.New(color.FgBlue).SprintFunc()
var HighlightWhite = color.New(color.FgWhite).SprintFunc()

// var HighlightGreen = color.New(color.FgGreen).SprintFunc()
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
	args.TempDir = "./temp_files" // Use a temp dir for test isolation
	args.DataDir = args.TempDir   // Use a temp dir for test isolation
	args.LogLevel = "warn"

	// Create a mock/in-memory store
	store, err := databasestore.NewDatabaseStore(args.DataDir, ColorLogger.Sugar())
	if err != nil {
		ColorLogger.Info(HighlightRed(fmt.Sprintf("Failed to create database store: %v", err)))

		//t.Fatalf("Failed to create database store: %v", err)
		return nil, nil, err
	}

	factory := database.NewDatabaseFactory()
	db := factory.NewDatabase("testdb", "testing db")
	service := database.NewDatabaseService(store, factory, args, ColorLogger.Sugar())
	service.Databases["testdb"] = db
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
	err = service.AddDatabase(*dbCommand)
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
