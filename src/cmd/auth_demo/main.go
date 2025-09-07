package main

import (
	"log"
	"syndrdb/src/internal/defaultDB"
	"syndrdb/src/internal/domain/database"
	"syndrdb/src/internal/server"
	"syndrdb/src/pkg/settings"

	"go.uber.org/zap"
)

func main() {
	// Set up logger
	logger, _ := zap.NewDevelopment()
	defer logger.Sync()
	sugar := logger.Sugar()

	// Initialize settings
	settings := &settings.Settings{
		DataDir: "./test_data",
	}

	// Create service manager
	databaseService := database.NewDatabaseService(sugar, settings)

	// Initialize Primary database
	err := defaultDB.InitializeInternalCatalog(databaseService, sugar)
	if err != nil {
		log.Fatalf("Failed to initialize Primary database: %v", err)
	}

	// Create a simple service manager for testing
	serviceManager := &TestServiceManager{
		DatabaseService: databaseService,
	}

	sugar.Info("Starting SyndrDB Authentication System Demo")

	// Test 1: Add a user
	sugar.Info("=== Test 1: Adding a user ===")
	result, err := server.CommandDirector(nil, serviceManager, "ADD USER admin WITH PASSWORD 'admin123'", sugar)
	if err != nil {
		sugar.Errorf("Failed to add user: %v", err)
	} else {
		sugar.Infof("Add user result: %+v", result)
	}

	// Test 2: Grant Admin permission to user
	sugar.Info("=== Test 2: Granting Admin permission ===")
	result, err = server.CommandDirector(nil, serviceManager, "GRANT Admin TO USER admin", sugar)
	if err != nil {
		sugar.Errorf("Failed to grant permission: %v", err)
	} else {
		sugar.Infof("Grant permission result: %+v", result)
	}

	// Test 3: Create a new database (will be auto-registered)
	sugar.Info("=== Test 3: Creating a new database ===")
	result, err = server.CommandDirector(nil, serviceManager, "CREATE DATABASE company_data", sugar)
	if err != nil {
		sugar.Errorf("Failed to create database: %v", err)
	} else {
		sugar.Infof("Create database result: %+v", result)
	}

	// Test 4: Attach user to database
	sugar.Info("=== Test 4: Attaching user to database ===")
	result, err = server.CommandDirector(nil, serviceManager, "ATTACH USER admin TO DATABASE company_data", sugar)
	if err != nil {
		sugar.Errorf("Failed to attach user to database: %v", err)
	} else {
		sugar.Infof("Attach user result: %+v", result)
	}

	// Test 5: Check if user has Admin permission
	sugar.Info("=== Test 5: Checking user permissions ===")
	hasPermission, err := server.CheckUserHasPermission("admin", "Admin", serviceManager)
	if err != nil {
		sugar.Errorf("Failed to check permission: %v", err)
	} else {
		sugar.Infof("User 'admin' has Admin permission: %v", hasPermission)
	}

	sugar.Info("Authentication system demo completed!")
}

// TestServiceManager is a minimal implementation for testing
type TestServiceManager struct {
	DatabaseService *database.DatabaseService
}

func (sm *TestServiceManager) GetDatabaseService() *database.DatabaseService {
	return sm.DatabaseService
}

// Implement the ServiceManager interface methods that are needed
func (sm *TestServiceManager) DatabaseService() *database.DatabaseService {
	return sm.DatabaseService
}
