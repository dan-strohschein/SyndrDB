package main

import (
	"fmt"
	"log"
	"strings"
	"syndrdb/src/internal/domain/bundle"
	"syndrdb/src/internal/domain/database"
	"syndrdb/src/internal/domain/models"
	"syndrdb/src/internal/server"
	"syndrdb/src/pkg/settings"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func main() {
	// Initialize logger
	config := zap.NewDevelopmentConfig()
	config.Level = zap.NewAtomicLevelAt(zapcore.InfoLevel)
	logger, err := config.Build()
	if err != nil {
		log.Fatal("Failed to initialize logger:", err)
	}
	defer logger.Sync()
	sugar := logger.Sugar()

	// Initialize settings
	appSettings := settings.GetSettings()
	appSettings.Debug = false // Reduce noise for this test

	// Initialize services
	databaseService := database.NewDatabaseService(sugar)
	bundleService := bundle.NewBundleService(sugar)

	// Initialize the service manager
	serviceManager := server.InitServiceManager(databaseService, bundleService, sugar)

	// Create a test database
	database := &models.Database{
		DatabaseID:    "test_db",
		Name:          "TestDB",
		Description:   "Test database for field selection",
		BundleFiles:   []string{},
		Bundles:       make(map[string]models.Bundle),
		DataDirectory: "./temp_files",
	}

	fmt.Println("🚀 Testing SyndrDB Field Selection Feature")
	fmt.Println(strings.Repeat("=", 60))

	// 1. Create a bundle with sample fields
	createBundleCmd := `CREATE BUNDLE "Users" WITH FIELDS (
		{"name", "STRING", true, false, ""},
		{"age", "INT", true, false, 0},
		{"email", "STRING", true, true, ""},
		{"city", "STRING", false, false, ""},
		{"salary", "INT", false, false, 0}
	)`

	fmt.Println("\n📦 Step 1: Creating Users bundle...")
	fmt.Printf("Command: %s\n", createBundleCmd)

	response, err := server.CommandDirector(database, *serviceManager, createBundleCmd, sugar)
	if err != nil {
		log.Fatalf("Failed to create bundle: %v", err)
	}
	fmt.Printf("✅ Result: %v\n", response)

	// 2. Insert sample documents
	insertCommands := []string{
		`INSERT INTO "Users" ({"name"="John Doe", "age"=30, "email"="john@example.com", "city"="New York", "salary"=75000})`,
		`INSERT INTO "Users" ({"name"="Jane Smith", "age"=28, "email"="jane@example.com", "city"="Los Angeles", "salary"=65000})`,
		`INSERT INTO "Users" ({"name"="Bob Johnson", "age"=35, "email"="bob@example.com", "city"="Chicago", "salary"=85000})`,
	}

	fmt.Println("\n📝 Step 2: Inserting sample documents...")
	for i, cmd := range insertCommands {
		fmt.Printf("Document %d: %s\n", i+1, cmd)
		_, err := server.CommandDirector(database, *serviceManager, cmd, sugar)
		if err != nil {
			log.Printf("Failed to insert document %d: %v", i+1, err)
		} else {
			fmt.Printf("✅ Inserted document %d\n", i+1)
		}
	}

	// 3. Test different SELECT queries
	testQueries := []struct {
		name        string
		query       string
		description string
	}{
		{
			name:        "Legacy SELECT DOCUMENTS",
			query:       `SELECT DOCUMENTS FROM "Users"`,
			description: "Returns all fields (legacy syntax)",
		},
		{
			name:        "Select specific fields",
			query:       `SELECT name, email FROM "Users"`,
			description: "Returns only name and email fields",
		},
		{
			name:        "Select with quoted fields",
			query:       `SELECT "name", 'age', city FROM "Users"`,
			description: "Returns name, age, and city (with mixed quotes)",
		},
		{
			name:        "Select with WHERE clause",
			query:       `SELECT name, salary FROM "Users" WHERE age > 30`,
			description: "Returns name and salary for users older than 30",
		},
		{
			name:        "Select single field",
			query:       `SELECT email FROM "Users"`,
			description: "Returns only email field",
		},
	}

	fmt.Println("\n🔍 Step 3: Testing field selection queries...")
	for i, test := range testQueries {
		fmt.Printf("\n--- Test %d: %s ---\n", i+1, test.name)
		fmt.Printf("Description: %s\n", test.description)
		fmt.Printf("Query: %s\n", test.query)

		response, err := server.CommandDirector(database, *serviceManager, test.query, sugar)
		if err != nil {
			fmt.Printf("❌ Error: %v\n", err)
			continue
		}

		if cmdResponse, ok := response.(*server.CommandResponse); ok {
			fmt.Printf("✅ Success: Found %d documents\n", cmdResponse.ResultCount)

			// Show field structure for first document (if any)
			if documents, ok := cmdResponse.Result.(map[string]*models.Document); ok && len(documents) > 0 {
				var firstDoc *models.Document
				for _, doc := range documents {
					firstDoc = doc
					break
				}

				fmt.Printf("Sample document fields: ")
				fieldNames := []string{}
				for fieldName := range firstDoc.Fields {
					fieldNames = append(fieldNames, fieldName)
				}
				fmt.Printf("%v\n", fieldNames)
			}
		} else {
			fmt.Printf("✅ Response: %v\n", response)
		}
	}

	fmt.Println("\n🎉 Field Selection Testing Complete!")
	fmt.Println(strings.Repeat("=", 60))
}
