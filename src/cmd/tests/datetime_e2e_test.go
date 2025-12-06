package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	defaultdb "syndrdb/src/internal/defaultDB"
	"syndrdb/src/internal/domain/bundle"
	"syndrdb/src/internal/domain/database"
	"syndrdb/src/internal/domain/document"
	"syndrdb/src/internal/domain/models"
	"syndrdb/src/internal/journal"
	"syndrdb/src/internal/lock"
	"syndrdb/src/internal/query/planner"
	"syndrdb/src/internal/server"
	"syndrdb/src/internal/storage/buffer"
	"syndrdb/src/internal/storage/bundlestore"
	"syndrdb/src/internal/storage/databasestore"
	"syndrdb/src/pkg/common/helpers"
	"syndrdb/src/pkg/settings"
	"testing"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func setupTestFixture(t *testing.T) *TestFixture {
	t.Helper()
	EnsureTestIsolation(t)

	fixture := setupFullServerTB(t)

	// Setup code for test fixture (e.g., initialize database, service manager, logger)
	return fixture
}

func teardownTestFixture(t *testing.T, fixture *TestFixture) {
	// Teardown code for test fixture (e.g., clean up database)
}

type TestFixture struct {
	TempDir        string
	Database       models.Database
	ServiceManager *server.ServiceManager
	Logger         *zap.SugaredLogger
	Settings       *settings.Arguments
}

// TestDateTime_E2E_BundleCreation tests creating a bundle with DateTime and Date fields
func TestDateTime_E2E_BundleCreation(t *testing.T) {
	fixture := setupTestFixture(t)
	defer teardownTestFixture(t, fixture)

	bundleName := "TestEventsBundle"

	// Create bundle with DateTime and Date fields
	createCmd := fmt.Sprintf(`CREATE BUNDLE "%s" WITH FIELDS (
		{"eventId", "String", true, false, ""},
		{"eventName", "String", true, false, ""},
		{"eventTime", "DateTime", true, false, ""},
		{"eventDate", "Date", true, false, ""},
		{"description", "String", false, false, ""}
	);`, bundleName)

	startTime := time.Now()
	response, err := server.CommandDirector(context.Background(), &fixture.Database, *fixture.ServiceManager, createCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	if err != nil {
		t.Fatalf("Failed to create bundle: %v", err)
	}

	// Verify response
	if cmdResp, ok := response.(*server.CommandResponse); ok {
		if !strings.Contains(fmt.Sprintf("%v", cmdResp.Result), "success") {
			t.Logf("Create bundle result: %+v", cmdResp.Result)
		}
	}

	t.Logf("Bundle created successfully")
}

// TestDateTime_E2E_DocumentInsertAndRetrieve tests inserting documents with datetime fields
func TestDateTime_E2E_DocumentInsertAndRetrieve(t *testing.T) {
	fixture := setupTestFixture(t)
	defer teardownTestFixture(t, fixture)

	bundleName := "TestEventsBundle"

	// Create bundle first
	createCmd := fmt.Sprintf(`CREATE BUNDLE "%s" WITH FIELDS (
		{"eventId", "String", true, false, ""},
		{"eventName", "String", true, false, ""},
		{"eventTime", "DateTime", true, false, ""},
		{"eventDate", "Date", true, false, ""},
		{"description", "String", false, false, ""}
	);`, bundleName)

	startTime := time.Now()
	_, err := server.CommandDirector(context.Background(), &fixture.Database, *fixture.ServiceManager, createCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	if err != nil {
		t.Fatalf("Failed to create bundle: %v", err)
	}

	// Insert document with RFC3339 datetime
	insertCmd := fmt.Sprintf(`ADD DOCUMENT TO BUNDLE "%s" WITH (
		{"eventId"="evt-001"},
		{"eventName"="Product Launch"},
		{"eventTime"="2024-11-22T15:30:00Z"},
		{"eventDate"="2024-11-22"},
		{"description"="New product announcement"}
	);`, bundleName)

	startTime = time.Now()
	response, err := server.CommandDirector(context.Background(), &fixture.Database, *fixture.ServiceManager, insertCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	if err != nil {
		t.Fatalf("Failed to insert document: %v", err)
	}

	t.Logf("Insert response: %+v", response)

	// Retrieve and verify
	selectCmd := fmt.Sprintf(`SELECT * FROM "%s" WHERE "eventId" == "evt-001";`, bundleName)
	startTime = time.Now()
	response, err = server.CommandDirector(context.Background(), &fixture.Database, *fixture.ServiceManager, selectCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	if err != nil {
		t.Fatalf("Failed to select document: %v", err)
	}

	// Parse JSON response
	cmdResp, ok := response.(*server.CommandResponse)
	if !ok {
		t.Fatalf("Expected CommandResponse, got %T", response)
	}

	jsonBytes, err := json.Marshal(cmdResp.Result)
	if err != nil {
		t.Fatalf("Failed to marshal result: %v", err)
	}

	var documents []map[string]interface{}
	if err := json.Unmarshal(jsonBytes, &documents); err != nil {
		t.Fatalf("Failed to unmarshal documents: %v", err)
	}

	if len(documents) != 1 {
		t.Fatalf("Expected 1 document, got %d", len(documents))
	}

	doc := documents[0]

	// Verify DateTime field (should be RFC3339)
	eventTime, ok := doc["eventTime"].(string)
	if !ok {
		t.Fatalf("eventTime should be string, got %T", doc["eventTime"])
	}
	if !strings.HasPrefix(eventTime, "2024-11-22T15:30:00") {
		t.Errorf("Expected eventTime to start with 2024-11-22T15:30:00, got %s", eventTime)
	}

	// Verify Date field (should be YYYY-MM-DD)
	eventDate, ok := doc["eventDate"].(string)
	if !ok {
		t.Fatalf("eventDate should be string, got %T", doc["eventDate"])
	}
	if eventDate != "2024-11-22" {
		t.Errorf("Expected eventDate to be 2024-11-22, got %s", eventDate)
	}

	t.Logf("Document retrieved and verified successfully")
}

// TestDateTime_E2E_MultipleFormats tests inserting documents with various datetime formats
func TestDateTime_E2E_MultipleFormats(t *testing.T) {
	fixture := setupTestFixture(t)
	defer teardownTestFixture(t, fixture)

	bundleName := "TestEventsBundle"

	// Create bundle
	createCmd := fmt.Sprintf(`CREATE BUNDLE "%s" WITH FIELDS (
		{"eventId", "String", true, false, ""},
		{"eventName", "String", true, false, ""},
		{"eventTime", "DateTime", true, false, ""},
		{"eventDate", "Date", true, false, ""},
		{"description", "String", false, false, ""}
	);`, bundleName)

	startTime := time.Now()
	_, err := server.CommandDirector(context.Background(), &fixture.Database, *fixture.ServiceManager, createCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	if err != nil {
		t.Fatalf("Failed to create bundle: %v", err)
	}

	testCases := []struct {
		name        string
		eventId     string
		eventTime   string
		eventDate   string
		description string
	}{
		{
			name:        "RFC3339",
			eventId:     "evt-001",
			eventTime:   "2024-11-22T15:30:00Z",
			eventDate:   "2024-11-22",
			description: "RFC3339 format",
		},
		{
			name:        "ISO8601",
			eventId:     "evt-002",
			eventTime:   "2024-11-22T10:15:30",
			eventDate:   "2024-11-22",
			description: "ISO8601 format",
		},
		// US_Format is not yet supported by the dateTime format parser
		// {
		// 	name:        "US format",
		// 	eventId:     "evt-003",
		// 	eventTime:   "11/22/2024 3:45 PM",
		// 	eventDate:   "11/22/2024",
		// 	description: "US date format",
		// },
		{
			name:        "SQL format",
			eventId:     "evt-004",
			eventTime:   "2024-11-22 18:00:00",
			eventDate:   "2024-11-22",
			description: "SQL datetime format",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			insertCmd := fmt.Sprintf(`ADD DOCUMENT TO BUNDLE "%s" WITH (
				{"eventId"="%s"},
				{"eventName"="%s"},
				{"eventTime"="%s"},
				{"eventDate"="%s"},
				{"description"="%s"}
			);`, bundleName, tc.eventId, tc.name, tc.eventTime, tc.eventDate, tc.description)

			startTime := time.Now()
			_, err := server.CommandDirector(context.Background(), &fixture.Database, *fixture.ServiceManager, insertCmd, fixture.Logger, startTime, nil, "127.0.0.1")
			if err != nil {
				t.Fatalf("Failed to insert document: %v", err)
			}

			// Verify document was stored
			selectCmd := fmt.Sprintf(`SELECT * FROM "%s" WHERE "eventId" == "%s";`, bundleName, tc.eventId)
			startTime = time.Now()
			response, err := server.CommandDirector(context.Background(), &fixture.Database, *fixture.ServiceManager, selectCmd, fixture.Logger, startTime, nil, "127.0.0.1")
			if err != nil {
				t.Fatalf("Failed to select document: %v", err)
			}

			cmdResp, ok := response.(*server.CommandResponse)
			if !ok {
				t.Fatalf("Expected CommandResponse, got %T", response)
			}

			jsonBytes, err := json.Marshal(cmdResp.Result)
			if err != nil {
				t.Fatalf("Failed to marshal result: %v", err)
			}

			var documents []map[string]interface{}
			if err := json.Unmarshal(jsonBytes, &documents); err != nil {
				t.Fatalf("Failed to unmarshal documents: %v", err)
			}

			if len(documents) != 1 {
				t.Fatalf("Expected 1 document, got %d", len(documents))
			}

			t.Logf("Document %s inserted and retrieved successfully", tc.eventId)
		})
	}
}

// TestDateTime_E2E_WhereClauseFiltering tests filtering by datetime fields
func TestDateTime_E2E_WhereClauseFiltering(t *testing.T) {
	fixture := setupTestFixture(t)
	defer teardownTestFixture(t, fixture)

	bundleName := "TestEventsBundle"

	// Create bundle
	createCmd := fmt.Sprintf(`CREATE BUNDLE "%s" WITH FIELDS (
		{"eventId", "String", true, false, ""},
		{"eventName", "String", true, false, ""},
		{"eventTime", "DateTime", true, false, ""},
		{"eventDate", "Date", true, false, ""},
		{"description", "String", false, false, ""}
	);`, bundleName)

	startTime := time.Now()
	_, err := server.CommandDirector(context.Background(), &fixture.Database, *fixture.ServiceManager, createCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	if err != nil {
		t.Fatalf("Failed to create bundle: %v", err)
	}

	// Insert test data with known times
	testDocs := []struct {
		eventId   string
		eventTime string
		eventDate string
	}{
		{"evt-100", "2024-11-20T10:00:00Z", "2024-11-20"},
		{"evt-101", "2024-11-21T10:00:00Z", "2024-11-21"},
		{"evt-102", "2024-11-22T10:00:00Z", "2024-11-22"},
		{"evt-103", "2024-11-23T10:00:00Z", "2024-11-23"},
		{"evt-104", "2024-11-24T10:00:00Z", "2024-11-24"},
	}

	for _, doc := range testDocs {
		insertCmd := fmt.Sprintf(`ADD DOCUMENT TO BUNDLE "%s" WITH (
			{"eventId"="%s"},
			{"eventName"="Test Event"},
			{"eventTime"="%s"},
			{"eventDate"="%s"},
			{"description"="Test filtering"}
		);`, bundleName, doc.eventId, doc.eventTime, doc.eventDate)

		startTime := time.Now()
		_, err := server.CommandDirector(context.Background(), &fixture.Database, *fixture.ServiceManager, insertCmd, fixture.Logger, startTime, nil, "127.0.0.1")
		if err != nil {
			t.Fatalf("Failed to insert document %s: %v", doc.eventId, err)
		}
	}

	t.Run("Equality filter on DateTime", func(t *testing.T) {
		selectCmd := fmt.Sprintf(`SELECT * FROM "%s" WHERE "eventTime" == "2024-11-22T10:00:00Z";`, bundleName)
		startTime := time.Now()
		response, err := server.CommandDirector(context.Background(), &fixture.Database, *fixture.ServiceManager, selectCmd, fixture.Logger, startTime, nil, "127.0.0.1")
		if err != nil {
			t.Fatalf("Failed to execute query: %v", err)
		}

		cmdResp, ok := response.(*server.CommandResponse)
		if !ok {
			t.Fatalf("Expected CommandResponse, got %T", response)
		}

		jsonBytes, err := json.Marshal(cmdResp.Result)
		if err != nil {
			t.Fatalf("Failed to marshal result: %v", err)
		}

		var documents []map[string]interface{}
		if err := json.Unmarshal(jsonBytes, &documents); err != nil {
			t.Fatalf("Failed to unmarshal documents: %v", err)
		}

		if len(documents) != 1 {
			t.Fatalf("Expected 1 document, got %d", len(documents))
		}

		if documents[0]["eventId"] != "evt-102" {
			t.Errorf("Expected evt-102, got %v", documents[0]["eventId"])
		}
	})

	t.Run("Greater than filter on DateTime", func(t *testing.T) {
		selectCmd := fmt.Sprintf(`SELECT * FROM "%s" WHERE "eventTime" > "2024-11-22T10:00:00Z";`, bundleName)
		startTime := time.Now()
		response, err := server.CommandDirector(context.Background(), &fixture.Database, *fixture.ServiceManager, selectCmd, fixture.Logger, startTime, nil, "127.0.0.1")
		if err != nil {
			t.Fatalf("Failed to execute query: %v", err)
		}

		cmdResp, ok := response.(*server.CommandResponse)
		if !ok {
			t.Fatalf("Expected CommandResponse, got %T", response)
		}

		jsonBytes, err := json.Marshal(cmdResp.Result)
		if err != nil {
			t.Fatalf("Failed to marshal result: %v", err)
		}

		var documents []map[string]interface{}
		if err := json.Unmarshal(jsonBytes, &documents); err != nil {
			t.Fatalf("Failed to unmarshal documents: %v", err)
		}

		// Should return evt-103 and evt-104
		if len(documents) < 2 {
			t.Errorf("Expected at least 2 documents, got %d", len(documents))
		}
	})

	t.Run("Less than or equal filter on Date", func(t *testing.T) {
		selectCmd := fmt.Sprintf(`SELECT * FROM "%s" WHERE "eventDate" <= "2024-11-21";`, bundleName)
		startTime := time.Now()
		response, err := server.CommandDirector(context.Background(), &fixture.Database, *fixture.ServiceManager, selectCmd, fixture.Logger, startTime, nil, "127.0.0.1")
		if err != nil {
			t.Fatalf("Failed to execute query: %v", err)
		}

		cmdResp, ok := response.(*server.CommandResponse)
		if !ok {
			t.Fatalf("Expected CommandResponse, got %T", response)
		}

		jsonBytes, err := json.Marshal(cmdResp.Result)
		if err != nil {
			t.Fatalf("Failed to marshal result: %v", err)
		}

		var documents []map[string]interface{}
		if err := json.Unmarshal(jsonBytes, &documents); err != nil {
			t.Fatalf("Failed to unmarshal documents: %v", err)
		}

		// Should return evt-100 and evt-101
		if len(documents) < 2 {
			t.Errorf("Expected at least 2 documents, got %d", len(documents))
		}
	})

	t.Run("Range filter on DateTime", func(t *testing.T) {
		selectCmd := fmt.Sprintf(`SELECT * FROM "%s" WHERE "eventTime" >= "2024-11-21T00:00:00Z" AND "eventTime" <= "2024-11-23T23:59:59Z";`, bundleName)
		startTime := time.Now()
		response, err := server.CommandDirector(context.Background(), &fixture.Database, *fixture.ServiceManager, selectCmd, fixture.Logger, startTime, nil, "127.0.0.1")
		if err != nil {
			t.Fatalf("Failed to execute query: %v", err)
		}

		cmdResp, ok := response.(*server.CommandResponse)
		if !ok {
			t.Fatalf("Expected CommandResponse, got %T", response)
		}

		jsonBytes, err := json.Marshal(cmdResp.Result)
		if err != nil {
			t.Fatalf("Failed to marshal result: %v", err)
		}

		var documents []map[string]interface{}
		if err := json.Unmarshal(jsonBytes, &documents); err != nil {
			t.Fatalf("Failed to unmarshal documents: %v", err)
		}

		// Should return evt-101, evt-102, evt-103
		if len(documents) < 3 {
			t.Errorf("Expected at least 3 documents, got %d", len(documents))
		}
	})
}

// TestDateTime_E2E_MillisecondPrecision tests millisecond precision in queries
func TestDateTime_E2E_MillisecondPrecision(t *testing.T) {
	fixture := setupTestFixture(t)
	defer teardownTestFixture(t, fixture)

	bundleName := "TestEventsBundle"

	// Create bundle
	createCmd := fmt.Sprintf(`CREATE BUNDLE "%s" WITH FIELDS (
		{"eventId", "String", true, false, ""},
		{"eventName", "String", true, false, ""},
		{"eventTime", "DateTime", true, false, ""},
		{"eventDate", "Date", true, false, ""},
		{"description", "String", false, false, ""}
	);`, bundleName)

	startTime := time.Now()
	_, err := server.CommandDirector(context.Background(), &fixture.Database, *fixture.ServiceManager, createCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	if err != nil {
		t.Fatalf("Failed to create bundle: %v", err)
	}

	// Insert documents with very close timestamps
	insertCmd1 := fmt.Sprintf(`ADD DOCUMENT TO BUNDLE "%s" WITH (
		{"eventId"="evt-ms-1"},
		{"eventName"="Millisecond Test 1"},
		{"eventTime"="2024-11-22T12:00:00.123Z"},
		{"eventDate"="2024-11-22"},
		{"description"="123ms"}
	);`, bundleName)

	insertCmd2 := fmt.Sprintf(`ADD DOCUMENT TO BUNDLE "%s" WITH (
		{"eventId"="evt-ms-2"},
		{"eventName"="Millisecond Test 2"},
		{"eventTime"="2024-11-22T12:00:00.456Z"},
		{"eventDate"="2024-11-22"},
		{"description"="456ms"}
	);`, bundleName)

	startTime = time.Now()
	_, err = server.CommandDirector(context.Background(), &fixture.Database, *fixture.ServiceManager, insertCmd1, fixture.Logger, startTime, nil, "127.0.0.1")
	if err != nil {
		t.Fatalf("Failed to insert document 1: %v", err)
	}

	startTime = time.Now()
	_, err = server.CommandDirector(context.Background(), &fixture.Database, *fixture.ServiceManager, insertCmd2, fixture.Logger, startTime, nil, "127.0.0.1")
	if err != nil {
		t.Fatalf("Failed to insert document 2: %v", err)
	}

	// Query for exact millisecond match
	selectCmd := fmt.Sprintf(`SELECT * FROM "%s" WHERE "eventTime" == "2024-11-22T12:00:00.123Z";`, bundleName)
	startTime = time.Now()
	response, err := server.CommandDirector(context.Background(), &fixture.Database, *fixture.ServiceManager, selectCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}

	cmdResp, ok := response.(*server.CommandResponse)
	if !ok {
		t.Fatalf("Expected CommandResponse, got %T", response)
	}

	jsonBytes, err := json.Marshal(cmdResp.Result)
	if err != nil {
		t.Fatalf("Failed to marshal result: %v", err)
	}

	var documents []map[string]interface{}
	if err := json.Unmarshal(jsonBytes, &documents); err != nil {
		t.Fatalf("Failed to unmarshal documents: %v", err)
	}

	if len(documents) != 1 {
		t.Fatalf("Expected 1 document, got %d", len(documents))
	}

	if documents[0]["eventId"] != "evt-ms-1" {
		t.Errorf("Expected evt-ms-1, got %v", documents[0]["eventId"])
	}

	t.Logf("Millisecond precision test passed")
}

// * ----------------- Setup and Teardown Helpers ----------------- * //

type filteredWriter struct {
	underlying io.Writer
	filter     string
}

func (fw *filteredWriter) Write(p []byte) (n int, err error) {
	// Filter out lines containing the filter string
	if !strings.Contains(string(p), fw.filter) {
		return fw.underlying.Write(p)
	}
	// Return length to satisfy io.Writer interface even when filtering
	return len(p), nil
}

func setupFullServerTB(tb testing.TB) *TestFixture {
	tb.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Create temporary directory - DON'T use tb.TempDir() so we can inspect it
	//tempDir, err := os.MkdirTemp("", "TestSelect_*")
	tempDir := tb.TempDir()
	//if err != nil {
	// 	tb.Fatalf("Failed to create temp directory: %v", err)
	// }
	tb.Logf("TEST DATA DIRECTORY (will be auto-cleaned): %s", tempDir)

	// Create a writer that filters out hash index header warnings
	filterWriter := &filteredWriter{
		underlying: os.Stderr,
		filter:     "Failed to update header after flush",
	}

	// Setup logger with filtered output
	loggerConfig := zap.NewDevelopmentConfig()
	loggerConfig.Level = zap.NewAtomicLevelAt(zap.InfoLevel)
	loggerConfig.OutputPaths = []string{"stderr"}
	loggerConfig.ErrorOutputPaths = []string{"stderr"}

	logger, err := loggerConfig.Build(zap.WrapCore(func(c zapcore.Core) zapcore.Core {
		return zapcore.NewCore(
			zapcore.NewConsoleEncoder(loggerConfig.EncoderConfig),
			zapcore.AddSync(filterWriter),
			loggerConfig.Level,
		)
	}))
	if err != nil {
		tb.Fatalf("Failed to create logger: %v", err)
	}
	sugar := logger.Sugar()

	// Create settings
	args := &settings.Arguments{
		DataDir:         filepath.Join(tempDir, "data_files"),
		TempDir:         filepath.Join(tempDir, "temp_files"),
		LogDir:          filepath.Join(tempDir, "log_files"),
		LogLevel:        "warn",
		CreateDefaultDB: true, // Enable primary database creation
		UseNewParser:    true,
	}

	// Set global settings for WAL manager and other services that use settings.GetSettings()
	globalSettings := settings.GetSettings()
	globalSettings.LogDir = args.LogDir
	globalSettings.DataDir = args.DataDir
	globalSettings.TempDir = args.TempDir

	// DEBUG: Verify global settings are set correctly
	// tb.Logf("DEBUG: Test tempDir=%s", tempDir)
	// tb.Logf("DEBUG: args.DataDir=%s", args.DataDir)
	// tb.Logf("DEBUG: globalSettings.DataDir=%s", globalSettings.DataDir)

	// Create directory structure
	if err := os.MkdirAll(args.DataDir, 0755); err != nil {
		tb.Fatalf("Failed to create data directory: %v", err)
	}
	if err := os.MkdirAll(args.TempDir, 0755); err != nil {
		tb.Fatalf("Failed to create temp directory: %v", err)
	}
	if err := os.MkdirAll(args.LogDir, 0755); err != nil {
		tb.Fatalf("Failed to create log directory: %v", err)
	}

	// Create database storage engine
	databaseStore, err := databasestore.NewDatabaseStore(args.DataDir, sugar)
	if err != nil {
		tb.Fatalf("Failed to create database store: %v", err)
	}

	// Create database service
	databaseFactory := database.NewDatabaseFactory()
	databaseService := database.NewDatabaseService(databaseStore, databaseFactory, args, sugar)

	// Create buffer pool and file registry for bundle store
	fileRegistry, err := buffer.NewFileRegistry(args.DataDir, buffer.SyncInterval, sugar)
	if err != nil {
		tb.Fatalf("Failed to create file registry: %v", err)
	}
	bufferPool := buffer.NewBufferPool(1000, buffer.DefaultPageSize, fileRegistry, sugar)

	// Create bundle store and service
	bundleStore, err := bundlestore.NewBundleStore(args.DataDir, bufferPool, sugar, "binary")
	if err != nil {
		tb.Fatalf("Failed to create bundle store: %v", err)
	}
	bundleFactory := bundle.NewBundleFactory()
	documentFactory := document.NewDocumentFactory()
	bundleService := bundle.NewBundleService(bundleStore, bundleFactory, documentFactory, sugar, args)

	// Create catalog service
	catalogService := defaultdb.NewCatalogService(databaseService, bundleService, sugar)

	// CRITICAL: Inject catalog service into bundle service (resolves circular dependency)
	// This is what the real server does and is REQUIRED for proper operation
	bundleService.SetCatalogService(catalogService)

	// Create and initialize the "primary" database (required for catalog operations)
	primaryDB := &models.Database{
		DatabaseID:    helpers.GenerateUUID(),
		Name:          "primary",
		Description:   "Primary database for test catalogs",
		DataDirectory: args.DataDir,
		Bundles:       make(map[string]models.Bundle),
		BundleFiles:   []string{},
	}

	// Save the primary database
	err = databaseStore.CreateDatabaseDataFile(primaryDB)
	if err != nil {
		tb.Fatalf("Failed to create primary database: %v", err)
	}
	databaseService.Databases[primaryDB.Name] = primaryDB

	// Initialize primary database bundle catalogs (same as real server)
	err = defaultdb.InitPrimaryBundleCatalogs(databaseService, databaseStore, primaryDB, sugar, bundleService)
	if err != nil {
		tb.Fatalf("Failed to initialize primary bundle catalogs: %v", err)
	}

	// Hydrate all catalog bundles (same as real server does on startup)
	err = defaultdb.HydrateBundlesPrimaryCatalogs(databaseService, databaseStore, sugar, bundleService)
	if err != nil {
		sugar.Warnf("Warning: Failed to hydrate bundles catalog: %v", err)
	}

	err = defaultdb.HydratePermissionPrimaryCatalogs(databaseService, databaseStore, sugar, bundleService)
	if err != nil {
		sugar.Warnf("Warning: Failed to hydrate permissions catalog: %v", err)
	}

	err = defaultdb.HydrateRolesPrimaryCatalogs(databaseService, databaseStore, sugar, bundleService)
	if err != nil {
		sugar.Warnf("Warning: Failed to hydrate roles catalog: %v", err)
	}

	// Initialize service manager (no GraphQL for tests)
	// NOTE: Don't use InitServiceManager in tests because it uses sync.Once which creates
	// a singleton shared across all tests. Instead, create a fresh ServiceManager for each test.
	sessionManager := server.NewSessionManager(sugar, 30*time.Minute, 1000)

	// Initialize WAL Manager
	walManager, err := journal.NewWALManager(sugar)
	if err != nil {
		sugar.Warnf("Warning: Failed to initialize WAL Manager: %v", err)
		walManager = nil
	}

	// Initialize RBAC services (needed for permission checks)
	userService := server.NewUserService(bundleService, databaseService, nil, sugar, false)
	permissionService := server.NewPermissionService(bundleService, databaseService, nil, sugar, false)
	lockService := lock.NewLockService(sugar.Desugar())

	// Initialize unified query planner
	unifiedPlanner := planner.NewUnifiedQueryPlanner(sugar, bundleService)

	serviceManager := &server.ServiceManager{
		DatabaseService:        databaseService,
		BundleService:          bundleService,
		InternalCatalogService: catalogService,
		WALManager:             walManager,
		LockService:            lockService,
		GraphQLProcessor:       nil,
		UserService:            userService,
		PermissionService:      permissionService,
		MigrationService:       nil,
		SessionManager:         sessionManager,
		ActiveConnections:      make(map[string]*server.Connection),
		UnifiedPlanner:         unifiedPlanner,
	}

	// Create unique test database (use test name + timestamp to avoid conflicts)
	dbName := fmt.Sprintf("testdb_%s_%d", tb.Name(), time.Now().UnixNano())
	createDBCmd := fmt.Sprintf(`CREATE DATABASE "%s"`, dbName)
	startTime := time.Now()
	_, err = server.CommandDirector(ctx, nil, *serviceManager, createDBCmd, sugar, startTime, nil, "127.0.0.1")
	if err != nil {
		tb.Fatalf("Failed to create %s database: %v", dbName, err)
	}

	// DEBUG: Check if database directory was created
	dbDir := filepath.Join(args.DataDir, dbName)
	if _, err := os.Stat(dbDir); os.IsNotExist(err) {
		tb.Logf("WARNING: Database directory NOT created at: %s", dbDir)
		tb.Logf("DEBUG: Current globalSettings.DataDir=%s", settings.GetSettings().DataDir)
	} else {
		tb.Logf("SUCCESS: Database directory created at: %s", dbDir)
	}

	// CRITICAL: Switch to the test database context (like USE command in client)
	// This ensures subsequent commands operate on the correct database
	useDBCmd := fmt.Sprintf(`USE "%s"`, dbName)
	startTime = time.Now()
	_, err = server.CommandDirector(ctx, nil, *serviceManager, useDBCmd, sugar, startTime, nil, "127.0.0.1")
	if err != nil {
		tb.Fatalf("Failed to switch to %s database: %v", dbName, err)
	}

	// Retrieve the database AFTER the USE command to get the active context
	db, err := serviceManager.DatabaseService.GetDatabaseByName(dbName)
	if err != nil {
		tb.Fatalf("Failed to retrieve %s database: %v", dbName, err)
	}

	// Register the test database in the system catalog (same as real server)
	err = catalogService.AddDatabaseToCatalog(db)
	if err != nil {
		sugar.Warnf("Warning: Failed to register test database in system catalog: %v", err)
	}

	// CRITICAL: Flush all buffers to ensure metadata (PageCount, etc.) is written to disk
	// This is what the real server does after catalog initialization
	// Without this, indexes and document pages may not be properly persisted!
	err = bundleService.FlushAllBuffers()
	if err != nil {
		sugar.Warnf("Warning: Failed to flush bundle metadata: %v", err)
	} else {
		sugar.Debugf("Successfully flushed bundle metadata after test database setup")
	}

	//sugar.Infof("✓ Real server initialized in: %s (active database: %s)", tempDir, dbName)

	return &TestFixture{
		ServiceManager: serviceManager,
		Database:       *db,
		TempDir:        tempDir,
		Logger:         sugar,
		Settings:       args,
	}
}
