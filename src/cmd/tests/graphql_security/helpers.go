package graphql_security

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"go.uber.org/zap"

	defaultdb "syndrdb/src/internal/defaultDB"
	"syndrdb/src/internal/domain/bundle"
	"syndrdb/src/internal/domain/database"
	"syndrdb/src/internal/domain/document"
	"syndrdb/src/internal/domain/models"
	graphql "syndrdb/src/internal/graphQL"
	"syndrdb/src/internal/graphQL/schema"
	"syndrdb/src/internal/server"
	"syndrdb/src/internal/storage/buffer"
	"syndrdb/src/internal/storage/bundlestore"
	"syndrdb/src/internal/storage/databasestore"
	"syndrdb/src/pkg/common/helpers"
	"syndrdb/src/pkg/settings"
)

// TestEnvironment holds all components needed for GraphQL security testing
type TestEnvironment struct {
	handler        *graphql.GraphQLHandler
	database       *models.Database
	serviceManager *server.ServiceManager
	tempDir        string
	logger         *zap.SugaredLogger
	cleanup        func()
}

// PartialTestEnvironment holds components before GraphQL handler creation
type PartialTestEnvironment struct {
	database       *models.Database
	serviceManager *server.ServiceManager
	schemaManager  *schema.SchemaManager
	tempDir        string
	logger         *zap.SugaredLogger
	config         SecurityTestConfig
	cleanup        func()
}

// SecurityTestConfig configures which security layers to enable for testing
type SecurityTestConfig struct {
	EnableComplexityLimit bool
	EnableRateLimit       bool
	EnableQueryTimeout    bool
	EnableQueryMonitor    bool
}

// setupTestEnvironment creates a complete test environment with all security layers enabled
func setupTestEnvironment(t *testing.T, enableSecurity bool) *TestEnvironment {
	config := SecurityTestConfig{
		EnableComplexityLimit: enableSecurity,
		EnableRateLimit:       enableSecurity,
		EnableQueryTimeout:    enableSecurity,
		EnableQueryMonitor:    enableSecurity,
	}
	return setupTestEnvironmentWithConfig(t, config)
}

// setupTestEnvironmentWithConfig creates test environment with custom security configuration
func setupTestEnvironmentWithConfig(t *testing.T, config SecurityTestConfig) *TestEnvironment {
	t.Helper()

	// Create temporary directory
	tempDir := t.TempDir()
	t.Logf("GraphQL Security Test Directory: %s", tempDir)

	// Setup logger
	loggerConfig := zap.NewDevelopmentConfig()
	loggerConfig.Level = zap.NewAtomicLevelAt(zap.InfoLevel)
	logger, err := loggerConfig.Build()
	if err != nil {
		t.Fatalf("Failed to create logger: %v", err)
	}
	sugar := logger.Sugar()

	// Create settings
	args := &settings.Arguments{
		DataDir:         filepath.Join(tempDir, "data_files"),
		TempDir:         filepath.Join(tempDir, "temp_files"),
		LogDir:          filepath.Join(tempDir, "log_files"),
		LogLevel:        "warn",
		CreateDefaultDB: true,
		UseNewParser:    true,
	}

	// Set global settings
	globalSettings := settings.GetSettings()
	globalSettings.LogDir = args.LogDir
	globalSettings.DataDir = args.DataDir
	globalSettings.TempDir = args.TempDir

	// Create directory structure
	if err := os.MkdirAll(args.DataDir, 0755); err != nil {
		t.Fatalf("Failed to create data directory: %v", err)
	}
	if err := os.MkdirAll(args.TempDir, 0755); err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	if err := os.MkdirAll(args.LogDir, 0755); err != nil {
		t.Fatalf("Failed to create log directory: %v", err)
	}

	// Create database storage engine
	databaseStore, err := databasestore.NewDatabaseStore(args.DataDir, sugar)
	if err != nil {
		t.Fatalf("Failed to create database store: %v", err)
	}

	// Create database service
	databaseFactory := database.NewDatabaseFactory()
	databaseService := database.NewDatabaseService(databaseStore, databaseFactory, args, sugar)

	// Create buffer pool and file registry for bundle store
	fileRegistry, err := buffer.NewFileRegistry(args.DataDir, buffer.SyncInterval, sugar)
	if err != nil {
		t.Fatalf("Failed to create file registry: %v", err)
	}
	bufferPool := buffer.NewBufferPool(1000, buffer.DefaultPageSize, fileRegistry, sugar)

	// Create bundle store and service
	bundleStore, err := bundlestore.NewBundleStore(args.DataDir, bufferPool, sugar, "json")
	if err != nil {
		t.Fatalf("Failed to create bundle store: %v", err)
	}
	bundleFactory := bundle.NewBundleFactory()
	documentFactory := document.NewDocumentFactory()
	bundleService := bundle.NewBundleService(bundleStore, bundleFactory, documentFactory, sugar, args)

	// Create catalog service
	catalogService := defaultdb.NewCatalogService(databaseService, bundleService, sugar)

	// Inject catalog service into bundle service (resolves circular dependency)
	bundleService.SetCatalogService(catalogService)

	// Create and initialize the "primary" database
	primaryDB := &models.Database{
		DatabaseID:    helpers.GenerateUUID(),
		Name:          "primary",
		Description:   "Primary database for security test catalogs",
		DataDirectory: args.DataDir,
		Bundles:       make(map[string]models.Bundle),
		BundleFiles:   []string{},
	}

	// Save the primary database
	err = databaseStore.CreateDatabaseDataFile(primaryDB)
	if err != nil {
		t.Fatalf("Failed to create primary database: %v", err)
	}
	databaseService.Databases[primaryDB.Name] = primaryDB

	// Initialize primary database bundle catalogs
	err = defaultdb.InitPrimaryBundleCatalogs(databaseService, databaseStore, primaryDB, sugar, bundleService)
	if err != nil {
		t.Fatalf("Failed to initialize primary bundle catalogs: %v", err)
	}

	// Hydrate all catalog bundles
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

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Initialize service manager
	serviceManager := server.InitServiceManager(databaseService, bundleService, catalogService, nil, nil, sugar, false)
	if serviceManager == nil {
		t.Fatal("Failed to initialize service manager")
	}

	// Create test database with unique name per test
	dbName := "graphql_security_testdb_" + strings.ReplaceAll(t.Name(), "/", "_")
	createDBCmd := fmt.Sprintf(`CREATE DATABASE "%s"`, dbName)
	startTime := time.Now()
	_, err = server.CommandDirector(ctx, nil, *serviceManager, createDBCmd, sugar, startTime, nil, "127.0.0.1")
	if err != nil {
		t.Fatalf("Failed to create test database: %v", err)
	}

	// Switch to the test database
	useDBCmd := fmt.Sprintf(`USE "%s"`, dbName)
	startTime = time.Now()
	_, err = server.CommandDirector(ctx, nil, *serviceManager, useDBCmd, sugar, startTime, nil, "127.0.0.1")
	if err != nil {
		t.Fatalf("Failed to switch to test database: %v", err)
	}

	// Get the active database
	testDB := databaseService.Databases[dbName]
	if testDB == nil {
		t.Fatalf("Test database %s not found after creation", dbName)
	}

	// Return full environment with handler
	return finalizeTestEnvironment(t, testDB, serviceManager, nil, sugar, tempDir, config)
}

// finalizeTestEnvironment creates the GraphQL handler after bundles are set up
func finalizeTestEnvironment(t *testing.T, testDB *models.Database, serviceManager *server.ServiceManager, schemaManager *schema.SchemaManager, sugar *zap.SugaredLogger, tempDir string, config SecurityTestConfig) *TestEnvironment {
	t.Helper()

	// Create schema manager if not provided (for legacy single-phase setup)
	var shouldCloseSchemaManager bool
	if schemaManager == nil {
		dbDirPath := filepath.Join(tempDir, "data_files", testDB.Name)
		schemaPath := filepath.Join(dbDirPath, fmt.Sprintf("%s_graphql.gql", testDB.Name))
		var err error
		schemaManager, err = schema.NewSchemaManager(schemaPath, testDB.Name, testDB.DatabaseID)
		if err != nil {
			t.Fatalf("Failed to create schema manager: %v", err)
		}
		shouldCloseSchemaManager = true
	}

	// Build GraphQL security configuration
	securityConfig := settings.GraphQLSecurityConfig{
		EnableComplexityLimit:          config.EnableComplexityLimit,
		EnableDepthLimit:               config.EnableComplexityLimit,
		AdminComplexityLimit:           1000,
		AuthenticatedComplexityLimit:   200,
		AnonymousComplexityLimit:       50,
		DepthLimit:                     20,
		EnableGraphQLRateLimit:         config.EnableRateLimit,
		AdminQueryRateLimit:            0, // Unlimited
		AuthenticatedQueryRateLimit:    100,
		AnonymousQueryRateLimit:        20,
		AdminMutationRateLimit:         0, // Unlimited
		AuthenticatedMutationRateLimit: 10,
		AnonymousMutationRateLimit:     2,
		RateAlgorithm:                  "token-bucket",
		InactivityTimeout:              5 * time.Minute,
		EnableQueryTimeout:             config.EnableQueryTimeout,
		AdminTimeout:                   10 * time.Minute,
		AuthenticatedQueryTimeout:      30 * time.Second,
		AuthenticatedMutationTimeout:   60 * time.Second,
		AnonymousQueryTimeout:          5 * time.Second,
		AnonymousMutationTimeout:       10 * time.Second,
		EnableQueryMonitoring:          config.EnableQueryMonitor,
		MaxMemoryMB:                    250,
		MetricsPurgeInterval:           5 * time.Minute,
		MetricsRetentionDuration:       30 * time.Minute,
	}

	// Create GraphQL handler with security configuration and schema manager
	handler, err := graphql.NewGraphQLHandler(
		*serviceManager,
		testDB,
		schemaManager,
		sugar,
		&securityConfig,
	)
	if err != nil {
		t.Fatalf("Failed to create GraphQL handler: %v", err)
	}

	// Cleanup function - only close schema manager if we created it here
	cleanup := func() {
		if shouldCloseSchemaManager && schemaManager != nil {
			schemaManager.Close()
		}
		// Logger cleanup handled by test framework
	}

	return &TestEnvironment{
		handler:        handler,
		database:       testDB,
		serviceManager: serviceManager,
		tempDir:        tempDir,
		logger:         sugar,
		cleanup:        cleanup,
	}
}

// setupPartialTestEnvironment creates environment without GraphQL handler
// This allows bundles to be created before handler initialization for proper schema generation
func setupPartialTestEnvironment(t *testing.T, enableSecurity bool) *PartialTestEnvironment {
	config := SecurityTestConfig{
		EnableComplexityLimit: enableSecurity,
		EnableRateLimit:       enableSecurity,
		EnableQueryTimeout:    enableSecurity,
		EnableQueryMonitor:    enableSecurity,
	}
	return setupPartialTestEnvironmentWithConfig(t, config)
}

// setupPartialTestEnvironmentWithConfig creates partial environment with custom config
func setupPartialTestEnvironmentWithConfig(t *testing.T, config SecurityTestConfig) *PartialTestEnvironment {
	t.Helper()

	// Create temporary directory
	tempDir := t.TempDir()
	t.Logf("GraphQL Security Test Directory: %s", tempDir)

	// Setup logger
	loggerConfig := zap.NewDevelopmentConfig()
	loggerConfig.Level = zap.NewAtomicLevelAt(zap.InfoLevel)
	logger, err := loggerConfig.Build()
	if err != nil {
		t.Fatalf("Failed to create logger: %v", err)
	}
	sugar := logger.Sugar()

	// Create settings
	args := &settings.Arguments{
		DataDir:         filepath.Join(tempDir, "data_files"),
		TempDir:         filepath.Join(tempDir, "temp_files"),
		LogDir:          filepath.Join(tempDir, "log_files"),
		LogLevel:        "warn",
		CreateDefaultDB: true,
		UseNewParser:    true,
	}

	// Set global settings
	globalSettings := settings.GetSettings()
	globalSettings.LogDir = args.LogDir
	globalSettings.DataDir = args.DataDir
	globalSettings.TempDir = args.TempDir

	// Create directory structure
	if err := os.MkdirAll(args.DataDir, 0755); err != nil {
		t.Fatalf("Failed to create data directory: %v", err)
	}
	if err := os.MkdirAll(args.TempDir, 0755); err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	if err := os.MkdirAll(args.LogDir, 0755); err != nil {
		t.Fatalf("Failed to create log directory: %v", err)
	}

	// Create database storage engine
	databaseStore, err := databasestore.NewDatabaseStore(args.DataDir, sugar)
	if err != nil {
		t.Fatalf("Failed to create database store: %v", err)
	}

	// Create database service
	databaseFactory := database.NewDatabaseFactory()
	databaseService := database.NewDatabaseService(databaseStore, databaseFactory, args, sugar)

	// Create buffer pool and file registry for bundle store
	fileRegistry, err := buffer.NewFileRegistry(args.DataDir, buffer.SyncInterval, sugar)
	if err != nil {
		t.Fatalf("Failed to create file registry: %v", err)
	}
	bufferPool := buffer.NewBufferPool(1000, buffer.DefaultPageSize, fileRegistry, sugar)

	// Create bundle store and service
	bundleStore, err := bundlestore.NewBundleStore(args.DataDir, bufferPool, sugar, "json")
	if err != nil {
		t.Fatalf("Failed to create bundle store: %v", err)
	}

	bundleFactory := bundle.NewBundleFactory()
	documentFactory := document.NewDocumentFactory()
	bundleService := bundle.NewBundleService(bundleStore, bundleFactory, documentFactory, sugar, args)

	// Create catalog service
	catalogService := defaultdb.NewCatalogService(databaseService, bundleService, sugar)

	// Inject catalog service into bundle service (resolves circular dependency)
	bundleService.SetCatalogService(catalogService)

	// Create and initialize the "primary" database
	primaryDB := &models.Database{
		DatabaseID:    helpers.GenerateUUID(),
		Name:          "primary",
		Description:   "Primary database for security test catalogs",
		DataDirectory: args.DataDir,
		Bundles:       make(map[string]models.Bundle),
		BundleFiles:   []string{},
	}

	// Save the primary database
	err = databaseStore.CreateDatabaseDataFile(primaryDB)
	if err != nil {
		t.Fatalf("Failed to create primary database: %v", err)
	}
	databaseService.Databases[primaryDB.Name] = primaryDB

	// Initialize primary database bundle catalogs
	err = defaultdb.InitPrimaryBundleCatalogs(databaseService, databaseStore, primaryDB, sugar, bundleService)
	if err != nil {
		t.Fatalf("Failed to initialize primary bundle catalogs: %v", err)
	}

	// Hydrate all catalog bundles
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

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Initialize service manager
	serviceManager := server.InitServiceManager(databaseService, bundleService, catalogService, nil, nil, sugar, false)
	if serviceManager == nil {
		t.Fatal("Failed to initialize service manager")
	}

	// Create test database with unique name per test
	dbName := "graphql_security_testdb_" + strings.ReplaceAll(t.Name(), "/", "_")
	createDBCmd := fmt.Sprintf(`CREATE DATABASE "%s"`, dbName)
	startTime := time.Now()
	_, err = server.CommandDirector(ctx, nil, *serviceManager, createDBCmd, sugar, startTime, nil, "127.0.0.1")
	if err != nil {
		t.Fatalf("Failed to create test database: %v", err)
	}

	// Switch to the test database
	useDBCmd := fmt.Sprintf(`USE "%s"`, dbName)
	startTime = time.Now()
	_, err = server.CommandDirector(ctx, nil, *serviceManager, useDBCmd, sugar, startTime, nil, "127.0.0.1")
	if err != nil {
		t.Fatalf("Failed to switch to test database: %v", err)
	}

	// Get the active database
	testDB := databaseService.Databases[dbName]
	if testDB == nil {
		t.Fatalf("Test database %s not found after creation", dbName)
	}

	// Create schema manager for the test database
	dbDirPath := filepath.Join(args.DataDir, dbName)
	schemaPath := filepath.Join(dbDirPath, fmt.Sprintf("%s_graphql.gql", dbName))
	schemaManager, err := schema.NewSchemaManager(schemaPath, dbName, testDB.DatabaseID)
	if err != nil {
		t.Fatalf("Failed to create schema manager: %v", err)
	}

	// Cleanup function
	cleanup := func() {
		if schemaManager != nil {
			schemaManager.Close()
		}
		// Logger cleanup handled by test framework
	}

	return &PartialTestEnvironment{
		database:       testDB,
		serviceManager: serviceManager,
		schemaManager:  schemaManager,
		tempDir:        tempDir,
		logger:         sugar,
		config:         config,
		cleanup:        cleanup,
	}
}

// FinalizeEnvironment creates the GraphQL handler from a partial environment
func (p *PartialTestEnvironment) FinalizeEnvironment(t *testing.T) *TestEnvironment {
	t.Helper()
	return finalizeTestEnvironment(t, p.database, p.serviceManager, p.schemaManager, p.logger, p.tempDir, p.config)
}

// registerBundleSchema registers a bundle's GraphQL schema with the schema manager
// This is needed in tests because CommandDirector doesn't have access to test schema managers
func registerBundleSchema(schemaManager *schema.SchemaManager, bundle *models.Bundle, logger *zap.SugaredLogger) error {
	// Generate GraphQL schema definition from bundle structure
	generator := schema.NewSchemaGenerator()
	schemaDef, err := generator.GenerateSchema(bundle)
	if err != nil {
		return fmt.Errorf("failed to generate schema for bundle '%s': %w", bundle.Name, err)
	}

	// Create unique IDs for the schema
	schemaID := helpers.GenerateUUID()
	var schemaIDBytes, bundleIDBytes [16]byte
	copy(schemaIDBytes[:], []byte(schemaID))
	copy(bundleIDBytes[:], []byte(bundle.BundleID))

	// Register the schema with version 1
	err = schemaManager.AddNewSchema(schemaIDBytes, bundleIDBytes, bundle.Name, schemaDef)
	if err != nil {
		return fmt.Errorf("failed to store schema for bundle '%s': %w", bundle.Name, err)
	}

	logger.Infof("✓ Registered GraphQL schema for bundle '%s' (version 1, %d fields)", bundle.Name, len(schemaDef.Fields))
	return nil
}

// createGraphQLRequest creates a GraphQL request with security context
func createGraphQLRequest(query, role, username string, isAdmin bool) graphql.GraphQLRequest {
	req := graphql.GraphQLRequest{
		Query:     query,
		Variables: make(map[string]interface{}),
		UserRole:  role,
		IsAdmin:   isAdmin,
		ClientIP:  "127.0.0.1",
	}

	if username != "" {
		req.Username = username
		req.UserID = "user-" + username
		req.SessionID = "session-" + username
	}

	return req
}
