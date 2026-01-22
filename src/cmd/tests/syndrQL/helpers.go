package syndrQL

/*
TEST HELPERS - SHARED TEST INFRASTRUCTURE

This file exports test helper functions and types for use in other test packages.
These provide common test setup functionality that can be reused across different test suites.

PHASE 6: MVCC - Export SetupFullServer and TestFixture for MVCC tests
*/

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	defaultdb "syndrdb/src/internal/defaultDB"
	"syndrdb/src/internal/domain/bundle"
	"syndrdb/src/internal/domain/database"
	"syndrdb/src/internal/domain/document"
	"syndrdb/src/internal/domain/migration"
	"syndrdb/src/internal/domain/models"
	"syndrdb/src/internal/journal"
	"syndrdb/src/internal/lock"
	"syndrdb/src/internal/query/planner"
	"syndrdb/src/internal/server"
	"syndrdb/src/internal/storage"
	"syndrdb/src/internal/storage/buffer"
	"syndrdb/src/internal/storage/bundlestore"
	"syndrdb/src/internal/storage/databasestore"
	"syndrdb/src/pkg/common/helpers"
	"syndrdb/src/pkg/settings"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// TestFixture holds real server components for E2E testing
// PHASE 6: MVCC - Exported for use in MVCC test package
type TestFixture struct {
	ServiceManager    *server.ServiceManager
	Database          *models.Database
	TempDir           string
	Logger            *zap.SugaredLogger
	Settings          *settings.Arguments
	IsSetup           bool
	AuthorDocumentIDs []string
}

// filteredWriter wraps an io.Writer and filters out lines containing specific text
type filteredWriter struct {
	underlying io.Writer
	filter     string
}

func (fw *filteredWriter) Write(p []byte) (n int, err error) {
	// If the message contains the filter text, don't write it
	if strings.Contains(string(p), fw.filter) {
		return len(p), nil // Pretend we wrote it
	}
	return fw.underlying.Write(p)
}

// SetupFullServer is the exported version for use in other test packages
// PHASE 6: MVCC - Export setup function for MVCC tests
func SetupFullServer(t *testing.T) *TestFixture {
	t.Helper()
	return setupFullServerTB(t)
}

// setupFullServerTB is the testing.TB version for benchmarks
func setupFullServerTB(tb testing.TB) *TestFixture {
	tb.Helper()

	// Create temporary directory
	tempDir := tb.TempDir()
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

	databaseStore, err := databasestore.NewDatabaseStore(args.DataDir, sugar)
	if err != nil {
		tb.Fatalf("Failed to create database store: %v", err)
	}

	databaseFactory := database.NewDatabaseFactory()
	databaseService := database.NewDatabaseService(databaseStore, databaseFactory, args, sugar)

	fileRegistry, err := buffer.NewFileRegistry(args.DataDir, buffer.SyncInterval, sugar)
	if err != nil {
		tb.Fatalf("Failed to create file registry: %v", err)
	}
	bufferPool := buffer.NewBufferPool(1000, buffer.DefaultPageSize, fileRegistry, sugar)

	bundleStore, err := bundlestore.NewBundleStore(args.DataDir, bufferPool, sugar, "binary")
	if err != nil {
		tb.Fatalf("Failed to create bundle store: %v", err)
	}
	bundleFactory := bundle.NewBundleFactory()
	documentFactory := document.NewDocumentFactory()
	bundleService := bundle.NewBundleService(bundleStore, bundleFactory, documentFactory, sugar, args)

	catalogService := defaultdb.NewCatalogService(databaseService, bundleService, sugar)

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

	// Initialize transaction lock manager
	lockManager := storage.NewLockManager(sugar)

	// Initialize Migration service (same as production)
	migrationConfig := migration.LoadConfigFromSettings(globalSettings)
	bundleServiceAdapter := server.NewBundleServiceAdapter(bundleService, databaseService, catalogService, walManager, sugar)
	migrationServiceCore := migration.NewMigrationService(bundleServiceAdapter, migrationConfig, sugar.Desugar())
	migrationService := server.NewMigrationServiceAdapter(migrationServiceCore, sugar)

	// PHASE 3: MVCC - Initialize conflict tracker
	conflictTracker := server.NewConflictTracker()

	serviceManager := &server.ServiceManager{
		DatabaseService:        databaseService,
		BundleService:          bundleService,
		InternalCatalogService: catalogService,
		WALManager:             walManager,
		LockService:            lockService,
		LockManager:            lockManager,
		GraphQLProcessor:       nil,
		UserService:            userService,
		PermissionService:      permissionService,
		MigrationService:       migrationService,
		SessionManager:         sessionManager,
		ActiveConnections:      make(map[string]*server.Connection),
		UnifiedPlanner:         unifiedPlanner,
		ConflictTracker:        conflictTracker,
	}

	// Create unique test database (use test name + timestamp to avoid conflicts)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	dbName := fmt.Sprintf("testdb_%s_%d", tb.Name(), time.Now().UnixNano())
	createDBCmd := fmt.Sprintf(`CREATE DATABASE "%s"`, dbName)
	startTime := time.Now()
	_, err = server.CommandDirector(ctx, nil, *serviceManager, createDBCmd, sugar, startTime, nil, "127.0.0.1")
	if err != nil {
		tb.Fatalf("Failed to create %s database: %v", dbName, err)
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

	return &TestFixture{
		ServiceManager: serviceManager,
		Database:       db,
		TempDir:        tempDir,
		Logger:         sugar,
		Settings:       args,
		IsSetup:        true,
	}
}
