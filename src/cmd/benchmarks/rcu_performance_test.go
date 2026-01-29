package benchmarks

/*
RCU (Read-Copy-Update) PERFORMANCE BENCHMARK

This file benchmarks the performance of RCU-based lock-free writes vs traditional
lock-based writes. The RCU model eliminates bundle-wide write locks for UPDATE and
DELETE operations, allowing concurrent writes to proceed in parallel.

Expected Results:
- INSERT: Similar performance (already uses rotation-only lock)
- UPDATE: ~60x faster at high concurrency with RCU enabled
- DELETE: ~60x faster at high concurrency with RCU enabled

Usage:
  go test -bench=BenchmarkRCU -benchtime=5s -count=1 ./src/cmd/tests/ -v

To compare RCU vs Lock-based:
  go test -bench="Benchmark(RCU|Lock)_Update" -benchtime=3s ./src/cmd/tests/ -v
*/

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	defaultdb "syndrdb/src/internal/defaultDB"
	"syndrdb/src/internal/domain/bundle"
	"syndrdb/src/internal/domain/database"
	"syndrdb/src/internal/domain/document"
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
)

// RCUTestFixture holds server components for RCU benchmarking
type RCUTestFixture struct {
	ServiceManager *server.ServiceManager
	Database       *models.Database
	TempDir        string
	Logger         *zap.SugaredLogger
}

// ============================================================================
// CONCURRENT UPDATE BENCHMARKS
// ============================================================================

func BenchmarkRCU_Update_Concurrent_10(b *testing.B) {
	benchmarkConcurrentUpdatesRCU(b, 10, true)
}

func BenchmarkRCU_Update_Concurrent_50(b *testing.B) {
	benchmarkConcurrentUpdatesRCU(b, 50, true)
}

func BenchmarkRCU_Update_Concurrent_100(b *testing.B) {
	benchmarkConcurrentUpdatesRCU(b, 100, true)
}

func BenchmarkLock_Update_Concurrent_10(b *testing.B) {
	benchmarkConcurrentUpdatesRCU(b, 10, false)
}

func BenchmarkLock_Update_Concurrent_50(b *testing.B) {
	benchmarkConcurrentUpdatesRCU(b, 50, false)
}

func BenchmarkLock_Update_Concurrent_100(b *testing.B) {
	benchmarkConcurrentUpdatesRCU(b, 100, false)
}

// ============================================================================
// CONCURRENT DELETE BENCHMARKS
// ============================================================================

func BenchmarkRCU_Delete_Concurrent_10(b *testing.B) {
	benchmarkConcurrentDeletesRCU(b, 10, true)
}

func BenchmarkRCU_Delete_Concurrent_50(b *testing.B) {
	benchmarkConcurrentDeletesRCU(b, 50, true)
}

func BenchmarkLock_Delete_Concurrent_10(b *testing.B) {
	benchmarkConcurrentDeletesRCU(b, 10, false)
}

func BenchmarkLock_Delete_Concurrent_50(b *testing.B) {
	benchmarkConcurrentDeletesRCU(b, 50, false)
}

// ============================================================================
// CONCURRENT INSERT BENCHMARKS (Baseline)
// ============================================================================

func BenchmarkRCU_Insert_Concurrent_50(b *testing.B) {
	benchmarkConcurrentInsertsRCU(b, 50, true)
}

func BenchmarkLock_Insert_Concurrent_50(b *testing.B) {
	benchmarkConcurrentInsertsRCU(b, 50, false)
}

// ============================================================================
// SETUP HELPERS
// ============================================================================

func setupRCUBenchmarkFixture(tb testing.TB) *RCUTestFixture {
	tb.Helper()

	tempDir := tb.TempDir()

	// Setup logger (minimal output)
	loggerConfig := zap.NewDevelopmentConfig()
	loggerConfig.Level = zap.NewAtomicLevelAt(zap.WarnLevel)
	logger, err := loggerConfig.Build()
	if err != nil {
		tb.Fatalf("Failed to create logger: %v", err)
	}
	sugar := logger.Sugar()

	// Create settings
	args := settings.GetSettings()
	args.DataDir = filepath.Join(tempDir, "data")
	args.TempDir = filepath.Join(tempDir, "temp")
	args.LogDir = filepath.Join(tempDir, "logs")
	args.CreateDefaultDB = true
	args.UseNewParser = true

	// Create directories
	for _, dir := range []string{args.DataDir, args.TempDir, args.LogDir} {
		if err := os.MkdirAll(dir, 0755); err != nil {
			tb.Fatalf("Failed to create directory %s: %v", dir, err)
		}
	}

	// Initialize services
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

	// Create primary database
	primaryDB := &models.Database{
		DatabaseID:    helpers.GenerateUUID(),
		Name:          "primary",
		Description:   "Primary database",
		DataDirectory: args.DataDir,
		Bundles:       make(map[string]models.Bundle),
	}

	if err := databaseStore.CreateDatabaseDataFile(primaryDB); err != nil {
		tb.Fatalf("Failed to create primary database: %v", err)
	}
	databaseService.Databases[primaryDB.Name] = primaryDB

	if err := defaultdb.InitPrimaryBundleCatalogs(databaseService, databaseStore, primaryDB, sugar, bundleService); err != nil {
		tb.Fatalf("Failed to init primary catalogs: %v", err)
	}

	// Initialize services
	sessionManager := server.NewSessionManager(sugar, 30*time.Minute, 1000)
	walManager, _ := journal.NewWALManager(sugar)
	userService := server.NewUserService(bundleService, databaseService, nil, sugar, false)
	permissionService := server.NewPermissionService(bundleService, databaseService, nil, sugar, false)
	lockService := lock.NewLockService(sugar.Desugar())
	unifiedPlanner := planner.NewUnifiedQueryPlanner(sugar, bundleService)
	lockManager := storage.NewLockManager(sugar)
	conflictTracker := server.NewConflictTracker()

	serviceManager := &server.ServiceManager{
		DatabaseService:        databaseService,
		BundleService:          bundleService,
		InternalCatalogService: catalogService,
		WALManager:             walManager,
		LockService:            lockService,
		LockManager:            lockManager,
		UserService:            userService,
		PermissionService:      permissionService,
		SessionManager:         sessionManager,
		ActiveConnections:      make(map[string]*server.Connection),
		UnifiedPlanner:         unifiedPlanner,
		ConflictTracker:        conflictTracker,
	}

	sessionManager.SetLockReleaser(lockManager)

	// Create test database
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	dbName := fmt.Sprintf("rcutest_%d", time.Now().UnixNano())
	createDBCmd := fmt.Sprintf(`CREATE DATABASE "%s"`, dbName)
	if _, err := server.CommandDirector(ctx, nil, *serviceManager, createDBCmd, sugar, time.Now(), nil, "127.0.0.1"); err != nil {
		tb.Fatalf("Failed to create database: %v", err)
	}

	useDBCmd := fmt.Sprintf(`USE "%s"`, dbName)
	if _, err := server.CommandDirector(ctx, nil, *serviceManager, useDBCmd, sugar, time.Now(), nil, "127.0.0.1"); err != nil {
		tb.Fatalf("Failed to use database: %v", err)
	}

	db, err := serviceManager.DatabaseService.GetDatabaseByName(dbName)
	if err != nil {
		tb.Fatalf("Failed to get database: %v", err)
	}

	// Create test bundle
	createBundleCmd := `CREATE BUNDLE "TestBundle" WITH FIELDS (
		{"ID", "INT", true, false},
		{"Value", "STRING", false, false},
		{"Counter", "INT", false, false}
	);`
	if _, err := server.CommandDirector(ctx, db, *serviceManager, createBundleCmd, sugar, time.Now(), nil, "127.0.0.1"); err != nil {
		tb.Fatalf("Failed to create bundle: %v", err)
	}

	tb.Cleanup(func() {
		if bundleService != nil {
			bundleService.Shutdown()
		}
	})

	return &RCUTestFixture{
		ServiceManager: serviceManager,
		Database:       db,
		TempDir:        tempDir,
		Logger:         sugar,
	}
}

// ============================================================================
// BENCHMARK IMPLEMENTATIONS
// ============================================================================

func benchmarkConcurrentUpdatesRCU(b *testing.B, concurrency int, enableRCU bool) {
	fixture := setupRCUBenchmarkFixture(b)

	// Configure RCU mode
	args := settings.GetSettings()
	originalRCU := args.EnableRCUWrites
	args.EnableRCUWrites = enableRCU
	defer func() { args.EnableRCUWrites = originalRCU }()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// Pre-populate documents
	numDocs := concurrency * 5
	for i := 0; i < numDocs; i++ {
		query := fmt.Sprintf(`ADD DOCUMENT TO BUNDLE "TestBundle" WITH ({"ID"=%d}, {"Value"="initial"}, {"Counter"=0});`, i)
		if _, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, query, fixture.Logger, time.Now(), nil, "127.0.0.1"); err != nil {
			b.Fatalf("Setup failed: %v", err)
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		var wg sync.WaitGroup
		var errorCount int64

		for g := 0; g < concurrency; g++ {
			wg.Add(1)
			go func(workerID int) {
				defer wg.Done()
				targetID := workerID % numDocs
				query := fmt.Sprintf(`UPDATE DOCUMENTS IN BUNDLE "TestBundle" ({"Value"="updated_%d_%d"}) WHERE ID = %d;`,
					i, workerID, targetID)
				if _, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, query, fixture.Logger, time.Now(), nil, "127.0.0.1"); err != nil {
					atomic.AddInt64(&errorCount, 1)
				}
			}(g)
		}
		wg.Wait()
	}
}

func benchmarkConcurrentDeletesRCU(b *testing.B, concurrency int, enableRCU bool) {
	fixture := setupRCUBenchmarkFixture(b)

	args := settings.GetSettings()
	originalRCU := args.EnableRCUWrites
	args.EnableRCUWrites = enableRCU
	defer func() { args.EnableRCUWrites = originalRCU }()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	var docCounter int64

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// Insert documents for this iteration
		baseID := atomic.AddInt64(&docCounter, int64(concurrency))
		for j := 0; j < concurrency; j++ {
			docID := baseID + int64(j)
			query := fmt.Sprintf(`ADD DOCUMENT TO BUNDLE "TestBundle" WITH ({"ID"=%d}, {"Value"="todelete"});`, docID)
			if _, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, query, fixture.Logger, time.Now(), nil, "127.0.0.1"); err != nil {
				b.Logf("Insert error: %v", err)
			}
		}

		// Concurrent deletes
		var wg sync.WaitGroup
		for g := 0; g < concurrency; g++ {
			wg.Add(1)
			go func(workerID int) {
				defer wg.Done()
				docID := baseID + int64(workerID)
				query := fmt.Sprintf(`DELETE FROM "TestBundle" WHERE ID = %d;`, docID)
				server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, query, fixture.Logger, time.Now(), nil, "127.0.0.1")
			}(g)
		}
		wg.Wait()
	}
}

func benchmarkConcurrentInsertsRCU(b *testing.B, concurrency int, enableRCU bool) {
	fixture := setupRCUBenchmarkFixture(b)

	args := settings.GetSettings()
	originalRCU := args.EnableRCUWrites
	args.EnableRCUWrites = enableRCU
	defer func() { args.EnableRCUWrites = originalRCU }()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	var docCounter int64

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		var wg sync.WaitGroup

		for g := 0; g < concurrency; g++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				docID := atomic.AddInt64(&docCounter, 1)
				query := fmt.Sprintf(`ADD DOCUMENT TO BUNDLE "TestBundle" WITH ({"ID"=%d}, {"Value"="concurrent"});`, docID)
				server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, query, fixture.Logger, time.Now(), nil, "127.0.0.1")
			}()
		}
		wg.Wait()
	}
}

// ============================================================================
// LATENCY COMPARISON TEST
// ============================================================================

func TestRCULatencyComparison(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping in short mode")
	}

	fixture := setupRCUBenchmarkFixture(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	args := settings.GetSettings()
	concurrencyLevels := []int{1, 10, 25, 50}

	t.Log("============================================================")
	t.Log("RCU vs Lock-Based UPDATE Latency Comparison")
	t.Log("============================================================")

	for _, concurrency := range concurrencyLevels {
		// Prepare documents
		baseID := time.Now().UnixNano()
		for i := 0; i < concurrency; i++ {
			query := fmt.Sprintf(`ADD DOCUMENT TO BUNDLE "TestBundle" WITH ({"ID"=%d}, {"Value"="test"});`, baseID+int64(i))
			server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, query, fixture.Logger, time.Now(), nil, "127.0.0.1")
		}

		// Test RCU
		args.EnableRCUWrites = true
		rcuStart := time.Now()
		var wg sync.WaitGroup
		for i := 0; i < concurrency; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				query := fmt.Sprintf(`UPDATE DOCUMENTS IN BUNDLE "TestBundle" ({"Value"="rcu_%d"}) WHERE ID = %d;`, id, baseID+int64(id))
				server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, query, fixture.Logger, time.Now(), nil, "127.0.0.1")
			}(i)
		}
		wg.Wait()
		rcuLatency := time.Since(rcuStart)

		// Prepare more documents for lock test
		baseID2 := time.Now().UnixNano()
		for i := 0; i < concurrency; i++ {
			query := fmt.Sprintf(`ADD DOCUMENT TO BUNDLE "TestBundle" WITH ({"ID"=%d}, {"Value"="test"});`, baseID2+int64(i))
			server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, query, fixture.Logger, time.Now(), nil, "127.0.0.1")
		}

		// Test Lock-based
		args.EnableRCUWrites = false
		lockStart := time.Now()
		for i := 0; i < concurrency; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				query := fmt.Sprintf(`UPDATE DOCUMENTS IN BUNDLE "TestBundle" ({"Value"="lock_%d"}) WHERE ID = %d;`, id, baseID2+int64(id))
				server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, query, fixture.Logger, time.Now(), nil, "127.0.0.1")
			}(i)
		}
		wg.Wait()
		lockLatency := time.Since(lockStart)

		speedup := float64(lockLatency) / float64(rcuLatency)
		t.Logf("Concurrency %3d: RCU=%12v  Lock=%12v  Speedup=%.2fx",
			concurrency, rcuLatency, lockLatency, speedup)
	}

	// Restore default
	args.EnableRCUWrites = true
}
