package main

import (
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"syndrdb/src/internal/domain/document"
	"syndrdb/src/internal/domain/models"
	"syndrdb/src/internal/storage/bundlestore"
	"syndrdb/src/pkg/settings"
)

// TestMetadataPersistence_DirtyFlag tests that dirty flag is set correctly when metadata changes
func TestMetadataPersistence_DirtyFlag(t *testing.T) {
	// Setup test environment
	service, cleanup := setupTestService(t)
	defer cleanup()

	// Create a test bundle
	db, bundle := createTestBundleWithDB(t, service, "TestDB", "TestBundle")
	assert.False(t, bundle.IsDirty, "Bundle should not be dirty initially")

	// Add a document to trigger metadata update - use DocumentCommand
	docCommand := &models.DocumentCommand{
		CommandType: "ADD_DOCUMENT",
		BundleName:  bundle.Name,
		Fields: []models.KeyValue{
			{Key: "name", Value: "test"},
		},
	}

	_, err := service.AddDocumentToBundle(db, bundle, docCommand)
	require.NoError(t, err)

	// Flush to apply updates to memory
	service.flushMetadataUpdates()

	// Verify bundle is marked dirty
	assert.True(t, bundle.IsDirty, "Bundle should be marked dirty after document add")
	assert.Equal(t, int64(1), bundle.TotalDocuments, "TotalDocuments should be 1")
}

// TestMetadataPersistence_SingleBundleImmediatePersist tests that a single active bundle
// persists immediately on flush (efficiency trigger)
func TestMetadataPersistence_SingleBundleImmediatePersist(t *testing.T) {
	service, cleanup := setupTestService(t)
	defer cleanup()

	db, bundle := createTestBundleWithDB(t, service, "TestDB", "SingleBundle")

	// Add document and flush
	doc := &models.Document{
		ID:       "doc-1",
		BundleID: bundle.Name,
		Fields:   map[string]interface{}{"name": "test"},
	}

	err := service.AddDocumentToBundleByStruct(db, bundle, doc)
	require.NoError(t, err)

	// Flush metadata updates
	service.flushMetadataUpdates()

	// Bundle should be persisted and no longer dirty
	assert.False(t, bundle.IsDirty, "Bundle should not be dirty after persistence")

	// Verify persistence by reloading from disk
	reloaded := reloadBundleFromDisk(t, service, db.Name, bundle.Name)
	assert.Equal(t, int64(1), reloaded.TotalDocuments, "Persisted TotalDocuments should be 1")
	assert.Equal(t, int64(1), reloaded.PageCount, "Persisted PageCount should be 1")
}

// TestMetadataPersistence_MultiBundleBatchedPersist tests that multiple bundles
// batch persistence until operation threshold is reached
func TestMetadataPersistence_MultiBundleBatchedPersist(t *testing.T) {
	service, cleanup := setupTestService(t)
	defer cleanup()

	// Set low threshold for testing (10 operations)
	service.metadataPersistInterval = 10

	// Create multiple bundles
	db, bundle1 := createTestBundleWithDB(t, service, "TestDB", "Bundle1")
	_, bundle2 := createTestBundleWithDB(t, service, "TestDB", "Bundle2")
	_, bundle3 := createTestBundleWithDB(t, service, "TestDB", "Bundle3")

	// Add 3 documents to each bundle (9 total operations - below threshold)
	for i := 0; i < 3; i++ {
		for _, bundle := range []*models.Bundle{bundle1, bundle2, bundle3} {
			doc := &models.Document{
				ID:       generateDocID(bundle.Name, i),
				BundleID: bundle.Name,
				Fields:   map[string]interface{}{"value": i},
			}
			err := service.AddDocumentToBundleByStruct(db, bundle, doc)
			require.NoError(t, err)
		}
	}

	// Flush metadata - should mark dirty but not persist (below threshold)
	service.flushMetadataUpdates()

	// All bundles should still be dirty
	assert.True(t, bundle1.IsDirty, "Bundle1 should be dirty before threshold")
	assert.True(t, bundle2.IsDirty, "Bundle2 should be dirty before threshold")
	assert.True(t, bundle3.IsDirty, "Bundle3 should be dirty before threshold")

	// Add one more document to push over threshold (10 operations total)
	doc := &models.Document{
		ID:       generateDocID(bundle1.Name, 100),
		BundleID: bundle1.Name,
		Fields:   map[string]interface{}{"value": 100},
	}
	err := service.AddDocumentToBundleByStruct(db, bundle1, doc)
	require.NoError(t, err)

	// Flush metadata - should persist all dirty bundles (threshold reached)
	service.flushMetadataUpdates()

	// All bundles should now be clean
	assert.False(t, bundle1.IsDirty, "Bundle1 should be clean after threshold persistence")
	assert.False(t, bundle2.IsDirty, "Bundle2 should be clean after threshold persistence")
	assert.False(t, bundle3.IsDirty, "Bundle3 should be clean after threshold persistence")

	// Verify all bundles persisted correctly
	reloaded1 := reloadBundleFromDisk(t, service, db.Name, bundle1.Name)
	reloaded2 := reloadBundleFromDisk(t, service, db.Name, bundle2.Name)
	reloaded3 := reloadBundleFromDisk(t, service, db.Name, bundle3.Name)

	assert.Equal(t, int64(4), reloaded1.TotalDocuments, "Bundle1 should have 4 documents")
	assert.Equal(t, int64(3), reloaded2.TotalDocuments, "Bundle2 should have 3 documents")
	assert.Equal(t, int64(3), reloaded3.TotalDocuments, "Bundle3 should have 3 documents")
}

// TestMetadataPersistence_ConcurrentUpdates tests thread safety of metadata updates
// with concurrent document additions across multiple goroutines
func TestMetadataPersistence_ConcurrentUpdates(t *testing.T) {
	service, cleanup := setupTestService(t)
	defer cleanup()

	service.metadataPersistInterval = 100

	db, bundle := createTestBundleWithDB(t, service, "TestDB", "ConcurrentBundle")

	// Concurrent document additions
	const numGoroutines = 10
	const docsPerGoroutine = 20
	const totalDocs = numGoroutines * docsPerGoroutine

	var wg sync.WaitGroup
	errors := make(chan error, totalDocs)

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			for j := 0; j < docsPerGoroutine; j++ {
				doc := &models.Document{
					ID:       generateDocID(bundle.Name, goroutineID*docsPerGoroutine+j),
					BundleID: bundle.Name,
					Fields:   map[string]interface{}{"goroutine": goroutineID, "index": j},
				}
				if err := service.AddDocumentToBundleByStruct(db, bundle, doc); err != nil {
					errors <- err
				}
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	// Check for errors
	for err := range errors {
		require.NoError(t, err)
	}

	// Flush all pending updates
	service.flushMetadataUpdates()

	// Verify final count
	assert.Equal(t, int64(totalDocs), bundle.TotalDocuments,
		"TotalDocuments should match concurrent additions")

	// Force persistence and verify
	service.forceMetadataPersistence()
	reloaded := reloadBundleFromDisk(t, service, db.Name, bundle.Name)
	assert.Equal(t, int64(totalDocs), reloaded.TotalDocuments,
		"Persisted count should match concurrent additions")
}

// TestMetadataPersistence_ShutdownForcesPersist tests that shutdown persists all dirty bundles
func TestMetadataPersistence_ShutdownForcesPersist(t *testing.T) {
	service, cleanup := setupTestService(t)
	defer cleanup()

	// Set very high threshold so normal persistence won't trigger
	service.metadataPersistInterval = 10000

	// Create bundles and add documents
	db, bundle1 := createTestBundleWithDB(t, service, "TestDB", "ShutdownBundle1")
	_, bundle2 := createTestBundleWithDB(t, service, "TestDB", "ShutdownBundle2")

	for i := 0; i < 5; i++ {
		doc1 := &models.Document{
			ID:       generateDocID(bundle1.Name, i),
			BundleID: bundle1.Name,
			Fields:   map[string]interface{}{"value": i},
		}
		doc2 := &models.Document{
			ID:       generateDocID(bundle2.Name, i),
			BundleID: bundle2.Name,
			Fields:   map[string]interface{}{"value": i},
		}
		require.NoError(t, service.AddDocumentToBundleByStruct(db, bundle1, doc1))
		require.NoError(t, service.AddDocumentToBundleByStruct(db, bundle2, doc2))
	}

	// Flush to memory (but not to disk due to high threshold)
	service.flushMetadataUpdates()

	// Bundles should be dirty
	assert.True(t, bundle1.IsDirty, "Bundle1 should be dirty before shutdown")
	assert.True(t, bundle2.IsDirty, "Bundle2 should be dirty before shutdown")

	// Force persistence (simulating shutdown)
	service.forceMetadataPersistence()

	// Bundles should be clean
	assert.False(t, bundle1.IsDirty, "Bundle1 should be clean after shutdown")
	assert.False(t, bundle2.IsDirty, "Bundle2 should be clean after shutdown")

	// Verify persistence
	reloaded1 := reloadBundleFromDisk(t, service, db.Name, bundle1.Name)
	reloaded2 := reloadBundleFromDisk(t, service, db.Name, bundle2.Name)
	assert.Equal(t, int64(5), reloaded1.TotalDocuments, "Bundle1 should have 5 documents")
	assert.Equal(t, int64(5), reloaded2.TotalDocuments, "Bundle2 should have 5 documents")
}

// TestMetadataPersistence_PartialFailureRetry tests that failed persistence keeps dirty flag
// so next cycle can retry
func TestMetadataPersistence_PartialFailureRetry(t *testing.T) {
	// TODO: Implement test for partial persistence failure
	// This requires a mock store that can simulate failures
	// For now, this serves as documentation for future test implementation
	t.Skip("Requires mock store implementation for failure simulation")
}

// TestMetadataPersistence_PageCountCalculation tests correct page count calculation
func TestMetadataPersistence_PageCountCalculation(t *testing.T) {
	service, cleanup := setupTestService(t)
	defer cleanup()

	db, bundle := createTestBundleWithDB(t, service, "TestDB", "PageCountBundle")
	bundle.PageSize = 100 // Set page size to 100 for easier testing

	testCases := []struct {
		docsToAdd     int
		expectedPages int64
		expectedTotal int64
		description   string
	}{
		{50, 1, 50, "Partial page"},
		{50, 1, 100, "Exactly one page"},
		{1, 2, 101, "Just over one page"},
		{99, 3, 200, "Exactly two pages"},
		{1, 3, 201, "Just over two pages"},
	}

	totalDocs := 0
	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			// Add documents
			for i := 0; i < tc.docsToAdd; i++ {
				doc := &models.Document{
					ID:       generateDocID(bundle.Name, totalDocs+i),
					BundleID: bundle.Name,
					Fields:   map[string]interface{}{"value": totalDocs + i},
				}
				require.NoError(t, service.AddDocumentToBundleByStruct(db, bundle, doc))
			}
			totalDocs += tc.docsToAdd

			// Flush and verify
			service.flushMetadataUpdates()

			assert.Equal(t, tc.expectedTotal, bundle.TotalDocuments,
				"TotalDocuments mismatch for: %s", tc.description)
			assert.Equal(t, tc.expectedPages, bundle.PageCount,
				"PageCount mismatch for: %s", tc.description)
		})
	}

	// Force persist and verify
	service.forceMetadataPersistence()
	reloaded := reloadBundleFromDisk(t, service, db.Name, bundle.Name)
	assert.Equal(t, int64(201), reloaded.TotalDocuments)
	assert.Equal(t, int64(3), reloaded.PageCount)
}

// TestMetadataPersistence_NoUnnecessaryPersistence tests that clean bundles aren't persisted
func TestMetadataPersistence_NoUnnecessaryPersistence(t *testing.T) {
	service, cleanup := setupTestService(t)
	defer cleanup()

	db, bundle := createTestBundleWithDB(t, service, "TestDB", "CleanBundle")

	// Add document and persist
	doc := &models.Document{
		ID:       "doc-1",
		BundleID: bundle.Name,
		Fields:   map[string]interface{}{"name": "test"},
	}
	require.NoError(t, service.AddDocumentToBundleByStruct(db, bundle, doc))
	service.flushMetadataUpdates()

	// Bundle should be clean
	assert.False(t, bundle.IsDirty, "Bundle should be clean after flush")

	// Call flush again with no changes
	service.flushMetadataUpdates()

	// Bundle should still be clean (no unnecessary persistence)
	assert.False(t, bundle.IsDirty, "Bundle should remain clean with no changes")
}

// TestMetadataPersistence_GetAllDirtyBundles tests the helper function
func TestMetadataPersistence_GetAllDirtyBundles(t *testing.T) {
	service, cleanup := setupTestService(t)
	defer cleanup()

	// Initially no dirty bundles
	dirty := service.getAllDirtyBundles()
	assert.Empty(t, dirty, "Should have no dirty bundles initially")

	// Create and dirty some bundles
	_, bundle1 := createTestBundleWithDB(t, service, "TestDB", "DirtyBundle1")
	_, bundle2 := createTestBundleWithDB(t, service, "TestDB", "DirtyBundle2")
	_, bundle3 := createTestBundleWithDB(t, service, "TestDB", "CleanBundle3")

	bundle1.IsDirty = true
	bundle2.IsDirty = true
	bundle3.IsDirty = false

	// Get dirty bundles
	dirty = service.getAllDirtyBundles()
	assert.Len(t, dirty, 2, "Should have 2 dirty bundles")

	// Verify correct bundles returned
	dirtyNames := make(map[string]bool)
	for _, b := range dirty {
		dirtyNames[b.Name] = true
	}
	assert.True(t, dirtyNames[bundle1.Name], "DirtyBundle1 should be in results")
	assert.True(t, dirtyNames[bundle2.Name], "DirtyBundle2 should be in results")
	assert.False(t, dirtyNames[bundle3.Name], "CleanBundle3 should not be in results")
}

// TestMetadataPersistence_HighThroughputScenario simulates high-throughput write scenario
func TestMetadataPersistence_HighThroughputScenario(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping high-throughput test in short mode")
	}

	service, cleanup := setupTestService(t)
	defer cleanup()

	service.metadataPersistInterval = 500 // Moderate threshold

	db, bundle := createTestBundleWithDB(t, service, "TestDB", "HighThroughputBundle")

	// Simulate high-throughput writes
	const totalDocs = 2000
	startTime := time.Now()

	for i := 0; i < totalDocs; i++ {
		doc := &models.Document{
			ID:       generateDocID(bundle.Name, i),
			BundleID: bundle.Name,
			Fields:   map[string]interface{}{"value": i},
		}
		require.NoError(t, service.AddDocumentToBundleByStruct(db, bundle, doc))

		// Periodic flush to simulate batching
		if i%100 == 99 {
			service.flushMetadataUpdates()
		}
	}

	// Final flush and force persist
	service.flushMetadataUpdates()
	service.forceMetadataPersistence()

	duration := time.Since(startTime)
	t.Logf("Added %d documents in %v (%.0f docs/sec)", totalDocs, duration,
		float64(totalDocs)/duration.Seconds())

	// Verify final state
	reloaded := reloadBundleFromDisk(t, service, db.Name, bundle.Name)
	assert.Equal(t, int64(totalDocs), reloaded.TotalDocuments,
		"All documents should be persisted")
	assert.Equal(t, int64(2), reloaded.PageCount, "Should have 2 pages (1000 docs each)")
}

// =============================================================================
// Helper Functions
// =============================================================================

func setupTestService(t *testing.T) (*BundleService, func()) {
	// Create temporary directory for test
	tmpDir, err := os.MkdirTemp("", "metadata_persistence_test_*")
	require.NoError(t, err)

	// Setup test settings
	args := &settings.Arguments{
		DataFileRoot: tmpDir,
		LogFileRoot:  filepath.Join(tmpDir, "logs"),
	}

	// Create log directory
	os.MkdirAll(args.LogFileRoot, 0755)

	// Create logger
	logger, _ := zap.NewDevelopment()
	sugarLogger := logger.Sugar()

	// Create dependencies
	store := bundlestore.NewBundleStorageEngine(args, sugarLogger)
	factory := NewBundleFactory(store, sugarLogger, args)
	docFactory := document.NewDocumentFactory()

	// Create service with test settings
	service := NewBundleService(store, factory, docFactory, sugarLogger, args)

	// Override intervals for faster testing
	service.indexUpdateBatchSize = 10
	service.indexUpdateInterval = 50 * time.Millisecond
	service.metadataPersistInterval = 20 // Default for most tests

	cleanup := func() {
		service.Shutdown()
		os.RemoveAll(tmpDir)
	}

	return service, cleanup
}

func createTestBundleWithDB(t *testing.T, service *BundleService, dbName, bundleName string) (*models.Database, *models.Bundle) {
	// Create database if doesn't exist
	var db *models.Database
	if !service.factory.DatabaseExists(dbName) {
		db = &models.Database{
			Name:    dbName,
			Bundles: make(map[string]*models.Bundle),
		}
		service.factory.databases[dbName] = db
	} else {
		db, _ = service.factory.GetDatabase(dbName)
	}

	// Create bundle
	bundle := &models.Bundle{
		Name:           bundleName,
		Database:       dbName,
		TotalDocuments: 0,
		PageCount:      0,
		PageSize:       1000, // Default page size
		IsDirty:        false,
		Indexes:        make(map[string]models.IndexReference),
		IndexNames:     []string{},
		Relationships:  make(map[string]models.Relationship),
		Constraints:    make(map[string]models.Constraint),
	}

	db.Bundles[bundleName] = bundle
	service.bundleMetadata[bundleName] = bundle

	return db, bundle
}

func reloadBundleFromDisk(t *testing.T, service *BundleService, dbName, bundleName string) *models.Bundle {
	// Reload bundle from disk to verify persistence
	reloaded, err := service.store.LoadBundleFile(dbName, bundleName)
	require.NoError(t, err, "Failed to reload bundle from disk")
	return reloaded
}

func generateDocID(bundleName string, index int) string {
	// Generate unique document IDs
	return bundleName + "_doc_" + string(rune('a'+index%26)) + string(rune('0'+(index/26)%10)) + string(rune('0'+index/260))
}

// TestMetadataPersistence_SingleBundleImmediatePersist tests that a single active bundle
// persists immediately on flush (efficiency trigger)
func TestMetadataPersistence_SingleBundleImmediatePersist(t *testing.T) {
	service, cleanup := setupTestService(t)
	defer cleanup()

	bundle := createTestBundle(t, service, "TestDB", "SingleBundle")

	// Add document and flush
	doc := &models.Document{
		ID:       "doc-1",
		BundleID: bundle.Name,
		Fields:   map[string]interface{}{"name": "test"},
	}

	err := service.AddDocument("TestDB", bundle.Name, doc)
	require.NoError(t, err)

	// Flush metadata updates
	service.flushMetadataUpdates()

	// Bundle should be persisted and no longer dirty
	bundle, _ = service.factory.GetBundle(bundle.Name)
	assert.False(t, bundle.IsDirty, "Bundle should not be dirty after persistence")

	// Verify persistence by reloading from disk
	reloadedBundle := reloadBundleFromDisk(t, service, "TestDB", bundle.Name)
	assert.Equal(t, int64(1), reloadedBundle.TotalDocuments, "Persisted TotalDocuments should be 1")
	assert.Equal(t, int64(1), reloadedBundle.PageCount, "Persisted PageCount should be 1")
}

// TestMetadataPersistence_MultiBundleBatchedPersist tests that multiple bundles
// batch persistence until operation threshold is reached
func TestMetadataPersistence_MultiBundleBatchedPersist(t *testing.T) {
	service, cleanup := setupTestService(t)
	defer cleanup()

	// Set low threshold for testing (10 operations)
	service.metadataPersistInterval = 10

	// Create multiple bundles
	bundle1 := createTestBundle(t, service, "TestDB", "Bundle1")
	bundle2 := createTestBundle(t, service, "TestDB", "Bundle2")
	bundle3 := createTestBundle(t, service, "TestDB", "Bundle3")

	// Add 3 documents to each bundle (9 total operations - below threshold)
	for i := 0; i < 3; i++ {
		for _, bundleName := range []string{bundle1.Name, bundle2.Name, bundle3.Name} {
			doc := &models.Document{
				ID:       generateDocID(bundleName, i),
				BundleID: bundleName,
				Fields:   map[string]interface{}{"value": i},
			}
			err := service.AddDocument("TestDB", bundleName, doc)
			require.NoError(t, err)
		}
	}

	// Flush metadata - should mark dirty but not persist (below threshold)
	service.flushMetadataUpdates()

	// All bundles should still be dirty
	b1, _ := service.factory.GetBundle(bundle1.Name)
	b2, _ := service.factory.GetBundle(bundle2.Name)
	b3, _ := service.factory.GetBundle(bundle3.Name)
	assert.True(t, b1.IsDirty, "Bundle1 should be dirty before threshold")
	assert.True(t, b2.IsDirty, "Bundle2 should be dirty before threshold")
	assert.True(t, b3.IsDirty, "Bundle3 should be dirty before threshold")

	// Add one more document to push over threshold (10 operations total)
	doc := &models.Document{
		ID:       generateDocID(bundle1.Name, 100),
		BundleID: bundle1.Name,
		Fields:   map[string]interface{}{"value": 100},
	}
	err := service.AddDocument("TestDB", bundle1.Name, doc)
	require.NoError(t, err)

	// Flush metadata - should persist all dirty bundles (threshold reached)
	service.flushMetadataUpdates()

	// All bundles should now be clean
	b1, _ = service.factory.GetBundle(bundle1.Name)
	b2, _ = service.factory.GetBundle(bundle2.Name)
	b3, _ = service.factory.GetBundle(bundle3.Name)
	assert.False(t, b1.IsDirty, "Bundle1 should be clean after threshold persistence")
	assert.False(t, b2.IsDirty, "Bundle2 should be clean after threshold persistence")
	assert.False(t, b3.IsDirty, "Bundle3 should be clean after threshold persistence")

	// Verify all bundles persisted correctly
	reloaded1 := reloadBundleFromDisk(t, service, "TestDB", bundle1.Name)
	reloaded2 := reloadBundleFromDisk(t, service, "TestDB", bundle2.Name)
	reloaded3 := reloadBundleFromDisk(t, service, "TestDB", bundle3.Name)

	assert.Equal(t, int64(4), reloaded1.TotalDocuments, "Bundle1 should have 4 documents")
	assert.Equal(t, int64(3), reloaded2.TotalDocuments, "Bundle2 should have 3 documents")
	assert.Equal(t, int64(3), reloaded3.TotalDocuments, "Bundle3 should have 3 documents")
}

// TestMetadataPersistence_ConcurrentUpdates tests thread safety of metadata updates
// with concurrent document additions across multiple goroutines
func TestMetadataPersistence_ConcurrentUpdates(t *testing.T) {
	service, cleanup := setupTestService(t)
	defer cleanup()

	service.metadataPersistInterval = 100

	bundle := createTestBundle(t, service, "TestDB", "ConcurrentBundle")

	// Concurrent document additions
	const numGoroutines = 10
	const docsPerGoroutine = 20
	const totalDocs = numGoroutines * docsPerGoroutine

	var wg sync.WaitGroup
	errors := make(chan error, totalDocs)

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			for j := 0; j < docsPerGoroutine; j++ {
				doc := &models.Document{
					ID:       generateDocID(bundle.Name, goroutineID*docsPerGoroutine+j),
					BundleID: bundle.Name,
					Fields:   map[string]interface{}{"goroutine": goroutineID, "index": j},
				}
				if err := service.AddDocument("TestDB", bundle.Name, doc); err != nil {
					errors <- err
				}
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	// Check for errors
	for err := range errors {
		require.NoError(t, err)
	}

	// Flush all pending updates
	service.flushMetadataUpdates()

	// Verify final count
	bundle, _ = service.factory.GetBundle(bundle.Name)
	assert.Equal(t, int64(totalDocs), bundle.TotalDocuments,
		"TotalDocuments should match concurrent additions")

	// Force persistence and verify
	service.forceMetadataPersistence()
	reloaded := reloadBundleFromDisk(t, service, "TestDB", bundle.Name)
	assert.Equal(t, int64(totalDocs), reloaded.TotalDocuments,
		"Persisted count should match concurrent additions")
}

// TestMetadataPersistence_ShutdownForcesPersist tests that shutdown persists all dirty bundles
func TestMetadataPersistence_ShutdownForcesPersist(t *testing.T) {
	service, cleanup := setupTestService(t)
	defer cleanup()

	// Set very high threshold so normal persistence won't trigger
	service.metadataPersistInterval = 10000

	// Create bundles and add documents
	bundle1 := createTestBundle(t, service, "TestDB", "ShutdownBundle1")
	bundle2 := createTestBundle(t, service, "TestDB", "ShutdownBundle2")

	for i := 0; i < 5; i++ {
		doc1 := &models.Document{
			ID:       generateDocID(bundle1.Name, i),
			BundleID: bundle1.Name,
			Fields:   map[string]interface{}{"value": i},
		}
		doc2 := &models.Document{
			ID:       generateDocID(bundle2.Name, i),
			BundleID: bundle2.Name,
			Fields:   map[string]interface{}{"value": i},
		}
		require.NoError(t, service.AddDocument("TestDB", bundle1.Name, doc1))
		require.NoError(t, service.AddDocument("TestDB", bundle2.Name, doc2))
	}

	// Flush to memory (but not to disk due to high threshold)
	service.flushMetadataUpdates()

	// Bundles should be dirty
	b1, _ := service.factory.GetBundle(bundle1.Name)
	b2, _ := service.factory.GetBundle(bundle2.Name)
	assert.True(t, b1.IsDirty, "Bundle1 should be dirty before shutdown")
	assert.True(t, b2.IsDirty, "Bundle2 should be dirty before shutdown")

	// Force persistence (simulating shutdown)
	service.forceMetadataPersistence()

	// Bundles should be clean
	b1, _ = service.factory.GetBundle(bundle1.Name)
	b2, _ = service.factory.GetBundle(bundle2.Name)
	assert.False(t, b1.IsDirty, "Bundle1 should be clean after shutdown")
	assert.False(t, b2.IsDirty, "Bundle2 should be clean after shutdown")

	// Verify persistence
	reloaded1 := reloadBundleFromDisk(t, service, "TestDB", bundle1.Name)
	reloaded2 := reloadBundleFromDisk(t, service, "TestDB", bundle2.Name)
	assert.Equal(t, int64(5), reloaded1.TotalDocuments, "Bundle1 should have 5 documents")
	assert.Equal(t, int64(5), reloaded2.TotalDocuments, "Bundle2 should have 5 documents")
}

// TestMetadataPersistence_PartialFailureRetry tests that failed persistence keeps dirty flag
// so next cycle can retry
func TestMetadataPersistence_PartialFailureRetry(t *testing.T) {
	// TODO: Implement test for partial persistence failure
	// This requires a mock store that can simulate failures
	// For now, this serves as documentation for future test implementation
	t.Skip("Requires mock store implementation for failure simulation")
}

// TestMetadataPersistence_PageCountCalculation tests correct page count calculation
func TestMetadataPersistence_PageCountCalculation(t *testing.T) {
	service, cleanup := setupTestService(t)
	defer cleanup()

	bundle := createTestBundle(t, service, "TestDB", "PageCountBundle")
	bundle.PageSize = 100 // Set page size to 100 for easier testing

	testCases := []struct {
		docsToAdd     int
		expectedPages int64
		expectedTotal int64
		description   string
	}{
		{50, 1, 50, "Partial page"},
		{50, 1, 100, "Exactly one page"},
		{1, 2, 101, "Just over one page"},
		{99, 3, 200, "Exactly two pages"},
		{1, 3, 201, "Just over two pages"},
	}

	totalDocs := 0
	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			// Add documents
			for i := 0; i < tc.docsToAdd; i++ {
				doc := &models.Document{
					ID:       generateDocID(bundle.Name, totalDocs+i),
					BundleID: bundle.Name,
					Fields:   map[string]interface{}{"value": totalDocs + i},
				}
				require.NoError(t, service.AddDocument("TestDB", bundle.Name, doc))
			}
			totalDocs += tc.docsToAdd

			// Flush and verify
			service.flushMetadataUpdates()
			bundle, _ = service.factory.GetBundle(bundle.Name)

			assert.Equal(t, tc.expectedTotal, bundle.TotalDocuments,
				"TotalDocuments mismatch for: %s", tc.description)
			assert.Equal(t, tc.expectedPages, bundle.PageCount,
				"PageCount mismatch for: %s", tc.description)
		})
	}

	// Force persist and verify
	service.forceMetadataPersistence()
	reloaded := reloadBundleFromDisk(t, service, "TestDB", bundle.Name)
	assert.Equal(t, int64(201), reloaded.TotalDocuments)
	assert.Equal(t, int64(3), reloaded.PageCount)
}

// TestMetadataPersistence_NoUnnecessaryPersistence tests that clean bundles aren't persisted
func TestMetadataPersistence_NoUnnecessaryPersistence(t *testing.T) {
	service, cleanup := setupTestService(t)
	defer cleanup()

	bundle := createTestBundle(t, service, "TestDB", "CleanBundle")

	// Add document and persist
	doc := &models.Document{
		ID:       "doc-1",
		BundleID: bundle.Name,
		Fields:   map[string]interface{}{"name": "test"},
	}
	require.NoError(t, service.AddDocument("TestDB", bundle.Name, doc))
	service.flushMetadataUpdates()

	// Bundle should be clean
	bundle, _ = service.factory.GetBundle(bundle.Name)
	assert.False(t, bundle.IsDirty, "Bundle should be clean after flush")

	// Call flush again with no changes
	service.flushMetadataUpdates()

	// Bundle should still be clean (no unnecessary persistence)
	bundle, _ = service.factory.GetBundle(bundle.Name)
	assert.False(t, bundle.IsDirty, "Bundle should remain clean with no changes")
}

// TestMetadataPersistence_GetAllDirtyBundles tests the helper function
func TestMetadataPersistence_GetAllDirtyBundles(t *testing.T) {
	service, cleanup := setupTestService(t)
	defer cleanup()

	// Initially no dirty bundles
	dirty := service.getAllDirtyBundles()
	assert.Empty(t, dirty, "Should have no dirty bundles initially")

	// Create and dirty some bundles
	bundle1 := createTestBundle(t, service, "TestDB", "DirtyBundle1")
	bundle2 := createTestBundle(t, service, "TestDB", "DirtyBundle2")
	bundle3 := createTestBundle(t, service, "TestDB", "CleanBundle3")

	bundle1.IsDirty = true
	bundle2.IsDirty = true
	bundle3.IsDirty = false

	// Get dirty bundles
	dirty = service.getAllDirtyBundles()
	assert.Len(t, dirty, 2, "Should have 2 dirty bundles")

	// Verify correct bundles returned
	dirtyNames := make(map[string]bool)
	for _, b := range dirty {
		dirtyNames[b.Name] = true
	}
	assert.True(t, dirtyNames[bundle1.Name], "DirtyBundle1 should be in results")
	assert.True(t, dirtyNames[bundle2.Name], "DirtyBundle2 should be in results")
	assert.False(t, dirtyNames[bundle3.Name], "CleanBundle3 should not be in results")
}

// TestMetadataPersistence_HighThroughputScenario simulates high-throughput write scenario
func TestMetadataPersistence_HighThroughputScenario(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping high-throughput test in short mode")
	}

	service, cleanup := setupTestService(t)
	defer cleanup()

	service.metadataPersistInterval = 500 // Moderate threshold

	bundle := createTestBundle(t, service, "TestDB", "HighThroughputBundle")

	// Simulate high-throughput writes
	const totalDocs = 2000
	startTime := time.Now()

	for i := 0; i < totalDocs; i++ {
		doc := &models.Document{
			ID:       generateDocID(bundle.Name, i),
			BundleID: bundle.Name,
			Fields:   map[string]interface{}{"value": i},
		}
		require.NoError(t, service.AddDocument("TestDB", bundle.Name, doc))

		// Periodic flush to simulate batching
		if i%100 == 99 {
			service.flushMetadataUpdates()
		}
	}

	// Final flush and force persist
	service.flushMetadataUpdates()
	service.forceMetadataPersistence()

	duration := time.Since(startTime)
	t.Logf("Added %d documents in %v (%.0f docs/sec)", totalDocs, duration,
		float64(totalDocs)/duration.Seconds())

	// Verify final state
	reloaded := reloadBundleFromDisk(t, service, "TestDB", bundle.Name)
	assert.Equal(t, int64(totalDocs), reloaded.TotalDocuments,
		"All documents should be persisted")
	assert.Equal(t, int64(2), reloaded.PageCount, "Should have 2 pages (1000 docs each)")
}

// TestMetadataPersistence_IdleFlushBehavior tests that idle flush triggers persistence
func TestMetadataPersistence_IdleFlushBehavior(t *testing.T) {
	service, cleanup := setupTestService(t)
	defer cleanup()

	// Set intervals for testing
	service.indexUpdateInterval = 100 * time.Millisecond
	service.metadataPersistInterval = 10000 // High threshold to test idle trigger

	bundle := createTestBundle(t, service, "TestDB", "IdleBundle")

	// Add document
	doc := &models.Document{
		ID:       "doc-1",
		BundleID: bundle.Name,
		Fields:   map[string]interface{}{"name": "test"},
	}
	require.NoError(t, service.AddDocument("TestDB", bundle.Name, doc))

	// Wait for idle period (5x interval = 500ms)
	time.Sleep(600 * time.Millisecond)

	// Trigger schedule which should detect idle period
	service.scheduleMetadataUpdate(bundle.Name, "increment_docs", 0)

	// Verify flush occurred
	bundle, _ = service.factory.GetBundle(bundle.Name)
	assert.Equal(t, int64(1), bundle.TotalDocuments, "Metadata should be updated after idle flush")
}

// =============================================================================
// Helper Functions
// =============================================================================

func setupTestService(t *testing.T) (*BundleService, func()) {
	// Create temporary directory for test
	tmpDir, err := os.MkdirTemp("", "metadata_persistence_test_*")
	require.NoError(t, err)

	// Setup test settings
	args := &settings.Arguments{
		DataFileRoot: tmpDir,
		LogFileRoot:  tmpDir,
	}

	// Create logger
	logger, _ := zap.NewDevelopment()
	sugarLogger := logger.Sugar()

	// Create dependencies
	store := bundlestore.NewBundleStorageEngine(args, sugarLogger)
	factory := NewBundleFactory(store, sugarLogger, args)
	docFactory := document.NewDocumentFactory()

	// Create service with test settings
	service := NewBundleService(store, factory, docFactory, sugarLogger, args)

	// Override intervals for faster testing
	service.indexUpdateBatchSize = 10
	service.indexUpdateInterval = 50 * time.Millisecond
	service.metadataPersistInterval = 20 // Default for most tests

	cleanup := func() {
		service.Shutdown()
		os.RemoveAll(tmpDir)
	}

	return service, cleanup
}

func createTestBundle(t *testing.T, service *BundleService, dbName, bundleName string) *models.Bundle {
	// Create database if doesn't exist
	if !service.factory.DatabaseExists(dbName) {
		db := &models.Database{
			Name:    dbName,
			Bundles: make(map[string]*models.Bundle),
		}
		service.factory.databases[dbName] = db
	}

	// Create bundle
	bundle := &models.Bundle{
		Name:           bundleName,
		Database:       dbName,
		TotalDocuments: 0,
		PageCount:      0,
		PageSize:       1000, // Default page size
		IsDirty:        false,
		Indexes:        make(map[string]models.IndexReference),
		Relationships:  make(map[string]models.Relationship),
		Constraints:    make(map[string]models.Constraint),
	}

	db, _ := service.factory.GetDatabase(dbName)
	db.Bundles[bundleName] = bundle
	service.bundleMetadata[bundleName] = bundle

	return bundle
}

func reloadBundleFromDisk(t *testing.T, service *BundleService, dbName, bundleName string) *models.Bundle {
	// Reload bundle from disk to verify persistence
	reloaded, err := service.store.LoadBundleFile(dbName, bundleName)
	require.NoError(t, err, "Failed to reload bundle from disk")
	return reloaded
}

func generateDocID(bundleName string, index int) string {
	return bundleName + "_doc_" + string(rune('a'+index%26)) + string(rune('0'+index/26))
}
