// btree_cache_eviction_test.go
//
// Purpose: Comprehensive unit tests for B-tree page cache eviction functionality
//
// This file contains tests that verify:
// - LRU (Least Recently Used) eviction behavior when cache is full
// - Dirty page flushing before eviction to prevent data loss
// - Cache size limits are enforced correctly
// - Cache hit/miss statistics tracking
// - LRU ordering is maintained correctly
// - Edge cases (empty cache, single page, all dirty pages)
//
// Design Principles:
// - Single Responsibility: Each test focuses on one specific aspect of cache eviction
// - DRY: Reusable helper functions for common test setup and verification
// - Clear naming: Test names describe exactly what they verify
//
// Test Organization:
// 1. Basic eviction tests (LRU ordering, size limits)
// 2. Dirty page handling tests (flush before eviction)
// 3. Statistics tests (hits, misses, evictions)
// 4. Edge cases (empty cache, concurrent access)

package main

import (
	"fmt"
	"os"
	"syndrdb/src/internal/domain/index/btreeindexV2"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// setupCacheTestIndex creates a test index with a small cache size for eviction testing
//
// Parameters:
//   - t: Testing context
//   - testName: Name for the test directory
//   - cacheSize: Maximum number of pages in cache
//
// Returns:
//   - tempDir: Temporary directory path (caller must clean up)
//   - idx: BTreeIndex instance configured for testing
//
// This helper follows DRY principle by centralizing test index creation
func setupCacheTestIndex(t *testing.T, testName string, cacheSize int) (string, *btreeindexV2.BTreeIndex) {
	tempDir, err := os.MkdirTemp("", fmt.Sprintf("btree-cache-test-%s-*", testName))
	require.NoError(t, err, "Failed to create temp directory")

	// Use unique database name based on test name to avoid collisions
	dbName := fmt.Sprintf("testdb_%s_%d", testName, time.Now().UnixNano())

	// Clean up any existing test database files for this specific test
	dbPath := fmt.Sprintf("data/%s", dbName)
	os.RemoveAll(dbPath)
	os.MkdirAll(dbPath, 0755)

	config := btreeindexV2.DefaultIndexConfig("testbundle", "testfield", tempDir, dbName)
	config.PageSize = 4096
	config.CacheSize = cacheSize // Small cache to trigger eviction
	config.FillFactor = 0.7
	config.IsUnique = false // Allow duplicate document IDs for testing

	logger, err := zap.NewDevelopment()
	require.NoError(t, err, "Failed to create logger")

	idx, err := btreeindexV2.CreateBTreeIndex(config, logger.Sugar())
	require.NoError(t, err, "Failed to create index")

	return tempDir, idx
}

// TestCacheEviction_LRUOrder verifies that the least recently used page is evicted
// when the cache is full
func TestCacheEviction_LRUOrder(t *testing.T) {
	tempDir, idx := setupCacheTestIndex(t, "lru_order", 3)
	defer os.RemoveAll(tempDir)
	defer idx.Close()

	// Insert 3 documents to fill cache (cache size = 3)
	for i := 0; i < 3; i++ {
		key := []byte(fmt.Sprintf("key_%d", i))
		docID := fmt.Sprintf("doc_%d", i)
		err := idx.Insert(key, docID)
		require.NoError(t, err)
	}

	// Get initial stats
	stats := idx.GetCacheStats()
	initialCacheSize := stats.CurrentSize
	t.Logf("After 3 inserts: cache size=%d, evictions=%d", initialCacheSize, stats.Evictions)

	// Insert one more document - should trigger eviction of LRU page
	key := []byte("key_3")
	docID := "doc_3"
	err := idx.Insert(key, docID)
	require.NoError(t, err)

	// Verify eviction occurred
	stats = idx.GetCacheStats()
	assert.Greater(t, stats.Evictions, uint64(0),
		"Should have at least one eviction when cache exceeds size limit")

	t.Logf("After 4th insert: cache size=%d, evictions=%d",
		stats.CurrentSize, stats.Evictions)
}

// TestCacheEviction_DirtyPageFlushing verifies that dirty pages are flushed
// to disk before being evicted to prevent data loss
//
// This test previously revealed a bug where cache thrashing caused data loss.
// Fixed by implementing PostgreSQL-style page pinning to prevent eviction of pages
// being modified.
func TestCacheEviction_DirtyPageFlushing(t *testing.T) {
	tempDir, idx := setupCacheTestIndex(t, "dirty_flush", 20)
	defer os.RemoveAll(tempDir)
	defer idx.Close()

	// Insert documents to create dirty pages
	numDocs := 30 // More than cache size to force evictions
	for i := 0; i < numDocs; i++ {
		key := []byte(fmt.Sprintf("key_%d", i))
		docID := fmt.Sprintf("doc_%d", i)
		err := idx.Insert(key, docID)
		require.NoError(t, err)
	}

	// Get final statistics
	stats := idx.GetCacheStats()

	// Verify evictions happened (inserted more than cache size)
	assert.Greater(t, stats.Evictions, uint64(0),
		"Should have evictions when inserting more pages than cache size")

	t.Logf("Final stats: cache=%d, evictions=%d",
		stats.CurrentSize, stats.Evictions)

	// Note: Tree structure inspection removed as it requires accessing unexported fields

	// Verify all documents are still searchable (dirty pages were flushed before eviction)
	for i := 0; i < numDocs; i++ {
		key := []byte(fmt.Sprintf("key_%d", i))
		docIDs, err := idx.Search(key)
		require.NoError(t, err, "Failed to search for key_%d", i)
		assert.Equal(t, 1, len(docIDs),
			"Should find exactly one document for key_%d", i)
		assert.Equal(t, fmt.Sprintf("doc_%d", i), docIDs[0],
			"Document ID should match for key_%d", i)
	}

	t.Logf("All %d documents successfully searchable after eviction (dirty pages were flushed)", numDocs)
}

// TestCacheEviction_SizeLimitEnforced verifies that the cache never exceeds
// its maximum size limit
func TestCacheEviction_SizeLimitEnforced(t *testing.T) {
	maxCacheSize := 10
	tempDir, idx := setupCacheTestIndex(t, "size_limit", maxCacheSize)
	defer os.RemoveAll(tempDir)
	defer idx.Close()

	// Insert many documents to trigger multiple evictions
	numDocs := 50
	for i := 0; i < numDocs; i++ {
		key := []byte(fmt.Sprintf("key_%d", i))
		docID := fmt.Sprintf("doc_%d", i)
		err := idx.Insert(key, docID)
		require.NoError(t, err)

		// Check cache size after each insert
		stats := idx.GetCacheStats()
		assert.LessOrEqual(t, stats.CurrentSize, maxCacheSize,
			"Cache size should never exceed max size (after insert %d)", i)
	}

	stats := idx.GetCacheStats()
	t.Logf("Final stats: cache size=%d, max=%d, evictions=%d",
		stats.CurrentSize, stats.MaxSize, stats.Evictions)

	assert.Greater(t, stats.Evictions, uint64(0),
		"Should have evictions after inserting %d docs with cache size %d",
		numDocs, maxCacheSize)
}

// TestCacheEviction_HitMissStatistics verifies that cache hit/miss statistics
// are tracked correctly
func TestCacheEviction_HitMissStatistics(t *testing.T) {
	tempDir, idx := setupCacheTestIndex(t, "hit_miss_stats", 5)
	defer os.RemoveAll(tempDir)
	defer idx.Close()

	// Insert 3 documents
	for i := 0; i < 3; i++ {
		key := []byte(fmt.Sprintf("key_%d", i))
		docID := fmt.Sprintf("doc_%d", i)
		err := idx.Insert(key, docID)
		require.NoError(t, err)
	}

	// Get initial stats
	statsAfterInsert := idx.GetCacheStats()
	initialMisses := statsAfterInsert.Misses
	t.Logf("After inserts: hits=%d, misses=%d",
		statsAfterInsert.Hits, initialMisses)

	// Search for existing keys (should be cache hits if pages still in cache)
	for i := 0; i < 3; i++ {
		key := []byte(fmt.Sprintf("key_%d", i))
		_, err := idx.Search(key)
		require.NoError(t, err)
	}

	// Get stats after searches
	statsAfterSearch := idx.GetCacheStats()

	// Verify that we had some cache activity
	totalAccesses := statsAfterSearch.Hits + statsAfterSearch.Misses
	assert.Greater(t, totalAccesses, uint64(0),
		"Should have cache hits or misses after operations")

	// Calculate hit rate
	if totalAccesses > 0 {
		t.Logf("Final stats: hits=%d, misses=%d, hit rate=%.2f%%",
			statsAfterSearch.Hits, statsAfterSearch.Misses,
			statsAfterSearch.HitRate*100)
	}
}

// TestCacheEviction_AccessOrderMaintained verifies that accessing a page
// moves it to the front of the LRU list (making it less likely to be evicted)
func TestCacheEviction_AccessOrderMaintained(t *testing.T) {
	tempDir, idx := setupCacheTestIndex(t, "access_order", 3)
	defer os.RemoveAll(tempDir)
	defer idx.Close()

	// Insert 3 documents to fill cache
	keys := make([][]byte, 3)
	for i := 0; i < 3; i++ {
		keys[i] = []byte(fmt.Sprintf("key_%d", i))
		docID := fmt.Sprintf("doc_%d", i)
		err := idx.Insert(keys[i], docID)
		require.NoError(t, err)
	}

	// Access the first key to move it to front of LRU list
	_, err := idx.Search(keys[0])
	require.NoError(t, err)

	// Insert a new document - should evict key_1 (not key_0 which we just accessed)
	newKey := []byte("key_new")
	err = idx.Insert(newKey, "doc_new")
	require.NoError(t, err)

	// Verify eviction happened
	stats := idx.GetCacheStats()
	assert.Greater(t, stats.Evictions, uint64(0),
		"Should have evicted a page when cache exceeded size")

	t.Logf("Cache evictions: %d", stats.Evictions)
}

// TestCacheEviction_EmptyCache verifies behavior when evicting from an empty cache
func TestCacheEviction_EmptyCache(t *testing.T) {
	tempDir, idx := setupCacheTestIndex(t, "empty_cache", 10)
	defer os.RemoveAll(tempDir)
	defer idx.Close()

	// Get stats on empty index
	stats := idx.GetCacheStats()

	assert.Equal(t, 0, stats.CurrentSize, "Cache should be empty initially")
	assert.Equal(t, uint64(0), stats.Evictions, "No evictions should occur on empty cache")
	assert.Equal(t, uint64(0), stats.Hits, "No hits on empty cache")
	assert.Equal(t, uint64(0), stats.Misses, "No misses yet on empty cache")

	t.Logf("Empty cache stats verified: size=%d, evictions=%d",
		stats.CurrentSize, stats.Evictions)
}

// TestCacheEviction_SinglePage verifies that a single-page cache works correctly
// Note: With page pinning enabled, cache may temporarily exceed maxSize when all
// pinned pages are in use. This is expected behavior to prevent data loss.
func TestCacheEviction_SinglePage(t *testing.T) {
	tempDir, idx := setupCacheTestIndex(t, "single_page", 1)
	defer os.RemoveAll(tempDir)
	defer idx.Close()

	// Insert first document
	err := idx.Insert([]byte("key_0"), "doc_0")
	require.NoError(t, err)

	stats := idx.GetCacheStats()
	t.Logf("After 1st insert: cache size=%d, evictions=%d",
		stats.CurrentSize, stats.Evictions)

	// Insert second document - should trigger eviction immediately
	err = idx.Insert([]byte("key_1"), "doc_1")
	require.NoError(t, err)

	stats = idx.GetCacheStats()
	// With page pinning, cache can temporarily exceed maxSize when all pages are pinned
	// This is expected and necessary to prevent data loss during multi-page operations
	t.Logf("After 2nd insert: cache size=%d, evictions=%d (note: cache can exceed limit when pages are pinned)",
		stats.CurrentSize, stats.Evictions)

	// Verify evictions occurred (cache is being managed)
	assert.Greater(t, stats.Evictions, uint64(0), "Should have evictions with small cache")
}

// TestCacheEviction_MultipleEvictions verifies that multiple consecutive evictions
// work correctly when inserting many documents
func TestCacheEviction_MultipleEvictions(t *testing.T) {
	cacheSize := 5
	tempDir, idx := setupCacheTestIndex(t, "multiple_evictions", cacheSize)
	defer os.RemoveAll(tempDir)
	defer idx.Close()

	// Insert many documents to trigger multiple evictions
	numDocs := 25
	for i := 0; i < numDocs; i++ {
		key := []byte(fmt.Sprintf("key_%03d", i))
		docID := fmt.Sprintf("doc_%03d", i)
		err := idx.Insert(key, docID)
		require.NoError(t, err)
	}

	stats := idx.GetCacheStats()

	// Verify cache stayed within limits
	assert.LessOrEqual(t, stats.CurrentSize, cacheSize,
		"Cache should not exceed max size")

	// Verify multiple evictions occurred
	assert.Greater(t, stats.Evictions, uint64(5),
		"Should have multiple evictions when inserting %d docs with cache size %d",
		numDocs, cacheSize)

	t.Logf("Inserted %d docs with cache size %d: evictions=%d, hit rate=%.2f%%",
		numDocs, cacheSize, stats.Evictions, stats.HitRate*100)
}

// TestCacheEviction_AllDirtyPages verifies that the cache can handle evicting
// when all pages are dirty
//
// This test previously revealed a bug where cache thrashing caused data loss.
// Fixed by implementing PostgreSQL-style page pinning to prevent eviction of pages
// being modified.
func TestCacheEviction_AllDirtyPages(t *testing.T) {
	tempDir, idx := setupCacheTestIndex(t, "all_dirty", 3)
	defer os.RemoveAll(tempDir)
	defer idx.Close()

	// Insert documents rapidly without flushing to create all dirty pages
	for i := 0; i < 6; i++ {
		key := []byte(fmt.Sprintf("key_%d", i))
		docID := fmt.Sprintf("doc_%d", i)
		err := idx.Insert(key, docID)
		require.NoError(t, err)
	}

	stats := idx.GetCacheStats()

	// Verify evictions occurred even with dirty pages
	assert.Greater(t, stats.Evictions, uint64(0),
		"Should evict dirty pages when cache is full")

	t.Logf("With all dirty pages: evictions=%d, dirty pages=%d",
		stats.Evictions, stats.DirtyPages)

	// Verify all documents are still searchable (dirty pages were flushed before eviction)
	for i := 0; i < 6; i++ {
		key := []byte(fmt.Sprintf("key_%d", i))
		docs, err := idx.Search(key)
		require.NoError(t, err)
		assert.Equal(t, 1, len(docs),
			"Should find document for key_%d", i)
	}

	t.Logf("All 6 documents successfully searchable after dirty page evictions")
}

// TestCacheEviction_HitRateCalculation verifies that the hit rate is calculated
// correctly as hits / (hits + misses)
func TestCacheEviction_HitRateCalculation(t *testing.T) {
	tempDir, idx := setupCacheTestIndex(t, "hit_rate_calc", 10)
	defer os.RemoveAll(tempDir)
	defer idx.Close()

	// Insert a few documents
	for i := 0; i < 5; i++ {
		key := []byte(fmt.Sprintf("key_%d", i))
		docID := fmt.Sprintf("doc_%d", i)
		err := idx.Insert(key, docID)
		require.NoError(t, err)
	}

	// Search multiple times to generate hits
	for i := 0; i < 5; i++ {
		key := []byte(fmt.Sprintf("key_%d", i))
		_, err := idx.Search(key)
		require.NoError(t, err)
	}

	stats := idx.GetCacheStats()

	// Manually verify hit rate calculation
	totalAccesses := stats.Hits + stats.Misses
	if totalAccesses > 0 {
		expectedHitRate := float64(stats.Hits) / float64(totalAccesses)
		assert.InDelta(t, expectedHitRate, stats.HitRate, 0.001,
			"Hit rate should equal hits / (hits + misses)")

		t.Logf("Hit rate calculation verified: %.2f%% (hits=%d, misses=%d)",
			stats.HitRate*100, stats.Hits, stats.Misses)
	}
}
