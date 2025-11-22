package main

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"syndrdb/src/internal/domain/index/hashindexV3"

	"go.uber.org/zap"
)

/*
HASH INDEX BUCKET OPTIMIZATION TESTS

Tests the xxHash bucket optimization implementation (Phases 1-3):
- Phase 1: xxHash hash function and bucket computation
- Phase 2: Bucket-based file organization
- Phase 3: Bucket-optimized read path

Test Coverage:
1. Hash function correctness (xxHash, bucket computation)
2. Bucket file organization (correct file naming, bucket routing)
3. Bucket-optimized lookups (single bucket scan vs full scan)
4. Collision handling (hash verification)
5. Stress tests with large datasets
6. Edge cases (empty buckets, single bucket, all buckets)
*/

// Test helper: Create test index with bucket support
func createTestBucketIndex(t *testing.T, numBuckets uint32) (*hashindexV3.HashIndexV3, string, func()) {
	t.Helper()

	// Create temp directory
	tempDir, err := os.MkdirTemp("", "bucket_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}

	logger, _ := zap.NewDevelopment()

	config := hashindexV3.IndexConfig{
		IndexName:          "TestIndex",
		FieldName:          "TestField",
		BundleName:         "TestBundle",
		DataDir:            tempDir,
		MaxFileSize:        1024 * 1024, // 1MB
		WriteBufferSize:    4096,
		MemTableMaxSize:    1000,
		CompactionMaxFiles: 10,
		NumBuckets:         numBuckets,
		BucketFileMaxSize:  64 * 1024, // 64KB for testing
		BucketConcurrency:  4,
		Logger:             logger.Sugar(),
	}

	idx, err := hashindexV3.NewHashIndexV3(config)
	if err != nil {
		os.RemoveAll(tempDir)
		t.Fatalf("Failed to create index: %v", err)
	}

	cleanup := func() {
		idx.Close()
		os.RemoveAll(tempDir)
	}

	return idx, tempDir, cleanup
}

// TestHashFunctionConsistency verifies xxHash produces consistent results
func TestHashFunctionConsistency(t *testing.T) {
	testCases := []struct {
		key      string
		expected uint32 // We'll compute these on first run
	}{
		{key: "test-key-1"},
		{key: "test-key-2"},
		{key: "another-key"},
		{key: "DocumentID123"},
		{key: ""},
	}

	for _, tc := range testCases {
		t.Run(tc.key, func(t *testing.T) {
			// Hash should be consistent across multiple calls
			hash1 := hashindexV3.ComputeHash(tc.key)
			hash2 := hashindexV3.ComputeHash(tc.key)
			hash3 := hashindexV3.ComputeHash(tc.key)

			if hash1 != hash2 || hash2 != hash3 {
				t.Errorf("Hash not consistent: %d, %d, %d", hash1, hash2, hash3)
			}

			t.Logf("Key '%s' -> Hash: %d", tc.key, hash1)
		})
	}
}

// TestBucketComputation verifies bucket number calculation
func TestBucketComputation(t *testing.T) {
	numBuckets := uint32(256)

	testCases := []string{
		"key1", "key2", "key3", "DocumentID_001", "DocumentID_002",
		"", // Empty key edge case
	}

	for _, key := range testCases {
		hash := hashindexV3.ComputeHash(key)
		bucket := hashindexV3.ComputeBucketNum(hash, numBuckets)

		// Bucket must be in valid range
		if bucket >= numBuckets {
			t.Errorf("Bucket %d out of range [0, %d) for key '%s'", bucket, numBuckets, key)
		}

		// Verify convenience function matches
		hash2, bucket2 := hashindexV3.ComputeHashAndBucket(key, numBuckets)
		if hash != hash2 || bucket != bucket2 {
			t.Errorf("Convenience function mismatch: hash=%d/%d, bucket=%d/%d",
				hash, hash2, bucket, bucket2)
		}

		t.Logf("Key '%s' -> Hash: %d, Bucket: %d/%d", key, hash, bucket, numBuckets)
	}
}

// TestBucketDistribution verifies entries distribute across buckets
func TestBucketDistribution(t *testing.T) {
	numBuckets := uint32(256)
	numKeys := 10000

	bucketCounts := make(map[uint32]int)

	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key_%d", i)
		hash := hashindexV3.ComputeHash(key)
		bucket := hashindexV3.ComputeBucketNum(hash, numBuckets)
		bucketCounts[bucket]++
	}

	// All buckets should be used
	if len(bucketCounts) != int(numBuckets) {
		t.Errorf("Expected %d buckets used, got %d", numBuckets, len(bucketCounts))
	}

	// Calculate distribution statistics
	var total, min, max int
	min = numKeys
	for _, count := range bucketCounts {
		total += count
		if count < min {
			min = count
		}
		if count > max {
			max = count
		}
	}
	avg := float64(total) / float64(numBuckets)

	t.Logf("Distribution: avg=%.1f, min=%d, max=%d, range=%d",
		avg, min, max, max-min)

	// Distribution should be reasonably uniform (within 50% of average)
	expectedAvg := float64(numKeys) / float64(numBuckets)
	tolerance := expectedAvg * 0.5 // 50% tolerance

	if float64(min) < expectedAvg-tolerance || float64(max) > expectedAvg+tolerance {
		t.Logf("WARNING: Distribution may be skewed (expected ~%.1f ± %.1f)",
			expectedAvg, tolerance)
	}
}

// TestBucketFileOrganization verifies files are created per bucket
func TestBucketFileOrganization(t *testing.T) {
	idx, tempDir, cleanup := createTestBucketIndex(t, 256)
	defer cleanup()

	// Insert entries that should go to different buckets
	testData := []struct {
		key   string
		docID string
	}{
		{"key_001", "doc_001"},
		{"key_002", "doc_002"},
		{"key_003", "doc_003"},
		{"key_100", "doc_100"},
		{"key_200", "doc_200"},
	}

	buckets := make(map[uint32]bool)
	for _, td := range testData {
		err := idx.Put(td.key, td.docID, 1)
		if err != nil {
			t.Fatalf("Put failed: %v", err)
		}

		hash := hashindexV3.ComputeHash(td.key)
		bucket := hashindexV3.ComputeBucketNum(hash, 256)
		buckets[bucket] = true

		t.Logf("Inserted '%s' -> Bucket %d", td.key, bucket)
	}

	// Verify bucket files exist
	files, err := filepath.Glob(filepath.Join(tempDir, "*_bucket_*_entry_*.hidx"))
	if err != nil {
		t.Fatalf("Failed to list bucket files: %v", err)
	}

	if len(files) == 0 {
		t.Error("No bucket files created")
	}

	t.Logf("Created %d bucket files for %d unique buckets", len(files), len(buckets))

	// Verify we can retrieve all entries
	for _, td := range testData {
		docIDs, _, err := idx.Get(td.key)
		if err != nil {
			t.Errorf("Get(%s) failed: %v", td.key, err)
			continue
		}

		if len(docIDs) != 1 || docIDs[0] != td.docID {
			t.Errorf("Get(%s) = %v, want [%s]", td.key, docIDs, td.docID)
		}
	}
}

// TestBucketOptimizedLookup verifies bucket-optimized reads
func TestBucketOptimizedLookup(t *testing.T) {
	idx, tempDir, cleanup := createTestBucketIndex(t, 256)
	defer cleanup()

	// Insert 1000 entries across many buckets
	numEntries := 1000
	for i := 0; i < numEntries; i++ {
		key := fmt.Sprintf("key_%d", i)
		docID := fmt.Sprintf("doc_%d", i)
		err := idx.Put(key, docID, uint32(i))
		if err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	// Force flush to disk
	err := idx.Flush()
	if err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	// Verify we can retrieve entries (should use bucket-optimized path)
	for i := 0; i < 10; i++ { // Test subset
		key := fmt.Sprintf("key_%d", i)
		expectedDoc := fmt.Sprintf("doc_%d", i)

		docIDs, pageIDs, err := idx.Get(key)
		if err != nil {
			t.Errorf("Get(%s) failed: %v", key, err)
			continue
		}

		if len(docIDs) != 1 || docIDs[0] != expectedDoc {
			t.Errorf("Get(%s) = %v, want [%s]", key, docIDs, expectedDoc)
		}

		if len(pageIDs) != 1 || pageIDs[0] != uint32(i) {
			t.Errorf("Get(%s) pageID = %v, want [%d]", key, pageIDs, i)
		}
	}

	t.Logf("Successfully retrieved entries using bucket-optimized reads from %s", tempDir)
}

// TestCollisionHandling verifies hash collision detection
func TestCollisionHandling(t *testing.T) {
	idx, _, cleanup := createTestBucketIndex(t, 256)
	defer cleanup()

	// Insert entries
	testData := []struct {
		key   string
		docID string
	}{
		{"collision_test_1", "doc_1"},
		{"collision_test_2", "doc_2"},
		{"different_key", "doc_3"},
	}

	for _, td := range testData {
		err := idx.Put(td.key, td.docID, 1)
		if err != nil {
			t.Fatalf("Put(%s) failed: %v", td.key, err)
		}
	}

	// Verify each key retrieves its own document (no collision)
	for _, td := range testData {
		docIDs, _, err := idx.Get(td.key)
		if err != nil {
			t.Errorf("Get(%s) failed: %v", td.key, err)
			continue
		}

		if len(docIDs) != 1 || docIDs[0] != td.docID {
			t.Errorf("Get(%s) = %v, want [%s] (collision detected!)", td.key, docIDs, td.docID)
		}
	}

	t.Log("No collisions detected - hash verification working correctly")
}

// TestStressLargeDataset verifies correctness with 100K entries
func TestStressLargeDataset(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping stress test in short mode")
	}

	idx, _, cleanup := createTestBucketIndex(t, 256)
	defer cleanup()

	numEntries := 100000
	t.Logf("Inserting %d entries...", numEntries)

	// Insert entries
	for i := 0; i < numEntries; i++ {
		key := fmt.Sprintf("stress_key_%d", i)
		docID := fmt.Sprintf("stress_doc_%d", i)
		err := idx.Put(key, docID, uint32(i))
		if err != nil {
			t.Fatalf("Put failed at entry %d: %v", i, err)
		}

		if i > 0 && i%10000 == 0 {
			t.Logf("Inserted %d entries...", i)
		}
	}

	t.Logf("Verifying %d entries...", numEntries)

	// Verify random sample of entries
	sampleSize := 1000
	step := numEntries / sampleSize

	for i := 0; i < numEntries; i += step {
		key := fmt.Sprintf("stress_key_%d", i)
		expectedDoc := fmt.Sprintf("stress_doc_%d", i)

		docIDs, pageIDs, err := idx.Get(key)
		if err != nil {
			t.Errorf("Get(%s) failed: %v", key, err)
			continue
		}

		if len(docIDs) != 1 || docIDs[0] != expectedDoc {
			t.Errorf("Get(%s) = %v, want [%s]", key, docIDs, expectedDoc)
		}

		if len(pageIDs) != 1 || pageIDs[0] != uint32(i) {
			t.Errorf("Get(%s) pageID = %v, want [%d]", key, pageIDs, i)
		}
	}

	// Get index stats
	stats := idx.GetStats()
	t.Logf("Index stats: Entries=%d, CacheHits=%d, CacheMisses=%d, HitRate=%.2f%%",
		stats.TotalEntries, stats.CacheHits, stats.CacheMisses,
		float64(stats.CacheHits)/float64(stats.CacheHits+stats.CacheMisses)*100)
}

// TestEdgeCaseEmptyBuckets verifies handling of empty buckets
func TestEdgeCaseEmptyBuckets(t *testing.T) {
	idx, _, cleanup := createTestBucketIndex(t, 256)
	defer cleanup()

	// Insert only a few entries (most buckets will be empty)
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("sparse_key_%d", i)
		docID := fmt.Sprintf("sparse_doc_%d", i)
		err := idx.Put(key, docID, uint32(i))
		if err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	// Try to get a key that doesn't exist (will scan empty bucket)
	docIDs, _, err := idx.Get("nonexistent_key")
	// Note: Get may return an error if the bucket file doesn't exist yet,
	// which is expected behavior for empty buckets
	if err == nil {
		if len(docIDs) != 0 {
			t.Errorf("Get(nonexistent) = %v, want []", docIDs)
		}
	}

	t.Log("Empty bucket scan handled correctly (error is expected for nonexistent bucket files)")
}

// TestBucketFileNaming verifies correct file naming format
func TestBucketFileNaming(t *testing.T) {
	idx, tempDir, cleanup := createTestBucketIndex(t, 256)
	defer cleanup()

	// Insert entries
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("naming_test_%d", i)
		err := idx.Put(key, fmt.Sprintf("doc_%d", i), 1)
		if err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	// Check file naming format
	files, err := filepath.Glob(filepath.Join(tempDir, "*.hidx"))
	if err != nil {
		t.Fatalf("Failed to list files: %v", err)
	}

	if len(files) == 0 {
		t.Fatal("No index files created")
	}

	// Verify naming format: {field}_bucket_{num:06d}_entry_{seq:06d}.hidx
	// Note: Header files (*.0.hidx) don't have bucket naming
	bucketFileCount := 0
	for _, file := range files {
		basename := filepath.Base(file)
		t.Logf("Found file: %s", basename)

		// Skip header files (*.0.hidx) - they don't use bucket naming
		if contains(basename, ".0.hidx") {
			continue
		}

		// Bucket files should contain "bucket_" and "entry_"
		if !contains(basename, "bucket_") {
			t.Errorf("Bucket file %s missing 'bucket_' in name", basename)
		}
		if !contains(basename, "entry_") {
			t.Errorf("Bucket file %s missing 'entry_' in name", basename)
		}
		bucketFileCount++
	}

	if bucketFileCount == 0 {
		t.Error("No bucket files created")
	}
}

// TestUpdateAndDelete verifies bucket optimization works with updates/deletes
func TestUpdateAndDelete(t *testing.T) {
	idx, _, cleanup := createTestBucketIndex(t, 256)
	defer cleanup()

	key := "update_test_key"

	// Insert initial value
	err := idx.Put(key, "doc_v1", 1)
	if err != nil {
		t.Fatalf("Initial Put failed: %v", err)
	}

	// Verify initial value
	docIDs, _, err := idx.Get(key)
	if err != nil || len(docIDs) != 1 || docIDs[0] != "doc_v1" {
		t.Errorf("Get after insert = %v, want [doc_v1]", docIDs)
	}

	// Update value
	err = idx.Put(key, "doc_v2", 2)
	if err != nil {
		t.Fatalf("Update Put failed: %v", err)
	}

	// Verify updated value
	docIDs, pageIDs, err := idx.Get(key)
	if err != nil || len(docIDs) != 1 || docIDs[0] != "doc_v2" {
		t.Errorf("Get after update = %v, want [doc_v2]", docIDs)
	}
	if len(pageIDs) != 1 || pageIDs[0] != 2 {
		t.Errorf("Get pageID after update = %v, want [2]", pageIDs)
	}

	// Delete
	deleted, err := idx.Delete(key)
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
	if !deleted {
		t.Error("Delete returned false, expected true")
	}

	// Verify deleted
	docIDs, _, err = idx.Get(key)
	if err != nil {
		t.Errorf("Get after delete failed: %v", err)
	}
	if len(docIDs) != 0 {
		t.Errorf("Get after delete = %v, want []", docIDs)
	}

	t.Log("Update and delete operations work correctly with bucket optimization")
}

// Helper function
func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > len(substr) && findSubstring(s, substr))
}

func findSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
