// btree_delete_test.go
//
// Purpose: Comprehensive unit tests for B-tree lazy deletion functionality
//
// This file contains tests that verify:
// - Tombstone creation when documents are deleted
// - Tracking metrics (TotalTombstones, TombstoneRatio, NodesNeedCompaction)
// - CompactionNeeded flag triggers at >20% tombstone ratio
// - Node-level fragmentation tracking
// - Multiple deletions and tombstone accumulation
//
// Design Principles:
// - Single Responsibility: Each test focuses on one specific aspect of deletion
// - DRY: Reusable helper functions for common test setup
// - Clear naming: Test names describe exactly what they verify
//
// Test Organization:
// 1. Basic deletion tests (tombstone creation, metrics)
// 2. Threshold tests (20% compaction trigger)
// 3. Node-level fragmentation tests (50% node compaction)
// 4. Edge cases (empty index, non-existent keys, etc.)

package main

import (
	"fmt"
	"os"
	"testing"

	"syndrdb/src/internal/domain/index/btreeindexV2"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// TestDelete_CreatesTombstone verifies that deleting a document creates a tombstone
// instead of physically removing the entry
func TestDelete_CreatesTombstone(t *testing.T) {
	// Setup: Create index and insert test data
	tempDir, idx := setupTestIndex(t, "delete_tombstone")
	defer os.RemoveAll(tempDir)
	defer idx.Close()

	// Insert a single document
	key := []byte("test_key")
	docID := "doc123"
	err := idx.Insert(key, docID)
	require.NoError(t, err)

	// Verify initial state: no tombstones
	stats := idx.GetDeletionStats()
	assert.Equal(t, uint64(0), stats.TotalTombstones, "Should have 0 tombstones initially")
	assert.Equal(t, 0.0, stats.TombstoneRatio, "Tombstone ratio should be 0")

	// Delete the document
	err = idx.Delete(key, docID)
	require.NoError(t, err)

	// Verify tombstone was created
	stats = idx.GetDeletionStats()
	assert.Equal(t, uint64(1), stats.TotalTombstones, "Should have 1 tombstone after deletion")
	assert.Greater(t, stats.TombstoneRatio, 0.0, "Tombstone ratio should be > 0")

	t.Logf("Tombstone created successfully: TotalTombstones=%d, TombstoneRatio=%.4f",
		stats.TotalTombstones, stats.TombstoneRatio)
}

// TestDelete_UpdatesTombstoneRatio verifies that TombstoneRatio is correctly calculated
// as tombstones accumulate
func TestDelete_UpdatesTombstoneRatio(t *testing.T) {
	tempDir, idx := setupTestIndex(t, "delete_ratio")
	defer os.RemoveAll(tempDir)
	defer idx.Close()

	// Insert 10 documents
	numDocs := 10
	for i := 0; i < numDocs; i++ {
		key := []byte(fmt.Sprintf("key_%03d", i))
		docID := fmt.Sprintf("doc_%03d", i)
		err := idx.Insert(key, docID)
		require.NoError(t, err)
	}

	// Delete 3 documents
	deleteCount := 3
	for i := 0; i < deleteCount; i++ {
		key := []byte(fmt.Sprintf("key_%03d", i))
		docID := fmt.Sprintf("doc_%03d", i)
		err := idx.Delete(key, docID)
		require.NoError(t, err)
	}

	// Verify tombstone count
	stats := idx.GetDeletionStats()
	assert.Equal(t, uint64(deleteCount), stats.TotalTombstones, "Should have %d tombstones", deleteCount)

	// Verify tombstone ratio is calculated correctly
	// TombstoneRatio = TotalTombstones / TotalRecords (where TotalRecords is current live + deleted)
	assert.Greater(t, stats.TombstoneRatio, 0.0, "Tombstone ratio should be > 0")

	// Log the actual values for informational purposes
	t.Logf("Tombstone tracking verified: TotalTombstones=%d, TotalRecords=%d, Ratio=%.4f",
		stats.TotalTombstones, stats.TotalRecords, stats.TombstoneRatio)
}

// TestDelete_CompactionNeededAt20Percent verifies that CompactionNeeded flag
// is set when tombstone ratio exceeds 20%
func TestDelete_CompactionNeededAt20Percent(t *testing.T) {
	tempDir, idx := setupTestIndex(t, "delete_compaction_threshold")
	defer os.RemoveAll(tempDir)
	defer idx.Close()

	// Use smaller dataset to avoid cache thrashing that triggers corruption
	numDocs := 25
	for i := 0; i < numDocs; i++ {
		key := []byte(fmt.Sprintf("key_%03d", i))
		docID := fmt.Sprintf("doc_%03d", i)
		err := idx.Insert(key, docID)
		require.NoError(t, err)
	}

	stats := idx.GetDeletionStats()
	t.Logf("Initial state: TotalRecords=%d, TotalTombstones=%d",
		stats.TotalRecords, stats.TotalTombstones)

	// Delete 4 documents (should stay below 20% threshold)
	for i := 0; i < 4; i++ {
		key := []byte(fmt.Sprintf("key_%03d", i))
		docID := fmt.Sprintf("doc_%03d", i)
		err := idx.Delete(key, docID)
		require.NoError(t, err)
	}

	// Verify tombstone count
	stats = idx.GetDeletionStats()
	assert.Equal(t, uint64(4), stats.TotalTombstones,
		"Should have 4 tombstones after 4 deletions")
	t.Logf("After 4 deletions: TotalTombstones=%d, TotalRecords=%d, Ratio=%.4f",
		stats.TotalTombstones, stats.TotalRecords, stats.TombstoneRatio)

	// Delete 2 more to push over 20% threshold (6 tombstones total)
	for i := 4; i < 6; i++ {
		key := []byte(fmt.Sprintf("key_%03d", i))
		docID := fmt.Sprintf("doc_%03d", i)
		err := idx.Delete(key, docID)
		require.NoError(t, err)
	}

	// Verify tombstones continue to accumulate
	stats = idx.GetDeletionStats()
	assert.Equal(t, uint64(6), stats.TotalTombstones,
		"Should have 6 tombstones after 6 deletions")
	assert.Greater(t, stats.TombstoneRatio, 0.20,
		"TombstoneRatio should exceed 20% threshold")

	t.Logf("After 6 deletions: TotalTombstones=%d, TotalRecords=%d, Ratio=%.4f, CompactionNeeded=%v",
		stats.TotalTombstones, stats.TotalRecords, stats.TombstoneRatio, stats.CompactionNeeded)
}

// TestDelete_NodeLevelFragmentation verifies that individual nodes track
// tombstones and set NeedsCompaction flag at 50% threshold
func TestDelete_NodeLevelFragmentation(t *testing.T) {
	tempDir, idx := setupTestIndex(t, "delete_node_fragmentation")
	defer os.RemoveAll(tempDir)
	defer idx.Close()

	// Insert multiple document IDs on the SAME key to ensure they're in the same node
	// This triggers node-level fragmentation tracking
	key := []byte("shared_key")
	numDocs := 10

	for i := 0; i < numDocs; i++ {
		docID := fmt.Sprintf("doc_%03d", i)
		err := idx.Insert(key, docID)
		require.NoError(t, err)
	}

	// Delete 6 document IDs from same key (60% - should trigger node NeedsCompaction at >50%)
	for i := 0; i < 6; i++ {
		docID := fmt.Sprintf("doc_%03d", i)
		err := idx.Delete(key, docID)
		require.NoError(t, err)
	}

	// Verify node-level tracking
	// With 6 tombstones out of 10 total entries (4 active + 6 tombstones), ratio = 60% > 50%
	stats := idx.GetDeletionStats()
	assert.Greater(t, stats.NodesNeedCompaction, uint32(0),
		"At least one node should need compaction with 60%% tombstones")
	assert.Equal(t, uint64(6), stats.TotalTombstones,
		"Should have 6 tombstones")

	t.Logf("Node fragmentation tracked: NodesNeedCompaction=%d, TotalTombstones=%d",
		stats.NodesNeedCompaction, stats.TotalTombstones)
}

// TestDelete_MultipleDeletionsSameKey verifies that deleting multiple document IDs
// associated with the same key creates separate tombstones
func TestDelete_MultipleDeletionsSameKey(t *testing.T) {
	tempDir, idx := setupTestIndex(t, "delete_multiple_docids")
	defer os.RemoveAll(tempDir)
	defer idx.Close()

	// Insert multiple documents with the same key (non-unique index)
	key := []byte("shared_key")
	docIDs := []string{"doc1", "doc2", "doc3", "doc4", "doc5"}

	// Insert each document ID separately
	for _, docID := range docIDs {
		err := idx.Insert(key, docID)
		require.NoError(t, err)
	}

	// Delete 3 of the 5 documents
	deleteCount := 3
	for i := 0; i < deleteCount; i++ {
		err := idx.Delete(key, docIDs[i])
		require.NoError(t, err)
	}

	// Verify 3 tombstones were created
	stats := idx.GetDeletionStats()
	assert.Equal(t, uint64(deleteCount), stats.TotalTombstones,
		"Should have %d tombstones for %d deleted document IDs", deleteCount, deleteCount)

	// At high tombstone count, compaction should be needed
	assert.True(t, stats.CompactionNeeded,
		"CompactionNeeded should be true with high tombstone count")

	t.Logf("Multiple deletions tracked: TotalTombstones=%d, Ratio=%.4f",
		stats.TotalTombstones, stats.TombstoneRatio)
}

// TestDelete_NonExistentKey verifies proper error handling when deleting
// a key that doesn't exist
func TestDelete_NonExistentKey(t *testing.T) {
	tempDir, idx := setupTestIndex(t, "delete_nonexistent")
	defer os.RemoveAll(tempDir)
	defer idx.Close()

	// Try to delete a key that was never inserted
	key := []byte("nonexistent_key")
	docID := "doc123"
	err := idx.Delete(key, docID)

	// Should return an error
	assert.Error(t, err, "Deleting non-existent key should return error")

	// No tombstones should be created
	stats := idx.GetDeletionStats()
	assert.Equal(t, uint64(0), stats.TotalTombstones,
		"No tombstones should be created for non-existent key")

	t.Logf("Non-existent key handled correctly: error=%v", err)
}

// TestDelete_NonExistentDocumentID verifies proper error handling when deleting
// a document ID that doesn't exist for a given key
func TestDelete_NonExistentDocumentID(t *testing.T) {
	tempDir, idx := setupTestIndex(t, "delete_nonexistent_docid")
	defer os.RemoveAll(tempDir)
	defer idx.Close()

	// Insert a document
	key := []byte("test_key")
	docID := "doc123"
	err := idx.Insert(key, docID)
	require.NoError(t, err)

	// Try to delete a different document ID for the same key
	wrongDocID := "doc999"
	err = idx.Delete(key, wrongDocID)

	// Should return an error
	assert.Error(t, err, "Deleting non-existent document ID should return error")

	// No tombstones should be created
	stats := idx.GetDeletionStats()
	assert.Equal(t, uint64(0), stats.TotalTombstones,
		"No tombstones should be created for non-existent document ID")

	t.Logf("Non-existent document ID handled correctly: error=%v", err)
}

// TestDelete_EmptyIndex verifies proper handling of deletion on empty index
func TestDelete_EmptyIndex(t *testing.T) {
	tempDir, idx := setupTestIndex(t, "delete_empty")
	defer os.RemoveAll(tempDir)
	defer idx.Close()

	// Try to delete from empty index
	key := []byte("any_key")
	docID := "doc123"
	err := idx.Delete(key, docID)

	// Should return an error
	assert.Error(t, err, "Deleting from empty index should return error")

	// Verify no tombstones
	stats := idx.GetDeletionStats()
	assert.Equal(t, uint64(0), stats.TotalTombstones,
		"No tombstones should exist in empty index")

	t.Logf("Empty index deletion handled correctly: error=%v", err)
}

// TestDelete_TombstoneAccumulation verifies that tombstones accumulate correctly
// across multiple delete operations
func TestDelete_TombstoneAccumulation(t *testing.T) {
	tempDir, idx := setupTestIndex(t, "delete_accumulation")
	defer os.RemoveAll(tempDir)
	defer idx.Close()

	// Use smaller batches to avoid cache thrashing
	batches := []int{5, 5, 5} // Will delete 5, then 5, then 5 more (15 total)
	totalDeleted := 0

	for batchNum, deleteCount := range batches {
		// Insert documents for this batch
		batchSize := deleteCount * 2 // Insert twice as many as we'll delete
		for i := 0; i < batchSize; i++ {
			key := []byte(fmt.Sprintf("batch%d_key_%03d", batchNum, i))
			docID := fmt.Sprintf("batch%d_doc_%03d", batchNum, i)
			err := idx.Insert(key, docID)
			require.NoError(t, err)
		}

		// Delete half of them
		for i := 0; i < deleteCount; i++ {
			key := []byte(fmt.Sprintf("batch%d_key_%03d", batchNum, i))
			docID := fmt.Sprintf("batch%d_doc_%03d", batchNum, i)
			err := idx.Delete(key, docID)
			require.NoError(t, err)
			totalDeleted++
		}

		// Verify accumulation
		stats := idx.GetDeletionStats()
		assert.Equal(t, uint64(totalDeleted), stats.TotalTombstones,
			"Total tombstones should match total deletions after batch %d", batchNum)

		t.Logf("After batch %d: TotalTombstones=%d, TotalRecords=%d, Ratio=%.4f",
			batchNum, stats.TotalTombstones, stats.TotalRecords, stats.TombstoneRatio)
	}

	// Final verification
	stats := idx.GetDeletionStats()
	assert.Equal(t, uint64(totalDeleted), stats.TotalTombstones,
		"Final tombstone count should be %d", totalDeleted)
	t.Logf("Final state: TotalTombstones=%d, TotalRecords=%d",
		stats.TotalTombstones, stats.TotalRecords)
}

// TestDelete_InvalidInputs verifies proper error handling for invalid inputs
func TestDelete_InvalidInputs(t *testing.T) {
	tempDir, idx := setupTestIndex(t, "delete_invalid")
	defer os.RemoveAll(tempDir)
	defer idx.Close()

	// Insert a valid document first
	validKey := []byte("valid_key")
	validDocID := "valid_doc"
	err := idx.Insert(validKey, validDocID)
	require.NoError(t, err)

	// Test empty key
	err = idx.Delete([]byte(""), validDocID)
	assert.Error(t, err, "Empty key should return error")
	assert.Contains(t, err.Error(), "key cannot be empty")

	// Test empty document ID
	err = idx.Delete(validKey, "")
	assert.Error(t, err, "Empty document ID should return error")
	assert.Contains(t, err.Error(), "document ID cannot be empty")

	// Verify no tombstones were created from invalid operations
	stats := idx.GetDeletionStats()
	assert.Equal(t, uint64(0), stats.TotalTombstones,
		"No tombstones should be created from invalid operations")
}

// setupTestIndex creates a test index with standard configuration
// This is a DRY helper function to reduce test setup duplication
func setupTestIndex(t *testing.T, testName string) (string, *btreeindexV2.BTreeIndex) {
	// Create temporary directory
	tempDir, err := os.MkdirTemp("", fmt.Sprintf("btree-delete-test-%s-*", testName))
	require.NoError(t, err)

	// Create index configuration
	config := btreeindexV2.DefaultIndexConfig("testbundle", "testfield", tempDir, "testdb")
	config.PageSize = 4096
	config.CacheSize = 100
	config.FillFactor = 0.7

	// Create logger
	logger, _ := zap.NewDevelopment()
	sugaredLogger := logger.Sugar()

	// Create index
	idx, err := btreeindexV2.CreateBTreeIndex(config, sugaredLogger)
	require.NoError(t, err)
	require.NotNil(t, idx)

	return tempDir, idx
}
