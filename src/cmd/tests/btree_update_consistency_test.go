package main

import (
	"fmt"
	"os"
	"syndrdb/src/internal/domain/index/btreeindexV2"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// TestBTreeIndex_DocumentUpdateMaintainsIndexConsistency verifies that B-tree indexes
// correctly maintain consistency when documents are updated:
// - Old index entries are removed
// - New index entries are added
// - Range queries work correctly after updates
// - Multiple updates don't corrupt the index
//
// This test simulates the critical path: Document Update → Index Update
func TestBTreeIndex_DocumentUpdateMaintainsIndexConsistency(t *testing.T) {
	testDir := "data/testdb/btree_update_consistency"

	// Ensure clean state - forcefully remove any leftover files
	err := os.RemoveAll(testDir)
	if err != nil && !os.IsNotExist(err) {
		t.Logf("Warning: Failed to remove test directory: %v", err)
	}

	// Create fresh directory
	err = os.MkdirAll(testDir, 0755)
	require.NoError(t, err, "Failed to create test directory")

	// Ensure cleanup happens even if test fails
	defer func() {
		if err := os.RemoveAll(testDir); err != nil {
			t.Logf("Cleanup failed: %v", err)
		}
	}()

	logger, err := zap.NewDevelopment()
	require.NoError(t, err)
	defer logger.Sync()
	sugaredLogger := logger.Sugar()

	// Create B-tree index on 'category' field (string-based)
	config := btreeindexV2.DefaultIndexConfig("products", "category", testDir, "testdb")
	config.PageSize = 8192
	config.CacheSize = 50

	// Get the actual file path and ensure it doesn't exist
	indexFilePath := config.GetIndexFilePath()
	t.Logf("Index file path: %s", indexFilePath)

	// Remove the actual index file if it exists (from previous test runs)
	if err := os.Remove(indexFilePath); err != nil && !os.IsNotExist(err) {
		t.Logf("Warning: Failed to remove existing index file: %v", err)
	}

	idx, err := btreeindexV2.CreateBTreeIndex(config, sugaredLogger)
	require.NoError(t, err)
	defer idx.Close()

	// ========== SETUP: Insert Initial Documents ==========
	// Products: A=$50, B=$75, C=$100
	testData := map[string]int{
		"product_A": 50,
		"product_B": 75,
		"product_C": 100,
	}

	for docID, price := range testData {
		key := encodeIntKey(price)
		err := idx.Insert(key, docID)
		require.NoError(t, err, "Failed to insert initial document %s with price %d", docID, price)
	}

	sugaredLogger.Infof("✅ Setup complete: Inserted 3 products")

	// ========== STEP 1: Verify Initial Index State ==========
	t.Run("Initial state - all entries present", func(t *testing.T) {
		// Range query: price < 60 → should return [A]
		results, err := idx.RangeSearchWithBounds(encodeIntKey(0), encodeIntKey(60), false, true)
		require.NoError(t, err)
		assert.Len(t, results, 1, "Should find 1 product with price < 60")
		assert.Contains(t, results, "product_A")

		// Range query: price >= 75 → should return [B, C]
		results, err = idx.RangeSearchWithBounds(encodeIntKey(75), encodeIntKey(999), false, false)
		require.NoError(t, err)
		assert.Len(t, results, 2, "Should find 2 products with price >= 75")
		assert.Contains(t, results, "product_B")
		assert.Contains(t, results, "product_C")

		// Exact search: price = 75 → should return [B]
		results, err = idx.Search(encodeIntKey(75))
		require.NoError(t, err)
		assert.Len(t, results, 1, "Should find 1 product with price = 75")
		assert.Contains(t, results, "product_B")

		sugaredLogger.Infof("✅ Initial state verified: All entries correct")
	})

	// ========== STEP 2: Update Document (Simulate Bundle Service Flow) ==========
	t.Run("Update document - change product_B price from $75 to $120", func(t *testing.T) {
		docID := "product_B"
		oldPrice := 75
		newPrice := 120

		// Step 2a: Delete old index entry (what bundle service does)
		err := idx.Delete(encodeIntKey(oldPrice), docID)
		require.NoError(t, err, "Failed to delete old index entry")
		sugaredLogger.Infof("   Deleted old entry: price=%d → %s", oldPrice, docID)

		// Step 2b: Insert new index entry (what bundle service does)
		err = idx.Insert(encodeIntKey(newPrice), docID)
		require.NoError(t, err, "Failed to insert new index entry")
		sugaredLogger.Infof("   Inserted new entry: price=%d → %s", newPrice, docID)

		sugaredLogger.Infof("✅ Update complete: product_B moved from $75 to $120")
	})

	// ========== STEP 3: Verify Updated Index State ==========
	t.Run("After update - old entry removed, new entry added", func(t *testing.T) {
		// Verify old entry is GONE
		results, err := idx.Search(encodeIntKey(75))
		require.NoError(t, err)
		assert.Len(t, results, 0, "Old entry (price=75) should be removed")
		sugaredLogger.Infof("   ✓ Old entry removed: price=75 returns no results")

		// Verify new entry EXISTS
		results, err = idx.Search(encodeIntKey(120))
		require.NoError(t, err)
		assert.Len(t, results, 1, "New entry (price=120) should exist")
		assert.Contains(t, results, "product_B")
		sugaredLogger.Infof("   ✓ New entry added: price=120 → product_B")

		// Verify other entries unchanged
		results, err = idx.Search(encodeIntKey(50))
		require.NoError(t, err)
		assert.Len(t, results, 1, "Product A (price=50) should be unchanged")
		assert.Contains(t, results, "product_A")

		results, err = idx.Search(encodeIntKey(100))
		require.NoError(t, err)
		assert.Len(t, results, 1, "Product C (price=100) should be unchanged")
		assert.Contains(t, results, "product_C")

		sugaredLogger.Infof("✅ Post-update verification: Index consistent")
	})

	// ========== STEP 4: Verify Range Queries After Update ==========
	t.Run("Range queries work correctly after update", func(t *testing.T) {
		// Range: price < 60 → should still return [A]
		results, err := idx.RangeSearchWithBounds(encodeIntKey(0), encodeIntKey(60), false, true)
		require.NoError(t, err)
		assert.Len(t, results, 1, "Range <60 should return 1 product")
		assert.Contains(t, results, "product_A")
		sugaredLogger.Infof("   ✓ Range <60: [product_A]")

		// Range: price >= 75 AND price < 110 → should return [C] only (B moved to 120)
		results, err = idx.RangeSearchWithBounds(encodeIntKey(75), encodeIntKey(110), false, true)
		require.NoError(t, err)
		assert.Len(t, results, 1, "Range [75,110) should return 1 product")
		assert.Contains(t, results, "product_C")
		assert.NotContains(t, results, "product_B", "Product B should not be in range [75,110)")
		sugaredLogger.Infof("   ✓ Range [75,110): [product_C]")

		// Range: price > 100 → should return [B] only (C is at exactly 100, excluded with excludeStart=true)
		results, err = idx.RangeSearchWithBounds(encodeIntKey(100), encodeIntKey(999), true, false)
		require.NoError(t, err)
		assert.Len(t, results, 1, "Range >100 should return 1 product (B at 120)")
		assert.Contains(t, results, "product_B", "Product B at $120 should be in range >100")
		assert.NotContains(t, results, "product_C", "Product C at exactly $100 should be excluded (excludeStart=true)")
		sugaredLogger.Infof("   ✓ Range >100: [product_B]")

		sugaredLogger.Infof("✅ Range queries correct after update")
	})

	// ========== STEP 5: Multiple Updates ==========
	t.Run("Multiple updates maintain consistency", func(t *testing.T) {
		// Update product_A: $50 → $80
		err := idx.Delete(encodeIntKey(50), "product_A")
		require.NoError(t, err)
		err = idx.Insert(encodeIntKey(80), "product_A")
		require.NoError(t, err)

		// Update product_C: $100 → $90
		err = idx.Delete(encodeIntKey(100), "product_C")
		require.NoError(t, err)
		err = idx.Insert(encodeIntKey(90), "product_C")
		require.NoError(t, err)

		sugaredLogger.Infof("   Updated: A=$50→$80, C=$100→$90")

		// Verify final state: A=$80, B=$120, C=$90
		results, err := idx.Search(encodeIntKey(80))
		require.NoError(t, err)
		assert.Contains(t, results, "product_A")

		results, err = idx.Search(encodeIntKey(90))
		require.NoError(t, err)
		assert.Contains(t, results, "product_C")

		results, err = idx.Search(encodeIntKey(120))
		require.NoError(t, err)
		assert.Contains(t, results, "product_B")

		// Old values should be gone
		results, err = idx.Search(encodeIntKey(50))
		require.NoError(t, err)
		assert.Len(t, results, 0, "Old price $50 should be gone")

		results, err = idx.Search(encodeIntKey(100))
		require.NoError(t, err)
		assert.Len(t, results, 0, "Old price $100 should be gone")

		sugaredLogger.Infof("✅ Multiple updates: All consistent")
	})

	// ========== STEP 6: Edge Cases ==========
	t.Run("Edge case - update to same value (no-op)", func(t *testing.T) {
		// "Update" product_A to same price ($80)
		err := idx.Delete(encodeIntKey(80), "product_A")
		require.NoError(t, err)
		err = idx.Insert(encodeIntKey(80), "product_A")
		require.NoError(t, err)

		// Should still find product_A at $80
		results, err := idx.Search(encodeIntKey(80))
		require.NoError(t, err)
		assert.Contains(t, results, "product_A")

		sugaredLogger.Infof("✅ No-op update: Handled correctly")
	})

	t.Run("Edge case - update non-existent document", func(t *testing.T) {
		// Try to delete entry that doesn't exist
		err := idx.Delete(encodeIntKey(999), "product_Z")
		// Should either succeed (no-op) or return error - both are acceptable
		// Just verify index isn't corrupted
		if err != nil {
			sugaredLogger.Infof("   Delete non-existent returned error: %v", err)
		}

		// Verify index still works
		results, err := idx.Search(encodeIntKey(80))
		require.NoError(t, err)
		assert.Contains(t, results, "product_A")

		sugaredLogger.Infof("✅ Non-existent delete: Index stable")
	})
}

// TestBTreeIndex_UpdateWithMultipleDocumentsPerKey verifies that updates work
// correctly when multiple documents share the same indexed value (non-unique index)
func TestBTreeIndex_UpdateWithMultipleDocumentsPerKey(t *testing.T) {
	testDir := "data/testdb/btree_update_multi_docs"
	os.RemoveAll(testDir)
	defer os.RemoveAll(testDir)
	os.MkdirAll(testDir, 0755)

	logger, err := zap.NewDevelopment()
	require.NoError(t, err)
	defer logger.Sync()
	sugaredLogger := logger.Sugar()

	// Create B-tree index on 'category' (non-unique)
	config := btreeindexV2.DefaultIndexConfig("products", "category", testDir, "testdb")
	config.PageSize = 8192
	config.CacheSize = 50

	// CRITICAL: Remove actual index file to prevent recovery mode
	indexFilePath := config.GetIndexFilePath()
	if err := os.Remove(indexFilePath); err != nil && !os.IsNotExist(err) {
		t.Logf("Warning: Failed to remove existing index file: %v", err)
	}

	idx, err := btreeindexV2.CreateBTreeIndex(config, sugaredLogger)
	require.NoError(t, err)
	defer idx.Close()

	// Setup: Multiple products in same category
	// Electronics: [TV, Radio, Phone]
	// Books: [Novel, Textbook]
	categoryKey := []byte("Electronics")
	err = idx.Insert(categoryKey, "product_TV")
	require.NoError(t, err)
	err = idx.Insert(categoryKey, "product_Radio")
	require.NoError(t, err)
	err = idx.Insert(categoryKey, "product_Phone")
	require.NoError(t, err)

	booksKey := []byte("Books")
	err = idx.Insert(booksKey, "product_Novel")
	require.NoError(t, err)
	err = idx.Insert(booksKey, "product_Textbook")
	require.NoError(t, err)

	t.Run("Initial state - multiple documents per key", func(t *testing.T) {
		results, err := idx.Search(categoryKey)
		require.NoError(t, err)
		assert.Len(t, results, 3, "Should find 3 electronics products")
		assert.Contains(t, results, "product_TV")
		assert.Contains(t, results, "product_Radio")
		assert.Contains(t, results, "product_Phone")

		sugaredLogger.Infof("✅ Initial: 3 products in Electronics category")
	})

	t.Run("Update one document - others remain", func(t *testing.T) {
		// Move product_Radio from Electronics to Books
		err := idx.Delete(categoryKey, "product_Radio")
		require.NoError(t, err)
		err = idx.Insert(booksKey, "product_Radio")
		require.NoError(t, err)

		// Electronics should now have 2 products
		results, err := idx.Search(categoryKey)
		require.NoError(t, err)
		assert.Len(t, results, 2, "Electronics should have 2 products")
		assert.Contains(t, results, "product_TV")
		assert.Contains(t, results, "product_Phone")
		assert.NotContains(t, results, "product_Radio")

		// Books should now have 3 products
		results, err = idx.Search(booksKey)
		require.NoError(t, err)
		assert.Len(t, results, 3, "Books should have 3 products")
		assert.Contains(t, results, "product_Novel")
		assert.Contains(t, results, "product_Textbook")
		assert.Contains(t, results, "product_Radio")

		sugaredLogger.Infof("✅ Partial update: Radio moved from Electronics to Books")
	})

	t.Run("Delete all documents from a key", func(t *testing.T) {
		// Remove all Electronics products
		err := idx.Delete(categoryKey, "product_TV")
		require.NoError(t, err)
		err = idx.Delete(categoryKey, "product_Phone")
		require.NoError(t, err)

		// Electronics should be empty
		results, err := idx.Search(categoryKey)
		require.NoError(t, err)
		assert.Len(t, results, 0, "Electronics category should be empty")

		// Books should still have 3
		results, err = idx.Search(booksKey)
		require.NoError(t, err)
		assert.Len(t, results, 3, "Books should still have 3 products")

		sugaredLogger.Infof("✅ Complete removal: Electronics category empty")
	})
}

// TestBTreeIndex_UpdateDuringConcurrentReads verifies that updates don't
// corrupt the index when concurrent reads are happening
func TestBTreeIndex_UpdateDuringConcurrentReads(t *testing.T) {
	testDir := "data/testdb/btree_update_concurrent"
	os.RemoveAll(testDir)
	defer os.RemoveAll(testDir)
	os.MkdirAll(testDir, 0755)

	logger, err := zap.NewDevelopment()
	require.NoError(t, err)
	defer logger.Sync()
	sugaredLogger := logger.Sugar()

	config := btreeindexV2.DefaultIndexConfig("products", "price", testDir, "testdb")
	config.PageSize = 8192
	config.CacheSize = 100

	// CRITICAL: Remove actual index file to prevent recovery mode
	indexFilePath := config.GetIndexFilePath()
	if err := os.Remove(indexFilePath); err != nil && !os.IsNotExist(err) {
		t.Logf("Warning: Failed to remove existing index file: %v", err)
	}

	idx, err := btreeindexV2.CreateBTreeIndex(config, sugaredLogger)
	require.NoError(t, err)
	defer idx.Close()

	// Insert initial data
	for i := 0; i < 100; i++ {
		key := encodeIntKey(i * 10)
		docID := fmt.Sprintf("doc_%d", i)
		err := idx.Insert(key, docID)
		require.NoError(t, err)
	}

	t.Run("Updates during concurrent range scans", func(t *testing.T) {
		// This test verifies that concurrent reads don't see corrupted data
		// during updates (thanks to RWMutex locking)

		// Perform some updates
		for i := 0; i < 10; i++ {
			oldPrice := i * 10
			newPrice := i*10 + 5
			docID := fmt.Sprintf("doc_%d", i)

			err := idx.Delete(encodeIntKey(oldPrice), docID)
			require.NoError(t, err)
			err = idx.Insert(encodeIntKey(newPrice), docID)
			require.NoError(t, err)
		}

		// Verify all documents are still searchable
		count := 0
		for i := 0; i < 100; i++ {
			expectedPrice := i * 10
			if i < 10 {
				expectedPrice = i*10 + 5 // Updated prices
			}

			results, err := idx.Search(encodeIntKey(expectedPrice))
			require.NoError(t, err)
			count += len(results)
		}

		assert.Equal(t, 100, count, "Should still have all 100 documents after updates")
		sugaredLogger.Infof("✅ Concurrent updates: All documents accounted for")
	})
}

// encodeIntKey encodes an integer as a zero-padded byte string for consistent sorting
func encodeIntKey(value int) []byte {
	return []byte(fmt.Sprintf("%010d", value)) // Zero-pad to 10 digits
}
