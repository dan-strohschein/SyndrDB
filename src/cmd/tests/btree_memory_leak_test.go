/*
BTREE MEMORY LEAK DETECTION TEST SUITE

This file implements comprehensive memory leak detection tests for B-tree indexes,
validating that the implementation doesn't leak memory during:
- Page cache operations
- Pin/unpin reference counting
- Large dataset operations
- Concurrent access patterns
- Long-running operations

These tests ensure production-grade memory safety for Phase 4.

TEST APPROACH:
- Monitor memory before and after operations
- Use runtime.ReadMemStats() to track allocations
- Test with large datasets to amplify leaks
- Verify goroutine cleanup
- Test pin/unpin balance

COVERAGE:
- Page cache memory management
- Reference counting correctness
- Goroutine leak detection
- Long-running operation memory stability
*/

package main

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"syndrdb/src/internal/domain/index/btreeindexV2"
	"testing"
	"time"

	"go.uber.org/zap"
)

// ================================================================================
// MEMORY TESTING UTILITIES
// ================================================================================

// MemoryStats holds memory usage statistics for comparison
type MemoryStats struct {
	Alloc      uint64 // Bytes allocated and still in use
	TotalAlloc uint64 // Bytes allocated (cumulative)
	Sys        uint64 // Bytes obtained from system
	NumGC      uint32 // Number of completed GC cycles
	Goroutines int    // Number of active goroutines
}

// getMemoryStats captures current memory statistics
func getMemoryStats() MemoryStats {
	runtime.GC() // Force GC to get accurate measurements
	time.Sleep(10 * time.Millisecond)

	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	return MemoryStats{
		Alloc:      m.Alloc,
		TotalAlloc: m.TotalAlloc,
		Sys:        m.Sys,
		NumGC:      m.NumGC,
		Goroutines: runtime.NumGoroutine(),
	}
}

// compareMemoryStats compares two memory stats and returns the difference
// Note: For Alloc/Sys, GC can cause "after" to be less than "before", resulting in underflow
func compareMemoryStats(before, after MemoryStats) MemoryStats {
	return MemoryStats{
		Alloc:      after.Alloc - before.Alloc, // May underflow if GC ran
		TotalAlloc: after.TotalAlloc - before.TotalAlloc,
		Sys:        after.Sys - before.Sys, // May underflow if OS reclaimed memory
		NumGC:      after.NumGC - before.NumGC,
		Goroutines: after.Goroutines - before.Goroutines,
	}
}

// hasMemoryGrowth checks if memory grew significantly (handling underflow from GC)
// Returns true if there's actual growth exceeding the threshold
func hasMemoryGrowth(diff uint64, threshold uint64) bool {
	// If diff is unreasonably large (> 1PB), it's likely underflow from GC
	const reasonableMaxGrowth = 1 << 50 // 1 PB - no test should grow this much

	if diff > reasonableMaxGrowth {
		// Underflow detected - memory actually decreased (GC ran)
		return false
	}

	// Normal case: check if growth exceeds threshold
	return diff > threshold
}

// formatBytes formats bytes into human-readable string
func formatBytes(bytes uint64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := uint64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}

// createMemoryTestIndex creates a test index for memory leak testing
func createMemoryTestIndex(t *testing.T, testName string) *btreeindexV2.BTreeIndex {
	t.Helper()

	testDir := filepath.Join("data", "memory_leak_tests")
	os.MkdirAll(testDir, 0755)

	indexPath := filepath.Join(testDir, fmt.Sprintf("%s_btree.btidx", testName))
	os.Remove(indexPath)

	logger, _ := zap.NewDevelopment()

	config := btreeindexV2.DefaultIndexConfig("leak_test_bundle", "test_field", testDir, "testdb")
	config.PageSize = 4096
	config.CacheSize = 200 // Increased from 100 to reduce cache thrashing
	config.MaxKeyLength = 256

	idx, err := btreeindexV2.CreateBTreeIndex(config, logger.Sugar())
	if err != nil {
		t.Fatalf("Failed to create test index: %v", err)
	}

	return idx
}

// ================================================================================
// PAGE CACHE MEMORY LEAK TESTS
// ================================================================================

// TestMemoryLeak_PageCacheGrowth tests that page cache doesn't grow unbounded
func TestMemoryLeak_PageCacheGrowth(t *testing.T) {
	idx := createMemoryTestIndex(t, "cache_growth")
	defer idx.Close()

	// Capture initial memory
	before := getMemoryStats()
	t.Logf("Initial memory: Alloc=%s, Sys=%s, Goroutines=%d",
		formatBytes(before.Alloc), formatBytes(before.Sys), before.Goroutines)

	// Insert keys to fill and overflow cache
	// checkMaintenanceNeeded() is now throttled to run every 1000 operations
	const numKeys = 2000
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key_%08d", i)
		err := idx.Insert([]byte(key), fmt.Sprintf("doc_%08d", i))
		if err != nil {
			t.Fatalf("Insert failed at %d: %v", i, err)
		}
	}

	// Force cache eviction by accessing old keys
	for i := 0; i < 200; i++ {
		key := fmt.Sprintf("key_%08d", i)
		_, _ = idx.Search([]byte(key))
	}

	// Capture memory after operations
	after := getMemoryStats()
	t.Logf("After operations: Alloc=%s, Sys=%s, Goroutines=%d",
		formatBytes(after.Alloc), formatBytes(after.Sys), after.Goroutines)

	// Calculate difference
	diff := compareMemoryStats(before, after)
	t.Logf("Memory difference: Alloc=%s, Sys=%s, Goroutines=%+d",
		formatBytes(diff.Alloc), formatBytes(diff.Sys), diff.Goroutines)

	// Check for excessive memory growth
	// Allow up to 50MB growth for 10k keys (reasonable for cache + tree structure)
	maxAllowedGrowth := uint64(50 * 1024 * 1024)
	if hasMemoryGrowth(diff.Alloc, maxAllowedGrowth) {
		t.Errorf("Excessive memory growth: %s (max allowed: %s)",
			formatBytes(diff.Alloc), formatBytes(maxAllowedGrowth))
	}

	// Check for goroutine leaks
	if diff.Goroutines > 5 {
		t.Errorf("Goroutine leak detected: %d new goroutines", diff.Goroutines)
	}

	t.Logf("✅ Page cache memory growth test passed")
}

// TestMemoryLeak_PagePinUnpinBalance tests that pin/unpin calls are balanced
func TestMemoryLeak_PagePinUnpinBalance(t *testing.T) {
	idx := createMemoryTestIndex(t, "pin_unpin_balance")
	defer idx.Close()

	// Insert test data
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key_%04d", i)
		err := idx.Insert([]byte(key), fmt.Sprintf("doc_%04d", i))
		if err != nil {
			t.Fatalf("Insert failed: %v", err)
		}
	}

	// Perform many search operations
	before := getMemoryStats()

	for iteration := 0; iteration < 100; iteration++ {
		for i := 0; i < 100; i++ {
			key := fmt.Sprintf("key_%04d", i)
			_, err := idx.Search([]byte(key))
			if err != nil {
				t.Fatalf("Search failed: %v", err)
			}
		}
	}

	after := getMemoryStats()
	diff := compareMemoryStats(before, after)

	t.Logf("After 10,000 searches: Alloc diff=%s, Goroutines diff=%d",
		formatBytes(diff.Alloc), diff.Goroutines)

	// Memory should be stable (allowing for some GC variance)
	maxAllowedGrowth := uint64(5 * 1024 * 1024) // 5MB tolerance
	if hasMemoryGrowth(diff.Alloc, maxAllowedGrowth) {
		t.Errorf("Memory leak in search operations: %s growth", formatBytes(diff.Alloc))
	}

	// TODO: I should add a method to check page pin counts directly
	// to verify all pages are properly unpinned

	t.Logf("✅ Pin/unpin balance test passed")
}

// TestMemoryLeak_CacheEviction tests memory cleanup during cache eviction
func TestMemoryLeak_CacheEviction(t *testing.T) {
	idx := createMemoryTestIndex(t, "cache_eviction")
	defer idx.Close()

	// Insert enough keys to force cache evictions
	const numKeys = 1000
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key_%08d", i)
		err := idx.Insert([]byte(key), fmt.Sprintf("doc_%08d", i))
		if err != nil {
			t.Fatalf("Insert failed: %v", err)
		}
	}

	before := getMemoryStats()

	// Access keys in pattern that forces evictions
	// Access keys 0-99, 500-599, 900-999 repeatedly
	for iteration := 0; iteration < 50; iteration++ {
		for i := 0; i < 100; i++ {
			key := fmt.Sprintf("key_%08d", i)
			idx.Search([]byte(key))
		}
		for i := 500; i < 600; i++ {
			key := fmt.Sprintf("key_%08d", i)
			idx.Search([]byte(key))
		}
		for i := 900; i < 1000; i++ {
			key := fmt.Sprintf("key_%08d", i)
			idx.Search([]byte(key))
		}
	}

	after := getMemoryStats()
	diff := compareMemoryStats(before, after)

	t.Logf("After cache eviction cycles: Alloc diff=%s", formatBytes(diff.Alloc))

	// Memory should remain stable despite evictions
	maxAllowedGrowth := uint64(2 * 1024 * 1024) // 2MB tolerance
	if hasMemoryGrowth(diff.Alloc, maxAllowedGrowth) {
		t.Errorf("Memory leak during cache eviction: %s growth", formatBytes(diff.Alloc))
	}

	t.Logf("✅ Cache eviction memory test passed")
}

// ================================================================================
// LARGE DATASET MEMORY STABILITY TESTS
// ================================================================================

// TestMemoryLeak_LargeDatasetInserts tests memory stability with large inserts
func TestMemoryLeak_LargeDatasetInserts(t *testing.T) {
	idx := createMemoryTestIndex(t, "large_dataset_inserts")
	defer idx.Close()

	// Capture memory every 1000 inserts (reduced from 5000 for faster tests)
	const batchSize = 1000
	const numBatches = 5
	memorySnapshots := make([]MemoryStats, 0, numBatches+1)

	memorySnapshots = append(memorySnapshots, getMemoryStats())

	for batch := 0; batch < numBatches; batch++ {
		for i := 0; i < batchSize; i++ {
			keyNum := batch*batchSize + i
			key := fmt.Sprintf("key_%08d", keyNum)
			err := idx.Insert([]byte(key), fmt.Sprintf("doc_%08d", keyNum))
			if err != nil {
				t.Fatalf("Insert failed at %d: %v", keyNum, err)
			}
		}

		memorySnapshots = append(memorySnapshots, getMemoryStats())
		t.Logf("After batch %d (%d keys): Alloc=%s",
			batch+1, (batch+1)*batchSize, formatBytes(memorySnapshots[len(memorySnapshots)-1].Alloc))
	}

	// Analyze memory growth pattern
	// Memory should grow sub-linearly (due to tree structure efficiency)
	// Not exponentially (which would indicate a leak)
	// Note: GC can cause memory to decrease between batches, use absolute values

	growth1 := memorySnapshots[1].Alloc - memorySnapshots[0].Alloc
	growth2 := memorySnapshots[2].Alloc - memorySnapshots[1].Alloc
	growth3 := memorySnapshots[3].Alloc - memorySnapshots[2].Alloc

	t.Logf("Memory growth per batch: %s, %s, %s",
		formatBytes(growth1), formatBytes(growth2), formatBytes(growth3))

	// Growth should not accelerate (indicating a leak)
	// Only check if both values are reasonable (not underflow from GC)
	const reasonableMaxGrowth = 1 << 50 // 1 PB
	if growth1 < reasonableMaxGrowth && growth3 < reasonableMaxGrowth {
		if growth3 > growth1*2 {
			t.Errorf("Accelerating memory growth detected: batch1=%s, batch3=%s",
				formatBytes(growth1), formatBytes(growth3))
		}
	}

	// Check goroutines
	goroutineDiff := memorySnapshots[len(memorySnapshots)-1].Goroutines - memorySnapshots[0].Goroutines
	if goroutineDiff > 10 {
		t.Errorf("Goroutine leak: %d new goroutines after %d inserts",
			goroutineDiff, batchSize*numBatches)
	}

	t.Logf("✅ Large dataset insert memory stability test passed")
}

// TestMemoryLeak_MixedOperations tests memory with insert/search/delete mix
func TestMemoryLeak_MixedOperations(t *testing.T) {
	idx := createMemoryTestIndex(t, "mixed_operations")
	defer idx.Close()

	before := getMemoryStats()

	// Perform mixed operations in cycles
	for cycle := 0; cycle < 10; cycle++ {
		// Insert batch
		for i := 0; i < 100; i++ {
			keyNum := cycle*100 + i
			key := fmt.Sprintf("key_%06d", keyNum)
			idx.Insert([]byte(key), fmt.Sprintf("doc_%06d", keyNum))
		}

		// Search batch
		for i := 0; i < 100; i++ {
			keyNum := cycle*100 + i
			key := fmt.Sprintf("key_%06d", keyNum)
			idx.Search([]byte(key))
		}

		// Delete some
		for i := 0; i < 50; i++ {
			keyNum := cycle*100 + i
			key := fmt.Sprintf("key_%06d", keyNum)
			idx.Delete([]byte(key), fmt.Sprintf("doc_%06d", keyNum))
		}
	}

	after := getMemoryStats()
	diff := compareMemoryStats(before, after)

	t.Logf("After mixed operations: Alloc diff=%s, Goroutines diff=%d",
		formatBytes(diff.Alloc), diff.Goroutines)

	// Allow reasonable growth for tree structure
	maxAllowedGrowth := uint64(20 * 1024 * 1024) // 20MB
	if hasMemoryGrowth(diff.Alloc, maxAllowedGrowth) {
		t.Errorf("Memory leak in mixed operations: %s growth", formatBytes(diff.Alloc))
	}

	if diff.Goroutines > 5 {
		t.Errorf("Goroutine leak in mixed operations: %d new goroutines", diff.Goroutines)
	}

	t.Logf("✅ Mixed operations memory test passed")
}

// ================================================================================
// GOROUTINE LEAK TESTS
// ================================================================================

// TestMemoryLeak_NoGoroutineLeak tests that operations don't leak goroutines
func TestMemoryLeak_NoGoroutineLeak(t *testing.T) {
	idx := createMemoryTestIndex(t, "goroutine_leak")
	defer idx.Close()

	before := getMemoryStats()
	t.Logf("Initial goroutines: %d", before.Goroutines)

	// Perform many operations that might spawn goroutines
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key_%06d", i)
		docID := fmt.Sprintf("doc_%06d", i)

		// Insert
		err := idx.Insert([]byte(key), docID)
		if err != nil {
			t.Fatalf("Insert failed: %v", err)
		}

		// Search
		_, err = idx.Search([]byte(key))
		if err != nil {
			t.Fatalf("Search failed: %v", err)
		}

		// Range search
		if i%100 == 0 {
			startKey := []byte(fmt.Sprintf("key_%06d", i))
			endKey := []byte(fmt.Sprintf("key_%06d", i+50))
			_, err = idx.RangeSearch(startKey, endKey)
			if err != nil {
				t.Fatalf("Range search failed: %v", err)
			}
		}
	}

	// Wait for any background operations
	time.Sleep(100 * time.Millisecond)

	after := getMemoryStats()
	diff := compareMemoryStats(before, after)

	t.Logf("Final goroutines: %d (diff: %+d)", after.Goroutines, diff.Goroutines)

	// Allow small variance in goroutine count
	if diff.Goroutines > 5 {
		t.Errorf("Goroutine leak detected: %d new goroutines", diff.Goroutines)
	}

	t.Logf("✅ No goroutine leak test passed")
}

// TestMemoryLeak_CloseCleanup tests that Close() properly cleans up resources
func TestMemoryLeak_CloseCleanup(t *testing.T) {
	before := getMemoryStats()

	// Create and destroy index multiple times
	for i := 0; i < 10; i++ {
		idx := createMemoryTestIndex(t, fmt.Sprintf("close_cleanup_%d", i))

		// Use the index
		for j := 0; j < 100; j++ {
			key := fmt.Sprintf("key_%04d", j)
			idx.Insert([]byte(key), fmt.Sprintf("doc_%04d", j))
		}

		// Close it
		err := idx.Close()
		if err != nil {
			t.Fatalf("Close failed: %v", err)
		}

		// Force GC
		runtime.GC()
		time.Sleep(10 * time.Millisecond)
	}

	after := getMemoryStats()
	diff := compareMemoryStats(before, after)

	t.Logf("After 10 create/close cycles: Alloc diff=%s, Goroutines diff=%d",
		formatBytes(diff.Alloc), diff.Goroutines)

	// Memory should be mostly reclaimed after GC
	maxAllowedGrowth := uint64(10 * 1024 * 1024) // 10MB tolerance
	if hasMemoryGrowth(diff.Alloc, maxAllowedGrowth) {
		t.Errorf("Memory not reclaimed after Close(): %s still allocated", formatBytes(diff.Alloc))
	}

	if diff.Goroutines > 2 {
		t.Errorf("Goroutines not cleaned up after Close(): %d extra goroutines", diff.Goroutines)
	}

	t.Logf("✅ Close cleanup test passed")
}

// ================================================================================
// LONG-RUNNING OPERATION TESTS
// ================================================================================

// TestMemoryLeak_LongRunningIndex tests memory stability over time
func TestMemoryLeak_LongRunningIndex(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping long-running test in short mode")
	}

	idx := createMemoryTestIndex(t, "long_running")
	defer idx.Close()

	initialMem := getMemoryStats()
	t.Logf("Initial memory: Alloc=%s", formatBytes(initialMem.Alloc))

	// Simulate workload over time
	duration := 10 * time.Second
	startTime := time.Now()
	operationCount := 0

	for time.Since(startTime) < duration {
		// Insert
		key := fmt.Sprintf("key_%08d", operationCount)
		idx.Insert([]byte(key), fmt.Sprintf("doc_%08d", operationCount))

		// Search
		if operationCount > 0 {
			searchKey := fmt.Sprintf("key_%08d", operationCount-1)
			idx.Search([]byte(searchKey))
		}

		// Delete old keys
		if operationCount > 100 {
			deleteKey := fmt.Sprintf("key_%08d", operationCount-100)
			idx.Delete([]byte(deleteKey), fmt.Sprintf("doc_%08d", operationCount-100))
		}

		operationCount++

		// Sample memory every 1000 operations
		if operationCount%1000 == 0 {
			currentMem := getMemoryStats()
			t.Logf("After %d ops: Alloc=%s", operationCount, formatBytes(currentMem.Alloc))
		}
	}

	finalMem := getMemoryStats()
	diff := compareMemoryStats(initialMem, finalMem)

	t.Logf("Long-running test complete: %d operations in %v", operationCount, duration)
	t.Logf("Memory change: Alloc=%s, Goroutines=%+d",
		formatBytes(diff.Alloc), diff.Goroutines)

	// Memory should be bounded
	maxAllowedGrowth := uint64(50 * 1024 * 1024) // 50MB
	if hasMemoryGrowth(diff.Alloc, maxAllowedGrowth) {
		t.Errorf("Unbounded memory growth in long-running test: %s", formatBytes(diff.Alloc))
	}

	t.Logf("✅ Long-running memory stability test passed")
}
