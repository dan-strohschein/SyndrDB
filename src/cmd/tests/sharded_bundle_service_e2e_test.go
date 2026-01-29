/*
PHASE 5 E2E TESTS: Sharded BundleService Caches

This file contains end-to-end tests for the Phase 5 lock contention fixes
that shard BundleService mutexes for concurrent access:
- ShardedBundleOperationLockMap (replaces bundleLockMutex)
- ShardedSchemaManagerMap (replaces schemaManagerMutex)
- ShardedLoadedDatabasesMap (replaces indexMemoryMutex for LRU tracking)
- ShardedPageCacheMap (replaces pageCacheMutex for document->page mappings)

These tests verify:
1. Concurrent access to different bundles doesn't block
2. Thread safety under high concurrency
3. FIFO eviction works correctly
4. No race conditions detected by -race flag

Run with: go test -v -race ./src/cmd/tests/ -run TestPhase5
*/

package main

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"syndrdb/src/internal/domain/bundle"

	"go.uber.org/zap"
)

// ============================================================================
// TEST: ShardedBundleOperationLockMap
// ============================================================================

func TestPhase5_ShardedBundleOperationLockMap_ConcurrentAccess(t *testing.T) {
	// Create logger
	logger, _ := zap.NewDevelopment()
	sugar := logger.Sugar()

	// Create sharded lock map
	lockMap := bundle.NewShardedBundleOperationLockMap(sugar)

	// Test concurrent access from 100 goroutines
	const numGoroutines = 100
	const numBundles = 20

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	start := time.Now()

	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()

			// Each goroutine accesses multiple bundles
			for j := 0; j < 10; j++ {
				bundleName := fmt.Sprintf("bundle_%d", (id+j)%numBundles)

				// Get the lock (creates if not exists)
				lock := lockMap.Get(bundleName)
				if lock == nil {
					t.Errorf("Got nil lock for bundle %s", bundleName)
					return
				}

				// Simulate some work with the lock
				time.Sleep(time.Microsecond)
			}
		}(i)
	}

	wg.Wait()
	elapsed := time.Since(start)

	t.Logf("Phase 5 ShardedBundleOperationLockMap: %d goroutines x 10 ops completed in %v", numGoroutines, elapsed)

	// Verify all bundles were created
	for i := 0; i < numBundles; i++ {
		bundleName := fmt.Sprintf("bundle_%d", i)
		lock := lockMap.Get(bundleName)
		if lock == nil {
			t.Errorf("Expected lock for %s to exist", bundleName)
		}
	}

	// Test delete
	lockMap.Delete("bundle_0")
}

// ============================================================================
// TEST: ShardedSchemaManagerMap
// ============================================================================

func TestPhase5_ShardedSchemaManagerMap_ConcurrentAccess(t *testing.T) {
	// Create sharded schema manager map
	schemaMap := bundle.NewShardedSchemaManagerMap()

	// Test concurrent access
	const numGoroutines = 100
	const numDatabases = 20

	var wg sync.WaitGroup
	var getCount atomic.Int64
	var createCount atomic.Int64

	wg.Add(numGoroutines)

	start := time.Now()

	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()

			for j := 0; j < 10; j++ {
				dbName := fmt.Sprintf("database_%d", (id+j)%numDatabases)

				// Try to get first
				if manager, exists := schemaMap.Get(dbName); exists {
					_ = manager
					getCount.Add(1)
				} else {
					// Set a nil value (we don't have real schema managers in test)
					schemaMap.Set(dbName, nil)
					createCount.Add(1)
				}

				time.Sleep(time.Microsecond)
			}
		}(i)
	}

	wg.Wait()
	elapsed := time.Since(start)

	t.Logf("Phase 5 ShardedSchemaManagerMap: %d goroutines x 10 ops completed in %v (gets: %d, creates: %d)",
		numGoroutines, elapsed, getCount.Load(), createCount.Load())

	// Test delete
	schemaMap.Delete("database_0")
}

// ============================================================================
// TEST: ShardedLoadedDatabasesMap
// ============================================================================

func TestPhase5_ShardedLoadedDatabasesMap_ConcurrentAccess(t *testing.T) {
	// Create sharded loaded databases map
	loadedMap := bundle.NewShardedLoadedDatabasesMap()

	// Test concurrent access
	const numGoroutines = 100
	const numDatabases = 20

	var wg sync.WaitGroup
	var touchCount atomic.Int64
	var setCount atomic.Int64

	wg.Add(numGoroutines)

	start := time.Now()

	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()

			for j := 0; j < 10; j++ {
				dbName := fmt.Sprintf("database_%d", (id+j)%numDatabases)

				// Try to touch (update last access time)
				if loadedMap.Touch(dbName) {
					touchCount.Add(1)
				} else {
					// Set initial value
					loadedMap.Set(dbName, time.Now())
					setCount.Add(1)
				}

				time.Sleep(time.Microsecond)
			}
		}(i)
	}

	wg.Wait()
	elapsed := time.Since(start)

	t.Logf("Phase 5 ShardedLoadedDatabasesMap: %d goroutines x 10 ops completed in %v (touches: %d, sets: %d)",
		numGoroutines, elapsed, touchCount.Load(), setCount.Load())

	// Test eviction detection
	// First set some old entries
	loadedMap.Set("old_db_1", time.Now().Add(-20*time.Minute))
	loadedMap.Set("old_db_2", time.Now().Add(-15*time.Minute))

	// Find databases to evict (idle > 10 minutes)
	toEvict := loadedMap.ForEachWithEviction(10 * time.Minute)
	if len(toEvict) < 2 {
		t.Errorf("Expected at least 2 databases to evict, got %d", len(toEvict))
	}

	// Verify eviction candidates include old databases
	foundOld1, foundOld2 := false, false
	for _, db := range toEvict {
		if db == "old_db_1" {
			foundOld1 = true
		}
		if db == "old_db_2" {
			foundOld2 = true
		}
	}
	if !foundOld1 || !foundOld2 {
		t.Errorf("Eviction candidates should include old_db_1 and old_db_2, got: %v", toEvict)
	}
}

// ============================================================================
// TEST: ShardedPageCacheMap
// ============================================================================

func TestPhase5_ShardedPageCacheMap_ConcurrentAccess(t *testing.T) {
	// Create logger
	logger, _ := zap.NewDevelopment()
	sugar := logger.Sugar()

	// Create sharded page cache with max 100 entries per bundle
	pageCache := bundle.NewShardedPageCacheMap(100, sugar)

	// Test concurrent access
	const numGoroutines = 100
	const numBundles = 10
	const docsPerBundle = 50

	var wg sync.WaitGroup
	var setCount atomic.Int64
	var getHits atomic.Int64
	var getMisses atomic.Int64

	wg.Add(numGoroutines)

	start := time.Now()

	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()

			for j := 0; j < 20; j++ {
				bundleName := fmt.Sprintf("bundle_%d", id%numBundles)
				docID := fmt.Sprintf("doc_%d_%d", id, j)
				pageID := uint32((id + j) % 10)

				// Alternate between set and get
				if j%2 == 0 {
					pageCache.SetPageID(bundleName, docID, pageID)
					setCount.Add(1)
				} else {
					if _, found := pageCache.GetPageID(bundleName, docID); found {
						getHits.Add(1)
					} else {
						getMisses.Add(1)
					}
				}

				time.Sleep(time.Microsecond)
			}
		}(i)
	}

	wg.Wait()
	elapsed := time.Since(start)

	t.Logf("Phase 5 ShardedPageCacheMap: %d goroutines x 20 ops completed in %v (sets: %d, hits: %d, misses: %d)",
		numGoroutines, elapsed, setCount.Load(), getHits.Load(), getMisses.Load())

	// Verify cache size
	totalSize := pageCache.Size()
	t.Logf("Total cached entries: %d", totalSize)

	// Test GetAllForBundle
	bundle0Cache := pageCache.GetAllForBundle("bundle_0")
	if bundle0Cache != nil {
		t.Logf("bundle_0 has %d cached entries", len(bundle0Cache))
	}

	// Test InvalidateDocument
	pageCache.SetPageID("test_bundle", "doc_to_invalidate", 123)
	if _, found := pageCache.GetPageID("test_bundle", "doc_to_invalidate"); !found {
		t.Error("Expected doc_to_invalidate to be cached")
	}
	pageCache.InvalidateDocument("test_bundle", "doc_to_invalidate")
	if _, found := pageCache.GetPageID("test_bundle", "doc_to_invalidate"); found {
		t.Error("Expected doc_to_invalidate to be invalidated")
	}

	// Test InvalidateBundle
	pageCache.InvalidateBundle("bundle_0")
	if pageCache.GetAllForBundle("bundle_0") != nil {
		t.Error("Expected bundle_0 cache to be invalidated")
	}
}

// ============================================================================
// TEST: FIFO Eviction
// ============================================================================

func TestPhase5_ShardedPageCacheMap_FIFOEviction(t *testing.T) {
	// Create logger
	logger, _ := zap.NewDevelopment()
	sugar := logger.Sugar()

	// Create sharded page cache with max 5 entries per bundle
	pageCache := bundle.NewShardedPageCacheMap(5, sugar)

	bundleName := "eviction_test_bundle"

	// Add 5 documents (should all fit)
	for i := 0; i < 5; i++ {
		docID := fmt.Sprintf("doc_%d", i)
		pageCache.SetPageID(bundleName, docID, uint32(i))
	}

	// Verify all 5 are cached
	for i := 0; i < 5; i++ {
		docID := fmt.Sprintf("doc_%d", i)
		if _, found := pageCache.GetPageID(bundleName, docID); !found {
			t.Errorf("Expected %s to be cached", docID)
		}
	}

	// Add 6th document - should evict doc_0 (FIFO)
	pageCache.SetPageID(bundleName, "doc_5", 5)

	// Verify doc_0 was evicted
	if _, found := pageCache.GetPageID(bundleName, "doc_0"); found {
		t.Error("Expected doc_0 to be evicted (FIFO)")
	}

	// Verify doc_5 is cached
	if _, found := pageCache.GetPageID(bundleName, "doc_5"); !found {
		t.Error("Expected doc_5 to be cached")
	}

	// Verify docs 1-4 are still cached
	for i := 1; i < 5; i++ {
		docID := fmt.Sprintf("doc_%d", i)
		if _, found := pageCache.GetPageID(bundleName, docID); !found {
			t.Errorf("Expected %s to still be cached", docID)
		}
	}

	t.Log("FIFO eviction working correctly")
}

// ============================================================================
// TEST: High Concurrency Stress Test
// ============================================================================

func TestPhase5_AllShardedCaches_HighConcurrency(t *testing.T) {
	// Create logger
	logger, _ := zap.NewDevelopment()
	sugar := logger.Sugar()

	// Create all sharded caches
	lockMap := bundle.NewShardedBundleOperationLockMap(sugar)
	schemaMap := bundle.NewShardedSchemaManagerMap()
	loadedMap := bundle.NewShardedLoadedDatabasesMap()
	pageCache := bundle.NewShardedPageCacheMap(1000, sugar)

	// Stress test with 200 goroutines
	const numGoroutines = 200
	const opsPerGoroutine = 50

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	start := time.Now()

	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()

			for j := 0; j < opsPerGoroutine; j++ {
				bundleName := fmt.Sprintf("bundle_%d", (id+j)%50)
				dbName := fmt.Sprintf("db_%d", (id+j)%20)
				docID := fmt.Sprintf("doc_%d_%d", id, j)

				// Test all caches
				_ = lockMap.Get(bundleName)
				schemaMap.Get(dbName)
				loadedMap.Touch(dbName)
				pageCache.SetPageID(bundleName, docID, uint32(j))
				pageCache.GetPageID(bundleName, docID)
			}
		}(i)
	}

	wg.Wait()
	elapsed := time.Since(start)

	totalOps := numGoroutines * opsPerGoroutine * 5 // 5 operations per iteration
	opsPerSecond := float64(totalOps) / elapsed.Seconds()

	t.Logf("Phase 5 High Concurrency: %d goroutines x %d iterations x 5 ops = %d total ops in %v (%.0f ops/sec)",
		numGoroutines, opsPerGoroutine, totalOps, elapsed, opsPerSecond)

	// Success criteria: should complete without deadlock or race
	if elapsed > 5*time.Second {
		t.Errorf("High concurrency test took too long: %v (expected < 5s)", elapsed)
	}
}
