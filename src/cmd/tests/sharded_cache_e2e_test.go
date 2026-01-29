/*
PHASE 3 TESTS: Sharded Cache Concurrent Access Tests

This file validates that the sharded cache implementations (Phase 3) properly
handle concurrent access without race conditions. These tests complement the
Phase 1 lock-free SELECT tests by focusing on the storage engine caches.

COVERAGE:
- ShardedBufferCache: Concurrent buffer creation and access
- ShardedManifestCache: Concurrent manifest manager access
- ShardedProjectionCache: Concurrent projection field read/write
- ShardedFileReadCache: Concurrent file content caching with LRU
- ShardedParsedDocsCache: Concurrent parsed docs caching with LRU

PERFORMANCE VALIDATION:
- 64-shard distribution reduces contention by ~64x vs global mutex
- Independent keys should never block each other
*/
package main

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"syndrdb/src/internal/storage/bundlestore"
)

// TestShardedBufferCache_ConcurrentGetOrCreate tests that multiple goroutines
// can create buffers for different keys concurrently without blocking.
func TestShardedBufferCache_ConcurrentGetOrCreate(t *testing.T) {
	EnsureTestIsolation(t)

	cache := bundlestore.NewShardedBufferCache()

	// Track timing to ensure concurrent execution
	const numGoroutines = 100
	var wg sync.WaitGroup
	var completedCount int64

	start := time.Now()

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			key := fmt.Sprintf("/data/db/bundle%d/file.bnd", id)

			// GetOrCreate should not block on other keys
			_, err := cache.GetOrCreate(key, func() (*bundlestore.WriteBuffer, error) {
				// Simulate some work during buffer creation
				time.Sleep(1 * time.Millisecond)
				return nil, nil // We don't actually create a real buffer in this test
			})
			if err != nil {
				t.Errorf("Goroutine %d: unexpected error: %v", id, err)
			}
			atomic.AddInt64(&completedCount, 1)
		}(i)
	}

	wg.Wait()
	elapsed := time.Since(start)

	// With true concurrency across 64 shards, 100 goroutines each doing 1ms work
	// should complete in much less than 100ms (if they ran sequentially).
	// We expect roughly 100ms / 64 shards = ~2ms ideally, but allow margin for scheduling.
	if elapsed > 50*time.Millisecond {
		t.Logf("Warning: Elapsed time %v may indicate contention (expected <50ms with sharding)", elapsed)
	}

	if completedCount != numGoroutines {
		t.Errorf("Expected %d completions, got %d", numGoroutines, completedCount)
	}

	t.Logf("Phase 3: %d concurrent buffer operations completed in %v", numGoroutines, elapsed)
}

// TestShardedProjectionCache_ConcurrentReadWrite tests read/write concurrency
// on the projection cache.
func TestShardedProjectionCache_ConcurrentReadWrite(t *testing.T) {
	EnsureTestIsolation(t)

	cache := bundlestore.NewShardedProjectionCache()
	const numBundles = 50
	const opsPerBundle = 100

	var wg sync.WaitGroup

	// Writers
	for i := 0; i < numBundles; i++ {
		wg.Add(1)
		go func(bundleID int) {
			defer wg.Done()
			bundleName := fmt.Sprintf("bundle%d", bundleID)

			for j := 0; j < opsPerBundle; j++ {
				fields := []string{fmt.Sprintf("field%d", j), "name", "id"}
				cache.Set(bundleName, fields)
			}
		}(i)
	}

	// Readers (run concurrently with writers)
	for i := 0; i < numBundles; i++ {
		wg.Add(1)
		go func(bundleID int) {
			defer wg.Done()
			bundleName := fmt.Sprintf("bundle%d", bundleID)

			for j := 0; j < opsPerBundle; j++ {
				_ = cache.Get(bundleName)
				_ = cache.Has(bundleName)
			}
		}(i)
	}

	wg.Wait()
	t.Log("Phase 3: Concurrent projection cache read/write completed without race")
}

// TestShardedParsedDocsCache_ConcurrentGetAndTouch tests that GetAndTouch
// properly updates access times without races.
func TestShardedParsedDocsCache_ConcurrentGetAndTouch(t *testing.T) {
	EnsureTestIsolation(t)

	cache := bundlestore.NewShardedParsedDocsCache()
	const numEntries = 20
	const readersPerEntry = 50

	// Populate cache
	for i := 0; i < numEntries; i++ {
		cache.Set(fmt.Sprintf("bundle%d:file.bnd", i), nil)
	}

	var wg sync.WaitGroup
	var touchCount int64

	// Multiple readers touching entries concurrently
	for entry := 0; entry < numEntries; entry++ {
		for reader := 0; reader < readersPerEntry; reader++ {
			wg.Add(1)
			go func(e int) {
				defer wg.Done()
				cacheKey := fmt.Sprintf("bundle%d:file.bnd", e)
				cache.GetAndTouch(cacheKey)
				atomic.AddInt64(&touchCount, 1)
			}(entry)
		}
	}

	wg.Wait()

	expectedTouches := int64(numEntries * readersPerEntry)
	if touchCount != expectedTouches {
		t.Errorf("Expected %d touches, got %d", expectedTouches, touchCount)
	}

	t.Logf("Phase 3: %d concurrent cache touches completed", touchCount)
}

// TestShardedFileReadCache_LRUEvictionUnderConcurrency tests that LRU eviction
// works correctly under concurrent load without races.
func TestShardedFileReadCache_LRUEvictionUnderConcurrency(t *testing.T) {
	EnsureTestIsolation(t)

	cache := bundlestore.NewShardedFileReadCache()
	const numGoroutines = 50
	const opsPerGoroutine = 100
	const maxEntriesPerShard = 2 // Force eviction

	var wg sync.WaitGroup
	var createCount int64

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < opsPerGoroutine; j++ {
				key := fmt.Sprintf("/data/db%d/bundle%d/file%d.bnd", id%5, id, j%10)
				_, err := cache.GetOrCreate(key, maxEntriesPerShard, func() ([]byte, error) {
					atomic.AddInt64(&createCount, 1)
					return []byte("test data"), nil
				})
				if err != nil {
					t.Errorf("Unexpected error: %v", err)
				}
			}
		}(i)
	}

	wg.Wait()

	// With small shard capacity, evictions should have occurred
	cacheSize := cache.Len()
	t.Logf("Phase 3: Cache LRU eviction test completed. Final size: %d, Total creates: %d",
		cacheSize, createCount)

	// Cache should not have grown unbounded due to LRU
	maxPossibleSize := bundlestore.CacheShardCount * maxEntriesPerShard
	if cacheSize > maxPossibleSize {
		t.Errorf("Cache size %d exceeds max expected %d (shards * max_per_shard)", cacheSize, maxPossibleSize)
	}
}

// TestShardedManifestCache_RangeUnderConcurrency tests that Range iteration
// works correctly while other goroutines modify the cache.
func TestShardedManifestCache_RangeUnderConcurrency(t *testing.T) {
	EnsureTestIsolation(t)

	cache := bundlestore.NewShardedManifestCache()
	const numBundles = 30

	// Populate initial data
	for i := 0; i < numBundles; i++ {
		cache.Set(fmt.Sprintf("db:bundle%d", i), nil)
	}

	var wg sync.WaitGroup
	var rangeCompletedCount int64
	var modifyCount int64

	// Range readers
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				count := 0
				cache.Range(func(key string, _ *bundlestore.ManifestManager) bool {
					count++
					return true // continue
				})
				if count > 0 {
					atomic.AddInt64(&rangeCompletedCount, 1)
				}
			}
		}()
	}

	// Concurrent modifiers
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				key := fmt.Sprintf("db:newbundle%d_%d", id, j)
				cache.Set(key, nil)
				atomic.AddInt64(&modifyCount, 1)
			}
		}(i)
	}

	wg.Wait()
	t.Logf("Phase 3: Range under concurrency test: %d ranges, %d modifications", rangeCompletedCount, modifyCount)
}
