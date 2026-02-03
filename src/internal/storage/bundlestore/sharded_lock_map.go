/*
SHARDED LOCK MAP - High-Concurrency Lock Management

This file provides a sharded map implementation for storing per-bundle locks.
The sharded design distributes lock contention across 64 shards, enabling
concurrent access to locks for different bundles without global serialization.

PROBLEM SOLVED:
Previously, all 150+ concurrent connections contended on a single mutex
(writeLocksMutex) just to look up per-bundle locks. This created a severe
bottleneck where threads serialized even for operations on different bundles.

DESIGN:
- 64 shards (matching LockManager pattern for consistency)
- Each shard has its own RWMutex protecting a map[string]*sync.RWMutex
- Shard selection via xxhash(bundleName) & 63 for O(1) distribution
- Fast path: RLock for existing locks, only take write lock for creation

PERFORMANCE CHARACTERISTICS:
- 150 connections → ~2.3 connections per shard average
- Reads (get existing lock): concurrent within same shard
- Writes (create new lock): exclusive but only blocks same-shard operations
- Both reads and writes benefit equally (ideal for 60/40 workloads)

USAGE:
  locks := NewShardedWriteLockMap()
  lock := locks.Get(bundleName)  // Creates if not exists
  lock.Lock()
  defer lock.Unlock()
*/

package bundlestore

import (
	"sync"

	"github.com/cespare/xxhash/v2"
)

const (
	// ShardedLockMapShards is the number of shards for lock distribution.
	// 64 shards matches LockManager for consistency and provides good distribution.
	ShardedLockMapShards = 64
)

// writeLockShard holds a partition of the write lock map
type writeLockShard struct {
	mu    sync.RWMutex
	locks map[string]*sync.RWMutex
}

// ShardedWriteLockMap provides concurrent access to per-bundle write locks
// with sharding to reduce contention under high concurrency.
type ShardedWriteLockMap struct {
	shards [ShardedLockMapShards]writeLockShard
}

// NewShardedWriteLockMap creates a new sharded write lock map
func NewShardedWriteLockMap() *ShardedWriteLockMap {
	m := &ShardedWriteLockMap{}
	for i := range m.shards {
		m.shards[i].locks = make(map[string]*sync.RWMutex)
	}
	return m
}

// shardIndex returns the shard index for a given bundle name
func (m *ShardedWriteLockMap) shardIndex(bundleName string) uint64 {
	return xxhash.Sum64String(bundleName) & (ShardedLockMapShards - 1)
}

// Get retrieves or creates a write lock for the specified bundle.
// Thread-safe: uses read-lock fast path for existing locks,
// write-lock slow path for creation with double-check.
func (m *ShardedWriteLockMap) Get(bundleName string) *sync.RWMutex {
	idx := m.shardIndex(bundleName)
	shard := &m.shards[idx]

	// Fast path: read lock for existing entry (common case)
	shard.mu.RLock()
	if lock, ok := shard.locks[bundleName]; ok {
		shard.mu.RUnlock()
		return lock
	}
	shard.mu.RUnlock()

	// Slow path: create new lock with write lock
	shard.mu.Lock()
	defer shard.mu.Unlock()

	// Double-check after acquiring write lock (another goroutine may have created it)
	if lock, ok := shard.locks[bundleName]; ok {
		return lock
	}

	lock := &sync.RWMutex{}
	shard.locks[bundleName] = lock
	return lock
}

// Delete removes a lock for the specified bundle.
// Used during bundle deletion to clean up resources.
func (m *ShardedWriteLockMap) Delete(bundleName string) {
	idx := m.shardIndex(bundleName)
	shard := &m.shards[idx]

	shard.mu.Lock()
	defer shard.mu.Unlock()
	delete(shard.locks, bundleName)
}

// mutexShard holds a partition of a mutex map (for non-RWMutex locks)
type mutexShard struct {
	mu    sync.RWMutex
	locks map[string]*sync.Mutex
}

// ShardedMutexMap provides concurrent access to per-bundle mutexes
// with sharding to reduce contention under high concurrency.
type ShardedMutexMap struct {
	shards [ShardedLockMapShards]mutexShard
}

// NewShardedMutexMap creates a new sharded mutex map
func NewShardedMutexMap() *ShardedMutexMap {
	m := &ShardedMutexMap{}
	for i := range m.shards {
		m.shards[i].locks = make(map[string]*sync.Mutex)
	}
	return m
}

// shardIndex returns the shard index for a given bundle name
func (m *ShardedMutexMap) shardIndex(bundleName string) uint64 {
	return xxhash.Sum64String(bundleName) & (ShardedLockMapShards - 1)
}

// Get retrieves or creates a mutex for the specified bundle.
func (m *ShardedMutexMap) Get(bundleName string) *sync.Mutex {
	idx := m.shardIndex(bundleName)
	shard := &m.shards[idx]

	// Fast path: read lock for existing entry
	shard.mu.RLock()
	if lock, ok := shard.locks[bundleName]; ok {
		shard.mu.RUnlock()
		return lock
	}
	shard.mu.RUnlock()

	// Slow path: create new lock
	shard.mu.Lock()
	defer shard.mu.Unlock()

	if lock, ok := shard.locks[bundleName]; ok {
		return lock
	}

	lock := &sync.Mutex{}
	shard.locks[bundleName] = lock
	return lock
}

// Delete removes a mutex for the specified bundle.
func (m *ShardedMutexMap) Delete(bundleName string) {
	idx := m.shardIndex(bundleName)
	shard := &m.shards[idx]

	shard.mu.Lock()
	defer shard.mu.Unlock()
	delete(shard.locks, bundleName)
}

// indexCacheShard holds a partition of the index cache
type indexCacheShard struct {
	mu    sync.RWMutex
	cache map[string]map[string]interface{} // bundleName -> indexName -> index instance
}

// ShardedIndexCache provides concurrent access to loaded index instances
// with sharding to reduce contention under high concurrency.
type ShardedIndexCache struct {
	shards [ShardedLockMapShards]indexCacheShard
}

// NewShardedIndexCache creates a new sharded index cache
func NewShardedIndexCache() *ShardedIndexCache {
	c := &ShardedIndexCache{}
	for i := range c.shards {
		c.shards[i].cache = make(map[string]map[string]interface{})
	}
	return c
}

// shardIndex returns the shard index for a given bundle name
func (c *ShardedIndexCache) shardIndex(bundleName string) uint64 {
	return xxhash.Sum64String(bundleName) & (ShardedLockMapShards - 1)
}

// Get retrieves an index from the cache.
// Returns the index and true if found, nil and false otherwise.
func (c *ShardedIndexCache) Get(bundleName, indexName string) (interface{}, bool) {
	idx := c.shardIndex(bundleName)
	shard := &c.shards[idx]

	shard.mu.RLock()
	defer shard.mu.RUnlock()

	bundleIndexes, ok := shard.cache[bundleName]
	if !ok {
		return nil, false
	}
	index, ok := bundleIndexes[indexName]
	return index, ok
}

// Set stores an index in the cache.
func (c *ShardedIndexCache) Set(bundleName, indexName string, index interface{}) {
	idx := c.shardIndex(bundleName)
	shard := &c.shards[idx]

	shard.mu.Lock()
	defer shard.mu.Unlock()

	if shard.cache[bundleName] == nil {
		shard.cache[bundleName] = make(map[string]interface{})
	}
	shard.cache[bundleName][indexName] = index
}

// Delete removes an index from the cache.
func (c *ShardedIndexCache) Delete(bundleName, indexName string) {
	idx := c.shardIndex(bundleName)
	shard := &c.shards[idx]

	shard.mu.Lock()
	defer shard.mu.Unlock()

	if bundleIndexes, ok := shard.cache[bundleName]; ok {
		delete(bundleIndexes, indexName)
		if len(bundleIndexes) == 0 {
			delete(shard.cache, bundleName)
		}
	}
}

// DeleteBundle removes all indexes for a bundle from the cache.
func (c *ShardedIndexCache) DeleteBundle(bundleName string) {
	idx := c.shardIndex(bundleName)
	shard := &c.shards[idx]

	shard.mu.Lock()
	defer shard.mu.Unlock()

	delete(shard.cache, bundleName)
}

// GetBundleIndexes returns all indexes for a bundle.
// Returns a copy to avoid concurrent modification issues.
func (c *ShardedIndexCache) GetBundleIndexes(bundleName string) map[string]interface{} {
	idx := c.shardIndex(bundleName)
	shard := &c.shards[idx]

	shard.mu.RLock()
	defer shard.mu.RUnlock()

	bundleIndexes, ok := shard.cache[bundleName]
	if !ok {
		return nil
	}

	// Return a copy to avoid concurrent modification
	result := make(map[string]interface{}, len(bundleIndexes))
	for k, v := range bundleIndexes {
		result[k] = v
	}
	return result
}

// GetOrLoad atomically gets or loads an index using the provided loader function.
// This provides double-check locking semantics to prevent multiple goroutines from
// loading the same index simultaneously, which is critical for BTree indexes that
// share file handles.
//
// Parameters:
//   - bundleName: The bundle containing the index
//   - indexName: The name of the index to get or load
//   - loader: Function to load the index if not cached (may return error)
//
// Returns:
//   - interface{}: The cached or newly loaded index
//   - error: Any error from the loader function
func (c *ShardedIndexCache) GetOrLoad(bundleName, indexName string, loader func() (interface{}, error)) (interface{}, error) {
	idx := c.shardIndex(bundleName)
	shard := &c.shards[idx]

	// Fast path: check cache with read lock
	shard.mu.RLock()
	if bundleIndexes, ok := shard.cache[bundleName]; ok {
		if index, found := bundleIndexes[indexName]; found {
			shard.mu.RUnlock()
			return index, nil
		}
	}
	shard.mu.RUnlock()

	// Slow path: acquire write lock and re-check before loading
	shard.mu.Lock()
	defer shard.mu.Unlock()

	// Double-check: another goroutine may have loaded it while we waited
	if bundleIndexes, ok := shard.cache[bundleName]; ok {
		if index, found := bundleIndexes[indexName]; found {
			return index, nil
		}
	}

	// Load the index
	index, err := loader()
	if err != nil {
		return nil, err
	}

	// Store in cache
	if shard.cache[bundleName] == nil {
		shard.cache[bundleName] = make(map[string]interface{})
	}
	shard.cache[bundleName][indexName] = index

	return index, nil
}

// ForEach iterates over all cached bundles and their indexes, calling the callback
// for each bundle. The callback receives a copy of the bundle's index map to avoid
// holding locks during iteration.
// Returns immediately if callback returns false.
//
// IMPORTANT: This method is designed for maintenance operations like LRU eviction.
// For high-frequency access patterns, use Get/GetOrLoad instead.
func (c *ShardedIndexCache) ForEach(callback func(bundleName string, indexes map[string]interface{}) bool) {
	for i := range c.shards {
		shard := &c.shards[i]

		// Get snapshot of bundle names and indexes for this shard
		shard.mu.RLock()
		bundles := make(map[string]map[string]interface{}, len(shard.cache))
		for bundleName, indexes := range shard.cache {
			// Copy the indexes map to avoid holding lock during callback
			indexesCopy := make(map[string]interface{}, len(indexes))
			for k, v := range indexes {
				indexesCopy[k] = v
			}
			bundles[bundleName] = indexesCopy
		}
		shard.mu.RUnlock()

		// Call callback for each bundle (no lock held)
		for bundleName, indexes := range bundles {
			if !callback(bundleName, indexes) {
				return
			}
		}
	}
}

// DeleteIndex removes a specific index from the cache (alias for Delete).
// Provided for clarity when performing eviction operations.
func (c *ShardedIndexCache) DeleteIndex(bundleName, indexName string) {
	c.Delete(bundleName, indexName)
}

// Flush completely clears all cached indexes from all shards.
// This is used for aggressive cache cleanup when all clients disconnect,
// preventing accumulated data from degrading performance on reconnect.
// Note: This will cause indexes to be reloaded from disk on next access.
// WARNING: This does NOT close the indexes - use FlushWithClose to properly
// release file handles and resources.
func (c *ShardedIndexCache) Flush() {
	for i := range c.shards {
		shard := &c.shards[i]
		shard.mu.Lock()
		shard.cache = make(map[string]map[string]interface{})
		shard.mu.Unlock()
	}
}

// IndexCloser is implemented by index types that need cleanup
type IndexCloser interface {
	Close() error
}

// FlushWithClose closes all cached indexes and then clears the cache.
// This properly releases file handles and resources before removing indexes.
// Use this when all clients disconnect to ensure clean state.
// Returns the count of indexes closed and any errors encountered.
func (c *ShardedIndexCache) FlushWithClose() (int, []error) {
	var errors []error
	count := 0

	for i := range c.shards {
		shard := &c.shards[i]
		shard.mu.Lock()

		// Close all indexes in this shard
		for bundleName, indexes := range shard.cache {
			for indexName, index := range indexes {
				if closer, ok := index.(IndexCloser); ok {
					if err := closer.Close(); err != nil {
						errors = append(errors, err)
					} else {
						count++
					}
				}
				// Remove from map even if close failed
				delete(indexes, indexName)
			}
			delete(shard.cache, bundleName)
		}

		// Replace with empty map
		shard.cache = make(map[string]map[string]interface{})
		shard.mu.Unlock()
	}

	return count, errors
}
