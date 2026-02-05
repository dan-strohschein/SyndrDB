/*
SHARDED CACHES - High-Concurrency Cache Management for BundleStorageEngine

This file provides sharded cache implementations to eliminate global mutex contention
in BundleStorageEngine. Each cache type distributes entries across 64 shards,
enabling concurrent access to different keys without serialization.

PROBLEM SOLVED:
Previously, all concurrent operations contended on global mutexes (bufferMutex,
projectionMutex, fileReadCacheMutex, parsedDocsCacheMutex) even when accessing
different bundles or files. This created severe bottlenecks under high concurrency.

DESIGN:
- 64 shards per cache (consistent with ShardedWriteLockMap)
- Each shard has its own RWMutex protecting its map
- Shard selection via xxhash(key) & 63 for O(1) distribution
- Fast path: RLock for reads, only take write lock for modifications

PERFORMANCE CHARACTERISTICS:
- 150 connections → ~2.3 connections per shard average
- Reads: concurrent within same shard
- Writes: exclusive but only blocks same-shard operations

CACHES IMPLEMENTED:
- ShardedBufferCache: WriteBuffer instances keyed by file path
- ShardedManifestCache: ManifestManager instances keyed by bundle name
- ShardedProjectionCache: Projection field lists keyed by bundle name
- ShardedFileReadCache: File content cache keyed by file path
- ShardedParsedDocsCache: Parsed document cache keyed by bundleName:filePath
*/

package bundlestore

import (
	"sync"
	"time"

	"github.com/cespare/xxhash/v2"
)

const (
	// CacheShardCount is the number of shards for all cache types.
	// 64 shards matches ShardedWriteLockMap for consistency.
	CacheShardCount = 64
)

// ============================================================================
// ShardedBufferCache - Replaces bufferMutex + writeBuffers map
// ============================================================================

// bufferCacheShard holds a partition of the write buffer cache
type bufferCacheShard struct {
	mu      sync.RWMutex
	buffers map[string]*WriteBuffer // filePath -> WriteBuffer
}

// ShardedBufferCache provides concurrent access to write buffers
// keyed by file path, with sharding to reduce contention.
type ShardedBufferCache struct {
	shards [CacheShardCount]bufferCacheShard
}

// NewShardedBufferCache creates a new sharded buffer cache
func NewShardedBufferCache() *ShardedBufferCache {
	c := &ShardedBufferCache{}
	for i := range c.shards {
		c.shards[i].buffers = make(map[string]*WriteBuffer)
	}
	return c
}

// shardIndex returns the shard index for a given key
func (c *ShardedBufferCache) shardIndex(key string) uint64 {
	return xxhash.Sum64String(key) & (CacheShardCount - 1)
}

// Get retrieves a buffer by key, returns nil if not found
func (c *ShardedBufferCache) Get(key string) *WriteBuffer {
	idx := c.shardIndex(key)
	shard := &c.shards[idx]

	shard.mu.RLock()
	buffer := shard.buffers[key]
	shard.mu.RUnlock()
	return buffer
}

// GetOrCreate retrieves or creates a buffer using the provided factory function.
// The factory is only called if the key doesn't exist, and only once per key.
func (c *ShardedBufferCache) GetOrCreate(key string, factory func() (*WriteBuffer, error)) (*WriteBuffer, error) {
	idx := c.shardIndex(key)
	shard := &c.shards[idx]

	// Fast path: read lock for existing entry
	shard.mu.RLock()
	if buffer, ok := shard.buffers[key]; ok {
		shard.mu.RUnlock()
		return buffer, nil
	}
	shard.mu.RUnlock()

	// Slow path: create new buffer with write lock
	shard.mu.Lock()
	defer shard.mu.Unlock()

	// Double-check after acquiring write lock
	if buffer, ok := shard.buffers[key]; ok {
		return buffer, nil
	}

	buffer, err := factory()
	if err != nil {
		return nil, err
	}

	shard.buffers[key] = buffer
	return buffer, nil
}

// Set stores a buffer with the given key
func (c *ShardedBufferCache) Set(key string, buffer *WriteBuffer) {
	idx := c.shardIndex(key)
	shard := &c.shards[idx]

	shard.mu.Lock()
	shard.buffers[key] = buffer
	shard.mu.Unlock()
}

// Delete removes a buffer by key
func (c *ShardedBufferCache) Delete(key string) {
	idx := c.shardIndex(key)
	shard := &c.shards[idx]

	shard.mu.Lock()
	delete(shard.buffers, key)
	shard.mu.Unlock()
}

// Range iterates over all buffers with a read lock on each shard.
// The callback receives key and buffer; return false to stop iteration.
func (c *ShardedBufferCache) Range(fn func(key string, buffer *WriteBuffer) bool) {
	for i := range c.shards {
		shard := &c.shards[i]
		shard.mu.RLock()
		for k, v := range shard.buffers {
			if !fn(k, v) {
				shard.mu.RUnlock()
				return
			}
		}
		shard.mu.RUnlock()
	}
}

// RangeWithLock iterates over all buffers with a write lock on each shard.
// Use this when the callback needs to modify buffers.
func (c *ShardedBufferCache) RangeWithLock(fn func(key string, buffer *WriteBuffer) bool) {
	for i := range c.shards {
		shard := &c.shards[i]
		shard.mu.Lock()
		for k, v := range shard.buffers {
			if !fn(k, v) {
				shard.mu.Unlock()
				return
			}
		}
		shard.mu.Unlock()
	}
}

// Len returns the total number of buffers across all shards
func (c *ShardedBufferCache) Len() int {
	total := 0
	for i := range c.shards {
		shard := &c.shards[i]
		shard.mu.RLock()
		total += len(shard.buffers)
		shard.mu.RUnlock()
	}
	return total
}

// DeleteMatching deletes entries matching the predicate while calling a callback on each.
// This is used for closing and removing buffers for a specific bundle.
// The callback should perform cleanup (like Close()) before deletion.
// Returns the number of deleted entries.
func (c *ShardedBufferCache) DeleteMatching(match func(key string) bool, cleanup func(key string, buffer *WriteBuffer) error) (int, []error) {
	var errors []error
	deleted := 0

	for i := range c.shards {
		shard := &c.shards[i]
		shard.mu.Lock()

		// Collect keys to delete (can't delete while iterating)
		var toDelete []string
		for k := range shard.buffers {
			if match(k) {
				toDelete = append(toDelete, k)
			}
		}

		// Execute cleanup and delete
		for _, k := range toDelete {
			buffer := shard.buffers[k]
			if cleanup != nil {
				if err := cleanup(k, buffer); err != nil {
					errors = append(errors, err)
				}
			}
			delete(shard.buffers, k)
			deleted++
		}

		shard.mu.Unlock()
	}

	return deleted, errors
}

// Compact recreates the underlying maps for all shards with only current entries.
// PERFORMANCE FIX: Go's map delete() doesn't shrink the bucket array. After many
// file operations, the shard maps accumulate empty bucket slots that degrade
// iteration and memory performance. This method recreates each shard's map with
// only current entries, reclaiming memory and restoring lookup speed.
// Returns the total number of buffers after compaction.
func (c *ShardedBufferCache) Compact() int {
	totalBuffers := 0

	for i := range c.shards {
		shard := &c.shards[i]

		shard.mu.Lock()
		// Create new map sized exactly for current buffers
		newMap := make(map[string]*WriteBuffer, len(shard.buffers))
		for key, buffer := range shard.buffers {
			newMap[key] = buffer
		}
		shard.buffers = newMap
		totalBuffers += len(newMap)
		shard.mu.Unlock()
	}

	return totalBuffers
}

// ============================================================================
// ShardedManifestCache - Replaces manifestManagersMutex + manifestManagers map
// ============================================================================

// manifestCacheShard holds a partition of the manifest manager cache
type manifestCacheShard struct {
	mu       sync.RWMutex
	managers map[string]*ManifestManager // bundleName -> ManifestManager
}

// ShardedManifestCache provides concurrent access to manifest managers
// keyed by bundle name, with sharding to reduce contention.
type ShardedManifestCache struct {
	shards [CacheShardCount]manifestCacheShard
}

// NewShardedManifestCache creates a new sharded manifest cache
func NewShardedManifestCache() *ShardedManifestCache {
	c := &ShardedManifestCache{}
	for i := range c.shards {
		c.shards[i].managers = make(map[string]*ManifestManager)
	}
	return c
}

// shardIndex returns the shard index for a given bundle name
func (c *ShardedManifestCache) shardIndex(bundleName string) uint64 {
	return xxhash.Sum64String(bundleName) & (CacheShardCount - 1)
}

// Get retrieves a manifest manager by bundle name, returns nil if not found
func (c *ShardedManifestCache) Get(bundleName string) *ManifestManager {
	idx := c.shardIndex(bundleName)
	shard := &c.shards[idx]

	shard.mu.RLock()
	manager := shard.managers[bundleName]
	shard.mu.RUnlock()
	return manager
}

// GetOrCreate retrieves or creates a manifest manager using the provided factory.
func (c *ShardedManifestCache) GetOrCreate(bundleName string, factory func() (*ManifestManager, error)) (*ManifestManager, error) {
	idx := c.shardIndex(bundleName)
	shard := &c.shards[idx]

	// Fast path: read lock for existing entry
	shard.mu.RLock()
	if manager, ok := shard.managers[bundleName]; ok {
		shard.mu.RUnlock()
		return manager, nil
	}
	shard.mu.RUnlock()

	// Slow path: create new manager with write lock
	shard.mu.Lock()
	defer shard.mu.Unlock()

	// Double-check after acquiring write lock
	if manager, ok := shard.managers[bundleName]; ok {
		return manager, nil
	}

	manager, err := factory()
	if err != nil {
		return nil, err
	}

	shard.managers[bundleName] = manager
	return manager, nil
}

// Set stores a manifest manager with the given bundle name
func (c *ShardedManifestCache) Set(bundleName string, manager *ManifestManager) {
	idx := c.shardIndex(bundleName)
	shard := &c.shards[idx]

	shard.mu.Lock()
	shard.managers[bundleName] = manager
	shard.mu.Unlock()
}

// Delete removes a manifest manager by bundle name
func (c *ShardedManifestCache) Delete(bundleName string) {
	idx := c.shardIndex(bundleName)
	shard := &c.shards[idx]

	shard.mu.Lock()
	delete(shard.managers, bundleName)
	shard.mu.Unlock()
}

// Range iterates over all manifest managers with a read lock on each shard.
// The callback receives key and manager; return false to stop iteration.
func (c *ShardedManifestCache) Range(fn func(key string, manager *ManifestManager) bool) {
	for i := range c.shards {
		shard := &c.shards[i]
		shard.mu.RLock()
		for k, v := range shard.managers {
			if !fn(k, v) {
				shard.mu.RUnlock()
				return
			}
		}
		shard.mu.RUnlock()
	}
}

// GetOrCreateSimple retrieves or creates a manifest manager using a factory that cannot fail.
// This is a convenience method for cases where the factory never returns an error.
func (c *ShardedManifestCache) GetOrCreateSimple(bundleName string, factory func() *ManifestManager) *ManifestManager {
	idx := c.shardIndex(bundleName)
	shard := &c.shards[idx]

	// Fast path: read lock for existing entry
	shard.mu.RLock()
	if manager, ok := shard.managers[bundleName]; ok {
		shard.mu.RUnlock()
		return manager
	}
	shard.mu.RUnlock()

	// Slow path: create new manager with write lock
	shard.mu.Lock()
	defer shard.mu.Unlock()

	// Double-check after acquiring write lock
	if manager, ok := shard.managers[bundleName]; ok {
		return manager
	}

	manager := factory()
	shard.managers[bundleName] = manager
	return manager
}

// Compact recreates the underlying maps for all shards with only current entries.
// PERFORMANCE FIX: Go's map delete() doesn't shrink the bucket array. After many
// bundle create/delete cycles, the shard maps accumulate empty bucket slots that degrade
// iteration and memory performance. This method recreates each shard's map with
// only current entries, reclaiming memory and restoring lookup speed.
// Returns the total number of entries after compaction.
func (c *ShardedManifestCache) Compact() int {
	totalManagers := 0

	for i := range c.shards {
		shard := &c.shards[i]

		shard.mu.Lock()
		// Create new map sized exactly for current managers
		newMap := make(map[string]*ManifestManager, len(shard.managers))
		for key, manager := range shard.managers {
			newMap[key] = manager
		}
		shard.managers = newMap
		totalManagers += len(newMap)
		shard.mu.Unlock()
	}

	return totalManagers
}

// ============================================================================
// ShardedProjectionCache - Replaces projectionMutex + projectionFields map
// ============================================================================

// projectionCacheShard holds a partition of the projection cache
type projectionCacheShard struct {
	mu     sync.RWMutex
	fields map[string][]string // bundleName -> projection field names
}

// ShardedProjectionCache provides concurrent access to projection field lists
// keyed by bundle name, with sharding to reduce contention.
type ShardedProjectionCache struct {
	shards [CacheShardCount]projectionCacheShard
}

// NewShardedProjectionCache creates a new sharded projection cache
func NewShardedProjectionCache() *ShardedProjectionCache {
	c := &ShardedProjectionCache{}
	for i := range c.shards {
		c.shards[i].fields = make(map[string][]string)
	}
	return c
}

// shardIndex returns the shard index for a given bundle name
func (c *ShardedProjectionCache) shardIndex(bundleName string) uint64 {
	return xxhash.Sum64String(bundleName) & (CacheShardCount - 1)
}

// Get retrieves projection fields by bundle name. Returns nil if not found.
// Returns the cached slice; callers must not mutate it (read-only). The cache
// stores a copy on Set so the returned slice is safe to read concurrently.
func (c *ShardedProjectionCache) Get(bundleName string) []string {
	idx := c.shardIndex(bundleName)
	shard := &c.shards[idx]

	shard.mu.RLock()
	fields := shard.fields[bundleName]
	shard.mu.RUnlock()
	return fields
}

// Set stores projection fields for a bundle name. Stores a copy so the cache
// is not affected by caller mutations and Get can return without copying.
// Pass nil to clear projection for this bundle.
func (c *ShardedProjectionCache) Set(bundleName string, fields []string) {
	idx := c.shardIndex(bundleName)
	shard := &c.shards[idx]

	shard.mu.Lock()
	if fields == nil {
		delete(shard.fields, bundleName)
	} else {
		stored := make([]string, len(fields))
		copy(stored, fields)
		shard.fields[bundleName] = stored
	}
	shard.mu.Unlock()
}

// Delete removes projection fields by bundle name
func (c *ShardedProjectionCache) Delete(bundleName string) {
	idx := c.shardIndex(bundleName)
	shard := &c.shards[idx]

	shard.mu.Lock()
	delete(shard.fields, bundleName)
	shard.mu.Unlock()
}

// Has checks if projection fields exist for a bundle name
func (c *ShardedProjectionCache) Has(bundleName string) bool {
	idx := c.shardIndex(bundleName)
	shard := &c.shards[idx]

	shard.mu.RLock()
	_, exists := shard.fields[bundleName]
	shard.mu.RUnlock()
	return exists
}

// Compact recreates the underlying maps for all shards with only current entries.
// PERFORMANCE FIX: Go's map delete() doesn't shrink the bucket array. After many
// projection create/delete cycles, the shard maps accumulate empty bucket slots that degrade
// iteration and memory performance. This method recreates each shard's map with
// only current entries, reclaiming memory and restoring lookup speed.
// Returns the total number of entries after compaction.
func (c *ShardedProjectionCache) Compact() int {
	totalEntries := 0

	for i := range c.shards {
		shard := &c.shards[i]

		shard.mu.Lock()
		// Create new map sized exactly for current entries
		newMap := make(map[string][]string, len(shard.fields))
		for key, fields := range shard.fields {
			newMap[key] = fields
		}
		shard.fields = newMap
		totalEntries += len(newMap)
		shard.mu.Unlock()
	}

	return totalEntries
}

// Flush completely clears all entries from all shards.
// This is more aggressive than Compact - it removes all cached data to free memory.
func (c *ShardedProjectionCache) Flush() {
	for i := range c.shards {
		shard := &c.shards[i]

		shard.mu.Lock()
		// Replace with empty map
		shard.fields = make(map[string][]string)
		shard.mu.Unlock()
	}
}

// ============================================================================
// ShardedFileReadCache - Replaces fileReadCacheMutex + fileReadCache map
// ============================================================================

// fileReadCacheShard holds a partition of the file read cache
type fileReadCacheShard struct {
	mu    sync.RWMutex
	cache map[string]*fileReadCacheEntry // filePath -> cached content
}

// ShardedFileReadCache provides concurrent access to cached file contents
// keyed by file path, with sharding to reduce contention.
type ShardedFileReadCache struct {
	shards [CacheShardCount]fileReadCacheShard
}

// NewShardedFileReadCache creates a new sharded file read cache
func NewShardedFileReadCache() *ShardedFileReadCache {
	c := &ShardedFileReadCache{}
	for i := range c.shards {
		c.shards[i].cache = make(map[string]*fileReadCacheEntry)
	}
	return c
}

// shardIndex returns the shard index for a given file path
func (c *ShardedFileReadCache) shardIndex(filePath string) uint64 {
	return xxhash.Sum64String(filePath) & (CacheShardCount - 1)
}

// Get retrieves a cache entry by file path, returns nil if not found
func (c *ShardedFileReadCache) Get(filePath string) *fileReadCacheEntry {
	idx := c.shardIndex(filePath)
	shard := &c.shards[idx]

	shard.mu.RLock()
	entry := shard.cache[filePath]
	shard.mu.RUnlock()
	return entry
}

// Set stores a cache entry for a file path
func (c *ShardedFileReadCache) Set(filePath string, entry *fileReadCacheEntry) {
	idx := c.shardIndex(filePath)
	shard := &c.shards[idx]

	shard.mu.Lock()
	shard.cache[filePath] = entry
	shard.mu.Unlock()
}

// Delete removes a cache entry by file path
func (c *ShardedFileReadCache) Delete(filePath string) {
	idx := c.shardIndex(filePath)
	shard := &c.shards[idx]

	shard.mu.Lock()
	delete(shard.cache, filePath)
	shard.mu.Unlock()
}

// GetShard returns the shard for a given file path with read lock held.
// Caller must call shard.mu.RUnlock() when done.
func (c *ShardedFileReadCache) GetShardRLock(filePath string) (*fileReadCacheShard, uint64) {
	idx := c.shardIndex(filePath)
	shard := &c.shards[idx]
	shard.mu.RLock()
	return shard, idx
}

// GetShardLock returns the shard for a given file path with write lock held.
// Caller must call shard.mu.Unlock() when done.
func (c *ShardedFileReadCache) GetShardLock(filePath string) (*fileReadCacheShard, uint64) {
	idx := c.shardIndex(filePath)
	shard := &c.shards[idx]
	shard.mu.Lock()
	return shard, idx
}

// Len returns the total number of entries across all shards
func (c *ShardedFileReadCache) Len() int {
	total := 0
	for i := range c.shards {
		shard := &c.shards[i]
		shard.mu.RLock()
		total += len(shard.cache)
		shard.mu.RUnlock()
	}
	return total
}

// ShardLen returns the number of entries in a specific shard
func (c *ShardedFileReadCache) ShardLen(shardIdx uint64) int {
	shard := &c.shards[shardIdx]
	shard.mu.RLock()
	l := len(shard.cache)
	shard.mu.RUnlock()
	return l
}

// EvictLRUFromShard evicts the least-recently-used entry from a specific shard.
// Returns true if an entry was evicted.
func (c *ShardedFileReadCache) EvictLRUFromShard(shardIdx uint64) bool {
	shard := &c.shards[shardIdx]
	shard.mu.Lock()
	defer shard.mu.Unlock()

	if len(shard.cache) == 0 {
		return false
	}

	// Find LRU entry
	var oldestKey string
	var oldestTime int64 = 1<<63 - 1 // Max int64
	for k, v := range shard.cache {
		t := v.lastAccess.Load()
		if t < oldestTime {
			oldestTime = t
			oldestKey = k
		}
	}

	if oldestKey != "" {
		delete(shard.cache, oldestKey)
		return true
	}
	return false
}

// evictLRUFromShardLocked evicts the least-recently-used entry from a specific shard.
// Caller must hold the shard's write lock.
func (c *ShardedFileReadCache) evictLRUFromShardLocked(shard *fileReadCacheShard) bool {
	if len(shard.cache) == 0 {
		return false
	}

	var oldestKey string
	var oldestTime int64 = 1<<63 - 1
	for k, v := range shard.cache {
		t := v.lastAccess.Load()
		if t < oldestTime {
			oldestTime = t
			oldestKey = k
		}
	}

	if oldestKey != "" {
		delete(shard.cache, oldestKey)
		return true
	}
	return false
}

// GetOrCreate retrieves or creates a cache entry using the provided factory function.
// Includes LRU eviction when maxEntriesPerShard is exceeded.
// The factory is only called if the key doesn't exist.
func (c *ShardedFileReadCache) GetOrCreate(filePath string, maxEntriesPerShard int, factory func() ([]byte, error)) ([]byte, error) {
	idx := c.shardIndex(filePath)
	shard := &c.shards[idx]

	// Fast path: read lock for existing entry
	shard.mu.RLock()
	if entry, ok := shard.cache[filePath]; ok {
		entry.lastAccess.Store(time.Now().UnixNano())
		data := entry.data
		shard.mu.RUnlock()
		return data, nil
	}
	shard.mu.RUnlock()

	// Read data outside of lock
	data, err := factory()
	if err != nil {
		return nil, err
	}

	// Slow path: create new entry with write lock
	shard.mu.Lock()
	defer shard.mu.Unlock()

	// Double-check after acquiring write lock
	if entry, ok := shard.cache[filePath]; ok {
		return entry.data, nil
	}

	// Evict LRU entries until we have room in this shard
	for len(shard.cache) >= maxEntriesPerShard {
		c.evictLRUFromShardLocked(shard)
	}

	e := &fileReadCacheEntry{data: data}
	e.lastAccess.Store(time.Now().UnixNano())
	shard.cache[filePath] = e
	return data, nil
}

// DeleteMatching deletes all entries matching the predicate
func (c *ShardedFileReadCache) DeleteMatching(match func(filePath string) bool) int {
	deleted := 0
	for i := range c.shards {
		shard := &c.shards[i]
		shard.mu.Lock()

		var toDelete []string
		for k := range shard.cache {
			if match(k) {
				toDelete = append(toDelete, k)
			}
		}
		for _, k := range toDelete {
			delete(shard.cache, k)
			deleted++
		}

		shard.mu.Unlock()
	}
	return deleted
}

// Compact recreates the underlying maps for all shards with only current entries.
// PERFORMANCE FIX: Go's map delete() doesn't shrink the bucket array. After many
// file read/evict cycles, the shard maps accumulate empty bucket slots that degrade
// iteration and memory performance. This method recreates each shard's map with
// only current entries, reclaiming memory and restoring lookup speed.
// Returns the total number of entries after compaction.
func (c *ShardedFileReadCache) Compact() int {
	totalEntries := 0

	for i := range c.shards {
		shard := &c.shards[i]

		shard.mu.Lock()
		// Create new map sized exactly for current entries
		newMap := make(map[string]*fileReadCacheEntry, len(shard.cache))
		for key, entry := range shard.cache {
			newMap[key] = entry
		}
		shard.cache = newMap
		totalEntries += len(newMap)
		shard.mu.Unlock()
	}

	return totalEntries
}

// Flush completely clears all entries from all shards.
// This is more aggressive than Compact - it removes all cached data to free memory.
func (c *ShardedFileReadCache) Flush() {
	for i := range c.shards {
		shard := &c.shards[i]

		shard.mu.Lock()
		// Replace with empty map
		shard.cache = make(map[string]*fileReadCacheEntry)
		shard.mu.Unlock()
	}
}

// ============================================================================
// ShardedParsedDocsCache - Replaces parsedDocsCacheMutex + parsedDocsCache map
// ============================================================================

// parsedDocsCacheShard holds a partition of the parsed docs cache
type parsedDocsCacheShard struct {
	mu    sync.RWMutex
	cache map[string]*parsedDocsCacheEntry // "bundleName:filePath" -> parsed docs
}

// ShardedParsedDocsCache provides concurrent access to cached parsed documents
// keyed by "bundleName:filePath", with sharding to reduce contention.
type ShardedParsedDocsCache struct {
	shards [CacheShardCount]parsedDocsCacheShard
}

// NewShardedParsedDocsCache creates a new sharded parsed docs cache
func NewShardedParsedDocsCache() *ShardedParsedDocsCache {
	c := &ShardedParsedDocsCache{}
	for i := range c.shards {
		c.shards[i].cache = make(map[string]*parsedDocsCacheEntry)
	}
	return c
}

// shardIndex returns the shard index for a given cache key
func (c *ShardedParsedDocsCache) shardIndex(cacheKey string) uint64 {
	return xxhash.Sum64String(cacheKey) & (CacheShardCount - 1)
}

// Get retrieves a cache entry by key, returns nil if not found
func (c *ShardedParsedDocsCache) Get(cacheKey string) *parsedDocsCacheEntry {
	idx := c.shardIndex(cacheKey)
	shard := &c.shards[idx]

	shard.mu.RLock()
	entry := shard.cache[cacheKey]
	shard.mu.RUnlock()
	return entry
}

// GetAndTouch retrieves a cache entry by key and updates its lastAccess time.
// Returns nil if not found.
func (c *ShardedParsedDocsCache) GetAndTouch(cacheKey string) *parsedDocsCacheEntry {
	idx := c.shardIndex(cacheKey)
	shard := &c.shards[idx]

	shard.mu.RLock()
	entry := shard.cache[cacheKey]
	if entry != nil {
		entry.lastAccess.Store(time.Now().UnixNano())
	}
	shard.mu.RUnlock()
	return entry
}

// Set stores a cache entry for a key
func (c *ShardedParsedDocsCache) Set(cacheKey string, entry *parsedDocsCacheEntry) {
	idx := c.shardIndex(cacheKey)
	shard := &c.shards[idx]

	shard.mu.Lock()
	shard.cache[cacheKey] = entry
	shard.mu.Unlock()
}

// Delete removes a cache entry by key
func (c *ShardedParsedDocsCache) Delete(cacheKey string) {
	idx := c.shardIndex(cacheKey)
	shard := &c.shards[idx]

	shard.mu.Lock()
	delete(shard.cache, cacheKey)
	shard.mu.Unlock()
}

// DeleteByPrefix removes all entries with keys starting with the given prefix.
// Useful for invalidating all cached entries for a bundle.
func (c *ShardedParsedDocsCache) DeleteByPrefix(prefix string) {
	// Must check all shards since we don't know which shards contain matching keys
	for i := range c.shards {
		shard := &c.shards[i]
		shard.mu.Lock()
		for k := range shard.cache {
			if len(k) >= len(prefix) && k[:len(prefix)] == prefix {
				delete(shard.cache, k)
			}
		}
		shard.mu.Unlock()
	}
}

// Len returns the total number of entries across all shards
func (c *ShardedParsedDocsCache) Len() int {
	total := 0
	for i := range c.shards {
		shard := &c.shards[i]
		shard.mu.RLock()
		total += len(shard.cache)
		shard.mu.RUnlock()
	}
	return total
}

// ShardLen returns the number of entries in a specific shard
func (c *ShardedParsedDocsCache) ShardLen(shardIdx uint64) int {
	shard := &c.shards[shardIdx]
	shard.mu.RLock()
	l := len(shard.cache)
	shard.mu.RUnlock()
	return l
}

// evictLRUFromShardLocked evicts the least-recently-used entry from a specific shard.
// Caller must hold the shard's write lock.
func (c *ShardedParsedDocsCache) evictLRUFromShardLocked(shard *parsedDocsCacheShard) bool {
	if len(shard.cache) == 0 {
		return false
	}

	var oldestKey string
	var oldestTime int64 = 1<<63 - 1
	for k, v := range shard.cache {
		t := v.lastAccess.Load()
		if t < oldestTime {
			oldestTime = t
			oldestKey = k
		}
	}

	if oldestKey != "" {
		delete(shard.cache, oldestKey)
		return true
	}
	return false
}

// SetWithLRU stores a cache entry with LRU eviction if the shard is at capacity.
// maxEntriesPerShard is the maximum entries allowed per shard before eviction.
func (c *ShardedParsedDocsCache) SetWithLRU(cacheKey string, entry *parsedDocsCacheEntry, maxEntriesPerShard int) {
	idx := c.shardIndex(cacheKey)
	shard := &c.shards[idx]

	shard.mu.Lock()
	defer shard.mu.Unlock()

	// Evict LRU entries until we have room
	for len(shard.cache) >= maxEntriesPerShard {
		c.evictLRUFromShardLocked(shard)
	}

	shard.cache[cacheKey] = entry
}

// Range iterates over all entries with a read lock on each shard.
// The callback receives key and entry; return false to stop iteration.
func (c *ShardedParsedDocsCache) Range(fn func(key string, entry *parsedDocsCacheEntry) bool) {
	for i := range c.shards {
		shard := &c.shards[i]
		shard.mu.RLock()
		for k, v := range shard.cache {
			if !fn(k, v) {
				shard.mu.RUnlock()
				return
			}
		}
		shard.mu.RUnlock()
	}
}

// Compact recreates the underlying maps for all shards with only current entries.
// PERFORMANCE FIX: Go's map delete() doesn't shrink the bucket array. After many
// document parse/evict cycles, the shard maps accumulate empty bucket slots that degrade
// iteration and memory performance. This method recreates each shard's map with
// only current entries, reclaiming memory and restoring lookup speed.
// Returns the total number of entries after compaction.
func (c *ShardedParsedDocsCache) Compact() int {
	totalEntries := 0

	for i := range c.shards {
		shard := &c.shards[i]

		shard.mu.Lock()
		// Create new map sized exactly for current entries
		newMap := make(map[string]*parsedDocsCacheEntry, len(shard.cache))
		for key, entry := range shard.cache {
			newMap[key] = entry
		}
		shard.cache = newMap
		totalEntries += len(newMap)
		shard.mu.Unlock()
	}

	return totalEntries
}

// Flush completely clears all entries from all shards.
// This is more aggressive than Compact - it removes all cached data to free memory.
func (c *ShardedParsedDocsCache) Flush() {
	for i := range c.shards {
		shard := &c.shards[i]

		shard.mu.Lock()
		// Replace with empty map
		shard.cache = make(map[string]*parsedDocsCacheEntry)
		shard.mu.Unlock()
	}
}
