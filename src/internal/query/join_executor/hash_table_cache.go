// Package joinexecutor provides query join execution strategies.
//
// File: hash_table_cache.go
//
// This file implements hash table caching for hash join operations.
// Two implementations are provided:
//
// 1. HashTableCache (DEPRECATED) - Single-lock LRU cache. Retained for fallback.
// 2. ShardedHashTableCache - 16-shard concurrent cache for high-throughput workloads.
//
// Cache Key: (bundleName, joinKey)
// Cache invalidation is EXPLICIT - the BundleService calls Invalidate(bundleName)
// when any INSERT, UPDATE, or DELETE occurs on that bundle.
//
// This approach is more reliable than trying to detect staleness via timestamps
// or document counts, which have historically had accuracy issues.
//
// PERFORMANCE: Reduces 400ms+ hash table builds to <1ms cache hits for repeated JOINs.
// Memory: Configurable maximum cache size with LRU eviction.
//
// Use GetHashTableCacheInterface() to obtain the appropriate cache implementation
// based on the UseShardedHashTableCache setting.
package joinexecutor

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/cespare/xxhash/v2"
	"syndrdb/src/internal/query/bloomfilter"
	"syndrdb/src/pkg/settings"
)

// HashTableCacheKey uniquely identifies a cached hash table
type HashTableCacheKey struct {
	BundleName string // Name of the bundle
	JoinKey    string // Field name used for join
}

// CachedHashTable stores a hash table along with its metadata
type CachedHashTable struct {
	HashTable   HashTable                // The cached hash table
	BloomFilter *bloomfilter.BloomFilter // Optional bloom filter
	Stats       *ScanStats               // Statistics from build phase
	CreatedAt   time.Time                // When this was cached
	Hits        int64                    // Number of cache hits
	ByteSize    int64                    // Estimated memory size
}

// hashTableCacheEntry is used for LRU tracking
type hashTableCacheEntry struct {
	key   HashTableCacheKey
	value *CachedHashTable
}

// DEPRECATED: HashTableCache is the legacy single-lock LRU cache for hash join hash tables.
// This implementation uses a single sync.RWMutex which causes contention under high concurrency.
// Use ShardedHashTableCache instead for production workloads with concurrent JOIN operations.
// TODO: Remove deprecated HashTableCache once sharded implementation is confirmed stable in production.
//
// HashTableCache provides an LRU cache for hash join hash tables.
// Thread-safe for concurrent access.
type HashTableCache struct {
	mu            sync.RWMutex
	cache         map[HashTableCacheKey]*CachedHashTable
	lruOrder      []HashTableCacheKey // Front = oldest, back = newest
	maxEntries    int                 // Maximum number of entries
	maxMemorySize int64               // Maximum total memory usage
	currentMemory int64               // Current memory usage
	hits          int64               // Total cache hits
	misses        int64               // Total cache misses
}

// Global hash table cache instance
var globalHashTableCache *HashTableCache
var hashTableCacheOnce sync.Once

// DEPRECATED: GetHashTableCache returns the legacy single-lock cache instance.
// Use GetHashTableCacheInterface() instead, which respects the UseShardedHashTableCache setting.
// TODO: Remove deprecated GetHashTableCache once sharded implementation is confirmed stable in production.
//
// GetHashTableCache returns the singleton hash table cache instance.
// Lazy initialization with 32 max entries and 256MB memory limit.
func GetHashTableCache() *HashTableCache {
	hashTableCacheOnce.Do(func() {
		globalHashTableCache = NewHashTableCache(32, 256*1024*1024) // 32 entries, 256MB
	})
	return globalHashTableCache
}

// NewHashTableCache creates a new hash table cache with the specified limits.
func NewHashTableCache(maxEntries int, maxMemorySize int64) *HashTableCache {
	return &HashTableCache{
		cache:         make(map[HashTableCacheKey]*CachedHashTable),
		lruOrder:      make([]HashTableCacheKey, 0, maxEntries),
		maxEntries:    maxEntries,
		maxMemorySize: maxMemorySize,
	}
}

// Get retrieves a cached hash table if it exists and is still valid.
// Returns (hashTable, bloomFilter, stats, found).
func (c *HashTableCache) Get(key HashTableCacheKey) (HashTable, *bloomfilter.BloomFilter, *ScanStats, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	entry, exists := c.cache[key]
	if !exists {
		c.misses++
		return nil, nil, nil, false
	}

	// Move to end of LRU (most recently used)
	c.moveToEnd(key)

	entry.Hits++
	c.hits++

	return entry.HashTable, entry.BloomFilter, entry.Stats, true
}

// TryGet attempts to retrieve a cached hash table without blocking.
// Returns (hashTable, bloomFilter, stats, found, acquired).
// If acquired is false, the lock was contended and the caller should proceed without cache.
func (c *HashTableCache) TryGet(key HashTableCacheKey) (HashTable, *bloomfilter.BloomFilter, *ScanStats, bool, bool) {
	metrics := GetLockMetrics()

	if !c.mu.TryLock() {
		// Lock contended - record failure and return without blocking
		metrics.RecordCacheAttempt(true)
		return nil, nil, nil, false, false
	}
	defer c.mu.Unlock()
	metrics.RecordCacheAttempt(false)

	entry, exists := c.cache[key]
	if !exists {
		c.misses++
		return nil, nil, nil, false, true
	}

	// Move to end of LRU (most recently used)
	c.moveToEnd(key)

	entry.Hits++
	c.hits++

	return entry.HashTable, entry.BloomFilter, entry.Stats, true, true
}

// Put stores a hash table in the cache.
// May evict older entries to stay within limits.
func (c *HashTableCache) Put(key HashTableCacheKey, hashTable HashTable, bloom *bloomfilter.BloomFilter, stats *ScanStats) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Calculate memory size
	byteSize := hashTable.GetMemoryUsage()
	if bloom != nil {
		byteSize += bloom.MemoryUsage() // Bloom filter memory in bytes
	}

	// Evict entries if needed
	for len(c.cache) >= c.maxEntries || (c.maxMemorySize > 0 && c.currentMemory+byteSize > c.maxMemorySize) {
		if len(c.lruOrder) == 0 {
			break
		}
		c.evictOldest()
	}

	// Create cache entry
	entry := &CachedHashTable{
		HashTable:   hashTable,
		BloomFilter: bloom,
		Stats:       stats,
		CreatedAt:   time.Now(),
		ByteSize:    byteSize,
	}

	c.cache[key] = entry
	c.lruOrder = append(c.lruOrder, key)
	c.currentMemory += byteSize
}

// Invalidate removes all cached hash tables for a given bundle.
// Called when bundle data changes.
func (c *HashTableCache) Invalidate(bundleName string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Find and remove all entries for this bundle
	var keysToRemove []HashTableCacheKey
	for key := range c.cache {
		if key.BundleName == bundleName {
			keysToRemove = append(keysToRemove, key)
		}
	}

	for _, key := range keysToRemove {
		c.removeEntry(key)
	}
}

// Clear removes all entries from the cache.
func (c *HashTableCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Clear all hash tables
	for _, entry := range c.cache {
		entry.HashTable.Clear()
	}

	c.cache = make(map[HashTableCacheKey]*CachedHashTable)
	c.lruOrder = c.lruOrder[:0]
	c.currentMemory = 0
}

// Stats returns cache statistics.
func (c *HashTableCache) Stats() (entries int, memoryUsed int64, hits int64, misses int64) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.cache), c.currentMemory, c.hits, c.misses
}

// moveToEnd moves a key to the end of the LRU list (most recently used).
// Caller must hold the lock.
func (c *HashTableCache) moveToEnd(key HashTableCacheKey) {
	// Find and remove the key
	for i, k := range c.lruOrder {
		if k == key {
			c.lruOrder = append(c.lruOrder[:i], c.lruOrder[i+1:]...)
			break
		}
	}
	// Add to end
	c.lruOrder = append(c.lruOrder, key)
}

// evictOldest removes the oldest (least recently used) entry.
// Caller must hold the lock.
func (c *HashTableCache) evictOldest() {
	if len(c.lruOrder) == 0 {
		return
	}

	oldestKey := c.lruOrder[0]
	c.lruOrder = c.lruOrder[1:]
	c.removeEntry(oldestKey)
}

// removeEntry removes an entry from the cache.
// Caller must hold the lock.
func (c *HashTableCache) removeEntry(key HashTableCacheKey) {
	entry, exists := c.cache[key]
	if !exists {
		return
	}

	// Clear the hash table to free memory
	entry.HashTable.Clear()

	c.currentMemory -= entry.ByteSize
	delete(c.cache, key)

	// Remove from LRU order
	for i, k := range c.lruOrder {
		if k == key {
			c.lruOrder = append(c.lruOrder[:i], c.lruOrder[i+1:]...)
			break
		}
	}
}

// =============================================================================
// HashTableCacheInterface - Common interface for cache implementations
// =============================================================================

// HashTableCacheInterface defines the contract for hash table cache implementations.
// Both the deprecated HashTableCache and new ShardedHashTableCache implement this interface.
type HashTableCacheInterface interface {
	// Get retrieves a cached hash table if it exists.
	// Returns (hashTable, bloomFilter, stats, found).
	Get(key HashTableCacheKey) (HashTable, *bloomfilter.BloomFilter, *ScanStats, bool)

	// TryGet attempts to retrieve a cached hash table without blocking.
	// Returns (hashTable, bloomFilter, stats, found, acquired).
	// If acquired is false, the caller should proceed without cache (lock was contended).
	TryGet(key HashTableCacheKey) (HashTable, *bloomfilter.BloomFilter, *ScanStats, bool, bool)

	// Put stores a hash table in the cache.
	Put(key HashTableCacheKey, hashTable HashTable, bloom *bloomfilter.BloomFilter, stats *ScanStats)

	// Invalidate removes all cached hash tables for a given bundle.
	Invalidate(bundleName string)

	// Clear removes all entries from the cache.
	Clear()

	// Stats returns cache statistics: (entries, memoryUsed, hits, misses).
	Stats() (int, int64, int64, int64)
}

// Ensure both cache types implement the interface
var _ HashTableCacheInterface = (*HashTableCache)(nil)
var _ HashTableCacheInterface = (*ShardedHashTableCache)(nil)

// =============================================================================
// ShardedHashTableCache - High-concurrency cache implementation
// =============================================================================

const (
	// hashTableCacheShardCount is the number of shards for the cache.
	// 16 shards provides good distribution with minimal overhead.
	hashTableCacheShardCount = 16
)

// hashTableCacheShard represents a single shard of the cache.
// Each shard has its own lock for independent concurrent access.
type hashTableCacheShard struct {
	mu            sync.RWMutex
	cache         map[HashTableCacheKey]*CachedHashTable
	lruOrder      []HashTableCacheKey
	maxEntries    int
	maxMemory     int64
	currentMemory int64
}

// ShardedHashTableCache is a high-concurrency hash table cache that
// distributes entries across multiple shards to reduce lock contention.
// Each shard operates independently with its own lock, allowing parallel
// access to different bundle caches.
type ShardedHashTableCache struct {
	shards        [hashTableCacheShardCount]*hashTableCacheShard
	hits          atomic.Int64
	misses        atomic.Int64
	maxEntries    int   // Total max entries across all shards
	maxMemorySize int64 // Total max memory across all shards
}

// Global sharded cache instance
var globalShardedHashTableCache *ShardedHashTableCache
var shardedHashTableCacheOnce sync.Once

// GetShardedHashTableCache returns the singleton sharded cache instance.
func GetShardedHashTableCache() *ShardedHashTableCache {
	shardedHashTableCacheOnce.Do(func() {
		// 32 entries total, 256MB memory - same as legacy cache
		globalShardedHashTableCache = NewShardedHashTableCache(32, 256*1024*1024)
	})
	return globalShardedHashTableCache
}

// NewShardedHashTableCache creates a new sharded hash table cache.
// Entries and memory are distributed evenly across shards.
func NewShardedHashTableCache(maxEntries int, maxMemorySize int64) *ShardedHashTableCache {
	cache := &ShardedHashTableCache{
		maxEntries:    maxEntries,
		maxMemorySize: maxMemorySize,
	}

	// Distribute limits across shards
	entriesPerShard := maxEntries / hashTableCacheShardCount
	if entriesPerShard < 1 {
		entriesPerShard = 1
	}
	memoryPerShard := maxMemorySize / hashTableCacheShardCount

	for i := 0; i < hashTableCacheShardCount; i++ {
		cache.shards[i] = &hashTableCacheShard{
			cache:      make(map[HashTableCacheKey]*CachedHashTable),
			lruOrder:   make([]HashTableCacheKey, 0, entriesPerShard),
			maxEntries: entriesPerShard,
			maxMemory:  memoryPerShard,
		}
	}

	return cache
}

// getShard returns the shard for a given cache key using xxhash.
func (c *ShardedHashTableCache) getShard(key HashTableCacheKey) *hashTableCacheShard {
	h := xxhash.Sum64String(key.BundleName)
	return c.shards[h%hashTableCacheShardCount]
}

// Get retrieves a cached hash table if it exists.
// Returns (hashTable, bloomFilter, stats, found).
func (c *ShardedHashTableCache) Get(key HashTableCacheKey) (HashTable, *bloomfilter.BloomFilter, *ScanStats, bool) {
	shard := c.getShard(key)

	shard.mu.Lock()
	defer shard.mu.Unlock()

	entry, exists := shard.cache[key]
	if !exists {
		c.misses.Add(1)
		return nil, nil, nil, false
	}

	// Move to end of LRU (most recently used)
	shard.moveToEnd(key)

	entry.Hits++
	c.hits.Add(1)

	return entry.HashTable, entry.BloomFilter, entry.Stats, true
}

// TryGet attempts to retrieve a cached hash table without blocking.
// Returns (hashTable, bloomFilter, stats, found, acquired).
// If acquired is false, the shard lock was contended and the caller should proceed without cache.
func (c *ShardedHashTableCache) TryGet(key HashTableCacheKey) (HashTable, *bloomfilter.BloomFilter, *ScanStats, bool, bool) {
	shard := c.getShard(key)
	metrics := GetLockMetrics()

	if !shard.mu.TryLock() {
		// Lock contended - record failure and return without blocking
		metrics.RecordCacheAttempt(true)
		return nil, nil, nil, false, false
	}
	defer shard.mu.Unlock()
	metrics.RecordCacheAttempt(false)

	entry, exists := shard.cache[key]
	if !exists {
		c.misses.Add(1)
		return nil, nil, nil, false, true
	}

	// Move to end of LRU (most recently used)
	shard.moveToEnd(key)

	entry.Hits++
	c.hits.Add(1)

	return entry.HashTable, entry.BloomFilter, entry.Stats, true, true
}

// Put stores a hash table in the cache.
func (c *ShardedHashTableCache) Put(key HashTableCacheKey, hashTable HashTable, bloom *bloomfilter.BloomFilter, stats *ScanStats) {
	shard := c.getShard(key)

	shard.mu.Lock()
	defer shard.mu.Unlock()

	// Calculate memory size
	byteSize := hashTable.GetMemoryUsage()
	if bloom != nil {
		byteSize += bloom.MemoryUsage()
	}

	// Evict entries if needed
	for len(shard.cache) >= shard.maxEntries || (shard.maxMemory > 0 && shard.currentMemory+byteSize > shard.maxMemory) {
		if len(shard.lruOrder) == 0 {
			break
		}
		shard.evictOldest()
	}

	// Create cache entry
	entry := &CachedHashTable{
		HashTable:   hashTable,
		BloomFilter: bloom,
		Stats:       stats,
		CreatedAt:   time.Now(),
		ByteSize:    byteSize,
	}

	shard.cache[key] = entry
	shard.lruOrder = append(shard.lruOrder, key)
	shard.currentMemory += byteSize
}

// Invalidate removes all cached hash tables for a given bundle.
// This scans only the relevant shard since all entries for a bundle
// hash to the same shard.
func (c *ShardedHashTableCache) Invalidate(bundleName string) {
	// Calculate the shard for this bundle
	h := xxhash.Sum64String(bundleName)
	shard := c.shards[h%hashTableCacheShardCount]

	shard.mu.Lock()
	defer shard.mu.Unlock()

	// Find and remove all entries for this bundle
	var keysToRemove []HashTableCacheKey
	for key := range shard.cache {
		if key.BundleName == bundleName {
			keysToRemove = append(keysToRemove, key)
		}
	}

	for _, key := range keysToRemove {
		shard.removeEntry(key)
	}
}

// Clear removes all entries from the cache.
func (c *ShardedHashTableCache) Clear() {
	for i := 0; i < hashTableCacheShardCount; i++ {
		shard := c.shards[i]
		shard.mu.Lock()

		// Clear all hash tables
		for _, entry := range shard.cache {
			entry.HashTable.Clear()
		}

		shard.cache = make(map[HashTableCacheKey]*CachedHashTable)
		shard.lruOrder = shard.lruOrder[:0]
		shard.currentMemory = 0

		shard.mu.Unlock()
	}

	c.hits.Store(0)
	c.misses.Store(0)
}

// Stats returns cache statistics aggregated across all shards.
func (c *ShardedHashTableCache) Stats() (entries int, memoryUsed int64, hits int64, misses int64) {
	for i := 0; i < hashTableCacheShardCount; i++ {
		shard := c.shards[i]
		shard.mu.RLock()
		entries += len(shard.cache)
		memoryUsed += shard.currentMemory
		shard.mu.RUnlock()
	}
	return entries, memoryUsed, c.hits.Load(), c.misses.Load()
}

// moveToEnd moves a key to the end of the LRU list (most recently used).
// Caller must hold the shard lock.
func (s *hashTableCacheShard) moveToEnd(key HashTableCacheKey) {
	for i, k := range s.lruOrder {
		if k == key {
			s.lruOrder = append(s.lruOrder[:i], s.lruOrder[i+1:]...)
			break
		}
	}
	s.lruOrder = append(s.lruOrder, key)
}

// evictOldest removes the oldest entry from the shard.
// Caller must hold the shard lock.
func (s *hashTableCacheShard) evictOldest() {
	if len(s.lruOrder) == 0 {
		return
	}

	oldestKey := s.lruOrder[0]
	s.lruOrder = s.lruOrder[1:]
	s.removeEntry(oldestKey)
}

// removeEntry removes an entry from the shard.
// Caller must hold the shard lock.
func (s *hashTableCacheShard) removeEntry(key HashTableCacheKey) {
	entry, exists := s.cache[key]
	if !exists {
		return
	}

	entry.HashTable.Clear()
	s.currentMemory -= entry.ByteSize
	delete(s.cache, key)

	for i, k := range s.lruOrder {
		if k == key {
			s.lruOrder = append(s.lruOrder[:i], s.lruOrder[i+1:]...)
			break
		}
	}
}

// =============================================================================
// Feature Flag Dispatch - GetHashTableCacheInterface
// =============================================================================

// Global interface cache for feature flag dispatch
var globalCacheInterface HashTableCacheInterface
var cacheInterfaceOnce sync.Once

// GetHashTableCacheInterface returns the appropriate cache implementation
// based on the UseShardedHashTableCache setting.
// This is the preferred method for obtaining a cache instance.
func GetHashTableCacheInterface() HashTableCacheInterface {
	cacheInterfaceOnce.Do(func() {
		if settings.GetSettings().UseShardedHashTableCache {
			globalCacheInterface = GetShardedHashTableCache()
		} else {
			globalCacheInterface = GetHashTableCache()
		}
	})
	return globalCacheInterface
}
