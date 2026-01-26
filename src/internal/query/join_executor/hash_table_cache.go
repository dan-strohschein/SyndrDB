// Package joinexecutor provides query join execution strategies.
//
// File: hash_table_cache.go
//
// This file implements an LRU cache for hash join hash tables to avoid
// rebuilding them for identical repeated joins. This is a major performance
// optimization for dashboard-style queries that repeatedly JOIN the same bundles.
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
package joinexecutor

import (
	"sync"
	"time"

	"syndrdb/src/internal/query/bloomfilter"
)

// HashTableCacheKey uniquely identifies a cached hash table
type HashTableCacheKey struct {
	BundleName string // Name of the bundle
	JoinKey    string // Field name used for join
}

// CachedHashTable stores a hash table along with its metadata
type CachedHashTable struct {
	HashTable   HashTable                 // The cached hash table
	BloomFilter *bloomfilter.BloomFilter  // Optional bloom filter
	Stats       *ScanStats                // Statistics from build phase
	CreatedAt   time.Time                 // When this was cached
	Hits        int64                     // Number of cache hits
	ByteSize    int64                     // Estimated memory size
}

// hashTableCacheEntry is used for LRU tracking
type hashTableCacheEntry struct {
	key   HashTableCacheKey
	value *CachedHashTable
}

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
