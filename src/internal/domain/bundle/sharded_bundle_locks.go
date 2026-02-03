/*
SHARDED BUNDLE LOCKS - High-Concurrency Lock Management for BundleService

This file provides sharded map implementations for bundle-level locks and caches
used by BundleService. The sharded design distributes lock contention across 64
shards, enabling concurrent access to locks for different bundles without global
serialization.

PROBLEM SOLVED:
Previously, all 150+ concurrent connections contended on single mutexes
(bundleLockMutex, schemaManagerMutex, pageCacheMutex) just to look up per-bundle
resources. This created severe bottlenecks where threads serialized even for
operations on different bundles.

DESIGN:
- 64 shards (matching LockManager and bundlestore patterns for consistency)
- Each shard has its own RWMutex protecting a map
- Shard selection via xxhash(key) & 63 for O(1) distribution
- Fast path: RLock for existing entries, only take write lock for creation/modification

TYPES PROVIDED:
- ShardedBundleOperationLockMap: For BundleOperationLock instances (bundleLockMutex replacement)
- ShardedSchemaManagerMap: For graphQLSchema.SchemaManager instances (schemaManagerMutex replacement)
- ShardedLoadedDatabasesMap: For tracking database last access times (indexMemoryMutex partial replacement)
- ShardedPageCacheMap: For document->page mappings with FIFO eviction (pageCacheMutex replacement)

PERFORMANCE CHARACTERISTICS:
- 150 connections → ~2.3 connections per shard average
- Reads (get existing resource): concurrent within same shard
- Writes (create/update resource): exclusive but only blocks same-shard operations

Author: Phase 5 Lock Contention Fix
*/

package bundle

import (
	"sync"
	"time"

	graphQLSchema "syndrdb/src/internal/graphQL/schema"

	"github.com/cespare/xxhash/v2"
	"go.uber.org/zap"
)

const (
	// BundleShardCount is the number of shards for all bundle-level sharded maps.
	// 64 shards matches bundlestore and LockManager for consistency.
	BundleShardCount = 64
)

// ============================================================================
// ShardedBundleOperationLockMap - Replaces bundleLockMutex
// ============================================================================

// bundleOperationLockShard holds a partition of the bundle operation lock map
type bundleOperationLockShard struct {
	mu    sync.RWMutex
	locks map[string]*BundleOperationLock
}

// ShardedBundleOperationLockMap provides concurrent access to per-bundle operation locks
// with sharding to reduce contention under high concurrency.
type ShardedBundleOperationLockMap struct {
	shards [BundleShardCount]bundleOperationLockShard
	logger *zap.SugaredLogger
}

// NewShardedBundleOperationLockMap creates a new sharded bundle operation lock map
func NewShardedBundleOperationLockMap(logger *zap.SugaredLogger) *ShardedBundleOperationLockMap {
	m := &ShardedBundleOperationLockMap{logger: logger}
	for i := range m.shards {
		m.shards[i].locks = make(map[string]*BundleOperationLock)
	}
	return m
}

// shardIndex returns the shard index for a given bundle name
func (m *ShardedBundleOperationLockMap) shardIndex(bundleName string) uint64 {
	return xxhash.Sum64String(bundleName) & (BundleShardCount - 1)
}

// Get retrieves or creates an operation lock for the specified bundle.
// Thread-safe: uses read-lock fast path for existing locks,
// write-lock slow path for creation with double-check.
func (m *ShardedBundleOperationLockMap) Get(bundleName string) *BundleOperationLock {
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

	// Create new lock with logger for error reporting
	lock := NewBundleOperationLock(bundleName, m.logger)
	shard.locks[bundleName] = lock
	return lock
}

// Delete removes a lock for the specified bundle.
// Used during bundle deletion to clean up resources.
func (m *ShardedBundleOperationLockMap) Delete(bundleName string) {
	idx := m.shardIndex(bundleName)
	shard := &m.shards[idx]

	shard.mu.Lock()
	defer shard.mu.Unlock()
	delete(shard.locks, bundleName)
}

// ============================================================================
// ShardedSchemaManagerMap - Replaces schemaManagerMutex
// ============================================================================

// schemaManagerShard holds a partition of the schema manager map
type schemaManagerShard struct {
	mu       sync.RWMutex
	managers map[string]*graphQLSchema.SchemaManager
}

// ShardedSchemaManagerMap provides concurrent access to per-database schema managers
// with sharding to reduce contention under high concurrency.
type ShardedSchemaManagerMap struct {
	shards [BundleShardCount]schemaManagerShard
}

// NewShardedSchemaManagerMap creates a new sharded schema manager map
func NewShardedSchemaManagerMap() *ShardedSchemaManagerMap {
	m := &ShardedSchemaManagerMap{}
	for i := range m.shards {
		m.shards[i].managers = make(map[string]*graphQLSchema.SchemaManager)
	}
	return m
}

// shardIndex returns the shard index for a given database name
func (m *ShardedSchemaManagerMap) shardIndex(databaseName string) uint64 {
	return xxhash.Sum64String(databaseName) & (BundleShardCount - 1)
}

// Get retrieves a schema manager for the specified database if it exists.
// Returns nil if not found. Thread-safe with read lock.
func (m *ShardedSchemaManagerMap) Get(databaseName string) (*graphQLSchema.SchemaManager, bool) {
	idx := m.shardIndex(databaseName)
	shard := &m.shards[idx]

	shard.mu.RLock()
	manager, exists := shard.managers[databaseName]
	shard.mu.RUnlock()
	return manager, exists
}

// Set stores a schema manager for the specified database.
// Thread-safe with write lock.
func (m *ShardedSchemaManagerMap) Set(databaseName string, manager *graphQLSchema.SchemaManager) {
	idx := m.shardIndex(databaseName)
	shard := &m.shards[idx]

	shard.mu.Lock()
	shard.managers[databaseName] = manager
	shard.mu.Unlock()
}

// GetOrCreate retrieves an existing schema manager or calls the creator function.
// Uses double-check locking to avoid redundant creation.
// The creator function is only called under write lock if the manager doesn't exist.
func (m *ShardedSchemaManagerMap) GetOrCreate(databaseName string, creator func() (*graphQLSchema.SchemaManager, error)) (*graphQLSchema.SchemaManager, error) {
	idx := m.shardIndex(databaseName)
	shard := &m.shards[idx]

	// Fast path: read lock for existing entry
	shard.mu.RLock()
	if manager, ok := shard.managers[databaseName]; ok {
		shard.mu.RUnlock()
		return manager, nil
	}
	shard.mu.RUnlock()

	// Slow path: create new manager with write lock
	shard.mu.Lock()
	defer shard.mu.Unlock()

	// Double-check after acquiring write lock
	if manager, ok := shard.managers[databaseName]; ok {
		return manager, nil
	}

	// Create new manager via callback
	manager, err := creator()
	if err != nil {
		return nil, err
	}
	if manager != nil {
		shard.managers[databaseName] = manager
	}
	return manager, nil
}

// Delete removes a schema manager for the specified database.
func (m *ShardedSchemaManagerMap) Delete(databaseName string) {
	idx := m.shardIndex(databaseName)
	shard := &m.shards[idx]

	shard.mu.Lock()
	defer shard.mu.Unlock()
	delete(shard.managers, databaseName)
}

// ============================================================================
// ShardedLoadedDatabasesMap - Replaces indexMemoryMutex for loadedDatabases
// ============================================================================

// loadedDatabaseShard holds a partition of the loaded databases map
type loadedDatabaseShard struct {
	mu        sync.RWMutex
	databases map[string]time.Time
}

// ShardedLoadedDatabasesMap provides concurrent access to database last-access tracking
// with sharding to reduce contention under high concurrency.
type ShardedLoadedDatabasesMap struct {
	shards [BundleShardCount]loadedDatabaseShard
}

// NewShardedLoadedDatabasesMap creates a new sharded loaded databases map
func NewShardedLoadedDatabasesMap() *ShardedLoadedDatabasesMap {
	m := &ShardedLoadedDatabasesMap{}
	for i := range m.shards {
		m.shards[i].databases = make(map[string]time.Time)
	}
	return m
}

// shardIndex returns the shard index for a given database name
func (m *ShardedLoadedDatabasesMap) shardIndex(databaseName string) uint64 {
	return xxhash.Sum64String(databaseName) & (BundleShardCount - 1)
}

// Get retrieves the last access time for a database.
func (m *ShardedLoadedDatabasesMap) Get(databaseName string) (time.Time, bool) {
	idx := m.shardIndex(databaseName)
	shard := &m.shards[idx]

	shard.mu.RLock()
	lastAccess, exists := shard.databases[databaseName]
	shard.mu.RUnlock()
	return lastAccess, exists
}

// Set updates the last access time for a database.
func (m *ShardedLoadedDatabasesMap) Set(databaseName string, lastAccess time.Time) {
	idx := m.shardIndex(databaseName)
	shard := &m.shards[idx]

	shard.mu.Lock()
	shard.databases[databaseName] = lastAccess
	shard.mu.Unlock()
}

// Touch updates the last access time to now for a database, returning whether it existed.
func (m *ShardedLoadedDatabasesMap) Touch(databaseName string) bool {
	idx := m.shardIndex(databaseName)
	shard := &m.shards[idx]

	shard.mu.Lock()
	_, exists := shard.databases[databaseName]
	if exists {
		shard.databases[databaseName] = time.Now()
	}
	shard.mu.Unlock()
	return exists
}

// Delete removes a database from tracking.
func (m *ShardedLoadedDatabasesMap) Delete(databaseName string) {
	idx := m.shardIndex(databaseName)
	shard := &m.shards[idx]

	shard.mu.Lock()
	delete(shard.databases, databaseName)
	shard.mu.Unlock()
}

// ForEachWithEviction iterates over all databases and returns those idle beyond the timeout.
// Returns slice of database names to evict. Safe for concurrent use.
func (m *ShardedLoadedDatabasesMap) ForEachWithEviction(idleTimeout time.Duration) []string {
	now := time.Now()
	var toEvict []string

	for i := range m.shards {
		shard := &m.shards[i]
		shard.mu.RLock()
		for dbName, lastAccess := range shard.databases {
			if now.Sub(lastAccess) > idleTimeout {
				toEvict = append(toEvict, dbName)
			}
		}
		shard.mu.RUnlock()
	}
	return toEvict
}

// ============================================================================
// ShardedPageCacheMap - Replaces pageCacheMutex for documentPageMap/FIFO
// ============================================================================

// documentPageCacheEntry holds document->page mappings and FIFO for one bundle
type documentPageCacheEntry struct {
	pageMap map[string]uint32 // documentID -> pageID
	fifo    []string          // FIFO of documentIDs for eviction
}

// documentPageCacheShard holds a partition of the document page cache
// (renamed from pageCacheShard to avoid collision with existing pageCacheShard in bundle_service.go)
type documentPageCacheShard struct {
	mu      sync.RWMutex
	bundles map[string]*documentPageCacheEntry // bundleName -> entry
}

// ShardedPageCacheMap provides concurrent access to document-to-page mappings
// with sharding to reduce contention under high concurrency.
type ShardedPageCacheMap struct {
	shards       [BundleShardCount]documentPageCacheShard
	maxCacheSize int // Maximum entries per bundle (for eviction)
	logger       *zap.SugaredLogger
}

// NewShardedPageCacheMap creates a new sharded page cache map
func NewShardedPageCacheMap(maxCacheSize int, logger *zap.SugaredLogger) *ShardedPageCacheMap {
	m := &ShardedPageCacheMap{maxCacheSize: maxCacheSize, logger: logger}
	for i := range m.shards {
		m.shards[i].bundles = make(map[string]*documentPageCacheEntry)
	}
	return m
}

// shardIndex returns the shard index for a given bundle name
func (m *ShardedPageCacheMap) shardIndex(bundleName string) uint64 {
	return xxhash.Sum64String(bundleName) & (BundleShardCount - 1)
}

// GetPageID retrieves the cached page ID for a document.
// Returns (pageID, true) if found, (0, false) if not cached.
func (m *ShardedPageCacheMap) GetPageID(bundleName, documentID string) (uint32, bool) {
	idx := m.shardIndex(bundleName)
	shard := &m.shards[idx]

	shard.mu.RLock()
	defer shard.mu.RUnlock()

	entry, exists := shard.bundles[bundleName]
	if !exists || entry.pageMap == nil {
		return 0, false
	}
	pageID, found := entry.pageMap[documentID]
	return pageID, found
}

// SetPageID caches a document's page ID, performing FIFO eviction if at capacity.
func (m *ShardedPageCacheMap) SetPageID(bundleName, documentID string, pageID uint32) {
	idx := m.shardIndex(bundleName)
	shard := &m.shards[idx]

	shard.mu.Lock()
	defer shard.mu.Unlock()

	entry, exists := shard.bundles[bundleName]
	if !exists {
		entry = &documentPageCacheEntry{
			pageMap: make(map[string]uint32),
			fifo:    make([]string, 0),
		}
		shard.bundles[bundleName] = entry
	}

	// Check if already cached (update pageID, no FIFO change)
	if _, alreadyCached := entry.pageMap[documentID]; alreadyCached {
		entry.pageMap[documentID] = pageID
		return
	}

	// Evict if at capacity
	if m.maxCacheSize > 0 && len(entry.pageMap) >= m.maxCacheSize {
		m.evictOneLocked(bundleName, entry)
	}

	// Add new entry
	entry.pageMap[documentID] = pageID
	entry.fifo = append(entry.fifo, documentID)
}

// evictOneLocked evicts one document from the cache using FIFO order.
// Caller must hold write lock on the shard.
func (m *ShardedPageCacheMap) evictOneLocked(bundleName string, entry *documentPageCacheEntry) {
	if len(entry.fifo) > 0 {
		docID := entry.fifo[0]
		entry.fifo = entry.fifo[1:]
		delete(entry.pageMap, docID)
		if m.logger != nil {
			m.logger.Debugf("Evicted document %s from page cache for bundle %s (FIFO)", docID, bundleName)
		}
		return
	}
	// Fallback: remove arbitrary entry if FIFO is empty somehow
	for docID := range entry.pageMap {
		delete(entry.pageMap, docID)
		if m.logger != nil {
			m.logger.Debugf("Evicted document %s from page cache for bundle %s (fallback)", docID, bundleName)
		}
		return
	}
}

// InvalidateDocument removes a specific document from the cache.
func (m *ShardedPageCacheMap) InvalidateDocument(bundleName, documentID string) {
	idx := m.shardIndex(bundleName)
	shard := &m.shards[idx]

	shard.mu.Lock()
	defer shard.mu.Unlock()

	entry, exists := shard.bundles[bundleName]
	if !exists || entry.pageMap == nil {
		return
	}

	delete(entry.pageMap, documentID)

	// Remove from FIFO
	for i, id := range entry.fifo {
		if id == documentID {
			entry.fifo = append(entry.fifo[:i], entry.fifo[i+1:]...)
			break
		}
	}

	// Clean up empty entries
	if len(entry.pageMap) == 0 {
		delete(shard.bundles, bundleName)
	}
}

// InvalidateBundle clears all cached page mappings for a bundle.
func (m *ShardedPageCacheMap) InvalidateBundle(bundleName string) {
	idx := m.shardIndex(bundleName)
	shard := &m.shards[idx]

	shard.mu.Lock()
	delete(shard.bundles, bundleName)
	shard.mu.Unlock()
}

// GetAllForBundle returns all cached document->page mappings for a bundle.
// Returns nil if no entries cached. Used for diagnostics/debugging.
func (m *ShardedPageCacheMap) GetAllForBundle(bundleName string) map[string]uint32 {
	idx := m.shardIndex(bundleName)
	shard := &m.shards[idx]

	shard.mu.RLock()
	defer shard.mu.RUnlock()

	entry, exists := shard.bundles[bundleName]
	if !exists || entry.pageMap == nil {
		return nil
	}

	// Return a copy to avoid concurrent modification
	result := make(map[string]uint32, len(entry.pageMap))
	for k, v := range entry.pageMap {
		result[k] = v
	}
	return result
}

// Size returns the total number of cached entries across all bundles.
func (m *ShardedPageCacheMap) Size() int {
	total := 0
	for i := range m.shards {
		shard := &m.shards[i]
		shard.mu.RLock()
		for _, entry := range shard.bundles {
			total += len(entry.pageMap)
		}
		shard.mu.RUnlock()
	}
	return total
}

// Flush completely clears all document-to-page mappings from all shards.
// This is used for aggressive cache cleanup when all clients disconnect,
// preventing accumulated data from degrading performance on reconnect.
func (m *ShardedPageCacheMap) Flush() {
	for i := range m.shards {
		shard := &m.shards[i]
		shard.mu.Lock()
		shard.bundles = make(map[string]*documentPageCacheEntry)
		shard.mu.Unlock()
	}
}
