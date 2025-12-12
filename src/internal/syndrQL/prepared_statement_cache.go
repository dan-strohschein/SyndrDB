/*
PREPARED STATEMENT CACHE - PostgreSQL-inspired session-scoped caching

This file implements a session-isolated prepared statement cache for SyndrDB featuring:
- 8-segment sharded cache for reduced lock contention per session
- LRU eviction when capacity is reached
- Per-statement execution statistics tracking
- Bundle version tracking for lazy invalidation
- Comprehensive per-shard metrics

ARCHITECTURE:
The cache uses 8 shards per session to balance between strict LRU ordering
and lock contention. Each shard independently manages its own LRU list.

DESIGN PRINCIPLES:
- Session Isolation: Each session has its own cache instance
- Single Responsibility: Each shard manages its own cache state
- Pattern Consistency: Follows plan_cache.go 8-shard architecture

CACHE KEY GENERATION:
Uses statement name (alphanumeric + underscore, max 64 chars) as the key.
Statement names are case-sensitive and session-scoped.

INVALIDATION STRATEGY:
- Lazy invalidation via bundle version tracking (O(1) check on access)
- Manual invalidation via DEALLOCATE command
- Automatic cleanup when session ends

LIFECYCLE:
1. PREPARE statement_name AS query_text
   - Parse query, store in cache with bundle version
2. EXECUTE statement_name(param1, param2, ...)
   - Retrieve from cache, check bundle version, execute with parameters
3. DEALLOCATE statement_name
   - Remove from cache, free resources

This implements prepared statements matching PostgreSQL semantics.
*/

package syndrQL

import (
	"container/list"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"syndrdb/src/internal/query/queryparser"

	"github.com/cespare/xxhash/v2"
	"go.uber.org/zap"
)

// PreparedStatement represents a prepared statement with metadata
type PreparedStatement struct {
	Name               string                          // Statement name (user-defined identifier)
	QueryText          string                          // Original query text with $1, $2, ... placeholders
	ParsedQuery        *queryparser.UnifiedSelectQuery // Parsed query structure
	BundleName         string                          // Primary bundle for invalidation tracking
	BundleVersion      uint64                          // Bundle version at preparation time
	ParameterCount     int                             // Expected number of parameters
	CreatedAt          time.Time                       // When the statement was prepared
	LastExecuted       time.Time                       // Last execution timestamp
	ExecutionCount     int64                           // Number of times executed
	TotalExecutionTime time.Duration                   // Cumulative execution time
	AvgExecutionTime   time.Duration                   // Average execution time
}

// ShardedPreparedStatementCache is the main cache structure with 8 shards
type ShardedPreparedStatementCache struct {
	shards [8]*PreparedStatementCacheShard

	// Global statistics (aggregated across all shards)
	stats PreparedStatementCacheStats

	logger *zap.SugaredLogger
}

// PreparedStatementCacheShard represents a single shard of the cache
type PreparedStatementCacheShard struct {
	mu       sync.RWMutex
	capacity int

	// LRU cache structures
	cache map[uint64]*list.Element // hash(name) -> list element
	lru   *list.List               // LRU list of PreparedStatementCacheEntry

	// Bundle version tracking for lazy invalidation
	bundleVersions map[string]uint64 // bundleName -> version

	// Per-shard statistics
	stats PreparedStatementShardStats

	logger *zap.SugaredLogger
}

// PreparedStatementCacheEntry wraps a prepared statement with LRU metadata
type PreparedStatementCacheEntry struct {
	nameHash  uint64             // Hash of statement name for fast lookup
	statement *PreparedStatement // The actual prepared statement
}

// PreparedStatementCacheStats tracks global cache statistics
type PreparedStatementCacheStats struct {
	prepares      atomic.Uint64
	executions    atomic.Uint64
	deallocates   atomic.Uint64
	evictions     atomic.Uint64
	invalidations atomic.Uint64
}

// PreparedStatementShardStats tracks per-shard statistics
type PreparedStatementShardStats struct {
	hits   atomic.Uint64
	misses atomic.Uint64
}

// NewShardedPreparedStatementCache creates a new sharded prepared statement cache
func NewShardedPreparedStatementCache(capacity int, logger *zap.SugaredLogger) *ShardedPreparedStatementCache {
	if capacity < 8 {
		capacity = 8
	}

	capacityPerShard := capacity / 8
	if capacityPerShard < 1 {
		capacityPerShard = 1
	}

	cache := &ShardedPreparedStatementCache{
		logger: logger,
	}

	// Initialize all 8 shards
	for i := 0; i < 8; i++ {
		cache.shards[i] = &PreparedStatementCacheShard{
			capacity:       capacityPerShard,
			cache:          make(map[uint64]*list.Element, capacityPerShard),
			lru:            list.New(),
			bundleVersions: make(map[string]uint64),
			logger:         logger,
		}
	}

	logger.Debugf("Prepared statement cache initialized: capacity=%d shards=8", capacity)

	return cache
}

// getShard returns the shard for a given statement name using xxhash
func (psc *ShardedPreparedStatementCache) getShard(name string) (*PreparedStatementCacheShard, uint64) {
	hash := xxhash.Sum64String(name)
	shard := psc.shards[hash&0x7] // hash & 7 is equivalent to hash % 8
	return shard, hash
}

// Prepare stores a prepared statement in the cache
func (psc *ShardedPreparedStatementCache) Prepare(stmt *PreparedStatement) error {
	shard, nameHash := psc.getShard(stmt.Name)

	shard.mu.Lock()
	defer shard.mu.Unlock()

	// Check if statement already exists
	if elem, exists := shard.cache[nameHash]; exists {
		// Update existing statement (re-PREPARE semantics)
		entry := elem.Value.(*PreparedStatementCacheEntry)
		entry.statement = stmt
		shard.lru.MoveToFront(elem)
		psc.logger.Debugf("Re-prepared statement: %s", stmt.Name)
		return nil
	}

	// Create new entry
	entry := &PreparedStatementCacheEntry{
		nameHash:  nameHash,
		statement: stmt,
	}

	// Add to LRU list and cache
	elem := shard.lru.PushFront(entry)
	shard.cache[nameHash] = elem

	// Track bundle version for invalidation
	shard.bundleVersions[stmt.BundleName] = stmt.BundleVersion

	// Evict LRU if at capacity
	if shard.lru.Len() > shard.capacity {
		shard.evictOldest()
	}

	psc.stats.prepares.Add(1)
	psc.logger.Debugf("Prepared statement: %s (params=%d, bundle=%s)",
		stmt.Name, stmt.ParameterCount, stmt.BundleName)

	return nil
}

// Get retrieves a prepared statement from the cache
func (psc *ShardedPreparedStatementCache) Get(name string, currentBundleVersion uint64) (*PreparedStatement, error) {
	shard, nameHash := psc.getShard(name)

	shard.mu.Lock()
	defer shard.mu.Unlock()

	elem, exists := shard.cache[nameHash]
	if !exists {
		shard.stats.misses.Add(1)
		return nil, fmt.Errorf("prepared statement does not exist: %s", name)
	}

	entry := elem.Value.(*PreparedStatementCacheEntry)
	stmt := entry.statement

	// Check for stale bundle version (lazy invalidation)
	if currentBundleVersion > stmt.BundleVersion {
		psc.logger.Warnf("Prepared statement '%s' invalidated due to bundle changes (cached: v%d, current: v%d)",
			name, stmt.BundleVersion, currentBundleVersion)

		// Remove stale statement
		shard.lru.Remove(elem)
		delete(shard.cache, nameHash)
		psc.stats.invalidations.Add(1)

		return nil, fmt.Errorf("prepared statement '%s' is stale and was invalidated", name)
	}

	// Move to front (mark as recently used)
	shard.lru.MoveToFront(elem)
	shard.stats.hits.Add(1)

	return stmt, nil
}

// Deallocate removes a prepared statement from the cache
func (psc *ShardedPreparedStatementCache) Deallocate(name string) error {
	shard, nameHash := psc.getShard(name)

	shard.mu.Lock()
	defer shard.mu.Unlock()

	elem, exists := shard.cache[nameHash]
	if !exists {
		return fmt.Errorf("prepared statement does not exist: %s", name)
	}

	// Remove from cache and LRU list
	shard.lru.Remove(elem)
	delete(shard.cache, nameHash)
	psc.stats.deallocates.Add(1)

	psc.logger.Debugf("Deallocated prepared statement: %s", name)
	return nil
}

// UpdateBundleVersion updates the tracked bundle version for invalidation
func (psc *ShardedPreparedStatementCache) UpdateBundleVersion(bundleName string, version uint64) {
	// Update all shards (statements may be in any shard)
	for i := 0; i < 8; i++ {
		shard := psc.shards[i]
		shard.mu.Lock()
		shard.bundleVersions[bundleName] = version
		shard.mu.Unlock()
	}
}

// Clear removes all prepared statements from the cache
func (psc *ShardedPreparedStatementCache) Clear() {
	for i := 0; i < 8; i++ {
		shard := psc.shards[i]
		shard.mu.Lock()
		shard.cache = make(map[uint64]*list.Element, shard.capacity)
		shard.lru = list.New()
		shard.bundleVersions = make(map[string]uint64)
		shard.mu.Unlock()
	}
	psc.logger.Debug("Cleared all prepared statements from cache")
}

// GetStatementCount returns the total number of prepared statements
func (psc *ShardedPreparedStatementCache) GetStatementCount() int {
	count := 0
	for i := 0; i < 8; i++ {
		shard := psc.shards[i]
		shard.mu.RLock()
		count += shard.lru.Len()
		shard.mu.RUnlock()
	}
	return count
}

// GetStats returns comprehensive cache statistics
func (psc *ShardedPreparedStatementCache) GetStats() map[string]interface{} {
	totalHits := uint64(0)
	totalMisses := uint64(0)

	for i := 0; i < 8; i++ {
		totalHits += psc.shards[i].stats.hits.Load()
		totalMisses += psc.shards[i].stats.misses.Load()
	}

	totalRequests := totalHits + totalMisses
	hitRate := 0.0
	if totalRequests > 0 {
		hitRate = float64(totalHits) / float64(totalRequests)
	}

	return map[string]interface{}{
		"statement_count": psc.GetStatementCount(),
		"prepares":        psc.stats.prepares.Load(),
		"executions":      psc.stats.executions.Load(),
		"deallocates":     psc.stats.deallocates.Load(),
		"evictions":       psc.stats.evictions.Load(),
		"invalidations":   psc.stats.invalidations.Load(),
		"hits":            totalHits,
		"misses":          totalMisses,
		"hit_rate":        hitRate,
	}
}

// evictOldest removes the least recently used prepared statement
// This method assumes the caller holds the write lock
func (shard *PreparedStatementCacheShard) evictOldest() {
	if shard.lru.Len() == 0 {
		return
	}

	oldest := shard.lru.Back()
	if oldest != nil {
		entry := oldest.Value.(*PreparedStatementCacheEntry)
		shard.lru.Remove(oldest)
		delete(shard.cache, entry.nameHash)
		shard.logger.Debugf("Evicted prepared statement: %s", entry.statement.Name)
	}
}

// RecordExecution updates execution statistics for a prepared statement
func (psc *ShardedPreparedStatementCache) RecordExecution(name string, executionTime time.Duration) {
	shard, nameHash := psc.getShard(name)

	shard.mu.Lock()
	defer shard.mu.Unlock()

	elem, exists := shard.cache[nameHash]
	if !exists {
		return
	}

	entry := elem.Value.(*PreparedStatementCacheEntry)
	stmt := entry.statement

	stmt.ExecutionCount++
	stmt.TotalExecutionTime += executionTime
	stmt.AvgExecutionTime = stmt.TotalExecutionTime / time.Duration(stmt.ExecutionCount)
	stmt.LastExecuted = time.Now()

	psc.stats.executions.Add(1)
}
