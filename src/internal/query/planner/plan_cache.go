/*
QUERY PLAN CACHE - PostgreSQL-inspired with MongoDB-style invalidation

This file implements a production-ready query plan cache for SyndrDB featuring:
- 8-segment sharded cache for reduced lock contention
- Adaptive generic/custom plan selection (PostgreSQL-style)
- Bundle-scoped lazy invalidation with version tracking
- Write-threshold based invalidation (MongoDB-style)
- Graceful stale plan serving with async rebuild
- Comprehensive per-bundle and per-shard metrics

ARCHITECTURE:
The cache uses approximate LRU with 8 shards to balance between strict LRU ordering
and lock contention. Each shard independently manages its own LRU list and version
tracking for surgical invalidation.

DESIGN PRINCIPLES:
- Single Responsibility: Each shard manages its own cache state
- Open/Closed: Extensible via TODO markers for future enhancements
- DRY: Shared statistics and invalidation logic

CACHE KEY GENERATION:
Uses xxhash (v2.3.0) for fast, high-quality hashing of normalized query structure.
Keys include query structure (not values) ensuring parameter-independent caching.

INVALIDATION STRATEGY:
- Lazy invalidation via version tracking (O(1) check on access)
- Write-threshold before invalidation (default 1000 writes like MongoDB)
- Immediate invalidation on index/schema changes
- Stale plan serving for SELECT queries during async rebuild

ADAPTIVE PLANNING:
- First 5 executions use custom plans (parameter-specific optimization)
- 6th execution builds generic plan
- Compares average custom cost vs generic cost (110% threshold)
- Switches to generic if within acceptable performance range

This implements the recommendation from docs/query_plan_cache.md
*/

package planner

import (
	"container/list"
	"context"
	"encoding/binary"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"syndrdb/src/internal/domain/models"
	"syndrdb/src/internal/query/queryparser"
	"syndrdb/src/pkg/extension"
	"syndrdb/src/pkg/settings"

	"github.com/cespare/xxhash/v2"
	"go.uber.org/zap"
)

// ShardedPlanCache is the main cache structure with 8 shards for reduced contention
// This isn't the same as a mongo shard, I just like the word "shard". I will probably
// change it later to reflect that it's just a segment of the cache.
type ShardedPlanCache struct {
	shards [8]*PlanCacheShard

	// Global statistics (aggregated across all shards)
	stats CacheStats

	// Per-bundle invalidation tracking
	bundleInvalidations sync.Map // bundleName -> *atomic.Uint64

	// Per-bundle stale serve tracking
	staleServesByBundle sync.Map // bundleName -> *atomic.Uint64

	// Observability: metrics reporter callback for exporting cache stats to GlobalServerMetrics
	metricsReporter func(metricName string, value uint64)

	logger *zap.SugaredLogger
}

// SetMetricsReporter sets the callback used to export plan cache metrics to GlobalServerMetrics
func (spc *ShardedPlanCache) SetMetricsReporter(reporter func(string, uint64)) {
	spc.metricsReporter = reporter
}

// PlanCacheShard represents a single shard of the cache
type PlanCacheShard struct {
	mu       sync.RWMutex
	capacity int

	// LRU cache structures
	cache map[uint64]*list.Element // key -> list element
	lru   *list.List               // LRU list of CacheEntry

	// Bundle version tracking for lazy invalidation
	collectionVersions map[string]uint64 // bundleName -> version

	// Per-shard statistics
	stats ShardStats

	// Parent cache for global stats and metrics reporting
	parent *ShardedPlanCache

	// Graceful shutdown support
	shutdown   atomic.Bool
	rebuildWg  sync.WaitGroup
	shutdownMu sync.Mutex // Protects rebuild launches during shutdown

	logger *zap.SugaredLogger
}

// CacheEntry represents a cached query plan with statistics
type CacheEntry struct {
	key               uint64
	plan              *ExecutionPlan
	bundleName        string
	collectionVersion uint64

	// Execution statistics
	execCount atomic.Int64
	totalTime time.Duration
	avgTime   time.Duration

	// Adaptive planning (PostgreSQL-style)
	customPlanCost  []float64 // First 5 execution costs
	genericPlanCost float64
	useGeneric      bool
	isGeneric       bool // Whether this entry is for generic plan

	// Metadata
	createdAt time.Time
	lastUsed  time.Time

	// Stale plan serving
	isStale       bool
	staleServedAt time.Time

	// Memory estimation (cached at plan creation)
	estimatedMemoryBytes int64
}

// CacheStats tracks global cache statistics (internal, with atomics)
type CacheStats struct {
	hits          atomic.Uint64
	misses        atomic.Uint64
	evictions     atomic.Uint64
	invalidations atomic.Uint64
	staleServes   atomic.Uint64
}

// CacheStatsSnapshot is a point-in-time snapshot of cache statistics for callers.
// Use Stats() to obtain current values; atomics are not exposed.
type CacheStatsSnapshot struct {
	Hits          uint64
	Misses        uint64
	Evictions     uint64
	Invalidations uint64
	StaleServes   uint64
}

// ShardStats tracks per-shard statistics
type ShardStats struct {
	hits   atomic.Uint64
	misses atomic.Uint64
}

// NewShardedPlanCache creates a new sharded plan cache
func NewShardedPlanCache(settings *settings.Arguments, logger *zap.SugaredLogger) *ShardedPlanCache {
	if !settings.PlanCacheEnabled {
		return nil
	}

	capacityPerShard := settings.PlanCacheCapacity / 8
	if capacityPerShard < 1 {
		capacityPerShard = 1
	}

	cache := &ShardedPlanCache{
		logger: logger,
	}

	// Initialize all 8 shards
	for i := 0; i < 8; i++ {
		cache.shards[i] = &PlanCacheShard{
			capacity:           capacityPerShard,
			cache:              make(map[uint64]*list.Element, capacityPerShard),
			lru:                list.New(),
			collectionVersions: make(map[string]uint64),
			parent:             cache,
			logger:             logger,
		}
	}

	logger.Infof("Plan cache initialized: enabled=%v capacity=%d shards=8 adaptive=%v",
		settings.PlanCacheEnabled, settings.PlanCacheCapacity, settings.PlanCacheAdaptivePlanning)

	return cache
}

// getShard returns the shard for a given key using bitwise AND for fast modulo
func (spc *ShardedPlanCache) getShard(key uint64) *PlanCacheShard {
	return spc.shards[key&0x7] // key & 7 is equivalent to key % 8
}

// Get retrieves a cached plan or builds a new one
func (spc *ShardedPlanCache) Get(
	ctx context.Context,
	query *queryparser.UnifiedSelectQuery,
	database *models.Database,
	buildPlanFunc func(useGeneric bool) (*ExecutionPlan, error),
) (*ExecutionPlan, error) {
	if spc == nil {
		// Cache disabled, build plan directly
		return buildPlanFunc(false)
	}

	// Determine if we should use generic plan
	settings := settings.GetSettings()
	useGeneric := false
	if settings.PlanCacheAdaptivePlanning {
		useGeneric = spc.shouldUseGenericPlan(query)
	}

	// Compute cache key
	key := spc.computeKey(query, useGeneric)
	shard := spc.getShard(key)

	// Check shutdown
	if shard.shutdown.Load() {
		return nil, fmt.Errorf("plan cache is shutting down")
	}

	// Try to get from cache
	plan, staleServed := shard.get(ctx, query, key, useGeneric)
	if plan != nil {
		// Enterprise adaptive optimizer: check if plan should be re-optimized
		if reg := extension.GetRegistry(); reg.HasAdaptiveOptimizerExtension() {
			if shouldReplan, _ := reg.GetAdaptiveOptimizerExtension().SuggestPlanChange(key); shouldReplan {
				// Evict stale plan and fall through to rebuild
				shard.mu.Lock()
				if elem, ok := shard.cache[key]; ok {
					shard.lru.Remove(elem)
					delete(shard.cache, key)
				}
				shard.mu.Unlock()
				plan = nil
			}
		}
	}
	if plan != nil {
		if staleServed {
			spc.stats.staleServes.Add(1)
			if spc.metricsReporter != nil {
				spc.metricsReporter("QueryPlanCacheStaleServes", 1)
			}
		}
		spc.stats.hits.Add(1)
		shard.stats.hits.Add(1)
		if spc.metricsReporter != nil {
			spc.metricsReporter("QueryPlanCacheHits", 1)
		}
		spc.logger.Debugf("Plan cache HIT: shard=%d bundle=%s key=%016x execCount=%d",
			key&0x7, query.FromBundle, key, plan.RootNode.GetEstimatedRows())
		return plan, nil
	}

	// Cache miss - build new plan
	spc.stats.misses.Add(1)
	shard.stats.misses.Add(1)
	if spc.metricsReporter != nil {
		spc.metricsReporter("QueryPlanCacheMisses", 1)
	}
	spc.logger.Debugf("Plan cache MISS: shard=%d bundle=%s key=%016x buildingPlan=true",
		key&0x7, query.FromBundle, key)

	newPlan, err := buildPlanFunc(useGeneric)
	if err != nil {
		return nil, err
	}

	// Insert into cache
	shard.insert(key, query.FromBundle, newPlan, useGeneric)

	return newPlan, nil
}

// get retrieves a plan from the shard, handling stale plan serving.
// Returns (plan, true) when serving a stale plan (caller must increment staleServes).
// Uses RLock fast path for cache hits; only upgrades to Lock 1-in-8 hits for MoveToFront.
func (shard *PlanCacheShard) get(
	ctx context.Context,
	query *queryparser.UnifiedSelectQuery,
	key uint64,
	useGeneric bool,
) (*ExecutionPlan, bool) {
	// RLock fast path: lookup + version check without exclusive lock
	shard.mu.RLock()
	elem, ok := shard.cache[key]
	if !ok {
		shard.mu.RUnlock()
		return nil, false
	}

	entry := elem.Value.(*CacheEntry)
	bundleName := entry.bundleName

	// Check if plan is still valid (version check)
	currentVersion := shard.collectionVersions[bundleName]
	isValid := entry.collectionVersion == currentVersion

	if isValid {
		// Plan is fresh - atomically increment execCount (lock-free)
		ec := entry.execCount.Add(1)
		plan := entry.plan
		isGeneric := entry.isGeneric
		shard.mu.RUnlock()

		// Probabilistic MoveToFront: upgrade to write lock only 1-in-8 hits.
		// LRU position is approximate anyway; this avoids write lock contention.
		if ec%8 == 0 {
			shard.mu.Lock()
			// Re-check elem is still in cache (could have been evicted)
			if _, stillOk := shard.cache[key]; stillOk {
				shard.lru.MoveToFront(elem)
			}
			shard.mu.Unlock()
		}

		// Clone before return so concurrent callers don't share mutable plan state
		p := plan.Clone()
		p.CacheKey = key
		p.IsGeneric = isGeneric
		return p, false
	}

	// Plan is stale - check if we can serve it anyway (SELECT queries only)
	shard.mu.RUnlock()

	// Safety check: only serve stale plans for SELECT queries
	isSelectQuery := query.QueryType == queryparser.SimpleQuery ||
		query.QueryType == queryparser.JoinQuery ||
		query.QueryType == queryparser.GroupByQuery ||
		query.QueryType == queryparser.ComplexQuery

	if !isSelectQuery {
		settings := settings.GetSettings()
		if settings.Debug {
			panic(fmt.Sprintf("FATAL: attempted to serve stale plan for non-SELECT query type=%v bundle=%s - this violates data consistency guarantees",
				query.QueryType, bundleName))
		}
		shard.logger.Warnf("Blocked stale serve for non-SELECT: queryType=%v bundle=%s debug=%v",
			query.QueryType, bundleName, settings.Debug)
		return nil, false
	}

	// Check stale serve window
	shard.mu.Lock()
	defer shard.mu.Unlock()

	// Re-check entry still exists after lock upgrade
	elem, ok = shard.cache[key]
	if !ok {
		return nil, false
	}
	entry = elem.Value.(*CacheEntry)

	staleDuration := time.Since(entry.staleServedAt)
	maxStale := time.Duration(settings.GetSettings().PlanCacheStaleServeSeconds) * time.Second

	if entry.isStale && staleDuration < maxStale {
		// Still within stale serve window - return clone of stale plan (Issue 6: count as stale serve)
		p := entry.plan.Clone()
		p.CacheKey = key
		p.IsGeneric = entry.isGeneric
		return p, true
	}

	// Either first time stale or window expired - mark stale and trigger rebuild
	entry.isStale = true
	entry.staleServedAt = time.Now()

	// Launch async rebuild (don't block current query)
	// I'll implement a proper async rebuild mechanism here someday
	// TODO: Implement async rebuild with proper context handling and WaitGroup tracking

	// First-time stale serve (Issue 6: count as stale serve)
	p := entry.plan.Clone()
	p.CacheKey = key
	p.IsGeneric = entry.isGeneric
	return p, true
}

// insert adds a plan to the cache with LRU eviction
func (shard *PlanCacheShard) insert(key uint64, bundleName string, plan *ExecutionPlan, isGeneric bool) {
	shard.mu.Lock()
	defer shard.mu.Unlock()

	// Check if already exists (race condition from concurrent builds)
	if elem, ok := shard.cache[key]; ok {
		shard.lru.MoveToFront(elem)
		return
	}

	// Evict if at capacity
	if shard.lru.Len() >= shard.capacity {
		oldest := shard.lru.Back()
		if oldest != nil {
			oldEntry := oldest.Value.(*CacheEntry)
			delete(shard.cache, oldEntry.key)
			shard.lru.Remove(oldest)

			if shard.parent != nil {
				shard.parent.stats.evictions.Add(1)
				if shard.parent.metricsReporter != nil {
					shard.parent.metricsReporter("QueryPlanCacheEvictions", 1)
				}
			}

			shard.logger.Debugf("Evicted LRU plan: shard=%d bundle=%s age=%.1fmin memoryBytes=%d",
				key&0x7, oldEntry.bundleName, time.Since(oldEntry.createdAt).Minutes(), oldEntry.estimatedMemoryBytes)
		}
	}

	// Create new entry
	entry := &CacheEntry{
		key:                  key,
		plan:                 plan,
		bundleName:           bundleName,
		collectionVersion:    shard.collectionVersions[bundleName],
		createdAt:            time.Now(),
		lastUsed:             time.Now(),
		isGeneric:            isGeneric,
		customPlanCost:       make([]float64, 0, 5),
		estimatedMemoryBytes: plan.estimatedMemoryBytes,
	}

	// Add to cache and LRU
	elem := shard.lru.PushFront(entry)
	shard.cache[key] = elem
}

// computeKey generates a cache key from query structure using xxhash
func (spc *ShardedPlanCache) computeKey(query *queryparser.UnifiedSelectQuery, useGeneric bool) uint64 {
	h := xxhash.New()

	// Write bundle name
	h.Write([]byte(query.FromBundle))

	// Write query type
	binary.Write(h, binary.LittleEndian, uint8(query.QueryType))

	// Write select fields (sorted for determinism)
	sortedFields := make([]string, len(query.SelectFields))
	copy(sortedFields, query.SelectFields)
	sort.Strings(sortedFields)
	for _, field := range sortedFields {
		h.Write([]byte(field))
		h.Write([]byte{0}) // null terminator
	}

	// Write aggregate fields (sorted)
	sortedAggs := make([]queryparser.AggregateFunction, len(query.AggregateFields))
	copy(sortedAggs, query.AggregateFields)
	sort.Slice(sortedAggs, func(i, j int) bool {
		return sortedAggs[i].Function+sortedAggs[i].Field < sortedAggs[j].Function+sortedAggs[j].Field
	})
	for _, agg := range sortedAggs {
		h.Write([]byte(agg.Function))
		h.Write([]byte(agg.Field))
		h.Write([]byte{0})
	}

	// Write boolean flags
	binary.Write(h, binary.LittleEndian, query.IsDistinct)
	binary.Write(h, binary.LittleEndian, query.IsCountOnly)

	// Write JOIN clauses (sorted by bundle name for deterministic ordering)
	sortedJoins := make([]queryparser.JoinClause, len(query.JoinClauses))
	copy(sortedJoins, query.JoinClauses)
	sort.Slice(sortedJoins, func(i, j int) bool {
		return sortedJoins[i].RightBundle < sortedJoins[j].RightBundle
	})
	for _, join := range sortedJoins {
		h.Write([]byte(join.RightBundle))
		binary.Write(h, binary.LittleEndian, int32(join.JoinType))
		for _, cond := range join.JoinConditions {
			h.Write([]byte(cond.LeftField))
			h.Write([]byte(cond.Operator))
			h.Write([]byte(cond.RightField))
		}
		h.Write([]byte{0})
	}

	// Write WHERE expression structure (not values) - canonical serialization (Issue 9)
	// Use Expression.String() when available for deterministic cache key (avoids fmt "%v" map iteration).
	if query.WhereExpression != nil {
		h.Write([]byte("WHERE:"))
		if stringer, ok := query.WhereExpression.(interface{ String() string }); ok {
			h.Write([]byte(stringer.String()))
		} else {
			h.Write([]byte(fmt.Sprintf("%v", query.WhereExpression)))
		}
		h.Write([]byte{0})
	} else {
		h.Write([]byte("NO_WHERE"))
		h.Write([]byte{0})
	}

	// Write GROUP BY fields (sorted)
	if query.GroupBy != nil {
		sortedGroupBy := make([]string, len(query.GroupBy.Fields))
		copy(sortedGroupBy, query.GroupBy.Fields)
		sort.Strings(sortedGroupBy)
		for _, field := range sortedGroupBy {
			h.Write([]byte(field))
			h.Write([]byte{0})
		}
	}

	// Write HAVING expression so queries that differ only in HAVING get different keys
	if query.HavingExpression != nil {
		h.Write([]byte("HAVING:"))
		if stringer, ok := query.HavingExpression.(interface{ String() string }); ok {
			h.Write([]byte(stringer.String()))
		} else {
			h.Write([]byte(fmt.Sprintf("%v", query.HavingExpression)))
		}
		h.Write([]byte{0})
	} else {
		h.Write([]byte("NO_HAVING"))
		h.Write([]byte{0})
	}

	// Write ORDER BY fields (sorted with direction)
	if query.OrderBy != nil {
		sortedOrderBy := make([]queryparser.OrderByField, len(query.OrderBy.Fields))
		copy(sortedOrderBy, query.OrderBy.Fields)
		sort.Slice(sortedOrderBy, func(i, j int) bool {
			return sortedOrderBy[i].FieldName < sortedOrderBy[j].FieldName
		})
		for _, orderField := range sortedOrderBy {
			h.Write([]byte(orderField.FieldName))
			binary.Write(h, binary.LittleEndian, int32(orderField.Direction))
		}
	}

	// Write limit/offset/top
	binary.Write(h, binary.LittleEndian, int64(query.Limit))
	binary.Write(h, binary.LittleEndian, int64(query.Offset))
	binary.Write(h, binary.LittleEndian, int64(query.TopCount))

	// Write generic marker
	if useGeneric {
		h.Write([]byte("generic"))
	}

	// TODO: When upgrading xxhash beyond v2.3.0, verify cache key compatibility by
	// comparing keys generated from identical queries across versions to ensure no
	// breaking changes in hash algorithm

	return h.Sum64()
}

// ComputeKeyExported exposes the internal computeKey so that enterprise result caching
// can reuse the same xxhash-based query fingerprint without duplicating the logic.
func (spc *ShardedPlanCache) ComputeKeyExported(query *queryparser.UnifiedSelectQuery, useGeneric bool) uint64 {
	return spc.computeKey(query, useGeneric)
}

// shouldUseGenericPlan determines if generic plan should be used (PostgreSQL-style)
func (spc *ShardedPlanCache) shouldUseGenericPlan(query *queryparser.UnifiedSelectQuery) bool {
	// Compute key for custom plan
	customKey := spc.computeKey(query, false)
	shard := spc.getShard(customKey)

	shard.mu.RLock()
	defer shard.mu.RUnlock()

	elem, ok := shard.cache[customKey]
	if !ok {
		return false // No custom plan yet, use custom
	}

	entry := elem.Value.(*CacheEntry)

	// Need at least threshold executions before considering generic
	threshold := settings.GetSettings().PlanCacheCustomThreshold
	if entry.execCount.Load() < int64(threshold) {
		return false
	}

	// Need custom plan costs collected
	if len(entry.customPlanCost) == 0 {
		return false
	}

	// Calculate average custom cost
	avgCustom := 0.0
	for _, cost := range entry.customPlanCost {
		avgCustom += cost
	}
	avgCustom /= float64(len(entry.customPlanCost))

	// Use generic if it exists and is within 110% of custom average
	return entry.genericPlanCost > 0 && entry.genericPlanCost <= avgCustom*1.1
}

// UpdatePlanStats updates execution statistics for a cached plan
func (spc *ShardedPlanCache) UpdatePlanStats(key uint64, duration time.Duration, isGeneric bool) {
	if spc == nil {
		return
	}

	shard := spc.getShard(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	elem, ok := shard.cache[key]
	if !ok {
		return
	}

	entry := elem.Value.(*CacheEntry)
	// Note: execCount already incremented atomically in get(); don't double-count
	entry.totalTime += duration
	ec := entry.execCount.Load()
	if ec > 0 {
		entry.avgTime = entry.totalTime / time.Duration(ec)
	}

	// Track costs for adaptive planning
	if !isGeneric && len(entry.customPlanCost) < 5 {
		entry.customPlanCost = append(entry.customPlanCost, duration.Seconds())
	} else if isGeneric {
		entry.genericPlanCost = duration.Seconds()
	}

	// Enterprise adaptive optimizer hook: record execution stats
	if reg := extension.GetRegistry(); reg.HasAdaptiveOptimizerExtension() {
		adaptiveExt := reg.GetAdaptiveOptimizerExtension()
		adaptiveExt.RecordExecution(key, extension.ExecutionStats{
			ElapsedMs: float64(duration.Milliseconds()),
			PlanCost:  entry.plan.Cost,
			Timestamp: time.Now(),
		})
	}
}

// RemoveBundleMetadata removes bundle entries from bundleInvalidations, staleServesByBundle,
// and each shard's collectionVersions. Call when a bundle is dropped to avoid unbounded growth.
func (spc *ShardedPlanCache) RemoveBundleMetadata(bundleName string) {
	if spc == nil {
		return
	}
	spc.bundleInvalidations.Delete(bundleName)
	spc.staleServesByBundle.Delete(bundleName)
	for i := 0; i < 8; i++ {
		shard := spc.shards[i]
		shard.mu.Lock()
		delete(shard.collectionVersions, bundleName)
		shard.mu.Unlock()
	}
	spc.logger.Debugf("Removed bundle %s from plan cache metadata", bundleName)
}

// InvalidateBundle invalidates all plans for a bundle (lazy invalidation via version bump)
func (spc *ShardedPlanCache) InvalidateBundle(bundleName string) {
	if spc == nil {
		return
	}

	// Increment version in all shards (lazy invalidation)
	for i := 0; i < 8; i++ {
		shard := spc.shards[i]
		shard.mu.Lock()
		shard.collectionVersions[bundleName]++
		newVersion := shard.collectionVersions[bundleName]
		shard.mu.Unlock()

		spc.logger.Debugf("Invalidated bundle %s on shard %d: newVersion=%d", bundleName, i, newVersion)
	}

	// Update global stats
	spc.stats.invalidations.Add(1)
	if spc.metricsReporter != nil {
		spc.metricsReporter("QueryPlanCacheInvalidations", 1)
	}

	// Update per-bundle invalidation counter
	counterInterface, _ := spc.bundleInvalidations.LoadOrStore(bundleName, &atomic.Uint64{})
	counter := counterInterface.(*atomic.Uint64)
	counter.Add(1)
}

// Shutdown gracefully shuts down the cache, draining async rebuilds
func (spc *ShardedPlanCache) Shutdown() {
	if spc == nil {
		return
	}

	spc.logger.Info("Shutting down plan cache...")

	// Set shutdown flag on all shards
	for i := 0; i < 8; i++ {
		spc.shards[i].shutdown.Store(true)
	}

	// Wait for all async rebuilds to complete
	for i := 0; i < 8; i++ {
		spc.shards[i].rebuildWg.Wait()
	}

	spc.logger.Info("Plan cache shutdown complete")
}

// Stats returns a snapshot of global cache statistics (hits, misses, evictions, etc.).
// Callers see current values from the live atomics.
func (spc *ShardedPlanCache) Stats() CacheStatsSnapshot {
	if spc == nil {
		return CacheStatsSnapshot{}
	}
	return CacheStatsSnapshot{
		Hits:          spc.stats.hits.Load(),
		Misses:        spc.stats.misses.Load(),
		Evictions:     spc.stats.evictions.Load(),
		Invalidations: spc.stats.invalidations.Load(),
		StaleServes:   spc.stats.staleServes.Load(),
	}
}

// HitRate returns the cache hit rate as a percentage
func (spc *ShardedPlanCache) HitRate() float64 {
	if spc == nil {
		return 0.0
	}

	hits := spc.stats.hits.Load()
	misses := spc.stats.misses.Load()
	total := hits + misses

	if total == 0 {
		return 0.0
	}

	return float64(hits) / float64(total)
}

// GetShardStats returns statistics for a specific shard
func (spc *ShardedPlanCache) GetShardStats(idx int) ShardStats {
	if spc == nil || idx < 0 || idx >= 8 {
		return ShardStats{}
	}
	return spc.shards[idx].stats
}

// GetBundleInvalidationCount returns the invalidation count for a bundle
func (spc *ShardedPlanCache) GetBundleInvalidationCount(bundleName string) uint64 {
	if spc == nil {
		return 0
	}

	counterInterface, ok := spc.bundleInvalidations.Load(bundleName)
	if !ok {
		return 0
	}
	return counterInterface.(*atomic.Uint64).Load()
}

// GetBundleStaleServeCount returns the stale serve count for a bundle
func (spc *ShardedPlanCache) GetBundleStaleServeCount(bundleName string) uint64 {
	if spc == nil {
		return 0
	}

	counterInterface, ok := spc.staleServesByBundle.Load(bundleName)
	if !ok {
		return 0
	}
	return counterInterface.(*atomic.Uint64).Load()
}

// TODO: Add SyndrQL directive support for query hints - parse `/*+ USE_GENERIC_PLAN */`
// and `/*+ USE_CUSTOM_PLAN */` from query string comments, store in QueryHints struct
// on UnifiedSelectQuery, check in shouldUseGenericPlan() to override automatic decision

// TODO: Add GraphQL directive support - parse `@plan(type: GENERIC)` or `@plan(type: CUSTOM)`
// from GraphQL operation definition, pass hint via execution context, provide override
// mechanism in plan cache Get() method
