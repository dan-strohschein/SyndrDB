Recommendation for SyndrDB
Given that SyndrDB is:

Written in Go
A document database with relational features
Has a GraphQL interface
Likely targeting modern workloads
My Recommendation: PostgreSQL-inspired with MongoDB-style invalidation
Here's the architecture I'd suggest:

Go
package queryplan

import (
    "sync"
    "time"
    "hash/fnv"
    "container/list"
)

// PlanCache is a thread-safe LRU cache for query plans
type PlanCache struct {
    mu          sync.RWMutex
    capacity    int
    cache       map[uint64]*list.Element
    lru         *list.List
    stats       CacheStats
    
    // Invalidation tracking
    collectionVersions map[string]uint64
}

type CacheEntry struct {
    key         uint64
    plan        *QueryPlan
    collection  string
    collectionVersion uint64
    
    // Statistics
    createdAt   time.Time
    lastUsed    time.Time
    execCount   int64
    totalTime   time.Duration
    avgTime     time.Duration
    
    // PostgreSQL-style: track generic vs custom
    customPlanCost  []float64  // First N executions
    genericPlanCost float64
    useGeneric      bool
}

type CacheStats struct {
    hits        atomic.Int64
    misses      atomic.Int64
    evictions   atomic.Int64
    invalidations atomic.Int64
}

// QueryPlan represents a compiled execution plan
type QueryPlan struct {
    Steps       []PlanStep
    Indexes     []string
    Estimated   PlanCost
    Fingerprint uint64
}

type PlanStep interface {
    Execute(ctx context.Context) (ResultSet, error)
    Explain() string
}

type PlanCost struct {
    EstimatedRows int64
    EstimatedCost float64
    IndexScans    int
    FullScans     int
}
Core Implementation:
Go
// Get retrieves or builds a plan
func (pc *PlanCache) Get(
    ctx context.Context,
    query *Query,
    useGeneric bool,
) (*QueryPlan, error) {
    key := pc.computeKey(query, useGeneric)
    
    pc.mu.RLock()
    if elem, ok := pc.cache[key]; ok {
        entry := elem.Value.(*CacheEntry)
        
        // Check if plan is still valid
        currentVersion := pc.collectionVersions[entry.collection]
        if entry.collectionVersion == currentVersion {
            pc.lru.MoveToFront(elem)
            entry.lastUsed = time.Now()
            entry.execCount++
            pc.mu.RUnlock()
            
            pc.stats.hits.Add(1)
            return entry. plan, nil
        }
        
        // Plan is stale
        pc.mu.RUnlock()
        pc.mu.Lock()
        delete(pc.cache, key)
        pc.lru.Remove(elem)
        pc.stats.invalidations.Add(1)
        pc.mu.Unlock()
    } else {
        pc.mu. RUnlock()
    }
    
    pc.stats.misses.Add(1)
    
    // Build new plan
    plan, err := pc.buildPlan(ctx, query)
    if err != nil {
        return nil, err
    }
    
    // Cache it
    pc.insert(key, query. Collection, plan, useGeneric)
    
    return plan, nil
}

// computeKey creates a cache key from query structure
func (pc *PlanCache) computeKey(query *Query, useGeneric bool) uint64 {
    h := fnv.New64a()
    
    // Normalize query
    normalized := query. Normalize()
    
    h.Write([]byte(normalized. Collection))
    h.Write([]byte(normalized.Operation))
    
    // Serialize filter structure (not values)
    for field, op := range normalized. Filters {
        h.Write([]byte(field))
        h.Write([]byte(op. Type))
    }
    
    // Include sort, projection, etc.
    for _, field := range normalized.Sort {
        h.Write([]byte(field))
    }
    
    if useGeneric {
        h. Write([]byte("generic"))
    }
    
    return h.Sum64()
}

// InvalidateCollection invalidates all plans for a collection
func (pc *PlanCache) InvalidateCollection(collection string) {
    pc.mu.Lock()
    defer pc.mu.Unlock()
    
    pc.collectionVersions[collection]++
    
    // Lazy invalidation - plans check version on access
    // Could also eagerly remove, but that's O(n)
}

// InvalidateOnWrite determines when to invalidate
type InvalidationPolicy struct {
    writesSinceInvalidation map[string]int64
    threshold               int64  // Default: 1000 like MongoDB
}

func (ip *InvalidationPolicy) OnWrite(collection string, cache *PlanCache) {
    count := atomic.AddInt64(&ip.writesSinceInvalidation[collection], 1)
    
    if count >= ip.threshold {
        cache.InvalidateCollection(collection)
        atomic.StoreInt64(&ip.writesSinceInvalidation[collection], 0)
    }
}

// insert adds a plan to cache with LRU eviction
func (pc *PlanCache) insert(key uint64, collection string, plan *QueryPlan, isGeneric bool) {
    pc.mu.Lock()
    defer pc.mu.Unlock()
    
    // Check if already exists (race condition)
    if elem, ok := pc.cache[key]; ok {
        pc.lru.MoveToFront(elem)
        return
    }
    
    // Evict if at capacity
    if pc.lru.Len() >= pc.capacity {
        oldest := pc.lru.Back()
        if oldest != nil {
            entry := oldest.Value.(*CacheEntry)
            delete(pc. cache, entry.key)
            pc.lru.Remove(oldest)
            pc.stats.evictions.Add(1)
        }
    }
    
    entry := &CacheEntry{
        key:               key,
        plan:              plan,
        collection:        collection,
        collectionVersion: pc.collectionVersions[collection],
        createdAt:         time.Now(),
        lastUsed:          time.Now(),
        useGeneric:        isGeneric,
        customPlanCost:    make([]float64, 0, 5),
    }
    
    elem := pc.lru.PushFront(entry)
    pc. cache[key] = elem
}

// PostgreSQL-style: decide generic vs custom
func (pc *PlanCache) ShouldUseGenericPlan(query *Query) bool {
    key := pc.computeKey(query, false)
    
    pc.mu.RLock()
    elem, ok := pc.cache[key]
    pc.mu.RUnlock()
    
    if !ok || elem.Value.(*CacheEntry).execCount < 5 {
        return false  // Use custom for first 5
    }
    
    entry := elem.Value.(*CacheEntry)
    
    // Compare average custom cost to generic cost
    avgCustom := 0.0
    for _, cost := range entry.customPlanCost {
        avgCustom += cost
    }
    avgCustom /= float64(len(entry.customPlanCost))
    
    // Use generic if it's within 110% of custom average
    return entry.genericPlanCost <= avgCustom*1.1
}

// Expose metrics for monitoring
func (pc *PlanCache) Stats() CacheStats {
    return CacheStats{
        hits:          pc.stats.hits.Load(),
        misses:        pc. stats.misses.Load(),
        evictions:     pc.stats.evictions.Load(),
        invalidations: pc.stats.invalidations.Load(),
    }
}

func (pc *PlanCache) HitRate() float64 {
    hits := pc.stats.hits.Load()
    misses := pc. stats.misses.Load()
    total := hits + misses
    
    if total == 0 {
        return 0
    }
    
    return float64(hits) / float64(total)
}
GraphQL Integration:
Go
// GraphQL-specific caching
type GraphQLPlanCache struct {
    planCache  *PlanCache
    queryCache sync.Map  // GraphQL query string -> parsed query
}

func (gpc *GraphQLPlanCache) Execute(
    ctx context.Context,
    gqlQuery string,
    variables map[string]interface{},
) (interface{}, error) {
    // Step 1: Parse GraphQL (cache this too)
    parsed, err := gpc.parseGraphQL(gqlQuery)
    if err != nil {
        return nil, err
    }
    
    // Step 2: Translate to database query
    dbQuery := gpc.translateToDBQuery(parsed, variables)
    
    // Step 3: Get execution plan (cached)
    useGeneric := len(variables) > 0
    plan, err := gpc.planCache.Get(ctx, dbQuery, useGeneric)
    if err != nil {
        return nil, err
    }
    
    // Step 4: Execute
    start := time.Now()
    result, err := plan.Execute(ctx, dbQuery, variables)
    duration := time.Since(start)
    
    // Step 5: Update statistics
    gpc.updatePlanStats(plan, duration)
    
    return result, err
}
Pros of This Recommendation
✅ 1. Proven Design Patterns
Combines battle-tested approaches from PostgreSQL and MongoDB
LRU eviction is simple and effective
Thread-safe with minimal lock contention (RWMutex)
✅ 2. Go-Native Performance
Go
// Go advantages:
- sync.Map for concurrent access without locks
- atomic operations for statistics
- Goroutine-friendly (no complex locking)
- Great profiling tools (pprof)
✅ 3. Smart Invalidation
Lazy invalidation: Version checking is O(1)
Write-threshold: Avoids invalidating on every write
Collection-scoped: Surgical invalidation
✅ 4. Adaptive Planning
Generic vs. custom plan logic handles parameter variance
Avoids SQL Server's "parameter sniffing" problems
Gracefully degrades for diverse workloads
✅ 5. Observable
Go
// Built-in metrics:
- Hit rate
- Avg execution time per plan
- Eviction rate
- Invalidation frequency
✅ 6. GraphQL-Optimized
Two-tier caching: GraphQL parse + execution plan
Handles parameterized GraphQL queries well
Fragment caching potential
✅ 7. Memory Bounded
Configurable capacity
LRU prevents unbounded growth
Per-entry size tracking possible
Cons of This Recommendation
❌ 1. Complex Implementation
Problem:

Plan building logic is complex
Generic vs. custom decision adds complexity
Normalization needs to be perfect
Mitigation:

Go
// Start simple, add complexity incrementally:

// Phase 1: Basic LRU cache
type SimplePlanCache struct {
    cache map[uint64]*QueryPlan
    lru   *list.List
}

// Phase 2: Add statistics

// Phase 3: Add generic/custom logic

// Phase 4: Add advanced invalidation

// Use feature flags:
type PlanCacheConfig struct {
    EnableGenericPlans bool
    EnableAdaptive     bool
    EnableStatistics   bool
}
❌ 2. Memory Overhead
Problem:

Each plan stores multiple copies of statistics
Large query plans consume significant memory
LRU linked list has pointer overhead
Mitigation:

Go
// 1. Configurable capacity
config := PlanCacheConfig{
    MaxEntries:     1000,    // Limit number of plans
    MaxPlanSize:    1 << 20, // 1MB per plan max
    MaxTotalMemory: 100 << 20, // 100MB total
}

// 2. Memory-aware eviction
type MemoryAwarePlanCache struct {
    currentMemory atomic.Int64
    maxMemory     int64
}

func (mc *MemoryAwarePlanCache) insert(entry *CacheEntry) {
    entrySize := entry.plan.EstimateMemoryUsage()
    
    // Evict until we have space
    for mc.currentMemory.Load()+entrySize > mc.maxMemory {
        mc.evictOne()
    }
    
    mc.currentMemory.Add(entrySize)
    // ... insert logic
}

// 3. Compression for old plans
func (entry *CacheEntry) Compress() {
    if time.Since(entry.lastUsed) > 5*time. Minute {
        entry.plan = CompressPlan(entry.plan)
    }
}
❌ 3. Invalidation Can Be Too Aggressive
Problem:

Index creation invalidates all plans
Statistics updates might invalidate unnecessarily
Cold start after invalidation is expensive
Mitigation:

Go
// 1. Granular invalidation
type InvalidationEvent struct {
    Type       string  // "index_add", "index_drop", "stats_update"
    Collection string
    Details    interface{}
}

func (pc *PlanCache) OnInvalidation(event InvalidationEvent) {
    switch event.Type {
    case "index_add":
        // Only invalidate plans that COULD benefit from new index
        pc.InvalidateIfCouldUsIndex(event.Collection, event.Details.(Index))
        
    case "index_drop":
        // Only invalidate plans that USED the dropped index
        pc.InvalidateIfUsedIndex(event.Collection, event.Details.(string))
        
    case "stats_update":
        // Only invalidate if statistics changed significantly
        if event.Details.(StatsDelta). IsSignificant() {
            pc. InvalidateCollection(event.Collection)
        }
    }
}

// 2. Warm-up after invalidation
func (pc *PlanCache) WarmupAfterInvalidation(collection string) {
    // Rebuild plans for most common queries
    go pc.warmupWorker(collection)
}

// 3. Graceful degradation
func (pc *PlanCache) Get(ctx context.Context, query *Query) (*QueryPlan, error) {
    plan, valid := pc.getIfValid(query)
    
    if !valid {
        // Use stale plan while building new one
        go pc.asyncRebuildPlan(query)
        
        if plan != nil {
            return plan, nil  // Stale but usable
        }
    }
    
    return plan, nil
}
❌ 4. Generic vs. Custom Logic Adds Latency
Problem:

First 5 executions use custom plans (slower)
Decision logic adds overhead
May choose wrong plan type
Mitigation:

Go
// 1. Make it configurable
type PlanCacheConfig struct {
    CustomPlanThreshold int  // Default 5, set to 0 to always use generic
    EnableAdaptive      bool // Disable for predictable latency
}

// 2.  Hints/directives
type QueryHints struct {
    ForceGenericPlan bool
    ForceCustomPlan  bool
}

// GraphQL directive:
query GetUser($id: ID!) @plan(type: GENERIC) {
    user(id: $id) { name }
}

// 3. Fast-path for simple queries
func (pc *PlanCache) Get(ctx context.Context, query *Query) (*QueryPlan, error) {
    if query.IsSimple() {
        // Skip generic/custom logic for simple queries
        return pc.getGenericPlan(query)
    }
    
    // ...  full logic for complex queries
}
❌ 5. Hash Collisions
Problem:

Different queries might hash to same key
Normalization bugs cause cache misses
fnv hash is fast but not cryptographic
Mitigation:

Go
// 1. Better hashing
import "crypto/sha256"

func (pc *PlanCache) computeKey(query *Query) uint64 {
    // For cache key, use fast hash
    h := fnv. New64a()
    normalized := query.Normalize()
    
    // Serialize deterministically
    bytes := normalized.ToBytes()  // Must be deterministic! 
    h.Write(bytes)
    
    key := h.Sum64()
    
    // Collision detection: store query fingerprint
    return key
}

// 2. Store query fingerprint for verification
type CacheEntry struct {
    key             uint64
    queryFingerprint string  // Human-readable query structure
    plan            *QueryPlan
}

func (pc *PlanCache) Get(ctx context.Context, query *Query) (*QueryPlan, error) {
    key := pc. computeKey(query)
    entry := pc.cache[key]
    
    // Verify it's actually the same query structure
    if entry.queryFingerprint != query.Fingerprint() {
        // Hash collision!  This is rare but handle it
        pc.stats.collisions.Add(1)
        return nil, ErrCacheMiss
    }
    
    return entry.plan, nil
}

// 3. Comprehensive normalization testing
func TestNormalization(t *testing.T) {
    tests := []struct{
        query1, query2 string
        shouldMatch bool
    }{
        {
            "SELECT * FROM users WHERE id = 1",
            "SELECT * FROM users WHERE id = 2",
            true,  // Should normalize to same structure
        },
        {
            "SELECT * FROM users WHERE id = 1",
            "SELECT * FROM users WHERE id = 1 AND active = true",
            false,  // Different structure
        },
    }
    
    for _, tt := range tests {
        q1 := Parse(tt.query1)
        q2 := Parse(tt. query2)
        
        key1 := computeKey(q1)
        key2 := computeKey(q2)
        
        if tt.shouldMatch && key1 != key2 {
            t. Errorf("Expected same key for %s and %s", tt.query1, tt.query2)
        }
    }
}
❌ 6. Lock Contention on Hot Queries
Problem:

RWMutex can become bottleneck
Single hot query serializes access
LRU updates require write lock
Mitigation:

Go
// 1. Sharded cache
type ShardedPlanCache struct {
    shards    []*PlanCache
    shardMask uint64
}

func (spc *ShardedPlanCache) getShard(key uint64) *PlanCache {
    return spc.shards[key&spc.shardMask]
}

func NewShardedPlanCache(shardCount int) *ShardedPlanCache {
    shards := make([]*PlanCache, shardCount)
    for i := range shards {
        shards[i] = NewPlanCache(capacity / shardCount)
    }
    
    return &ShardedPlanCache{
        shards:    shards,
        shardMask: uint64(shardCount - 1),
    }
}

// 2.  Lock-free fast path
type LockFreePlanCache struct {
    cache sync.Map  // Lock-free reads
    lru   *LRU      // Only for eviction
}

func (lfpc *LockFreePlanCache) Get(key uint64) (*QueryPlan, bool) {
    // Lock-free read
    val, ok := lfpc.cache.Load(key)
    if ! ok {
        return nil, false
    }
    
    entry := val.(*CacheEntry)
    
    // Update stats without locking
    atomic.AddInt64(&entry.execCount, 1)
    atomic.StoreInt64(&entry.lastUsedNano, time.Now().UnixNano())
    
    return entry.plan, true
}

// 3. Approximate LRU (less locking)
type ApproximateLRU struct {
    segments [8]*LRUSegment  // 8 segments
}

func (alru *ApproximateLRU) Access(key uint64) {
    segment := alru.segments[key%8]
    segment.MoveToFront(key)  // Only locks 1/8 of cache
}
❌ 7. Testing Complexity
Problem:

Hard to test invalidation logic
Timing-dependent bugs
Race conditions
Mitigation:

Go
// 1.  Deterministic testing
type MockClock struct {
    now time.Time
}

func (mc *MockClock) Now() time.Time {
    return mc. now
}

func (mc *MockClock) Advance(d time.Duration) {
    mc.now = mc.now.Add(d)
}

// Inject clock into cache
type PlanCache struct {
    clock Clock  // Interface: Now() time.Time
}

// 2. Race detector
// go test -race ./... 

// 3. Chaos testing
func TestCacheChaos(t *testing. T) {
    cache := NewPlanCache(100)
    
    // Concurrent: gets, inserts, invalidations
    var wg sync.WaitGroup
    for i := 0; i < 10; i++ {
        wg.Add(3)
        
        go func() {
            defer wg.Done()
            for j := 0; j < 1000; j++ {
                cache.Get(randomQuery())
            }
        }()
        
        go func() {
            defer wg.Done()
            for j := 0; j < 100; j++ {
                cache. Insert(randomQuery(), randomPlan())
            }
        }()
        
        go func() {
            defer wg.Done()
            for j := 0; j < 50; j++ {
                cache.InvalidateCollection(randomCollection())
            }
        }()
    }
    
    wg.Wait()
    
    // Verify invariants
    assert.True(t, cache.lru.Len() <= cache.capacity)
    assert.Equal(t, len(cache.cache), cache.lru.Len())
}
Alternative Approaches & Tradeoffs
Alternative 1: No Generic Plans (Always Custom)
Go
// Simpler: just cache exact query shapes
type SimplePlanCache struct {
    cache map[uint64]*QueryPlan
    lru   *list. List
}
Pros:

Simpler implementation
Optimal plans for each query
No "first 5 executions" warmup
Cons:

More cache entries (cache bloat)
Parameter sniffing issues like SQL Server
Lower hit rate
When to use: If you have predictable query patterns

Alternative 2: Time-based Invalidation (No Version Tracking)
Go
type TTLPlanCache struct {
    entries map[uint64]*CacheEntry
}

type CacheEntry struct {
    plan      *QueryPlan
    expiresAt time.Time
}
Pros:

Dead simple
No invalidation logic needed
Predictable memory usage
Cons:

Stale plans used until TTL expires
Wastes CPU rebuilding fresh plans
Hard to tune TTL
When to use: If you can tolerate stale plans

Alternative 3: Per-Connection Cache (No Sharing)
Go
// Each connection has its own cache
type Connection struct {
    planCache *PlanCache
}
Pros:

No lock contention
No synchronization needed
Simple lifetime management
Cons:

Massive memory waste
No plan reuse across connections
Connection pooling defeats the purpose
When to use: Single-tenant workloads

Implementation Roadmap for SyndrDB
Phase 1: MVP (Week 1-2)
Go
// Basic LRU cache, no fancy features
type BasicPlanCache struct {
    mu       sync.RWMutex
    capacity int
    cache    map[uint64]*QueryPlan
    lru      *list.List
}

// Features:
- Simple LRU eviction
- Manual invalidation only
- Basic hit/miss stats
- No generic/custom logic
Phase 2: Statistics (Week 3)
Go
// Add observability
- Execution count
- Average execution time
- Hit rate metrics
- Prometheus integration
Phase 3: Smart Invalidation (Week 4-5)
Go
// Add automatic invalidation
- Version tracking
- Write-threshold invalidation
- Index change detection
Phase 4: Adaptive Planning (Week 6-8)
Go
// Add generic/custom logic
- First-N custom execution
- Cost comparison
- Automatic switching
Phase 5: Production Hardening (Week 9-10)
Go
// Add robustness
- Memory limits
- Graceful degradation
- Warmup after invalidation
- Comprehensive testing
Final Recommendation Summary
For SyndrDB, I recommend:

✅ Start with Phase 1 (basic LRU) and ship it
✅ Implement PostgreSQL-style generic/custom logic in Phase 4
✅ Use MongoDB-style write-threshold invalidation
✅ Shard the cache (8-16 shards) for concurrency
✅ Make everything configurable via config file
Go
// config.yaml
plan_cache:
  enabled: true
  capacity: 10000
  shards: 16
  adaptive_planning: true
  custom_plan_threshold: 5
  invalidation:
    write_threshold: 1000
    on_index_change: true
    on_stats_change: true
  memory:
    max_total_mb: 500
    max_per_plan_mb: 5
This gives you:

✅ Production-ready performance
✅ Observable and debuggable
✅ Incremental delivery
✅ Proven design patterns
✅ Go-native implementation