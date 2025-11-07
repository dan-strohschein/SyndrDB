# DataLoader Pattern & Deep Relationship Optimization - Implementation Plan

**Phase**: 10 (Post-Phase 8 & 9)  
**Status**: Planning  
**Priority**: High (Resolves N+1 Query Problem)  
**Estimated Complexity**: 8/10

---

## Table of Contents

1. [Problem Statement](#problem-statement)
2. [Solution Architecture](#solution-architecture)
3. [Implementation Strategy](#implementation-strategy)
4. [File Structure](#file-structure)
5. [Detailed Component Design](#detailed-component-design)
6. [Integration Points](#integration-points)
7. [Testing Strategy](#testing-strategy)
8. [Performance Benchmarks](#performance-benchmarks)
9. [Migration Path](#migration-path)
10. [Risk Analysis](#risk-analysis)

---

## Problem Statement

### Current N+1 Query Problem

**Scenario**: Query 100 authors with their books
```graphql
{
  authors(first: 100) {
    id
    name
    books {
      id
      title
    }
  }
}
```

**Current Execution** (`relationship_resolver.go` lines 180-250):
```
1. Query authors: SELECT FROM authors LIMIT 100          [1 query]
2. For each author (100 iterations):
   - Query books: SELECT FROM books WHERE authorId = ?   [100 queries]
   
Total: 101 queries (1 + 100)
Time: ~2000ms (assuming 20ms per query)
```

**With Deep Nesting** (3 levels):
```graphql
{
  authors {
    books {
      reviews {
        user {
          ...
        }
      }
    }
  }
}
```

**Execution**:
```
- 1 query for authors (10 results)
- 10 queries for books (50 total books)
- 50 queries for reviews (200 total reviews)
- 200 queries for users

Total: 261 queries for 4-level query
Time: ~5,220ms
```

### Deep Relationship Optimization Challenges

1. **Query Explosion**: Each level multiplies query count
2. **Sequential Execution**: Each query waits for previous to complete
3. **Duplicate Queries**: Same relationships queried multiple times
4. **Memory Pressure**: Holding all intermediate results
5. **Cache Inefficiency**: No coordination between relationship resolvers

---

## Solution Architecture

### DataLoader Pattern Overview

**Core Concept**: Batch and cache relationship resolution within a single request context.

```
┌─────────────────────────────────────────────────────────────────────┐
│                      GraphQL Request Context                        │
│                                                                     │
│  ┌──────────────────────────────────────────────────────────────┐  │
│  │                     Request Lifecycle                        │  │
│  │                                                              │  │
│  │  1. Parse Query                                              │  │
│  │  2. Create DataLoader for each bundle type                   │  │
│  │  3. Execute query (collects load requests)                   │  │
│  │  4. DataLoader batches pending loads (10ms window)           │  │
│  │  5. Execute batched queries                                  │  │
│  │  6. Return results (served from cache)                       │  │
│  └──────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────┐
│                       DataLoader Architecture                       │
│                                                                     │
│  ┌─────────────────┐    ┌─────────────────┐    ┌───────────────┐  │
│  │  Request Queue  │───▶│  Batch Window   │───▶│ Batch Loader  │  │
│  │                 │    │  (10ms timeout)  │    │               │  │
│  │ - Load(id1)     │    │                  │    │ LoadMany(ids) │  │
│  │ - Load(id2)     │    │ Collect: id1-id5 │    │ ↓             │  │
│  │ - Load(id3)     │    │                  │    │ Single Query  │  │
│  │ - Load(id4)     │    │ When full or     │    │ WHERE IN (...)│  │
│  │ - Load(id5)     │    │ timeout expires  │    │               │  │
│  └─────────────────┘    └─────────────────┘    └───────────────┘  │
│                                                          │          │
│  ┌─────────────────────────────────────────────────────┘          │
│  ▼                                                                  │
│  ┌─────────────────┐                                                │
│  │  Result Cache   │                                                │
│  │                 │                                                │
│  │ id1 → doc1      │  ◀── Subsequent Load(id1) returns cached     │
│  │ id2 → doc2      │                                                │
│  │ id3 → doc3      │                                                │
│  └─────────────────┘                                                │
└─────────────────────────────────────────────────────────────────────┘
```

### Deep Relationship Optimization Strategy

**Approach**: Depth-First Query Planning with Batching

```
┌─────────────────────────────────────────────────────────────────────┐
│              Deep Relationship Query Planner                        │
│                                                                     │
│  1. QUERY ANALYSIS PHASE:                                           │
│     - Parse query selection set                                     │
│     - Build relationship tree                                       │
│     - Calculate depth (max: 5 levels)                               │
│     - Estimate query count                                          │
│                                                                     │
│  2. OPTIMIZATION DECISION:                                          │
│                                                                     │
│     IF depth <= 2 AND breadth <= 50:                                │
│       → Use DataLoader pattern (standard batching)                  │
│                                                                     │
│     ELSE IF depth <= 4:                                             │
│       → Use JOIN-based optimization (single query with JOINs)       │
│                                                                     │
│     ELSE:                                                           │
│       → Reject query (complexity limit exceeded)                    │
│                                                                     │
│  3. EXECUTION STRATEGIES:                                           │
│                                                                     │
│     Strategy A: DataLoader Batching (Default)                       │
│     ┌──────────────────────────────────────────────┐               │
│     │ Level 0: authors           [1 query]         │               │
│     │          ↓ (collect ids)                     │               │
│     │ Level 1: books             [1 batched query] │               │
│     │          ↓ (collect ids)                     │               │
│     │ Level 2: reviews           [1 batched query] │               │
│     │                                              │               │
│     │ Total: 3 queries (vs 151 without batching)  │               │
│     └──────────────────────────────────────────────┘               │
│                                                                     │
│     Strategy B: JOIN Optimization (Deep queries)                    │
│     ┌──────────────────────────────────────────────┐               │
│     │ SELECT                                        │               │
│     │   a.*, b.*, r.*                               │               │
│     │ FROM authors a                                │               │
│     │ LEFT JOIN books b ON b.authorId = a.id       │               │
│     │ LEFT JOIN reviews r ON r.bookId = b.id       │               │
│     │                                              │               │
│     │ Total: 1 query (best performance)            │               │
│     └──────────────────────────────────────────────┘               │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Implementation Strategy

### Phase 1: Core DataLoader (Week 1-2)

**Priority 1 Components**:
1. `dataloader.go` - Generic DataLoader implementation
2. `request_context.go` - Per-request loader registry
3. `loader_factory.go` - Bundle-specific loader creation

**Deliverables**:
- ✅ DataLoader with batching window (10ms default)
- ✅ Per-request caching
- ✅ Load, LoadMany, Prime methods
- ✅ Unit tests (95%+ coverage)

### Phase 2: Relationship Integration (Week 3)

**Priority 2 Components**:
1. Update `relationship_resolver.go` to use DataLoader
2. Update `handler.go` to create request context
3. Add loader factory for each bundle type

**Deliverables**:
- ✅ Relationship resolver uses DataLoader
- ✅ Backward compatible with existing queries
- ✅ Performance benchmarks showing improvement

### Phase 3: Deep Query Optimization (Week 4-5)

**Priority 3 Components**:
1. `query_analyzer.go` - Depth/complexity analysis
2. `join_optimizer.go` - JOIN-based query generation
3. `query_strategy_selector.go` - Strategy selection logic

**Deliverables**:
- ✅ Automatic strategy selection
- ✅ JOIN optimization for deep queries
- ✅ Query complexity limits

### Phase 4: Advanced Features (Week 6)

**Priority 4 Components**:
1. Parallel batch execution
2. Smart cache warming
3. Query result streaming
4. Performance monitoring

---

## File Structure

```
src/internal/graphQL/
├── dataloader/
│   ├── dataloader.go                  # NEW: Core DataLoader implementation
│   ├── batch_scheduler.go             # NEW: Batch window management
│   ├── cache.go                       # NEW: Request-scoped cache
│   └── dataloader_test.go             # NEW: Unit tests
│
├── context/
│   ├── request_context.go             # NEW: Per-request context with loaders
│   ├── loader_registry.go             # NEW: Manages loader instances
│   └── context_test.go                # NEW: Context tests
│
├── optimization/
│   ├── query_analyzer.go              # NEW: Analyzes query depth/complexity
│   ├── join_optimizer.go              # NEW: Generates JOIN-based queries
│   ├── strategy_selector.go           # NEW: Selects execution strategy
│   └── optimization_test.go           # NEW: Optimization tests
│
├── relationship_resolver.go           # UPDATED: Use DataLoader
├── handler.go                         # UPDATED: Create request context
└── pagination.go                      # UPDATED: Use DataLoader for cursors

tests/
└── graphQL/
    ├── dataloader_integration_test.go # NEW: E2E DataLoader tests
    ├── deep_query_test.go             # NEW: Deep relationship tests
    └── performance_benchmark_test.go  # NEW: Benchmark comparisons
```

---

## Detailed Component Design

### 1. Core DataLoader (`dataloader/dataloader.go`)

```go
// Package dataloader implements a generic batching and caching loader for SyndrDB
//
// PURPOSE:
// This file solves the N+1 query problem by batching multiple Load() requests
// into a single database query. It's designed specifically for GraphQL relationship
// resolution where multiple documents may need to load the same related data.
//
// KEY FEATURES:
// - Automatic request batching with configurable window (default 10ms)
// - Per-request caching to avoid duplicate loads
// - Generic implementation works with any key-value pairs
// - Thread-safe for concurrent GraphQL field resolution
//
// DATALOADER ALGORITHM:
//
//   Request 1: Load("user-1")  ─┐
//   Request 2: Load("user-2")  ─┤
//   Request 3: Load("user-1")  ─┤── Batch Window (10ms)
//   Request 4: Load("user-3")  ─┤
//   Request 5: Load("user-4")  ─┘
//                                │
//                                ▼
//   LoadBatch(["user-1", "user-2", "user-3", "user-4"])
//   Single query: WHERE id IN ('user-1', 'user-2', 'user-3', 'user-4')
//                                │
//                                ▼
//   Return results to all 5 requesters (Request 3 served from cache)
//
// DESIGN PRINCIPLES:
// - Single Responsibility: Only handles batching and caching logic
// - Open/Closed: Extensible via BatchLoadFunc without modification
// - Dependency Inversion: Depends on BatchLoadFunc interface, not concrete types

package dataloader

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// BatchLoadFunc is a function that loads documents by their IDs
// Returns a map of ID → document for all successfully loaded items
// Missing IDs are returned as nil in the result map
type BatchLoadFunc func(ctx context.Context, keys []string) (map[string]interface{}, error)

// DataLoader batches and caches Load requests
type DataLoader struct {
	// Configuration
	batchLoadFunc BatchLoadFunc
	batchWindow   time.Duration // How long to wait before dispatching batch
	maxBatchSize  int           // Maximum items per batch (prevent huge queries)

	// State
	mu       sync.Mutex
	cache    map[string]interface{}  // Request-scoped cache
	queue    []string                // Pending load requests
	dispatch map[string][]chan result // key → channels waiting for result
	timer    *time.Timer             // Batch window timer
}

// result is the internal structure for communicating load results
type result struct {
	value interface{}
	err   error
}

// DataLoaderConfig configures DataLoader behavior
type DataLoaderConfig struct {
	BatchWindow  time.Duration // Default: 10ms
	MaxBatchSize int           // Default: 1000
	EnableCache  bool          // Default: true
}

// NewDataLoader creates a new DataLoader instance
//
// PARAMETERS:
//   - batchLoadFunc: Function that loads multiple items by ID
//   - config: Configuration options (nil uses defaults)
//
// RETURNS:
//   - *DataLoader: Configured loader ready for use
//
// EXAMPLE:
//   loader := NewDataLoader(func(ctx context.Context, ids []string) (map[string]interface{}, error) {
//       // Load books WHERE id IN (ids...)
//       return sm.BundleService.GetDocumentsByIDs(bundle, ids)
//   }, &DataLoaderConfig{
//       BatchWindow: 10 * time.Millisecond,
//       MaxBatchSize: 1000,
//   })
func NewDataLoader(batchLoadFunc BatchLoadFunc, config *DataLoaderConfig) *DataLoader {
	if config == nil {
		config = &DataLoaderConfig{
			BatchWindow:  10 * time.Millisecond,
			MaxBatchSize: 1000,
			EnableCache:  true,
		}
	}

	return &DataLoader{
		batchLoadFunc: batchLoadFunc,
		batchWindow:   config.BatchWindow,
		maxBatchSize:  config.MaxBatchSize,
		cache:         make(map[string]interface{}),
		queue:         make([]string, 0),
		dispatch:      make(map[string][]chan result),
	}
}

// Load loads a single item by key
//
// This is the primary method used by relationship resolvers. Multiple concurrent
// Load() calls are automatically batched together if they occur within the batch window.
//
// ALGORITHM:
//  1. Check cache - return immediately if cached
//  2. Add key to pending queue
//  3. Register result channel for this request
//  4. Start batch timer if not already running
//  5. Wait for result on channel
//  6. Return value when batch completes
//
// THREAD SAFETY: Safe to call from multiple goroutines
//
// EXAMPLE:
//   // In relationship resolver, instead of:
//   docs := sm.Query("books WHERE authorId = ?", authorID)
//
//   // Use DataLoader:
//   books := loader.Load(ctx, authorID)
func (dl *DataLoader) Load(ctx context.Context, key string) (interface{}, error) {
	// Check cache first
	dl.mu.Lock()
	if cached, found := dl.cache[key]; found {
		dl.mu.Unlock()
		return cached, nil
	}

	// Create result channel for this request
	resultChan := make(chan result, 1)

	// Add to queue and register for notification
	dl.queue = append(dl.queue, key)
	dl.dispatch[key] = append(dl.dispatch[key], resultChan)

	// Start timer if not running
	if dl.timer == nil {
		dl.timer = time.AfterFunc(dl.batchWindow, func() {
			dl.dispatchBatch(ctx)
		})
	}

	// Check if batch is full
	if len(dl.queue) >= dl.maxBatchSize {
		// Dispatch immediately
		dl.timer.Stop()
		dl.timer = nil
		go dl.dispatchBatch(ctx)
	}

	dl.mu.Unlock()

	// Wait for result
	select {
	case res := <-resultChan:
		return res.value, res.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// LoadMany loads multiple items by keys
//
// This is more efficient than calling Load() multiple times because it knows
// all keys up front and can dispatch immediately.
//
// RETURNS:
//   - map[string]interface{}: Map of key → value for all found items
//   - error: Any errors during batch load
func (dl *DataLoader) LoadMany(ctx context.Context, keys []string) (map[string]interface{}, error) {
	results := make(map[string]interface{})
	var errs []error

	for _, key := range keys {
		value, err := dl.Load(ctx, key)
		if err != nil {
			errs = append(errs, err)
		} else {
			results[key] = value
		}
	}

	if len(errs) > 0 {
		return results, fmt.Errorf("errors loading %d keys: %v", len(errs), errs)
	}

	return results, nil
}

// Prime seeds the cache with a known value
//
// Use this to pre-populate the cache with data you already have, avoiding
// unnecessary database queries.
//
// EXAMPLE:
//   // After loading parent documents, prime the loader with their IDs
//   for _, author := range authors {
//       loader.Prime(author.ID, author)
//   }
func (dl *DataLoader) Prime(key string, value interface{}) {
	dl.mu.Lock()
	defer dl.mu.Unlock()

	dl.cache[key] = value
}

// dispatchBatch executes the pending batch
//
// ALGORITHM:
//  1. Collect all pending keys (deduplicate)
//  2. Call batchLoadFunc with keys
//  3. Distribute results to waiting channels
//  4. Cache results for future requests
//  5. Clear queue for next batch
//
// THREAD SAFETY: Called from timer goroutine, uses mutex
func (dl *DataLoader) dispatchBatch(ctx context.Context) {
	dl.mu.Lock()

	// Copy queue and clear for next batch
	keys := make([]string, len(dl.queue))
	copy(keys, dl.queue)
	dl.queue = dl.queue[:0]

	// Copy dispatch map and clear
	dispatch := dl.dispatch
	dl.dispatch = make(map[string][]chan result)

	dl.mu.Unlock()

	// Deduplicate keys
	uniqueKeys := deduplicateKeys(keys)

	// Execute batch load
	results, err := dl.batchLoadFunc(ctx, uniqueKeys)

	// Distribute results
	for key, channels := range dispatch {
		var res result

		if err != nil {
			res.err = err
		} else if value, found := results[key]; found {
			res.value = value
			// Cache successful result
			dl.mu.Lock()
			dl.cache[key] = value
			dl.mu.Unlock()
		} else {
			res.err = fmt.Errorf("key not found: %s", key)
		}

		// Send result to all waiting channels
		for _, ch := range channels {
			ch <- res
			close(ch)
		}
	}
}

// ClearCache clears the request-scoped cache
//
// Call this at the end of each GraphQL request to prevent memory leaks.
// DO NOT call during request processing.
func (dl *DataLoader) ClearCache() {
	dl.mu.Lock()
	defer dl.mu.Unlock()

	dl.cache = make(map[string]interface{})
}

// GetCacheSize returns the current cache size (for monitoring)
func (dl *DataLoader) GetCacheSize() int {
	dl.mu.Lock()
	defer dl.mu.Unlock()

	return len(dl.cache)
}

// Helper: deduplicateKeys removes duplicate keys while preserving order
func deduplicateKeys(keys []string) []string {
	seen := make(map[string]bool)
	result := make([]string, 0, len(keys))

	for _, key := range keys {
		if !seen[key] {
			seen[key] = true
			result = append(result, key)
		}
	}

	return result
}
```

### 2. Request Context (`context/request_context.go`)

```go
// Package context provides per-request context management for GraphQL queries
//
// PURPOSE:
// This file implements a request-scoped context that holds DataLoader instances
// for each bundle type. This ensures that loaders (and their caches) are isolated
// per-request and automatically cleaned up when the request completes.
//
// KEY FEATURES:
// - One DataLoader per bundle type per request
// - Automatic cleanup after request completes
// - Thread-safe loader creation and access
// - Middleware integration for automatic context injection
//
// REQUEST LIFECYCLE:
//
//   HTTP Request arrives
//          │
//          ▼
//   Create RequestContext (with loaders for all bundles)
//          │
//          ▼
//   Execute GraphQL query
//     ├── Relationship resolver uses GetLoader("books")
//     ├── Relationship resolver uses GetLoader("authors")
//     └── ... (all loaders batched and cached)
//          │
//          ▼
//   Request completes
//          │
//          ▼
//   Cleanup() called (clear all loader caches)
//
// DESIGN PRINCIPLES:
// - Single Responsibility: Only manages loader lifecycle per request
// - Dependency Inversion: Depends on LoaderFactory interface
// - Resource Management: Automatic cleanup prevents memory leaks

package context

import (
	"context"
	"sync"

	"syndrdb/src/internal/graphQL/dataloader"
	"syndrdb/src/internal/server"

	"go.uber.org/zap"
)

// contextKey is a private type for context keys to avoid collisions
type contextKey string

const requestContextKey contextKey = "syndrdb_request_context"

// RequestContext holds per-request state for GraphQL execution
//
// Each GraphQL request gets its own RequestContext with dedicated DataLoaders.
// This ensures that:
// - Batching only occurs within a single request
// - Caching is request-scoped (no cross-request pollution)
// - Memory is automatically freed when request completes
type RequestContext struct {
	loaders        map[string]*dataloader.DataLoader // bundleName → DataLoader
	serviceManager server.ServiceManager
	logger         *zap.SugaredLogger
	mu             sync.RWMutex
}

// NewRequestContext creates a new request context
//
// PARAMETERS:
//   - serviceManager: Access to bundle services for data loading
//   - logger: Structured logger
//
// RETURNS:
//   - *RequestContext: New context ready for use
//
// USAGE:
//   ctx := NewRequestContext(serviceManager, logger)
//   defer ctx.Cleanup() // Important: always cleanup!
//
//   // Inject into Go context
//   goCtx := context.WithValue(r.Context(), requestContextKey, ctx)
func NewRequestContext(serviceManager server.ServiceManager, logger *zap.SugaredLogger) *RequestContext {
	return &RequestContext{
		loaders:        make(map[string]*dataloader.DataLoader),
		serviceManager: serviceManager,
		logger:         logger,
	}
}

// GetLoader retrieves or creates a DataLoader for a bundle
//
// This method is called by relationship resolvers to get the appropriate loader
// for fetching related documents. Loaders are created lazily and cached for the
// duration of the request.
//
// THREAD SAFETY: Safe to call from multiple goroutines
//
// EXAMPLE:
//   // In relationship resolver
//   loader := requestCtx.GetLoader("books")
//   books, err := loader.LoadMany(ctx, []string{"book-1", "book-2"})
func (rc *RequestContext) GetLoader(bundleName string) *dataloader.DataLoader {
	// Fast path: read lock for existing loader
	rc.mu.RLock()
	loader, exists := rc.loaders[bundleName]
	rc.mu.RUnlock()

	if exists {
		return loader
	}

	// Slow path: write lock for creating new loader
	rc.mu.Lock()
	defer rc.mu.Unlock()

	// Double-check after acquiring write lock
	if loader, exists := rc.loaders[bundleName]; exists {
		return loader
	}

	// Create new loader for this bundle
	loader = rc.createLoaderForBundle(bundleName)
	rc.loaders[bundleName] = loader

	rc.logger.Debugf("[RequestContext] Created DataLoader for bundle: %s", bundleName)

	return loader
}

// createLoaderForBundle creates a DataLoader for a specific bundle
//
// The batch load function queries documents by ID using the bundle service.
func (rc *RequestContext) createLoaderForBundle(bundleName string) *dataloader.DataLoader {
	batchLoadFunc := func(ctx context.Context, ids []string) (map[string]interface{}, error) {
		// TODO: Implement BundleService.GetDocumentsByIDs method
		// For now, use GetDocumentsByFilter with WHERE id IN (...)

		rc.logger.Debugf("[DataLoader] Batch loading %d documents from bundle: %s", len(ids), bundleName)

		// Build WHERE IN clause
		// TODO: In production, use prepared statements or parameterized queries
		whereClause := buildWhereInClause("DocumentID", ids)

		// Query documents
		// NOTE: This requires GetDocumentsByFilter to exist
		// documents, err := rc.serviceManager.BundleService.GetDocumentsByFilter(bundleName, whereClause)

		// For now, return empty map (will be implemented when integrating)
		results := make(map[string]interface{})

		// TODO: Convert documents to map[string]interface{}
		// for _, doc := range documents {
		//     results[doc.DocumentID] = documentToMap(doc)
		// }

		return results, nil
	}

	return dataloader.NewDataLoader(batchLoadFunc, &dataloader.DataLoaderConfig{
		BatchWindow:  10 * time.Millisecond,
		MaxBatchSize: 1000,
		EnableCache:  true,
	})
}

// Cleanup clears all loader caches and releases resources
//
// IMPORTANT: Always call this when the request completes to prevent memory leaks.
// Best practice: defer ctx.Cleanup() immediately after creating context.
func (rc *RequestContext) Cleanup() {
	rc.mu.Lock()
	defer rc.mu.Unlock()

	for bundleName, loader := range rc.loaders {
		loader.ClearCache()
		rc.logger.Debugf("[RequestContext] Cleaned up DataLoader for bundle: %s", bundleName)
	}

	rc.loaders = make(map[string]*dataloader.DataLoader)
}

// GetCacheStats returns cache statistics for monitoring
func (rc *RequestContext) GetCacheStats() map[string]int {
	rc.mu.RLock()
	defer rc.mu.RUnlock()

	stats := make(map[string]int)
	for bundleName, loader := range rc.loaders {
		stats[bundleName] = loader.GetCacheSize()
	}

	return stats
}

// Helper functions

// FromContext extracts RequestContext from Go context
func FromContext(ctx context.Context) (*RequestContext, bool) {
	reqCtx, ok := ctx.Value(requestContextKey).(*RequestContext)
	return reqCtx, ok
}

// WithRequestContext adds RequestContext to Go context
func WithRequestContext(ctx context.Context, reqCtx *RequestContext) context.Context {
	return context.WithValue(ctx, requestContextKey, reqCtx)
}

// buildWhereInClause builds a WHERE field IN (...) clause
func buildWhereInClause(field string, values []string) string {
	// TODO: Use proper SQL escaping or parameterized queries
	// This is a simplified version for the plan

	if len(values) == 0 {
		return "1=0" // No results
	}

	quoted := make([]string, len(values))
	for i, v := range values {
		// TODO: Proper escaping
		quoted[i] = fmt.Sprintf("'%s'", v)
	}

	return fmt.Sprintf("%s IN (%s)", field, strings.Join(quoted, ", "))
}
```

### 3. Query Analyzer (`optimization/query_analyzer.go`)

```go
// Package optimization provides query analysis and optimization for deep relationships
//
// PURPOSE:
// This file analyzes GraphQL queries to determine their complexity and select
// the optimal execution strategy. It prevents expensive queries from overwhelming
// the system and automatically chooses between DataLoader batching and JOIN optimization.
//
// KEY FEATURES:
// - Query depth and breadth analysis
// - Complexity scoring
// - Automatic strategy selection
// - Query rejection for excessive complexity
//
// COMPLEXITY ANALYSIS:
//
//   Query: { authors { books { reviews { user { profile } } } } }
//
//   Depth: 5 levels (authors → books → reviews → user → profile)
//   Breadth: 1 field per level (except root)
//   Estimated queries (no batching): 1 + N + N*M + N*M*K + N*M*K*L
//                                    = 1 + 100 + 100*10 + 100*10*20 + 100*10*20*5
//                                    = 1 + 100 + 1000 + 20000 + 100000
//                                    = 121,101 queries ❌ REJECT
//
//   With DataLoader batching: 5 queries (one per level) ✅ ALLOW
//
// DESIGN PRINCIPLES:
// - Fail Fast: Reject expensive queries before execution
// - Transparent: Log analysis results for debugging
// - Configurable: Limits can be adjusted per deployment

package optimization

import (
	"fmt"

	"github.com/vektah/gqlparser/v2/ast"
	"go.uber.org/zap"
)

// QueryComplexityLimits defines maximum allowed query complexity
type QueryComplexityLimits struct {
	MaxDepth          int // Maximum relationship nesting (default: 5)
	MaxBreadth        int // Maximum fields per level (default: 50)
	MaxTotalFields    int // Maximum total fields in query (default: 500)
	MaxEstimatedCost  int // Maximum estimated query cost (default: 1000)
	RejectOnExceeded  bool // Reject query if limits exceeded (default: true)
}

// DefaultComplexityLimits returns sensible default limits
func DefaultComplexityLimits() QueryComplexityLimits {
	return QueryComplexityLimits{
		MaxDepth:         5,
		MaxBreadth:       50,
		MaxTotalFields:   500,
		MaxEstimatedCost: 1000,
		RejectOnExceeded: true,
	}
}

// QueryAnalysis contains the results of query analysis
type QueryAnalysis struct {
	Depth              int                    // Maximum nesting depth
	Breadth            int                    // Maximum fields at any level
	TotalFields        int                    // Total field count
	EstimatedQueryCost int                    // Estimated number of queries
	RelationshipChain  []string               // Path of relationships
	ExceedsLimits      bool                   // True if any limit exceeded
	RecommendedStrategy ExecutionStrategy     // Recommended execution approach
	Metadata           map[string]interface{} // Additional analysis data
}

// ExecutionStrategy defines how to execute the query
type ExecutionStrategy int

const (
	// StrategyDataLoader uses DataLoader batching (default)
	StrategyDataLoader ExecutionStrategy = iota

	// StrategyJoinOptimization uses JOIN-based single query
	StrategyJoinOptimization

	// StrategyReject query is too complex to execute
	StrategyReject
)

func (s ExecutionStrategy) String() string {
	switch s {
	case StrategyDataLoader:
		return "DataLoader"
	case StrategyJoinOptimization:
		return "JOIN Optimization"
	case StrategyReject:
		return "Reject (too complex)"
	default:
		return "Unknown"
	}
}

// QueryAnalyzer analyzes GraphQL queries for complexity
type QueryAnalyzer struct {
	limits QueryComplexityLimits
	logger *zap.SugaredLogger
}

// NewQueryAnalyzer creates a new query analyzer
func NewQueryAnalyzer(limits QueryComplexityLimits, logger *zap.SugaredLogger) *QueryAnalyzer {
	return &QueryAnalyzer{
		limits: limits,
		logger: logger,
	}
}

// AnalyzeQuery analyzes a GraphQL query for complexity
//
// ALGORITHM:
//  1. Traverse selection set depth-first
//  2. Calculate depth (max nesting level)
//  3. Calculate breadth (max fields at any level)
//  4. Estimate query cost (depth * avg_breadth * avg_cardinality)
//  5. Select optimal strategy based on analysis
//  6. Reject if exceeds limits
//
// RETURNS:
//   - *QueryAnalysis: Complete analysis results
//   - error: If query should be rejected
func (qa *QueryAnalyzer) AnalyzeQuery(field *ast.Field) (*QueryAnalysis, error) {
	analysis := &QueryAnalysis{
		RelationshipChain: []string{},
		Metadata:          make(map[string]interface{}),
	}

	// Traverse query and collect metrics
	qa.analyzeSelectionSet(field.SelectionSet, 1, analysis)

	// Estimate query cost (simplified heuristic)
	analysis.EstimatedQueryCost = qa.estimateQueryCost(analysis)

	// Select execution strategy
	analysis.RecommendedStrategy = qa.selectStrategy(analysis)

	// Check if limits exceeded
	analysis.ExceedsLimits = qa.checkLimits(analysis)

	// Log analysis results
	qa.logger.Infof("[QueryAnalyzer] Query analysis: depth=%d, breadth=%d, cost=%d, strategy=%s",
		analysis.Depth, analysis.Breadth, analysis.EstimatedQueryCost, analysis.RecommendedStrategy)

	// Reject if necessary
	if analysis.ExceedsLimits && qa.limits.RejectOnExceeded {
		return analysis, fmt.Errorf("query exceeds complexity limits: depth=%d (max %d), breadth=%d (max %d), cost=%d (max %d)",
			analysis.Depth, qa.limits.MaxDepth,
			analysis.Breadth, qa.limits.MaxBreadth,
			analysis.EstimatedQueryCost, qa.limits.MaxEstimatedCost)
	}

	return analysis, nil
}

// analyzeSelectionSet recursively analyzes a selection set
func (qa *QueryAnalyzer) analyzeSelectionSet(selectionSet ast.SelectionSet, depth int, analysis *QueryAnalysis) {
	if selectionSet == nil {
		return
	}

	// Update depth
	if depth > analysis.Depth {
		analysis.Depth = depth
	}

	// Count fields at this level
	fieldCount := len(selectionSet)
	if fieldCount > analysis.Breadth {
		analysis.Breadth = fieldCount
	}

	analysis.TotalFields += fieldCount

	// Recurse into nested selections
	for _, selection := range selectionSet {
		if field, ok := selection.(*ast.Field); ok {
			// Add to relationship chain
			if field.SelectionSet != nil {
				analysis.RelationshipChain = append(analysis.RelationshipChain, field.Name)
			}

			// Recurse
			qa.analyzeSelectionSet(field.SelectionSet, depth+1, analysis)
		}
	}
}

// estimateQueryCost estimates the number of database queries
func (qa *QueryAnalyzer) estimateQueryCost(analysis *QueryAnalysis) int {
	// Simplified cost model:
	// - Without batching: exponential growth per level
	// - With DataLoader: linear growth (one query per level)
	// - With JOIN: constant (1 query)

	// Assume DataLoader is used
	cost := analysis.Depth

	// Add cost for breadth (more fields = more complex queries)
	cost += analysis.Breadth / 10

	return cost
}

// selectStrategy selects the optimal execution strategy
func (qa *QueryAnalyzer) selectStrategy(analysis *QueryAnalysis) ExecutionStrategy {
	// Strategy selection logic:
	//
	// 1. If depth <= 2 and breadth <= 50: DataLoader (simple, fast)
	// 2. If depth <= 4 and breadth <= 20: JOIN optimization (single query)
	// 3. If depth > 5 or breadth > 50: Reject (too complex)

	if analysis.Depth > qa.limits.MaxDepth {
		return StrategyReject
	}

	if analysis.Depth <= 2 && analysis.Breadth <= 50 {
		return StrategyDataLoader
	}

	if analysis.Depth <= 4 && analysis.Breadth <= 20 {
		return StrategyJoinOptimization
	}

	if analysis.Depth > 4 || analysis.Breadth > 50 {
		return StrategyReject
	}

	// Default to DataLoader
	return StrategyDataLoader
}

// checkLimits checks if query exceeds any limits
func (qa *QueryAnalyzer) checkLimits(analysis *QueryAnalysis) bool {
	if analysis.Depth > qa.limits.MaxDepth {
		qa.logger.Warnf("[QueryAnalyzer] Query exceeds max depth: %d > %d", analysis.Depth, qa.limits.MaxDepth)
		return true
	}

	if analysis.Breadth > qa.limits.MaxBreadth {
		qa.logger.Warnf("[QueryAnalyzer] Query exceeds max breadth: %d > %d", analysis.Breadth, qa.limits.MaxBreadth)
		return true
	}

	if analysis.TotalFields > qa.limits.MaxTotalFields {
		qa.logger.Warnf("[QueryAnalyzer] Query exceeds max total fields: %d > %d", analysis.TotalFields, qa.limits.MaxTotalFields)
		return true
	}

	if analysis.EstimatedQueryCost > qa.limits.MaxEstimatedCost {
		qa.logger.Warnf("[QueryAnalyzer] Query exceeds max estimated cost: %d > %d", analysis.EstimatedQueryCost, qa.limits.MaxEstimatedCost)
		return true
	}

	return false
}
```

---

## Integration Points

### 1. Update `relationship_resolver.go`

**Changes Required** (lines 180-250):

```go
// OLD (current implementation):
func (rr *RelationshipResolver) resolveForwardRelationship(...) (interface{}, error) {
    whereClause := fmt.Sprintf("%s = '%v'", relationship.DestinationField, sourceValue)
    relatedDocs, err := rr.serviceManager.BundleService.GetDocumentsByFilter(destBundle, whereClause)
    // ❌ N+1 problem: one query per parent document
}

// NEW (with DataLoader):
func (rr *RelationshipResolver) resolveForwardRelationship(...) (interface{}, error) {
    // Get DataLoader from request context
    reqCtx, ok := context.FromContext(ctx)
    if !ok {
        // Fallback to direct query if no context (backward compatible)
        return rr.resolveForwardRelationshipDirect(...)
    }

    loader := reqCtx.GetLoader(relationship.DestinationBundle)

    // Load related documents using DataLoader
    // ✅ Batched: multiple calls to Load() within 10ms batched into one query
    relatedDocs, err := loader.LoadMany(ctx, []string{sourceValue})
    // ...
}
```

### 2. Update `handler.go`

**Changes Required** (lines 300-450):

```go
// OLD:
func (h *GraphQLHandler) HandleGraphQLQuery(dbName, query string) *GraphQLResult {
    // Parse and execute query directly
}

// NEW:
func (h *GraphQLHandler) HandleGraphQLQuery(dbName, query string) *GraphQLResult {
    // 1. Create request context with DataLoaders
    reqCtx := context.NewRequestContext(h.serviceManager, h.logger)
    defer reqCtx.Cleanup()

    // 2. Add to Go context
    ctx := context.WithRequestContext(context.Background(), reqCtx)

    // 3. Analyze query complexity
    analyzer := optimization.NewQueryAnalyzer(
        optimization.DefaultComplexityLimits(),
        h.logger,
    )

    analysis, err := analyzer.AnalyzeQuery(field)
    if err != nil {
        // Query rejected due to complexity
        return &GraphQLResult{
            Errors: []GraphQLError{{Message: err.Error()}},
        }
    }

    // 4. Execute query with selected strategy
    switch analysis.RecommendedStrategy {
    case optimization.StrategyDataLoader:
        return h.executeWithDataLoader(ctx, field, variables)
    case optimization.StrategyJoinOptimization:
        return h.executeWithJOINOptimization(ctx, field, variables)
    case optimization.StrategyReject:
        return &GraphQLResult{
            Errors: []GraphQLError{{Message: "Query too complex"}},
        }
    }
}
```

---

## Testing Strategy

### Unit Tests

**1. DataLoader Tests** (`dataloader_test.go`):
```go
func TestDataLoader_BasicLoad(t *testing.T)
func TestDataLoader_BatchingMultipleLoads(t *testing.T)
func TestDataLoader_Caching(t *testing.T)
func TestDataLoader_Prime(t *testing.T)
func TestDataLoader_LoadMany(t *testing.T)
func TestDataLoader_ConcurrentAccess(t *testing.T)
func TestDataLoader_BatchWindowTiming(t *testing.T)
func TestDataLoader_MaxBatchSize(t *testing.T)
func TestDataLoader_ErrorHandling(t *testing.T)
```

**2. RequestContext Tests** (`context_test.go`):
```go
func TestRequestContext_CreateAndGetLoader(t *testing.T)
func TestRequestContext_LoaderIsolation(t *testing.T)
func TestRequestContext_Cleanup(t *testing.T)
func TestRequestContext_ConcurrentAccess(t *testing.T)
func TestRequestContext_ContextExtraction(t *testing.T)
```

**3. QueryAnalyzer Tests** (`optimization_test.go`):
```go
func TestQueryAnalyzer_SimpleQuery(t *testing.T)
func TestQueryAnalyzer_DeepQuery(t *testing.T)
func TestQueryAnalyzer_BroadQuery(t *testing.T)
func TestQueryAnalyzer_ExceedsDepthLimit(t *testing.T)
func TestQueryAnalyzer_StrategySelection(t *testing.T)
```

### Integration Tests

**1. N+1 Problem Verification** (`dataloader_integration_test.go`):
```go
func TestIntegration_N plus1_Resolution(t *testing.T) {
    // Query 100 authors with books
    // Verify: Only 2 queries executed (authors + batched books)
    // Assert: Query count = 2 (not 101)
}
```

**2. Deep Relationship Tests** (`deep_query_test.go`):
```go
func TestIntegration_DeepRelationships_3Levels(t *testing.T) {
    // Query: authors { books { reviews } }
    // Verify: 3 batched queries (one per level)
}

func TestIntegration_DeepRelationships_5Levels(t *testing.T) {
    // Query: authors { books { reviews { user { profile } } } }
    // Verify: 5 batched queries
}
```

### Performance Benchmarks

**1. Before vs After Comparison**:
```go
func BenchmarkRelationshipResolution_Without_DataLoader(b *testing.B) {
    // Baseline: current implementation
}

func BenchmarkRelationshipResolution_With_DataLoader(b *testing.B) {
    // New implementation with batching
}
```

**Expected Results**:
```
BenchmarkRelationshipResolution_Without_DataLoader-8
    100 authors × 10 books each = 101 queries
    Time: 2.02 seconds (20ms per query)

BenchmarkRelationshipResolution_With_DataLoader-8
    100 authors × 10 books each = 2 queries (batched)
    Time: 0.04 seconds (20ms per query × 2)
    
    Improvement: 50x faster ✅
```

---

## Performance Benchmarks

### Target Metrics

| Scenario | Without DataLoader | With DataLoader | Improvement |
|----------|-------------------|-----------------|-------------|
| **100 authors + books** | 101 queries, ~2.0s | 2 queries, ~0.04s | **50x faster** |
| **10 authors + 50 books + 200 reviews** | 261 queries, ~5.2s | 3 queries, ~0.06s | **86x faster** |
| **Deep nesting (5 levels)** | 10,000+ queries, timeout | 5 queries, ~0.1s | **100x+ faster** |
| **Memory usage** | Baseline | +10% (caching) | Acceptable |

### Query Cost Analysis

**Query**: 100 authors, each with 10 books
```
Without DataLoader:
├─ SELECT FROM authors LIMIT 100          [20ms]
└─ For each author (100 iterations):
   └─ SELECT FROM books WHERE authorId = ? [20ms]
   
Total: 101 queries × 20ms = 2,020ms
```

**With DataLoader**:
```
└─ SELECT FROM authors LIMIT 100                           [20ms]
└─ (DataLoader batches next 100 requests into one)
   └─ SELECT FROM books WHERE authorId IN (100 ids...)     [20ms]
   
Total: 2 queries × 20ms = 40ms  ✅ 50x improvement
```

---

## Migration Path

### Phase 1: Add DataLoader (No Breaking Changes)

**Week 1**: Core implementation
- Create `dataloader/` package
- Create `context/` package
- Unit tests (95%+ coverage)

**Week 2**: Integration
- Update `relationship_resolver.go` (backward compatible)
- Update `handler.go` (create context)
- Integration tests

### Phase 2: Enable by Default (Opt-out Available)

**Week 3**: Production rollout
- Enable DataLoader by default
- Add feature flag: `USE_DATALOADER=true`
- Monitor performance metrics
- Rollback capability if issues arise

### Phase 3: Query Optimization

**Week 4-5**: Advanced features
- Add QueryAnalyzer
- Implement JOIN optimization
- Add complexity limits

### Phase 4: Deprecate Old Path

**Week 6**: Cleanup
- Remove feature flag
- Remove backward compatibility code
- Update documentation

---

## Risk Analysis

### Risk 1: Breaking Changes
**Probability**: Low  
**Impact**: High  
**Mitigation**:
- Maintain backward compatibility
- Feature flag for gradual rollout
- Comprehensive testing

### Risk 2: Performance Regression
**Probability**: Low  
**Impact**: High  
**Mitigation**:
- Benchmark before/after
- Load testing in staging
- Rollback plan

### Risk 3: Memory Leaks
**Probability**: Medium  
**Impact**: High  
**Mitigation**:
- Automatic cleanup on request completion
- Memory profiling tests
- Monitoring cache sizes

### Risk 4: Complex Queries Timeout
**Probability**: Medium  
**Impact**: Medium  
**Mitigation**:
- Query complexity limits
- Timeout configurations
- Clear error messages

### Risk 5: DataLoader Bugs
**Probability**: Low  
**Impact**: High  
**Mitigation**:
- Use battle-tested algorithm
- Extensive unit tests
- Integration tests
- Reference implementation study

---

## Open Questions

1. **BundleService API**: Does `GetDocumentsByIDs(bundle, []string)` exist?
   - If not, need to implement or use `GetDocumentsByFilter` with WHERE IN

2. **Context Propagation**: How to pass Go context through relationship resolver?
   - Need to update function signatures to accept `context.Context`

3. **JOIN Implementation**: Does SyndrDB support JOIN queries?
   - If yes, can leverage existing JOIN planner
   - If no, JOIN optimization requires new implementation

4. **Transaction Isolation**: Should DataLoader respect transaction boundaries?
   - Current plan: No, operates at request level only

5. **Caching Strategy**: Should cache persist across requests?
   - Current plan: No, request-scoped only
   - Future: Add optional Redis cache for cross-request caching

---

## Success Criteria

✅ **Phase 1 Complete When**:
1. DataLoader implementation passes all unit tests
2. Request context properly creates and manages loaders
3. Zero breaking changes to existing queries
4. Documentation complete

✅ **Phase 2 Complete When**:
1. Relationship resolver uses DataLoader
2. Integration tests show 10x+ performance improvement
3. No regressions in existing tests
4. Production-ready with feature flag

✅ **Phase 3 Complete When**:
1. Query analyzer correctly identifies query complexity
2. Automatic strategy selection works
3. JOIN optimization shows additional 2-5x improvement
4. Complexity limits prevent expensive queries

✅ **Phase 4 Complete When**:
1. All features enabled by default
2. Performance metrics show sustained improvement
3. Zero production incidents
4. Documentation updated

---

## Next Steps

### Immediate Actions (Post-Approval):
1. Create feature branch: `feature/dataloader-n-plus-1`
2. Set up package structure: `dataloader/`, `context/`, `optimization/`
3. Implement core DataLoader (Week 1)
4. Write comprehensive unit tests
5. Create integration test suite
6. Benchmark performance improvements

### Review Points:
- [ ] Architecture approved
- [ ] File structure approved
- [ ] Integration strategy approved
- [ ] Testing strategy approved
- [ ] Migration path approved
- [ ] Risk mitigation approved

---

**Document Version**: 1.0  
**Last Updated**: January 2025  
**Author**: SyndrDB Development Team  
**Status**: ⏳ Awaiting Approval
