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
//
// USAGE EXAMPLE:
//
//   // Create loader for a bundle
//   loader := NewDataLoader(func(ctx context.Context, keys []string) (map[string]interface{}, error) {
//       return bundleService.GetDocumentsByIDs(bundleName, keys)
//   }, nil)
//
//   // Load documents (automatically batched)
//   doc1, err := loader.Load(ctx, "doc-id-1")
//   doc2, err := loader.Load(ctx, "doc-id-2")  // Batched with above if within 10ms
//
// AUTHOR: SyndrDB Development Team
// CREATED: November 2025
// PHASE: 10 (DataLoader & Deep Relationship Optimization)

package dataloader

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// BatchLoadFunc is a function that loads documents by their IDs
// Returns a map of ID → document for all successfully loaded items
// Missing IDs should be omitted from the result map (not returned as nil)
type BatchLoadFunc func(ctx context.Context, keys []string) (map[string]interface{}, error)

// DataLoader batches and caches Load requests within a single request context
//
// THREAD SAFETY: All public methods are thread-safe and can be called
// concurrently from multiple goroutines (e.g., GraphQL field resolvers).
type DataLoader struct {
	// Configuration
	batchLoadFunc BatchLoadFunc
	batchWindow   time.Duration // How long to wait before dispatching batch
	maxBatchSize  int           // Maximum items per batch (prevent huge queries)

	// State
	mu       sync.Mutex
	cache    map[string]interface{}   // Request-scoped cache
	queue    []string                 // Pending load requests (may contain duplicates)
	dispatch map[string][]chan result // key → channels waiting for result
	timer    *time.Timer              // Batch window timer

	// Statistics (for monitoring and debugging)
	stats DataLoaderStats
}

// DataLoaderStats tracks performance metrics
type DataLoaderStats struct {
	TotalLoads       int64         // Total number of Load() calls
	CacheHits        int64         // Number of cache hits
	CacheMisses      int64         // Number of cache misses
	BatchCount       int64         // Number of batches executed
	TotalKeysLoaded  int64         // Total keys loaded across all batches
	AverageBatchSize float64       // Average keys per batch
	TotalBatchTime   time.Duration // Total time spent in batch operations
}

// result is the internal structure for communicating load results
type result struct {
	value interface{}
	err   error
}

// DataLoaderConfig configures DataLoader behavior
type DataLoaderConfig struct {
	BatchWindow  time.Duration // Time to wait for batching (default: 10ms)
	MaxBatchSize int           // Maximum items per batch (default: 1000)
	EnableCache  bool          // Enable per-request caching (default: true)
}

// NewDataLoader creates a new DataLoader instance
//
// PARAMETERS:
//   - batchLoadFunc: Function that loads multiple items by ID in a single operation
//   - config: Configuration options (nil uses defaults)
//
// RETURNS:
//   - *DataLoader: Configured loader ready for use
//
// DEFAULT CONFIG:
//   - BatchWindow: 10ms (good balance between batching and latency)
//   - MaxBatchSize: 1000 (prevents excessively large queries)
//   - EnableCache: true (eliminates duplicate loads within request)
//
// EXAMPLE:
//
//	loader := NewDataLoader(func(ctx context.Context, ids []string) (map[string]interface{}, error) {
//	    // Load books WHERE id IN (ids...)
//	    return sm.BundleService.GetDocumentsByIDs("books", ids)
//	}, &DataLoaderConfig{
//	    BatchWindow: 10 * time.Millisecond,
//	    MaxBatchSize: 1000,
//	})
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
//  5. Wait for result on channel (blocks until batch completes)
//  6. Return value when batch completes
//
// THREAD SAFETY: Safe to call from multiple goroutines
//
// PERFORMANCE:
//   - Cache hit: O(1) immediate return
//   - Cache miss: O(1) + batch wait time (max: batchWindow)
//
// EXAMPLE:
//
//	// In relationship resolver, instead of:
//	docs, err := sm.Query("books WHERE authorId = ?", authorID)
//
//	// Use DataLoader:
//	books, err := loader.Load(ctx, authorID)
func (dl *DataLoader) Load(ctx context.Context, key string) (interface{}, error) {
	// Track total loads
	dl.mu.Lock()
	dl.stats.TotalLoads++

	// Fast path: check cache first
	if cached, found := dl.cache[key]; found {
		dl.stats.CacheHits++
		dl.mu.Unlock()
		return cached, nil
	}
	dl.stats.CacheMisses++

	// Create result channel for this request
	resultChan := make(chan result, 1)

	// Add to queue and register for notification
	dl.queue = append(dl.queue, key)
	dl.dispatch[key] = append(dl.dispatch[key], resultChan)

	// Start timer if not running
	if dl.timer == nil {
		dl.timer = time.AfterFunc(dl.batchWindow, func() {
			dl.dispatchBatch(context.Background())
		})
	}

	// Check if batch is full - dispatch immediately if so
	if len(dl.queue) >= dl.maxBatchSize {
		// Stop timer and dispatch now
		if dl.timer != nil {
			dl.timer.Stop()
			dl.timer = nil
		}
		// Unlock before dispatching (dispatchBatch will re-acquire lock)
		dl.mu.Unlock()
		go dl.dispatchBatch(context.Background())
		dl.mu.Lock()
	}

	dl.mu.Unlock()

	// Wait for result (blocks until batch completes)
	select {
	case res := <-resultChan:
		return res.value, res.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// LoadMany loads multiple items by keys
//
// This is more efficient than calling Load() multiple times in sequence because
// it triggers an immediate batch dispatch with all keys.
//
// THREAD SAFETY: Safe to call from multiple goroutines
//
// RETURNS:
//   - map[string]interface{}: Map of key → value for all successfully loaded items
//   - error: First error encountered, if any
//
// EXAMPLE:
//
//	// Load multiple related documents at once
//	authorIDs := []string{"author-1", "author-2", "author-3"}
//	authors, err := loader.LoadMany(ctx, authorIDs)
func (dl *DataLoader) LoadMany(ctx context.Context, keys []string) (map[string]interface{}, error) {
	results := make(map[string]interface{})
	var firstError error

	// Use a WaitGroup to load all keys concurrently
	var wg sync.WaitGroup
	var mu sync.Mutex

	for _, key := range keys {
		wg.Add(1)
		go func(k string) {
			defer wg.Done()

			value, err := dl.Load(ctx, k)

			mu.Lock()
			if err != nil && firstError == nil {
				firstError = err
			}
			if value != nil {
				results[k] = value
			}
			mu.Unlock()
		}(key)
	}

	wg.Wait()

	return results, firstError
}

// Prime seeds the cache with a known value
//
// Use this to pre-populate the cache with data you already have, avoiding
// unnecessary database queries.
//
// THREAD SAFETY: Safe to call from multiple goroutines
//
// EXAMPLE:
//
//	// After loading parent documents, prime the loader with their IDs
//	for _, author := range authors {
//	    loader.Prime(author.ID, author)
//	}
//
//	// Later Load() calls for these IDs will return cached value
//	author, _ := loader.Load(ctx, author.ID)  // Returns immediately from cache
func (dl *DataLoader) Prime(key string, value interface{}) {
	dl.mu.Lock()
	defer dl.mu.Unlock()

	dl.cache[key] = value
}

// dispatchBatch executes the pending batch
//
// ALGORITHM:
//  1. Collect all pending keys (deduplicate)
//  2. Call batchLoadFunc with unique keys
//  3. Distribute results to all waiting channels
//  4. Cache results for future requests
//  5. Clear queue for next batch
//
// THREAD SAFETY: Called from timer goroutine, uses mutex internally
// This method should not be called directly - it's called automatically
// by the batch timer or when maxBatchSize is reached.
func (dl *DataLoader) dispatchBatch(ctx context.Context) {
	startTime := time.Now()

	dl.mu.Lock()

	// Copy queue and clear for next batch
	keys := make([]string, len(dl.queue))
	copy(keys, dl.queue)
	dl.queue = dl.queue[:0]

	// Copy dispatch map and clear
	dispatch := dl.dispatch
	dl.dispatch = make(map[string][]chan result)

	// Clear timer
	dl.timer = nil

	// Update batch statistics
	dl.stats.BatchCount++
	dl.stats.TotalKeysLoaded += int64(len(keys))
	if dl.stats.BatchCount > 0 {
		dl.stats.AverageBatchSize = float64(dl.stats.TotalKeysLoaded) / float64(dl.stats.BatchCount)
	}

	dl.mu.Unlock()

	// Nothing to do if queue was empty
	if len(keys) == 0 {
		return
	}

	// Deduplicate keys for the batch query
	uniqueKeys := deduplicateKeys(keys)

	// Execute batch load
	results, err := dl.batchLoadFunc(ctx, uniqueKeys)

	// Track batch execution time
	batchDuration := time.Since(startTime)
	dl.mu.Lock()
	dl.stats.TotalBatchTime += batchDuration
	dl.mu.Unlock()

	// Distribute results to all waiting channels
	for key, channels := range dispatch {
		var res result

		if err != nil {
			// If batch load failed, all requests fail
			res.err = err
		} else if value, found := results[key]; found {
			// Success: key was found in results
			res.value = value

			// Cache successful result
			dl.mu.Lock()
			dl.cache[key] = value
			dl.mu.Unlock()
		} else {
			// Key not found in results (document doesn't exist)
			res.err = fmt.Errorf("key not found: %s", key)
		}

		// Send result to all waiting channels for this key
		for _, ch := range channels {
			ch <- res
			close(ch)
		}
	}
}

// ClearCache clears the request-scoped cache
//
// Call this at the end of each GraphQL request to prevent memory leaks.
// DO NOT call during request processing - it will break in-flight requests.
//
// THREAD SAFETY: Safe to call from any goroutine
//
// EXAMPLE:
//
//	defer loader.ClearCache()  // Cleanup when request completes
func (dl *DataLoader) ClearCache() {
	dl.mu.Lock()
	defer dl.mu.Unlock()

	dl.cache = make(map[string]interface{})
}

// GetCacheSize returns the current cache size (for monitoring/debugging)
//
// Useful for:
// - Monitoring memory usage
// - Debugging cache behavior
// - Performance analysis
func (dl *DataLoader) GetCacheSize() int {
	dl.mu.Lock()
	defer dl.mu.Unlock()

	return len(dl.cache)
}

// GetQueueSize returns the current queue size (for monitoring/debugging)
//
// Useful for understanding batching behavior and tuning batchWindow.
func (dl *DataLoader) GetQueueSize() int {
	dl.mu.Lock()
	defer dl.mu.Unlock()

	return len(dl.queue)
}

// GetStats returns a copy of current DataLoader statistics
//
// Use this for:
// - Performance monitoring
// - Cache hit rate analysis
// - Query optimization decisions
//
// EXAMPLE:
//
//	stats := loader.GetStats()
//	hitRate := float64(stats.CacheHits) / float64(stats.TotalLoads)
//	fmt.Printf("Cache hit rate: %.2f%%\n", hitRate * 100)
func (dl *DataLoader) GetStats() DataLoaderStats {
	dl.mu.Lock()
	defer dl.mu.Unlock()

	// Return a copy to prevent race conditions
	return dl.stats
}

// ResetStats resets all statistics counters
//
// Useful for:
// - Starting fresh measurements
// - Per-query statistics tracking
func (dl *DataLoader) ResetStats() {
	dl.mu.Lock()
	defer dl.mu.Unlock()

	dl.stats = DataLoaderStats{}
}

// Helper: deduplicateKeys removes duplicate keys while preserving first occurrence order
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
