/*
BTREE PAGE MANAGER SYSTEM

This file implements the page caching and memory management system for BTree indexes in SyndrDB.
It provides an LRU (Least Recently Used) page cache with efficient memory management, following
the buffer pool patterns used in PostgreSQL, MySQL, and SQL Server database systems.

PAGE CACHING OVERVIEW:
The page manager implements a sophisticated caching system that:
- Maintains frequently accessed pages in memory for fast retrieval
- Uses LRU eviction policy to manage memory usage efficiently
- Tracks dirty pages that need to be written back to disk
- Provides thread-safe operations for concurrent access
- Implements efficient cache hit/miss statistics for performance monitoring

LRU CACHE ALGORITHM:
The LRU cache uses a combination of:
- Doubly-linked list for O(1) insertion/deletion at ends
- Hash map for O(1) lookup of cache entries
- Dirty page tracking for write-back optimization
- Cache statistics for performance analysis and tuning

MEMORY MANAGEMENT:
The page manager provides:
- Configurable cache size limits to control memory usage
- Automatic eviction of least recently used pages when cache is full
- Dirty page write-back coordination with the file manager
- Memory usage tracking and reporting for system monitoring

PERFORMANCE OPTIMIZATION:
Key optimizations include:
- Minimal memory allocations through object reuse
- Efficient data structures for fast cache operations
- Batch operations for improved I/O performance
- Statistics collection for cache tuning and optimization

THREAD SAFETY:
All operations are thread-safe through:
- Read-write mutexes for concurrent access control
- Atomic operations for statistics updates
- Proper locking order to prevent deadlocks
- Safe resource cleanup and management

This implementation follows the Single Responsibility Principle by focusing
exclusively on page caching and memory management while delegating file I/O
operations to the BTreeFileManager component.
*/

package btreeindexV2

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
)

// BTreePageManager manages page caching with LRU eviction policy
// This structure provides efficient memory management for BTree index pages
// with comprehensive statistics tracking and thread-safe operations
type BTreePageManager struct {
	cache       map[uint32]*cacheEntry // Hash map for O(1) page lookup
	lruHead     *cacheEntry            // Head of LRU doubly-linked list
	lruTail     *cacheEntry            // Tail of LRU doubly-linked list
	maxSize     int                    // Maximum number of pages to cache
	currentSize int                    // Current number of cached pages
	pageSize    uint32                 // Size of each page in bytes
	mutex       sync.RWMutex           // Thread safety for cache operations
	logger      *zap.SugaredLogger     // Logger for debug and error messages

	// Writer function for flushing dirty pages during eviction
	// TODO: I could enhance this to support batched writes for better performance
	pageWriter func(uint32, interface{}) error

	// Statistics (using atomic operations for thread safety)
	stats cacheStatistics

	// Dirty page tracking and auto-flush infrastructure (PostgreSQL-style in-memory caching)
	dirtyPageCount   int32         // Current number of dirty pages (atomic access)
	flushTriggerChan chan struct{} // Buffered channel (size 1) for signaling background flush worker
	// TODO: Make dirty page threshold configurable via settings (currently hardcoded to 80%)
	// Automatic flush triggers when dirtyPageCount/cacheSize > 0.8 to prevent memory pressure
}

// TODO: Add GetDirtyPageCount() method for monitoring and metrics integration
// func (pm *BTreePageManager) GetDirtyPageCount() int {
//     pm.mutex.RLock()
//     defer pm.mutex.RUnlock()
//     return pm.dirtyPageCount
// }

func (pm *BTreePageManager) ClearCache() {
	pm.mutex.Lock()
	defer pm.mutex.Unlock()

	pm.logger.Debugf("Clearing all pages from cache")

	// Clear the cache map
	pm.cache = make(map[uint32]*cacheEntry, pm.maxSize)

	// Reset LRU list
	pm.lruHead.next = pm.lruTail
	pm.lruTail.prev = pm.lruHead

	// Reset counters
	pm.currentSize = 0
	atomic.StoreUint64(&pm.stats.memoryUsage, 0)

	pm.logger.Debugf("Cleared all pages from cache")
}

// cacheEntry represents a single entry in the LRU cache
// Each entry contains the page data and maintains LRU list pointers
type cacheEntry struct {
	pageNum     uint32      // Page number identifier
	pageData    interface{} // The actual page data (BTreeNode or BTreeMetadata)
	isDirty     bool        // Whether the page has been modified
	lastAccess  time.Time   // Last access timestamp for statistics
	accessCount uint64      // Number of times this page has been accessed
	pinCount    int         // Number of active pins preventing eviction (PostgreSQL-style)

	// LRU doubly-linked list pointers
	prev *cacheEntry // Previous entry in LRU list
	next *cacheEntry // Next entry in LRU list
}

// cacheStatistics tracks comprehensive cache performance metrics
// These statistics are used for monitoring and tuning cache performance
type cacheStatistics struct {
	hits        uint64 // Total number of cache hits
	misses      uint64 // Total number of cache misses
	evictions   uint64 // Total number of page evictions
	flushes     uint64 // Total number of flush operations
	dirtyWrites uint64 // Total number of dirty page writes
	memoryUsage uint64 // Current memory usage in bytes
}

// CacheStats represents cache performance statistics for external reporting
// This structure provides a snapshot of cache performance metrics
type CacheStats struct {
	HitRate     float64 // Cache hit rate percentage (0.0-1.0)
	Hits        uint64  // Total cache hits
	Misses      uint64  // Total cache misses
	Evictions   uint64  // Total evictions
	CurrentSize int     // Current number of cached pages
	MaxSize     int     // Maximum cache size
	MemoryUsage uint64  // Current memory usage in bytes
	DirtyPages  int     // Number of dirty pages
}

// NewBTreePageManager creates a new page manager with LRU caching
// Parameters:
//   - pageSize: Size of each page in bytes
//   - cacheSize: Maximum number of pages to cache in memory
//   - logger: Logger for debug and error messages
//
// Returns:
//   - *BTreePageManager: The created page manager instance
//   - error: Any error that occurred during creation
func NewBTreePageManager(pageSize uint32, cacheSize int, logger *zap.SugaredLogger) (*BTreePageManager, error) {
	if pageSize == 0 {
		return nil, fmt.Errorf("page size must be greater than 0")
	}

	if cacheSize <= 0 {
		return nil, fmt.Errorf("cache size must be greater than 0")
	}

	if logger == nil {
		return nil, fmt.Errorf("logger cannot be nil")
	}

	logger.Debugf("Creating BTree page manager with cache size %d and page size %d", cacheSize, pageSize)

	// Create sentinel nodes for the LRU list
	head := &cacheEntry{}
	tail := &cacheEntry{}
	head.next = tail
	tail.prev = head

	pm := &BTreePageManager{
		cache:            make(map[uint32]*cacheEntry, cacheSize),
		lruHead:          head,
		lruTail:          tail,
		maxSize:          cacheSize,
		currentSize:      0,
		pageSize:         pageSize,
		mutex:            sync.RWMutex{},
		logger:           logger,
		stats:            cacheStatistics{},
		dirtyPageCount:   0,
		flushTriggerChan: make(chan struct{}, 1), // Buffered size 1 for non-blocking sends
	}

	logger.Debugf("Successfully created BTree page manager (maxSize: %d, pageSize: %d)",
		cacheSize, pageSize)

	return pm, nil
}

// StartFlushWorker launches a background goroutine that monitors dirty page ratio
// and triggers flush operations when the 80% threshold is exceeded.
// This prevents memory pressure and ensures dirty pages don't accumulate indefinitely.
//
// The worker runs until the flushTriggerChan is closed (during index Close).
// TODO: Add configurable cooldown period if continuous flushing impacts performance
func (pm *BTreePageManager) StartFlushWorker() {
	go func() {
		pm.logger.Debug("B-tree flush worker started")
		for range pm.flushTriggerChan {
			// Flush triggered by PutPage when dirty ratio exceeded 80%
			pm.logger.Debug("Flush worker triggered by dirty page threshold")

			// Note: pageWriter must be set before flush can work
			if pm.pageWriter != nil {
				if err := pm.Flush(pm.pageWriter); err != nil {
					pm.logger.Warnw("Background flush failed", zap.Error(err))
				} else {
					pm.logger.Debug("Background flush completed successfully")
				}
			} else {
				pm.logger.Warn("Flush triggered but pageWriter not set, skipping")
			}
		}
		pm.logger.Debug("B-tree flush worker stopped")
	}()
}

// GetPage retrieves a page from cache or loads it using the provided loader function
// Parameters:
//   - pageNum: The page number to retrieve
//   - loader: Function to load the page if not in cache
//
// Returns:
//   - interface{}: The page data
//   - error: Any error that occurred during loading
func (pm *BTreePageManager) GetPage(pageNum uint32, loader func(uint32) (interface{}, error)) (interface{}, error) {
	pm.mutex.Lock()
	defer pm.mutex.Unlock()

	// Check if page is in cache
	if entry, exists := pm.cache[pageNum]; exists {
		// Update access time and count
		entry.lastAccess = time.Now()
		atomic.AddUint64(&entry.accessCount, 1)

		// Move to front of LRU list
		pm.moveToFront(entry)

		atomic.AddUint64(&pm.stats.hits, 1)

		// INTENSIVE DEBUG: Log details for internal nodes
		// if node, ok := entry.pageData.(*BTreeNode); ok && !node.IsLeaf {
		// 	pm.logger.Debugf("GetPage CACHE HIT: page %d is internal node with %d keys (ptr=%p)",
		// 		pageNum, node.KeyCount, entry.pageData)
		// } else {
		// 	pm.logger.Debugf("Cache hit for page %d", pageNum)
		// }

		return entry.pageData, nil
	}

	// Cache miss - load from storage
	atomic.AddUint64(&pm.stats.misses, 1)
	pm.logger.Debugf("Cache miss for page %d, loading from storage", pageNum)

	pageData, err := loader(pageNum)
	if err != nil {
		return nil, err
	}

	// INTENSIVE DEBUG: Log details for loaded internal nodes
	// if node, ok := pageData.(*BTreeNode); ok && !node.IsLeaf {
	// 	pm.logger.Debugf("GetPage LOADED FROM DISK: page %d is internal node with %d keys (ptr=%p)",
	// 		pageNum, node.KeyCount, pageData)
	// }

	// Add to cache
	pm.addToCache(pageNum, pageData, false)

	return pageData, nil
}

// GetPageReadOnly is for read-only traversals (e.g. Search, Exists). On cache hit it uses
// RLock and skips moveToFront/lastAccess to reduce latency and allow concurrent readers.
// On cache miss it falls back to GetPage.
func (pm *BTreePageManager) GetPageReadOnly(pageNum uint32, loader func(uint32) (interface{}, error)) (interface{}, error) {
	pm.mutex.RLock()
	entry, exists := pm.cache[pageNum]
	if exists {
		atomic.AddUint64(&pm.stats.hits, 1)
		pageData := entry.pageData
		pm.mutex.RUnlock()
		return pageData, nil
	}
	pm.mutex.RUnlock()
	return pm.GetPage(pageNum, loader)
}

// PutPage stores a page in the cache, optionally marking it as dirty
// Parameters:
//   - pageNum: The page number to store
//   - pageData: The page data to store
//   - dirty: Whether the page has been modified and needs to be written back
func (pm *BTreePageManager) PutPage(pageNum uint32, pageData interface{}, dirty bool) {
	pm.mutex.Lock()
	defer pm.mutex.Unlock()

	// INTENSIVE DEBUG: Log details for internal nodes
	// if node, ok := pageData.(*BTreeNode); ok && !node.IsLeaf {
	// 	pm.logger.Debugf("PutPage CALLED: page %d is internal node with %d keys, dirty=%t (ptr=%p)",
	// 		pageNum, node.KeyCount, dirty, pageData)
	// } else {
	// 	pm.logger.Debugf("Putting page %d in cache (dirty: %t)", pageNum, dirty)
	// }

	// Check if page is already in cache
	if entry, exists := pm.cache[pageNum]; exists {
		// Track dirty page count changes
		wasAlreadyDirty := entry.isDirty

		// CRITICAL: Only update pageData if it's a different object
		// If same pointer, just update dirty flag to avoid no-op overwrites
		if entry.pageData != pageData {
			oldNode, oldOk := entry.pageData.(*BTreeNode)
			newNode, newOk := pageData.(*BTreeNode)
			if oldOk && newOk && !oldNode.IsLeaf && !newNode.IsLeaf {
				pm.logger.Warnf("PutPage REPLACING: page %d old ptr=%p with %d keys -> new ptr=%p with %d keys",
					pageNum, entry.pageData, oldNode.KeyCount, pageData, newNode.KeyCount)
			} else {
				pm.logger.Debugf("Replacing cached page %d with new object", pageNum)
			}
			entry.pageData = pageData
		} else {
			// if node, ok := pageData.(*BTreeNode); ok && !node.IsLeaf {
			// 	pm.logger.Debugf("PutPage SAME POINTER: page %d already cached with same object (ptr=%p), just updating dirty flag",
			// 		pageNum, pageData)
			// }
		}

		entry.isDirty = entry.isDirty || dirty // Once dirty, stays dirty until flushed

		// Atomically increment dirty page count if newly marked dirty
		if !wasAlreadyDirty && entry.isDirty {
			newCount := atomic.AddInt32(&pm.dirtyPageCount, 1)
			pm.logger.Debugf("Dirty page count increased to %d after marking page %d dirty", newCount, pageNum)
		}

		entry.lastAccess = time.Now()
		atomic.AddUint64(&entry.accessCount, 1)

		// Move to front of LRU list
		pm.moveToFront(entry)

		pm.logger.Debugf("Updated existing cache entry for page %d", pageNum)

		// Check if dirty page ratio exceeds 80% threshold and trigger auto-flush
		pm.checkAndTriggerFlush()

		return
	}

	// Add new entry to cache
	if node, ok := pageData.(*BTreeNode); ok && !node.IsLeaf {
		pm.logger.Debugf("PutPage ADDING NEW: page %d internal node with %d keys (ptr=%p)",
			pageNum, node.KeyCount, pageData)
	}
	pm.addToCache(pageNum, pageData, dirty)

	// Check if dirty page ratio exceeds 80% threshold and trigger auto-flush
	pm.checkAndTriggerFlush()
}

// checkAndTriggerFlush checks if dirty page ratio exceeds 80% and triggers background flush
// This is called by PutPage and MarkPageDirty after modifying pages
// Note: Must be called while holding pm.mutex lock
func (pm *BTreePageManager) checkAndTriggerFlush() {
	if pm.currentSize == 0 {
		return // Avoid division by zero
	}

	dirtyCount := atomic.LoadInt32(&pm.dirtyPageCount)
	dirtyRatio := float64(dirtyCount) / float64(pm.currentSize)

	// Trigger flush if dirty ratio exceeds 80% threshold
	if dirtyRatio > 0.8 {
		pm.logger.Debugf("Dirty page ratio %.2f%% exceeds 80%% threshold (%d/%d), triggering background flush",
			dirtyRatio*100, dirtyCount, pm.currentSize)

		// Non-blocking send to flush trigger channel (buffered size 1)
		// If channel already has a pending signal, skip (flush already queued)
		select {
		case pm.flushTriggerChan <- struct{}{}:
			pm.logger.Debug("Sent flush trigger signal to background worker")
		default:
			pm.logger.Debug("Flush already queued, skipping duplicate trigger")
		}
	}
}

// MarkPageDirty marks a cached page as dirty without replacing its data
// This is more efficient than PutPage when you've modified a cached page in-place
func (pm *BTreePageManager) MarkPageDirty(pageNum uint32) error {
	pm.mutex.Lock()
	defer pm.mutex.Unlock()

	entry, exists := pm.cache[pageNum]
	if !exists {
		return fmt.Errorf("cannot mark page %d as dirty: not in cache", pageNum)
	}

	wasAlreadyDirty := entry.isDirty
	entry.isDirty = true

	// Atomically increment dirty page count if newly marked dirty
	if !wasAlreadyDirty {
		newCount := atomic.AddInt32(&pm.dirtyPageCount, 1)
		pm.logger.Debugf("Dirty page count increased to %d after marking page %d dirty", newCount, pageNum)
	}

	entry.lastAccess = time.Now()
	pm.logger.Debugf("Marked page %d as dirty", pageNum)

	// Check if dirty page ratio exceeds 80% threshold and trigger auto-flush
	pm.checkAndTriggerFlush()

	return nil
}

// Flush writes all dirty pages to storage using the provided writer function
// Parameters:
//   - writer: Function to write pages to storage
//
// Returns:
//   - error: Any error that occurred during flushing
func (pm *BTreePageManager) Flush(writer func(uint32, interface{}) error) error {
	pm.mutex.Lock()
	defer pm.mutex.Unlock()

	pm.logger.Debugf("Flushing dirty pages to storage")

	dirtyCount := 0
	var flushErrors []error

	// Iterate through all cache entries to find dirty pages
	for pageNum, entry := range pm.cache {
		if entry.isDirty {
			pm.logger.Debugf("Flushing dirty page %d", pageNum)

			if err := writer(pageNum, entry.pageData); err != nil {
				flushErrors = append(flushErrors, fmt.Errorf("failed to flush page %d: %w", pageNum, err))
				continue
			}

			// Mark as clean after successful write
			entry.isDirty = false
			dirtyCount++
			atomic.AddUint64(&pm.stats.dirtyWrites, 1)
		}
	}

	// Atomically reset dirty page count after flushing all dirty pages
	if dirtyCount > 0 {
		atomic.StoreInt32(&pm.dirtyPageCount, 0)
		pm.logger.Debugf("Reset dirty page count to 0 after flushing %d pages", dirtyCount)
	}

	atomic.AddUint64(&pm.stats.flushes, 1)

	pm.logger.Debugf("Flushed %d dirty pages to storage", dirtyCount)

	// Return first error if any occurred
	if len(flushErrors) > 0 {
		return flushErrors[0]
	}

	return nil
}

// InvalidatePage removes a page from the cache
// Parameters:
//   - pageNum: The page number to remove from cache
func (pm *BTreePageManager) InvalidatePage(pageNum uint32) {
	pm.mutex.Lock()
	defer pm.mutex.Unlock()

	pm.logger.Debugf("Invalidating page %d from cache", pageNum)

	if entry, exists := pm.cache[pageNum]; exists {
		// Remove from LRU list
		pm.removeFromLRU(entry)

		// Remove from cache map
		delete(pm.cache, pageNum)
		pm.currentSize--

		// Update memory usage
		pm.updateMemoryUsage()

		pm.logger.Debugf("Invalidated page %d from cache", pageNum)
	}
}

// GetPageManagerCacheStats returns current cache performance statistics
// Returns:
//   - *CacheStats: Current cache statistics
func (pm *BTreePageManager) GetPageManagerCacheStats() *CacheStats {
	pm.mutex.RLock()
	defer pm.mutex.RUnlock()

	hits := atomic.LoadUint64(&pm.stats.hits)
	misses := atomic.LoadUint64(&pm.stats.misses)
	total := hits + misses

	var hitRate float64
	if total > 0 {
		hitRate = float64(hits) / float64(total)
	}

	// Count dirty pages
	dirtyPages := 0
	for _, entry := range pm.cache {
		if entry.isDirty {
			dirtyPages++
		}
	}

	return &CacheStats{
		HitRate:     hitRate,
		Hits:        hits,
		Misses:      misses,
		Evictions:   atomic.LoadUint64(&pm.stats.evictions),
		CurrentSize: pm.currentSize,
		MaxSize:     pm.maxSize,
		MemoryUsage: atomic.LoadUint64(&pm.stats.memoryUsage),
		DirtyPages:  dirtyPages,
	}
}

// GetDirtyPageCount returns the number of dirty pages in cache
// Returns:
//   - int: Number of dirty pages
func (pm *BTreePageManager) GetDirtyPageCount() int {
	pm.mutex.RLock()
	defer pm.mutex.RUnlock()

	dirtyCount := 0
	for _, entry := range pm.cache {
		if entry.isDirty {
			dirtyCount++
		}
	}

	return dirtyCount
}

// PinPage increments the pin count for a page, preventing it from being evicted
// This is critical for preventing data loss during operations that modify pages.
// Following PostgreSQL's buffer pool pinning strategy to ensure pages being
// actively modified cannot be evicted, even under memory pressure.
//
// IMPORTANT: Every PinPage call MUST be paired with UnpinPage when done.
// Failure to unpin will cause memory leaks as pages cannot be evicted.
//
// Single Responsibility: Manages pin count to prevent premature eviction
//
// Parameters:
//   - pageNum: The page number to pin
//
// Returns:
//   - error: Returns error if page is not in cache
//
// TODO: I could add pin count tracking statistics for monitoring pin/unpin balance
func (pm *BTreePageManager) PinPage(pageNum uint32) error {
	pm.mutex.Lock()
	defer pm.mutex.Unlock()

	entry, exists := pm.cache[pageNum]
	if !exists {
		return fmt.Errorf("cannot pin page %d: page not in cache", pageNum)
	}

	entry.pinCount++
	pm.logger.Debugf("Pinned page %d (pinCount: %d)", pageNum, entry.pinCount)

	return nil
}

// UnpinPage decrements the pin count for a page, allowing it to be evicted
// This releases the eviction protection set by PinPage.
//
// IMPORTANT: Must be called exactly once for each PinPage call.
// Calling UnpinPage without a matching PinPage is a programming error.
//
// Single Responsibility: Manages pin count release for eviction eligibility
//
// Parameters:
//   - pageNum: The page number to unpin
//
// Returns:
//   - error: Returns error if page is not in cache or pin count would go negative
func (pm *BTreePageManager) UnpinPage(pageNum uint32) error {
	pm.mutex.Lock()
	defer pm.mutex.Unlock()

	entry, exists := pm.cache[pageNum]
	if !exists {
		// Page might have been evicted already - this is not necessarily an error
		// in normal operation, so we log it but don't return an error
		pm.logger.Debugf("UnpinPage called for page %d which is not in cache", pageNum)
		return nil
	}

	if entry.pinCount <= 0 {
		return fmt.Errorf("cannot unpin page %d: pin count already zero", pageNum)
	}

	entry.pinCount--
	pm.logger.Debugf("Unpinned page %d (pinCount: %d)", pageNum, entry.pinCount)

	return nil
}

// Clear removes all pages from the cache
// Note: This does not flush dirty pages - call Flush first if needed
func (pm *BTreePageManager) Clear() {
	pm.mutex.Lock()
	defer pm.mutex.Unlock()

	pm.logger.Debugf("Clearing all pages from cache")

	// Clear the cache map
	pm.cache = make(map[uint32]*cacheEntry, pm.maxSize)

	// Reset LRU list
	pm.lruHead.next = pm.lruTail
	pm.lruTail.prev = pm.lruHead

	// Reset counters
	pm.currentSize = 0
	atomic.StoreUint64(&pm.stats.memoryUsage, 0)

	pm.logger.Debugf("Cleared all pages from cache")
}

// Private helper methods

// addToCache adds a new page to the cache, potentially evicting LRU pages
func (pm *BTreePageManager) addToCache(pageNum uint32, pageData interface{}, dirty bool) {
	// Check if we need to evict pages to make room
	if pm.currentSize >= pm.maxSize {
		pm.evictLRU()
	}

	// Create new cache entry
	entry := &cacheEntry{
		pageNum:     pageNum,
		pageData:    pageData,
		isDirty:     dirty,
		lastAccess:  time.Now(),
		accessCount: 1,
	}

	// Add to cache map
	pm.cache[pageNum] = entry
	pm.currentSize++

	// Atomically increment dirty page count if adding dirty page
	if dirty {
		newCount := atomic.AddInt32(&pm.dirtyPageCount, 1)
		pm.logger.Debugf("Dirty page count increased to %d after adding page %d", newCount, pageNum)
	}

	// Add to front of LRU list
	pm.addToFront(entry)

	// Update memory usage
	pm.updateMemoryUsage()

	pm.logger.Debugf("Added page %d to cache (currentSize: %d)", pageNum, pm.currentSize)
}

// evictLRU removes the least recently used page from cache
//
// Following PostgreSQL's buffer pool eviction strategy with pinning:
// 1. Find LRU unpinned page (tail of list, skip pinned pages)
// 2. If dirty, FLUSH to disk before evicting
// 3. Remove from cache and free memory
// 4. Update statistics
//
// CRITICAL: Pinned pages are NEVER evicted, preventing data loss during
// active modifications. This ensures pages being worked on stay in cache.
//
// Single Responsibility: Handles eviction logic with pin awareness
//
// TODO: I could implement clock-sweep algorithm for better performance on
// workloads with sequential scans (PostgreSQL uses this in production)
func (pm *BTreePageManager) evictLRU() {
	if pm.currentSize == 0 {
		return
	}

	// Two-pass eviction: prefer clean pages to avoid synchronous write during eviction.
	// First pass: find unpinned clean page (no I/O).
	// Second pass: if none, find unpinned dirty page (must flush before eviction).
	var lru *cacheEntry
	current := pm.lruTail.prev
	for current != pm.lruHead {
		if current.pinCount == 0 && !current.isDirty {
			lru = current
			break
		}
		if current.pinCount != 0 {
			pm.logger.Debugf("Skipping pinned page %d (pinCount: %d) during eviction",
				current.pageNum, current.pinCount)
		}
		current = current.prev
	}
	if lru == nil {
		current = pm.lruTail.prev
		for current != pm.lruHead {
			if current.pinCount == 0 {
				lru = current
				break
			}
			pm.logger.Debugf("Skipping pinned page %d (pinCount: %d) during eviction",
				current.pageNum, current.pinCount)
			current = current.prev
		}
	}

	// If all pages are pinned, we cannot evict
	if lru == nil {
		pm.logger.Warnf("Cannot evict: all %d cached pages are pinned", pm.currentSize)
		return
	}

	pm.logger.Debugf("Evicting LRU page %d from cache", lru.pageNum)

	// CRITICAL BUG FIX: Verify cache entry still matches LRU entry before flush
	// Between finding LRU entry and evicting, another thread could have updated the page
	// with different data. We MUST use the current cache data, not stale LRU pointer data.
	cacheEntry, exists := pm.cache[lru.pageNum]
	if !exists {
		// Page was already removed from cache by another thread - safe to skip
		pm.logger.Debugf("Page %d already removed from cache, skipping eviction", lru.pageNum)
		pm.removeFromLRU(lru)
		pm.currentSize--
		pm.updateMemoryUsage()
		return
	}

	// Keep reference to LRU list entry for list manipulation
	lruListEntry := lru

	// Track which data to flush - use current cache data to avoid stale writes
	pageDataToFlush := cacheEntry.pageData
	isDirtyFlag := cacheEntry.isDirty

	// If page was updated in cache after we selected LRU entry,
	// the cache map and LRU list are out of sync - log this for debugging
	if cacheEntry.pageData != lru.pageData {
		pm.logger.Warnf("RACE DETECTED: Page %d was updated during eviction (old ptr=%p, new ptr=%p) - using current cache data to prevent corruption",
			lru.pageNum, lru.pageData, cacheEntry.pageData)
	}

	// CRITICAL: Flush dirty pages before eviction to prevent data loss
	// Use current cache data, not stale LRU pointer data
	if isDirtyFlag {
		if pm.pageWriter != nil {
			if err := pm.pageWriter(lru.pageNum, pageDataToFlush); err != nil {
				// Log error but continue with eviction
				// TODO: I could implement retry logic with exponential backoff
				// for transient I/O errors to improve reliability
				pm.logger.Errorf("FLUSH FAILED FOR PAGE %d: %v", lru.pageNum, err)
			} else {
				// Successfully flushed
				atomic.AddUint64(&pm.stats.dirtyWrites, 1)
				// pm.logger.Warnf("SUCCESSFULLY FLUSHED PAGE %d BEFORE EVICTION", lru.pageNum)
			}
		} else {
			// No writer configured - this should not happen in production
			pm.logger.Errorf("CRITICAL: Evicting dirty page %d without flush - no writer configured (DATA WILL BE LOST)", lru.pageNum)
		}
	}

	// Remove from cache map
	delete(pm.cache, lru.pageNum)
	pm.currentSize--

	// Remove from LRU list using the original LRU list entry pointer
	pm.removeFromLRU(lruListEntry)

	// Update statistics
	atomic.AddUint64(&pm.stats.evictions, 1)
	pm.updateMemoryUsage()

	pm.logger.Debugf("Successfully evicted page %d (currentSize: %d)", lru.pageNum, pm.currentSize)
}

// moveToFront moves an entry to the front of the LRU list
func (pm *BTreePageManager) moveToFront(entry *cacheEntry) {
	// Remove from current position
	pm.removeFromLRU(entry)

	// Add to front
	pm.addToFront(entry)
}

// addToFront adds an entry to the front of the LRU list
func (pm *BTreePageManager) addToFront(entry *cacheEntry) {
	entry.prev = pm.lruHead
	entry.next = pm.lruHead.next
	pm.lruHead.next.prev = entry
	pm.lruHead.next = entry
}

// removeFromLRU removes an entry from the LRU list
func (pm *BTreePageManager) removeFromLRU(entry *cacheEntry) {
	if entry.prev != nil {
		entry.prev.next = entry.next
	}
	if entry.next != nil {
		entry.next.prev = entry.prev
	}
}

// updateMemoryUsage calculates and updates the current memory usage
func (pm *BTreePageManager) updateMemoryUsage() {
	memoryUsage := uint64(pm.currentSize) * uint64(pm.pageSize)
	atomic.StoreUint64(&pm.stats.memoryUsage, memoryUsage)
}

// SetWriter configures the page writer function for flushing dirty pages during eviction
//
// This allows the page manager to persist dirty pages before evicting them from cache,
// preventing data loss and ensuring durability.
//
// Single Responsibility: Dependency injection for page persistence
// DRY Principle: Reuses same writer function for both explicit flushes and evictions
//
// Parameters:
//   - writer: Function to write pages to storage (pageNum, pageData) -> error
//
// TODO: I could extend this to support batched writes where multiple dirty pages
// are flushed in a single I/O operation for improved performance
func (pm *BTreePageManager) SetWriter(writer func(uint32, interface{}) error) {
	pm.mutex.Lock()
	defer pm.mutex.Unlock()

	pm.pageWriter = writer
	pm.logger.Debugf("Configured page writer for dirty page eviction")
}

// GetMaxSize returns the maximum cache size
// Returns:
//   - int: Maximum number of pages that can be cached
func (pm *BTreePageManager) GetMaxSize() int {
	return pm.maxSize
}

// GetCurrentSize returns the current cache size
// Returns:
//   - int: Current number of pages in cache
func (pm *BTreePageManager) GetCurrentSize() int {
	pm.mutex.RLock()
	defer pm.mutex.RUnlock()

	return pm.currentSize
}

// GetPageSize returns the page size
// Returns:
//   - uint32: Size of each page in bytes
func (pm *BTreePageManager) GetPageSize() uint32 {
	return pm.pageSize
}

// IsPageCached checks if a page is currently in cache
// Parameters:
//   - pageNum: The page number to check
//
// Returns:
//   - bool: True if the page is cached
func (pm *BTreePageManager) IsPageCached(pageNum uint32) bool {
	pm.mutex.RLock()
	defer pm.mutex.RUnlock()

	_, exists := pm.cache[pageNum]
	return exists
}

// IsPageDirty checks if a cached page is dirty
// Parameters:
//   - pageNum: The page number to check
//
// Returns:
//   - bool: True if the page is cached and dirty
func (pm *BTreePageManager) IsPageDirty(pageNum uint32) bool {
	pm.mutex.RLock()
	defer pm.mutex.RUnlock()

	if entry, exists := pm.cache[pageNum]; exists {
		return entry.isDirty
	}

	return false
}
