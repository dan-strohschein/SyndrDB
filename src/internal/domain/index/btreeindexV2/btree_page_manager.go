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

	// Statistics (using atomic operations for thread safety)
	stats cacheStatistics
}

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

	pm.logger.Infof("Cleared all pages from cache")
}

// cacheEntry represents a single entry in the LRU cache
// Each entry contains the page data and maintains LRU list pointers
type cacheEntry struct {
	pageNum     uint32      // Page number identifier
	pageData    interface{} // The actual page data (BTreeNode or BTreeMetadata)
	isDirty     bool        // Whether the page has been modified
	lastAccess  time.Time   // Last access timestamp for statistics
	accessCount uint64      // Number of times this page has been accessed

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
		cache:       make(map[uint32]*cacheEntry, cacheSize),
		lruHead:     head,
		lruTail:     tail,
		maxSize:     cacheSize,
		currentSize: 0,
		pageSize:    pageSize,
		mutex:       sync.RWMutex{},
		logger:      logger,
		stats:       cacheStatistics{},
	}

	logger.Infof("Successfully created BTree page manager (maxSize: %d, pageSize: %d)",
		cacheSize, pageSize)

	return pm, nil
}

// GetPage retrieves a page from cache or loads it using the provided loader function
// Parameters:
//   - pageNum: The page number to retrieve
//   - loader: Function to load the page if not in cache
//
// Returns:
//   - interface{}: The page data
//   - error: Any error that occurred during retrieval
func (pm *BTreePageManager) GetPage(pageNum uint32, loader func(uint32) (interface{}, error)) (interface{}, error) {
	pm.mutex.Lock()
	defer pm.mutex.Unlock()

	pm.logger.Debugf("Getting page %d from cache", pageNum)

	// Check if page is in cache
	if entry, exists := pm.cache[pageNum]; exists {
		// Cache hit - move to front of LRU list
		pm.moveToFront(entry)
		entry.lastAccess = time.Now()
		atomic.AddUint64(&entry.accessCount, 1)
		atomic.AddUint64(&pm.stats.hits, 1)

		pm.logger.Debugf("Cache hit for page %d", pageNum)
		return entry.pageData, nil
	}

	// Cache miss - load page using loader function
	atomic.AddUint64(&pm.stats.misses, 1)
	pm.logger.Debugf("Cache miss for page %d, loading from storage", pageNum)

	pageData, err := loader(pageNum)
	if err != nil {
		return nil, fmt.Errorf("failed to load page %d: %w", pageNum, err)
	}

	// Add to cache
	pm.addToCache(pageNum, pageData, false)

	return pageData, nil
}

// PutPage stores a page in the cache, optionally marking it as dirty
// Parameters:
//   - pageNum: The page number to store
//   - pageData: The page data to store
//   - dirty: Whether the page has been modified and needs to be written back
func (pm *BTreePageManager) PutPage(pageNum uint32, pageData interface{}, dirty bool) {
	pm.mutex.Lock()
	defer pm.mutex.Unlock()

	pm.logger.Debugf("Putting page %d in cache (dirty: %t)", pageNum, dirty)

	// Check if page is already in cache
	if entry, exists := pm.cache[pageNum]; exists {
		// Update existing entry
		entry.pageData = pageData
		entry.isDirty = entry.isDirty || dirty // Once dirty, stays dirty until flushed
		entry.lastAccess = time.Now()
		atomic.AddUint64(&entry.accessCount, 1)

		// Move to front of LRU list
		pm.moveToFront(entry)

		pm.logger.Debugf("Updated existing cache entry for page %d", pageNum)
		return
	}

	// Add new entry to cache
	pm.addToCache(pageNum, pageData, dirty)
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

	atomic.AddUint64(&pm.stats.flushes, 1)

	pm.logger.Infof("Flushed %d dirty pages to storage", dirtyCount)

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

	pm.logger.Infof("Cleared all pages from cache")
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

	// Add to front of LRU list
	pm.addToFront(entry)

	// Update memory usage
	pm.updateMemoryUsage()

	pm.logger.Debugf("Added page %d to cache (currentSize: %d)", pageNum, pm.currentSize)
}

// evictLRU removes the least recently used page from cache
func (pm *BTreePageManager) evictLRU() {
	if pm.currentSize == 0 {
		return
	}

	// Get the least recently used entry (tail of list)
	lru := pm.lruTail.prev
	if lru == pm.lruHead {
		return // Empty list
	}

	pm.logger.Debugf("Evicting LRU page %d from cache", lru.pageNum)

	// Warn if evicting a dirty page (should have been flushed)
	if lru.isDirty {
		pm.logger.Warnf("Evicting dirty page %d - data may be lost", lru.pageNum)
	}

	// Remove from cache map
	delete(pm.cache, lru.pageNum)
	pm.currentSize--

	// Remove from LRU list
	pm.removeFromLRU(lru)

	// Update statistics
	atomic.AddUint64(&pm.stats.evictions, 1)
	pm.updateMemoryUsage()
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
