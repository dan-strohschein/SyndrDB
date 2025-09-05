/*
PAGE MANAGEMENT SYSTEM

This file implements an LRU (Least Recently Used) cache for hash index pages
to improve performance by keeping frequently accessed pages in memory.

ALGORITHM OVERVIEW:
The page manager maintains an in-memory cache of the most recently used pages
using an LRU eviction policy. This reduces disk I/O for frequently accessed
buckets and overflow pages.

LRU CACHE IMPLEMENTATION:
- Uses a doubly-linked list to track access order
- Hash map provides O(1) access to cached pages
- When cache is full, least recently used page is evicted
- Cache size is configurable based on available memory

PAGE TYPES:
- Metadata pages: Contains index configuration
- Bucket pages: Primary hash buckets
- Overflow pages: Additional storage for bucket overflow

CONCURRENCY:
Page manager operations are thread-safe and can be called concurrently
from multiple goroutines accessing the hash index.
*/

package hashindexV2

import (
	"container/list"
	"fmt"
	"sync"

	"go.uber.org/zap"
)

// PageManager manages an LRU cache of hash index pages
type PageManager struct {
	cache        map[uint32]*list.Element
	dirty        map[uint32]bool
	maxPages     int
	mutex        sync.RWMutex
	logger       *zap.SugaredLogger
	lruList      *list.List                      // Doubly-linked list for LRU tracking
	maxCacheSize int                             // Maximum number of pages to cache
	flushFunc    func(uint32, interface{}) error // Function to flush dirty pages
}

// CachedPage represents a page stored in the cache
type CachedPage struct {
	PageNum uint32
	Data    interface{} // Can be *BucketPage, *OverflowPage, etc.
	Dirty   bool        // Whether page has been modified
}

// NewPageManager creates a new page manager with LRU cache
// Parameters:
//   - pageSize: Size of each page in bytes
//   - maxCacheSize: Maximum number of pages to cache
//   - logger: Logger for debug/error messages
//
// Returns:
//   - *PageManager: The page manager instance
//   - error: Any error that occurred during creation
func NewPageManager(pageSize uint32, cacheSize int, logger *zap.SugaredLogger) (*PageManager, error) {
	logger.Infof("Creating PageManager with cache size: %d pages", cacheSize)
	return &PageManager{
		cache:        make(map[uint32]*list.Element),
		dirty:        make(map[uint32]bool),
		maxPages:     cacheSize,
		maxCacheSize: cacheSize,
		lruList:      list.New(),
		logger:       logger,
		flushFunc:    nil, // Will be set later by the hash index
	}, nil
}

// SetFlushFunction sets the function used to flush dirty pages during eviction
func (pm *PageManager) SetFlushFunction(flushFunc func(uint32, interface{}) error) {
	pm.mutex.Lock()
	defer pm.mutex.Unlock()
	pm.flushFunc = flushFunc
}

// GetBucket is a convenience method that retrieves a bucket page
// Parameters:
//   - bucketNum: The bucket number to retrieve
//
// Returns:
//   - *BucketPage: The bucket page
//   - error: Any error that occurred
func (pm *PageManager) GetBucket(bucketNum uint32) (*BucketPage, error) {
	pageNum := bucketNumberToPageNumber(bucketNum)

	pageData, err := pm.GetPage(pageNum, func(pageNum uint32) (interface{}, error) {
		// This would be provided by the file manager
		return nil, fmt.Errorf("no loader function provided")
	})
	if err != nil {
		return nil, err
	}

	bucketPage, ok := pageData.(*BucketPage)
	if !ok {
		return nil, fmt.Errorf("page %d is not a bucket page", pageNum)
	}

	return bucketPage, nil
}

// GetPage retrieves a page from cache or storage
// Parameters:
//   - pageNum: The page number to retrieve
//   - loader: Function to load page from storage if not cached
//
// Returns:
//   - interface{}: The page data
//   - error: Any error that occurred during retrieval
func (pm *PageManager) GetPage(pageNum uint32, loader func(uint32) (interface{}, error)) (interface{}, error) {
	pm.mutex.RLock()
	if element, exists := pm.cache[pageNum]; exists {
		// Move to front of LRU list (mark as recently used)
		pm.mutex.RUnlock()
		pm.mutex.Lock()
		pm.lruList.MoveToFront(element)
		cachedPage := element.Value.(*CachedPage)
		pm.mutex.Unlock()
		return cachedPage.Data, nil // Return cachedPage.Data, not pageData
	}
	pm.mutex.RUnlock()

	// Load from storage
	pageData, err := loader(pageNum)
	if err != nil {
		return nil, err
	}

	// Cache the page
	pm.PutPage(pageNum, pageData, false)
	return pageData, nil
}

// PutPage stores a page in the cache and marks it as dirty
// Parameters:
//   - pageNum: The page number
//   - pageData: The page data to store
//   - dirty: Whether the page has been modified
func (pm *PageManager) PutPage(pageNum uint32, pageData interface{}, isDirty bool) {

	pm.mutex.Lock()
	defer pm.mutex.Unlock()

	pm.addToCache(pageNum, pageData, isDirty)
}

// Flush writes all dirty pages using the writer function
func (pm *PageManager) Flush(writer func(uint32, interface{}) error) error {
	pm.mutex.Lock()
	defer pm.mutex.Unlock()

	for element := pm.lruList.Front(); element != nil; element = element.Next() {
		cachedPage := element.Value.(*CachedPage)
		if cachedPage.Dirty {
			if err := writer(cachedPage.PageNum, cachedPage.Data); err != nil {
				return err
			}
			cachedPage.Dirty = false
		}
	}
	return nil
}

// addToCache adds a page to the cache with LRU eviction
// Parameters:
//   - pageNum: The page number
//   - pageData: The page data
//   - dirty: Whether the page is dirty
func (pm *PageManager) addToCache(pageNum uint32, pageData interface{}, dirty bool) {
	// Create cached page
	cachedPage := &CachedPage{
		PageNum: pageNum,
		Data:    pageData,
		Dirty:   dirty,
	}

	// Check if we need to evict a page
	if pm.lruList.Len() >= pm.maxCacheSize {
		pm.evictLeastRecentlyUsed()
	}

	// Add to front of LRU list
	element := pm.lruList.PushFront(cachedPage)
	pm.cache[pageNum] = element

	pm.logger.Debugf("Added page %d to cache (size: %d)", pageNum, pm.lruList.Len())
}

// evictLeastRecentlyUsed removes the least recently used page from cache
func (pm *PageManager) evictLeastRecentlyUsed() {
	if pm.lruList.Len() == 0 {
		return
	}

	// Get least recently used page (back of list)
	element := pm.lruList.Back()
	cachedPage := element.Value.(*CachedPage)

	// If page is dirty, attempt to flush it before evicting
	if cachedPage.Dirty {
		if pm.flushFunc != nil {
			if err := pm.flushFunc(cachedPage.PageNum, cachedPage.Data); err != nil {
				pm.logger.Errorf("Failed to flush dirty page %d before eviction: %v", cachedPage.PageNum, err)
			} else {
				pm.logger.Debugf("Successfully flushed dirty page %d before eviction", cachedPage.PageNum)
				cachedPage.Dirty = false
			}
		} else {
			pm.logger.Warnf("Evicting dirty page %d - changes may be lost (no flush function set)", cachedPage.PageNum)
		}
	}

	// Remove from cache and LRU list
	delete(pm.cache, cachedPage.PageNum)
	pm.lruList.Remove(element)

	pm.logger.Debugf("Evicted page %d from cache", cachedPage.PageNum)
}

// FlushDirtyPages writes all dirty pages to storage
// Parameters:
//   - writer: Function to write a page to storage
//
// Returns:
//   - error: Any error that occurred during flushing
func (pm *PageManager) FlushDirtyPages(writer func(uint32, interface{}) error) error {
	pm.mutex.RLock()
	defer pm.mutex.RUnlock()

	dirtyCount := 0
	for element := pm.lruList.Front(); element != nil; element = element.Next() {
		cachedPage := element.Value.(*CachedPage)
		if cachedPage.Dirty {
			err := writer(cachedPage.PageNum, cachedPage.Data)
			if err != nil {
				return fmt.Errorf("failed to write dirty page %d: %w", cachedPage.PageNum, err)
			}
			cachedPage.Dirty = false
			dirtyCount++
		}
	}

	pm.logger.Debugf("Flushed %d dirty pages", dirtyCount)
	return nil
}

// InvalidatePage removes a page from the cache
// Parameters:
//   - pageNum: The page number to invalidate
func (pm *PageManager) InvalidatePage(pageNum uint32) {
	pm.mutex.Lock()
	defer pm.mutex.Unlock()

	if element, exists := pm.cache[pageNum]; exists {
		pm.lruList.Remove(element)
		delete(pm.cache, pageNum)
		pm.logger.Debugf("Invalidated page %d", pageNum)
	}
}

// GetCacheStats returns current cache statistics
// Returns:
//   - CacheStats: Current cache statistics
func (pm *PageManager) GetCacheStats() CacheStats {
	pm.mutex.RLock()
	defer pm.mutex.RUnlock()

	dirtyCount := 0
	for element := pm.lruList.Front(); element != nil; element = element.Next() {
		cachedPage := element.Value.(*CachedPage)
		if cachedPage.Dirty {
			dirtyCount++
		}
	}

	return CacheStats{
		TotalPages:   pm.lruList.Len(),
		DirtyPages:   dirtyCount,
		MaxCacheSize: pm.maxCacheSize,
		HitRate:      0.0, // Would need to track hits/misses to calculate
	}
}

// CacheStats contains cache performance statistics

// Clear removes all pages from the cache
func (pm *PageManager) Clear() {
	pm.mutex.Lock()
	defer pm.mutex.Unlock()

	pm.cache = make(map[uint32]*list.Element)
	pm.lruList = list.New()
	pm.logger.Debugf("Cleared page cache")
}
