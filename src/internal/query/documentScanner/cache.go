package documentscanner

import (
	"container/list"
	"sync"
)

// SimpleLRUCache implements a basic LRU (Least Recently Used) cache
// This is a simple implementation for the document scanner that can be replaced
// with more sophisticated caching solutions like Redis or memcached
type SimpleLRUCache struct {
	maxSize int                           // Maximum number of entries
	cache   map[interface{}]*list.Element // Hash map for O(1) access
	lru     *list.List                    // Doubly-linked list for LRU ordering
	mu      sync.RWMutex                  // Protects cache and lru
}

// cacheEntry represents an entry in the cache with key-value pair
type cacheEntry struct {
	key   interface{}
	value interface{}
}

// NewSimpleLRUCache creates a new LRU cache with the specified maximum size
// maxSize: Maximum number of entries the cache can hold
func NewSimpleLRUCache(maxSize int) *SimpleLRUCache {
	return &SimpleLRUCache{
		maxSize: maxSize,
		cache:   make(map[interface{}]*list.Element),
		lru:     list.New(),
	}
}

// Get retrieves a value from the cache and marks it as recently used
// Returns the value and true if found, nil and false if not found
func (c *SimpleLRUCache) Get(key interface{}) (interface{}, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if element, found := c.cache[key]; found {
		// Move to front (mark as recently used)
		c.lru.MoveToFront(element)
		entry := element.Value.(*cacheEntry)
		return entry.value, true
	}

	return nil, false
}

// Put stores a value in the cache
// If the cache is full, it removes the least recently used item
func (c *SimpleLRUCache) Put(key interface{}, value interface{}) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// If key already exists, update it
	if element, found := c.cache[key]; found {
		c.lru.MoveToFront(element)
		entry := element.Value.(*cacheEntry)
		entry.value = value
		return
	}

	// Add new entry
	entry := &cacheEntry{key: key, value: value}
	element := c.lru.PushFront(entry)
	c.cache[key] = element

	// Remove oldest entry if cache is full
	if c.lru.Len() > c.maxSize {
		c.removeOldest()
	}
}

// Contains checks if a key exists in the cache without retrieving it
// This is useful for checking cache status without affecting LRU order
func (c *SimpleLRUCache) Contains(key interface{}) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()

	_, found := c.cache[key]
	return found
}

// Remove removes a key from the cache
func (c *SimpleLRUCache) Remove(key interface{}) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if element, found := c.cache[key]; found {
		c.lru.Remove(element)
		delete(c.cache, key)
	}
}

// Clear removes all entries from the cache
func (c *SimpleLRUCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.cache = make(map[interface{}]*list.Element)
	c.lru = list.New()
}

// Size returns the current number of cached items
func (c *SimpleLRUCache) Size() int {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.lru.Len()
}

// removeOldest removes the least recently used item from the cache
// This method assumes the caller holds the write lock
func (c *SimpleLRUCache) removeOldest() {
	if c.lru.Len() == 0 {
		return
	}

	oldest := c.lru.Back()
	if oldest != nil {
		c.lru.Remove(oldest)
		entry := oldest.Value.(*cacheEntry)
		delete(c.cache, entry.key)
	}
}

// GetStats returns cache statistics for monitoring
func (c *SimpleLRUCache) GetStats() map[string]interface{} {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return map[string]interface{}{
		"size":     c.lru.Len(),
		"max_size": c.maxSize,
		"usage":    float64(c.lru.Len()) / float64(c.maxSize),
	}
}
