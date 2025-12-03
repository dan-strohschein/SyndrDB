package utils

import (
	"container/list"
	"fmt"
	"sync"
	"time"
)

// TimezoneCache provides a two-tier caching strategy for timezone lookups:
// 1. Fast-path: sync.Map for 20 most common timezones (hot zones)
// 2. Slow-path: LRU cache for rare timezones (configurable size)
//
// Design Goals:
// - <5μs hot cache lookup time
// - <50μs LRU cache lookup time
// - Thread-safe for concurrent query execution
// - Zero allocation for hot zone lookups
//
// TODO: I will add cache hit/miss metrics when telemetry framework is implemented
// TODO: I will add cache size adjustment based on miss rate when profiling shows >5% misses
type TimezoneCache struct {
	// Hot zone cache (sync.Map for lock-free reads)
	hotZones sync.Map // map[string]*time.Location

	// LRU cache for rare timezones
	lru      *lruCache
	lruMutex sync.RWMutex // Protects LRU cache operations
}

// lruCache implements a simple LRU eviction policy
// TODO: I will optimize to doubly-linked list + map if profiling shows >10μs eviction time
type lruCache struct {
	capacity int
	items    map[string]*list.Element // map[timezone_name]*list.Element
	order    *list.List               // Most recently used at front
}

// lruEntry represents a cached timezone entry
type lruEntry struct {
	key      string
	location *time.Location
}

// NewTimezoneCache creates a new timezone cache with the given LRU capacity
// TODO: I will add validation for capacity ≥ 16 when configuration validation is implemented
func NewTimezoneCache(lruCapacity int) *TimezoneCache {
	cache := &TimezoneCache{
		lru: &lruCache{
			capacity: lruCapacity,
			items:    make(map[string]*list.Element, lruCapacity),
			order:    list.New(),
		},
	}

	// Preload 20 most common timezones into hot zone cache
	// TODO: I will make this list configurable via settings when usage patterns show different hot zones
	hotZoneNames := []string{
		"UTC",
		"America/New_York",
		"America/Chicago",
		"America/Denver",
		"America/Los_Angeles",
		"America/Phoenix",
		"America/Toronto",
		"America/Vancouver",
		"Europe/London",
		"Europe/Paris",
		"Europe/Berlin",
		"Europe/Moscow",
		"Asia/Tokyo",
		"Asia/Shanghai",
		"Asia/Hong_Kong",
		"Asia/Singapore",
		"Asia/Dubai",
		"Australia/Sydney",
		"Pacific/Auckland",
		"America/Sao_Paulo",
	}

	for _, name := range hotZoneNames {
		if loc, err := time.LoadLocation(name); err == nil {
			cache.hotZones.Store(name, loc)
		}
		// TODO: I will log failed preloads when logger integration is available
	}

	return cache
}

// Get retrieves a timezone location, checking hot zones first, then LRU cache
// Returns error if timezone is invalid
// TODO: I will add telemetry for cache hit rates when metrics framework is ready
func (tc *TimezoneCache) Get(timezone string) (*time.Location, error) {
	// Fast path: Check hot zones (lock-free read)
	if loc, ok := tc.hotZones.Load(timezone); ok {
		return loc.(*time.Location), nil
	}

	// Slow path: Check LRU cache
	tc.lruMutex.RLock()
	if elem, ok := tc.lru.items[timezone]; ok {
		// Move to front (most recently used)
		tc.lru.order.MoveToFront(elem)
		entry := elem.Value.(*lruEntry)
		tc.lruMutex.RUnlock()
		return entry.location, nil
	}
	tc.lruMutex.RUnlock()

	// Miss: Load from system and cache in LRU
	loc, err := time.LoadLocation(timezone)
	if err != nil {
		return nil, fmt.Errorf("invalid timezone '%s': %w", timezone, err)
	}

	// Add to LRU cache
	tc.lruMutex.Lock()
	defer tc.lruMutex.Unlock()

	// Check again after acquiring write lock (double-check pattern)
	if elem, ok := tc.lru.items[timezone]; ok {
		tc.lru.order.MoveToFront(elem)
		return elem.Value.(*lruEntry).location, nil
	}

	// Evict oldest if at capacity
	if tc.lru.order.Len() >= tc.lru.capacity {
		oldest := tc.lru.order.Back()
		if oldest != nil {
			oldEntry := oldest.Value.(*lruEntry)
			delete(tc.lru.items, oldEntry.key)
			tc.lru.order.Remove(oldest)
		}
	}

	// Insert at front
	entry := &lruEntry{
		key:      timezone,
		location: loc,
	}
	elem := tc.lru.order.PushFront(entry)
	tc.lru.items[timezone] = elem

	return loc, nil
}

// GetUTC returns the UTC location (hot zone, never fails)
// TODO: I will inline this in critical paths if profiling shows call overhead
func (tc *TimezoneCache) GetUTC() *time.Location {
	loc, _ := tc.hotZones.Load("UTC")
	return loc.(*time.Location)
}

// Clear removes all entries from the LRU cache (hot zones remain)
// TODO: I will add selective eviction (e.g., clear zones not used in last N minutes) when needed
func (tc *TimezoneCache) Clear() {
	tc.lruMutex.Lock()
	defer tc.lruMutex.Unlock()

	tc.lru.items = make(map[string]*list.Element, tc.lru.capacity)
	tc.lru.order.Init()
}

// Size returns the current number of entries in LRU cache (hot zones excluded)
// TODO: I will add GetStats() method for cache hit/miss ratios when metrics are needed
func (tc *TimezoneCache) Size() int {
	tc.lruMutex.RLock()
	defer tc.lruMutex.RUnlock()
	return tc.lru.order.Len()
}
