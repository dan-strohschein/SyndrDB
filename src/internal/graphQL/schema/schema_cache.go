package schema

import (
	"fmt"
	"sync"
	"time"
)

// SchemaCache provides thread-safe in-memory caching for GraphQL schemas
// Optimized for fast lookups (< 5μs) in the query hot path
type SchemaCache struct {
	mu      sync.RWMutex
	entries map[string]*CacheEntry
	stats   CacheStats
}

// CacheEntry represents a cached schema with metadata
type CacheEntry struct {
	Schema    *SchemaRecord
	LoadedAt  time.Time
	HitCount  uint64
	LastHitAt time.Time
}

// CacheStats tracks cache performance metrics
type CacheStats struct {
	Hits           uint64
	Misses         uint64
	Evictions      uint64
	Invalidations  uint64
	TotalEntries   int
	MemoryBytes    uint64
	LastAccessTime time.Time
}

// NewSchemaCache creates a new schema cache
func NewSchemaCache() *SchemaCache {
	return &SchemaCache{
		entries: make(map[string]*CacheEntry),
		stats:   CacheStats{},
	}
}

// Get retrieves a schema from cache by bundle name
// Returns nil if not found (cache miss)
// This is the hot path - must be < 5μs
func (c *SchemaCache) Get(bundleName string) *SchemaRecord {
	c.mu.RLock()
	defer c.mu.RUnlock()

	entry, exists := c.entries[bundleName]
	if !exists {
		c.stats.Misses++
		return nil
	}

	// Update stats (without additional locking for performance)
	c.stats.Hits++
	c.stats.LastAccessTime = time.Now()
	entry.HitCount++
	entry.LastHitAt = time.Now()

	return entry.Schema
}

// Set stores a schema in the cache
// If the entry already exists, it will be replaced
func (c *SchemaCache) Set(bundleName string, schema *SchemaRecord) error {
	if schema == nil {
		return fmt.Errorf("cannot cache nil schema")
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// Calculate approximate memory size
	memSize := c.calculateMemorySize(schema)

	// Store or replace entry
	if existing, exists := c.entries[bundleName]; exists {
		// Update stats for replacement
		c.stats.MemoryBytes -= c.calculateMemorySize(existing.Schema)
	}

	c.entries[bundleName] = &CacheEntry{
		Schema:    schema,
		LoadedAt:  time.Now(),
		HitCount:  0,
		LastHitAt: time.Now(),
	}

	c.stats.TotalEntries = len(c.entries)
	c.stats.MemoryBytes += memSize

	return nil
}

// Invalidate removes a schema from the cache
// Used when schemas are updated or tombstoned
func (c *SchemaCache) Invalidate(bundleName string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if entry, exists := c.entries[bundleName]; exists {
		c.stats.MemoryBytes -= c.calculateMemorySize(entry.Schema)
		delete(c.entries, bundleName)
		c.stats.Invalidations++
		c.stats.TotalEntries = len(c.entries)
	}
}

// InvalidateAll clears the entire cache
// Used during database operations or bulk updates
func (c *SchemaCache) InvalidateAll() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.entries = make(map[string]*CacheEntry)
	c.stats.Invalidations += uint64(c.stats.TotalEntries)
	c.stats.TotalEntries = 0
	c.stats.MemoryBytes = 0
}

// Preload loads multiple schemas into the cache
// Returns the number of schemas successfully loaded
func (c *SchemaCache) Preload(schemas []*SchemaRecord) (int, error) {
	loaded := 0

	for _, schema := range schemas {
		if schema == nil || !schema.IsActive() {
			continue
		}

		bundleName := schema.GetBundleName()
		if err := c.Set(bundleName, schema); err != nil {
			return loaded, fmt.Errorf("failed to preload schema %s: %w", bundleName, err)
		}
		loaded++
	}

	return loaded, nil
}

// GetStats returns a copy of the current cache statistics
func (c *SchemaCache) GetStats() CacheStats {
	c.mu.RLock()
	defer c.mu.RUnlock()

	// Return a copy to avoid race conditions
	return CacheStats{
		Hits:           c.stats.Hits,
		Misses:         c.stats.Misses,
		Evictions:      c.stats.Evictions,
		Invalidations:  c.stats.Invalidations,
		TotalEntries:   c.stats.TotalEntries,
		MemoryBytes:    c.stats.MemoryBytes,
		LastAccessTime: c.stats.LastAccessTime,
	}
}

// GetHitRate returns the cache hit rate as a percentage
func (c *SchemaCache) GetHitRate() float64 {
	c.mu.RLock()
	defer c.mu.RUnlock()

	total := c.stats.Hits + c.stats.Misses
	if total == 0 {
		return 0.0
	}

	return (float64(c.stats.Hits) / float64(total)) * 100.0
}

// Size returns the number of entries in the cache
func (c *SchemaCache) Size() int {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return len(c.entries)
}

// Contains checks if a bundle name exists in the cache
func (c *SchemaCache) Contains(bundleName string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()

	_, exists := c.entries[bundleName]
	return exists
}

// GetEntry retrieves the full cache entry including metadata
// Used for diagnostics and testing
func (c *SchemaCache) GetEntry(bundleName string) *CacheEntry {
	c.mu.RLock()
	defer c.mu.RUnlock()

	entry, exists := c.entries[bundleName]
	if !exists {
		return nil
	}

	// Return a copy to prevent external modification
	return &CacheEntry{
		Schema:    entry.Schema,
		LoadedAt:  entry.LoadedAt,
		HitCount:  entry.HitCount,
		LastHitAt: entry.LastHitAt,
	}
}

// ListBundles returns a list of all cached bundle names
func (c *SchemaCache) ListBundles() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	bundles := make([]string, 0, len(c.entries))
	for bundleName := range c.entries {
		bundles = append(bundles, bundleName)
	}

	return bundles
}

// calculateMemorySize estimates the memory footprint of a schema
// This is an approximation for memory tracking
func (c *SchemaCache) calculateMemorySize(schema *SchemaRecord) uint64 {
	if schema == nil {
		return 0
	}

	// Base struct size
	size := uint64(FixedRecordHeaderSize)

	// Add payload size
	size += uint64(schema.PayloadSize)

	// Add overhead for map entry, pointers, etc. (estimate ~100 bytes)
	size += 100

	return size
}

// Evict removes the least recently used entry
// Used for implementing cache size limits (future enhancement)
func (c *SchemaCache) Evict() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.entries) == 0 {
		return
	}

	// Find LRU entry
	var oldestBundle string
	var oldestTime time.Time = time.Now()

	for bundleName, entry := range c.entries {
		if entry.LastHitAt.Before(oldestTime) {
			oldestTime = entry.LastHitAt
			oldestBundle = bundleName
		}
	}

	// Remove the LRU entry
	if oldestBundle != "" {
		if entry, exists := c.entries[oldestBundle]; exists {
			c.stats.MemoryBytes -= c.calculateMemorySize(entry.Schema)
			delete(c.entries, oldestBundle)
			c.stats.Evictions++
			c.stats.TotalEntries = len(c.entries)
		}
	}
}

// WarmUp preloads schemas from a schema file
// Returns the number loaded and time taken
func (c *SchemaCache) WarmUp(schemaFile *SchemaFile) (int, time.Duration, error) {
	startTime := time.Now()

	// Read all active schemas
	schemas, err := schemaFile.ReadActiveSchemas()
	if err != nil {
		return 0, 0, fmt.Errorf("failed to read schemas: %w", err)
	}

	// Preload into cache
	loaded, err := c.Preload(schemas)
	if err != nil {
		return loaded, time.Since(startTime), err
	}

	return loaded, time.Since(startTime), nil
}

// ResetStats resets all cache statistics
// Used for testing or periodic monitoring
func (c *SchemaCache) ResetStats() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.stats.Hits = 0
	c.stats.Misses = 0
	c.stats.Evictions = 0
	c.stats.Invalidations = 0
	// Keep TotalEntries and MemoryBytes
}
