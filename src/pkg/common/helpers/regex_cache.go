package helpers

import (
	"container/list"
	"regexp"
	"sync"
)

// PHASE C: LRU cache for compiled regex patterns to avoid recompilation
// This dramatically reduces allocations in query parsing and LIKE operations

const defaultCacheSize = 100

// regexCacheEntry holds a compiled regex and its LRU list element
type regexCacheEntry struct {
	pattern  string
	compiled *regexp.Regexp
	element  *list.Element
}

// RegexCache is a thread-safe LRU cache for compiled regex patterns
type RegexCache struct {
	mu      sync.RWMutex
	cache   map[string]*regexCacheEntry
	lruList *list.List
	maxSize int
	hits    uint64
	misses  uint64
}

// Global regex cache instance
var globalRegexCache = NewRegexCache(defaultCacheSize)

// NewRegexCache creates a new LRU regex cache with the specified max size
func NewRegexCache(maxSize int) *RegexCache {
	if maxSize <= 0 {
		maxSize = defaultCacheSize
	}
	return &RegexCache{
		cache:   make(map[string]*regexCacheEntry, maxSize),
		lruList: list.New(),
		maxSize: maxSize,
	}
}

// MustCompileCached compiles a regex pattern, using the cache if available
// Panics if the pattern is invalid (same behavior as regexp.MustCompile)
func MustCompileCached(pattern string) *regexp.Regexp {
	re, err := CompileCached(pattern)
	if err != nil {
		panic(`regexp: Compile(` + pattern + `): ` + err.Error())
	}
	return re
}

// CompileCached compiles a regex pattern, using the cache if available
// Returns the compiled regex or an error if compilation fails
func CompileCached(pattern string) (*regexp.Regexp, error) {
	return globalRegexCache.Compile(pattern)
}

// Compile compiles a regex pattern with LRU caching
func (rc *RegexCache) Compile(pattern string) (*regexp.Regexp, error) {
	// Fast path: check cache with read lock
	rc.mu.RLock()
	if entry, found := rc.cache[pattern]; found {
		// Move to front of LRU list (most recently used)
		rc.lruList.MoveToFront(entry.element)
		rc.hits++
		rc.mu.RUnlock()
		return entry.compiled, nil
	}
	rc.misses++
	rc.mu.RUnlock()

	// Slow path: compile regex and cache it
	compiled, err := regexp.Compile(pattern)
	if err != nil {
		return nil, err
	}

	rc.mu.Lock()
	defer rc.mu.Unlock()

	// Double-check: another goroutine might have cached it while we were compiling
	if entry, found := rc.cache[pattern]; found {
		rc.lruList.MoveToFront(entry.element)
		return entry.compiled, nil
	}

	// Evict least recently used if cache is full
	if rc.lruList.Len() >= rc.maxSize {
		oldest := rc.lruList.Back()
		if oldest != nil {
			oldEntry := oldest.Value.(*regexCacheEntry)
			delete(rc.cache, oldEntry.pattern)
			rc.lruList.Remove(oldest)
		}
	}

	// Add to cache
	elem := rc.lruList.PushFront(&regexCacheEntry{
		pattern:  pattern,
		compiled: compiled,
	})

	rc.cache[pattern] = &regexCacheEntry{
		pattern:  pattern,
		compiled: compiled,
		element:  elem,
	}

	return compiled, nil
}

// Stats returns cache hit/miss statistics
func (rc *RegexCache) Stats() (hits, misses uint64, size int) {
	rc.mu.RLock()
	defer rc.mu.RUnlock()
	return rc.hits, rc.misses, len(rc.cache)
}

// Clear empties the cache
func (rc *RegexCache) Clear() {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	rc.cache = make(map[string]*regexCacheEntry, rc.maxSize)
	rc.lruList = list.New()
	rc.hits = 0
	rc.misses = 0
}

// GlobalRegexCacheStats returns statistics for the global regex cache
func GlobalRegexCacheStats() (hits, misses uint64, size int) {
	return globalRegexCache.Stats()
}

// ClearGlobalRegexCache clears the global regex cache
func ClearGlobalRegexCache() {
	globalRegexCache.Clear()
}
