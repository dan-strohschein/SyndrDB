package syndrQL

import (
	"container/list"
	"hash/fnv"
	"sync"
	"sync/atomic"

	"go.uber.org/zap"
)

/*
expression_cache.go

Shared LRU cache for parsed WHERE clause expressions. This eliminates redundant
tokenization and parsing when the same WHERE clause is used multiple times
(e.g., repeated UPDATE/DELETE/SELECT with same filter).

Performance impact:
- Avoids O(tokens) parsing per query
- Reduces allocations from tokenizer and parser
- Thread-safe with RWMutex (reads don't block each other)

Usage:
    expr, found := GlobalExpressionCache.Get(whereClause)
    if !found {
        expr, err = parseExpression(whereClause)
        GlobalExpressionCache.Put(whereClause, expr)
    }
*/

// CachedExpression holds a parsed expression and usage stats
type CachedExpression struct {
	Expression Expression
	Tokens     []Token // Pre-tokenized for potential reuse
	HitCount   uint64
}

// ExpressionCache is a thread-safe LRU cache for parsed expressions
type ExpressionCache struct {
	cache   map[uint64]*list.Element
	lru     *list.List
	mu      sync.RWMutex
	maxSize int
	hits    uint64
	misses  uint64
	logger  *zap.SugaredLogger
}

// lruEntry holds the key and value for LRU eviction
type lruEntry struct {
	key   uint64
	value *CachedExpression
}

// NewExpressionCache creates a new expression cache with the given max size
func NewExpressionCache(maxSize int, logger *zap.SugaredLogger) *ExpressionCache {
	return &ExpressionCache{
		cache:   make(map[uint64]*list.Element, maxSize),
		lru:     list.New(),
		maxSize: maxSize,
		logger:  logger,
	}
}

// hashWhereClause returns a fast hash of the WHERE clause string
func hashWhereClause(whereClause string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(whereClause))
	return h.Sum64()
}

// Get retrieves a cached expression by WHERE clause string
// Returns (expression, tokens, found)
func (c *ExpressionCache) Get(whereClause string) (Expression, []Token, bool) {
	if whereClause == "" {
		return nil, nil, false
	}

	key := hashWhereClause(whereClause)

	c.mu.RLock()
	elem, found := c.cache[key]
	c.mu.RUnlock()

	if !found {
		atomic.AddUint64(&c.misses, 1)
		return nil, nil, false
	}

	// Move to front (most recently used)
	c.mu.Lock()
	c.lru.MoveToFront(elem)
	c.mu.Unlock()

	entry := elem.Value.(*lruEntry)
	atomic.AddUint64(&entry.value.HitCount, 1)
	atomic.AddUint64(&c.hits, 1)

	return entry.value.Expression, entry.value.Tokens, true
}

// Put adds or updates an expression in the cache
func (c *ExpressionCache) Put(whereClause string, expr Expression, tokens []Token) {
	if whereClause == "" || expr == nil {
		return
	}

	key := hashWhereClause(whereClause)

	c.mu.Lock()
	defer c.mu.Unlock()

	// Check if already exists
	if elem, exists := c.cache[key]; exists {
		c.lru.MoveToFront(elem)
		entry := elem.Value.(*lruEntry)
		entry.value.Expression = expr
		entry.value.Tokens = tokens
		return
	}

	// Evict oldest if at capacity
	if c.lru.Len() >= c.maxSize {
		oldest := c.lru.Back()
		if oldest != nil {
			c.lru.Remove(oldest)
			delete(c.cache, oldest.Value.(*lruEntry).key)
		}
	}

	// Add new entry
	entry := &lruEntry{
		key: key,
		value: &CachedExpression{
			Expression: expr,
			Tokens:     tokens,
			HitCount:   0,
		},
	}
	elem := c.lru.PushFront(entry)
	c.cache[key] = elem
}

// Stats returns cache statistics
func (c *ExpressionCache) Stats() map[string]interface{} {
	c.mu.RLock()
	defer c.mu.RUnlock()

	hits := atomic.LoadUint64(&c.hits)
	misses := atomic.LoadUint64(&c.misses)
	total := hits + misses
	hitRate := float64(0)
	if total > 0 {
		hitRate = float64(hits) / float64(total) * 100
	}

	return map[string]interface{}{
		"size":     c.lru.Len(),
		"max_size": c.maxSize,
		"hits":     hits,
		"misses":   misses,
		"hit_rate": hitRate,
	}
}

// Clear removes all entries from the cache
func (c *ExpressionCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.cache = make(map[uint64]*list.Element, c.maxSize)
	c.lru.Init()
	atomic.StoreUint64(&c.hits, 0)
	atomic.StoreUint64(&c.misses, 0)
}

// Global expression cache singleton
var (
	globalExprCache     *ExpressionCache
	globalExprCacheOnce sync.Once
)

// GlobalExpressionCache returns the global expression cache singleton
// Lazy initialized on first access with default size of 10000 entries
func GlobalExpressionCache() *ExpressionCache {
	globalExprCacheOnce.Do(func() {
		globalExprCache = NewExpressionCache(10000, nil)
	})
	return globalExprCache
}

// InitGlobalExpressionCache initializes the global cache with custom settings
// Should be called during server startup before any queries are processed
func InitGlobalExpressionCache(maxSize int, logger *zap.SugaredLogger) {
	globalExprCacheOnce.Do(func() {
		globalExprCache = NewExpressionCache(maxSize, logger)
		if logger != nil {
			logger.Infof("Initialized global expression cache with max size %d", maxSize)
		}
	})
}

// ParseExpressionCached parses a WHERE clause, using cache when available
// This is the main entry point for cached expression parsing
func ParseExpressionCached(whereClause string, logger *zap.SugaredLogger) (Expression, error) {
	if whereClause == "" {
		return nil, nil
	}

	cache := GlobalExpressionCache()

	// Check cache first
	if expr, _, found := cache.Get(whereClause); found {
		return expr, nil
	}

	// Cache miss - parse the expression
	tokenizer := NewTokenizer(whereClause)
	tokens, err := tokenizer.Tokenize()
	if err != nil {
		return nil, err
	}

	parser := NewExpressionParser(tokens, logger)
	expr, err := parser.Parse()
	if err != nil {
		return nil, err
	}

	// Store in cache
	cache.Put(whereClause, expr, tokens)

	return expr, nil
}
