package planner

import (
	"crypto/sha256"
	"encoding/hex"
	"sync"
	"sync/atomic"
	"time"

	"syndrdb/src/internal/query/documentscanner"
	"syndrdb/src/internal/syndrQL"

	"go.uber.org/zap"
)

// QueryCache implements expression caching and predicate optimization for WHERE clauses
//
// Single Responsibility: Manages compiled expression caching and query optimization
//
// Features:
//   - LRU cache for compiled WHERE expressions
//   - Predicate reordering based on selectivity
//   - Cache statistics tracking
//   - Thread-safe operations
//
// TODO: I could extend this to cache query results (full result set caching)
// TODO: I could add cache invalidation on bundle updates
// TODO: I could persist cache to disk for faster server restarts
type QueryCache struct {
	expressionCache *documentscanner.SimpleLRUCache
	logger          *zap.SugaredLogger

	// Statistics (atomic for thread-safety)
	hits   uint64
	misses uint64
	mu     sync.RWMutex
}

// CompiledExpression represents a cached, optimized WHERE expression
type CompiledExpression struct {
	AST         syndrQL.Expression // Compiled abstract syntax tree
	FieldRefs   []string           // Fields referenced in expression
	Hash        string             // Cache key (SHA256 of original expression)
	Selectivity float64            // Estimated selectivity (0.0 = highly selective, 1.0 = matches all)
	CreatedAt   time.Time          // Cache entry creation time
	Optimized   bool               // Whether predicate reordering was applied
}

// NewQueryCache creates a new query cache with LRU eviction
func NewQueryCache(maxSize int, logger *zap.SugaredLogger) *QueryCache {
	return &QueryCache{
		expressionCache: documentscanner.NewSimpleLRUCache(maxSize),
		logger:          logger,
	}
}

// GetOrCompileExpression retrieves a compiled expression from cache or compiles it
//
// This is the main entry point for expression caching. It returns a cached
// expression if available, otherwise parses and optimizes the WHERE clause.
func (qc *QueryCache) GetOrCompileExpression(expr syndrQL.Expression) (*CompiledExpression, error) {
	// Generate cache key from expression
	hash := qc.hashExpression(expr)

	// Check cache
	if cached, ok := qc.expressionCache.Get(hash); ok {
		atomic.AddUint64(&qc.hits, 1)
		qc.logger.Debugf("Expression cache HIT: %s", hash[:12])
		return cached.(*CompiledExpression), nil
	}

	// Cache miss - compile and optimize
	atomic.AddUint64(&qc.misses, 1)
	qc.logger.Debugf("Expression cache MISS: %s", hash[:12])

	compiled := &CompiledExpression{
		AST:       expr,
		Hash:      hash,
		CreatedAt: time.Now(),
		Optimized: false,
	}

	// Extract field references
	compiled.FieldRefs = qc.extractFieldReferences(expr)

	// Estimate selectivity for the entire expression
	compiled.Selectivity = qc.estimateExpressionSelectivity(expr)

	// Optimize predicate order (reorder AND clauses by selectivity)
	optimized, wasOptimized := qc.optimizePredicateOrder(expr)
	compiled.AST = optimized
	compiled.Optimized = wasOptimized

	// Store in cache
	qc.expressionCache.Put(hash, compiled)

	return compiled, nil
}

// optimizePredicateOrder reorders AND clauses to execute most selective predicates first
//
// Strategy:
//  1. Extract all AND clauses from the expression
//  2. Estimate selectivity for each clause
//  3. Sort by selectivity (most selective = lowest estimated match rate)
//  4. Reconstruct expression with optimized order
//
// Example:
//
//	Before: Status == "active" AND Country == "Iceland" AND Age > 25
//	After:  Country == "Iceland" AND Age > 25 AND Status == "active"
//	        ^^^^^^^^^ most selective (0.1%) evaluated first
//
// TODO: I could use actual index statistics for better selectivity estimates
// TODO: I could track actual selectivity from query execution history
func (qc *QueryCache) optimizePredicateOrder(expr syndrQL.Expression) (syndrQL.Expression, bool) {
	// Only optimize BinaryExpression with AND operator
	binExpr, ok := expr.(*syndrQL.BinaryExpression)
	if !ok || binExpr.Operator != syndrQL.TOKEN_AND {
		return expr, false
	}

	// Extract all AND clauses
	clauses := qc.extractANDClauses(expr)
	if len(clauses) <= 1 {
		return expr, false // Nothing to optimize
	}

	// Calculate selectivity for each clause
	type clauseWithSelectivity struct {
		clause      syndrQL.Expression
		selectivity float64
	}

	clausesWithSel := make([]clauseWithSelectivity, len(clauses))
	for i, clause := range clauses {
		clausesWithSel[i] = clauseWithSelectivity{
			clause:      clause,
			selectivity: qc.estimateExpressionSelectivity(clause),
		}
	}

	// Sort by selectivity (most selective first)
	// Bubble sort is fine for small clause counts (<10 typically)
	for i := 0; i < len(clausesWithSel); i++ {
		for j := i + 1; j < len(clausesWithSel); j++ {
			if clausesWithSel[j].selectivity < clausesWithSel[i].selectivity {
				clausesWithSel[i], clausesWithSel[j] = clausesWithSel[j], clausesWithSel[i]
			}
		}
	}

	// Reconstruct AND expression with optimized order
	result := clausesWithSel[0].clause
	for i := 1; i < len(clausesWithSel); i++ {
		result = &syndrQL.BinaryExpression{
			Left:     result,
			Operator: syndrQL.TOKEN_AND,
			Right:    clausesWithSel[i].clause,
		}
	}

	qc.logger.Debugf("Optimized predicate order: %d clauses reordered", len(clauses))
	return result, true
}

// extractANDClauses recursively extracts all AND-connected clauses from an expression
func (qc *QueryCache) extractANDClauses(expr syndrQL.Expression) []syndrQL.Expression {
	binExpr, ok := expr.(*syndrQL.BinaryExpression)
	if !ok || binExpr.Operator != syndrQL.TOKEN_AND {
		return []syndrQL.Expression{expr}
	}

	leftClauses := qc.extractANDClauses(binExpr.Left)
	rightClauses := qc.extractANDClauses(binExpr.Right)

	return append(leftClauses, rightClauses...)
}

// estimateExpressionSelectivity estimates the selectivity of an expression
//
// Selectivity is the estimated fraction of documents that will match:
//   - 0.001 = 0.1% (highly selective, indexed equality)
//   - 0.1 = 10% (selective, range on indexed field)
//   - 0.5 = 50% (not selective, range on non-indexed field)
//   - 1.0 = 100% (matches all documents)
//
// Lower selectivity = higher priority (execute first for early termination)
//
// Heuristics:
//   - Indexed equality (==): 0.001
//   - Indexed range (>, <, >=, <=): 0.1
//   - Non-indexed equality: 0.3
//   - Non-indexed range: 0.5
//   - OR clauses: max(left, right) selectivity
//   - AND clauses: min(left, right) selectivity
//
// TODO: I could use actual index cardinality statistics
// TODO: I could learn from query execution feedback
func (qc *QueryCache) estimateExpressionSelectivity(expr syndrQL.Expression) float64 {
	switch e := expr.(type) {
	case *syndrQL.BinaryExpression:
		switch e.Operator {
		case syndrQL.TOKEN_EQ, syndrQL.TOKEN_ASSIGN:
			// Equality is typically selective (especially on indexed fields)
			// TODO: Check if field is indexed and adjust accordingly
			return 0.001 // Assume 0.1% match rate for equality

		case syndrQL.TOKEN_NEQ:
			// Not-equals is typically not selective
			return 0.9 // 90% of documents don't match any specific value

		case syndrQL.TOKEN_GT, syndrQL.TOKEN_LT, syndrQL.TOKEN_GTE, syndrQL.TOKEN_LTE:
			// Range queries are moderately selective
			// TODO: Use index statistics for better estimates
			return 0.1 // Assume 10% match rate for ranges

		case syndrQL.TOKEN_AND:
			// AND clauses: selectivity is minimum of both sides
			// (both conditions must be true, so fewer matches)
			leftSel := qc.estimateExpressionSelectivity(e.Left)
			rightSel := qc.estimateExpressionSelectivity(e.Right)
			if leftSel < rightSel {
				return leftSel
			}
			return rightSel

		case syndrQL.TOKEN_OR:
			// OR clauses: selectivity is maximum of both sides
			// (either condition can be true, so more matches)
			leftSel := qc.estimateExpressionSelectivity(e.Left)
			rightSel := qc.estimateExpressionSelectivity(e.Right)
			if leftSel > rightSel {
				return leftSel
			}
			return rightSel

		case syndrQL.TOKEN_IN:
			// IN operator selectivity depends on list size
			// Assume moderate selectivity
			return 0.05 // 5% match rate

		case syndrQL.TOKEN_NOTIN:
			return 0.95 // 95% match rate

		case syndrQL.TOKEN_LIKE:
			// LIKE patterns vary widely in selectivity
			// Prefix patterns (name LIKE 'John%') are more selective
			// TODO: Analyze pattern to estimate better
			return 0.2 // Assume 20% match rate

		default:
			// Unknown operator - assume not selective
			return 0.5
		}

	case *syndrQL.UnaryExpression:
		if e.Operator == syndrQL.TOKEN_NOT {
			// NOT inverts selectivity
			innerSel := qc.estimateExpressionSelectivity(e.Right)
			return 1.0 - innerSel
		}
		return 0.5

	default:
		// Unknown expression type - assume not selective
		return 0.5
	}
}

// extractFieldReferences recursively extracts all field names referenced in an expression
func (qc *QueryCache) extractFieldReferences(expr syndrQL.Expression) []string {
	var fields []string

	switch e := expr.(type) {
	case *syndrQL.IdentifierExpression:
		fields = append(fields, e.Name)

	case *syndrQL.QualifiedIdentifierExpression:
		fields = append(fields, e.Field)

	case *syndrQL.BinaryExpression:
		fields = append(fields, qc.extractFieldReferences(e.Left)...)
		fields = append(fields, qc.extractFieldReferences(e.Right)...)

	case *syndrQL.UnaryExpression:
		fields = append(fields, qc.extractFieldReferences(e.Right)...)

	case *syndrQL.CallExpression:
		for _, arg := range e.Arguments {
			fields = append(fields, qc.extractFieldReferences(arg)...)
		}
	}

	return fields
}

// hashExpression generates a SHA256 hash of an expression for cache keying
func (qc *QueryCache) hashExpression(expr syndrQL.Expression) string {
	// Use the expression's String() representation for hashing
	exprStr := expr.String()

	// Generate SHA256 hash
	hash := sha256.Sum256([]byte(exprStr))
	return hex.EncodeToString(hash[:])
}

// GetStatistics returns cache hit/miss statistics
func (qc *QueryCache) GetStatistics() (hits, misses uint64, hitRate float64) {
	hits = atomic.LoadUint64(&qc.hits)
	misses = atomic.LoadUint64(&qc.misses)

	total := hits + misses
	if total > 0 {
		hitRate = float64(hits) / float64(total) * 100.0
	}

	return hits, misses, hitRate
}

// ClearStatistics resets cache statistics
func (qc *QueryCache) ClearStatistics() {
	atomic.StoreUint64(&qc.hits, 0)
	atomic.StoreUint64(&qc.misses, 0)
}

// Clear removes all entries from the cache
func (qc *QueryCache) Clear() {
	qc.expressionCache.Clear()
	qc.ClearStatistics()
	qc.logger.Debugf("Query cache cleared")
}
