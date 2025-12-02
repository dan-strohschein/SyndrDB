/*
SUBQUERY EXECUTION NODE

This file implements the SubqueryNode execution node for Tier 1 subquery support.
It handles IN, EXISTS, and NOT IN subqueries with automatic strategy selection.

ARCHITECTURE:
The SubqueryNode wraps a SubqueryExecutor and integrates into the execution tree.
It evaluates the subquery once during execution and caches the result for use
by the parent WHERE clause filter.

DESIGN PRINCIPLES:
- Single Responsibility: Only responsible for subquery execution and result caching
- Open/Closed: Implements ExecutionNode interface without modifying existing code
- Strategy Pattern: Delegates strategy selection to SubqueryExecutor

EXECUTION MODEL:
1. Execute subquery using SubqueryExecutor
2. Materialize results using selected strategy (InList/HashJoin)
3. Cache materialized result for parent filter evaluation
4. Track metrics (execution time, strategy used, row count)

TIER 1 LIMITATIONS:
- Uncorrelated subqueries only (inner query does not reference outer query)
- Single-column subqueries (IN/EXISTS on single field)
- Maximum nesting depth of 3 levels

This node is part of Tier 1 subquery implementation.
*/

package planner

import (
	"context"
	"fmt"
	"syndrdb/src/internal/domain/models"
	"syndrdb/src/internal/query/planner/subquery"
	"syndrdb/src/internal/syndrQL"

	"go.uber.org/zap"
)

// SubqueryNode implements subquery execution for IN/EXISTS/NOT IN clauses
type SubqueryNode struct {
	// Inner query to execute
	InnerQuery *syndrQL.SelectStatement

	// Subquery type (IN, EXISTS, NOT IN)
	SubqueryType syndrQL.SubqueryType

	// Database context for execution
	Database *models.Database

	// Executor for running the subquery
	Executor subquery.SubqueryExecutor

	// Cached result after execution
	cachedResult *subquery.SubqueryResult

	// Cost estimation
	Cost          float64
	EstimatedRows int

	// Logger for debugging
	Logger *zap.SugaredLogger
}

// NewSubqueryNode creates a new subquery execution node
func NewSubqueryNode(
	innerQuery *syndrQL.SelectStatement,
	subqueryType syndrQL.SubqueryType,
	database *models.Database,
	executor subquery.SubqueryExecutor,
	logger *zap.SugaredLogger,
) *SubqueryNode {

	// Estimate row count for cost calculation
	estimatedRows, err := executor.EstimateRowCount(innerQuery, database)
	if err != nil {
		logger.Warnf("Failed to estimate subquery row count: %v", err)
		estimatedRows = 100 // Default estimate
	}

	// Calculate cost based on strategy
	strategy := executor.SelectStrategy(estimatedRows)
	cost := calculateSubqueryCost(strategy, estimatedRows)

	node := &SubqueryNode{
		InnerQuery:    innerQuery,
		SubqueryType:  subqueryType,
		Database:      database,
		Executor:      executor,
		Cost:          cost,
		EstimatedRows: estimatedRows,
		Logger:        logger,
	}

	logger.Debugf("Created SubqueryNode: Type=%s, Strategy=%s, EstimatedRows=%d, Cost=%.4f",
		subqueryType, strategy, estimatedRows, cost)

	return node
}

// Execute runs the subquery and caches the materialized result
// Note: SubqueryNode doesn't return documents directly - it's used for
// side effects (caching subquery result) during WHERE clause evaluation
func (n *SubqueryNode) Execute(ctx context.Context) (map[string]*models.Document, error) {
	// Execute subquery if not already cached
	if n.cachedResult == nil {
		result, err := n.Executor.Execute(ctx, n.InnerQuery, n.Database)
		if err != nil {
			return nil, fmt.Errorf("subquery execution failed: %w", err)
		}

		n.cachedResult = result
		n.Logger.Debugf("Subquery executed: Type=%s, Strategy=%s, RowCount=%d, MemoryBytes=%d, ContainsNull=%v",
			n.SubqueryType, result.Strategy, result.RowCount, result.MemoryBytes, result.ContainsNull)
	}

	// SubqueryNode doesn't produce documents itself - it's evaluated during WHERE filtering
	// Return empty map to satisfy ExecutionNode interface
	return make(map[string]*models.Document), nil
}

// GetCost returns the estimated execution cost
func (n *SubqueryNode) GetCost() float64 {
	return n.Cost
}

// GetEstimatedRows returns the estimated number of rows
func (n *SubqueryNode) GetEstimatedRows() int {
	return n.EstimatedRows
}

// EstimateMemoryUsage returns estimated memory consumption
func (n *SubqueryNode) EstimateMemoryUsage() int64 {
	if n.cachedResult != nil {
		return n.cachedResult.MemoryBytes
	}

	// Estimate based on row count and strategy
	// Conservative estimate: 64 bytes per row
	return int64(n.EstimatedRows * 64)
}

// GetCachedResult returns the cached subquery result
// Used by parent FilterNode during WHERE clause evaluation
func (n *SubqueryNode) GetCachedResult() *subquery.SubqueryResult {
	return n.cachedResult
}

// calculateSubqueryCost estimates execution cost based on strategy and row count
func calculateSubqueryCost(strategy subquery.MaterializationStrategy, estimatedRows int) float64 {
	baseCost := 10.0 // Base cost for subquery setup

	switch strategy {
	case subquery.STRATEGY_INLIST:
		// InList: Scan all rows + materialize array
		// Cost: O(N) scan + array allocation
		return baseCost + (float64(estimatedRows) * 0.1)

	case subquery.STRATEGY_HASHJOIN:
		// HashJoin: Scan all rows + build hash table
		// Cost: O(N) scan + hash table construction
		return baseCost + (float64(estimatedRows) * 0.2)

	case subquery.STRATEGY_INDEXED_LOOKUP:
		// IndexedLookup: Multiple index scans (Tier 3 feature)
		// Cost: O(M * log(N)) where M=outer rows, N=inner rows
		// For now, estimate similar to HashJoin
		return baseCost + (float64(estimatedRows) * 0.15)

	default:
		// Unknown strategy - use conservative estimate
		return baseCost + (float64(estimatedRows) * 0.2)
	}
}
