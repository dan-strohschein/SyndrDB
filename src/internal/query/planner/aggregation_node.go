/*
AGGREGATION EXECUTION NODE - PHASE 2

This file implements the AggregationNode execution node for the unified query system.
It provides GROUP BY and aggregate function (COUNT, SUM, AVG, MIN, MAX) functionality
by wrapping the existing GroupByExecutor component.

ARCHITECTURE:
The AggregationNode follows the Adapter pattern, delegating aggregation logic to the
well-tested GroupByExecutor component while implementing the ExecutionNode interface.

DESIGN PRINCIPLES:
- Single Responsibility: Only responsible for coordinating aggregation execution in the query plan
- Open/Closed: Extends ExecutionNode without modifying existing code
- Dependency Inversion: Depends on ExecutionNode abstraction and GroupByExecutor

EXECUTION MODEL:
1. Pull documents from child node
2. Convert UnifiedSelectQuery to SelectQueryWithGroupBy format
3. Delegate to GroupByExecutor for actual aggregation
4. Return aggregated documents

SUPPORTED AGGREGATES:
- COUNT(*), COUNT(field)
- SUM(field)
- AVG(field)
- MIN(field), MAX(field)

EXECUTION STRATEGIES:
- Hash Aggregate: Fast in-memory grouping
- Sort + GroupAggregate: Memory-efficient for large datasets

PERFORMANCE:
- Hash Aggregate: O(n) time, O(distinct_groups) space
- Sort + GroupAggregate: O(n log n) time, O(1) space (with disk spill)

COST ESTIMATION:
Cost = ChildCost + (n * aggregation_cost_factor)
Where aggregation_cost_factor depends on strategy (0.01 for hash, 0.02 for sort)

This node is part of Phase 2 of the unified query system implementation.
*/

package planner

import (
	"fmt"
	"syndrdb/src/internal/domain/models"
	"syndrdb/src/internal/query/executor"
	"syndrdb/src/internal/query/queryparser"

	"go.uber.org/zap"
)

// AggregationNode implements GROUP BY and aggregate function execution
// PHASE 2: Execution Nodes - Aggregation Operation
type AggregationNode struct {
	// Child node providing input documents
	Child ExecutionNode

	// GroupBy clause specifying grouping fields
	GroupBy *queryparser.GroupByClause

	// AggregateFields specifies aggregate functions to compute
	AggregateFields []queryparser.AggregateFunction

	// HavingClause filters groups after aggregation
	HavingClause *queryparser.HavingClause

	// OrderBy clause for result ordering (optional)
	OrderBy *queryparser.OrderByClause

	// Cost is the estimated execution cost
	Cost float64

	// EstimatedRows is the expected number of output groups
	EstimatedRows int

	// Logger for debugging and monitoring
	Logger *zap.SugaredLogger

	// executor delegates to existing GroupByExecutor implementation
	executor *executor.GroupByExecutor

	// executionStrategy determines aggregation algorithm
	executionStrategy queryparser.GroupByStrategy
}

// NewAggregationNode creates a new aggregation execution node
// PHASE 2: Factory function for AggregationNode creation
//
// Parameters:
//   - child: ExecutionNode providing input documents
//   - groupBy: GROUP BY clause specification
//   - aggregateFields: Aggregate functions to compute
//   - havingClause: HAVING clause for group filtering (can be nil)
//   - orderBy: ORDER BY clause for result sorting (can be nil)
//   - logger: Logger for debugging
//
// Returns:
//   - *AggregationNode: Configured aggregation execution node
func NewAggregationNode(
	child ExecutionNode,
	groupBy *queryparser.GroupByClause,
	aggregateFields []queryparser.AggregateFunction,
	havingClause *queryparser.HavingClause,
	orderBy *queryparser.OrderByClause,
	logger *zap.SugaredLogger,
) *AggregationNode {

	// Determine execution strategy based on input size
	childRows := child.GetEstimatedRows()
	var strategy queryparser.GroupByStrategy
	var costFactor float64

	// Hash aggregate is faster for smaller datasets or when memory is available
	// Sort+GroupAggregate is better for very large datasets that may need disk spilling
	if childRows < 10000 {
		strategy = queryparser.HashAggregate
		costFactor = 0.01 // Hash aggregate is O(n)
	} else {
		strategy = queryparser.SortGroupAggregate
		costFactor = 0.02 // Sort+GroupAggregate is O(n log n)
	}

	// Estimate number of output groups (assume 10% uniqueness for now)
	// This is a heuristic - actual cardinality depends on data distribution
	estimatedGroups := childRows / 10
	if estimatedGroups < 1 {
		estimatedGroups = 1
	}
	if estimatedGroups > childRows {
		estimatedGroups = childRows
	}

	node := &AggregationNode{
		Child:             child,
		GroupBy:           groupBy,
		AggregateFields:   aggregateFields,
		HavingClause:      havingClause,
		OrderBy:           orderBy,
		Logger:            logger,
		EstimatedRows:     estimatedGroups,
		executionStrategy: strategy,
	}

	// Calculate cost: child cost + aggregation processing cost
	aggregationCost := float64(childRows) * costFactor
	node.Cost = child.GetCost() + aggregationCost

	logger.Debugf("Created AggregationNode: Strategy=%s, EstimatedGroups=%d, Cost=%.4f (child=%.4f, aggregation=%.4f)",
		strategy.String(), estimatedGroups, node.Cost, child.GetCost(), aggregationCost)

	return node
}

// Execute performs the aggregation operation
// PHASE 2: Main execution method for AggregationNode
//
// Execution flow:
// 1. Execute child node to get input documents
// 2. Convert query components to SelectQueryWithGroupBy format
// 3. Create GroupByExecutor with proper configuration
// 4. Delegate to GroupByExecutor for aggregation
// 5. Return aggregated results
//
// Returns:
//   - map[string]*models.Document: Aggregated group documents
//   - error: Any error during execution
func (n *AggregationNode) Execute() (map[string]*models.Document, error) {
	n.Logger.Infof("Executing AggregationNode with %d GROUP BY fields, %d aggregates",
		len(n.GroupBy.Fields), len(n.AggregateFields))

	// Execute child node to get input documents
	documents, err := n.Child.Execute()
	if err != nil {
		return nil, fmt.Errorf("AggregationNode: child execution failed: %w", err)
	}

	n.Logger.Debugf("AggregationNode received %d documents from child", len(documents))

	// Handle empty result set
	if len(documents) == 0 {
		n.Logger.Debug("AggregationNode: no documents to aggregate, returning empty result")
		return documents, nil
	}

	// Build SelectQueryWithGroupBy structure for GroupByExecutor
	// This is an adapter to convert unified query format to legacy format
	groupByQuery := n.buildGroupByQuery()

	// Create GroupByExecutor (reuses existing, well-tested component)
	n.executor = executor.NewGroupByExecutor(groupByQuery, n.Logger)

	// Delegate to GroupByExecutor for actual aggregation
	resultDocs, err := n.executor.Execute(documents)
	if err != nil {
		return nil, fmt.Errorf("AggregationNode: aggregation execution failed: %w", err)
	}

	n.Logger.Infof("AggregationNode completed: produced %d groups from %d documents",
		len(resultDocs), len(documents))

	return resultDocs, nil
}

// buildGroupByQuery converts unified query components to SelectQueryWithGroupBy format
// PHASE 2: Adapter method for query structure conversion
//
// This method bridges the unified query system with the existing GroupByExecutor.
// It constructs a SelectQueryWithGroupBy from the node's configuration.
//
// Returns:
//   - *queryparser.SelectQueryWithGroupBy: Query structure for GroupByExecutor
func (n *AggregationNode) buildGroupByQuery() *queryparser.SelectQueryWithGroupBy {
	query := &queryparser.SelectQueryWithGroupBy{
		FromBundle:        "", // Not used by executor
		GroupBy:           n.GroupBy,
		AggregateFields:   n.AggregateFields,
		HavingClause:      n.HavingClause,
		OrderBy:           n.OrderBy,
		WhereClause:       "", // WHERE is already applied by child node
		ExecutionStrategy: n.executionStrategy,
	}

	return query
}

// GetCost returns the estimated execution cost
// PHASE 2: Cost accessor for query planning
func (n *AggregationNode) GetCost() float64 {
	return n.Cost
}

// GetEstimatedRows returns the estimated number of output groups
// PHASE 2: Cardinality accessor for query planning
func (n *AggregationNode) GetEstimatedRows() int {
	return n.EstimatedRows
}

// GetExecutionStrategy returns the chosen aggregation strategy
// PHASE 2: Helper method for query analysis and debugging
func (n *AggregationNode) GetExecutionStrategy() queryparser.GroupByStrategy {
	return n.executionStrategy
}

// HasHavingClause returns true if a HAVING clause is specified
// PHASE 2: Helper method for query analysis
func (n *AggregationNode) HasHavingClause() bool {
	return n.HavingClause != nil && n.HavingClause.Condition != ""
}
