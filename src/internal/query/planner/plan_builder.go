/*
PLAN BUILDER - PHASE 3

This file implements the PlanBuilder component that composes execution plans
by wrapping base execution trees with Phase 2 nodes (sorting, limiting, grouping).

ARCHITECTURE:
The PlanBuilder follows the Composite pattern, building execution trees by
wrapping nodes with additional behavior (decorators).

DESIGN PRINCIPLES:
- Single Responsibility: Compose execution trees from nodes
- Open/Closed: Can add new node types without modifying existing composition
- Dependency Inversion: Works with ExecutionNode interface

COMPOSITION LOGIC:
- Base Tree → Add AggregationNode (if GROUP BY) → Add SortNode (if ORDER BY)
  → Add LimitNode (if TOP/LIMIT) → Add HierarchicalTransformNode (if WITH RELATIONSHIP)

This is part of Phase 3 of the unified query system implementation.
*/

package planner

import (
	"fmt"
	"syndrdb/src/internal/domain/models"
	"syndrdb/src/internal/query/queryparser"

	"go.uber.org/zap"
)

// PlanBuilder composes execution plans by wrapping base trees with Phase 2 nodes
// PHASE 3: Plan Builder Component
type PlanBuilder struct {
	// bundleService for relationship metadata
	bundleService BundleServiceInterface

	// logger for debugging
	logger *zap.SugaredLogger
}

// NewPlanBuilder creates a new plan builder
// PHASE 3: Factory function for PlanBuilder creation
//
// Parameters:
//   - bundleService: Service for bundle/relationship metadata
//   - logger: Logger for debugging
//
// Returns:
//   - *PlanBuilder: Configured plan builder
func NewPlanBuilder(
	bundleService BundleServiceInterface,
	logger *zap.SugaredLogger,
) *PlanBuilder {
	return &PlanBuilder{
		bundleService: bundleService,
		logger:        logger,
	}
}

// BuildPlan composes a complete execution plan from base tree and query clauses
// PHASE 3: Main composition method
//
// Composition order (bottom to top):
// 1. Base tree (from router)
// 2. AggregationNode (if GROUP BY)
// 3. SortNode (if ORDER BY)
// 4. LimitNode (if TOP/LIMIT/OFFSET)
// 5. HierarchicalTransformNode (if WITH RELATIONSHIP)
//
// Parameters:
//   - baseTree: Base execution tree from router
//   - query: UnifiedSelectQuery with clauses
//   - database: Database context for metadata
//
// Returns:
//   - ExecutionNode: Complete execution tree
//   - error: Any error during composition
func (pb *PlanBuilder) BuildPlan(
	baseTree ExecutionNode,
	query *queryparser.UnifiedSelectQuery,
	database *models.Database,
) (ExecutionNode, error) {

	pb.logger.Debug("Building execution plan from base tree")

	currentTree := baseTree

	// Add aggregation if GROUP BY present
	if query.HasGroupBy() {
		aggNode, err := pb.addAggregationNode(currentTree, query, database)
		if err != nil {
			return nil, fmt.Errorf("failed to add aggregation: %w", err)
		}
		currentTree = aggNode
		pb.logger.Debug("Added AggregationNode to tree")
	}

	// Add DISTINCT deduplication if SELECT DISTINCT present
	// Must come after aggregation but before sorting for correctness
	if query.IsDistinct {
		distinctNode, err := pb.addDistinctNode(currentTree, query, database)
		if err != nil {
			return nil, fmt.Errorf("failed to add distinct: %w", err)
		}
		currentTree = distinctNode
		pb.logger.Debug("Added DistinctNode to tree")
	}

	// Add sorting if ORDER BY present
	if query.HasOrderBy() {
		sortNode, err := pb.addSortNode(currentTree, query)
		if err != nil {
			return nil, fmt.Errorf("failed to add sort: %w", err)
		}
		currentTree = sortNode
		pb.logger.Debug("Added SortNode to tree")
	}

	// Add limiting if TOP/LIMIT/OFFSET present
	if query.HasLimit() || query.Offset > 0 {
		limitNode, err := pb.addLimitNode(currentTree, query)
		if err != nil {
			return nil, fmt.Errorf("failed to add limit: %w", err)
		}
		currentTree = limitNode
		pb.logger.Debug("Added LimitNode to tree")
	}

	// Add hierarchical transform if WITH RELATIONSHIP present
	if query.RelationshipName != "" {
		transformNode, err := pb.addHierarchicalTransformNode(currentTree, query, database)
		if err != nil {
			return nil, fmt.Errorf("failed to add hierarchical transform: %w", err)
		}
		currentTree = transformNode
		pb.logger.Debug("Added HierarchicalTransformNode to tree")
	}

	pb.logger.Debugf("Plan building complete: Final tree type=%T", currentTree)
	return currentTree, nil
}

// addAggregationNode wraps tree with aggregation
// PHASE 3: Aggregation composition
func (pb *PlanBuilder) addAggregationNode(
	child ExecutionNode,
	query *queryparser.UnifiedSelectQuery,
	database *models.Database,
) (ExecutionNode, error) {

	// Create aggregation node with GROUP BY clause
	// Note: ORDER BY is handled separately by SortNode (added after aggregation)
	aggNode := NewAggregationNode(
		child,
		query.GroupBy,
		query.AggregateFields,
		query.HavingExpression,
		nil, // OrderBy handled by SortNode
		pb.logger,
	)

	return aggNode, nil
}

// addDistinctNode wraps tree with DISTINCT deduplication
// PHASE 3: Deduplication composition
func (pb *PlanBuilder) addDistinctNode(
	child ExecutionNode,
	query *queryparser.UnifiedSelectQuery,
	database *models.Database,
) (ExecutionNode, error) {

	// Get bundle for index-based optimization
	bundle, err := pb.bundleService.GetBundleByName(database, query.FromBundle)
	if err != nil {
		return nil, fmt.Errorf("failed to get bundle '%s': %w", query.FromBundle, err)
	}

	// Extract DISTINCT fields from SelectFields
	// If SelectFields is empty, extractDistinctFields will handle SELECT DISTINCT *
	distinctFields := query.SelectFields

	// Calculate memory limit: use conservative 256MB default
	// TODO: Could make this configurable via query planner settings
	memoryLimitMB := 256                                             // 256MB default
	memoryLimit := int64(float64(memoryLimitMB) * 0.8 * 1024 * 1024) // Convert MB to bytes, use 80%

	// Create DISTINCT node
	// Note: BloomFilterEnabled parameter removed - always enabled internally
	distinctNode := NewDistinctNode(
		child,
		distinctFields,
		memoryLimit,
		bundle,
		pb.logger,
	)

	return distinctNode, nil
}

// addSortNode wraps tree with sorting
// PHASE 3: Sorting composition
func (pb *PlanBuilder) addSortNode(
	child ExecutionNode,
	query *queryparser.UnifiedSelectQuery,
) (ExecutionNode, error) {

	// Create sort node with ORDER BY clause
	sortNode := NewSortNode(
		child,
		query.OrderBy,
		pb.logger,
	)

	return sortNode, nil
}

// addLimitNode wraps tree with limiting
// PHASE 3: Limiting composition
func (pb *PlanBuilder) addLimitNode(
	child ExecutionNode,
	query *queryparser.UnifiedSelectQuery,
) (ExecutionNode, error) {

	// Create limit node using GetEffectiveLimit() to handle both TOP and LIMIT clauses
	limitNode := NewLimitNode(
		child,
		query.GetEffectiveLimit(),
		query.Offset,
		pb.logger,
	)

	return limitNode, nil
}

// addHierarchicalTransformNode wraps tree with relationship transform
// PHASE 3: Hierarchical transform composition
func (pb *PlanBuilder) addHierarchicalTransformNode(
	child ExecutionNode,
	query *queryparser.UnifiedSelectQuery,
	database *models.Database,
) (ExecutionNode, error) {

	// Create hierarchical transform node
	// Signature: (child, relationshipName, joinClauses, fromBundle, logger)
	transformNode := NewHierarchicalTransformNode(
		child,
		query.RelationshipName,
		query.JoinClauses,
		query.FromBundle,
		pb.logger,
	)

	return transformNode, nil
}
