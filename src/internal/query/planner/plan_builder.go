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
	"strings"
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

	// DEBUG: Log query analysis
	pb.logger.Debugf("PlanBuilder.BuildPlan ANALYSIS:")
	pb.logger.Debugf("  - HasGroupBy(): %v", query.HasGroupBy())
	pb.logger.Debugf("  - IsAggregateQuery(): %v", query.IsAggregateQuery())
	pb.logger.Debugf("  - IsAggregateOnly: %v", query.IsAggregateOnly)
	pb.logger.Debugf("  - AggregateFields count: %d", len(query.AggregateFields))
	if len(query.AggregateFields) > 0 {
		for i, aggField := range query.AggregateFields {
			pb.logger.Debugf("    [%d] Function=%s, Field=%s, Alias=%s",
				i, aggField.Function, aggField.Field, aggField.Alias)
		}
	}
	pb.logger.Debugf("  - GroupBy clause: %v", query.GroupBy)

	currentTree := baseTree

	// Add aggregation if GROUP BY present OR if aggregate-only query (SELECT COUNT(*))
	if query.HasGroupBy() || query.IsAggregateOnly {
		aggNode, err := pb.addAggregationNode(currentTree, query, database)
		if err != nil {
			return nil, fmt.Errorf("failed to add aggregation: %w", err)
		}
		currentTree = aggNode
		if query.HasGroupBy() {
			pb.logger.Debug("Added AggregationNode for GROUP BY query")
		} else {
			pb.logger.Debug("Added AggregationNode for aggregate-only query")
		}
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

	// OPTIMIZATION: Early termination for simple LIMIT-only queries (no WHERE, GROUP BY, ORDER BY)
	// If query has LIMIT but no WHERE, GROUP BY, or ORDER BY, set MaxDocuments on FullScanNode
	// to stop loading documents early. Skip SortNode (no ORDER BY), but keep LimitNode if OFFSET present.
	if query.HasLimit() && !query.HasWhere() && !query.HasGroupBy() && !query.HasOrderBy() && !query.IsDistinct {
		if fullScan, ok := currentTree.(*FullScanNode); ok {
			// Calculate effective limit (LIMIT + OFFSET) for early termination
			effectiveLimit := query.Limit
			if query.Offset > 0 {
				effectiveLimit = query.Offset + query.Limit
			}
			fullScan.MaxDocuments = effectiveLimit
			pb.logger.Debugf("OPTIMIZATION: Simple LIMIT-only query detected. Setting MaxDocuments=%d on FullScanNode (skipping SortNode)", effectiveLimit)

			// If there's OFFSET, we still need LimitNode to skip OFFSET documents
			// Otherwise, return early (skip both SortNode and LimitNode)
			if query.Offset > 0 {
				pb.logger.Debug("Query has OFFSET - will add LimitNode after FullScanNode for OFFSET handling")
				// Continue to add LimitNode below for OFFSET handling
			} else {
				// No OFFSET - skip both SortNode and LimitNode, return early
				return currentTree, nil
			}
		}
	}

	// Add sorting for deterministic ordering, but skip for aggregate-only queries
	// (no GROUP BY) when there's no explicit ORDER BY, as there's nothing to sort
	if !query.IsAggregateOnly || query.HasOrderBy() {
		// OPTIMIZATION: Skip SortNode if base tree is BTreeOrderedScanNode with matching ORDER BY
		// BTreeOrderedScanNode already returns documents in sorted order (ASC or DESC)
		if btreeNode, ok := currentTree.(*BTreeOrderedScanNode); ok && query.HasOrderBy() && query.OrderBy != nil {
			if len(query.OrderBy.Fields) == 1 {
				orderField := query.OrderBy.Fields[0]
				fieldName := strings.Trim(orderField.FieldName, `"'`)
				if dotIdx := strings.LastIndex(fieldName, "."); dotIdx >= 0 {
					fieldName = fieldName[dotIdx+1:]
				}
				fieldName = strings.Trim(fieldName, `"'`)

				isDescending := orderField.Direction == queryparser.SortDesc
				directionStr := "ASC"
				if isDescending {
					directionStr = "DESC"
				}

				// Check if BTreeOrderedScanNode matches the ORDER BY requirements
				if btreeNode.OrderedByFieldName == fieldName && btreeNode.Descending == isDescending {
					pb.logger.Debugf("SKIP SortNode: BTreeOrderedScanNode already provides ORDER BY %s %s", fieldName, directionStr)

					// Also skip LimitNode if BTreeOrderedScanNode already applied the limit
					if btreeNode.Limit > 0 && (query.HasLimit() || query.Offset > 0) {
						pb.logger.Debugf("SKIP LimitNode: BTreeOrderedScanNode already applied limit=%d", btreeNode.Limit)

						// Handle OFFSET by wrapping with LimitNode that only applies OFFSET
						if query.Offset > 0 {
							// Need LimitNode to skip OFFSET documents from the pre-limited result
							limitNode := NewLimitNode(
								currentTree,
								query.GetEffectiveLimit(),
								query.Offset,
								pb.logger,
							)
							currentTree = limitNode
							pb.logger.Debug("Added LimitNode for OFFSET handling only")
						}

						// Skip to hierarchical transform
						goto addHierarchicalTransform
					}

					// No limit was applied by BTreeOrderedScanNode, continue to add LimitNode if needed
					goto addLimitNode
				}
			}
		}

		// OPTIMIZATION: Skip SortNode if JoinExecutionNode uses streaming Top-N (already sorted)
		// JoinExecutionNode with OrderBy + Limit uses heap-based merge producing sorted output
		if pb.checkJoinProvidesSortedOutput(currentTree, query) {
			pb.logger.Debugf("SKIP SortNode: JoinExecutionNode already provides sorted output via streaming Top-N")
			goto addLimitNode
		}

		// PROJECTION PUSHDOWN: Extract ORDER BY field names and pass to FullScanNode for optimization
		// This allows the storage layer to deserialize only the sort fields (e.g., "name") instead of all fields
		// For query: SELECT DocumentID, name FROM products ORDER BY name
		// We only need to deserialize "name" field during document loading, saving ~80-90% deserialization overhead
		if fullScan, ok := currentTree.(*FullScanNode); ok && query.HasOrderBy() && query.OrderBy != nil {
			projectionFields := make([]string, 0, len(query.OrderBy.Fields))
			for _, orderField := range query.OrderBy.Fields {
				// Extract field name from ORDER BY clause (handle qualified names like "products"."name")
				fieldName := orderField.FieldName
				// Remove bundle qualifier if present (e.g., "products"."name" -> "name")
				if dotIdx := strings.LastIndex(fieldName, "."); dotIdx >= 0 {
					fieldName = fieldName[dotIdx+1:]
				}
				// Remove quotes if present (e.g., "name" -> name)
				fieldName = strings.Trim(fieldName, `"'`)
				if fieldName != "" {
					projectionFields = append(projectionFields, fieldName)
				}
			}
			if len(projectionFields) > 0 {
				fullScan.ProjectionFields = projectionFields
				pb.logger.Debugf("PROJECTION PUSHDOWN: Set ProjectionFields=%v on FullScanNode for ORDER BY optimization", projectionFields)
			}
		}

		// Add sorting if:
		// 1. Not an aggregate-only query (has other fields to sort), OR
		// 2. Aggregate-only query but user explicitly specified ORDER BY
		sortNode, err := pb.addSortNode(currentTree, query)
		if err != nil {
			return nil, fmt.Errorf("failed to add sort: %w", err)
		}
		currentTree = sortNode
		if query.HasOrderBy() {
			pb.logger.Debug("Added SortNode with user ORDER BY clause")
		} else {
			pb.logger.Debug("Added SortNode with default CreatedAt ASC ordering")
		}
	} else {
		// Skip sort node for aggregate-only queries without explicit ORDER BY
		pb.logger.Debug("Skipping SortNode for aggregate-only query without ORDER BY (nothing to sort)")
	}

addLimitNode:
	// Add limiting if TOP/LIMIT/OFFSET present
	if query.HasLimit() || query.Offset > 0 {
		limitNode, err := pb.addLimitNode(currentTree, query)
		if err != nil {
			return nil, fmt.Errorf("failed to add limit: %w", err)
		}
		currentTree = limitNode
		pb.logger.Debug("Added LimitNode to tree")
	}

addHierarchicalTransform:
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

	// DEBUG: Log aggregate fields being passed
	pb.logger.Debugf("addAggregationNode: Creating aggregation with %d aggregate fields", len(query.AggregateFields))
	for i, aggField := range query.AggregateFields {
		pb.logger.Debugf("  Aggregate[%d]: Function=%s, Field=%s, Alias=%s",
			i, aggField.Function, aggField.Field, aggField.Alias)
	}

	// DEBUG: Log HAVING expression presence and type
	if query.HavingExpression == nil {
		pb.logger.Debugf("addAggregationNode: No HAVING expression")
	} else {
		pb.logger.Debugf("addAggregationNode: HAVING expression present (type: %T)", query.HavingExpression)
	}

	// Calculate limit for early termination (only safe when no HAVING/ORDER BY/OFFSET)
	limitForEarlyTermination := 0
	if query.HasLimit() &&
		query.HavingExpression == nil && // No HAVING clause
		!query.HasOrderBy() && // No ORDER BY clause
		query.Offset == 0 { // No OFFSET
		limitForEarlyTermination = query.Limit
		pb.logger.Debugf("Enabling early termination for aggregation with limit=%d", limitForEarlyTermination)
	}

	// Create aggregation node with GROUP BY clause
	// Note: ORDER BY is handled separately by SortNode (added after aggregation)
	aggNode := NewAggregationNode(
		child,
		query.GroupBy,
		query.AggregateFields,
		query.HavingExpression,
		nil, // OrderBy handled by SortNode
		limitForEarlyTermination,
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
// OPTIMIZATION: Only sort when user explicitly specifies ORDER BY
// Sorting is expensive O(n log n) and unnecessary for:
// - Write operations (UPDATE/DELETE) - document order doesn't matter
// - SELECT without ORDER BY - user didn't request ordering
func (pb *PlanBuilder) addSortNode(
	child ExecutionNode,
	query *queryparser.UnifiedSelectQuery,
) (ExecutionNode, error) {

	// Only add sort if user explicitly specified ORDER BY
	// Skip default sorting - it's expensive and unnecessary for write operations
	if !query.HasOrderBy() {
		pb.logger.Debug("No ORDER BY specified, skipping sort node (optimization for write operations)")
		return child, nil
	}

	// Create sort node with user-specified ORDER BY clause
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

// checkJoinProvidesSortedOutput checks if JoinExecutionNode uses streaming Top-N optimization
// Returns true if the JOIN node will produce sorted output, allowing SortNode to be skipped
func (pb *PlanBuilder) checkJoinProvidesSortedOutput(
	tree ExecutionNode,
	query *queryparser.UnifiedSelectQuery,
) bool {
	// Check if query has ORDER BY and LIMIT (required for streaming Top-N)
	if !query.HasOrderBy() || !query.HasLimit() {
		return false
	}

	// Try to find JoinExecutionNode (may be wrapped in FilterNode)
	var joinNode *JoinExecutionNode

	// Direct JoinExecutionNode
	if jen, ok := tree.(*JoinExecutionNode); ok {
		joinNode = jen
	}

	// FilterNode wrapping JoinExecutionNode
	if filterNode, ok := tree.(*FilterNode); ok {
		if jen, ok := filterNode.Child.(*JoinExecutionNode); ok {
			joinNode = jen
		}
	}

	if joinNode == nil {
		return false
	}

	// Check if JoinExecutionNode has ORDER BY and LIMIT configured for streaming Top-N
	if joinNode.OrderBy == nil || len(joinNode.OrderBy.Fields) == 0 {
		return false
	}

	if joinNode.Limit <= 0 {
		return false
	}

	// Streaming Top-N threshold: result set must be > 10x limit
	// Since we don't know the result set size at planning time, rely on the node's configured limit
	// The actual decision happens at runtime in JoinExecutionNode.Execute()
	pb.logger.Debugf("JoinExecutionNode configured for streaming Top-N: OrderBy=%d fields, Limit=%d",
		len(joinNode.OrderBy.Fields), joinNode.Limit)

	return true
}
