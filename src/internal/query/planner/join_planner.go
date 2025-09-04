/*
JOIN QUERY PLANNER SYSTEM

This file extends the existing query planner to handle JOIN operations in SyndrDB.
It implements PostgreSQL-style join planning logic that selects the most efficient
join algorithm based on data characteristics and available indexes.

JOIN PLANNING STRATEGY:
1. Analyzes join conditions and available indexes
2. Estimates costs for different join algorithms:
   - Nested Loop Join: Good for small datasets or indexed lookups
   - Hash Join: Efficient for equality joins with large datasets
   - Merge Join: Optimal when both sides are sorted
3. Selects the lowest-cost join algorithm
4. Creates an execution plan tree with appropriate join nodes

COST-BASED OPTIMIZATION:
Following PostgreSQL's approach, the planner estimates:
- I/O costs for reading data
- CPU costs for processing and comparison
- Memory usage for hash tables
- Sort costs when required

RELATIONSHIP INTEGRATION:
The planner can leverage existing relationships between bundles to optimize
join operations by using relationship metadata and indexes.

This implementation follows the Single Responsibility Principle by focusing
on join planning while delegating execution to specialized join nodes.
*/

package planner

import (
	"fmt"
	"strings"
	"syndrdb/src/internal/domain/models"
	"syndrdb/src/internal/query/queryparser"

	"go.uber.org/zap"
)

// JoinPlannerInterface extends the existing planner interface for JOIN operations
type JoinPlannerInterface interface {
	BundleServiceInterface
	CreateJoinExecutionPlan(query *queryparser.SelectJoinQuery, database *models.Database) (*ExecutionPlan, error)
}

// JoinQueryPlanner extends the existing QueryPlanner with JOIN capabilities
type JoinQueryPlanner struct {
	*QueryPlanner // Embed existing planner
}

// NewJoinQueryPlanner creates a new join-capable query planner
func NewJoinQueryPlanner(logger *zap.SugaredLogger, bundleService BundleServiceInterface) *JoinQueryPlanner {
	return &JoinQueryPlanner{
		QueryPlanner: NewQueryPlannerWithService(logger, bundleService),
	}
}

// CreateJoinExecutionPlan creates an execution plan for a JOIN query
// This is the main entry point for JOIN query planning
func (jp *JoinQueryPlanner) CreateJoinExecutionPlan(query *queryparser.SelectJoinQuery, database *models.Database) (*ExecutionPlan, error) {
	jp.Logger.Infof("Creating execution plan for JOIN query: FROM %s with %d joins",
		query.FromBundle, len(query.JoinClauses))

	// Validate that all referenced bundles exist
	bundles := make(map[string]*models.Bundle)

	// Get the primary bundle
	fromBundle, exists := database.Bundles[query.FromBundle]
	if !exists {
		return nil, fmt.Errorf("bundle '%s' does not exist", query.FromBundle)
	}
	bundles[query.FromBundle] = &fromBundle

	// Get all joined bundles
	for _, joinClause := range query.JoinClauses {
		rightBundle, exists := database.Bundles[joinClause.RightBundle]
		if !exists {
			return nil, fmt.Errorf("joined bundle '%s' does not exist", joinClause.RightBundle)
		}
		bundles[joinClause.RightBundle] = &rightBundle
	}

	// Validate the query
	if err := queryparser.ValidateJoinQuery(query, bundles, jp.Logger); err != nil {
		return nil, fmt.Errorf("query validation failed: %w", err)
	}

	// Create base execution node for the FROM bundle
	var currentNode ExecutionNode
	currentBundle := bundles[query.FromBundle]

	// Check if we can optimize the FROM bundle scan with WHERE conditions
	if query.WhereClause != nil {
		// Extract WHERE conditions that apply to the FROM bundle
		fromConditions := jp.extractBundleConditions(query.WhereClause, query.FromBundle)
		if len(fromConditions) > 0 {
			// Create optimized plan for FROM bundle with WHERE conditions
			whereClauseStr := jp.reconstructWhereClause(fromConditions)
			plan, err := jp.QueryPlanner.CreateExecutionPlan(currentBundle, whereClauseStr)
			if err != nil {
				return nil, fmt.Errorf("failed to create base plan for bundle '%s': %w", query.FromBundle, err)
			}
			currentNode = plan.RootNode
		} else {
			// No applicable WHERE conditions - use full scan
			currentNode = &FullScanNode{
				Bundle:        currentBundle,
				Cost:          float64(len(*currentBundle.Documents)),
				EstimatedRows: len(*currentBundle.Documents),
				Logger:        jp.Logger,
			}
		}
	} else {
		// No WHERE clause - use full scan
		currentNode = &FullScanNode{
			Bundle:        currentBundle,
			Cost:          float64(len(*currentBundle.Documents)),
			EstimatedRows: len(*currentBundle.Documents),
			Logger:        jp.Logger,
		}
	}

	// Process each JOIN clause sequentially
	for i, joinClause := range query.JoinClauses {
		jp.Logger.Debugf("Planning join %d: %s %s", i+1, joinClause.JoinType.String(), joinClause.RightBundle)

		rightBundle := bundles[joinClause.RightBundle]

		// Create execution node for the right side of the join
		rightNode := jp.createRightSideNode(rightBundle, query.WhereClause, joinClause.RightBundle)

		// Choose the best join algorithm
		bestJoinNode, err := jp.chooseBestJoinAlgorithm(currentNode, rightNode, joinClause)
		if err != nil {
			return nil, fmt.Errorf("failed to create join plan for %s: %w", joinClause.RightBundle, err)
		}

		currentNode = bestJoinNode
		jp.Logger.Debugf("Selected join algorithm: %T with cost %.2f", bestJoinNode, bestJoinNode.GetCost())
	}

	// Apply any remaining WHERE conditions that weren't pushed down
	if query.WhereClause != nil {
		remainingConditions := jp.extractRemainingConditions(query.WhereClause, bundles)
		if len(remainingConditions) > 0 {
			filterNode := &FilterNode{
				Child:   currentNode,
				Clauses: remainingConditions,
				Logger:  jp.Logger,
			}
			filterNode.Cost = currentNode.GetCost() + float64(currentNode.GetEstimatedRows())*0.1
			filterNode.EstimatedRows = int(float64(currentNode.GetEstimatedRows()) * 0.5) // Assume 50% selectivity
			currentNode = filterNode
		}
	}

	// Create the final execution plan
	plan := &ExecutionPlan{
		RootNode:      currentNode,
		Cost:          currentNode.GetCost(),
		EstimatedRows: currentNode.GetEstimatedRows(),
		IndexesUsed:   []string{}, // TODO: Track indexes used in joins
		Logger:        jp.Logger,
	}

	jp.Logger.Infof("Created JOIN execution plan with cost %.2f, estimated rows: %d",
		plan.Cost, plan.EstimatedRows)

	return plan, nil
}

// chooseBestJoinAlgorithm selects the most efficient join algorithm
func (jp *JoinQueryPlanner) chooseBestJoinAlgorithm(leftNode, rightNode ExecutionNode, joinClause queryparser.JoinClause) (ExecutionNode, error) {
	jp.Logger.Debugf("Choosing join algorithm for %d conditions", len(joinClause.JoinConditions))

	// Create candidate join nodes
	candidates := make([]ExecutionNode, 0, 3)

	// Nested Loop Join - always available
	nestedLoopNode := NewNestedLoopJoinNode(leftNode, rightNode, joinClause.JoinConditions, joinClause.JoinType, jp.Logger)
	candidates = append(candidates, nestedLoopNode)

	// Hash Join - only for equality joins
	if jp.hasEqualityJoinConditions(joinClause.JoinConditions) {
		hashJoinNode := NewHashJoinNode(leftNode, rightNode, joinClause.JoinConditions, joinClause.JoinType, jp.Logger)
		candidates = append(candidates, hashJoinNode)
	}

	// Merge Join - only if both sides can be sorted efficiently
	// For now, we'll skip merge join as it requires more complex sort detection
	// TODO: Implement merge join when we have better sort detection

	// Choose the candidate with the lowest cost
	var bestNode ExecutionNode
	bestCost := float64(^uint(0) >> 1) // Max float64

	for _, candidate := range candidates {
		cost := candidate.GetCost()
		jp.Logger.Debugf("Candidate %T: cost=%.2f, rows=%d", candidate, cost, candidate.GetEstimatedRows())

		if cost < bestCost {
			bestCost = cost
			bestNode = candidate
		}
	}

	if bestNode == nil {
		return nil, fmt.Errorf("no suitable join algorithm found")
	}

	return bestNode, nil
}

// hasEqualityJoinConditions checks if all join conditions use equality
func (jp *JoinQueryPlanner) hasEqualityJoinConditions(conditions []queryparser.JoinCondition) bool {
	for _, condition := range conditions {
		if condition.Operator != "==" {
			return false
		}
	}
	return true
}

// createRightSideNode creates an execution node for the right side of a join
func (jp *JoinQueryPlanner) createRightSideNode(bundle *models.Bundle, whereClause *queryparser.WhereGroup, bundleName string) ExecutionNode {
	// Check if there are WHERE conditions that apply to this bundle
	if whereClause != nil {
		bundleConditions := jp.extractBundleConditions(whereClause, bundleName)
		if len(bundleConditions) > 0 {
			// Try to create an optimized plan for this bundle
			whereClauseStr := jp.reconstructWhereClause(bundleConditions)
			plan, err := jp.QueryPlanner.CreateExecutionPlan(bundle, whereClauseStr)
			if err == nil {
				jp.Logger.Debugf("Created optimized right-side plan for bundle '%s'", bundleName)
				return plan.RootNode
			}
			jp.Logger.Warnf("Failed to create optimized plan for bundle '%s', using full scan: %v", bundleName, err)
		}
	}

	// Default to full scan
	return &FullScanNode{
		Bundle:        bundle,
		Cost:          float64(len(*bundle.Documents)),
		EstimatedRows: len(*bundle.Documents),
		Logger:        jp.Logger,
	}
}

// extractBundleConditions extracts WHERE conditions that apply to a specific bundle
func (jp *JoinQueryPlanner) extractBundleConditions(whereGroup *queryparser.WhereGroup, bundleName string) []queryparser.WhereClause {
	var conditions []queryparser.WhereClause

	// Extract conditions from direct clauses
	for _, clause := range whereGroup.Clauses {
		// Check if this condition references the bundle (either directly or with bundle prefix)
		if jp.conditionAppliesToBundle(clause, bundleName) {
			conditions = append(conditions, clause)
		}
	}

	// Recursively extract from subgroups
	for _, subGroup := range whereGroup.SubGroups {
		subConditions := jp.extractBundleConditions(&subGroup, bundleName)
		conditions = append(conditions, subConditions...)
	}

	return conditions
}

// conditionAppliesToBundle checks if a WHERE condition applies to a specific bundle
func (jp *JoinQueryPlanner) conditionAppliesToBundle(clause queryparser.WhereClause, bundleName string) bool {
	// Check for bundle-prefixed field names like "Bundle_Name.Field_Name"
	if strings.Contains(clause.Field, ".") {
		parts := strings.Split(clause.Field, ".")
		if len(parts) == 2 && parts[0] == bundleName {
			return true
		}
	}

	// For now, assume unprefixed fields apply to the current bundle
	// TODO: Improve this logic when we have better field resolution
	return !strings.Contains(clause.Field, ".")
}

// extractRemainingConditions extracts WHERE conditions that haven't been pushed down
func (jp *JoinQueryPlanner) extractRemainingConditions(whereGroup *queryparser.WhereGroup, bundles map[string]*models.Bundle) []queryparser.WhereClause {
	var conditions []queryparser.WhereClause

	// For now, include all conditions that reference multiple bundles
	for _, clause := range whereGroup.Clauses {
		// This is a simplified implementation - in practice, we'd need more sophisticated analysis
		if strings.Contains(clause.Field, ".") {
			// This might be a cross-bundle condition that needs to be evaluated after the join
			conditions = append(conditions, clause)
		}
	}

	// Recursively extract from subgroups
	for _, subGroup := range whereGroup.SubGroups {
		subConditions := jp.extractRemainingConditions(&subGroup, bundles)
		conditions = append(conditions, subConditions...)
	}

	return conditions
}

// reconstructWhereClause reconstructs a WHERE clause string from conditions
func (jp *JoinQueryPlanner) reconstructWhereClause(conditions []queryparser.WhereClause) string {
	if len(conditions) == 0 {
		return ""
	}

	var parts []string
	for i, condition := range conditions {
		if i > 0 {
			parts = append(parts, "AND")
		}

		// Remove bundle prefix if present
		fieldName := condition.Field
		if strings.Contains(fieldName, ".") {
			parts := strings.Split(fieldName, ".")
			if len(parts) == 2 {
				fieldName = parts[1]
			}
		}

		// Format the condition
		if condition.Value != nil {
			switch v := condition.Value.(type) {
			case string:
				parts = append(parts, fmt.Sprintf("%s %s \"%s\"", fieldName, condition.Operator, v))
			default:
				parts = append(parts, fmt.Sprintf("%s %s %v", fieldName, condition.Operator, v))
			}
		}
	}

	return strings.Join(parts, " ")
}
