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
	"context"
	"fmt"
	"strings"
	"sync"
	"syndrdb/src/internal/domain/bundle"
	"syndrdb/src/internal/domain/document"
	"syndrdb/src/internal/domain/models"
	"syndrdb/src/internal/query/documentscanner"
	joinexecutor "syndrdb/src/internal/query/join_executor" // NEW: For JOIN executor integration
	"syndrdb/src/internal/query/queryparser"
	"syndrdb/src/internal/syndrQL" // For predicate pushdown Expression handling
	"time"                         // For document timestamps and performance timing

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
func NewJoinQueryPlanner(logger *zap.SugaredLogger, bundleServiceInt BundleServiceInterface, bundleService interface {
	GetOrCreateDocumentScanner(bundle *models.Bundle) (documentscanner.DocumentScannerInterface, error)
}) *JoinQueryPlanner {
	return &JoinQueryPlanner{
		QueryPlanner: NewQueryPlannerWithService(logger, bundleServiceInt, bundleService),
	}
}

// CreateJoinExecutionPlan creates an execution plan for a JOIN query
// NEW: Now uses the Phase 1 JOIN executor for improved performance and extensibility
func (jp *JoinQueryPlanner) CreateJoinExecutionPlan(query *queryparser.SelectJoinQuery, database *models.Database) (*ExecutionPlan, error) {
	jp.Logger.Infof("Creating execution plan using new JOIN executor: FROM %s with %d joins",
		query.FromBundle, len(query.JoinClauses))

	// Validate that all referenced bundles exist
	bundles := make(map[string]*models.Bundle)

	// Get the primary bundle
	// fromBundle, exists := database.Bundles[query.FromBundle]
	// if !exists {
	// 	return nil, fmt.Errorf("bundle '%s' does not exist", query.FromBundle)
	// }
	fromBundle, err := jp.BundleServiceInt.GetBundleByName(database, query.FromBundle)
	if err != nil {
		return nil, fmt.Errorf("bundle '%s' does not exist: %w", query.FromBundle, err)
	}
	bundles[query.FromBundle] = fromBundle

	// Get all joined bundles
	for _, joinClause := range query.JoinClauses {
		// rightBundle, exists := database.Bundles[joinClause.RightBundle]
		// if !exists {
		// 	return nil, fmt.Errorf("joined bundle '%s' does not exist", joinClause.RightBundle)
		// }
		rightBundle, err := jp.BundleServiceInt.GetBundleByName(database, joinClause.RightBundle)
		if err != nil {
			return nil, fmt.Errorf("joined bundle '%s' had an err %v", joinClause.RightBundle, err)
		}
		bundles[joinClause.RightBundle] = rightBundle
	}

	// Validate the query
	if err := queryparser.ValidateJoinQuery(query, bundles, jp.Logger); err != nil {
		return nil, fmt.Errorf("query validation failed: %w", err)
	}

	// NEW: Create JOIN executor with pattern tracking and SIMD support
	// TODO: Pass SIMD configuration from server settings instead of hardcoding true
	useSIMD := true                                                                       // Enable SIMD acceleration by default (auto-detects AVX2/NEON support)
	joinExecutor := joinexecutor.NewDefaultJoinExecutor(jp.Logger, 64*1024*1024, useSIMD) // 64MB memory limit

	// Estimate execution cost based on bundle sizes
	// NOTE: Use TotalDocuments metadata instead of Documents field (which is nil for paginated bundles)
	leftSize := int(bundles[query.FromBundle].TotalDocuments)
	rightSize := 0
	if len(query.JoinClauses) > 0 {
		rightSize = int(bundles[query.JoinClauses[0].RightBundle].TotalDocuments)
	}

	// Cost estimation: hash join cost is roughly O(M + N) where M and N are bundle sizes
	estimatedCost := float64(leftSize + rightSize)
	estimatedRows := leftSize / 10 // Assume 10% selectivity as default

	// NEW: Predicate pushdown optimization for WHERE clause
	// Analyzes WHERE expression to identify single-bundle predicates that can be
	// pushed down to filter during JOIN instead of after
	var leftBundleInterface, rightBundleInterface documentscanner.BundleInterface
	hasWhereExpression := query.WhereExpression != nil
	var remainingPredicate syndrQL.Expression

	if hasWhereExpression {
		// Cast WhereExpression to syndrQL.Expression for analysis
		if whereExpr, ok := query.WhereExpression.(syndrQL.Expression); ok {
			// Get join type for safety analysis
			joinType := queryparser.InnerJoin // Default
			if len(query.JoinClauses) > 0 {
				joinType = query.JoinClauses[0].JoinType
			}

			// Analyze predicates for pushdown
			rightBundleName := ""
			if len(query.JoinClauses) > 0 {
				rightBundleName = query.JoinClauses[0].RightBundle
			}

			pushdownResult := AnalyzePredicateForPushdown(
				whereExpr,
				query.FromBundle,
				rightBundleName,
				joinType,
				jp.Logger,
			)

			// Log pushdown analysis results
			jp.Logger.Infof("Predicate pushdown analysis: left=%d, right=%d, remaining=%d predicates",
				len(pushdownResult.LeftPredicates),
				len(pushdownResult.RightPredicates),
				len(pushdownResult.RemainingPredicates))

			// Store remaining predicates for post-JOIN filtering
			remainingPredicate = pushdownResult.CombinedRemainingPredicate

			// Log pushdown analysis
			if pushdownResult.CombinedLeftPredicate != nil {
				jp.Logger.Infof("LEFT predicate pushdown enabled: %s", pushdownResult.CombinedLeftPredicate.String())
			}
			if pushdownResult.CombinedRightPredicate != nil {
				jp.Logger.Infof("RIGHT predicate pushdown enabled: %s", pushdownResult.CombinedRightPredicate.String())
			}
		} else {
			jp.Logger.Info("WHERE expression detected but not syndrQL.Expression - will apply post-JOIN filtering")
			remainingPredicate = nil
		}
	}

	// NEW: Create service manager adapter for the execution node
	serviceManager := &PlannerServiceManager{
		bundles:       bundles,
		logger:        jp.Logger,
		bundleService: jp.BundleServiceInt,
	}

	// Extract pushdown predicates if analysis was performed
	var leftPredicate, rightPredicate syndrQL.Expression
	if hasWhereExpression {
		if whereExpr, ok := query.WhereExpression.(syndrQL.Expression); ok {
			joinType := queryparser.InnerJoin
			if len(query.JoinClauses) > 0 {
				joinType = query.JoinClauses[0].JoinType
			}
			rightBundleName := ""
			if len(query.JoinClauses) > 0 {
				rightBundleName = query.JoinClauses[0].RightBundle
			}
			pushdownResult := AnalyzePredicateForPushdown(
				whereExpr,
				query.FromBundle,
				rightBundleName,
				joinType,
				jp.Logger,
			)
			leftPredicate = pushdownResult.CombinedLeftPredicate
			rightPredicate = pushdownResult.CombinedRightPredicate
		}
	}

	// Compute MergeRequiredFields for opt #3 (merge only required fields)
	var mergeRequired map[string]bool
	if len(query.JoinClauses) > 0 {
		mergeRequired = computeMergeRequiredFields(query, query.JoinClauses[0])
	}

	// Create the new JOIN execution node with pushdown predicates
	joinNode := &JoinExecutionNode{
		Query:                query,
		Database:             database,
		ServiceManager:       serviceManager,
		Logger:               jp.Logger,
		Cost:                 estimatedCost,
		EstimatedRows:        estimatedRows,
		JoinExecutor:         joinExecutor,
		LeftBundleInterface:  leftBundleInterface,  // May be nil or FilteredBundleAdapter
		RightBundleInterface: rightBundleInterface, // May be nil or FilteredBundleAdapter
		LeftPredicate:        leftPredicate,        // Predicate to push to left bundle
		RightPredicate:       rightPredicate,       // Predicate to push to right bundle
		MergeRequiredFields:  mergeRequired,
	}

	jp.Logger.Infof("Created JOIN execution plan: cost=%.2f, estimated_rows=%d, algorithm=hash_join",
		estimatedCost, estimatedRows)

	// Wrap with FilterNode only if there are remaining WHERE conditions
	var rootNode ExecutionNode = joinNode
	finalCost := estimatedCost
	finalEstimatedRows := estimatedRows

	// Check for remaining predicates that couldn't be pushed down
	if remainingPredicate != nil {
		// Create FilterNode to apply only the remaining predicates after JOIN
		filterNode := &FilterNode{
			Child:            joinNode,
			WhereExpression:  remainingPredicate,                         // Only remaining predicates, not all!
			Cost:             estimatedCost + float64(estimatedRows)*0.1, // Add filter cost
			EstimatedRows:    estimatedRows / 10,                         // Assume 10% selectivity
			Logger:           jp.Logger,
			SubqueryExecutor: nil, // TIER 1: TODO - wire in subquery executor from router
		}
		rootNode = filterNode
		finalCost = filterNode.Cost
		finalEstimatedRows = filterNode.EstimatedRows
		jp.Logger.Infof("Wrapped JOIN with FilterNode for remaining predicates: %s", remainingPredicate.String())
	} else if hasWhereExpression && (leftPredicate != nil || rightPredicate != nil) {
		jp.Logger.Info("All WHERE conditions pushed down - no post-JOIN filtering needed")
	} else if hasWhereExpression {
		// No predicates could be pushed down, apply full WHERE expression
		filterNode := &FilterNode{
			Child:            joinNode,
			WhereExpression:  query.WhereExpression,
			Cost:             estimatedCost + float64(estimatedRows)*0.1,
			EstimatedRows:    estimatedRows / 10,
			Logger:           jp.Logger,
			SubqueryExecutor: nil,
		}
		rootNode = filterNode
		finalCost = filterNode.Cost
		finalEstimatedRows = filterNode.EstimatedRows
		jp.Logger.Info("No predicates could be pushed down - applying full WHERE expression after JOIN")
	}

	// Create the final execution plan using the wrapped node
	plan := &ExecutionPlan{
		RootNode:      rootNode,
		Cost:          finalCost,
		EstimatedRows: finalEstimatedRows,
		IndexesUsed:   []string{}, // Phase 1: Basic implementation, Phase 2 will add index usage tracking
		Logger:        jp.Logger,
	}

	jp.Logger.Infof("Created new JOIN execution plan with cost %.2f, estimated rows: %d",
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
		Bundle:           bundle,
		Cost:             float64(len(*bundle.Documents)),
		EstimatedRows:    len(*bundle.Documents),
		Logger:           jp.Logger,
		BundleServiceInt: jp.BundleServiceInt,
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

// extractAllWhereClauses recursively extracts all WHERE clauses from a WhereGroup
// This is used to apply filtering after JOIN execution
func (jp *JoinQueryPlanner) extractAllWhereClauses(whereGroup *queryparser.WhereGroup) []queryparser.WhereClause {
	if whereGroup == nil {
		return []queryparser.WhereClause{}
	}

	var allClauses []queryparser.WhereClause

	// Add direct clauses
	allClauses = append(allClauses, whereGroup.Clauses...)

	// Recursively add clauses from subgroups
	for _, subGroup := range whereGroup.SubGroups {
		subClauses := jp.extractAllWhereClauses(&subGroup)
		allClauses = append(allClauses, subClauses...)
	}

	return allClauses
}

// NEW JOIN INTEGRATION: JoinExecutionNode adapts the new JOIN executor to work with execution plans
// This bridges the new Phase 1 JOIN executor with the existing planner infrastructure
type JoinExecutionNode struct {
	Query          *queryparser.SelectJoinQuery // Original JOIN query
	Database       *models.Database             // Database context
	ServiceManager interface {                  // Service manager for bundle operations
		GetBundleByName(database *models.Database, name string) (*models.Bundle, error)
	}
	Logger               *zap.SugaredLogger              // Logger for debugging
	Cost                 float64                         // Estimated execution cost
	EstimatedRows        int                             // Estimated result rows
	JoinExecutor         joinexecutor.JoinExecutor       // NEW: The Phase 1 JOIN executor
	joinedResults        []*joinexecutor.JoinedDocument  // PHASE 3: Store JoinedDocument results for hierarchical transformation
	LeftBundleInterface  documentscanner.BundleInterface // NEW: Optional filtered bundle for LEFT side (predicate pushdown)
	RightBundleInterface documentscanner.BundleInterface // NEW: Optional filtered bundle for RIGHT side (predicate pushdown)
	LeftPredicate        syndrQL.Expression              // Predicate to push to left bundle (may be nil)
	RightPredicate       syndrQL.Expression              // Predicate to push to right bundle (may be nil)
	// MergeRequiredFields: when non-nil and non-empty, mergeJoinedDocument copies only these fields (opt #3)
	MergeRequiredFields map[string]bool
}

// Execute implements ExecutionNode interface using the new JOIN executor
func (jen *JoinExecutionNode) Execute(ctx context.Context) (map[string]*models.Document, error) {
	jen.Logger.Infof("Executing JOIN using Phase 1 JOIN executor")

	// Opt #1: set projection for join bundles to reduce I/O and deserialization
	var bundleService BundleServiceInterface
	if psm, ok := jen.ServiceManager.(*PlannerServiceManager); ok {
		bundleService = psm.bundleService
	}
	if bundleService != nil && len(jen.Query.JoinClauses) > 0 {
		first := jen.Query.JoinClauses[0]
		leftF, rightF := computeRequiredFieldsForJoin(jen.Query, first, jen.Query.FromBundle, first.RightBundle)
		// Do not set projection when a predicate is pushed to that side: the predicate may reference
		// fields (e.g. rating) that computeRequiredFieldsForJoin does not include (it only uses join
		// keys, SelectFields, OrderBy; it does not extract fields from the Expression predicate).
		// Projecting would omit those fields and cause the filter to drop all rows (e.g. 0 results).
		if len(leftF) > 0 && jen.LeftPredicate == nil {
			bundleService.SetProjectionFieldsForBundle(jen.Query.FromBundle, leftF)
			defer bundleService.SetProjectionFieldsForBundle(jen.Query.FromBundle, nil)
		}
		if len(rightF) > 0 && jen.RightPredicate == nil {
			bundleService.SetProjectionFieldsForBundle(first.RightBundle, rightF)
			defer bundleService.SetProjectionFieldsForBundle(first.RightBundle, nil)
		}
	}

	// Convert query to JOIN request format
	joinRequest, err := jen.convertQueryToJoinRequest()
	if err != nil {
		return nil, fmt.Errorf("failed to convert query to JOIN request: %w", err)
	}

	// Execute JOIN using the new executor
	result, err := jen.JoinExecutor.Execute(joinRequest)
	if err != nil {
		return nil, fmt.Errorf("JOIN execution failed: %w", err)
	}

	// OPTIMIZATION: Ensure pooled JoinedDocuments are returned after merge completes
	// This follows the same pattern as document_pool.go's FreeDocuments() for bulk cleanup
	// The JoinedDocuments are only used during the merge process below, then can be recycled
	defer joinexecutor.FreeJoinedDocuments(result.Documents)

	// PHASE 3: Store JoinedDocument results for hierarchical transformation
	jen.joinedResults = result.Documents

	// Convert JOIN results back to document map
	documents := make(map[string]*models.Document)
	for i, joinedDoc := range result.Documents {
		// Create merged document from JOIN result
		mergedDoc := jen.mergeJoinedDocument(joinedDoc, i)
		documents[mergedDoc.DocumentID] = mergedDoc
	}

	jen.Logger.Infof("JOIN execution completed: %d results, algorithm=%s, memory=%d bytes",
		len(documents), result.Algorithm, result.MemoryUsed)

	return documents, nil
}

// GetCost implements ExecutionNode interface
func (jen *JoinExecutionNode) GetCost() float64 {
	return jen.Cost
}

// GetEstimatedRows implements ExecutionNode interface
func (jen *JoinExecutionNode) GetEstimatedRows() int {
	return jen.EstimatedRows
}

// GetJoinedResults returns the JoinedDocument results for hierarchical transformation
// PHASE 3: Expose JOIN results to hierarchical transformer
func (jen *JoinExecutionNode) GetJoinedResults() []*joinexecutor.JoinedDocument {
	return jen.joinedResults
}

// convertQueryToJoinRequest converts the parsed query to JOIN executor format
func (jen *JoinExecutionNode) convertQueryToJoinRequest() (*joinexecutor.JoinRequest, error) {
	// Get left bundle
	leftBundle, err := jen.ServiceManager.GetBundleByName(jen.Database, jen.Query.FromBundle)
	if err != nil {
		return nil, fmt.Errorf("failed to get left bundle '%s': %w", jen.Query.FromBundle, err)
	}

	// Handle first JOIN clause (Phase 1 supports single JOIN)
	if len(jen.Query.JoinClauses) == 0 {
		return nil, fmt.Errorf("no JOIN clauses found")
	}

	firstJoin := jen.Query.JoinClauses[0]

	// Get right bundle
	rightBundle, err := jen.ServiceManager.GetBundleByName(jen.Database, firstJoin.RightBundle)
	if err != nil {
		return nil, fmt.Errorf("failed to get right bundle '%s': %w", firstJoin.RightBundle, err)
	}

	// Create bundle adapters with bundleService for document loading
	// Extract bundleService from ServiceManager if it implements the full interface
	var bundleService BundleServiceInterface
	if psm, ok := jen.ServiceManager.(*PlannerServiceManager); ok {
		bundleService = psm.bundleService
	}

	// Use filtered bundle interfaces if predicate pushdown was applied
	// Otherwise create standard adapters from bundles
	var leftAdapter, rightAdapter documentscanner.BundleInterface

	// Create base adapters first
	baseLeftAdapter := &PlannerBundleAdapter{
		bundle:        leftBundle,
		logger:        jen.Logger,
		bundleService: bundleService,
	}
	baseRightAdapter := &PlannerBundleAdapter{
		bundle:        rightBundle,
		logger:        jen.Logger,
		bundleService: bundleService,
	}

	// Apply predicate pushdown if predicates are available
	if jen.LeftBundleInterface != nil {
		// Use pre-created filtered adapter
		leftAdapter = jen.LeftBundleInterface
		jen.Logger.Infof("Using pre-created predicate-filtered LEFT bundle adapter")
	} else if jen.LeftPredicate != nil {
		// Create filtered adapter with pushdown predicate; pass streaming opts when bundleService available
		var leftStreamOpts *ExprFilterAdapterOpts
		if bundleService != nil {
			leftStreamOpts = &ExprFilterAdapterOpts{BundleService: bundleService, BundleName: leftBundle.Name}
		}
		leftAdapter = NewExpressionFilteredBundleAdapter(baseLeftAdapter, jen.LeftPredicate, jen.Logger, leftStreamOpts)
		jen.Logger.Infof("Created ExpressionFilteredBundleAdapter for LEFT bundle with predicate: %s", jen.LeftPredicate.String())
	} else {
		// Use standard adapter
		leftAdapter = baseLeftAdapter
	}

	if jen.RightBundleInterface != nil {
		// Use pre-created filtered adapter
		rightAdapter = jen.RightBundleInterface
		jen.Logger.Infof("Using pre-created predicate-filtered RIGHT bundle adapter")
	} else if jen.RightPredicate != nil {
		// Create filtered adapter with pushdown predicate; pass streaming opts when bundleService available
		var rightStreamOpts *ExprFilterAdapterOpts
		if bundleService != nil {
			rightStreamOpts = &ExprFilterAdapterOpts{BundleService: bundleService, BundleName: rightBundle.Name}
		}
		rightAdapter = NewExpressionFilteredBundleAdapter(baseRightAdapter, jen.RightPredicate, jen.Logger, rightStreamOpts)
		jen.Logger.Infof("Created ExpressionFilteredBundleAdapter for RIGHT bundle with predicate: %s", jen.RightPredicate.String())
	} else {
		// Use standard adapter
		rightAdapter = baseRightAdapter
	}

	// Convert JOIN conditions
	var conditions []joinexecutor.JoinCondition
	for _, condition := range firstJoin.JoinConditions {
		conditions = append(conditions, joinexecutor.JoinCondition{
			LeftKey:  condition.LeftField,
			RightKey: condition.RightField,
			Operator: condition.Operator,
		})
	}

	// Convert JOIN type
	var joinType joinexecutor.JoinType
	switch firstJoin.JoinType {
	case queryparser.InnerJoin:
		joinType = joinexecutor.InnerJoin
	case queryparser.LeftJoin:
		joinType = joinexecutor.LeftJoin
	case queryparser.RightJoin:
		joinType = joinexecutor.RightJoin
	case queryparser.FullOuterJoin:
		joinType = joinexecutor.FullOuterJoin
	default:
		joinType = joinexecutor.InnerJoin
	}

	return &joinexecutor.JoinRequest{
		LeftBundle:         leftAdapter,
		RightBundle:        rightAdapter,
		JoinType:           joinType,
		Conditions:         conditions,
		ExpectedResultSize: int64(jen.EstimatedRows),
		MemoryLimit:        64 * 1024 * 1024, // 64MB default
		AllowDiskSpillover: true,
	}, nil
}

// normalizeQualifiedField converts "bundle"."field" (parser may leave embedded ".")
// to "bundle.field" so prefix/base splitting works. No-op if pattern absent.
func normalizeQualifiedField(s string) string {
	return strings.ReplaceAll(s, `"."`, ".")
}

// stripBundlePrefix returns the field name after the first ".", or s if no dot. e.g. "products.name" -> "name".
func stripBundlePrefix(s string) string {
	s = normalizeQualifiedField(s)
	if i := strings.Index(s, "."); i >= 0 && i < len(s)-1 {
		return s[i+1:]
	}
	return s
}

// computeRequiredFieldsForJoin returns field lists for projection pushdown (opt #1).
// leftBundleName and rightBundleName are from query.FromBundle and firstJoin.RightBundle.
// Join keys, SELECT, and ORDER BY are attributed to left or right when they have a "bundle." prefix.
func computeRequiredFieldsForJoin(
	query *queryparser.SelectJoinQuery,
	firstJoin queryparser.JoinClause,
	leftBundleName, rightBundleName string,
) (leftFields, rightFields []string) {
	leftSet := make(map[string]bool)
	rightSet := make(map[string]bool)
	addLeft := func(f string) { leftSet[stripBundlePrefix(f)] = true }
	addRight := func(f string) { rightSet[stripBundlePrefix(f)] = true }

	// Helper: add to left or right when field has "bundle.field" and bundle matches.
	// Normalize "bundle"."field" -> bundle.field so the split works (parser can leave embedded ".").
	// Unqualified fields (e.g. "name", "rating" with no dot) are added to BOTH sides so we never
	// drop them; the side that lacks the field simply won't have it, the other will.
	addByPrefix := func(s string) {
		s = normalizeQualifiedField(s)
		if s == "" {
			return
		}
		if i := strings.Index(s, "."); i >= 0 && i < len(s)-1 {
			prefix, base := s[:i], s[i+1:]
			if prefix == leftBundleName {
				leftSet[base] = true
			}
			if prefix == rightBundleName {
				rightSet[base] = true
			}
		} else {
			leftSet[s] = true
			rightSet[s] = true
		}
	}

	for _, c := range firstJoin.JoinConditions {
		if c.LeftBundle == leftBundleName {
			addLeft(c.LeftField)
		}
		if c.RightBundle == rightBundleName {
			addRight(c.RightField)
		}
	}
	for _, f := range query.SelectFields {
		addByPrefix(f)
	}
	for _, f := range query.OrderBy {
		addByPrefix(f)
	}

	for k := range leftSet {
		leftFields = append(leftFields, k)
	}
	for k := range rightSet {
		rightFields = append(rightFields, k)
	}
	return leftFields, rightFields
}

// computeMergeRequiredFields returns the set of field names to copy in mergeJoinedDocument (opt #3).
// Includes: join key names (stripped), SelectFields (stripped), OrderBy (stripped), and "join_key".
func computeMergeRequiredFields(query *queryparser.SelectJoinQuery, firstJoin queryparser.JoinClause) map[string]bool {
	set := make(map[string]bool)
	add := func(f string) { set[stripBundlePrefix(f)] = true }
	for _, c := range firstJoin.JoinConditions {
		add(c.LeftField)
		add(c.RightField)
	}
	for _, f := range query.SelectFields {
		add(f)
	}
	for _, f := range query.OrderBy {
		add(f)
	}
	set["join_key"] = true
	return set
}

// mergeJoinedDocument creates a single document from JOIN results
// LIFECYCLE: After this function copies fields to the final result document, the input JoinedDocument
// will be returned to the pool via deferred cleanup in the Execute() function.
// This follows the same pattern as document_pool.go's FreeDocuments() for bulk cleanup.
// Opt #3: when MergeRequiredFields is non-nil and non-empty, only those fields are copied.
// The merged Fields map is from document.GetPooledFieldMap(); ReturnPooledDocument will
// return it via doc.PooledFields.
func (jen *JoinExecutionNode) mergeJoinedDocument(joinedDoc *joinexecutor.JoinedDocument, index int) *models.Document {
	mergedFields := document.GetPooledFieldMap()
	onlyRequired := len(jen.MergeRequiredFields) > 0

	// Add left document fields
	if joinedDoc.LeftDocument != nil {
		for fieldName, field := range joinedDoc.LeftDocument.Fields {
			if onlyRequired && !jen.MergeRequiredFields[fieldName] {
				continue
			}
			mergedFields[fieldName] = field
		}
	}

	// Add right document fields (right overwrites left on collision)
	if joinedDoc.RightDocument != nil {
		for fieldName, field := range joinedDoc.RightDocument.Fields {
			if onlyRequired && !jen.MergeRequiredFields[fieldName] {
				continue
			}
			mergedFields[fieldName] = field
		}
	}

	// Add JOIN metadata
	mergedFields["join_key"] = models.Field{
		Name:  "join_key",
		Value: models.NewStringValue(joinedDoc.JoinKey),
	}

	doc := document.GetPooledDocument()
	doc.DocumentID = joinedDoc.JoinKey
	doc.Fields = mergedFields
	doc.PooledFields = true
	doc.CreatedAt = time.Now()
	doc.UpdatedAt = time.Now()
	return doc
}

// PlannerBundleAdapter adapts a Bundle to implement BundleInterface for the JOIN executor
// This avoids circular import issues between planner and server packages
type PlannerBundleAdapter struct {
	bundle        *models.Bundle
	logger        *zap.SugaredLogger
	bundleService BundleServiceInterface
}

// GetDocumentIDs returns all document IDs in the bundle
func (pba *PlannerBundleAdapter) GetDocumentIDs() []string {
	if pba.bundle == nil {
		return []string{}
	}

	// Load documents via bundleService if available
	if pba.bundleService != nil {
		docs, err := pba.bundleService.GetAllDocumentsForIndexing(pba.bundle.Name)
		if err != nil {
			pba.logger.Warnf("Failed to load documents for bundle '%s': %v", pba.bundle.Name, err)
			return []string{}
		}
		ids := make([]string, 0, len(docs))
		for _, doc := range docs {
			ids = append(ids, doc.DocumentID)
		}
		return ids
	}

	// Fallback to deprecated Documents field (legacy bundles)
	if pba.bundle.Documents == nil {
		return []string{}
	}

	// CRITICAL FIX: Use copy-on-read pattern to prevent concurrent map iteration
	pba.bundle.DocumentsMutex.RLock()
	ids := make([]string, 0, len(*pba.bundle.Documents))
	for docID := range *pba.bundle.Documents {
		ids = append(ids, docID)
	}
	pba.bundle.DocumentsMutex.RUnlock()

	return ids
}

// GetDocument retrieves a document by its ID
func (pba *PlannerBundleAdapter) GetDocument(docID string) *models.Document {
	if pba.bundle == nil {
		return nil
	}

	// Load documents via bundleService if available
	if pba.bundleService != nil {
		docs, err := pba.bundleService.GetAllDocumentsForIndexing(pba.bundle.Name)
		if err != nil {
			pba.logger.Warnf("Failed to load documents for bundle '%s': %v", pba.bundle.Name, err)
			return nil
		}
		for _, doc := range docs {
			if doc.DocumentID == docID {
				return doc
			}
		}
		return nil
	}

	// Fallback to deprecated Documents field (legacy bundles)
	if pba.bundle.Documents == nil {
		return nil
	}

	documents := *pba.bundle.Documents
	doc, exists := documents[docID]
	if !exists {
		return nil
	}

	return &doc
}

// GetAllDocuments returns all documents in the bundle as a map
func (pba *PlannerBundleAdapter) GetAllDocuments() map[string]*models.Document {
	if pba.bundle == nil {
		return make(map[string]*models.Document)
	}

	// Load documents via bundleService if available
	if pba.bundleService != nil {
		docs, err := pba.bundleService.GetAllDocumentsForIndexing(pba.bundle.Name)
		if err != nil {
			pba.logger.Warnf("Failed to load documents for bundle '%s': %v", pba.bundle.Name, err)
			return make(map[string]*models.Document)
		}
		// Convert []*models.Document to map[string]*models.Document
		result := make(map[string]*models.Document, len(docs))
		for _, doc := range docs {
			result[doc.DocumentID] = doc
		}
		pba.logger.Debugf("Loaded %d documents for bundle '%s' via bundleService", len(result), pba.bundle.Name)
		return result
	}

	// Fallback to deprecated Documents field (legacy bundles)
	if pba.bundle.Documents == nil {
		return make(map[string]*models.Document)
	}

	// Convert from map[string]models.Document to map[string]*models.Document
	result := make(map[string]*models.Document)
	documents := *pba.bundle.Documents
	for docID, doc := range documents {
		docCopy := doc // Create copy to avoid address issues
		result[docID] = &docCopy
	}

	return result
}

// GetAllDocumentsWithLimit returns documents up to the specified limit with early termination
// OPTIMIZATION: For PlannerBundleAdapter, this is a simple wrapper that delegates to GetAllDocuments()
// and then truncates to the limit. Full early termination would require changes to bundleService.
func (pba *PlannerBundleAdapter) GetAllDocumentsWithLimit(limit int) map[string]*models.Document {
	if limit <= 0 {
		// No limit or invalid limit - delegate to regular GetAllDocuments()
		return pba.GetAllDocuments()
	}

	// Get all documents first (PlannerBundleAdapter is used in join contexts where
	// early termination at page level is less applicable)
	allDocs := pba.GetAllDocuments()

	// Truncate to limit
	if len(allDocs) <= limit {
		return allDocs
	}

	// Return only first 'limit' documents
	result := make(map[string]*models.Document, limit)
	count := 0
	for docID, doc := range allDocs {
		if count >= limit {
			break
		}
		result[docID] = doc
		count++
	}

	return result
}

// ScanDocumentChunks streams documents in chunks via BundleService.GetDocumentChunksForIndexing.
func (pba *PlannerBundleAdapter) ScanDocumentChunks(ctx context.Context, chunkSize int, fn func(chunk []*models.Document) (stop bool)) error {
	if pba.bundle == nil {
		return fmt.Errorf("PlannerBundleAdapter: bundle is nil")
	}
	if pba.bundleService == nil {
		return fmt.Errorf("PlannerBundleAdapter: bundleService is nil")
	}
	return pba.bundleService.GetDocumentChunksForIndexing(ctx, pba.bundle.Name, chunkSize, fn)
}

// GetName returns the bundle name for logging and metrics
func (pba *PlannerBundleAdapter) GetName() string {
	if pba.bundle == nil {
		return "unknown_bundle"
	}
	return pba.bundle.Name
}

// GetTotalDocuments returns the total number of documents in the bundle
func (pba *PlannerBundleAdapter) GetTotalDocuments() int {
	if pba.bundle == nil {
		return 0
	}
	// Use metadata field instead of loading all documents
	return int(pba.bundle.TotalDocuments)
}

// GetHashIndexForField retrieves the hash index for a specific field
// Returns nil if no index exists for the field
// This is used by join executors for index-assisted operations
// Supports both foreign key indexes and regular hash indexes
func (pba *PlannerBundleAdapter) GetHashIndexForField(fieldName string) interface{} {
	if pba.bundle == nil || pba.bundle.Indexes == nil {
		return nil
	}

	// Iterate through all indexes to find one that has this field
	for indexName, indexRef := range pba.bundle.Indexes {
		// Only consider hash indexes (skip btree, etc.)
		if indexRef.IndexType != "hash" {
			continue
		}

		// Check if any field in this index matches the requested field name
		matched := false
		for _, field := range indexRef.Fields {
			if field.Name == fieldName {
				matched = true
				break
			}
		}
		// Fallback: IndexReference.Fields may be nil when populated by discoverBundleIndexes
		// before the fix; HashIndexField.FieldName can be "product_id_fk" for document field "product_id"
		if !matched && indexRef.HashIndexField.FieldName != "" {
			hf := indexRef.HashIndexField.FieldName
			if hf == fieldName || strings.TrimSuffix(hf, "_fk") == fieldName {
				matched = true
			}
		}
		if !matched {
			continue
		}

		// Load index from disk when IndexInstance is nil (not persisted)
		inst := indexRef.IndexInstance
		if inst == nil && pba.bundleService != nil {
			loaded, err := pba.bundleService.GetOrLoadHashIndexInterface(pba.bundle, indexName, indexRef)
			if err == nil {
				inst = loaded
			}
		}
		return inst
	}

	return nil
}

// HasIndexOnField checks if an index exists for the specified field
// This is a quick check without loading the index
// Supports both foreign key indexes and regular hash indexes
func (pba *PlannerBundleAdapter) HasIndexOnField(fieldName string) bool {
	if pba.bundle == nil || pba.bundle.Indexes == nil {
		return false
	}

	// Iterate through all indexes to find one that has this field
	for _, indexRef := range pba.bundle.Indexes {
		// Only consider hash indexes (skip btree, etc.)
		if indexRef.IndexType != "hash" {
			continue
		}

		// Check if any field in this index matches the requested field name
		for _, field := range indexRef.Fields {
			if field.Name == fieldName {
				return true
			}
		}
		// Fallback: Fields may be nil when from discoverBundleIndexes before fix; HashIndexField may be "product_id_fk" for "product_id"
		if indexRef.HashIndexField.FieldName != "" {
			hf := indexRef.HashIndexField.FieldName
			if hf == fieldName || strings.TrimSuffix(hf, "_fk") == fieldName {
				return true
			}
		}
	}

	return false
}

// LoadPage implements BundleInterface - loads a page by page ID
func (pba *PlannerBundleAdapter) LoadPage(pageID uint32) (*models.DocumentPage, error) {
	if pba.bundle == nil {
		return nil, fmt.Errorf("bundle is nil")
	}
	
	if pba.bundleService == nil {
		return nil, fmt.Errorf("bundleService is not available")
	}
	
	// Get database name
	databaseName := ""
	if pba.bundle.Database != nil {
		databaseName = pba.bundle.Database.Name
	}
	
	// Use bundleService to get document page (uses shared cache)
	return pba.bundleService.GetDocumentPage(pba.bundle.Name, databaseName, pageID)
}

// PlannerServiceManager adapts bundle operations for the JOIN execution node
// This provides the service interface needed by the JOIN executor without circular imports
type PlannerServiceManager struct {
	bundles       map[string]*models.Bundle
	logger        *zap.SugaredLogger
	bundleService BundleServiceInterface
}

// GetBundleByName retrieves a bundle by name from the planner's bundle map
func (psm *PlannerServiceManager) GetBundleByName(database *models.Database, name string) (*models.Bundle, error) {
	bundle, exists := psm.bundles[name]
	if !exists {
		return nil, fmt.Errorf("bundle '%s' not found", name)
	}
	return bundle, nil
}

// =============================================================================
// PREDICATE PUSHDOWN - Phase 2 Optimization
// =============================================================================

// PredicatePushdownResult holds the result of analyzing WHERE predicates for pushdown
type PredicatePushdownResult struct {
	// LeftPredicates are predicates that only reference the left (FROM) bundle
	LeftPredicates []syndrQL.Expression
	// RightPredicates are predicates that only reference the right (JOIN) bundle
	RightPredicates []syndrQL.Expression
	// RemainingPredicates are predicates that span multiple bundles or can't be pushed
	RemainingPredicates []syndrQL.Expression
	// CombinedLeftPredicate is all left predicates combined with AND (nil if none)
	CombinedLeftPredicate syndrQL.Expression
	// CombinedRightPredicate is all right predicates combined with AND (nil if none)
	CombinedRightPredicate syndrQL.Expression
	// CombinedRemainingPredicate is all remaining predicates combined with AND (nil if none)
	CombinedRemainingPredicate syndrQL.Expression
}

// AnalyzePredicateForPushdown analyzes a WHERE expression and separates predicates
// by which bundle they reference. This enables pushing single-bundle predicates
// down to be evaluated during the JOIN instead of after.
//
// Safety rules:
// - INNER JOIN: Both left and right predicates can be pushed down
// - LEFT JOIN: Only left predicates can be pushed (right side must preserve NULLs)
// - RIGHT JOIN: Only right predicates can be pushed (left side must preserve NULLs)
// - FULL OUTER JOIN: No predicates can be pushed (both sides must preserve NULLs)
func AnalyzePredicateForPushdown(
	expr syndrQL.Expression,
	leftBundleName string,
	rightBundleName string,
	joinType queryparser.JoinType,
	logger *zap.SugaredLogger,
) *PredicatePushdownResult {
	result := &PredicatePushdownResult{
		LeftPredicates:      make([]syndrQL.Expression, 0),
		RightPredicates:     make([]syndrQL.Expression, 0),
		RemainingPredicates: make([]syndrQL.Expression, 0),
	}

	if expr == nil {
		return result
	}

	// Split the expression into individual predicates connected by AND
	predicates := splitByAnd(expr)

	for _, pred := range predicates {
		// Analyze which bundles this predicate references
		bundles := extractReferencedBundles(pred, leftBundleName, rightBundleName)

		switch {
		case len(bundles) == 0:
			// Predicate doesn't reference any bundle fields (e.g., "1 = 1")
			// Keep in remaining predicates
			result.RemainingPredicates = append(result.RemainingPredicates, pred)

		case len(bundles) == 1 && bundles[0] == leftBundleName:
			// Only references left bundle
			// Safe to push for INNER JOIN, LEFT JOIN
			if joinType == queryparser.InnerJoin || joinType == queryparser.LeftJoin {
				result.LeftPredicates = append(result.LeftPredicates, pred)
				logger.Debugf("Pushing predicate to LEFT bundle: %s", pred.String())
			} else {
				result.RemainingPredicates = append(result.RemainingPredicates, pred)
			}

		case len(bundles) == 1 && bundles[0] == rightBundleName:
			// Only references right bundle
			// Safe to push for INNER JOIN, RIGHT JOIN
			// NOT safe for LEFT JOIN (would filter out NULL rows)
			if joinType == queryparser.InnerJoin || joinType == queryparser.RightJoin {
				result.RightPredicates = append(result.RightPredicates, pred)
				logger.Debugf("Pushing predicate to RIGHT bundle: %s", pred.String())
			} else {
				result.RemainingPredicates = append(result.RemainingPredicates, pred)
				logger.Debugf("Cannot push right predicate for %s: %s", joinType.String(), pred.String())
			}

		default:
			// References multiple bundles - cannot push
			result.RemainingPredicates = append(result.RemainingPredicates, pred)
			logger.Debugf("Predicate spans multiple bundles, keeping in filter: %s", pred.String())
		}
	}

	// Combine predicates with AND for each category
	result.CombinedLeftPredicate = combineWithAnd(result.LeftPredicates)
	result.CombinedRightPredicate = combineWithAnd(result.RightPredicates)
	result.CombinedRemainingPredicate = combineWithAnd(result.RemainingPredicates)

	return result
}

// splitByAnd recursively splits an expression tree at AND nodes
// Returns individual predicates that were connected by AND
func splitByAnd(expr syndrQL.Expression) []syndrQL.Expression {
	if expr == nil {
		return nil
	}

	// Check if this is a binary AND expression
	if binary, ok := expr.(*syndrQL.BinaryExpression); ok {
		if binary.Operator == syndrQL.TOKEN_AND {
			// Recursively split both sides
			left := splitByAnd(binary.Left)
			right := splitByAnd(binary.Right)
			return append(left, right...)
		}
	}

	// Check if this is a grouped expression containing AND
	if grouped, ok := expr.(*syndrQL.GroupedExpression); ok {
		return splitByAnd(grouped.Expression)
	}

	// Not an AND - return as single predicate
	return []syndrQL.Expression{expr}
}

// extractReferencedBundles finds which bundles are referenced by field identifiers
func extractReferencedBundles(expr syndrQL.Expression, leftBundleName, rightBundleName string) []string {
	bundleSet := make(map[string]bool)
	extractBundleRefs(expr, leftBundleName, rightBundleName, bundleSet)

	bundles := make([]string, 0, len(bundleSet))
	for bundle := range bundleSet {
		bundles = append(bundles, bundle)
	}
	return bundles
}

// extractBundleRefs recursively extracts bundle references from an expression
func extractBundleRefs(expr syndrQL.Expression, leftBundleName, rightBundleName string, bundleSet map[string]bool) {
	if expr == nil {
		return
	}

	switch e := expr.(type) {
	case *syndrQL.QualifiedIdentifierExpression:
		// Qualified field like "reviews"."rating" - extract bundle name
		bundleSet[e.Bundle] = true

	case *syndrQL.IdentifierExpression:
		// Unqualified field name - need to determine which bundle it belongs to
		// For now, assume unqualified fields belong to the right (joined) bundle
		// since the query is typically "FROM left JOIN right WHERE right.field ..."
		// This is a simplification - a more robust implementation would check
		// field definitions in both bundles
		bundleSet[rightBundleName] = true

	case *syndrQL.BinaryExpression:
		extractBundleRefs(e.Left, leftBundleName, rightBundleName, bundleSet)
		extractBundleRefs(e.Right, leftBundleName, rightBundleName, bundleSet)

	case *syndrQL.UnaryExpression:
		extractBundleRefs(e.Right, leftBundleName, rightBundleName, bundleSet)

	case *syndrQL.GroupedExpression:
		extractBundleRefs(e.Expression, leftBundleName, rightBundleName, bundleSet)

	case *syndrQL.CallExpression:
		for _, arg := range e.Arguments {
			extractBundleRefs(arg, leftBundleName, rightBundleName, bundleSet)
		}

	case *syndrQL.ArrayExpression:
		for _, elem := range e.Elements {
			extractBundleRefs(elem, leftBundleName, rightBundleName, bundleSet)
		}

	case *syndrQL.LiteralExpression:
		// Literals don't reference bundles
	}
}

// combineWithAnd combines multiple predicates into a single AND expression
// Returns nil if the slice is empty
func combineWithAnd(predicates []syndrQL.Expression) syndrQL.Expression {
	if len(predicates) == 0 {
		return nil
	}
	if len(predicates) == 1 {
		return predicates[0]
	}

	// Build AND chain: pred1 AND pred2 AND pred3 ...
	result := predicates[0]
	for i := 1; i < len(predicates); i++ {
		result = &syndrQL.BinaryExpression{
			Left:     result,
			Operator: syndrQL.TOKEN_AND,
			Right:    predicates[i],
		}
	}
	return result
}

// =============================================================================
// ExpressionFilteredBundleAdapter - Applies Expression predicates during iteration
// =============================================================================

// parallelFilterThreshold is the minimum number of documents to use parallel filtering
const parallelFilterThreshold = 500

// maxFilterWorkers is the maximum number of parallel filter workers
const maxFilterWorkers = 4

// ExprFilterAdapterOpts enables streaming filter and parallel page loading when provided to
// NewExpressionFilteredBundleAdapter. When nil, the adapter falls back to load-all-then-filter.
type ExprFilterAdapterOpts struct {
	BundleService BundleServiceInterface // required for streaming+parallel path
	BundleName    string                 // bundle to load
}

// ExpressionFilteredBundleAdapter wraps a BundleInterface and applies a syndrQL.Expression
// predicate filter during document retrieval. This enables predicate pushdown optimization
// for the modern Expression-based WHERE clause system.
//
// OPTIMIZATION: When streamingOpts is set, uses streaming filter-while-loading and parallel
// page loading via BundleService.GetAllDocumentsForIndexingWithOptions. Otherwise uses
// parallel in-memory filtering for large sets (>500 docs).
//
// Note: This is different from FilteredBundleAdapter in predicate_pushdown.go which uses
// the older WhereClause system. This adapter uses the unified syndrQL.Expression system.
type ExpressionFilteredBundleAdapter struct {
	inner         documentscanner.BundleInterface
	predicate     syndrQL.Expression
	logger        *zap.SugaredLogger
	evaluator     *syndrQL.ExpressionEvaluator
	streamingOpts *ExprFilterAdapterOpts // optional; when set, use streaming+parallel load
}

// NewExpressionFilteredBundleAdapter creates a new expression-filtered bundle adapter.
// opts is optional: when non-nil and BundleService/BundleName are set, uses streaming
// filter-while-loading and parallel page loading.
func NewExpressionFilteredBundleAdapter(
	inner documentscanner.BundleInterface,
	predicate syndrQL.Expression,
	logger *zap.SugaredLogger,
	opts *ExprFilterAdapterOpts,
) *ExpressionFilteredBundleAdapter {
	return &ExpressionFilteredBundleAdapter{
		inner:         inner,
		predicate:     predicate,
		logger:        logger,
		evaluator:     syndrQL.NewExpressionEvaluator(logger),
		streamingOpts: opts,
	}
}

// GetDocumentIDs returns document IDs that match the predicate
// Note: This requires evaluating all documents, so it's less efficient
// than GetAllDocuments for filtered access
func (f *ExpressionFilteredBundleAdapter) GetDocumentIDs() []string {
	all := f.inner.GetAllDocuments()
	filtered := make([]string, 0, len(all)/2) // Estimate 50% selectivity

	for id, doc := range all {
		if f.matchesPredicate(doc) {
			filtered = append(filtered, id)
		}
	}

	return filtered
}

// GetDocument retrieves a document by ID only if it matches the predicate
func (f *ExpressionFilteredBundleAdapter) GetDocument(docID string) *models.Document {
	doc := f.inner.GetDocument(docID)
	if doc == nil {
		return nil
	}

	if f.matchesPredicate(doc) {
		return doc
	}
	return nil
}

// filterResult holds a document that passed the predicate filter
type filterResult struct {
	docID string
	doc   *models.Document
}

// GetAllDocuments returns all documents that match the predicate
// OPTIMIZATION: When streamingOpts is set, uses streaming filter-while-loading and parallel
// page loading. Otherwise uses load-all-then-filter (with parallel in-memory filter for large sets).
func (f *ExpressionFilteredBundleAdapter) GetAllDocuments() map[string]*models.Document {
	startTime := time.Now()

	// Streaming + parallel path: filter while loading pages, never materialize full unfiltered set
	if f.streamingOpts != nil && f.streamingOpts.BundleService != nil && f.streamingOpts.BundleName != "" {
		filter := func(d *models.Document) bool {
			r, err := f.evaluator.EvaluateAsBool(f.predicate, d, nil, nil, nil)
			if err != nil {
				f.logger.Warnf("streaming filter eval error for doc %s: %v", d.DocumentID, err)
				return false
			}
			return r
		}
		// opts := &bundle.IndexingOptions{Filter: filter, Concurrency: 4} // lower probe concurrency
		opts := &bundle.IndexingOptions{Filter: filter, Concurrency: 8}
		docs, err := f.streamingOpts.BundleService.GetAllDocumentsForIndexingWithOptions(f.streamingOpts.BundleName, opts)
		if err != nil {
			f.logger.Warnf("GetAllDocumentsForIndexingWithOptions failed, falling back to load-then-filter: %v", err)
			// fall through to load-then-filter
		} else {
			m := make(map[string]*models.Document, len(docs))
			for _, d := range docs {
				m[d.DocumentID] = d
			}
			f.logger.Infof("ExpressionFilteredBundleAdapter: streaming+parallel returned %d documents in %v",
				len(m), time.Since(startTime))
			return m
		}
	}

	// Fallback: load all from inner, then filter (parallel or sequential)
	all := f.inner.GetAllDocuments()
	f.logger.Infof("ExpressionFilteredBundleAdapter.GetAllDocuments: loaded %d documents, parallelThreshold=%d",
		len(all), parallelFilterThreshold)

	if len(all) >= parallelFilterThreshold {
		return f.getAllDocumentsParallel(all)
	}

	filtered := make(map[string]*models.Document, len(all)/2)
	matchCount := 0
	for id, doc := range all {
		if f.matchesPredicate(doc) {
			filtered[id] = doc
			matchCount++
		}
	}

	f.logger.Infof("ExpressionFilteredBundleAdapter: %d/%d documents matched predicate in %v (sequential)",
		matchCount, len(all), time.Since(startTime))

	return filtered
}

// getAllDocumentsParallel filters documents using parallel workers
func (f *ExpressionFilteredBundleAdapter) getAllDocumentsParallel(all map[string]*models.Document) map[string]*models.Document {
	startTime := time.Now()

	// Determine number of workers based on document count
	numWorkers := maxFilterWorkers
	if len(all) < parallelFilterThreshold*2 {
		numWorkers = 2 // Use fewer workers for smaller sets
	}

	// Convert map to slice for partitioning
	type docEntry struct {
		id  string
		doc *models.Document
	}
	docs := make([]docEntry, 0, len(all))
	for id, doc := range all {
		docs = append(docs, docEntry{id, doc})
	}

	// Create result channel
	resultChan := make(chan filterResult, len(docs))
	var wg sync.WaitGroup

	// Calculate partition size
	partitionSize := (len(docs) + numWorkers - 1) / numWorkers

	// Launch workers
	for w := 0; w < numWorkers; w++ {
		start := w * partitionSize
		end := start + partitionSize
		if end > len(docs) {
			end = len(docs)
		}
		if start >= len(docs) {
			break
		}

		wg.Add(1)
		go func(partition []docEntry) {
			defer wg.Done()

			// Each worker has its own evaluator to avoid mutex contention
			evaluator := syndrQL.NewExpressionEvaluator(f.logger)

			for _, entry := range partition {
				result, err := evaluator.EvaluateAsBool(f.predicate, entry.doc, nil, nil, nil)
				if err != nil {
					// Skip documents with evaluation errors
					continue
				}
				if result {
					resultChan <- filterResult{docID: entry.id, doc: entry.doc}
				}
			}
		}(docs[start:end])
	}

	// Close channel when all workers complete
	go func() {
		wg.Wait()
		close(resultChan)
	}()

	// Collect results
	filtered := make(map[string]*models.Document, len(all)/2)
	for result := range resultChan {
		filtered[result.docID] = result.doc
	}

	f.logger.Infof("ExpressionFilteredBundleAdapter: %d/%d documents matched predicate in %v (parallel, %d workers)",
		len(filtered), len(all), time.Since(startTime), numWorkers)

	return filtered
}

// GetAllDocumentsWithLimit returns filtered documents up to the limit
func (f *ExpressionFilteredBundleAdapter) GetAllDocumentsWithLimit(limit int) map[string]*models.Document {
	if limit <= 0 {
		return f.GetAllDocuments()
	}

	all := f.inner.GetAllDocuments()
	filtered := make(map[string]*models.Document, limit)

	count := 0
	for id, doc := range all {
		if count >= limit {
			break
		}
		if f.matchesPredicate(doc) {
			filtered[id] = doc
			count++
		}
	}

	return filtered
}

// ScanDocumentChunks delegates to inner and filters each chunk by the predicate.
func (f *ExpressionFilteredBundleAdapter) ScanDocumentChunks(ctx context.Context, chunkSize int, fn func(chunk []*models.Document) (stop bool)) error {
	return f.inner.ScanDocumentChunks(ctx, chunkSize, func(chunk []*models.Document) bool {
		filtered := make([]*models.Document, 0, len(chunk))
		for _, doc := range chunk {
			if f.matchesPredicate(doc) {
				filtered = append(filtered, doc)
			}
		}
		if len(filtered) == 0 {
			return true
		}
		return fn(filtered)
	})
}

// GetName returns the bundle name with a filter indicator
func (f *ExpressionFilteredBundleAdapter) GetName() string {
	return f.inner.GetName() + " [expr-filtered]"
}

// GetTotalDocuments returns an estimate of matching documents
// Note: Returns inner total as upper bound since we can't know exact count
// without evaluating all documents
func (f *ExpressionFilteredBundleAdapter) GetTotalDocuments() int {
	// Return inner total as upper bound
	// The actual count will be determined during iteration
	return f.inner.GetTotalDocuments()
}

// GetHashIndexForField delegates to the inner bundle
func (f *ExpressionFilteredBundleAdapter) GetHashIndexForField(fieldName string) interface{} {
	return f.inner.GetHashIndexForField(fieldName)
}

// HasIndexOnField delegates to the inner bundle
func (f *ExpressionFilteredBundleAdapter) HasIndexOnField(fieldName string) bool {
	return f.inner.HasIndexOnField(fieldName)
}

// LoadPage implements BundleInterface - delegates to inner and filters documents by predicate
func (f *ExpressionFilteredBundleAdapter) LoadPage(pageID uint32) (*models.DocumentPage, error) {
	// Delegate to inner adapter
	page, err := f.inner.LoadPage(pageID)
	if err != nil {
		return nil, err
	}
	
	// Filter documents by predicate
	filteredPage := &models.DocumentPage{
		PageID:         page.PageID,
		BundleID:       page.BundleID,
		Documents:      make(map[string]models.Document),
		NextPageID:     page.NextPageID,
		PreviousPageID: page.PreviousPageID,
		IsDirty:        page.IsDirty,
		LoadedAt:       page.LoadedAt,
		DocumentCount:  0,
	}
	
	for docID, doc := range page.Documents {
		docPtr := &doc
		if f.matchesPredicate(docPtr) {
			filteredPage.Documents[docID] = doc
			filteredPage.DocumentCount++
		}
	}
	
	return filteredPage, nil
}

// matchesPredicate evaluates the predicate against a document
func (f *ExpressionFilteredBundleAdapter) matchesPredicate(doc *models.Document) bool {
	if f.predicate == nil {
		return true
	}

	result, err := f.evaluator.EvaluateAsBool(f.predicate, doc, nil, nil, nil)
	if err != nil {
		// Log error but don't crash - treat as non-match
		f.logger.Warnf("Predicate evaluation error for doc %s: %v", doc.DocumentID, err)
		return false
	}

	return result
}
