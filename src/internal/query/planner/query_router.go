/*
QUERY ROUTER - PHASE 3

This file implements the QueryRouter component that routes queries to the
appropriate planner based on query type and characteristics.

ARCHITECTURE:
The QueryRouter follows the Strategy pattern, selecting the appropriate planning
strategy (base planner vs JOIN planner) based on query analysis.

DESIGN PRINCIPLES:
- Single Responsibility: Route queries to correct planner
- Open/Closed: Routing logic can be extended without modifying planners
- Dependency Inversion: Depends on planner interfaces, not concrete implementations

ROUTING LOGIC:
- SIMPLE queries → QueryPlanner (scan + filter optimization)
- JOIN queries → JoinQueryPlanner (JOIN optimization)
- GROUPBY queries → QueryPlanner (aggregation added by PlanBuilder)
- COMPLEX queries (JOIN + GROUP BY) → JoinQueryPlanner + PlanBuilder

This is part of Phase 3 of the unified query system implementation.
*/

package planner

import (
	"fmt"
	"syndrdb/src/internal/domain/models"
	"syndrdb/src/internal/query/documentscanner"
	"syndrdb/src/internal/query/queryparser"
	"syndrdb/src/internal/syndrQL"
	"syndrdb/src/pkg/settings"

	"go.uber.org/zap"
)

// QueryRouter routes queries to appropriate planners based on query characteristics
// PHASE 3: Query Router Component
type QueryRouter struct {
	// basePlanner handles simple queries with scan/filter
	basePlanner *QueryPlanner

	// joinPlanner handles queries with JOINs
	joinPlanner *JoinQueryPlanner

	// bundleService for bundle metadata access
	bundleService BundleServiceInterface

	// queryCache for expression caching and predicate reordering (Priority 4)
	queryCache *QueryCache

	// TIER 1 SUBQUERY SUPPORT: Subquery executor for IN/EXISTS evaluation
	subqueryExecutor interface{} // Passed from UnifiedQueryPlanner

	// logger for debugging
	logger *zap.SugaredLogger
}

// NewQueryRouter creates a new query router
// PHASE 3: Factory function for QueryRouter creation
//
// Parameters:
//   - basePlanner: QueryPlanner for scan/filter queries
//   - joinPlanner: JoinQueryPlanner for JOIN queries
//   - bundleService: Service for bundle metadata access
//   - logger: Logger for debugging
//
// Returns:
//   - *QueryRouter: Configured query router
func NewQueryRouter(
	basePlanner *QueryPlanner,
	joinPlanner *JoinQueryPlanner,
	bundleService BundleServiceInterface,
	logger *zap.SugaredLogger,
) *QueryRouter {
	// Initialize QueryCache with configured size
	args := settings.GetSettings()
	queryCache := NewQueryCache(args.WhereExpressionCacheSize, logger)

	return &QueryRouter{
		basePlanner:   basePlanner,
		joinPlanner:   joinPlanner,
		bundleService: bundleService,
		queryCache:    queryCache,
		logger:        logger,
	}
}

// SetSubqueryExecutor sets the subquery executor after router construction
// This avoids circular dependency issues during initialization
func (qr *QueryRouter) SetSubqueryExecutor(executor interface{}) {
	qr.subqueryExecutor = executor
}

// RouteQuery routes a unified query to the appropriate planner
// PHASE 3: Main routing method
//
// Routing decisions:
// - Queries with JOINs → JoinQueryPlanner
// - Simple queries → QueryPlanner
// - GROUP BY queries → QueryPlanner (aggregation handled by PlanBuilder)
//
// Parameters:
//   - query: UnifiedSelectQuery to route
//   - database: Database context
//
// Returns:
//   - ExecutionNode: Base execution tree from planner
//   - []string: Indexes used by plan
//   - error: Any error during planning
func (qr *QueryRouter) RouteQuery(
	query *queryparser.UnifiedSelectQuery,
	database *models.Database,
) (ExecutionNode, []string, error) {

	qr.logger.Debugf("Routing query: Type=%s, HasJoin=%v, HasGroupBy=%v, HasWhere=%v",
		query.QueryType, query.HasJoin(), query.HasGroupBy(), query.HasWhere())

	// Route based on query characteristics
	if query.HasJoin() {
		return qr.routeJoinQuery(query, database)
	}

	// For simple queries and GROUP BY queries, use base planner
	return qr.routeSimpleQuery(query, database)
}

// routeJoinQuery handles queries with JOIN clauses
// PHASE 3: JOIN query routing
func (qr *QueryRouter) routeJoinQuery(
	query *queryparser.UnifiedSelectQuery,
	database *models.Database,
) (ExecutionNode, []string, error) {

	qr.logger.Debug("Routing to JoinQueryPlanner")

	// Convert UnifiedSelectQuery to SelectJoinQuery format
	joinQuery := qr.convertToJoinQuery(query)

	// Delegate to existing JOIN planner
	plan, err := qr.joinPlanner.CreateJoinExecutionPlan(joinQuery, database)
	if err != nil {
		return nil, nil, fmt.Errorf("JOIN planner failed: %w", err)
	}

	return plan.RootNode, plan.IndexesUsed, nil
}

// routeSimpleQuery handles queries without JOINs
// PHASE 3: Simple query routing
func (qr *QueryRouter) routeSimpleQuery(
	query *queryparser.UnifiedSelectQuery,
	database *models.Database,
) (ExecutionNode, []string, error) {

	qr.logger.Debug("Routing to base QueryPlanner")

	// Get the bundle using BundleService (supports paginated bundles)
	bundle, err := qr.bundleService.GetBundleByName(database, query.FromBundle)
	if err != nil {
		return nil, nil, fmt.Errorf("bundle '%s' not found: %w", query.FromBundle, err)
	}
	if bundle == nil {
		return nil, nil, fmt.Errorf("bundle '%s' not found", query.FromBundle)
	}

	// Create or get document scanner for paginated bundle support
	docScanner, err := qr.bundleService.GetOrCreateDocumentScanner(bundle)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create document scanner for bundle '%s': %w", query.FromBundle, err)
	}

	// Handle GROUP BY queries with manual tree construction
	if query.HasGroupBy() {
		return qr.routeGroupByQuery(query, database, bundle, docScanner)
	}

	// If there's no WHERE clause, create a simple full scan
	if !query.HasWhere() {
		fullScan := &FullScanNode{
			Bundle:           bundle,
			Cost:             float64(bundle.TotalDocuments),
			EstimatedRows:    int(bundle.TotalDocuments),
			Logger:           qr.logger,
			BundleServiceInt: qr.bundleService,
			DocumentScanner:  docScanner,
		}
		return fullScan, []string{}, nil
	}

	// NEW: Use Expression-based planning if WhereExpression is available
	if query.WhereExpression != nil {
		return qr.createExpressionBasedPlan(query, bundle, database, docScanner)
	}

	// No WHERE clause - create simple full scan
	qr.logger.Debug("No WHERE clause, creating full scan plan")
	scanNode := &FullScanNode{
		Bundle:           bundle,
		Cost:             float64(bundle.TotalDocuments),
		EstimatedRows:    int(bundle.TotalDocuments),
		Logger:           qr.logger,
		BundleServiceInt: qr.bundleService,
		DocumentScanner:  docScanner,
	}

	return scanNode, nil, nil
}

// routeGroupByQuery handles GROUP BY queries by creating a base execution tree
// PHASE 3: GROUP BY query routing
//
// Execution tree structure:
//
//	ScanNode → FilterNode (if WHERE exists) → [AggregationNode added by PlanBuilder]
//
// The QueryRouter only creates the base tree (scan + filter).
// The PlanBuilder will add AggregationNode, SortNode, and LimitNode on top.
//
// TODO: Phase 2 - Add intelligent index selection for GROUP BY fields to optimize aggregation
// TODO: Phase 2 - Consider cost-based optimization for choosing between index scan vs full scan
// TODO: Phase 2 - Add support for covering indexes that include both WHERE and GROUP BY fields
func (qr *QueryRouter) routeGroupByQuery(
	query *queryparser.UnifiedSelectQuery,
	database *models.Database,
	bundle *models.Bundle,
	docScanner documentscanner.DocumentScannerInterface,
) (ExecutionNode, []string, error) {

	qr.logger.Debugf("Routing GROUP BY query for bundle '%s'", query.FromBundle)

	var rootNode ExecutionNode
	var indexesUsed []string

	// Step 1: Create scan node (either index scan or full scan)
	// TODO: Phase 2 - Optimize scan selection based on WHERE clause and GROUP BY fields
	// For now, I'll create a full scan as the base
	scanNode := &FullScanNode{
		Bundle:           bundle,
		Cost:             float64(bundle.TotalDocuments),
		EstimatedRows:    int(bundle.TotalDocuments),
		Logger:           qr.logger,
		BundleServiceInt: qr.bundleService,
		DocumentScanner:  docScanner,
	}

	rootNode = scanNode

	// Step 2: Add FilterNode if WHERE clause exists
	if query.HasWhere() {
		qr.logger.Debug("Adding FilterNode for WHERE clause in GROUP BY query")

		// Create BundleContext for qualified field resolution
		bundleCtx := syndrQL.NewBundleContextForSingleBundle(bundle)

		filterNode := &FilterNode{
			Child:            scanNode,
			WhereExpression:  query.WhereExpression,
			BundleContext:    bundleCtx,
			Cost:             scanNode.GetCost(),
			EstimatedRows:    scanNode.GetEstimatedRows() / 2, // Rough estimate: WHERE filters ~50%
			Logger:           qr.logger,
			QueryCache:       qr.queryCache,       // Priority 4: Enable expression caching
			SubqueryExecutor: qr.subqueryExecutor, // TIER 1: Enable subquery support
			Database:         database,            // TIER 1: Database for subquery execution
		}

		rootNode = filterNode

		// TODO: Phase 2 - Track which indexes were considered for the WHERE clause
		// TODO: Phase 2 - Add index statistics to improve EstimatedRows calculation
	}

	qr.logger.Debugf("Created base execution tree for GROUP BY")

	return rootNode, indexesUsed, nil
}

// convertToJoinQuery converts UnifiedSelectQuery to SelectJoinQuery
// PHASE 3: Query format conversion
// LEGACY: JOIN queries still use old parser, so WhereExpression may contain WhereGroup
func (qr *QueryRouter) convertToJoinQuery(query *queryparser.UnifiedSelectQuery) *queryparser.SelectJoinQuery {
	// Try to convert WhereExpression to WhereGroup for legacy JOIN parser
	var whereClause *queryparser.WhereGroup
	if query.WhereExpression != nil {
		if wg, ok := query.WhereExpression.(*queryparser.WhereGroup); ok {
			whereClause = wg
		}
	}

	joinQuery := &queryparser.SelectJoinQuery{
		SelectFields:     query.SelectFields,
		FromBundle:       query.FromBundle,
		JoinClauses:      query.JoinClauses,
		WhereClause:      whereClause,
		WhereExpression:  query.WhereExpression, // Pass Expression for new executor
		OrderBy:          []string{},            // ORDER BY handled by PlanBuilder
		Limit:            query.Limit,
		Offset:           query.Offset,
		RelationshipName: query.RelationshipName,
	}

	return joinQuery
}

// createExpressionBasedPlan creates an execution plan using Expression-based WHERE evaluation
// NEW: Expression-based query planning (replaces old string-based approach)
//
// This method uses the new SyndrQL Expression AST for WHERE clause evaluation,
// enabling qualified field names, better error messages, and improved performance.
//
// Plan structure:
//   - Try to optimize with index scan (hash or BTree)
//   - Fall back to full scan + filter if no index available
//   - Create BundleContext for qualified field resolution
func (qr *QueryRouter) createExpressionBasedPlan(
	query *queryparser.UnifiedSelectQuery,
	bundle *models.Bundle,
	database *models.Database, // TIER 1: Added for subquery support
	docScanner documentscanner.DocumentScannerInterface,
) (ExecutionNode, []string, error) {

	expr, ok := query.WhereExpression.(syndrQL.Expression)
	if !ok {
		return nil, nil, fmt.Errorf("WhereExpression is not a syndrQL.Expression: %T", query.WhereExpression)
	}

	qr.logger.Debugf("Creating Expression-based execution plan for bundle '%s'", bundle.Name)

	// Create BundleContext for qualified field resolution
	// For simple queries, we only have one bundle
	bundleCtx := syndrQL.NewBundleContextForSingleBundle(bundle)

	// Try to optimize with indexes using expression helpers
	indexNode, indexName := qr.tryIndexOptimization(bundle, expr, docScanner)
	if indexNode != nil {
		qr.logger.Infof("Using index '%s' for WHERE clause optimization", indexName)
		// No FilterNode needed - index scan already filters
		return indexNode, []string{indexName}, nil
	}

	// Fall back to full scan + filter
	qr.logger.Debug("No suitable index found, using full scan + filter")

	fullScan := &FullScanNode{
		Bundle:           bundle,
		Cost:             float64(bundle.TotalDocuments),
		EstimatedRows:    int(bundle.TotalDocuments),
		Logger:           qr.logger,
		BundleServiceInt: qr.bundleService,
		DocumentScanner:  docScanner,
	}

	// Create FilterNode with Expression
	filterNode := &FilterNode{
		Child:            fullScan,
		WhereExpression:  expr,
		BundleContext:    bundleCtx,
		Cost:             fullScan.Cost + (float64(bundle.TotalDocuments) * 0.01), // Small cost for filtering
		EstimatedRows:    int(bundle.TotalDocuments) / 2,                          // Assume 50% selectivity
		Logger:           qr.logger,
		DocumentScanner:  docScanner,
		QueryCache:       qr.queryCache,       // Priority 4: Enable expression caching
		SubqueryExecutor: qr.subqueryExecutor, // TIER 1: Enable subquery support
		Database:         database,            // TIER 1: Database for subquery execution
	}

	return filterNode, []string{}, nil
}

// tryIndexOptimization attempts to use an index for the WHERE expression
// Returns (IndexScanNode, indexName) if optimization is possible, (nil, "") otherwise
func (qr *QueryRouter) tryIndexOptimization(
	bundle *models.Bundle,
	expr syndrQL.Expression,
	docScanner documentscanner.DocumentScannerInterface,
) (ExecutionNode, string) {

	// Try hash index optimization for simple equality
	if field, value, ok := syndrQL.ExtractSimpleEquality(expr); ok {
		qr.logger.Debugf("Found simple equality: %s == %v", field, value)

		// Check if hash index exists for this field
		for indexName, indexRef := range bundle.Indexes {
			if indexRef.IndexType == "hash" && len(indexRef.Fields) == 1 && indexRef.Fields[0].Name == field {
				qr.logger.Debugf("Found hash index '%s' on field '%s'", indexName, field)

				return &IndexScanNode{
					Bundle:           bundle,
					IndexName:        indexName,
					ScanType:         HashIndexScan,
					SearchKey:        value,
					Cost:             1.0, // Hash lookup is O(1)
					EstimatedRows:    1,   // Exact match
					Logger:           qr.logger,
					BundleServiceInt: qr.bundleService,
					DocumentScanner:  docScanner,
				}, indexName
			}
		}
	}

	// Try BTree index optimization for range conditions
	if field, operator, value, ok := syndrQL.ExtractRangeCondition(expr); ok {
		qr.logger.Debugf("Found range condition: %s %s %v", field, operator, value)

		// Check if BTree index exists for this field
		for indexName, indexRef := range bundle.Indexes {
			if indexRef.IndexType == "btree" && len(indexRef.Fields) == 1 && indexRef.Fields[0].Name == field {
				qr.logger.Debugf("Found BTree index '%s' on field '%s'", indexName, field)

				return &IndexScanNode{
					Bundle:           bundle,
					IndexName:        indexName,
					ScanType:         BTreeRangeScan,
					Operator:         operator,
					RangeStart:       value,
					RangeEnd:         value,
					Cost:             float64(bundle.TotalDocuments) * 0.1, // BTree scan
					EstimatedRows:    int(bundle.TotalDocuments) / 10,      // Estimate
					Logger:           qr.logger,
					BundleServiceInt: qr.bundleService,
					DocumentScanner:  docScanner,
				}, indexName
			}
		}
	}

	// Try to optimize AND clauses (can use multiple indexes)
	clauses := syndrQL.ExtractANDClauses(expr)
	if len(clauses) > 1 {
		// Try to find an index for any of the AND clauses
		for _, clause := range clauses {
			if node, indexName := qr.tryIndexOptimization(bundle, clause, docScanner); node != nil {
				// Found an index for one clause
				// TODO: Add FilterNode for remaining clauses
				return node, indexName
			}
		}
	}

	// No index optimization possible
	return nil, ""
}

// convertWhereClauseToString converts WhereGroup to string format
// PHASE 3: WHERE clause conversion
//
// Note: This is a simplified conversion for compatibility with existing planner.
// The existing planner will parse it back into WhereGroup internally.
// This maintains backward compatibility while we gradually enhance the system.
func (qr *QueryRouter) convertWhereClauseToString(whereGroup *queryparser.WhereGroup) string {
	if whereGroup == nil {
		return ""
	}

	// For now, we'll reconstruct a simplified WHERE clause string
	// This is acceptable because the existing planner will parse it correctly
	// TODO: Pass WhereGroup directly when we enhance the base planner interface

	if len(whereGroup.Clauses) == 0 {
		return ""
	}

	// Simple conversion for single clause (most common case)
	if len(whereGroup.Clauses) == 1 && len(whereGroup.SubGroups) == 0 {
		clause := whereGroup.Clauses[0]
		valueStr := qr.formatWhereValue(clause.Value)
		// CRITICAL FIX: Don't add quotes around field name - string will be re-parsed
		return fmt.Sprintf("%s %s %s", clause.Field, clause.Operator, valueStr)
	}

	// For more complex clauses, build a WHERE string
	// This is a simplified implementation - the existing planner will handle the full parsing
	return qr.buildWhereString(whereGroup)
}

// buildWhereString recursively builds WHERE clause string
// PHASE 3: Helper for WHERE clause string construction
func (qr *QueryRouter) buildWhereString(whereGroup *queryparser.WhereGroup) string {
	if whereGroup == nil {
		return ""
	}

	var parts []string

	// Add direct clauses
	for _, clause := range whereGroup.Clauses {
		valueStr := qr.formatWhereValue(clause.Value)
		// CRITICAL FIX: Don't add quotes around field names - this string will be re-parsed
		// Adding quotes causes the filter parser to see "age" instead of age
		parts = append(parts, fmt.Sprintf("%s %s %s", clause.Field, clause.Operator, valueStr))
	}

	// Add subgroups
	for _, subGroup := range whereGroup.SubGroups {
		if subStr := qr.buildWhereString(&subGroup); subStr != "" {
			parts = append(parts, fmt.Sprintf("(%s)", subStr))
		}
	}

	// Join with operator
	operator := " AND "
	if whereGroup.Operator == "OR" {
		operator = " OR "
	}

	result := ""
	for i, part := range parts {
		if i > 0 {
			result += operator
		}
		result += part
	}

	return result
}

// formatWhereValue formats a WHERE clause value for string representation
// Strings are quoted, other types use their natural representation
func (qr *QueryRouter) formatWhereValue(value interface{}) string {
	if value == nil {
		return "null"
	}

	switch v := value.(type) {
	case string:
		// Strings must be quoted
		return fmt.Sprintf("\"%s\"", v)
	case int, int32, int64, uint, uint32, uint64:
		// Integers don't need quotes
		return fmt.Sprintf("%d", v)
	case float32, float64:
		// Floats don't need quotes
		return fmt.Sprintf("%f", v)
	case bool:
		// Booleans don't need quotes
		return fmt.Sprintf("%t", v)
	default:
		// Fallback: convert to string and quote it
		return fmt.Sprintf("\"%v\"", v)
	}
}
