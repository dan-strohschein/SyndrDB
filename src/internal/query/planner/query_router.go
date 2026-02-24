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
	"strings"

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

	// TIER 3: Correlated subquery support
	correlationAnalyzer *CorrelationAnalyzer
	subqueryRewriter    *SubqueryRewriter

	// COST INTELLIGENCE: Column statistics store for cost-based optimization
	statsStore             *StatsStore
	costModel              *CostModel
	selectivityEstimator   *SelectivityEstimator

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

	// TIER 3: Initialize correlated subquery infrastructure
	correlationAnalyzer := NewCorrelationAnalyzer(logger)
	// SubqueryRewriter needs statistics storage; pass nil — strategy analysis handles nil gracefully
	subqueryRewriter := NewSubqueryRewriter(correlationAnalyzer, nil, nil, logger)

	return &QueryRouter{
		basePlanner:         basePlanner,
		joinPlanner:         joinPlanner,
		bundleService:       bundleService,
		queryCache:          queryCache,
		correlationAnalyzer: correlationAnalyzer,
		subqueryRewriter:    subqueryRewriter,
		costModel:           NewCostModel(),
		logger:              logger,
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

	// Semantic analysis for JOIN queries: register all involved bundles
	if query.WhereExpression != nil {
		analyzer := syndrQL.NewSemanticAnalyzer(qr.logger).
			SetPrimaryBundle(query.FromBundle)
		if primaryBundle, bErr := qr.bundleService.GetBundleByName(database, query.FromBundle); bErr == nil && primaryBundle != nil {
			analyzer.WithBundle(primaryBundle)
		}
		for _, jc := range query.JoinClauses {
			if joinedBundle, bErr := qr.bundleService.GetBundleByName(database, jc.RightBundle); bErr == nil && joinedBundle != nil {
				analyzer.WithBundle(joinedBundle)
			}
		}
		if whereExpr, ok := query.WhereExpression.(syndrQL.Expression); ok {
			if err := analyzer.AnalyzeExpression(whereExpr); err != nil {
				qr.logger.Warnf("Semantic analysis warning for JOIN WHERE: %v", err)
			}
		}
	}

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

	// Semantic analysis: annotate WHERE expression nodes with ResolvedType
	if query.WhereExpression != nil {
		if whereExpr, ok := query.WhereExpression.(syndrQL.Expression); ok {
			analyzer := syndrQL.NewSemanticAnalyzer(qr.logger).
				WithBundle(bundle).
				SetPrimaryBundle(query.FromBundle)
			if err := analyzer.AnalyzeExpression(whereExpr); err != nil {
				qr.logger.Warnf("Semantic analysis warning for WHERE clause: %v", err)
			}
		}
	}

	// Handle GROUP BY queries with manual tree construction
	if query.HasGroupBy() {
		return qr.routeGroupByQuery(query, database, bundle, docScanner)
	}

	// OPTIMIZATION: ORDER BY with B-tree index optimization
	// For queries like: SELECT ... FROM bundle ORDER BY field [ASC|DESC] [LIMIT n]
	// If a B-tree index exists on the ORDER BY field, use BTreeOrderedScanNode
	// to avoid loading all documents and sorting. The index provides pre-sorted iteration.
	if !query.HasWhere() && query.HasOrderBy() && query.OrderBy != nil && len(query.OrderBy.Fields) == 1 {
		orderField := query.OrderBy.Fields[0]
		fieldName := extractFieldNameForProjection(orderField.FieldName)
		if idxName, found := findBTreeIndexForGroupByField(bundle, fieldName); found {
			// Calculate effective limit (LIMIT + OFFSET)
			limit := 0
			if query.HasLimit() {
				limit = query.GetEffectiveLimit()
				if query.Offset > 0 {
					limit = query.Offset + query.GetEffectiveLimit()
				}
			}

			isDescending := orderField.Direction == queryparser.SortDesc
			directionStr := "ASC"
			if isDescending {
				directionStr = "DESC"
			}

			qr.logger.Debugf("ORDER BY INDEX OPTIMIZATION: Using BTreeOrderedScanNode on index '%s' for ORDER BY %s %s (limit=%d)",
				idxName, fieldName, directionStr, limit)

			// Return BTreeOrderedScanNode - this returns documents already sorted
			// The PlanBuilder will still add SortNode, but it will be a no-op since data is already sorted
			// TODO: Consider adding a flag to skip SortNode entirely when using BTreeOrderedScanNode
			orderedNode := &BTreeOrderedScanNode{
				Bundle:             bundle,
				IndexName:          idxName,
				OrderedByFieldName: fieldName,
				Logger:             qr.logger,
				BundleServiceInt:   qr.bundleService,
				Cost:               qr.costModel.BTreeOrderedScanCost(bundle.TotalDocuments, limit),
				EstimatedRows:      limit,
				Descending:         isDescending,
				Limit:              limit,
			}

			if limit == 0 {
				orderedNode.Cost = qr.costModel.BTreeOrderedScanCost(bundle.TotalDocuments, 0)
				orderedNode.EstimatedRows = int(bundle.TotalDocuments)
			}

			return orderedNode, []string{idxName}, nil
		}
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
		Cost:             qr.costModel.FullScanCost(bundle.TotalDocuments),
		EstimatedRows:    int(bundle.TotalDocuments),
		Logger:           qr.logger,
		BundleServiceInt: qr.bundleService,
		DocumentScanner:  docScanner,
	}

	return scanNode, nil, nil
}

// extractFieldNameForProjection strips bundle qualifiers from a field reference.
// e.g. "Authors"."Name" -> Name, "name" -> name
func extractFieldNameForProjection(qualifiedName string) string {
	s := strings.Trim(qualifiedName, "\"'")
	parts := strings.Split(s, ".")
	if len(parts) > 1 {
		return strings.Trim(parts[len(parts)-1], "\"'")
	}
	return s
}

// getProjectionFieldsForGroupBy returns the distinct set of field names needed for
// GROUP BY and aggregates: GroupBy.Fields plus each agg.Field for SUM/AVG/MIN/MAX/COUNT(field).
// Used for projection pushdown so the storage layer deserializes only those fields.
func getProjectionFieldsForGroupBy(query *queryparser.UnifiedSelectQuery) []string {
	seen := make(map[string]struct{})
	var out []string
	add := func(name string) {
		if name == "" {
			return
		}
		if _, ok := seen[name]; ok {
			return
		}
		seen[name] = struct{}{}
		out = append(out, name)
	}
	if query.GroupBy != nil {
		for _, f := range query.GroupBy.Fields {
			add(extractFieldNameForProjection(f))
		}
	}
	for _, a := range query.AggregateFields {
		if a.Field != "*" {
			add(extractFieldNameForProjection(a.Field))
		}
	}
	return out
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

	// Semantic analysis for GROUP BY queries: annotate WHERE and HAVING expressions
	if query.WhereExpression != nil || query.HavingExpression != nil {
		analyzer := syndrQL.NewSemanticAnalyzer(qr.logger).
			WithBundle(bundle).
			SetPrimaryBundle(query.FromBundle)
		if query.WhereExpression != nil {
			if whereExpr, ok := query.WhereExpression.(syndrQL.Expression); ok {
				if err := analyzer.AnalyzeExpression(whereExpr); err != nil {
					qr.logger.Warnf("Semantic analysis warning for GROUP BY WHERE: %v", err)
				}
			}
		}
		if query.HavingExpression != nil {
			if havingExpr, ok := query.HavingExpression.(syndrQL.Expression); ok {
				if err := analyzer.AnalyzeExpression(havingExpr); err != nil {
					qr.logger.Warnf("Semantic analysis warning for HAVING: %v", err)
				}
			}
		}
	}

	var rootNode ExecutionNode
	var indexesUsed []string

	// When WHERE exists and WhereExpression is set, use createExpressionBasedPlan to allow
	// index optimization (hash/BTree) for the WHERE clause. Fallback is FullScan+FilterNode.
	if query.HasWhere() && query.WhereExpression != nil {
		var err error
		rootNode, indexesUsed, err = qr.createExpressionBasedPlan(query, bundle, database, docScanner)
		if err != nil {
			return nil, nil, err
		}
		qr.logger.Debugf("Created base execution tree for GROUP BY using createExpressionBasedPlan (indexes: %v)", indexesUsed)
		return rootNode, indexesUsed, nil
	}

	// Index-ordered scan for single-field GROUP BY when no WHERE: use B-tree full range
	// to supply rows already ordered by the GROUP BY field, so AggregationNode can skip the sort.
	// WRITE-THROUGH CACHE: Documents always available through page cache via BundleServiceInt.GetDocument()
	// if !query.HasWhere() && query.GroupBy != nil && len(query.GroupBy.Fields) == 1 {
	// 	fieldName := extractFieldNameForProjection(query.GroupBy.Fields[0])
	// 	if idxName, found := findBTreeIndexForGroupByField(bundle, fieldName); found {
	// 		rootNode = &BTreeOrderedScanNode{
	// 			Bundle:             bundle,
	// 			IndexName:          idxName,
	// 			OrderedByFieldName: fieldName,
	// 			Logger:             qr.logger,
	// 			BundleServiceInt:   qr.bundleService,
	// 			Cost:               float64(bundle.TotalDocuments) * 0.5, // cheaper than full scan + sort
	// 			EstimatedRows:      int(bundle.TotalDocuments),
	// 		}
	// 		indexesUsed = []string{idxName}
	// 		qr.logger.Debugf("Using BTreeOrderedScanNode on index %s for GROUP BY %s (skip sort)", idxName, fieldName)
	// 		return rootNode, indexesUsed, nil
	// 	}
	// }

	// No WHERE, or legacy WHERE (WhereExpression is nil): create full scan as the base
	scanNode := &FullScanNode{
		Bundle:           bundle,
		Cost:             qr.costModel.FullScanCost(bundle.TotalDocuments),
		EstimatedRows:    int(bundle.TotalDocuments),
		Logger:           qr.logger,
		BundleServiceInt: qr.bundleService,
		DocumentScanner:  docScanner,
	}

	// PROJECTION PUSHDOWN FOR GROUP BY: Only deserialize fields needed for aggregation
	// This dramatically improves performance for GROUP BY queries on bundles with many fields
	// The deserializer now supports case-insensitive field matching, so this is safe to enable
	if projectionFields := getProjectionFieldsForGroupBy(query); len(projectionFields) > 0 {
		scanNode.ProjectionFields = projectionFields
		qr.logger.Debugf("PROJECTION PUSHDOWN: Set ProjectionFields=%v on FullScanNode for GROUP BY optimization", projectionFields)
	}

	rootNode = scanNode

	// Add FilterNode if WHERE clause exists (legacy path: WhereExpression is nil but HasWhere)
	if query.HasWhere() {
		qr.logger.Debug("Adding FilterNode for WHERE clause in GROUP BY query (legacy path)")

		bundleCtx := syndrQL.NewBundleContextForSingleBundle(bundle)

		// Use selectivity estimator for accurate row estimates when available
		groupByFilterRows := scanNode.GetEstimatedRows() / 2
		if qr.selectivityEstimator != nil {
			if whereExpr, ok := query.WhereExpression.(syndrQL.Expression); ok {
				groupByFilterRows = int(qr.selectivityEstimator.EstimateRows(whereExpr, bundle.Name, bundle.TotalDocuments))
			}
		}
		filterNode := &FilterNode{
			Child:            scanNode,
			WhereExpression:  query.WhereExpression,
			BundleContext:    bundleCtx,
			Cost:             qr.costModel.FilterCost(scanNode.GetCost(), bundle.TotalDocuments, 1),
			EstimatedRows:    groupByFilterRows,
			Logger:           qr.logger,
			QueryCache:       qr.queryCache,
			SubqueryExecutor: qr.subqueryExecutor,
			Database:         database,
		}

		rootNode = filterNode
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
		OrderBy:          []string{},            // ORDER BY handled by PlanBuilder (fields stored separately)
		Limit:            query.Limit,
		Offset:           query.Offset,
		RelationshipName: query.RelationshipName,
	}

	// Store ORDER BY clause in the query for streaming Top-N optimization
	// The PlanBuilder will also wrap with SortNode, but JoinExecutionNode can use this
	// to perform streaming Top-N during merge when LIMIT is present
	if query.OrderBy != nil {
		joinQuery.OrderByClause = query.OrderBy
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

	// TIER 3: Detect and handle correlated subqueries (EXISTS/NOT EXISTS with cross-bundle references)
	if correlatedNode, usedIndexes, err := qr.tryCorrelatedSubqueryPlan(query, expr, bundle, database, docScanner, bundleCtx); err != nil {
		return nil, nil, err
	} else if correlatedNode != nil {
		return correlatedNode, usedIndexes, nil
	}

	// Try to optimize with indexes using expression helpers
	indexNode, indexName := qr.tryIndexOptimization(bundle, expr, docScanner)
	if indexNode != nil {
		qr.logger.Debugf("Using index '%s' for WHERE clause optimization", indexName)

		// Check if index-only scan is possible (all needed fields in the index)
		if indexRef, exists := bundle.Indexes[indexName]; exists && isQueryCoveredByIndex(query, indexRef) {
			qr.logger.Debugf("INDEX-ONLY SCAN: query fully covered by index '%s'", indexName)
			return &IndexOnlyScanNode{
				Child:           indexNode,
				Bundle:          bundle,
				IndexName:       indexName,
				IndexRef:        indexRef,
				Cost:            indexNode.GetCost() * 0.5, // 50% cheaper: no heap fetch
				EstimatedRows:   indexNode.GetEstimatedRows(),
				Logger:          qr.logger,
			}, []string{indexName}, nil
		}

		// No FilterNode needed - index scan already filters
		return indexNode, []string{indexName}, nil
	}

	// Fall back to full scan + filter
	qr.logger.Debug("No suitable index found, using full scan + filter")

	fullScan := &FullScanNode{
		Bundle:           bundle,
		Cost:             qr.costModel.FullScanCost(bundle.TotalDocuments),
		EstimatedRows:    int(bundle.TotalDocuments),
		Logger:           qr.logger,
		BundleServiceInt: qr.bundleService,
		DocumentScanner:  docScanner,
	}

	// PREDICATE PUSHDOWN: Try to compile the WHERE expression into a scanner predicate.
	// When successful, the FullScanNode uses ScanWithPredicate to filter during page iteration,
	// avoiding copying non-matching documents. FilterNode still applies the full expression
	// for correctness (handles edge cases the simple predicate compiler may miss).
	if predicate := compileExpressionToPredicate(expr, bundleCtx, qr.logger); predicate != nil {
		fullScan.Predicate = predicate
		qr.logger.Debugf("PREDICATE PUSHDOWN: Compiled WHERE expression to scanner predicate")
	}

	// PAGE BLOOM FILTER: Extract equality predicates as bloom hints for page-skip optimization.
	// When the scanner has a page bloom map, these hints let it skip pages where the
	// searched field-value pairs are definitely absent.
	if hints := extractBloomHints(expr); len(hints) > 0 {
		fullScan.BloomHints = hints
		qr.logger.Debugf("PAGE BLOOM: Extracted %d bloom hints from WHERE expression", len(hints))
	}

	// Create FilterNode with Expression
	// Use cost model and selectivity estimator for accurate cost/row estimates
	estimatedFilterRows := int(bundle.TotalDocuments) / 2 // Default: 50% selectivity
	if qr.selectivityEstimator != nil {
		estimatedFilterRows = int(qr.selectivityEstimator.EstimateRows(expr, bundle.Name, bundle.TotalDocuments))
	}
	filterNode := &FilterNode{
		Child:            fullScan,
		WhereExpression:  expr,
		BundleContext:    bundleCtx,
		Cost:             qr.costModel.FilterCost(fullScan.Cost, bundle.TotalDocuments, countPredicates(expr)),
		EstimatedRows:    estimatedFilterRows,
		Logger:           qr.logger,
		DocumentScanner:  docScanner,
		QueryCache:       qr.queryCache,       // Priority 4: Enable expression caching
		SubqueryExecutor: qr.subqueryExecutor, // TIER 1: Enable subquery support
		Database:         database,            // TIER 1: Database for subquery execution
	}

	// PERFORMANCE OPTIMIZATION: LIMIT pushdown for non-ORDER BY queries
	// When LIMIT is present but ORDER BY is not, we can stop filtering after finding enough matches.
	// This is safe because without ORDER BY, result order is undefined.
	// NOTE: We also require no GROUP BY since aggregation needs all matching documents.
	if query.HasLimit() && !query.HasOrderBy() && !query.HasGroupBy() && !query.IsDistinct && !query.IsAggregateOnly {
		effectiveLimit := query.Limit
		if query.Offset > 0 {
			effectiveLimit = query.Offset + query.Limit // Need extra for OFFSET
		}
		filterNode.MaxDocuments = effectiveLimit
		qr.logger.Debugf("OPTIMIZATION: LIMIT pushdown to FilterNode: MaxDocuments=%d (LIMIT %d + OFFSET %d)",
			effectiveLimit, query.Limit, query.Offset)
	}

	return filterNode, []string{}, nil
}

// tryCorrelatedSubqueryPlan detects correlated subqueries in the WHERE expression
// and creates a CorrelatedSubqueryNode when a semi-join/anti-join strategy is chosen.
// Returns (nil, nil, nil) if no correlated subquery is found or if the fallback path should be used.
func (qr *QueryRouter) tryCorrelatedSubqueryPlan(
	query *queryparser.UnifiedSelectQuery,
	expr syndrQL.Expression,
	bundle *models.Bundle,
	database *models.Database,
	docScanner documentscanner.DocumentScannerInterface,
	bundleCtx *syndrQL.BundleContext,
) (ExecutionNode, []string, error) {

	// Find subqueries in the WHERE expression
	subqueries := findSubqueries(expr)
	if len(subqueries) == 0 {
		return nil, nil, nil
	}

	// Build a temporary SelectStatement for the rewriter (it needs outer query context)
	outerSelect := &syndrQL.SelectStatement{
		BundleName: query.FromBundle,
	}

	// Run rewriter directly on the expression tree to analyze and annotate all subqueries
	if err := qr.subqueryRewriter.rewriteExpression(outerSelect, expr, 0); err != nil {
		qr.logger.Warnf("Expression rewrite failed: %v, falling back to standard plan", err)
		return nil, nil, nil
	}

	// Re-scan subqueries after rewriting (types may have changed, e.g., EXISTS → NOT_EXISTS)
	subqueries = findSubqueries(expr)

	// Check if any correlated subquery has a semi-join or anti-join strategy
	for _, subExpr := range subqueries {
		if subExpr.RewriteStrategy != REWRITE_SEMI_JOIN && subExpr.RewriteStrategy != REWRITE_ANTI_JOIN {
			continue
		}

		if len(subExpr.OuterJoinFields) == 0 || len(subExpr.InnerJoinFields) == 0 {
			qr.logger.Warnf("Correlated subquery has no join fields, falling back")
			continue
		}

		// Determine join type
		joinType := SEMI_JOIN
		if subExpr.RewriteStrategy == REWRITE_ANTI_JOIN {
			joinType = ANTI_JOIN
		}

		// Get inner bundle
		innerBundleName := subExpr.InnerQuery.BundleName
		innerBundle, err := qr.bundleService.GetBundleByName(database, innerBundleName)
		if err != nil {
			qr.logger.Warnf("Cannot get inner bundle '%s': %v, falling back", innerBundleName, err)
			continue
		}

		innerScanner, err := qr.bundleService.GetOrCreateDocumentScanner(innerBundle)
		if err != nil {
			qr.logger.Warnf("Cannot create inner scanner: %v, falling back", err)
			continue
		}

		// Create outer scan node
		outerScan := &FullScanNode{
			Bundle:           bundle,
			Cost:             qr.costModel.FullScanCost(bundle.TotalDocuments),
			EstimatedRows:    int(bundle.TotalDocuments),
			Logger:           qr.logger,
			BundleServiceInt: qr.bundleService,
			DocumentScanner:  docScanner,
		}

		// Create inner scan node
		innerScan := &FullScanNode{
			Bundle:           innerBundle,
			Cost:             qr.costModel.FullScanCost(innerBundle.TotalDocuments),
			EstimatedRows:    int(innerBundle.TotalDocuments),
			Logger:           qr.logger,
			BundleServiceInt: qr.bundleService,
			DocumentScanner:  innerScanner,
		}

		// If inner query has non-correlated WHERE predicates, wrap inner scan in FilterNode
		var innerNode ExecutionNode = innerScan
		if subExpr.NonCorrelatedWhere != nil {
			innerBundleCtx := syndrQL.NewBundleContextForSingleBundle(innerBundle)
			innerNode = &FilterNode{
				Child:           innerScan,
				WhereExpression: subExpr.NonCorrelatedWhere,
				BundleContext:   innerBundleCtx,
				Cost:            qr.costModel.FilterCost(innerScan.Cost, innerBundle.TotalDocuments, 1),
				EstimatedRows:   int(innerBundle.TotalDocuments) / 2,
				Logger:          qr.logger,
				DocumentScanner: innerScanner,
			}
		}

		// Extract remaining non-subquery predicates from outer WHERE
		remainingFilter := extractNonSubqueryPredicates(expr, subExpr)

		// Create CorrelatedSubqueryNode
		correlatedNode := &CorrelatedSubqueryNode{
			OuterChild:      outerScan,
			InnerChild:      innerNode,
			JoinType:        joinType,
			OuterJoinFields: subExpr.OuterJoinFields,
			InnerJoinFields: subExpr.InnerJoinFields,
			RemainingFilter: remainingFilter,
			BundleContext:   bundleCtx,
			Cost:            EstimateSemiJoinCost(int64(bundle.TotalDocuments), int64(innerBundle.TotalDocuments)),
			EstimatedRows:   int(EstimateSemiJoinCardinality(int64(bundle.TotalDocuments), int64(innerBundle.TotalDocuments), 0.5)),
			Logger:          qr.logger,
		}

		qr.logger.Infof("Created CorrelatedSubqueryNode: %s on %s.%v ↔ %s.%v",
			joinTypeName(joinType), bundle.Name, subExpr.OuterJoinFields,
			innerBundleName, subExpr.InnerJoinFields)

		return correlatedNode, []string{}, nil
	}

	// No correlated subquery converted to semi-join/anti-join
	return nil, nil, nil
}

// extractNonSubqueryPredicates extracts the non-subquery portions of a WHERE expression.
// Given "A AND EXISTS(...)" or "NOT EXISTS(...) AND B", returns just "A" or "B".
// Returns nil if the entire expression is the subquery.
func extractNonSubqueryPredicates(expr syndrQL.Expression, target *syndrQL.SubqueryExpression) syndrQL.Expression {
	if expr == nil {
		return nil
	}

	switch e := expr.(type) {
	case *syndrQL.SubqueryExpression:
		if e == target {
			return nil // This IS the subquery, no remaining predicates
		}
		return expr

	case *syndrQL.UnaryExpression:
		// NOT EXISTS(...) — check if it wraps the target
		if sub, ok := e.Right.(*syndrQL.SubqueryExpression); ok && sub == target {
			return nil
		}
		return expr

	case *syndrQL.BinaryExpression:
		if e.Operator == syndrQL.TOKEN_AND {
			left := extractNonSubqueryPredicates(e.Left, target)
			right := extractNonSubqueryPredicates(e.Right, target)

			if left == nil && right == nil {
				return nil
			}
			if left == nil {
				return right
			}
			if right == nil {
				return left
			}
			return &syndrQL.BinaryExpression{
				Left:     left,
				Operator: syndrQL.TOKEN_AND,
				Right:    right,
			}
		}

		// For IN/NOT IN: check right side for subquery
		if e.Operator == syndrQL.TOKEN_IN || e.Operator == syndrQL.TOKEN_NOTIN {
			if sub, ok := e.Right.(*syndrQL.SubqueryExpression); ok && sub == target {
				return nil
			}
		}
		return expr

	default:
		return expr
	}
}

// tryIndexOptimization attempts to use an index for the WHERE expression
// Returns (IndexScanNode, indexName) if optimization is possible, (nil, "") otherwise
func (qr *QueryRouter) tryIndexOptimization(
	bundle *models.Bundle,
	expr syndrQL.Expression,
	docScanner documentscanner.DocumentScannerInterface,
) (ExecutionNode, string) {

	// Try expression index optimization for function-call equality (e.g., LOWER(name) = 'john')
	if funcName, fieldName, value, ok := syndrQL.ExtractFunctionCallEquality(expr); ok {
		exprStr := funcName + "(" + fieldName + ")"
		qr.logger.Debugf("INDEX OPTIMIZATION: Found expression equality: %s = %v", exprStr, value)

		for indexName, indexRef := range bundle.Indexes {
			if indexRef.Expression != "" && strings.EqualFold(indexRef.Expression, exprStr) {
				// Skip partial indexes whose predicate is not implied by the query
				if indexRef.WherePredicate != "" && !queryImpliesIndexPredicate(expr, indexRef.WherePredicate) {
					continue
				}
				qr.logger.Debugf("INDEX SELECTED: Using expression index '%s' for %s", indexName, exprStr)
				return &IndexScanNode{
					Bundle:           bundle,
					IndexName:        indexName,
					ScanType:         BTreeIndexScan,
					SearchKey:        value,
					Cost:             qr.costModel.HashIndexScanCost(1),
					EstimatedRows:    1,
					Logger:           qr.logger,
					BundleServiceInt: qr.bundleService,
					DocumentScanner:  docScanner,
				}, indexName
			}
		}
	}

	// Try hash index optimization for simple equality
	if field, value, ok := syndrQL.ExtractSimpleEquality(expr); ok {
		qr.logger.Debugf("INDEX OPTIMIZATION: Found simple equality: %s == %v", field, value)

		// Check if hash index exists for this field
		for indexName, indexRef := range bundle.Indexes {
			if indexRef.IndexType == "hash" {
				// Skip partial indexes whose predicate is not implied by the query
				if indexRef.WherePredicate != "" && !queryImpliesIndexPredicate(expr, indexRef.WherePredicate) {
					continue
				}
				// Hash indexes use HashIndexField.FieldName, not Fields array
				hashFieldName := indexRef.HashIndexField.FieldName
				qr.logger.Debugf("INDEX CHECK: index='%s' type='hash' hashField='%s' matchesField=%v",
					indexName, hashFieldName, hashFieldName == field)

				if hashFieldName == field {
					qr.logger.Debugf("INDEX SELECTED: Using hash index '%s' on field '%s'", indexName, field)

					return &IndexScanNode{
						Bundle:           bundle,
						IndexName:        indexName,
						ScanType:         HashIndexScan,
						SearchKey:        value,
						Cost:             qr.costModel.HashIndexScanCost(1),
						EstimatedRows:    1,
						Logger:           qr.logger,
						BundleServiceInt: qr.bundleService,
						DocumentScanner:  docScanner,
					}, indexName
				}
			}
		}
		qr.logger.Debugf("INDEX OPTIMIZATION: No hash index found for field '%s'", field)
	} else {
		qr.logger.Debugf("INDEX OPTIMIZATION: ExtractSimpleEquality failed - not a simple equality expression")
	}

	// Try BTree index optimization for range conditions
	if field, operator, value, ok := syndrQL.ExtractRangeCondition(expr); ok {
		qr.logger.Debugf("Found range condition: %s %s %v", field, operator, value)

		// Check if BTree index exists for this field
		for indexName, indexRef := range bundle.Indexes {
			if indexRef.IndexType == "btree" && len(indexRef.Fields) == 1 && indexRef.Fields[0].Name == field && indexRef.Expression == "" {
				// Skip partial indexes whose predicate is not implied by the query
				if indexRef.WherePredicate != "" && !queryImpliesIndexPredicate(expr, indexRef.WherePredicate) {
					continue
				}
				qr.logger.Debugf("Found BTree index '%s' on field '%s'", indexName, field)

				return &IndexScanNode{
					Bundle:           bundle,
					IndexName:        indexName,
					ScanType:         BTreeRangeScan,
					Operator:         operator,
					RangeStart:       value,
					RangeEnd:         value,
					Cost:             qr.costModel.BTreeRangeScanCost(bundle.TotalDocuments, bundle.TotalDocuments/10),
					EstimatedRows:    int(bundle.TotalDocuments) / 10,
					Logger:           qr.logger,
					BundleServiceInt: qr.bundleService,
					DocumentScanner:  docScanner,
				}, indexName
			}
		}
	}

	// Try BRIN index optimization for range conditions
	if field, operator, value, ok := syndrQL.ExtractRangeCondition(expr); ok {
		for indexName, indexRef := range bundle.Indexes {
			if indexRef.IndexType == "brin" && len(indexRef.Fields) == 1 && indexRef.Fields[0].Name == field {
				qr.logger.Debugf("Found BRIN index '%s' on field '%s' for operator '%s'", indexName, field, operator)
				return &BRINScanNode{
					Bundle:           bundle,
					IndexName:        indexName,
					FieldName:        field,
					Operator:         operator,
					SearchValue:      value,
					Cost:             float64(bundle.TotalDocuments) * 0.05,
					EstimatedRows:    int(bundle.TotalDocuments) / 20,
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
		// Try multi-column B-tree index (left-prefix matching)
		for indexName, indexRef := range bundle.Indexes {
			if indexRef.IndexType != "btree" || len(indexRef.Fields) <= 1 {
				continue
			}
			// Check left-prefix match: query has equality on a left prefix of index fields?
			matchedFields := 0
			matchedValues := make([]interface{}, 0, len(indexRef.Fields))
			for _, fieldDef := range indexRef.Fields {
				found := false
				for _, clause := range clauses {
					if f, val, ok := syndrQL.ExtractSimpleEquality(clause); ok && f == fieldDef.Name {
						matchedValues = append(matchedValues, val)
						found = true
						break
					}
				}
				if !found {
					break // Left prefix broken
				}
				matchedFields++
			}
			if matchedFields >= 2 {
				qr.logger.Debugf("MULTI-COLUMN INDEX: Using '%s' with %d prefix fields", indexName, matchedFields)
				// Build composite search key from matched values
				encoder := NewKeyEncoder()
				compositeKey, encErr := encoder.EncodeCompositeKey(matchedValues)
				if encErr != nil {
					qr.logger.Warnf("Failed to encode composite key: %v", encErr)
					continue
				}
				return &IndexScanNode{
					Bundle:           bundle,
					IndexName:        indexName,
					ScanType:         BTreeIndexScan,
					SearchKey:        compositeKey,
					Cost:             qr.costModel.BTreeRangeScanCost(bundle.TotalDocuments, bundle.TotalDocuments/100),
					EstimatedRows:    int(bundle.TotalDocuments) / 100,
					Logger:           qr.logger,
					BundleServiceInt: qr.bundleService,
					DocumentScanner:  docScanner,
				}, indexName
			}
		}

		// Try to find an index for any single AND clause
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

// isQueryCoveredByIndex checks if all columns needed by the query
// (SELECT fields, WHERE fields) are present in the index.
// Returns true if the index can satisfy the query without a heap fetch.
func isQueryCoveredByIndex(query *queryparser.UnifiedSelectQuery, indexRef models.IndexReference) bool {
	if query == nil {
		return false
	}

	// SELECT * requires all fields - can't be covered by index
	for _, sf := range query.SelectFields {
		if sf == "*" {
			return false
		}
	}

	// Aggregate-only queries may need all docs
	if query.IsAggregateOnly && !query.IsCountOnly {
		return false
	}

	// Collect all fields needed by the query
	neededFields := make(map[string]bool)
	for _, sf := range query.SelectFields {
		if sf != "" && sf != "*" {
			neededFields[sf] = true
		}
	}
	// WHERE fields
	if query.WhereExpression != nil {
		if expr, ok := query.WhereExpression.(syndrQL.Expression); ok {
			for _, f := range syndrQL.ExtractFieldNames(expr) {
				neededFields[f] = true
			}
		}
	}
	// ORDER BY fields
	if query.OrderBy != nil {
		for _, ob := range query.OrderBy.Fields {
			neededFields[ob.FieldName] = true
		}
	}

	// COUNT(*) only needs the index to exist, not specific fields
	if query.IsCountOnly && len(neededFields) == 0 {
		return true
	}

	// Check if index covers all needed fields
	// Include both key fields and INCLUDE (covering) fields
	indexedFields := make(map[string]bool)
	for _, fd := range indexRef.Fields {
		indexedFields[fd.Name] = true
	}
	for _, f := range indexRef.IncludeFields {
		indexedFields[f] = true
	}

	for f := range neededFields {
		if !indexedFields[f] {
			return false
		}
	}
	return true
}

// queryImpliesIndexPredicate checks whether the query's WHERE clause implies a partial index predicate.
// For a partial index with predicate P, the query can only use the index if the query's WHERE clause
// logically implies P (i.e., every row matching the query's WHERE also matches P).
//
// Strategy: Parse the index predicate into AND clauses. For each clause, check if the same clause
// (or a stronger one) appears in the query's AND clauses using normalized string comparison.
func queryImpliesIndexPredicate(queryExpr syndrQL.Expression, indexPredicate string) bool {
	if indexPredicate == "" {
		return true // No predicate = all rows qualify
	}

	// Parse the index predicate
	predExpr, err := syndrQL.ParseExpression(indexPredicate)
	if err != nil {
		return false // Can't parse predicate = can't verify implication
	}

	// Extract AND clauses from both
	predClauses := syndrQL.ExtractANDClauses(predExpr)
	queryClauses := syndrQL.ExtractANDClauses(queryExpr)

	// For each predicate clause, check if the query has a matching clause
	for _, predClause := range predClauses {
		predStr := strings.ToLower(syndrQL.ExpressionToString(predClause))
		found := false
		for _, queryClause := range queryClauses {
			queryStr := strings.ToLower(syndrQL.ExpressionToString(queryClause))
			if predStr == queryStr {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}

	return true
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

// compileExpressionToPredicate attempts to compile a simple WHERE expression into a
// func(*Document) bool predicate for scanner-level pushdown. Only handles simple
// binary comparisons (field op literal) and AND combinations thereof.
// Returns nil if the expression is too complex to compile.
func compileExpressionToPredicate(expr syndrQL.Expression, bundleCtx interface{}, logger *zap.SugaredLogger) func(*models.Document) bool {
	if expr == nil {
		return nil
	}

	// Extract schema from bundleCtx for O(1) field lookups in predicates
	var schema *models.BundleFieldSchema
	if bc, ok := bundleCtx.(*syndrQL.BundleContext); ok && bc != nil && bc.PrimaryBundle != "" {
		if b := bc.Bundles[bc.PrimaryBundle]; b != nil {
			schema = b.DocumentStructure.FieldSchema()
		}
	}

	return compileExpressionToPredicateWithSchema(expr, schema, logger)
}

// compileExpressionToPredicateWithSchema is the internal implementation that carries
// the resolved schema through recursive calls.
func compileExpressionToPredicateWithSchema(expr syndrQL.Expression, schema *models.BundleFieldSchema, logger *zap.SugaredLogger) func(*models.Document) bool {
	if expr == nil {
		return nil
	}

	// Unwrap GroupedExpression
	if grouped, ok := expr.(*syndrQL.GroupedExpression); ok {
		return compileExpressionToPredicateWithSchema(grouped.Expression, schema, logger)
	}

	// Handle AND: compile both sides and combine
	if binary, ok := expr.(*syndrQL.BinaryExpression); ok && binary.Operator == syndrQL.TOKEN_AND {
		left := compileExpressionToPredicateWithSchema(binary.Left, schema, logger)
		right := compileExpressionToPredicateWithSchema(binary.Right, schema, logger)
		if left != nil && right != nil {
			return func(doc *models.Document) bool {
				return left(doc) && right(doc)
			}
		}
		// If one side compiles, use it (partial pushdown is still beneficial)
		if left != nil {
			return left
		}
		if right != nil {
			return right
		}
		return nil
	}

	// Handle simple binary comparison: field op literal
	binary, ok := expr.(*syndrQL.BinaryExpression)
	if !ok {
		return nil
	}

	// Extract field name and literal value
	fieldName, literalValue, isFieldOpLiteral := extractFieldAndLiteral(binary)
	if !isFieldOpLiteral {
		return nil
	}

	// Strip quotes from field name
	fieldName = strings.Trim(fieldName, "\"'")
	// Handle qualified names: "BundleName"."FieldName"
	parts := strings.Split(fieldName, ".")
	if len(parts) > 1 {
		fieldName = strings.Trim(parts[len(parts)-1], "\"'")
	}

	// Compile comparison based on operator
	switch binary.Operator {
	case syndrQL.TOKEN_EQ:
		return makeEqPredicate(fieldName, literalValue, schema)
	case syndrQL.TOKEN_NEQ:
		eq := makeEqPredicate(fieldName, literalValue, schema)
		if eq == nil {
			return nil
		}
		return func(doc *models.Document) bool { return !eq(doc) }
	case syndrQL.TOKEN_GT:
		return makeComparisonPredicate(fieldName, literalValue, schema, func(a, b float64) bool { return a > b })
	case syndrQL.TOKEN_GTE:
		return makeComparisonPredicate(fieldName, literalValue, schema, func(a, b float64) bool { return a >= b })
	case syndrQL.TOKEN_LT:
		return makeComparisonPredicate(fieldName, literalValue, schema, func(a, b float64) bool { return a < b })
	case syndrQL.TOKEN_LTE:
		return makeComparisonPredicate(fieldName, literalValue, schema, func(a, b float64) bool { return a <= b })
	default:
		return nil // Complex operators (LIKE, IN, etc.) not supported in pushdown
	}
}

// extractFieldAndLiteral extracts field name and literal value from a binary expression.
// Handles both field op literal and literal op field orderings.
func extractFieldAndLiteral(binary *syndrQL.BinaryExpression) (string, interface{}, bool) {
	// Try: field op literal
	if ident, ok := binary.Left.(*syndrQL.IdentifierExpression); ok {
		if lit, ok := binary.Right.(*syndrQL.LiteralExpression); ok {
			return ident.Name, lit.Value, true
		}
	}
	// Try: literal op field (reversed)
	if lit, ok := binary.Left.(*syndrQL.LiteralExpression); ok {
		if ident, ok := binary.Right.(*syndrQL.IdentifierExpression); ok {
			return ident.Name, lit.Value, true
		}
	}
	// Try qualified identifier on left: "bundle"."field"
	if qual, ok := binary.Left.(*syndrQL.QualifiedIdentifierExpression); ok {
		if lit, ok := binary.Right.(*syndrQL.LiteralExpression); ok {
			return qual.Field, lit.Value, true
		}
	}
	return "", nil, false
}

func makeEqPredicate(fieldName string, literalValue interface{}, schema *models.BundleFieldSchema) func(*models.Document) bool {
	// Pre-compute lowercase field name for CI fallback
	lowerFieldName := strings.ToLower(fieldName)
	return func(doc *models.Document) bool {
		var fieldVal interface{}
		if fv, ok := doc.GetFieldValue(schema, fieldName); ok {
			fieldVal = fv.AsInterface()
		} else if schema != nil {
			// O(1) case-insensitive fallback
			if fv, ok := doc.GetFieldValueCI(schema, lowerFieldName); ok {
				fieldVal = fv.AsInterface()
			}
		}
		if fieldVal == nil && doc.Data != nil {
			fieldVal = doc.Data[fieldName]
			if fieldVal == nil {
				for k, v := range doc.Data {
					if strings.ToLower(k) == lowerFieldName {
						fieldVal = v
						break
					}
				}
			}
		}
		if fieldVal == nil && literalValue != nil {
			return false
		}
		if fieldVal == nil && literalValue == nil {
			return true
		}
		if fieldVal == nil || literalValue == nil {
			return false
		}
		// Try numeric comparison first (handles int vs float64 mismatches)
		if fv, fok := predicateToFloat64(fieldVal); fok {
			if lv, lok := predicateToFloat64(literalValue); lok {
				return fv == lv
			}
		}
		return fmt.Sprintf("%v", fieldVal) == fmt.Sprintf("%v", literalValue)
	}
}

// makeComparisonPredicate creates a numeric comparison predicate
func makeComparisonPredicate(fieldName string, literalValue interface{}, schema *models.BundleFieldSchema, cmp func(float64, float64) bool) func(*models.Document) bool {
	// Pre-convert literal to float64
	litFloat, ok := predicateToFloat64(literalValue)
	if !ok {
		return nil // Can't compare non-numeric literals with pushdown
	}
	// Pre-compute lowercase field name for CI fallback
	lowerFieldName := strings.ToLower(fieldName)
	return func(doc *models.Document) bool {
		var fv models.FieldValue
		if v, ok := doc.GetFieldValue(schema, fieldName); ok {
			fv = v
		} else if schema != nil {
			// O(1) case-insensitive fallback
			if v, ok := doc.GetFieldValueCI(schema, lowerFieldName); ok {
				fv = v
			}
		}
		if fv.Type == 0 && doc.Data != nil {
			if v, ok := doc.Data[fieldName]; ok {
				fv = models.NewInterfaceValue(v)
			} else {
				for k, v := range doc.Data {
					if strings.ToLower(k) == lowerFieldName {
						fv = models.NewInterfaceValue(v)
						break
					}
				}
			}
		}
		docFloat, ok := fieldValueToFloat64(fv)
		if !ok {
			return false
		}
		return cmp(docFloat, litFloat)
	}
}

// predicateToFloat64 converts an interface value to float64 for predicate pushdown
func predicateToFloat64(v interface{}) (float64, bool) {
	switch val := v.(type) {
	case int:
		return float64(val), true
	case int32:
		return float64(val), true
	case int64:
		return float64(val), true
	case float32:
		return float64(val), true
	case float64:
		return val, true
	default:
		return 0, false
	}
}

// fieldValueToFloat64 converts a models.FieldValue to float64
func fieldValueToFloat64(fv models.FieldValue) (float64, bool) {
	switch fv.Type {
	case models.FieldTypeFloat:
		return fv.FloatVal, true
	case models.FieldTypeInt:
		return float64(fv.IntVal), true
	default:
		return 0, false
	}
}
