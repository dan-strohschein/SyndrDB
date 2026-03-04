/*
UNIFIED QUERY PLANNER - PHASE 3

This file implements the main unified query planner for SyndrDB's query system.
It orchestrates existing planners and Phase 2 execution nodes to create complete
execution plans for all types of SELECT queries.

ARCHITECTURE:
The UnifiedQueryPlanner follows the Facade pattern, providing a single entry point
for query planning while delegating to specialized planners (QueryPlanner for scans/filters,
JoinQueryPlanner for JOINs) and composing results with Phase 2 execution nodes.

DESIGN PRINCIPLES:
- Single Responsibility: Orchestrates plan creation, delegates to specialists
- Open/Closed: Extends existing planners without modifying them
- Dependency Inversion: Depends on ExecutionNode and planner interfaces

PLANNING FLOW:
1. Accept UnifiedSelectQuery from Phase 1 parser
2. Route to appropriate planner based on query type (via QueryRouter)
3. Get base execution tree from existing planner
4. Enhance with Phase 2 nodes using PlanBuilder (Sort, Limit, Aggregation)
5. Return complete ExecutionPlan

REUSE STRATEGY:
- 100% reuse of QueryPlanner for scan/filter optimization
- 100% reuse of JoinQueryPlanner for JOIN optimization
- 100% reuse of all Phase 2 execution nodes
- Minimal new code (~100 LOC) for orchestration

This is the main component of Phase 3 of the unified query system implementation.
*/

package planner

import (
	"context"
	"fmt"
	"time"

	"syndrdb/src/internal/domain/models"
	"syndrdb/src/internal/domain/view"
	"syndrdb/src/internal/query/planner/subquery"
	"syndrdb/src/internal/query/queryparser"
	"syndrdb/src/internal/syndrQL"
	"syndrdb/src/pkg/extension"
	"syndrdb/src/pkg/settings"

	"go.uber.org/zap"
)

// UnifiedQueryPlanner is the main planner for all SELECT query types
// PHASE 3: Unified Query Planner - Main Orchestrator
type UnifiedQueryPlanner struct {
	// Existing planners (Phase 1 & Phase 2)
	basePlanner *QueryPlanner     // Simple queries (scan + filter)
	joinPlanner *JoinQueryPlanner // JOIN queries

	// Phase 3 components
	router  *QueryRouter // Routes queries to appropriate planner
	builder *PlanBuilder // Composes plans with additional nodes

	// bundleService for bundle metadata (e.g. GROUP BY schema validation)
	bundleService BundleServiceInterface

	// Query plan cache - PostgreSQL-inspired sharded LRU with MongoDB-style invalidation
	planCache       *ShardedPlanCache    // 8-shard cache with adaptive planning and stale serving
	invalidationMgr *InvalidationManager // Write-threshold invalidation manager

	// TIER 1 SUBQUERY SUPPORT: Subquery executor for IN/EXISTS subquery evaluation
	subqueryExecutor interface{} // *subquery.StandardSubqueryExecutor - use interface{} to avoid import cycle

	// VIEW SUPPORT: View registry and query rewriter for PostgreSQL-style views
	viewRegistry      *view.ViewRegistry      // In-memory cache of views for fast lookups
	viewQueryRewriter *view.ViewQueryRewriter // Query rewriter for view expansion

	// COST INTELLIGENCE: Column statistics store for cost-based optimization
	statsStore *StatsStore

	// logger for debugging
	logger *zap.SugaredLogger
}

// NewUnifiedQueryPlanner creates a new unified query planner
// PHASE 3: Factory function for UnifiedQueryPlanner creation
//
// Parameters:
//   - logger: Logger for debugging
//   - bundleServiceInt: Bundle service interface for data access
//   - bundleService: Bundle service for document scanning
//
// Returns:
//   - *UnifiedQueryPlanner: Configured unified planner
func NewUnifiedQueryPlanner(
	logger *zap.SugaredLogger,
	bundleService BundleServiceInterface,
) *UnifiedQueryPlanner {

	// Create base planner (reuses existing QueryPlanner)
	basePlanner := NewQueryPlannerWithService(logger, bundleService, bundleService)

	// Create JOIN planner (reuses existing JoinQueryPlanner)
	joinPlanner := NewJoinQueryPlanner(logger, bundleService, bundleService)

	// Create router for query routing
	router := NewQueryRouter(basePlanner, joinPlanner, bundleService, logger)

	// Create builder for execution tree composition
	builder := NewPlanBuilder(bundleService, logger)

	// Initialize view support components
	// TODO: I should wire this into the actual ViewService when it's fully implemented
	// For now, create standalone instances for query planning integration
	viewRegistry := view.NewViewRegistry(logger)
	viewQueryRewriter := view.NewViewQueryRewriter(logger, viewRegistry)

	// Initialize query plan cache if enabled
	args := settings.GetSettings()
	var planCache *ShardedPlanCache
	var invalidationMgr *InvalidationManager

	if args.PlanCacheEnabled {
		planCache = NewShardedPlanCache(args, logger)
		invalidationMgr = NewInvalidationManager(
			planCache,
			int64(args.PlanCacheWriteThreshold),
			logger,
		)
		logger.Debugf("Query plan cache enabled: capacity=%d per shard (%d total), adaptive=%t",
			args.PlanCacheCapacity, args.PlanCacheCapacity*8, args.PlanCacheAdaptivePlanning)
	} else {
		logger.Debugf("Query plan cache disabled")
	}

	// Create the unified planner first (needed for circular dependency resolution)
	planner := &UnifiedQueryPlanner{
		basePlanner:       basePlanner,
		joinPlanner:       joinPlanner,
		router:            router,
		builder:           builder,
		bundleService:     bundleService,
		logger:            logger,
		planCache:         planCache,
		invalidationMgr:   invalidationMgr,
		viewRegistry:      viewRegistry,
		viewQueryRewriter: viewQueryRewriter,
	}

	// TIER 1 SUBQUERY SUPPORT: Initialize subquery executor with config and metrics
	// Note: We pass a planner adapter instead of the planner directly to satisfy the interface
	// This creates a controlled circular dependency that's safe because the executor only uses
	// the planner's CreatePlan method, not the executor field.
	if args.SubqueryEnabled {
		subqueryConfig := subquery.NewSubqueryConfigFromSettings(args)
		// Create adapter that implements subquery.QueryPlannerInterface
		plannerAdapter := &queryPlannerAdapter{planner: planner}
		// TODO: Wire in metrics recorder when available (pass nil for now)
		executor := subquery.NewStandardSubqueryExecutor(
			subqueryConfig,
			plannerAdapter, // Adapter implements the correct interface
			nil,            // No metrics recorder yet
			logger,
		)
		planner.subqueryExecutor = executor

		// Inject subquery executor into router so it can pass to FilterNode
		router.SetSubqueryExecutor(executor)

		logger.Debugf("Subquery support enabled: small_threshold=%d, hash_threshold=%d, max_depth=%d",
			args.SubquerySmallThreshold, args.SubqueryHashThreshold, args.SubqueryMaxDepth)
	} else {
		logger.Debugf("Subquery support disabled")
	}

	return planner
}

// queryPlannerAdapter adapts UnifiedQueryPlanner to subquery.QueryPlannerInterface
type queryPlannerAdapter struct {
	planner *UnifiedQueryPlanner
}

// CreatePlan implements subquery.QueryPlannerInterface
func (adapter *queryPlannerAdapter) CreatePlan(
	query *queryparser.UnifiedSelectQuery,
	database *models.Database,
) (subquery.ExecutionPlanInterface, error) {
	plan, err := adapter.planner.CreatePlan(query, database)
	if err != nil {
		return nil, err
	}
	return plan, nil // *ExecutionPlan implements subquery.ExecutionPlanInterface
}

// CreatePlan creates a complete execution plan for a unified SELECT query
// PHASE 3: Main entry point for unified query planning
//
// This method orchestrates the entire planning process:
// 1. Routes query to appropriate base planner
// 2. Gets base execution tree (scan/filter or JOIN)
// 3. Enhances tree with Phase 2 nodes (Sort, Limit, Aggregation, Hierarchical)
// 4. Returns complete execution plan with cost estimates
//
// Parameters:
//   - query: UnifiedSelectQuery from Phase 1 parser
//   - database: Database containing bundles to query
//
// Returns:
//   - *ExecutionPlan: Complete execution plan ready to execute
//   - error: Any error during planning
func (uqp *UnifiedQueryPlanner) CreatePlan(
	query *queryparser.UnifiedSelectQuery,
	database *models.Database,
) (*ExecutionPlan, error) {

	uqp.logger.Debugf("Creating unified execution plan for query type: %s", query.QueryType)

	// Validate GROUP BY fields against bundle schema (fail fast, before planning or cache)
	if query.GroupBy != nil && len(query.GroupBy.Fields) > 0 {
		getBundle := func(db *models.Database, n string) (*models.Bundle, error) {
			return uqp.bundleService.GetBundleByName(db, n)
		}
		if err := ValidateGroupByFieldsAgainstSchema(query, database, getBundle, uqp.logger); err != nil {
			return nil, err
		}
	}

	// Check plan cache first to avoid re-planning identical queries
	if uqp.planCache != nil {
		// Use cache with adaptive planning and stale serving
		return uqp.planCache.Get(
			context.Background(),
			query,
			database,
			func(useGeneric bool) (*ExecutionPlan, error) {
				// Build plan function - called on cache miss or stale rebuild
				return uqp.buildPlanInternal(query, database)
			},
		)
	}

	// Cache disabled - build plan directly
	return uqp.buildPlanInternal(query, database)
}

// buildPlanInternal performs the actual plan building (extracted for cache integration)
// This method contains the core planning logic that was previously inline in CreatePlan
func (uqp *UnifiedQueryPlanner) buildPlanInternal(
	query *queryparser.UnifiedSelectQuery,
	database *models.Database,
) (*ExecutionPlan, error) {

	// Check for expression-only query (no FROM clause)
	if query.FromBundle == "" {
		return uqp.createExpressionOnlyPlan(query, database)
	}

	// VIEW SUPPORT: Check if FromBundle is a view and rewrite query if needed
	if uqp.viewRegistry.IsView(database.Name, query.FromBundle) {
		viewObj := uqp.viewRegistry.GetView(database.Name, query.FromBundle)
		if viewObj == nil {
			return nil, fmt.Errorf("view '%s' registered but not found in database '%s'", query.FromBundle, database.Name)
		}

		if viewObj.Type == view.ViewTypeMaterialized {
			// Materialized view: transparently redirect to the hidden _mv_ bundle
			uqp.logger.Debugf("Redirecting materialized view '%s' to bundle '%s'", query.FromBundle, viewObj.DataBundleName)
			query.FromBundle = viewObj.DataBundleName
		} else {
			// Regular view: AST-level rewrite
			rewrittenQuery, err := uqp.rewriteViewQuery(query, viewObj, database)
			if err != nil {
				return nil, fmt.Errorf("failed to rewrite view query for '%s': %w", viewObj.ViewName, err)
			}
			query = rewrittenQuery
			uqp.logger.Debugf("View '%s' rewritten to query against bundle '%s'", viewObj.ViewName, query.FromBundle)
		}
	}

	// Enterprise planner extension hook: allow extensions (e.g., FTS) to handle the query
	if reg := extension.GetRegistry(); len(reg.GetPlannerExtensions()) > 0 {
		for _, pe := range reg.GetPlannerExtensions() {
			if planNode, ok := pe.PlanQuery(context.Background(), query.FromBundle, query); ok {
				uqp.logger.Debugf("PlannerExtension handled query for bundle '%s'", query.FromBundle)
				_ = planNode // Extension plan nodes are handled at execution time
				break
			}
		}
	}

	// Step 1: Route to appropriate planner and get base execution tree
	baseNode, indexesUsed, err := uqp.router.RouteQuery(query, database)
	if err != nil {
		return nil, fmt.Errorf("failed to create base execution tree: %w", err)
	}

	uqp.logger.Debugf("Base execution tree created: cost=%.2f, rows=%d, indexes=%v",
		baseNode.GetCost(), baseNode.GetEstimatedRows(), indexesUsed)

	// Step 2: Compose complete execution tree with additional nodes
	finalNode, err := uqp.builder.BuildPlan(baseNode, query, database)
	if err != nil {
		return nil, fmt.Errorf("failed to build execution tree: %w", err)
	}

	uqp.logger.Debugf("Complete execution tree built: Cost=%.2f, EstimatedRows=%d",
		finalNode.GetCost(), finalNode.GetEstimatedRows())

	// Step 3: Calculate memory estimate for the plan
	memoryEstimate := finalNode.EstimateMemoryUsage()

	// Step 4: Return complete execution plan
	plan := &ExecutionPlan{
		RootNode:             finalNode,
		Cost:                 finalNode.GetCost(),
		EstimatedRows:        finalNode.GetEstimatedRows(),
		IndexesUsed:          indexesUsed,
		Logger:               uqp.logger,
		estimatedMemoryBytes: memoryEstimate,
	}

	// Step 5: Set result schema from the source bundle for Values→field name mapping
	if query.FromBundle != "" {
		if bundle, err := uqp.bundleService.GetBundleByName(database, query.FromBundle); err == nil && bundle != nil {
			plan.ResultSchema = bundle.DocumentStructure.FieldSchema()
		}
	}

	// Step 5b: Carry temporal clause if present (for execution-time filtering)
	if query.TemporalClause != nil {
		plan.TemporalClause = query.TemporalClause
	}

	// Step 6: Check if iterator path is beneficial for this query
	if ShouldUseIterator(query) {
		plan.UseIterator = true
		plan.IteratorFactory = BuildIteratorPipeline(finalNode, query)
		uqp.logger.Debugf("Iterator path enabled for query (LIMIT=%d, OrderBy=%v)",
			query.Limit, query.OrderBy != nil)
	}

	uqp.logger.Debugf("Unified execution plan created successfully: "+
		"Type=%s, Cost=%.2f, EstimatedRows=%d, IndexesUsed=%v, MemoryEstimate=%d bytes, UseIterator=%v",
		query.QueryType, plan.Cost, plan.EstimatedRows, plan.IndexesUsed, memoryEstimate, plan.UseIterator)

	return plan, nil
}

// createExpressionOnlyPlan creates an execution plan for expression-only SELECT queries (no FROM clause)
func (uqp *UnifiedQueryPlanner) createExpressionOnlyPlan(
	query *queryparser.UnifiedSelectQuery,
	database *models.Database,
) (*ExecutionPlan, error) {

	// Validate that query doesn't have DISTINCT
	if query.IsDistinct {
		return nil, fmt.Errorf("DISTINCT modifier cannot be used in expression-only SELECT queries")
		// TODO: Consider allowing DISTINCT in future for consistency with SQL standard,
		// though result is always identical for single-row expression evaluation.
	}

	// Parse select fields once at plan build time (Issue 11: avoid re-parse every Execute)
	parsedFields, err := ParseSelectFields(query.SelectFields, uqp.logger)
	if err != nil {
		return nil, fmt.Errorf("expression-only plan: %w", err)
	}

	// Create expression evaluation node
	evalNode := &ExpressionEvaluationNode{
		SelectFields: query.SelectFields,
		ParsedFields: parsedFields,
		Logger:       uqp.logger,
	}

	plan := &ExecutionPlan{
		RootNode:             evalNode,
		Cost:                 evalNode.GetCost(),
		EstimatedRows:        evalNode.GetEstimatedRows(),
		IndexesUsed:          []string{}, // No indexes used for expression-only
		Logger:               uqp.logger,
		estimatedMemoryBytes: evalNode.EstimateMemoryUsage(),
	}

	uqp.logger.Debugf("Expression-only execution plan created: Fields=%d, Cost=%.2f, Rows=%d",
		len(query.SelectFields), plan.Cost, plan.EstimatedRows)

	return plan, nil
}

// InvalidateBundleCache invalidates all cached plans for a specific bundle
// Call this on INSERT/UPDATE/DELETE or schema changes to ensure fresh plans
func (uqp *UnifiedQueryPlanner) InvalidateBundleCache(bundleName string) {
	if uqp.planCache != nil {
		uqp.planCache.InvalidateBundle(bundleName)
		uqp.logger.Debugf("Invalidated cached plans for bundle: %s", bundleName)
	}
}

// RemoveBundleMetadata removes the bundle from plan-cache metadata (bundleInvalidations,
// staleServesByBundle, collectionVersions) and from the invalidation manager's write
// trackers. Call when a bundle is dropped (Issue 10).
func (uqp *UnifiedQueryPlanner) RemoveBundleMetadata(bundleName string) {
	if uqp.planCache != nil {
		uqp.planCache.RemoveBundleMetadata(bundleName)
	}
	if uqp.invalidationMgr != nil {
		uqp.invalidationMgr.RemoveBundle(bundleName)
	}
}

// RecordPlanStats records execution duration for a plan that was served from the cache.
// Call after Execute() so adaptive planning can compare custom vs generic plan performance.
// If plan was not from cache (CacheKey == 0), this is a no-op.
func (uqp *UnifiedQueryPlanner) RecordPlanStats(plan interface{}, duration time.Duration) {
	if uqp.planCache == nil {
		return
	}
	ep, ok := plan.(*ExecutionPlan)
	if !ok || ep.CacheKey == 0 {
		return
	}
	uqp.planCache.UpdatePlanStats(ep.CacheKey, duration, ep.IsGeneric)
}

// InvalidateViewCache invalidates all cached plans that reference a specific view
// Call this when a view definition changes or a view is dropped
// This ensures queries against the view will be re-planned with the new definition
func (uqp *UnifiedQueryPlanner) InvalidateViewCache(databaseName, viewName string) {
	if uqp.planCache != nil {
		// For regular views, we invalidate by treating the view name as a bundle name
		// since the plan cache uses FromBundle as part of the cache key
		uqp.planCache.InvalidateBundle(viewName)

		// For materialized views, also invalidate the hidden data bundle
		viewObj := uqp.viewRegistry.GetView(databaseName, viewName)
		if viewObj != nil && viewObj.Type == view.ViewTypeMaterialized && viewObj.DataBundleName != "" {
			uqp.planCache.InvalidateBundle(viewObj.DataBundleName)
		}

		uqp.logger.Debugf("Invalidated cached plans for view: %s in database: %s", viewName, databaseName)
	}
}

// GetInvalidationManager returns the invalidation manager (for hooking into write operations)
// This allows document operations to call OnWrite() for write-threshold invalidation
func (uqp *UnifiedQueryPlanner) GetInvalidationManager() *InvalidationManager {
	return uqp.invalidationMgr
}

// GetViewRegistry returns the view registry for external access
// This allows the ViewService to register/unregister views
func (uqp *UnifiedQueryPlanner) GetViewRegistry() *view.ViewRegistry {
	return uqp.viewRegistry
}

// PlanCache returns the underlying sharded plan cache for reuse by result caching.
func (uqp *UnifiedQueryPlanner) PlanCache() *ShardedPlanCache {
	return uqp.planCache
}

// Shutdown gracefully shuts down the plan cache
// Call this during server shutdown to ensure all resources are released
func (uqp *UnifiedQueryPlanner) Shutdown() {
	if uqp.planCache != nil {
		uqp.planCache.Shutdown()
		uqp.logger.Debugf("Plan cache shut down")
	}
}

// GetBasePlanner returns the underlying base planner (for testing/debugging)
// PHASE 3: Accessor for testing
func (uqp *UnifiedQueryPlanner) GetBasePlanner() *QueryPlanner {
	return uqp.basePlanner
}

// GetJoinPlanner returns the underlying JOIN planner (for testing/debugging)
// PHASE 3: Accessor for testing
func (uqp *UnifiedQueryPlanner) GetJoinPlanner() *JoinQueryPlanner {
	return uqp.joinPlanner
}

// SetStatsStore wires the column statistics store into the planner and router.
// Called during server initialization after both StatsStore and planner are created.
func (uqp *UnifiedQueryPlanner) SetStatsStore(ss *StatsStore) {
	uqp.statsStore = ss
	if uqp.router != nil {
		uqp.router.statsStore = ss
		uqp.router.selectivityEstimator = NewSelectivityEstimator(ss, uqp.logger)
	}
}

// GetStatsStore returns the column statistics store.
func (uqp *UnifiedQueryPlanner) GetStatsStore() *StatsStore {
	return uqp.statsStore
}

// SetMetricsReporter sets the callback used to export plan cache metrics to GlobalServerMetrics.
func (uqp *UnifiedQueryPlanner) SetMetricsReporter(reporter func(string, uint64)) {
	if uqp.planCache != nil {
		uqp.planCache.SetMetricsReporter(reporter)
	}
}

// PlanCacheStats returns a snapshot of plan cache statistics.
func (uqp *UnifiedQueryPlanner) PlanCacheStats() CacheStatsSnapshot {
	if uqp.planCache != nil {
		return uqp.planCache.Stats()
	}
	return CacheStatsSnapshot{}
}

// rewriteViewQuery performs AST-level rewriting of a query against a regular view.
// It parses the view definition, replaces the FromBundle with the actual bundle,
// and merges WHERE clauses.
func (uqp *UnifiedQueryPlanner) rewriteViewQuery(
	outerQuery *queryparser.UnifiedSelectQuery,
	viewObj *view.View,
	database *models.Database,
) (*queryparser.UnifiedSelectQuery, error) {

	// Parse the view's definition SQL using the new SyndrQL parser (produces syndrQL.Expression
	// for WhereExpression, unlike the legacy ParseUnifiedSelectQuery which produces WhereGroup)
	tokenizer := syndrQL.NewTokenizer(viewObj.Definition)
	tokens, tokenErr := tokenizer.Tokenize()
	if tokenErr != nil {
		return nil, fmt.Errorf("failed to tokenize view definition: %w", tokenErr)
	}
	parser := syndrQL.NewSelectParser(tokens, uqp.logger)
	stmt, parseErr := parser.Parse(uqp.logger, viewObj.Definition)
	if parseErr != nil {
		return nil, fmt.Errorf("failed to parse view definition: %w", parseErr)
	}
	adapter := syndrQL.NewSelectStatementAdapter(uqp.logger)
	viewQuery, err := adapter.ToUnifiedSelectQuery(stmt)
	if err != nil {
		return nil, fmt.Errorf("failed to adapt view definition: %w", err)
	}

	// Reject nested views (view-of-view)
	if uqp.viewRegistry.IsView(database.Name, viewQuery.FromBundle) {
		return nil, fmt.Errorf("nested views are not supported: view '%s' references another view '%s'",
			viewObj.ViewName, viewQuery.FromBundle)
	}

	// Build the rewritten query by starting from the view query as base
	rewritten := &queryparser.UnifiedSelectQuery{
		QueryType:   outerQuery.QueryType,
		FromBundle:  viewQuery.FromBundle,
		JoinClauses: viewQuery.JoinClauses,
		ForUpdate:   outerQuery.ForUpdate,
	}

	// SELECT fields: if outer selects *, use view's fields; otherwise use outer's fields
	if len(outerQuery.SelectFields) == 0 || (len(outerQuery.SelectFields) == 1 && outerQuery.SelectFields[0] == "*") {
		rewritten.SelectFields = viewQuery.SelectFields
	} else {
		rewritten.SelectFields = outerQuery.SelectFields
	}

	// Aggregate fields: prefer outer query's aggregates if present
	if len(outerQuery.AggregateFields) > 0 {
		rewritten.AggregateFields = outerQuery.AggregateFields
		rewritten.IsAggregateOnly = outerQuery.IsAggregateOnly
		rewritten.IsCountOnly = outerQuery.IsCountOnly
	} else {
		rewritten.AggregateFields = viewQuery.AggregateFields
		rewritten.IsAggregateOnly = viewQuery.IsAggregateOnly
		rewritten.IsCountOnly = viewQuery.IsCountOnly
	}

	rewritten.IsDistinct = outerQuery.IsDistinct || viewQuery.IsDistinct

	// Merge WHERE clauses: (view.WHERE) AND (outer.WHERE)
	rewritten.WhereExpression = mergeWhereExpressions(viewQuery.WhereExpression, outerQuery.WhereExpression)

	// Carry forward outer query's ORDER BY, LIMIT, OFFSET, GROUP BY, HAVING
	if outerQuery.OrderBy != nil {
		rewritten.OrderBy = outerQuery.OrderBy
	} else {
		rewritten.OrderBy = viewQuery.OrderBy
	}

	if outerQuery.Limit > 0 {
		rewritten.Limit = outerQuery.Limit
	}
	if outerQuery.Offset > 0 {
		rewritten.Offset = outerQuery.Offset
	}
	if outerQuery.TopCount > 0 {
		rewritten.TopCount = outerQuery.TopCount
	}

	if outerQuery.GroupBy != nil {
		rewritten.GroupBy = outerQuery.GroupBy
	} else {
		rewritten.GroupBy = viewQuery.GroupBy
	}

	if outerQuery.HavingExpression != nil {
		rewritten.HavingExpression = outerQuery.HavingExpression
	} else {
		rewritten.HavingExpression = viewQuery.HavingExpression
	}

	if outerQuery.RelationshipName != "" {
		rewritten.RelationshipName = outerQuery.RelationshipName
	}

	return rewritten, nil
}

// mergeWhereExpressions combines two WHERE clause expressions using AND.
// If either is nil, returns the other. If both are nil, returns nil.
func mergeWhereExpressions(viewWhere, outerWhere interface{}) interface{} {
	if viewWhere == nil {
		return outerWhere
	}
	if outerWhere == nil {
		return viewWhere
	}

	// Both are non-nil: combine with AND
	viewExpr, viewOk := viewWhere.(syndrQL.Expression)
	outerExpr, outerOk := outerWhere.(syndrQL.Expression)

	if !viewOk || !outerOk {
		// Fallback: if they aren't proper Expression types, prefer outer
		return outerWhere
	}

	return &syndrQL.BinaryExpression{
		Left:     viewExpr,
		Operator: syndrQL.TOKEN_AND,
		Right:    outerExpr,
	}
}
