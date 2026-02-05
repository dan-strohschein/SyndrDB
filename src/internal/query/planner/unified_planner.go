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
	// This implements PostgreSQL-style query rewriting for regular views
	if uqp.viewRegistry.IsView(database.Name, query.FromBundle) {
		uqp.logger.Debugf("Detected view reference: %s in database %s", query.FromBundle, database.Name)

		// TODO: I should extract username from query context when authentication is wired
		// For now, use empty username (will be populated later by ViewService)
		username := "" // Placeholder - will be populated from query context

		// Rewrite query to expand view definition
		// Note: The query is already parsed, so we need to serialize it back to SQL
		// TODO: I should add a query serializer to convert UnifiedSelectQuery back to SQL
		// For now, this is a placeholder showing the integration point
		originalQuerySQL := "" // TODO: Serialize query to SQL string

		rewrittenQuerySQL, err := uqp.viewQueryRewriter.RewriteQuery(originalQuerySQL, username, database.Name)
		if err != nil {
			return nil, fmt.Errorf("failed to rewrite view query: %w", err)
		}

		// Parse rewritten query back to UnifiedSelectQuery
		// TODO: I should parse the rewritten SQL back to UnifiedSelectQuery
		// For now, this is a placeholder showing the integration point
		_ = rewrittenQuerySQL // Avoid unused variable error

		uqp.logger.Debugf("View query rewritten successfully for view: %s", query.FromBundle)

		// TODO: I should continue planning with rewritten query instead of original
		// Once query serialization and re-parsing are implemented, replace 'query' with
		// the rewritten query object throughout the rest of this function
	}

	// Step 1: Route to appropriate planner and get base execution tree

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

	uqp.logger.Debugf("Unified execution plan created successfully: "+
		"Type=%s, Cost=%.2f, EstimatedRows=%d, IndexesUsed=%v, MemoryEstimate=%d bytes",
		query.QueryType, plan.Cost, plan.EstimatedRows, plan.IndexesUsed, memoryEstimate)

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
