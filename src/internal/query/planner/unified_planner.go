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
	"syndrdb/src/internal/domain/models"
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

	// Query plan cache - PostgreSQL-inspired sharded LRU with MongoDB-style invalidation
	planCache       *ShardedPlanCache    // 8-shard cache with adaptive planning and stale serving
	invalidationMgr *InvalidationManager // Write-threshold invalidation manager

	// TIER 1 SUBQUERY SUPPORT: Subquery executor for IN/EXISTS subquery evaluation
	subqueryExecutor interface{} // *subquery.StandardSubqueryExecutor - use interface{} to avoid import cycle

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
		logger.Infof("Query plan cache enabled: capacity=%d per shard (%d total), adaptive=%t",
			args.PlanCacheCapacity, args.PlanCacheCapacity*8, args.PlanCacheAdaptivePlanning)
	} else {
		logger.Infof("Query plan cache disabled")
	}

	// Create the unified planner first (needed for circular dependency resolution)
	planner := &UnifiedQueryPlanner{
		basePlanner:     basePlanner,
		joinPlanner:     joinPlanner,
		router:          router,
		builder:         builder,
		logger:          logger,
		planCache:       planCache,
		invalidationMgr: invalidationMgr,
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

		logger.Infof("Subquery support enabled: small_threshold=%d, hash_threshold=%d, max_depth=%d",
			args.SubquerySmallThreshold, args.SubqueryHashThreshold, args.SubqueryMaxDepth)
	} else {
		logger.Infof("Subquery support disabled")
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

// InvalidateBundleCache invalidates all cached plans for a specific bundle
// Call this on INSERT/UPDATE/DELETE or schema changes to ensure fresh plans
func (uqp *UnifiedQueryPlanner) InvalidateBundleCache(bundleName string) {
	if uqp.planCache != nil {
		uqp.planCache.InvalidateBundle(bundleName)
		uqp.logger.Debugf("Invalidated cached plans for bundle: %s", bundleName)
	}
}

// GetInvalidationManager returns the invalidation manager (for hooking into write operations)
// This allows document operations to call OnWrite() for write-threshold invalidation
func (uqp *UnifiedQueryPlanner) GetInvalidationManager() *InvalidationManager {
	return uqp.invalidationMgr
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
