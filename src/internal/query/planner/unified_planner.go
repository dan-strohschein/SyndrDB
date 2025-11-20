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
	"crypto/sha256"
	"fmt"
	"syndrdb/src/internal/domain/models"
	"syndrdb/src/internal/query/queryparser"

	lru "github.com/hashicorp/golang-lru/v2"
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

	// STEP 2: Query plan cache to reduce planning allocations
	// LRU cache with 128 entries, keyed by query hash
	// TODO: Option C - Global plan cache with weak references for cross-session plan reuse
	planCache *lru.Cache[string, *ExecutionPlan]

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

	// STEP 2: Initialize query plan cache (128 entries)
	// Cache invalidation handled on INSERT/UPDATE/DELETE operations
	planCache, err := lru.New[string, *ExecutionPlan](128)
	if err != nil {
		// Fallback to no caching if LRU init fails (shouldn't happen in practice)
		logger.Warnf("Failed to initialize query plan cache: %v - proceeding without caching", err)
		planCache = nil
	}

	return &UnifiedQueryPlanner{
		basePlanner: basePlanner,
		joinPlanner: joinPlanner,
		router:      router,
		builder:     builder,
		logger:      logger,
		planCache:   planCache,
	}
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

	// STEP 2: Check plan cache first to avoid re-planning identical queries
	if uqp.planCache != nil {
		cacheKey := uqp.planCacheKey(query, database)
		if cachedPlan, ok := uqp.planCache.Get(cacheKey); ok {
			uqp.logger.Debugf("Query plan cache HIT for key: %s", cacheKey[:16])
			return cachedPlan, nil
		}
		uqp.logger.Debugf("Query plan cache MISS for key: %s", cacheKey[:16])
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

	// Step 3: Return complete execution plan
	plan := &ExecutionPlan{
		RootNode:      finalNode,
		Cost:          finalNode.GetCost(),
		EstimatedRows: finalNode.GetEstimatedRows(),
		IndexesUsed:   indexesUsed,
		Logger:        uqp.logger,
	}

	// STEP 2: Store plan in cache for future reuse
	if uqp.planCache != nil {
		cacheKey := uqp.planCacheKey(query, database)
		uqp.planCache.Add(cacheKey, plan)
		uqp.logger.Debugf("Stored plan in cache with key: %s", cacheKey[:16])
	}

	uqp.logger.Debugf("Unified execution plan created successfully: "+
		"Type=%s, Cost=%.2f, EstimatedRows=%d, IndexesUsed=%v",
		query.QueryType, plan.Cost, plan.EstimatedRows, plan.IndexesUsed)

	return plan, nil
}

// planCacheKey generates a cache key from the query structure
// STEP 2: SHA256 hash of normalized query for cache lookup
func (uqp *UnifiedQueryPlanner) planCacheKey(query *queryparser.UnifiedSelectQuery, database *models.Database) string {
	h := sha256.New()

	// Include all query components that affect the execution plan
	h.Write([]byte(fmt.Sprintf("%d", query.QueryType)))
	h.Write([]byte(query.FromBundle))
	h.Write([]byte(database.Name))

	// SELECT fields
	for _, field := range query.SelectFields {
		h.Write([]byte(field))
	}

	// Aggregates
	for _, agg := range query.AggregateFields {
		h.Write([]byte(fmt.Sprintf("%s:%s", agg.Function, agg.Field)))
	}

	// Flags
	h.Write([]byte(fmt.Sprintf("%t:%t", query.IsDistinct, query.IsCountOnly)))

	// Where clause (if present)
	if query.WhereExpression != nil {
		h.Write([]byte(fmt.Sprintf("%v", query.WhereExpression)))
	}

	// Order by
	if query.OrderBy != nil {
		for _, orderBy := range query.OrderBy.Fields {
			h.Write([]byte(orderBy.FieldName))
			h.Write([]byte(fmt.Sprintf("%d", orderBy.Direction)))
		}
	}

	// Group by
	if query.GroupBy != nil {
		for _, groupBy := range query.GroupBy.Fields {
			h.Write([]byte(groupBy))
		}
	}

	// Having clause
	if query.HavingExpression != nil {
		h.Write([]byte(fmt.Sprintf("%v", query.HavingExpression)))
	}

	// JOINs
	for _, join := range query.JoinClauses {
		h.Write([]byte(fmt.Sprintf("%d:%s", join.JoinType, join.RightBundle)))
		for _, cond := range join.JoinConditions {
			h.Write([]byte(fmt.Sprintf("%s:%s:%s", cond.LeftField, cond.Operator, cond.RightField)))
		}
	}

	// Relationship
	if query.RelationshipName != "" {
		h.Write([]byte(query.RelationshipName))
	}

	// Limit/Offset
	h.Write([]byte(fmt.Sprintf("%d:%d:%d", query.Limit, query.Offset, query.TopCount)))

	return fmt.Sprintf("%x", h.Sum(nil))
}

// InvalidatePlanCache clears the query plan cache
// STEP 2: Call this on INSERT/UPDATE/DELETE to ensure fresh plans
func (uqp *UnifiedQueryPlanner) InvalidatePlanCache() {
	if uqp.planCache != nil {
		uqp.planCache.Purge()
		uqp.logger.Debugf("Query plan cache invalidated")
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
