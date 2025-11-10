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
	"syndrdb/src/internal/query/queryparser"

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
	return &QueryRouter{
		basePlanner:   basePlanner,
		joinPlanner:   joinPlanner,
		bundleService: bundleService,
		logger:        logger,
	}
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

	qr.logger.Infof("Routing query: Type=%s, HasJoin=%v, HasGroupBy=%v, HasWhere=%v",
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

	// Convert WHERE clause to string for existing planner
	// Note: Existing planner expects string WHERE clause
	// For now, we'll use a simplified conversion
	// TODO: Enhance to preserve full WhereGroup structure
	whereClause := qr.convertWhereClauseToString(query.WhereClause)

	// Delegate to existing base planner
	plan, err := qr.basePlanner.CreateExecutionPlan(bundle, whereClause)
	if err != nil {
		return nil, nil, fmt.Errorf("base planner failed: %w", err)
	}

	return plan.RootNode, plan.IndexesUsed, nil
}

// convertToJoinQuery converts UnifiedSelectQuery to SelectJoinQuery
// PHASE 3: Query format conversion
func (qr *QueryRouter) convertToJoinQuery(query *queryparser.UnifiedSelectQuery) *queryparser.SelectJoinQuery {
	joinQuery := &queryparser.SelectJoinQuery{
		SelectFields:     query.SelectFields,
		FromBundle:       query.FromBundle,
		JoinClauses:      query.JoinClauses,
		WhereClause:      query.WhereClause,
		OrderBy:          []string{}, // ORDER BY handled by PlanBuilder
		Limit:            query.Limit,
		Offset:           query.Offset,
		RelationshipName: query.RelationshipName,
	}

	return joinQuery
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
