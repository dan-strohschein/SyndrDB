package planner

import (
	"context"
	"fmt"
	"syndrdb/src/internal/domain/models"
	"syndrdb/src/internal/query/planner/subquery"
	"syndrdb/src/internal/syndrQL"

	"go.uber.org/zap"
)

/*
subquery_helper.go

This file implements helper functions for detecting and executing subqueries
within WHERE expressions. It traverses the expression tree to find SubqueryExpression
nodes and executes them before the main filter evaluation.

DESIGN PRINCIPLES:
- Separation of Concerns: Isolates subquery detection from expression evaluation
- DRY: Reusable subquery detection logic
- OCP: Extensible for future subquery types (Tier 2/3)

INTEGRATION POINT:
FilterNode.Execute() calls DetectAndExecuteSubqueries() before evaluating WHERE expression
*/

// CorrelatedSubqueryHandler manages per-document execution and caching for
// correlated subqueries that use the NESTED_LOOP fallback strategy.
// Speed over memory: caches all results keyed on outer field values.
type CorrelatedSubqueryHandler struct {
	cache    map[string]*subquery.SubqueryResult // Keyed on outer field values
	executor subquery.SubqueryExecutor
	database *models.Database
	logger   *zap.SugaredLogger
}

// NewCorrelatedSubqueryHandler creates a handler for nested-loop correlated subqueries
func NewCorrelatedSubqueryHandler(executor subquery.SubqueryExecutor, database *models.Database, logger *zap.SugaredLogger) *CorrelatedSubqueryHandler {
	return &CorrelatedSubqueryHandler{
		cache:    make(map[string]*subquery.SubqueryResult, 256), // Pre-size for speed
		executor: executor,
		database: database,
		logger:   logger,
	}
}

// ExecuteForDocument executes a correlated subquery for a specific outer document,
// using cached results when possible.
func (h *CorrelatedSubqueryHandler) ExecuteForDocument(
	ctx context.Context,
	subExpr *syndrQL.SubqueryExpression,
	outerDoc *models.Document,
) (*subquery.SubqueryResult, error) {
	// Build cache key from outer join field values
	cacheKey := h.buildCacheKey(subExpr, outerDoc)

	// Check cache first
	if cached, ok := h.cache[cacheKey]; ok {
		return cached, nil
	}

	// Bind outer values into inner query's WHERE clause by replacing
	// the correlated qualified references with literal values
	boundWhere := h.bindOuterValues(subExpr, outerDoc)

	// Create a modified inner query with the bound WHERE
	boundInner := *subExpr.InnerQuery // Shallow copy
	boundInner.WhereClause = boundWhere

	// Execute the bound inner query
	result, err := h.executor.Execute(ctx, &boundInner, h.database)
	if err != nil {
		return nil, fmt.Errorf("correlated subquery execution failed: %w", err)
	}

	// Cache the result
	h.cache[cacheKey] = result

	return result, nil
}

// buildCacheKey creates a cache key from outer document's join field values
func (h *CorrelatedSubqueryHandler) buildCacheKey(subExpr *syndrQL.SubqueryExpression, doc *models.Document) string {
	if len(subExpr.OuterJoinFields) == 0 {
		return ""
	}

	key := ""
	for i, field := range subExpr.OuterJoinFields {
		if i > 0 {
			key += "|"
		}
		if fv, ok := doc.GetFieldValue(nil, field); ok && !fv.IsNil() {
			key += fmt.Sprint(fv.AsInterface())
		} else if doc.Data != nil {
			if v, exists := doc.Data[field]; exists {
				key += fmt.Sprint(v)
			}
		}
	}
	return key
}

// bindOuterValues replaces qualified references to the outer bundle with literal values
// from the outer document in the inner WHERE clause
func (h *CorrelatedSubqueryHandler) bindOuterValues(subExpr *syndrQL.SubqueryExpression, outerDoc *models.Document) syndrQL.Expression {
	if subExpr.InnerQuery.WhereClause == nil {
		return nil
	}

	return h.replaceOuterRefs(subExpr.InnerQuery.WhereClause, subExpr.OuterJoinFields, subExpr.InnerJoinFields, outerDoc)
}

// replaceOuterRefs walks the expression tree and replaces correlated references
func (h *CorrelatedSubqueryHandler) replaceOuterRefs(
	expr syndrQL.Expression,
	outerFields []string,
	innerFields []string,
	outerDoc *models.Document,
) syndrQL.Expression {
	if expr == nil {
		return nil
	}

	switch e := expr.(type) {
	case *syndrQL.BinaryExpression:
		// For a correlated equality predicate, replace the outer qualified ref with a literal
		newLeft := h.replaceOuterRefs(e.Left, outerFields, innerFields, outerDoc)
		newRight := h.replaceOuterRefs(e.Right, outerFields, innerFields, outerDoc)
		return &syndrQL.BinaryExpression{
			Left:     newLeft,
			Operator: e.Operator,
			Right:    newRight,
		}

	case *syndrQL.QualifiedIdentifierExpression:
		// Check if this references the outer bundle via join fields
		for i, outerField := range outerFields {
			if e.Field == outerField && i < len(innerFields) {
				// Replace with the literal value from the outer document
				val := h.getDocFieldValue(outerDoc, outerField)
				return &syndrQL.LiteralExpression{
					Token: syndrQL.TOKEN_STRING,
					Value: val,
				}
			}
		}
		return expr

	case *syndrQL.UnaryExpression:
		newRight := h.replaceOuterRefs(e.Right, outerFields, innerFields, outerDoc)
		return &syndrQL.UnaryExpression{
			Operator: e.Operator,
			Right:    newRight,
		}

	case *syndrQL.GroupedExpression:
		newInner := h.replaceOuterRefs(e.Expression, outerFields, innerFields, outerDoc)
		return &syndrQL.GroupedExpression{Expression: newInner}

	default:
		return expr
	}
}

func (h *CorrelatedSubqueryHandler) getDocFieldValue(doc *models.Document, field string) interface{} {
	if fv, ok := doc.GetFieldValue(nil, field); ok && !fv.IsNil() {
		return fv.AsInterface()
	}
	if doc.Data != nil {
		if v, exists := doc.Data[field]; exists {
			return v
		}
	}
	return nil
}

// DetectAndExecuteSubqueries finds all subqueries in the WHERE expression and executes them
// This must be called BEFORE evaluating the WHERE clause to populate subquery results
// Returns syndrQL.SubqueryExecutionContext (map[*SubqueryExpression]interface{})
//
// For correlated subqueries with SEMI_JOIN/ANTI_JOIN strategy, these are handled by
// CorrelatedSubqueryNode and should not reach here. For NESTED_LOOP strategy, they are
// skipped here and handled per-document in FilterNode.Execute().
func DetectAndExecuteSubqueries(
	ctx context.Context,
	whereExpr syndrQL.Expression,
	database *models.Database,
	executor interface{}, // *subquery.StandardSubqueryExecutor - use interface{} to avoid type assertion issues
	logger *zap.SugaredLogger,
) (syndrQL.SubqueryExecutionContext, error) {

	execCtx := make(syndrQL.SubqueryExecutionContext)

	// Traverse expression tree to find all SubqueryExpression nodes
	subqueries := findSubqueries(whereExpr)

	if len(subqueries) == 0 {
		// No subqueries - return empty context
		return execCtx, nil
	}

	// Type assert executor to SubqueryExecutor interface
	subqueryExec, ok := executor.(subquery.SubqueryExecutor)
	if !ok {
		return nil, fmt.Errorf("executor is not a SubqueryExecutor: %T", executor)
	}

	logger.Debugf("Found %d subqueries in WHERE expression", len(subqueries))

	// Execute each subquery and store result
	for _, subqueryExpr := range subqueries {
		// Skip correlated subqueries — they are handled by CorrelatedSubqueryNode (semi/anti-join)
		// or per-document in FilterNode (nested loop). Do not pre-execute them.
		if subqueryExpr.IsCorrelated {
			logger.Debugf("Skipping correlated subquery (strategy=%d): %s on %s",
				subqueryExpr.RewriteStrategy, subqueryExpr.SubqueryType, subqueryExpr.InnerQuery.BundleName)
			continue
		}

		logger.Debugf("Executing subquery: Type=%s, Bundle=%s",
			subqueryExpr.SubqueryType, subqueryExpr.InnerQuery.BundleName)

		// Execute subquery using executor
		result, err := subqueryExec.Execute(ctx, subqueryExpr.InnerQuery, database)
		if err != nil {
			return nil, fmt.Errorf("failed to execute subquery on bundle %s: %w",
				subqueryExpr.InnerQuery.BundleName, err)
		}

		// Store result in context - map SubqueryExpression pointer to materialized result
		execCtx[subqueryExpr] = result

		logger.Debugf("Subquery executed: RowCount=%d, Strategy=%s, ContainsNull=%v",
			result.RowCount, result.Strategy, result.ContainsNull)
	}

	return execCtx, nil
}

// HasCorrelatedNestedLoop checks if any subquery in the expression uses NESTED_LOOP strategy
func HasCorrelatedNestedLoop(whereExpr syndrQL.Expression) bool {
	subqueries := findSubqueries(whereExpr)
	for _, sq := range subqueries {
		if sq.IsCorrelated && sq.RewriteStrategy == REWRITE_NESTED_LOOP {
			return true
		}
	}
	return false
}

// findSubqueries recursively traverses expression tree to find all SubqueryExpression nodes
func findSubqueries(expr syndrQL.Expression) []*syndrQL.SubqueryExpression {
	if expr == nil {
		return nil
	}

	var subqueries []*syndrQL.SubqueryExpression

	// Check if this expression itself is a subquery
	if subqueryExpr, ok := expr.(*syndrQL.SubqueryExpression); ok {
		subqueries = append(subqueries, subqueryExpr)
		return subqueries
	}

	// Recursively search in child expressions
	switch e := expr.(type) {
	case *syndrQL.BinaryExpression:
		// Search left and right operands
		subqueries = append(subqueries, findSubqueries(e.Left)...)
		subqueries = append(subqueries, findSubqueries(e.Right)...)

	case *syndrQL.UnaryExpression:
		// Search operand
		subqueries = append(subqueries, findSubqueries(e.Right)...)

	case *syndrQL.CallExpression:
		// Search function arguments
		for _, arg := range e.Arguments {
			subqueries = append(subqueries, findSubqueries(arg)...)
		}

	case *syndrQL.ArrayExpression:
		// Search array elements
		for _, elem := range e.Elements {
			subqueries = append(subqueries, findSubqueries(elem)...)
		}

	// Leaf nodes - no children to search
	case *syndrQL.LiteralExpression:
		// No children
	case *syndrQL.IdentifierExpression:
		// No children
	case *syndrQL.QualifiedIdentifierExpression:
		// No children

	default:
		// Unknown expression type - log warning but continue
		// This allows forward compatibility with future expression types
	}

	return subqueries
}

// ValidateSubqueryDepth checks that subquery nesting doesn't exceed max depth
// Returns error if any subquery violates depth limit
func ValidateSubqueryDepth(
	whereExpr syndrQL.Expression,
	maxDepth int,
) error {
	return validateSubqueryDepthRecursive(whereExpr, 1, maxDepth)
}

// validateSubqueryDepthRecursive checks nesting depth recursively
func validateSubqueryDepthRecursive(
	expr syndrQL.Expression,
	currentDepth int,
	maxDepth int,
) error {
	if expr == nil {
		return nil
	}

	// Check if this is a subquery
	if subqueryExpr, ok := expr.(*syndrQL.SubqueryExpression); ok {
		// Check depth limit
		if currentDepth > maxDepth {
			return &subquery.SubqueryDepthExceededError{
				CurrentDepth: currentDepth,
				MaxDepth:     maxDepth,
			}
		}

		// Update the nesting depth field
		subqueryExpr.NestingDepth = currentDepth

		// Recursively check subquery's WHERE clause
		if subqueryExpr.InnerQuery != nil && subqueryExpr.InnerQuery.WhereClause != nil {
			if err := validateSubqueryDepthRecursive(
				subqueryExpr.InnerQuery.WhereClause,
				currentDepth+1,
				maxDepth,
			); err != nil {
				return err
			}
		}

		return nil
	}

	// Recursively check child expressions at same depth
	switch e := expr.(type) {
	case *syndrQL.BinaryExpression:
		if err := validateSubqueryDepthRecursive(e.Left, currentDepth, maxDepth); err != nil {
			return err
		}
		if err := validateSubqueryDepthRecursive(e.Right, currentDepth, maxDepth); err != nil {
			return err
		}

	case *syndrQL.UnaryExpression:
		if err := validateSubqueryDepthRecursive(e.Right, currentDepth, maxDepth); err != nil {
			return err
		}

	case *syndrQL.CallExpression:
		for _, arg := range e.Arguments {
			if err := validateSubqueryDepthRecursive(arg, currentDepth, maxDepth); err != nil {
				return err
			}
		}

	case *syndrQL.ArrayExpression:
		for _, elem := range e.Elements {
			if err := validateSubqueryDepthRecursive(elem, currentDepth, maxDepth); err != nil {
				return err
			}
		}
	}

	return nil
}
