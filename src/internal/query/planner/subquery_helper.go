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

// DetectAndExecuteSubqueries finds all subqueries in the WHERE expression and executes them
// This must be called BEFORE evaluating the WHERE clause to populate subquery results
// Returns syndrQL.SubqueryExecutionContext (map[*SubqueryExpression]interface{})
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
