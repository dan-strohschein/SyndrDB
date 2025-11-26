package syndrQL

import (
	"fmt"
	"syndrdb/src/internal/query/queryparser"
	"syndrdb/src/internal/utils"

	"go.uber.org/zap"
)

/*
expression_adapter.go

This file provides adapters to convert our new Expression AST into the existing
WhereGroup and WhereClause structures used by the legacy filter_parser system.
This allows incremental migration while maintaining backward compatibility.

Key responsibilities:
- Converting Expression AST nodes to WhereGroup/WhereClause structures
- Preserving expression semantics during conversion
- Handling edge cases and complex nested expressions
- Supporting gradual migration strategy

The adapter follows the Adapter pattern, translating between two different
but semantically equivalent representations of WHERE clauses.
*/

// ExpressionAdapter converts Expression AST to existing WhereGroup structures
type ExpressionAdapter struct {
	logger *zap.SugaredLogger
}

// NewExpressionAdapter creates a new expression adapter
func NewExpressionAdapter(logger *zap.SugaredLogger) *ExpressionAdapter {
	return &ExpressionAdapter{
		logger: logger,
	}
}

// ToWhereGroup converts an Expression AST to a WhereGroup structure
// This is the main entry point for expression conversion
func (a *ExpressionAdapter) ToWhereGroup(expr Expression) (*queryparser.WhereGroup, error) {
	if expr == nil {
		return nil, nil
	}

	// Start with an empty WhereGroup
	group := &queryparser.WhereGroup{
		Clauses:   []queryparser.WhereClause{},
		SubGroups: []queryparser.WhereGroup{},
		Operator:  "AND", // Default to AND
	}

	// Convert the expression tree into clauses and subgroups
	err := a.convertExpression(expr, group, "AND")
	if err != nil {
		return nil, fmt.Errorf("failed to convert expression to WhereGroup: %w", err)
	}

	return group, nil
}

// convertExpression recursively converts an expression into WhereGroup clauses
func (a *ExpressionAdapter) convertExpression(expr Expression, group *queryparser.WhereGroup, currentLogic string) error {
	switch expr := expr.(type) {
	case *BinaryExpression:
		return a.convertBinaryExpression(expr, group, currentLogic)

	case *UnaryExpression:
		return a.convertUnaryExpression(expr, group, currentLogic)

	case *GroupedExpression:
		// Grouped expressions become subgroups
		return a.convertGroupedExpression(expr, group, currentLogic)

	case *LiteralExpression, *IdentifierExpression:
		// Standalone literals/identifiers don't make sense in WHERE clause
		return fmt.Errorf("unexpected standalone literal or identifier in WHERE clause")

	case *CallExpression:
		// TODO: I need to handle function calls when we implement them
		return fmt.Errorf("function calls in WHERE clause not yet supported")

	case *ArrayExpression:
		// TODO: I need to handle array expressions for IN clauses
		return fmt.Errorf("array expressions in WHERE clause not yet supported")

	default:
		return fmt.Errorf("unsupported expression type: %T", expr)
	}
}

// convertBinaryExpression converts a binary expression to WhereClause or SubGroup
func (a *ExpressionAdapter) convertBinaryExpression(expr *BinaryExpression, group *queryparser.WhereGroup, currentLogic string) error {
	// Handle logical operators (AND, OR) - these create structure
	if expr.Operator == TOKEN_AND || expr.Operator == TOKEN_OR {
		logic := "AND"
		if expr.Operator == TOKEN_OR {
			logic = "OR"
		}

		// If the logic matches the current group's logic, add to this group
		// Otherwise, create a subgroup
		if logic == group.Operator {
			// Add both sides to the current group
			if err := a.convertExpression(expr.Left, group, logic); err != nil {
				return err
			}
			if err := a.convertExpression(expr.Right, group, logic); err != nil {
				return err
			}
		} else {
			// Create a new subgroup with the different logic
			subGroup := queryparser.WhereGroup{
				Clauses:   []queryparser.WhereClause{},
				SubGroups: []queryparser.WhereGroup{},
				Operator:  logic,
			}

			if err := a.convertExpression(expr.Left, &subGroup, logic); err != nil {
				return err
			}
			if err := a.convertExpression(expr.Right, &subGroup, logic); err != nil {
				return err
			}

			group.SubGroups = append(group.SubGroups, subGroup)
		}

		return nil
	}

	// Handle comparison operators - these create clauses
	if a.isComparisonOperator(expr.Operator) {
		// Extract field and value from the expression
		field, value, err := a.extractFieldAndValue(expr)
		if err != nil {
			return fmt.Errorf("failed to extract field and value: %w", err)
		}

		// Convert operator token to string
		operator := a.tokenToOperatorString(expr.Operator)

		// Create the WhereClause
		clause := queryparser.WhereClause{
			Field:    field,
			Operator: operator,
			Value:    value,
			Logic:    currentLogic,
		}

		group.Clauses = append(group.Clauses, clause)
		return nil
	}

	// Handle arithmetic operators in WHERE clause (rare but possible)
	// TODO: I might need to support computed expressions in WHERE clauses later
	return fmt.Errorf("arithmetic operators in WHERE clause not yet supported")
}

// convertUnaryExpression converts a unary expression (typically NOT)
func (a *ExpressionAdapter) convertUnaryExpression(expr *UnaryExpression, group *queryparser.WhereGroup, currentLogic string) error {
	if expr.Operator == TOKEN_NOT {
		// NOT expressions are tricky - we need to negate the inner expression
		// For now, we'll create a subgroup with the negated logic
		// TODO: I need to implement proper NOT handling - might need to enhance WhereGroup structure
		// TODO: I could also evaluate the NOT at runtime in the evaluator instead of during conversion
		return fmt.Errorf("NOT operator in WHERE clause not yet fully supported in adapter (use evaluator instead)")
	}

	// Other unary operators don't make sense in WHERE clause
	return fmt.Errorf("unsupported unary operator in WHERE clause: %s", expr.Operator)
}

// convertGroupedExpression converts a grouped (parenthesized) expression into a subgroup
func (a *ExpressionAdapter) convertGroupedExpression(expr *GroupedExpression, group *queryparser.WhereGroup, currentLogic string) error {
	// Create a new subgroup for the grouped expression
	subGroup := queryparser.WhereGroup{
		Clauses:   []queryparser.WhereClause{},
		SubGroups: []queryparser.WhereGroup{},
		Operator:  currentLogic, // Inherit the current logic
	}

	// Convert the inner expression into the subgroup
	if err := a.convertExpression(expr.Expression, &subGroup, currentLogic); err != nil {
		return err
	}

	// Add the subgroup to the parent group
	group.SubGroups = append(group.SubGroups, subGroup)
	return nil
}

// extractFieldAndValue extracts the field name and comparison value from a binary expression
// This handles both "field == value" and "value == field" orderings
// Field names can be either unquoted identifiers or quoted strings
func (a *ExpressionAdapter) extractFieldAndValue(expr *BinaryExpression) (string, interface{}, error) {
	// Try left side as qualified identifier ("Bundle"."Field")
	if qualIdent, ok := expr.Left.(*QualifiedIdentifierExpression); ok {
		value, err := a.extractLiteralValue(expr.Right)
		if err != nil {
			return "", nil, fmt.Errorf("right side is not a literal value: %w", err)
		}
		// Format as "Bundle"."Field"
		return fmt.Sprintf(`"%s"."%s"`, qualIdent.Bundle, qualIdent.Field), value, nil
	}

	// Try right side as qualified identifier (reversed comparison)
	if qualIdent, ok := expr.Right.(*QualifiedIdentifierExpression); ok {
		value, err := a.extractLiteralValue(expr.Left)
		if err != nil {
			return "", nil, fmt.Errorf("left side is not a literal value: %w", err)
		}
		// Format as "Bundle"."Field"
		return fmt.Sprintf(`"%s"."%s"`, qualIdent.Bundle, qualIdent.Field), value, nil
	}

	// Try left side as field (unquoted identifier)
	if ident, ok := expr.Left.(*IdentifierExpression); ok {
		value, err := a.extractLiteralValue(expr.Right)
		if err != nil {
			return "", nil, fmt.Errorf("right side is not a literal value: %w", err)
		}
		return ident.Name, value, nil
	}

	// Try right side as field (reversed comparison - unquoted identifier)
	if ident, ok := expr.Right.(*IdentifierExpression); ok {
		value, err := a.extractLiteralValue(expr.Left)
		if err != nil {
			return "", nil, fmt.Errorf("left side is not a literal value: %w", err)
		}
		// Note: For reversed comparisons, we might need to flip the operator
		// TODO: I need to handle operator reversal for "value < field" vs "field > value"
		return ident.Name, value, nil
	}

	// Handle quoted field names: "field" == value or value == "field"
	// When both sides are literals, assume left side is the field name (standard SQL convention)
	leftLit, leftIsLit := expr.Left.(*LiteralExpression)
	rightLit, rightIsLit := expr.Right.(*LiteralExpression)

	if leftIsLit && rightIsLit {
		// Both sides are literals - assume left is field, right is value
		// This handles: "DocumentID" == "123"
		if fieldName, isString := leftLit.Value.(string); isString {
			return fieldName, rightLit.Value, nil
		}
	}

	// Single literal on left (not identifier case already handled above)
	// This shouldn't normally happen but handle it for completeness
	if leftIsLit {
		if fieldName, isString := leftLit.Value.(string); isString {
			value, err := a.extractLiteralValue(expr.Right)
			if err != nil {
				return "", nil, fmt.Errorf("right side is not a literal value: %w", err)
			}
			return fieldName, value, nil
		}
	}

	return "", nil, fmt.Errorf("comparison must have at least one identifier or quoted field name")
}

// extractLiteralValue extracts the value from a literal expression
func (a *ExpressionAdapter) extractLiteralValue(expr Expression) (interface{}, error) {
	switch expr := expr.(type) {
	case *LiteralExpression:
		// Check if the literal is a DateTime/Date string and parse it to time.Time
		// This is necessary for WHERE clause comparisons to work correctly
		if strVal, ok := expr.Value.(string); ok {
			// Check if it looks like a DateTime/Date (ISO 8601 format)
			if len(strVal) >= 10 && strVal[4] == '-' && strVal[7] == '-' {
				// Try to parse as DateTime
				if parsedTime, _, err := utils.ParseDateTime(strVal); err == nil {
					return parsedTime, nil
				}
			}
		}
		return expr.Value, nil
	case *IdentifierExpression:
		// In some cases, an identifier might be used as a value (field-to-field comparison)
		// TODO: I need to decide how to handle field-to-field comparisons
		return expr.Name, nil
	case *GroupedExpression:
		// Recurse into grouped expressions
		return a.extractLiteralValue(expr.Expression)
	default:
		return nil, fmt.Errorf("expected literal value, got %T", expr)
	}
}

// isComparisonOperator checks if a token is a comparison operator
func (a *ExpressionAdapter) isComparisonOperator(token TokenType) bool {
	switch token {
	case TOKEN_EQ, TOKEN_NEQ, TOKEN_LT, TOKEN_LTE, TOKEN_GT, TOKEN_GTE:
		return true
	default:
		return false
	}
}

// tokenToOperatorString converts a token type to an operator string
func (a *ExpressionAdapter) tokenToOperatorString(token TokenType) string {
	switch token {
	case TOKEN_EQ:
		return "=="
	case TOKEN_NEQ:
		return "!="
	case TOKEN_LT:
		return "<"
	case TOKEN_LTE:
		return "<="
	case TOKEN_GT:
		return ">"
	case TOKEN_GTE:
		return ">="
	default:
		return token.String()
	}
}

// ToWhereClause converts a simple comparison expression directly to a WhereClause
// This is an optimization for simple WHERE clauses that don't need full WhereGroup structure
func (a *ExpressionAdapter) ToWhereClause(expr Expression) (*queryparser.WhereClause, error) {
	binaryExpr, ok := expr.(*BinaryExpression)
	if !ok {
		return nil, fmt.Errorf("expected binary comparison expression, got %T", expr)
	}

	if !a.isComparisonOperator(binaryExpr.Operator) {
		return nil, fmt.Errorf("expected comparison operator, got %s", binaryExpr.Operator)
	}

	field, value, err := a.extractFieldAndValue(binaryExpr)
	if err != nil {
		return nil, err
	}

	operator := a.tokenToOperatorString(binaryExpr.Operator)

	return &queryparser.WhereClause{
		Field:    field,
		Operator: operator,
		Value:    value,
		Logic:    "AND", // Default logic
	}, nil
}
