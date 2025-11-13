package syndrQL

import (
	"fmt"
	"strconv"
	"strings"
	"syndrdb/src/internal/domain/models"
	"syndrdb/src/internal/query/queryparser"

	"go.uber.org/zap"
)

/*
evaluator.go

This file implements runtime evaluation of Expression AST nodes against document data.
It provides the bridge between the parsed Expression tree and actual document field values,
enabling WHERE clause filtering, HAVING clause evaluation, and computed field expressions.

Key responsibilities:
- Evaluating Expression nodes against document data
- Type coercion and comparison logic
- NULL value handling (::SYNDR_NULL:: magic value)
- Error propagation for invalid expressions

The evaluator follows the Visitor pattern, with each Expression type implementing
its own evaluation logic through the Evaluate method.
*/

// ExpressionEvaluator evaluates Expression AST nodes against document data
type ExpressionEvaluator struct {
	logger *zap.SugaredLogger
}

// NewExpressionEvaluator creates a new expression evaluator
func NewExpressionEvaluator(logger *zap.SugaredLogger) *ExpressionEvaluator {
	return &ExpressionEvaluator{
		logger: logger,
	}
}

// Evaluate evaluates an expression against a document and returns the result
// This is the main entry point for expression evaluation
func (e *ExpressionEvaluator) Evaluate(expr Expression, doc *models.Document) (interface{}, error) {
	if expr == nil {
		return nil, fmt.Errorf("cannot evaluate nil expression")
	}

	if doc == nil {
		return nil, fmt.Errorf("cannot evaluate expression against nil document")
	}

	switch expr := expr.(type) {
	case *LiteralExpression:
		return e.evaluateLiteral(expr)
	case *IdentifierExpression:
		return e.evaluateIdentifier(expr, doc)
	case *BinaryExpression:
		return e.evaluateBinary(expr, doc)
	case *UnaryExpression:
		return e.evaluateUnary(expr, doc)
	case *GroupedExpression:
		return e.evaluateGrouped(expr, doc)
	case *CallExpression:
		return e.evaluateCall(expr, doc)
	case *ArrayExpression:
		return e.evaluateArray(expr, doc)
	default:
		return nil, fmt.Errorf("unsupported expression type: %T", expr)
	}
}

// evaluateLiteral evaluates a literal value (string, number, boolean, null)
func (e *ExpressionEvaluator) evaluateLiteral(expr *LiteralExpression) (interface{}, error) {
	return expr.Value, nil
}

// evaluateIdentifier evaluates an identifier (field name) by looking it up in the document
func (e *ExpressionEvaluator) evaluateIdentifier(expr *IdentifierExpression, doc *models.Document) (interface{}, error) {
	fieldName := expr.Name

	// Handle bundle.field notation - strip bundle prefix
	if strings.Contains(fieldName, ".") {
		parts := strings.SplitN(fieldName, ".", 2)
		fieldName = parts[1]
	}

	// Remove quotes if present
	fieldName = strings.Trim(fieldName, "\"")

	// Special case: DocumentID field
	if strings.EqualFold(fieldName, "documentid") {
		return doc.DocumentID, nil
	}

	// Look up field in document
	if field, exists := doc.Fields[fieldName]; exists {
		return field.Value, nil
	}

	// Field not found - return nil (will be treated as NULL in comparisons)
	e.logger.Debugf("Field '%s' not found in document, treating as NULL", fieldName)
	return nil, nil
}

// evaluateBinary evaluates a binary expression (e.g., a == b, x AND y)
func (e *ExpressionEvaluator) evaluateBinary(expr *BinaryExpression, doc *models.Document) (interface{}, error) {
	// Evaluate left side
	left, err := e.Evaluate(expr.Left, doc)
	if err != nil {
		return nil, fmt.Errorf("error evaluating left side of binary expression: %w", err)
	}

	// For logical operators, implement short-circuit evaluation
	if expr.Operator == TOKEN_AND {
		// Short-circuit: if left is false, return false without evaluating right
		leftBool, err := e.toBool(left)
		if err != nil {
			return nil, fmt.Errorf("AND operator requires boolean operands: %w", err)
		}
		if !leftBool {
			return false, nil
		}

		// Left is true, evaluate right
		right, err := e.Evaluate(expr.Right, doc)
		if err != nil {
			return nil, fmt.Errorf("error evaluating right side of AND: %w", err)
		}

		rightBool, err := e.toBool(right)
		if err != nil {
			return nil, fmt.Errorf("AND operator requires boolean operands: %w", err)
		}

		return leftBool && rightBool, nil
	}

	if expr.Operator == TOKEN_OR {
		// Short-circuit: if left is true, return true without evaluating right
		leftBool, err := e.toBool(left)
		if err != nil {
			return nil, fmt.Errorf("OR operator requires boolean operands: %w", err)
		}
		if leftBool {
			return true, nil
		}

		// Left is false, evaluate right
		right, err := e.Evaluate(expr.Right, doc)
		if err != nil {
			return nil, fmt.Errorf("error evaluating right side of OR: %w", err)
		}

		rightBool, err := e.toBool(right)
		if err != nil {
			return nil, fmt.Errorf("OR operator requires boolean operands: %w", err)
		}

		return leftBool || rightBool, nil
	}

	// For all other operators, evaluate right side
	right, err := e.Evaluate(expr.Right, doc)
	if err != nil {
		return nil, fmt.Errorf("error evaluating right side of binary expression: %w", err)
	}

	// Handle comparison operators
	switch expr.Operator {
	case TOKEN_EQ: // ==
		return e.compareValues(left, right, func(a, b float64) bool { return a == b })
	case TOKEN_NEQ: // !=
		return e.compareValues(left, right, func(a, b float64) bool { return a != b })
	case TOKEN_LT: // <
		return e.compareValues(left, right, func(a, b float64) bool { return a < b })
	case TOKEN_LTE: // <=
		return e.compareValues(left, right, func(a, b float64) bool { return a <= b })
	case TOKEN_GT: // >
		return e.compareValues(left, right, func(a, b float64) bool { return a > b })
	case TOKEN_GTE: // >=
		return e.compareValues(left, right, func(a, b float64) bool { return a >= b })

	// LIKE operator for pattern matching
	case TOKEN_LIKE:
		return e.evaluateLike(left, right, false)

	// Arithmetic operators (for computed expressions)
	case TOKEN_PLUS:
		return e.arithmeticOp(left, right, func(a, b float64) float64 { return a + b })
	case TOKEN_MINUS:
		return e.arithmeticOp(left, right, func(a, b float64) float64 { return a - b })
	case TOKEN_MULTIPLY:
		return e.arithmeticOp(left, right, func(a, b float64) float64 { return a * b })
	case TOKEN_DIVIDE:
		return e.arithmeticOp(left, right, func(a, b float64) float64 { return a / b })
	case TOKEN_MODULO:
		return e.arithmeticOp(left, right, func(a, b float64) float64 { return float64(int(a) % int(b)) })

	default:
		return nil, fmt.Errorf("unsupported binary operator: %s", expr.Operator)
	}
}

// evaluateUnary evaluates a unary expression (e.g., NOT, -, +)
func (e *ExpressionEvaluator) evaluateUnary(expr *UnaryExpression, doc *models.Document) (interface{}, error) {
	// Evaluate operand (Right field in UnaryExpression)
	operand, err := e.Evaluate(expr.Right, doc)
	if err != nil {
		return nil, fmt.Errorf("error evaluating unary operand: %w", err)
	}

	switch expr.Operator {
	case TOKEN_NOT:
		boolVal, err := e.toBool(operand)
		if err != nil {
			return nil, fmt.Errorf("NOT operator requires boolean operand: %w", err)
		}
		return !boolVal, nil

	case TOKEN_MINUS:
		numVal, err := e.toFloat64(operand)
		if err != nil {
			return nil, fmt.Errorf("unary minus requires numeric operand: %w", err)
		}
		return -numVal, nil

	case TOKEN_PLUS:
		numVal, err := e.toFloat64(operand)
		if err != nil {
			return nil, fmt.Errorf("unary plus requires numeric operand: %w", err)
		}
		return numVal, nil

	default:
		return nil, fmt.Errorf("unsupported unary operator: %s", expr.Operator)
	}
}

// evaluateGrouped evaluates a grouped expression (parenthesized)
func (e *ExpressionEvaluator) evaluateGrouped(expr *GroupedExpression, doc *models.Document) (interface{}, error) {
	return e.Evaluate(expr.Expression, doc)
}

// evaluateCall evaluates a function call expression
// TODO: I need to implement function evaluation when we add support for functions like UPPER(), LOWER(), etc.
func (e *ExpressionEvaluator) evaluateCall(expr *CallExpression, doc *models.Document) (interface{}, error) {
	// TODO: I will implement built-in functions here (UPPER, LOWER, CONCAT, etc.)
	// TODO: I might want to support user-defined functions in the future
	return nil, fmt.Errorf("function calls not yet implemented: %s", expr.Function)
}

// evaluateArray evaluates an array expression
// TODO: I need to implement array evaluation for IN clauses and array operations
func (e *ExpressionEvaluator) evaluateArray(expr *ArrayExpression, doc *models.Document) (interface{}, error) {
	// TODO: I will implement array element evaluation here
	// TODO: I might want to support array operations like CONTAINS, ANY, ALL
	return nil, fmt.Errorf("array expressions not yet implemented")
}

// compareValues compares two values using the provided comparison function
// This is the same logic as the existing filter_parser.go compareValues function
func (e *ExpressionEvaluator) compareValues(a, b interface{}, compare func(float64, float64) bool) (bool, error) {
	// Handle NULL comparisons using SyndrDB's magic value
	aStr, aIsString := a.(string)
	bStr, bIsString := b.(string)

	// If either value is the NULL magic value
	if (aIsString && aStr == "::SYNDR_NULL::") || (bIsString && bStr == "::SYNDR_NULL::") {
		// NULL == NULL is true, NULL != NULL is false
		// For other comparisons, NULL is always false
		if aIsString && bIsString && aStr == "::SYNDR_NULL::" && bStr == "::SYNDR_NULL::" {
			return compare(0, 0), nil // Compare as equal values
		}
		return false, nil
	}

	// Handle nil values (treat as NULL)
	if a == nil || b == nil {
		return false, nil
	}

	// Try string comparison first (most common case)
	if aIsString && bIsString {
		// For string comparisons, use lexicographical ordering
		switch {
		case compare(0, 0): // Equality check
			return aStr == bStr, nil
		case compare(1, 0): // Greater than
			return aStr > bStr, nil
		case compare(0, 1): // Less than
			return aStr < bStr, nil
		default:
			return aStr == bStr, nil
		}
	}

	// Try boolean comparison
	aBool, aIsBool := a.(bool)
	bBool, bIsBool := b.(bool)
	if aIsBool && bIsBool {
		// Convert bool to float for comparison (true=1, false=0)
		aNum := 0.0
		bNum := 0.0
		if aBool {
			aNum = 1.0
		}
		if bBool {
			bNum = 1.0
		}
		return compare(aNum, bNum), nil
	}

	// Try numeric comparison
	aNum, aErr := e.toFloat64(a)
	bNum, bErr := e.toFloat64(b)

	if aErr == nil && bErr == nil {
		return compare(aNum, bNum), nil
	}

	// If we can't compare, return false
	e.logger.Debugf("Cannot compare values of types %T and %T", a, b)
	return false, nil
}

// arithmeticOp performs arithmetic operations on two values
func (e *ExpressionEvaluator) arithmeticOp(a, b interface{}, op func(float64, float64) float64) (interface{}, error) {
	aNum, err := e.toFloat64(a)
	if err != nil {
		return nil, fmt.Errorf("left operand is not numeric: %w", err)
	}

	bNum, err := e.toFloat64(b)
	if err != nil {
		return nil, fmt.Errorf("right operand is not numeric: %w", err)
	}

	return op(aNum, bNum), nil
}

// evaluateLike evaluates LIKE pattern matching
// Parameters:
//   - left: The value to match (should be a string)
//   - right: The pattern (should be a string with % and _ wildcards)
//   - negate: true for NOT LIKE, false for LIKE
//
// Returns:
//   - bool: whether the pattern matches
//   - error: if either operand is not a string
func (e *ExpressionEvaluator) evaluateLike(left interface{}, right interface{}, negate bool) (interface{}, error) {
	// Type validation - both operands must be strings
	leftStr, leftOk := left.(string)
	if !leftOk {
		return nil, fmt.Errorf("LIKE operator requires string operands, got %T for left side", left)
	}

	rightStr, rightOk := right.(string)
	if !rightOk {
		return nil, fmt.Errorf("LIKE operator requires string operands, got %T for right side (pattern)", right)
	}

	// Handle NULL values - return false (SQL standard)
	if leftStr == "::SYNDR_NULL::" || rightStr == "::SYNDR_NULL::" {
		e.logger.Debugf("LIKE operator: NULL value detected, returning false")
		return negate, nil // false for LIKE, true for NOT LIKE
	}

	// Parse pattern to determine type
	// Note: Case sensitivity is handled in the pattern string itself (N prefix would be in the pattern)
	// For SyndrQL expressions, we'll assume case-sensitive matching unless pattern starts with special marker
	caseInsensitive := false
	pattern := rightStr

	// Check for N prefix marker (if implemented in expression parsing)
	// For now, we'll keep it case-sensitive

	// Analyze pattern
	patternType, _, _, err := queryparser.ParseLikePattern(pattern)
	if err != nil {
		return nil, fmt.Errorf("invalid LIKE pattern: %w", err)
	}

	// Perform pattern matching
	matched := queryparser.MatchLikePattern(leftStr, pattern, patternType, caseInsensitive)

	// Apply negation for NOT LIKE
	if negate {
		matched = !matched
	}

	return matched, nil
}

// toBool converts a value to boolean
func (e *ExpressionEvaluator) toBool(v interface{}) (bool, error) {
	if v == nil {
		return false, nil
	}

	switch val := v.(type) {
	case bool:
		return val, nil
	case string:
		// Empty string is false, non-empty is true
		// Special case: "false", "0", "" are false
		if val == "" || val == "false" || val == "0" || val == "::SYNDR_NULL::" {
			return false, nil
		}
		return true, nil
	case int, int64, float64:
		// Zero is false, non-zero is true
		num, _ := e.toFloat64(val)
		return num != 0, nil
	default:
		return false, fmt.Errorf("cannot convert %T to boolean", v)
	}
}

// toFloat64 converts a value to float64 for numeric comparisons
// This matches the existing filter_parser.go type coercion logic
func (e *ExpressionEvaluator) toFloat64(v interface{}) (float64, error) {
	if v == nil {
		return 0, fmt.Errorf("cannot convert nil to float64")
	}

	switch val := v.(type) {
	case float64:
		return val, nil
	case int:
		return float64(val), nil
	case int64:
		return float64(val), nil
	case string:
		// Handle NULL magic value
		if val == "::SYNDR_NULL::" {
			return 0, fmt.Errorf("cannot convert NULL to numeric value")
		}
		// Try to parse as float
		f, err := strconv.ParseFloat(val, 64)
		if err != nil {
			return 0, fmt.Errorf("cannot convert string '%s' to float64: %w", val, err)
		}
		return f, nil
	case bool:
		// Convert bool to numeric (true=1, false=0)
		if val {
			return 1.0, nil
		}
		return 0.0, nil
	default:
		return 0, fmt.Errorf("cannot convert %T to float64", v)
	}
}

// EvaluateAsBool evaluates an expression and returns the result as a boolean
// This is a convenience method for WHERE clause evaluation
func (e *ExpressionEvaluator) EvaluateAsBool(expr Expression, doc *models.Document) (bool, error) {
	result, err := e.Evaluate(expr, doc)
	if err != nil {
		return false, err
	}

	return e.toBool(result)
}
