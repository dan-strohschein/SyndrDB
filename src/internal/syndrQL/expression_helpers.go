package syndrQL

import (
	"fmt"
	"strings"
)

/*
expression_helpers.go

This file provides helper functions for extracting structured information from
Expression AST nodes. These helpers are primarily used for query optimization,
particularly index selection and predicate pushdown.

Key responsibilities:
- Extracting simple equality conditions for hash index optimization
- Extracting range conditions for BTree index optimization
- Flattening AND/OR expressions for multi-condition analysis
- Providing structured data from expressions without string parsing

These helpers enable the query planner to make intelligent decisions about
index usage and query execution strategies based on the Expression AST structure.
*/

// ExtractSimpleEquality extracts field and value from a simple equality expression
// Returns (field, value, true) if expr is "field == value" or "Bundle.field == value"
// Returns ("", nil, false) for any other expression pattern
//
// # This is used for hash index optimization where we need exact equality lookups
//
// Examples:
//   - age == 25 → ("age", 25, true)
//   - "Users"."email" == "test@example.com" → ("email", "test@example.com", true)
//   - age > 25 → ("", nil, false) - not equality
//   - age == x + 1 → ("", nil, false) - right side not literal
func ExtractSimpleEquality(expr Expression) (field string, value interface{}, ok bool) {
	// Must be a binary expression
	binary, isBinary := expr.(*BinaryExpression)
	if !isBinary {
		return "", nil, false
	}

	// Must be equality operator
	if binary.Operator != TOKEN_EQ {
		return "", nil, false
	}

	// Extract field name from left side
	var fieldName string
	switch left := binary.Left.(type) {
	case *IdentifierExpression:
		fieldName = strings.Trim(left.Name, "\"")
	case *QualifiedIdentifierExpression:
		// For qualified names, use just the field part (bundle validated elsewhere)
		fieldName = strings.Trim(left.Field, "\"")
	default:
		// Left side is not a simple field reference
		return "", nil, false
	}

	// Extract literal value from right side
	literal, isLiteral := binary.Right.(*LiteralExpression)
	if !isLiteral {
		// Right side is not a literal value
		return "", nil, false
	}

	return fieldName, literal.Value, true
}

// ExtractRangeCondition extracts field, operator, and value from a range comparison
// Returns (field, operator, value, true) for expressions like "field > value"
// Returns ("", "", nil, false) for non-range expressions
//
// # This is used for BTree index optimization where we can use range scans
//
// Supported operators: >, >=, <, <=, !=
//
// Examples:
//   - age > 18 → ("age", ">", 18, true)
//   - price <= 99.99 → ("price", "<=", 99.99, true)
//   - status != "deleted" → ("status", "!=", "deleted", true)
func ExtractRangeCondition(expr Expression) (field string, operator string, value interface{}, ok bool) {
	binary, isBinary := expr.(*BinaryExpression)
	if !isBinary {
		return "", "", nil, false
	}

	// Check for range operators
	var op string
	switch binary.Operator {
	case TOKEN_GT:
		op = ">"
	case TOKEN_GTE:
		op = ">="
	case TOKEN_LT:
		op = "<"
	case TOKEN_LTE:
		op = "<="
	case TOKEN_NEQ:
		op = "!="
	default:
		// Not a range operator
		return "", "", nil, false
	}

	// Extract field name from left side
	var fieldName string
	switch left := binary.Left.(type) {
	case *IdentifierExpression:
		fieldName = strings.Trim(left.Name, "\"")
	case *QualifiedIdentifierExpression:
		fieldName = strings.Trim(left.Field, "\"")
	default:
		return "", "", nil, false
	}

	// Extract literal value from right side
	literal, isLiteral := binary.Right.(*LiteralExpression)
	if !isLiteral {
		return "", "", nil, false
	}

	return fieldName, op, literal.Value, true
}

// ExtractANDClauses flattens a tree of AND expressions into a slice of individual conditions
// This is useful for analyzing compound WHERE clauses and identifying optimization opportunities
//
// Examples:
//   - (age > 18 AND status == "active") → [age > 18, status == "active"]
//   - (a AND (b AND c)) → [a, b, c] (fully flattened)
//   - (age > 18) → [age > 18] (single condition)
//   - (age > 18 OR status == "active") → [(age > 18 OR status == "active")] (OR not flattened)
func ExtractANDClauses(expr Expression) []Expression {
	binary, isBinary := expr.(*BinaryExpression)
	if !isBinary || binary.Operator != TOKEN_AND {
		// Not an AND expression, return as single-element slice
		return []Expression{expr}
	}

	// Recursively flatten AND expressions
	var clauses []Expression
	clauses = append(clauses, ExtractANDClauses(binary.Left)...)
	clauses = append(clauses, ExtractANDClauses(binary.Right)...)
	return clauses
}

// ExtractORClauses flattens a tree of OR expressions into a slice of individual conditions
// This is useful for union-based query optimization
//
// Examples:
//   - (status == "active" OR status == "pending") → [status == "active", status == "pending"]
//   - (a OR (b OR c)) → [a, b, c] (fully flattened)
//   - (status == "active") → [status == "active"] (single condition)
//   - (age > 18 AND status == "active") → [(age > 18 AND status == "active")] (AND not flattened)
func ExtractORClauses(expr Expression) []Expression {
	binary, isBinary := expr.(*BinaryExpression)
	if !isBinary || binary.Operator != TOKEN_OR {
		// Not an OR expression, return as single-element slice
		return []Expression{expr}
	}

	// Recursively flatten OR expressions
	var clauses []Expression
	clauses = append(clauses, ExtractORClauses(binary.Left)...)
	clauses = append(clauses, ExtractORClauses(binary.Right)...)
	return clauses
}

// GetQualifiedFieldName extracts the full qualified field name from an expression
// Returns the field name for IdentifierExpression or QualifiedIdentifierExpression
// Returns empty string for other expression types
//
// Examples:
//   - age → "age"
//   - "Users"."email" → "Users.email"
//   - 25 → "" (literal, not a field)
func GetQualifiedFieldName(expr Expression) string {
	switch e := expr.(type) {
	case *IdentifierExpression:
		return strings.Trim(e.Name, "\"")
	case *QualifiedIdentifierExpression:
		bundle := strings.Trim(e.Bundle, "\"")
		field := strings.Trim(e.Field, "\"")
		return fmt.Sprintf("%s.%s", bundle, field)
	default:
		return ""
	}
}

// TODO: I should add ExtractLIKEPattern() for LIKE operator optimization with indexes
// TODO: I should add ExtractINList() for IN clause optimization
// TODO: I should add CanPushDownToIndex() to determine if an expression can use an index
