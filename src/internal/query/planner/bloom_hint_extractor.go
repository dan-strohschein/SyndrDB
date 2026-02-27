package planner

import (
	"fmt"

	syndrQL "syndrdb/src/internal/syndrQL"
	"syndrdb/src/internal/query/documentscanner"
)

// extractBloomHints decomposes a WHERE expression into equality predicates
// that can be checked against page-level bloom filters. Returns nil if the
// expression is not bloom-friendly (e.g., contains OR, non-equality ops).
//
// Bloom filters only benefit from equality predicates (field == literal).
// For AND combinations, all child hints are collected (skip page if ANY
// hint says definitely absent). For OR, we return nil since a page could
// match on either branch.
func extractBloomHints(whereExpr interface{}) []documentscanner.BloomHint {
	if whereExpr == nil {
		return nil
	}

	expr, ok := whereExpr.(syndrQL.Expression)
	if !ok {
		return nil
	}

	return extractBloomHintsFromExpr(expr)
}

// extractBloomHintsFromExpr recursively extracts bloom hints from an expression tree.
func extractBloomHintsFromExpr(expr syndrQL.Expression) []documentscanner.BloomHint {
	if expr == nil {
		return nil
	}

	binaryExpr, ok := expr.(*syndrQL.BinaryExpression)
	if !ok {
		return nil
	}

	// AND: recurse both sides, combine all hints
	if binaryExpr.Operator == syndrQL.TOKEN_AND {
		left := extractBloomHintsFromExpr(binaryExpr.Left)
		right := extractBloomHintsFromExpr(binaryExpr.Right)
		if left == nil && right == nil {
			return nil
		}
		var combined []documentscanner.BloomHint
		if left != nil {
			combined = append(combined, left...)
		}
		if right != nil {
			combined = append(combined, right...)
		}
		return combined
	}

	// OR: not bloom-friendly (page could match on either branch)
	if binaryExpr.Operator == syndrQL.TOKEN_OR {
		return nil
	}

	// Only equality predicates are useful for bloom filters
	if binaryExpr.Operator != syndrQL.TOKEN_EQ {
		return nil
	}

	// Try: field == literal
	if hint := tryExtractBloomHint(binaryExpr.Left, binaryExpr.Right); hint != nil {
		return []documentscanner.BloomHint{*hint}
	}
	// Try: literal == field (reversed)
	if hint := tryExtractBloomHint(binaryExpr.Right, binaryExpr.Left); hint != nil {
		return []documentscanner.BloomHint{*hint}
	}

	return nil
}

// tryExtractBloomHint tries to extract a bloom hint from (fieldExpr, literalExpr).
func tryExtractBloomHint(fieldExpr, literalExpr syndrQL.Expression) *documentscanner.BloomHint {
	// Resolve field name
	var fieldName string
	switch f := fieldExpr.(type) {
	case *syndrQL.IdentifierExpression:
		fieldName = f.Name
	case *syndrQL.QualifiedIdentifierExpression:
		fieldName = f.Field
	default:
		return nil
	}

	// Resolve literal value
	literal, ok := literalExpr.(*syndrQL.LiteralExpression)
	if !ok || literal.Value == nil {
		return nil
	}

	return &documentscanner.BloomHint{
		FieldName:      fieldName,
		ValueKeyString: fmt.Sprint(literal.Value),
	}
}
