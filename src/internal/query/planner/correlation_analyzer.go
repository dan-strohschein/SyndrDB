/*
correlation_analyzer.go

This file implements correlation analysis for subqueries, detecting field
references between outer and inner queries.

DESIGN PRINCIPLES:
- Field Reference Detection: Identify outer query fields used in inner query
- Scope Analysis: Properly handle field name resolution across query levels
- Single Responsibility: Only analyze correlation, don't make execution decisions

CORRELATION DETECTION:
1. Walk inner query's WHERE clause
2. Identify field references
3. Check if fields exist in outer query
4. Mark as correlated if outer field references found
5. Collect all correlated field names

This implements PostgreSQL's correlation detection with adaptations
for SyndrDB's document-based field model.
*/

package planner

import (
	"syndrdb/src/internal/syndrQL"

	"go.uber.org/zap"
)

// CorrelationAnalyzer detects correlated field references in subqueries
type CorrelationAnalyzer struct {
	logger *zap.SugaredLogger
}

// NewCorrelationAnalyzer creates a new correlation analyzer
func NewCorrelationAnalyzer(logger *zap.SugaredLogger) *CorrelationAnalyzer {
	return &CorrelationAnalyzer{
		logger: logger,
	}
}

// AnalyzeSubquery analyzes a subquery for correlation with outer query
// Returns: (isCorrelated bool, correlatedFields []string)
func (ca *CorrelationAnalyzer) AnalyzeSubquery(outerQuery *syndrQL.SelectStatement, innerQuery *syndrQL.SelectStatement) (bool, []string) {
	if outerQuery == nil || innerQuery == nil {
		return false, nil
	}

	// Build set of outer query's available fields
	outerFields := ca.getAvailableFields(outerQuery)
	if len(outerFields) == 0 {
		ca.logger.Debug("No fields in outer query, cannot be correlated")
		return false, nil
	}

	// Collect field references from inner query
	innerFieldRefs := make(map[string]bool)
	ca.collectFieldReferences(innerQuery.WhereClause, innerFieldRefs)

	// TODO: I will add support for correlation detection in SELECT and HAVING clauses
	// when multi-column subquery support is implemented

	// Find intersection: fields referenced in inner that exist in outer
	correlatedFields := make([]string, 0)
	for fieldName := range innerFieldRefs {
		if outerFields[fieldName] {
			correlatedFields = append(correlatedFields, fieldName)
		}
	}

	isCorrelated := len(correlatedFields) > 0

	if isCorrelated {
		ca.logger.Debugf("Detected correlation: inner query references outer fields: %v", correlatedFields)
	} else {
		ca.logger.Debug("No correlation detected")
	}

	return isCorrelated, correlatedFields
}

// getAvailableFields returns all fields available in a query (from bundles and aliases)
func (ca *CorrelationAnalyzer) getAvailableFields(query *syndrQL.SelectStatement) map[string]bool {
	fields := make(map[string]bool)

	// Fields from main bundle
	// Note: In real implementation, would need access to bundle schema
	// For now, we'll rely on field references in WHERE clause
	if query.WhereClause != nil {
		ca.collectFieldReferences(query.WhereClause, fields)
	}

	// Fields from SELECT clause
	for _, field := range query.Fields {
		if ident, ok := field.Expression.(*syndrQL.IdentifierExpression); ok {
			if ident.Name != "" && ident.Name != "*" {
				fields[ident.Name] = true
			}
		}
	}

	// TODO: I will add support for JOIN tables and table-qualified field names
	// when implementing multi-table correlation detection

	return fields
}

// collectFieldReferences walks an expression tree collecting field references
func (ca *CorrelationAnalyzer) collectFieldReferences(expr syndrQL.Expression, fields map[string]bool) {
	if expr == nil {
		return
	}

	switch e := expr.(type) {
	case *syndrQL.IdentifierExpression:
		// Direct field reference
		if e.Name != "" {
			fields[e.Name] = true
		}

	case *syndrQL.QualifiedIdentifierExpression:
		// Qualified field reference like "Bundle"."Field"
		// Store as "Bundle"."Field" format for cross-bundle detection
		if e.Bundle != "" && e.Field != "" {
			qualifiedName := e.Bundle + "." + e.Field
			fields[qualifiedName] = true
		}

	case *syndrQL.BinaryExpression:
		// Recurse into both sides
		ca.collectFieldReferences(e.Left, fields)
		ca.collectFieldReferences(e.Right, fields)

	case *syndrQL.UnaryExpression:
		// Recurse into operand
		ca.collectFieldReferences(e.Right, fields)

	case *syndrQL.CallExpression:
		// Recurse into function arguments
		for _, arg := range e.Arguments {
			ca.collectFieldReferences(arg, fields)
		}

	case *syndrQL.SubqueryExpression:
		// Don't recurse into subqueries - we analyze them separately
		// This prevents mistakenly treating nested subquery fields as current scope

	case *syndrQL.GroupedExpression:
		ca.collectFieldReferences(e.Expression, fields)

	// Literal types don't contain field references
	case *syndrQL.LiteralExpression:
		// No fields - literals don't have field references
	default:
		// Unknown expression type - log warning
		ca.logger.Debugf("Unknown expression type in correlation analysis: %T", e)
	}
}

// IsFieldCorrelated checks if a specific field is correlated
func (ca *CorrelationAnalyzer) IsFieldCorrelated(outerQuery *syndrQL.SelectStatement, innerQuery *syndrQL.SelectStatement, fieldName string) bool {
	isCorrelated, correlatedFields := ca.AnalyzeSubquery(outerQuery, innerQuery)

	if !isCorrelated {
		return false
	}

	for _, f := range correlatedFields {
		if f == fieldName {
			return true
		}
	}

	return false
}

// JoinFieldPair represents a pair of fields for semi-join/anti-join correlation
type JoinFieldPair struct {
	OuterField string // Field name in outer bundle
	InnerField string // Field name in inner bundle
}

// QualifiedCorrelationResult holds the result of qualified correlation analysis
type QualifiedCorrelationResult struct {
	IsCorrelated      bool
	JoinPairs         []JoinFieldPair
	NonCorrelatedExpr syndrQL.Expression // Inner WHERE with correlated predicates removed
}

// AnalyzeCorrelationQualified performs qualified field name correlation analysis.
// It walks the inner WHERE for BinaryExpression nodes with == operator where one side
// is a QualifiedIdentifierExpression referencing the outer bundle. Returns join field
// pairs and the remaining non-correlated inner WHERE expression.
func (ca *CorrelationAnalyzer) AnalyzeCorrelationQualified(outerBundleName string, innerWhere syndrQL.Expression) *QualifiedCorrelationResult {
	result := &QualifiedCorrelationResult{}

	if innerWhere == nil {
		return result
	}

	// Split inner WHERE into correlated and non-correlated predicates
	joinPairs, remaining := ca.splitCorrelatedPredicatesQualified(outerBundleName, innerWhere)

	if len(joinPairs) == 0 {
		return result
	}

	result.IsCorrelated = true
	result.JoinPairs = joinPairs
	result.NonCorrelatedExpr = remaining

	ca.logger.Debugf("Qualified correlation: found %d join pairs for outer bundle '%s'", len(joinPairs), outerBundleName)
	return result
}

// splitCorrelatedPredicatesQualified walks the expression tree and separates
// correlated equality predicates (referencing outerBundleName) from non-correlated ones.
// Returns join field pairs and the remaining non-correlated expression.
func (ca *CorrelationAnalyzer) splitCorrelatedPredicatesQualified(outerBundleName string, expr syndrQL.Expression) ([]JoinFieldPair, syndrQL.Expression) {
	if expr == nil {
		return nil, nil
	}

	switch e := expr.(type) {
	case *syndrQL.BinaryExpression:
		// Check for AND — split recursively
		if e.Operator == syndrQL.TOKEN_AND {
			leftPairs, leftRemaining := ca.splitCorrelatedPredicatesQualified(outerBundleName, e.Left)
			rightPairs, rightRemaining := ca.splitCorrelatedPredicatesQualified(outerBundleName, e.Right)

			pairs := append(leftPairs, rightPairs...)

			// Reconstruct remaining expression
			var remaining syndrQL.Expression
			if leftRemaining != nil && rightRemaining != nil {
				remaining = &syndrQL.BinaryExpression{
					Left:     leftRemaining,
					Operator: syndrQL.TOKEN_AND,
					Right:    rightRemaining,
				}
			} else if leftRemaining != nil {
				remaining = leftRemaining
			} else if rightRemaining != nil {
				remaining = rightRemaining
			}

			return pairs, remaining
		}

		// Check for equality predicate with qualified identifier referencing outer bundle
		if e.Operator == syndrQL.TOKEN_EQ || e.Operator == syndrQL.TOKEN_ASSIGN {
			if pair := ca.extractJoinPair(outerBundleName, e); pair != nil {
				// This is a correlated predicate — no remaining expression
				return []JoinFieldPair{*pair}, nil
			}
		}

		// Not a correlated predicate — return as remaining
		return nil, expr

	case *syndrQL.GroupedExpression:
		return ca.splitCorrelatedPredicatesQualified(outerBundleName, e.Expression)

	default:
		// Any other expression is non-correlated
		return nil, expr
	}
}

// extractJoinPair checks if a binary equality expression has one QualifiedIdentifierExpression
// referencing the outer bundle and extracts the join field pair.
func (ca *CorrelationAnalyzer) extractJoinPair(outerBundleName string, expr *syndrQL.BinaryExpression) *JoinFieldPair {
	leftQual, leftIsQual := expr.Left.(*syndrQL.QualifiedIdentifierExpression)
	rightQual, rightIsQual := expr.Right.(*syndrQL.QualifiedIdentifierExpression)
	leftIdent, leftIsIdent := expr.Left.(*syndrQL.IdentifierExpression)
	rightIdent, rightIsIdent := expr.Right.(*syndrQL.IdentifierExpression)

	// Pattern 1: "Outer"."Field" == "Inner"."Field"
	if leftIsQual && rightIsQual {
		if leftQual.Bundle == outerBundleName {
			return &JoinFieldPair{OuterField: leftQual.Field, InnerField: rightQual.Field}
		}
		if rightQual.Bundle == outerBundleName {
			return &JoinFieldPair{OuterField: rightQual.Field, InnerField: leftQual.Field}
		}
	}

	// Pattern 2: "Outer"."Field" == InnerField (unqualified inner)
	if leftIsQual && rightIsIdent {
		if leftQual.Bundle == outerBundleName {
			return &JoinFieldPair{OuterField: leftQual.Field, InnerField: rightIdent.Name}
		}
	}
	if rightIsQual && leftIsIdent {
		if rightQual.Bundle == outerBundleName {
			return &JoinFieldPair{OuterField: rightQual.Field, InnerField: leftIdent.Name}
		}
	}

	// Pattern 3: "Outer"."Field" == literal — not a join pair
	return nil
}

// GetCorrelationInfo returns detailed correlation information
type CorrelationInfo struct {
	IsCorrelated      bool
	CorrelatedFields  []string
	OuterFields       []string
	InnerFields       []string
	CorrelationDegree float64 // Ratio of correlated fields to total inner fields
}

// GetDetailedCorrelationInfo provides comprehensive correlation analysis
func (ca *CorrelationAnalyzer) GetDetailedCorrelationInfo(outerQuery *syndrQL.SelectStatement, innerQuery *syndrQL.SelectStatement) *CorrelationInfo {
	info := &CorrelationInfo{}

	// Get outer fields
	outerFields := ca.getAvailableFields(outerQuery)
	info.OuterFields = make([]string, 0, len(outerFields))
	for f := range outerFields {
		info.OuterFields = append(info.OuterFields, f)
	}

	// Get inner field references
	innerFieldRefs := make(map[string]bool)
	ca.collectFieldReferences(innerQuery.WhereClause, innerFieldRefs)
	info.InnerFields = make([]string, 0, len(innerFieldRefs))
	for f := range innerFieldRefs {
		info.InnerFields = append(info.InnerFields, f)
	}

	// Find correlated fields
	for fieldName := range innerFieldRefs {
		if outerFields[fieldName] {
			info.CorrelatedFields = append(info.CorrelatedFields, fieldName)
		}
	}

	info.IsCorrelated = len(info.CorrelatedFields) > 0

	// Calculate correlation degree
	if len(innerFieldRefs) > 0 {
		info.CorrelationDegree = float64(len(info.CorrelatedFields)) / float64(len(innerFieldRefs))
	}

	return info
}
