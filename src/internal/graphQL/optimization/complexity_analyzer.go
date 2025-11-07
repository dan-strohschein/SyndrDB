// Package optimization provides query optimization and analysis for GraphQL queries
//
// PURPOSE:
// This file implements query complexity analysis to protect against expensive
// queries that could cause performance issues or resource exhaustion.
//
// KEY FEATURES:
// - Query depth analysis (prevent deeply nested queries)
// - Query breadth analysis (prevent overly wide field selections)
// - Cost estimation based on relationships and data size
// - Configurable limits and thresholds
//
// DESIGN PRINCIPLES:
// - Single Responsibility: Only analyzes query complexity
// - Fail Fast: Reject expensive queries before execution
// - Configurable: Limits can be adjusted per deployment
//
// EXAMPLE USAGE:
//
//   analyzer := NewComplexityAnalyzer(&ComplexityConfig{
//       MaxDepth:        5,  // Maximum nesting depth
//       MaxBreadth:      50, // Maximum fields at any level
//       MaxComplexity:   100, // Maximum total cost
//   })
//
//   result := analyzer.AnalyzeQuery(query, database)
//   if !result.IsAllowed {
//       return fmt.Errorf("query too complex: %s", result.Reason)
//   }
//
// COMPLEXITY CALCULATION:
// - Each field: 1 point
// - Each relationship: 5 points (potentially expensive)
// - Each level of nesting: multiply by 2
//
// AUTHOR: SyndrDB Development Team
// CREATED: November 2025
// PHASE: 10 (DataLoader & Deep Relationship Optimization)

package optimization

import (
	"fmt"
	"syndrdb/src/internal/domain/models"

	"github.com/vektah/gqlparser/v2/ast"
)

// ComplexityConfig defines limits for query complexity
type ComplexityConfig struct {
	// MaxDepth is the maximum allowed nesting depth (default: 5)
	// Example: author.books.reviews.user.posts would be depth 5
	MaxDepth int

	// MaxBreadth is the maximum number of fields at any single level (default: 50)
	// Example: { field1 field2 field3 ... field50 } = breadth 50
	MaxBreadth int

	// MaxComplexity is the maximum total complexity score (default: 100)
	// Calculated based on fields, relationships, and nesting depth
	MaxComplexity int

	// WarnThreshold is when to log warnings but still allow (default: 70% of max)
	WarnThreshold int
}

// DefaultComplexityConfig returns sensible default limits
func DefaultComplexityConfig() *ComplexityConfig {
	return &ComplexityConfig{
		MaxDepth:      5,
		MaxBreadth:    50,
		MaxComplexity: 100,
		WarnThreshold: 70,
	}
}

// ComplexityResult contains the analysis results
type ComplexityResult struct {
	// IsAllowed indicates if the query should be executed
	IsAllowed bool

	// Depth is the maximum nesting depth found
	Depth int

	// Breadth is the maximum field count at any level
	Breadth int

	// Complexity is the calculated complexity score
	Complexity int

	// Reason explains why query was rejected (if IsAllowed = false)
	Reason string

	// Warnings contains non-fatal issues (high complexity, etc.)
	Warnings []string

	// RecommendedStrategy suggests optimization approach
	// "dataloader", "join", "cache", "paginate"
	RecommendedStrategy string
}

// ComplexityAnalyzer analyzes GraphQL query complexity
type ComplexityAnalyzer struct {
	config *ComplexityConfig
}

// NewComplexityAnalyzer creates a new complexity analyzer
func NewComplexityAnalyzer(config *ComplexityConfig) *ComplexityAnalyzer {
	if config == nil {
		config = DefaultComplexityConfig()
	}
	return &ComplexityAnalyzer{
		config: config,
	}
}

// AnalyzeQuery analyzes a GraphQL query and returns complexity metrics
//
// PARAMETERS:
//
//	query: The parsed GraphQL query AST
//	database: The database context for relationship lookup
//
// RETURNS:
//
//	ComplexityResult with analysis and recommendations
func (ca *ComplexityAnalyzer) AnalyzeQuery(query *ast.QueryDocument, database *models.Database) *ComplexityResult {
	result := &ComplexityResult{
		IsAllowed:           true,
		Warnings:            []string{},
		RecommendedStrategy: "dataloader", // Default recommendation
	}

	// Analyze each operation in the query
	for _, operation := range query.Operations {
		ca.analyzeOperation(operation, database, result, 0)
	}

	// Check against limits
	if result.Depth > ca.config.MaxDepth {
		result.IsAllowed = false
		result.Reason = fmt.Sprintf("query depth %d exceeds maximum %d", result.Depth, ca.config.MaxDepth)
		return result
	}

	if result.Breadth > ca.config.MaxBreadth {
		result.IsAllowed = false
		result.Reason = fmt.Sprintf("query breadth %d exceeds maximum %d", result.Breadth, ca.config.MaxBreadth)
		return result
	}

	if result.Complexity > ca.config.MaxComplexity {
		result.IsAllowed = false
		result.Reason = fmt.Sprintf("query complexity %d exceeds maximum %d", result.Complexity, ca.config.MaxComplexity)
		return result
	}

	// Add warnings for high but allowed values
	if result.Complexity > ca.config.WarnThreshold {
		result.Warnings = append(result.Warnings,
			fmt.Sprintf("High complexity: %d (threshold: %d)", result.Complexity, ca.config.WarnThreshold))
	}

	if result.Depth > 3 {
		result.Warnings = append(result.Warnings,
			fmt.Sprintf("Deep nesting detected: %d levels", result.Depth))
	}

	// Recommend strategy based on characteristics
	if result.Depth > 4 {
		result.RecommendedStrategy = "join" // Deep queries benefit from JOINs
	} else if result.Breadth > 30 {
		result.RecommendedStrategy = "paginate" // Wide queries need pagination
	} else {
		result.RecommendedStrategy = "dataloader" // Most queries benefit from batching
	}

	return result
}

// analyzeOperation recursively analyzes a query operation
func (ca *ComplexityAnalyzer) analyzeOperation(
	operation *ast.OperationDefinition,
	database *models.Database,
	result *ComplexityResult,
	currentDepth int,
) {
	// Update max depth
	if currentDepth > result.Depth {
		result.Depth = currentDepth
	}

	// Analyze selection set
	ca.analyzeSelectionSet(operation.SelectionSet, database, result, currentDepth+1)
}

// analyzeSelectionSet analyzes a selection set (list of fields)
func (ca *ComplexityAnalyzer) analyzeSelectionSet(
	selectionSet ast.SelectionSet,
	database *models.Database,
	result *ComplexityResult,
	currentDepth int,
) {
	if selectionSet == nil || len(selectionSet) == 0 {
		return
	}

	// Update max depth
	if currentDepth > result.Depth {
		result.Depth = currentDepth
	}

	// Count fields at this level (breadth)
	fieldCount := len(selectionSet)
	if fieldCount > result.Breadth {
		result.Breadth = fieldCount
	}

	// Add base complexity for each field
	result.Complexity += fieldCount

	// Analyze each field
	for _, selection := range selectionSet {
		ca.analyzeSelection(selection, database, result, currentDepth)
	}

	// Apply depth multiplier (deeper queries cost more)
	// Depth 1: x1, Depth 2: x1.5, Depth 3: x2, Depth 4: x2.5, etc.
	depthMultiplier := 1.0 + (float64(currentDepth-1) * 0.5)
	result.Complexity = int(float64(result.Complexity) * depthMultiplier)
}

// analyzeSelection analyzes a single selection (field, fragment, etc.)
func (ca *ComplexityAnalyzer) analyzeSelection(
	selection ast.Selection,
	database *models.Database,
	result *ComplexityResult,
	currentDepth int,
) {
	switch sel := selection.(type) {
	case *ast.Field:
		ca.analyzeField(sel, database, result, currentDepth)
	case *ast.FragmentSpread:
		// Fragment spreads add complexity but are currently not deeply analyzed
		result.Complexity += 2
	case *ast.InlineFragment:
		ca.analyzeSelectionSet(sel.SelectionSet, database, result, currentDepth)
	}
}

// analyzeField analyzes a single field and its nested selections
func (ca *ComplexityAnalyzer) analyzeField(
	field *ast.Field,
	database *models.Database,
	result *ComplexityResult,
	currentDepth int,
) {
	// Check if this field has nested selections (indicates relationship)
	if len(field.SelectionSet) > 0 {
		// This is a relationship field - add relationship cost
		result.Complexity += 5 // Relationships are more expensive

		// Recursively analyze nested fields
		ca.analyzeSelectionSet(field.SelectionSet, database, result, currentDepth+1)
	}
}

// AnalyzeQueryDepth is a quick utility to check just the depth
//
// Returns the maximum nesting depth of the query.
// Useful for quick checks before full analysis.
func AnalyzeQueryDepth(query *ast.QueryDocument) int {
	maxDepth := 0

	for _, operation := range query.Operations {
		depth := analyzeSelectionSetDepth(operation.SelectionSet, 1)
		if depth > maxDepth {
			maxDepth = depth
		}
	}

	return maxDepth
}

// analyzeSelectionSetDepth recursively calculates nesting depth
func analyzeSelectionSetDepth(selectionSet ast.SelectionSet, currentDepth int) int {
	if selectionSet == nil || len(selectionSet) == 0 {
		return currentDepth
	}

	maxDepth := currentDepth

	for _, selection := range selectionSet {
		var nestedDepth int

		switch sel := selection.(type) {
		case *ast.Field:
			if len(sel.SelectionSet) > 0 {
				nestedDepth = analyzeSelectionSetDepth(sel.SelectionSet, currentDepth+1)
			} else {
				nestedDepth = currentDepth
			}
		case *ast.InlineFragment:
			nestedDepth = analyzeSelectionSetDepth(sel.SelectionSet, currentDepth+1)
		default:
			nestedDepth = currentDepth
		}

		if nestedDepth > maxDepth {
			maxDepth = nestedDepth
		}
	}

	return maxDepth
}

// ShouldUseDataLoader determines if DataLoader is the best strategy
//
// Returns true if:
// - Query depth is reasonable (< 5)
// - Has relationships (benefit from batching)
// - Not too wide (pagination better for width)
func ShouldUseDataLoader(result *ComplexityResult) bool {
	return result.Depth <= 4 && result.Breadth <= 30 && result.Complexity > 10
}

// ShouldUseJoins determines if JOIN-based optimization is better
//
// Returns true if:
// - Query is very deep (> 4 levels)
// - Relationships are sequential (author→books→reviews→users)
func ShouldUseJoins(result *ComplexityResult) bool {
	return result.Depth > 4
}

// ShouldPaginate determines if pagination is required
//
// Returns true if:
// - Query is very wide (many fields)
// - High complexity score
func ShouldPaginate(result *ComplexityResult) bool {
	return result.Breadth > 30 || result.Complexity > 80
}
