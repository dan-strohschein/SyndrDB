// Package view provides the domain model and business logic for database views in SyndrDB.
// This file contains validation logic for view updatability and query restrictions.
//
// The validator enforces SyndrDB's view updatability rules:
// 1. Regular views with simple JOINs are updatable
// 2. Regular views with aggregate functions (SUM, COUNT, AVG, etc.) are NOT updatable
// 3. Regular views with DISTINCT are NOT updatable
// 4. Materialized views are NEVER updatable (always read-only snapshots)
//
// Updatable views allow INSERT/UPDATE/DELETE operations that pass through to
// the underlying bundles. Non-updatable views are read-only.
//
// When a view is determined to be non-updatable due to aggregates or DISTINCT,
// the validator returns a helpful error message directing users to the enterprise
// edition for advanced features.
//
// Validation checks:
// - Query contains aggregate functions (SUM, COUNT, AVG, MIN, MAX, GROUP_CONCAT)
// - Query contains DISTINCT keyword
// - Query contains GROUP BY clause
// - Query contains HAVING clause
// - View type (materialized views always read-only)
//
// Enterprise upsell messages:
// - "Aggregate functions in views require SyndrDB Enterprise Edition. Consider using materialized views for pre-computed aggregates."
// - "DISTINCT in views requires SyndrDB Enterprise Edition. Consider using materialized views for unique result sets."
//
// Main functions:
// - IsViewUpdatable: Determine if a view allows INSERT/UPDATE/DELETE
// - ContainsAggregates: Check if query has aggregate functions
// - ContainsDistinct: Check if query has DISTINCT keyword
// - ValidateViewDefinition: Comprehensive validation of view query
//
// TODO: I should implement AST-based validation instead of string matching for better accuracy
// TODO: I should add validation for window functions when implemented
package view

import (
	"fmt"
	"strings"

	"go.uber.org/zap"
)

var (
	// AggregateFunctions is a list of SQL aggregate functions that make views non-updatable
	AggregateFunctions = []string{
		"SUM", "COUNT", "AVG", "MIN", "MAX", "GROUP_CONCAT",
	}
)

// ViewValidator validates view definitions and determines updatability
type ViewValidator struct {
	logger *zap.SugaredLogger
}

// NewViewValidator creates a new ViewValidator
func NewViewValidator(logger *zap.SugaredLogger) *ViewValidator {
	return &ViewValidator{
		logger: logger,
	}
}

// IsViewUpdatable determines if a view allows INSERT/UPDATE/DELETE operations
// Returns true if updatable, false otherwise
// Returns error if view definition is invalid
func (v *ViewValidator) IsViewUpdatable(view *View) (bool, error) {
	// Materialized views are never updatable
	if view.Type == ViewTypeMaterialized {
		return false, nil
	}

	// Check for aggregates
	if v.ContainsAggregates(view.Definition) {
		return false, nil
	}

	// Check for DISTINCT
	if v.ContainsDistinct(view.Definition) {
		return false, nil
	}

	// Check for GROUP BY
	if v.ContainsGroupBy(view.Definition) {
		return false, nil
	}

	// Check for HAVING
	if v.ContainsHaving(view.Definition) {
		return false, nil
	}

	// View is updatable
	return true, nil
}

// ContainsAggregates checks if a query contains aggregate functions
// Returns true if aggregates found, false otherwise
func (v *ViewValidator) ContainsAggregates(query string) bool {
	upperQuery := strings.ToUpper(query)

	for _, aggFunc := range AggregateFunctions {
		// Look for function calls: SUM(, COUNT(, etc.
		pattern := aggFunc + "("
		if strings.Contains(upperQuery, pattern) {
			return true
		}
	}

	return false
}

// ContainsDistinct checks if a query contains the DISTINCT keyword
// Returns true if DISTINCT found, false otherwise
func (v *ViewValidator) ContainsDistinct(query string) bool {
	upperQuery := strings.ToUpper(query)
	return strings.Contains(upperQuery, "DISTINCT")
}

// ContainsGroupBy checks if a query contains GROUP BY clause
// Returns true if GROUP BY found, false otherwise
func (v *ViewValidator) ContainsGroupBy(query string) bool {
	upperQuery := strings.ToUpper(query)
	return strings.Contains(upperQuery, "GROUP BY")
}

// ContainsHaving checks if a query contains HAVING clause
// Returns true if HAVING found, false otherwise
func (v *ViewValidator) ContainsHaving(query string) bool {
	upperQuery := strings.ToUpper(query)
	return strings.Contains(upperQuery, "HAVING")
}

// ValidateViewDefinition performs comprehensive validation of a view definition
// Returns error with enterprise upsell message if aggregates/DISTINCT detected
func (v *ViewValidator) ValidateViewDefinition(definition string, viewType ViewType) error {
	// Check for aggregates
	if v.ContainsAggregates(definition) {
		return fmt.Errorf("Aggregate functions in views require SyndrDB Enterprise Edition. Consider using materialized views for pre-computed aggregates.")
	}

	// Check for DISTINCT
	if v.ContainsDistinct(definition) {
		return fmt.Errorf("DISTINCT in views requires SyndrDB Enterprise Edition. Consider using materialized views for unique result sets.")
	}

	// Check for GROUP BY
	if v.ContainsGroupBy(definition) {
		return fmt.Errorf("GROUP BY in views requires SyndrDB Enterprise Edition. Consider using materialized views for grouped data.")
	}

	// Check for HAVING
	if v.ContainsHaving(definition) {
		return fmt.Errorf("HAVING in views requires SyndrDB Enterprise Edition. Consider using materialized views for filtered aggregates.")
	}

	// Definition is valid
	return nil
}

// ValidateUpdateOperation validates if an update operation is allowed on a view
// Returns error if view is not updatable or operation is not supported
func (v *ViewValidator) ValidateUpdateOperation(view *View, operation string) error {
	// Check if view is updatable
	updatable, err := v.IsViewUpdatable(view)
	if err != nil {
		return err
	}

	if !updatable {
		if view.Type == ViewTypeMaterialized {
			return fmt.Errorf("Cannot %s materialized view '%s'. Materialized views are read-only snapshots. Use REFRESH MATERIALIZED VIEW to update data.", operation, view.ViewName)
		}
		return fmt.Errorf("Cannot %s view '%s'. View contains aggregates, DISTINCT, GROUP BY, or HAVING which make it non-updatable.", operation, view.ViewName)
	}

	return nil
}
