package syndrQL

import (
	"fmt"
	"syndrdb/src/pkg/settings"
)

/*
parameter_context.go

This file implements parameter context for parameterized queries in SyndrDB.
It manages parameter binding and validation for queries using $1, $2, $3 syntax.

Key Features:
- 1-indexed parameter storage matching PostgreSQL convention
- Parameter reuse support (e.g., WHERE x=$1 AND y=$1)
- Consecutive parameter numbering validation (no gaps allowed)
- Maximum parameter limit enforcement via settings
- Scalar type support (string, int, float64, bool, SyndrNULL)
- Runtime type coercion matching existing literal handling

Architecture:
- ParameterContext stores parameter values in map for O(1) access
- Validates parameter count against MaxParametersPerQuery setting
- Ensures consecutive numbering starting from $1
- Integrates with expression evaluator for parameter substitution

Design Principles:
- Single Responsibility: Manages parameter storage and validation
- DRY: Reuses existing type coercion logic from evaluator
- Open/Closed: Extensible for future complex type support

TODO: Could add type hint validation here for PREPARE stmt (text, integer) FROM ... syntax in future enhancement
TODO: Could support complex types (arrays, nested objects) as parameters when SyndrDB adds full JSON/array type support
*/

// ParameterContext manages parameter values for parameterized queries
type ParameterContext struct {
	Values map[int]interface{} // 1-indexed parameter values ($1 = Values[1], $2 = Values[2], etc.)
}

// NewParameterContext creates a new parameter context from a slice of values
// Parameters are 1-indexed: values[0] becomes $1, values[1] becomes $2, etc.
// Enforces MaxParametersPerQuery limit from settings
func NewParameterContext(values []interface{}) (*ParameterContext, error) {
	maxParams := settings.GetSettings().MaxParametersPerQuery
	if len(values) > maxParams {
		return nil, fmt.Errorf("parameter count %d exceeds maximum of %d", len(values), maxParams)
	}

	ctx := &ParameterContext{
		Values: make(map[int]interface{}, len(values)),
	}

	// Convert 0-indexed slice to 1-indexed map
	for i, val := range values {
		ctx.Values[i+1] = val
	}

	return ctx, nil
}

// Get retrieves the value for a given parameter index (1-indexed)
// Returns error if parameter is not bound
func (pc *ParameterContext) Get(index int) (interface{}, error) {
	if pc == nil {
		return nil, fmt.Errorf("no parameter context provided")
	}

	val, ok := pc.Values[index]
	if !ok {
		return nil, fmt.Errorf("parameter $%d not bound", index)
	}

	return val, nil
}

// Validate ensures all required parameters are bound with consecutive numbering
// Parameters must start at $1 and have no gaps (e.g., $1, $2, $3 is valid; $1, $3 is not)
func (pc *ParameterContext) Validate(expectedCount int) error {
	if pc == nil {
		if expectedCount > 0 {
			return fmt.Errorf("expected %d parameters but no parameter context provided", expectedCount)
		}
		return nil
	}

	// Check if we have the expected number of parameters
	if len(pc.Values) != expectedCount {
		return fmt.Errorf("expected %d parameters, got %d", expectedCount, len(pc.Values))
	}

	// Validate consecutive numbering starting from $1
	for i := 1; i <= expectedCount; i++ {
		if _, ok := pc.Values[i]; !ok {
			return fmt.Errorf("parameter numbering must be consecutive starting at $1, found gap at $%d", i)
		}
	}

	return nil
}

// Count returns the number of parameters in this context
func (pc *ParameterContext) Count() int {
	if pc == nil {
		return 0
	}
	return len(pc.Values)
}
