package subquery

import (
	"fmt"
	"syndrdb/src/internal/domain/models"
)

/*
types.go

This file defines core types and enums for subquery execution.
Includes MaterializationStrategy enum, SubqueryResult structure, and error types.

Design follows OCP: New strategies can be added without modifying existing code
*/

// MaterializationStrategy defines how subquery results are materialized for outer query
type MaterializationStrategy int

const (
	// STRATEGY_INLIST materializes subquery into memory array for small result sets
	// Used when: rowCount < SmallThreshold AND estimatedMemory < MaxInListMemoryMB
	// Complexity: O(N*M) where N=outer rows, M=subquery rows (acceptable for small M)
	STRATEGY_INLIST MaterializationStrategy = iota

	// STRATEGY_HASHJOIN builds hash table from subquery for O(1) lookups
	// Used when: SmallThreshold <= rowCount < MediumThreshold
	// OR: rowCount < SmallThreshold BUT estimatedMemory > MaxInListMemoryMB
	// Complexity: O(N+M) build + O(N) probe
	STRATEGY_HASHJOIN

	// STRATEGY_INDEXED_LOOKUP uses existing index on outer bundle for direct lookups
	// Used when: rowCount >= MediumThreshold AND index exists on correlation field
	// Complexity: O(M * log(N)) using index scans
	// Tier 1: NOT IMPLEMENTED (requires correlation field detection)
	STRATEGY_INDEXED_LOOKUP

	// STRATEGY_UNKNOWN indicates strategy not yet determined
	STRATEGY_UNKNOWN
)

// String returns string representation of MaterializationStrategy
func (ms MaterializationStrategy) String() string {
	switch ms {
	case STRATEGY_INLIST:
		return "InList"
	case STRATEGY_HASHJOIN:
		return "HashJoin"
	case STRATEGY_INDEXED_LOOKUP:
		return "IndexedLookup"
	case STRATEGY_UNKNOWN:
		return "Unknown"
	default:
		return fmt.Sprintf("MaterializationStrategy(%d)", ms)
	}
}

// SubqueryResult holds materialized results from subquery execution
type SubqueryResult struct {
	// Values contains the result values from subquery (for IN/NOT IN)
	// For EXISTS: empty if false, non-empty if true
	Values []models.FieldValue

	// ContainsNull tracks if subquery result contains NULL values
	// Critical for NOT IN semantics: NOT IN (1, 2, NULL) returns empty set
	ContainsNull bool

	// Strategy indicates which materialization strategy was used
	Strategy MaterializationStrategy

	// RowCount is the number of rows returned by subquery
	RowCount int

	// MemoryBytes is estimated memory consumption of materialized result
	MemoryBytes int64
}

// IsEmpty returns true if subquery returned no rows
func (sr *SubqueryResult) IsEmpty() bool {
	return sr.RowCount == 0
}

// HasValues returns true if subquery returned at least one row
func (sr *SubqueryResult) HasValues() bool {
	return sr.RowCount > 0
}

// SubqueryConfigError represents configuration validation error
type SubqueryConfigError struct {
	Field  string
	Value  interface{}
	Reason string
}

func (e *SubqueryConfigError) Error() string {
	return fmt.Sprintf("invalid subquery config: field=%s value=%v reason=%s", e.Field, e.Value, e.Reason)
}

// SubqueryExecutionError represents runtime subquery execution error
type SubqueryExecutionError struct {
	SubquerySQL string
	Phase       string // "parsing", "execution", "materialization"
	Cause       error
}

func (e *SubqueryExecutionError) Error() string {
	return fmt.Sprintf("subquery execution failed in %s phase: %v\nSubquery: %s", e.Phase, e.Cause, e.SubquerySQL)
}

func (e *SubqueryExecutionError) Unwrap() error {
	return e.Cause
}

// SubqueryDepthExceededError indicates nesting depth limit exceeded
type SubqueryDepthExceededError struct {
	CurrentDepth int
	MaxDepth     int
}

func (e *SubqueryDepthExceededError) Error() string {
	return fmt.Sprintf("subquery nesting depth %d exceeds maximum %d", e.CurrentDepth, e.MaxDepth)
}
