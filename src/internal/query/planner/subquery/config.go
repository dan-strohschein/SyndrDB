package subquery

import (
	"syndrdb/src/pkg/settings"
)

/*
config.go

This file defines configuration structures for subquery execution strategy selection.
Configuration values are loaded from syndrdb.yml and control runtime behavior of
the subquery executor.

STRATEGY THRESHOLDS:
- SmallThreshold: Below this, use IN-list materialization (default: 100)
- MediumThreshold: Below this, use HashJoin strategy (default: 10,000)
- Above medium: Use IndexedLookup if index available, else HashJoin

SAFETY LIMITS:
- MaxDepth: Maximum nesting depth (default: 3) to prevent stack overflow
- MaxInListMemoryMB: Memory limit for IN-list (default: 10MB) forces HashJoin fallback

Design follows SRP: Single source of truth for subquery configuration
*/

// SubqueryConfig holds configuration for subquery execution
type SubqueryConfig struct {
	// SmallThreshold is the row count below which IN-list strategy is used
	// Default: 100 rows
	SmallThreshold int

	// MediumThreshold is the row count below which HashJoin strategy is used
	// Above this threshold, IndexedLookup is preferred if an index is available
	// Default: 10,000 rows
	MediumThreshold int

	// MaxDepth is the maximum allowed nesting depth for subqueries
	// Prevents stack overflow and ensures reasonable query complexity
	// Default: 3 levels
	MaxDepth int

	// MaxInListMemoryMB is the maximum memory allowed for IN-list materialization
	// If estimated memory exceeds this, forces HashJoin strategy even below SmallThreshold
	// Default: 10 MB
	MaxInListMemoryMB int
}

// DefaultSubqueryConfig returns configuration with default values
func DefaultSubqueryConfig() *SubqueryConfig {
	return &SubqueryConfig{
		SmallThreshold:    100,
		MediumThreshold:   10000,
		MaxDepth:          3,
		MaxInListMemoryMB: 10,
	}
}

// NewSubqueryConfigFromSettings creates configuration from application settings
func NewSubqueryConfigFromSettings(args *settings.Arguments) *SubqueryConfig {
	return &SubqueryConfig{
		SmallThreshold:    args.SubquerySmallThreshold,
		MediumThreshold:   args.SubqueryHashThreshold,
		MaxDepth:          args.SubqueryMaxDepth,
		MaxInListMemoryMB: args.SubqueryMaxInListMemoryMB,
	}
}

// ValidateConfig validates configuration values and returns error if invalid
func (c *SubqueryConfig) ValidateConfig() error {
	if c.SmallThreshold < 0 {
		return &SubqueryConfigError{Field: "SmallThreshold", Value: c.SmallThreshold, Reason: "must be non-negative"}
	}
	if c.MediumThreshold < c.SmallThreshold {
		return &SubqueryConfigError{Field: "MediumThreshold", Value: c.MediumThreshold, Reason: "must be >= SmallThreshold"}
	}
	if c.MaxDepth < 1 {
		return &SubqueryConfigError{Field: "MaxDepth", Value: c.MaxDepth, Reason: "must be at least 1"}
	}
	if c.MaxInListMemoryMB < 1 {
		return &SubqueryConfigError{Field: "MaxInListMemoryMB", Value: c.MaxInListMemoryMB, Reason: "must be at least 1"}
	}
	return nil
}
