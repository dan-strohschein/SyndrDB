package server

import (
	"syndrdb/src/internal/query/planner/subquery"
)

/*
subquery_metrics.go

This file implements the MetricsRecorder interface for subquery execution metrics.
It adapts the ServerMetrics to the interface expected by the subquery executor.

Design follows Adapter pattern: Bridges subquery package and server metrics
without creating circular dependencies.
*/

// SubqueryMetricsRecorder implements subquery.MetricsRecorder for GlobalServerMetrics
type SubqueryMetricsRecorder struct {
	metrics *GlobalServerMetrics
}

// NewSubqueryMetricsRecorder creates a new subquery metrics recorder
func NewSubqueryMetricsRecorder(metrics *GlobalServerMetrics) *SubqueryMetricsRecorder {
	return &SubqueryMetricsRecorder{
		metrics: metrics,
	}
}

// RecordSubqueryExecution records metrics for a completed subquery execution
func (r *SubqueryMetricsRecorder) RecordSubqueryExecution(strategy subquery.MaterializationStrategy, containsNull bool) {
	// Increment total subquery executions
	r.metrics.SubqueryExecutionsTotal.Add(1)

	// Increment strategy-specific counter
	switch strategy {
	case subquery.STRATEGY_INLIST:
		r.metrics.SubqueryInListStrategy.Add(1)
	case subquery.STRATEGY_HASHJOIN:
		r.metrics.SubqueryHashJoinStrategy.Add(1)
	case subquery.STRATEGY_INDEXED_LOOKUP:
		r.metrics.SubqueryIndexedLookupStrategy.Add(1)
	}

	// Track NULL presence (important for NOT IN semantics)
	if containsNull {
		r.metrics.SubqueryContainsNull.Add(1)
	}
}
