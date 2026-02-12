package planner

import (
	"syndrdb/src/internal/query/queryparser"
)

// ShouldUseIterator determines whether the query plan benefits from pull-based
// iterator execution. The iterator path avoids full materialization and enables
// early termination, yielding the biggest wins for LIMIT queries.
//
// Returns false for queries where all nodes must materialize anyway (e.g. GROUP BY,
// aggregate-only, DISTINCT) since the iterator adds overhead with no benefit.
func ShouldUseIterator(query *queryparser.UnifiedSelectQuery) bool {
	if query == nil {
		return false
	}

	// GROUP BY, aggregate-only, DISTINCT all require materialization → no iterator benefit
	if query.GroupBy != nil && len(query.GroupBy.Fields) > 0 {
		return false
	}
	if query.IsAggregateOnly || query.IsCountOnly {
		return false
	}
	if query.IsDistinct {
		return false
	}
	// JOINs are complex and wrapped with MaterializingIterator anyway
	if len(query.JoinClauses) > 0 {
		return false
	}

	// LIMIT queries benefit most: early termination after N rows
	if query.Limit > 0 {
		return true
	}

	// Simple scan+filter without ORDER BY benefits from streaming (no materialization)
	if query.OrderBy == nil {
		return true
	}

	// ORDER BY without LIMIT must materialize for full sort → marginal benefit
	return false
}

// BuildIteratorPipeline converts an execution node tree into an iterator chain.
// Nodes that natively support iteration get native iterators; others get
// MaterializingIterator wrappers. Returns a factory function that creates fresh
// iterators per execution (iterators are not reusable after Close).
func BuildIteratorPipeline(root ExecutionNode, query *queryparser.UnifiedSelectQuery) func() IteratorNode {
	return func() IteratorNode {
		return buildIterator(root, query)
	}
}

func buildIterator(node ExecutionNode, query *queryparser.UnifiedSelectQuery) IteratorNode {
	switch n := node.(type) {
	case *LimitNode:
		childIter := buildIterator(n.Child, query)

		// Propagate limit to SortIterator for Top-N optimization
		if sortIter, ok := childIter.(*SortIterator); ok {
			sortIter.SetLimit(n.Offset + n.Limit)
		}

		return NewLimitIterator(n, childIter)

	case *SortNode:
		childIter := buildIterator(n.Child, query)
		return NewSortIterator(n, childIter)

	case *FilterNode:
		childIter := buildIterator(n.Child, query)
		return NewFilterIterator(n, childIter)

	case *FullScanNode:
		return NewFullScanIterator(n)

	case *AggregationNode:
		return NewAggregationIterator(n)

	case *IndexOnlyScanNode:
		// Index-only scan delegates to its child; build iterator for the child
		return buildIterator(n.Child, query)

	default:
		// Fallback: wrap any node with MaterializingIterator
		return NewMaterializingIterator(node)
	}
}
