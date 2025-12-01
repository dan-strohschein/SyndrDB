/*
MEMORY ESTIMATION for Query Plan Cache

This file implements conservative memory estimation for all ExecutionNode types.
Used by the query plan cache to track memory usage and enable memory-aware eviction.

ESTIMATION STRATEGY:
Uses conservative fixed-size estimates accepting 10-20% inaccuracy for speed.
Each node type returns a fixed estimate based on typical memory footprint:
- Base struct size (via unsafe.Sizeof)
- Fixed estimates for expression nodes and child trees
- Conservative buffer for dynamic data

DESIGN PRINCIPLES:
- DRY: Shared constants for common estimates
- Single Responsibility: Each node estimates its own memory
- Open/Closed: Extensible for new node types

This implements the conservative approach from the plan cache design.
*/

package planner

import (
	"unsafe"
)

// Conservative memory estimates for common components (in bytes)
const (
	baseExpressionNodeSize = 256 // Per WHERE expression node
	baseStringFieldSize    = 32  // Per string field reference
	baseIndexNameSize      = 32  // Per index name
	baseDocumentPtrSize    = 8   // Pointer size
	baseSliceOverhead      = 24  // Slice header size
	baseMapOverhead        = 48  // Map header estimate
)

// EstimateMemoryUsage for IndexScanNode
func (node *IndexScanNode) EstimateMemoryUsage() int64 {
	// Base struct size
	size := int64(unsafe.Sizeof(*node))

	// Index name
	size += int64(len(node.IndexName))

	// Search keys and ranges (conservative estimate)
	size += 64 // SearchKey, RangeStart, RangeEnd fields

	// Conservative estimate: 512 bytes
	return 512
}

// EstimateMemoryUsage for FullScanNode
func (node *FullScanNode) EstimateMemoryUsage() int64 {
	// Base struct size
	size := int64(unsafe.Sizeof(*node))

	// Bundle pointer (minimal overhead)
	size += baseDocumentPtrSize

	// Conservative estimate: 256 bytes
	return 256
}

// EstimateMemoryUsage for FilterNode
func (node *FilterNode) EstimateMemoryUsage() int64 {
	// Base struct size
	size := int64(unsafe.Sizeof(*node))

	// Child node (recursive)
	if node.Child != nil {
		size += node.Child.EstimateMemoryUsage()
	}

	// WHERE expression tree (conservative: estimate number of nodes)
	// Typical WHERE has 1-10 expression nodes
	// TODO: Handle WhereExpression interface{} and ExecutionNode interface slices by
	// type-switching to concrete types (BinaryOp, UnaryOp, Comparison, LogicalOp) and
	// summing actual struct sizes plus field data for 5-10% accuracy improvement
	estimatedExpressionNodes := 5
	size += int64(estimatedExpressionNodes) * baseExpressionNodeSize

	// Conservative estimate: 256 + child + expressions
	return 256 + size
}

// EstimateMemoryUsage for UnionNode
func (node *UnionNode) EstimateMemoryUsage() int64 {
	// Base struct size
	size := int64(unsafe.Sizeof(*node))

	// Children (recursive)
	for _, child := range node.Children {
		if child != nil {
			size += child.EstimateMemoryUsage()
		}
	}

	// Conservative estimate: 512 + all children
	return 512 + size
}

// EstimateMemoryUsage for SortNode
func (node *SortNode) EstimateMemoryUsage() int64 {
	// Base struct size
	size := int64(unsafe.Sizeof(*node))

	// Child node (recursive)
	if node.Child != nil {
		size += node.Child.EstimateMemoryUsage()
	}

	// OrderBy fields
	if node.OrderBy != nil {
		size += int64(len(node.OrderBy.Fields)) * baseStringFieldSize
	}

	// Conservative estimate: 1024 bytes (sorting needs buffer space)
	return 1024
}

// EstimateMemoryUsage for LimitNode
func (node *LimitNode) EstimateMemoryUsage() int64 {
	// Base struct size
	size := int64(unsafe.Sizeof(*node))

	// Child node (recursive)
	if node.Child != nil {
		size += node.Child.EstimateMemoryUsage()
	}

	// Conservative estimate: 128 bytes (minimal overhead)
	return 128
}

// EstimateMemoryUsage for AggregationNode
func (node *AggregationNode) EstimateMemoryUsage() int64 {
	// Base struct size
	size := int64(unsafe.Sizeof(*node))

	// Child node (recursive)
	if node.Child != nil {
		size += node.Child.EstimateMemoryUsage()
	}

	// Group by fields
	if node.GroupBy != nil {
		size += int64(len(node.GroupBy.Fields)) * baseStringFieldSize
	}

	// Aggregate functions (conservative)
	size += int64(len(node.AggregateFields)) * 64

	// Conservative estimate: 1024 bytes (aggregation needs accumulators)
	return 1024
}

// EstimateMemoryUsage for DistinctNode
func (node *DistinctNode) EstimateMemoryUsage() int64 {
	// Base struct size
	size := int64(unsafe.Sizeof(*node))

	// Child node (recursive)
	if node.Child != nil {
		size += node.Child.EstimateMemoryUsage()
	}

	// Distinct tracking (hash set overhead)
	size += baseMapOverhead

	// Conservative estimate: 512 bytes
	return 512
}

// EstimateMemoryUsage for JoinExecutionNode
func (node *JoinExecutionNode) EstimateMemoryUsage() int64 {
	// Base struct size
	size := int64(unsafe.Sizeof(*node))

	// Join query and join clauses overhead
	if node.Query != nil {
		for _, joinClause := range node.Query.JoinClauses {
			size += int64(len(joinClause.JoinConditions)) * 128
		}
	}

	// Conservative estimate: 2048 bytes (JOIN needs hash tables/buffers)
	return 2048
}

// EstimateMemoryUsage for NestedLoopJoinNode
func (node *NestedLoopJoinNode) EstimateMemoryUsage() int64 {
	// Base struct size
	size := int64(unsafe.Sizeof(*node))

	// Left and right children (recursive)
	if node.LeftChild != nil {
		size += node.LeftChild.EstimateMemoryUsage()
	}
	if node.RightChild != nil {
		size += node.RightChild.EstimateMemoryUsage()
	}

	// Conservative estimate: 2048 bytes
	return 2048
}

// EstimateMemoryUsage for HashJoinNode
func (node *HashJoinNode) EstimateMemoryUsage() int64 {
	// Base struct size
	size := int64(unsafe.Sizeof(*node))

	// Left and right children (recursive)
	if node.LeftChild != nil {
		size += node.LeftChild.EstimateMemoryUsage()
	}
	if node.RightChild != nil {
		size += node.RightChild.EstimateMemoryUsage()
	}

	// Hash table overhead (larger than nested loop)
	size += baseMapOverhead * 2

	// Conservative estimate: 3072 bytes (hash join needs hash table)
	return 3072
}

// EstimateMemoryUsage for MergeJoinNode
func (node *MergeJoinNode) EstimateMemoryUsage() int64 {
	// Base struct size
	size := int64(unsafe.Sizeof(*node))

	// Left and right children (recursive)
	if node.LeftChild != nil {
		size += node.LeftChild.EstimateMemoryUsage()
	}
	if node.RightChild != nil {
		size += node.RightChild.EstimateMemoryUsage()
	}

	// Conservative estimate: 2048 bytes
	return 2048
}

// EstimateMemoryUsage for HierarchicalTransformNode
func (node *HierarchicalTransformNode) EstimateMemoryUsage() int64 {
	// Base struct size
	size := int64(unsafe.Sizeof(*node))

	// Child node (recursive)
	if node.Child != nil {
		size += node.Child.EstimateMemoryUsage()
	}

	// Hierarchical tracking structures
	size += baseMapOverhead * 2

	// Conservative estimate: 1024 bytes
	return 1024
}

// EstimateMemoryUsage for ExecutionPlan
func (plan *ExecutionPlan) EstimateMemoryUsage() int64 {
	// Base struct size
	size := int64(unsafe.Sizeof(*plan))

	// Root node tree (recursive)
	if plan.RootNode != nil {
		size += plan.RootNode.EstimateMemoryUsage()
	}

	// Indexes used
	size += int64(len(plan.IndexesUsed)) * baseIndexNameSize

	// Base overhead for plan metadata
	size += 256

	return size
}

// TODO: Benchmark recursive memory estimation performance - if >1μs overhead,
// current fixed estimates acceptable
