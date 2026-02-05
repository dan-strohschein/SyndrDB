package planner

import (
	"testing"

	"go.uber.org/zap"
)

// collectExecutionNodePointers gathers all ExecutionNode pointers in the tree into the set.
// Used to verify clone produces a disjoint set of node pointers (no sharing).
func collectExecutionNodePointers(node ExecutionNode, set map[ExecutionNode]struct{}) {
	if node == nil {
		return
	}
	set[node] = struct{}{}
	switch n := node.(type) {
	case *LimitNode:
		collectExecutionNodePointers(n.Child, set)
	case *SortNode:
		collectExecutionNodePointers(n.Child, set)
	case *AggregationNode:
		collectExecutionNodePointers(n.Child, set)
	case *FilterNode:
		collectExecutionNodePointers(n.Child, set)
	case *UnionNode:
		for _, c := range n.Children {
			collectExecutionNodePointers(c, set)
		}
	case *DistinctNode:
		collectExecutionNodePointers(n.Child, set)
	case *HierarchicalTransformNode:
		collectExecutionNodePointers(n.Child, set)
	case *NestedLoopJoinNode:
		collectExecutionNodePointers(n.LeftChild, set)
		collectExecutionNodePointers(n.RightChild, set)
	case *HashJoinNode:
		collectExecutionNodePointers(n.LeftChild, set)
		collectExecutionNodePointers(n.RightChild, set)
	case *MergeJoinNode:
		collectExecutionNodePointers(n.LeftChild, set)
		collectExecutionNodePointers(n.RightChild, set)
	case *JoinExecutionNode, *SubqueryNode, *FullScanNode, *IndexScanNode, *BTreeOrderedScanNode, *ExpressionEvaluationNode:
		// Leaf or no child fields we recurse on
	default:
		// Unhandled type - do not recurse to avoid panic in test
	}
}

// TestCloneExecutionNode_NoSharedPointers ensures that cloning a plan produces a tree
// with no shared ExecutionNode pointers (no pointer in the clone equals any in the original).
// This prevents data races when the same plan is served from cache to multiple callers.
func TestCloneExecutionNode_NoSharedPointers(t *testing.T) {
	logger := zap.NewNop().Sugar()

	// Build a tree covering multiple node types: Limit -> Sort -> Filter -> FullScan
	fullScan := &FullScanNode{
		Cost:          1,
		EstimatedRows: 10,
		Logger:        logger,
	}
	filter := &FilterNode{
		Child:      fullScan,
		Cost:       2,
		EstimatedRows: 10,
		Logger:     logger,
	}
	sortNode := NewSortNode(filter, nil, logger)
	limitNode := NewLimitNode(sortNode, 5, 0, logger)

	originalSet := make(map[ExecutionNode]struct{})
	collectExecutionNodePointers(limitNode, originalSet)
	if len(originalSet) != 4 {
		t.Fatalf("expected 4 nodes in original tree, got %d", len(originalSet))
	}

	cloned := CloneExecutionNode(limitNode)
	if cloned == nil {
		t.Fatal("CloneExecutionNode returned nil")
	}
	cloneSet := make(map[ExecutionNode]struct{})
	collectExecutionNodePointers(cloned, cloneSet)
	if len(cloneSet) != 4 {
		t.Fatalf("expected 4 nodes in cloned tree, got %d", len(cloneSet))
	}

	for ptr := range cloneSet {
		if _, ok := originalSet[ptr]; ok {
			t.Errorf("clone shares ExecutionNode pointer %p with original tree", ptr)
		}
	}
}

// TestCloneExecutionPlan_NoSharedRoot ensures ExecutionPlan.Clone() returns a plan
// whose RootNode is a clone, not the original.
func TestCloneExecutionPlan_NoSharedRoot(t *testing.T) {
	logger := zap.NewNop().Sugar()
	root := &FullScanNode{Cost: 1, EstimatedRows: 10, Logger: logger}
	ep := &ExecutionPlan{
		RootNode:      root,
		Cost:         1,
		EstimatedRows: 10,
		Logger:        logger,
	}
	cloned := ep.Clone()
	if cloned == nil {
		t.Fatal("Clone() returned nil")
	}
	if cloned.RootNode == root {
		t.Error("cloned plan shares RootNode with original")
	}
}
