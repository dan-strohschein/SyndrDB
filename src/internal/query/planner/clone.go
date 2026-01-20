/*
PLAN CLONE - Concurrency-safe cache hit

Clones ExecutionPlan and its node tree so the plan cache can return a dedicated
copy to each caller. This avoids:
1. Data race: concurrent queries with the same plan key both writing to
   SortNode.sortedDocuments / LimitNode.sortedDocuments.
2. Result retention: the cached plan is never executed, so it does not hold
   result slices; only the clone is executed.

The cloned plan shares read-only parts (Logger, Bundle, OrderBy, Config, etc.)
and clears execution-state (sortedDocuments, joinedResults, cachedResult, etc.).
*/

package planner

// CloneExecutionNode returns a deep clone of the execution node tree.
// Execution state (sortedDocuments, joinedResults, cachedResult, leftDocs, etc.)
// is cleared in the clone. Reference-type config (Logger, Bundle, OrderBy, etc.)
// is shared.
func CloneExecutionNode(node ExecutionNode) ExecutionNode {
	if node == nil {
		return nil
	}
	switch n := node.(type) {
	case *LimitNode:
		n2 := *n
		n2.Child = CloneExecutionNode(n.Child)
		n2.sortedDocuments = nil
		return &n2
	case *SortNode:
		n2 := *n
		n2.Child = CloneExecutionNode(n.Child)
		n2.sortedDocuments = nil
		return &n2
	case *AggregationNode:
		n2 := *n
		n2.Child = CloneExecutionNode(n.Child)
		return &n2
	case *FilterNode:
		n2 := *n
		n2.Child = CloneExecutionNode(n.Child)
		return &n2
	case *UnionNode:
		n2 := *n
		n2.Children = make([]ExecutionNode, len(n.Children))
		for i, c := range n.Children {
			n2.Children[i] = CloneExecutionNode(c)
		}
		return &n2
	case *DistinctNode:
		n2 := *n
		n2.Child = CloneExecutionNode(n.Child)
		return &n2
	case *HierarchicalTransformNode:
		n2 := *n
		n2.Child = CloneExecutionNode(n.Child)
		return &n2
	case *NestedLoopJoinNode:
		n2 := *n
		n2.LeftChild = CloneExecutionNode(n.LeftChild)
		n2.RightChild = CloneExecutionNode(n.RightChild)
		n2.leftDocs = nil
		n2.rightDocs = nil
		n2.processed = false
		return &n2
	case *HashJoinNode:
		n2 := *n
		n2.LeftChild = CloneExecutionNode(n.LeftChild)
		n2.RightChild = CloneExecutionNode(n.RightChild)
		n2.hashTable = nil
		n2.probeDocuments = nil
		n2.processed = false
		return &n2
	case *MergeJoinNode:
		n2 := *n
		n2.LeftChild = CloneExecutionNode(n.LeftChild)
		n2.RightChild = CloneExecutionNode(n.RightChild)
		n2.leftDocs = nil
		n2.rightDocs = nil
		n2.processed = false
		return &n2
	case *JoinExecutionNode:
		n2 := *n
		n2.joinedResults = nil
		return &n2
	case *SubqueryNode:
		n2 := *n
		n2.cachedResult = nil
		return &n2
	case *FullScanNode:
		n2 := *n
		return &n2
	case *IndexScanNode:
		n2 := *n
		return &n2
	case *BTreeOrderedScanNode:
		n2 := *n
		return &n2
	case *ExpressionEvaluationNode:
		n2 := *n
		return &n2
	default:
		// Shallow copy for any unhandled node type.
		// Prefer adding an explicit case above for nodes with Child/Children or execution state.
		return node
	}
}

// Clone returns a deep clone of the execution plan for concurrency-safe use.
// The cached plan is never executed; callers execute the clone.
// Returns nil if ep is nil.
func (ep *ExecutionPlan) Clone() *ExecutionPlan {
	if ep == nil {
		return nil
	}
	root := CloneExecutionNode(ep.RootNode)
	if root == nil && ep.RootNode != nil {
		// CloneExecutionNode returned nil only when input is nil
		return nil
	}
	return &ExecutionPlan{
		RootNode:             root,
		Cost:                 ep.Cost,
		EstimatedRows:        ep.EstimatedRows,
		IndexesUsed:          append([]string(nil), ep.IndexesUsed...),
		Logger:               ep.Logger,
		estimatedMemoryBytes: ep.estimatedMemoryBytes,
	}
}
