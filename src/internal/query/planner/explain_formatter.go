package planner

import (
	"fmt"
	"syndrdb/src/internal/query/queryparser"
)

/*
EXPLAIN FORMATTER

This file implements JSON formatting for EXPLAIN and EXPLAIN ANALYZE output.
It converts ExecutionPlan structures into JSON-serializable maps suitable for
client consumption.

RESPONSIBILITIES:
- Format ExecutionPlan as JSON with cost estimates and index information
- Recursively format execution tree nodes with type-specific details
- Include cost calculation formulas for transparency
- Add actual execution metrics when ANALYZE mode is enabled

ARCHITECTURE:
- FormatExplainOutput: Top-level formatter for complete plan
- formatExecutionTree: Recursive formatter for individual nodes
- Type switches handle all ExecutionNode implementations

DESIGN PRINCIPLES:
- Single Responsibility: Only handles plan formatting (no execution or planning logic)
- Open/Closed: Easily extended for new node types via type switch
- DRY: Reuses node type information and cost data from existing structures

OUTPUT FORMAT:
{
  "QueryPlan": {
    "QueryType": "SimpleSelect",
    "PlanType": "IndexScan -> Filter -> Sort -> Limit",
    "Cost": 156.78,
    "EstimatedRows": 10,
    "IndexesUsed": ["authors_name_hash_idx"],
    "MemoryEstimate": 8192,
    "CostFormulas": {
      "HashIndexCost": "1.0 (base) + 0.1 (lookup)",
      "FilterCost": "10.0 (per-row evaluation)"
    },
    "ExecutionTree": {
      "NodeType": "LimitNode",
      "Cost": 156.78,
      "EstimatedRows": 10,
      "ActualExecutionTime": 2.34,  // Only in ANALYZE mode
      "ActualRowsReturned": 10,      // Only in ANALYZE mode
      "Child": { ... }
    }
  }
}
*/

// NodeMetrics stores actual execution metrics for a node during EXPLAIN ANALYZE
// This is imported from explain_operations.go but defined here for reference
type NodeMetrics struct {
	NodeID              string
	ActualExecutionTime float64 // milliseconds
	ActualRowsReturned  int
}

// FormatExplainOutput formats an ExecutionPlan as a JSON-serializable map
//
// This function creates the top-level EXPLAIN output structure containing:
// - Query metadata (type, cost, rows)
// - Index usage information
// - Cost calculation formulas
// - Recursively formatted execution tree
// - Actual metrics (when analyze=true)
//
// Parameters:
//   - plan: ExecutionPlan to format
//   - query: Original query for metadata extraction
//   - analyze: Whether ANALYZE mode is enabled (includes actual metrics)
//   - nodeMetricsMap: Map of node IDs to actual execution metrics (for ANALYZE)
//
// Returns:
//   - map[string]interface{}: JSON-serializable plan representation
func FormatExplainOutput(
	plan *ExecutionPlan,
	query *queryparser.UnifiedSelectQuery,
	analyze bool,
	nodeMetricsMap map[string]*NodeMetrics,
) map[string]interface{} {

	// Build execution tree representation
	executionTree := formatExecutionTree(plan.RootNode, analyze, nodeMetricsMap)

	// Generate plan type description (e.g., "IndexScan -> Filter -> Sort")
	planType := describePlanType(plan.RootNode)

	// Build cost formulas map
	costFormulas := buildCostFormulas(plan.RootNode)

	// Create query plan output
	queryPlan := map[string]interface{}{
		"QueryType":      query.QueryType.String(),
		"PlanType":       planType,
		"Cost":           plan.Cost,
		"EstimatedRows":  plan.EstimatedRows,
		"IndexesUsed":    plan.IndexesUsed,
		"MemoryEstimate": plan.estimatedMemoryBytes,
		"CostFormulas":   costFormulas,
		"ExecutionTree":  executionTree,
	}

	// Wrap in QueryPlan envelope
	return map[string]interface{}{
		"QueryPlan": queryPlan,
	}
}

// formatExecutionTree recursively formats an execution node and its children
//
// This function uses type switching to handle different node types and extract
// their specific properties. It formats each node as a JSON-serializable map
// containing node type, cost estimates, and any type-specific metadata.
//
// Parameters:
//   - node: ExecutionNode to format
//   - analyze: Whether to include actual execution metrics
//   - nodeMetricsMap: Map of node metrics (for ANALYZE mode)
//
// Returns:
//   - map[string]interface{}: JSON-serializable node representation
func formatExecutionTree(
	node ExecutionNode,
	analyze bool,
	nodeMetricsMap map[string]*NodeMetrics,
) map[string]interface{} {

	if node == nil {
		return nil
	}

	// Base fields common to all nodes
	result := map[string]interface{}{
		"Cost":          node.GetCost(),
		"EstimatedRows": node.GetEstimatedRows(),
		"MemoryUsage":   node.EstimateMemoryUsage(),
	}

	// Add actual metrics if ANALYZE mode is enabled
	// TODO: Currently we don't have a way to correlate nodes with metrics
	// since we don't store node IDs in the nodes themselves. For now, skip actual metrics.
	// In future, add NodeID field to ExecutionNode interface for proper correlation.
	if analyze {
		result["ActualExecutionTime"] = 0.0
		result["ActualRowsReturned"] = 0
	}

	// Type-specific formatting with child node recursion
	switch n := node.(type) {
	case *IndexScanNode:
		result["NodeType"] = "IndexScanNode"
		result["IndexName"] = n.IndexName
		result["ScanType"] = formatScanType(n.ScanType)
		result["SearchKey"] = n.SearchKey
		result["Operator"] = n.Operator
		if n.RangeStart != nil {
			result["RangeStart"] = n.RangeStart
		}
		if n.RangeEnd != nil {
			result["RangeEnd"] = n.RangeEnd
		}

	case *FullScanNode:
		result["NodeType"] = "FullScanNode"
		result["BundleName"] = n.Bundle.Name

	case *BTreeOrderedScanNode:
		result["NodeType"] = "BTreeOrderedScanNode"
		result["IndexName"] = n.IndexName
		result["OrderedByField"] = n.OrderedByFieldName
		result["BundleName"] = n.Bundle.Name

	case *FilterNode:
		result["NodeType"] = "FilterNode"
		result["FilterConditions"] = len(n.Conditions)
		result["Child"] = formatExecutionTree(n.Child, analyze, nodeMetricsMap)

	case *SortNode:
		result["NodeType"] = "SortNode"
		if n.OrderBy != nil {
			result["SortFields"] = formatOrderByClause(n.OrderBy)
		}
		result["Child"] = formatExecutionTree(n.Child, analyze, nodeMetricsMap)

	case *LimitNode:
		result["NodeType"] = "LimitNode"
		result["Limit"] = n.Limit
		result["Offset"] = n.Offset
		result["Child"] = formatExecutionTree(n.Child, analyze, nodeMetricsMap)

	case *AggregationNode:
		result["NodeType"] = "AggregationNode"
		if n.GroupBy != nil {
			result["GroupByFields"] = n.GroupBy.Fields
		}
		result["AggregateFields"] = formatAggregateFields(n.AggregateFields)
		result["Child"] = formatExecutionTree(n.Child, analyze, nodeMetricsMap)

	case *DistinctNode:
		result["NodeType"] = "DistinctNode"
		result["DistinctFields"] = n.DistinctFields
		result["Child"] = formatExecutionTree(n.Child, analyze, nodeMetricsMap)

	case *UnionNode:
		result["NodeType"] = "UnionNode"
		children := make([]interface{}, len(n.Children))
		for i, child := range n.Children {
			children[i] = formatExecutionTree(child, analyze, nodeMetricsMap)
		}
		result["Children"] = children

	case *JoinExecutionNode:
		result["NodeType"] = "JoinExecutionNode"
		if n.Query != nil {
			result["FromBundle"] = n.Query.FromBundle
			if len(n.Query.JoinClauses) > 0 {
				result["JoinType"] = n.Query.JoinClauses[0].JoinType.String()
				result["RightBundle"] = n.Query.JoinClauses[0].RightBundle
			}
		}
		// Note: JoinExecutionNode doesn't have child nodes in the traditional sense
		// It delegates to the JOIN executor which handles its own execution

	case *NestedLoopJoinNode:
		result["NodeType"] = "NestedLoopJoinNode"
		result["JoinType"] = n.JoinType.String()
		result["LeftChild"] = formatExecutionTree(n.LeftChild, analyze, nodeMetricsMap)
		result["RightChild"] = formatExecutionTree(n.RightChild, analyze, nodeMetricsMap)

	case *HashJoinNode:
		result["NodeType"] = "HashJoinNode"
		result["JoinType"] = n.JoinType.String()
		result["BuildSide"] = "Left" // Hash joins typically build on left side
		result["LeftChild"] = formatExecutionTree(n.LeftChild, analyze, nodeMetricsMap)
		result["RightChild"] = formatExecutionTree(n.RightChild, analyze, nodeMetricsMap)

	case *MergeJoinNode:
		result["NodeType"] = "MergeJoinNode"
		result["JoinType"] = n.JoinType.String()
		result["LeftChild"] = formatExecutionTree(n.LeftChild, analyze, nodeMetricsMap)
		result["RightChild"] = formatExecutionTree(n.RightChild, analyze, nodeMetricsMap)

	case *SubqueryNode:
		result["NodeType"] = "SubqueryNode"
		result["SubqueryType"] = n.SubqueryType.String()
		result["BundleName"] = n.InnerQuery.BundleName

		// Include materialization strategy if subquery has been executed
		if n.GetCachedResult() != nil {
			cached := n.GetCachedResult()
			result["Strategy"] = cached.Strategy.String()
			result["ActualRowCount"] = cached.RowCount
			result["ContainsNull"] = cached.ContainsNull
			result["MemoryBytes"] = cached.MemoryBytes
		} else {
			// Show estimated strategy
			strategy := n.Executor.SelectStrategy(n.EstimatedRows)
			result["EstimatedStrategy"] = strategy.String()
		}

	default:
		result["NodeType"] = fmt.Sprintf("%T", node)
	}

	return result
}

// describePlanType generates a human-readable description of the plan structure
//
// This function recursively traverses the execution tree and builds a string
// representation of the plan flow (e.g., "IndexScan -> Filter -> Sort -> Limit").
//
// Parameters:
//   - node: Root execution node
//
// Returns:
//   - string: Plan type description
func describePlanType(node ExecutionNode) string {
	if node == nil {
		return "Empty"
	}

	switch n := node.(type) {
	case *IndexScanNode:
		return formatScanType(n.ScanType)
	case *FullScanNode:
		return "FullScan"
	case *BTreeOrderedScanNode:
		return "BTreeOrderedScan(" + n.OrderedByFieldName + ")"
	case *FilterNode:
		childType := describePlanType(n.Child)
		return childType + " -> Filter"
	case *SortNode:
		childType := describePlanType(n.Child)
		return childType + " -> Sort"
	case *LimitNode:
		childType := describePlanType(n.Child)
		return childType + " -> Limit"
	case *AggregationNode:
		childType := describePlanType(n.Child)
		return childType + " -> Aggregation"
	case *DistinctNode:
		childType := describePlanType(n.Child)
		return childType + " -> Distinct"
	case *UnionNode:
		return "Union"
	case *JoinExecutionNode:
		if n.Query != nil && len(n.Query.JoinClauses) > 0 {
			return n.Query.JoinClauses[0].JoinType.String() + " Join"
		}
		return "Join"
	case *NestedLoopJoinNode:
		return "NestedLoopJoin"
	case *HashJoinNode:
		return "HashJoin"
	case *MergeJoinNode:
		return "MergeJoin"
	default:
		return fmt.Sprintf("%T", node)
	}
}

// buildCostFormulas generates cost calculation formulas for transparency
//
// This function extracts cost information from the execution tree and formats
// it as human-readable formulas showing how costs were calculated.
//
// Parameters:
//   - node: Root execution node
//
// Returns:
//   - map[string]string: Map of cost component to formula description
func buildCostFormulas(node ExecutionNode) map[string]string {
	formulas := make(map[string]string)

	if node == nil {
		return formulas
	}

	// Recursively collect formulas from the tree
	collectCostFormulas(node, formulas)

	return formulas
}

// collectCostFormulas recursively collects cost formulas from execution tree
//
// This helper function traverses the execution tree and adds cost formula
// descriptions for each node type encountered.
//
// Parameters:
//   - node: Current execution node
//   - formulas: Map to accumulate formulas (modified in place)
func collectCostFormulas(node ExecutionNode, formulas map[string]string) {
	if node == nil {
		return
	}

	switch n := node.(type) {
	case *IndexScanNode:
		if n.ScanType == HashIndexScan {
			formulas["HashIndexCost"] = "1.0 (base) + 0.1 (lookup)"
		} else if n.ScanType == BTreeIndexScan {
			formulas["BTreeIndexCost"] = "log2(N) + 0.5 (traversal)"
		} else if n.ScanType == BTreeRangeScan {
			formulas["BTreeRangeCost"] = "log2(N) + M (range size)"
		}

	case *FullScanNode:
		formulas["FullScanCost"] = "N * 1.0 (linear scan)"

	case *BTreeOrderedScanNode:
		formulas["BTreeOrderedScanCost"] = "0.5*N (full index range, ordered)"

	case *FilterNode:
		formulas["FilterCost"] = "N * 0.1 (per-row evaluation)"
		collectCostFormulas(n.Child, formulas)

	case *SortNode:
		formulas["SortCost"] = "N * log2(N) * 0.1 (quicksort)"
		collectCostFormulas(n.Child, formulas)

	case *LimitNode:
		formulas["LimitCost"] = "min(N, LIMIT) * 1.0"
		collectCostFormulas(n.Child, formulas)

	case *AggregationNode:
		formulas["AggregationCost"] = "N * 0.2 (hash aggregation)"
		collectCostFormulas(n.Child, formulas)

	case *DistinctNode:
		formulas["DistinctCost"] = "N * 0.15 (hash deduplication)"
		collectCostFormulas(n.Child, formulas)

	case *UnionNode:
		formulas["UnionCost"] = "SUM(child_costs) + merge_cost"
		for _, child := range n.Children {
			collectCostFormulas(child, formulas)
		}

	case *JoinExecutionNode:
		formulas["JoinCost"] = "Delegated to JOIN executor"
		// JoinExecutionNode doesn't have traditional child nodes

	case *NestedLoopJoinNode:
		formulas["NestedLoopJoinCost"] = "M * N (cartesian product)"
		collectCostFormulas(n.LeftChild, formulas)
		collectCostFormulas(n.RightChild, formulas)

	case *HashJoinNode:
		formulas["HashJoinCost"] = "M + N (hash build + probe)"
		collectCostFormulas(n.LeftChild, formulas)
		collectCostFormulas(n.RightChild, formulas)

	case *MergeJoinNode:
		formulas["MergeJoinCost"] = "M*log(M) + N*log(N) + M + N (sort + merge)"
		collectCostFormulas(n.LeftChild, formulas)
		collectCostFormulas(n.RightChild, formulas)
	}
}

// Helper formatters for node-specific fields

func formatScanType(scanType ScanType) string {
	switch scanType {
	case HashIndexScan:
		return "HashIndexScan"
	case BTreeIndexScan:
		return "BTreeIndexScan"
	case BTreeRangeScan:
		return "BTreeRangeScan"
	case FullBundleScan:
		return "FullBundleScan"
	default:
		return fmt.Sprintf("UnknownScan(%d)", scanType)
	}
}

func formatOrderByClause(orderBy *queryparser.OrderByClause) []map[string]interface{} {
	if orderBy == nil || len(orderBy.Fields) == 0 {
		return nil
	}

	result := make([]map[string]interface{}, len(orderBy.Fields))
	for i, field := range orderBy.Fields {
		direction := "ASC"
		if field.Direction == queryparser.SortDesc {
			direction = "DESC"
		}
		result[i] = map[string]interface{}{
			"Field":     field.FieldName,
			"Direction": direction,
		}
	}
	return result
}

func formatAggregateFields(fields []queryparser.AggregateFunction) []map[string]interface{} {
	result := make([]map[string]interface{}, len(fields))
	for i, field := range fields {
		result[i] = map[string]interface{}{
			"Function": field.Function,
			"Field":    field.Field,
			"Alias":    field.Alias,
		}
	}
	return result
}
