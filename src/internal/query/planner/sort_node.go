/*
SORT EXECUTION NODE - PHASE 2

This file implements the SortNode execution node for the unified query system.
It provides ORDER BY functionality by wrapping the existing DocumentSorter component.

ARCHITECTURE:
The SortNode follows the Adapter pattern, delegating all sorting logic to the
well-tested DocumentSorter component while implementing the ExecutionNode interface.

DESIGN PRINCIPLES:
- Single Responsibility: Only responsible for coordinating sort execution in the query plan
- Open/Closed: Extends ExecutionNode without modifying existing code
- Dependency Inversion: Depends on ExecutionNode abstraction and DocumentSorter

EXECUTION MODEL:
1. Pull documents from child node
2. Convert document map to sortable slice
3. Delegate to DocumentSorter for actual sorting
4. Return sorted documents as map (preserving compatibility)

PERFORMANCE:
- Time Complexity: O(n log n) where n is number of documents
- Space Complexity: O(n) for temporary slice during sorting
- Uses Go's stable sort algorithm for consistent results

COST ESTIMATION:
Cost = ChildCost + (n * log(n) * 0.001)
Where n is estimated number of rows from child node

This node is part of Phase 2 of the unified query system implementation.
*/

package planner

import (
	"fmt"
	"syndrdb/src/internal/domain/models"
	"syndrdb/src/internal/query/queryparser"

	"go.uber.org/zap"
)

// SortNode implements ORDER BY clause execution
// PHASE 2: Execution Nodes - Sort Operation
type SortNode struct {
	// Child node providing input documents
	Child ExecutionNode

	// OrderBy clause specifying sort fields and directions
	OrderBy *queryparser.OrderByClause

	// Cost is the estimated execution cost
	Cost float64

	// EstimatedRows is the expected number of output rows
	EstimatedRows int

	// Logger for debugging and monitoring
	Logger *zap.SugaredLogger

	// sorter delegates to existing DocumentSorter implementation
	sorter *queryparser.DocumentSorter

	// sortedDocuments stores the sorted document slice to preserve order for downstream nodes
	sortedDocuments []*models.Document
}

// NewSortNode creates a new sort execution node
// PHASE 2: Factory function for SortNode creation
//
// Parameters:
//   - child: ExecutionNode providing input documents
//   - orderBy: ORDER BY clause specification
//   - logger: Logger for debugging
//
// Returns:
//   - *SortNode: Configured sort execution node
func NewSortNode(child ExecutionNode, orderBy *queryparser.OrderByClause, logger *zap.SugaredLogger) *SortNode {
	if orderBy == nil {
		logger.Warn("Creating SortNode with nil OrderBy clause")
	}

	// Create document sorter instance (reuses existing component)
	sorter := queryparser.NewDocumentSorter(orderBy, logger)

	node := &SortNode{
		Child:         child,
		OrderBy:       orderBy,
		Logger:        logger,
		EstimatedRows: child.GetEstimatedRows(),
		sorter:        sorter,
	}

	// Calculate cost: child cost + sorting cost
	// Sorting cost = n * log(n) * 0.001 (small constant for comparison operations)
	n := float64(child.GetEstimatedRows())
	sortCost := 0.0
	if n > 0 {
		sortCost = n * logBase2(n) * 0.001
	}
	node.Cost = child.GetCost() + sortCost

	logger.Debugf("Created SortNode: EstimatedRows=%d, Cost=%.4f (child=%.4f, sort=%.4f)",
		node.EstimatedRows, node.Cost, child.GetCost(), sortCost)

	return node
}

// Execute performs the sort operation
// PHASE 2: Main execution method for SortNode
//
// Execution flow:
// 1. Execute child node to get input documents
// 2. Delegate to DocumentSorter for sorting
// 3. Return sorted documents
//
// Returns:
//   - map[string]*models.Document: Sorted documents
//   - error: Any error during execution
func (n *SortNode) Execute() (map[string]*models.Document, error) {
	n.Logger.Infof("Executing SortNode with %d ORDER BY fields", len(n.OrderBy.Fields))

	// Execute child node to get input documents
	documents, err := n.Child.Execute()
	if err != nil {
		return nil, fmt.Errorf("SortNode: child execution failed: %w", err)
	}

	n.Logger.Debugf("SortNode received %d documents from child", len(documents))

	// Handle empty result set
	if len(documents) == 0 {
		n.Logger.Debug("SortNode: no documents to sort, returning empty result")
		return documents, nil
	}

	// Handle nil or empty ORDER BY clause (pass-through)
	if n.OrderBy == nil || len(n.OrderBy.Fields) == 0 {
		n.Logger.Debug("SortNode: no ORDER BY fields specified, returning unsorted documents")
		return documents, nil
	}

	// Delegate to DocumentSorter (reuses existing, well-tested component)
	sortedDocs, err := n.sorter.SortDocumentMap(documents)
	if err != nil {
		return nil, fmt.Errorf("SortNode: sorting failed: %w", err)
	}

	// Store sorted documents for downstream nodes (like LimitNode) to preserve order
	n.sortedDocuments = sortedDocs

	// Convert sorted slice back to map (preserves ExecutionNode interface compatibility)
	sortedMap := make(map[string]*models.Document, len(sortedDocs))
	for _, doc := range sortedDocs {
		sortedMap[doc.DocumentID] = doc
	}

	n.Logger.Infof("SortNode completed: sorted %d documents by %d fields",
		len(sortedMap), len(n.OrderBy.Fields))

	return sortedMap, nil
}

// GetCost returns the estimated execution cost
// PHASE 2: Cost accessor for query planning
func (n *SortNode) GetCost() float64 {
	return n.Cost
}

// GetEstimatedRows returns the estimated number of output rows
// PHASE 2: Cardinality accessor for query planning
func (n *SortNode) GetEstimatedRows() int {
	return n.EstimatedRows
}

// GetSortedDocuments returns the documents in sorted order
// This allows downstream nodes (like LimitNode) to preserve the sort order
func (n *SortNode) GetSortedDocuments() []*models.Document {
	return n.sortedDocuments
}

// logBase2 calculates log base 2 of n
// PHASE 2: Helper function for sorting cost estimation
func logBase2(n float64) float64 {
	if n <= 1 {
		return 0
	}
	// log2(n) = log(n) / log(2)
	return 3.321928094887362 * log10(n) // log(2) ≈ 0.30103, 1/0.30103 ≈ 3.32193
}

// log10 calculates log base 10 of n using natural log
// PHASE 2: Helper function for logarithm calculation
func log10(n float64) float64 {
	if n <= 0 {
		return 0
	}
	// Approximate log10 using natural log
	// log10(n) = ln(n) / ln(10)
	// We'll use a simple approximation for small values
	result := 0.0
	temp := n
	for temp >= 10 {
		temp /= 10
		result++
	}
	// Add fractional part (rough approximation)
	if temp > 1 {
		result += (temp - 1) / 9
	}
	return result
}
