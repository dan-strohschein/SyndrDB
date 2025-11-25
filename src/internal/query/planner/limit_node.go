/*
LIMIT EXECUTION NODE - PHASE 2

This file implements the LimitNode execution node for the unified query system.
It provides TOP/LIMIT/OFFSET functionality for result set pagination and limitation.

ARCHITECTURE:
The LimitNode implements simple result set limiting logic following PostgreSQL's
LIMIT/OFFSET semantics. It processes results from a child node and applies
row-count restrictions.

DESIGN PRINCIPLES:
- Single Responsibility: Only responsible for limiting result set size
- Open/Closed: Extends ExecutionNode without modifying existing code
- Dependency Inversion: Depends on ExecutionNode abstraction

EXECUTION MODEL:
1. Pull documents from child node
2. Apply OFFSET (skip first N documents)
3. Apply LIMIT (take next M documents)
4. Return limited result set

PERFORMANCE:
- Time Complexity: O(n) where n is child result set size
- Space Complexity: O(limit) for result set
- Short-circuit: Can potentially signal child to stop early (future optimization)

COST ESTIMATION:
Cost = ChildCost + (n * 0.0001)
EstimatedRows = min(ChildRows - Offset, Limit)

LIMIT vs TOP:
- TOP N: Equivalent to LIMIT N with OFFSET 0
- LIMIT M OFFSET N: Skip first N, return next M
- Both are supported through the same node

This node is part of Phase 2 of the unified query system implementation.
*/

package planner

import (
	"context"
	"fmt"
	"sort"
	"syndrdb/src/internal/domain/models"

	"go.uber.org/zap"
)

// LimitNode implements LIMIT/OFFSET/TOP clause execution
// PHASE 2: Execution Nodes - Limit/Offset Operation
type LimitNode struct {
	// Child node providing input documents
	Child ExecutionNode

	// Limit is the maximum number of rows to return (0 = no limit)
	Limit int

	// Offset is the number of rows to skip (0 = no offset)
	Offset int

	// Cost is the estimated execution cost
	Cost float64

	// EstimatedRows is the expected number of output rows
	EstimatedRows int

	// Logger for debugging and monitoring
	Logger *zap.SugaredLogger

	// sortedDocuments stores documents in order when child is SortNode
	// This preserves ORDER BY sort order for proper result formatting
	sortedDocuments []*models.Document
}

// NewLimitNode creates a new limit execution node
// PHASE 2: Factory function for LimitNode creation
//
// Parameters:
//   - child: ExecutionNode providing input documents
//   - limit: Maximum number of rows to return (0 or negative = no limit)
//   - offset: Number of rows to skip (0 or negative = no offset)
//   - logger: Logger for debugging
//
// Returns:
//   - *LimitNode: Configured limit execution node
func NewLimitNode(child ExecutionNode, limit, offset int, logger *zap.SugaredLogger) *LimitNode {
	// Normalize negative values to zero
	if limit < 0 {
		limit = 0
	}
	if offset < 0 {
		offset = 0
	}

	// Calculate estimated output rows
	childRows := child.GetEstimatedRows()
	estimatedRows := childRows

	// Apply offset
	if offset > 0 {
		estimatedRows -= offset
		if estimatedRows < 0 {
			estimatedRows = 0
		}
	}

	// Apply limit
	if limit > 0 && estimatedRows > limit {
		estimatedRows = limit
	}

	// Ensure non-negative
	if estimatedRows < 0 {
		estimatedRows = 0
	}

	node := &LimitNode{
		Child:         child,
		Limit:         limit,
		Offset:        offset,
		EstimatedRows: estimatedRows,
		Logger:        logger,
	}

	// Calculate cost: child cost + small per-row processing cost
	// LimitNode is very cheap - just filtering in-memory results
	processingCost := float64(childRows) * 0.0001
	node.Cost = child.GetCost() + processingCost

	logger.Debugf("Created LimitNode: Limit=%d, Offset=%d, EstimatedRows=%d, Cost=%.4f",
		limit, offset, estimatedRows, node.Cost)

	return node
}

// Execute performs the limit/offset operation
// PHASE 2: Main execution method for LimitNode
//
// Execution flow:
// 1. Execute child node to get input documents
// 2. Convert to slice for deterministic ordering
// 3. Apply offset (skip first N documents)
// 4. Apply limit (take next M documents)
// 5. Convert back to map and return
//
// Returns:
//   - map[string]*models.Document: Limited documents
//   - error: Any error during execution
func (n *LimitNode) Execute(ctx context.Context) (map[string]*models.Document, error) {
	n.Logger.Infof("Executing LimitNode: Limit=%d, Offset=%d", n.Limit, n.Offset)

	// Execute child node to get input documents
	documents, err := n.Child.Execute(ctx)
	if err != nil {
		return nil, fmt.Errorf("LimitNode: child execution failed: %w", err)
	}

	n.Logger.Debugf("LimitNode received %d documents from child", len(documents))

	// Handle empty result set
	if len(documents) == 0 {
		n.Logger.Debug("LimitNode: no documents to limit, returning empty result")
		return documents, nil
	}

	// If no limit and no offset, return all documents (pass-through)
	if n.Limit <= 0 && n.Offset <= 0 {
		n.Logger.Debug("LimitNode: no limit or offset specified, returning all documents")
		return documents, nil
	}

	// Convert map to slice for deterministic processing
	// Check if child is a SortNode to use Top-N optimization
	var docSlice []*models.Document
	if sortNode, isSortNode := n.Child.(*SortNode); isSortNode {
		// Child is SortNode - use Top-N optimization if beneficial
		n.Logger.Debugf("LimitNode: child is SortNode, attempting Top-N optimization")

		// Calculate effective limit (LIMIT + OFFSET)
		effectiveLimit := n.GetEffectiveLimit()

		// Call ExecuteWithLimit to enable Top-N heapsort
		var err error
		docSlice, err = sortNode.ExecuteWithLimit(ctx, effectiveLimit)
		if err != nil {
			return nil, fmt.Errorf("LimitNode: SortNode.ExecuteWithLimit failed: %w", err)
		}

		n.Logger.Debugf("LimitNode: received %d sorted documents from SortNode (Top-N optimized)", len(docSlice))

		// Store the sorted documents slice to preserve ORDER BY sort order
		// This is critical! The documents are correctly sorted here, we must preserve that order
		n.sortedDocuments = docSlice
	} else {
		// Child is not using Top-N optimization
		// Check if child is a SortNode that has sorted documents available
		if sortNode, isSortNode := n.Child.(*SortNode); isSortNode {
			sortedDocs := sortNode.GetSortedDocuments()
			if sortedDocs != nil {
				// SortNode executed a full sort - use those sorted documents!
				n.Logger.Debugf("LimitNode: using sorted documents from SortNode (%d documents)", len(sortedDocs))
				docSlice = sortedDocs
				n.sortedDocuments = sortedDocs
			} else {
				// SortNode hasn't executed yet? This shouldn't happen, but fall back
				n.Logger.Warn("LimitNode: SortNode child has no sorted documents, falling back to DocumentID order")
				docSlice = buildDocSliceByDocumentID(documents)
			}
		} else {
			// No SortNode - sort by DocumentID for deterministic ordering
			n.Logger.Debugf("LimitNode: using DocumentID order (%d documents)", len(documents))
			docSlice = buildDocSliceByDocumentID(documents)
		}
	}

	n.Logger.Debugf("LimitNode: processing %d documents", len(docSlice))

	// Apply OFFSET (skip first N documents)
	startIndex := n.Offset
	if startIndex < 0 {
		startIndex = 0
	}
	if startIndex >= len(docSlice) {
		// Offset is beyond result set, return empty
		n.Logger.Infof("LimitNode: offset %d >= document count %d, returning empty result",
			n.Offset, len(docSlice))
		return make(map[string]*models.Document), nil
	}

	// Apply LIMIT (take next M documents)
	endIndex := len(docSlice)
	if n.Limit > 0 {
		endIndex = startIndex + n.Limit
		if endIndex > len(docSlice) {
			endIndex = len(docSlice)
		}
	}

	// Extract the limited slice
	limitedSlice := docSlice[startIndex:endIndex]

	n.Logger.Debugf("LimitNode: extracting documents [%d:%d] from %d total",
		startIndex, endIndex, len(docSlice))

	// Update sortedDocuments to contain only the limited subset
	// This is what GetSortedDocuments() will return to preserve ordering
	n.sortedDocuments = limitedSlice

	// Convert back to map
	resultMap := make(map[string]*models.Document, len(limitedSlice))
	for _, doc := range limitedSlice {
		resultMap[doc.DocumentID] = doc
	}

	n.Logger.Infof("LimitNode completed: returned %d documents (offset=%d, limit=%d from %d total)",
		len(resultMap), n.Offset, n.Limit, len(documents))

	return resultMap, nil
}

// GetCost returns the estimated execution cost
// PHASE 2: Cost accessor for query planning
func (n *LimitNode) GetCost() float64 {
	return n.Cost
}

// GetEstimatedRows returns the estimated number of output rows
// PHASE 2: Cardinality accessor for query planning
func (n *LimitNode) GetEstimatedRows() int {
	return n.EstimatedRows
}

// HasLimit returns true if a limit is specified
// PHASE 2: Helper method for query analysis
func (n *LimitNode) HasLimit() bool {
	return n.Limit > 0
}

// HasOffset returns true if an offset is specified
// PHASE 2: Helper method for query analysis
func (n *LimitNode) HasOffset() bool {
	return n.Offset > 0
}

// GetEffectiveLimit returns the effective limit considering both limit and offset
// PHASE 2: Helper method for result set size estimation
// This is used by SortNode for Top-N heap sizing (must fetch OFFSET + LIMIT docs)
func (n *LimitNode) GetEffectiveLimit() int {
	if n.Limit <= 0 {
		return 0 // No limit
	}
	// Effective limit = OFFSET + LIMIT (must fetch both to skip offset docs)
	return n.Offset + n.Limit
}

// GetSortedDocuments returns the documents in their sorted order (if available)
// Returns nil if documents were not sorted or order is not preserved
// This is used by result formatters to preserve ORDER BY sort order
func (n *LimitNode) GetSortedDocuments() []*models.Document {
	return n.sortedDocuments
}

// buildDocSliceByDocumentID creates a document slice sorted by DocumentID
// This provides deterministic ordering when no explicit ORDER BY is present
func buildDocSliceByDocumentID(documents map[string]*models.Document) []*models.Document {
	// Extract document IDs
	docIDs := make([]string, 0, len(documents))
	for docID := range documents {
		docIDs = append(docIDs, docID)
	}

	// Sort document IDs for deterministic ordering
	sort.Strings(docIDs)

	// Build document slice in sorted ID order
	docSlice := make([]*models.Document, 0, len(documents))
	for _, docID := range docIDs {
		docSlice = append(docSlice, documents[docID])
	}

	return docSlice
}
