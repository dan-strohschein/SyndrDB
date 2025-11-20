/*
⚠️ DEPRECATED: JOIN EXECUTION NODES - NOT USED IN PRODUCTION ⚠️

This file contains legacy JOIN execution node implementations that are NOT actively used
in the current SyndrDB execution engine. The active JOIN implementation is located in:

	src/internal/query/join_executor/

Actual execution flow:
	1. Query parsing creates UnifiedSelectQuery
	2. QueryRouter detects JOIN and routes to JoinQueryPlanner
	3. JoinQueryPlanner creates JoinExecutionNode
	4. JoinExecutionNode.Execute() calls DefaultJoinExecutor.Execute()
	5. DefaultJoinExecutor selects strategy (HashJoin or NestedLoop)
	6. Strategy executes and returns JoinResult

This file is preserved for historical reference and potential future use. The implementations
below (NestedLoopJoinNode, HashJoinNode, MergeJoinNode) are complete but unused.

For active JOIN implementation, see:
	- src/internal/query/join_executor/join_executor.go (coordinator)
	- src/internal/query/join_executor/hash_join.go (primary strategy)
	- src/internal/query/join_executor/nested_loop_join.go (fallback strategy)

---

ORIGINAL DOCUMENTATION (for reference):

JOIN EXECUTION NODES SYSTEM

This file implements the execution nodes for JOIN operations in SyndrDB's query planner.
It provides three different join algorithms following PostgreSQL's approach:
1. Nested Loop Join - Good for small datasets or when one side has indexes
2. Hash Join - Efficient for equality joins with large datasets
3. Merge Join - Optimal when both inputs are sorted

JOIN EXECUTION STRATEGY:
Each join node implements the ExecutionNode interface and produces a stream
of joined documents by pulling rows from left and right child nodes.
The execution follows PostgreSQL's pull-based iterator model where nodes
request rows from their children on demand.

MEMORY MANAGEMENT:
- Nested Loop Join: Minimal memory usage, streams both inputs
- Hash Join: Builds hash table from smaller input, probes with larger
- Merge Join: Requires sorted inputs, minimal memory for merge operation

MVCC COMPATIBILITY:
All join nodes are designed to work with SyndrDB's document visibility
and transaction isolation requirements.

This implementation follows the Single Responsibility Principle with each
join algorithm in its own specialized execution node.
*/

package planner

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"syndrdb/src/internal/domain/document"
	"syndrdb/src/internal/domain/models"
	"syndrdb/src/internal/query/queryparser"

	"go.uber.org/zap"
)

// JoinNode represents the base interface for all join operations
type JoinNode interface {
	ExecutionNode
	GetJoinType() queryparser.JoinType
	GetLeftChild() ExecutionNode
	GetRightChild() ExecutionNode
}

// NestedLoopJoinNode implements nested loop join algorithm
// This is the simplest join algorithm where for each row in the outer table,
// we scan the entire inner table looking for matches
type NestedLoopJoinNode struct {
	LeftChild      ExecutionNode               // Outer loop (typically smaller dataset)
	RightChild     ExecutionNode               // Inner loop (potentially indexed)
	JoinConditions []queryparser.JoinCondition // Join predicates
	JoinType       queryparser.JoinType        // Type of join
	Cost           float64                     // Estimated cost
	EstimatedRows  int                         // Estimated result rows
	Logger         *zap.SugaredLogger          // Logger for debugging
	leftDocs       map[string]*models.Document // Cached left documents
	rightDocs      map[string]*models.Document // Cached right documents
	processed      bool                        // Whether execution has started
}

// HashJoinNode implements hash join algorithm
// Builds a hash table from the smaller input and probes it with the larger input
type HashJoinNode struct {
	LeftChild      ExecutionNode                 // Build side (typically smaller)
	RightChild     ExecutionNode                 // Probe side
	JoinConditions []queryparser.JoinCondition   // Join predicates
	JoinType       queryparser.JoinType          // Type of join
	Cost           float64                       // Estimated cost
	EstimatedRows  int                           // Estimated result rows
	Logger         *zap.SugaredLogger            // Logger for debugging
	hashTable      map[string][]*models.Document // Hash table for join
	probeDocuments map[string]*models.Document   // Documents from probe side
	processed      bool                          // Whether execution has started
}

// MergeJoinNode implements merge join algorithm
// Requires both inputs to be sorted on the join key
type MergeJoinNode struct {
	LeftChild      ExecutionNode               // Left sorted input
	RightChild     ExecutionNode               // Right sorted input
	JoinConditions []queryparser.JoinCondition // Join predicates
	JoinType       queryparser.JoinType        // Type of join
	Cost           float64                     // Estimated cost
	EstimatedRows  int                         // Estimated result rows
	Logger         *zap.SugaredLogger          // Logger for debugging
	leftDocs       []*models.Document          // Sorted left documents
	rightDocs      []*models.Document          // Sorted right documents
	processed      bool                        // Whether execution has started
}

// NewNestedLoopJoinNode creates a new nested loop join node
func NewNestedLoopJoinNode(leftChild, rightChild ExecutionNode,
	joinConditions []queryparser.JoinCondition, joinType queryparser.JoinType,
	logger *zap.SugaredLogger) *NestedLoopJoinNode {

	node := &NestedLoopJoinNode{
		LeftChild:      leftChild,
		RightChild:     rightChild,
		JoinConditions: joinConditions,
		JoinType:       joinType,
		Logger:         logger,
		processed:      false,
	}

	// Estimate cost: O(M * N) where M and N are input sizes
	leftRows := float64(leftChild.GetEstimatedRows())
	rightRows := float64(rightChild.GetEstimatedRows())
	node.Cost = leftChild.GetCost() + rightChild.GetCost() + (leftRows * rightRows * 0.01)

	// Estimate output rows based on join type
	node.EstimatedRows = estimateJoinRows(leftChild.GetEstimatedRows(),
		rightChild.GetEstimatedRows(), joinType)

	return node
}

// NewHashJoinNode creates a new hash join node
func NewHashJoinNode(leftChild, rightChild ExecutionNode,
	joinConditions []queryparser.JoinCondition, joinType queryparser.JoinType,
	logger *zap.SugaredLogger) *HashJoinNode {

	node := &HashJoinNode{
		LeftChild:      leftChild,
		RightChild:     rightChild,
		JoinConditions: joinConditions,
		JoinType:       joinType,
		Logger:         logger,
		hashTable:      make(map[string][]*models.Document),
		processed:      false,
	}

	// Estimate cost: O(M + N) for hash join where M and N are input sizes
	leftRows := float64(leftChild.GetEstimatedRows())
	rightRows := float64(rightChild.GetEstimatedRows())
	node.Cost = leftChild.GetCost() + rightChild.GetCost() + leftRows + rightRows

	// Estimate output rows based on join type
	node.EstimatedRows = estimateJoinRows(leftChild.GetEstimatedRows(),
		rightChild.GetEstimatedRows(), joinType)

	return node
}

// NewMergeJoinNode creates a new merge join node
func NewMergeJoinNode(leftChild, rightChild ExecutionNode,
	joinConditions []queryparser.JoinCondition, joinType queryparser.JoinType,
	logger *zap.SugaredLogger) *MergeJoinNode {

	node := &MergeJoinNode{
		LeftChild:      leftChild,
		RightChild:     rightChild,
		JoinConditions: joinConditions,
		JoinType:       joinType,
		Logger:         logger,
		processed:      false,
	}

	// Estimate cost: O(M log M + N log N + M + N) for sorting and merging
	leftRows := float64(leftChild.GetEstimatedRows())
	rightRows := float64(rightChild.GetEstimatedRows())
	sortCost := leftRows*math.Log2(leftRows) + rightRows*math.Log2(rightRows)
	node.Cost = leftChild.GetCost() + rightChild.GetCost() + sortCost + leftRows + rightRows

	// Estimate output rows based on join type
	node.EstimatedRows = estimateJoinRows(leftChild.GetEstimatedRows(),
		rightChild.GetEstimatedRows(), joinType)

	return node
}

// Execute implements ExecutionNode interface for NestedLoopJoinNode
func (n *NestedLoopJoinNode) Execute() (map[string]*models.Document, error) {
	if !n.processed {
		// Execute child nodes to get input documents
		leftDocs, err := n.LeftChild.Execute()
		if err != nil {
			return nil, fmt.Errorf("failed to execute left child in nested loop join: %w", err)
		}
		n.leftDocs = leftDocs

		rightDocs, err := n.RightChild.Execute()
		if err != nil {
			return nil, fmt.Errorf("failed to execute right child in nested loop join: %w", err)
		}
		n.rightDocs = rightDocs

		n.processed = true
	}

	n.Logger.Debugf("Executing nested loop join: %d left docs, %d right docs",
		len(n.leftDocs), len(n.rightDocs))

	// Perform nested loop join
	result := make(map[string]*models.Document)

	for leftDocID, leftDoc := range n.leftDocs {
		foundMatch := false

		for rightDocID, rightDoc := range n.rightDocs {
			if n.evaluateJoinConditions(leftDoc, rightDoc) {
				foundMatch = true

				// Create joined document
				joinedDoc := n.createJoinedDocument(leftDoc, rightDoc, leftDocID, rightDocID)
				joinedDocID := fmt.Sprintf("%s_%s", leftDocID, rightDocID)
				result[joinedDocID] = joinedDoc
			}
		}

		// Handle non-matching rows based on join type
		if !foundMatch && (n.JoinType == queryparser.LeftJoin || n.JoinType == queryparser.FullOuterJoin) {
			// Include left row with nulls for right side
			joinedDoc := n.createJoinedDocument(leftDoc, nil, leftDocID, "")
			result[leftDocID] = joinedDoc
		}
	}

	// Handle right outer join case
	if n.JoinType == queryparser.RightJoin || n.JoinType == queryparser.FullOuterJoin {
		for rightDocID, rightDoc := range n.rightDocs {
			foundMatch := false

			for _, leftDoc := range n.leftDocs {
				if n.evaluateJoinConditions(leftDoc, rightDoc) {
					foundMatch = true
					break
				}
			}

			if !foundMatch {
				// Include right row with nulls for left side
				joinedDoc := n.createJoinedDocument(nil, rightDoc, "", rightDocID)
				result[rightDocID] = joinedDoc
			}
		}
	}

	n.Logger.Debugf("Nested loop join produced %d result documents", len(result))
	return result, nil
}

// Execute implements ExecutionNode interface for HashJoinNode
func (h *HashJoinNode) Execute() (map[string]*models.Document, error) {
	if !h.processed {
		// Build phase: create hash table from left child (build side)
		leftDocs, err := h.LeftChild.Execute()
		if err != nil {
			return nil, fmt.Errorf("failed to execute left child in hash join: %w", err)
		}

		h.Logger.Debugf("Building hash table from %d documents", len(leftDocs))

		// Build hash table
		for _, doc := range leftDocs {
			hashKey := h.getHashKey(doc, true) // true for left side
			if hashKey != "" {
				h.hashTable[hashKey] = append(h.hashTable[hashKey], doc)
			}
		}

		// Probe phase: get right child documents
		rightDocs, err := h.RightChild.Execute()
		if err != nil {
			return nil, fmt.Errorf("failed to execute right child in hash join: %w", err)
		}
		h.probeDocuments = rightDocs

		h.processed = true
	}

	h.Logger.Debugf("Probing hash table with %d documents", len(h.probeDocuments))

	result := make(map[string]*models.Document)

	// Probe phase
	for rightDocID, rightDoc := range h.probeDocuments {
		hashKey := h.getHashKey(rightDoc, false) // false for right side
		foundMatch := false

		if hashKey != "" {
			if matchingDocs, exists := h.hashTable[hashKey]; exists {
				for _, leftDoc := range matchingDocs {
					if h.evaluateJoinConditions(leftDoc, rightDoc) {
						foundMatch = true

						// Create joined document
						joinedDoc := h.createJoinedDocument(leftDoc, rightDoc, "", rightDocID)
						joinedDocID := fmt.Sprintf("%s_%s", leftDoc.DocumentID, rightDocID)
						result[joinedDocID] = joinedDoc
					}
				}
			}
		}

		// Handle non-matching rows for right/full outer joins
		if !foundMatch && (h.JoinType == queryparser.RightJoin || h.JoinType == queryparser.FullOuterJoin) {
			joinedDoc := h.createJoinedDocument(nil, rightDoc, "", rightDocID)
			result[rightDocID] = joinedDoc
		}
	}

	h.Logger.Debugf("Hash join produced %d result documents", len(result))
	return result, nil
}

// Execute implements ExecutionNode interface for MergeJoinNode
func (m *MergeJoinNode) Execute() (map[string]*models.Document, error) {
	if !m.processed {
		// Get documents from both children
		leftDocs, err := m.LeftChild.Execute()
		if err != nil {
			return nil, fmt.Errorf("failed to execute left child in merge join: %w", err)
		}

		rightDocs, err := m.RightChild.Execute()
		if err != nil {
			return nil, fmt.Errorf("failed to execute right child in merge join: %w", err)
		}

		// Convert to slices and sort
		m.leftDocs = make([]*models.Document, 0, len(leftDocs))
		for _, doc := range leftDocs {
			m.leftDocs = append(m.leftDocs, doc)
		}

		m.rightDocs = make([]*models.Document, 0, len(rightDocs))
		for _, doc := range rightDocs {
			m.rightDocs = append(m.rightDocs, doc)
		}

		// Sort both sides by join key
		m.sortDocuments(m.leftDocs)
		m.sortDocuments(m.rightDocs)

		m.processed = true
	}

	m.Logger.Debugf("Executing merge join: %d left docs, %d right docs",
		len(m.leftDocs), len(m.rightDocs))

	result := make(map[string]*models.Document)

	// Merge join algorithm
	leftIdx := 0
	rightIdx := 0

	for leftIdx < len(m.leftDocs) && rightIdx < len(m.rightDocs) {
		leftDoc := m.leftDocs[leftIdx]
		rightDoc := m.rightDocs[rightIdx]

		comparison := m.compareJoinKeys(leftDoc, rightDoc)

		if comparison == 0 {
			// Keys match - join these documents
			if m.evaluateJoinConditions(leftDoc, rightDoc) {
				joinedDoc := m.createJoinedDocument(leftDoc, rightDoc, leftDoc.DocumentID, rightDoc.DocumentID)
				joinedDocID := fmt.Sprintf("%s_%s", leftDoc.DocumentID, rightDoc.DocumentID)
				result[joinedDocID] = joinedDoc
			}

			// Advance both pointers
			leftIdx++
			rightIdx++
		} else if comparison < 0 {
			// Left key is smaller - advance left pointer
			leftIdx++
		} else {
			// Right key is smaller - advance right pointer
			rightIdx++
		}
	}

	m.Logger.Debugf("Merge join produced %d result documents", len(result))
	return result, nil
}

// GetCost implements ExecutionNode interface
func (n *NestedLoopJoinNode) GetCost() float64 { return n.Cost }
func (h *HashJoinNode) GetCost() float64       { return h.Cost }
func (m *MergeJoinNode) GetCost() float64      { return m.Cost }

// GetEstimatedRows implements ExecutionNode interface
func (n *NestedLoopJoinNode) GetEstimatedRows() int { return n.EstimatedRows }
func (h *HashJoinNode) GetEstimatedRows() int       { return h.EstimatedRows }
func (m *MergeJoinNode) GetEstimatedRows() int      { return m.EstimatedRows }

// GetJoinType implements JoinNode interface
func (n *NestedLoopJoinNode) GetJoinType() queryparser.JoinType { return n.JoinType }
func (h *HashJoinNode) GetJoinType() queryparser.JoinType       { return h.JoinType }
func (m *MergeJoinNode) GetJoinType() queryparser.JoinType      { return m.JoinType }

// GetLeftChild implements JoinNode interface
func (n *NestedLoopJoinNode) GetLeftChild() ExecutionNode { return n.LeftChild }
func (h *HashJoinNode) GetLeftChild() ExecutionNode       { return h.LeftChild }
func (m *MergeJoinNode) GetLeftChild() ExecutionNode      { return m.LeftChild }

// GetRightChild implements JoinNode interface
func (n *NestedLoopJoinNode) GetRightChild() ExecutionNode { return n.RightChild }
func (h *HashJoinNode) GetRightChild() ExecutionNode       { return h.RightChild }
func (m *MergeJoinNode) GetRightChild() ExecutionNode      { return m.RightChild }

// Helper methods

// evaluateJoinConditions checks if two documents satisfy the join conditions
func (n *NestedLoopJoinNode) evaluateJoinConditions(leftDoc, rightDoc *models.Document) bool {
	return evaluateJoinConditions(leftDoc, rightDoc, n.JoinConditions, n.Logger)
}

func (h *HashJoinNode) evaluateJoinConditions(leftDoc, rightDoc *models.Document) bool {
	return evaluateJoinConditions(leftDoc, rightDoc, h.JoinConditions, h.Logger)
}

func (m *MergeJoinNode) evaluateJoinConditions(leftDoc, rightDoc *models.Document) bool {
	return evaluateJoinConditions(leftDoc, rightDoc, m.JoinConditions, m.Logger)
}

// createJoinedDocument creates a new document by combining fields from both sides
func (n *NestedLoopJoinNode) createJoinedDocument(leftDoc, rightDoc *models.Document, leftDocID, rightDocID string) *models.Document {
	return createJoinedDocument(leftDoc, rightDoc, leftDocID, rightDocID, n.Logger)
}

func (h *HashJoinNode) createJoinedDocument(leftDoc, rightDoc *models.Document, leftDocID, rightDocID string) *models.Document {
	return createJoinedDocument(leftDoc, rightDoc, leftDocID, rightDocID, h.Logger)
}

func (m *MergeJoinNode) createJoinedDocument(leftDoc, rightDoc *models.Document, leftDocID, rightDocID string) *models.Document {
	return createJoinedDocument(leftDoc, rightDoc, leftDocID, rightDocID, m.Logger)
}

// getHashKey generates a hash key for the document based on join conditions
func (h *HashJoinNode) getHashKey(doc *models.Document, isLeftSide bool) string {
	var keyParts []string

	for _, condition := range h.JoinConditions {
		var fieldName string
		if isLeftSide {
			fieldName = condition.LeftField
		} else {
			fieldName = condition.RightField
		}

		if field, exists := doc.Fields[fieldName]; exists {
			keyParts = append(keyParts, fmt.Sprintf("%v", field.Value))
		} else {
			// Field doesn't exist - return empty key to exclude from join
			return ""
		}
	}

	return strings.Join(keyParts, "|")
}

// sortDocuments sorts documents by join key
func (m *MergeJoinNode) sortDocuments(docs []*models.Document) {
	sort.Slice(docs, func(i, j int) bool {
		return m.compareJoinKeys(docs[i], docs[j]) < 0
	})
}

// compareJoinKeys compares join keys of two documents
func (m *MergeJoinNode) compareJoinKeys(leftDoc, rightDoc *models.Document) int {
	for _, condition := range m.JoinConditions {
		leftField := condition.LeftField
		rightField := condition.RightField

		leftValue, leftExists := leftDoc.Fields[leftField]
		rightValue, rightExists := rightDoc.Fields[rightField]

		if !leftExists && !rightExists {
			continue
		}
		if !leftExists {
			return -1
		}
		if !rightExists {
			return 1
		}

		// Compare values as strings for simplicity
		leftStr := fmt.Sprintf("%v", leftValue.Value)
		rightStr := fmt.Sprintf("%v", rightValue.Value)

		if leftStr < rightStr {
			return -1
		} else if leftStr > rightStr {
			return 1
		}
	}
	return 0
}

// Shared utility functions

// evaluateJoinConditions checks if join conditions are satisfied
func evaluateJoinConditions(leftDoc, rightDoc *models.Document, conditions []queryparser.JoinCondition, logger *zap.SugaredLogger) bool {
	if leftDoc == nil || rightDoc == nil {
		return false
	}

	for _, condition := range conditions {
		leftField, leftExists := leftDoc.Fields[condition.LeftField]
		rightField, rightExists := rightDoc.Fields[condition.RightField]

		if !leftExists || !rightExists {
			return false
		}

		// Evaluate condition based on operator
		if !evaluateComparison(leftField.Value, condition.Operator, rightField.Value, logger) {
			return false
		}
	}

	return true
}

// evaluateComparison evaluates a comparison operation
func evaluateComparison(leftValue interface{}, operator string, rightValue interface{}, logger *zap.SugaredLogger) bool {
	leftStr := fmt.Sprintf("%v", leftValue)
	rightStr := fmt.Sprintf("%v", rightValue)

	switch operator {
	case "==":
		return leftStr == rightStr
	case "!=":
		return leftStr != rightStr
	case ">":
		return leftStr > rightStr
	case "<":
		return leftStr < rightStr
	case ">=":
		return leftStr >= rightStr
	case "<=":
		return leftStr <= rightStr
	default:
		logger.Warnf("Unknown join operator: %s", operator)
		return false
	}
}

// createJoinedDocument combines fields from two documents
func createJoinedDocument(leftDoc, rightDoc *models.Document, leftDocID, rightDocID string, logger *zap.SugaredLogger) *models.Document {
	logger.Infof("Creating joined document from left ID: %s and right ID: %s", leftDocID, rightDocID)
	// STEP 1: Use document pool to reduce allocations
	// TODO: Option C - Implement reference counting for automatic pool return
	joinedDoc := document.GetPooledDocument()
	joinedDoc.DocumentID = fmt.Sprintf("joined_%s_%s", leftDocID, rightDocID)
	joinedDoc.Fields = make(map[string]models.Field)

	// Add fields from left document with prefix
	if leftDoc != nil {
		for fieldName, field := range leftDoc.Fields {
			prefixedName := fmt.Sprintf("left_%s", fieldName)
			joinedDoc.Fields[prefixedName] = field
		}
		joinedDoc.Fields["left_DocumentID"] = models.Field{
			Name:  "left_DocumentID",
			Value: models.NewStringValue(leftDoc.DocumentID),
		}
	}

	// Add fields from right document with prefix
	if rightDoc != nil {
		for fieldName, field := range rightDoc.Fields {
			prefixedName := fmt.Sprintf("right_%s", fieldName)
			joinedDoc.Fields[prefixedName] = field
		}
		joinedDoc.Fields["right_DocumentID"] = models.Field{
			Name:  "right_DocumentID",
			Value: models.NewStringValue(rightDoc.DocumentID),
		}
	}

	return joinedDoc
}

// estimateJoinRows estimates the number of rows a join will produce
func estimateJoinRows(leftRows, rightRows int, joinType queryparser.JoinType) int {
	switch joinType {
	case queryparser.InnerJoin:
		// Conservative estimate: assume some selectivity
		return int(float64(leftRows*rightRows) * 0.1)
	case queryparser.LeftJoin:
		// At least as many as left side
		return max(leftRows, int(float64(leftRows*rightRows)*0.1))
	case queryparser.RightJoin:
		// At least as many as right side
		return max(rightRows, int(float64(leftRows*rightRows)*0.1))
	case queryparser.FullOuterJoin:
		// At least as many as both sides
		return max(leftRows+rightRows, int(float64(leftRows*rightRows)*0.1))
	default:
		return leftRows * rightRows
	}
}

// max returns the maximum of two integers
func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
