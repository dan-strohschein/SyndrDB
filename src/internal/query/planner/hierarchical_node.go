/*
HIERARCHICAL TRANSFORM EXECUTION NODE - PHASE 2

This file implements the HierarchicalTransformNode execution node for the unified query system.
It provides WITH RELATIONSHIP functionality by wrapping the existing HierarchicalTransformer component.

ARCHITECTURE:
The HierarchicalTransformNode follows the Adapter pattern, delegating hierarchical transformation
logic to the well-tested HierarchicalTransformer component while implementing the ExecutionNode interface.

DESIGN PRINCIPLES:
- Single Responsibility: Only responsible for coordinating hierarchical transformation in the query plan
- Open/Closed: Extends ExecutionNode without modifying existing code
- Dependency Inversion: Depends on ExecutionNode abstraction and HierarchicalTransformer

EXECUTION MODEL:
1. Pull JOIN result documents from child node
2. Extract relationship metadata from query
3. Delegate to HierarchicalTransformer for grouping and nesting
4. Return hierarchically structured documents

HIERARCHICAL TRANSFORMATION:
Converts flat JOIN results into nested document structures:
- 1:1 relationships → Nested object
- 1:Many relationships → Array of nested objects
- Many:Many relationships → Array of nested objects with join table fields

EXAMPLE:
Input (flat JOIN):
  [{User: {id:1, name:"Alice"}, Order: {id:101, total:50}},
   {User: {id:1, name:"Alice"}, Order: {id:102, total:75}}]

Output (hierarchical):
  {id:1, name:"Alice", Orders: [{id:101, total:50}, {id:102, total:75}]}

PERFORMANCE:
- Time Complexity: O(n) where n is number of JOIN results
- Space Complexity: O(n) for hierarchical structure
- Single pass grouping algorithm

COST ESTIMATION:
Cost = ChildCost + (n * 0.005)
Where n is estimated number of join result rows

This node is part of Phase 2 of the unified query system implementation.
*/

package planner

import (
	"context"
	"fmt"
	"syndrdb/src/internal/domain/models"
	joinexecutor "syndrdb/src/internal/query/join_executor"
	"syndrdb/src/internal/query/queryparser"
	"syndrdb/src/internal/query/results"

	"go.uber.org/zap"
)

// HierarchicalTransformNode implements WITH RELATIONSHIP hierarchical transformation
// PHASE 2: Execution Nodes - Hierarchical Transform Operation
type HierarchicalTransformNode struct {
	// Child node providing JOIN result documents
	Child ExecutionNode

	// RelationshipName specifies the name for the nested relationship field
	RelationshipName string

	// JoinClauses contains metadata about the joins performed
	JoinClauses []queryparser.JoinClause

	// FromBundle is the parent bundle name
	FromBundle string

	// Cost is the estimated execution cost
	Cost float64

	// EstimatedRows is the expected number of parent documents
	EstimatedRows int

	// Logger for debugging and monitoring
	Logger *zap.SugaredLogger

	// transformer delegates to existing HierarchicalTransformer implementation
	transformer *results.HierarchicalTransformer
}

// NewHierarchicalTransformNode creates a new hierarchical transform execution node
// PHASE 2: Factory function for HierarchicalTransformNode creation
//
// Parameters:
//   - child: ExecutionNode providing JOIN result documents
//   - relationshipName: Name for the nested relationship field
//   - joinClauses: JOIN metadata for transformation
//   - fromBundle: Parent bundle name
//   - logger: Logger for debugging
//
// Returns:
//   - *HierarchicalTransformNode: Configured hierarchical transform node
func NewHierarchicalTransformNode(
	child ExecutionNode,
	relationshipName string,
	joinClauses []queryparser.JoinClause,
	fromBundle string,
	logger *zap.SugaredLogger,
) *HierarchicalTransformNode {

	// Create hierarchical transformer instance (reuses existing component)
	transformer := results.NewHierarchicalTransformer(logger)

	// Estimate number of parent documents
	// Assume hierarchical grouping reduces result set by factor of 5 (heuristic)
	childRows := child.GetEstimatedRows()
	estimatedParents := childRows / 5
	if estimatedParents < 1 {
		estimatedParents = 1
	}
	if estimatedParents > childRows {
		estimatedParents = childRows
	}

	node := &HierarchicalTransformNode{
		Child:            child,
		RelationshipName: relationshipName,
		JoinClauses:      joinClauses,
		FromBundle:       fromBundle,
		Logger:           logger,
		EstimatedRows:    estimatedParents,
		transformer:      transformer,
	}

	// Calculate cost: child cost + transformation cost
	// Transformation is O(n) with small constant factor
	transformCost := float64(childRows) * 0.005
	node.Cost = child.GetCost() + transformCost

	logger.Debugf("Created HierarchicalTransformNode: Relationship='%s', EstimatedParents=%d, Cost=%.4f",
		relationshipName, estimatedParents, node.Cost)

	return node
}

// Execute performs the hierarchical transformation
// PHASE 2: Main execution method for HierarchicalTransformNode
//
// NOTE: Full implementation requires JOIN execution integration (Phase 3).
// For Phase 2, this node structure is defined but transformation logic
// will be fully integrated when the unified planner orchestrates JOIN nodes.
//
// Execution flow:
// 1. Execute child node to get JOIN result documents
// 2. Build relationship metadata from JOIN clauses
// 3. Delegate to HierarchicalTransformer for grouping and nesting
// 4. Return hierarchically structured parent documents
//
// Returns:
//   - map[string]*models.Document: Hierarchically structured documents (or flat results if not yet integrated with JOIN)
//   - error: Any error during execution
func (n *HierarchicalTransformNode) Execute(ctx context.Context) (map[string]*models.Document, error) {
	n.Logger.Infof("Executing HierarchicalTransformNode with relationship '%s'", n.RelationshipName)

	// Execute child node to get documents
	documents, err := n.Child.Execute(ctx)
	if err != nil {
		return nil, fmt.Errorf("HierarchicalTransformNode: child execution failed: %w", err)
	}

	n.Logger.Debugf("HierarchicalTransformNode received %d documents from child", len(documents))

	// Handle empty result set
	if len(documents) == 0 {
		n.Logger.Debug("HierarchicalTransformNode: no documents to transform, returning empty result")
		return documents, nil
	}

	// Handle no relationship name (pass-through results)
	if n.RelationshipName == "" {
		n.Logger.Debug("HierarchicalTransformNode: no relationship name specified, returning documents as-is")
		return documents, nil
	}

	// PHASE 3 ACTIVATION: Find JoinExecutionNode (may be wrapped by FilterNode or other nodes)
	joinNode := n.findJoinExecutionNode(n.Child)
	if joinNode != nil {
		joinedResults := joinNode.GetJoinedResults()
		if len(joinedResults) > 0 {
			// CRITICAL FIX: Filter JOIN results to only include documents that passed through child filters
			// The child node (FilterNode, etc.) has already filtered the documents, so we need to
			// only transform the JOIN results that correspond to the filtered documents
			filteredJoinResults := n.filterJoinResultsByDocumentIDs(joinedResults, documents)

			n.Logger.Infof("HierarchicalTransformNode: Activating transformation for %d filtered JOIN results (from %d total)",
				len(filteredJoinResults), len(joinedResults))

			// Build relationship metadata
			relationshipMetadata, err := n.buildRelationshipMetadata()
			if err != nil {
				n.Logger.Warnf("Failed to build relationship metadata: %v. Returning flat results.", err)
				return documents, nil
			}

			// Create transformation request with FILTERED results
			transformRequest := results.HierarchicalTransformRequest{
				JoinResults:    filteredJoinResults,
				Relationship:   relationshipMetadata,
				SelectedFields: []string{}, // Empty = all fields
				Logger:         n.Logger,
			}

			// Perform hierarchical transformation
			transformResult, err := n.transformer.Transform(transformRequest)
			if err != nil {
				return nil, fmt.Errorf("hierarchical transformation failed: %w", err)
			}

			n.Logger.Infof("HierarchicalTransformNode: Transformation completed - %d parent documents with %d total children",
				transformResult.ParentCount, transformResult.TotalChildDocuments)

			return transformResult.Documents, nil
		}
	}

	// Fallback: Pass-through mode if JoinExecutionNode not found
	n.Logger.Infof("HierarchicalTransformNode: Pass-through mode - JoinExecutionNode not found or has no results")
	return documents, nil
}

// findJoinExecutionNode traverses the execution tree to find a JoinExecutionNode
// PHASE 3: Helper method to find JOIN node even if wrapped by FilterNode or other nodes
//
// This handles the case where the execution tree is:
// HierarchicalTransformNode -> FilterNode -> JoinExecutionNode
//
// Parameters:
//   - node: The execution node to check
//
// Returns:
//   - *JoinExecutionNode: The found JOIN node, or nil if not found
func (n *HierarchicalTransformNode) findJoinExecutionNode(node ExecutionNode) *JoinExecutionNode {
	// Direct check - is this node a JoinExecutionNode?
	if joinNode, ok := node.(*JoinExecutionNode); ok {
		n.Logger.Debug("Found JoinExecutionNode directly")
		return joinNode
	}

	// Check if it's a FilterNode wrapping a JoinExecutionNode
	if filterNode, ok := node.(*FilterNode); ok {
		n.Logger.Debug("Found FilterNode, checking its child")
		return n.findJoinExecutionNode(filterNode.Child)
	}

	// Check if it's a SortNode wrapping a JoinExecutionNode
	if sortNode, ok := node.(*SortNode); ok {
		n.Logger.Debug("Found SortNode, checking its child")
		return n.findJoinExecutionNode(sortNode.Child)
	}

	// Check if it's a LimitNode wrapping a JoinExecutionNode
	if limitNode, ok := node.(*LimitNode); ok {
		n.Logger.Debug("Found LimitNode, checking its child")
		return n.findJoinExecutionNode(limitNode.Child)
	}

	// Check if it's an AggregationNode wrapping a JoinExecutionNode
	if aggNode, ok := node.(*AggregationNode); ok {
		n.Logger.Debug("Found AggregationNode, checking its child")
		return n.findJoinExecutionNode(aggNode.Child)
	}

	// Not found
	n.Logger.Debugf("Node type %T is not a known wrapper, cannot traverse further", node)
	return nil
}

// filterJoinResultsByDocumentIDs filters JOIN results to only include those matching the given document IDs
// PHASE 3: Helper method to filter JOIN results based on filtered documents from child nodes
//
// SYNDRDB RULE: When a JOIN query has TOP/LIMIT/WHERE, these clauses apply to the LEFT side (parent)
// documents, not the total JOIN results. For example:
//
//	SELECT TOP 10 FROM "Authors" JOIN "Books" ...
//
// Returns 10 Author documents with ALL their Books nested, not 10 total JOIN results.
//
// This method ensures that filtering/limiting done by FilterNode or LimitNode is respected
// by only transforming the JOIN results for the parent documents that passed those filters.
//
// Parameters:
//   - joinResults: All JOIN results from the JoinExecutionNode
//   - filteredDocs: Documents that passed through child node filters (FilterNode/LimitNode)
//
// Returns:
//   - []*joinexecutor.JoinedDocument: Filtered JOIN results matching the LEFT side document IDs
func (n *HierarchicalTransformNode) filterJoinResultsByDocumentIDs(
	joinResults []*joinexecutor.JoinedDocument,
	filteredDocs map[string]*models.Document,
) []*joinexecutor.JoinedDocument {

	// Extract LEFT side document IDs from filtered documents
	// The filtered docs are merged documents, but they contain the original LEFT document fields
	// We need to extract the LEFT side DocumentID to match against JOIN results
	leftDocIDs := make(map[string]bool)

	for docID, doc := range filteredDocs {
		// First try: Use the document's own DocumentID (from the merge)
		// The mergeJoinedDocument sets DocumentID to joinedDoc.JoinKey
		leftDocIDs[docID] = true

		// Also try: Extract DocumentID from fields (in case it's stored there)
		if docIDField, exists := doc.Fields["DocumentID"]; exists {
			if fieldDocID, ok := docIDField.Value.AsString(); ok {
				leftDocIDs[fieldDocID] = true
				n.Logger.Debugf("Extracted DocumentID from field: %s", fieldDocID)
			}
		}
	}

	n.Logger.Infof("Filtering JOIN results by LEFT side: %d parent document IDs from %d filtered docs",
		len(leftDocIDs), len(filteredDocs))

	// Log the IDs for debugging
	for id := range leftDocIDs {
		n.Logger.Debugf("Parent document ID: %s", id)
	}

	// Filter JOIN results to only include those with LEFT document IDs in the filtered set
	// SYNDRDB RULE: LIMIT/WHERE applies to LEFT side (parent) documents
	var filtered []*joinexecutor.JoinedDocument
	matchCount := 0
	for _, joinResult := range joinResults {
		if joinResult.LeftDocument != nil {
			if leftDocIDs[joinResult.LeftDocument.DocumentID] {
				filtered = append(filtered, joinResult)
				matchCount++
			}
		}
	}

	n.Logger.Infof("Filtered JOIN results: %d results for %d parent documents (from %d total JOIN results, %d matches)",
		len(filtered), len(leftDocIDs), len(joinResults), matchCount)

	return filtered
}

// buildRelationshipMetadata creates relationship metadata from JOIN clauses
// PHASE 2: Helper method to construct relationship metadata
//
// Returns:
//   - results.RelationshipMetadata: Metadata for hierarchical transformation
//   - error: Any error building metadata
func (n *HierarchicalTransformNode) buildRelationshipMetadata() (results.RelationshipMetadata, error) {
	if len(n.JoinClauses) == 0 {
		return results.RelationshipMetadata{}, fmt.Errorf("no JOIN clauses provided for hierarchical transformation")
	}

	// Use first JOIN clause for relationship metadata
	// TODO: Support multiple JOINs with nested hierarchies
	firstJoin := n.JoinClauses[0]

	// Determine relationship cardinality (default to 1:Many for now)
	// In a full implementation, this would be determined by analyzing the data or schema
	cardinality := "1:Many"

	// Extract join condition fields
	parentField := ""
	childField := ""
	joinKey := ""
	if len(firstJoin.JoinConditions) > 0 {
		// Get first condition for relationship metadata
		condition := firstJoin.JoinConditions[0]
		parentField = condition.LeftField
		childField = condition.RightField
		joinKey = parentField // Use parent field as join key
	}

	// Determine cardinality type
	var cardinalityType results.CardinalityType
	switch cardinality {
	case "1:1":
		cardinalityType = results.OneToOne
	case "1:Many":
		cardinalityType = results.OneToMany
	case "Many:Many":
		cardinalityType = results.ManyToMany
	default:
		cardinalityType = results.OneToMany
	}

	metadata := results.RelationshipMetadata{
		RelationshipName: n.RelationshipName,
		ParentBundle:     n.FromBundle,
		ChildBundle:      firstJoin.RightBundle,
		Cardinality:      cardinalityType,
		JoinKey:          joinKey,
		ParentKey:        parentField,
		ChildKey:         childField,
	}

	n.Logger.Debugf("Built relationship metadata: Parent=%s, Child=%s, Cardinality=%s",
		metadata.ParentBundle, metadata.ChildBundle, metadata.Cardinality)

	return metadata, nil
}

// GetCost returns the estimated execution cost
// PHASE 2: Cost accessor for query planning
func (n *HierarchicalTransformNode) GetCost() float64 {
	return n.Cost
}

// GetEstimatedRows returns the estimated number of parent documents
// PHASE 2: Cardinality accessor for query planning
func (n *HierarchicalTransformNode) GetEstimatedRows() int {
	return n.EstimatedRows
}

// GetRelationshipName returns the relationship name
// PHASE 2: Helper method for query analysis
func (n *HierarchicalTransformNode) GetRelationshipName() string {
	return n.RelationshipName
}

// HasRelationship returns true if a relationship name is specified
// PHASE 2: Helper method for query analysis
func (n *HierarchicalTransformNode) HasRelationship() bool {
	return n.RelationshipName != ""
}
