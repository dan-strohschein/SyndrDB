package planner

import (
	"fmt"

	"syndrdb/src/internal/domain/index/btreeindexV2"
	"syndrdb/src/internal/domain/index/hashindexV2"
	"syndrdb/src/internal/domain/models"
	"syndrdb/src/internal/query/queryparser"

	"go.mongodb.org/mongo-driver/bson/primitive"
	// Import your B-tree index package when ready
)

// Execute methods for different node types

func (node *IndexScanNode) Execute() (map[string]*models.Document, error) {
	switch node.ScanType {
	case HashIndexScan:
		return node.executeHashIndexScan()
	case BTreeIndexScan:
		return node.executeBTreeIndexScan()
	case BTreeRangeScan:
		return node.executeBTreeRangeScan()
	default:
		return nil, fmt.Errorf("unsupported scan type: %v", node.ScanType)
	}
}

func (node *IndexScanNode) executeHashIndexScan() (map[string]*models.Document, error) {
	node.Logger.Infof("Executing hash index scan on %s for key %v", node.IndexName, node.SearchKey)

	// Find the hash index in the bundle
	if node.Bundle.Indexes == nil {
		return nil, fmt.Errorf("no indexes found in bundle %s", node.Bundle.Name)
	}

	indexRef, exists := node.Bundle.Indexes[node.IndexName]
	if !exists {
		return nil, fmt.Errorf("hash index %s not found in bundle %s", node.IndexName, node.Bundle.Name)
	}

	if indexRef.IndexType != "hash" {
		return nil, fmt.Errorf("index %s is not a hash index (type: %s)", node.IndexName, indexRef.IndexType)
	}

	// Cast to the V2 hash index
	_, ok := indexRef.IndexInstance.(primitive.D)
	if indexRef.IndexInstance == nil || ok {
		node.Logger.Infof("IndexRef is NIL - Loading hash index %s for bundle %s", node.IndexName, node.Bundle.Name)
		var err error
		indexRef.IndexInstance, err = queryparser.EnsureHashIndexLoaded(node.Bundle, &indexRef, node.Logger)
		if err != nil {
			return nil, fmt.Errorf("failed to load hash index %s: %w", node.IndexName, err)
		}
	}

	hashIndex, ok := indexRef.IndexInstance.(*hashindexV2.HashIndex)
	if !ok {
		return nil, fmt.Errorf("hash index %s is not of type *hashindexV2.HashIndex", node.IndexName)
	}

	// Convert search key to string
	searchKeyStr := fmt.Sprintf("%v", node.SearchKey)

	// Search the hash index for document IDs
	documentIDs, err := hashIndex.Search(searchKeyStr)
	if err != nil {
		return nil, fmt.Errorf("hash index search failed: %w", err)
	}

	node.Logger.Debugf("Hash index returned %d document IDs for key %v", len(documentIDs), node.SearchKey)

	// Retrieve the actual documents from the bundle
	results := make(map[string]*models.Document)

	if node.Bundle.Documents == nil {
		node.Logger.Warnf("Bundle %s has no documents loaded", node.Bundle.Name)
		return results, nil
	}

	for _, docID := range documentIDs {
		if doc, exists := (*node.Bundle.Documents)[docID]; exists {
			// Make a copy of the document to avoid modification issues
			docCopy := doc
			results[docID] = &docCopy
			node.Logger.Debugf("Retrieved document %s from bundle", docID)
		} else {
			// Document ID is in index but not in bundle - this could indicate data inconsistency
			node.Logger.Warnf("Document ID %s found in hash index but not in bundle documents", docID)
		}
	}

	node.Logger.Infof("Hash index scan returned %d documents for key %v", len(results), node.SearchKey)
	return results, nil
}

// // verifyHashIndex verifies the hash index is accessible
// func (node *IndexScanNode) verifyHashIndex(hashIndex *hashindexV2.HashIndex) error {
// 	// TODO add a simple health check here
// 	// For example, checking if the index file exists and is readable
// 	return nil
// }

func (node *IndexScanNode) executeBTreeIndexScan() (map[string]*models.Document, error) {
	node.Logger.Infof("Executing B-tree index scan on %s for key %v", node.IndexName, node.SearchKey)

	// Find the B-tree index in the bundle
	if node.Bundle.Indexes == nil {
		return nil, fmt.Errorf("no indexes found in bundle %s", node.Bundle.Name)
	}

	indexRef, exists := node.Bundle.Indexes[node.IndexName]
	if !exists {
		return nil, fmt.Errorf("btree index %s not found in bundle %s", node.IndexName, node.Bundle.Name)
	}

	if indexRef.IndexType != "btree" {
		return nil, fmt.Errorf("index %s is not a B-tree index (type: %s)", node.IndexName, indexRef.IndexType)
	}

	// Cast to the V2 B-tree index
	btreeIndex, ok := indexRef.IndexInstance.(*btreeindexV2.BTreeIndex)
	if !ok {
		return nil, fmt.Errorf("btree index %s is not of type *btreeindexV2.BTreeIndex", node.IndexName)
	}

	// Assert that SearchKey is of type []byte
	searchKeyBytes, ok := node.SearchKey.([]byte)
	if !ok {
		return nil, fmt.Errorf("btree index search key must be of type []byte, got %T", node.SearchKey)
	}

	// Search the B-tree index for document IDs
	documentIDs, err := btreeIndex.Search(searchKeyBytes)
	if err != nil {
		return nil, fmt.Errorf("btree index search failed: %w", err)
	}

	node.Logger.Debugf("B-tree index returned %d document IDs for key %v", len(documentIDs), node.SearchKey)

	// Retrieve the actual documents from the bundle
	results := make(map[string]*models.Document)
	for _, docID := range documentIDs {
		if doc, exists := (*node.Bundle.Documents)[docID]; exists {
			// Make a copy of the document to avoid modification issues
			docCopy := doc
			results[docID] = &docCopy
			node.Logger.Debugf("Retrieved document %s from bundle", docID)
		} else {
			// Document ID is in index but not in bundle - this could indicate data inconsistency
			node.Logger.Warnf("Document ID %s found in B-tree index but not in bundle documents", docID)
		}
	}

	node.Logger.Infof("B-tree index scan returned %d documents for key %v", len(results), node.SearchKey)
	return results, nil
}

func (node *IndexScanNode) executeBTreeRangeScan() (map[string]*models.Document, error) {
	node.Logger.Infof("Executing B-tree range scan on %s", node.IndexName)

	// Find the B-tree index in the bundle
	if node.Bundle.Indexes == nil {
		return nil, fmt.Errorf("no indexes found in bundle %s", node.Bundle.Name)
	}

	indexRef, exists := node.Bundle.Indexes[node.IndexName]
	if !exists {
		return nil, fmt.Errorf("btree index %s not found in bundle %s",
			node.IndexName, node.Bundle.Name)
	}

	if indexRef.IndexType != "btree" {
		return nil, fmt.Errorf("index %s is not a B-tree index (type:%s)", node.IndexName, indexRef.IndexType)
	}

	// Cast to the V2 B-tree index
	btreeIndex, ok := indexRef.IndexInstance.(*btreeindexV2.BTreeIndex)
	if !ok {
		return nil, fmt.Errorf("btree index %s is not of type *btreeindexV2.BTreeIndex",
			node.IndexName)
	}

	// Perform the range scan
	if node.RangeStart == nil || node.RangeEnd == nil {
		return nil, fmt.Errorf("btree range scan requires both RangeStart and RangeEnd to be set")
	}

	rangeStartBytes, ok := node.RangeStart.([]byte)
	if !ok {
		return nil, fmt.Errorf("btree range scan RangeStart must be of type []byte, got %T", node.RangeStart)
	}
	rangeEndBytes, ok := node.RangeEnd.([]byte)
	if !ok {
		return nil, fmt.Errorf("btree range scan RangeEnd must be of type []byte, got %T", node.RangeEnd)
	}

	rootPageNum := btreeIndex.GetRootPageNum()
	if rootPageNum == 0 {
		return nil, fmt.Errorf("btree index %s has no root page", node.IndexName)
	}

	searchResults, err := btreeindexV2.RangeSearch(btreeIndex, rangeStartBytes, rangeEndBytes, rootPageNum)
	if err != nil {
		return nil, fmt.Errorf("btree range scan failed: %w", err)
	}

	node.Logger.Debugf("B-tree range scan returned %d document IDs for range [%v, %v]", len(searchResults.DocumentIDs), node.RangeStart, node.RangeEnd)

	// Retrieve the actual documents from the bundle
	if node.Bundle.Documents == nil {
		node.Logger.Warnf("Bundle %s has no documents loaded", node.Bundle.Name)
		return nil, nil
	}

	results := make(map[string]*models.Document)
	for _, docID := range searchResults.DocumentIDs {
		if doc, exists := (*node.Bundle.Documents)[docID]; exists {
			// Make a copy of the document to avoid modification issues
			docCopy := doc
			results[docID] = &docCopy
			node.Logger.Debugf("Retrieved document %s from bundle", docID)
		} else {
			// Document ID is in index but not in bundle - this could indicate data inconsistency
			node.Logger.Warnf("Document ID %s found in B-tree index but not in bundle documents", docID)
		}
	}

	node.Logger.Infof("B-tree range scan returned %d documents for range [%v, %v]", len(results), node.RangeStart, node.RangeEnd)
	return results, nil
}

func (node *FullScanNode) Execute() (map[string]*models.Document, error) {
	node.Logger.Infof("Executing full bundle scan on %s", node.Bundle.Name)

	// Return all documents from the bundle
	results := make(map[string]*models.Document)
	for docID, doc := range *node.Bundle.Documents {
		docCopy := doc
		results[docID] = &docCopy
	}

	return results, nil
}

func (node *FilterNode) Execute() (map[string]*models.Document, error) {
	// Execute child node first
	documents, err := node.Child.Execute()
	if err != nil {
		return nil, err
	}

	// Apply filters
	filtered := make(map[string]*models.Document)
	for docID, doc := range documents {
		if node.matchesConditions(doc) {
			filtered[docID] = doc
		}
	}

	node.Logger.Infof("Filter node reduced %d documents to %d", len(documents), len(filtered))
	return filtered, nil
}

func (node *FilterNode) matchesConditions(doc *models.Document) bool {
	for _, clause := range node.Clauses {
		if !clause.Matches(doc, node.Logger) {
			return false
		}
	}
	return true
}

func (node *UnionNode) Execute() (map[string]*models.Document, error) {
	allResults := make(map[string]*models.Document)

	for _, child := range node.Children {
		results, err := child.Execute()
		if err != nil {
			return nil, err
		}

		// Merge results (union automatically deduplicates by document ID)
		for docID, doc := range results {
			allResults[docID] = doc
		}
	}

	node.Logger.Infof("Union node combined %d children into %d results", len(node.Children), len(allResults))
	return allResults, nil
}

func (node *UnionNode) GetCost() float64      { return node.Cost }
func (node *UnionNode) GetEstimatedRows() int { return node.EstimatedRows }

// Cost and estimation methods
func (node *IndexScanNode) GetCost() float64      { return node.Cost }
func (node *IndexScanNode) GetEstimatedRows() int { return node.EstimatedRows }
func (node *FullScanNode) GetCost() float64       { return node.Cost }
func (node *FullScanNode) GetEstimatedRows() int  { return node.EstimatedRows }
func (node *FilterNode) GetCost() float64         { return node.Cost }
func (node *FilterNode) GetEstimatedRows() int    { return node.EstimatedRows }
