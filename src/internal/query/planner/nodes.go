package planner

import (
	"fmt"

	"syndrdb/src/internal/domain/index/btreeindexV2"
	// "syndrdb/src/internal/domain/index/hashindexV2" // OLD - Sprint 5: Replaced with V3
	hashindexV3 "syndrdb/src/internal/domain/index/hashindexV3" // NEW - Sprint 5: LSM-style hash index
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

	// SPRINT 5 FIX: Cast to the V3 LSM-style hash index
	_, ok := indexRef.IndexInstance.(primitive.D)
	if indexRef.IndexInstance == nil || ok {
		node.Logger.Infof("IndexRef is NIL - Loading hash index V3 %s for bundle %s", node.IndexName, node.Bundle.Name)
		var err error
		indexRef.IndexInstance, err = queryparser.EnsureHashIndexV3Loaded(node.Bundle, &indexRef, node.Logger)
		if err != nil {
			return nil, fmt.Errorf("failed to load hash index V3 %s: %w", node.IndexName, err)
		}
	}

	hashIndex, ok := indexRef.IndexInstance.(*hashindexV3.HashIndexV3)
	if !ok {
		return nil, fmt.Errorf("hash index %s is not of type *hashindexV3.HashIndexV3 (actual type: %T)", node.IndexName, indexRef.IndexInstance)
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

	// Use bundle service to load the B-tree index if not already loaded
	var btreeIndex *btreeindexV2.BTreeIndex
	if indexRef.IndexInstance == nil {
		node.Logger.Debugf("B-tree index instance is nil, loading from disk using bundle service")
		if node.BundleServiceInt == nil {
			return nil, fmt.Errorf("bundle service is required for lazy loading B-tree indexes")
		}

		loadedIndex, err := node.BundleServiceInt.GetOrLoadBTreeIndex(node.Bundle, node.IndexName, indexRef)
		if err != nil {
			return nil, fmt.Errorf("failed to load B-tree index %s: %w", node.IndexName, err)
		}

		var ok bool
		btreeIndex, ok = loadedIndex.(*btreeindexV2.BTreeIndex)
		if !ok {
			return nil, fmt.Errorf("loaded index is not of type *btreeindexV2.BTreeIndex")
		}
	} else {
		// Cast to the V2 B-tree index
		var ok bool
		btreeIndex, ok = indexRef.IndexInstance.(*btreeindexV2.BTreeIndex)
		if !ok {
			return nil, fmt.Errorf("btree index %s is not of type *btreeindexV2.BTreeIndex", node.IndexName)
		}
	}

	// Convert search key to bytes for B-tree index lookup
	// The search key could be a string, number, etc. - convert to string first, then to bytes
	var searchKeyBytes []byte
	switch v := node.SearchKey.(type) {
	case string:
		searchKeyBytes = []byte(v)
	case []byte:
		searchKeyBytes = v
	default:
		// Convert other types to string representation, then to bytes
		searchKeyStr := fmt.Sprintf("%v", v)
		searchKeyBytes = []byte(searchKeyStr)
	}

	node.Logger.Debugf("Converted search key %v (%T) to bytes: %v", node.SearchKey, node.SearchKey, string(searchKeyBytes))

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
	node.Logger.Infof("Executing B-tree range scan on %s for operator %s with value %v",
		node.IndexName, node.Operator, node.SearchKey)

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

	// Cast to the V2 B-tree index to verify it's available
	_, ok := indexRef.IndexInstance.(*btreeindexV2.BTreeIndex)
	if !ok {
		return nil, fmt.Errorf("btree index %s is not of type *btreeindexV2.BTreeIndex",
			node.IndexName)
	}

	// For now, implement range scans as filtered full scans
	// TODO: Implement proper range scan functionality in B-Tree V2
	node.Logger.Warnf("Range scan not yet fully implemented for operator %s, falling back to filtered scan", node.Operator)

	// For range queries, we'll need to implement a different approach
	// For now, return error to indicate this needs implementation
	return nil, fmt.Errorf("range scan operations (>, <, >=, <=) not yet fully implemented for B-tree indexes")
}

func (node *FullScanNode) Execute() (map[string]*models.Document, error) {
	node.Logger.Infof("Executing optimized full bundle scan on %s using document scanner", node.Bundle.Name)

	results := make(map[string]*models.Document)

	// CRITICAL: Check if documents are complete before using fast path
	// If DocumentsComplete is false, Documents is a memtable (recent writes only), not a complete cache
	// MUST use scanner to merge memtable with disk data
	if node.Bundle.Documents != nil && node.Bundle.DocumentsComplete {
		node.Logger.Debugf("Using complete in-memory documents for bundle %s", node.Bundle.Name)
		for docID, doc := range *node.Bundle.Documents {
			docCopy := doc
			results[docID] = &docCopy
		}
		return results, nil
	}

	// If Documents is nil OR DocumentsComplete is false (memtable mode), use scanner
	// Scanner will merge memtable with disk data for complete results
	if node.Bundle.Documents != nil && !node.Bundle.DocumentsComplete {
		node.Logger.Debugf("Bundle %s has memtable with %d documents - using scanner to merge with disk",
			node.Bundle.Name, len(*node.Bundle.Documents))
	}

	// Use document scanner for optimized scanning with batching and caching
	if node.DocumentScanner == nil {
		return nil, fmt.Errorf("document scanner is required for paginated document scanning")
	}

	// Use predicate-based scan to get all documents
	// This leverages the scanner's batching, caching, and memory management
	scanResult, err := node.DocumentScanner.ScanWithPredicate(func(doc *models.Document) bool {
		return true // Accept all documents for full scan
	})
	if err != nil {
		return nil, fmt.Errorf("document scanner failed: %w", err)
	}

	// Convert scanner result to the expected map format
	for i, doc := range scanResult.Documents {
		docID := scanResult.DocumentIDs[i]
		results[docID] = doc
	}

	node.Logger.Infof("Document scanner completed full scan: %d documents in %v (batches: %d, cache hits: %d)",
		len(results), scanResult.ScanLatency, scanResult.BatchesUsed, scanResult.CacheHits)

	return results, nil
}

// ExecuteStreaming processes documents in batches using the document scanner
// This approach is memory-efficient for very large bundles
func (node *FullScanNode) ExecuteStreaming(callback func(map[string]*models.Document) error) error {
	node.Logger.Infof("Executing streaming scan on bundle %s", node.Bundle.Name)

	if node.DocumentScanner == nil {
		return fmt.Errorf("document scanner is required for streaming scan")
	}

	// Use the scanner's built-in batching for memory-efficient processing
	scanResult, err := node.DocumentScanner.ScanWithPredicate(func(doc *models.Document) bool {
		return true // Accept all documents
	})
	if err != nil {
		return fmt.Errorf("document scanner streaming failed: %w", err)
	}

	// Convert scanner result to map format and process
	documents := make(map[string]*models.Document)
	for i, doc := range scanResult.Documents {
		docID := scanResult.DocumentIDs[i]
		documents[docID] = doc
	}

	// Process the complete result set
	// Note: The scanner already handles batching internally for memory efficiency
	if err := callback(documents); err != nil {
		return err
	}

	node.Logger.Infof("Streaming scan completed: %d documents processed in %d batches",
		len(documents), scanResult.BatchesUsed)

	return nil
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
