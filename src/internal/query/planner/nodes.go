package planner

import (
	"context"
	"fmt"

	"syndrdb/src/internal/domain/index/brinindex"
	"syndrdb/src/internal/domain/index/btreeindexV2"
	// "syndrdb/src/internal/domain/index/hashindexV2" // OLD - Sprint 5: Replaced with V3
	hashindexV3 "syndrdb/src/internal/domain/index/hashindexV3" // NEW - Sprint 5: LSM-style hash index
	"syndrdb/src/internal/domain/models"
	"syndrdb/src/internal/query/documentscanner"
	"syndrdb/src/internal/syndrQL"
	"syndrdb/src/pkg/common/conversion"
	"syndrdb/src/pkg/settings"
)

// Execute methods for different node types

func (node *IndexScanNode) Execute(ctx context.Context) (map[string]*models.Document, error) {
	switch node.ScanType {
	case HashIndexScan:
		return node.executeHashIndexScan(ctx)
	case BTreeIndexScan:
		return node.executeBTreeIndexScan(ctx)
	case BTreeRangeScan:
		return node.executeBTreeRangeScan(ctx)
	default:
		return nil, fmt.Errorf("unsupported scan type: %v", node.ScanType)
	}
}

func (node *IndexScanNode) executeHashIndexScan(ctx context.Context) (map[string]*models.Document, error) {
	node.Logger.Debugf("Executing hash index scan on %s for key %v", node.IndexName, node.SearchKey)

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

	// Use BundleService's cached index instance (same one that writes go to)
	// so we see the latest MemTable entries, not a stale disk-only copy.
	if node.BundleServiceInt == nil {
		return nil, fmt.Errorf("BundleServiceInt is required for hash index scan on bundle %s", node.Bundle.Name)
	}
	loadedIndex, err := node.BundleServiceInt.GetOrLoadHashIndexInterface(node.Bundle, node.IndexName, indexRef)
	if err != nil {
		return nil, fmt.Errorf("failed to load hash index %s: %w", node.IndexName, err)
	}
	hashIndex, ok := loadedIndex.(*hashindexV3.HashIndexV3)
	if !ok {
		return nil, fmt.Errorf("hash index %s is not of type *hashindexV3.HashIndexV3 (actual type: %T)", node.IndexName, loadedIndex)
	}

	// Convert search key to string using the same conversion as index writes
	searchKeyStr := conversion.ValueToString(node.SearchKey)

	// Get both docIDs and pageIDs from the hash index
	// NOTE: Do not pass snapshot sequence here — most index entries have CommitSequence==0
	// (autocommit writes don't populate it), so MVCC filtering would discard valid results.
	// Document-level visibility is handled downstream by GetDocument/GetDocumentPage.
	documentIDs, pageIDs, err := hashIndex.Get(searchKeyStr)
	if err != nil {
		return nil, fmt.Errorf("hash index search failed: %w", err)
	}

	node.Logger.Debugf("Hash index returned %d document IDs for key %v", len(documentIDs), node.SearchKey)

	// When index returns no doc IDs, fall back to full scan + filter so queries still return
	// correct results (e.g. index empty, index created after data load, or legacy wrong-key entries).
	if len(documentIDs) == 0 {
		node.Logger.Warnf("[HASH-IDX] MISS (fallback): bundle=%s index=%s field=%s key=%q → full scan (index empty for key; ~15ms typical)",
			node.Bundle.Name, node.IndexName, indexRef.HashIndexField.FieldName, searchKeyStr)
		fallbackResults, fallbackErr := node.executeHashIndexScanFallback(ctx, indexRef, searchKeyStr)
		if fallbackErr != nil {
			return nil, fallbackErr
		}
		node.Logger.Warnf("[HASH-IDX] fallback returned %d document(s) for key %q", len(fallbackResults), searchKeyStr)
		return fallbackResults, nil
	}

	node.Logger.Infof("[HASH-IDX] HIT: bundle=%s index=%s field=%s key=%q docCount=%d",
		node.Bundle.Name, node.IndexName, indexRef.HashIndexField.FieldName, searchKeyStr, len(documentIDs))

	// Retrieve the actual documents using BundleService (write-through page cache)
	results := make(map[string]*models.Document)

	if node.BundleServiceInt == nil {
		node.Logger.Warnf("BundleServiceInt is nil, cannot retrieve documents for bundle %s", node.Bundle.Name)
		return results, nil
	}

	for i, docID := range documentIDs {
		// Check context every 1000 documents
		if i%1000 == 0 {
			select {
			case <-ctx.Done():
				return results, ctx.Err()
			default:
			}
		}

		var doc *models.Document
		// Try direct page lookup using index-provided pageID (skips findDocumentPage).
		// NOTE: Page IDs are 0-based, so pageID=0 is valid (first physical page).
		// Always attempt the fast path when the index provides a pageID; fall through
		// to GetDocument only if the document is not found on the specified page.
		if i < len(pageIDs) {
			page, pageErr := node.BundleServiceInt.GetDocumentPageReadOnly(
				node.Bundle.Name, node.Bundle.Database.Name, pageIDs[i])
			if pageErr == nil && page != nil {
				if d, exists := page.Documents[docID]; exists {
					doc = &d
				}
			}
		}
		// Fallback to GetDocument if direct lookup missed (stale pageID)
		if doc == nil {
			doc, err = node.BundleServiceInt.GetDocument(node.Bundle.Name, node.Bundle.Database.Name, docID)
			if err != nil {
				// Orphan index entry: the hash index references a document that no
				// longer exists on any storage page. This can happen when:
				//   - A document insert partially completed (index updated, disk write lost)
				//   - A document was deleted but the field-level index was not tombstoned
				//   - Page boundaries shifted and the page scan limit was reached
				// Skip the orphan gracefully instead of failing the entire query.
				// This matches how Postgres/MySQL handle dead index tuples.
				node.Logger.Warnf("[HASH-IDX] Skipping orphan index entry: docID=%s key=%v index=%s err=%v",
					docID, node.SearchKey, node.IndexName, err)
				continue // Skip this orphan entry and process remaining documents
			}
		}
		if doc != nil {
			results[docID] = doc
			node.Logger.Debugf("Retrieved document %s from bundle", docID)
		}
	}

	node.Logger.Debugf("Hash index scan returned %d documents for key %v", len(results), node.SearchKey)
	return results, nil
}

// executeHashIndexScanFallback runs a full scan of the bundle and filters by indexed field == searchKeyStr.
// Used when the hash index returns 0 document IDs so queries still return correct results (e.g. index
// empty, index created after data load, or legacy wrong-key entries).
func (node *IndexScanNode) executeHashIndexScanFallback(ctx context.Context, indexRef models.IndexReference, searchKeyStr string) (map[string]*models.Document, error) {
	fieldName := indexRef.HashIndexField.FieldName
	results := make(map[string]*models.Document)

	if node.DocumentScanner == nil {
		node.Logger.Debugf("Hash index fallback skipped: no DocumentScanner for bundle %s", node.Bundle.Name)
		return results, nil
	}

	// Full scan then filter by field value (same key normalization as index lookups).
	scanResult, err := node.DocumentScanner.ScanAllDocuments()
	if err != nil {
		return nil, fmt.Errorf("hash index fallback scan failed for bundle %s: %w", node.Bundle.Name, err)
	}
	for _, doc := range scanResult.Documents {
		if doc == nil {
			continue
		}
		var val interface{}
		if fv, ok := doc.GetFieldValue(nil, fieldName); ok {
			val = fv.AsInterface()
		} else if doc.Data != nil {
			val = doc.Data[fieldName]
		}
		if val == nil {
			continue
		}
		docKeyStr := conversion.ValueToString(val)
		if docKeyStr == searchKeyStr {
			results[doc.DocumentID] = doc
		}
	}

	node.Logger.Debugf("Hash index fallback returned %d documents for key %q on field %s", len(results), searchKeyStr, fieldName)
	return results, nil
}

// // verifyHashIndex verifies the hash index is accessible
// func (node *IndexScanNode) verifyHashIndex(hashIndex *hashindexV2.HashIndex) error {
// 	// TODO add a simple health check here
// 	// For example, checking if the index file exists and is readable
// 	return nil
// }

func (node *IndexScanNode) executeBTreeIndexScan(ctx context.Context) (map[string]*models.Document, error) {
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

	// Retrieve the actual documents using BundleService (write-through page cache)
	results := make(map[string]*models.Document)

	// WRITE-THROUGH CACHE: All document retrieval goes through page cache
	if node.BundleServiceInt == nil {
		node.Logger.Warnf("BundleServiceInt is nil, cannot retrieve documents for bundle %s", node.Bundle.Name)
		return results, nil
	}

	docCount := 0
	for _, docID := range documentIDs {
		// Check context every 1000 documents
		// TODO: I can make this check frequency adaptive based on document processing rate
		if docCount%1000 == 0 {
			select {
			case <-ctx.Done():
				// Return partial results on timeout
				return results, ctx.Err()
			default:
				// Continue processing
			}
		}
		// Retrieve document from page cache via BundleService
		doc, err := node.BundleServiceInt.GetDocument(node.Bundle.Name, node.Bundle.Database.Name, docID)
		if err != nil {
			// Issue 4: Fail fast — do not return partial results when GetDocument fails
			return nil, fmt.Errorf("B-tree index scan: GetDocument failed for document ID %s: %w", docID, err)
		}
		if doc != nil {
			results[docID] = doc
			node.Logger.Debugf("Retrieved document %s from bundle", docID)
		}
		docCount++
	}

	node.Logger.Debugf("B-tree index scan returned %d documents for key %v", len(results), node.SearchKey)
	return results, nil
}

func (node *IndexScanNode) ExecuteBTreeRangeScan() (map[string]*models.Document, error) {
	return node.executeBTreeRangeScan(context.Background())
}

func (node *IndexScanNode) executeBTreeRangeScan(ctx context.Context) (map[string]*models.Document, error) {
	node.Logger.Debugf("Executing B-tree range scan on %s for operator %s with value %v",
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
			return nil, fmt.Errorf("btree index %s is not of type *btreeindexV2.BTreeIndex (actual type: %T)",
				node.IndexName, indexRef.IndexInstance)
		}
	}

	// Convert operator and value to start/end key range for B-tree RangeSearch
	startKey, endKey, excludeStart, excludeEnd, err := node.operatorToKeyRange(node.Operator, node.SearchKey, node.RangeStart, node.RangeEnd)
	if err != nil {
		return nil, fmt.Errorf("failed to convert operator to key range: %w", err)
	}

	node.Logger.Debugf("Range scan: operator=%s, startKey=%s, endKey=%s, excludeStart=%v, excludeEnd=%v",
		node.Operator, string(startKey), string(endKey), excludeStart, excludeEnd)

	// Perform the range search on the B-tree index with proper bound exclusion
	documentIDs, err := btreeIndex.RangeSearchWithBounds(startKey, endKey, excludeStart, excludeEnd)
	if err != nil {
		return nil, fmt.Errorf("btree range search failed: %w", err)
	}

	node.Logger.Debugf("B-tree range scan returned %d document IDs", len(documentIDs))

	// Retrieve the actual documents using BundleService (write-through page cache)
	results := make(map[string]*models.Document)

	// WRITE-THROUGH CACHE: All document retrieval goes through page cache
	if node.BundleServiceInt == nil {
		node.Logger.Warnf("BundleServiceInt is nil, cannot retrieve documents for bundle %s", node.Bundle.Name)
		return results, nil
	}

	docCount := 0
	for _, docID := range documentIDs {
		// Check context every 1000 documents
		// TODO: I can make this check frequency adaptive based on document processing rate
		if docCount%1000 == 0 {
			select {
			case <-ctx.Done():
				// Return partial results on timeout
				return results, ctx.Err()
			default:
				// Continue processing
			}
		}
		// Retrieve document from page cache via BundleService
		doc, err := node.BundleServiceInt.GetDocument(node.Bundle.Name, node.Bundle.Database.Name, docID)
		if err != nil {
			// Issue 4: Fail fast — do not return partial results when GetDocument fails
			return nil, fmt.Errorf("B-tree range scan: GetDocument failed for document ID %s: %w", docID, err)
		}
		if doc != nil {
			results[docID] = doc
			node.Logger.Debugf("Retrieved document %s from bundle", docID)
		}
		docCount++
	}

	node.Logger.Infof("B-tree range scan returned %d documents for operator %s", len(results), node.Operator)
	return results, nil
}

func (node *IndexScanNode) OperatorToKeyRange(operator string, searchKey, rangeStart, rangeEnd interface{}) ([]byte, []byte, bool, bool, error) {
	return node.operatorToKeyRange(operator, searchKey, rangeStart, rangeEnd)
}

// operatorToKeyRange converts query operators (>, <, >=, <=, BETWEEN) to B-tree key ranges
// This function implements the critical operator → key range conversion for range queries
//
// B-tree RangeSearchWithBounds(startKey, endKey, excludeStart, excludeEnd) returns all keys where:
//   - excludeStart=false, excludeEnd=false: startKey <= key <= endKey (both inclusive)
//   - excludeStart=true, excludeEnd=false:  startKey < key <= endKey  (exclusive start)
//   - excludeStart=false, excludeEnd=true:  startKey <= key < endKey  (exclusive end)
//   - excludeStart=true, excludeEnd=true:   startKey < key < endKey   (both exclusive)
//
// Operator Mappings:
//   - ">":  (value, ∞)  - exclusive lower bound → excludeStart=true, excludeEnd=false
//   - ">=": [value, ∞)  - inclusive lower bound → excludeStart=false, excludeEnd=false
//   - "<":  (-∞, value) - exclusive upper bound → excludeStart=false, excludeEnd=true
//   - "<=": (-∞, value] - inclusive upper bound → excludeStart=false, excludeEnd=false
//   - "BETWEEN": [rangeStart, rangeEnd] - inclusive both bounds → excludeStart=false, excludeEnd=false
//
// Returns: startKey, endKey, excludeStart, excludeEnd, error
func (node *IndexScanNode) operatorToKeyRange(operator string, searchKey, rangeStart, rangeEnd interface{}) ([]byte, []byte, bool, bool, error) {
	switch operator {
	case ">":
		// Greater than: Start from searchKey (exclusive), end at maximum
		keyBytes := node.convertToBytes(searchKey)
		startKey := keyBytes
		endKey := []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF} // Maximum key (8 bytes of 0xFF)
		return startKey, endKey, true, false, nil                        // excludeStart=true for exclusive lower bound

	case ">=":
		// Greater than or equal: Start from searchKey (inclusive), end at maximum
		startKey := node.convertToBytes(searchKey)
		endKey := []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}
		return startKey, endKey, false, false, nil // both inclusive

	case "<":
		// Less than: Start from minimum, end at searchKey (exclusive)
		keyBytes := node.convertToBytes(searchKey)
		startKey := []byte{0x00} // Minimum key
		endKey := keyBytes
		return startKey, endKey, false, true, nil // excludeEnd=true for exclusive upper bound

	case "<=":
		// Less than or equal: Start from minimum, end at searchKey (inclusive)
		startKey := []byte{0x00}
		endKey := node.convertToBytes(searchKey)
		return startKey, endKey, false, false, nil // both inclusive

	case "BETWEEN":
		// Between: Use rangeStart and rangeEnd (both inclusive)
		if rangeStart == nil || rangeEnd == nil {
			return nil, nil, false, false, fmt.Errorf("BETWEEN operator requires both rangeStart and rangeEnd")
		}
		startKey := node.convertToBytes(rangeStart)
		endKey := node.convertToBytes(rangeEnd)
		return startKey, endKey, false, false, nil // both inclusive

	default:
		return nil, nil, false, false, fmt.Errorf("unsupported range operator: %s (supported: >, >=, <, <=, BETWEEN)", operator)
	}
}

func (node *IndexScanNode) ConvertToBytes(value interface{}) []byte {
	return node.convertToBytes(value)
}

// convertToBytes converts various types to byte slices for B-tree index operations
// Uses KeyEncoder for proper numeric ordering
func (node *IndexScanNode) convertToBytes(value interface{}) []byte {
	encoder := NewKeyEncoder()
	encoded, err := encoder.EncodeKey(value)
	if err != nil {
		// Fallback to string representation if encoding fails
		node.Logger.Warnf("Failed to encode key value %v: %v, falling back to string", value, err)
		return []byte(fmt.Sprintf("%v", value))
	}
	return encoded
}

// Full-range key bounds for B-tree ordered scan (all keys)
var (
	btreeOrderedScanStart = []byte{0x00}
	btreeOrderedScanEnd   = []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}
)

// executeBTreeOrderedScanFull runs a full B-tree range scan and returns documents in index key order.
// Supports both ASC (default) and DESC order, with optional LIMIT early termination.
// Uses BundleServiceInt.GetDocument() to retrieve documents through the write-through page cache.
func (node *BTreeOrderedScanNode) executeBTreeOrderedScanFull(ctx context.Context) ([]*models.Document, error) {
	if node.Bundle.Indexes == nil {
		return nil, fmt.Errorf("no indexes found in bundle %s", node.Bundle.Name)
	}
	indexRef, exists := node.Bundle.Indexes[node.IndexName]
	if !exists {
		return nil, fmt.Errorf("btree index %s not found in bundle %s", node.IndexName, node.Bundle.Name)
	}
	if indexRef.IndexType != "btree" {
		return nil, fmt.Errorf("index %s is not a btree (type: %s)", node.IndexName, indexRef.IndexType)
	}

	var btreeIndex *btreeindexV2.BTreeIndex
	if indexRef.IndexInstance == nil {
		if node.BundleServiceInt == nil {
			return nil, fmt.Errorf("BundleServiceInt required to load btree index %s", node.IndexName)
		}
		loaded, err := node.BundleServiceInt.GetOrLoadBTreeIndex(node.Bundle, node.IndexName, indexRef)
		if err != nil {
			return nil, err
		}
		var ok bool
		btreeIndex, ok = loaded.(*btreeindexV2.BTreeIndex)
		if !ok {
			return nil, fmt.Errorf("index %s is not *btreeindexV2.BTreeIndex", node.IndexName)
		}
	} else {
		var ok bool
		btreeIndex, ok = indexRef.IndexInstance.(*btreeindexV2.BTreeIndex)
		if !ok {
			return nil, fmt.Errorf("index %s is not *btreeindexV2.BTreeIndex", node.IndexName)
		}
	}

	// OPTIMIZATION: Use limit-aware and direction-aware range search
	// This avoids loading all document IDs when we only need LIMIT documents
	var docIDs []string
	var err error

	if node.Descending {
		// DESC order: Use reverse range search for optimal ORDER BY DESC LIMIT performance
		node.Logger.Debugf("BTreeOrderedScanNode: Using reverse range search (DESC) with limit=%d", node.Limit)
		docIDs, err = btreeIndex.ReverseRangeSearchWithLimit(btreeOrderedScanStart, btreeOrderedScanEnd, node.Limit)
	} else if node.Limit > 0 {
		// ASC order with LIMIT: Use forward range search with early termination
		node.Logger.Debugf("BTreeOrderedScanNode: Using forward range search (ASC) with limit=%d", node.Limit)
		docIDs, err = btreeIndex.RangeSearchWithLimit(btreeOrderedScanStart, btreeOrderedScanEnd, node.Limit)
	} else {
		// Full range search (no limit, ASC order)
		docIDs, err = btreeIndex.RangeSearchWithBounds(btreeOrderedScanStart, btreeOrderedScanEnd, false, false)
	}
	if err != nil {
		return nil, fmt.Errorf("BTreeOrderedScanNode: range search failed: %w", err)
	}

	// WRITE-THROUGH CACHE: All document retrieval goes through page cache
	if node.BundleServiceInt == nil {
		return nil, fmt.Errorf("BTreeOrderedScanNode: BundleServiceInt is nil for bundle %s", node.Bundle.Name)
	}

	// Pre-allocate based on actual docIDs count (may be limited)
	out := make([]*models.Document, 0, len(docIDs))
	for i, docID := range docIDs {
		if i > 0 && i%1000 == 0 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			default:
			}
		}
		// Retrieve document from page cache via BundleService
		doc, err := node.BundleServiceInt.GetDocument(node.Bundle.Name, node.Bundle.Database.Name, docID)
		if err != nil {
			// Issue 4: Fail fast — do not return partial results when GetDocument fails
			return nil, fmt.Errorf("BTreeOrderedScan: GetDocument failed for document ID %s: %w", docID, err)
		}
		if doc != nil {
			out = append(out, doc)
		}
	}

	node.Logger.Debugf("BTreeOrderedScanNode: returned %d documents (limit=%d, desc=%v)", len(out), node.Limit, node.Descending)
	return out, nil
}

func (node *BTreeOrderedScanNode) Execute(ctx context.Context) (map[string]*models.Document, error) {
	slice, err := node.executeBTreeOrderedScanFull(ctx)
	if err != nil {
		return nil, err
	}
	m := make(map[string]*models.Document, len(slice))
	for _, d := range slice {
		m[d.DocumentID] = d
	}
	return m, nil
}

func (node *BTreeOrderedScanNode) ExecuteOrdered(ctx context.Context) ([]*models.Document, error) {
	return node.executeBTreeOrderedScanFull(ctx)
}

func (node *BTreeOrderedScanNode) OrderedByField() string {
	return node.OrderedByFieldName
}

func (node *BTreeOrderedScanNode) GetCost() float64           { return node.Cost }
func (node *BTreeOrderedScanNode) GetEstimatedRows() int      { return node.EstimatedRows }
func (node *BTreeOrderedScanNode) EstimateMemoryUsage() int64 { return 0 }

func (node *FullScanNode) Execute(ctx context.Context) (map[string]*models.Document, error) {
	node.Logger.Debugf("Executing optimized full bundle scan on %s using document scanner", node.Bundle.Name)

	// Pre-allocate map after scan completes (capacity set below after scan)
	var results map[string]*models.Document

	// Get memory tracker from context for per-query memory limit
	memoryTracker := GetMemoryTrackerFromContext(ctx)

	// PHASE 4: MVCC - Get snapshot info from context for in-memory filtering
	var snapshotInfo *SnapshotInfo
	if snapshotInfo = GetSnapshotInfoFromContext(ctx); snapshotInfo != nil {
		node.Logger.Debugf("MVCC: Using snapshot filtering: seq=%d, txID=%d", snapshotInfo.SnapshotSequence, snapshotInfo.TransactionID)
	}

	// WRITE-THROUGH CACHE: All document access goes through page cache via document scanner
	// Use document scanner for optimized scanning with batching and caching
	if node.DocumentScanner == nil {
		return nil, fmt.Errorf("document scanner is required for paginated document scanning")
	}

	// PHASE 4: MVCC - Set snapshot on scanner if available in context
	if snapshotInfo := GetSnapshotInfoFromContext(ctx); snapshotInfo != nil {
		if smartScanner, ok := node.DocumentScanner.(interface {
			SetSnapshot(snapshotSeq uint64, txID uint64, activeTxIDs map[uint64]bool)
		}); ok {
			smartScanner.SetSnapshot(snapshotInfo.SnapshotSequence, snapshotInfo.TransactionID, snapshotInfo.ActiveTxIDs)
			node.Logger.Debugf("MVCC: Set snapshot on scanner: seq=%d, txID=%d", snapshotInfo.SnapshotSequence, snapshotInfo.TransactionID)
		}
	}

	// PROJECTION: Establish projection for this scan (overwrites any previous query's projection).
	// - When ProjectionFields is nil/empty: clear storage and adapter so we get full deserialization.
	//   (A prior ORDER BY or other query may have set projection; leaving it causes "GROUP BY field X
	//   not found in document" when this scan expects all fields.)
	// - When ProjectionFields is set: use it for projection pushdown; defer clear when done.
	// CRITICAL: Always use defer to ensure projection is cleared, even if ProjectionFields is empty.
	// This prevents projection from a previous query from affecting this query.
	if node.BundleServiceInt != nil {
		node.BundleServiceInt.SetProjectionFieldsForBundle(node.Bundle.Name, node.ProjectionFields)
		defer node.BundleServiceInt.SetProjectionFieldsForBundle(node.Bundle.Name, nil) // Always clear
	}
	if smartScanner, ok := node.DocumentScanner.(interface {
		GetBundle() documentscanner.BundleInterface
	}); ok {
		if bundleAdapter, ok := smartScanner.GetBundle().(*documentscanner.BundleAdapter); ok {
			bundleAdapter.SetProjectionFields(node.ProjectionFields)
			defer bundleAdapter.SetProjectionFields(nil) // Always clear
			if len(node.ProjectionFields) > 0 {
				node.Logger.Infof("PROJECTION PUSHDOWN: Set projection fields %v on BundleAdapter", node.ProjectionFields)
			}
		}
	}

	// Use ScanAllDocuments() for full scans - this bypasses the O(n*m) GetDocumentIDs() + GetDocument() loop
	// by using the efficient GetAllDocuments() method that loads pages sequentially
	// OPTIMIZATION: If MaxDocuments > 0 (simple LIMIT-only query), use early termination
	var scanResult *documentscanner.ScanResult
	var err error
	if node.MaxDocuments > 0 {
		node.Logger.Debugf("Using early termination optimization: MaxDocuments=%d", node.MaxDocuments)
		// Use limit-aware scanner method for early termination
		if scannerWithLimit, ok := node.DocumentScanner.(interface {
			ScanAllDocumentsWithLimit(int) (*documentscanner.ScanResult, error)
		}); ok {
			scanResult, err = scannerWithLimit.ScanAllDocumentsWithLimit(node.MaxDocuments)
		} else {
			// Fallback to regular scan if method not available
			scanResult, err = node.DocumentScanner.ScanAllDocuments()
		}
	} else {
		scanResult, err = node.DocumentScanner.ScanAllDocuments()
	}
	if err != nil {
		return nil, fmt.Errorf("document scanner failed: %w", err)
	}

	// Convert scanner result to the expected map format
	// Pre-allocate map with known capacity to avoid rehashing during insertion
	results = make(map[string]*models.Document, len(scanResult.Documents))
	docCount := 0

	for i, doc := range scanResult.Documents {
		// Check context every 1000 documents
		// TODO: I can make this check frequency adaptive based on document processing rate
		if docCount%1000 == 0 {
			select {
			case <-ctx.Done():
				// Return partial results on timeout
				return results, ctx.Err()
			default:
				// Continue processing
			}
		}

		// Memory tracking: Sample every 500th document
		if memoryTracker != nil && i%500 == 0 {
			docSize := models.EstimateDocumentSize(doc, nil)
			if err := memoryTracker.Sample(docSize, i); err != nil {
				return nil, err
			}

			// Check if projected memory exceeds limit.
			// When ScanAllDocumentsWithLimit was used, scanResult.Documents is already capped;
			// totalDocs is the count we will process for this execution (Issue 5).
			totalDocs := len(scanResult.Documents)
			if memoryTracker.WillExceedLimit(totalDocs) {
				errorMsg := memoryTracker.FormatErrorMessage(totalDocs)
				node.Logger.Warnf("Memory limit exceeded during scanner full scan: %s", errorMsg)
				return nil, ErrMemoryLimitExceeded
			}
		}

		docID := scanResult.DocumentIDs[i]
		results[docID] = doc
		docCount++
	}

	node.Logger.Debugf("Document scanner completed full scan: %d documents in %v (batches: %d, cache hits: %d)",
		len(results), scanResult.ScanLatency, scanResult.BatchesUsed, scanResult.CacheHits)

	return results, nil
}

// ExecuteSlice returns scan results as a slice, avoiding the O(n) map insertion overhead.
// This is used by FilterNode and other consumers that iterate linearly over results.
func (node *FullScanNode) ExecuteSlice(ctx context.Context) ([]*models.Document, []string, error) {
	node.Logger.Debugf("Executing slice-based full bundle scan on %s", node.Bundle.Name)

	// Get memory tracker from context for per-query memory limit
	memoryTracker := GetMemoryTrackerFromContext(ctx)

	// PHASE 4: MVCC - Set snapshot on scanner if available in context
	if snapshotInfo := GetSnapshotInfoFromContext(ctx); snapshotInfo != nil {
		if smartScanner, ok := node.DocumentScanner.(interface {
			SetSnapshot(snapshotSeq uint64, txID uint64, activeTxIDs map[uint64]bool)
		}); ok {
			smartScanner.SetSnapshot(snapshotInfo.SnapshotSequence, snapshotInfo.TransactionID, snapshotInfo.ActiveTxIDs)
		}
	}

	// PROJECTION: Establish projection for this scan
	if node.BundleServiceInt != nil {
		node.BundleServiceInt.SetProjectionFieldsForBundle(node.Bundle.Name, node.ProjectionFields)
		defer node.BundleServiceInt.SetProjectionFieldsForBundle(node.Bundle.Name, nil)
	}
	if smartScanner, ok := node.DocumentScanner.(interface {
		GetBundle() documentscanner.BundleInterface
	}); ok {
		if bundleAdapter, ok := smartScanner.GetBundle().(*documentscanner.BundleAdapter); ok {
			bundleAdapter.SetProjectionFields(node.ProjectionFields)
			defer bundleAdapter.SetProjectionFields(nil)
		}
	}

	if node.DocumentScanner == nil {
		return nil, nil, fmt.Errorf("document scanner is required for paginated document scanning")
	}

	var scanResult *documentscanner.ScanResult
	var err error
	// PREDICATE PUSHDOWN: When a predicate is set, use ScanWithPredicate to filter
	// during page iteration. This avoids copying non-matching documents entirely.
	if node.Predicate != nil {
		scanResult, err = node.DocumentScanner.ScanWithPredicate(node.Predicate)
	} else if node.MaxDocuments > 0 {
		if scannerWithLimit, ok := node.DocumentScanner.(interface {
			ScanAllDocumentsWithLimit(int) (*documentscanner.ScanResult, error)
		}); ok {
			scanResult, err = scannerWithLimit.ScanAllDocumentsWithLimit(node.MaxDocuments)
		} else {
			scanResult, err = node.DocumentScanner.ScanAllDocuments()
		}
	} else {
		scanResult, err = node.DocumentScanner.ScanAllDocuments()
	}
	if err != nil {
		return nil, nil, fmt.Errorf("document scanner failed: %w", err)
	}

	// Memory tracking on sampled documents
	if memoryTracker != nil {
		for i, doc := range scanResult.Documents {
			if i%500 == 0 {
				docSize := models.EstimateDocumentSize(doc, nil)
				if err := memoryTracker.Sample(docSize, i); err != nil {
					return nil, nil, err
				}
				totalDocs := len(scanResult.Documents)
				if memoryTracker.WillExceedLimit(totalDocs) {
					return nil, nil, ErrMemoryLimitExceeded
				}
			}
		}
	}

	node.Logger.Debugf("Slice-based scan completed: %d documents in %v",
		len(scanResult.Documents), scanResult.ScanLatency)

	return scanResult.Documents, scanResult.DocumentIDs, nil
}

// ExecuteStreaming processes documents in batches using the document scanner
// This approach is memory-efficient for very large bundles
func (node *FullScanNode) ExecuteStreaming(callback func(map[string]*models.Document) error) error {
	node.Logger.Debugf("Executing streaming scan on bundle %s", node.Bundle.Name)

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

	node.Logger.Debugf("Streaming scan completed: %d documents processed in %d batches",
		len(documents), scanResult.BatchesUsed)

	return nil
}

func (node *FilterNode) Execute(ctx context.Context) (map[string]*models.Document, error) {
	// Get settings for optimization configuration
	args := settings.GetSettings()

	// Prepare expression and subquery context (shared by both map and slice paths)
	var subqueryContext syndrQL.SubqueryExecutionContext
	var err error
	if node.WhereExpression != nil && node.SubqueryExecutor != nil && node.Database != nil {
		if expr, ok := node.WhereExpression.(syndrQL.Expression); ok {
			subqueryContext, err = DetectAndExecuteSubqueries(ctx, expr, node.Database, node.SubqueryExecutor, node.Logger)
			if err != nil {
				node.Logger.Errorf("Subquery execution failed: %v", err)
				return nil, fmt.Errorf("subquery execution failed: %w", err)
			}
			if len(subqueryContext) > 0 {
				node.Logger.Debugf("Executed %d subqueries before WHERE evaluation", len(subqueryContext))
			}
		}
	}

	var exprToUse syndrQL.Expression
	if node.WhereExpression != nil {
		if expr, ok := node.WhereExpression.(syndrQL.Expression); ok {
			exprToUse = expr
		}
	}
	if args.WhereExpressionCacheEnabled && node.QueryCache != nil && exprToUse != nil {
		compiled, err := node.QueryCache.GetOrCompileExpression(exprToUse)
		if err != nil {
			node.Logger.Warnf("Query cache compilation failed, using original expression: %v", err)
		} else {
			exprToUse = compiled.AST
			if compiled.Optimized {
				node.Logger.Debugf("Using reordered predicates (selectivity: %.4f, fields: %v)",
					compiled.Selectivity, compiled.FieldRefs)
			}
		}
	}

	evaluator := syndrQL.NewExpressionEvaluatorWithSIMD(node.Logger, args.WhereSIMDEnabled)

	var bundleCtx *syndrQL.BundleContext
	if node.BundleContext != nil {
		if bc, ok := node.BundleContext.(*syndrQL.BundleContext); ok {
			bundleCtx = bc
		} else {
			node.Logger.Errorf("BundleContext is not a *syndrQL.BundleContext: %T", node.BundleContext)
		}
	}

	// OPTIMIZATION: Use slice-based execution path when child supports it.
	// This avoids the O(n) map insertion in FullScanNode.Execute() for scan-heavy queries.
	if sliceChild, ok := node.Child.(SliceExecutionNode); ok {
		docSlice, docIDs, err := sliceChild.ExecuteSlice(ctx)
		if err != nil {
			return nil, err
		}

		// SIMD BATCH OPTIMIZATION: Try batch evaluation on simple predicates before scalar loop.
		// This activates SIMD-accelerated int64/float64/string comparisons and LIKE patterns.
		if args.WhereBatchSIMDEnabled && len(docSlice) >= 100 && exprToUse != nil {
			var schema *models.BundleFieldSchema
			if bundleCtx != nil {
				if b, exists := bundleCtx.Bundles[bundleCtx.PrimaryBundle]; exists && b != nil {
					schema = b.DocumentStructure.FieldSchema()
				}
			}

			// Try simple comparison predicate (==, !=, >, <, >=, <=)
			if pred := extractSimplePredicate(exprToUse); pred != nil {
				batchEval := NewBatchWhereEvaluator(100, node.Logger)
				resultDocs, resultIDs, used := batchEval.EvaluateBatchSlice(docSlice, docIDs, *pred, schema)
				if used {
					node.Logger.Debugf("Batch SIMD (slice path): %d → %d documents", len(docSlice), len(resultDocs))
					filtered := make(map[string]*models.Document, len(resultDocs))
					for i, doc := range resultDocs {
						filtered[resultIDs[i]] = doc
					}
					return filtered, nil
				}
			}

			// Try LIKE/CONTAINS predicate
			if pred := extractLikePredicate(exprToUse); pred != nil {
				batchEval := NewBatchWhereEvaluator(100, node.Logger)
				resultDocs, resultIDs, used := batchEval.EvaluateLikeBatchSlice(docSlice, docIDs, *pred, schema)
				if used {
					node.Logger.Debugf("Batch SIMD LIKE (slice path): %d → %d documents", len(docSlice), len(resultDocs))
					filtered := make(map[string]*models.Document, len(resultDocs))
					for i, doc := range resultDocs {
						filtered[resultIDs[i]] = doc
					}
					return filtered, nil
				}
			}

			// Try compound AND/OR predicate with bitmap combination
			if compound := extractCompoundPredicate(exprToUse); compound != nil {
				batchEval := NewBatchWhereEvaluator(100, node.Logger)
				resultDocs, resultIDs, used := batchEval.EvaluateCompoundBatchSlice(docSlice, docIDs, *compound, schema)
				if used {
					node.Logger.Debugf("Batch SIMD compound (slice path): %d → %d documents", len(docSlice), len(resultDocs))
					filtered := make(map[string]*models.Document, len(resultDocs))
					for i, doc := range resultDocs {
						filtered[resultIDs[i]] = doc
					}
					return filtered, nil
				}
			}
		}

		filtered := make(map[string]*models.Document, len(docSlice)/4) // Estimate 25% selectivity
		memoryTracker := GetMemoryTrackerFromContext(ctx)
		docCount := 0
		matchCount := 0

		for i, doc := range docSlice {
			if node.MaxDocuments > 0 && matchCount >= node.MaxDocuments {
				node.Logger.Debugf("Early termination: reached LIMIT of %d matching documents", node.MaxDocuments)
				break
			}

			if docCount%1000 == 0 {
				select {
				case <-ctx.Done():
					return filtered, ctx.Err()
				default:
				}
			}

			if memoryTracker != nil && docCount%100 == 0 {
				docSize := models.EstimateDocumentSize(doc, nil)
				if err := memoryTracker.Sample(docSize, docCount); err != nil {
					return nil, err
				}
				if memoryTracker.WillExceedLimit(len(docSlice)) {
					return nil, ErrMemoryLimitExceeded
				}
			}

			if node.matchesConditions(doc, subqueryContext, exprToUse, evaluator, bundleCtx) {
				filtered[docIDs[i]] = doc
				matchCount++
			}
			docCount++
		}

		node.Logger.Debugf("Filter node (slice path) reduced %d documents to %d", len(docSlice), len(filtered))
		return filtered, nil
	}

	// Fallback: map-based execution path
	documents, err := node.Child.Execute(ctx)
	if err != nil {
		return nil, err
	}

	// SIMD BATCH OPTIMIZATION (map path): Try batch evaluation on simple predicates before scalar loop.
	if args.WhereBatchSIMDEnabled && len(documents) >= 100 && exprToUse != nil {
		var schema *models.BundleFieldSchema
		if bundleCtx != nil {
			if b, exists := bundleCtx.Bundles[bundleCtx.PrimaryBundle]; exists && b != nil {
				schema = b.DocumentStructure.FieldSchema()
			}
		}

		if pred := extractSimplePredicate(exprToUse); pred != nil {
			batchEval := NewBatchWhereEvaluator(100, node.Logger)
			result, used := batchEval.EvaluateBatch(documents, *pred, schema)
			if used {
				node.Logger.Debugf("Batch SIMD (map path): %d → %d documents", len(documents), len(result))
				return result, nil
			}
		}

		if pred := extractLikePredicate(exprToUse); pred != nil {
			batchEval := NewBatchWhereEvaluator(100, node.Logger)
			result, used := batchEval.EvaluateLikeBatch(documents, *pred, schema)
			if used {
				node.Logger.Debugf("Batch SIMD LIKE (map path): %d → %d documents", len(documents), len(result))
				return result, nil
			}
		}
	}

	// PRIORITY 2: Bloom filter pre-filtering for multi-condition AND queries (cheapest, run first)
	if args.WhereBloomEnabled && len(documents) >= args.WhereBloomMinDocuments && exprToUse != nil {
		bloomOpt := NewWhereBloomOptimizer(args.WhereBloomMinDocuments, 0.01, node.Logger)
		if bloomOpt.ShouldUseBloom(len(documents), exprToUse) {
			bloom, selectivePred, err := bloomOpt.BuildBloomForMostSelective(
				documents,
				exprToUse,
				node.BundleContext,
			)
			if err == nil && bloom != nil {
				originalCount := len(documents)
				documents = bloomOpt.PrefilterWithBloom(documents, bloom)
				node.Logger.Debugf("Bloom filter pre-filter: %d → %d documents (%.1f%% reduction) on condition: %s %s %v",
					originalCount,
					len(documents),
					(1.0-float64(len(documents))/float64(originalCount))*100,
					selectivePred.FieldName,
					selectivePred.Operator,
					selectivePred.Value,
				)
			}
		}
	}

	// Apply full WHERE expression to remaining documents (post-optimization or all if skipped)
	filtered := make(map[string]*models.Document)
	memoryTracker := GetMemoryTrackerFromContext(ctx)
	docCount := 0
	matchCount := 0 // Track matching documents for early termination
	for docID, doc := range documents {
		if node.MaxDocuments > 0 && matchCount >= node.MaxDocuments {
			node.Logger.Debugf("Early termination: reached LIMIT of %d matching documents", node.MaxDocuments)
			break
		}

		if docCount%1000 == 0 {
			select {
			case <-ctx.Done():
				return filtered, ctx.Err()
			default:
			}
		}

		if memoryTracker != nil && docCount%100 == 0 {
			docSize := models.EstimateDocumentSize(doc, nil)
			if err := memoryTracker.Sample(docSize, docCount); err != nil {
				return nil, err
			}
			if memoryTracker.WillExceedLimit(len(documents)) {
				errorMsg := memoryTracker.FormatErrorMessage(len(documents))
				node.Logger.Warnf("Memory limit exceeded during filter: %s", errorMsg)
				return nil, ErrMemoryLimitExceeded
			}
		}

		if node.matchesConditions(doc, subqueryContext, exprToUse, evaluator, bundleCtx) {
			filtered[docID] = doc
			matchCount++
		}
		docCount++
	}

	node.Logger.Debugf("Filter node reduced %d documents to %d (scanned: %d)", len(documents), len(filtered), docCount)
	return filtered, nil
}

// matchesConditions evaluates the given expression against the document using the pre-created evaluator.
// expr is the expression to use for this evaluation (e.g. cache-optimized); when nil, all documents match.
// evaluator is the pre-created ExpressionEvaluator (created once in Execute, reused for all documents).
// bundleCtx is the pre-resolved BundleContext (created once in Execute, reused for all documents).
// Callers must not mutate node.WhereExpression; pass a local exprToUse so the same plan is safe for reuse (Issue 2).
func (node *FilterNode) matchesConditions(doc *models.Document, subqueryContext syndrQL.SubqueryExecutionContext, expr syndrQL.Expression, evaluator *syndrQL.ExpressionEvaluator, bundleCtx *syndrQL.BundleContext) bool {
	if expr == nil {
		return true
	}

	result, err := evaluator.EvaluateAsBool(expr, doc, bundleCtx, subqueryContext, nil)
	if err != nil {
		node.Logger.Errorf("Expression evaluation failed: %v", err)
		return false
	}

	return result
}

func (node *UnionNode) Execute(ctx context.Context) (map[string]*models.Document, error) {
	allResults := make(map[string]*models.Document)

	for _, child := range node.Children {
		results, err := child.Execute(ctx)
		if err != nil {
			return nil, err
		}

		// Merge results (union automatically deduplicates by document ID)
		for docID, doc := range results {
			allResults[docID] = doc
		}
	}

	node.Logger.Debugf("Union node combined %d children into %d results", len(node.Children), len(allResults))
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

// Execute performs a BRIN index scan: queries the BRIN index to find matching page ranges,
// then loads only documents from those pages. Skips ranges whose min/max doesn't overlap the query.
func (node *BRINScanNode) Execute(ctx context.Context) (map[string]*models.Document, error) {
	node.Logger.Debugf("Executing BRIN scan on index '%s' field '%s' operator '%s'",
		node.IndexName, node.FieldName, node.Operator)

	// Load BRIN index instance from bundle metadata
	indexRef, exists := node.Bundle.Indexes[node.IndexName]
	if !exists {
		return nil, fmt.Errorf("BRIN index '%s' not found in bundle '%s'", node.IndexName, node.Bundle.Name)
	}
	brinIdx, ok := indexRef.IndexInstance.(*brinindex.BRINIndex)
	if !ok {
		return nil, fmt.Errorf("index '%s' is not a BRIN index (type: %T)", node.IndexName, indexRef.IndexInstance)
	}

	// Get matching page ranges from BRIN index
	var pageRanges []brinindex.PageRange
	switch node.Operator {
	case "BETWEEN":
		pageRanges = brinIdx.ScanRanges(node.SearchValue, node.SearchValueEnd)
	case ">", ">=":
		pageRanges = brinIdx.ScanRanges(node.SearchValue, nil)
	case "<", "<=":
		pageRanges = brinIdx.ScanRanges(nil, node.SearchValue)
	case "=":
		pageRanges = brinIdx.ScanRanges(node.SearchValue, node.SearchValue)
	default:
		pageRanges = brinIdx.AllRanges()
	}

	node.Logger.Debugf("BRIN scan: %d ranges match (out of %d total)",
		len(pageRanges), brinIdx.EntryCount())

	// Load documents only from matching page ranges
	results := make(map[string]*models.Document)
	for _, pr := range pageRanges {
		for pageID := pr.StartPageID; pageID <= pr.EndPageID; pageID++ {
			select {
			case <-ctx.Done():
				return results, ctx.Err()
			default:
			}

			docs, err := node.BundleServiceInt.SnapshotPageDocuments(
				node.Bundle.Name, node.Bundle.Database.Name, pageID)
			if err != nil {
				continue // Page may not exist (sparse allocation)
			}
			for i := range docs {
				doc := &docs[i]
				results[doc.DocumentID] = doc
			}
		}
	}

	node.Logger.Debugf("BRIN scan returned %d documents", len(results))
	return results, nil
}

// Execute delegates to the underlying index scan node.
// When the index has INCLUDE fields and covers the query, constructs lightweight documents
// from index data without heap fetch. Falls back to child execution otherwise.
func (node *IndexOnlyScanNode) Execute(ctx context.Context) (map[string]*models.Document, error) {
	node.Logger.Debugf("Executing index-only scan on '%s' (projected fields: %v, include fields: %v)",
		node.IndexName, node.ProjectedFields, node.IndexRef.IncludeFields)

	// If the index has INCLUDE fields, attempt to build documents from index entries directly.
	// This avoids the heap fetch entirely for covered queries.
	// For now, delegate to child since B-tree traversal with INCLUDE data retrieval
	// requires the B-tree instance to expose a method for getting entries with INCLUDE data.
	// The coverage check already ensures this path is chosen only when beneficial.
	return node.Child.Execute(ctx)
}
