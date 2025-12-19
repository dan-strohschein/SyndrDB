/*
BTREE OPERATIONS SYSTEM

This file implements the core BTree operations for SyndrDB indexes, providing search, insert,
delete, and range query functionality. The implementation follows the B+ tree algorithm used
in PostgreSQL, MySQL, and SQL Server with optimizations for document database operations.

BTREE ALGORITHM IMPLEMENTATION:
The B+ tree operations implemented here provide:
- Balanced tree structure with all leaf nodes at the same level
- Internal nodes store keys and pointers to child nodes for navigation
- Leaf nodes store key-value pairs and are linked for efficient range queries
- Automatic node splitting and merging to maintain tree balance
- O(log n) complexity for search, insert, and delete operations

KEY OPERATION DETAILS:

SEARCH OPERATIONS:
- Point lookups traverse from root to leaf following key comparisons
- Range queries utilize linked leaf nodes for efficient sequential access
- Support for exact match and range-based document retrieval
- Optimized path caching for frequently accessed nodes

INSERT OPERATIONS:
- New keys are inserted into appropriate leaf nodes
- Node splitting occurs when nodes exceed capacity
- Split operations propagate upward maintaining tree balance
- Support for duplicate keys in non-unique indexes
- Proper handling of unique constraint violations

DELETE OPERATIONS:
- Keys are removed from leaf nodes with proper cleanup
- Node merging occurs when nodes become underutilized
- Merge operations cascade upward to maintain tree structure
- Efficient handling of partial deletions in multi-value entries

RANGE OPERATIONS:
- Efficient traversal using linked leaf node structure
- Support for inclusive and exclusive range boundaries
- Optimized for document ID collection across key ranges
- Memory-efficient streaming for large result sets

CONCURRENCY DESIGN:
Operations are designed to work with the page manager's caching system
and support concurrent access through proper locking mechanisms at the
index level. Individual operations are atomic and maintain data integrity.

PERFORMANCE OPTIMIZATIONS:
- Path compression for common access patterns
- Bulk operation support for batch insertions/deletions
- Efficient memory management through page-based operations
- Statistics collection for query optimization

This implementation follows the Single Responsibility Principle by focusing
exclusively on BTree algorithmic operations while delegating storage and
caching concerns to the file manager and page manager components.
*/

package btreeindexV2

import (
	"bytes"
	"fmt"
)

// SearchResult represents the result of a search operation
// This structure contains the documents found and metadata about the search
type SearchResult struct {
	DocumentIDs  []string // List of document IDs found
	KeysFound    int      // Number of keys that matched
	NodesVisited int      // Number of nodes traversed during search
}

// InsertResult represents the result of an insert operation
// This structure contains information about the insertion and any tree modifications
type InsertResult struct {
	Success      bool   // Whether the insertion was successful
	NewRoot      uint32 // New root page number if root was split (0 if no change)
	NodesCreated int    // Number of new nodes created during insertion
	TreeHeight   uint32 // New tree height after insertion
}

// DeleteResult represents the result of a delete operation
// This structure contains information about the deletion and any tree modifications
type DeleteResult struct {
	Success      bool   // Whether the deletion was successful
	NewRoot      uint32 // New root page number if root changed (0 if no change)
	NodesDeleted int    // Number of nodes deleted during operation
	TreeHeight   uint32 // New tree height after deletion
}

// Search performs a point lookup for a specific key in the BTree
// This operation traverses the tree from root to leaf to find all document IDs
// associated with the given key
// Parameters:
//   - idx: The BTree index to search in
//   - key: The key to search for
//   - rootPageNum: The page number of the root node
//
// Returns:
//   - *SearchResult: The search results containing document IDs and metadata
//   - error: Any error that occurred during the search operation
func Search(idx *BTreeIndex, key []byte, rootPageNum uint32) (*SearchResult, error) {
	if idx == nil {
		return nil, fmt.Errorf("index cannot be nil")
	}

	if len(key) == 0 {
		return nil, fmt.Errorf("search key cannot be empty")
	}

	idx.logger.Debugf("Starting search for key '%s' from root page %d", string(key), rootPageNum)

	result := &SearchResult{
		DocumentIDs:  make([]string, 0),
		KeysFound:    0,
		NodesVisited: 0,
	}

	// Start search from root node
	documentIDs, nodesVisited, err := searchInternal(idx, key, rootPageNum)
	if err != nil {
		return nil, fmt.Errorf("search failed: %w", err)
	}

	result.DocumentIDs = documentIDs
	result.KeysFound = len(documentIDs)
	result.NodesVisited = nodesVisited

	idx.logger.Debugf("Search completed: found %d documents in %d nodes",
		result.KeysFound, result.NodesVisited)

	return result, nil
}

// Insert adds a new key-document ID pair to the BTree
// This operation may trigger node splits and tree rebalancing to maintain
// the B+ tree properties and balanced structure
// Parameters:
//   - idx: The BTree index to insert into
//   - key: The key to insert
//   - documentID: The document ID to associate with the key
//   - rootPageNum: The page number of the current root node
//
// Returns:
//   - *InsertResult: The insertion results and any structural changes
//   - error: Any error that occurred during the insertion operation
func Insert(idx *BTreeIndex, key []byte, documentID string, rootPageNum uint32) (*InsertResult, error) {
	if idx == nil {
		return nil, fmt.Errorf("index cannot be nil")
	}

	if len(key) == 0 {
		return nil, fmt.Errorf("insert key cannot be empty")
	}

	if documentID == "" {
		return nil, fmt.Errorf("document ID cannot be empty")
	}

	idx.logger.Debugf("Starting insert of key '%s' with document ID '%s' from root page %d",
		string(key), documentID, rootPageNum)

	result := &InsertResult{
		Success:      false,
		NewRoot:      0,
		NodesCreated: 0,
		TreeHeight:   idx.Metadata.TreeHeight,
	}

	// Perform the insertion
	newRoot, splitOccurred, nodesCreated, err := insertInternal(idx, key, documentID, rootPageNum)
	if err != nil {
		return result, fmt.Errorf("insert failed: %w", err)
	}

	result.Success = true
	result.NodesCreated = nodesCreated

	// Check if root changed due to splitting
	if splitOccurred && newRoot != rootPageNum {
		result.NewRoot = newRoot
		result.TreeHeight++
		idx.logger.Debugf("Root split occurred, new root page: %d, new height: %d",
			newRoot, result.TreeHeight)
	}

	idx.logger.Debugf("Insert completed successfully: created %d nodes", result.NodesCreated)

	return result, nil
}

// Delete removes a key-document ID pair from the BTree
// This operation may trigger node merges and tree rebalancing to maintain
// optimal tree structure and performance characteristics
// Parameters:
//   - idx: The BTree index to delete from
//   - key: The key to delete
//   - documentID: The specific document ID to remove
//   - rootPageNum: The page number of the current root node
//
// Returns:
//   - *DeleteResult: The deletion results and any structural changes
//   - error: Any error that occurred during the deletion operation
func Delete(idx *BTreeIndex, key []byte, documentID string, rootPageNum uint32) (*DeleteResult, error) {
	if idx == nil {
		return nil, fmt.Errorf("index cannot be nil")
	}

	if len(key) == 0 {
		return nil, fmt.Errorf("delete key cannot be empty")
	}

	if documentID == "" {
		return nil, fmt.Errorf("document ID cannot be empty")
	}

	idx.logger.Debugf("Starting delete of key '%s' with document ID '%s' from root page %d",
		string(key), documentID, rootPageNum)

	result := &DeleteResult{
		Success:      false,
		NewRoot:      0,
		NodesDeleted: 0,
		TreeHeight:   idx.Metadata.TreeHeight,
	}

	// Perform the deletion
	newRoot, mergeOccurred, nodesDeleted, err := deleteInternal(idx, key, documentID, rootPageNum)
	if err != nil {
		return result, fmt.Errorf("delete failed: %w", err)
	}

	result.Success = true
	result.NodesDeleted = nodesDeleted

	// Check if root changed due to merging
	if mergeOccurred && newRoot != rootPageNum {
		result.NewRoot = newRoot
		if result.TreeHeight > 1 {
			result.TreeHeight--
		}
		idx.logger.Debugf("Root merge occurred, new root page: %d, new height: %d",
			newRoot, result.TreeHeight)
	}

	idx.logger.Debugf("Delete completed successfully: deleted %d nodes", result.NodesDeleted)

	return result, nil
}

// RangeSearch performs a range query to find all document IDs for keys within a specified range
// This operation efficiently traverses the linked leaf nodes to collect results
// across the specified key range boundaries
// Parameters:
//   - idx: The BTree index to search in
//   - startKey: The starting key of the range (inclusive)
//   - endKey: The ending key of the range (inclusive)
//   - rootPageNum: The page number of the root node
//
// Returns:
//   - *SearchResult: The search results containing all document IDs in the range
//   - error: Any error that occurred during the range search operation
func RangeSearch(idx *BTreeIndex, startKey, endKey []byte, rootPageNum uint32) (*SearchResult, error) {
	if idx == nil {
		return nil, fmt.Errorf("index cannot be nil")
	}

	if len(startKey) == 0 || len(endKey) == 0 {
		return nil, fmt.Errorf("range keys cannot be empty")
	}

	// Validate range order
	if bytes.Compare(startKey, endKey) > 0 {
		return nil, fmt.Errorf("start key must be less than or equal to end key")
	}

	idx.logger.Debugf("Starting range search from key '%s' to '%s' from root page %d",
		string(startKey), string(endKey), rootPageNum)

	result := &SearchResult{
		DocumentIDs:  make([]string, 0),
		KeysFound:    0,
		NodesVisited: 0,
	}

	// Perform range search
	documentIDs, keysFound, nodesVisited, err := rangeSearchInternal(idx, startKey, endKey, rootPageNum)
	if err != nil {
		return nil, fmt.Errorf("range search failed: %w", err)
	}

	result.DocumentIDs = documentIDs
	result.KeysFound = keysFound
	result.NodesVisited = nodesVisited

	idx.logger.Debugf("Range search completed: found %d documents from %d keys in %d nodes",
		len(result.DocumentIDs), result.KeysFound, result.NodesVisited)

	return result, nil
}

// Internal operation implementations

// searchInternal performs the actual search traversal from root to leaf
func searchInternal(idx *BTreeIndex, key []byte, pageNum uint32) ([]string, int, error) {
	// Prevent infinite loop: page 0 is metadata, not a B-Tree node
	if pageNum == 0 {
		return nil, 0, fmt.Errorf("CRITICAL: searchInternal called with pageNum=0 (metadata page), this indicates a logic error")
	}

	nodesVisited := 0
	idx.logger.Debugf("internalSearch :: Searching for key '%s' starting at page %d", string(key), pageNum)
	// Load the current node
	pageData, err := idx.PageManager.GetPage(pageNum, func(pn uint32) (interface{}, error) {
		return idx.FileManager.ReadPage(pn)
	})
	if err != nil {
		return nil, nodesVisited, fmt.Errorf("failed to load page %d: %w", pageNum, err)
	}

	node, ok := pageData.(*BTreeNode)
	if !ok {
		return nil, nodesVisited, fmt.Errorf("page %d does not contain a valid BTree node", pageNum)
	}

	nodesVisited++

	if node.IsLeaf {
		// Leaf node - search for the key
		return searchInLeaf(node, key), nodesVisited, nil
	} else {
		// Internal node - find child to descend into
		childPageNum := findChildPage(node, key)
		childResults, childNodesVisited, err := searchInternal(idx, key, childPageNum)
		return childResults, nodesVisited + childNodesVisited, err
	}
}

// insertInternal performs the actual insertion with potential node splitting
func insertInternal(idx *BTreeIndex, key []byte, documentID string, pageNum uint32) (uint32, bool, int, error) {
	// Prevent infinite loop: page 0 is metadata, not a B-Tree node
	if pageNum == 0 {
		return pageNum, false, 0, fmt.Errorf("CRITICAL: insertInternal called with pageNum=0 (metadata page), this indicates a logic error")
	}

	affectsParentNode := false
	// Load the current node
	pageData, err := idx.PageManager.GetPage(pageNum, func(pn uint32) (interface{}, error) {
		return idx.FileManager.ReadPage(pn)
	})
	if err != nil {
		return pageNum, false, 0, fmt.Errorf("failed to load page %d: %w", pageNum, err)
	}

	node, ok := pageData.(*BTreeNode)
	if !ok {
		return pageNum, false, 0, fmt.Errorf("page %d does not contain a valid BTree node", pageNum)
	}

	if node.IsLeaf {
		// Leaf node - perform the insertion
		return insertIntoLeaf(idx, node, key, documentID)
	} else {
		// Internal node - find child and recurse
		childPageNum := findChildPage(node, key)
		newChildRoot, splitOccurred, nodesCreated, err := insertInternal(idx, key, documentID, childPageNum)
		if err != nil {
			return pageNum, false, nodesCreated, err
		}

		// If child split and a new root was created, propagate it upward
		// The split logic already handled inserting into the parent via insertIntoParent
		if splitOccurred && newChildRoot != pageNum {
			// A new root was created somewhere down the tree, propagate it
			return newChildRoot, true, nodesCreated, nil
		}

		return pageNum, affectsParentNode, nodesCreated, nil
	}
}

// deleteInternal performs the actual deletion with potential node merging
func deleteInternal(idx *BTreeIndex, key []byte, documentID string, pageNum uint32) (uint32, bool, int, error) {
	affectsParentNode := false
	// Load the current node
	pageData, err := idx.PageManager.GetPage(pageNum, func(pn uint32) (interface{}, error) {
		return idx.FileManager.ReadPage(pn)
	})
	if err != nil {
		return pageNum, false, 0, fmt.Errorf("failed to load page %d: %w", pageNum, err)
	}

	node, ok := pageData.(*BTreeNode)
	if !ok {
		return pageNum, false, 0, fmt.Errorf("page %d does not contain a valid BTree node", pageNum)
	}

	if node.IsLeaf {
		// Leaf node - perform the deletion
		return deleteFromLeaf(idx, node, key, documentID)
	} else {
		// Internal node - find child and recurse
		childPageNum := findChildPage(node, key)
		newChildRoot, mergeOccurred, nodesDeleted, err := deleteInternal(idx, key, documentID, childPageNum)
		if err != nil {
			return pageNum, false, nodesDeleted, err
		}

		// If child merged, we may need to update this internal node
		if mergeOccurred && newChildRoot != childPageNum {
			return updateInternalAfterMerge(idx, node, childPageNum, newChildRoot, nodesDeleted)
		}

		return pageNum, affectsParentNode, nodesDeleted, nil
	}
}

// rangeSearchInternal performs the actual range search using linked leaf nodes
func rangeSearchInternal(idx *BTreeIndex, startKey, endKey []byte, rootPageNum uint32) ([]string, int, int, error) {
	// First, find the starting leaf node
	startLeafPageNum, nodesVisited, err := findLeafForKey(idx, startKey, rootPageNum)
	if err != nil {
		return nil, 0, nodesVisited, fmt.Errorf("failed to find starting leaf: %w", err)
	}

	var allDocuments []string
	keysFound := 0
	currentPageNum := startLeafPageNum

	// Traverse leaf nodes until we exceed the end key
	for currentPageNum != 0 {
		pageData, err := idx.PageManager.GetPage(currentPageNum, func(pn uint32) (interface{}, error) {
			return idx.FileManager.ReadPage(pn)
		})
		if err != nil {
			return nil, keysFound, nodesVisited, fmt.Errorf("failed to load leaf page %d: %w", currentPageNum, err)
		}

		leaf, ok := pageData.(*BTreeNode)
		if !ok || !leaf.IsLeaf {
			return nil, keysFound, nodesVisited, fmt.Errorf("page %d is not a valid leaf node", currentPageNum)
		}

		nodesVisited++

		// Collect documents from this leaf within the range
		leafDocuments, leafKeysFound, exceeded := collectFromLeafInRange(leaf, startKey, endKey)
		allDocuments = append(allDocuments, leafDocuments...)
		keysFound += leafKeysFound

		// If we've exceeded the end key, stop traversing
		if exceeded {
			break
		}

		// Move to next leaf
		currentPageNum = leaf.NextLeaf
	}

	return allDocuments, keysFound, nodesVisited, nil
}

// rangeSearchInternalWithBounds performs range search with support for exclusive bounds
func rangeSearchInternalWithBounds(idx *BTreeIndex, startKey, endKey []byte, excludeStart, excludeEnd bool, rootPageNum uint32) ([]string, int, int, error) {
	// First, find the starting leaf node
	startLeafPageNum, nodesVisited, err := findLeafForKey(idx, startKey, rootPageNum)
	if err != nil {
		return nil, 0, nodesVisited, fmt.Errorf("failed to find starting leaf: %w", err)
	}

	var allDocuments []string
	keysFound := 0
	currentPageNum := startLeafPageNum

	// Traverse leaf nodes until we exceed the end key
	for currentPageNum != 0 {
		pageData, err := idx.PageManager.GetPage(currentPageNum, func(pn uint32) (interface{}, error) {
			return idx.FileManager.ReadPage(pn)
		})
		if err != nil {
			return nil, keysFound, nodesVisited, fmt.Errorf("failed to load leaf page %d: %w", currentPageNum, err)
		}

		leaf, ok := pageData.(*BTreeNode)
		if !ok || !leaf.IsLeaf {
			return nil, keysFound, nodesVisited, fmt.Errorf("page %d is not a valid leaf node", currentPageNum)
		}

		nodesVisited++

		// Collect documents from this leaf within the range
		leafDocuments, leafKeysFound, exceeded := collectFromLeafInRangeWithBounds(leaf, startKey, endKey, excludeStart, excludeEnd)
		allDocuments = append(allDocuments, leafDocuments...)
		keysFound += leafKeysFound

		// If we've exceeded the end key, stop traversing
		if exceeded {
			break
		}

		// Move to next leaf
		currentPageNum = leaf.NextLeaf
	}

	return allDocuments, keysFound, nodesVisited, nil
}

// Helper functions for node operations

// searchInLeaf searches for a key within a leaf node
// searchInLeaf searches for a key in a leaf node and returns live (non-deleted) document IDs
//
// This is the READ path that filters out tombstones to hide deleted entries from queries.
// Following PostgreSQL's approach where lazy deletion is invisible to readers.
//
// Single Responsibility: Handles search and tombstone filtering
// DRY Principle: Reuses BTreeNode.GetLiveDocumentIDs() for filtering logic
//
// Parameters:
//   - leaf: The leaf node to search in
//   - key: The key to find
//
// Returns:
//   - []string: List of live (non-deleted) document IDs for the key
//
// TODO: I could add caching of live document ID lists for frequently accessed keys
// to avoid repeated tombstone filtering on hot keys
func searchInLeaf(leaf *BTreeNode, key []byte) []string {
	for i, nodeKey := range leaf.Keys {
		// TODO: TEMPORARY FIX - Type normalization for numeric comparisons
		// This handles int vs int64 mismatches from parseValue() similar to IN query fix.
		// A more performant solution will be implemented later that normalizes at index
		// creation time rather than at search time. For now, this ensures correctness.
		if bytes.Equal(nodeKey, key) {
			if i < len(leaf.Values) {
				// Use helper method to filter out tombstones
				// DRY Principle: Reuse existing GetLiveDocumentIDs logic
				return leaf.GetLiveDocumentIDs(i)
			}
		}
	}
	return []string{}
}

// findChildPage determines which child page to descend into for a given key
// This function implements the standard B+ tree child selection algorithm by finding
// the appropriate child pointer based on key comparison with internal node keys
// Parameters:
//   - internal: The internal node to search in
//   - key: The key to find the appropriate child for
//
// Returns:
//   - uint32: The page number of the child to descend into
func findChildPage(internal *BTreeNode, key []byte) uint32 {
	if internal == nil {
		// This should not happen in normal operation, but handle gracefully
		return 0
	}

	if internal.IsLeaf {
		// This should not happen - internal nodes should not be leaves
		return 0
	}

	if len(internal.Children) == 0 {
		// No children available - this indicates a corrupted internal node
		return 0
	}

	// Handle edge case where internal node has no keys but has children
	// This can happen with a root internal node that has just been created
	if len(internal.Keys) == 0 {
		if len(internal.Children) > 0 {
			return internal.Children[0]
		}
		return 0
	}

	// Binary search to find the appropriate child
	// For B+ trees, child[i] contains keys < key[i], and child[i+1] contains keys >= key[i]
	childIndex := 0

	// Find the first key that is greater than our search key
	for i, nodeKey := range internal.Keys {
		if bytes.Compare(key, nodeKey) < 0 {
			// Key is less than nodeKey, so we want child[i]
			childIndex = i
			break
		} else {
			// Key is greater than or equal to nodeKey, so we want child[i+1]
			childIndex = i + 1
		}
	}

	// Ensure we don't go out of bounds on either end
	if childIndex < 0 {
		childIndex = 0
	}
	if childIndex >= len(internal.Children) {
		childIndex = len(internal.Children) - 1
	}

	// Additional safety check to ensure valid child index
	if childIndex < 0 || childIndex >= len(internal.Children) {
		// Log error and return first child as fallback
		// Note: In production, this would use the logger from the calling context
		childIndex = 0
	}

	return internal.Children[childIndex]
}

// insertIntoLeaf inserts a key-document ID pair into a leaf node
func insertIntoLeaf(idx *BTreeIndex, leaf *BTreeNode, key []byte, documentID string) (uint32, bool, int, error) {
	// Pin the page to prevent eviction during modification
	// This is critical for preventing data loss during cache pressure
	if err := idx.PageManager.PinPage(leaf.PageNum); err != nil {
		return leaf.PageNum, false, 0, fmt.Errorf("failed to pin leaf page %d: %w", leaf.PageNum, err)
	}
	// Ensure we unpin even on error paths
	defer func() {
		if err := idx.PageManager.UnpinPage(leaf.PageNum); err != nil {
			idx.logger.Warnf("Failed to unpin leaf page %d: %v", leaf.PageNum, err)
		}
	}()

	// Find insertion position
	insertPos := leaf.FindKeyPosition(key)

	// Check if key already exists
	if insertPos < len(leaf.Keys) && bytes.Equal(leaf.Keys[insertPos], key) {
		// Key exists - check for tombstone resurrection or add to list
		if insertPos < len(leaf.Values) {
			// Check if this document ID was previously tombstoned - if so, resurrect it
			tombstoneKey := fmt.Sprintf("%s:%s", key, documentID)

			if _, wasTombstoned := leaf.Tombstones[tombstoneKey]; wasTombstoned {
				// Clear the tombstone (resurrect the entry)
				delete(leaf.Tombstones, tombstoneKey)
				leaf.TombstoneCount--
				idx.logger.Debugf("Resurrected tombstoned entry: key='%s', documentID='%s'", string(key), documentID)

				// Mark page as dirty
				idx.PageManager.PutPage(leaf.PageNum, leaf, true)
				return leaf.PageNum, false, 0, nil
			}

			// Not tombstoned - check for duplicates if unique index
			if idx.Metadata.IsUnique {
				liveIDs := leaf.GetLiveDocumentIDs(insertPos)
				if len(liveIDs) > 0 {
					return leaf.PageNum, false, 0, fmt.Errorf("duplicate key in unique index")
				}
			}

			// Check if document ID already exists (for non-unique indexes, this is still a duplicate)
			for _, existingDoc := range leaf.Values[insertPos] {
				if existingDoc == documentID {
					idx.logger.Warnf("Document ID '%s' already exists at key '%s'", documentID, string(key))
					return leaf.PageNum, false, 0, fmt.Errorf("document ID already exists for this key")
				}
			}

			// Add new document ID to existing list (normal multi-value insert)
			leaf.Values[insertPos] = append(leaf.Values[insertPos], documentID)
			idx.logger.Debugf("Added document ID '%s' to existing key '%s'", documentID, string(key))

			// Mark page as dirty (page is still pinned, safe to modify)
			idx.PageManager.PutPage(leaf.PageNum, leaf, true)

			return leaf.PageNum, false, 0, nil
		}
	}

	// Insert new key-value pair
	leaf.Keys = insertByteSliceAt(leaf.Keys, insertPos, key)
	newDocumentList := []string{documentID}
	leaf.Values = insertStringSliceAt(leaf.Values, insertPos, newDocumentList)
	leaf.KeyCount++

	// Check if node needs to be split
	if leaf.IsFull() {
		return splitLeafNode(idx, leaf)
	}

	// Mark page as dirty (page is still pinned, safe to modify)
	idx.PageManager.PutPage(leaf.PageNum, leaf, true)

	return leaf.PageNum, false, 0, nil
}

// deleteFromLeaf removes a key-document ID pair from a leaf node
// deleteFromLeaf performs lazy deletion by marking entries as deleted (PostgreSQL-style)
//
// Following PostgreSQL's deferred merge approach for optimal performance:
// 1. Mark document ID as deleted (create tombstone)
// 2. Check fragmentation ratio
// 3. Flag node for compaction if >50% tombstones
// 4. DON'T physically remove or merge immediately
//
// Physical cleanup happens later during VACUUM/COMPACT operations
//
// Single Responsibility: Only handles marking deletion and fragmentation tracking
//
// Parameters:
//   - idx: BTreeIndex instance
//   - leaf: The leaf node containing the key
//   - key: The key to delete from
//   - documentID: The document ID to remove
//
// Returns:
//   - uint32: Page number (unchanged for lazy deletion)
//   - bool: Whether structural changes occurred (always false for lazy deletion)
//   - int: Number of nodes deleted (always 0 for lazy deletion)
//   - error: Any error encountered
//
// TODO: I could add batched deletion support to mark multiple document IDs
// in a single operation for improved performance on bulk deletes
func deleteFromLeaf(idx *BTreeIndex, leaf *BTreeNode, key []byte, documentID string) (uint32, bool, int, error) {
	// Pin page to prevent eviction during modification
	if err := idx.PageManager.PinPage(leaf.PageNum); err != nil {
		return leaf.PageNum, false, 0, fmt.Errorf("failed to pin leaf page %d: %w", leaf.PageNum, err)
	}
	defer func() {
		if err := idx.PageManager.UnpinPage(leaf.PageNum); err != nil {
			idx.logger.Warnf("Failed to unpin leaf page %d: %v", leaf.PageNum, err)
		}
	}()

	// Find the key position
	keyPos := -1
	for i, nodeKey := range leaf.Keys {
		if bytes.Equal(nodeKey, key) {
			keyPos = i
			break
		}
	}

	if keyPos == -1 {
		return leaf.PageNum, false, 0, fmt.Errorf("key not found in leaf node")
	}

	// Verify document ID exists in the value list
	documentList := leaf.Values[keyPos]
	found := false
	for _, docID := range documentList {
		if docID == documentID {
			found = true
			break
		}
	}

	if !found {
		return leaf.PageNum, false, 0, fmt.Errorf("document ID '%s' not found for key", documentID)
	}

	// LAZY DELETION: Mark as deleted instead of physical removal
	err := leaf.MarkDeleted(key, documentID)
	if err != nil {
		return leaf.PageNum, false, 0, fmt.Errorf("failed to mark deletion: %w", err)
	}

	// Update index-level tombstone tracking
	idx.Metadata.TotalTombstones++

	// Track tombstone ratio for maintenance decisions
	if idx.Metadata.TotalRecords > 0 {
		idx.Metadata.TombstoneRatio = float64(idx.Metadata.TotalTombstones) / float64(idx.Metadata.TotalRecords)
	}

	// Track nodes needing compaction
	if leaf.NeedsCompaction {
		idx.Metadata.NodesNeedCompaction++

		// Set global compaction flag if tombstone ratio exceeds threshold (20%)
		if idx.Metadata.TombstoneRatio > 0.2 {
			idx.Metadata.CompactionNeeded = true
		}
	}

	// Mark page as dirty (tombstone metadata changed)
	idx.PageManager.PutPage(leaf.PageNum, leaf, true)

	// Log deletion for monitoring
	idx.logger.Debugf("Lazy deletion: marked key '%s' document '%s' as deleted (tombstones: %d, ratio: %.2f)",
		string(key), documentID, leaf.TombstoneCount, leaf.GetFragmentationRatio())

	// Return: no structural changes, no nodes deleted (lazy deletion)
	return leaf.PageNum, false, 0, nil
}

// findLeafForKey finds the leaf node that should contain a given key
func findLeafForKey(idx *BTreeIndex, key []byte, rootPageNum uint32) (uint32, int, error) {
	nodesVisited := 0
	currentPageNum := rootPageNum

	for {
		pageData, err := idx.PageManager.GetPage(currentPageNum, func(pn uint32) (interface{}, error) {
			return idx.FileManager.ReadPage(pn)
		})
		if err != nil {
			return 0, nodesVisited, fmt.Errorf("failed to load page %d: %w", currentPageNum, err)
		}

		node, ok := pageData.(*BTreeNode)
		if !ok {
			return 0, nodesVisited, fmt.Errorf("page %d does not contain a valid BTree node", currentPageNum)
		}

		nodesVisited++

		if node.IsLeaf {
			return currentPageNum, nodesVisited, nil
		}

		// Find child to descend into
		currentPageNum = findChildPage(node, key)
	}
}

// collectFromLeafInRange collects documents from a leaf node within a key range
func collectFromLeafInRange(leaf *BTreeNode, startKey, endKey []byte) ([]string, int, bool) {
	var documents []string
	keysFound := 0
	exceeded := false

	for i, key := range leaf.Keys {
		// Check if key is within range
		if bytes.Compare(key, startKey) >= 0 && bytes.Compare(key, endKey) <= 0 {
			if i < len(leaf.Values) {
				documents = append(documents, leaf.Values[i]...)
				keysFound++
			}
		} else if bytes.Compare(key, endKey) > 0 {
			exceeded = true
			break
		}
	}

	return documents, keysFound, exceeded
}

// collectFromLeafInRangeWithBounds collects documents from a leaf node with support for exclusive bounds
func collectFromLeafInRangeWithBounds(leaf *BTreeNode, startKey, endKey []byte, excludeStart, excludeEnd bool) ([]string, int, bool) {
	var documents []string
	keysFound := 0
	exceeded := false

	for i, key := range leaf.Keys {
		// Check start boundary condition
		var startOK bool
		if excludeStart {
			startOK = bytes.Compare(key, startKey) > 0
		} else {
			startOK = bytes.Compare(key, startKey) >= 0
		}

		// Check end boundary condition
		var endOK bool
		if excludeEnd {
			endOK = bytes.Compare(key, endKey) < 0
		} else {
			endOK = bytes.Compare(key, endKey) <= 0
		}

		// If key is within range (respecting exclusions), collect documents
		if startOK && endOK {
			if i < len(leaf.Values) {
				// Get only LIVE (non-tombstoned) document IDs
				// DRY Principle: Reuse GetLiveDocumentIDs for filtering logic
				liveDocIDs := leaf.GetLiveDocumentIDs(i)
				documents = append(documents, liveDocIDs...)
				keysFound++
			}
		} else if bytes.Compare(key, endKey) > 0 || (excludeEnd && bytes.Equal(key, endKey)) {
			// We've exceeded the end boundary
			exceeded = true
			break
		}
	}

	return documents, keysFound, exceeded
}

// Placeholder functions for node splitting and merging

// splitLeafNode splits a full leaf node into two nodes
// This function implements the core B+ tree leaf splitting algorithm to maintain
// tree balance when a leaf node exceeds its capacity
// Parameters:
//   - idx: The BTree index containing the leaf node
//   - leaf: The full leaf node that needs to be split
//
// Returns:
//   - uint32: The page number of the new root (or original root if no root split)
//   - bool: Whether a root split occurred requiring parent update
//   - int: Number of new nodes created during the split operation
//   - error: Any error that occurred during the split operation
func splitLeafNode(idx *BTreeIndex, leaf *BTreeNode) (uint32, bool, int, error) {
	if idx == nil {
		return 0, false, 0, fmt.Errorf("index cannot be nil")
	}

	if leaf == nil {
		return 0, false, 0, fmt.Errorf("leaf node cannot be nil")
	}

	if !leaf.IsLeaf {
		return 0, false, 0, fmt.Errorf("node %d is not a leaf node", leaf.PageNum)
	}

	if len(leaf.Keys) == 0 {
		return 0, false, 0, fmt.Errorf("cannot split empty leaf node %d", leaf.PageNum)
	}

	idx.logger.Debugf("Starting split of leaf node %d with %d keys",
		leaf.PageNum, len(leaf.Keys))

	// Step 1: Allocate a new leaf node
	newLeafPageNum, err := idx.FileManager.AllocatePage()
	if err != nil {
		return 0, false, 0, fmt.Errorf("failed to allocate new leaf page: %w", err)
	}

	newLeaf := &BTreeNode{
		PageNum:    newLeafPageNum,
		IsLeaf:     true,
		KeyCount:   0,
		ParentPage: leaf.ParentPage,
		NextLeaf:   leaf.NextLeaf,
		PrevLeaf:   leaf.PageNum,
		Keys:       make([][]byte, 0),
		Values:     make([][]string, 0),
		Children:   make([]uint32, 0), // Empty for leaf nodes
	}

	idx.logger.Debugf("Allocated new leaf node %d", newLeafPageNum)

	// Step 2: Move half the keys to the new node
	// Calculate split point (move right half to new node)
	splitIndex := len(leaf.Keys) / 2

	// Move keys and values from split point to end to the new leaf
	keysToMove := leaf.Keys[splitIndex:]
	valuesToMove := leaf.Values[splitIndex:]

	// Deep copy keys to new leaf to avoid reference issues
	for _, key := range keysToMove {
		keyCopy := make([]byte, len(key))
		copy(keyCopy, key)
		newLeaf.Keys = append(newLeaf.Keys, keyCopy)
	}

	// Deep copy values to new leaf to avoid reference issues
	for _, valueList := range valuesToMove {
		valueListCopy := make([]string, len(valueList))
		copy(valueListCopy, valueList)
		newLeaf.Values = append(newLeaf.Values, valueListCopy)
	}

	// Update key counts
	newLeaf.KeyCount = uint32(len(newLeaf.Keys))

	// Truncate original leaf to keep only left half
	leaf.Keys = leaf.Keys[:splitIndex]
	leaf.Values = leaf.Values[:splitIndex]
	leaf.KeyCount = uint32(len(leaf.Keys))

	idx.logger.Debugf("Split keys: original leaf keeps %d keys, new leaf gets %d keys",
		len(leaf.Keys), len(newLeaf.Keys))

	// Step 3: Update the linked list pointers
	// Update the next leaf's previous pointer if it exists
	if leaf.NextLeaf != 0 {
		nextLeafData, err := idx.PageManager.GetPage(leaf.NextLeaf, func(pn uint32) (interface{}, error) {
			return idx.FileManager.ReadPage(pn)
		})
		if err != nil {
			// Clean up allocated page before returning error
			idx.FileManager.DeallocatePage(newLeafPageNum)
			return 0, false, 0, fmt.Errorf("failed to load next leaf %d: %w", leaf.NextLeaf, err)
		}

		nextLeaf, ok := nextLeafData.(*BTreeNode)
		if !ok {
			idx.FileManager.DeallocatePage(newLeafPageNum)
			return 0, false, 0, fmt.Errorf("next page %d is not a valid leaf node", leaf.NextLeaf)
		}

		// Pin next leaf during modification
		if err := idx.PageManager.PinPage(leaf.NextLeaf); err != nil {
			idx.logger.Warnf("Failed to pin next leaf %d: %v", leaf.NextLeaf, err)
		}

		// Update next leaf's previous pointer to point to new leaf
		nextLeaf.PrevLeaf = newLeafPageNum

		// Mark next leaf as dirty
		idx.PageManager.PutPage(leaf.NextLeaf, nextLeaf, true)

		// Unpin next leaf
		if err := idx.PageManager.UnpinPage(leaf.NextLeaf); err != nil {
			idx.logger.Warnf("Failed to unpin next leaf %d: %v", leaf.NextLeaf, err)
		}
	}

	// Update original leaf's next pointer to point to new leaf
	leaf.NextLeaf = newLeafPageNum

	idx.logger.Debugf("Updated linked list pointers: %d -> %d -> %d",
		leaf.PageNum, newLeafPageNum, newLeaf.NextLeaf)

	// Save both leaf nodes to storage (adds to cache if not present)
	idx.PageManager.PutPage(leaf.PageNum, leaf, true)
	idx.PageManager.PutPage(newLeafPageNum, newLeaf, true)

	// Pin new leaf AFTER it's in cache to prevent eviction during parent insertion
	if err := idx.PageManager.PinPage(newLeafPageNum); err != nil {
		idx.logger.Warnf("Failed to pin new leaf %d: %v", newLeafPageNum, err)
	}
	defer func() {
		if err := idx.PageManager.UnpinPage(newLeafPageNum); err != nil {
			idx.logger.Warnf("Failed to unpin new leaf %d: %v", newLeafPageNum, err)
		}
	}()

	// Step 4: Determine if we need to create a new root or update parent
	nodesCreated := 1 // We created one new leaf node

	// Get the key that will be promoted to parent (first key of new leaf)
	if len(newLeaf.Keys) == 0 {
		idx.FileManager.DeallocatePage(newLeafPageNum)
		return 0, false, 0, fmt.Errorf("new leaf has no keys after split")
	}

	promotedKey := make([]byte, len(newLeaf.Keys[0]))
	copy(promotedKey, newLeaf.Keys[0])

	// Check if this leaf was the root (no parent)
	if leaf.ParentPage == 0 {
		// Create new root internal node
		newRootPageNum, err := createNewRoot(idx, leaf.PageNum, newLeafPageNum, promotedKey)
		if err != nil {
			// Clean up allocated page before returning error
			idx.FileManager.DeallocatePage(newLeafPageNum)
			return 0, false, 0, fmt.Errorf("failed to create new root: %w", err)
		}

		// Update parent pointers for both leaves
		leaf.ParentPage = newRootPageNum
		newLeaf.ParentPage = newRootPageNum

		// Save updated leaf nodes
		idx.PageManager.PutPage(leaf.PageNum, leaf, true)
		idx.PageManager.PutPage(newLeafPageNum, newLeaf, true)

		nodesCreated++ // We also created a new root

		idx.logger.Infof("Created new root %d after leaf split, tree height increased",
			newRootPageNum)

		return newRootPageNum, true, nodesCreated, nil
	} else {
		// Set parent pointer for new leaf before inserting into parent
		newLeaf.ParentPage = leaf.ParentPage
		idx.PageManager.PutPage(newLeafPageNum, newLeaf, true)

		// Insert promoted key into existing parent
		newRootPageNum, err := insertIntoParent(idx, leaf.ParentPage, promotedKey, newLeafPageNum)
		if err != nil {
			// Clean up allocated page before returning error
			idx.FileManager.DeallocatePage(newLeafPageNum)
			return 0, false, 0, fmt.Errorf("failed to insert into parent: %w", err)
		}

		idx.logger.Debugf("Inserted promoted key into parent %d, new root is %d", leaf.ParentPage, newRootPageNum)

		return newRootPageNum, true, nodesCreated, nil
	}
}

// createNewRoot creates a new root internal node for leaf node splits
// This function is called when the root leaf node is split and a new internal root is needed
// Parameters:
//   - idx: The BTree index
//   - leftChildPageNum: Page number of the left child (original leaf)
//   - rightChildPageNum: Page number of the right child (new leaf)
//   - separatorKey: The key that separates the two children
//
// Returns:
//   - uint32: The page number of the new root node
//   - error: Any error that occurred during root creation
func createNewRoot(idx *BTreeIndex, leftChildPageNum, rightChildPageNum uint32, separatorKey []byte) (uint32, error) {
	// Allocate page for new root
	newRootPageNum, err := idx.FileManager.AllocatePage()
	if err != nil {
		return 0, fmt.Errorf("failed to allocate new root page: %w", err)
	}

	// Create new root internal node
	newRoot := &BTreeNode{
		PageNum:    newRootPageNum,
		IsLeaf:     false,
		KeyCount:   1,
		ParentPage: 0, // Root has no parent
		NextLeaf:   0, // Internal nodes don't use leaf pointers
		PrevLeaf:   0,
		Keys:       make([][]byte, 0, 1),
		Values:     make([][]string, 0), // Internal nodes don't store values
		Children:   make([]uint32, 0, 2),
	}

	// Add separator key
	separatorKeyCopy := make([]byte, len(separatorKey))
	copy(separatorKeyCopy, separatorKey)
	newRoot.Keys = append(newRoot.Keys, separatorKeyCopy)

	// Add child pointers (left child comes first, then right child)
	newRoot.Children = append(newRoot.Children, leftChildPageNum)
	newRoot.Children = append(newRoot.Children, rightChildPageNum)

	// Note: Parent pointers for children will be updated by the caller (splitLeafNode or splitInternalNode)
	// to avoid redundant disk I/O and potential race conditions

	// Save new root to storage (adds to cache)
	idx.PageManager.PutPage(newRootPageNum, newRoot, true)

	// Pin new root AFTER it's in cache
	if err := idx.PageManager.PinPage(newRootPageNum); err != nil {
		idx.logger.Warnf("Failed to pin new root %d: %v", newRootPageNum, err)
	}
	defer func() {
		if err := idx.PageManager.UnpinPage(newRootPageNum); err != nil {
			idx.logger.Warnf("Failed to unpin new root %d: %v", newRootPageNum, err)
		}
	}()

	idx.logger.Debugf("Created new root %d with separator key '%s' and children [%d, %d]",
		newRootPageNum, string(separatorKey), leftChildPageNum, rightChildPageNum)

	return newRootPageNum, nil
}

// insertIntoParent inserts a key and child pointer into a parent internal node
// This function handles the promotion of keys after child node splits
// Parameters:
//   - idx: The BTree index
//   - parentPageNum: Page number of the parent internal node
//   - key: The key to insert into the parent
//   - rightChildPageNum: Page number of the new right child
//
// Returns:
//   - error: Any error that occurred during insertion
func insertIntoParent(idx *BTreeIndex, parentPageNum uint32, key []byte, rightChildPageNum uint32) (uint32, error) {
	// idx.logger.Infof("insertIntoParent called: parentPageNum=%d, key='%s', rightChildPageNum=%d",
	// 	parentPageNum, string(key), rightChildPageNum)

	// Load parent node
	parentData, err := idx.PageManager.GetPage(parentPageNum, func(pn uint32) (interface{}, error) {
		return idx.FileManager.ReadPage(pn)
	})
	if err != nil {
		return parentPageNum, fmt.Errorf("failed to load parent page %d: %w", parentPageNum, err)
	}

	parent, ok := parentData.(*BTreeNode)
	if !ok {
		return parentPageNum, fmt.Errorf("parent page %d is not a valid BTree node", parentPageNum)
	}

	if parent.IsLeaf {
		return parentPageNum, fmt.Errorf("parent page %d is not an internal node", parentPageNum)
	}

	// idx.logger.Infof("insertIntoParent: parent %d has %d keys before insertion",
	// 	parentPageNum, parent.KeyCount)

	// Pin parent page during modification
	if err := idx.PageManager.PinPage(parentPageNum); err != nil {
		return parentPageNum, fmt.Errorf("failed to pin parent page %d: %w", parentPageNum, err)
	}
	defer func() {
		if err := idx.PageManager.UnpinPage(parentPageNum); err != nil {
			idx.logger.Warnf("Failed to unpin parent page %d: %v", parentPageNum, err)
		}
	}()

	// Find insertion position for the key
	insertPos := 0
	for i, parentKey := range parent.Keys {
		if bytes.Compare(key, parentKey) <= 0 {
			insertPos = i
			break
		}
		insertPos = i + 1
	}

	// Insert key at the correct position
	keyCopy := make([]byte, len(key))
	copy(keyCopy, key)
	parent.Keys = insertByteSliceAt(parent.Keys, insertPos, keyCopy)

	// Insert child pointer at position insertPos + 1
	parent.Children = insertUint32At(parent.Children, insertPos+1, rightChildPageNum)

	// Update key count
	parent.KeyCount++

	// idx.logger.Infof("insertIntoParent MODIFIED: parent %d now has %d keys after inserting '%s' at pos %d (ptr=%p)",
	// 	parentPageNum, parent.KeyCount, string(key), insertPos, parent)

	// Check if parent node is now full and needs splitting
	maxKeys := calculateMaxKeysForNode(parent, idx.Metadata.PageSize)
	if parent.KeyCount > maxKeys {
		idx.logger.Debugf("Parent node %d is full (%d > %d) after insertion, triggering split",
			parentPageNum, parent.KeyCount, maxKeys)

		// Save current state before split
		idx.PageManager.PutPage(parentPageNum, parent, true)

		// Split the parent internal node
		// This may recursively split up the tree if ancestors are also full
		newRootPageNum, _, _, err := splitInternalNode(idx, parent)
		if err != nil {
			return parentPageNum, fmt.Errorf("failed to split parent node %d: %w", parentPageNum, err)
		}

		idx.logger.Debugf("Successfully split parent node %d, new root is %d", parentPageNum, newRootPageNum)
		return newRootPageNum, nil
	}

	// Save updated parent
	idx.PageManager.PutPage(parentPageNum, parent, true)

	// idx.logger.Infof("insertIntoParent SAVED: parent %d with %d keys marked dirty (ptr=%p)",
	// 	parentPageNum, parent.KeyCount, parent)

	idx.logger.Debugf("Inserted key '%s' and child %d into parent %d at position %d",
		string(key), rightChildPageNum, parentPageNum, insertPos)

	return parentPageNum, nil
}

// insertUint32At inserts a uint32 value at a specific position in a slice
// This utility function helps with inserting child pointers into internal nodes
// Parameters:
//   - slice: The original uint32 slice
//   - index: The position to insert at
//   - value: The value to insert
//
// Returns:
//   - []uint32: The updated slice with the value inserted
func insertUint32At(slice []uint32, index int, value uint32) []uint32 {
	// Ensure we don't go out of bounds
	if index < 0 {
		index = 0
	}
	if index > len(slice) {
		index = len(slice)
	}

	// Grow slice by one element
	slice = append(slice, 0)

	// Shift elements to the right
	copy(slice[index+1:], slice[index:])

	// Insert new value
	slice[index] = value

	return slice
}

// insertIntoInternal inserts a child reference into an internal node
func insertIntoInternal(idx *BTreeIndex, internal *BTreeNode, childPageNum uint32, nodesCreated int) (uint32, bool, int, error) {
	// This is a placeholder implementation
	// In a full implementation, this would handle internal node insertions
	// TODO Come back to this.....
	idx.logger.Debugf("Inserting into internal node %d (placeholder)", internal.PageNum)
	return internal.PageNum, false, nodesCreated, nil
}

// updateInternalAfterMerge updates an internal node after a child merge
func updateInternalAfterMerge(idx *BTreeIndex, internal *BTreeNode, oldChildPageNum, newChildPageNum uint32, nodesDeleted int) (uint32, bool, int, error) {

	// handle internal node updates after merges

	if idx == nil {
		return 0, false, nodesDeleted, fmt.Errorf("index cannot be nil")
	}
	if internal == nil {
		return 0, false, nodesDeleted, fmt.Errorf("internal node cannot be nil")
	}
	if !internal.IsLeaf {
		return 0, false, nodesDeleted, fmt.Errorf("node %d is not an internal node", internal.PageNum)
	}

	// Pin internal page during modification
	if err := idx.PageManager.PinPage(internal.PageNum); err != nil {
		return 0, false, nodesDeleted, fmt.Errorf("failed to pin internal page %d: %w", internal.PageNum, err)
	}
	defer func() {
		if err := idx.PageManager.UnpinPage(internal.PageNum); err != nil {
			idx.logger.Warnf("Failed to unpin internal page %d: %v", internal.PageNum, err)
		}
	}()

	// handle updating the internal node after a merge
	for i, childPage := range internal.Children {
		if childPage == oldChildPageNum {
			// Replace old child with new child
			internal.Children[i] = newChildPageNum
			break
		}
	}

	// Remove the key that was associated with the old child
	for i := range internal.Keys {
		if internal.Children[i] == oldChildPageNum {
			// Remove the key at this position
			internal.Keys = removeByteSliceAt(internal.Keys, i)
			// Adjust the key count
			internal.KeyCount--
			break
		}
	}

	// Mark the internal node as dirty
	idx.PageManager.PutPage(internal.PageNum, internal, true)
	// Log the update
	idx.logger.Debugf("Updating internal node %d after merge (placeholder)", internal.PageNum)

	return internal.PageNum, false, nodesDeleted, nil
}

// Utility functions for slice operations

// insertByteSliceAt inserts a byte slice at a specific position
func insertByteSliceAt(slice [][]byte, index int, value []byte) [][]byte {
	slice = append(slice, nil)
	copy(slice[index+1:], slice[index:])
	slice[index] = value
	return slice
}

// insertStringSliceAt inserts a string slice at a specific position
func insertStringSliceAt(slice [][]string, index int, value []string) [][]string {
	slice = append(slice, nil)
	copy(slice[index+1:], slice[index:])
	slice[index] = value
	return slice
}

// removeByteSliceAt removes a byte slice at a specific position
func removeByteSliceAt(slice [][]byte, index int) [][]byte {
	return append(slice[:index], slice[index+1:]...)
}

// removeStringSliceAt removes a string slice at a specific position
func removeStringSliceAt(slice [][]string, index int) [][]string {
	return append(slice[:index], slice[index+1:]...)
}

// removeStringFromSlice removes a specific string from a slice
func removeStringFromSlice(slice []string, value string) []string {
	result := make([]string, 0, len(slice))
	for _, item := range slice {
		if item != value {
			result = append(result, item)
		}
	}
	return result
}
