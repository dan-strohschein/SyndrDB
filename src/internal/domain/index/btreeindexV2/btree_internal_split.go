package btreeindexV2

import (
	"bytes"
	"fmt"
)

// splitInternalNode handles the splitting of an internal (non-leaf) node that has become full
// This is a critical operation for maintaining tree balance during heavy insert workloads
//
// Algorithm:
// 1. Allocate a new internal node (right sibling)
// 2. Find the midpoint key (this key will be promoted to parent)
// 3. Split keys and children between original and new node
// 4. Update parent pointers for all affected children
// 5. Promote midpoint key to parent (or create new root if splitting the root)
//
// Parameters:
//   - idx: The BTree index
//   - internal: The full internal node to split
//
// Returns:
//   - newRootPageNum: New root page number (if root was split), otherwise parent page number
//   - rootChanged: True if root page changed (root was split)
//   - nodesCreated: Number of new nodes created (1 for new sibling, +1 if new root)
//   - error: Any error that occurred during the split
func splitInternalNode(idx *BTreeIndex, internal *BTreeNode) (uint32, bool, int, error) {
	if internal.IsLeaf {
		return 0, false, 0, fmt.Errorf("cannot split internal node: node %d is a leaf", internal.PageNum)
	}

	idx.logger.Debugf("Starting split of internal node %d with %d keys", internal.PageNum, internal.KeyCount)

	// Step 1: Allocate new internal node (right sibling)
	newInternalPageNum, err := idx.FileManager.AllocatePage()
	if err != nil {
		return 0, false, 0, fmt.Errorf("failed to allocate page for new internal node: %w", err)
	}

	newInternal := &BTreeNode{
		PageNum:    newInternalPageNum,
		IsLeaf:     false,
		KeyCount:   0,
		Keys:       make([][]byte, 0),
		Children:   make([]uint32, 0),
		ParentPage: internal.ParentPage, // Initially same parent
	}

	idx.logger.Debugf("Allocated new internal node %d", newInternalPageNum)

	// Step 2: Find midpoint for split
	// For internal nodes: midpoint key is PROMOTED to parent, not duplicated
	midpoint := len(internal.Keys) / 2
	if midpoint == 0 {
		// Safety check: ensure we have at least one key to promote
		idx.FileManager.DeallocatePage(newInternalPageNum)
		return 0, false, 0, fmt.Errorf("internal node %d has insufficient keys to split", internal.PageNum)
	}

	// The key at midpoint will be promoted to parent
	promotedKey := make([]byte, len(internal.Keys[midpoint]))
	copy(promotedKey, internal.Keys[midpoint])

	idx.logger.Debugf("Split midpoint: %d, promoted key: '%s'", midpoint, string(promotedKey))

	// Step 3: Split keys and children
	// Original node keeps: keys[0:midpoint] and children[0:midpoint+1]
	// New node gets: keys[midpoint+1:] and children[midpoint+1:]
	// Note: The key at midpoint is promoted, not stored in either node

	// Keys for new internal node (everything after midpoint)
	newInternal.Keys = make([][]byte, len(internal.Keys[midpoint+1:]))
	for i, key := range internal.Keys[midpoint+1:] {
		keyCopy := make([]byte, len(key))
		copy(keyCopy, key)
		newInternal.Keys[i] = keyCopy
	}
	newInternal.KeyCount = uint32(len(newInternal.Keys))

	// Children for new internal node (start from midpoint+1)
	newInternal.Children = make([]uint32, len(internal.Children[midpoint+1:]))
	copy(newInternal.Children, internal.Children[midpoint+1:])

	// Truncate original node (keep only keys[0:midpoint] and children[0:midpoint+1])
	internal.Keys = internal.Keys[:midpoint]
	internal.Children = internal.Children[:midpoint+1]
	internal.KeyCount = uint32(len(internal.Keys))

	idx.logger.Debugf("Split complete: original node %d has %d keys, new node %d has %d keys",
		internal.PageNum, internal.KeyCount, newInternalPageNum, newInternal.KeyCount)

	// Step 4: Update parent pointers for children of new internal node
	// CRITICAL: All parent pointer updates must succeed or the split fails
	// Inconsistent parent pointers can cause cycles and tree corruption
	failedChildren := []uint32{}
	for _, childPageNum := range newInternal.Children {
		childData, err := idx.PageManager.GetPage(childPageNum, func(pn uint32) (interface{}, error) {
			return idx.FileManager.ReadPage(pn)
		})
		if err != nil {
			idx.logger.Errorf("Failed to load child %d to update parent pointer: %v", childPageNum, err)
			failedChildren = append(failedChildren, childPageNum)
			continue
		}

		child, ok := childData.(*BTreeNode)
		if !ok {
			idx.logger.Errorf("Child page %d is not a valid BTree node", childPageNum)
			failedChildren = append(failedChildren, childPageNum)
			continue
		}

		// Update parent pointer
		child.ParentPage = newInternalPageNum
		idx.PageManager.PutPage(childPageNum, child, true)

		// CRITICAL: Flush child page immediately to ensure parent pointer is persisted
		// This prevents corruption if a crash occurs before the full split is committed
		if err := idx.FileManager.WritePage(childPageNum, child); err != nil {
			idx.logger.Errorf("Failed to flush child %d after parent pointer update: %v", childPageNum, err)
			failedChildren = append(failedChildren, childPageNum)
			continue
		}

		idx.logger.Debugf("Updated child %d parent pointer to new internal node %d", childPageNum, newInternalPageNum)
	}

	// If any child updates failed, we cannot safely complete the split
	// Rollback by deallocating the new node and returning an error
	if len(failedChildren) > 0 {
		idx.FileManager.DeallocatePage(newInternalPageNum)
		return 0, false, 0, fmt.Errorf("failed to update parent pointers for %d children: %v - split aborted to prevent corruption", len(failedChildren), failedChildren)
	}

	// Step 5: Save both internal nodes to storage
	idx.PageManager.PutPage(internal.PageNum, internal, true)
	idx.PageManager.PutPage(newInternalPageNum, newInternal, true)

	// Pin new internal node to prevent eviction during parent insertion
	if err := idx.PageManager.PinPage(newInternalPageNum); err != nil {
		idx.logger.Warnf("Failed to pin new internal node %d: %v", newInternalPageNum, err)
	}
	defer func() {
		if err := idx.PageManager.UnpinPage(newInternalPageNum); err != nil {
			idx.logger.Warnf("Failed to unpin new internal node %d: %v", newInternalPageNum, err)
		}
	}()

	// Step 6: Handle parent insertion or root creation
	nodesCreated := 1 // We created one new internal node

	// Check if we're splitting the root (no parent)
	if internal.ParentPage == 0 {
		// Create new root
		newRootPageNum, err := createNewRoot(idx, internal.PageNum, newInternalPageNum, promotedKey)
		if err != nil {
			// Clean up allocated page before returning error
			idx.FileManager.DeallocatePage(newInternalPageNum)
			return 0, false, 0, fmt.Errorf("failed to create new root after internal split: %w", err)
		}

		// Update parent pointers for both internal nodes
		internal.ParentPage = newRootPageNum
		newInternal.ParentPage = newRootPageNum

		// Save updated internal nodes
		idx.PageManager.PutPage(internal.PageNum, internal, true)
		idx.PageManager.PutPage(newInternalPageNum, newInternal, true)

		// CRITICAL: Flush both internal nodes immediately after root split
		// These nodes are referenced by the new root and must exist on disk
		if err := idx.FileManager.WritePage(internal.PageNum, internal); err != nil {
			idx.logger.Errorf("Failed to flush original internal page %d: %v", internal.PageNum, err)
			idx.FileManager.DeallocatePage(newInternalPageNum)
			return 0, false, 0, fmt.Errorf("failed to persist original internal node: %w", err)
		}
		if err := idx.FileManager.WritePage(newInternalPageNum, newInternal); err != nil {
			idx.logger.Errorf("Failed to flush new internal page %d: %v", newInternalPageNum, err)
			idx.FileManager.DeallocatePage(newInternalPageNum)
			return 0, false, 0, fmt.Errorf("failed to persist new internal node: %w", err)
		}

		// CRITICAL: Flush the new root immediately to ensure it's persisted
		// The root is essential for tree traversal - if it's lost, the entire tree becomes inaccessible
		rootData, err := idx.PageManager.GetPage(newRootPageNum, func(pn uint32) (interface{}, error) {
			return idx.FileManager.ReadPage(pn)
		})
		if err != nil {
			idx.logger.Errorf("Failed to retrieve new root page %d: %v", newRootPageNum, err)
			return 0, false, 0, fmt.Errorf("failed to retrieve new root for flushing: %w", err)
		}
		if err := idx.FileManager.WritePage(newRootPageNum, rootData); err != nil {
			idx.logger.Errorf("Failed to flush new root page %d: %v", newRootPageNum, err)
			return 0, false, 0, fmt.Errorf("failed to persist new root: %w", err)
		}

		nodesCreated++ // We also created a new root

		idx.logger.Infof("Created new root %d after internal node split, tree height increased",
			newRootPageNum)

		return newRootPageNum, true, nodesCreated, nil
	} else {
		// Insert promoted key into existing parent
		// Note: This may recursively trigger another internal split if parent is also full
		newRootPageNum, err := insertIntoParent(idx, internal.ParentPage, promotedKey, newInternalPageNum)
		if err != nil {
			// Clean up allocated page before returning error
			idx.FileManager.DeallocatePage(newInternalPageNum)
			return 0, false, 0, fmt.Errorf("failed to insert into parent after internal split: %w", err)
		}

		idx.logger.Debugf("Inserted promoted key into parent %d, new root is %d", internal.ParentPage, newRootPageNum)

		return newRootPageNum, true, nodesCreated, nil
	}
}

// insertIntoInternalNode inserts a key and child pointer into an internal node
// This is used during node splits to propagate keys up the tree
// If the internal node becomes full, it will trigger an internal node split
//
// Parameters:
//   - idx: The BTree index
//   - internal: The internal node to insert into
//   - key: The key to insert
//   - rightChildPageNum: Page number of the right child
//
// Returns:
//   - newRootPageNum: New root if split occurred, otherwise same as parent
//   - rootChanged: True if root changed
//   - nodesCreated: Number of nodes created during insertion
//   - error: Any error that occurred
func insertIntoInternalNode(idx *BTreeIndex, internal *BTreeNode, key []byte, rightChildPageNum uint32) (uint32, bool, int, error) {
	if internal.IsLeaf {
		return 0, false, 0, fmt.Errorf("cannot insert into internal node: node %d is a leaf", internal.PageNum)
	}

	// Pin internal node during modification
	if err := idx.PageManager.PinPage(internal.PageNum); err != nil {
		return 0, false, 0, fmt.Errorf("failed to pin internal node %d: %w", internal.PageNum, err)
	}
	defer func() {
		if err := idx.PageManager.UnpinPage(internal.PageNum); err != nil {
			idx.logger.Warnf("Failed to unpin internal node %d: %v", internal.PageNum, err)
		}
	}()

	// Find insertion position for the key
	insertPos := 0
	for i, internalKey := range internal.Keys {
		if bytes.Compare(key, internalKey) <= 0 {
			insertPos = i
			break
		}
		insertPos = i + 1
	}

	// Insert key at the correct position
	keyCopy := make([]byte, len(key))
	copy(keyCopy, key)
	internal.Keys = insertByteSliceAt(internal.Keys, insertPos, keyCopy)

	// Insert child pointer at position insertPos + 1
	internal.Children = insertUint32At(internal.Children, insertPos+1, rightChildPageNum)

	// Update key count
	internal.KeyCount++

	idx.logger.Debugf("Inserted key '%s' and child %d into internal node %d at position %d",
		string(key), rightChildPageNum, internal.PageNum, insertPos)

	// Check if internal node needs to be split
	maxKeys := calculateMaxKeysForNode(internal, idx.Metadata.PageSize)
	if internal.KeyCount > maxKeys {
		idx.logger.Debugf("Internal node %d is full (%d > %d), splitting",
			internal.PageNum, internal.KeyCount, maxKeys)

		// Save current state before split
		idx.PageManager.PutPage(internal.PageNum, internal, true)

		// Split the internal node
		return splitInternalNode(idx, internal)
	}

	// Save updated internal node
	idx.PageManager.PutPage(internal.PageNum, internal, true)

	return internal.PageNum, false, 0, nil
}
