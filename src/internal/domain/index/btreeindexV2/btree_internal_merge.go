package btreeindexV2

import (
	"fmt"
)

// mergeInternalNodes handles merging of two sibling internal nodes when they are underutilized
// This is critical for maintaining tree balance during heavy delete workloads
//
// Algorithm:
// 1. Verify nodes are siblings and can be merged
// 2. Pull down the separator key from parent
// 3. Merge keys and children from right node into left node
// 4. Update parent pointers for all affected children
// 5. Remove separator key from parent and update parent's children
// 6. Deallocate the right node
// 7. Recursively handle parent underflow if needed
//
// B-Tree Property: Internal nodes should have at least ⌈order/2⌉ - 1 keys
// When a node falls below this threshold, we attempt to merge with a sibling
//
// Parameters:
//   - idx: The BTree index
//   - leftInternal: The left sibling internal node
//   - rightInternal: The right sibling internal node
//   - separatorKey: The key in the parent that separates these siblings
//   - parentPageNum: The parent page number
//
// Returns:
//   - bool: True if parent was affected (may need rebalancing)
//   - error: Any error that occurred during the merge
func mergeInternalNodes(idx *BTreeIndex, leftInternal, rightInternal *BTreeNode, separatorKey []byte, parentPageNum uint32) (bool, error) {
	// Validate inputs
	if leftInternal.IsLeaf || rightInternal.IsLeaf {
		return false, fmt.Errorf("cannot merge internal nodes: one or both nodes are leaves")
	}

	if leftInternal.ParentPage != parentPageNum || rightInternal.ParentPage != parentPageNum {
		return false, fmt.Errorf("nodes do not share the same parent")
	}

	idx.logger.Debugf("Merging internal nodes %d and %d (separator key: '%s')",
		leftInternal.PageNum, rightInternal.PageNum, string(separatorKey))

	// Pin both nodes to prevent eviction during merge
	if err := idx.PageManager.PinPage(leftInternal.PageNum); err != nil {
		return false, fmt.Errorf("failed to pin left node %d: %w", leftInternal.PageNum, err)
	}
	defer idx.PageManager.UnpinPage(leftInternal.PageNum)

	if err := idx.PageManager.PinPage(rightInternal.PageNum); err != nil {
		return false, fmt.Errorf("failed to pin right node %d: %w", rightInternal.PageNum, err)
	}
	defer idx.PageManager.UnpinPage(rightInternal.PageNum)

	// Step 1: Verify merge is safe (combined size doesn't exceed capacity)
	maxKeys := calculateMaxKeysForNode(leftInternal, idx.Metadata.PageSize)
	combinedKeyCount := leftInternal.KeyCount + rightInternal.KeyCount + 1 // +1 for separator key

	if combinedKeyCount > maxKeys {
		return false, fmt.Errorf("cannot merge: combined size (%d) exceeds capacity (%d)",
			combinedKeyCount, maxKeys)
	}

	// Step 2: Pull down separator key from parent
	// In internal nodes, the separator key becomes part of the merged node
	separatorKeyCopy := make([]byte, len(separatorKey))
	copy(separatorKeyCopy, separatorKey)
	leftInternal.Keys = append(leftInternal.Keys, separatorKeyCopy)

	// Step 3: Append all keys from right node to left node
	for _, key := range rightInternal.Keys {
		keyCopy := make([]byte, len(key))
		copy(keyCopy, key)
		leftInternal.Keys = append(leftInternal.Keys, keyCopy)
	}

	// Step 4: Append all children from right node to left node
	leftInternal.Children = append(leftInternal.Children, rightInternal.Children...)

	// Update key count
	leftInternal.KeyCount = uint32(len(leftInternal.Keys))

	idx.logger.Debugf("Merged nodes: left node %d now has %d keys and %d children",
		leftInternal.PageNum, leftInternal.KeyCount, len(leftInternal.Children))

	// Step 5: Update parent pointers for all children that moved from right to left
	for _, childPageNum := range rightInternal.Children {
		childData, err := idx.PageManager.GetPage(childPageNum, func(pn uint32) (interface{}, error) {
			return idx.FileManager.ReadPage(pn)
		})
		if err != nil {
			idx.logger.Warnf("Failed to load child %d to update parent pointer: %v", childPageNum, err)
			continue
		}

		child, ok := childData.(*BTreeNode)
		if !ok {
			idx.logger.Warnf("Page %d is not a valid BTree node", childPageNum)
			continue
		}

		// Update parent pointer to left node
		child.ParentPage = leftInternal.PageNum
		idx.PageManager.PutPage(childPageNum, child, true)
	}

	// Step 6: Save the merged left node
	idx.PageManager.PutPage(leftInternal.PageNum, leftInternal, true)

	// Step 7: Remove separator key and right node from parent
	if err := removeKeyFromInternalNode(idx, parentPageNum, separatorKey, rightInternal.PageNum); err != nil {
		return false, fmt.Errorf("failed to remove separator from parent: %w", err)
	}

	// Step 8: Deallocate the right node (no longer needed)
	if err := idx.FileManager.DeallocatePage(rightInternal.PageNum); err != nil {
		idx.logger.Warnf("Failed to deallocate merged node %d: %v", rightInternal.PageNum, err)
	}

	idx.logger.Infof("Successfully merged internal nodes %d and %d", leftInternal.PageNum, rightInternal.PageNum)

	// Parent was modified, may need rebalancing
	return true, nil
}

// removeKeyFromInternalNode removes a key and its corresponding child pointer from an internal node
// This is used during merge operations when we need to remove the separator key from the parent
//
// Parameters:
//   - idx: The BTree index
//   - parentPageNum: The page number of the parent node
//   - key: The key to remove
//   - childPageNum: The child page number to remove (right child of the key)
//
// Returns:
//   - error: Any error that occurred
func removeKeyFromInternalNode(idx *BTreeIndex, parentPageNum uint32, key []byte, childPageNum uint32) error {
	// Load parent node
	parentData, err := idx.PageManager.GetPage(parentPageNum, func(pn uint32) (interface{}, error) {
		return idx.FileManager.ReadPage(pn)
	})
	if err != nil {
		return fmt.Errorf("failed to load parent %d: %w", parentPageNum, err)
	}

	parent, ok := parentData.(*BTreeNode)
	if !ok {
		return fmt.Errorf("parent page %d is not a valid BTree node", parentPageNum)
	}

	if parent.IsLeaf {
		return fmt.Errorf("parent page %d is a leaf node", parentPageNum)
	}

	// Pin parent during modification
	if err := idx.PageManager.PinPage(parentPageNum); err != nil {
		return fmt.Errorf("failed to pin parent %d: %w", parentPageNum, err)
	}
	defer idx.PageManager.UnpinPage(parentPageNum)

	// Find the key position
	keyPos := -1
	for i, parentKey := range parent.Keys {
		if len(parentKey) == len(key) && string(parentKey) == string(key) {
			keyPos = i
			break
		}
	}

	if keyPos == -1 {
		return fmt.Errorf("separator key not found in parent %d", parentPageNum)
	}

	// Remove key from parent
	parent.Keys = append(parent.Keys[:keyPos], parent.Keys[keyPos+1:]...)

	// Remove corresponding child pointer (the right child of this key)
	// In B-tree structure: child[i] < key[i] <= child[i+1]
	// So we remove child[keyPos+1]
	childPos := -1
	for i, child := range parent.Children {
		if child == childPageNum {
			childPos = i
			break
		}
	}

	if childPos == -1 {
		return fmt.Errorf("child page %d not found in parent's children", childPageNum)
	}

	parent.Children = append(parent.Children[:childPos], parent.Children[childPos+1:]...)
	parent.KeyCount = uint32(len(parent.Keys))

	idx.logger.Debugf("Removed key at position %d from parent %d (new key count: %d)",
		keyPos, parentPageNum, parent.KeyCount)

	// Save modified parent
	idx.PageManager.PutPage(parentPageNum, parent, true)

	// Check if parent is now underutilized and needs rebalancing
	// TODO: I could implement adaptive thresholds based on workload patterns to optimize merge frequency
	if shouldRebalanceInternal(parent, idx.Metadata.PageSize) {
		idx.logger.Debugf("Parent %d is underutilized after merge, may need rebalancing", parentPageNum)

		// If parent is root and has no keys, make the only child the new root
		if parent.ParentPage == 0 && parent.KeyCount == 0 {
			if len(parent.Children) == 1 {
				return promoteChildToRoot(idx, parent.Children[0])
			}
		} else if parent.ParentPage != 0 {
			// Non-root node: attempt to rebalance with sibling
			return rebalanceInternalNode(idx, parent)
		}
	}

	return nil
}

// shouldRebalanceInternal determines if an internal node needs rebalancing
// Uses the standard B-tree constraint: internal nodes should have at least ⌈order/2⌉ - 1 keys
//
// Parameters:
//   - node: The internal node to check
//   - pageSize: The page size for capacity calculations
//
// Returns:
//   - bool: True if node should be rebalanced
func shouldRebalanceInternal(node *BTreeNode, pageSize uint32) bool {
	if node.IsLeaf {
		return false // Only for internal nodes
	}

	maxKeys := calculateMaxKeysForNode(node, pageSize)
	minKeys := maxKeys / 2 // Standard B-tree minimum: ⌈order/2⌉ - 1

	// Root can have fewer keys
	if node.ParentPage == 0 {
		return node.KeyCount == 0 && len(node.Children) > 1
	}

	return node.KeyCount < minKeys
}

// promoteChildToRoot makes a child node the new root when the old root becomes empty
// This reduces tree height by one level
//
// Parameters:
//   - idx: The BTree index
//   - childPageNum: The page number of the child to promote
//
// Returns:
//   - error: Any error that occurred
func promoteChildToRoot(idx *BTreeIndex, childPageNum uint32) error {
	idx.logger.Infof("Promoting child %d to new root (tree height decreasing)", childPageNum)

	// Load the child node
	childData, err := idx.PageManager.GetPage(childPageNum, func(pn uint32) (interface{}, error) {
		return idx.FileManager.ReadPage(pn)
	})
	if err != nil {
		return fmt.Errorf("failed to load child %d: %w", childPageNum, err)
	}

	child, ok := childData.(*BTreeNode)
	if !ok {
		return fmt.Errorf("child page %d is not a valid BTree node", childPageNum)
	}

	// Update child to be the new root (clear parent pointer)
	child.ParentPage = 0

	// Save the new root
	idx.PageManager.PutPage(childPageNum, child, true)

	// Update metadata to point to new root
	oldRootPageNum := idx.Metadata.RootPageNum
	idx.Metadata.RootPageNum = childPageNum

	// TODO: I could implement root page recycling to reuse the old root page number for consistency
	// For now, we just deallocate it
	if err := idx.FileManager.DeallocatePage(oldRootPageNum); err != nil {
		idx.logger.Warnf("Failed to deallocate old root %d: %v", oldRootPageNum, err)
	}

	idx.logger.Infof("Successfully promoted child %d to new root (old root %d deallocated)",
		childPageNum, oldRootPageNum)

	return nil
}

// rebalanceInternalNode attempts to rebalance an underutilized internal node
// Strategy: Try to borrow from sibling, if that fails, merge with sibling
//
// Parameters:
//   - idx: The BTree index
//   - node: The internal node that needs rebalancing
//
// Returns:
//   - error: Any error that occurred
func rebalanceInternalNode(idx *BTreeIndex, node *BTreeNode) error {
	if node.ParentPage == 0 {
		return nil // Root doesn't need rebalancing
	}

	idx.logger.Debugf("Attempting to rebalance internal node %d", node.PageNum)

	// Load parent to find siblings
	parentData, err := idx.PageManager.GetPage(node.ParentPage, func(pn uint32) (interface{}, error) {
		return idx.FileManager.ReadPage(pn)
	})
	if err != nil {
		return fmt.Errorf("failed to load parent %d: %w", node.ParentPage, err)
	}

	parent, ok := parentData.(*BTreeNode)
	if !ok {
		return fmt.Errorf("parent page %d is not a valid BTree node", node.ParentPage)
	}

	// Find our position in parent's children
	ourPos := -1
	for i, child := range parent.Children {
		if child == node.PageNum {
			ourPos = i
			break
		}
	}

	if ourPos == -1 {
		return fmt.Errorf("node %d not found in parent's children", node.PageNum)
	}

	// Try to borrow from right sibling first
	if ourPos < len(parent.Children)-1 {
		rightSiblingPageNum := parent.Children[ourPos+1]
		if canBorrowFromSibling(idx, rightSiblingPageNum) {
			return borrowFromRightInternalSibling(idx, node, rightSiblingPageNum, parent, ourPos)
		}
	}

	// Try to borrow from left sibling
	if ourPos > 0 {
		leftSiblingPageNum := parent.Children[ourPos-1]
		if canBorrowFromSibling(idx, leftSiblingPageNum) {
			return borrowFromLeftInternalSibling(idx, node, leftSiblingPageNum, parent, ourPos)
		}
	}

	// Can't borrow, must merge
	// Prefer merging with left sibling if available
	if ourPos > 0 {
		leftSiblingPageNum := parent.Children[ourPos-1]
		separatorKey := parent.Keys[ourPos-1]

		return mergeWithLeftInternalSibling(idx, node, leftSiblingPageNum, separatorKey, parent.PageNum)
	} else if ourPos < len(parent.Children)-1 {
		// Merge with right sibling (we become the left sibling in the merge)
		rightSiblingPageNum := parent.Children[ourPos+1]
		separatorKey := parent.Keys[ourPos]

		return mergeWithRightInternalSibling(idx, node, rightSiblingPageNum, separatorKey, parent.PageNum)
	}

	return fmt.Errorf("unable to rebalance node %d: no siblings available", node.PageNum)
}

// canBorrowFromSibling checks if a sibling has enough keys to lend one
//
// Parameters:
//   - idx: The BTree index
//   - siblingPageNum: The page number of the sibling
//
// Returns:
//   - bool: True if sibling can lend a key
func canBorrowFromSibling(idx *BTreeIndex, siblingPageNum uint32) bool {
	siblingData, err := idx.PageManager.GetPage(siblingPageNum, func(pn uint32) (interface{}, error) {
		return idx.FileManager.ReadPage(pn)
	})
	if err != nil {
		return false
	}

	sibling, ok := siblingData.(*BTreeNode)
	if !ok {
		return false
	}

	maxKeys := calculateMaxKeysForNode(sibling, idx.Metadata.PageSize)
	minKeys := maxKeys / 2

	// Can borrow if sibling has more than minimum keys
	return sibling.KeyCount > minKeys
}

// borrowFromRightInternalSibling borrows a key from the right sibling through parent rotation
// This is a standard B-tree rebalancing operation
//
// TODO: I could optimize this by tracking node utilization to predict when borrowing vs merging is better
//
// Parameters:
//   - idx: The BTree index
//   - node: The underutilized node
//   - rightSiblingPageNum: The right sibling page number
//   - parent: The parent node
//   - nodePos: Position of node in parent's children
//
// Returns:
//   - error: Any error that occurred
func borrowFromRightInternalSibling(idx *BTreeIndex, node *BTreeNode, rightSiblingPageNum uint32, parent *BTreeNode, nodePos int) error {
	// Load right sibling
	rightSiblingData, err := idx.PageManager.GetPage(rightSiblingPageNum, func(pn uint32) (interface{}, error) {
		return idx.FileManager.ReadPage(pn)
	})
	if err != nil {
		return fmt.Errorf("failed to load right sibling %d: %w", rightSiblingPageNum, err)
	}

	rightSibling, ok := rightSiblingData.(*BTreeNode)
	if !ok {
		return fmt.Errorf("right sibling %d is not a valid BTree node", rightSiblingPageNum)
	}

	// Pin both nodes during operation
	if err := idx.PageManager.PinPage(node.PageNum); err != nil {
		return fmt.Errorf("failed to pin node %d: %w", node.PageNum, err)
	}
	defer idx.PageManager.UnpinPage(node.PageNum)

	if err := idx.PageManager.PinPage(rightSiblingPageNum); err != nil {
		return fmt.Errorf("failed to pin right sibling %d: %w", rightSiblingPageNum, err)
	}
	defer idx.PageManager.UnpinPage(rightSiblingPageNum)

	// Rotation: parent key comes down, first key of right sibling goes up
	separatorKey := parent.Keys[nodePos]
	separatorKeyCopy := make([]byte, len(separatorKey))
	copy(separatorKeyCopy, separatorKey)

	// Add separator to our node
	node.Keys = append(node.Keys, separatorKeyCopy)
	node.KeyCount++

	// Move first child from right sibling to our node
	if len(rightSibling.Children) > 0 {
		firstChild := rightSibling.Children[0]
		node.Children = append(node.Children, firstChild)

		// Update child's parent pointer
		childData, err := idx.PageManager.GetPage(firstChild, func(pn uint32) (interface{}, error) {
			return idx.FileManager.ReadPage(pn)
		})
		if err == nil {
			if child, ok := childData.(*BTreeNode); ok {
				child.ParentPage = node.PageNum
				idx.PageManager.PutPage(firstChild, child, true)
			}
		}
	}

	// Promote first key of right sibling to parent
	promotedKey := make([]byte, len(rightSibling.Keys[0]))
	copy(promotedKey, rightSibling.Keys[0])
	parent.Keys[nodePos] = promotedKey

	// Remove first key and child from right sibling
	rightSibling.Keys = rightSibling.Keys[1:]
	if len(rightSibling.Children) > 0 {
		rightSibling.Children = rightSibling.Children[1:]
	}
	rightSibling.KeyCount = uint32(len(rightSibling.Keys))

	// Save all modified nodes
	idx.PageManager.PutPage(node.PageNum, node, true)
	idx.PageManager.PutPage(rightSiblingPageNum, rightSibling, true)
	idx.PageManager.PutPage(parent.PageNum, parent, true)

	idx.logger.Debugf("Borrowed key from right sibling %d to node %d", rightSiblingPageNum, node.PageNum)

	return nil
}

// borrowFromLeftInternalSibling borrows a key from the left sibling through parent rotation
//
// Parameters:
//   - idx: The BTree index
//   - node: The underutilized node
//   - leftSiblingPageNum: The left sibling page number
//   - parent: The parent node
//   - nodePos: Position of node in parent's children
//
// Returns:
//   - error: Any error that occurred
func borrowFromLeftInternalSibling(idx *BTreeIndex, node *BTreeNode, leftSiblingPageNum uint32, parent *BTreeNode, nodePos int) error {
	// Load left sibling
	leftSiblingData, err := idx.PageManager.GetPage(leftSiblingPageNum, func(pn uint32) (interface{}, error) {
		return idx.FileManager.ReadPage(pn)
	})
	if err != nil {
		return fmt.Errorf("failed to load left sibling %d: %w", leftSiblingPageNum, err)
	}

	leftSibling, ok := leftSiblingData.(*BTreeNode)
	if !ok {
		return fmt.Errorf("left sibling %d is not a valid BTree node", leftSiblingPageNum)
	}

	// Pin both nodes during operation
	if err := idx.PageManager.PinPage(node.PageNum); err != nil {
		return fmt.Errorf("failed to pin node %d: %w", node.PageNum, err)
	}
	defer idx.PageManager.UnpinPage(node.PageNum)

	if err := idx.PageManager.PinPage(leftSiblingPageNum); err != nil {
		return fmt.Errorf("failed to pin left sibling %d: %w", leftSiblingPageNum, err)
	}
	defer idx.PageManager.UnpinPage(leftSiblingPageNum)

	// Rotation: parent key comes down, last key of left sibling goes up
	separatorKey := parent.Keys[nodePos-1]
	separatorKeyCopy := make([]byte, len(separatorKey))
	copy(separatorKeyCopy, separatorKey)

	// Insert separator at beginning of our node
	node.Keys = append([][]byte{separatorKeyCopy}, node.Keys...)
	node.KeyCount++

	// Move last child from left sibling to beginning of our children
	if len(leftSibling.Children) > 0 {
		lastChild := leftSibling.Children[len(leftSibling.Children)-1]
		node.Children = append([]uint32{lastChild}, node.Children...)

		// Update child's parent pointer
		childData, err := idx.PageManager.GetPage(lastChild, func(pn uint32) (interface{}, error) {
			return idx.FileManager.ReadPage(pn)
		})
		if err == nil {
			if child, ok := childData.(*BTreeNode); ok {
				child.ParentPage = node.PageNum
				idx.PageManager.PutPage(lastChild, child, true)
			}
		}
	}

	// Promote last key of left sibling to parent
	promotedKey := make([]byte, len(leftSibling.Keys[len(leftSibling.Keys)-1]))
	copy(promotedKey, leftSibling.Keys[len(leftSibling.Keys)-1])
	parent.Keys[nodePos-1] = promotedKey

	// Remove last key and child from left sibling
	leftSibling.Keys = leftSibling.Keys[:len(leftSibling.Keys)-1]
	if len(leftSibling.Children) > 0 {
		leftSibling.Children = leftSibling.Children[:len(leftSibling.Children)-1]
	}
	leftSibling.KeyCount = uint32(len(leftSibling.Keys))

	// Save all modified nodes
	idx.PageManager.PutPage(node.PageNum, node, true)
	idx.PageManager.PutPage(leftSiblingPageNum, leftSibling, true)
	idx.PageManager.PutPage(parent.PageNum, parent, true)

	idx.logger.Debugf("Borrowed key from left sibling %d to node %d", leftSiblingPageNum, node.PageNum)

	return nil
}

// mergeWithLeftInternalSibling merges node with its left sibling
//
// Parameters:
//   - idx: The BTree index
//   - node: The underutilized node
//   - leftSiblingPageNum: The left sibling page number
//   - separatorKey: The key separating the siblings in parent
//   - parentPageNum: The parent page number
//
// Returns:
//   - error: Any error that occurred
func mergeWithLeftInternalSibling(idx *BTreeIndex, node *BTreeNode, leftSiblingPageNum uint32, separatorKey []byte, parentPageNum uint32) error {
	leftSiblingData, err := idx.PageManager.GetPage(leftSiblingPageNum, func(pn uint32) (interface{}, error) {
		return idx.FileManager.ReadPage(pn)
	})
	if err != nil {
		return fmt.Errorf("failed to load left sibling %d: %w", leftSiblingPageNum, err)
	}

	leftSibling, ok := leftSiblingData.(*BTreeNode)
	if !ok {
		return fmt.Errorf("left sibling %d is not a valid BTree node", leftSiblingPageNum)
	}

	// Merge: left gets separator and all of our content
	_, err = mergeInternalNodes(idx, leftSibling, node, separatorKey, parentPageNum)
	return err
}

// mergeWithRightInternalSibling merges node with its right sibling
//
// Parameters:
//   - idx: The BTree index
//   - node: The underutilized node
//   - rightSiblingPageNum: The right sibling page number
//   - separatorKey: The key separating the siblings in parent
//   - parentPageNum: The parent page number
//
// Returns:
//   - error: Any error that occurred
func mergeWithRightInternalSibling(idx *BTreeIndex, node *BTreeNode, rightSiblingPageNum uint32, separatorKey []byte, parentPageNum uint32) error {
	rightSiblingData, err := idx.PageManager.GetPage(rightSiblingPageNum, func(pn uint32) (interface{}, error) {
		return idx.FileManager.ReadPage(pn)
	})
	if err != nil {
		return fmt.Errorf("failed to load right sibling %d: %w", rightSiblingPageNum, err)
	}

	rightSibling, ok := rightSiblingData.(*BTreeNode)
	if !ok {
		return fmt.Errorf("right sibling %d is not a valid BTree node", rightSiblingPageNum)
	}

	// Merge: we get separator and all of right sibling's content
	_, err = mergeInternalNodes(idx, node, rightSibling, separatorKey, parentPageNum)
	return err
}
