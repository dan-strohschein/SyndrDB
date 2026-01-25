/*
B-TREE INDEX ITERATOR FOR SORTED CURSOR ACCESS

This file implements a cursor-based iterator for B-tree indexes, enabling efficient
sorted traversal for merge join operations and ordered result sets.

KEY FEATURES:
1. Forward and backward traversal using leaf node links
2. Range-bounded iteration (start/end keys)
3. Lazy loading of leaf pages for memory efficiency
4. Cursor positioning for random access within sorted order

POSTGRESQL ALIGNMENT:
- Follows PostgreSQL's index scan iterator pattern
- Uses leaf node links (NextLeaf, PrevLeaf) for sequential access
- Supports both ascending and descending order traversal

USAGE FOR MERGE JOIN:
The iterator enables efficient O(n + m) merge joins by providing:
1. Sorted iteration over join keys
2. Ability to seek to specific key positions
3. Sequential access without building in-memory copies

LIFECYCLE:
1. Create iterator with NewBTreeIterator()
2. Position with SeekFirst(), SeekLast(), or SeekToKey()
3. Iterate with Next() or Prev()
4. Check HasNext() or HasPrev() for bounds
5. Call Close() when done to release resources
*/

package btreeindexV2

import (
	"bytes"
	"fmt"
	"sync"

	"go.uber.org/zap"
)

// BTreeIterator provides cursor-based access to B-tree entries in sorted order
type BTreeIterator struct {
	index  *BTreeIndex // Reference to the underlying B-tree index
	logger *zap.SugaredLogger

	// Current position
	currentNode  *BTreeNode // Current leaf node
	currentIndex int        // Current entry index within node

	// Range bounds
	startKey     []byte // Start key for range iteration (nil = from beginning)
	endKey       []byte // End key for range iteration (nil = to end)
	excludeStart bool   // Exclude start key from results
	excludeEnd   bool   // Exclude end key from results

	// State
	initialized bool          // Whether iterator has been positioned
	exhausted   bool          // Whether iteration has reached the end
	direction   IterDirection // Current iteration direction

	// Statistics
	nodesVisited int // Count of leaf nodes visited
	keysReturned int // Count of keys returned

	mutex sync.Mutex // Protects iterator state
}

// IterDirection indicates the direction of iteration
type IterDirection int

const (
	IterForward  IterDirection = iota // Ascending order
	IterBackward                      // Descending order
)

// IteratorEntry represents a single entry returned by the iterator
type IteratorEntry struct {
	Key         []byte   // The key value
	DocumentIDs []string // Document IDs associated with this key
}

// NewBTreeIterator creates a new iterator for the given B-tree index
// Returns an unpositioned iterator - call SeekFirst(), SeekLast(), or SeekToKey() before iterating
func NewBTreeIterator(index *BTreeIndex, logger *zap.SugaredLogger) *BTreeIterator {
	return &BTreeIterator{
		index:     index,
		logger:    logger,
		direction: IterForward,
	}
}

// NewBTreeRangeIterator creates an iterator bounded by start and end keys
// startKey: Starting key (nil = from beginning)
// endKey: Ending key (nil = to end)
// excludeStart: If true, skip the start key itself
// excludeEnd: If true, skip the end key itself
func NewBTreeRangeIterator(
	index *BTreeIndex,
	logger *zap.SugaredLogger,
	startKey, endKey []byte,
	excludeStart, excludeEnd bool,
) *BTreeIterator {
	return &BTreeIterator{
		index:        index,
		logger:       logger,
		startKey:     startKey,
		endKey:       endKey,
		excludeStart: excludeStart,
		excludeEnd:   excludeEnd,
		direction:    IterForward,
	}
}

// SeekFirst positions the iterator at the first entry (or startKey if bounded)
// Returns error if index is empty or no entries match the range
func (it *BTreeIterator) SeekFirst() error {
	it.mutex.Lock()
	defer it.mutex.Unlock()

	it.direction = IterForward
	it.exhausted = false
	it.nodesVisited = 0
	it.keysReturned = 0

	if it.startKey != nil {
		return it.seekToKeyInternal(it.startKey, true)
	}

	// Find the leftmost leaf node
	firstLeaf, err := it.findLeftmostLeaf()
	if err != nil {
		return fmt.Errorf("failed to find first leaf: %w", err)
	}

	if firstLeaf == nil || firstLeaf.KeyCount == 0 {
		it.exhausted = true
		return nil
	}

	it.currentNode = firstLeaf
	it.currentIndex = 0
	it.initialized = true
	it.nodesVisited++

	// Skip if excludeStart and we're at the start key
	if it.excludeStart && it.startKey != nil {
		if bytes.Equal(firstLeaf.Keys[0], it.startKey) {
			return it.advanceForward()
		}
	}

	return nil
}

// SeekLast positions the iterator at the last entry (or endKey if bounded)
func (it *BTreeIterator) SeekLast() error {
	it.mutex.Lock()
	defer it.mutex.Unlock()

	it.direction = IterBackward
	it.exhausted = false
	it.nodesVisited = 0
	it.keysReturned = 0

	if it.endKey != nil {
		return it.seekToKeyInternal(it.endKey, false)
	}

	// Find the rightmost leaf node
	lastLeaf, err := it.findRightmostLeaf()
	if err != nil {
		return fmt.Errorf("failed to find last leaf: %w", err)
	}

	if lastLeaf == nil || lastLeaf.KeyCount == 0 {
		it.exhausted = true
		return nil
	}

	it.currentNode = lastLeaf
	it.currentIndex = int(lastLeaf.KeyCount) - 1
	it.initialized = true
	it.nodesVisited++

	return nil
}

// SeekToKey positions the iterator at or after the given key
// findFirst: If true, find the first occurrence; if false, find the last
func (it *BTreeIterator) SeekToKey(key []byte) error {
	it.mutex.Lock()
	defer it.mutex.Unlock()
	return it.seekToKeyInternal(key, true)
}

// seekToKeyInternal positions the iterator at the given key
func (it *BTreeIterator) seekToKeyInternal(key []byte, findFirst bool) error {
	it.exhausted = false

	// Use the B-tree's internal search to find the leaf containing this key
	node, keyIndex, found, err := it.findLeafContainingKey(key)
	if err != nil {
		return fmt.Errorf("failed to find key: %w", err)
	}

	if node == nil {
		it.exhausted = true
		return nil
	}

	it.currentNode = node
	it.nodesVisited++

	if found {
		it.currentIndex = keyIndex
	} else if findFirst {
		// Key not found, position at next greater key
		if keyIndex >= int(node.KeyCount) {
			// Need to move to next leaf
			return it.advanceForward()
		}
		it.currentIndex = keyIndex
	} else {
		// Position at previous key
		it.currentIndex = keyIndex - 1
		if it.currentIndex < 0 {
			return it.advanceBackward()
		}
	}

	it.initialized = true
	return nil
}

// Current returns the current entry without advancing
// Returns nil if iterator is not positioned or exhausted
func (it *BTreeIterator) Current() *IteratorEntry {
	it.mutex.Lock()
	defer it.mutex.Unlock()

	if !it.initialized || it.exhausted || it.currentNode == nil {
		return nil
	}

	if it.currentIndex < 0 || it.currentIndex >= int(it.currentNode.KeyCount) {
		return nil
	}

	key := it.currentNode.Keys[it.currentIndex]

	// Check range bounds
	if it.endKey != nil {
		cmp := bytes.Compare(key, it.endKey)
		if cmp > 0 || (cmp == 0 && it.excludeEnd) {
			return nil
		}
	}

	return &IteratorEntry{
		Key:         key,
		DocumentIDs: it.currentNode.Values[it.currentIndex],
	}
}

// Next advances to the next entry and returns it
// Returns nil when exhausted
func (it *BTreeIterator) Next() *IteratorEntry {
	it.mutex.Lock()
	defer it.mutex.Unlock()

	if !it.initialized || it.exhausted {
		return nil
	}

	// Return current and advance
	entry := it.getCurrentEntryInternal()
	if entry == nil {
		it.exhausted = true
		return nil
	}

	it.keysReturned++
	it.advanceForward()

	return entry
}

// Prev moves to the previous entry and returns it
// Returns nil when exhausted
func (it *BTreeIterator) Prev() *IteratorEntry {
	it.mutex.Lock()
	defer it.mutex.Unlock()

	if !it.initialized || it.exhausted {
		return nil
	}

	// Return current and move backward
	entry := it.getCurrentEntryInternal()
	if entry == nil {
		it.exhausted = true
		return nil
	}

	it.keysReturned++
	it.advanceBackward()

	return entry
}

// HasNext returns true if there are more entries in forward direction
func (it *BTreeIterator) HasNext() bool {
	it.mutex.Lock()
	defer it.mutex.Unlock()

	if !it.initialized || it.exhausted || it.currentNode == nil {
		return false
	}

	// Check current position
	if it.currentIndex >= int(it.currentNode.KeyCount) {
		// Would need to advance to next node
		return it.currentNode.NextLeaf != 0
	}

	// Check end bound
	if it.endKey != nil {
		key := it.currentNode.Keys[it.currentIndex]
		cmp := bytes.Compare(key, it.endKey)
		if cmp > 0 || (cmp == 0 && it.excludeEnd) {
			return false
		}
	}

	return true
}

// HasPrev returns true if there are more entries in backward direction
func (it *BTreeIterator) HasPrev() bool {
	it.mutex.Lock()
	defer it.mutex.Unlock()

	if !it.initialized || it.exhausted || it.currentNode == nil {
		return false
	}

	// Check current position
	if it.currentIndex < 0 {
		// Would need to advance to previous node
		return it.currentNode.PrevLeaf != 0
	}

	// Check start bound
	if it.startKey != nil {
		key := it.currentNode.Keys[it.currentIndex]
		cmp := bytes.Compare(key, it.startKey)
		if cmp < 0 || (cmp == 0 && it.excludeStart) {
			return false
		}
	}

	return true
}

// Statistics returns iteration statistics
func (it *BTreeIterator) Statistics() (nodesVisited, keysReturned int) {
	it.mutex.Lock()
	defer it.mutex.Unlock()
	return it.nodesVisited, it.keysReturned
}

// Close releases iterator resources
func (it *BTreeIterator) Close() error {
	it.mutex.Lock()
	defer it.mutex.Unlock()

	it.currentNode = nil
	it.initialized = false
	it.exhausted = true

	it.logger.Debugf("Iterator closed: %d nodes visited, %d keys returned",
		it.nodesVisited, it.keysReturned)

	return nil
}

// Internal helper methods

func (it *BTreeIterator) getCurrentEntryInternal() *IteratorEntry {
	if it.currentNode == nil || it.currentIndex < 0 || it.currentIndex >= int(it.currentNode.KeyCount) {
		return nil
	}

	key := it.currentNode.Keys[it.currentIndex]

	// Check range bounds
	if it.endKey != nil && it.direction == IterForward {
		cmp := bytes.Compare(key, it.endKey)
		if cmp > 0 || (cmp == 0 && it.excludeEnd) {
			return nil
		}
	}

	if it.startKey != nil && it.direction == IterBackward {
		cmp := bytes.Compare(key, it.startKey)
		if cmp < 0 || (cmp == 0 && it.excludeStart) {
			return nil
		}
	}

	return &IteratorEntry{
		Key:         key,
		DocumentIDs: it.currentNode.Values[it.currentIndex],
	}
}

func (it *BTreeIterator) advanceForward() error {
	it.currentIndex++

	// Check if we need to move to next leaf
	for it.currentIndex >= int(it.currentNode.KeyCount) {
		if it.currentNode.NextLeaf == 0 {
			it.exhausted = true
			return nil
		}

		// Load next leaf
		nextNode, err := it.loadNode(it.currentNode.NextLeaf)
		if err != nil {
			return fmt.Errorf("failed to load next leaf: %w", err)
		}

		it.currentNode = nextNode
		it.currentIndex = 0
		it.nodesVisited++
	}

	// Check end bound
	if it.endKey != nil {
		key := it.currentNode.Keys[it.currentIndex]
		cmp := bytes.Compare(key, it.endKey)
		if cmp > 0 || (cmp == 0 && it.excludeEnd) {
			it.exhausted = true
		}
	}

	return nil
}

func (it *BTreeIterator) advanceBackward() error {
	it.currentIndex--

	// Check if we need to move to previous leaf
	for it.currentIndex < 0 {
		if it.currentNode.PrevLeaf == 0 {
			it.exhausted = true
			return nil
		}

		// Load previous leaf
		prevNode, err := it.loadNode(it.currentNode.PrevLeaf)
		if err != nil {
			return fmt.Errorf("failed to load previous leaf: %w", err)
		}

		it.currentNode = prevNode
		it.currentIndex = int(prevNode.KeyCount) - 1
		it.nodesVisited++
	}

	// Check start bound
	if it.startKey != nil {
		key := it.currentNode.Keys[it.currentIndex]
		cmp := bytes.Compare(key, it.startKey)
		if cmp < 0 || (cmp == 0 && it.excludeStart) {
			it.exhausted = true
		}
	}

	return nil
}

func (it *BTreeIterator) findLeftmostLeaf() (*BTreeNode, error) {
	if it.index.rootPageNum == 0 {
		return nil, nil
	}

	node, err := it.loadNode(it.index.rootPageNum)
	if err != nil {
		return nil, err
	}

	// Traverse down the leftmost path to find the first leaf
	for !node.IsLeaf {
		if len(node.Children) == 0 {
			return nil, fmt.Errorf("internal node has no children")
		}
		node, err = it.loadNode(node.Children[0])
		if err != nil {
			return nil, err
		}
		it.nodesVisited++
	}

	return node, nil
}

func (it *BTreeIterator) findRightmostLeaf() (*BTreeNode, error) {
	if it.index.rootPageNum == 0 {
		return nil, nil
	}

	node, err := it.loadNode(it.index.rootPageNum)
	if err != nil {
		return nil, err
	}

	// Traverse down the rightmost path to find the last leaf
	for !node.IsLeaf {
		if len(node.Children) == 0 {
			return nil, fmt.Errorf("internal node has no children")
		}
		lastChild := node.Children[len(node.Children)-1]
		node, err = it.loadNode(lastChild)
		if err != nil {
			return nil, err
		}
		it.nodesVisited++
	}

	return node, nil
}

func (it *BTreeIterator) findLeafContainingKey(key []byte) (*BTreeNode, int, bool, error) {
	if it.index.rootPageNum == 0 {
		return nil, 0, false, nil
	}

	node, err := it.loadNode(it.index.rootPageNum)
	if err != nil {
		return nil, 0, false, err
	}

	// Navigate down to the leaf level
	for !node.IsLeaf {
		// Find the appropriate child
		childIndex := 0
		for i := 0; i < int(node.KeyCount); i++ {
			if bytes.Compare(key, node.Keys[i]) >= 0 {
				childIndex = i + 1
			} else {
				break
			}
		}

		if childIndex >= len(node.Children) {
			childIndex = len(node.Children) - 1
		}

		node, err = it.loadNode(node.Children[childIndex])
		if err != nil {
			return nil, 0, false, err
		}
		it.nodesVisited++
	}

	// Binary search within the leaf
	keyIndex, found := it.binarySearchLeaf(node, key)
	return node, keyIndex, found, nil
}

func (it *BTreeIterator) binarySearchLeaf(node *BTreeNode, key []byte) (int, bool) {
	low, high := 0, int(node.KeyCount)-1

	for low <= high {
		mid := (low + high) / 2
		cmp := bytes.Compare(key, node.Keys[mid])
		if cmp == 0 {
			return mid, true
		} else if cmp < 0 {
			high = mid - 1
		} else {
			low = mid + 1
		}
	}

	return low, false
}

func (it *BTreeIterator) loadNode(pageNum uint32) (*BTreeNode, error) {
	it.index.mutex.RLock()
	defer it.index.mutex.RUnlock()

	pageData, err := it.index.FileManager.ReadPage(pageNum)
	if err != nil {
		return nil, fmt.Errorf("failed to load page %d: %w", pageNum, err)
	}

	node, ok := pageData.(*BTreeNode)
	if !ok {
		return nil, fmt.Errorf("page %d is not a BTreeNode", pageNum)
	}

	return node, nil
}

// IteratorToSlice is a convenience function that collects all entries into a slice
// Use with caution on large ranges as it loads all entries into memory
func IteratorToSlice(it *BTreeIterator) ([]IteratorEntry, error) {
	if err := it.SeekFirst(); err != nil {
		return nil, err
	}

	var entries []IteratorEntry
	for it.HasNext() {
		entry := it.Next()
		if entry != nil {
			entries = append(entries, *entry)
		}
	}

	return entries, nil
}

// IteratorForEach iterates over all entries and calls the function for each
// Returns early if the function returns false
func IteratorForEach(it *BTreeIterator, fn func(entry *IteratorEntry) bool) error {
	if err := it.SeekFirst(); err != nil {
		return err
	}

	for it.HasNext() {
		entry := it.Next()
		if entry == nil {
			break
		}
		if !fn(entry) {
			break
		}
	}

	return nil
}
