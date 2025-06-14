package btreeindex

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sync"
)

/*

This b-Tree index implementation uses the Clock sweep algorithm for page replacement.
# Clock Sweep Algorithm Implementation for B-Tree Page Cache

The Clock Sweep algorithm is an excellent approximation of LRU that's more efficient to implement.

## How the Clock Sweep Algorithm Works

1. **Conceptual Model**: Imagine all cached pages arranged in a circular list with a clock hand pointing to one page.

2. **Access Tracking**: When a page is accessed, its access flag is set to `true`.

3. **Eviction Process**:
   - The clock hand moves clockwise around the circle
   - For each page it encounters:
     - If access flag is `true`: Give the page another chance by setting the flag to `false` and moving the hand
     - If access flag is `false`: Evict this page and stop

4. **Second-Chance Policy**: Pages that were recently accessed (flag=`true`) get a "second chance" before eviction.

## Advantages Over Your Current Implementation

1. **Better Approximation of LRU**: Clock sweep approximates LRU well without the overhead of maintaining access timestamps or a linked list.

2. **Constant-Time Operations**: Adding and evicting pages is O(1) amortized time.

3. **Scan-Resistant**: Unlike simple LRU, clock sweep is somewhat resistant to sequential scans overwhelming the cache.

4. **Dirty Page Handling**: The implementation writes dirty pages to disk before eviction to preserve changes.

## Additional Optimization Suggestions

1. **Pre-allocation**: Pre-allocate the clockEntries array to the maximum cache size to avoid resizing operations.

2. **Dirty Bit Queueing**: You could add a separate queue for dirty pages to batch write operations.

3. **Reference Counting**: For advanced usage patterns, you could add reference counting to prevent eviction of pages currently in use.

4. **Fine-Grained Locking**: If performance becomes an issue, consider more fine-grained locking around the cache operations.

This implementation provides an excellent balance of simplicity and effectiveness for your B-tree page cache. It's a standard algorithm used in many database systems, including some variations of PostgreSQL.

Additional Optimization Ideas
Pre-allocation: Pre-allocate the clockEntries array to the maximum cache size to avoid resizing operations.

Dirty Bit Queueing: You could add a separate queue for dirty pages to batch write operations.

Reference Counting: Potentially add reference counting to prevent eviction of pages currently in use.

Fine-Grained Locking: If performance becomes an issue, maybe use more fine-grained locking around the cache operations.


*/

// BTreeFile represents a file-backed B-tree index
type BTreeFile struct {
	sync.RWMutex
	path         string
	file         *os.File
	metaPage     *BTreePage
	rootPageNum  uint32
	height       uint16
	pageCache    map[uint32]*BTreePage
	cacheSize    int
	maxCacheSize int

	// Clock sweep algorithm implementation fields
	clockHand    int             // Current position in the clock hand
	clockEntries []uint32        // Array of page numbers in clock order
	accessFlags  map[uint32]bool // Tracks whether pages were accessed since last check

}

// OpenBTreeFile opens an existing B-tree index file
func OpenBTreeFile(path string, cacheSize int) (*BTreeFile, error) {
	file, err := os.OpenFile(path, os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open B-tree file: %w", err)
	}

	btree := &BTreeFile{
		path:         path,
		file:         file,
		pageCache:    make(map[uint32]*BTreePage),
		cacheSize:    0,
		maxCacheSize: cacheSize,
		clockHand:    0,
		clockEntries: make([]uint32, 0, cacheSize),
		accessFlags:  make(map[uint32]bool),
	}

	// Read the meta page
	metaPage, err := btree.readPage(0)
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to read meta page: %w", err)
	}

	// Parse metadata
	if len(metaPage.Entries) == 0 {
		file.Close()
		return nil, fmt.Errorf("invalid meta page: no entries")
	}

	metadata, err := decodeMetadata(metaPage.Entries[0].Value)
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to decode metadata: %w", err)
	}

	btree.metaPage = metaPage
	btree.rootPageNum = metadata.rootPage
	btree.height = metadata.height

	return btree, nil
}

// Close closes the B-tree file
func (bt *BTreeFile) Close() error {
	bt.Lock()
	defer bt.Unlock()

	bt.pageCache = nil
	if bt.file != nil {
		return bt.file.Close()
	}
	return nil
}

// Flush writes all cached pages to disk
func (bt *BTreeFile) Flush() error {
	bt.Lock()
	defer bt.Unlock()

	// Write all dirty pages to disk
	for pageNum, page := range bt.pageCache {
		if err := bt.writePage(pageNum, page); err != nil {
			return fmt.Errorf("failed to write page %d: %w", pageNum, err)
		}
	}
	return nil
}

// Find searches for a key in the B-tree
func (bt *BTreeFile) Find(key []byte) (*IndexTuple, error) {
	bt.RLock()
	defer bt.RUnlock()

	// Start from the root page
	pageNum := bt.rootPageNum

	// Traverse down the tree
	for level := int(bt.height) - 1; level >= 0; level-- {
		page, err := bt.readPage(pageNum)
		if err != nil {
			return nil, fmt.Errorf("failed to read page %d: %w", pageNum, err)
		}

		// If this is a leaf page, search for the key
		if page.Level == 0 {
			for _, entry := range page.Entries {
				if bytes.Equal(entry.Key, key) {
					// Found the key
					tid, err := decodeTID(entry.Value)
					if err != nil {
						return nil, err
					}

					return &IndexTuple{
						Key:   entry.Key,
						DocID: entry.DocID,
						TID:   tid,
					}, nil
				}
			}
			// Key not found
			return nil, nil
		}

		// For internal nodes, find the child page to follow
		// Search for the first entry where key < entry.Key
		childPageNum := uint32(0)
		for i, entry := range page.Entries {
			// If this is the first entry, it's the leftmost child
			if i == 0 {
				childPageNum = decodeChildPointer(entry.Value)
			}

			// If key < entry.Key, follow the previous child
			if bytes.Compare(key, entry.Key) < 0 {
				break
			}

			// Otherwise, follow this child
			childPageNum = decodeChildPointer(entry.Value)
		}

		pageNum = childPageNum
	}

	// Key not found
	return nil, nil
}

// FindRange searches for all keys in a given range
func (bt *BTreeFile) FindRange(startKey, endKey []byte) ([]*IndexTuple, error) {
	bt.RLock()
	defer bt.RUnlock()

	var results []*IndexTuple

	// Find the leaf page containing startKey (or the first key greater than startKey)
	leafPage, err := bt.findLeafPage(startKey)
	if err != nil {
		return nil, err
	}

	// Keep scanning until we reach endKey or run out of pages
	currentPage := leafPage
	done := false

	for !done {
		for _, entry := range currentPage.Entries {
			// If we've passed endKey, we're done
			if endKey != nil && bytes.Compare(entry.Key, endKey) > 0 {
				done = true
				break
			}

			// If the key is >= startKey, add it to results
			if startKey == nil || bytes.Compare(entry.Key, startKey) >= 0 {
				tid, err := decodeTID(entry.Value)
				if err != nil {
					return nil, err
				}

				results = append(results, &IndexTuple{
					Key:   entry.Key,
					DocID: entry.DocID,
					TID:   tid,
				})
			}
		}

		// If there's no next page or we're done, break
		if currentPage.NextPage == 0 || done {
			break
		}

		// Read the next leaf page
		currentPage, err = bt.readPage(currentPage.NextPage)
		if err != nil {
			return nil, fmt.Errorf("failed to read next leaf page: %w", err)
		}
	}

	return results, nil
}

// findLeafPage finds the leaf page that would contain the given key
func (bt *BTreeFile) findLeafPage(key []byte) (*BTreePage, error) {
	// Start from the root
	pageNum := bt.rootPageNum

	// Traverse down the tree
	for level := int(bt.height) - 1; level > 0; level-- {
		page, err := bt.readPage(pageNum)
		if err != nil {
			return nil, fmt.Errorf("failed to read page %d: %w", pageNum, err)
		}

		// Find the child page to follow
		childPageNum := uint32(0)
		for i, entry := range page.Entries {
			// If this is the first entry, it's the leftmost child
			if i == 0 {
				childPageNum = decodeChildPointer(entry.Value)
			}

			// If key < entry.Key, follow the previous child
			if key != nil && bytes.Compare(key, entry.Key) < 0 {
				break
			}

			// Otherwise, follow this child
			childPageNum = decodeChildPointer(entry.Value)
		}

		pageNum = childPageNum
	}

	// Read the leaf page
	return bt.readPage(pageNum)
}

// readPage reads a page from the B-tree file, using cache if available
func (bt *BTreeFile) readPage(pageNum uint32) (*BTreePage, error) {
	// Check if page is in cache
	if page, found := bt.pageCache[pageNum]; found {
		// Mark the page as accessed
		bt.accessFlags[pageNum] = true
		return page, nil
	}

	// Calculate file offset
	offset := int64(pageNum) * int64(BTreePageSize)

	// Read the page data
	pageData := make([]byte, BTreePageSize)
	if _, err := bt.file.ReadAt(pageData, offset); err != nil {
		return nil, fmt.Errorf("failed to read page data: %w", err)
	}

	// Parse the page
	page, err := parsePage(pageData)
	if err != nil {
		return nil, fmt.Errorf("failed to parse page: %w", err)
	}

	// Add to cache
	bt.addToCache(pageNum, page)

	return page, nil
}

// writePage writes a page to the B-tree file
func (bt *BTreeFile) writePage(pageNum uint32, page *BTreePage) error {
	// Serialize the page
	pageData, err := serializePage(page)
	if err != nil {
		return fmt.Errorf("failed to serialize page: %w", err)
	}

	// Calculate file offset
	offset := int64(pageNum) * int64(BTreePageSize)

	// Write the page data
	if _, err := bt.file.WriteAt(pageData, offset); err != nil {
		return fmt.Errorf("failed to write page data: %w", err)
	}

	// Add/update in cache
	bt.addToCache(pageNum, page)

	return nil
}

// addToCache adds a page to the cache, evicting if necessary
func (bt *BTreeFile) addToCache(pageNum uint32, page *BTreePage) {
	// If already in cache, just update the page
	if _, found := bt.pageCache[pageNum]; found {
		bt.pageCache[pageNum] = page
		bt.accessFlags[pageNum] = true
		return
	}

	// If cache is full, evict a page using clock sweep
	if bt.cacheSize >= bt.maxCacheSize {
		bt.evictPage()
	}

	// Add new page to cache
	bt.pageCache[pageNum] = page
	bt.clockEntries = append(bt.clockEntries, pageNum)
	bt.accessFlags[pageNum] = true
	bt.cacheSize++
}

func (bt *BTreeFile) evictPage() {
	// If cache is empty, nothing to evict
	if len(bt.clockEntries) == 0 {
		return
	}

	// Perform clock sweep until we find a page to evict
	for {
		// Move the clock hand
		bt.clockHand = (bt.clockHand + 1) % len(bt.clockEntries)
		pageNum := bt.clockEntries[bt.clockHand]

		// Check access flag
		if bt.accessFlags[pageNum] {
			// Page was accessed since last check, give it a second chance
			bt.accessFlags[pageNum] = false
		} else {
			// Page wasn't accessed, evict it

			// Check if the page is dirty and needs to be written to disk
			page := bt.pageCache[pageNum]
			if page.IsDirty {
				// Write the page to disk before evicting
				if err := bt.writePageToDisk(pageNum, page); err != nil {
					// Log the error but continue with eviction
					// In production, you might want different error handling
					fmt.Printf("Error writing page %d to disk: %v\n", pageNum, err)
				}
			}

			// Remove from cache structures
			delete(bt.pageCache, pageNum)
			delete(bt.accessFlags, pageNum)

			// Remove from clock entries by replacing with the last entry and truncating
			lastIdx := len(bt.clockEntries) - 1
			bt.clockEntries[bt.clockHand] = bt.clockEntries[lastIdx]
			bt.clockEntries = bt.clockEntries[:lastIdx]

			// Adjust clock hand if needed
			if bt.clockHand >= len(bt.clockEntries) {
				bt.clockHand = 0
			}

			bt.cacheSize--
			return
		}
	}
}

func (bt *BTreeFile) writePageToDisk(pageNum uint32, page *BTreePage) error {
	// Serialize the page
	pageData, err := serializePage(page)
	if err != nil {
		return fmt.Errorf("failed to serialize page: %w", err)
	}

	// Calculate file offset
	offset := int64(pageNum) * int64(BTreePageSize)

	// Write the page data
	if _, err := bt.file.WriteAt(pageData, offset); err != nil {
		return fmt.Errorf("failed to write page data: %w", err)
	}

	// Mark as clean
	page.IsDirty = false

	return nil
}

// parsePage deserializes a page from bytes
func parsePage(data []byte) (*BTreePage, error) {
	if len(data) < 24 {
		return nil, fmt.Errorf("page data too short")
	}

	reader := bytes.NewReader(data)

	// Read page header
	var pageType uint32
	var pageNum uint32
	var parentPage uint32
	var prevPage uint32
	var nextPage uint32
	var level uint16
	var numEntries uint16
	var freeSpace uint16

	binary.Read(reader, binary.LittleEndian, &pageType)
	binary.Read(reader, binary.LittleEndian, &pageNum)
	binary.Read(reader, binary.LittleEndian, &parentPage)
	binary.Read(reader, binary.LittleEndian, &prevPage)
	binary.Read(reader, binary.LittleEndian, &nextPage)
	binary.Read(reader, binary.LittleEndian, &level)
	binary.Read(reader, binary.LittleEndian, &numEntries)
	binary.Read(reader, binary.LittleEndian, &freeSpace)

	page := &BTreePage{
		PageType:   int(pageType),
		PageNum:    pageNum,
		ParentPage: parentPage,
		PrevPage:   prevPage,
		NextPage:   nextPage,
		Level:      level,
		NumEntries: numEntries,
		FreeSpace:  freeSpace,
		Entries:    make([]BTreeEntry, 0, numEntries),
	}

	// Read entries
	for i := uint16(0); i < numEntries; i++ {
		// Read key length
		var keyLen uint32
		if err := binary.Read(reader, binary.LittleEndian, &keyLen); err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}

		// Read key
		key := make([]byte, keyLen)
		if _, err := io.ReadFull(reader, key); err != nil {
			return nil, err
		}

		// Read value length
		var valueLen uint32
		if err := binary.Read(reader, binary.LittleEndian, &valueLen); err != nil {
			return nil, err
		}

		// Read value
		value := make([]byte, valueLen)
		if _, err := io.ReadFull(reader, value); err != nil {
			return nil, err
		}

		// Create entry
		entry := BTreeEntry{
			Key:   key,
			Value: value,
		}

		// If this is a leaf entry, try to decode DocID from Value
		if page.Level == 0 && len(value) >= 8 {
			// Extract TID and DocID if present
			// In a real implementation, you'd store these in the value
		}

		page.Entries = append(page.Entries, entry)
	}

	return page, nil
}

// serializePage serializes a page to bytes
func serializePage(page *BTreePage) ([]byte, error) {
	buffer := new(bytes.Buffer)

	// Write page header
	binary.Write(buffer, binary.LittleEndian, uint32(page.PageType))
	binary.Write(buffer, binary.LittleEndian, page.PageNum)
	binary.Write(buffer, binary.LittleEndian, page.ParentPage)
	binary.Write(buffer, binary.LittleEndian, page.PrevPage)
	binary.Write(buffer, binary.LittleEndian, page.NextPage)
	binary.Write(buffer, binary.LittleEndian, page.Level)
	binary.Write(buffer, binary.LittleEndian, page.NumEntries)
	binary.Write(buffer, binary.LittleEndian, page.FreeSpace)

	// Write entries
	for _, entry := range page.Entries {
		// Write key
		binary.Write(buffer, binary.LittleEndian, uint32(len(entry.Key)))
		buffer.Write(entry.Key)

		// Write value
		binary.Write(buffer, binary.LittleEndian, uint32(len(entry.Value)))
		buffer.Write(entry.Value)
	}

	// Pad to page size
	if buffer.Len() < BTreePageSize {
		padding := make([]byte, BTreePageSize-buffer.Len())
		buffer.Write(padding)
	} else if buffer.Len() > BTreePageSize {
		return nil, fmt.Errorf("serialized page exceeds page size: %d > %d", buffer.Len(), BTreePageSize)
	}

	return buffer.Bytes(), nil
}

// Helper functions

// decodeChildPointer extracts a child page pointer from a value
func decodeChildPointer(value []byte) uint32 {
	if len(value) < 4 {
		return 0
	}
	return binary.LittleEndian.Uint32(value)
}

// decodeTID extracts a TID from a value
func decodeTID(value []byte) (uint64, error) {
	if len(value) < 8 {
		return 0, fmt.Errorf("value too short to contain TID")
	}
	return binary.LittleEndian.Uint64(value), nil
}

func (bt *BTreeFile) markPageDirty(pageNum uint32) {
	if page, found := bt.pageCache[pageNum]; found {
		page.IsDirty = true
	}
}

// Metadata structure for decoding the meta page
type btreeMetadata struct {
	rootPage   uint32
	height     uint16
	totalPages uint32
	indexField string
	isUnique   bool
	collation  string
	created    string
}

// decodeMetadata decodes metadata from bytes
func decodeMetadata(data []byte) (btreeMetadata, error) {
	result := btreeMetadata{}
	buffer := bytes.NewReader(data)

	// Read number of items
	var itemCount uint32
	if err := binary.Read(buffer, binary.LittleEndian, &itemCount); err != nil {
		return result, err
	}

	// Read each key-value pair
	for i := uint32(0); i < itemCount; i++ {
		// Read key length
		var keyLen uint32
		if err := binary.Read(buffer, binary.LittleEndian, &keyLen); err != nil {
			return result, err
		}

		// Read key
		keyBytes := make([]byte, keyLen)
		if _, err := io.ReadFull(buffer, keyBytes); err != nil {
			return result, err
		}
		key := string(keyBytes)

		// Read value length
		var valueLen uint32
		if err := binary.Read(buffer, binary.LittleEndian, &valueLen); err != nil {
			return result, err
		}

		// Read value
		valueBytes := make([]byte, valueLen)
		if _, err := io.ReadFull(buffer, valueBytes); err != nil {
			return result, err
		}
		value := string(valueBytes)

		// Set the appropriate field based on the key
		switch key {
		case "rootPage":
			pageNum := uint32(0)
			fmt.Sscanf(value, "%d", &pageNum)
			result.rootPage = pageNum
		case "height":
			height := uint16(0)
			fmt.Sscanf(value, "%d", &height)
			result.height = height
		case "totalPages":
			pages := uint32(0)
			fmt.Sscanf(value, "%d", &pages)
			result.totalPages = pages
		case "indexField":
			result.indexField = value
		case "isUnique":
			result.isUnique = (value == "true")
		case "collation":
			result.collation = value
		case "created":
			result.created = value
		}
	}

	return result, nil
}
