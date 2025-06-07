package btreeindex

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sync"
)

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

	// Add to cache if there's room
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
	// If already in cache, just update
	if _, found := bt.pageCache[pageNum]; found {
		bt.pageCache[pageNum] = page
		return
	}

	// If cache is full, evict a page
	if bt.cacheSize >= bt.maxCacheSize && len(bt.pageCache) > 0 {
		// Simple LRU could be implemented here, or Clock Sweep.
		// For now, just remove a random page
		for num := range bt.pageCache {
			delete(bt.pageCache, num)
			break
		}
		bt.cacheSize--
	}

	// Add to cache
	bt.pageCache[pageNum] = page
	bt.cacheSize++
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
