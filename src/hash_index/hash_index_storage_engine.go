package hashindex

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"time"
)

// readPage reads a page from disk, using cache if available
func (hi *HashIndex) readPage(pageNum uint32) (*HashIndexPage, error) {
	// Check if page is in cache
	if page, found := hi.pageCache[pageNum]; found {
		return page, nil
	}

	// Calculate file offset
	offset := int64(pageNum-1) * int64(HashPageSize)

	// Read the page data
	pageData := make([]byte, HashPageSize)
	if _, err := hi.file.ReadAt(pageData, offset); err != nil {
		return nil, fmt.Errorf("failed to read page data: %w", err)
	}

	// Parse the page
	page, err := parseHashPage(pageData)
	if err != nil {
		return nil, fmt.Errorf("failed to parse page: %w", err)
	}

	// Add to cache if there's room
	hi.addToCache(pageNum, page)

	return page, nil
}

// writePage writes a page to disk
func (hi *HashIndex) writePage(pageNum uint32, page *HashIndexPage) error {
	// Serialize the page
	page.PageNum = pageNum
	page.LastUpdated = time.Now()

	pageData, err := serializeHashPage(page)
	if err != nil {
		return fmt.Errorf("failed to serialize page: %w", err)
	}

	// Calculate file offset
	offset := int64(pageNum-1) * int64(HashPageSize)

	// Write the page data
	if _, err := hi.file.WriteAt(pageData, offset); err != nil {
		return fmt.Errorf("failed to write page data: %w", err)
	}

	// Update cache
	hi.addToCache(pageNum, page)

	return nil
}

// writeMetaPage writes the metadata to page 0
func (hi *HashIndex) writeMetaPage(page *HashIndexPage) error {
	// Serialize metadata
	metadataBytes, err := serializeHashMetadata(&hi.metadata)
	if err != nil {
		return fmt.Errorf("failed to serialize metadata: %w", err)
	}

	// Create a meta page item to store metadata
	metaItem := HashIndexItem{
		HashValue: 0,
		Key:       []byte("metadata"),
		DocID:     "",
		TID:       0,
	}

	page.Items = []HashIndexItem{metaItem}
	page.ItemCount = 1

	// Serialize the page with a special marker for metadata
	buffer := new(bytes.Buffer)

	// Write standard page header
	binary.Write(buffer, binary.LittleEndian, uint32(HashMetaPage))
	binary.Write(buffer, binary.LittleEndian, uint32(0)) // Page 0
	binary.Write(buffer, binary.LittleEndian, uint32(0)) // No next page
	binary.Write(buffer, binary.LittleEndian, uint16(1)) // One item (metadata)
	binary.Write(buffer, binary.LittleEndian, uint16(0)) // Free space not used for meta

	// Write current time
	timeBytes, _ := time.Now().MarshalBinary()
	binary.Write(buffer, binary.LittleEndian, uint32(len(timeBytes)))
	buffer.Write(timeBytes)

	// Write "METADATA" marker
	marker := []byte("METADATA")
	binary.Write(buffer, binary.LittleEndian, uint32(len(marker)))
	buffer.Write(marker)

	// Write metadata length and content
	binary.Write(buffer, binary.LittleEndian, uint32(len(metadataBytes)))
	buffer.Write(metadataBytes)

	// Pad to page size
	if buffer.Len() < HashPageSize {
		padding := make([]byte, HashPageSize-buffer.Len())
		buffer.Write(padding)
	}

	// Write the meta page
	if _, err := hi.file.WriteAt(buffer.Bytes(), 0); err != nil {
		return fmt.Errorf("failed to write meta page: %w", err)
	}

	hi.dirty = false
	return nil
}

// Close flushes any pending changes and closes the index file
func (hi *HashIndex) Close() error {
	hi.Lock()
	defer hi.Unlock()

	// Update metadata if dirty
	if hi.dirty {
		metaPage := &HashIndexPage{
			PageType:  HashMetaPage,
			PageNum:   0,
			ItemCount: 0,
		}

		if err := hi.writeMetaPage(metaPage); err != nil {
			hi.logger.Errorf("Failed to write meta page during close: %v", err)
		}
	}

	// Close the file
	if hi.file != nil {
		return hi.file.Close()
	}

	return nil
}

// addToCache adds a page to the cache, evicting if necessary
func (hi *HashIndex) addToCache(pageNum uint32, page *HashIndexPage) {
	// If already in cache, just update
	if _, found := hi.pageCache[pageNum]; found {
		hi.pageCache[pageNum] = page
		return
	}

	// If cache is full, evict a page
	if hi.cacheSize >= hi.maxCacheSize && len(hi.pageCache) > 0 {
		// Simple eviction strategy - remove random page
		for num := range hi.pageCache {
			delete(hi.pageCache, num)
			break
		}
		hi.cacheSize--
	}

	// Add to cache
	hi.pageCache[pageNum] = page
	hi.cacheSize++
}

// parseHashPage deserializes a page from bytes
func parseHashPage(data []byte) (*HashIndexPage, error) {
	if len(data) < 16 {
		return nil, fmt.Errorf("page data too short")
	}

	reader := bytes.NewReader(data)

	// Read page header
	var pageType uint32
	var pageNum uint32
	var nextPage uint32
	var itemCount uint16
	var freeSpace uint16

	binary.Read(reader, binary.LittleEndian, &pageType)
	binary.Read(reader, binary.LittleEndian, &pageNum)
	binary.Read(reader, binary.LittleEndian, &nextPage)
	binary.Read(reader, binary.LittleEndian, &itemCount)
	binary.Read(reader, binary.LittleEndian, &freeSpace)

	// Read last updated timestamp
	var timeLen uint32
	binary.Read(reader, binary.LittleEndian, &timeLen)

	timeBytes := make([]byte, timeLen)
	reader.Read(timeBytes)

	var lastUpdated time.Time
	lastUpdated.UnmarshalBinary(timeBytes)

	page := &HashIndexPage{
		PageType:    int(pageType),
		PageNum:     pageNum,
		NextPage:    nextPage,
		ItemCount:   itemCount,
		FreeSpace:   freeSpace,
		LastUpdated: lastUpdated,
		Items:       make([]HashIndexItem, 0, itemCount),
	}

	// Special handling for meta page
	if pageType == HashMetaPage {
		// Read metadata marker
		var markerLen uint32
		binary.Read(reader, binary.LittleEndian, &markerLen)

		marker := make([]byte, markerLen)
		reader.Read(marker)

		if string(marker) != "METADATA" {
			return nil, fmt.Errorf("invalid metadata marker")
		}

		// Skip actual metadata - it's handled separately
		return page, nil
	}

	// Read items
	for i := uint16(0); i < itemCount; i++ {
		var hashValue uint32
		binary.Read(reader, binary.LittleEndian, &hashValue)

		// Read key
		var keyLen uint32
		binary.Read(reader, binary.LittleEndian, &keyLen)

		key := make([]byte, keyLen)
		reader.Read(key)

		// Read DocID
		var docIDLen uint32
		binary.Read(reader, binary.LittleEndian, &docIDLen)

		docID := make([]byte, docIDLen)
		reader.Read(docID)

		// Read TID
		var tid uint64
		binary.Read(reader, binary.LittleEndian, &tid)

		page.Items = append(page.Items, HashIndexItem{
			HashValue: hashValue,
			Key:       key,
			DocID:     string(docID),
			TID:       tid,
		})
	}

	return page, nil
}

// serializeHashPage serializes a page to bytes
func serializeHashPage(page *HashIndexPage) ([]byte, error) {
	buffer := new(bytes.Buffer)

	// Write page header
	binary.Write(buffer, binary.LittleEndian, uint32(page.PageType))
	binary.Write(buffer, binary.LittleEndian, page.PageNum)
	binary.Write(buffer, binary.LittleEndian, page.NextPage)
	binary.Write(buffer, binary.LittleEndian, page.ItemCount)
	binary.Write(buffer, binary.LittleEndian, page.FreeSpace)

	// Write timestamp
	timeBytes, err := page.LastUpdated.MarshalBinary()
	if err != nil {
		return nil, err
	}

	binary.Write(buffer, binary.LittleEndian, uint32(len(timeBytes)))
	buffer.Write(timeBytes)

	// Write items
	for _, item := range page.Items {
		// Hash value
		binary.Write(buffer, binary.LittleEndian, item.HashValue)

		// Key
		binary.Write(buffer, binary.LittleEndian, uint32(len(item.Key)))
		buffer.Write(item.Key)

		// DocID
		docIDBytes := []byte(item.DocID)
		binary.Write(buffer, binary.LittleEndian, uint32(len(docIDBytes)))
		buffer.Write(docIDBytes)

		// TID
		binary.Write(buffer, binary.LittleEndian, item.TID)
	}

	// Pad to page size
	if buffer.Len() < HashPageSize {
		padding := make([]byte, HashPageSize-buffer.Len())
		buffer.Write(padding)
	} else if buffer.Len() > HashPageSize {
		return nil, fmt.Errorf("serialized page exceeds page size: %d > %d", buffer.Len(), HashPageSize)
	}

	return buffer.Bytes(), nil
}

// serializeHashMetadata serializes metadata to bytes
func serializeHashMetadata(metadata *HashIndexMetadata) ([]byte, error) {
	buffer := new(bytes.Buffer)

	binary.Write(buffer, binary.LittleEndian, metadata.MaxBucket)
	binary.Write(buffer, binary.LittleEndian, metadata.HighMask)
	binary.Write(buffer, binary.LittleEndian, metadata.LowMask)
	binary.Write(buffer, binary.LittleEndian, metadata.FillFactor)
	binary.Write(buffer, binary.LittleEndian, metadata.NumTuples)
	binary.Write(buffer, binary.LittleEndian, metadata.BitmapPages)
	binary.Write(buffer, binary.LittleEndian, metadata.OverflowPages)

	// Index field name
	fieldBytes := []byte(metadata.IndexField)
	binary.Write(buffer, binary.LittleEndian, uint32(len(fieldBytes)))
	buffer.Write(fieldBytes)

	// Is unique flag
	if metadata.IsUnique {
		buffer.WriteByte(1)
	} else {
		buffer.WriteByte(0)
	}

	// Created timestamp
	timeBytes, err := metadata.Created.MarshalBinary()
	if err != nil {
		return nil, err
	}

	binary.Write(buffer, binary.LittleEndian, uint32(len(timeBytes)))
	buffer.Write(timeBytes)

	return buffer.Bytes(), nil
}

// deserializeHashMetadata deserializes metadata from bytes
func deserializeHashMetadata(data []byte) (*HashIndexMetadata, error) {
	reader := bytes.NewReader(data)

	var metadata HashIndexMetadata

	binary.Read(reader, binary.LittleEndian, &metadata.MaxBucket)
	binary.Read(reader, binary.LittleEndian, &metadata.HighMask)
	binary.Read(reader, binary.LittleEndian, &metadata.LowMask)
	binary.Read(reader, binary.LittleEndian, &metadata.FillFactor)
	binary.Read(reader, binary.LittleEndian, &metadata.NumTuples)
	binary.Read(reader, binary.LittleEndian, &metadata.BitmapPages)
	binary.Read(reader, binary.LittleEndian, &metadata.OverflowPages)

	// Index field name
	var fieldLen uint32
	binary.Read(reader, binary.LittleEndian, &fieldLen)

	fieldBytes := make([]byte, fieldLen)
	reader.Read(fieldBytes)
	metadata.IndexField = string(fieldBytes)

	// Is unique flag
	uniqueByte, _ := reader.ReadByte()
	metadata.IsUnique = (uniqueByte == 1)

	// Created timestamp
	var timeLen uint32
	binary.Read(reader, binary.LittleEndian, &timeLen)

	timeBytes := make([]byte, timeLen)
	reader.Read(timeBytes)

	metadata.Created.UnmarshalBinary(timeBytes)

	return &metadata, nil
}
