package hash

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"time"
)

// readPage reads a page from disk, using cache if available
func (hi *HashIndexFile) ReadPage(pageNum uint32) (*HashIndexPage, error) {
	// Check if page is in cache
	if page, found := hi.PageCache[pageNum]; found {
		return page, nil
	}

	var file_err error
	if hi.File == nil {

		hi.File, file_err = os.OpenFile(hi.FilePath, os.O_RDWR|os.O_CREATE, 0755)
		if file_err != nil {
			return nil, fmt.Errorf("INSERT.ReadPage:: failed to open hash index file: %w", file_err)
		}
	} else {
		_, err := hi.File.Stat()
		if err != nil {
			hi.File, file_err = os.OpenFile(hi.FilePath, os.O_RDWR|os.O_CREATE, 0755)
			if file_err != nil {
				return nil, fmt.Errorf("INSERT.ReadPage2:: failed to open hash index file: %w", file_err)
			}
		} else {
			fmt.Println("File is open and valid.")
		}
	}

	if hi.File == nil {
		return nil, fmt.Errorf("INSERT.ReadPage:: hash index file is not open, COULD NOT BECAUSE")
	}

	// TODO: When the an index is brand new, it's empty. When a document is added to the bundle
	// it will try to load the index, but it won't have any pages yet. We need to handle this case.
	var offset int64
	if pageNum == 0 {
		offset = 0
		//return nil, fmt.Errorf("cannot read page 0, it is reserved for metadata")
	} else {
		// Calculate file offset
		offset = int64(pageNum-1) * int64(HashPageSize)
	}

	// Read the page data
	pageData := make([]byte, HashPageSize)
	if _, err := hi.File.ReadAt(pageData, offset); err != nil {
		return nil, fmt.Errorf("failed to read page data: %w", err)
	}

	// Parse the page
	page, err := ParseHashPage(pageData)
	if err != nil {
		return nil, fmt.Errorf("failed to parse page: %w", err)
	}

	// Add to cache if there's room
	hi.AddToCache(pageNum, page)

	return page, nil
}

// writePage writes a page to disk
func (hi *HashIndexFile) WritePage(pageNum uint32, page *HashIndexPage) error {
	// Serialize the page
	page.PageNum = pageNum
	page.LastUpdated = time.Now()

	pageData, err := SerializeHashPage(page)
	if err != nil {
		return fmt.Errorf("failed to serialize page: %w", err)
	}

	// Calculate file offset
	offset := int64(pageNum-1) * int64(HashPageSize)

	// Write the page data
	if _, err := hi.File.WriteAt(pageData, offset); err != nil {
		return fmt.Errorf("failed to write page data: %w", err)
	}

	// Update cache
	hi.AddToCache(pageNum, page)

	return nil
}

// writeMetaPage writes the metadata to page 0
func (hi *HashIndexFile) WriteMetaPage(page *HashIndexPage) error {
	// Serialize metadata
	metadataBytes, err := SerializeHashMetadata(&hi.Metadata)
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
	if _, err := hi.File.WriteAt(buffer.Bytes(), 0); err != nil {
		return fmt.Errorf("failed to write meta page: %w", err)
	}

	hi.Dirty = false
	return nil
}

func (hi *HashIndexFile) TempReadMetaPage() (*HashIndexMetadata, error) {
	// Read the first page (meta page)
	pageData := make([]byte, HashPageSize*4)
	_, err := hi.File.ReadAt(pageData, 0)

	if err != nil {
		return nil, fmt.Errorf("failed to read meta page: %w", err)
	}

	// Print raw bytes for debugging
	fmt.Printf("RAW DATA: % x\n", pageData)

	// Parse the metadata from the page
	return nil, nil
}

func (hi *HashIndexFile) ReadMetaPage() (*HashIndexMetadata, error) {
	pageData := make([]byte, HashPageSize)
	if _, err := hi.File.ReadAt(pageData, 0); err != nil {
		return nil, fmt.Errorf("failed to read meta page: %w", err)
	}

	// Print raw bytes for debugging
	//fmt.Printf("Raw pageData bytes: % x\n", pageData)

	reader := bytes.NewReader(pageData)

	// Read standard page header
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
	fmt.Printf("Read meta page: type=%d, num=%d, next=%d, items=%d, free=%d\n", pageType, pageNum, nextPage, itemCount, freeSpace)
	// Read current time
	var timeLen uint32
	binary.Read(reader, binary.LittleEndian, &timeLen)
	timeBytes := make([]byte, timeLen)
	reader.Read(timeBytes)

	// Read "METADATA" marker
	var markerLen uint32
	binary.Read(reader, binary.LittleEndian, &markerLen)

	marker := make([]byte, markerLen)
	reader.Read(marker)
	fmt.Println("Marker:", string(marker))
	if string(marker) != "METADATA" {
		return nil, fmt.Errorf("invalid metadata marker: %s", string(marker))
	}

	// Read metadata length and content
	var metadataLen uint32
	binary.Read(reader, binary.LittleEndian, &metadataLen)
	metadataBytes := make([]byte, metadataLen)
	reader.Read(metadataBytes)

	// Deserialize metadata
	metadata, err := DeserializeHashMetadata(metadataBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to deserialize metadata: %w", err)
	}

	return metadata, nil
}

// Close flushes any pending changes and closes the index file
func (hi *HashIndexFile) Close() error {
	hi.Lock()
	defer hi.Unlock()

	// Update metadata if dirty
	if hi.Dirty {
		metaPage := &HashIndexPage{
			PageType:  HashMetaPage,
			PageNum:   0,
			ItemCount: 0,
		}

		if err := hi.WriteMetaPage(metaPage); err != nil {
			hi.Logger.Errorf("Failed to write meta page during close: %v", err)
		}
	}

	// Close the file
	if hi.File != nil {
		return hi.File.Close()
	}

	return nil
}

// addToCache adds a page to the cache, evicting if necessary
func (hi *HashIndexFile) AddToCache(pageNum uint32, page *HashIndexPage) {
	// If already in cache, just update
	if _, found := hi.PageCache[pageNum]; found {
		hi.PageCache[pageNum] = page
		return
	}

	// If cache is full, evict a page
	if hi.CacheSize >= hi.MaxCacheSize && len(hi.PageCache) > 0 {
		// Simple eviction strategy - remove random page
		for num := range hi.PageCache {
			delete(hi.PageCache, num)
			break
		}
		hi.CacheSize--
	}

	// Add to cache
	hi.PageCache[pageNum] = page
	hi.CacheSize++
}

// parseHashPage deserializes a page from bytes
func ParseHashPage(data []byte) (*HashIndexPage, error) {
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
func SerializeHashPage(page *HashIndexPage) ([]byte, error) {
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
func SerializeHashMetadata(metadata *HashIndexMetadata) ([]byte, error) {
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
func DeserializeHashMetadata(data []byte) (*HashIndexMetadata, error) {
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
