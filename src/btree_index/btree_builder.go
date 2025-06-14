package btreeindex

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syndrdb/src/helpers"
)

// Constants for B-tree structure
const (
	// Page size similar to PostgreSQL's default (8KB)
	BTreePageSize = 8192

	// Page types
	BTreeMetaPage  = 0
	BTreeRootPage  = 1
	BTreeInnerPage = 2
	BTreeLeafPage  = 3

	// Fill factor (percentage of page to fill, leaving room for future insertions)
	BTreeFillFactor = 70

	// Maximum entries per page (will be calculated based on average entry size)
	// This is a ballpark estimate; actual value depends on key size
	// TODO Determine the real key size dynamically
	BTreeDefaultMaxKeysPerPage = 100
)

// BTreePage represents a page in the B-tree
type BTreePage struct {
	PageType   int
	PageNum    uint32
	ParentPage uint32
	PrevPage   uint32
	NextPage   uint32
	Level      uint16 // Distance from leaf (0 = leaf)
	NumEntries uint16
	FreeSpace  uint16
	Entries    []BTreeEntry
	IsDirty    bool // Indicates if the page has been modified since last write
}

// BTreeEntry represents a single entry in a B-tree page
type BTreeEntry struct {
	Key   []byte
	Value []byte // For leaf nodes: tuple reference; for internal nodes: child page pointer
	DocID string // Only for leaf entries
	TID   uint64 // Only for leaf entries
}

// BTreeIndex represents a complete B-tree index
type BTreeIndex struct {
	Name       string
	FileName   string
	MetaPage   BTreePage
	RootPage   uint32
	Height     uint16
	TotalPages uint32
	PageSize   uint32
	IndexField IndexField
}

// buildBTreeFromTuples constructs a B-tree from sorted tuples
func (bts *BTreeService) buildBTreeFromTuples(indexName string, tuples []IndexTuple, indexField IndexField) (*BTreeIndex, error) {
	// Initialize the B-tree structure
	btree := &BTreeIndex{
		Name:       indexName,
		FileName:   filepath.Join(bts.dataDir, indexName+".idx"),
		PageSize:   BTreePageSize,
		IndexField: indexField,
	}

	// Create the file
	file, err := os.Create(btree.FileName)
	if err != nil {
		return nil, fmt.Errorf("failed to create index file: %w", err)
	}
	defer file.Close()

	// Calculate how many tuples can fit in a leaf page
	// This is an estimate that should be refined based on actual key sizes
	avgKeySize := calculateAverageKeySize(tuples)
	entriesPerLeafPage := calculateEntriesPerPage(avgKeySize, BTreePageSize, BTreeFillFactor)

	bts.logger.Infof("Average key size: %d bytes, estimated entries per leaf page: %d",
		avgKeySize, entriesPerLeafPage)

	// Step 1: Create leaf pages from sorted tuples
	leafPages, err := bts.createLeafPages(tuples, entriesPerLeafPage)
	if err != nil {
		return nil, fmt.Errorf("failed to create leaf pages: %w", err)
	}

	// Step 2: Build internal nodes bottom-up
	rootPageNum, height, err := bts.buildInternalNodes(leafPages, file)
	if err != nil {
		return nil, fmt.Errorf("failed to build internal nodes: %w", err)
	}

	// Step 3: Create and write meta page
	metaPage := BTreePage{
		PageType:   BTreeMetaPage,
		PageNum:    0,
		NumEntries: 1,
		Entries: []BTreeEntry{
			{
				// The meta entry contains metadata about the tree
				Key: []byte{0}, // Special key for metadata
				Value: encodeMetadata(map[string]interface{}{
					"rootPage":   rootPageNum,
					"height":     height,
					"totalPages": len(leafPages) + int(height),
					"indexField": indexField.FieldName,
					"isUnique":   indexField.IsUnique,
					"collation":  indexField.Collation,
					"created":    helpers.TimeNow(),
				}),
			},
		},
	}

	// Write meta page to file
	if err := writePage(file, 0, metaPage); err != nil {
		return nil, fmt.Errorf("failed to write meta page: %w", err)
	}

	// Update B-tree structure with final info
	btree.RootPage = rootPageNum
	btree.Height = height
	btree.TotalPages = uint32(len(leafPages) + int(height))
	btree.MetaPage = metaPage

	bts.logger.Infof("Built B-tree index with %d leaf pages, height %d, root page %d",
		len(leafPages), height, rootPageNum)

	return btree, nil
}

// createLeafPages creates leaf pages from sorted tuples
func (bts *BTreeService) createLeafPages(tuples []IndexTuple, entriesPerPage int) ([]BTreePage, error) {
	if len(tuples) == 0 {
		return nil, fmt.Errorf("cannot create B-tree from empty tuple list")
	}

	var leafPages []BTreePage
	pageNum := uint32(1) // Start from 1 (0 is reserved for meta page)

	// Create leaf pages by grouping tuples
	for i := 0; i < len(tuples); i += entriesPerPage {
		end := i + entriesPerPage
		if end > len(tuples) {
			end = len(tuples)
		}

		// Create a new leaf page
		page := BTreePage{
			PageType:   BTreeLeafPage,
			PageNum:    pageNum,
			Level:      0, // Leaf level
			NumEntries: uint16(end - i),
		}

		// Previous page link
		if pageNum > 1 {
			page.PrevPage = pageNum - 1
		}

		// Add entries to the page
		for j := i; j < end; j++ {
			// For each tuple, create a B-tree entry
			valueBuffer := new(bytes.Buffer)
			binary.Write(valueBuffer, binary.LittleEndian, tuples[j].TID)

			entry := BTreeEntry{
				Key:   tuples[j].Key,
				Value: valueBuffer.Bytes(),
				DocID: tuples[j].DocID,
				TID:   tuples[j].TID,
			}

			page.Entries = append(page.Entries, entry)
		}

		// Calculate free space (simplified)
		page.FreeSpace = uint16(BTreePageSize - estimatePageSize(page))

		// Add to the list of pages
		leafPages = append(leafPages, page)
		pageNum++
	}

	// Set next page pointers
	for i := 0; i < len(leafPages)-1; i++ {
		leafPages[i].NextPage = leafPages[i+1].PageNum
	}

	return leafPages, nil
}

// buildInternalNodes builds internal nodes of the B-tree bottom-up
func (bts *BTreeService) buildInternalNodes(leafPages []BTreePage, file *os.File) (uint32, uint16, error) {
	if len(leafPages) == 0 {
		return 0, 0, fmt.Errorf("no leaf pages to build tree from")
	}

	// Write all leaf pages to the file first
	for _, page := range leafPages {
		if err := writePage(file, page.PageNum, page); err != nil {
			return 0, 0, fmt.Errorf("failed to write leaf page %d: %w", page.PageNum, err)
		}
	}

	// If only one leaf page, it becomes the root
	if len(leafPages) == 1 {
		// This is both a leaf and the root
		rootPage := leafPages[0]
		rootPage.PageType = BTreeRootPage

		// Update the page in the file
		if err := writePage(file, rootPage.PageNum, rootPage); err != nil {
			return 0, 0, fmt.Errorf("failed to write root page %d: %w", rootPage.PageNum, err)
		}

		return rootPage.PageNum, 1, nil // Height = 1 (just the root)
	}

	// Build internal nodes level by level, starting from one level above leaves
	currentLevel := leafPages
	nextPageNum := uint32(len(leafPages) + 1) // Next available page number
	height := uint16(1)                       // Start with height 1 (leaf level)

	// Calculate how many pointers can fit in an internal page
	// This should be estimated based on key sizes
	pointersPerPage := calculateEntriesPerPage(24, BTreePageSize, BTreeFillFactor) // Assuming pointer is ~24 bytes

	// Continue building levels until we reach a single root page
	for len(currentLevel) > 1 {
		height++
		var nextLevel []BTreePage

		// Group pages into parent pages
		for i := 0; i < len(currentLevel); i += pointersPerPage {
			end := i + pointersPerPage
			if end > len(currentLevel) {
				end = len(currentLevel)
			}

			// Create new parent page
			parentPage := BTreePage{
				PageType:   BTreeInnerPage,
				PageNum:    nextPageNum,
				Level:      currentLevel[0].Level + 1,
				NumEntries: uint16(end - i),
			}

			// Set previous page link if not the first parent
			if len(nextLevel) > 0 {
				parentPage.PrevPage = nextLevel[len(nextLevel)-1].PageNum
				nextLevel[len(nextLevel)-1].NextPage = parentPage.PageNum
			}

			// Add entries to link to children
			for j := i; j < end; j++ {
				childPage := currentLevel[j]

				// Set child's parent pointer
				childPage.ParentPage = parentPage.PageNum
				if err := writePage(file, childPage.PageNum, childPage); err != nil {
					return 0, 0, fmt.Errorf("failed to update child page %d: %w", childPage.PageNum, err)
				}

				// First child is special - we use its lowest key as separator
				if j == i {
					// For first child, we just store the page number
					valueBuffer := new(bytes.Buffer)
					binary.Write(valueBuffer, binary.LittleEndian, childPage.PageNum)

					entry := BTreeEntry{
						// Use the first key of the child page as separator key
						Key:   childPage.Entries[0].Key,
						Value: valueBuffer.Bytes(), // Page number as the value
					}
					parentPage.Entries = append(parentPage.Entries, entry)
				} else {
					// For subsequent children, store highest key from previous child as separator
					prevChild := currentLevel[j-1]
					highestKey := prevChild.Entries[len(prevChild.Entries)-1].Key

					valueBuffer := new(bytes.Buffer)
					binary.Write(valueBuffer, binary.LittleEndian, childPage.PageNum)

					entry := BTreeEntry{
						Key:   highestKey,
						Value: valueBuffer.Bytes(),
					}
					parentPage.Entries = append(parentPage.Entries, entry)
				}
			}

			// Calculate free space
			parentPage.FreeSpace = uint16(BTreePageSize - estimatePageSize(parentPage))

			// Write the parent page
			if err := writePage(file, parentPage.PageNum, parentPage); err != nil {
				return 0, 0, fmt.Errorf("failed to write internal page %d: %w", parentPage.PageNum, err)
			}

			nextLevel = append(nextLevel, parentPage)
			nextPageNum++
		}

		// Move up to the next level
		currentLevel = nextLevel
	}

	// The last page is the root
	rootPage := currentLevel[0]
	rootPage.PageType = BTreeRootPage

	// Update the root page
	if err := writePage(file, rootPage.PageNum, rootPage); err != nil {
		return 0, 0, fmt.Errorf("failed to write root page %d: %w", rootPage.PageNum, err)
	}

	return rootPage.PageNum, height, nil
}

// Helper functions

// calculateAverageKeySize estimates the average key size from a sample of tuples
func calculateAverageKeySize(tuples []IndexTuple) int {
	if len(tuples) == 0 {
		return 0
	}

	// Take a sample of keys to calculate average size
	sampleSize := 100
	if len(tuples) < sampleSize {
		sampleSize = len(tuples)
	}

	totalSize := 0
	for i := 0; i < sampleSize; i++ {
		// Use approximately evenly spaced samples
		idx := (i * len(tuples)) / sampleSize
		totalSize += len(tuples[idx].Key)
	}

	return totalSize / sampleSize
}

// calculateEntriesPerPage estimates how many entries can fit in a page
func calculateEntriesPerPage(avgKeySize int, pageSize uint32, fillFactor int) int {
	// Estimate overhead per entry (key length, pointer, overhead)
	entryOverhead := 16 // bytes (conservative estimate)

	// Estimate page header size
	pageHeaderSize := 24 // bytes

	// Calculate usable space per page
	usableSpace := int(pageSize) * fillFactor / 100

	// Estimate entries per page
	entriesPerPage := (usableSpace - pageHeaderSize) / (avgKeySize + entryOverhead)

	// Ensure at least 2 entries per page
	if entriesPerPage < 2 {
		entriesPerPage = 2
	}

	return entriesPerPage
}

// estimatePageSize estimates the size of a page in bytes
func estimatePageSize(page BTreePage) int {
	// Page header
	size := 24 // Base header size

	// Calculate entries size
	for _, entry := range page.Entries {
		size += 8 + len(entry.Key) + len(entry.Value) // Overhead + key + value
	}

	return size
}

// writePage writes a page to the file at the specified position
func writePage(file *os.File, pageNum uint32, page BTreePage) error {
	// Serialize the page to bytes
	pageBuffer := new(bytes.Buffer)

	// Write page header
	binary.Write(pageBuffer, binary.LittleEndian, uint32(page.PageType))
	binary.Write(pageBuffer, binary.LittleEndian, page.PageNum)
	binary.Write(pageBuffer, binary.LittleEndian, page.ParentPage)
	binary.Write(pageBuffer, binary.LittleEndian, page.PrevPage)
	binary.Write(pageBuffer, binary.LittleEndian, page.NextPage)
	binary.Write(pageBuffer, binary.LittleEndian, page.Level)
	binary.Write(pageBuffer, binary.LittleEndian, page.NumEntries)
	binary.Write(pageBuffer, binary.LittleEndian, page.FreeSpace)

	// Write entries
	for _, entry := range page.Entries {
		// Write key length and key
		binary.Write(pageBuffer, binary.LittleEndian, uint32(len(entry.Key)))
		pageBuffer.Write(entry.Key)

		// Write value length and value
		binary.Write(pageBuffer, binary.LittleEndian, uint32(len(entry.Value)))
		pageBuffer.Write(entry.Value)
	}

	// Calculate page padding (fill to page size)
	padding := int(BTreePageSize) - pageBuffer.Len()
	if padding > 0 {
		pageBuffer.Write(make([]byte, padding))
	}

	// Seek to the correct position in the file
	pos := int64(pageNum) * int64(BTreePageSize)
	if _, err := file.Seek(pos, io.SeekStart); err != nil {
		return fmt.Errorf("failed to seek to page position: %w", err)
	}

	// Write the page buffer to the file
	if _, err := pageBuffer.WriteTo(file); err != nil {
		return fmt.Errorf("failed to write page data: %w", err)
	}

	return nil
}

// encodeMetadata serializes metadata for storage
func encodeMetadata(metadata map[string]interface{}) []byte {
	// TODO Simplified version - in production code, use a more robust encoding
	buffer := new(bytes.Buffer)

	// Write number of items
	binary.Write(buffer, binary.LittleEndian, uint32(len(metadata)))

	// Write each key-value pair
	for k, v := range metadata {
		// Write key
		keyBytes := []byte(k)
		binary.Write(buffer, binary.LittleEndian, uint32(len(keyBytes)))
		buffer.Write(keyBytes)

		// Simplified value storage - in production, handle different types properly
		valueBytes := []byte(fmt.Sprintf("%v", v))
		binary.Write(buffer, binary.LittleEndian, uint32(len(valueBytes)))
		buffer.Write(valueBytes)
	}

	return buffer.Bytes()
}
