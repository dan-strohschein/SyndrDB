package hashindexV2

import (
	"fmt"
	"time"
)

// Add this to the existing hash_data_structures.go file:

// IndexRecord represents a single record in the hash index
// Each record contains a document ID and associated metadata
type IndexRecord struct {
	DocumentID string    // The document ID being indexed
	HashValue  uint32    // Precomputed hash value for this document ID
	Timestamp  time.Time // When this record was added to the index
	Key        string
	Value      string
}

// NewIndexRecord creates a new index record
// Parameters:
//   - documentID: The document ID to index
//   - hashValue: The precomputed hash value
//
// Returns:
//   - *IndexRecord: The created index record
func NewIndexRecord(documentID string, hashValue uint32) *IndexRecord {
	return &IndexRecord{
		DocumentID: documentID,
		HashValue:  hashValue,
		Timestamp:  time.Now(),
	}
}

// Size returns the estimated size of this record in bytes
// Returns:
//   - uint32: Estimated size in bytes
func (r *IndexRecord) Size() uint32 {
	// Base fields: HashValue (4) + Timestamp (8) = 12 bytes
	baseSize := uint32(12)

	// Document ID length + length prefix (4 bytes)
	docIDSize := uint32(len(r.DocumentID)) + 4

	// Add some padding for alignment
	return baseSize + docIDSize + 4
}

// IsValid checks if the record is valid
// Returns:
//   - bool: Whether the record is valid
func (r *IndexRecord) IsValid() bool {
	return r.DocumentID != "" && !r.Timestamp.IsZero()
}

// String returns a string representation of the record
// Returns:
//   - string: String representation
func (r *IndexRecord) String() string {
	return fmt.Sprintf("IndexRecord{DocumentID: %s, HashValue: %d, Timestamp: %s}",
		r.DocumentID, r.HashValue, r.Timestamp.Format(time.RFC3339))
}

// NewIndexMetadata creates a new metadata structure with default values
// Parameters:
//   - bucketCount: Initial number of buckets
//   - pageSize: Size of each page in bytes
//   - loadFactor: Target load factor
//   - debugMode: Whether to enable debug mode
//
// Returns:
//   - *IndexMetadata: The created metadata structure
func NewIndexMetadata(bucketCount uint32, pageSize uint32, loadFactor float64, debugMode bool) *HashIndexMetadata {
	highMask := bucketCount - 1
	lowMask := (bucketCount >> 1) - 1

	return &HashIndexMetadata{
		Version:      1,
		CreatedAt:    time.Now(),
		LastModified: time.Now(),
		BucketCount:  bucketCount,
		TotalRecords: 0,
		LoadFactor:   loadFactor,
		PageSize:     pageSize,
		SplitPointer: 0,
		HashSeed:     GenerateHashSeed(),
		NextPageNum:  bucketCount, // Start after initial buckets
		DebugMode:    debugMode,

		MaxBucket:  bucketCount - 1,    // 0-based maximum bucket number
		HighMask:   highMask,           // For bucket calculation
		LowMask:    lowMask,            // For bucket calculation
		FillFactor: uint32(loadFactor), // Same as LoadFactor for compatibility

		BitmapPages:   0, // Not used yet
		OverflowPages: 0, // No overflow pages initially

	}
}

// IsValid checks if the metadata is valid
// Returns:
//   - bool: Whether the metadata is valid
func (m *HashIndexMetadata) IsValid() bool {
	return m.Version > 0 &&
		m.BucketCount > 0 &&
		m.PageSize > 0 &&
		m.LoadFactor > 0 &&
		m.LoadFactor <= 1.0 &&
		m.FillFactor > 0 &&
		m.FillFactor <= 1.0 &&
		m.MaxBucket == m.BucketCount-1 &&
		!m.CreatedAt.IsZero()
}

// String returns a string representation of the metadata
// Returns:
//   - string: String representation
func (m *HashIndexMetadata) String() string {
	return fmt.Sprintf("IndexMetadata{Version: %d, Buckets: %d, Records: %d, LoadFactor: %.2f, PageSize: %d, Split: %d}",
		m.Version, m.BucketCount, m.TotalRecords, m.LoadFactor, m.PageSize, m.SplitPointer)
}

// PageType constants for identifying different page types
const (
	PageTypeMetadata = 0
	PageTypeBucket   = 1
	PageTypeOverflow = 2
)

// PageHeader represents the common header for all page types
// Every page starts with this header for identification and validation
type PageHeader struct {
	PageType     uint32    // Type of page (metadata, bucket, overflow)
	PageNumber   uint32    // Page number in the file
	LastModified time.Time // When this page was last modified
	Checksum     uint32    // Checksum for integrity verification
}

// NewPageHeader creates a new page header
// Parameters:
//   - pageType: The type of page
//   - pageNumber: The page number
//
// Returns:
//   - *PageHeader: The created page header
func NewPageHeader(pageType uint32, pageNumber uint32) *PageHeader {
	return &PageHeader{
		PageType:     pageType,
		PageNumber:   pageNumber,
		LastModified: time.Now(),
		Checksum:     0, // Will be calculated when writing
	}
}

// IsValid checks if the page header is valid
// Returns:
//   - bool: Whether the header is valid
func (ph *PageHeader) IsValid() bool {
	return ph.PageType <= PageTypeOverflow && !ph.LastModified.IsZero()
}

// Update the existing BucketPage to include PageHeader
type BucketPage struct {
	*PageHeader                    // Embedded page header
	BucketNumber    uint32         // Which bucket this page represents
	RecordCount     uint32         // Number of records in this page
	FreeSpace       uint32         // Remaining free space in bytes
	OverflowPageNum uint32         // Page number of first overflow page (0 if none)
	Records         []*IndexRecord // The actual records stored in this page
	BucketNum       uint32
	ItemCount       uint32
	Items           []*IndexRecord
	OverflowPage    uint32 // Page number of first overflow page (0 if none)
}

// Update NewBucketPage to include the header
func NewBucketPage(bucketNumber uint32, pageSize uint32) *BucketPage {
	header := NewPageHeader(PageTypeBucket, bucketNumberToPageNumber(bucketNumber))

	// Calculate free space (page size minus header and fixed fields)
	headerSize := uint32(64) // Estimated header size
	freeSpace := pageSize - headerSize

	return &BucketPage{
		PageHeader:      header,
		BucketNumber:    bucketNumber,
		RecordCount:     0,
		FreeSpace:       freeSpace,
		OverflowPageNum: 0,
		Records:         make([]*IndexRecord, 0),
	}
}

// Update the existing OverflowPage to include PageHeader
type OverflowPage struct {
	*PageHeader                     // Embedded page header
	RecordCount      uint32         // Number of records in this page
	FreeSpace        uint32         // Remaining free space in bytes
	ParentBucket     uint32         // The bucket index this overflow belongs to
	NextOverflowPage uint32         // Next overflow page in chain (0 if none)
	Records          []*IndexRecord // The actual records stored in this page
	ItemCount        uint32
	Items            []*IndexRecord
}

// Update NewOverflowPage to include the header
func NewOverflowPage(pageNumber uint32, pageSize uint32) *OverflowPage {
	header := NewPageHeader(PageTypeOverflow, pageNumber)

	// Calculate free space (page size minus header and fixed fields)
	headerSize := uint32(48) // Estimated header size
	freeSpace := pageSize - headerSize

	return &OverflowPage{
		PageHeader:       header,
		RecordCount:      0,
		FreeSpace:        freeSpace,
		ParentBucket:     0,
		NextOverflowPage: 0,
		Records:          make([]*IndexRecord, 0),
	}
}

// CacheStats represents statistics about the page cache
type CacheStats struct {
	TotalPages   int     // Total number of pages in cache
	DirtyPages   int     // Number of pages that need to be written
	HitRate      float64 // Cache hit rate (0.0 to 1.0)
	Hits         uint64  // Total cache hits
	Misses       uint64  // Total cache misses
	MaxCacheSize int
}

// String returns a string representation of the cache stats
// Returns:
//   - string: String representation
func (cs *CacheStats) String() string {
	return fmt.Sprintf("CacheStats{Pages: %d, Dirty: %d, HitRate: %.2f%%, Hits: %d, Misses: %d}",
		cs.TotalPages, cs.DirtyPages, cs.HitRate*100, cs.Hits, cs.Misses)
}

// FileStats represents statistics about the hash index file
type FileStats struct {
	FilePath     string    // Path to the index file
	FileSize     int64     // Size of the file in bytes
	PageCount    uint32    // Total number of pages in the file
	LastAccessed time.Time // When the file was last accessed
	LastModified time.Time // When the file was last modified
}

// String returns a string representation of the file stats
// Returns:
//   - string: String representation
func (fs *FileStats) String() string {
	return fmt.Sprintf("FileStats{Path: %s, Size: %d bytes, Pages: %d, Modified: %s}",
		fs.FilePath, fs.FileSize, fs.PageCount, fs.LastModified.Format(time.RFC3339))
}

// Update the CanFitRecord method for BucketPage:
func (bp *BucketPage) CanFitRecord(record *IndexRecord) bool {
	estimatedSize := uint32(32 + len(record.DocumentID)) // Header + document ID
	return bp.FreeSpace >= estimatedSize
}

// Update the AddRecord method for BucketPage:
func (bp *BucketPage) AddRecord(record *IndexRecord) {
	bp.Records = append(bp.Records, record)
	bp.RecordCount = uint32(len(bp.Records))

	// Update free space (rough estimate)
	recordSize := uint32(32 + len(record.DocumentID))
	if bp.FreeSpace >= recordSize {
		bp.FreeSpace -= recordSize
	} else {
		bp.FreeSpace = 0
	}

	bp.LastModified = time.Now()
}

// Update the CanFitRecord method for OverflowPage:
func (op *OverflowPage) CanFitRecord(record *IndexRecord) bool {
	estimatedSize := uint32(32 + len(record.DocumentID)) // Header + document ID
	return op.FreeSpace >= estimatedSize

}

// Update the AddRecord method for OverflowPage:
func (op *OverflowPage) AddRecord(record *IndexRecord) {
	op.Records = append(op.Records, record)
	op.RecordCount = uint32(len(op.Records))

	recordSize := uint32(32 + len(record.DocumentID))
	if op.FreeSpace >= recordSize {
		op.FreeSpace -= recordSize
	} else {
		op.FreeSpace = 0
	}
}

// Add helper methods to keep NumTuples and TotalRecords in sync:
func (m *HashIndexMetadata) IncrementRecordCount() {
	m.TotalRecords++

	m.LastModified = time.Now()
}

func (m *HashIndexMetadata) DecrementRecordCount() {
	if m.TotalRecords > 0 {
		m.TotalRecords--

		m.LastModified = time.Now()
	}
}

// Add method to update masks when buckets are split:
func (m *HashIndexMetadata) UpdateMasksForSplit() {
	m.BucketCount++
	m.MaxBucket = m.BucketCount - 1
	m.HighMask = m.BucketCount - 1
	m.LowMask = (m.BucketCount >> 1) - 1
	m.SplitPointer++

	// Reset split pointer if we've completed a round
	if m.SplitPointer >= m.BucketCount/2 {
		m.SplitPointer = 0
	}

	m.LastModified = time.Now()
}
