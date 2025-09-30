package hashindexV2

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"time"
)

// BINARY SERIALIZATION STRUCTURES
// These structures provide efficient binary encoding for hash index data

// BinaryIndexRecord represents a fixed-size binary format for index records
// This optimizes storage and eliminates ASCII parsing overhead
type BinaryIndexRecord struct {
	HashValue    uint32   // 4 bytes - precomputed hash
	DocumentID   [64]byte // 64 bytes - fixed-size DocumentID (null-padded)
	TimestampSec int64    // 8 bytes - Unix timestamp seconds
	TimestampNs  int32    // 4 bytes - nanosecond component
	_            [4]byte  // 4 bytes - padding for alignment
	// Total: 84 bytes per record (vs ~100+ bytes ASCII)
}

// ToBinary converts IndexRecord to BinaryIndexRecord
func (r *IndexRecord) ToBinary() *BinaryIndexRecord {
	binary := &BinaryIndexRecord{
		HashValue:    r.HashValue,
		TimestampSec: r.Timestamp.Unix(),
		TimestampNs:  int32(r.Timestamp.Nanosecond()),
	}

	// Copy DocumentID with null-padding
	copy(binary.DocumentID[:], []byte(r.DocumentID))

	return binary
}

// ToIndexRecord converts BinaryIndexRecord back to IndexRecord
func (b *BinaryIndexRecord) ToIndexRecord() *IndexRecord {
	// Find null terminator for DocumentID
	docID := string(bytes.TrimRight(b.DocumentID[:], "\x00"))

	return &IndexRecord{
		DocumentID: docID,
		HashValue:  b.HashValue,
		Timestamp:  time.Unix(b.TimestampSec, int64(b.TimestampNs)),
	}
}

// BinaryBucketPageHeader represents the fixed header of a bucket page
type BinaryBucketPageHeader struct {
	PageNumber      uint32  // 4 bytes
	BucketNumber    uint32  // 4 bytes
	RecordCount     uint32  // 4 bytes
	OverflowPage    uint32  // 4 bytes
	FreeSpace       uint32  // 4 bytes
	LastModifiedSec int64   // 8 bytes
	LastModifiedNs  int32   // 4 bytes
	_               [4]byte // 4 bytes padding
	// Total: 36 bytes header
}

// BinaryOverflowPageHeader represents the fixed header of an overflow page
type BinaryOverflowPageHeader struct {
	PageNumber       uint32  // 4 bytes
	ParentBucket     uint32  // 4 bytes
	NextOverflowPage uint32  // 4 bytes
	RecordCount      uint32  // 4 bytes
	FreeSpace        uint32  // 4 bytes
	LastModifiedSec  int64   // 8 bytes
	LastModifiedNs   int32   // 4 bytes
	_                [4]byte // 4 bytes padding
	// Total: 36 bytes header
}

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
	// PERFORMANCE FIX: Use binary format size calculations
	// Each binary record is exactly 84 bytes (fixed size)
	const binaryRecordSize = 84
	const binaryHeaderSize = 36

	// Calculate current usage: header + (record count * record size)
	currentUsage := binaryHeaderSize + (len(bp.Records) * binaryRecordSize)

	// Check if adding one more record would exceed page size
	newUsage := currentUsage + binaryRecordSize

	// Check against page size limit (8KB = 8192 bytes)
	return newUsage <= 8192
} // Add helper method to estimate current page size
func (bp *BucketPage) EstimateSerializedSize() uint32 {
	// Base page header size
	size := uint32(200) // PageNumber, BucketNumber, RecordCount, OverflowPage fields

	// Add size for each existing record
	for _, record := range bp.Records {
		size += uint32(70 + len(record.DocumentID)) // Record format in ASCII
	}

	// Add footer size
	size += 30

	return size
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
	// PERFORMANCE FIX: Use binary format size calculations
	// Each binary record is exactly 84 bytes (fixed size)
	const binaryRecordSize = 84
	const binaryHeaderSize = 36

	// Calculate current usage: header + (record count * record size)
	currentUsage := binaryHeaderSize + (len(op.Records) * binaryRecordSize)

	// Check if adding one more record would exceed page size
	newUsage := currentUsage + binaryRecordSize

	// Check against page size limit (8KB = 8192 bytes)
	return newUsage <= 8192
} // Add helper method to estimate current overflow page size
func (op *OverflowPage) EstimateSerializedSize() uint32 {
	// Base page header size
	size := uint32(250) // PageNumber, ParentBucket, NextOverflowPage, RecordCount fields

	// Add size for each existing record
	for _, record := range op.Records {
		size += uint32(70 + len(record.DocumentID)) // Record format in ASCII
	}

	// Add footer size
	size += 30

	return size
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

// BINARY SERIALIZATION METHODS
// These methods provide high-performance binary I/O for hash index pages

// SerializeBinary converts BucketPage to binary format for disk storage
func (bp *BucketPage) SerializeBinary() ([]byte, error) {
	buf := new(bytes.Buffer)

	// Write header
	header := BinaryBucketPageHeader{
		PageNumber:      bp.PageNumber,
		BucketNumber:    bp.BucketNumber,
		RecordCount:     bp.RecordCount,
		OverflowPage:    bp.OverflowPageNum,
		FreeSpace:       bp.FreeSpace,
		LastModifiedSec: bp.LastModified.Unix(),
		LastModifiedNs:  int32(bp.LastModified.Nanosecond()),
	}

	if err := binary.Write(buf, binary.LittleEndian, &header); err != nil {
		return nil, fmt.Errorf("failed to write bucket header: %w", err)
	}

	// Write records
	for _, record := range bp.Records {
		binaryRecord := record.ToBinary()
		if err := binary.Write(buf, binary.LittleEndian, binaryRecord); err != nil {
			return nil, fmt.Errorf("failed to write record: %w", err)
		}
	}

	return buf.Bytes(), nil
}

// DeserializeBinary reconstructs BucketPage from binary data
func (bp *BucketPage) DeserializeBinary(data []byte) error {
	buf := bytes.NewReader(data)

	// Read header
	var header BinaryBucketPageHeader
	if err := binary.Read(buf, binary.LittleEndian, &header); err != nil {
		return fmt.Errorf("failed to read bucket header: %w", err)
	}

	// Set fields from header
	bp.PageNumber = header.PageNumber
	bp.BucketNumber = header.BucketNumber
	bp.RecordCount = header.RecordCount
	bp.OverflowPageNum = header.OverflowPage
	bp.FreeSpace = header.FreeSpace
	bp.LastModified = time.Unix(header.LastModifiedSec, int64(header.LastModifiedNs))

	// Read records
	bp.Records = make([]*IndexRecord, 0, header.RecordCount)
	for i := uint32(0); i < header.RecordCount; i++ {
		var binaryRecord BinaryIndexRecord
		if err := binary.Read(buf, binary.LittleEndian, &binaryRecord); err != nil {
			return fmt.Errorf("failed to read record %d: %w", i, err)
		}
		bp.Records = append(bp.Records, binaryRecord.ToIndexRecord())
	}

	return nil
}

// SerializeBinary converts OverflowPage to binary format for disk storage
func (op *OverflowPage) SerializeBinary() ([]byte, error) {
	buf := new(bytes.Buffer)

	// Write header
	header := BinaryOverflowPageHeader{
		PageNumber:       op.PageNumber,
		ParentBucket:     op.ParentBucket,
		NextOverflowPage: op.NextOverflowPage,
		RecordCount:      op.RecordCount,
		FreeSpace:        op.FreeSpace,
		LastModifiedSec:  op.LastModified.Unix(),
		LastModifiedNs:   int32(op.LastModified.Nanosecond()),
	}

	if err := binary.Write(buf, binary.LittleEndian, &header); err != nil {
		return nil, fmt.Errorf("failed to write overflow header: %w", err)
	}

	// Write records
	for _, record := range op.Records {
		binaryRecord := record.ToBinary()
		if err := binary.Write(buf, binary.LittleEndian, binaryRecord); err != nil {
			return nil, fmt.Errorf("failed to write record: %w", err)
		}
	}

	return buf.Bytes(), nil
}

// DeserializeBinary reconstructs OverflowPage from binary data
func (op *OverflowPage) DeserializeBinary(data []byte) error {
	buf := bytes.NewReader(data)

	// Read header
	var header BinaryOverflowPageHeader
	if err := binary.Read(buf, binary.LittleEndian, &header); err != nil {
		return fmt.Errorf("failed to read overflow header: %w", err)
	}

	// Set fields from header
	op.PageNumber = header.PageNumber
	op.ParentBucket = header.ParentBucket
	op.NextOverflowPage = header.NextOverflowPage
	op.RecordCount = header.RecordCount
	op.FreeSpace = header.FreeSpace
	op.LastModified = time.Unix(header.LastModifiedSec, int64(header.LastModifiedNs))

	// Read records
	op.Records = make([]*IndexRecord, 0, header.RecordCount)
	for i := uint32(0); i < header.RecordCount; i++ {
		var binaryRecord BinaryIndexRecord
		if err := binary.Read(buf, binary.LittleEndian, &binaryRecord); err != nil {
			return fmt.Errorf("failed to read record %d: %w", i, err)
		}
		op.Records = append(op.Records, binaryRecord.ToIndexRecord())
	}

	return nil
}
