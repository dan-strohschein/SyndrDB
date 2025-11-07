package schema

// file_header.go
//
// This file implements the GraphQL schema file header structure for SyndrDB.
// The header provides metadata about the schema file, including version info,
// database identification, record counts, timestamps, and integrity checksums.
//
// The header follows SyndrDB's architectural patterns for file formats:
// - Fixed 256-byte header for fast access
// - Magic number validation for file type identification
// - CRC32 checksums for data integrity
// - Versioning support for format evolution
// - Timestamps for monitoring and maintenance
//
// Design Principles:
// - Single Responsibility: Manages only file header structure and validation
// - Open/Closed: Extensible through reserved bytes without breaking compatibility
// - DRY: Reuses binary serialization patterns from bundle files

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"time"
	"unsafe"
)

const (
	// FileHeaderSize is the fixed size of the file header (256 bytes)
	FileHeaderSize = 256

	// Magic number for GraphQL schema files: "SNDR"
	MagicNumber = 0x534E4452 // "SNDR" in little-endian

	// FileTypeGraphQLSchema identifies this as a GraphQL schema file
	FileTypeGraphQLSchema = 0x03

	// CurrentFormatVersion is the current file format version
	CurrentFormatVersion = 1

	// CurrentSchemaVersion is the GraphQL spec version supported
	CurrentSchemaVersion = 1 // June 2018 spec
)

// FileHeader represents the fixed 256-byte header of a GraphQL schema file
type FileHeader struct {
	// Magic number: "SNDR" (4 bytes) - Offset 0-3
	Magic uint32

	// Version info (4 bytes)
	FormatVersion uint16 // Offset 4-5: File format version (current: 1)
	SchemaVersion uint16 // Offset 6-7: GraphQL schema version supported

	// File type (1 byte) - Offset 8
	FileType byte // 0x03 = GraphQL Schema File

	// Flags (1 byte) - Offset 9
	// Bit 0: HasSchemaIndex (0=no, 1=yes)
	// Bit 1: CompressedRecords (0=no, 1=yes)
	// Bit 2-7: Reserved
	Flags byte

	// Reserved for alignment (2 bytes) - Offset 10-11
	Reserved1 [2]byte

	// Database info (68 bytes) - Offset 12-79
	DatabaseName [64]byte // UTF-8, null-terminated
	DatabaseID   [4]byte  // Database UUID (first 4 bytes)

	// Record counts (24 bytes) - Offset 80-103
	TotalRecords   int64 // Total records ever written
	ActiveRecords  int64 // Currently active schemas
	TombstoneCount int64 // Tombstoned records

	// Timestamps (24 bytes) - Offset 104-127
	CreatedAt       int64 // Unix timestamp (seconds)
	UpdatedAt       int64 // Unix timestamp (seconds)
	LastCompactedAt int64 // Unix timestamp (seconds), 0 = never

	// Compaction config (16 bytes) - Offset 128-143
	CompactionThreshold float64 // Tombstone ratio trigger (0.3 = 30%)
	RetentionSeconds    int64   // Tombstone retention (604800 = 7 days)

	// Schema index info (16 bytes) - Offset 144-159
	SchemaIndexOffset int64 // Byte offset to schema index, 0 = no index
	SchemaIndexSize   int64 // Size of schema index in bytes

	// File integrity (8 bytes) - Offset 160-167
	HeaderChecksum uint32 // CRC32 of bytes 0-159
	FileChecksum   uint32 // CRC32 of entire file

	// Padding to 256 bytes (88 bytes) - Offset 168-255
	Reserved2 [88]byte
}

// NewFileHeader creates a new file header with default values
func NewFileHeader(databaseName string, databaseID string) *FileHeader {
	now := time.Now().Unix()

	header := &FileHeader{
		Magic:               MagicNumber,
		FormatVersion:       CurrentFormatVersion,
		SchemaVersion:       CurrentSchemaVersion,
		FileType:            FileTypeGraphQLSchema,
		Flags:               0,
		TotalRecords:        0,
		ActiveRecords:       0,
		TombstoneCount:      0,
		CreatedAt:           now,
		UpdatedAt:           now,
		LastCompactedAt:     0,
		CompactionThreshold: 0.3,              // 30% tombstone ratio triggers compaction
		RetentionSeconds:    7 * 24 * 60 * 60, // 7 days
		SchemaIndexOffset:   0,
		SchemaIndexSize:     0,
	}

	// Copy database name (null-terminated)
	copy(header.DatabaseName[:], []byte(databaseName))

	// Copy first 4 bytes of database ID
	if len(databaseID) >= 4 {
		copy(header.DatabaseID[:], []byte(databaseID)[:4])
	}

	return header
}

// Serialize converts the header to a 256-byte array for writing to disk
func (h *FileHeader) Serialize() ([]byte, error) {
	data := make([]byte, FileHeaderSize)

	// Write magic number (0-3)
	binary.LittleEndian.PutUint32(data[0:4], h.Magic)

	// Write version info (4-7)
	binary.LittleEndian.PutUint16(data[4:6], h.FormatVersion)
	binary.LittleEndian.PutUint16(data[6:8], h.SchemaVersion)

	// Write file type and flags (8-9)
	data[8] = h.FileType
	data[9] = h.Flags

	// Reserved1 (10-11) - already zero-initialized

	// Write database info (12-79)
	copy(data[12:76], h.DatabaseName[:])
	copy(data[76:80], h.DatabaseID[:])

	// Write record counts (80-103)
	binary.LittleEndian.PutUint64(data[80:88], uint64(h.TotalRecords))
	binary.LittleEndian.PutUint64(data[88:96], uint64(h.ActiveRecords))
	binary.LittleEndian.PutUint64(data[96:104], uint64(h.TombstoneCount))

	// Write timestamps (104-127)
	binary.LittleEndian.PutUint64(data[104:112], uint64(h.CreatedAt))
	binary.LittleEndian.PutUint64(data[112:120], uint64(h.UpdatedAt))
	binary.LittleEndian.PutUint64(data[120:128], uint64(h.LastCompactedAt))

	// Write compaction config (128-143)
	binary.LittleEndian.PutUint64(data[128:136], floatToBits(h.CompactionThreshold))
	binary.LittleEndian.PutUint64(data[136:144], uint64(h.RetentionSeconds))

	// Write schema index info (144-159)
	binary.LittleEndian.PutUint64(data[144:152], uint64(h.SchemaIndexOffset))
	binary.LittleEndian.PutUint64(data[152:160], uint64(h.SchemaIndexSize))

	// Calculate header checksum (CRC32 of bytes 0-159)
	h.HeaderChecksum = crc32.ChecksumIEEE(data[0:160])
	binary.LittleEndian.PutUint32(data[160:164], h.HeaderChecksum)

	// File checksum will be updated when writing records
	binary.LittleEndian.PutUint32(data[164:168], h.FileChecksum)

	// Reserved2 (168-255) - already zero-initialized

	return data, nil
}

// Deserialize reads a 256-byte array and populates the header struct
func (h *FileHeader) Deserialize(data []byte) error {
	if len(data) < FileHeaderSize {
		return fmt.Errorf("invalid header size: expected %d bytes, got %d", FileHeaderSize, len(data))
	}

	// Read magic number (0-3)
	h.Magic = binary.LittleEndian.Uint32(data[0:4])
	if h.Magic != MagicNumber {
		return fmt.Errorf("invalid magic number: expected 0x%08X, got 0x%08X", MagicNumber, h.Magic)
	}

	// Read version info (4-7)
	h.FormatVersion = binary.LittleEndian.Uint16(data[4:6])
	h.SchemaVersion = binary.LittleEndian.Uint16(data[6:8])

	// Read file type and flags (8-9)
	h.FileType = data[8]
	if h.FileType != FileTypeGraphQLSchema {
		return fmt.Errorf("invalid file type: expected 0x%02X, got 0x%02X", FileTypeGraphQLSchema, h.FileType)
	}
	h.Flags = data[9]

	// Read database info (12-79)
	copy(h.DatabaseName[:], data[12:76])
	copy(h.DatabaseID[:], data[76:80])

	// Read record counts (80-103)
	h.TotalRecords = int64(binary.LittleEndian.Uint64(data[80:88]))
	h.ActiveRecords = int64(binary.LittleEndian.Uint64(data[88:96]))
	h.TombstoneCount = int64(binary.LittleEndian.Uint64(data[96:104]))

	// Read timestamps (104-127)
	h.CreatedAt = int64(binary.LittleEndian.Uint64(data[104:112]))
	h.UpdatedAt = int64(binary.LittleEndian.Uint64(data[112:120]))
	h.LastCompactedAt = int64(binary.LittleEndian.Uint64(data[120:128]))

	// Read compaction config (128-143)
	h.CompactionThreshold = bitsToFloat(binary.LittleEndian.Uint64(data[128:136]))
	h.RetentionSeconds = int64(binary.LittleEndian.Uint64(data[136:144]))

	// Read schema index info (144-159)
	h.SchemaIndexOffset = int64(binary.LittleEndian.Uint64(data[144:152]))
	h.SchemaIndexSize = int64(binary.LittleEndian.Uint64(data[152:160]))

	// Read and validate header checksum (160-163)
	storedChecksum := binary.LittleEndian.Uint32(data[160:164])
	calculatedChecksum := crc32.ChecksumIEEE(data[0:160])
	if storedChecksum != calculatedChecksum {
		return fmt.Errorf("header checksum mismatch: expected 0x%08X, got 0x%08X", calculatedChecksum, storedChecksum)
	}
	h.HeaderChecksum = storedChecksum

	// Read file checksum (164-167)
	h.FileChecksum = binary.LittleEndian.Uint32(data[164:168])

	return nil
}

// Validate performs validation checks on the header
func (h *FileHeader) Validate() error {
	if h.Magic != MagicNumber {
		return fmt.Errorf("invalid magic number: 0x%08X", h.Magic)
	}

	if h.FileType != FileTypeGraphQLSchema {
		return fmt.Errorf("invalid file type: 0x%02X", h.FileType)
	}

	if h.FormatVersion > CurrentFormatVersion {
		return fmt.Errorf("unsupported format version: %d (current: %d)", h.FormatVersion, CurrentFormatVersion)
	}

	if h.TotalRecords < 0 {
		return fmt.Errorf("invalid TotalRecords: %d", h.TotalRecords)
	}

	if h.ActiveRecords < 0 {
		return fmt.Errorf("invalid ActiveRecords: %d", h.ActiveRecords)
	}

	if h.TombstoneCount < 0 {
		return fmt.Errorf("invalid TombstoneCount: %d", h.TombstoneCount)
	}

	if h.ActiveRecords+h.TombstoneCount > h.TotalRecords {
		return fmt.Errorf("active+tombstone count (%d) exceeds total records (%d)",
			h.ActiveRecords+h.TombstoneCount, h.TotalRecords)
	}

	return nil
}

// GetDatabaseName returns the database name as a string (removing null terminator)
func (h *FileHeader) GetDatabaseName() string {
	// Find null terminator
	for i, b := range h.DatabaseName {
		if b == 0 {
			return string(h.DatabaseName[:i])
		}
	}
	return string(h.DatabaseName[:])
}

// HasSchemaIndex returns true if the file contains a schema index
func (h *FileHeader) HasSchemaIndex() bool {
	return h.Flags&0x01 != 0
}

// SetSchemaIndex sets or clears the schema index flag
func (h *FileHeader) SetSchemaIndex(hasIndex bool) {
	if hasIndex {
		h.Flags |= 0x01
	} else {
		h.Flags &^= 0x01
	}
}

// HasCompressedRecords returns true if records are compressed
func (h *FileHeader) HasCompressedRecords() bool {
	return h.Flags&0x02 != 0
}

// SetCompressedRecords sets or clears the compressed records flag
func (h *FileHeader) SetCompressedRecords(compressed bool) {
	if compressed {
		h.Flags |= 0x02
	} else {
		h.Flags &^= 0x02
	}
}

// GetTombstoneRatio returns the ratio of tombstoned records to total records
func (h *FileHeader) GetTombstoneRatio() float64 {
	if h.TotalRecords == 0 {
		return 0.0
	}
	return float64(h.TombstoneCount) / float64(h.TotalRecords)
}

// ShouldCompact returns true if the file should be compacted based on tombstone ratio
func (h *FileHeader) ShouldCompact() bool {
	return h.GetTombstoneRatio() >= h.CompactionThreshold
}

// floatToBits converts a float64 to uint64 for binary encoding
func floatToBits(f float64) uint64 {
	return binary.LittleEndian.Uint64((*[8]byte)(unsafe.Pointer(&f))[:])
}

// bitsToFloat converts a uint64 to float64 for binary decoding
func bitsToFloat(bits uint64) float64 {
	f := float64(0)
	*(*uint64)(unsafe.Pointer(&f)) = bits
	return f
}
