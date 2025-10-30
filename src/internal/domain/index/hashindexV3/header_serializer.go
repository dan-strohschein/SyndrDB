package hashindexV3

/*
HEADER SERIALIZER - BSON SERIALIZATION FOR INDEX HEADERS

This file handles serialization and deserialization of IndexHeader structures
using BSON format. It reuses the existing helper functions from format.go
to maintain consistency with bundle metadata serialization.

KEY RESPONSIBILITIES:
- Serialize IndexHeader to BSON bytes
- Deserialize BSON bytes to IndexHeader
- Calculate header size including fixed fields
- Validate header integrity

DESIGN PRINCIPLES:
- Single Responsibility: Only handles header serialization
- DRY: Reuses existing BSON helpers from storage/format package
- Open/Closed: Versioned format allows future extensions
- Dependency Inversion: Depends on format package abstractions

SERIALIZATION FORMAT:
[Magic Number: 4 bytes]  → 0x48494458 ("HIDX")
[Version: 4 bytes]        → 1 (current version)
[Header Size: 4 bytes]    → Total size including this header
[BSON Metadata: variable] → IndexHeader serialized as BSON

The header size allows readers to skip directly to entry data:
File Position 0:                    Magic Number
File Position 4:                    Version
File Position 8:                    Header Size
File Position 12:                   BSON Metadata Start
File Position (12 + HeaderSize):    First Entry Start

BSON FORMAT:
Uses MongoDB's BSON encoding for:
- Cross-language compatibility
- Efficient binary representation
- Built-in type safety
- Schema evolution support

TODO: Future extensions
- Compression of header metadata
- Checksums for header validation
- Digital signatures for tamper detection
- Header versioning for migration
*/

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	"go.mongodb.org/mongo-driver/bson"
)

// HeaderSerializer handles serialization of IndexHeader structures
// Follows Single Responsibility Principle: Only handles header encoding/decoding
type HeaderSerializer struct {
	// No state needed - all methods are stateless
}

// NewHeaderSerializer creates a new header serializer
func NewHeaderSerializer() *HeaderSerializer {
	return &HeaderSerializer{}
}

// SerializeHeader converts an IndexHeader to bytes
// Returns:
//   - []byte: Complete header bytes including magic, version, size, and BSON data
//   - error: If serialization fails
//
// Format: [Magic:4][Version:4][HeaderSize:4][BSONData:variable]
func (hs *HeaderSerializer) SerializeHeader(header *IndexHeader) ([]byte, error) {
	if header == nil {
		return nil, fmt.Errorf("header cannot be nil")
	}

	// Ensure magic and version are set correctly
	header.Magic = IndexFileMagic
	header.Version = IndexFileVersion

	// Serialize the header struct to BSON
	bsonData, err := bson.Marshal(header)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal header to BSON: %w", err)
	}

	// Calculate total header size: Magic(4) + Version(4) + HeaderSize(4) + BSON data
	totalSize := 4 + 4 + 4 + len(bsonData)
	header.HeaderSize = int32(totalSize)

	// Re-serialize with updated HeaderSize
	bsonData, err = bson.Marshal(header)
	if err != nil {
		return nil, fmt.Errorf("failed to re-marshal header with size: %w", err)
	}

	// Recalculate in case HeaderSize field changed the size (it shouldn't, but be safe)
	totalSize = 4 + 4 + 4 + len(bsonData)
	if int32(totalSize) != header.HeaderSize {
		header.HeaderSize = int32(totalSize)
		bsonData, err = bson.Marshal(header)
		if err != nil {
			return nil, fmt.Errorf("failed to final marshal header: %w", err)
		}
		totalSize = 4 + 4 + 4 + len(bsonData)
	}

	// Build the complete header bytes
	buf := new(bytes.Buffer)

	// Write magic number (4 bytes, little-endian)
	if err := binary.Write(buf, binary.LittleEndian, header.Magic); err != nil {
		return nil, fmt.Errorf("failed to write magic number: %w", err)
	}

	// Write version (4 bytes, little-endian)
	if err := binary.Write(buf, binary.LittleEndian, header.Version); err != nil {
		return nil, fmt.Errorf("failed to write version: %w", err)
	}

	// Write header size (4 bytes, little-endian)
	if err := binary.Write(buf, binary.LittleEndian, header.HeaderSize); err != nil {
		return nil, fmt.Errorf("failed to write header size: %w", err)
	}

	// Write BSON data
	if _, err := buf.Write(bsonData); err != nil {
		return nil, fmt.Errorf("failed to write BSON data: %w", err)
	}

	return buf.Bytes(), nil
}

// DeserializeHeader reads an IndexHeader from bytes
// Parameters:
//   - data: Byte slice containing serialized header
//
// Returns:
//   - *IndexHeader: Deserialized header
//   - error: If deserialization fails or validation fails
func (hs *HeaderSerializer) DeserializeHeader(data []byte) (*IndexHeader, error) {
	if len(data) < HeaderMetadataPosition {
		return nil, fmt.Errorf("data too short: need at least %d bytes, got %d", HeaderMetadataPosition, len(data))
	}

	buf := bytes.NewReader(data)

	// Read magic number
	var magic uint32
	if err := binary.Read(buf, binary.LittleEndian, &magic); err != nil {
		return nil, fmt.Errorf("failed to read magic number: %w", err)
	}
	if magic != IndexFileMagic {
		return nil, fmt.Errorf("invalid magic number: expected 0x%X, got 0x%X", IndexFileMagic, magic)
	}

	// Read version
	var version uint32
	if err := binary.Read(buf, binary.LittleEndian, &version); err != nil {
		return nil, fmt.Errorf("failed to read version: %w", err)
	}
	if version != IndexFileVersion {
		return nil, fmt.Errorf("unsupported version: expected %d, got %d", IndexFileVersion, version)
	}

	// Read header size
	var headerSize int32
	if err := binary.Read(buf, binary.LittleEndian, &headerSize); err != nil {
		return nil, fmt.Errorf("failed to read header size: %w", err)
	}
	if headerSize <= 0 {
		return nil, fmt.Errorf("invalid header size: %d", headerSize)
	}

	// Calculate BSON data size
	bsonSize := int(headerSize) - HeaderMetadataPosition
	if bsonSize <= 0 {
		return nil, fmt.Errorf("invalid BSON data size: %d", bsonSize)
	}

	// Ensure we have enough data
	if len(data) < int(headerSize) {
		return nil, fmt.Errorf("data too short: need %d bytes for header, got %d", headerSize, len(data))
	}

	// Extract BSON data
	bsonData := data[HeaderMetadataPosition:headerSize]

	// Deserialize BSON to IndexHeader
	var header IndexHeader
	if err := bson.Unmarshal(bsonData, &header); err != nil {
		return nil, fmt.Errorf("failed to unmarshal BSON data: %w", err)
	}

	// Validate the header
	if err := header.Validate(); err != nil {
		return nil, fmt.Errorf("header validation failed: %w", err)
	}

	return &header, nil
}

// ReadHeaderFromFile reads a header from an open file
// Parameters:
//   - file: Open file positioned at the start (offset 0)
//
// Returns:
//   - *IndexHeader: Deserialized header
//   - int64: Total bytes read (for seeking past header)
//   - error: If reading or deserialization fails
func (hs *HeaderSerializer) ReadHeaderFromFile(file io.ReadSeeker) (*IndexHeader, int64, error) {
	// Seek to start of file
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return nil, 0, fmt.Errorf("failed to seek to start: %w", err)
	}

	// Read fixed header fields first to get size
	fixedHeader := make([]byte, HeaderMetadataPosition)
	n, err := io.ReadFull(file, fixedHeader)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to read fixed header: %w", err)
	}
	if n != HeaderMetadataPosition {
		return nil, 0, fmt.Errorf("incomplete fixed header: read %d bytes, expected %d", n, HeaderMetadataPosition)
	}

	// Extract header size
	headerSizeBytes := fixedHeader[8:12]
	headerSize := int32(binary.LittleEndian.Uint32(headerSizeBytes))
	if headerSize <= 0 {
		return nil, 0, fmt.Errorf("invalid header size: %d", headerSize)
	}

	// Read remaining header data
	remainingSize := int(headerSize) - HeaderMetadataPosition
	if remainingSize < 0 {
		return nil, 0, fmt.Errorf("invalid remaining size: %d", remainingSize)
	}

	remainingData := make([]byte, remainingSize)
	if remainingSize > 0 {
		n, err = io.ReadFull(file, remainingData)
		if err != nil {
			return nil, 0, fmt.Errorf("failed to read remaining header: %w", err)
		}
		if n != remainingSize {
			return nil, 0, fmt.Errorf("incomplete remaining header: read %d bytes, expected %d", n, remainingSize)
		}
	}

	// Combine fixed and remaining parts
	fullHeader := append(fixedHeader, remainingData...)

	// Deserialize
	header, err := hs.DeserializeHeader(fullHeader)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to deserialize header: %w", err)
	}

	return header, int64(headerSize), nil
}

// WriteHeaderToFile writes a header to an open file
// Parameters:
//   - file: Open file positioned at the start (offset 0)
//   - header: Header to write
//
// Returns:
//   - int64: Number of bytes written
//   - error: If writing or serialization fails
func (hs *HeaderSerializer) WriteHeaderToFile(file io.WriteSeeker, header *IndexHeader) (int64, error) {
	// Seek to start of file
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return 0, fmt.Errorf("failed to seek to start: %w", err)
	}

	// Serialize header
	headerBytes, err := hs.SerializeHeader(header)
	if err != nil {
		return 0, fmt.Errorf("failed to serialize header: %w", err)
	}

	// Write to file
	n, err := file.Write(headerBytes)
	if err != nil {
		return 0, fmt.Errorf("failed to write header: %w", err)
	}
	if n != len(headerBytes) {
		return 0, fmt.Errorf("incomplete write: wrote %d bytes, expected %d", n, len(headerBytes))
	}

	return int64(n), nil
}

// CalculateHeaderSize estimates the size of a header when serialized
// Useful for pre-allocating buffers or validating size constraints
func (hs *HeaderSerializer) CalculateHeaderSize(header *IndexHeader) (int32, error) {
	headerBytes, err := hs.SerializeHeader(header)
	if err != nil {
		return 0, err
	}
	return int32(len(headerBytes)), nil
}

// ValidateHeaderBytes checks if bytes represent a valid header without full deserialization
// Useful for quick file type validation
func (hs *HeaderSerializer) ValidateHeaderBytes(data []byte) error {
	if len(data) < HeaderMetadataPosition {
		return fmt.Errorf("data too short for header")
	}

	// Check magic
	magic := binary.LittleEndian.Uint32(data[0:4])
	if magic != IndexFileMagic {
		return fmt.Errorf("invalid magic number: 0x%X", magic)
	}

	// Check version
	version := binary.LittleEndian.Uint32(data[4:8])
	if version != IndexFileVersion {
		return fmt.Errorf("unsupported version: %d", version)
	}

	// Check header size
	headerSize := int32(binary.LittleEndian.Uint32(data[8:12]))
	if headerSize <= HeaderMetadataPosition {
		return fmt.Errorf("invalid header size: %d", headerSize)
	}

	return nil
}

// ReadHeaderSize reads only the header size from a file without deserializing the full header
// This is useful for seeking past the header efficiently without reading all metadata
// Parameters:
//   - file: Open file positioned at the start (offset 0)
//
// Returns:
//   - int64: Header size in bytes (offset where entry data begins)
//   - error: If reading fails or header is invalid
func (hs *HeaderSerializer) ReadHeaderSize(file io.ReadSeeker) (int64, error) {
	// Seek to start of file
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return 0, fmt.Errorf("failed to seek to start: %w", err)
	}

	// Read only the fixed header portion (magic + version + headerSize)
	// This is 12 bytes: [Magic:4][Version:4][HeaderSize:4]
	fixedHeader := make([]byte, 12)
	n, err := io.ReadFull(file, fixedHeader)
	if err != nil {
		return 0, fmt.Errorf("failed to read header size: %w", err)
	}
	if n != 12 {
		return 0, fmt.Errorf("incomplete header read: got %d bytes, expected 12", n)
	}

	// Validate magic number
	magic := binary.LittleEndian.Uint32(fixedHeader[0:4])
	if magic != IndexFileMagic {
		return 0, fmt.Errorf("invalid magic number: 0x%X (expected 0x%X)", magic, IndexFileMagic)
	}

	// Validate version
	version := binary.LittleEndian.Uint32(fixedHeader[4:8])
	if version != IndexFileVersion {
		return 0, fmt.Errorf("unsupported version: %d", version)
	}

	// Extract header size
	headerSize := int32(binary.LittleEndian.Uint32(fixedHeader[8:12]))
	if headerSize <= 0 {
		return 0, fmt.Errorf("invalid header size: %d", headerSize)
	}

	return int64(headerSize), nil
}
