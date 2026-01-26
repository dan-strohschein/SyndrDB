package helpers

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"sync"
	"syndrdb/src/internal/domain/models"
	"time"

	"github.com/cespare/xxhash/v2"
)

// Format version constants
const (
	FormatVersionV1 byte = 0x01 // Original format (no field directory)
	FormatVersionV2 byte = 0x02 // Extended format (with field directory)
)

// Format flags for V2
const (
	FlagHasFieldDirectory byte = 0x01
	FlagHasProjection     byte = 0x02 // Only selected fields serialized
	FlagCompressed        byte = 0x04 // Future: compressed data section
)

// V2 binary field type codes (distinct from models.FieldValueType)
const (
	V2TypeNull     byte = 0x00
	V2TypeString   byte = 0x01
	V2TypeInt32    byte = 0x02
	V2TypeInt64    byte = 0x03
	V2TypeFloat64  byte = 0x04
	V2TypeBool     byte = 0x05
	V2TypeDateTime byte = 0x06
	V2TypeDate     byte = 0x07
	V2TypeArray    byte = 0x08
	V2TypeObject   byte = 0x09
	V2TypeBytes    byte = 0x0A
	V2TypeUUID     byte = 0x0B
)

// FieldDirectoryEntry describes one field in the V2 field directory (24 bytes)
type FieldDirectoryEntry struct {
	NameHash64 uint64 // xxHash64 of field name
	NameOffset uint32 // Offset to field name in names section
	NameLen    uint16 // Length of field name
	FieldType  byte   // V2 type code
	_pad       byte   // Alignment padding
	DataOffset uint32 // Offset from data section start
	DataLen    uint32 // Length of field data
}

// HashFieldName64 returns xxHash64 of field name (fast, low collision)
func HashFieldName64(name string) uint64 {
	return xxhash.Sum64String(name)
}

// FastDocumentSerializer provides high-performance document serialization
// Replaces BSON encoding with direct binary format for write optimization
// Designed specifically for append-only document storage with minimal overhead
type FastDocumentSerializer struct {
	buffer []byte     // Reusable buffer to avoid allocations
	mu     sync.Mutex // Protects buffer from concurrent access
}

// NewFastDocumentSerializer creates a new fast serializer instance
func NewFastDocumentSerializer() *FastDocumentSerializer {
	return &FastDocumentSerializer{
		buffer: make([]byte, 0, 4096), // Pre-allocate 4KB buffer
	}
}

// SerializeDocument converts a document to optimized binary format
// Binary format: [DocumentID_len][DocumentID][field_count][field1][field2]...
// Each field: [name_len][name][type][value_len][value]
func (s *FastDocumentSerializer) SerializeDocument(document *models.Document) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// CRITICAL: Reset buffer length but preserve capacity to prevent reallocations
	// Reallocations during append() can cause race conditions if old buffer is still being copied
	s.buffer = s.buffer[:0]

	// If buffer is getting too large, reallocate fresh to prevent unbounded growth
	if cap(s.buffer) > 1024*1024 { // 1MB cap
		s.buffer = make([]byte, 0, 65536) // Reset to 64KB
	}

	// Write document ID
	docIDBytes := []byte(document.DocumentID)
	s.writeUint32(uint32(len(docIDBytes)))
	s.buffer = append(s.buffer, docIDBytes...)

	// Write field count
	s.writeUint32(uint32(len(document.Fields)))

	// Write each field
	for fieldName, field := range document.Fields {
		if err := s.writeField(fieldName, field); err != nil {
			return nil, fmt.Errorf("failed to write field %s: %w", fieldName, err)
		}
	}

	// Write timestamps (as Unix nanoseconds for performance)
	s.writeInt64(document.CreatedAt.UnixNano())
	s.writeInt64(document.UpdatedAt.UnixNano())

	// Write MVCC version metadata (4 × uint64 = 32 bytes)
	s.writeUint64(document.CreatedByTxID)
	s.writeUint64(document.DeletedByTxID)
	s.writeUint64(document.CommitSequence)
	s.writeUint64(document.VersionSequence)

	// Return copy of buffer to avoid mutation
	result := make([]byte, len(s.buffer))
	copy(result, s.buffer)
	return result, nil
}

// SerializeDocumentMap converts a map-based document to binary format
// Optimized for the current AppendDocumentToBundleFile usage pattern
func (s *FastDocumentSerializer) SerializeDocumentMap(docEntry map[string]interface{}) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// CRITICAL: Reset buffer length but preserve capacity to prevent reallocations
	// Reallocations during append() can cause race conditions if old buffer is still being copied
	s.buffer = s.buffer[:0]

	// If buffer is getting too large, reallocate fresh to prevent unbounded growth
	if cap(s.buffer) > 1024*1024 { // 1MB cap
		s.buffer = make([]byte, 0, 65536) // Reset to 64KB
	}

	// Write DocumentID
	if docID, ok := docEntry["DocumentID"].(string); ok {
		docIDBytes := []byte(docID)
		s.writeUint32(uint32(len(docIDBytes)))
		s.buffer = append(s.buffer, docIDBytes...)
	} else {
		return nil, fmt.Errorf("DocumentID is required and must be a string")
	}

	// Write Fields
	if fieldsInterface, ok := docEntry["Fields"]; ok {
		if fields, ok := fieldsInterface.(map[string]models.Field); ok {
			s.writeUint32(uint32(len(fields)))

			for fieldName, field := range fields {
				if err := s.writeField(fieldName, field); err != nil {
					return nil, fmt.Errorf("failed to write field %s: %w", fieldName, err)
				}
			}
		} else {
			s.writeUint32(0) // No fields
		}
	} else {
		s.writeUint32(0) // No fields
	}

	// Write timestamps
	if createdAt, ok := docEntry["CreatedAt"].(time.Time); ok {
		s.writeInt64(createdAt.UnixNano())
	} else {
		s.writeInt64(time.Now().UnixNano())
	}

	if updatedAt, ok := docEntry["UpdatedAt"].(time.Time); ok {
		s.writeInt64(updatedAt.UnixNano())
	} else {
		s.writeInt64(time.Now().UnixNano())
	}

	// Write MVCC version metadata (4 × uint64 = 32 bytes)
	// Default to 0 if not present (backward compatibility)
	var createdByTxID, deletedByTxID, commitSequence, versionSequence uint64
	if val, ok := docEntry["CreatedByTxID"].(uint64); ok {
		createdByTxID = val
	}
	if val, ok := docEntry["DeletedByTxID"].(uint64); ok {
		deletedByTxID = val
	}
	if val, ok := docEntry["CommitSequence"].(uint64); ok {
		commitSequence = val
	}
	if val, ok := docEntry["VersionSequence"].(uint64); ok {
		versionSequence = val
	}
	s.writeUint64(createdByTxID)
	s.writeUint64(deletedByTxID)
	s.writeUint64(commitSequence)
	s.writeUint64(versionSequence)

	// Return copy of buffer
	result := make([]byte, len(s.buffer))
	copy(result, s.buffer)
	return result, nil
}

// SerializeDocumentV2 converts a document to V2 binary format with field directory for O(1) field access.
// V2 Format Layout:
//
//	[Version:1][Flags:1][DocIDLen:4][DocID:N][FieldCount:4]
//	[FieldDirectory: FieldCount × 24 bytes]
//	[NameTable: variable length field names]
//	[DataSection: field values in same order as directory]
//	[CreatedAt:8][UpdatedAt:8][CreatedByTxID:8][DeletedByTxID:8][CommitSeq:8][VersionSeq:8]
//
// Each FieldDirectoryEntry (24 bytes):
//
//	[NameHash64:8][NameOffset:4][NameLen:2][FieldType:1][_pad:1][DataOffset:4][DataLen:4]
//
// Benefits:
//   - O(1) field lookup via xxHash64 matching in directory
//   - Projection pushdown: deserialize only requested fields by offset
//   - 3-5x faster hash computation than FNV-1a
func (s *FastDocumentSerializer) SerializeDocumentV2(document *models.Document) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Reset buffer but preserve capacity
	s.buffer = s.buffer[:0]
	if cap(s.buffer) > 1024*1024 {
		s.buffer = make([]byte, 0, 65536)
	}

	// ─────────────────────────────────────────────────────────────
	// 1. Write header: Version + Flags
	// ─────────────────────────────────────────────────────────────
	s.buffer = append(s.buffer, FormatVersionV2)
	flags := byte(FlagHasFieldDirectory) // Always set for V2
	s.buffer = append(s.buffer, flags)

	// ─────────────────────────────────────────────────────────────
	// 2. Write DocumentID
	// ─────────────────────────────────────────────────────────────
	docIDBytes := []byte(document.DocumentID)
	s.writeUint32(uint32(len(docIDBytes)))
	s.buffer = append(s.buffer, docIDBytes...)

	// ─────────────────────────────────────────────────────────────
	// 3. Write field count and reserve space for directory
	// ─────────────────────────────────────────────────────────────
	fieldCount := uint32(len(document.Fields))
	s.writeUint32(fieldCount)

	// Remember where directory starts - we'll back-patch offsets later
	directoryOffset := len(s.buffer)
	directorySize := int(fieldCount) * 24 // 24 bytes per FieldDirectoryEntry
	// Reserve space with zeros (will be filled in later)
	for i := 0; i < directorySize; i++ {
		s.buffer = append(s.buffer, 0)
	}

	// ─────────────────────────────────────────────────────────────
	// 4. Build field directory entries and write name table
	// ─────────────────────────────────────────────────────────────
	type fieldInfo struct {
		name      string
		field     models.Field
		entry     FieldDirectoryEntry
		nameStart uint32 // Offset in buffer where name starts
	}
	fieldInfos := make([]fieldInfo, 0, len(document.Fields))

	nameTableOffset := len(s.buffer)
	for fieldName, field := range document.Fields {
		info := fieldInfo{
			name:      fieldName,
			field:     field,
			nameStart: uint32(len(s.buffer) - nameTableOffset),
		}
		info.entry.NameHash64 = HashFieldName64(fieldName)
		info.entry.NameLen = uint16(len(fieldName))
		info.entry.FieldType = s.getFieldTypeCode(field)

		// Write field name to name table
		s.buffer = append(s.buffer, []byte(fieldName)...)
		fieldInfos = append(fieldInfos, info)
	}

	// ─────────────────────────────────────────────────────────────
	// 5. Write data section and record offsets
	// ─────────────────────────────────────────────────────────────
	dataSectionOffset := len(s.buffer)
	for i := range fieldInfos {
		dataStart := uint32(len(s.buffer) - dataSectionOffset)
		fieldInfos[i].entry.DataOffset = dataStart

		// Write field value based on type
		dataLen, err := s.writeFieldValueV2(fieldInfos[i].field)
		if err != nil {
			return nil, fmt.Errorf("failed to write field %s: %w", fieldInfos[i].name, err)
		}
		fieldInfos[i].entry.DataLen = dataLen
	}

	// ─────────────────────────────────────────────────────────────
	// 6. Back-patch directory with computed offsets
	// ─────────────────────────────────────────────────────────────
	for i, info := range fieldInfos {
		entryOffset := directoryOffset + (i * 24)
		info.entry.NameOffset = uint32(nameTableOffset + int(info.nameStart))

		// Write 24-byte directory entry
		binary.LittleEndian.PutUint64(s.buffer[entryOffset:entryOffset+8], info.entry.NameHash64)
		binary.LittleEndian.PutUint32(s.buffer[entryOffset+8:entryOffset+12], info.entry.NameOffset)
		binary.LittleEndian.PutUint16(s.buffer[entryOffset+12:entryOffset+14], info.entry.NameLen)
		s.buffer[entryOffset+14] = info.entry.FieldType
		s.buffer[entryOffset+15] = 0 // padding
		binary.LittleEndian.PutUint32(s.buffer[entryOffset+16:entryOffset+20], uint32(dataSectionOffset)+info.entry.DataOffset)
		binary.LittleEndian.PutUint32(s.buffer[entryOffset+20:entryOffset+24], info.entry.DataLen)
	}

	// ─────────────────────────────────────────────────────────────
	// 7. Write timestamps and MVCC metadata (48 bytes total)
	// ─────────────────────────────────────────────────────────────
	s.writeInt64(document.CreatedAt.UnixNano())
	s.writeInt64(document.UpdatedAt.UnixNano())
	s.writeUint64(document.CreatedByTxID)
	s.writeUint64(document.DeletedByTxID)
	s.writeUint64(document.CommitSequence)
	s.writeUint64(document.VersionSequence)

	// Return copy to prevent mutation
	result := make([]byte, len(s.buffer))
	copy(result, s.buffer)
	return result, nil
}

// getFieldTypeCode returns the V2 binary type code for a field
func (s *FastDocumentSerializer) getFieldTypeCode(field models.Field) byte {
	switch field.Value.Type {
	case models.FieldTypeNil:
		return V2TypeNull
	case models.FieldTypeString:
		return V2TypeString
	case models.FieldTypeInt:
		return V2TypeInt64
	case models.FieldTypeFloat:
		return V2TypeFloat64
	case models.FieldTypeBool:
		return V2TypeBool
	case models.FieldTypeDateTime:
		return V2TypeDateTime
	case models.FieldTypeDate:
		return V2TypeDate
	case models.FieldTypeInterface:
		// Interface can hold arrays, objects, or other complex types
		// Try to detect what it actually is
		val := field.Value.InterfaceVal
		switch val.(type) {
		case []interface{}, []string, []int, []float64:
			return V2TypeArray
		case map[string]interface{}:
			return V2TypeObject
		default:
			return V2TypeObject // Default to object for unknown
		}
	default:
		return V2TypeNull
	}
}

// writeFieldValueV2 writes just the field value (no name, no type prefix) and returns bytes written
func (s *FastDocumentSerializer) writeFieldValueV2(field models.Field) (uint32, error) {
	startLen := len(s.buffer)

	switch field.Value.Type {
	case models.FieldTypeNil:
		// Null has no data
		return 0, nil

	case models.FieldTypeString:
		strVal := field.Value.StringVal
		strBytes := []byte(strVal)
		s.writeUint32(uint32(len(strBytes)))
		s.buffer = append(s.buffer, strBytes...)

	case models.FieldTypeInt:
		s.writeInt64(field.Value.IntVal)

	case models.FieldTypeFloat:
		s.writeFloat64(field.Value.FloatVal)

	case models.FieldTypeBool:
		if field.Value.BoolVal {
			s.buffer = append(s.buffer, 1)
		} else {
			s.buffer = append(s.buffer, 0)
		}

	case models.FieldTypeDateTime:
		s.writeInt64(field.Value.DateTimeVal.UnixNano())

	case models.FieldTypeDate:
		s.writeInt64(field.Value.DateVal.UnixNano())

	case models.FieldTypeInterface:
		// InterfaceVal can hold arrays, objects, bytes, or other complex types
		// Serialize as JSON
		// TODO: I could implement native array/object serialization for better performance
		valBytes, err := json.Marshal(field.Value.InterfaceVal)
		if err != nil {
			return 0, fmt.Errorf("failed to marshal interface value: %w", err)
		}
		s.writeUint32(uint32(len(valBytes)))
		s.buffer = append(s.buffer, valBytes...)

	default:
		// Fallback: use AsInterface() and JSON marshal
		val := field.Value.AsInterface()
		valBytes, err := json.Marshal(val)
		if err != nil {
			return 0, fmt.Errorf("failed to marshal field value: %w", err)
		}
		s.writeUint32(uint32(len(valBytes)))
		s.buffer = append(s.buffer, valBytes...)
	}

	return uint32(len(s.buffer) - startLen), nil
}

// writeField writes a single field to the buffer
func (s *FastDocumentSerializer) writeField(fieldName string, field models.Field) error {
	// Write field name
	nameBytes := []byte(fieldName)
	s.writeUint32(uint32(len(nameBytes)))
	s.buffer = append(s.buffer, nameBytes...)

	// ✅ Check FieldValue type FIRST for DateTime/Date (before AsInterface loses type info)
	if field.Value.Type == models.FieldTypeDateTime {
		// DateTime type: store as Unix nanoseconds (type code 6)
		s.buffer = append(s.buffer, 6) // Type: datetime
		s.writeUint32(8)               // Size: 8 bytes
		s.writeInt64(field.Value.DateTimeVal.UnixNano())
		return nil
	}
	if field.Value.Type == models.FieldTypeDate {
		// Date type: store as Unix nanoseconds (type code 7)
		s.buffer = append(s.buffer, 7) // Type: date
		s.writeUint32(8)               // Size: 8 bytes
		s.writeInt64(field.Value.DateVal.UnixNano())
		return nil
	}

	// Write field value based on type
	switch value := field.Value.AsInterface().(type) { // ✅ Convert FieldValue to interface{} for type switch
	case string:
		s.buffer = append(s.buffer, 1) // Type: string
		valueBytes := []byte(value)
		s.writeUint32(uint32(len(valueBytes)))
		s.buffer = append(s.buffer, valueBytes...)

	case int:
		s.buffer = append(s.buffer, 2) // Type: int
		s.writeUint32(4)               // Size: 4 bytes
		s.writeInt32(int32(value))

	case int64:
		s.buffer = append(s.buffer, 3) // Type: int64
		s.writeUint32(8)               // Size: 8 bytes
		s.writeInt64(value)

	case float64:
		s.buffer = append(s.buffer, 4) // Type: float64
		s.writeUint32(8)               // Size: 8 bytes
		s.writeFloat64(value)

	case bool:
		s.buffer = append(s.buffer, 5) // Type: bool
		s.writeUint32(1)               // Size: 1 byte
		if value {
			s.buffer = append(s.buffer, 1)
		} else {
			s.buffer = append(s.buffer, 0)
		}

	case time.Time:
		// This case handles any time.Time values that weren't caught by FieldType check above
		// (e.g., from map data structures without FieldValue wrapper)
		// Default to DateTime type (type code 6)
		s.buffer = append(s.buffer, 6) // Type: datetime
		s.writeUint32(8)               // Size: 8 bytes
		s.writeInt64(value.UnixNano())

	default:
		// Fallback to string representation
		s.buffer = append(s.buffer, 1) // Type: string
		valueStr := fmt.Sprintf("%v", value)
		valueBytes := []byte(valueStr)
		s.writeUint32(uint32(len(valueBytes)))
		s.buffer = append(s.buffer, valueBytes...)
	}

	return nil
}

// Helper methods for writing binary data
func (s *FastDocumentSerializer) writeUint32(value uint32) {
	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, value)
	s.buffer = append(s.buffer, buf...)
}

func (s *FastDocumentSerializer) writeInt32(value int32) {
	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, uint32(value))
	s.buffer = append(s.buffer, buf...)
}

func (s *FastDocumentSerializer) writeInt64(value int64) {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, uint64(value))
	s.buffer = append(s.buffer, buf...)
}

func (s *FastDocumentSerializer) writeFloat64(value float64) {
	s.writeInt64(int64(math.Float64bits(value)))
}

func (s *FastDocumentSerializer) writeUint64(value uint64) {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, value)
	s.buffer = append(s.buffer, buf...)
}

// Global fast serializer instance with mutex protection
// OPTION 1: Mutex-protected shared serializer (chosen for production)
// Provides thread-safety while avoiding per-call allocations
var globalFastSerializer = NewFastDocumentSerializer()

// EncodeFastBinary replaces EncodeBSON for high-performance document serialization
// Provides 5-10x faster serialization by eliminating BSON overhead
//
// CRITICAL FIX: Uses mutex-protected global serializer to prevent race conditions
// Previous unprotected implementation caused corruption when multiple goroutines serialized:
// 1. Thread A: s.buffer[:0], writes DocumentID + Fields to shared buffer
// 2. Thread B: s.buffer[:0] ← RESETS Thread A's buffer during serialization!
// 3. Thread B overwrites buffer, Thread A makes copy of CORRUPTED data
// 4. Thread A writes garbage to disk → invalid field lengths (411199058, 3744236832 bytes)
// Mutex adds ~100ns overhead but prevents all corruption - acceptable tradeoff
func EncodeFastBinary(docEntry map[string]interface{}) ([]byte, error) {
	return globalFastSerializer.SerializeDocumentMap(docEntry)
}

// EncodeFastBinaryV2 serializes a document using V2 format with field directory.
// Provides O(1) field lookup during deserialization via xxHash64 field directory.
// Use this for new documents; existing documents can still be read with auto-detection.
func EncodeFastBinaryV2(doc *models.Document) ([]byte, error) {
	return globalFastSerializer.SerializeDocumentV2(doc)
}
