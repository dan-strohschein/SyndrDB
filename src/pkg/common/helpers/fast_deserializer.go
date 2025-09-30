package helpers

import (
	"encoding/binary"
	"fmt"
	"math"
	"syndrdb/src/internal/domain/models"
	"time"
)

// FastDocumentDeserializer provides high-performance document deserialization
// Mirrors FastDocumentSerializer by reading the same binary format
// Designed specifically for reading append-only document storage with minimal overhead
type FastDocumentDeserializer struct {
	data   []byte // Current data being read
	offset int    // Current read position
}

// NewFastDocumentDeserializer creates a new fast deserializer instance
func NewFastDocumentDeserializer() *FastDocumentDeserializer {
	return &FastDocumentDeserializer{}
}

// DeserializeDocument converts optimized binary format back to a document
// Binary format: [DocumentID_len][DocumentID][field_count][field1][field2]...[CreatedAt][UpdatedAt]
// Each field: [name_len][name][type][value_len][value]
func (d *FastDocumentDeserializer) DeserializeDocument(data []byte) (*models.Document, error) {
	d.data = data
	d.offset = 0

	// Read document ID
	documentID, err := d.readString()
	if err != nil {
		return nil, fmt.Errorf("failed to read document ID: %w", err)
	}

	// Read field count
	fieldCount, err := d.readUint32()
	if err != nil {
		return nil, fmt.Errorf("failed to read field count: %w", err)
	}

	// Read each field
	fields := make(map[string]models.Field)
	for i := uint32(0); i < fieldCount; i++ {
		fieldName, field, err := d.readField()
		if err != nil {
			return nil, fmt.Errorf("failed to read field %d: %w", i, err)
		}
		fields[fieldName] = field
	}

	// Read timestamps
	createdAtNano, err := d.readInt64()
	if err != nil {
		return nil, fmt.Errorf("failed to read CreatedAt: %w", err)
	}

	updatedAtNano, err := d.readInt64()
	if err != nil {
		return nil, fmt.Errorf("failed to read UpdatedAt: %w", err)
	}

	// Convert timestamps back to time.Time
	createdAt := time.Unix(0, createdAtNano)
	updatedAt := time.Unix(0, updatedAtNano)

	return &models.Document{
		DocumentID: documentID,
		Fields:     fields,
		CreatedAt:  createdAt,
		UpdatedAt:  updatedAt,
	}, nil
}

// DeserializeDocumentMap converts binary format back to a map-based document
// Mirrors SerializeDocumentMap for compatibility with current usage patterns
func (d *FastDocumentDeserializer) DeserializeDocumentMap(data []byte) (map[string]interface{}, error) {
	d.data = data
	d.offset = 0

	docEntry := make(map[string]interface{})

	// Read DocumentID
	documentID, err := d.readString()
	if err != nil {
		return nil, fmt.Errorf("failed to read document ID: %w", err)
	}
	docEntry["DocumentID"] = documentID

	// Read field count
	fieldCount, err := d.readUint32()
	if err != nil {
		return nil, fmt.Errorf("failed to read field count: %w", err)
	}

	// Read each field
	fields := make(map[string]models.Field)
	for i := uint32(0); i < fieldCount; i++ {
		fieldName, field, err := d.readField()
		if err != nil {
			return nil, fmt.Errorf("failed to read field %d: %w", i, err)
		}
		fields[fieldName] = field
	}
	docEntry["Fields"] = fields

	// Read timestamps
	createdAtNano, err := d.readInt64()
	if err != nil {
		return nil, fmt.Errorf("failed to read CreatedAt: %w", err)
	}

	updatedAtNano, err := d.readInt64()
	if err != nil {
		return nil, fmt.Errorf("failed to read UpdatedAt: %w", err)
	}

	// Convert timestamps back to time.Time
	docEntry["CreatedAt"] = time.Unix(0, createdAtNano)
	docEntry["UpdatedAt"] = time.Unix(0, updatedAtNano)

	return docEntry, nil
}

// readField reads a single field from the buffer
func (d *FastDocumentDeserializer) readField() (string, models.Field, error) {
	// Read field name
	fieldName, err := d.readString()
	if err != nil {
		return "", models.Field{}, fmt.Errorf("failed to read field name: %w", err)
	}

	// Read field type
	fieldType, err := d.readByte()
	if err != nil {
		return "", models.Field{}, fmt.Errorf("failed to read field type: %w", err)
	}

	// Read field value based on type
	var value interface{}
	switch fieldType {
	case 1: // string
		valueLen, err := d.readUint32()
		if err != nil {
			return "", models.Field{}, fmt.Errorf("failed to read string length: %w", err)
		}
		valueBytes, err := d.readBytes(int(valueLen))
		if err != nil {
			return "", models.Field{}, fmt.Errorf("failed to read string value: %w", err)
		}
		value = string(valueBytes)

	case 2: // int (stored as int32)
		_, err := d.readUint32() // Read size (should be 4)
		if err != nil {
			return "", models.Field{}, fmt.Errorf("failed to read int size: %w", err)
		}
		intVal, err := d.readInt32()
		if err != nil {
			return "", models.Field{}, fmt.Errorf("failed to read int value: %w", err)
		}
		value = int(intVal)

	case 3: // int64
		_, err := d.readUint32() // Read size (should be 8)
		if err != nil {
			return "", models.Field{}, fmt.Errorf("failed to read int64 size: %w", err)
		}
		value, err = d.readInt64()
		if err != nil {
			return "", models.Field{}, fmt.Errorf("failed to read int64 value: %w", err)
		}

	case 4: // float64
		_, err := d.readUint32() // Read size (should be 8)
		if err != nil {
			return "", models.Field{}, fmt.Errorf("failed to read float64 size: %w", err)
		}
		value, err = d.readFloat64()
		if err != nil {
			return "", models.Field{}, fmt.Errorf("failed to read float64 value: %w", err)
		}

	case 5: // bool
		_, err := d.readUint32() // Read size (should be 1)
		if err != nil {
			return "", models.Field{}, fmt.Errorf("failed to read bool size: %w", err)
		}
		boolByte, err := d.readByte()
		if err != nil {
			return "", models.Field{}, fmt.Errorf("failed to read bool value: %w", err)
		}
		value = boolByte == 1

	default:
		return "", models.Field{}, fmt.Errorf("unknown field type: %d", fieldType)
	}

	return fieldName, models.Field{
		Name:  fieldName,
		Value: value,
	}, nil
}

// Helper methods for reading binary data
func (d *FastDocumentDeserializer) readUint32() (uint32, error) {
	if d.offset+4 > len(d.data) {
		return 0, fmt.Errorf("insufficient data for uint32 at offset %d", d.offset)
	}
	value := binary.LittleEndian.Uint32(d.data[d.offset : d.offset+4])
	d.offset += 4
	return value, nil
}

func (d *FastDocumentDeserializer) readInt32() (int32, error) {
	value, err := d.readUint32()
	if err != nil {
		return 0, err
	}
	return int32(value), nil
}

func (d *FastDocumentDeserializer) readInt64() (int64, error) {
	if d.offset+8 > len(d.data) {
		return 0, fmt.Errorf("insufficient data for int64 at offset %d", d.offset)
	}
	value := binary.LittleEndian.Uint64(d.data[d.offset : d.offset+8])
	d.offset += 8
	return int64(value), nil
}

func (d *FastDocumentDeserializer) readFloat64() (float64, error) {
	intVal, err := d.readInt64()
	if err != nil {
		return 0, err
	}
	return math.Float64frombits(uint64(intVal)), nil
}

func (d *FastDocumentDeserializer) readByte() (byte, error) {
	if d.offset+1 > len(d.data) {
		return 0, fmt.Errorf("insufficient data for byte at offset %d", d.offset)
	}
	value := d.data[d.offset]
	d.offset++
	return value, nil
}

func (d *FastDocumentDeserializer) readBytes(length int) ([]byte, error) {
	if d.offset+length > len(d.data) {
		return nil, fmt.Errorf("insufficient data for %d bytes at offset %d", length, d.offset)
	}
	value := d.data[d.offset : d.offset+length]
	d.offset += length
	return value, nil
}

func (d *FastDocumentDeserializer) readString() (string, error) {
	length, err := d.readUint32()
	if err != nil {
		return "", fmt.Errorf("failed to read string length: %w", err)
	}
	bytes, err := d.readBytes(int(length))
	if err != nil {
		return "", fmt.Errorf("failed to read string bytes: %w", err)
	}
	return string(bytes), nil
}

// Global fast deserializer instance to avoid repeated allocations
var globalFastDeserializer = NewFastDocumentDeserializer()

// DecodeFastBinary replaces DecodeBSON for high-performance document deserialization
// Provides 5-10x faster deserialization by eliminating BSON overhead
// Mirrors EncodeFastBinary function from fast_serializer.go
func DecodeFastBinary(data []byte) (map[string]interface{}, error) {
	return globalFastDeserializer.DeserializeDocumentMap(data)
}

// DecodeFastBinaryToDocument deserializes binary data directly to a models.Document
// Provides direct deserialization without the map conversion overhead
func DecodeFastBinaryToDocument(data []byte) (*models.Document, error) {
	return globalFastDeserializer.DeserializeDocument(data)
}
