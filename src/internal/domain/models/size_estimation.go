package models

/*
DOCUMENT SIZE ESTIMATION FOR MEMORY TRACKING

This file implements memory size estimation for Document structures used in per-query
memory limit tracking. It provides conservative estimates by recursively traversing
all fields and nested structures to calculate total byte usage.

DESIGN PRINCIPLES:
- Single Responsibility: Only estimates memory footprint of Document structures
- Conservative Estimation: Overestimates rather than underestimates to prevent OOM
- Recursive Traversal: Handles nested maps, slices, and complex field values
- Zero-Allocation: Uses values not pointers where possible

SIZE CALCULATION:
- Base document overhead: ~200 bytes (struct fields, metadata)
- DocumentID: len(string) bytes
- Fields map: ~16 bytes per entry + field name + field value
- Field values: Varies by FieldValueType (string, int64, float64, etc.)
- Nested structures: Recursive calculation for maps and slices

ACCURACY VS PERFORMANCE:
- This is a fast conservative estimate, not precise memory profiling
- Assumes typical Go runtime overhead (map entries, slice headers, etc.)
- Does not account for memory alignment or GC metadata
- Good enough for DoS protection (5-10% error acceptable)

FIELD VALUE TYPES SUPPORTED:
- FieldTypeString: len(string) bytes
- FieldTypeInt: 8 bytes (int64)
- FieldTypeFloat: 8 bytes (float64)
- FieldTypeBool: 1 byte
- FieldTypeDateTime/Date: 24 bytes (time.Time struct)
- FieldTypeInterface: Recursive calculation for maps/slices
- FieldTypeNil: 0 bytes

FUTURE ENHANCEMENTS:
- TODO: I could add runtime.MemStats profiling for more accurate measurements if needed
- TODO: I could cache average document sizes per bundle for faster estimation
- TODO: I could add size estimation for Data map (raw document data) if used
*/

import (
	"time"
)

// EstimateDocumentSize calculates the approximate memory footprint of a Document in bytes
// This is a conservative estimate used for memory limit tracking
// Returns estimated bytes including all fields, field names, and nested structures
func EstimateDocumentSize(doc *Document) int64 {
	if doc == nil {
		return 0
	}

	// Base document structure overhead
	// Includes: DocumentID, Fields map, Data map, CreatedAt, UpdatedAt
	size := int64(200) // Conservative base overhead estimate

	// DocumentID string
	size += int64(len(doc.DocumentID))

	// Fields map
	if doc.Fields != nil {
		// Map overhead: ~48 bytes base + ~16 bytes per entry
		size += 48 + int64(len(doc.Fields))*16

		// Each field: field name + field value
		for fieldName, field := range doc.Fields {
			size += int64(len(fieldName))               // Field name
			size += estimateFieldValueSize(field.Value) // Field value
		}
	}

	// Data map (if present - used for storage compatibility)
	if doc.Data != nil {
		// Map overhead
		size += 48 + int64(len(doc.Data))*16

		// Each data entry
		for key, value := range doc.Data {
			size += int64(len(key))
			size += estimateInterfaceSize(value)
		}
	}

	// time.Time fields (CreatedAt, UpdatedAt)
	// time.Time is 24 bytes each
	size += 24 + 24

	return size
}

// estimateFieldValueSize calculates the size of a FieldValue based on its type
// This handles the typed union structure efficiently
func estimateFieldValueSize(fv FieldValue) int64 {
	// FieldValue struct overhead: discriminator + union storage
	// Approximately 64 bytes for the struct itself
	size := int64(64)

	switch fv.Type {
	case FieldTypeString:
		// String value: len(string)
		size += int64(len(fv.StringVal))

	case FieldTypeInt:
		// int64: already counted in struct size (8 bytes)
		// No additional allocation needed

	case FieldTypeFloat:
		// float64: already counted in struct size (8 bytes)
		// No additional allocation needed

	case FieldTypeBool:
		// bool: already counted in struct size (1 byte)
		// No additional allocation needed

	case FieldTypeDateTime, FieldTypeDate:
		// time.Time: 24 bytes
		// Already counted in struct size

	case FieldTypeInterface:
		// Complex types (arrays, maps, objects)
		size += estimateInterfaceSize(fv.InterfaceVal)

	case FieldTypeNil:
		// Nil value: no additional size

	default:
		// Unknown type: conservative 64 byte estimate
		size += 64
	}

	return size
}

// estimateInterfaceSize estimates the size of an interface{} value
// Handles maps, slices, strings, numbers, and nested structures recursively
func estimateInterfaceSize(value interface{}) int64 {
	if value == nil {
		return 0
	}

	switch v := value.(type) {
	case string:
		return int64(len(v))

	case int, int8, int16, int32, int64:
		return 8 // Conservative 8 bytes for any integer

	case uint, uint8, uint16, uint32, uint64:
		return 8 // Conservative 8 bytes for any unsigned integer

	case float32, float64:
		return 8 // 8 bytes for floats

	case bool:
		return 1

	case time.Time:
		return 24 // time.Time struct size

	case []interface{}:
		// Slice: header (24 bytes) + elements
		size := int64(24)
		for _, item := range v {
			size += estimateInterfaceSize(item)
		}
		return size

	case map[string]interface{}:
		// Map: base overhead (48 bytes) + entries
		size := int64(48 + len(v)*16) // Map overhead + entry overhead
		for key, val := range v {
			size += int64(len(key))
			size += estimateInterfaceSize(val)
		}
		return size

	case []map[string]interface{}:
		// Array of objects
		size := int64(24) // Slice header
		for _, item := range v {
			size += estimateInterfaceSize(item)
		}
		return size

	case []string:
		// String slice
		size := int64(24) // Slice header
		for _, str := range v {
			size += int64(len(str))
		}
		return size

	case []int:
		// Int slice: header + 8 bytes per int
		return int64(24 + len(v)*8)

	case []int64:
		// Int64 slice: header + 8 bytes per int64
		return int64(24 + len(v)*8)

	case []float64:
		// Float64 slice: header + 8 bytes per float64
		return int64(24 + len(v)*8)

	case []bool:
		// Bool slice: header + 1 byte per bool
		return int64(24 + len(v))

	default:
		// Unknown type: conservative 64 byte estimate
		// This handles custom types, pointers, channels, etc.
		return 64
	}
}

// EstimateBatchSize estimates the total size of a batch of documents
// This is more efficient than calling EstimateDocumentSize for each document separately
// when processing large batches
func EstimateBatchSize(docs []*Document) int64 {
	if len(docs) == 0 {
		return 0
	}

	var totalSize int64
	for _, doc := range docs {
		totalSize += EstimateDocumentSize(doc)
	}

	return totalSize
}

// EstimateAverageDocumentSize estimates the average size of documents in a sample
// Useful for projection calculations when total document count is known
// Returns 0 if sample is empty
func EstimateAverageDocumentSize(sample []*Document) int64 {
	if len(sample) == 0 {
		return 0
	}

	totalSize := EstimateBatchSize(sample)
	return totalSize / int64(len(sample))
}
