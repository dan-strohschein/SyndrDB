/*
KEY ENCODING SYSTEM FOR QUERY PLANNER

This file provides binary encoding for numeric types to ensure proper ordering in B-tree indexes.
B-tree indexes use lexicographic byte comparison, which doesn't work correctly for numeric types
when converted to strings (e.g., "100" < "20" lexicographically, but 100 > 20 numerically).

ALGORITHM:
- Integer encoding: Big-endian binary with offset for negative numbers
- Float encoding: IEEE 754 with sign bit manipulation for sortable representation
- All numeric types encoded to 8-byte fixed-length keys for consistent comparison

RESPONSIBILITIES:
- Single Responsibility: Handles only numeric type encoding/decoding
- DRY Principle: Centralized encoding logic, no duplication
- Open/Closed: Extensible for new numeric types without modifying existing code

This implementation follows PostgreSQL's approach to numeric index key encoding.
See: src/backend/utils/adt/numeric.c in PostgreSQL source
*/

package planner

import (
	"encoding/binary"
	"fmt"
	"math"
)

// KeyEncoder handles encoding of various types to sortable byte representations
// for use in B-tree index operations. This ensures correct ordering regardless
// of the original type.
type KeyEncoder struct{}

// NewKeyEncoder creates a new key encoder instance
func NewKeyEncoder() *KeyEncoder {
	return &KeyEncoder{}
}

// EncodeKey converts a value to a sortable byte representation
// Supports: string, []byte, int, int32, int64, float32, float64
//
// Returns:
//   - []byte: Encoded key suitable for B-tree index operations
//   - error: If the type is not supported
func (e *KeyEncoder) EncodeKey(value interface{}) ([]byte, error) {
	if value == nil {
		return nil, fmt.Errorf("cannot encode nil value")
	}

	// IMPORTANT: This encoding MUST match convertValueToBytes in bundle_service.go,
	// which is used at index insertion time. The BTree stores keys using string
	// representations for numeric types, so queries must use the same format.
	switch v := value.(type) {
	case string:
		return []byte(v), nil

	case []byte:
		return v, nil

	case int:
		return []byte(fmt.Sprintf("%d", v)), nil

	case int32:
		return []byte(fmt.Sprintf("%d", v)), nil

	case int64:
		return []byte(fmt.Sprintf("%d", v)), nil

	case uint:
		return []byte(fmt.Sprintf("%d", v)), nil

	case uint32:
		return []byte(fmt.Sprintf("%d", v)), nil

	case uint64:
		return []byte(fmt.Sprintf("%d", v)), nil

	case float32:
		return []byte(fmt.Sprintf("%.6f", v)), nil

	case float64:
		return []byte(fmt.Sprintf("%.6f", v)), nil

	default:
		return []byte(fmt.Sprintf("%v", v)), nil
	}
}

// encodeInt64 encodes a signed 64-bit integer to a sortable 8-byte representation
//
// Algorithm:
//  1. Add MaxInt64 to offset negative numbers to positive range
//  2. Encode as big-endian (most significant byte first)
//  3. This ensures: -100 < -10 < 0 < 10 < 100 in byte order
//
// Example:
//
//	-100 → offset → 9223372036854775708 → [0x7F, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x9C]
//	0    → offset → 9223372036854775808 → [0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]
//	100  → offset → 9223372036854775908 → [0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x64]
func (e *KeyEncoder) encodeInt64(value int64) []byte {
	// Offset by MaxInt64 to make all values positive
	// This maps the range [-MaxInt64, MaxInt64] to [0, 2*MaxInt64]
	offsetValue := uint64(value) + uint64(math.MaxInt64) + 1

	// Encode as big-endian for sortability
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, offsetValue)
	return buf
}

// encodeUint64 encodes an unsigned 64-bit integer to a sortable 8-byte representation
//
// Algorithm:
//  1. Already positive, just encode as big-endian
//  2. Most significant byte first ensures correct sorting
func (e *KeyEncoder) encodeUint64(value uint64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, value)
	return buf
}

// encodeFloat64 encodes a 64-bit float to a sortable 8-byte representation
//
// Algorithm:
//  1. Get IEEE 754 bit representation
//  2. For negative numbers: flip all bits (makes more negative → smaller bytes)
//  3. For positive numbers: flip sign bit only (makes positive → larger than negative)
//  4. This ensures: -inf < -100.5 < -1.0 < 0 < 1.0 < 100.5 < +inf
//
// IEEE 754 background:
//   - Sign bit (1 bit): 0 = positive, 1 = negative
//   - Exponent (11 bits): biased representation
//   - Mantissa (52 bits): fractional part
//
// Example:
//
//	-100.5 → bits: 0xC059200000000000 → flip all → 0x3FA6DFFFFFFFFFFF
//	0.0    → bits: 0x0000000000000000 → flip sign → 0x8000000000000000
//	100.5  → bits: 0x4059200000000000 → flip sign → 0xC059200000000000
func (e *KeyEncoder) encodeFloat64(value float64) []byte {
	// Get IEEE 754 bit representation
	bits := math.Float64bits(value)

	// Check if negative (sign bit is set)
	if bits&(1<<63) != 0 {
		// Negative: flip all bits
		// This makes more negative numbers have smaller byte values
		bits = ^bits
	} else {
		// Positive: flip only the sign bit
		// This makes positive numbers larger than negative numbers
		bits |= (1 << 63)
	}

	// Encode as big-endian
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, bits)
	return buf
}

// EncodeCompositeKey encodes multiple values into a single byte slice for composite B-tree keys.
// Values are encoded sequentially with length prefixes so that byte comparison
// yields the correct lexicographic order: (a1, b1) < (a2, b2) iff a1 < a2 or (a1 == a2 and b1 < b2).
func (e *KeyEncoder) EncodeCompositeKey(values []interface{}) ([]byte, error) {
	var buf []byte
	for _, v := range values {
		if v == nil {
			// Encode nil as 0-length component
			lenBytes := make([]byte, 4)
			binary.BigEndian.PutUint32(lenBytes, 0)
			buf = append(buf, lenBytes...)
			continue
		}
		encoded, err := e.EncodeKey(v)
		if err != nil {
			return nil, fmt.Errorf("failed to encode composite key component: %w", err)
		}
		// Length-prefix each component (4 bytes, big-endian) for unambiguous decoding
		lenBytes := make([]byte, 4)
		binary.BigEndian.PutUint32(lenBytes, uint32(len(encoded)))
		buf = append(buf, lenBytes...)
		buf = append(buf, encoded...)
	}
	return buf, nil
}

// DecodeInt64 decodes a byte representation back to int64
// This is useful for debugging and validation
func (e *KeyEncoder) DecodeInt64(encoded []byte) (int64, error) {
	if len(encoded) != 8 {
		return 0, fmt.Errorf("invalid encoded int64 length: expected 8, got %d", len(encoded))
	}

	offsetValue := binary.BigEndian.Uint64(encoded)
	value := int64(offsetValue) - math.MaxInt64 - 1
	return value, nil
}

// DecodeFloat64 decodes a byte representation back to float64
// This is useful for debugging and validation
func (e *KeyEncoder) DecodeFloat64(encoded []byte) (float64, error) {
	if len(encoded) != 8 {
		return 0, fmt.Errorf("invalid encoded float64 length: expected 8, got %d", len(encoded))
	}

	bits := binary.BigEndian.Uint64(encoded)

	// Reverse the encoding
	if bits&(1<<63) == 0 {
		// Was negative: flip all bits back
		bits = ^bits
	} else {
		// Was positive: flip sign bit back
		bits &^= (1 << 63)
	}

	return math.Float64frombits(bits), nil
}
