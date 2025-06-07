package hashindex

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"
)

// encodeFieldValue encodes a field value into a byte slice optimized for hash indexing
func encodeFieldValue(value interface{}, indexField IndexField) ([]byte, string, error) {
	var buffer bytes.Buffer
	var keyString string

	switch v := value.(type) {
	case string:
		keyString = v
		// For string fields, apply collation if specified
		if indexField.Collation == "case_insensitive" {
			v = strings.ToLower(v)
		}

		// Type tag for string (1 byte)
		buffer.WriteByte(1)

		// Write string directly (no length prefix needed for hash indexes)
		buffer.Write([]byte(v))

	case int:
		keyString = fmt.Sprintf("%d", v)
		buffer.WriteByte(2) // Type tag for integer

		// Use fixed 8 bytes for all integers for consistent hashing
		binary.Write(&buffer, binary.LittleEndian, int64(v))

	case int64:
		keyString = fmt.Sprintf("%d", v)
		buffer.WriteByte(2) // Type tag for integer
		binary.Write(&buffer, binary.LittleEndian, v)

	case float64:
		keyString = fmt.Sprintf("%g", v)
		buffer.WriteByte(3) // Type tag for float

		// Ensure NaN and Infinity are handled consistently
		if math.IsNaN(v) {
			binary.Write(&buffer, binary.LittleEndian, math.Float64bits(math.NaN()))
		} else if math.IsInf(v, 1) {
			binary.Write(&buffer, binary.LittleEndian, math.Float64bits(math.Inf(1)))
		} else if math.IsInf(v, -1) {
			binary.Write(&buffer, binary.LittleEndian, math.Float64bits(math.Inf(-1)))
		} else {
			binary.Write(&buffer, binary.LittleEndian, v)
		}

	case bool:
		keyString = fmt.Sprintf("%t", v)
		buffer.WriteByte(4) // Type tag for boolean
		if v {
			buffer.WriteByte(1)
		} else {
			buffer.WriteByte(0)
		}

	case time.Time:
		keyString = v.Format(time.RFC3339)
		buffer.WriteByte(5) // Type tag for timestamp

		// For time values, use Unix nanoseconds for precise and fast comparison
		binary.Write(&buffer, binary.LittleEndian, v.UnixNano())

	case nil:
		keyString = "NULL"
		buffer.WriteByte(0) // Type tag for NULL

	case []byte:
		// Binary data - use as is with type tag
		keyString = fmt.Sprintf("BINARY[%d bytes]", len(v))
		buffer.WriteByte(6) // Type tag for binary data
		buffer.Write(v)

	case map[string]interface{}:
		// For hash indexes, complex objects aren't ideal
		// But we provide support by converting to a JSON-like string
		keyString = fmt.Sprintf("object:%p", v)
		buffer.WriteByte(7) // Type tag for object
		str := objectToString(v)
		buffer.Write([]byte(str))

	default:
		// For any other type, convert to string
		str := fmt.Sprintf("%v", v)
		keyString = str
		buffer.WriteByte(9) // Type tag for other
		buffer.Write([]byte(str))
	}

	return buffer.Bytes(), keyString, nil
}

// objectToString converts a map to a deterministic string representation
func objectToString(obj map[string]interface{}) string {
	// Sort keys for deterministic output
	keys := make([]string, 0, len(obj))
	for k := range obj {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var b strings.Builder
	b.WriteString("{")
	for i, k := range keys {
		if i > 0 {
			b.WriteString(",")
		}
		b.WriteString(k)
		b.WriteString(":")

		// Handle nested objects
		switch v := obj[k].(type) {
		case map[string]interface{}:
			b.WriteString(objectToString(v))
		case []interface{}:
			b.WriteString("[")
			for j, item := range v {
				if j > 0 {
					b.WriteString(",")
				}
				if m, ok := item.(map[string]interface{}); ok {
					b.WriteString(objectToString(m))
				} else {
					b.WriteString(fmt.Sprintf("%v", item))
				}
			}
			b.WriteString("]")
		default:
			b.WriteString(fmt.Sprintf("%v", v))
		}
	}
	b.WriteString("}")
	return b.String()
}
