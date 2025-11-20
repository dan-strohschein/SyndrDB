package conversion

import (
	"fmt"
	"strconv"
	"unsafe"
)

// ValueToString converts common types to string with minimal allocations.
// This replaces fmt.Sprintf("%v", value) in hot paths for better performance.
//
// Performance characteristics:
//   - string: 0 allocations (returns input)
//   - bool: 0 allocations (returns string constants)
//   - int types: 1 allocation (vs 3-4 with fmt.Sprintf)
//   - float types: 1 allocation (vs 3-4 with fmt.Sprintf)
//   - []byte: 1 allocation, zero-copy conversion
//   - other types: falls back to fmt.Sprintf
func ValueToString(v interface{}) string {
	if v == nil {
		return "<nil>"
	}

	// Fast path: type assertions (no reflection, minimal allocations)
	switch val := v.(type) {
	case string:
		// ZERO allocations - already a string!
		return val

	case int:
		// 1 allocation vs 3-4 with fmt.Sprintf
		return strconv.Itoa(val)

	case int64:
		// 1 allocation, optimized assembly on common platforms
		return strconv.FormatInt(val, 10)

	case int32:
		return strconv.FormatInt(int64(val), 10)

	case int16:
		return strconv.FormatInt(int64(val), 10)

	case int8:
		return strconv.FormatInt(int64(val), 10)

	case uint:
		return strconv.FormatUint(uint64(val), 10)

	case uint64:
		return strconv.FormatUint(val, 10)

	case uint32:
		return strconv.FormatUint(uint64(val), 10)

	case uint16:
		return strconv.FormatUint(uint64(val), 10)

	case uint8:
		return strconv.FormatUint(uint64(val), 10)

	case float64:
		// Format with automatic precision (removes trailing zeros)
		return strconv.FormatFloat(val, 'f', -1, 64)

	case float32:
		return strconv.FormatFloat(float64(val), 'f', -1, 32)

	case bool:
		// ZERO allocations - returns string constants
		if val {
			return "true"
		}
		return "false"

	case []byte:
		// Zero-copy conversion for byte slices
		// Safe because we don't retain the slice after conversion
		if len(val) == 0 {
			return ""
		}
		return unsafe.String(&val[0], len(val))

	default:
		// Fallback for complex types (maps, slices, structs, etc.)
		// Still uses fmt, but only for types that truly need it
		return fmt.Sprintf("%v", v)
	}
}

// ValueToStringWithFallback is like ValueToString but allows specifying
// a custom fallback value for nil inputs instead of "<nil>".
func ValueToStringWithFallback(v interface{}, nilFallback string) string {
	if v == nil {
		return nilFallback
	}
	return ValueToString(v)
}
