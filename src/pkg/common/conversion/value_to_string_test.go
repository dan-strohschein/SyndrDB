package conversion

import (
	"fmt"
	"strings"
	"testing"
)

// TestValueToString_AllTypes tests all supported type conversions
func TestValueToString_AllTypes(t *testing.T) {
	tests := []struct {
		name     string
		input    interface{}
		expected string
	}{
		// Nil
		{"nil", nil, "<nil>"},

		// String types
		{"string_empty", "", ""},
		{"string_simple", "hello", "hello"},
		{"string_unicode", "世界", "世界"},

		// Integer types
		{"int_positive", 42, "42"},
		{"int_negative", -42, "-42"},
		{"int_zero", 0, "0"},
		{"int64_max", int64(9223372036854775807), "9223372036854775807"},
		{"int64_min", int64(-9223372036854775808), "-9223372036854775808"},
		{"int32", int32(2147483647), "2147483647"},
		{"int16", int16(32767), "32767"},
		{"int8", int8(127), "127"},

		// Unsigned integer types
		{"uint", uint(42), "42"},
		{"uint64", uint64(18446744073709551615), "18446744073709551615"},
		{"uint32", uint32(4294967295), "4294967295"},
		{"uint16", uint16(65535), "65535"},
		{"uint8", uint8(255), "255"},

		// Float types
		{"float64_simple", 3.14, "3.14"},
		{"float64_integer", 42.0, "42"},
		{"float64_negative", -3.14, "-3.14"},
		{"float64_zero", 0.0, "0"},
		{"float32", float32(3.14), "3.14"},
		{"float64_small", 0.000001, "0.000001"},
		{"float64_large", 1234567.89, "1234567.89"},

		// Boolean types
		{"bool_true", true, "true"},
		{"bool_false", false, "false"},

		// Byte slice
		{"bytes_empty", []byte{}, ""},
		{"bytes_simple", []byte("hello"), "hello"},
		{"bytes_binary", []byte{0x48, 0x65, 0x6c, 0x6c, 0x6f}, "Hello"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ValueToString(tt.input)
			if result != tt.expected {
				t.Errorf("ValueToString(%v) = %q, want %q", tt.input, result, tt.expected)
			}
		})
	}
}

// TestValueToString_ComplexTypes tests fallback to fmt.Sprintf for complex types
func TestValueToString_ComplexTypes(t *testing.T) {
	tests := []struct {
		name  string
		input interface{}
	}{
		{"slice", []int{1, 2, 3}},
		{"map", map[string]int{"a": 1, "b": 2}},
		{"struct", struct{ Name string }{"test"}},
		{"pointer", new(int)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ValueToString(tt.input)
			expected := fmt.Sprintf("%v", tt.input)
			if result != expected {
				t.Errorf("ValueToString(%v) = %q, want %q", tt.input, result, expected)
			}
		})
	}
}

// TestValueToStringWithFallback tests custom nil handling
func TestValueToStringWithFallback(t *testing.T) {
	tests := []struct {
		name     string
		input    interface{}
		fallback string
		expected string
	}{
		{"nil_custom", nil, "NULL", "NULL"},
		{"nil_empty", nil, "", ""},
		{"string_ignore_fallback", "hello", "NULL", "hello"},
		{"int_ignore_fallback", 42, "NULL", "42"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ValueToStringWithFallback(tt.input, tt.fallback)
			if result != tt.expected {
				t.Errorf("ValueToStringWithFallback(%v, %q) = %q, want %q",
					tt.input, tt.fallback, result, tt.expected)
			}
		})
	}
}

// TestValueToString_Compatibility verifies output matches fmt.Sprintf for all types
func TestValueToString_Compatibility(t *testing.T) {
	tests := []interface{}{
		"string",
		42,
		int64(42),
		int32(42),
		int16(42),
		int8(42),
		uint(42),
		uint64(42),
		uint32(42),
		uint16(42),
		uint8(42),
		3.14,
		float32(3.14),
		true,
		false,
		[]byte("hello"),
	}

	for _, input := range tests {
		t.Run(fmt.Sprintf("%T_%v", input, input), func(t *testing.T) {
			result := ValueToString(input)
			expected := fmt.Sprintf("%v", input)

			// Special case: []byte prints as "[104 101 108 108 111]" with %v
			// but we want "hello" for performance reasons
			if _, ok := input.([]byte); ok {
				expected = string(input.([]byte))
			}

			if result != expected {
				t.Errorf("ValueToString(%v) = %q, want %q", input, result, expected)
			}
		})
	}
}

// BenchmarkValueToString_String tests zero-allocation string handling
func BenchmarkValueToString_String(b *testing.B) {
	input := "test_string_value"
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = ValueToString(input)
	}
}

// BenchmarkFmtSprintf_String baseline comparison
func BenchmarkFmtSprintf_String(b *testing.B) {
	input := "test_string_value"
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = fmt.Sprintf("%v", input)
	}
}

// BenchmarkValueToString_Int tests optimized integer conversion
func BenchmarkValueToString_Int(b *testing.B) {
	input := 123456789
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = ValueToString(input)
	}
}

// BenchmarkFmtSprintf_Int baseline comparison
func BenchmarkFmtSprintf_Int(b *testing.B) {
	input := 123456789
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = fmt.Sprintf("%v", input)
	}
}

// BenchmarkValueToString_Int64 tests int64 conversion
func BenchmarkValueToString_Int64(b *testing.B) {
	input := int64(123456789)
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = ValueToString(input)
	}
}

// BenchmarkFmtSprintf_Int64 baseline comparison
func BenchmarkFmtSprintf_Int64(b *testing.B) {
	input := int64(123456789)
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = fmt.Sprintf("%v", input)
	}
}

// BenchmarkValueToString_Float tests float conversion
func BenchmarkValueToString_Float(b *testing.B) {
	input := 3.14159265359
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = ValueToString(input)
	}
}

// BenchmarkFmtSprintf_Float baseline comparison
func BenchmarkFmtSprintf_Float(b *testing.B) {
	input := 3.14159265359
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = fmt.Sprintf("%v", input)
	}
}

// BenchmarkValueToString_Bool tests zero-allocation bool handling
func BenchmarkValueToString_Bool(b *testing.B) {
	input := true
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = ValueToString(input)
	}
}

// BenchmarkFmtSprintf_Bool baseline comparison
func BenchmarkFmtSprintf_Bool(b *testing.B) {
	input := true
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = fmt.Sprintf("%v", input)
	}
}

// BenchmarkValueToString_Mixed simulates real-world mixed type usage
func BenchmarkValueToString_Mixed(b *testing.B) {
	inputs := []interface{}{
		"string_key",
		123,
		int64(456),
		3.14,
		true,
		"another_string",
		789,
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		for _, input := range inputs {
			_ = ValueToString(input)
		}
	}
}

// BenchmarkFmtSprintf_Mixed baseline comparison
func BenchmarkFmtSprintf_Mixed(b *testing.B) {
	inputs := []interface{}{
		"string_key",
		123,
		int64(456),
		3.14,
		true,
		"another_string",
		789,
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		for _, input := range inputs {
			_ = fmt.Sprintf("%v", input)
		}
	}
}

// TestValueToString_EdgeCases tests boundary conditions
func TestValueToString_EdgeCases(t *testing.T) {
	tests := []struct {
		name     string
		input    interface{}
		validate func(string) bool
	}{
		{
			name:  "very_long_string",
			input: strings.Repeat("a", 10000),
			validate: func(s string) bool {
				return len(s) == 10000 && s[0] == 'a'
			},
		},
		{
			name:  "empty_bytes",
			input: []byte{},
			validate: func(s string) bool {
				return s == ""
			},
		},
		{
			name:  "single_byte",
			input: []byte{0x41},
			validate: func(s string) bool {
				return s == "A"
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ValueToString(tt.input)
			if !tt.validate(result) {
				t.Errorf("ValueToString(%v) validation failed, got %q", tt.input, result)
			}
		})
	}
}
