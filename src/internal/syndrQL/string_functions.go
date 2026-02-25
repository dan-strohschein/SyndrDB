package syndrQL

import (
	"fmt"
	"strings"

	"syndrdb/src/internal/domain/models"

	syndrdbsimd "github.com/dan-strohschein/syndrdb-simd"
)

// String functions: LOWER, UPPER, TRIM, LENGTH, SUBSTRING, CONCAT
// Registered via init() following the same pattern as datetime_functions.go

func init() {
	registry := GetRegistry()

	// LOWER(string) → string
	// Uses SIMD-accelerated ASCII case conversion with fallback to strings.ToLower for non-ASCII
	registry.Register(&FunctionSignature{
		Name:        "LOWER",
		MinArgs:     1,
		MaxArgs:     1,
		ReturnType:  FieldTypeString,
		Description: "Converts a string to lowercase",
		Implementation: func(args []models.FieldValue, evalCtx *EvaluationContext) (models.FieldValue, error) {
			str, err := fieldValueToString(args[0])
			if err != nil {
				return models.FieldValue{}, fmt.Errorf("LOWER: %w", err)
			}
			return models.NewStringValue(simdToLower(str)), nil
		},
	})

	// UPPER(string) → string
	// Uses SIMD-accelerated ASCII case conversion with fallback to strings.ToUpper for non-ASCII
	registry.Register(&FunctionSignature{
		Name:        "UPPER",
		MinArgs:     1,
		MaxArgs:     1,
		ReturnType:  FieldTypeString,
		Description: "Converts a string to uppercase",
		Implementation: func(args []models.FieldValue, evalCtx *EvaluationContext) (models.FieldValue, error) {
			str, err := fieldValueToString(args[0])
			if err != nil {
				return models.FieldValue{}, fmt.Errorf("UPPER: %w", err)
			}
			return models.NewStringValue(simdToUpper(str)), nil
		},
	})

	// TRIM(string) → string
	registry.Register(&FunctionSignature{
		Name:        "TRIM",
		MinArgs:     1,
		MaxArgs:     1,
		ReturnType:  FieldTypeString,
		Description: "Removes leading and trailing whitespace from a string",
		Implementation: func(args []models.FieldValue, evalCtx *EvaluationContext) (models.FieldValue, error) {
			str, err := fieldValueToString(args[0])
			if err != nil {
				return models.FieldValue{}, fmt.Errorf("TRIM: %w", err)
			}
			return models.NewStringValue(strings.TrimSpace(str)), nil
		},
	})

	// LENGTH(string) → int
	registry.Register(&FunctionSignature{
		Name:        "LENGTH",
		MinArgs:     1,
		MaxArgs:     1,
		ReturnType:  FieldTypeInt,
		Description: "Returns the length of a string",
		Implementation: func(args []models.FieldValue, evalCtx *EvaluationContext) (models.FieldValue, error) {
			str, err := fieldValueToString(args[0])
			if err != nil {
				return models.FieldValue{}, fmt.Errorf("LENGTH: %w", err)
			}
			return models.NewIntValue(int64(len(str))), nil
		},
	})

	// SUBSTRING(string, start [, length]) → string
	// Uses 1-based indexing (SQL standard)
	registry.Register(&FunctionSignature{
		Name:        "SUBSTRING",
		MinArgs:     2,
		MaxArgs:     3,
		ReturnType:  FieldTypeString,
		Description: "Extracts a substring starting at the given position (1-based)",
		Implementation: func(args []models.FieldValue, evalCtx *EvaluationContext) (models.FieldValue, error) {
			str, err := fieldValueToString(args[0])
			if err != nil {
				return models.FieldValue{}, fmt.Errorf("SUBSTRING: %w", err)
			}
			if args[1].Type != models.FieldTypeInt {
				return models.FieldValue{}, fmt.Errorf("SUBSTRING: start position must be an integer")
			}
			startIdx := int(args[1].IntVal) - 1 // convert to 0-based
			if startIdx < 0 {
				startIdx = 0
			}
			if startIdx >= len(str) {
				return models.NewStringValue(""), nil
			}
			if len(args) == 3 {
				if args[2].Type != models.FieldTypeInt {
					return models.FieldValue{}, fmt.Errorf("SUBSTRING: length must be an integer")
				}
				endIdx := startIdx + int(args[2].IntVal)
				if endIdx > len(str) {
					endIdx = len(str)
				}
				if endIdx <= startIdx {
					return models.NewStringValue(""), nil
				}
				return models.NewStringValue(str[startIdx:endIdx]), nil
			}
			return models.NewStringValue(str[startIdx:]), nil
		},
	})

	// CONCAT(string, string, ...) → string (variadic, 2-10 args)
	// NULL arguments are skipped (PostgreSQL behavior)
	registry.Register(&FunctionSignature{
		Name:        "CONCAT",
		MinArgs:     2,
		MaxArgs:     10,
		ReturnType:  FieldTypeString,
		Description: "Concatenates two or more strings, skipping NULLs",
		Implementation: func(args []models.FieldValue, evalCtx *EvaluationContext) (models.FieldValue, error) {
			var sb strings.Builder
			for _, arg := range args {
				if arg.Type == models.FieldTypeNil {
					continue
				}
				str, err := fieldValueToString(arg)
				if err != nil {
					continue
				}
				sb.WriteString(str)
			}
			return models.NewStringValue(sb.String()), nil
		},
	})
}

// isASCII returns true if all bytes in the string are in the ASCII range (< 128).
func isASCII(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] >= 128 {
			return false
		}
	}
	return true
}

// simdToLower converts a string to lowercase using SIMD for ASCII strings,
// falling back to strings.ToLower for non-ASCII content.
func simdToLower(s string) string {
	if len(s) == 0 {
		return s
	}
	if isASCII(s) {
		b := []byte(s)
		syndrdbsimd.StrToLower(b)
		return string(b)
	}
	return strings.ToLower(s)
}

// simdToUpper converts a string to uppercase using SIMD for ASCII strings,
// falling back to strings.ToUpper for non-ASCII content.
func simdToUpper(s string) string {
	if len(s) == 0 {
		return s
	}
	if isASCII(s) {
		b := []byte(s)
		syndrdbsimd.StrToUpper(b)
		return string(b)
	}
	return strings.ToUpper(s)
}

// fieldValueToString extracts a string from a FieldValue using its typed accessors
func fieldValueToString(fv models.FieldValue) (string, error) {
	if s, ok := fv.AsString(); ok {
		return s, nil
	}
	// Fallback: convert any value to string representation
	v := fv.AsInterface()
	if v == nil {
		return "", nil
	}
	return fmt.Sprintf("%v", v), nil
}
