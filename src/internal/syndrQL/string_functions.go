package syndrQL

import (
	"fmt"
	"strings"

	"syndrdb/src/internal/domain/models"
)

// String functions: LOWER, UPPER, TRIM, LENGTH
// Registered via init() following the same pattern as datetime_functions.go

func init() {
	registry := GetRegistry()

	// LOWER(string) → string
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
			return models.NewStringValue(strings.ToLower(str)), nil
		},
	})

	// UPPER(string) → string
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
			return models.NewStringValue(strings.ToUpper(str)), nil
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
