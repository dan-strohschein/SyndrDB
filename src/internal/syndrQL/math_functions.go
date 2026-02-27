package syndrQL

import (
	"fmt"
	"math"

	"syndrdb/src/internal/domain/models"
)

// Math functions: ABS, CEIL, FLOOR, ROUND, COALESCE
// Registered via init() following the same pattern as datetime_functions.go and string_functions.go

func init() {
	registry := GetRegistry()

	// ABS(number) → number
	registry.Register(&FunctionSignature{
		Name:        "ABS",
		MinArgs:     1,
		MaxArgs:     1,
		ReturnType:  FieldTypeFloat,
		Description: "Returns the absolute value of a number",
		Implementation: func(args []models.FieldValue, evalCtx *EvaluationContext) (models.FieldValue, error) {
			switch args[0].Type {
			case models.FieldTypeInt:
				v := args[0].IntVal
				if v < 0 {
					v = -v
				}
				return models.NewIntValue(v), nil
			case models.FieldTypeFloat:
				return models.NewFloatValue(math.Abs(args[0].FloatVal)), nil
			default:
				return models.FieldValue{}, fmt.Errorf("ABS: argument must be numeric, got %v", args[0].Type)
			}
		},
	})

	// CEIL(number) → int
	registry.Register(&FunctionSignature{
		Name:        "CEIL",
		MinArgs:     1,
		MaxArgs:     1,
		ReturnType:  FieldTypeInt,
		Description: "Returns the smallest integer greater than or equal to the argument",
		Implementation: func(args []models.FieldValue, evalCtx *EvaluationContext) (models.FieldValue, error) {
			switch args[0].Type {
			case models.FieldTypeInt:
				return args[0], nil // already integer
			case models.FieldTypeFloat:
				return models.NewIntValue(int64(math.Ceil(args[0].FloatVal))), nil
			default:
				return models.FieldValue{}, fmt.Errorf("CEIL: argument must be numeric, got %v", args[0].Type)
			}
		},
	})

	// FLOOR(number) → int
	registry.Register(&FunctionSignature{
		Name:        "FLOOR",
		MinArgs:     1,
		MaxArgs:     1,
		ReturnType:  FieldTypeInt,
		Description: "Returns the largest integer less than or equal to the argument",
		Implementation: func(args []models.FieldValue, evalCtx *EvaluationContext) (models.FieldValue, error) {
			switch args[0].Type {
			case models.FieldTypeInt:
				return args[0], nil
			case models.FieldTypeFloat:
				return models.NewIntValue(int64(math.Floor(args[0].FloatVal))), nil
			default:
				return models.FieldValue{}, fmt.Errorf("FLOOR: argument must be numeric, got %v", args[0].Type)
			}
		},
	})

	// ROUND(number [, decimals]) → number
	registry.Register(&FunctionSignature{
		Name:        "ROUND",
		MinArgs:     1,
		MaxArgs:     2,
		ReturnType:  FieldTypeFloat,
		Description: "Rounds a number to the specified number of decimal places (default 0)",
		Implementation: func(args []models.FieldValue, evalCtx *EvaluationContext) (models.FieldValue, error) {
			decimals := int64(0)
			if len(args) == 2 {
				if args[1].Type != models.FieldTypeInt {
					return models.FieldValue{}, fmt.Errorf("ROUND: decimals argument must be an integer")
				}
				decimals = args[1].IntVal
			}

			var val float64
			switch args[0].Type {
			case models.FieldTypeInt:
				if decimals == 0 {
					return args[0], nil
				}
				val = float64(args[0].IntVal)
			case models.FieldTypeFloat:
				val = args[0].FloatVal
			default:
				return models.FieldValue{}, fmt.Errorf("ROUND: first argument must be numeric, got %v", args[0].Type)
			}

			if decimals == 0 {
				return models.NewIntValue(int64(math.Round(val))), nil
			}
			pow := math.Pow(10, float64(decimals))
			return models.NewFloatValue(math.Round(val*pow) / pow), nil
		},
	})

	// COALESCE(val1, val2, ...) → first non-null value (variadic, 2-10 args)
	registry.Register(&FunctionSignature{
		Name:        "COALESCE",
		MinArgs:     2,
		MaxArgs:     10,
		ReturnType:  FieldTypeUnknown, // depends on input types
		Description: "Returns the first non-null argument",
		Implementation: func(args []models.FieldValue, evalCtx *EvaluationContext) (models.FieldValue, error) {
			for _, arg := range args {
				if arg.Type != models.FieldTypeNil {
					return arg, nil
				}
			}
			return models.FieldValue{Type: models.FieldTypeNil}, nil
		},
	})
}
