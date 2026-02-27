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

	// POWER(base, exponent) → float64
	registry.Register(&FunctionSignature{
		Name:        "POWER",
		MinArgs:     2,
		MaxArgs:     2,
		ReturnType:  FieldTypeFloat,
		Description: "Returns base raised to the power of exponent",
		Implementation: func(args []models.FieldValue, evalCtx *EvaluationContext) (models.FieldValue, error) {
			if args[0].Type == models.FieldTypeNil || args[1].Type == models.FieldTypeNil {
				return models.FieldValue{Type: models.FieldTypeNil}, nil
			}
			base, ok := fieldValueToFloat64(args[0])
			if !ok {
				return models.FieldValue{}, fmt.Errorf("POWER: base must be numeric, got %v", args[0].Type)
			}
			exp, ok := fieldValueToFloat64(args[1])
			if !ok {
				return models.FieldValue{}, fmt.Errorf("POWER: exponent must be numeric, got %v", args[1].Type)
			}
			return models.NewFloatValue(math.Pow(base, exp)), nil
		},
	})

	// SQRT(number) → float64
	registry.Register(&FunctionSignature{
		Name:        "SQRT",
		MinArgs:     1,
		MaxArgs:     1,
		ReturnType:  FieldTypeFloat,
		Description: "Returns the square root of a number",
		Implementation: func(args []models.FieldValue, evalCtx *EvaluationContext) (models.FieldValue, error) {
			if args[0].Type == models.FieldTypeNil {
				return models.FieldValue{Type: models.FieldTypeNil}, nil
			}
			v, ok := fieldValueToFloat64(args[0])
			if !ok {
				return models.FieldValue{}, fmt.Errorf("SQRT: argument must be numeric, got %v", args[0].Type)
			}
			if v < 0 {
				return models.FieldValue{}, fmt.Errorf("SQRT: cannot take square root of negative number")
			}
			return models.NewFloatValue(math.Sqrt(v)), nil
		},
	})

	// MOD(dividend, divisor) → int if both int, else float
	registry.Register(&FunctionSignature{
		Name:        "MOD",
		MinArgs:     2,
		MaxArgs:     2,
		ReturnType:  FieldTypeFloat,
		Description: "Returns the remainder of dividend divided by divisor",
		Implementation: func(args []models.FieldValue, evalCtx *EvaluationContext) (models.FieldValue, error) {
			if args[0].Type == models.FieldTypeNil || args[1].Type == models.FieldTypeNil {
				return models.FieldValue{Type: models.FieldTypeNil}, nil
			}
			// Integer path: both args are int
			if args[0].Type == models.FieldTypeInt && args[1].Type == models.FieldTypeInt {
				if args[1].IntVal == 0 {
					return models.FieldValue{}, fmt.Errorf("MOD: division by zero")
				}
				return models.NewIntValue(args[0].IntVal % args[1].IntVal), nil
			}
			// Float path
			dividend, ok := fieldValueToFloat64(args[0])
			if !ok {
				return models.FieldValue{}, fmt.Errorf("MOD: dividend must be numeric, got %v", args[0].Type)
			}
			divisor, ok := fieldValueToFloat64(args[1])
			if !ok {
				return models.FieldValue{}, fmt.Errorf("MOD: divisor must be numeric, got %v", args[1].Type)
			}
			if divisor == 0 {
				return models.FieldValue{}, fmt.Errorf("MOD: division by zero")
			}
			return models.NewFloatValue(math.Mod(dividend, divisor)), nil
		},
	})

	// LOG(number) → natural log, or LOG(base, number) → log base
	registry.Register(&FunctionSignature{
		Name:        "LOG",
		MinArgs:     1,
		MaxArgs:     2,
		ReturnType:  FieldTypeFloat,
		Description: "Returns the natural logarithm (1 arg) or logarithm with specified base (2 args)",
		Implementation: func(args []models.FieldValue, evalCtx *EvaluationContext) (models.FieldValue, error) {
			if args[0].Type == models.FieldTypeNil {
				return models.FieldValue{Type: models.FieldTypeNil}, nil
			}
			if len(args) == 1 {
				// Natural log
				v, ok := fieldValueToFloat64(args[0])
				if !ok {
					return models.FieldValue{}, fmt.Errorf("LOG: argument must be numeric, got %v", args[0].Type)
				}
				if v <= 0 {
					return models.FieldValue{}, fmt.Errorf("LOG: argument must be positive")
				}
				return models.NewFloatValue(math.Log(v)), nil
			}
			// LOG(base, number)
			if args[1].Type == models.FieldTypeNil {
				return models.FieldValue{Type: models.FieldTypeNil}, nil
			}
			base, ok := fieldValueToFloat64(args[0])
			if !ok {
				return models.FieldValue{}, fmt.Errorf("LOG: base must be numeric, got %v", args[0].Type)
			}
			number, ok := fieldValueToFloat64(args[1])
			if !ok {
				return models.FieldValue{}, fmt.Errorf("LOG: number must be numeric, got %v", args[1].Type)
			}
			if base <= 0 || base == 1 {
				return models.FieldValue{}, fmt.Errorf("LOG: base must be positive and not equal to 1")
			}
			if number <= 0 {
				return models.FieldValue{}, fmt.Errorf("LOG: number must be positive")
			}
			return models.NewFloatValue(math.Log(number) / math.Log(base)), nil
		},
	})

	// SIGN(number) → -1, 0, or 1
	registry.Register(&FunctionSignature{
		Name:        "SIGN",
		MinArgs:     1,
		MaxArgs:     1,
		ReturnType:  FieldTypeInt,
		Description: "Returns -1, 0, or 1 depending on the sign of the argument",
		Implementation: func(args []models.FieldValue, evalCtx *EvaluationContext) (models.FieldValue, error) {
			if args[0].Type == models.FieldTypeNil {
				return models.FieldValue{Type: models.FieldTypeNil}, nil
			}
			switch args[0].Type {
			case models.FieldTypeInt:
				v := args[0].IntVal
				if v > 0 {
					return models.NewIntValue(1), nil
				} else if v < 0 {
					return models.NewIntValue(-1), nil
				}
				return models.NewIntValue(0), nil
			case models.FieldTypeFloat:
				v := args[0].FloatVal
				if v > 0 {
					return models.NewIntValue(1), nil
				} else if v < 0 {
					return models.NewIntValue(-1), nil
				}
				return models.NewIntValue(0), nil
			default:
				return models.FieldValue{}, fmt.Errorf("SIGN: argument must be numeric, got %v", args[0].Type)
			}
		},
	})
}

// fieldValueToFloat64 extracts a float64 from a FieldValue.
func fieldValueToFloat64(fv models.FieldValue) (float64, bool) {
	switch fv.Type {
	case models.FieldTypeFloat:
		return fv.FloatVal, true
	case models.FieldTypeInt:
		return float64(fv.IntVal), true
	default:
		return 0, false
	}
}
