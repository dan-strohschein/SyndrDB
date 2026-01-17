package syndrQL

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"syndrdb/src/internal/domain/models"
)

// DateTime function implementations
// All functions registered in init() for automatic registration at startup
// TODO: I will add F:DATE_PART() as alias for F:EXTRACT() when implementing SQL compatibility layer
// TODO: I will add F:CURRENT_DATE, F:CURRENT_TIME, F:CURRENT_TIMESTAMP when implementing full SQL standard compliance

func init() {
	registry := GetRegistry()

	// Register F:NOW() - returns current timestamp in UTC
	registry.Register(&FunctionSignature{
		Name:    "NOW",
		MinArgs: 0,
		MaxArgs: 0,
		Implementation: func(args []models.FieldValue, evalCtx *EvaluationContext) (models.FieldValue, error) {
			// Use cached current time from evaluation context (query-scoped)
			if !evalCtx.CurrentTime.IsZero() {
				return models.NewDateTimeValue(evalCtx.CurrentTime), nil
			}
			// Fallback to time.Now() if context not initialized
			return models.NewDateTimeValue(time.Now().UTC()), nil
		},
		Description: "Returns the current timestamp in UTC",
	})

	// Register F:EXTRACT("unit", datetime) - extracts date part
	// Syntax: F:EXTRACT("YEAR", datetime_field) or F:EXTRACT("MONTH", "2024-11-22T15:30:45Z")
	registry.Register(&FunctionSignature{
		Name:    "EXTRACT",
		MinArgs: 2,
		MaxArgs: 2,
		Implementation: func(args []models.FieldValue, evalCtx *EvaluationContext) (models.FieldValue, error) {
			// Defensive bounds checking
			if len(args) < 2 {
				return models.FieldValue{}, fmt.Errorf("EXTRACT: requires 2 arguments, got %d", len(args))
			}
			
			// args[0]: unit (string: 'YEAR', 'MONTH', 'DAY', 'HOUR', 'MINUTE', 'SECOND')
			// args[1]: datetime value
			if args[0].Type != models.FieldTypeString {
				return models.FieldValue{}, fmt.Errorf("EXTRACT: first argument must be a string (unit)")
			}
			if args[1].Type != models.FieldTypeDateTime {
				return models.FieldValue{}, fmt.Errorf("EXTRACT: second argument must be a DATETIME")
			}

			unit := strings.ToUpper(args[0].StringVal)
			dt := args[1].DateTimeVal

			var result int64
			switch unit {
			case "YEAR":
				result = int64(dt.Year())
			case "MONTH":
				result = int64(dt.Month())
			case "DAY":
				result = int64(dt.Day())
			case "HOUR":
				result = int64(dt.Hour())
			case "MINUTE":
				result = int64(dt.Minute())
			case "SECOND":
				result = int64(dt.Second())
			default:
				return models.FieldValue{}, fmt.Errorf("EXTRACT: unsupported unit '%s'. Supported: YEAR, MONTH, DAY, HOUR, MINUTE, SECOND", unit)
			}

			return models.NewIntValue(result), nil
		},
		Description: "Extracts a date part (YEAR, MONTH, DAY, HOUR, MINUTE, SECOND) from a DATETIME",
	})

	// Register F:DATE_TRUNC("unit", datetime) - truncates to specified unit
	// Syntax: F:DATE_TRUNC("DAY", datetime_field) or F:DATE_TRUNC("HOUR", "2024-11-22T15:30:45Z")
	registry.Register(&FunctionSignature{
		Name:    "DATE_TRUNC",
		MinArgs: 2,
		MaxArgs: 2,
		Implementation: func(args []models.FieldValue, evalCtx *EvaluationContext) (models.FieldValue, error) {
			// Defensive bounds checking
			if len(args) < 2 {
				return models.FieldValue{}, fmt.Errorf("DATE_TRUNC: requires 2 arguments, got %d", len(args))
			}
			
			// args[0]: unit (string: 'YEAR', 'MONTH', 'DAY', 'HOUR', 'MINUTE', 'SECOND')
			// args[1]: datetime value
			if args[0].Type != models.FieldTypeString {
				return models.FieldValue{}, fmt.Errorf("DATE_TRUNC: first argument must be a string (unit)")
			}
			if args[1].Type != models.FieldTypeDateTime {
				return models.FieldValue{}, fmt.Errorf("DATE_TRUNC: second argument must be a DATETIME")
			}

			unit := strings.ToUpper(args[0].StringVal)
			dt := args[1].DateTimeVal

			var truncated time.Time
			switch unit {
			case "YEAR":
				truncated = time.Date(dt.Year(), 1, 1, 0, 0, 0, 0, dt.Location())
			case "MONTH":
				truncated = time.Date(dt.Year(), dt.Month(), 1, 0, 0, 0, 0, dt.Location())
			case "DAY":
				truncated = time.Date(dt.Year(), dt.Month(), dt.Day(), 0, 0, 0, 0, dt.Location())
			case "HOUR":
				truncated = time.Date(dt.Year(), dt.Month(), dt.Day(), dt.Hour(), 0, 0, 0, dt.Location())
			case "MINUTE":
				truncated = time.Date(dt.Year(), dt.Month(), dt.Day(), dt.Hour(), dt.Minute(), 0, 0, dt.Location())
			case "SECOND":
				truncated = time.Date(dt.Year(), dt.Month(), dt.Day(), dt.Hour(), dt.Minute(), dt.Second(), 0, dt.Location())
			default:
				return models.FieldValue{}, fmt.Errorf("DATE_TRUNC: unsupported unit '%s'. Supported: YEAR, MONTH, DAY, HOUR, MINUTE, SECOND", unit)
			}

			return models.NewDateTimeValue(truncated), nil
		},
		Description: "Truncates a DATETIME to the specified unit (YEAR, MONTH, DAY, HOUR, MINUTE, SECOND)",
	})

	// Register F:DATE_ADD(datetime, amount, "unit") - adds interval to datetime
	// Syntax: F:DATE_ADD(datetime_field, 7, "DAY") or F:DATE_ADD("2024-11-22T15:30:00Z", 1, "MONTH")
	registry.Register(&FunctionSignature{
		Name:    "DATE_ADD",
		MinArgs: 3,
		MaxArgs: 3,
		Implementation: func(args []models.FieldValue, evalCtx *EvaluationContext) (models.FieldValue, error) {
			// Defensive bounds checking
			if len(args) < 3 {
				return models.FieldValue{}, fmt.Errorf("DATE_ADD: requires 3 arguments, got %d", len(args))
			}
			
			// args[0]: datetime value
			// args[1]: interval amount (int or float)
			// args[2]: unit (string: 'YEAR', 'MONTH', 'DAY', 'HOUR', 'MINUTE', 'SECOND')
			if args[0].Type != models.FieldTypeDateTime {
				return models.FieldValue{}, fmt.Errorf("DATE_ADD: first argument must be a DATETIME")
			}
			if args[1].Type != models.FieldTypeInt && args[1].Type != models.FieldTypeFloat {
				return models.FieldValue{}, fmt.Errorf("DATE_ADD: second argument must be an INT or FLOAT (interval amount)")
			}
			if args[2].Type != models.FieldTypeString {
				return models.FieldValue{}, fmt.Errorf("DATE_ADD: third argument must be a string (unit)")
			}

			dt := args[0].DateTimeVal
			unit := strings.ToUpper(args[2].StringVal)

			var amount int64
			if args[1].Type == models.FieldTypeInt {
				amount = args[1].IntVal
			} else {
				amount = int64(args[1].FloatVal)
			}

			var result time.Time
			switch unit {
			case "YEAR":
				result = dt.AddDate(int(amount), 0, 0)
			case "MONTH":
				result = dt.AddDate(0, int(amount), 0)
			case "DAY":
				result = dt.AddDate(0, 0, int(amount))
			case "HOUR":
				result = dt.Add(time.Duration(amount) * time.Hour)
			case "MINUTE":
				result = dt.Add(time.Duration(amount) * time.Minute)
			case "SECOND":
				result = dt.Add(time.Duration(amount) * time.Second)
			default:
				return models.FieldValue{}, fmt.Errorf("DATE_ADD: unsupported unit '%s'. Supported: YEAR, MONTH, DAY, HOUR, MINUTE, SECOND", unit)
			}

			// Validate result is within time.Time range
			if result.Year() < 1 || result.Year() > 9999 {
				return models.FieldValue{}, fmt.Errorf("DATE_ADD: result year %d out of range [1, 9999]", result.Year())
			}

			return models.NewDateTimeValue(result), nil
		},
		Description: "Adds an interval to a DATETIME (unit: YEAR, MONTH, DAY, HOUR, MINUTE, SECOND)",
	})

	// Register F:DATE_SUB(datetime, amount, "unit") - subtracts interval from datetime
	// Syntax: F:DATE_SUB(datetime_field, 7, "DAY") or F:DATE_SUB("2024-11-22T15:30:00Z", 1, "MONTH")
	registry.Register(&FunctionSignature{
		Name:    "DATE_SUB",
		MinArgs: 3,
		MaxArgs: 3,
		Implementation: func(args []models.FieldValue, evalCtx *EvaluationContext) (models.FieldValue, error) {
			// Defensive bounds checking
			if len(args) < 3 {
				return models.FieldValue{}, fmt.Errorf("DATE_SUB: requires 3 arguments, got %d", len(args))
			}
			
			// args[0]: datetime value
			// args[1]: interval amount (int or float)
			// args[2]: unit (string: 'YEAR', 'MONTH', 'DAY', 'HOUR', 'MINUTE', 'SECOND')
			if args[0].Type != models.FieldTypeDateTime {
				return models.FieldValue{}, fmt.Errorf("DATE_SUB: first argument must be a DATETIME")
			}
			if args[1].Type != models.FieldTypeInt && args[1].Type != models.FieldTypeFloat {
				return models.FieldValue{}, fmt.Errorf("DATE_SUB: second argument must be an INT or FLOAT (interval amount)")
			}
			if args[2].Type != models.FieldTypeString {
				return models.FieldValue{}, fmt.Errorf("DATE_SUB: third argument must be a string (unit)")
			}

			dt := args[0].DateTimeVal
			unit := strings.ToUpper(args[2].StringVal)

			var amount int64
			if args[1].Type == models.FieldTypeInt {
				amount = args[1].IntVal
			} else {
				amount = int64(args[1].FloatVal)
			}

			// Negate amount for subtraction
			amount = -amount

			var result time.Time
			switch unit {
			case "YEAR":
				result = dt.AddDate(int(amount), 0, 0)
			case "MONTH":
				result = dt.AddDate(0, int(amount), 0)
			case "DAY":
				result = dt.AddDate(0, 0, int(amount))
			case "HOUR":
				result = dt.Add(time.Duration(amount) * time.Hour)
			case "MINUTE":
				result = dt.Add(time.Duration(amount) * time.Minute)
			case "SECOND":
				result = dt.Add(time.Duration(amount) * time.Second)
			default:
				return models.FieldValue{}, fmt.Errorf("DATE_SUB: unsupported unit '%s'. Supported: YEAR, MONTH, DAY, HOUR, MINUTE, SECOND", unit)
			}

			// Validate result is within time.Time range
			if result.Year() < 1 || result.Year() > 9999 {
				return models.FieldValue{}, fmt.Errorf("DATE_SUB: result year %d out of range [1, 9999]", result.Year())
			}

			return models.NewDateTimeValue(result), nil
		},
		Description: "Subtracts an interval from a DATETIME (unit: YEAR, MONTH, DAY, HOUR, MINUTE, SECOND)",
	})

	// Register F:AGE(datetime1, datetime2) - calculates interval between two datetimes
	// Syntax: F:AGE("2024-11-22T15:30:00Z", "2023-11-22T15:30:00Z") or F:AGE(created_at, updated_at)
	registry.Register(&FunctionSignature{
		Name:    "AGE",
		MinArgs: 1,
		MaxArgs: 2,
		Implementation: func(args []models.FieldValue, evalCtx *EvaluationContext) (models.FieldValue, error) {
			// Defensive bounds checking
			if len(args) < 1 {
				return models.FieldValue{}, fmt.Errorf("AGE: requires at least 1 argument, got %d", len(args))
			}
			
			var dt1, dt2 time.Time

			if len(args) == 1 {
				// Single argument: calculate age from now
				if args[0].Type != models.FieldTypeDateTime {
					return models.FieldValue{}, fmt.Errorf("AGE: argument must be a DATETIME")
				}
				dt1 = evalCtx.CurrentTime
				if dt1.IsZero() {
					dt1 = time.Now().UTC()
				}
				dt2 = args[0].DateTimeVal
			} else {
				// Two arguments: calculate age between two datetimes
				// Defensive check for second argument
				if len(args) < 2 {
					return models.FieldValue{}, fmt.Errorf("AGE: requires 2 arguments for two-datetime calculation, got %d", len(args))
				}
				if args[0].Type != models.FieldTypeDateTime {
					return models.FieldValue{}, fmt.Errorf("AGE: first argument must be a DATETIME")
				}
				if args[1].Type != models.FieldTypeDateTime {
					return models.FieldValue{}, fmt.Errorf("AGE: second argument must be a DATETIME")
				}
				dt1 = args[0].DateTimeVal
				dt2 = args[1].DateTimeVal
			}

			// Calculate difference in seconds
			duration := dt1.Sub(dt2)
			totalSeconds := int64(duration.Seconds())

			// Format as human-readable string: "X years Y months Z days HH:MM:SS"
			// TODO: I will return structured INTERVAL type when implementing interval arithmetic
			years := totalSeconds / (365 * 24 * 3600)
			remaining := totalSeconds % (365 * 24 * 3600)
			months := remaining / (30 * 24 * 3600)
			remaining = remaining % (30 * 24 * 3600)
			days := remaining / (24 * 3600)
			remaining = remaining % (24 * 3600)
			hours := remaining / 3600
			remaining = remaining % 3600
			minutes := remaining / 60
			seconds := remaining % 60

			var parts []string
			if years > 0 {
				parts = append(parts, fmt.Sprintf("%d years", years))
			}
			if months > 0 {
				parts = append(parts, fmt.Sprintf("%d months", months))
			}
			if days > 0 {
				parts = append(parts, fmt.Sprintf("%d days", days))
			}
			if hours > 0 || minutes > 0 || seconds > 0 || len(parts) == 0 {
				parts = append(parts, fmt.Sprintf("%02d:%02d:%02d", hours, minutes, seconds))
			}

			ageString := strings.Join(parts, " ")
			return models.NewStringValue(ageString), nil
		},
		Description: "Calculates the interval between two DATETIMEs (or from now to a DATETIME)",
	})
}

// ParseInterval parses an INTERVAL string value into duration components
// Supports both numeric format: '7' DAY and string format: '7 days', '1 month 2 days'
// TODO: I will add support for ISO 8601 duration format (P1Y2M3DT4H5M6S) when implementing standards compliance
// TODO: I will optimize parsing with precompiled regex when profiling shows >1% CPU time
func ParseInterval(value string, unit TokenType, isNumericFormat bool) (years int, months int, days int, hours int, minutes int, seconds int, err error) {
	if isNumericFormat {
		// Numeric format: INTERVAL '7' DAY
		amount, parseErr := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
		if parseErr != nil {
			err = fmt.Errorf("invalid interval value '%s': %w", value, parseErr)
			return
		}

		switch unit {
		case TOKEN_YEAR:
			years = int(amount)
		case TOKEN_MONTH:
			months = int(amount)
		case TOKEN_DAY:
			days = int(amount)
		case TOKEN_HOUR:
			hours = int(amount)
		case TOKEN_MINUTE:
			minutes = int(amount)
		case TOKEN_SECOND:
			seconds = int(amount)
		default:
			err = fmt.Errorf("unsupported interval unit: %s", unit.String())
			return
		}
	} else {
		// String format: INTERVAL '7 days', '1 month 2 days', '01:30:00'
		parts := strings.Fields(value)
		if len(parts) == 0 {
			err = fmt.Errorf("empty interval value")
			return
		}

		// Check for time format (HH:MM:SS)
		if strings.Contains(value, ":") {
			timeParts := strings.Split(value, ":")
			if len(timeParts) >= 3 {
				// Defensive bounds checking before accessing indices
				hours, _ = strconv.Atoi(timeParts[0])
				minutes, _ = strconv.Atoi(timeParts[1])
				seconds, _ = strconv.Atoi(timeParts[2])
				return
			} else if len(timeParts) > 0 {
				// Invalid time format
				err = fmt.Errorf("invalid time format: expected HH:MM:SS, got '%s'", value)
				return
			}
		}

		// Parse "X unit Y unit" format
		for i := 0; i < len(parts); i += 2 {
			// Defensive bounds checking
			if i >= len(parts) {
				err = fmt.Errorf("invalid interval format: unexpected end at index %d", i)
				return
			}
			if i+1 >= len(parts) {
				err = fmt.Errorf("invalid interval format: '%s' - missing unit after value at index %d", value, i)
				return
			}

			amount, parseErr := strconv.ParseInt(parts[i], 10, 64)
			if parseErr != nil {
				err = fmt.Errorf("invalid interval amount '%s': %w", parts[i], parseErr)
				return
			}

			unitStr := strings.ToUpper(strings.TrimSuffix(parts[i+1], "S")) // Remove plural 's'
			switch unitStr {
			case "YEAR":
				years += int(amount)
			case "MONTH":
				months += int(amount)
			case "DAY":
				days += int(amount)
			case "HOUR":
				hours += int(amount)
			case "MINUTE":
				minutes += int(amount)
			case "SECOND":
				seconds += int(amount)
			default:
				err = fmt.Errorf("unsupported interval unit: %s", parts[i+1])
				return
			}
		}
	}

	return
}
