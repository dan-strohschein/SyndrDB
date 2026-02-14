package bundle

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"syndrdb/src/internal/domain/models"
	"syndrdb/src/internal/utils"
	"syndrdb/src/pkg/common/conversion"
)

// Fast type converter functions - eliminate reflection overhead
func convertToString(value interface{}) (interface{}, error) {
	if value == nil {
		return nil, nil
	}
	// Fast path: already a string
	if strVal, ok := value.(string); ok {
		// Allow NULL magic values to pass through without conversion
		if strings.HasPrefix(strVal, "::SYNDR_") {
			return strVal, nil
		}
		return strVal, nil
	}
	// Convert other types to string without reflection
	return conversion.ValueToString(value), nil
}

func convertToInt(value interface{}) (interface{}, error) {
	if value == nil {
		return nil, nil
	}
	// Fast path: direct type assertions (no reflection)
	switch v := value.(type) {
	case int:
		return int64(v), nil
	case int8:
		return int64(v), nil
	case int16:
		return int64(v), nil
	case int32:
		return int64(v), nil
	case int64:
		return v, nil
	case float64:
		// Check if float64 represents a whole number
		if v != float64(int64(v)) {
			return nil, fmt.Errorf("expected integer but got float with decimal places: %v", v)
		}
		return int64(v), nil
	case float32:
		// Check if float32 represents a whole number
		if v != float32(int32(v)) {
			return nil, fmt.Errorf("expected integer but got float with decimal places: %v", v)
		}
		return int64(v), nil
	case string:
		// Allow NULL magic values to pass through - NULLs are valid for any type
		if strings.HasPrefix(v, "::SYNDR_") {
			return v, nil
		}
		// Parse string as integer - only expensive operation left
		intVal, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("cannot convert string '%s' to integer: %v", v, err)
		}
		return intVal, nil
	default:
		return nil, fmt.Errorf("expected integer but got %T", value)
	}
}

func convertToFloat(value interface{}) (interface{}, error) {
	if value == nil {
		return nil, nil
	}
	// Fast path: direct type assertions (no reflection)
	switch v := value.(type) {
	case float32:
		return float64(v), nil
	case float64:
		return v, nil
	case int:
		return float64(v), nil
	case int8:
		return float64(v), nil
	case int16:
		return float64(v), nil
	case int32:
		return float64(v), nil
	case int64:
		return float64(v), nil
	case string:
		// Allow NULL magic values to pass through - NULLs are valid for any type
		if strings.HasPrefix(v, "::SYNDR_") {
			return v, nil
		}
		// Parse string as float - only expensive operation left
		floatVal, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return nil, fmt.Errorf("cannot convert string '%s' to float: %v", v, err)
		}
		return floatVal, nil
	default:
		return nil, fmt.Errorf("expected number but got %T", value)
	}
}

func convertToBool(value interface{}) (interface{}, error) {
	if value == nil {
		return nil, nil
	}
	// Fast path: direct type assertions (no reflection)
	switch v := value.(type) {
	case bool:
		return v, nil
	case string:
		// Allow NULL magic values to pass through - NULLs are valid for any type
		if strings.HasPrefix(v, "::SYNDR_") {
			return v, nil
		}
		// Parse string as boolean
		if strings.EqualFold(v, "true") {
			return true, nil
		}
		if strings.EqualFold(v, "false") {
			return false, nil
		}
		return nil, fmt.Errorf("cannot convert string '%s' to boolean (expected 'true' or 'false')", v)
	default:
		return nil, fmt.Errorf("expected boolean but got %T", value)
	}
}

func convertToDateTime(value interface{}) (interface{}, error) {
	if value == nil {
		return nil, nil
	}
	// Fast path: already a time.Time
	if timeVal, ok := value.(time.Time); ok {
		// Return FieldValue directly so type info preserved (DateTime)
		return models.NewDateTimeValue(timeVal.UTC()), nil
	}
	// Handle string values
	if strVal, ok := value.(string); ok {
		// Allow NULL magic values to pass through
		if strings.HasPrefix(strVal, "::SYNDR_") {
			return strVal, nil
		}
		// Parse datetime string - this was already done in parseValue, but handle legacy cases
		if parsedTime, _, err := utils.ParseDateTime(strVal); err == nil {
			// Return FieldValue directly so type info preserved (DateTime)
			return models.NewDateTimeValue(parsedTime.UTC()), nil
		} else {
			return nil, fmt.Errorf("cannot convert string '%s' to datetime: %v", strVal, err)
		}
	}
	return nil, fmt.Errorf("expected datetime but got %T", value)
}

func convertToDate(value interface{}) (interface{}, error) {
	if value == nil {
		return nil, nil
	}
	// Fast path: already a time.Time
	if timeVal, ok := value.(time.Time); ok {
		// Date: zero out time component to midnight UTC
		utc := timeVal.UTC()
		dateTime := time.Date(utc.Year(), utc.Month(), utc.Day(), 0, 0, 0, 0, time.UTC)
		// Return FieldValue directly so type info preserved (Date)
		return models.NewDateValue(dateTime), nil
	}
	// Handle string values
	if strVal, ok := value.(string); ok {
		// Allow NULL magic values to pass through
		if strings.HasPrefix(strVal, "::SYNDR_") {
			return strVal, nil
		}
		// Parse date string
		if parsedTime, _, err := utils.ParseDateTime(strVal); err == nil {
			// Zero out time to midnight UTC for Date type
			utc := parsedTime.UTC()
			dateTime := time.Date(utc.Year(), utc.Month(), utc.Day(), 0, 0, 0, 0, time.UTC)
			// Return FieldValue directly so type info preserved (Date)
			return models.NewDateValue(dateTime), nil
		} else {
			return nil, fmt.Errorf("cannot convert string '%s' to date: %v", strVal, err)
		}
	}
	return nil, fmt.Errorf("expected date but got %T", value)
}
