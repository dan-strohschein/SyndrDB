package index

import (
	"fmt"
	"regexp"
	"strings"
)

// CompileIndexExpression compiles an expression string (e.g., "LOWER(name)") into:
// - fieldName: the underlying field referenced by the expression
// - fn: a function that transforms a raw field value into the indexed value
// - err: any parsing or compilation error
//
// Supported expressions: LOWER(field), UPPER(field), TRIM(field), LENGTH(field),
// YEAR(field), MONTH(field).
func CompileIndexExpression(expr string) (fieldName string, fn func(interface{}) interface{}, err error) {
	expr = strings.TrimSpace(expr)

	// Match FUNCTION("field") or FUNCTION(field) pattern
	re := regexp.MustCompile(`(?i)^(LOWER|UPPER|TRIM|LENGTH|YEAR|MONTH)\s*\(\s*"?([^")\s]+)"?\s*\)$`)
	match := re.FindStringSubmatch(expr)
	if match == nil {
		return "", nil, fmt.Errorf("unsupported index expression: %s", expr)
	}

	funcName := strings.ToUpper(match[1])
	fieldName = match[2]

	switch funcName {
	case "LOWER":
		fn = func(v interface{}) interface{} {
			if s, ok := toString(v); ok {
				return strings.ToLower(s)
			}
			return v
		}
	case "UPPER":
		fn = func(v interface{}) interface{} {
			if s, ok := toString(v); ok {
				return strings.ToUpper(s)
			}
			return v
		}
	case "TRIM":
		fn = func(v interface{}) interface{} {
			if s, ok := toString(v); ok {
				return strings.TrimSpace(s)
			}
			return v
		}
	case "LENGTH":
		fn = func(v interface{}) interface{} {
			if s, ok := toString(v); ok {
				return int64(len(s))
			}
			return v
		}
	case "YEAR":
		fn = func(v interface{}) interface{} {
			// Extract year from date string or time value
			if s, ok := toString(v); ok {
				if len(s) >= 4 {
					return s[:4] // Extract YYYY from date string
				}
			}
			return v
		}
	case "MONTH":
		fn = func(v interface{}) interface{} {
			// Extract month from date string
			if s, ok := toString(v); ok {
				if len(s) >= 7 {
					return s[5:7] // Extract MM from YYYY-MM-... string
				}
			}
			return v
		}
	default:
		return "", nil, fmt.Errorf("unsupported expression function: %s", funcName)
	}

	return fieldName, fn, nil
}

// toString converts various types to string
func toString(v interface{}) (string, bool) {
	if v == nil {
		return "", false
	}
	switch s := v.(type) {
	case string:
		return s, true
	case fmt.Stringer:
		return s.String(), true
	default:
		return fmt.Sprintf("%v", v), true
	}
}
