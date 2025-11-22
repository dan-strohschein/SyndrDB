package utils

import (
	"fmt"
	"strings"
	"time"
)

// Common datetime format patterns to try when parsing datetime strings
// Ordered by likelihood for performance optimization
var datetimeFormats = []string{
	time.RFC3339,               // "2006-01-02T15:04:05Z07:00" - ISO 8601 with timezone
	time.RFC3339Nano,           // "2006-01-02T15:04:05.999999999Z07:00" - ISO 8601 with nanoseconds
	"2006-01-02T15:04:05",      // ISO 8601 without timezone
	"2006-01-02 15:04:05",      // SQL-style datetime
	"2006-01-02T15:04:05.999Z", // ISO 8601 with milliseconds
	"2006-01-02T15:04:05.999",  // ISO 8601 with milliseconds, no timezone
	"2006-01-02 15:04:05.999",  // SQL-style with milliseconds
	"2006-01-02",               // Date only (for Date type or DateTime at midnight)
	"01/02/2006 15:04:05",      // US format with time
	"01/02/2006",               // US date format
	"2006/01/02 15:04:05",      // Alternative format
	"2006/01/02",               // Alternative date format
	time.RFC1123,               // "Mon, 02 Jan 2006 15:04:05 MST"
	time.RFC1123Z,              // "Mon, 02 Jan 2006 15:04:05 -0700"
	time.RFC822,                // "02 Jan 06 15:04 MST"
	time.RFC822Z,               // "02 Jan 06 15:04 -0700"
}

// ParseDateTime attempts to parse a datetime string into a time.Time value
// Returns the parsed time in UTC, a boolean indicating if it's date-only, and any error
func ParseDateTime(dateStr string) (time.Time, bool, error) {
	// Trim quotes and whitespace
	dateStr = strings.Trim(dateStr, "\"' \t\n\r")

	if dateStr == "" {
		return time.Time{}, false, fmt.Errorf("empty datetime string")
	}

	// Try each format until one succeeds
	var lastErr error
	for _, format := range datetimeFormats {
		t, err := time.Parse(format, dateStr)
		if err == nil {
			// Successfully parsed
			isDateOnly := format == "2006-01-02" ||
				format == "01/02/2006" ||
				format == "2006/01/02"

			// Convert to UTC for consistent storage
			return t.UTC(), isDateOnly, nil
		}
		lastErr = err
	}

	// All formats failed
	return time.Time{}, false, fmt.Errorf("unable to parse datetime '%s': %w", dateStr, lastErr)
}

// IsLikelyDateTime performs a heuristic check to see if a string looks like a datetime
// This helps parsers decide whether to attempt datetime parsing on a quoted string
func IsLikelyDateTime(s string) bool {
	// Remove quotes
	s = strings.Trim(s, "\"' ")

	// Quick heuristic: contains a dash or slash, and has numeric characters
	// More sophisticated than regex for performance
	hasSeparator := strings.Contains(s, "-") || strings.Contains(s, "/") || strings.Contains(s, "T")
	hasDigits := false

	for _, ch := range s {
		if ch >= '0' && ch <= '9' {
			hasDigits = true
			break
		}
	}

	// TODO: I could add more sophisticated heuristics here, such as checking for common
	// datetime patterns or validating month/day ranges, but this is sufficient for now
	return hasSeparator && hasDigits && len(s) >= 8 // Minimum date: "01/02/06" or "2006-01"
}
