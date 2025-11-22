package main

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"syndrdb/src/internal/domain/models"
	"syndrdb/src/internal/utils"
)

// TestDateTime_ParsingUtility tests the datetime parser utility
func TestDateTime_ParsingUtility(t *testing.T) {
	tests := []struct {
		name       string
		input      string
		expectErr  bool
		isDateOnly bool
	}{
		{"RFC3339", "2024-11-22T15:30:45Z", false, false},
		{"RFC3339 with timezone", "2024-11-22T15:30:45-05:00", false, false},
		{"ISO8601 no timezone", "2024-11-22T15:30:45", false, false},
		{"Date only", "2024-11-22", false, true},
		{"US format", "11/22/2024", false, true},
		{"SQL format", "2024-11-22 15:30:45", false, false},
		{"With milliseconds", "2024-11-22T15:30:45.999Z", false, false},
		{"Empty string", "", true, false},
		{"Invalid format", "not-a-date", true, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parsedTime, isDateOnly, err := utils.ParseDateTime(tt.input)

			if tt.expectErr {
				if err == nil {
					t.Errorf("Expected error for input %q, got none", tt.input)
				}
				return
			}

			if err != nil {
				t.Errorf("Unexpected error for input %q: %v", tt.input, err)
				return
			}

			// Verify UTC conversion
			if parsedTime.Location() != time.UTC {
				t.Errorf("Parsed time should be in UTC, got %v", parsedTime.Location())
			}

			// Verify date-only detection
			if isDateOnly != tt.isDateOnly {
				t.Errorf("Expected isDateOnly=%v for input %q, got %v", tt.isDateOnly, tt.input, isDateOnly)
			}
		})
	}
}

// TestDateTime_TypeConversion tests field type conversion for datetime and date
func TestDateTime_TypeConversion(t *testing.T) {
	t.Run("DateTime from string", func(t *testing.T) {
		// This would be tested through the bundle service convertToDateTime
		// For now, verify the FieldValue constructor works
		testTime := time.Date(2024, 11, 22, 15, 30, 45, 0, time.FixedZone("EST", -5*3600))
		fv := models.NewDateTimeValue(testTime)

		// Verify UTC conversion happened
		retrieved, ok := fv.AsDateTime()
		if !ok {
			t.Fatal("Failed to retrieve DateTime")
		}

		if retrieved.Location() != time.UTC {
			t.Errorf("DateTime should be converted to UTC, got %v", retrieved.Location())
		}

		// Verify the time is correct after UTC conversion
		expectedUTC := testTime.UTC()
		if !retrieved.Equal(expectedUTC) {
			t.Errorf("Time mismatch after UTC conversion: expected %v, got %v", expectedUTC, retrieved)
		}
	})

	t.Run("Date from string with time", func(t *testing.T) {
		// Test that Date zeros out time component
		testTime := time.Date(2024, 11, 22, 15, 30, 45, 999, time.Local)
		fv := models.NewDateValue(testTime)

		retrieved, ok := fv.AsDate()
		if !ok {
			t.Fatal("Failed to retrieve Date")
		}

		// Should be midnight UTC
		expected := time.Date(2024, 11, 22, 0, 0, 0, 0, time.UTC)
		if !retrieved.Equal(expected) {
			t.Errorf("Date should zero time to midnight UTC: expected %v, got %v", expected, retrieved)
		}
	})
}

// TestDateTime_StringRepresentation tests String() method for debugging
func TestDateTime_StringRepresentation(t *testing.T) {
	t.Run("DateTime String()", func(t *testing.T) {
		testTime := time.Date(2024, 11, 22, 15, 30, 45, 0, time.UTC)
		fv := models.NewDateTimeValue(testTime)

		str := fv.String()
		// Should be RFC3339 format
		if !strings.Contains(str, "2024-11-22") || !strings.Contains(str, "15:30:45") {
			t.Errorf("DateTime String() should contain date and time: got %s", str)
		}
	})

	t.Run("Date String()", func(t *testing.T) {
		testTime := time.Date(2024, 11, 22, 0, 0, 0, 0, time.UTC)
		fv := models.NewDateValue(testTime)

		str := fv.String()
		expected := "2024-11-22"
		if str != expected {
			t.Errorf("Date String() mismatch: expected %s, got %s", expected, str)
		}
	})
}

// TestDateTime_TimezoneConversion tests various timezone inputs
func TestDateTime_TimezoneConversion(t *testing.T) {
	testCases := []struct {
		name     string
		input    time.Time
		expected time.Time
	}{
		{
			name:     "EST to UTC",
			input:    time.Date(2024, 11, 22, 10, 0, 0, 0, time.FixedZone("EST", -5*3600)),
			expected: time.Date(2024, 11, 22, 15, 0, 0, 0, time.UTC),
		},
		{
			name:     "PST to UTC",
			input:    time.Date(2024, 11, 22, 10, 0, 0, 0, time.FixedZone("PST", -8*3600)),
			expected: time.Date(2024, 11, 22, 18, 0, 0, 0, time.UTC),
		},
		{
			name:     "JST to UTC",
			input:    time.Date(2024, 11, 22, 10, 0, 0, 0, time.FixedZone("JST", 9*3600)),
			expected: time.Date(2024, 11, 22, 1, 0, 0, 0, time.UTC),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			fv := models.NewDateTimeValue(tc.input)
			retrieved, ok := fv.AsDateTime()
			if !ok {
				t.Fatal("Failed to retrieve DateTime")
			}

			if !retrieved.Equal(tc.expected) {
				t.Errorf("Timezone conversion failed: expected %v, got %v", tc.expected, retrieved)
			}
		})
	}
}

// TestDateTime_MillisecondPrecision tests millisecond precision in comparisons
func TestDateTime_MillisecondPrecision(t *testing.T) {
	base := time.Date(2024, 11, 22, 10, 30, 45, 123456789, time.UTC) // 123.456789 ms

	// Create values with different nanosecond components
	tests := []struct {
		name        string
		time        time.Time
		shouldEqual bool
	}{
		{
			name:        "Same millisecond, different nanoseconds",
			time:        time.Date(2024, 11, 22, 10, 30, 45, 123999999, time.UTC), // 123.999999 ms
			shouldEqual: true,                                                     // Both truncate to 123ms
		},
		{
			name:        "Different millisecond",
			time:        time.Date(2024, 11, 22, 10, 30, 45, 124000000, time.UTC), // 124 ms
			shouldEqual: false,
		},
		{
			name:        "Exact match",
			time:        time.Date(2024, 11, 22, 10, 30, 45, 123456789, time.UTC),
			shouldEqual: true,
		},
	}

	baseFV := models.NewDateTimeValue(base)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testFV := models.NewDateTimeValue(tt.time)
			result := baseFV.Equals(testFV)

			if result != tt.shouldEqual {
				t.Errorf("Expected Equals()=%v for %s, got %v", tt.shouldEqual, tt.name, result)
			}
		})
	}
}

// TestDateTime_BSONRoundTrip tests BSON serialization through document fields
// Note: Direct FieldValue BSON marshaling requires document context, so we test
// through the AsInterface() → NewInterfaceValue() cycle which is how BSON actually works
func TestDateTime_BSONRoundTrip(t *testing.T) {
	t.Run("DateTime via AsInterface round trip", func(t *testing.T) {
		original := models.NewDateTimeValue(time.Date(2024, 11, 22, 15, 30, 45, 123000000, time.UTC))

		// Simulate what happens during BSON marshaling
		asInterface := original.AsInterface()
		timeVal, ok := asInterface.(time.Time)
		if !ok {
			t.Fatalf("AsInterface() should return time.Time, got %T", asInterface)
		}

		// Simulate unmarshaling by creating new FieldValue from interface
		restored := models.NewInterfaceValue(timeVal)

		// Verify type
		if restored.Type != models.FieldTypeDateTime {
			t.Errorf("Expected FieldTypeDateTime after round trip, got %v", restored.Type)
		}

		// Verify value (with millisecond precision)
		origTime, _ := original.AsDateTime()
		restoredTime, ok := restored.AsDateTime()
		if !ok {
			t.Fatal("Failed to retrieve DateTime from restored value")
		}

		origTrunc := origTime.Truncate(time.Millisecond)
		restoredTrunc := restoredTime.Truncate(time.Millisecond)
		if !origTrunc.Equal(restoredTrunc) {
			t.Errorf("Round trip failed: expected %v, got %v", origTrunc, restoredTrunc)
		}
	})

	t.Run("Date via AsInterface round trip", func(t *testing.T) {
		original := models.NewDateValue(time.Date(2024, 11, 22, 15, 30, 45, 0, time.UTC))

		// Get as interface
		asInterface := original.AsInterface()
		timeVal, ok := asInterface.(time.Time)
		if !ok {
			t.Fatalf("AsInterface() should return time.Time, got %T", asInterface)
		}

		// Create new FieldValue (simulates unmarshaling)
		// Note: NewInterfaceValue creates DateTime by default, not Date
		restored := models.NewInterfaceValue(timeVal)

		// Verify it's a DateTime (since NewInterfaceValue doesn't distinguish)
		if restored.Type != models.FieldTypeDateTime {
			t.Errorf("Expected FieldTypeDateTime from NewInterfaceValue, got %v", restored.Type)
		}

		// The time should still be midnight UTC
		restoredTime, ok := restored.AsDateTime()
		if !ok {
			t.Fatal("Failed to retrieve time from restored value")
		}

		if restoredTime.Hour() != 0 || restoredTime.Minute() != 0 {
			t.Errorf("Time should be midnight, got %v", restoredTime)
		}
	})
}

// TestDateTime_EdgeCases tests edge cases and boundary conditions
func TestDateTime_EdgeCases(t *testing.T) {
	t.Run("Zero time", func(t *testing.T) {
		zeroTime := time.Time{}
		fv := models.NewDateTimeValue(zeroTime)

		retrieved, ok := fv.AsDateTime()
		if !ok {
			t.Fatal("Failed to retrieve zero DateTime")
		}

		// Should be 0001-01-01 00:00:00 UTC
		if retrieved.Year() != 1 || retrieved.Month() != time.January || retrieved.Day() != 1 {
			t.Errorf("Zero time components incorrect: got %v", retrieved)
		}
	})

	t.Run("Far future date", func(t *testing.T) {
		farFuture := time.Date(9999, 12, 31, 23, 59, 59, 0, time.UTC)
		fv := models.NewDateTimeValue(farFuture)

		retrieved, ok := fv.AsDateTime()
		if !ok {
			t.Fatal("Failed to retrieve far future DateTime")
		}

		if !retrieved.Equal(farFuture) {
			t.Errorf("Far future date mismatch: expected %v, got %v", farFuture, retrieved)
		}
	})

	t.Run("Leap year date", func(t *testing.T) {
		leapDay := time.Date(2024, 2, 29, 12, 0, 0, 0, time.UTC)
		fv := models.NewDateValue(leapDay)

		retrieved, ok := fv.AsDate()
		if !ok {
			t.Fatal("Failed to retrieve leap day Date")
		}

		expected := time.Date(2024, 2, 29, 0, 0, 0, 0, time.UTC)
		if !retrieved.Equal(expected) {
			t.Errorf("Leap day mismatch: expected %v, got %v", expected, retrieved)
		}
	})
}

// TestDateTime_NewInterfaceValue tests automatic DateTime detection in NewInterfaceValue
func TestDateTime_NewInterfaceValue(t *testing.T) {
	testTime := time.Date(2024, 11, 22, 15, 30, 45, 0, time.UTC)

	// Pass time.Time through NewInterfaceValue
	fv := models.NewInterfaceValue(testTime)

	// Should detect as DateTime
	if fv.Type != models.FieldTypeDateTime {
		t.Errorf("NewInterfaceValue should detect time.Time as DateTime, got %v", fv.Type)
	}

	// Should store in UTC
	retrieved, ok := fv.AsDateTime()
	if !ok {
		t.Fatal("Failed to retrieve DateTime from NewInterfaceValue")
	}

	if !retrieved.Equal(testTime) {
		t.Errorf("Time mismatch: expected %v, got %v", testTime, retrieved)
	}
}

// TestDateTime_ComparisonOperators tests comparison logic used in WHERE clauses
func TestDateTime_ComparisonOperators(t *testing.T) {
	// Create test times
	earlier := time.Date(2024, 11, 22, 10, 0, 0, 0, time.UTC)
	middle := time.Date(2024, 11, 22, 15, 0, 0, 0, time.UTC)
	later := time.Date(2024, 11, 22, 20, 0, 0, 0, time.UTC)

	tests := []struct {
		name     string
		a        time.Time
		b        time.Time
		op       string
		expected bool
	}{
		{"Equal - same", middle, middle, "==", true},
		{"Equal - different", earlier, later, "==", false},
		{"Not equal - different", earlier, later, "!=", true},
		{"Not equal - same", middle, middle, "!=", false},
		{"Less than - true", earlier, later, "<", true},
		{"Less than - false", later, earlier, "<", false},
		{"Less than or equal - less", earlier, later, "<=", true},
		{"Less than or equal - equal", middle, middle, "<=", true},
		{"Greater than - true", later, earlier, ">", true},
		{"Greater than - false", earlier, later, ">", false},
		{"Greater than or equal - greater", later, earlier, ">=", true},
		{"Greater than or equal - equal", middle, middle, ">=", true},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("%s %s", tt.op, tt.name), func(t *testing.T) {
			var result bool

			// Simulate the comparison logic from compareValues
			aTrunc := tt.a.Truncate(time.Millisecond)
			bTrunc := tt.b.Truncate(time.Millisecond)
			aMillis := float64(aTrunc.UnixMilli())
			bMillis := float64(bTrunc.UnixMilli())

			switch tt.op {
			case "==":
				result = aMillis == bMillis
			case "!=":
				result = aMillis != bMillis
			case "<":
				result = aMillis < bMillis
			case "<=":
				result = aMillis <= bMillis
			case ">":
				result = aMillis > bMillis
			case ">=":
				result = aMillis >= bMillis
			}

			if result != tt.expected {
				t.Errorf("Comparison %v %s %v: expected %v, got %v",
					tt.a.Format(time.RFC3339), tt.op, tt.b.Format(time.RFC3339), tt.expected, result)
			}
		})
	}
}
