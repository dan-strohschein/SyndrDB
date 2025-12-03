package main

import (
	"testing"
	"time"

	"syndrdb/src/internal/domain/models"
)

// TestDateTime_BasicFieldValue tests DateTime FieldValue creation and accessors
func TestDateTime_BasicFieldValue(t *testing.T) {
	// Create a DateTime value
	now := time.Now()
	fv := models.NewDateTimeValue(now)

	// Verify type
	if fv.Type != models.FieldTypeDateTime {
		t.Errorf("Expected FieldTypeDateTime, got %v", fv.Type)
	}

	// Verify value is stored in UTC
	retrieved, ok := fv.AsDateTime()
	if !ok {
		t.Fatal("Failed to retrieve DateTime value")
	}

	if retrieved.Location() != time.UTC {
		t.Errorf("DateTime should be in UTC, got %v", retrieved.Location())
	}

	// Verify millisecond precision comparison
	expected := now.UTC().Truncate(time.Millisecond)
	actual := retrieved.Truncate(time.Millisecond)
	if !expected.Equal(actual) {
		t.Errorf("DateTime mismatch: expected %v, got %v", expected, actual)
	}
}

// TestDate_BasicFieldValue tests Date FieldValue creation and accessors
func TestDate_BasicFieldValue(t *testing.T) {
	// Create a Date value
	testTime := time.Date(2024, 11, 22, 15, 30, 45, 999, time.Local)
	fv := models.NewDateValue(testTime)

	// Verify type
	if fv.Type != models.FieldTypeDate {
		t.Errorf("Expected FieldTypeDate, got %v", fv.Type)
	}

	// Verify value is stored as date-only at midnight UTC
	retrieved, ok := fv.AsDate()
	if !ok {
		t.Fatal("Failed to retrieve Date value")
	}

	// Check it's midnight UTC
	if retrieved.Hour() != 0 || retrieved.Minute() != 0 || retrieved.Second() != 0 || retrieved.Nanosecond() != 0 {
		t.Errorf("Date should be at midnight UTC, got %v", retrieved)
	}

	// Check date components match
	if retrieved.Year() != 2024 || retrieved.Month() != time.November || retrieved.Day() != 22 {
		t.Errorf("Date components mismatch: expected 2024-11-22, got %v", retrieved.Format("2006-01-02"))
	}

	if retrieved.Location() != time.UTC {
		t.Errorf("Date should be in UTC, got %v", retrieved.Location())
	}
}

// TestDateTime_Comparison tests DateTime field comparison with millisecond precision
func TestDateTime_Comparison(t *testing.T) {
	// Create two DateTime values that differ by microseconds
	base := time.Date(2024, 11, 22, 10, 30, 45, 500*1000*1000, time.UTC)             // 500ms
	similar := time.Date(2024, 11, 22, 10, 30, 45, 500*1000*1000+123*1000, time.UTC) // 500.123ms
	different := time.Date(2024, 11, 22, 10, 30, 45, 501*1000*1000, time.UTC)        // 501ms

	fv1 := models.NewDateTimeValue(base)
	fv2 := models.NewDateTimeValue(similar)
	fv3 := models.NewDateTimeValue(different)

	// Within same millisecond should be equal (millisecond precision)
	if !fv1.Equals(fv2) {
		t.Errorf("DateTime values within same millisecond should be equal")
	}

	// Different millisecond should not be equal
	if fv1.Equals(fv3) {
		t.Errorf("DateTime values in different milliseconds should not be equal")
	}
}

// TestDate_Comparison tests Date field comparison
func TestDate_Comparison(t *testing.T) {
	// Create Date values from different times on the same day
	morning := time.Date(2024, 11, 22, 8, 0, 0, 0, time.UTC)
	evening := time.Date(2024, 11, 22, 20, 30, 45, 999, time.UTC)
	nextDay := time.Date(2024, 11, 23, 0, 0, 0, 0, time.UTC)

	fv1 := models.NewDateValue(morning)
	fv2 := models.NewDateValue(evening)
	fv3 := models.NewDateValue(nextDay)

	// Same day should be equal (time component zeroed)
	if !fv1.Equals(fv2) {
		t.Errorf("Date values from same day should be equal regardless of time")
	}

	// Different day should not be equal
	if fv1.Equals(fv3) {
		t.Errorf("Date values from different days should not be equal")
	}
}

// TestDateTime_JSONMarshaling tests DateTime JSON output format
func TestDateTime_JSONMarshaling(t *testing.T) {
	testTime := time.Date(2024, 11, 22, 15, 30, 45, 0, time.UTC)
	fv := models.NewDateTimeValue(testTime)

	jsonBytes, err := fv.MarshalJSON()
	if err != nil {
		t.Fatalf("Failed to marshal DateTime to JSON: %v", err)
	}

	// Should be RFC3339 format
	expected := "\"2024-11-22T15:30:45Z\""
	actual := string(jsonBytes)
	if actual != expected {
		t.Errorf("DateTime JSON format mismatch: expected %s, got %s", expected, actual)
	}
}

// TestDate_JSONMarshaling tests Date JSON output format
func TestDate_JSONMarshaling(t *testing.T) {
	testTime := time.Date(2024, 11, 22, 15, 30, 45, 0, time.UTC)
	fv := models.NewDateValue(testTime)

	jsonBytes, err := fv.MarshalJSON()
	if err != nil {
		t.Fatalf("Failed to marshal Date to JSON: %v", err)
	}

	// Should be YYYY-MM-DD format
	expected := "\"2024-11-22\""
	actual := string(jsonBytes)
	if actual != expected {
		t.Errorf("Date JSON format mismatch: expected %s, got %s", expected, actual)
	}
}

// TestDateTime_NullHandling tests NULL datetime values
func TestDateTime_NullHandling(t *testing.T) {
	// Create nil FieldValue
	fv := models.FieldValue{Type: models.FieldTypeNil}

	if !fv.IsNil() {
		t.Error("FieldValue should be nil")
	}

	// Accessing as DateTime should return false
	_, ok := fv.AsDateTime()
	if ok {
		t.Error("Nil FieldValue should not convert to DateTime")
	}

	// Accessing as Date should return false
	_, ok = fv.AsDate()
	if ok {
		t.Error("Nil FieldValue should not convert to Date")
	}
}

// TestDateTime_AsInterface tests DateTime AsInterface conversion
// NOTE: AsInterface returns RFC3339 string for JSON compatibility, not time.Time
func TestDateTime_AsInterface(t *testing.T) {
	testTime := time.Date(2024, 11, 22, 15, 30, 45, 0, time.UTC)
	fv := models.NewDateTimeValue(testTime)

	val := fv.AsInterface()
	strVal, ok := val.(string)
	if !ok {
		t.Fatalf("AsInterface should return string (RFC3339), got %T", val)
	}

	// Should be RFC3339 formatted
	expected := testTime.Format(time.RFC3339)
	if strVal != expected {
		t.Errorf("AsInterface string mismatch: expected %v, got %v", expected, strVal)
	}

	// Verify it can be parsed back to time.Time
	parsed, err := time.Parse(time.RFC3339, strVal)
	if err != nil {
		t.Fatalf("Failed to parse RFC3339 string: %v", err)
	}
	if !parsed.Equal(testTime) {
		t.Errorf("Parsed time mismatch: expected %v, got %v", testTime, parsed)
	}
}

// TestDate_AsInterface tests Date AsInterface conversion
// NOTE: AsInterface returns YYYY-MM-DD string for JSON compatibility, not time.Time
func TestDate_AsInterface(t *testing.T) {
	testTime := time.Date(2024, 11, 22, 0, 0, 0, 0, time.UTC)
	fv := models.NewDateValue(testTime)

	val := fv.AsInterface()
	strVal, ok := val.(string)
	if !ok {
		t.Fatalf("AsInterface should return string (YYYY-MM-DD), got %T", val)
	}

	// Should be YYYY-MM-DD formatted
	expected := "2024-11-22"
	if strVal != expected {
		t.Errorf("AsInterface string mismatch: expected %v, got %v", expected, strVal)
	}

	// Verify it can be parsed back to date
	parsed, err := time.Parse("2006-01-02", strVal)
	if err != nil {
		t.Fatalf("Failed to parse date string: %v", err)
	}
	expectedTime := time.Date(2024, 11, 22, 0, 0, 0, 0, time.UTC)
	if !parsed.Equal(expectedTime) {
		t.Errorf("Parsed date mismatch: expected %v, got %v", expectedTime, parsed)
	}
}
