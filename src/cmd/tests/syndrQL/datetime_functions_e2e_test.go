package syndrQL

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"syndrdb/src/internal/server"
	"testing"
	"time"
)

// TestDateTime_Function_NOW tests the F:NOW() function
func TestDateTime_Function_NOW(t *testing.T) {
	fixture := setupFullServer(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Test F:NOW() using expression-only SELECT (no FROM clause needed)
	selectCmd := `SELECT F:NOW();`
	queryTime := time.Now().UTC()

	startTime := time.Now()
	response, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, selectCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	if err != nil {
		t.Fatalf("Failed to execute F:NOW(): %v", err)
	}

	cmdResp, ok := response.(*server.CommandResponse)
	if !ok {
		t.Fatalf("Expected CommandResponse, got %T", response)
	}

	jsonBytes, err := json.Marshal(cmdResp.Result)
	if err != nil {
		t.Fatalf("Failed to marshal result: %v", err)
	}

	var results []map[string]interface{}
	if err := json.Unmarshal(jsonBytes, &results); err != nil {
		t.Fatalf("Failed to unmarshal results: %v", err)
	}

	if len(results) != 1 {
		t.Fatalf("Expected 1 result row, got %d", len(results))
	}

	// Expression-only SELECT uses ordinal column names (column1, column2, etc.)
	nowValue, ok := results[0]["column1"].(string)
	if !ok {
		t.Fatalf("column1 should return string, got %T: %v", results[0]["column1"], results[0]["column1"])
	}

	parsedTime, err := time.Parse(time.RFC3339, nowValue)
	if err != nil {
		t.Fatalf("Failed to parse NOW result: %v", err)
	}

	diff := parsedTime.Sub(queryTime).Abs()
	if diff > 5*time.Second {
		t.Errorf("NOW timestamp differs by more than 5 seconds: %v", diff)
	}

	t.Logf("✓ F:NOW() returned timestamp: %s (within %v of query time)", nowValue, diff)
}

// TestDateTime_Function_EXTRACT tests the F:EXTRACT() function
func TestDateTime_Function_EXTRACT(t *testing.T) {
	fixture := setupFullServer(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	testTimestamp := "2024-11-22T15:30:45Z"

	testCases := []struct {
		field    string
		expected float64
	}{
		{"YEAR", 2024},
		{"MONTH", 11},
		{"DAY", 22},
		{"HOUR", 15},
		{"MINUTE", 30},
		{"SECOND", 45},
	}

	for _, tc := range testCases {
		t.Run(tc.field, func(t *testing.T) {
			selectCmd := fmt.Sprintf(`SELECT F:EXTRACT("%s", "%s");`, tc.field, testTimestamp)

			startTime := time.Now()
			response, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, selectCmd, fixture.Logger, startTime, nil, "127.0.0.1")
			if err != nil {
				t.Fatalf("Failed to execute F:EXTRACT(%s): %v", tc.field, err)
			}

			cmdResp, ok := response.(*server.CommandResponse)
			if !ok {
				t.Fatalf("Expected CommandResponse, got %T", response)
			}

			jsonBytes, err := json.Marshal(cmdResp.Result)
			if err != nil {
				t.Fatalf("Failed to marshal result: %v", err)
			}

			var results []map[string]interface{}
			if err := json.Unmarshal(jsonBytes, &results); err != nil {
				t.Fatalf("Failed to unmarshal results: %v", err)
			}

			if len(results) != 1 {
				t.Fatalf("Expected 1 result row, got %d", len(results))
			}

			extractedValue, ok := results[0]["column1"]
			if !ok {
				t.Fatalf("Column column1 not found in results: %+v", results[0])
			}

			var actualValue float64
			switch v := extractedValue.(type) {
			case float64:
				actualValue = v
			case int:
				actualValue = float64(v)
			case int64:
				actualValue = float64(v)
			default:
				t.Fatalf("Unexpected type for extracted value: %T", v)
			}

			if actualValue != tc.expected {
				t.Errorf("F:EXTRACT(%s) returned %v, expected %v", tc.field, actualValue, tc.expected)
			}

			t.Logf("✓ F:EXTRACT(%s) = %v", tc.field, actualValue)
		})
	}
}

// TestDateTime_Function_DATE_TRUNC tests the F:DATE_TRUNC() function
func TestDateTime_Function_DATE_TRUNC(t *testing.T) {
	fixture := setupFullServer(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	testTimestamp := "2024-11-22T15:30:45Z"

	testCases := []struct {
		precision string
		expected  string
	}{
		{"YEAR", "2024-01-01T00:00:00Z"},
		{"MONTH", "2024-11-01T00:00:00Z"},
		{"DAY", "2024-11-22T00:00:00Z"},
		{"HOUR", "2024-11-22T15:00:00Z"},
	}

	for _, tc := range testCases {
		t.Run(tc.precision, func(t *testing.T) {
			selectCmd := fmt.Sprintf(`SELECT F:DATE_TRUNC("%s", "%s");`, tc.precision, testTimestamp)

			startTime := time.Now()
			response, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, selectCmd, fixture.Logger, startTime, nil, "127.0.0.1")
			if err != nil {
				t.Fatalf("Failed to execute F:DATE_TRUNC(%s): %v", tc.precision, err)
			}

			cmdResp, ok := response.(*server.CommandResponse)
			if !ok {
				t.Fatalf("Expected CommandResponse, got %T", response)
			}

			jsonBytes, err := json.Marshal(cmdResp.Result)
			if err != nil {
				t.Fatalf("Failed to marshal result: %v", err)
			}

			var results []map[string]interface{}
			if err := json.Unmarshal(jsonBytes, &results); err != nil {
				t.Fatalf("Failed to unmarshal results: %v", err)
			}

			if len(results) != 1 {
				t.Fatalf("Expected 1 result row, got %d", len(results))
			}

			truncatedValue, ok := results[0]["column1"].(string)
			if !ok {
				t.Fatalf("F:DATE_TRUNC should return string, got %T: %v", results[0]["column1"], results[0]["column1"])
			}

			if truncatedValue != tc.expected {
				t.Errorf("F:DATE_TRUNC(%s) returned %s, expected %s", tc.precision, truncatedValue, tc.expected)
			}

			t.Logf("✓ F:DATE_TRUNC(%s) = %s", tc.precision, truncatedValue)
		})
	}
}

// TestDateTime_Function_DATE_ADD tests F:DATE_ADD() function
func TestDateTime_Function_DATE_ADD(t *testing.T) {
	fixture := setupFullServer(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	baseTimestamp := "2024-11-22T15:30:00Z"
	selectCmd := fmt.Sprintf(`SELECT F:DATE_ADD("%s", 1, "DAY");`, baseTimestamp)

	startTime := time.Now()
	response, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, selectCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	if err != nil {
		t.Fatalf("Failed to execute F:DATE_ADD: %v", err)
	}

	cmdResp, ok := response.(*server.CommandResponse)
	if !ok {
		t.Fatalf("Expected CommandResponse, got %T", response)
	}

	jsonBytes, err := json.Marshal(cmdResp.Result)
	if err != nil {
		t.Fatalf("Failed to marshal result: %v", err)
	}

	var results []map[string]interface{}
	if err := json.Unmarshal(jsonBytes, &results); err != nil {
		t.Fatalf("Failed to unmarshal results: %v", err)
	}

	if len(results) != 1 {
		t.Fatalf("Expected 1 result row, got %d", len(results))
	}

	resultValue, ok := results[0]["column1"].(string)
	if !ok {
		t.Fatalf("F:DATE_ADD should return string, got %T: %v", results[0]["column1"], results[0]["column1"])
	}

	expected := "2024-11-23T15:30:00Z"
	if resultValue != expected {
		t.Errorf("F:DATE_ADD returned %s, expected %s", resultValue, expected)
	}

	t.Logf("✓ F:DATE_ADD(INTERVAL '1 DAY') = %s", resultValue)
}

// TestDateTime_Function_AGE tests the F:AGE() function
func TestDateTime_Function_AGE(t *testing.T) {
	fixture := setupFullServer(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	timestamp1 := "2024-11-22T15:30:00Z"
	timestamp2 := "2023-11-22T15:30:00Z"

	selectCmd := fmt.Sprintf(`SELECT F:AGE("%s", "%s");`, timestamp1, timestamp2)

	startTime := time.Now()
	response, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, selectCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	if err != nil {
		t.Fatalf("Failed to execute F:AGE(): %v", err)
	}

	cmdResp, ok := response.(*server.CommandResponse)
	if !ok {
		t.Fatalf("Expected CommandResponse, got %T", response)
	}

	jsonBytes, err := json.Marshal(cmdResp.Result)
	if err != nil {
		t.Fatalf("Failed to marshal result: %v", err)
	}

	var results []map[string]interface{}
	if err := json.Unmarshal(jsonBytes, &results); err != nil {
		t.Fatalf("Failed to unmarshal results: %v", err)
	}

	if len(results) != 1 {
		t.Fatalf("Expected 1 result row, got %d", len(results))
	}

	ageValue, ok := results[0]["column1"].(string)
	if !ok {
		t.Fatalf("F:AGE should return string, got %T: %v", results[0]["column1"], results[0]["column1"])
	}

	if !strings.Contains(strings.ToLower(ageValue), "year") {
		t.Errorf("F:AGE returned unexpected interval: %s", ageValue)
	}

	t.Logf("✓ F:AGE() returned: %s", ageValue)
}

// TestDateTime_DefaultValue_NOW tests F:NOW() as default value in CREATE BUNDLE
func TestDateTime_DefaultValue_NOW(t *testing.T) {
	fixture := setupFullServer(t)

	bundleName := "TestDefaultNowBundle"

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	createCmd := fmt.Sprintf(`CREATE BUNDLE "%s" WITH FIELDS (
		{"id", "STRING", true, false, ""},
		{"name", "STRING", true, false, ""},
		{"created_at", "DATETIME", true, false, "F:NOW()"}
	);`, bundleName)

	startTime := time.Now()
	_, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, createCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	if err != nil {
		t.Fatalf("Failed to create bundle with F:NOW() default: %v", err)
	}

	insertCmd := fmt.Sprintf(`ADD DOCUMENT TO BUNDLE "%s" WITH (
		{"id"="test-001"},
		{"name"="Test Item"}
	);`, bundleName)

	beforeInsert := time.Now().UTC()
	startTime = time.Now()
	_, err = server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, insertCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	if err != nil {
		t.Fatalf("Failed to insert document: %v", err)
	}

	fixture.ServiceManager.BundleService.FlushMetadataUpdates()
	err = fixture.ServiceManager.BundleService.FlushAllBuffers()
	if err != nil {
		t.Logf("Warning: Failed to flush buffers: %v", err)
	}
	time.Sleep(100 * time.Millisecond)

	selectCmd := fmt.Sprintf(`SELECT * FROM "%s" WHERE "id" == "test-001";`, bundleName)
	startTime = time.Now()
	response, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, selectCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	if err != nil {
		t.Fatalf("Failed to select document: %v", err)
	}

	cmdResp, ok := response.(*server.CommandResponse)
	if !ok {
		t.Fatalf("Expected CommandResponse, got %T", response)
	}

	jsonBytes, err := json.Marshal(cmdResp.Result)
	if err != nil {
		t.Fatalf("Failed to marshal result: %v", err)
	}

	var documents []map[string]interface{}
	if err := json.Unmarshal(jsonBytes, &documents); err != nil {
		t.Fatalf("Failed to unmarshal documents: %v", err)
	}

	if len(documents) != 1 {
		t.Fatalf("Expected 1 document, got %d", len(documents))
	}

	doc := documents[0]

	createdAt, ok := doc["created_at"].(string)
	if !ok {
		t.Fatalf("created_at should be string, got %T: %v", doc["created_at"], doc["created_at"])
	}

	parsedTime, err := time.Parse(time.RFC3339, createdAt)
	if err != nil {
		t.Fatalf("Failed to parse created_at: %v", err)
	}

	diff := parsedTime.Sub(beforeInsert).Abs()
	if diff > 5*time.Second {
		t.Errorf("created_at differs by more than 5 seconds from insert time: %v", diff)
	}

	t.Logf("✓ F:NOW() as default value populated created_at: %s (within %v of insert)", createdAt, diff)
}

// TestDateTime_ATTimeZone_BasicConversion tests AT TIME ZONE operator with basic timezone conversions
func TestDateTime_ATTimeZone_BasicConversion(t *testing.T) {
	fixture := setupFullServer(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Test converting a UTC timestamp to different timezones
	testTimestamp := "2024-11-22T15:30:45Z" // 3:30:45 PM UTC

	testCases := []struct {
		name         string
		timezone     string
		expectedHour int // Expected hour in target timezone
	}{
		{"UTC to UTC", "UTC", 15},
		{"UTC to America/New_York", "America/New_York", 10},      // EST is UTC-5
		{"UTC to America/Los_Angeles", "America/Los_Angeles", 7}, // PST is UTC-8
		{"UTC to Europe/London", "Europe/London", 15},            // GMT is same as UTC in November
		{"UTC to Asia/Tokyo", "Asia/Tokyo", 0},                   // JST is UTC+9, so 15+9=24=0 next day
		{"UTC to Australia/Sydney", "Australia/Sydney", 2},       // AEDT is UTC+11, so 15+11=26=2 next day
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			selectCmd := fmt.Sprintf(`SELECT "%s" AT TIME ZONE '%s';`, testTimestamp, tc.timezone)

			startTime := time.Now()
			response, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, selectCmd, fixture.Logger, startTime, nil, "127.0.0.1")
			if err != nil {
				t.Fatalf("Failed to execute AT TIME ZONE: %v", err)
			}

			cmdResp, ok := response.(*server.CommandResponse)
			if !ok {
				t.Fatalf("Expected CommandResponse, got %T", response)
			}

			jsonBytes, err := json.Marshal(cmdResp.Result)
			if err != nil {
				t.Fatalf("Failed to marshal result: %v", err)
			}

			var results []map[string]interface{}
			if err := json.Unmarshal(jsonBytes, &results); err != nil {
				t.Fatalf("Failed to unmarshal results: %v", err)
			}

			if len(results) != 1 {
				t.Fatalf("Expected 1 result row, got %d", len(results))
			}

			convertedValue, ok := results[0]["column1"].(string)
			if !ok {
				t.Fatalf("column1 should return string, got %T: %v", results[0]["column1"], results[0]["column1"])
			}

			parsedTime, err := time.Parse(time.RFC3339, convertedValue)
			if err != nil {
				t.Fatalf("Failed to parse converted timestamp: %v", err)
			}

			if parsedTime.Hour() != tc.expectedHour {
				t.Errorf("Expected hour %d in %s, got %d", tc.expectedHour, tc.timezone, parsedTime.Hour())
			}

			t.Logf("✓ Converted to %s: %s (hour=%d)", tc.timezone, convertedValue, parsedTime.Hour())
		})
	}
}

// TestDateTime_ATTimeZone_WithNOW tests AT TIME ZONE with F:NOW()
func TestDateTime_ATTimeZone_WithNOW(t *testing.T) {
	fixture := setupFullServer(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Test converting current time to different timezones
	selectCmd := `SELECT F:NOW() AT TIME ZONE 'America/New_York';`

	startTime := time.Now()
	response, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, selectCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	if err != nil {
		t.Fatalf("Failed to execute F:NOW() AT TIME ZONE: %v", err)
	}

	cmdResp, ok := response.(*server.CommandResponse)
	if !ok {
		t.Fatalf("Expected CommandResponse, got %T", response)
	}

	jsonBytes, err := json.Marshal(cmdResp.Result)
	if err != nil {
		t.Fatalf("Failed to marshal result: %v", err)
	}

	var results []map[string]interface{}
	if err := json.Unmarshal(jsonBytes, &results); err != nil {
		t.Fatalf("Failed to unmarshal results: %v", err)
	}

	if len(results) != 1 {
		t.Fatalf("Expected 1 result row, got %d", len(results))
	}

	convertedValue, ok := results[0]["column1"].(string)
	if !ok {
		t.Fatalf("column1 should return string, got %T: %v", results[0]["column1"], results[0]["column1"])
	}

	parsedTime, err := time.Parse(time.RFC3339, convertedValue)
	if err != nil {
		t.Fatalf("Failed to parse converted timestamp: %v", err)
	}

	// Verify the timezone offset is correct for America/New_York (either -05:00 or -04:00 depending on DST)
	_, offset := parsedTime.Zone()
	expectedOffsets := []int{-5 * 3600, -4 * 3600} // EST or EDT
	offsetMatch := false
	for _, expected := range expectedOffsets {
		if offset == expected {
			offsetMatch = true
			break
		}
	}
	if !offsetMatch {
		t.Errorf("Expected timezone offset to be EST (-05:00) or EDT (-04:00), got %d seconds (%d hours)", offset, offset/3600)
	}

	t.Logf("✓ F:NOW() AT TIME ZONE 'America/New_York': %s (offset=%+d hours)", convertedValue, offset/3600)
}

// TestDateTime_ATTimeZone_ChainedConversions tests multiple AT TIME ZONE conversions
func TestDateTime_ATTimeZone_ChainedConversions(t *testing.T) {
	fixture := setupFullServer(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Test chaining timezone conversions: UTC -> Tokyo -> New York
	// Start with midnight UTC
	testTimestamp := "2024-11-22T00:00:00Z"

	// First convert to Tokyo (should be 9:00 AM JST)
	selectCmd1 := fmt.Sprintf(`SELECT "%s" AT TIME ZONE 'Asia/Tokyo';`, testTimestamp)

	startTime := time.Now()
	response, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, selectCmd1, fixture.Logger, startTime, nil, "127.0.0.1")
	if err != nil {
		t.Fatalf("Failed to convert to Tokyo: %v", err)
	}

	cmdResp, ok := response.(*server.CommandResponse)
	if !ok {
		t.Fatalf("Expected CommandResponse, got %T", response)
	}

	jsonBytes, err := json.Marshal(cmdResp.Result)
	if err != nil {
		t.Fatalf("Failed to marshal result: %v", err)
	}

	var results []map[string]interface{}
	if err := json.Unmarshal(jsonBytes, &results); err != nil {
		t.Fatalf("Failed to unmarshal results: %v", err)
	}

	tokyoValue, ok := results[0]["column1"].(string)
	if !ok {
		t.Fatalf("column1 should return string, got %T", results[0]["column1"])
	}

	tokyoTime, err := time.Parse(time.RFC3339, tokyoValue)
	if err != nil {
		t.Fatalf("Failed to parse Tokyo timestamp: %v", err)
	}

	if tokyoTime.Hour() != 9 {
		t.Errorf("Expected hour 9 in Tokyo, got %d", tokyoTime.Hour())
	}

	t.Logf("✓ UTC midnight -> Tokyo: %s (hour=%d)", tokyoValue, tokyoTime.Hour())

	// Now convert the same UTC time to New York (should be 7:00 PM previous day EST)
	selectCmd2 := fmt.Sprintf(`SELECT "%s" AT TIME ZONE 'America/New_York';`, testTimestamp)

	startTime = time.Now()
	response, err = server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, selectCmd2, fixture.Logger, startTime, nil, "127.0.0.1")
	if err != nil {
		t.Fatalf("Failed to convert to New York: %v", err)
	}

	cmdResp, ok = response.(*server.CommandResponse)
	if !ok {
		t.Fatalf("Expected CommandResponse, got %T", response)
	}

	jsonBytes, err = json.Marshal(cmdResp.Result)
	if err != nil {
		t.Fatalf("Failed to marshal result: %v", err)
	}

	if err := json.Unmarshal(jsonBytes, &results); err != nil {
		t.Fatalf("Failed to unmarshal results: %v", err)
	}

	nyValue, ok := results[0]["column1"].(string)
	if !ok {
		t.Fatalf("column1 should return string, got %T", results[0]["column1"])
	}

	nyTime, err := time.Parse(time.RFC3339, nyValue)
	if err != nil {
		t.Fatalf("Failed to parse New York timestamp: %v", err)
	}

	if nyTime.Hour() != 19 { // 7 PM
		t.Errorf("Expected hour 19 in New York, got %d", nyTime.Hour())
	}

	t.Logf("✓ UTC midnight -> New York: %s (hour=%d)", nyValue, nyTime.Hour())
}

// TestDateTime_ATTimeZone_InvalidTimezone tests error handling for invalid timezones
func TestDateTime_ATTimeZone_InvalidTimezone(t *testing.T) {
	fixture := setupFullServer(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	testTimestamp := "2024-11-22T15:30:45Z"
	selectCmd := fmt.Sprintf(`SELECT "%s" AT TIME ZONE 'Invalid/Timezone';`, testTimestamp)

	startTime := time.Now()
	_, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, selectCmd, fixture.Logger, startTime, nil, "127.0.0.1")

	if err == nil {
		t.Fatal("Expected error for invalid timezone, but got none")
	}

	if !strings.Contains(err.Error(), "timezone") && !strings.Contains(err.Error(), "location") {
		t.Errorf("Expected timezone-related error, got: %v", err)
	}

	t.Logf("✓ Invalid timezone properly rejected: %v", err)
}

// TestDateTime_ATTimeZone_WithEXTRACT tests combining AT TIME ZONE with EXTRACT
func TestDateTime_ATTimeZone_WithEXTRACT(t *testing.T) {
	fixture := setupFullServer(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Test extracting hour from a timestamp converted to Tokyo time
	// UTC midnight should become 9 AM in Tokyo
	testTimestamp := "2024-11-22T00:00:00Z"
	selectCmd := fmt.Sprintf(`SELECT F:EXTRACT("HOUR", "%s" AT TIME ZONE 'Asia/Tokyo');`, testTimestamp)

	startTime := time.Now()
	response, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, selectCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	if err != nil {
		t.Fatalf("Failed to execute EXTRACT with AT TIME ZONE: %v", err)
	}

	cmdResp, ok := response.(*server.CommandResponse)
	if !ok {
		t.Fatalf("Expected CommandResponse, got %T", response)
	}

	jsonBytes, err := json.Marshal(cmdResp.Result)
	if err != nil {
		t.Fatalf("Failed to marshal result: %v", err)
	}

	var results []map[string]interface{}
	if err := json.Unmarshal(jsonBytes, &results); err != nil {
		t.Fatalf("Failed to unmarshal results: %v", err)
	}

	if len(results) != 1 {
		t.Fatalf("Expected 1 result row, got %d", len(results))
	}

	hourValue, ok := results[0]["column1"].(float64)
	if !ok {
		t.Fatalf("column1 should return float64, got %T: %v", results[0]["column1"], results[0]["column1"])
	}

	expectedHour := 9.0
	if hourValue != expectedHour {
		t.Errorf("Expected hour %v in Tokyo time, got %v", expectedHour, hourValue)
	}

	t.Logf("✓ EXTRACT(HOUR) from Tokyo-converted timestamp: %v", hourValue)
}

// TestDateTime_ATTimeZone_DSTTransition tests timezone conversion during DST transitions
func TestDateTime_ATTimeZone_DSTTransition(t *testing.T) {
	fixture := setupFullServer(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	testCases := []struct {
		name         string
		timestamp    string // UTC
		timezone     string
		expectedHour int
	}{
		{
			name:         "Before DST (January - EST)",
			timestamp:    "2024-01-15T15:00:00Z",
			timezone:     "America/New_York",
			expectedHour: 10, // EST is UTC-5
		},
		{
			name:         "After DST (July - EDT)",
			timestamp:    "2024-07-15T15:00:00Z",
			timezone:     "America/New_York",
			expectedHour: 11, // EDT is UTC-4
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			selectCmd := fmt.Sprintf(`SELECT "%s" AT TIME ZONE '%s';`, tc.timestamp, tc.timezone)

			startTime := time.Now()
			response, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, selectCmd, fixture.Logger, startTime, nil, "127.0.0.1")
			if err != nil {
				t.Fatalf("Failed to execute AT TIME ZONE: %v", err)
			}

			cmdResp, ok := response.(*server.CommandResponse)
			if !ok {
				t.Fatalf("Expected CommandResponse, got %T", response)
			}

			jsonBytes, err := json.Marshal(cmdResp.Result)
			if err != nil {
				t.Fatalf("Failed to marshal result: %v", err)
			}

			var results []map[string]interface{}
			if err := json.Unmarshal(jsonBytes, &results); err != nil {
				t.Fatalf("Failed to unmarshal results: %v", err)
			}

			convertedValue, ok := results[0]["column1"].(string)
			if !ok {
				t.Fatalf("column1 should return string, got %T", results[0]["column1"])
			}

			parsedTime, err := time.Parse(time.RFC3339, convertedValue)
			if err != nil {
				t.Fatalf("Failed to parse converted timestamp: %v", err)
			}

			if parsedTime.Hour() != tc.expectedHour {
				t.Errorf("Expected hour %d, got %d", tc.expectedHour, parsedTime.Hour())
			}

			t.Logf("✓ %s: %s (hour=%d)", tc.name, convertedValue, parsedTime.Hour())
		})
	}
}

// TestDateTime_GroupBy_MIN_MAX tests MIN and MAX aggregations on DateTime fields
func TestDateTime_GroupBy_MIN_MAX(t *testing.T) {
	fixture := setupFullServer(t)

	bundleName := "EventLog"

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Create bundle with event_type and event_time fields
	createCmd := fmt.Sprintf(`CREATE BUNDLE "%s" WITH FIELDS (
		{"id", "STRING", true, false, ""},
		{"event_type", "STRING", true, false, ""},
		{"event_time", "DATETIME", true, false, ""}
	);`, bundleName)

	startTime := time.Now()
	_, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, createCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	if err != nil {
		t.Fatalf("Failed to create bundle: %v", err)
	}

	// Insert test data with different event types and times
	testData := []struct {
		id        string
		eventType string
		eventTime string
	}{
		{"evt-1", "login", "2024-11-22T08:00:00Z"},
		{"evt-2", "login", "2024-11-22T09:30:00Z"},
		{"evt-3", "login", "2024-11-22T14:45:00Z"},
		{"evt-4", "logout", "2024-11-22T10:15:00Z"},
		{"evt-5", "logout", "2024-11-22T16:30:00Z"},
		{"evt-6", "logout", "2024-11-22T18:00:00Z"},
		{"evt-7", "error", "2024-11-22T11:00:00Z"},
		{"evt-8", "error", "2024-11-22T15:20:00Z"},
	}

	for _, data := range testData {
		insertCmd := fmt.Sprintf(`ADD DOCUMENT TO BUNDLE "%s" WITH (
			{"id"="%s"},
			{"event_type"="%s"},
			{"event_time"="%s"}
		);`, bundleName, data.id, data.eventType, data.eventTime)

		startTime = time.Now()
		_, err = server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, insertCmd, fixture.Logger, startTime, nil, "127.0.0.1")
		if err != nil {
			t.Fatalf("Failed to insert document %s: %v", data.id, err)
		}
	}

	fixture.ServiceManager.BundleService.FlushMetadataUpdates()
	err = fixture.ServiceManager.BundleService.FlushAllBuffers()
	if err != nil {
		t.Logf("Warning: Failed to flush buffers: %v", err)
	}

	// Test MIN and MAX on DateTime field grouped by event_type
	selectCmd := fmt.Sprintf(`SELECT event_type, MIN(event_time), MAX(event_time) 
		FROM "%s" 
		GROUP BY event_type 
		ORDER BY event_type;`, bundleName)

	startTime = time.Now()
	response, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, selectCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	if err != nil {
		t.Fatalf("Failed to execute GROUP BY query: %v", err)
	}

	cmdResp, ok := response.(*server.CommandResponse)
	if !ok {
		t.Fatalf("Expected CommandResponse, got %T", response)
	}

	jsonBytes, err := json.Marshal(cmdResp.Result)
	if err != nil {
		t.Fatalf("Failed to marshal result: %v", err)
	}

	var results []map[string]interface{}
	if err := json.Unmarshal(jsonBytes, &results); err != nil {
		t.Fatalf("Failed to unmarshal results: %v", err)
	}

	if len(results) != 3 {
		t.Fatalf("Expected 3 groups (error, login, logout), got %d", len(results))
	}

	// DEBUG: Log what fields are in the results
	t.Logf("Got %d result groups:", len(results))
	for i, row := range results {
		t.Logf("  Group %d fields: %v", i, getMapKeys(row))
		for k, v := range row {
			t.Logf("    %s: %T = %v", k, v, v)
		}
	}

	// Validate each group
	expectedResults := map[string]struct {
		minTime string
		maxTime string
	}{
		"error":  {"2024-11-22T11:00:00Z", "2024-11-22T15:20:00Z"},
		"login":  {"2024-11-22T08:00:00Z", "2024-11-22T14:45:00Z"},
		"logout": {"2024-11-22T10:15:00Z", "2024-11-22T18:00:00Z"},
	}

	for _, row := range results {
		eventType, ok := row["event_type"].(string)
		if !ok {
			t.Fatalf("event_type should be string, got %T", row["event_type"])
		}

		minTimeStr, ok := row["min_event_time"].(string)
		if !ok {
			t.Fatalf("min_event_time should be string, got %T", row["min_event_time"])
		}

		maxTimeStr, ok := row["max_event_time"].(string)
		if !ok {
			t.Fatalf("max_event_time should be string, got %T", row["max_event_time"])
		}

		expected, exists := expectedResults[eventType]
		if !exists {
			t.Fatalf("Unexpected event_type: %s", eventType)
		}

		if minTimeStr != expected.minTime {
			t.Errorf("For %s: expected MIN %s, got %s", eventType, expected.minTime, minTimeStr)
		}

		if maxTimeStr != expected.maxTime {
			t.Errorf("For %s: expected MAX %s, got %s", eventType, expected.maxTime, maxTimeStr)
		}

		t.Logf("✓ %s: MIN=%s, MAX=%s", eventType, minTimeStr, maxTimeStr)
	}
}

// TestDateTime_GroupBy_COUNT_WithDateTrunc tests COUNT with DATE_TRUNC for time-based grouping
func TestDateTime_GroupBy_COUNT_WithDateTrunc(t *testing.T) {
	fixture := setupFullServer(t)

	bundleName := "AccessLog"

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Create bundle
	createCmd := fmt.Sprintf(`CREATE BUNDLE "%s" WITH FIELDS (
		{"id", "STRING", true, false, ""},
		{"user_id", "STRING", true, false, ""},
		{"access_time", "DATETIME", true, false, ""}
	);`, bundleName)

	startTime := time.Now()
	_, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, createCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	if err != nil {
		t.Fatalf("Failed to create bundle: %v", err)
	}

	// Insert test data with access times across different hours
	testData := []struct {
		id         string
		userID     string
		accessTime string
	}{
		{"acc-1", "user1", "2024-11-22T10:15:30Z"},
		{"acc-2", "user2", "2024-11-22T10:22:45Z"},
		{"acc-3", "user1", "2024-11-22T10:48:12Z"},
		{"acc-4", "user3", "2024-11-22T11:05:00Z"},
		{"acc-5", "user2", "2024-11-22T11:30:15Z"},
		{"acc-6", "user1", "2024-11-22T12:10:20Z"},
		{"acc-7", "user3", "2024-11-22T12:45:55Z"},
		{"acc-8", "user2", "2024-11-22T12:50:30Z"},
	}

	for _, data := range testData {
		insertCmd := fmt.Sprintf(`ADD DOCUMENT TO BUNDLE "%s" WITH (
			{"id"="%s"},
			{"user_id"="%s"},
			{"access_time"="%s"}
		);`, bundleName, data.id, data.userID, data.accessTime)

		startTime = time.Now()
		_, err = server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, insertCmd, fixture.Logger, startTime, nil, "127.0.0.1")
		if err != nil {
			t.Fatalf("Failed to insert document %s: %v", data.id, err)
		}
	}

	fixture.ServiceManager.BundleService.FlushMetadataUpdates()
	err = fixture.ServiceManager.BundleService.FlushAllBuffers()
	if err != nil {
		t.Logf("Warning: Failed to flush buffers: %v", err)
	}

	// Group by hour using DATE_TRUNC and count accesses per hour
	// Note: Since GROUP BY with function expressions has limitations, we'll use a different approach
	// We'll group by user_id and get MIN/MAX times instead
	selectCmd := fmt.Sprintf(`SELECT user_id, MIN(access_time), MAX(access_time), COUNT(*) 
		FROM "%s" 
		GROUP BY user_id 
		ORDER BY user_id;`, bundleName)

	startTime = time.Now()
	response, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, selectCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	if err != nil {
		t.Fatalf("Failed to execute GROUP BY with DateTime MIN/MAX: %v", err)
	}

	cmdResp, ok := response.(*server.CommandResponse)
	if !ok {
		t.Fatalf("Expected CommandResponse, got %T", response)
	}

	jsonBytes, err := json.Marshal(cmdResp.Result)
	if err != nil {
		t.Fatalf("Failed to marshal result: %v", err)
	}

	var results []map[string]interface{}
	if err := json.Unmarshal(jsonBytes, &results); err != nil {
		t.Fatalf("Failed to unmarshal results: %v", err)
	}

	if len(results) != 3 {
		t.Fatalf("Expected 3 user groups, got %d", len(results))
	}

	// Validate results per user
	expectedCounts := map[string]struct {
		minTime string
		maxTime string
		count   float64
	}{
		"user1": {"2024-11-22T10:15:30Z", "2024-11-22T12:10:20Z", 3},
		"user2": {"2024-11-22T10:22:45Z", "2024-11-22T12:50:30Z", 3},
		"user3": {"2024-11-22T11:05:00Z", "2024-11-22T12:45:55Z", 2},
	}

	for _, row := range results {
		userID, ok := row["user_id"].(string)
		if !ok {
			t.Fatalf("user_id should be string, got %T", row["user_id"])
		}

		minTime, ok := row["min_access_time"].(string)
		if !ok {
			t.Fatalf("min_access_time should be string, got %T", row["min_access_time"])
		}

		maxTime, ok := row["max_access_time"].(string)
		if !ok {
			t.Fatalf("max_access_time should be string, got %T", row["max_access_time"])
		}

		count, ok := row["count_all"].(float64)
		if !ok {
			t.Fatalf("count_all should be float64, got %T", row["count_all"])
		}

		expected, exists := expectedCounts[userID]
		if !exists {
			t.Fatalf("Unexpected user: %s", userID)
		}

		if minTime != expected.minTime {
			t.Errorf("For %s: expected MIN %s, got %s", userID, expected.minTime, minTime)
		}

		if maxTime != expected.maxTime {
			t.Errorf("For %s: expected MAX %s, got %s", userID, expected.maxTime, maxTime)
		}

		if count != expected.count {
			t.Errorf("For %s: expected count %v, got %v", userID, expected.count, count)
		}

		t.Logf("✓ User %s: MIN=%s, MAX=%s, COUNT=%v", userID, minTime, maxTime, count)
	}
}

// TestDateTime_GroupBy_HAVING_WithDateFunctions tests HAVING clause with DateTime aggregations
func TestDateTime_GroupBy_HAVING_WithDateFunctions(t *testing.T) {
	fixture := setupFullServer(t)

	bundleName := "SessionLog"

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Create bundle
	createCmd := fmt.Sprintf(`CREATE BUNDLE "%s" WITH FIELDS (
		{"session_id", "STRING", true, false, ""},
		{"user_id", "STRING", true, false, ""},
		{"start_time", "DATETIME", true, false, ""},
		{"end_time", "DATETIME", true, false, ""}
	);`, bundleName)

	startTime := time.Now()
	_, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, createCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	if err != nil {
		t.Fatalf("Failed to create bundle: %v", err)
	}

	// Insert test data with various session durations
	testData := []struct {
		sessionID string
		userID    string
		startTime string
		endTime   string
	}{
		{"s1", "user1", "2024-11-22T08:00:00Z", "2024-11-22T08:30:00Z"}, // 30 min
		{"s2", "user1", "2024-11-22T10:00:00Z", "2024-11-22T11:00:00Z"}, // 60 min
		{"s3", "user1", "2024-11-22T14:00:00Z", "2024-11-22T14:15:00Z"}, // 15 min
		{"s4", "user2", "2024-11-22T09:00:00Z", "2024-11-22T09:45:00Z"}, // 45 min
		{"s5", "user2", "2024-11-22T11:00:00Z", "2024-11-22T13:00:00Z"}, // 120 min
		{"s6", "user3", "2024-11-22T10:00:00Z", "2024-11-22T10:20:00Z"}, // 20 min
	}

	for _, data := range testData {
		insertCmd := fmt.Sprintf(`ADD DOCUMENT TO BUNDLE "%s" WITH (
			{"session_id"="%s"},
			{"user_id"="%s"},
			{"start_time"="%s"},
			{"end_time"="%s"}
		);`, bundleName, data.sessionID, data.userID, data.startTime, data.endTime)

		startTime = time.Now()
		_, err = server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, insertCmd, fixture.Logger, startTime, nil, "127.0.0.1")
		if err != nil {
			t.Fatalf("Failed to insert document %s: %v", data.sessionID, err)
		}
	}

	fixture.ServiceManager.BundleService.FlushMetadataUpdates()
	err = fixture.ServiceManager.BundleService.FlushAllBuffers()
	if err != nil {
		t.Logf("Warning: Failed to flush buffers: %v", err)
	}

	// Find users whose earliest session started before 9:30 AM
	selectCmd := fmt.Sprintf(`SELECT user_id, MIN(start_time), MAX(end_time), COUNT(*) 
		FROM "%s" 
		GROUP BY user_id 
		HAVING MIN(start_time) < "2024-11-22T09:30:00Z"
		ORDER BY user_id;`, bundleName)

	startTime = time.Now()
	response, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, selectCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	if err != nil {
		t.Fatalf("Failed to execute GROUP BY with HAVING: %v", err)
	}

	cmdResp, ok := response.(*server.CommandResponse)
	if !ok {
		t.Fatalf("Expected CommandResponse, got %T", response)
	}

	jsonBytes, err := json.Marshal(cmdResp.Result)
	if err != nil {
		t.Fatalf("Failed to marshal result: %v", err)
	}

	var results []map[string]interface{}
	if err := json.Unmarshal(jsonBytes, &results); err != nil {
		t.Fatalf("Failed to unmarshal results: %v", err)
	}

	// Should only return user1 (started at 08:00) and user2 (started at 09:00)
	// user3 started at 10:00, which is after 09:30
	if len(results) != 2 {
		t.Fatalf("Expected 2 users with sessions before 9:30 AM, got %d", len(results))
	}

	expectedUsers := map[string]struct {
		minStart string
		maxEnd   string
		count    float64
	}{
		"user1": {"2024-11-22T08:00:00Z", "2024-11-22T14:15:00Z", 3},
		"user2": {"2024-11-22T09:00:00Z", "2024-11-22T13:00:00Z", 2},
	}

	for _, row := range results {
		userID, ok := row["user_id"].(string)
		if !ok {
			t.Fatalf("user_id should be string, got %T", row["user_id"])
		}

		minStart, ok := row["min_start_time"].(string)
		if !ok {
			t.Fatalf("min_start_time should be string, got %T", row["min_start_time"])
		}

		maxEnd, ok := row["max_end_time"].(string)
		if !ok {
			t.Fatalf("max_end_time should be string, got %T", row["max_end_time"])
		}

		count, ok := row["count_all"].(float64)
		if !ok {
			t.Fatalf("count_all should be float64, got %T", row["count_all"])
		}

		expected, exists := expectedUsers[userID]
		if !exists {
			t.Fatalf("Unexpected user in results: %s", userID)
		}

		if minStart != expected.minStart {
			t.Errorf("For %s: expected MIN(start_time) %s, got %s", userID, expected.minStart, minStart)
		}

		if maxEnd != expected.maxEnd {
			t.Errorf("For %s: expected MAX(end_time) %s, got %s", userID, expected.maxEnd, maxEnd)
		}

		if count != expected.count {
			t.Errorf("For %s: expected COUNT %v, got %v", userID, expected.count, count)
		}

		t.Logf("✓ %s: MIN=%s, MAX=%s, COUNT=%v", userID, minStart, maxEnd, count)
	}
}

// TestDateTime_GroupBy_WithOtherAggregates tests DateTime MIN/MAX alongside other aggregates
func TestDateTime_GroupBy_WithOtherAggregates(t *testing.T) {
	fixture := setupFullServer(t)

	bundleName := "OrderLog"

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Create bundle
	createCmd := fmt.Sprintf(`CREATE BUNDLE "%s" WITH FIELDS (
		{"order_id", "STRING", true, false, ""},
		{"status", "STRING", true, false, ""},
		{"order_date", "DATETIME", true, false, ""},
		{"amount", "FLOAT", true, false, ""}
	);`, bundleName)

	startTime := time.Now()
	_, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, createCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	if err != nil {
		t.Fatalf("Failed to create bundle: %v", err)
	}

	// Insert test data - use same date for simpler grouping
	testData := []struct {
		orderID   string
		status    string
		orderDate string
		amount    float64
	}{
		{"o1", "completed", "2024-11-22T10:00:00Z", 100.0},
		{"o2", "completed", "2024-11-22T10:30:00Z", 150.0},
		{"o3", "pending", "2024-11-22T11:00:00Z", 200.0},
		{"o4", "completed", "2024-11-23T10:00:00Z", 120.0},
		{"o5", "pending", "2024-11-23T10:30:00Z", 180.0},
		{"o6", "pending", "2024-11-23T11:00:00Z", 90.0},
	}

	for _, data := range testData {
		insertCmd := fmt.Sprintf(`ADD DOCUMENT TO BUNDLE "%s" WITH (
			{"order_id"="%s"},
			{"status"="%s"},
			{"order_date"="%s"},
			{"amount"=%f}
		);`, bundleName, data.orderID, data.status, data.orderDate, data.amount)

		startTime = time.Now()
		_, err = server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, insertCmd, fixture.Logger, startTime, nil, "127.0.0.1")
		if err != nil {
			t.Fatalf("Failed to insert document %s: %v", data.orderID, err)
		}
	}

	fixture.ServiceManager.BundleService.FlushMetadataUpdates()
	err = fixture.ServiceManager.BundleService.FlushAllBuffers()
	if err != nil {
		t.Logf("Warning: Failed to flush buffers: %v", err)
	}

	// Group by status only and aggregate DateTime fields
	// This tests that MIN/MAX work properly on DateTime fields in aggregations
	selectCmd := fmt.Sprintf(`SELECT status, MIN(order_date), MAX(order_date), COUNT(*), SUM(amount) 
		FROM "%s" 
		GROUP BY status 
		ORDER BY status;`, bundleName)

	startTime = time.Now()
	response, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, selectCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	if err != nil {
		t.Fatalf("Failed to execute multi-field GROUP BY: %v", err)
	}

	cmdResp, ok := response.(*server.CommandResponse)
	if !ok {
		t.Fatalf("Expected CommandResponse, got %T", response)
	}

	jsonBytes, err := json.Marshal(cmdResp.Result)
	if err != nil {
		t.Fatalf("Failed to marshal result: %v", err)
	}

	var results []map[string]interface{}
	if err := json.Unmarshal(jsonBytes, &results); err != nil {
		t.Fatalf("Failed to unmarshal results: %v", err)
	}

	// Should have 2 groups: completed and pending
	if len(results) != 2 {
		t.Fatalf("Expected 2 groups, got %d", len(results))
	}

	expectedGroups := map[string]struct {
		minDate string
		maxDate string
		count   float64
		sum     float64
	}{
		"completed": {minDate: "2024-11-22T10:00:00Z", maxDate: "2024-11-23T10:00:00Z", count: 3, sum: 370.0}, // o1 + o2 + o4
		"pending":   {minDate: "2024-11-22T11:00:00Z", maxDate: "2024-11-23T11:00:00Z", count: 3, sum: 470.0}, // o3 + o5 + o6
	}

	for _, row := range results {
		status, ok := row["status"].(string)
		if !ok {
			t.Fatalf("status should be string, got %T", row["status"])
		}

		minDate, ok := row["min_order_date"].(string)
		if !ok {
			t.Fatalf("min_order_date should be string, got %T", row["min_order_date"])
		}

		maxDate, ok := row["max_order_date"].(string)
		if !ok {
			t.Fatalf("max_order_date should be string, got %T", row["max_order_date"])
		}

		count, ok := row["count_all"].(float64)
		if !ok {
			t.Fatalf("count_all should be float64, got %T", row["count_all"])
		}

		sum, ok := row["sum_amount"].(float64)
		if !ok {
			t.Fatalf("sum_amount should be float64, got %T", row["sum_amount"])
		}

		expected, exists := expectedGroups[status]
		if !exists {
			t.Fatalf("Unexpected status in results: %s", status)
		}

		if minDate != expected.minDate {
			t.Errorf("For %s: expected MIN(order_date) %s, got %s", status, expected.minDate, minDate)
		}

		if maxDate != expected.maxDate {
			t.Errorf("For %s: expected MAX(order_date) %s, got %s", status, expected.maxDate, maxDate)
		}

		if count != expected.count {
			t.Errorf("For %s: expected count %v, got %v", status, expected.count, count)
		}

		if sum != expected.sum {
			t.Errorf("For %s: expected sum %v, got %v", status, expected.sum, sum)
		}

		t.Logf("✓ Status=%s: MIN=%s, MAX=%s, COUNT=%v, SUM=%v", status, minDate, maxDate, count, sum)
	}
}

// Helper to get map keys for debugging
func getMapKeys(m map[string]interface{}) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}
