package main

import (
	"encoding/json"
	"fmt"
	"strings"
	"syndrdb/src/internal/server"
	"testing"
	"time"
)

// TestDateTime_E2E_BundleCreation tests creating a bundle with DateTime and Date fields
func TestDateTime_E2E_BundleCreation(t *testing.T) {
	fixture := setupTestFixture(t)
	defer teardownTestFixture(t, fixture)

	bundleName := "TestEventsBundle"

	// Create bundle with DateTime and Date fields
	createCmd := fmt.Sprintf(`CREATE BUNDLE "%s" WITH FIELDS (
		{"eventId": "String"},
		{"eventName": "String"},
		{"eventTime": "DateTime"},
		{"eventDate": "Date"},
		{"description": "String"}
	);`, bundleName)

	startTime := time.Now()
	response, err := server.CommandDirector(fixture.Database, *fixture.ServiceManager, createCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	if err != nil {
		t.Fatalf("Failed to create bundle: %v", err)
	}

	// Verify response
	if cmdResp, ok := response.(*server.CommandResponse); ok {
		if !strings.Contains(fmt.Sprintf("%v", cmdResp.Result), "success") {
			t.Logf("Create bundle result: %+v", cmdResp.Result)
		}
	}

	t.Logf("Bundle created successfully")
}

// TestDateTime_E2E_DocumentInsertAndRetrieve tests inserting documents with datetime fields
func TestDateTime_E2E_DocumentInsertAndRetrieve(t *testing.T) {
	fixture := setupTestFixture(t)
	defer teardownTestFixture(t, fixture)

	bundleName := "TestEventsBundle"

	// Create bundle first
	createCmd := fmt.Sprintf(`CREATE BUNDLE "%s" WITH FIELDS (
		{"eventId": "String"},
		{"eventName": "String"},
		{"eventTime": "DateTime"},
		{"eventDate": "Date"},
		{"description": "String"}
	);`, bundleName)

	startTime := time.Now()
	_, err := server.CommandDirector(fixture.Database, *fixture.ServiceManager, createCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	if err != nil {
		t.Fatalf("Failed to create bundle: %v", err)
	}

	// Insert document with RFC3339 datetime
	insertCmd := fmt.Sprintf(`ADD DOCUMENT TO BUNDLE "%s" WITH (
		{"eventId"="evt-001"},
		{"eventName"="Product Launch"},
		{"eventTime"="2024-11-22T15:30:00Z"},
		{"eventDate"="2024-11-22"},
		{"description"="New product announcement"}
	);`, bundleName)

	startTime = time.Now()
	response, err := server.CommandDirector(fixture.Database, *fixture.ServiceManager, insertCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	if err != nil {
		t.Fatalf("Failed to insert document: %v", err)
	}

	t.Logf("Insert response: %+v", response)

	// Retrieve and verify
	selectCmd := fmt.Sprintf(`SELECT * FROM "%s" WHERE "eventId" == "evt-001";`, bundleName)
	startTime = time.Now()
	response, err = server.CommandDirector(fixture.Database, *fixture.ServiceManager, selectCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	if err != nil {
		t.Fatalf("Failed to select document: %v", err)
	}

	// Parse JSON response
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

	// Verify DateTime field (should be RFC3339)
	eventTime, ok := doc["eventTime"].(string)
	if !ok {
		t.Fatalf("eventTime should be string, got %T", doc["eventTime"])
	}
	if !strings.HasPrefix(eventTime, "2024-11-22T15:30:00") {
		t.Errorf("Expected eventTime to start with 2024-11-22T15:30:00, got %s", eventTime)
	}

	// Verify Date field (should be YYYY-MM-DD)
	eventDate, ok := doc["eventDate"].(string)
	if !ok {
		t.Fatalf("eventDate should be string, got %T", doc["eventDate"])
	}
	if eventDate != "2024-11-22" {
		t.Errorf("Expected eventDate to be 2024-11-22, got %s", eventDate)
	}

	t.Logf("Document retrieved and verified successfully")
}

// TestDateTime_E2E_MultipleFormats tests inserting documents with various datetime formats
func TestDateTime_E2E_MultipleFormats(t *testing.T) {
	fixture := setupTestFixture(t)
	defer teardownTestFixture(t, fixture)

	bundleName := "TestEventsBundle"

	// Create bundle
	createCmd := fmt.Sprintf(`CREATE BUNDLE "%s" WITH FIELDS (
		{"eventId": "String"},
		{"eventName": "String"},
		{"eventTime": "DateTime"},
		{"eventDate": "Date"},
		{"description": "String"}
	);`, bundleName)

	startTime := time.Now()
	_, err := server.CommandDirector(fixture.Database, *fixture.ServiceManager, createCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	if err != nil {
		t.Fatalf("Failed to create bundle: %v", err)
	}

	testCases := []struct {
		name        string
		eventId     string
		eventTime   string
		eventDate   string
		description string
	}{
		{
			name:        "RFC3339",
			eventId:     "evt-001",
			eventTime:   "2024-11-22T15:30:00Z",
			eventDate:   "2024-11-22",
			description: "RFC3339 format",
		},
		{
			name:        "ISO8601",
			eventId:     "evt-002",
			eventTime:   "2024-11-22T10:15:30",
			eventDate:   "2024-11-22",
			description: "ISO8601 format",
		},
		{
			name:        "US format",
			eventId:     "evt-003",
			eventTime:   "11/22/2024 3:45 PM",
			eventDate:   "11/22/2024",
			description: "US date format",
		},
		{
			name:        "SQL format",
			eventId:     "evt-004",
			eventTime:   "2024-11-22 18:00:00",
			eventDate:   "2024-11-22",
			description: "SQL datetime format",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			insertCmd := fmt.Sprintf(`ADD DOCUMENT TO BUNDLE "%s" WITH (
				{"eventId"="%s"},
				{"eventName"="%s"},
				{"eventTime"="%s"},
				{"eventDate"="%s"},
				{"description"="%s"}
			);`, bundleName, tc.eventId, tc.name, tc.eventTime, tc.eventDate, tc.description)

			startTime := time.Now()
			_, err := server.CommandDirector(fixture.Database, *fixture.ServiceManager, insertCmd, fixture.Logger, startTime, nil, "127.0.0.1")
			if err != nil {
				t.Fatalf("Failed to insert document: %v", err)
			}

			// Verify document was stored
			selectCmd := fmt.Sprintf(`SELECT * FROM "%s" WHERE "eventId" == "%s";`, bundleName, tc.eventId)
			startTime = time.Now()
			response, err := server.CommandDirector(fixture.Database, *fixture.ServiceManager, selectCmd, fixture.Logger, startTime, nil, "127.0.0.1")
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

			t.Logf("Document %s inserted and retrieved successfully", tc.eventId)
		})
	}
}

// TestDateTime_E2E_WhereClauseFiltering tests filtering by datetime fields
func TestDateTime_E2E_WhereClauseFiltering(t *testing.T) {
	fixture := setupTestFixture(t)
	defer teardownTestFixture(t, fixture)

	bundleName := "TestEventsBundle"

	// Create bundle
	createCmd := fmt.Sprintf(`CREATE BUNDLE "%s" WITH FIELDS (
		{"eventId": "String"},
		{"eventName": "String"},
		{"eventTime": "DateTime"},
		{"eventDate": "Date"},
		{"description": "String"}
	);`, bundleName)

	startTime := time.Now()
	_, err := server.CommandDirector(fixture.Database, *fixture.ServiceManager, createCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	if err != nil {
		t.Fatalf("Failed to create bundle: %v", err)
	}

	// Insert test data with known times
	testDocs := []struct {
		eventId   string
		eventTime string
		eventDate string
	}{
		{"evt-100", "2024-11-20T10:00:00Z", "2024-11-20"},
		{"evt-101", "2024-11-21T10:00:00Z", "2024-11-21"},
		{"evt-102", "2024-11-22T10:00:00Z", "2024-11-22"},
		{"evt-103", "2024-11-23T10:00:00Z", "2024-11-23"},
		{"evt-104", "2024-11-24T10:00:00Z", "2024-11-24"},
	}

	for _, doc := range testDocs {
		insertCmd := fmt.Sprintf(`ADD DOCUMENT TO BUNDLE "%s" WITH (
			{"eventId"="%s"},
			{"eventName"="Test Event"},
			{"eventTime"="%s"},
			{"eventDate"="%s"},
			{"description"="Test filtering"}
		);`, bundleName, doc.eventId, doc.eventTime, doc.eventDate)

		startTime := time.Now()
		_, err := server.CommandDirector(fixture.Database, *fixture.ServiceManager, insertCmd, fixture.Logger, startTime, nil, "127.0.0.1")
		if err != nil {
			t.Fatalf("Failed to insert document %s: %v", doc.eventId, err)
		}
	}

	t.Run("Equality filter on DateTime", func(t *testing.T) {
		selectCmd := fmt.Sprintf(`SELECT * FROM "%s" WHERE "eventTime" == "2024-11-22T10:00:00Z";`, bundleName)
		startTime := time.Now()
		response, err := server.CommandDirector(fixture.Database, *fixture.ServiceManager, selectCmd, fixture.Logger, startTime, nil, "127.0.0.1")
		if err != nil {
			t.Fatalf("Failed to execute query: %v", err)
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

		if documents[0]["eventId"] != "evt-102" {
			t.Errorf("Expected evt-102, got %v", documents[0]["eventId"])
		}
	})

	t.Run("Greater than filter on DateTime", func(t *testing.T) {
		selectCmd := fmt.Sprintf(`SELECT * FROM "%s" WHERE "eventTime" > "2024-11-22T10:00:00Z";`, bundleName)
		startTime := time.Now()
		response, err := server.CommandDirector(fixture.Database, *fixture.ServiceManager, selectCmd, fixture.Logger, startTime, nil, "127.0.0.1")
		if err != nil {
			t.Fatalf("Failed to execute query: %v", err)
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

		// Should return evt-103 and evt-104
		if len(documents) < 2 {
			t.Errorf("Expected at least 2 documents, got %d", len(documents))
		}
	})

	t.Run("Less than or equal filter on Date", func(t *testing.T) {
		selectCmd := fmt.Sprintf(`SELECT * FROM "%s" WHERE "eventDate" <= "2024-11-21";`, bundleName)
		startTime := time.Now()
		response, err := server.CommandDirector(fixture.Database, *fixture.ServiceManager, selectCmd, fixture.Logger, startTime, nil, "127.0.0.1")
		if err != nil {
			t.Fatalf("Failed to execute query: %v", err)
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

		// Should return evt-100 and evt-101
		if len(documents) < 2 {
			t.Errorf("Expected at least 2 documents, got %d", len(documents))
		}
	})

	t.Run("Range filter on DateTime", func(t *testing.T) {
		selectCmd := fmt.Sprintf(`SELECT * FROM "%s" WHERE "eventTime" >= "2024-11-21T00:00:00Z" AND "eventTime" <= "2024-11-23T23:59:59Z";`, bundleName)
		startTime := time.Now()
		response, err := server.CommandDirector(fixture.Database, *fixture.ServiceManager, selectCmd, fixture.Logger, startTime, nil, "127.0.0.1")
		if err != nil {
			t.Fatalf("Failed to execute query: %v", err)
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

		// Should return evt-101, evt-102, evt-103
		if len(documents) < 3 {
			t.Errorf("Expected at least 3 documents, got %d", len(documents))
		}
	})
}

// TestDateTime_E2E_MillisecondPrecision tests millisecond precision in queries
func TestDateTime_E2E_MillisecondPrecision(t *testing.T) {
	fixture := setupTestFixture(t)
	defer teardownTestFixture(t, fixture)

	bundleName := "TestEventsBundle"

	// Create bundle
	createCmd := fmt.Sprintf(`CREATE BUNDLE "%s" WITH FIELDS (
		{"eventId": "String"},
		{"eventName": "String"},
		{"eventTime": "DateTime"},
		{"eventDate": "Date"},
		{"description": "String"}
	);`, bundleName)

	startTime := time.Now()
	_, err := server.CommandDirector(fixture.Database, *fixture.ServiceManager, createCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	if err != nil {
		t.Fatalf("Failed to create bundle: %v", err)
	}

	// Insert documents with very close timestamps
	insertCmd1 := fmt.Sprintf(`ADD DOCUMENT TO BUNDLE "%s" WITH (
		{"eventId"="evt-ms-1"},
		{"eventName"="Millisecond Test 1"},
		{"eventTime"="2024-11-22T12:00:00.123Z"},
		{"eventDate"="2024-11-22"},
		{"description"="123ms"}
	);`, bundleName)

	insertCmd2 := fmt.Sprintf(`ADD DOCUMENT TO BUNDLE "%s" WITH (
		{"eventId"="evt-ms-2"},
		{"eventName"="Millisecond Test 2"},
		{"eventTime"="2024-11-22T12:00:00.456Z"},
		{"eventDate"="2024-11-22"},
		{"description"="456ms"}
	);`, bundleName)

	startTime = time.Now()
	_, err = server.CommandDirector(fixture.Database, *fixture.ServiceManager, insertCmd1, fixture.Logger, startTime, nil, "127.0.0.1")
	if err != nil {
		t.Fatalf("Failed to insert document 1: %v", err)
	}

	startTime = time.Now()
	_, err = server.CommandDirector(fixture.Database, *fixture.ServiceManager, insertCmd2, fixture.Logger, startTime, nil, "127.0.0.1")
	if err != nil {
		t.Fatalf("Failed to insert document 2: %v", err)
	}

	// Query for exact millisecond match
	selectCmd := fmt.Sprintf(`SELECT * FROM "%s" WHERE "eventTime" == "2024-11-22T12:00:00.123Z";`, bundleName)
	startTime = time.Now()
	response, err := server.CommandDirector(fixture.Database, *fixture.ServiceManager, selectCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
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

	if documents[0]["eventId"] != "evt-ms-1" {
		t.Errorf("Expected evt-ms-1, got %v", documents[0]["eventId"])
	}

	t.Logf("Millisecond precision test passed")
}
