package main

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"syndrdb/src/internal/server"
)

// TestDateTime_WHERE_Debug tests WHERE clause with DateTime to debug filtering
func TestDateTime_WHERE_Debug(t *testing.T) {
	fixture := setupTestFixture(t)
	defer teardownTestFixture(t, fixture)

	bundleName := "DebugEventsBundle"

	// Create bundle
	createCmd := fmt.Sprintf(`CREATE BUNDLE "%s" WITH FIELDS (
		{"eventId", "String", true, false},
		{"eventTime", "DateTime", true, false}
	);`, bundleName)

	startTime := time.Now()
	_, err := server.CommandDirector(context.Background(), &fixture.Database, *fixture.ServiceManager, createCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	if err != nil {
		t.Fatalf("Failed to create bundle: %v", err)
	}

	// Insert test data
	insertCmd1 := fmt.Sprintf(`ADD DOCUMENT TO BUNDLE "%s" WITH (
		{"eventId"="evt-100"},
		{"eventTime"="2024-11-20T10:00:00Z"}
	);`, bundleName)

	insertCmd2 := fmt.Sprintf(`ADD DOCUMENT TO BUNDLE "%s" WITH (
		{"eventId"="evt-103"},
		{"eventTime"="2024-11-23T10:00:00Z"}
	);`, bundleName)

	insertCmd3 := fmt.Sprintf(`ADD DOCUMENT TO BUNDLE "%s" WITH (
		{"eventId"="evt-104"},
		{"eventTime"="2024-11-24T10:00:00Z"}
	);`, bundleName)

	for _, cmd := range []string{insertCmd1, insertCmd2, insertCmd3} {
		startTime := time.Now()
		_, err := server.CommandDirector(context.Background(), &fixture.Database, *fixture.ServiceManager, cmd, fixture.Logger, startTime, nil, "127.0.0.1")
		if err != nil {
			t.Fatalf("Failed to insert document: %v", err)
		}
	}

	// Test: SELECT without WHERE (should return all 3)
	t.Run("SELECT all documents", func(t *testing.T) {
		selectCmd := fmt.Sprintf(`SELECT * FROM "%s" ORDER BY "eventId";`, bundleName)
		fixture.Logger.Infof("DEBUG: Executing query: %s", selectCmd)

		startTime := time.Now()
		response, err := server.CommandDirector(context.Background(), &fixture.Database, *fixture.ServiceManager, selectCmd, fixture.Logger, startTime, nil, "127.0.0.1")
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

		t.Logf("DEBUG: Retrieved %d documents without WHERE", len(documents))
		for _, doc := range documents {
			t.Logf("DEBUG: Doc eventId=%v, eventTime=%v", doc["eventId"], doc["eventTime"])
		}

		if len(documents) != 3 {
			t.Errorf("Expected 3 documents, got %d", len(documents))
		}
	})

	// Test: SELECT with WHERE (should return 2)
	t.Run("SELECT with WHERE eventTime > date", func(t *testing.T) {
		selectCmd := fmt.Sprintf(`SELECT * FROM "%s" WHERE "eventTime" > "2024-11-22T10:00:00Z" ORDER BY "eventId";`, bundleName)
		fixture.Logger.Infof("DEBUG: Executing query: %s", selectCmd)

		startTime := time.Now()
		response, err := server.CommandDirector(context.Background(), &fixture.Database, *fixture.ServiceManager, selectCmd, fixture.Logger, startTime, nil, "127.0.0.1")
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

		t.Logf("DEBUG: Retrieved %d documents with WHERE clause", len(documents))
		for _, doc := range documents {
			t.Logf("DEBUG: Doc eventId=%v, eventTime=%v", doc["eventId"], doc["eventTime"])
		}

		if len(documents) != 2 {
			t.Errorf("Expected 2 documents (evt-103, evt-104), got %d", len(documents))
		}
	})
}
