package syndrQL

import (
	"context"
	"encoding/json"
	"syndrdb/src/internal/server"
	"testing"
	"time"
)

// TestSelectExpressionOnly_E2E_SimpleLiteral tests SELECT with a simple literal
func TestSelectExpressionOnly_E2E_SimpleLiteral(t *testing.T) {
	fixture := setupFullServer(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	selectCmd := "SELECT 1;"
	startTime := time.Now()
	response, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, selectCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	if err != nil {
		t.Fatalf("Failed to execute SELECT 1: %v", err)
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

	value, ok := results[0]["column1"]
	if !ok {
		t.Fatalf("Expected column1, result: %v", results[0])
	}

	numValue, ok := value.(float64)
	if !ok {
		t.Fatalf("Expected numeric value, got %T: %v", value, value)
	}

	if numValue != 1 {
		t.Errorf("Expected value 1, got %v", numValue)
	}

	t.Log("✓ SELECT 1 returned column1=1")
}
