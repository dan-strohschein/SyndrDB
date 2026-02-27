package syndrQL

import (
	"context"
	"encoding/json"
	"math"
	"syndrdb/src/internal/server"
	"testing"
	"time"
)

func executeMathQuery(t *testing.T, fixture *TestFixture, query string) []map[string]interface{} {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	response, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, query, fixture.Logger, time.Now(), nil, "127.0.0.1")
	if err != nil {
		t.Fatalf("Failed to execute %q: %v", query, err)
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
	return results
}

func getFloat64(t *testing.T, results []map[string]interface{}, col string) float64 {
	t.Helper()
	if len(results) == 0 {
		t.Fatalf("No results")
	}
	v, ok := results[0][col]
	if !ok {
		t.Fatalf("Column %q not found, got %v", col, results[0])
	}
	f, ok := v.(float64)
	if !ok {
		t.Fatalf("Expected float64 for %q, got %T: %v", col, v, v)
	}
	return f
}

func TestMathE2E_POWER(t *testing.T) {
	fixture := setupFullServer(t)

	results := executeMathQuery(t, fixture, "SELECT F:POWER(2, 10);")
	v := getFloat64(t, results, "column1")
	if v != 1024.0 {
		t.Errorf("POWER(2, 10) = %v, want 1024.0", v)
	}
}

func TestMathE2E_POWER_ZeroExponent(t *testing.T) {
	fixture := setupFullServer(t)

	results := executeMathQuery(t, fixture, "SELECT F:POWER(5, 0);")
	v := getFloat64(t, results, "column1")
	if v != 1.0 {
		t.Errorf("POWER(5, 0) = %v, want 1.0", v)
	}
}

func TestMathE2E_SQRT(t *testing.T) {
	fixture := setupFullServer(t)

	results := executeMathQuery(t, fixture, "SELECT F:SQRT(144);")
	v := getFloat64(t, results, "column1")
	if v != 12.0 {
		t.Errorf("SQRT(144) = %v, want 12.0", v)
	}
}

func TestMathE2E_MOD(t *testing.T) {
	fixture := setupFullServer(t)

	results := executeMathQuery(t, fixture, "SELECT F:MOD(17, 5);")
	v := getFloat64(t, results, "column1")
	if v != 2.0 {
		t.Errorf("MOD(17, 5) = %v, want 2", v)
	}
}

func TestMathE2E_LOG_TwoArgs(t *testing.T) {
	fixture := setupFullServer(t)

	results := executeMathQuery(t, fixture, "SELECT F:LOG(10, 1000);")
	v := getFloat64(t, results, "column1")
	if math.Abs(v-3.0) > 0.001 {
		t.Errorf("LOG(10, 1000) = %v, want ~3.0", v)
	}
}

func TestMathE2E_LOG_NaturalLog(t *testing.T) {
	fixture := setupFullServer(t)

	results := executeMathQuery(t, fixture, "SELECT F:LOG(1);")
	v := getFloat64(t, results, "column1")
	if v != 0.0 {
		t.Errorf("LOG(1) = %v, want 0.0", v)
	}
}

func TestMathE2E_SIGN(t *testing.T) {
	fixture := setupFullServer(t)

	results := executeMathQuery(t, fixture, "SELECT F:SIGN(100);")
	v := getFloat64(t, results, "column1")
	if v != 1 {
		t.Errorf("SIGN(100) = %v, want 1", v)
	}

	results = executeMathQuery(t, fixture, "SELECT F:SIGN(0);")
	v = getFloat64(t, results, "column1")
	if v != 0 {
		t.Errorf("SIGN(0) = %v, want 0", v)
	}
}
