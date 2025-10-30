package server

import (
	"syndrdb/src/pkg/settings"
	"testing"

	"go.uber.org/zap"
)

// TestShouldUseNewParser tests the feature flag check
func TestShouldUseNewParser(t *testing.T) {
	// Save original value
	originalValue := settings.GetSettings().UseNewParser
	defer func() {
		settings.GetSettings().UseNewParser = originalValue
	}()

	// Test with flag OFF
	settings.GetSettings().UseNewParser = false
	if shouldUseNewParser() {
		t.Error("Expected shouldUseNewParser to return false when flag is OFF")
	}

	// Test with flag ON
	settings.GetSettings().UseNewParser = true
	if !shouldUseNewParser() {
		t.Error("Expected shouldUseNewParser to return true when flag is ON")
	}
}

// TestParseQueryWithLegacyParser tests that legacy parser is used when flag is OFF
func TestParseQueryWithLegacyParser(t *testing.T) {
	// Save original value
	originalValue := settings.GetSettings().UseNewParser
	defer func() {
		settings.GetSettings().UseNewParser = originalValue
	}()

	// Set flag to OFF
	settings.GetSettings().UseNewParser = false

	logger := zap.NewNop().Sugar()
	query := "SELECT * FROM users WHERE age > 18"

	// Reset metrics
	globalParserMetrics = &ParserMetrics{}

	result, err := parseQuery(query, logger)

	if err != nil {
		t.Fatalf("Expected no error with legacy parser, got: %v", err)
	}

	if result == nil {
		t.Fatal("Expected non-nil result")
	}

	// Verify metrics show no new parser attempts
	metrics := GetParserMetrics()
	if metrics["new_parser_attempts"] != 0 {
		t.Errorf("Expected 0 new parser attempts with flag OFF, got: %d", metrics["new_parser_attempts"])
	}
}

// TestParseQueryWithNewParser tests that new parser is used when flag is ON
func TestParseQueryWithNewParser(t *testing.T) {
	// Save original value
	originalValue := settings.GetSettings().UseNewParser
	defer func() {
		settings.GetSettings().UseNewParser = originalValue
	}()

	// Set flag to ON
	settings.GetSettings().UseNewParser = true

	logger := zap.NewNop().Sugar()
	query := "SELECT * FROM users WHERE age > 18"

	// Reset metrics
	globalParserMetrics = &ParserMetrics{}

	result, err := parseQuery(query, logger)

	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

	if result == nil {
		t.Fatal("Expected non-nil result")
	}

	// Verify metrics show new parser attempt
	metrics := GetParserMetrics()
	if metrics["new_parser_attempts"] != 1 {
		t.Errorf("Expected 1 new parser attempt with flag ON, got: %d", metrics["new_parser_attempts"])
	}

	// Either new parser succeeded OR fallback was triggered (both are valid outcomes)
	if metrics["new_parser_successes"]+metrics["fallbacks_triggered"] != 1 {
		t.Errorf("Expected either 1 success or 1 fallback, got successes=%d, fallbacks=%d",
			metrics["new_parser_successes"], metrics["fallbacks_triggered"])
	}
}

// TestParseQueryFallback tests that fallback to legacy parser works
func TestParseQueryFallback(t *testing.T) {
	// Save original value
	originalValue := settings.GetSettings().UseNewParser
	defer func() {
		settings.GetSettings().UseNewParser = originalValue
	}()

	// Set flag to ON
	settings.GetSettings().UseNewParser = true

	logger := zap.NewNop().Sugar()

	// Use a query that the new parser might not support yet
	// (This is just to test fallback mechanism)
	query := "SELECT * FROM users"

	// Reset metrics
	globalParserMetrics = &ParserMetrics{}

	result, err := parseQuery(query, logger)

	// Result should still work (via fallback)
	if err != nil {
		t.Fatalf("Expected no error (fallback should work), got: %v", err)
	}

	if result == nil {
		t.Fatal("Expected non-nil result from fallback")
	}

	// Verify metrics
	metrics := GetParserMetrics()
	if metrics["new_parser_attempts"] < 1 {
		t.Errorf("Expected at least 1 new parser attempt, got: %d", metrics["new_parser_attempts"])
	}
}

// TestGetParserMetrics tests the metrics getter function
func TestGetParserMetrics(t *testing.T) {
	// Reset metrics
	globalParserMetrics = &ParserMetrics{}

	// Set some values
	globalParserMetrics.NewParserAttempts.Add(5)
	globalParserMetrics.NewParserSuccesses.Add(3)
	globalParserMetrics.NewParserFailures.Add(2)
	globalParserMetrics.FallbacksTriggered.Add(2)

	metrics := GetParserMetrics()

	if metrics["new_parser_attempts"] != 5 {
		t.Errorf("Expected 5 attempts, got: %d", metrics["new_parser_attempts"])
	}
	if metrics["new_parser_successes"] != 3 {
		t.Errorf("Expected 3 successes, got: %d", metrics["new_parser_successes"])
	}
	if metrics["new_parser_failures"] != 2 {
		t.Errorf("Expected 2 failures, got: %d", metrics["new_parser_failures"])
	}
	if metrics["fallbacks_triggered"] != 2 {
		t.Errorf("Expected 2 fallbacks, got: %d", metrics["fallbacks_triggered"])
	}
}

// BenchmarkParseQueryLegacy benchmarks legacy parser
func BenchmarkParseQueryLegacy(b *testing.B) {
	settings.GetSettings().UseNewParser = false
	logger := zap.NewNop().Sugar()
	query := "SELECT * FROM users WHERE age > 18 AND active == true"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = parseQuery(query, logger)
	}
}

// BenchmarkParseQueryNew benchmarks new parser
func BenchmarkParseQueryNew(b *testing.B) {
	settings.GetSettings().UseNewParser = true
	logger := zap.NewNop().Sugar()
	query := "SELECT * FROM users WHERE age > 18 AND active == true"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = parseQuery(query, logger)
	}
}
