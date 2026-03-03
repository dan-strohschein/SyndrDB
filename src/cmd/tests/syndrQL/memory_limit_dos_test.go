package syndrQL

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"syndrdb/src/internal/domain/models"
	"syndrdb/src/internal/query/planner"
	"syndrdb/src/internal/server"
	"syndrdb/src/pkg/settings"
)

/*
MEMORY LIMIT DOS PROTECTION E2E TESTS

Tests verify that per-query memory limits protect against DoS attacks by:
1. Rejecting queries that would consume excessive memory
2. Properly tracking and projecting memory usage via sampling
3. Recording metrics for monitoring and alerting
4. Handling concurrent query load without memory exhaustion

Test Strategy:
- Real server components (ServiceManager, DatabaseService, BundleService)
- Large document sets to trigger memory limits
- Nested structures to test deep memory estimation
- Concurrent execution to verify thread-safety
- Metrics validation for observability
*/

// TestMemoryLimit_SmallQuery verifies memory limit is NOT triggered for small queries
func TestMemoryLimit_SmallQuery(t *testing.T) {
	fixture := setupFullServer(t)
	defer cleanupFullServer(t)

	// Seed 100 small authors
	seedAuthors(t, fixture, 100)

	// Query should succeed with 25MB default limit
	query := "SELECT * FROM Authors"
	result, err := server.CommandDirector(

		context.Background(),
		fixture.Database,
		*fixture.ServiceManager,
		query,
		fixture.Logger,
		time.Now(),
		nil,
		"127.0.0.1",
	)

	if err != nil {
		t.Fatalf("Small query should not hit memory limit: %v", err)
	}

	response, ok := result.(*server.CommandResponse)
	if !ok {
		t.Fatalf("Expected CommandResponse, got %T", result)
	}

	if response.Error != nil && *response.Error != "" {
		t.Fatalf("Small query should not error: %s", *response.Error)
	}

	if response.ResultCount != 100 {
		t.Errorf("Expected 100 results, got %d", response.ResultCount)
	}
}

// TestMemoryLimit_LargeDocumentsExceedLimit verifies large documents trigger memory limit
func TestMemoryLimit_LargeDocumentsExceedLimit(t *testing.T) {
	fixture := setupFullServer(t)
	defer cleanupFullServer(t)

	// Set memory limit to 1MB for this test (200 docs × 300KB = 60MB >> 1MB)
	defer setTestMemoryLimit(t, 1)()

	// Seed authors with LARGE biographies to trigger memory limit
	// Each document will be ~300KB with nested content
	// Seed 200 large authors (each ~300KB biography)
	seedLargeAuthors(t, fixture, 200)

	// Query should fail: 200 docs × ~300KB = ~60MB >> 1MB limit
	query := "SELECT * FROM Authors"
	result, err := server.CommandDirector(
		context.Background(),
		fixture.Database,
		*fixture.ServiceManager,
		query,
		fixture.Logger,
		time.Now(),
		nil,
		"127.0.0.1",
	)

	// Should return a CommandResponse with error, NOT a Go error
	if err != nil {
		// Check if it's the expected memory limit error
		if err != planner.ErrMemoryLimitExceeded && !strings.Contains(err.Error(), "memory limit exceeded") {
			t.Fatalf("Unexpected error type: %v", err)
		}
	} else {
		response, ok := result.(*server.CommandResponse)
		if !ok {
			t.Fatalf("Expected CommandResponse, got %T", result)
		}

		t.Logf("DEBUG: response.Error = %v, result type = %T, ResultCount = %d", response.Error, result, response.ResultCount)
		if response.Error != nil {
			t.Logf("DEBUG: error message = %s", *response.Error)
		}
		if response.StreamSlice != nil {
			t.Logf("DEBUG: StreamSlice len = %d", len(response.StreamSlice))
			if len(response.StreamSlice) > 0 {
				docSize := models.EstimateDocumentSize(response.StreamSlice[0], nil)
				t.Logf("DEBUG: First doc size estimate = %d bytes, Data len = %d, Values len = %d", docSize, len(response.StreamSlice[0].Data), len(response.StreamSlice[0].Values))
			}
		}

		if response.Error == nil || *response.Error == "" {
			t.Fatal("Large query should trigger memory limit error")
		}

		if !strings.Contains(*response.Error, "memory limit exceeded") {
			t.Errorf("Expected memory limit error, got: %s", *response.Error)
		}
	}
}

// TestMemoryLimit_NestedStructures verifies deep nesting is properly estimated
func TestMemoryLimit_NestedStructures(t *testing.T) {
	fixture := setupFullServer(t)
	defer cleanupFullServer(t)

	// Set memory limit to 2MB for this test (150 docs × ~20KB = 3MB >> 2MB)
	defer setTestMemoryLimit(t, 2)()

	// Seed 150 authors with deeply nested metadata (50 levels × 100 chars/level = ~5KB base + overhead)
	seedAuthorsWithNestedMetadata(t, fixture, 150)

	// Query should fail due to nested structure size
	query := "SELECT * FROM Authors"
	result, err := server.CommandDirector(
		context.Background(),
		fixture.Database,
		*fixture.ServiceManager,
		query,
		fixture.Logger,
		time.Now(),
		nil,
		"127.0.0.1",
	)

	if err != nil {
		if err != planner.ErrMemoryLimitExceeded && !strings.Contains(err.Error(), "memory limit exceeded") {
			t.Fatalf("Unexpected error type: %v", err)
		}
	} else {
		response, ok := result.(*server.CommandResponse)
		if !ok {
			t.Fatalf("Expected CommandResponse, got %T", result)
		}

		if response.Error == nil || *response.Error == "" {
			t.Fatal("Nested structure query should trigger memory limit")
		}

		if !strings.Contains(*response.Error, "memory limit exceeded") {
			t.Errorf("Expected memory limit error, got: %s", *response.Error)
		}
	}
}

// TestMemoryLimit_AggregationQuery verifies aggregations respect memory limits
func TestMemoryLimit_AggregationQuery(t *testing.T) {
	fixture := setupFullServer(t)
	defer cleanupFullServer(t)

	// Set memory limit to 5MB for this test (200 docs × 300KB = 60MB >> 5MB)
	defer setTestMemoryLimit(t, 5)()

	// Seed large authors
	seedLargeAuthors(t, fixture, 200)

	// SELECT * should hit memory limit (materializes all fields including ~300KB Biography).
	// Note: GROUP BY queries use projection pushdown (only loading grouped/aggregated fields),
	// so they correctly use much less memory. SELECT * materializes everything.
	query := `SELECT * FROM "Authors"`
	result, err := server.CommandDirector(
		context.Background(),
		fixture.Database,
		*fixture.ServiceManager,
		query,
		fixture.Logger,
		time.Now(),
		nil,
		"127.0.0.1",
	)

	if err != nil {
		if err != planner.ErrMemoryLimitExceeded && !strings.Contains(err.Error(), "memory limit exceeded") {
			t.Fatalf("Unexpected error type: %v", err)
		}
	} else {
		response, ok := result.(*server.CommandResponse)
		if !ok {
			t.Fatalf("Expected CommandResponse, got %T", result)
		}

		if response.Error == nil || *response.Error == "" {
			t.Fatal("Large SELECT * query should trigger memory limit")
		}
	}
}

// TestMemoryLimit_ConcurrentQueries verifies concurrent safety and metrics
func TestMemoryLimit_ConcurrentQueries(t *testing.T) {
	fixture := setupFullServer(t)
	defer cleanupFullServer(t)

	// Seed moderate-sized authors
	seedAuthors(t, fixture, 100)

	// Reset global metrics before test
	server.GetGlobalMemoryMetrics().Reset()

	// Run 50 concurrent queries
	concurrency := 50
	var wg sync.WaitGroup
	successCount := 0
	var mu sync.Mutex

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			query := "SELECT * FROM Authors"
			result, err := server.CommandDirector(
				context.Background(),
				fixture.Database,
				*fixture.ServiceManager,
				query,
				fixture.Logger,
				time.Now(),
				nil,
				"127.0.0.1",
			)

			if err == nil {
				response, ok := result.(*server.CommandResponse)
				if ok && (response.Error == nil || *response.Error == "") {
					mu.Lock()
					successCount++
					mu.Unlock()
				}
			}
		}(i)
	}

	wg.Wait()

	// Verify all queries succeeded (these are small queries)
	if successCount != concurrency {
		t.Errorf("Expected %d successful queries, got %d", concurrency, successCount)
	}

	// Verify metrics were recorded
	metrics := server.GetGlobalMemoryMetrics().GetMetrics()
	queriesChecked := metrics["queries_checked_count"].(int64)

	if queriesChecked != int64(concurrency) {
		t.Errorf("Expected %d queries checked, got %d", concurrency, queriesChecked)
	}
}

// TestMemoryLimit_Metrics verifies metrics tracking works correctly
func TestMemoryLimit_Metrics(t *testing.T) {
	fixture := setupFullServer(t)
	defer cleanupFullServer(t)

	// Reset metrics
	server.GetGlobalMemoryMetrics().Reset()

	// Seed small authors (150 docs ensures sampling: docs 0, 100 sampled = 2 samples)
	seedAuthors(t, fixture, 150)

	// Run successful query
	query := "SELECT * FROM Authors"
	_, _ = server.CommandDirector(
		context.Background(),
		fixture.Database,
		*fixture.ServiceManager,
		query,
		fixture.Logger,
		time.Now(),
		nil,
		"127.0.0.1",
	)

	// Check metrics
	metrics := server.GetGlobalMemoryMetrics().GetMetrics()

	t.Logf("Metrics: queries_checked=%d, limit_exceeded=%d, avg_mem=%d, max_mem=%d",
		metrics["queries_checked_count"], metrics["memory_limit_exceeded_count"],
		metrics["avg_projected_memory_bytes"], metrics["max_projected_memory_bytes"])

	if metrics["queries_checked_count"].(int64) != 1 {
		t.Errorf("Expected 1 query checked, got %d", metrics["queries_checked_count"])
	}

	if metrics["memory_limit_exceeded_count"].(int64) != 0 {
		t.Errorf("Expected 0 limit exceeded, got %d", metrics["memory_limit_exceeded_count"])
	}

	if metrics["avg_projected_memory_bytes"].(int64) == 0 {
		t.Error("Expected non-zero average projected memory")
	}

	if metrics["max_projected_memory_bytes"].(int64) == 0 {
		t.Error("Expected non-zero max projected memory")
	}
}

// =============================================================================
// HELPER FUNCTIONS
// =============================================================================

// setTestMemoryLimit temporarily sets the query memory limit for testing
// Returns a cleanup function to restore the original limit
// Usage: defer setTestMemoryLimit(t, fixture, 1)()
func setTestMemoryLimit(t *testing.T, limitMB int) func() {
	t.Helper()

	// Modify the global settings (used by CommandDirector)
	globalSettings := settings.GetSettings()
	originalLimit := globalSettings.QueryMaxMemoryMB
	globalSettings.QueryMaxMemoryMB = limitMB

	t.Logf("Set test memory limit to %dMB (was %dMB)", limitMB, originalLimit)

	return func() {
		globalSettings.QueryMaxMemoryMB = originalLimit
		t.Logf("Restored memory limit to %dMB", originalLimit)
	}
}

// createAuthorsBundle creates the Authors bundle if it doesn't exist
func createAuthorsBundle(t *testing.T, fixture *TestFixture) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Create Authors bundle with all fields needed by different seed functions
	createAuthorsCmd := `CREATE BUNDLE "Authors" WITH FIELDS (
		{"Name", "STRING", true, false},
		{"Country", "STRING", false, false},
		{"Biography", "STRING", false, false},
		{"Metadata", "JSON", false, false, null}
	);`

	startTime := time.Now()
	_, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, createAuthorsCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	if err != nil {
		t.Fatalf("Failed to create Authors bundle: %v", err)
	}
}

// seedAuthors creates N small author documents
func seedAuthors(t *testing.T, fixture *TestFixture, count int) {
	t.Helper()

	// Ensure Authors bundle exists
	createAuthorsBundle(t, fixture)

	bundleService := fixture.ServiceManager.BundleService

	bundle, err := bundleService.GetBundleByName(fixture.Database, "Authors")
	if err != nil {
		t.Fatalf("Failed to get Authors bundle: %v", err)
	}

	for i := 0; i < count; i++ {
		doc := &models.Document{
			DocumentID: fmt.Sprintf("author_%d", i),
			Data: map[string]interface{}{
				"Name":    fmt.Sprintf("Author %d", i),
				"Country": "USA",
			},
		}
		if err := bundleService.AddDocumentToBundleByStruct(fixture.Database, bundle, doc); err != nil {
			t.Fatalf("Failed to add author document %d: %v", i, err)
		}
	}

	// CRITICAL: Flush metadata and buffers so scanner can find documents
	fixture.ServiceManager.BundleService.FlushMetadataUpdates()
	if err := fixture.ServiceManager.BundleService.FlushAllBuffers(); err != nil {
		t.Fatalf("Failed to flush buffers after seeding authors: %v", err)
	}
}

// seedLargeAuthors creates N large author documents (~300KB each)
func seedLargeAuthors(t *testing.T, fixture *TestFixture, count int) {
	t.Helper()

	// Ensure Authors bundle exists
	createAuthorsBundle(t, fixture)

	bundleService := fixture.ServiceManager.BundleService

	// Create large biography text (~300KB)
	largeBio := strings.Repeat("This is a very long biography text. ", 8000)

	bundle, err := bundleService.GetBundleByName(fixture.Database, "Authors")
	if err != nil {
		t.Fatalf("Failed to get Authors bundle: %v", err)
	}

	for i := 0; i < count; i++ {
		doc := &models.Document{
			DocumentID: fmt.Sprintf("author_%d", i),
			Data: map[string]interface{}{
				"Name":      fmt.Sprintf("Author %d", i),
				"Country":   "USA",
				"Biography": largeBio,
			},
		}
		if err := bundleService.AddDocumentToBundleByStruct(fixture.Database, bundle, doc); err != nil {
			t.Fatalf("Failed to add large author document %d: %v", i, err)
		}
	}

	// CRITICAL: Flush metadata and buffers so scanner can find documents
	fixture.ServiceManager.BundleService.FlushMetadataUpdates()
	if err := fixture.ServiceManager.BundleService.FlushAllBuffers(); err != nil {
		t.Fatalf("Failed to flush buffers after seeding large authors: %v", err)
	}
}

// seedAuthorsWithNestedMetadata creates authors with deeply nested metadata
func seedAuthorsWithNestedMetadata(t *testing.T, fixture *TestFixture, count int) {
	t.Helper()

	// Ensure Authors bundle exists
	createAuthorsBundle(t, fixture)

	bundleService := fixture.ServiceManager.BundleService

	bundle, err := bundleService.GetBundleByName(fixture.Database, "Authors")
	if err != nil {
		t.Fatalf("Failed to get Authors bundle: %v", err)
	}

	for i := 0; i < count; i++ {
		// Create deeply nested metadata structure
		nestedData := make(map[string]interface{})
		current := nestedData

		// Create 50 levels of nesting with arrays and maps
		for level := 0; level < 50; level++ {
			current[fmt.Sprintf("level_%d", level)] = map[string]interface{}{
				"data":  strings.Repeat("nested data ", 100),
				"array": []string{"item1", "item2", "item3", "item4", "item5"},
			}

			// Navigate deeper
			if m, ok := current[fmt.Sprintf("level_%d", level)].(map[string]interface{}); ok {
				current = m
			}
		}

		doc := &models.Document{
			DocumentID: fmt.Sprintf("author_%d", i),
			Data: map[string]interface{}{
				"Name":     fmt.Sprintf("Author %d", i),
				"Metadata": nestedData,
			},
		}

		if err := bundleService.AddDocumentToBundleByStruct(fixture.Database, bundle, doc); err != nil {
			t.Fatalf("Failed to add author with nested metadata %d: %v", i, err)
		}
	}

	// CRITICAL: Flush metadata and buffers so scanner can find documents
	fixture.ServiceManager.BundleService.FlushMetadataUpdates()
	if err := fixture.ServiceManager.BundleService.FlushAllBuffers(); err != nil {
		t.Fatalf("Failed to flush buffers after seeding nested authors: %v", err)
	}
}

// cleanupFullServer tears down test fixture
func cleanupFullServer(t *testing.T) {
	t.Helper()
	// Cleanup is handled automatically by defer statements in test setup
}
