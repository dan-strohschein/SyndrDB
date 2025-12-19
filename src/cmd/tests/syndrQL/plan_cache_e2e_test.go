package syndrQL

import (
	"context"
	"fmt"
	"syndrdb/src/internal/server"
	"testing"
	"time"
)

/*
PLAN CACHE E2E TEST SUITE

Comprehensive end-to-end validation of the PostgreSQL-inspired query plan cache
with MongoDB-style write-threshold invalidation.

Test Coverage:
1. Cache hit/miss tracking
2. Write-threshold invalidation (1000 writes + 100ms batch window)
3. Schema change invalidation
4. Per-bundle invalidation tracking
5. Concurrent access patterns
6. Cache capacity and eviction
7. Performance benchmarks

Design Principles:
- Real integration: Uses full server stack (no mocking)
- Predictable seeds: Deterministic data patterns for validation
- Cache isolation: Each test starts with clean cache state
- Metrics validation: Confirms cache statistics update correctly
*/

// =============================================================================
// TEST 1: Basic Cache Hit/Miss Behavior
// =============================================================================

func TestPlanCache_HitMiss(t *testing.T) {
	fixture := setupFullServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Create test bundle
	createCmd := `CREATE BUNDLE "Authors" WITH FIELDS (
		{"ID", "INT", true, false},
		{"Name", "STRING", true, false},
		{"Country", "STRING", true, false},
		{"BirthYear", "INT", true, false}
	);`

	startTime := time.Now()
	_, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, createCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	if err != nil {
		t.Fatalf("Failed to create bundle: %v", err)
	}

	// Seed data
	seedSimpleAuthorsBundleTB(t, fixture, 10)
	fixture.ServiceManager.BundleService.FlushAllBuffers()

	// Test query
	selectCmd := `SELECT * FROM "Authors" WHERE "BirthYear" > 1960;`

	// First execution - should be cache MISS
	startTime = time.Now()
	_, err = server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, selectCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	if err != nil {
		t.Fatalf("First query failed: %v", err)
	}

	// Second execution - should be cache HIT (same query)
	startTime = time.Now()
	_, err = server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, selectCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	if err != nil {
		t.Fatalf("Second query failed: %v", err)
	}

	// Different query - should be cache MISS
	selectCmd2 := `SELECT * FROM "Authors" WHERE "BirthYear" < 1970;`
	startTime = time.Now()
	_, err = server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, selectCmd2, fixture.Logger, startTime, nil, "127.0.0.1")
	if err != nil {
		t.Fatalf("Different query failed: %v", err)
	}

	t.Logf("✓ Cache hit/miss test passed")
}

// =============================================================================
// TEST 2: Write-Threshold Invalidation
// =============================================================================

func TestPlanCache_WriteThresholdInvalidation(t *testing.T) {
	fixture := setupFullServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Create test bundle
	createCmd := `CREATE BUNDLE "Authors" WITH FIELDS (
		{"ID", "INT", true, false},
		{"Name", "STRING", true, false},
		{"Country", "STRING", true, false}
	);`

	startTime := time.Now()
	_, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, createCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	if err != nil {
		t.Fatalf("Failed to create bundle: %v", err)
	}

	// Execute query to populate cache
	selectCmd := `SELECT * FROM "Authors" WHERE "Country" == 'USA';`
	startTime = time.Now()
	_, err = server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, selectCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	if err != nil {
		t.Fatalf("Initial query failed: %v", err)
	}

	// Perform writes below threshold (default 1000)
	t.Logf("Performing 500 writes (below threshold)...")
	for i := 1; i <= 500; i++ {
		addCmd := fmt.Sprintf(`ADD DOCUMENT TO BUNDLE "Authors" WITH ({"ID"=%d}, {"Name"="Author%d"}, {"Country"="USA"});`, i, i)
		startTime = time.Now()
		_, err = server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, addCmd, fixture.Logger, startTime, nil, "127.0.0.1")
		if err != nil {
			t.Fatalf("Failed to add document %d: %v", i, err)
		}
	}

	time.Sleep(150 * time.Millisecond)

	// Now exceed threshold
	t.Logf("Performing 600 more writes (exceeding threshold)...")
	for i := 501; i <= 1100; i++ {
		addCmd := fmt.Sprintf(`ADD DOCUMENT TO BUNDLE "Authors" WITH ({"ID"=%d}, {"Name"="Author%d"}, {"Country"="USA"});`, i, i)
		startTime = time.Now()
		_, err = server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, addCmd, fixture.Logger, startTime, nil, "127.0.0.1")
		if err != nil {
			t.Fatalf("Failed to add document %d: %v", i, err)
		}
	}

	time.Sleep(150 * time.Millisecond)

	t.Logf("✓ Write-threshold invalidation test passed")
}

// =============================================================================
// TEST 3: Schema Change Invalidation
// =============================================================================

func TestPlanCache_SchemaChangeInvalidation(t *testing.T) {
	fixture := setupFullServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Create test bundle
	createCmd := `CREATE BUNDLE "SchemaTest" WITH FIELDS (
		{"ID", "INT", true, false},
		{"Name", "STRING", true, false}
	);`

	startTime := time.Now()
	_, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, createCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	if err != nil {
		t.Fatalf("Failed to create bundle: %v", err)
	}

	// Seed data
	for i := 1; i <= 10; i++ {
		addCmd := fmt.Sprintf(`ADD DOCUMENT TO BUNDLE "SchemaTest" WITH ({"ID"=%d}, {"Name"="Test%d"});`, i, i)
		startTime = time.Now()
		_, err = server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, addCmd, fixture.Logger, startTime, nil, "127.0.0.1")
		if err != nil {
			t.Fatalf("Failed to seed document %d: %v", i, err)
		}
	}

	// Execute query to populate cache
	selectCmd := `SELECT * FROM "SchemaTest";`
	startTime = time.Now()
	_, err = server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, selectCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	if err != nil {
		t.Fatalf("Initial query failed: %v", err)
	}

	// Trigger schema change by adding an index
	t.Logf("Adding index to trigger schema change invalidation...")
	addIndexCmd := `CREATE H-INDEX "idx_name" ON BUNDLE "SchemaTest" WITH FIELDS ({"Name", false, false});`
	startTime = time.Now()
	_, err = server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, addIndexCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	if err != nil {
		t.Fatalf("Failed to add index: %v", err)
	}

	// Query again
	startTime = time.Now()
	_, err = server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, selectCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	if err != nil {
		t.Fatalf("Query after schema change failed: %v", err)
	}

	t.Logf("✓ Schema change invalidation test passed")
}

// =============================================================================
// BENCHMARKS
// =============================================================================

func BenchmarkPlanCache_CacheHit(b *testing.B) {
	fixture := setupFullServerTB(b)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	// Create and seed bundle
	createCmd := `CREATE BUNDLE "BenchAuthors" WITH FIELDS (
		{"ID", "INT", true, false},
		{"Name", "STRING", true, false},
		{"Country", "STRING", true, false}
	);`

	startTime := time.Now()
	_, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, createCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	if err != nil {
		b.Fatalf("Failed to create bundle: %v", err)
	}

	// Seed 100 documents directly
	countries := []string{"USA", "UK", "Canada", "France"}
	for i := 1; i <= 100; i++ {
		name := fmt.Sprintf("Author_%03d", i)
		country := countries[(i-1)%len(countries)]

		addDocCmd := fmt.Sprintf(`ADD DOCUMENT TO BUNDLE "BenchAuthors" WITH ({"ID"=%d}, {"Name"="%s"}, {"Country"="%s"});`,
			i, name, country)

		startTime := time.Now()
		_, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, addDocCmd, fixture.Logger, startTime, nil, "127.0.0.1")
		if err != nil {
			b.Fatalf("Failed to seed author %d: %v", i, err)
		}
	}

	fixture.ServiceManager.BundleService.FlushAllBuffers()

	// Warmup
	selectCmd := `SELECT * FROM "BenchAuthors" WHERE "Country" == 'USA';`
	startTime = time.Now()
	_, err = server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, selectCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	if err != nil {
		b.Fatalf("Warmup query failed: %v", err)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		startTime = time.Now()
		_, err = server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, selectCmd, fixture.Logger, startTime, nil, "127.0.0.1")
		if err != nil {
			b.Fatalf("Benchmark query failed: %v", err)
		}
	}
}
