package syndrQL

import (
	"syndrdb/src/internal/server"
	"syndrdb/src/pkg/common/helpers"
	"testing"
	"time"
)

// BenchmarkSelect_AllFields_Small benchmarks SELECT * query on 100 rows
func BenchmarkSelect_AllFields_Small(b *testing.B) {
	// Setup once outside the measured loop
	fixture := setupRealServerTB(b)
	seedSimpleAuthorsBundleTB(b, fixture, 100)

	query := `SELECT * FROM "Authors"`

	// Reset timer to exclude setup time from measurements
	b.ResetTimer()
	b.ReportAllocs()

	// The actual benchmark loop - only the query execution is timed
	for i := 0; i < b.N; i++ {
		result, err := server.CommandDirector(
			fixture.Database,
			*fixture.ServiceManager,
			query,
			fixture.Logger,
			time.Now(),
			nil,
			"127.0.0.1",
		)
		if err != nil {
			b.Fatalf("Query failed: %v", err)
		}

		// PHASE A: Clean up pooled maps after each query
		if cmdResp, ok := result.(*server.CommandResponse); ok && len(cmdResp.PooledMaps) > 0 {
			helpers.FreeResultSet(cmdResp.PooledMaps)
		}
	}
}

// BenchmarkSelect_DocumentPooling benchmarks document pool integration
// STEP 1: Measures allocation reduction from document pooling
func BenchmarkSelect_DocumentPooling(b *testing.B) {
	// Setup once outside the measured loop
	fixture := setupRealServerTB(b)
	seedSimpleAuthorsBundleTB(b, fixture, 100)

	query := `SELECT * FROM "Authors"`

	// Reset timer to exclude setup time from measurements
	b.ResetTimer()
	b.ReportAllocs()

	// The actual benchmark loop - only the query execution is timed
	for i := 0; i < b.N; i++ {
		result, err := server.CommandDirector(
			fixture.Database,
			*fixture.ServiceManager,
			query,
			fixture.Logger,
			time.Now(),
			nil,
			"127.0.0.1",
		)
		if err != nil {
			b.Fatalf("Query failed: %v", err)
		}

		// STEP 1: Clean up both pooled maps and pooled documents
		if cmdResp, ok := result.(*server.CommandResponse); ok {
			if len(cmdResp.PooledMaps) > 0 {
				helpers.FreeResultSet(cmdResp.PooledMaps)
			}
			// Note: PooledDocuments cleanup is handled in sendResult() defer
			// This benchmark measures the allocation reduction from using pools
		}
	}
}

// BenchmarkSelect_PlanCaching benchmarks query plan caching
// STEP 2: Measures allocation reduction from plan caching (same query repeated)
func BenchmarkSelect_PlanCaching(b *testing.B) {
	// Setup once outside the measured loop
	fixture := setupRealServerTB(b)
	seedSimpleAuthorsBundleTB(b, fixture, 100)

	query := `SELECT Name, Country FROM "Authors" WHERE ID > 10 ORDER BY Name LIMIT 50`

	// Reset timer to exclude setup time from measurements
	b.ResetTimer()
	b.ReportAllocs()

	// The actual benchmark loop
	// First iteration: Cache MISS (plan created and cached)
	// Subsequent iterations: Cache HIT (plan reused from cache)
	for i := 0; i < b.N; i++ {
		result, err := server.CommandDirector(
			fixture.Database,
			*fixture.ServiceManager,
			query,
			fixture.Logger,
			time.Now(),
			nil,
			"127.0.0.1",
		)
		if err != nil {
			b.Fatalf("Query failed: %v", err)
		}

		// Clean up pooled resources
		if cmdResp, ok := result.(*server.CommandResponse); ok {
			if len(cmdResp.PooledMaps) > 0 {
				helpers.FreeResultSet(cmdResp.PooledMaps)
			}
		}
	}
}

// BenchmarkSelect_PlanCaching_NoCacheControl benchmarks without plan cache benefit
// STEP 2: Control benchmark - invalidates cache each iteration to measure baseline
func BenchmarkSelect_PlanCaching_NoCacheControl(b *testing.B) {
	// Setup once outside the measured loop
	fixture := setupRealServerTB(b)
	seedSimpleAuthorsBundleTB(b, fixture, 100)

	query := `SELECT Name, Country FROM "Authors" WHERE ID > 10 ORDER BY Name LIMIT 50`

	// Reset timer to exclude setup time from measurements
	b.ResetTimer()
	b.ReportAllocs()

	// The actual benchmark loop
	// Invalidate cache each iteration to measure planning cost without caching
	for i := 0; i < b.N; i++ {
		// Invalidate cache before each query to simulate fresh planning
		if fixture.ServiceManager.UnifiedPlanner != nil {
			fixture.ServiceManager.UnifiedPlanner.InvalidatePlanCache()
		}

		result, err := server.CommandDirector(
			fixture.Database,
			*fixture.ServiceManager,
			query,
			fixture.Logger,
			time.Now(),
			nil,
			"127.0.0.1",
		)
		if err != nil {
			b.Fatalf("Query failed: %v", err)
		}

		// Clean up pooled resources
		if cmdResp, ok := result.(*server.CommandResponse); ok {
			if len(cmdResp.PooledMaps) > 0 {
				helpers.FreeResultSet(cmdResp.PooledMaps)
			}
		}
	}
}

// BenchmarkSelect_Join_HashJoin benchmarks JOIN query with hash join execution
// This exercises the hot paths optimized with ValueToString:
// - Hash table key generation (hash_table.go)
// - Bloom filter operations (hash_join.go)
// - Value comparisons (join executors)
func BenchmarkSelect_Join_HashJoin(b *testing.B) {
	// Setup once outside the measured loop
	fixture := setupRealServerTB(b)

	// Seed Authors (100 records)
	seedSimpleAuthorsBundleTB(b, fixture, 100)

	// Seed Books (500 records, ~5 books per author)
	seedSimpleBooksBundleTB(b, fixture, 500)

	// JOIN query that will use hash join
	// Note: AuthorsID is the foreign key field created by the relationship
	query := `SELECT * FROM "Authors" JOIN "Books" ON "Authors"."DocumentID" == "Books"."AuthorsID"`

	// Reset timer to exclude setup time from measurements
	b.ResetTimer()
	b.ReportAllocs()

	// The actual benchmark loop
	for i := 0; i < b.N; i++ {
		result, err := server.CommandDirector(
			fixture.Database,
			*fixture.ServiceManager,
			query,
			fixture.Logger,
			time.Now(),
			nil,
			"127.0.0.1",
		)
		if err != nil {
			b.Fatalf("Query failed: %v", err)
		}

		// Clean up pooled resources
		if cmdResp, ok := result.(*server.CommandResponse); ok {
			if len(cmdResp.PooledMaps) > 0 {
				helpers.FreeResultSet(cmdResp.PooledMaps)
			}
		}
	}
}
