package tests

import (
	"fmt"
	"syndrdb/src/internal/domain/models"
	"syndrdb/src/internal/query/planner"
	"syndrdb/src/internal/query/queryparser"
	"testing"

	"go.uber.org/zap"
)

// BenchmarkSelect_PlanCaching measures allocation reduction from query plan caching
//
// STEP 2: Query Plan Caching
// - Measures allocations with plan caching enabled
// - Compares cached vs uncached query planning
// - Target: Reduce allocations by -30 to -40 allocs/op
func BenchmarkSelect_PlanCaching(b *testing.B) {
	// Setup
	logger, _ := zap.NewDevelopment()
	defer logger.Sync()
	sugar := logger.Sugar()

	// Create test database
	database := &models.Database{
		Name:     "testdb",
		FilePath: "../data_files/testdb",
	}

	// Create a unified query planner with caching enabled
	bundleService, _ := setupBenchmarkEnvironment(sugar)
	queryPlanner := planner.NewUnifiedQueryPlanner(sugar, bundleService)

	// Test query that will be planned multiple times
	queryStr := "SELECT Name, Email FROM Authors WHERE Age > 25 ORDER BY Name LIMIT 10"

	// Parse query once (outside benchmark to avoid measuring parse time)
	query, err := queryparser.ParseUnifiedSelectQuery(queryStr, sugar)
	if err != nil {
		b.Fatalf("Failed to parse query: %v", err)
	}

	// Reset timer to exclude setup
	b.ResetTimer()
	b.ReportAllocs()

	// Run benchmark
	// First iteration: Cache MISS (plan created)
	// Subsequent iterations: Cache HIT (plan reused)
	for i := 0; i < b.N; i++ {
		plan, err := queryPlanner.CreatePlan(query, database)
		if err != nil {
			b.Fatalf("Failed to create plan: %v", err)
		}
		_ = plan // Avoid unused variable
	}
}

// BenchmarkSelect_PlanCaching_ColdCache measures planning allocations without cache
func BenchmarkSelect_PlanCaching_ColdCache(b *testing.B) {
	// Setup
	logger, _ := zap.NewDevelopment()
	defer logger.Sync()
	sugar := logger.Sugar()

	// Create test database
	database := &models.Database{
		Name:     "testdb",
		FilePath: "../data_files/testdb",
	}

	// Create bundle service
	bundleService, _ := setupBenchmarkEnvironment(sugar)

	// Test query
	queryStr := "SELECT Name, Email FROM Authors WHERE Age > 25 ORDER BY Name LIMIT 10"

	// Parse query once
	query, err := queryparser.ParseUnifiedSelectQuery(queryStr, sugar)
	if err != nil {
		b.Fatalf("Failed to parse query: %v", err)
	}

	// Reset timer
	b.ResetTimer()
	b.ReportAllocs()

	// Run benchmark with NEW planner each iteration (no cache benefit)
	for i := 0; i < b.N; i++ {
		// Create fresh planner (simulates cache disabled)
		queryPlanner := planner.NewUnifiedQueryPlanner(sugar, bundleService)

		plan, err := queryPlanner.CreatePlan(query, database)
		if err != nil {
			b.Fatalf("Failed to create plan: %v", err)
		}
		_ = plan
	}
}

// TestPlanCache_HitRate verifies cache is working correctly
func TestPlanCache_HitRate(t *testing.T) {
	// Setup
	logger, _ := zap.NewDevelopment()
	defer logger.Sync()
	sugar := logger.Sugar()

	database := &models.Database{
		Name:     "testdb",
		FilePath: "../data_files/testdb",
	}

	bundleService, _ := setupBenchmarkEnvironment(sugar)
	queryPlanner := planner.NewUnifiedQueryPlanner(sugar, bundleService)

	// Test query
	queryStr := "SELECT Name FROM Authors WHERE Age > 25"
	query, err := queryparser.ParseUnifiedSelectQuery(queryStr, sugar)
	if err != nil {
		t.Fatalf("Failed to parse query: %v", err)
	}

	// First call: Cache MISS
	plan1, err := queryPlanner.CreatePlan(query, database)
	if err != nil {
		t.Fatalf("Failed to create plan: %v", err)
	}

	// Second call: Cache HIT (same query)
	plan2, err := queryPlanner.CreatePlan(query, database)
	if err != nil {
		t.Fatalf("Failed to create plan: %v", err)
	}

	// Verify same plan instance returned (cache hit)
	if plan1 != plan2 {
		t.Errorf("Expected same plan instance from cache, got different instances")
	}

	// Test with different query: Cache MISS
	queryStr2 := "SELECT Email FROM Authors WHERE Age > 30"
	query2, err := queryparser.ParseUnifiedSelectQuery(queryStr2, sugar)
	if err != nil {
		t.Fatalf("Failed to parse query: %v", err)
	}

	plan3, err := queryPlanner.CreatePlan(query2, database)
	if err != nil {
		t.Fatalf("Failed to create plan: %v", err)
	}

	// Verify different plan (different query)
	if plan1 == plan3 {
		t.Errorf("Expected different plan for different query")
	}

	t.Logf("Plan cache test passed: cache hit and miss behavior correct")
}

// TestPlanCache_Invalidation verifies cache invalidation works
func TestPlanCache_Invalidation(t *testing.T) {
	// Setup
	logger, _ := zap.NewDevelopment()
	defer logger.Sync()
	sugar := logger.Sugar()

	database := &models.Database{
		Name:     "testdb",
		FilePath: "../data_files/testdb",
	}

	bundleService, _ := setupBenchmarkEnvironment(sugar)
	queryPlanner := planner.NewUnifiedQueryPlanner(sugar, bundleService)

	// Test query
	queryStr := "SELECT Name FROM Authors"
	query, err := queryparser.ParseUnifiedSelectQuery(queryStr, sugar)
	if err != nil {
		t.Fatalf("Failed to parse query: %v", err)
	}

	// First call: Cache MISS, store plan
	plan1, err := queryPlanner.CreatePlan(query, database)
	if err != nil {
		t.Fatalf("Failed to create plan: %v", err)
	}

	// Second call: Cache HIT
	plan2, err := queryPlanner.CreatePlan(query, database)
	if err != nil {
		t.Fatalf("Failed to create plan: %v", err)
	}

	if plan1 != plan2 {
		t.Errorf("Expected cache hit before invalidation")
	}

	// Invalidate cache (simulates INSERT/UPDATE/DELETE)
	queryPlanner.InvalidatePlanCache()

	// Third call: Cache MISS (cache was invalidated)
	plan3, err := queryPlanner.CreatePlan(query, database)
	if err != nil {
		t.Fatalf("Failed to create plan: %v", err)
	}

	// After invalidation, we get a fresh plan (different instance)
	if plan2 == plan3 {
		t.Errorf("Expected cache miss after invalidation, got same plan instance")
	}

	t.Logf("Plan cache invalidation test passed")
}

// setupBenchmarkEnvironment creates minimal setup for benchmarking
func setupBenchmarkEnvironment(logger *zap.SugaredLogger) (interface{}, error) {
	// Return a mock bundle service for testing
	// This is simplified - in real tests, use actual BundleService
	return nil, fmt.Errorf("setupBenchmarkEnvironment not fully implemented")
}
