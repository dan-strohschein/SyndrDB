package main

import (
	"os"
	"testing"
	"time"

	"syndrdb/src/internal/graphQL/schema"
)

// TestSchemaCacheBasicOperations tests basic cache operations
func TestSchemaCacheBasicOperations(t *testing.T) {
	cache := schema.NewSchemaCache()

	// Test cache miss
	result := cache.Get("nonexistent")
	if result != nil {
		t.Error("Expected nil for cache miss")
	}

	// Create a test schema
	definition := &schema.GraphQLSchemaDefinition{
		TypeName: "User",
		Fields: []schema.GraphQLField{
			{Name: "id", Type: "ID!"},
			{Name: "name", Type: "String"},
		},
	}

	var schemaID, bundleID [16]byte
	copy(schemaID[:], []byte("schema-id-000001"))
	copy(bundleID[:], []byte("bundle-id-000001"))

	record := schema.NewSchemaRecord(schemaID, bundleID, "testdb", "users", 1, definition)

	// Test Set
	err := cache.Set("users", record)
	if err != nil {
		t.Fatalf("Failed to set cache: %v", err)
	}

	// Test cache hit
	cached := cache.Get("users")
	if cached == nil {
		t.Fatal("Expected cache hit")
	}

	if cached.GetBundleName() != "users" {
		t.Errorf("Expected bundle name 'users', got '%s'", cached.GetBundleName())
	}

	// Test Size
	if cache.Size() != 1 {
		t.Errorf("Expected size 1, got %d", cache.Size())
	}

	// Test Contains
	if !cache.Contains("users") {
		t.Error("Expected cache to contain 'users'")
	}

	// Test stats
	stats := cache.GetStats()
	if stats.Hits != 1 {
		t.Errorf("Expected 1 hit, got %d", stats.Hits)
	}
	if stats.Misses != 1 {
		t.Errorf("Expected 1 miss, got %d", stats.Misses)
	}
}

// TestSchemaCacheInvalidation tests cache invalidation
func TestSchemaCacheInvalidation(t *testing.T) {
	cache := schema.NewSchemaCache()

	// Add multiple entries
	definition := &schema.GraphQLSchemaDefinition{
		TypeName: "TestType",
		Fields:   []schema.GraphQLField{{Name: "id", Type: "ID!"}},
	}

	var schemaID, bundleID [16]byte
	bundles := []string{"bundle1", "bundle2", "bundle3"}

	for _, bundleName := range bundles {
		copy(schemaID[:], []byte("schema-"+bundleName))
		copy(bundleID[:], []byte("bundle-"+bundleName))
		record := schema.NewSchemaRecord(schemaID, bundleID, "testdb", bundleName, 1, definition)
		cache.Set(bundleName, record)
	}

	if cache.Size() != 3 {
		t.Fatalf("Expected 3 entries, got %d", cache.Size())
	}

	// Test single invalidation
	cache.Invalidate("bundle2")
	if cache.Size() != 2 {
		t.Errorf("Expected 2 entries after invalidation, got %d", cache.Size())
	}

	if cache.Contains("bundle2") {
		t.Error("Expected bundle2 to be invalidated")
	}

	// Test InvalidateAll
	cache.InvalidateAll()
	if cache.Size() != 0 {
		t.Errorf("Expected 0 entries after invalidate all, got %d", cache.Size())
	}

	stats := cache.GetStats()
	// Should be 1 (single invalidation) + 2 (bulk invalidation of remaining entries) = 3
	if stats.Invalidations != 3 {
		t.Errorf("Expected 3 invalidations (1 single + 2 bulk), got %d", stats.Invalidations)
	}
}

// TestSchemaCachePreload tests cache preloading
func TestSchemaCachePreload(t *testing.T) {
	cache := schema.NewSchemaCache()

	definition := &schema.GraphQLSchemaDefinition{
		TypeName: "PreloadType",
		Fields:   []schema.GraphQLField{{Name: "id", Type: "ID!"}},
	}

	var schemaID, bundleID [16]byte
	schemas := make([]*schema.SchemaRecord, 5)

	for i := 0; i < 5; i++ {
		bundleName := string(rune('a' + i))
		copy(schemaID[:], []byte("schema-"+bundleName))
		copy(bundleID[:], []byte("bundle-"+bundleName))
		schemas[i] = schema.NewSchemaRecord(schemaID, bundleID, "testdb", bundleName, 1, definition)
	}

	// Preload schemas
	loaded, err := cache.Preload(schemas)
	if err != nil {
		t.Fatalf("Preload failed: %v", err)
	}

	if loaded != 5 {
		t.Errorf("Expected 5 schemas loaded, got %d", loaded)
	}

	if cache.Size() != 5 {
		t.Errorf("Expected cache size 5, got %d", cache.Size())
	}
}

// TestSchemaCacheHitRate tests hit rate calculation
func TestSchemaCacheHitRate(t *testing.T) {
	cache := schema.NewSchemaCache()

	definition := &schema.GraphQLSchemaDefinition{
		TypeName: "HitRateType",
		Fields:   []schema.GraphQLField{{Name: "id", Type: "ID!"}},
	}

	var schemaID, bundleID [16]byte
	copy(schemaID[:], []byte("schema-id-000001"))
	copy(bundleID[:], []byte("bundle-id-000001"))
	record := schema.NewSchemaRecord(schemaID, bundleID, "testdb", "tesbundle", 1, definition)

	cache.Set("testbundle", record)

	// Generate some hits and misses
	for i := 0; i < 8; i++ {
		cache.Get("testbundle") // Hit
	}
	for i := 0; i < 2; i++ {
		cache.Get("nonexistent") // Miss
	}

	// Should be 80% hit rate (8 hits / 10 total)
	hitRate := cache.GetHitRate()
	if hitRate < 79.0 || hitRate > 81.0 {
		t.Errorf("Expected ~80%% hit rate, got %.1f%%", hitRate)
	}
}

// TestSchemaCacheWarmUp tests cache warming from file
func TestSchemaCacheWarmUp(t *testing.T) {
	tmpFile := "/tmp/test_warmup.gql"
	defer os.Remove(tmpFile)

	// Create manager with test data
	manager, err := schema.NewSchemaManager(tmpFile, "testdb", "test1234")
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}
	defer manager.Close()

	// Add some schemas
	definition := &schema.GraphQLSchemaDefinition{
		TypeName: "WarmUpType",
		Fields:   []schema.GraphQLField{{Name: "id", Type: "ID!"}},
	}

	var schemaID, bundleID [16]byte
	for i := 1; i <= 10; i++ {
		bundleName := string(rune('a' + i - 1))
		copy(schemaID[:], []byte("schema-00000"+string(rune('0'+i))))
		copy(bundleID[:], []byte("bundle-00000"+string(rune('0'+i))))
		err = manager.AddNewSchema(schemaID, bundleID, bundleName, definition)
		if err != nil {
			t.Fatalf("Failed to add schema: %v", err)
		}
	}

	// Clear cache and warm up
	manager.ClearCache()

	loaded, duration, err := manager.PreloadCache()
	if err != nil {
		t.Fatalf("PreloadCache failed: %v", err)
	}

	if loaded != 10 {
		t.Errorf("Expected 10 schemas loaded, got %d", loaded)
	}

	// Check performance requirement (< 100ms for 100 schemas)
	// With 10 schemas, should be much faster
	if duration > 50*time.Millisecond {
		t.Errorf("PreloadCache too slow: %v (expected < 50ms for 10 schemas)", duration)
	}

	t.Logf("Loaded %d schemas in %v", loaded, duration)

	// Verify cache is populated
	stats := manager.GetCacheStats()
	if stats.TotalEntries != 10 {
		t.Errorf("Expected 10 cache entries, got %d", stats.TotalEntries)
	}
}

// TestSchemaCacheLookupPerformance tests cache lookup speed
func TestSchemaCacheLookupPerformance(t *testing.T) {
	tmpFile := "/tmp/test_lookup_perf.gql"
	defer os.Remove(tmpFile)

	manager, err := schema.NewSchemaManager(tmpFile, "testdb", "test1234")
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}
	defer manager.Close()

	// Add test schema
	definition := &schema.GraphQLSchemaDefinition{
		TypeName: "PerfTestType",
		Fields:   []schema.GraphQLField{{Name: "id", Type: "ID!"}},
	}

	var schemaID, bundleID [16]byte
	copy(schemaID[:], []byte("schema-id-000001"))
	copy(bundleID[:], []byte("bundle-id-000001"))
	err = manager.AddNewSchema(schemaID, bundleID, "perfbundle", definition)
	if err != nil {
		t.Fatalf("Failed to add schema: %v", err)
	}

	// Warm up cache
	_, _, err = manager.PreloadCache()
	if err != nil {
		t.Fatalf("PreloadCache failed: %v", err)
	}

	// Measure cache lookup performance
	iterations := 10000
	start := time.Now()

	for i := 0; i < iterations; i++ {
		schema, err := manager.GetCachedSchema("perfbundle")
		if err != nil || schema == nil {
			t.Fatalf("GetCachedSchema failed on iteration %d: %v", i, err)
		}
	}

	duration := time.Since(start)
	avgPerLookup := duration / time.Duration(iterations)

	t.Logf("Performed %d lookups in %v (avg: %v per lookup)", iterations, duration, avgPerLookup)

	// Requirement: < 5μs per lookup
	if avgPerLookup > 5*time.Microsecond {
		t.Errorf("Cache lookup too slow: %v per lookup (required < 5μs)", avgPerLookup)
	}

	// Verify 100% hit rate
	hitRate := manager.GetCacheHitRate()
	if hitRate < 99.9 {
		t.Errorf("Expected ~100%% hit rate, got %.2f%%", hitRate)
	}
}

// TestSchemaCacheIntegrationWithManager tests cache integration with manager operations
func TestSchemaCacheIntegrationWithManager(t *testing.T) {
	tmpFile := "/tmp/test_cache_integration.gql"
	defer os.Remove(tmpFile)

	manager, err := schema.NewSchemaManager(tmpFile, "testdb", "test1234")
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}
	defer manager.Close()

	definition := &schema.GraphQLSchemaDefinition{
		TypeName: "IntegrationTest",
		Fields:   []schema.GraphQLField{{Name: "id", Type: "ID!"}},
	}

	var schemaID, bundleID [16]byte
	copy(schemaID[:], []byte("schema-id-000001"))
	copy(bundleID[:], []byte("bundle-id-000001"))

	// Add schema - should invalidate cache
	err = manager.AddNewSchema(schemaID, bundleID, "integration", definition)
	if err != nil {
		t.Fatalf("Failed to add schema: %v", err)
	}

	// First lookup - cache miss, loads from disk
	schema1, err := manager.GetCachedSchema("integration")
	if err != nil || schema1 == nil {
		t.Fatalf("GetCachedSchema failed: %v", err)
	}

	// Second lookup - cache hit
	schema2, err := manager.GetCachedSchema("integration")
	if err != nil || schema2 == nil {
		t.Fatalf("GetCachedSchema failed: %v", err)
	}

	// Update schema - should invalidate cache
	newDefinition := &schema.GraphQLSchemaDefinition{
		TypeName: "IntegrationTestV2",
		Fields: []schema.GraphQLField{
			{Name: "id", Type: "ID!"},
			{Name: "name", Type: "String"},
		},
	}

	copy(schemaID[:], []byte("schema-id-000002"))
	err = manager.UpdateSchema(schemaID, bundleID, "integration", newDefinition)
	if err != nil {
		t.Fatalf("UpdateSchema failed: %v", err)
	}

	// Get updated schema - should load new version
	schema3, err := manager.GetCachedSchema("integration")
	if err != nil || schema3 == nil {
		t.Fatalf("GetCachedSchema failed after update: %v", err)
	}

	if schema3.SchemaVersion != 2 {
		t.Errorf("Expected version 2, got %d", schema3.SchemaVersion)
	}

	// Tombstone schema - should invalidate cache
	err = manager.TombstoneSchema("integration", 2)
	if err != nil {
		t.Fatalf("TombstoneSchema failed: %v", err)
	}

	// Try to get tombstoned schema - should return nil
	schema4, err := manager.GetCachedSchema("integration")
	if schema4 != nil {
		t.Error("Expected nil for tombstoned schema")
	}
}

// TestSchemaCacheMemoryFootprint tests memory usage requirements
func TestSchemaCacheMemoryFootprint(t *testing.T) {
	cache := schema.NewSchemaCache()

	definition := &schema.GraphQLSchemaDefinition{
		TypeName: "MemoryTestType",
		Fields: []schema.GraphQLField{
			{Name: "id", Type: "ID!"},
			{Name: "field1", Type: "String"},
			{Name: "field2", Type: "Int"},
			{Name: "field3", Type: "Boolean"},
		},
	}

	var schemaID, bundleID [16]byte

	// Add 100 schemas
	for i := 0; i < 100; i++ {
		bundleName := string(rune('a'+i%26)) + string(rune('0'+i/26))
		copy(schemaID[:], []byte("schema-"+bundleName))
		copy(bundleID[:], []byte("bundle-"+bundleName))
		record := schema.NewSchemaRecord(schemaID, bundleID, "testdb", bundleName, 1, definition)
		cache.Set(bundleName, record)
	}

	stats := cache.GetStats()

	// Requirement: < 100KB per cached schema
	avgMemoryPerSchema := stats.MemoryBytes / uint64(stats.TotalEntries)
	maxMemoryPerSchema := uint64(100 * 1024) // 100KB

	if avgMemoryPerSchema > maxMemoryPerSchema {
		t.Errorf("Memory footprint too high: %d bytes per schema (max: %d)",
			avgMemoryPerSchema, maxMemoryPerSchema)
	}

	t.Logf("Memory stats: %d schemas, %d bytes total, %d bytes per schema",
		stats.TotalEntries, stats.MemoryBytes, avgMemoryPerSchema)
}

// TestSchemaCachePreloadBundles tests selective bundle preloading
func TestSchemaCachePreloadBundles(t *testing.T) {
	tmpFile := "/tmp/test_preload_bundles.gql"
	defer os.Remove(tmpFile)

	manager, err := schema.NewSchemaManager(tmpFile, "testdb", "test1234")
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}
	defer manager.Close()

	// Add multiple schemas
	definition := &schema.GraphQLSchemaDefinition{
		TypeName: "SelectivePreload",
		Fields:   []schema.GraphQLField{{Name: "id", Type: "ID!"}},
	}

	var schemaID, bundleID [16]byte
	bundles := []string{"users", "posts", "comments", "likes", "shares"}

	for _, bundleName := range bundles {
		copy(schemaID[:], []byte("schema-"+bundleName))
		copy(bundleID[:], []byte("bundle-"+bundleName))
		err = manager.AddNewSchema(schemaID, bundleID, bundleName, definition)
		if err != nil {
			t.Fatalf("Failed to add schema: %v", err)
		}
	}

	// Clear cache
	manager.ClearCache()

	// Preload only specific bundles
	targetBundles := []string{"users", "posts", "comments"}
	loaded, err := manager.PreloadBundles(targetBundles)
	if err != nil {
		t.Fatalf("PreloadBundles failed: %v", err)
	}

	if loaded != 3 {
		t.Errorf("Expected 3 bundles loaded, got %d", loaded)
	}

	// Verify only target bundles are in cache
	stats := manager.GetCacheStats()
	if stats.TotalEntries != 3 {
		t.Errorf("Expected 3 cache entries, got %d", stats.TotalEntries)
	}

	// Verify specific bundles are cached
	for _, bundleName := range targetBundles {
		schema, err := manager.GetCachedSchema(bundleName)
		if err != nil || schema == nil {
			t.Errorf("Expected bundle %s to be cached", bundleName)
		}
	}
}

// TestSchemaCacheConcurrency tests thread-safe concurrent access
func TestSchemaCacheConcurrency(t *testing.T) {
	cache := schema.NewSchemaCache()

	definition := &schema.GraphQLSchemaDefinition{
		TypeName: "ConcurrencyTest",
		Fields:   []schema.GraphQLField{{Name: "id", Type: "ID!"}},
	}

	var schemaID, bundleID [16]byte
	copy(schemaID[:], []byte("schema-id-000001"))
	copy(bundleID[:], []byte("bundle-id-000001"))
	record := schema.NewSchemaRecord(schemaID, bundleID, "testdb", "concurrent", 1, definition)

	// Add initial entry
	cache.Set("concurrent", record)

	// Launch concurrent readers and writers
	done := make(chan bool)
	iterations := 1000

	// Readers
	for i := 0; i < 10; i++ {
		go func() {
			for j := 0; j < iterations; j++ {
				cache.Get("concurrent")
			}
			done <- true
		}()
	}

	// Writers
	for i := 0; i < 5; i++ {
		go func(id int) {
			for j := 0; j < iterations; j++ {
				bundleName := "concurrent"
				copy(schemaID[:], []byte("schema-"+string(rune('0'+id))))
				newRecord := schema.NewSchemaRecord(schemaID, bundleID, "testdb", bundleName, 1, definition)
				cache.Set(bundleName, newRecord)
			}
			done <- true
		}(i)
	}

	// Invalidators
	for i := 0; i < 2; i++ {
		go func() {
			for j := 0; j < iterations; j++ {
				cache.Invalidate("concurrent")
			}
			done <- true
		}()
	}

	// Wait for all goroutines
	for i := 0; i < 17; i++ {
		<-done
	}

	// If we get here without deadlock or panic, test passes
	t.Log("Concurrent access test completed successfully")

	stats := cache.GetStats()
	t.Logf("Final stats: Hits=%d, Misses=%d, Invalidations=%d",
		stats.Hits, stats.Misses, stats.Invalidations)
}
