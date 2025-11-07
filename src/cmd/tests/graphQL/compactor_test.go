package main

import (
	"os"
	"testing"
	"time"

	"syndrdb/src/internal/graphQL/schema"
)

func TestCompactorBasicCompaction(t *testing.T) {
	tmpFile := "/tmp/test_compaction.gql"
	defer os.Remove(tmpFile)

	manager, err := schema.NewSchemaManager(tmpFile, "testdb", "test1234")
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}
	defer manager.Close()

	// Add some schemas
	definition := &schema.GraphQLSchemaDefinition{
		TypeName: "User",
		Fields:   []schema.GraphQLField{{Name: "id", Type: "ID!"}},
	}

	var schemaID, bundleID [16]byte
	copy(bundleID[:], []byte("bundle-id-000001"))

	// Set short retention for testing (1 second) - MUST be done BEFORE creating tombstones
	policy := schema.RetentionPolicy{
		RetentionSeconds:    1,
		CompactionThreshold: 0.3,
	}
	err = manager.SetRetentionPolicy(policy)
	if err != nil {
		t.Fatalf("Failed to set retention policy: %v", err)
	}

	// Add 10 versions
	for i := 1; i <= 10; i++ {
		copy(schemaID[:], []byte("schema-id-00000"+string(rune('0'+i%10))))
		err = manager.AddNewSchema(schemaID, bundleID, "users", definition)
		if err != nil {
			t.Fatalf("Failed to add schema %d: %v", i, err)
		}
	}

	// Tombstone the first 8 versions (leaving 2 active)
	for i := 1; i <= 8; i++ {
		err = manager.TombstoneSchema("users", int64(i))
		if err != nil {
			t.Fatalf("Failed to tombstone version %d: %v", i, err)
		}
	}

	// Wait for tombstones to expire
	time.Sleep(2 * time.Second)

	// Get stats before compaction
	statsBefore := manager.GetStats()
	t.Logf("Before compaction: Total=%d, Active=%d, Tombstones=%d",
		statsBefore.TotalRecords, statsBefore.ActiveSchemas, statsBefore.TombstoneRecords)

	// Perform compaction
	result, err := manager.Compact()
	if err != nil {
		t.Fatalf("Compaction failed: %v", err)
	}

	t.Logf("Compaction result: %d bytes saved (%.1f%% reduction)",
		result.BytesSaved, result.PercentReduction)
	t.Logf("Records: before=%d, after=%d, purged=%d",
		result.RecordsBefore, result.RecordsAfter, result.RecordsPurged)

	// Verify active schemas preserved
	if result.ActivePreserved != 2 {
		t.Errorf("Expected 2 active schemas preserved, got %d", result.ActivePreserved)
	}

	// Verify tombstones purged
	if result.TombstonesPurged != 8 {
		t.Errorf("Expected 8 tombstones purged, got %d", result.TombstonesPurged)
	}

	// Verify file size reduced
	if result.BytesSaved <= 0 {
		t.Error("Expected positive bytes saved")
	}

	// Verify active schemas still accessible
	activeSchema, err := manager.GetActiveSchemaForBundle("users")
	if err != nil {
		t.Fatalf("Failed to get active schema after compaction: %v", err)
	}
	if activeSchema.SchemaVersion != 10 {
		t.Errorf("Expected active version 10, got %d", activeSchema.SchemaVersion)
	}
}

func TestCompactorNoCompactionNeeded(t *testing.T) {
	tmpFile := "/tmp/test_no_compaction.gql"
	defer os.Remove(tmpFile)

	manager, err := schema.NewSchemaManager(tmpFile, "testdb", "test1234")
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}
	defer manager.Close()

	// Add just a few active schemas (no tombstones)
	definition := &schema.GraphQLSchemaDefinition{
		TypeName: "Product",
		Fields:   []schema.GraphQLField{{Name: "id", Type: "ID!"}},
	}

	var schemaID, bundleID [16]byte
	copy(bundleID[:], []byte("bundle-id-000001"))

	for i := 1; i <= 3; i++ {
		copy(schemaID[:], []byte("schema-id-00000"+string(rune('0'+i))))
		err = manager.AddNewSchema(schemaID, bundleID, "products", definition)
		if err != nil {
			t.Fatalf("Failed to add schema: %v", err)
		}
	}

	// Check if compaction is needed
	if manager.NeedsCompaction() {
		t.Error("Should not need compaction with no tombstones")
	}

	// Try compaction anyway
	result, err := manager.CompactIfNeeded()
	if err != nil {
		t.Fatalf("CompactIfNeeded failed: %v", err)
	}

	// Should have done nothing
	if result.RecordsPurged > 0 {
		t.Errorf("Expected 0 records purged, got %d", result.RecordsPurged)
	}
}

func TestCompactorRetentionWindow(t *testing.T) {
	tmpFile := "/tmp/test_retention.gql"
	defer os.Remove(tmpFile)

	manager, err := schema.NewSchemaManager(tmpFile, "testdb", "test1234")
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}
	defer manager.Close()

	// Set 10-second retention
	policy := schema.RetentionPolicy{
		RetentionSeconds:    10,
		CompactionThreshold: 0.3,
	}
	err = manager.SetRetentionPolicy(policy)
	if err != nil {
		t.Fatalf("Failed to set retention policy: %v", err)
	}

	// Add and tombstone some schemas
	definition := &schema.GraphQLSchemaDefinition{
		TypeName: "Order",
		Fields:   []schema.GraphQLField{{Name: "id", Type: "ID!"}},
	}

	var schemaID, bundleID [16]byte
	copy(bundleID[:], []byte("bundle-id-000001"))

	for i := 1; i <= 5; i++ {
		copy(schemaID[:], []byte("schema-id-00000"+string(rune('0'+i))))
		err = manager.AddNewSchema(schemaID, bundleID, "orders", definition)
		if err != nil {
			t.Fatalf("Failed to add schema: %v", err)
		}
	}

	// Tombstone first 3
	for i := 1; i <= 3; i++ {
		err = manager.TombstoneSchema("orders", int64(i))
		if err != nil {
			t.Fatalf("Failed to tombstone: %v", err)
		}
	}

	// Compact immediately (tombstones should be retained)
	result, err := manager.Compact()
	if err != nil {
		t.Fatalf("Compaction failed: %v", err)
	}

	// Tombstones should be retained (within window)
	if result.TombstonesRetained != 3 {
		t.Errorf("Expected 3 tombstones retained, got %d", result.TombstonesRetained)
	}

	if result.TombstonesPurged != 0 {
		t.Errorf("Expected 0 tombstones purged, got %d", result.TombstonesPurged)
	}
}

func TestCompactorAtomicity(t *testing.T) {
	tmpFile := "/tmp/test_atomic.gql"
	defer os.Remove(tmpFile)

	manager, err := schema.NewSchemaManager(tmpFile, "testdb", "test1234")
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}
	defer manager.Close()

	// Add schemas
	definition := &schema.GraphQLSchemaDefinition{
		TypeName: "Comment",
		Fields:   []schema.GraphQLField{{Name: "id", Type: "ID!"}},
	}

	var schemaID, bundleID [16]byte
	copy(bundleID[:], []byte("bundle-id-000001"))

	for i := 1; i <= 5; i++ {
		copy(schemaID[:], []byte("schema-id-00000"+string(rune('0'+i))))
		err = manager.AddNewSchema(schemaID, bundleID, "comments", definition)
		if err != nil {
			t.Fatalf("Failed to add schema: %v", err)
		}
	}

	// Perform compaction
	result, err := manager.Compact()
	if err != nil {
		t.Fatalf("Compaction failed: %v", err)
	}

	// Verify file is valid after compaction
	err = manager.Close()
	if err != nil {
		t.Fatalf("Failed to close: %v", err)
	}

	// Reopen and verify integrity
	manager2, err := schema.NewSchemaManager(tmpFile, "testdb", "test1234")
	if err != nil {
		t.Fatalf("Failed to reopen after compaction: %v", err)
	}
	defer manager2.Close()

	// Verify active schemas
	activeSchemas, err := manager2.GetVersionHistory("comments")
	if err != nil {
		t.Fatalf("Failed to get history: %v", err)
	}

	activeCount := 0
	for _, schema := range activeSchemas {
		if schema.IsActive() {
			activeCount++
		}
	}

	if int64(activeCount) != result.ActivePreserved {
		t.Errorf("Active count mismatch: expected %d, got %d",
			result.ActivePreserved, activeCount)
	}

	t.Logf("Atomicity verified: %d active schemas preserved", activeCount)
}

func TestCompactorPerformance(t *testing.T) {
	tmpFile := "/tmp/test_performance.gql"
	defer os.Remove(tmpFile)

	manager, err := schema.NewSchemaManager(tmpFile, "testdb", "test1234")
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}
	defer manager.Close()

	// Set very short retention
	policy := schema.RetentionPolicy{
		RetentionSeconds:    1,
		CompactionThreshold: 0.3,
	}
	err = manager.SetRetentionPolicy(policy)
	if err != nil {
		t.Fatalf("Failed to set retention policy: %v", err)
	}

	// Add 100 schemas across 10 bundles
	definition := &schema.GraphQLSchemaDefinition{
		TypeName: "TestType",
		Fields:   []schema.GraphQLField{{Name: "id", Type: "ID!"}},
	}

	var schemaID, bundleID [16]byte

	for bundle := 1; bundle <= 10; bundle++ {
		bundleName := "bundle" + string(rune('0'+bundle))
		copy(bundleID[:], []byte("bundle-"+string(rune('0'+bundle))))

		for version := 1; version <= 10; version++ {
			copy(schemaID[:], []byte("schema-v"+string(rune('0'+version))))
			err = manager.AddNewSchema(schemaID, bundleID, bundleName, definition)
			if err != nil {
				t.Fatalf("Failed to add schema: %v", err)
			}

			// Tombstone older versions
			if version > 2 {
				err = manager.TombstoneSchema(bundleName, int64(version-2))
				if err != nil {
					t.Fatalf("Failed to tombstone: %v", err)
				}
			}
		}
	}

	// Wait for tombstones to expire
	time.Sleep(2 * time.Second)

	// Measure compaction time
	start := time.Now()
	result, err := manager.Compact()
	duration := time.Since(start)

	if err != nil {
		t.Fatalf("Compaction failed: %v", err)
	}

	t.Logf("Compaction of ~100 records took %v", duration)
	t.Logf("Active preserved: %d, Tombstones purged: %d",
		result.ActivePreserved, result.TombstonesPurged)

	// Should complete reasonably fast (< 1 second for 100 records)
	if duration > 1*time.Second {
		t.Errorf("Compaction took too long: %v (expected < 1s)", duration)
	}

	// Verify reduction
	if result.PercentReduction < 10 {
		t.Logf("Warning: Low reduction percentage: %.1f%%", result.PercentReduction)
	} else {
		t.Logf("Good reduction: %.1f%%", result.PercentReduction)
	}
}

func TestCompactionStats(t *testing.T) {
	tmpFile := "/tmp/test_stats.gql"
	defer os.Remove(tmpFile)

	manager, err := schema.NewSchemaManager(tmpFile, "testdb", "test1234")
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}
	defer manager.Close()

	// Add schemas with tombstones
	definition := &schema.GraphQLSchemaDefinition{
		TypeName: "Stats",
		Fields:   []schema.GraphQLField{{Name: "id", Type: "ID!"}},
	}

	var schemaID, bundleID [16]byte
	copy(bundleID[:], []byte("bundle-id-000001"))

	for i := 1; i <= 10; i++ {
		copy(schemaID[:], []byte("schema-id-00000"+string(rune('0'+i%10))))
		err = manager.AddNewSchema(schemaID, bundleID, "stats", definition)
		if err != nil {
			t.Fatalf("Failed to add schema: %v", err)
		}
	}

	// Tombstone half
	for i := 1; i <= 5; i++ {
		err = manager.TombstoneSchema("stats", int64(i))
		if err != nil {
			t.Fatalf("Failed to tombstone: %v", err)
		}
	}

	// Get compaction stats
	stats, err := manager.GetCompactionStats()
	if err != nil {
		t.Fatalf("Failed to get stats: %v", err)
	}

	t.Logf("Compaction stats:")
	t.Logf("  File size: %d bytes", stats.FileSize)
	t.Logf("  Total records: %d", stats.TotalRecords)
	t.Logf("  Active records: %d", stats.ActiveRecords)
	t.Logf("  Tombstone records: %d", stats.TombstoneRecords)
	t.Logf("  Expired tombstones: %d", stats.ExpiredTombstones)
	t.Logf("  Needs compaction: %v", stats.NeedsCompaction)
	t.Logf("  Tombstone ratio: %.2f", stats.TombstoneRatio)

	// Verify counts
	if stats.ActiveRecords != 5 {
		t.Errorf("Expected 5 active records, got %d", stats.ActiveRecords)
	}

	if stats.TombstoneRecords != 5 {
		t.Errorf("Expected 5 tombstone records, got %d", stats.TombstoneRecords)
	}
}
