package main

import (
	"os"
	"testing"
	"time"

	"syndrdb/src/internal/graphQL/schema"
)

func TestSchemaManagerAddNewSchema(t *testing.T) {
	tmpFile := "/tmp/test_schema_manager.gql"
	defer os.Remove(tmpFile)

	manager, err := schema.NewSchemaManager(tmpFile, "testdb", "test1234")
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}
	defer manager.Close()

	// Create test schema definition
	definition := &schema.GraphQLSchemaDefinition{
		TypeName:    "User",
		Description: "User type",
		Fields: []schema.GraphQLField{
			{Name: "id", Type: "ID!", BundleField: "DocumentID"},
			{Name: "name", Type: "String!", BundleField: "name"},
		},
	}

	// Add first version
	var schemaID, bundleID [16]byte
	copy(schemaID[:], []byte("schema-id-000001"))
	copy(bundleID[:], []byte("bundle-id-000001"))

	err = manager.AddNewSchema(schemaID, bundleID, "users", definition)
	if err != nil {
		t.Fatalf("Failed to add schema: %v", err)
	}

	// Verify version is 1
	latestVersion, err := manager.GetLatestVersionForBundle("users")
	if err != nil {
		t.Fatalf("Failed to get latest version: %v", err)
	}
	if latestVersion != 1 {
		t.Errorf("Expected version 1, got %d", latestVersion)
	}

	// Add second version
	copy(schemaID[:], []byte("schema-id-000002"))
	err = manager.AddNewSchema(schemaID, bundleID, "users", definition)
	if err != nil {
		t.Fatalf("Failed to add second schema: %v", err)
	}

	// Verify version is 2
	latestVersion, err = manager.GetLatestVersionForBundle("users")
	if err != nil {
		t.Fatalf("Failed to get latest version: %v", err)
	}
	if latestVersion != 2 {
		t.Errorf("Expected version 2, got %d", latestVersion)
	}
}

func TestSchemaManagerTombstoneSchema(t *testing.T) {
	tmpFile := "/tmp/test_tombstone.gql"
	defer os.Remove(tmpFile)

	manager, err := schema.NewSchemaManager(tmpFile, "testdb", "test1234")
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}
	defer manager.Close()

	// Add a schema
	definition := &schema.GraphQLSchemaDefinition{
		TypeName: "Post",
		Fields:   []schema.GraphQLField{{Name: "id", Type: "ID!"}},
	}

	var schemaID, bundleID [16]byte
	copy(schemaID[:], []byte("schema-id-000001"))
	copy(bundleID[:], []byte("bundle-id-000001"))

	err = manager.AddNewSchema(schemaID, bundleID, "posts", definition)
	if err != nil {
		t.Fatalf("Failed to add schema: %v", err)
	}

	// Tombstone it
	err = manager.TombstoneSchema("posts", 1)
	if err != nil {
		t.Fatalf("Failed to tombstone schema: %v", err)
	}

	// Verify it's tombstoned (should not be in active schemas)
	_, err = manager.GetActiveSchemaForBundle("posts")
	if err == nil {
		t.Error("Expected error getting tombstoned schema, got nil")
	}

	// But should still exist in version history
	history, err := manager.GetVersionHistory("posts")
	if err != nil {
		t.Fatalf("Failed to get version history: %v", err)
	}
	if len(history) < 2 {
		t.Errorf("Expected at least 2 records (active + tombstone), got %d", len(history))
	}
}

func TestSchemaManagerUpdateSchema(t *testing.T) {
	tmpFile := "/tmp/test_update.gql"
	defer os.Remove(tmpFile)

	manager, err := schema.NewSchemaManager(tmpFile, "testdb", "test1234")
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}
	defer manager.Close()

	// Add initial schema (version 1)
	definition1 := &schema.GraphQLSchemaDefinition{
		TypeName: "Product",
		Fields: []schema.GraphQLField{
			{Name: "id", Type: "ID!"},
			{Name: "name", Type: "String!"},
		},
	}

	var schemaID, bundleID [16]byte
	copy(schemaID[:], []byte("schema-id-000001"))
	copy(bundleID[:], []byte("bundle-id-000001"))

	err = manager.AddNewSchema(schemaID, bundleID, "products", definition1)
	if err != nil {
		t.Fatalf("Failed to add initial schema: %v", err)
	}

	// Update schema (should create version 2 and tombstone version 1)
	definition2 := &schema.GraphQLSchemaDefinition{
		TypeName: "Product",
		Fields: []schema.GraphQLField{
			{Name: "id", Type: "ID!"},
			{Name: "name", Type: "String!"},
			{Name: "price", Type: "Float!"},
		},
	}

	copy(schemaID[:], []byte("schema-id-000002"))
	err = manager.UpdateSchema(schemaID, bundleID, "products", definition2)
	if err != nil {
		t.Fatalf("Failed to update schema: %v", err)
	}

	// Verify version 2 is active
	latestVersion, err := manager.GetLatestVersionForBundle("products")
	if err != nil {
		t.Fatalf("Failed to get latest version: %v", err)
	}
	if latestVersion != 2 {
		t.Errorf("Expected version 2, got %d", latestVersion)
	}

	// Verify version 1 is tombstoned
	activeSchema, err := manager.GetActiveSchemaForBundle("products")
	if err != nil {
		t.Fatalf("Failed to get active schema: %v", err)
	}
	if activeSchema.SchemaVersion != 2 {
		t.Errorf("Expected active version 2, got %d", activeSchema.SchemaVersion)
	}
}

func TestSchemaManagerGetVersionHistory(t *testing.T) {
	tmpFile := "/tmp/test_history.gql"
	defer os.Remove(tmpFile)

	manager, err := schema.NewSchemaManager(tmpFile, "testdb", "test1234")
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}
	defer manager.Close()

	// Add multiple versions
	definition := &schema.GraphQLSchemaDefinition{
		TypeName: "Order",
		Fields:   []schema.GraphQLField{{Name: "id", Type: "ID!"}},
	}

	var schemaID, bundleID [16]byte
	copy(bundleID[:], []byte("bundle-id-000001"))

	for i := 1; i <= 3; i++ {
		copy(schemaID[:], []byte("schema-id-00000"+string(rune('0'+i))))
		err = manager.AddNewSchema(schemaID, bundleID, "orders", definition)
		if err != nil {
			t.Fatalf("Failed to add schema version %d: %v", i, err)
		}
		time.Sleep(10 * time.Millisecond) // Ensure different timestamps
	}

	// Get history
	history, err := manager.GetVersionHistory("orders")
	if err != nil {
		t.Fatalf("Failed to get version history: %v", err)
	}

	if len(history) != 3 {
		t.Errorf("Expected 3 versions, got %d", len(history))
	}

	// Verify versions are in order
	for i, record := range history {
		expectedVersion := int64(i + 1)
		if record.SchemaVersion != expectedVersion {
			t.Errorf("Expected version %d at index %d, got %d", expectedVersion, i, record.SchemaVersion)
		}
	}
}

func TestSchemaManagerRetentionPolicy(t *testing.T) {
	tmpFile := "/tmp/test_retention.gql"
	defer os.Remove(tmpFile)

	manager, err := schema.NewSchemaManager(tmpFile, "testdb", "test1234")
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}
	defer manager.Close()

	// Get default policy
	policy := manager.GetRetentionPolicy()
	if policy.RetentionSeconds != 604800 { // 7 days
		t.Errorf("Expected default retention 604800s, got %d", policy.RetentionSeconds)
	}

	// Update policy
	newPolicy := schema.RetentionPolicy{
		RetentionSeconds:    86400, // 1 day
		CompactionThreshold: 0.5,   // 50%
	}

	err = manager.SetRetentionPolicy(newPolicy)
	if err != nil {
		t.Fatalf("Failed to set retention policy: %v", err)
	}

	// Verify update
	updated := manager.GetRetentionPolicy()
	if updated.RetentionSeconds != 86400 {
		t.Errorf("Expected retention 86400s, got %d", updated.RetentionSeconds)
	}
	if updated.CompactionThreshold != 0.5 {
		t.Errorf("Expected threshold 0.5, got %f", updated.CompactionThreshold)
	}
}

func TestSchemaManagerTombstoneAllSchemasForBundle(t *testing.T) {
	tmpFile := "/tmp/test_tombstone_all.gql"
	defer os.Remove(tmpFile)

	manager, err := schema.NewSchemaManager(tmpFile, "testdb", "test1234")
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}
	defer manager.Close()

	// Add multiple versions
	definition := &schema.GraphQLSchemaDefinition{
		TypeName: "Comment",
		Fields:   []schema.GraphQLField{{Name: "id", Type: "ID!"}},
	}

	var schemaID, bundleID [16]byte
	copy(bundleID[:], []byte("bundle-id-000001"))

	for i := 1; i <= 3; i++ {
		copy(schemaID[:], []byte("schema-id-00000"+string(rune('0'+i))))
		err = manager.AddNewSchema(schemaID, bundleID, "comments", definition)
		if err != nil {
			t.Fatalf("Failed to add schema: %v", err)
		}
	}

	// Tombstone all
	err = manager.TombstoneAllSchemasForBundle("comments")
	if err != nil {
		t.Fatalf("Failed to tombstone all: %v", err)
	}

	// Verify no active schemas
	_, err = manager.GetActiveSchemaForBundle("comments")
	if err == nil {
		t.Error("Expected error getting active schema after tombstoning all, got nil")
	}

	// But history should still exist
	history, err := manager.GetVersionHistory("comments")
	if err != nil {
		t.Fatalf("Failed to get history: %v", err)
	}
	if len(history) < 3 {
		t.Errorf("Expected at least 3 records in history, got %d", len(history))
	}
}

func TestSchemaRecordSerializeDeserialize(t *testing.T) {
	// Create a simple record
	definition := &schema.GraphQLSchemaDefinition{
		TypeName: "Test",
		Fields:   []schema.GraphQLField{{Name: "id", Type: "ID!"}},
	}

	var schemaID, bundleID [16]byte
	copy(schemaID[:], []byte("test-schema-id"))
	copy(bundleID[:], []byte("test-bundle-id"))

	record := schema.NewSchemaRecord(schemaID, bundleID, "testdb", "testbundle", 1, definition)

	// Serialize
	data, err := record.Serialize()
	if err != nil {
		t.Fatalf("Serialize failed: %v", err)
	}

	t.Logf("Serialized %d bytes", len(data))
	t.Logf("RecordSize: %d", record.RecordSize)
	t.Logf("PayloadSize: %d", record.PayloadSize)
	t.Logf("Checksum: 0x%08X", record.RecordChecksum)

	// Deserialize
	record2 := &schema.SchemaRecord{}
	err = record2.Deserialize(data)
	if err != nil {
		t.Fatalf("Deserialize failed: %v", err)
	}

	if record.SchemaVersion != record2.SchemaVersion {
		t.Errorf("Version mismatch: %d != %d", record.SchemaVersion, record2.SchemaVersion)
	}
}
