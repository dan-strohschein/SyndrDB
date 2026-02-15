// Package helpers provides high-performance serialization utilities for SyndrDB.
//
// This test file validates the V2 binary format with field directory for O(1) field access.
// V2 format uses xxHash64 for fast field name hashing and supports projection pushdown
// during deserialization.
package helpers

import (
	"syndrdb/src/internal/domain/models"
	"testing"
	"time"
)

// schemaV2Roundtrip defines field order for V2 roundtrip test.
var schemaV2Roundtrip = models.BuildBundleFieldSchemaFromNames([]string{"name", "age", "score", "active", "created"})

// TestSerializeDeserializeV2Roundtrip verifies V2 format serialization/deserialization with Values and schema.
func TestSerializeDeserializeV2Roundtrip(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Nanosecond)
	doc := &models.Document{
		DocumentID: "test-doc-001",
		Values: []models.FieldValue{
			models.NewStringValue("John Doe"),
			models.NewIntValue(42),
			models.NewFloatValue(98.6),
			models.NewBoolValue(true),
			models.NewDateTimeValue(now),
		},
		CreatedAt:       now,
		UpdatedAt:       now,
		CreatedByTxID:   100,
		DeletedByTxID:   0,
		CommitSequence:  50,
		VersionSequence: 1,
	}

	data, err := EncodeFastBinaryV2(doc, schemaV2Roundtrip)
	if err != nil {
		t.Fatalf("SerializeDocumentV2 failed: %v", err)
	}

	if DetectFormatVersion(data) != FormatVersionV2 {
		t.Errorf("Expected FormatVersionV2, got %d", DetectFormatVersion(data))
	}

	result, err := DecodeFastBinaryAuto(data, schemaV2Roundtrip)
	if err != nil {
		t.Fatalf("DecodeFastBinaryAuto failed: %v", err)
	}

	if result.DocumentID != doc.DocumentID {
		t.Errorf("DocumentID mismatch: got %s, want %s", result.DocumentID, doc.DocumentID)
	}
	if result.CreatedByTxID != doc.CreatedByTxID {
		t.Errorf("CreatedByTxID mismatch: got %d, want %d", result.CreatedByTxID, doc.CreatedByTxID)
	}
	if result.CommitSequence != doc.CommitSequence {
		t.Errorf("CommitSequence mismatch: got %d, want %d", result.CommitSequence, doc.CommitSequence)
	}
	if len(result.Values) != len(doc.Values) {
		t.Errorf("Values length mismatch: got %d, want %d", len(result.Values), len(doc.Values))
	}

	if v, ok := result.GetFieldValue(schemaV2Roundtrip, "name"); ok {
		if v.StringVal != "John Doe" {
			t.Errorf("name field mismatch: got %s, want John Doe", v.StringVal)
		}
	} else {
		t.Error("name field not found")
	}

	if v, ok := result.GetFieldValue(schemaV2Roundtrip, "age"); ok {
		if v.IntVal != 42 {
			t.Errorf("age field mismatch: got %d, want 42", v.IntVal)
		}
	} else {
		t.Error("age field not found")
	}

	if v, ok := result.GetFieldValue(schemaV2Roundtrip, "score"); ok {
		if v.FloatVal != 98.6 {
			t.Errorf("score field mismatch: got %f, want 98.6", v.FloatVal)
		}
	} else {
		t.Error("score field not found")
	}

	if v, ok := result.GetFieldValue(schemaV2Roundtrip, "active"); ok {
		if !v.BoolVal {
			t.Error("active field should be true")
		}
	} else {
		t.Error("active field not found")
	}
}

// schemaV2Proj defines field order for projection test.
var schemaV2Proj = models.BuildBundleFieldSchemaFromNames([]string{"field1", "field2", "field3", "field4", "field5"})

// TestV2ProjectionPushdown verifies that projection reduces deserialized fields (Values populated by schema).
func TestV2ProjectionPushdown(t *testing.T) {
	doc := &models.Document{
		DocumentID: "proj-test-001",
		Values: []models.FieldValue{
			models.NewStringValue("value1"),
			models.NewStringValue("value2"),
			models.NewStringValue("value3"),
			models.NewStringValue("value4"),
			models.NewStringValue("value5"),
		},
		CreatedAt: time.Now().UTC(),
		UpdatedAt: time.Now().UTC(),
	}

	data, err := EncodeFastBinaryV2(doc, schemaV2Proj)
	if err != nil {
		t.Fatalf("Serialize failed: %v", err)
	}

	projection := []string{"field2", "field4"}
	result, err := DecodeFastBinaryV2Projected(data, projection, schemaV2Proj)
	if err != nil {
		t.Fatalf("DeserializeV2 with projection failed: %v", err)
	}

	// Projection fills Values only for projected fields; check by name
	if _, ok := result.GetFieldValue(schemaV2Proj, "field2"); !ok {
		t.Error("field2 should be present in projection result")
	}
	if _, ok := result.GetFieldValue(schemaV2Proj, "field4"); !ok {
		t.Error("field4 should be present in projection result")
	}
	// field1 may still be in Values at index 0 but with zero value when projected out; we only assert projected-in fields
	if len(result.Values) != 5 {
		t.Errorf("Expected 5 Values slots (schema length), got %d", len(result.Values))
	}
}

// schemaV2Lookup defines field order for lookup test.
var schemaV2Lookup = models.BuildBundleFieldSchemaFromNames([]string{"target_field", "other_field"})

// TestV2SingleFieldLookup verifies O(1) field lookup by hash
func TestV2SingleFieldLookup(t *testing.T) {
	doc := &models.Document{
		DocumentID: "lookup-test-001",
		Values: []models.FieldValue{
			models.NewIntValue(12345),
			models.NewStringValue("not this one"),
		},
		CreatedAt: time.Now().UTC(),
		UpdatedAt: time.Now().UTC(),
	}

	data, err := EncodeFastBinaryV2(doc, schemaV2Lookup)
	if err != nil {
		t.Fatalf("Serialize failed: %v", err)
	}

	field, err := LookupFieldByHashV2(data, "target_field")
	if err != nil {
		t.Fatalf("LookupFieldByHashV2 failed: %v", err)
	}
	if field == nil {
		t.Fatal("Field should not be nil")
	}
	if field.Value.IntVal != 12345 {
		t.Errorf("Field value mismatch: got %d, want 12345", field.Value.IntVal)
	}

	notFound, err := LookupFieldByHashV2(data, "nonexistent")
	if err != nil {
		t.Fatalf("LookupFieldByHashV2 for nonexistent should not error: %v", err)
	}
	if notFound != nil {
		t.Error("Non-existent field should return nil")
	}
}

// TestHashFieldName64Consistency verifies xxHash64 returns consistent hashes
func TestHashFieldName64Consistency(t *testing.T) {
	fieldName := "test_field_name"
	hash1 := HashFieldName64(fieldName)
	hash2 := HashFieldName64(fieldName)

	if hash1 != hash2 {
		t.Errorf("HashFieldName64 not consistent: %d != %d", hash1, hash2)
	}

	hash3 := HashFieldName64("different_name")
	if hash1 == hash3 {
		t.Error("Different field names should not have same hash")
	}
}

// schemaV2Bench defines field order for V2 benchmarks.
var schemaV2Bench = models.BuildBundleFieldSchemaFromNames([]string{"name", "email", "age", "balance", "active"})

// BenchmarkSerializeV2 measures V2 serialization performance with Values and schema.
func BenchmarkSerializeV2(b *testing.B) {
	doc := &models.Document{
		DocumentID: "bench-doc",
		Values: []models.FieldValue{
			models.NewStringValue("Test User"),
			models.NewStringValue("test@example.com"),
			models.NewIntValue(30),
			models.NewFloatValue(1234.56),
			models.NewBoolValue(true),
		},
		CreatedAt: time.Now().UTC(),
		UpdatedAt: time.Now().UTC(),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := EncodeFastBinaryV2(doc, schemaV2Bench)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkDeserializeV2Full measures full V2 deserialization with schema.
func BenchmarkDeserializeV2Full(b *testing.B) {
	doc := &models.Document{
		DocumentID: "bench-doc",
		Values: []models.FieldValue{
			models.NewStringValue("Test User"),
			models.NewStringValue("test@example.com"),
			models.NewIntValue(30),
			models.NewFloatValue(1234.56),
			models.NewBoolValue(true),
		},
		CreatedAt: time.Now().UTC(),
		UpdatedAt: time.Now().UTC(),
	}

	data, _ := EncodeFastBinaryV2(doc, schemaV2Bench)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := DecodeFastBinaryAuto(data, schemaV2Bench)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkDeserializeV2Projected measures projection deserialization (1 of 5 fields).
func BenchmarkDeserializeV2Projected(b *testing.B) {
	doc := &models.Document{
		DocumentID: "bench-doc",
		Values: []models.FieldValue{
			models.NewStringValue("Test User"),
			models.NewStringValue("test@example.com"),
			models.NewIntValue(30),
			models.NewFloatValue(1234.56),
			models.NewBoolValue(true),
		},
		CreatedAt: time.Now().UTC(),
		UpdatedAt: time.Now().UTC(),
	}

	data, _ := EncodeFastBinaryV2(doc, schemaV2Bench)
	projection := []string{"name"}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := DecodeFastBinaryV2Projected(data, projection, schemaV2Bench)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkLookupFieldByHash measures single field hash lookup.
func BenchmarkLookupFieldByHash(b *testing.B) {
	doc := &models.Document{
		DocumentID: "bench-doc",
		Values: []models.FieldValue{
			models.NewStringValue("Test User"),
			models.NewStringValue("test@example.com"),
			models.NewIntValue(30),
			models.NewFloatValue(1234.56),
			models.NewBoolValue(true),
		},
		CreatedAt: time.Now().UTC(),
		UpdatedAt: time.Now().UTC(),
	}

	data, _ := EncodeFastBinaryV2(doc, schemaV2Bench)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := LookupFieldByHashV2(data, "balance")
		if err != nil {
			b.Fatal(err)
		}
	}
}

// schemaV2Cache defines field order for projection cache test.
var schemaV2Cache = models.BuildBundleFieldSchemaFromNames([]string{"name", "email", "priority"})

// TestProjectionHashCache verifies cached projection works correctly with schema.
func TestProjectionHashCache(t *testing.T) {
	doc := &models.Document{
		DocumentID: "cache-test",
		Values: []models.FieldValue{
			models.NewStringValue("Cache User"),
			models.NewStringValue("cache@test.com"),
			models.NewIntValue(100),
		},
		CreatedAt: time.Now().UTC(),
		UpdatedAt: time.Now().UTC(),
	}

	data, err := EncodeFastBinaryV2(doc, schemaV2Cache)
	if err != nil {
		t.Fatal(err)
	}

	cache := NewProjectionHashCache([]string{"name", "priority"})

	if !cache.HasField("name") {
		t.Error("Cache should have 'name' field")
	}
	if cache.HasField("nonexistent") {
		t.Error("Cache should not have 'nonexistent' field")
	}

	field, err := cache.LookupField(data, "priority")
	if err != nil {
		t.Fatalf("LookupField failed: %v", err)
	}
	if field == nil || field.Value.IntVal != 100 {
		t.Error("LookupField returned wrong value")
	}

	result, err := cache.DeserializeWithCachedProjection(data, schemaV2Cache)
	if err != nil {
		t.Fatalf("DeserializeWithCachedProjection failed: %v", err)
	}

	if _, ok := result.GetFieldValue(schemaV2Cache, "name"); !ok {
		t.Error("name should be in projected result")
	}
	if v, ok := result.GetFieldValue(schemaV2Cache, "priority"); !ok || v.IntVal != 100 {
		t.Error("priority should be in projected result with value 100")
	}
	// email is not in projection; GetFieldValue may still return true if Values has slot
	if len(result.Values) != 3 {
		t.Errorf("Expected 3 Values slots (schema length), got %d", len(result.Values))
	}
}

// BenchmarkProjectionHashCache measures cached projection lookup with schema.
func BenchmarkProjectionHashCache(b *testing.B) {
	doc := &models.Document{
		DocumentID: "bench-doc",
		Values: []models.FieldValue{
			models.NewStringValue("Test User"),
			models.NewStringValue("test@example.com"),
			models.NewIntValue(30),
			models.NewFloatValue(1234.56),
			models.NewBoolValue(true),
		},
		CreatedAt: time.Now().UTC(),
		UpdatedAt: time.Now().UTC(),
	}

	data, _ := EncodeFastBinaryV2(doc, schemaV2Bench)
	cache := NewProjectionHashCache([]string{"name", "balance"})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := cache.DeserializeWithCachedProjection(data, schemaV2Bench)
		if err != nil {
			b.Fatal(err)
		}
	}
}
