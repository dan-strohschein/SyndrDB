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

// TestSerializeDeserializeV2Roundtrip verifies V2 format serialization/deserialization
func TestSerializeDeserializeV2Roundtrip(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Nanosecond)
	doc := &models.Document{
		DocumentID: "test-doc-001",
		Fields: map[string]models.Field{
			"name":    {Name: "name", Value: models.NewStringValue("John Doe")},
			"age":     {Name: "age", Value: models.NewIntValue(42)},
			"score":   {Name: "score", Value: models.NewFloatValue(98.6)},
			"active":  {Name: "active", Value: models.NewBoolValue(true)},
			"created": {Name: "created", Value: models.NewDateTimeValue(now)},
		},
		CreatedAt:       now,
		UpdatedAt:       now,
		CreatedByTxID:   100,
		DeletedByTxID:   0,
		CommitSequence:  50,
		VersionSequence: 1,
	}

	data, err := EncodeFastBinaryV2(doc)
	if err != nil {
		t.Fatalf("SerializeDocumentV2 failed: %v", err)
	}

	if DetectFormatVersion(data) != FormatVersionV2 {
		t.Errorf("Expected FormatVersionV2, got %d", DetectFormatVersion(data))
	}

	result, err := DecodeFastBinaryAuto(data)
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
	if len(result.Fields) != len(doc.Fields) {
		t.Errorf("Field count mismatch: got %d, want %d", len(result.Fields), len(doc.Fields))
	}

	if nameField, ok := result.Fields["name"]; ok {
		if nameField.Value.StringVal != "John Doe" {
			t.Errorf("name field mismatch: got %s, want John Doe", nameField.Value.StringVal)
		}
	} else {
		t.Error("name field not found")
	}

	if ageField, ok := result.Fields["age"]; ok {
		if ageField.Value.IntVal != 42 {
			t.Errorf("age field mismatch: got %d, want 42", ageField.Value.IntVal)
		}
	} else {
		t.Error("age field not found")
	}

	if scoreField, ok := result.Fields["score"]; ok {
		if scoreField.Value.FloatVal != 98.6 {
			t.Errorf("score field mismatch: got %f, want 98.6", scoreField.Value.FloatVal)
		}
	} else {
		t.Error("score field not found")
	}

	if activeField, ok := result.Fields["active"]; ok {
		if !activeField.Value.BoolVal {
			t.Error("active field should be true")
		}
	} else {
		t.Error("active field not found")
	}
}

// TestV2ProjectionPushdown verifies that projection reduces deserialized fields
func TestV2ProjectionPushdown(t *testing.T) {
	doc := &models.Document{
		DocumentID: "proj-test-001",
		Fields: map[string]models.Field{
			"field1": {Name: "field1", Value: models.NewStringValue("value1")},
			"field2": {Name: "field2", Value: models.NewStringValue("value2")},
			"field3": {Name: "field3", Value: models.NewStringValue("value3")},
			"field4": {Name: "field4", Value: models.NewStringValue("value4")},
			"field5": {Name: "field5", Value: models.NewStringValue("value5")},
		},
		CreatedAt: time.Now().UTC(),
		UpdatedAt: time.Now().UTC(),
	}

	data, err := EncodeFastBinaryV2(doc)
	if err != nil {
		t.Fatalf("Serialize failed: %v", err)
	}

	projection := []string{"field2", "field4"}
	result, err := DecodeFastBinaryV2Projected(data, projection)
	if err != nil {
		t.Fatalf("DeserializeV2 with projection failed: %v", err)
	}

	if len(result.Fields) != 2 {
		t.Errorf("Expected 2 fields with projection, got %d", len(result.Fields))
	}

	if _, ok := result.Fields["field2"]; !ok {
		t.Error("field2 should be present in projection result")
	}
	if _, ok := result.Fields["field4"]; !ok {
		t.Error("field4 should be present in projection result")
	}
	if _, ok := result.Fields["field1"]; ok {
		t.Error("field1 should NOT be present (not in projection)")
	}
}

// TestV2SingleFieldLookup verifies O(1) field lookup by hash
func TestV2SingleFieldLookup(t *testing.T) {
	doc := &models.Document{
		DocumentID: "lookup-test-001",
		Fields: map[string]models.Field{
			"target_field": {Name: "target_field", Value: models.NewIntValue(12345)},
			"other_field":  {Name: "other_field", Value: models.NewStringValue("not this one")},
		},
		CreatedAt: time.Now().UTC(),
		UpdatedAt: time.Now().UTC(),
	}

	data, err := EncodeFastBinaryV2(doc)
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

// BenchmarkSerializeV2 measures V2 serialization performance
func BenchmarkSerializeV2(b *testing.B) {
	doc := &models.Document{
		DocumentID: "bench-doc",
		Fields: map[string]models.Field{
			"name":    {Name: "name", Value: models.NewStringValue("Test User")},
			"email":   {Name: "email", Value: models.NewStringValue("test@example.com")},
			"age":     {Name: "age", Value: models.NewIntValue(30)},
			"balance": {Name: "balance", Value: models.NewFloatValue(1234.56)},
			"active":  {Name: "active", Value: models.NewBoolValue(true)},
		},
		CreatedAt: time.Now().UTC(),
		UpdatedAt: time.Now().UTC(),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := EncodeFastBinaryV2(doc)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkDeserializeV2Full measures full V2 deserialization
func BenchmarkDeserializeV2Full(b *testing.B) {
	doc := &models.Document{
		DocumentID: "bench-doc",
		Fields: map[string]models.Field{
			"name":    {Name: "name", Value: models.NewStringValue("Test User")},
			"email":   {Name: "email", Value: models.NewStringValue("test@example.com")},
			"age":     {Name: "age", Value: models.NewIntValue(30)},
			"balance": {Name: "balance", Value: models.NewFloatValue(1234.56)},
			"active":  {Name: "active", Value: models.NewBoolValue(true)},
		},
		CreatedAt: time.Now().UTC(),
		UpdatedAt: time.Now().UTC(),
	}

	data, _ := EncodeFastBinaryV2(doc)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := DecodeFastBinaryAuto(data)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkDeserializeV2Projected measures projection deserialization (1 of 5 fields)
func BenchmarkDeserializeV2Projected(b *testing.B) {
	doc := &models.Document{
		DocumentID: "bench-doc",
		Fields: map[string]models.Field{
			"name":    {Name: "name", Value: models.NewStringValue("Test User")},
			"email":   {Name: "email", Value: models.NewStringValue("test@example.com")},
			"age":     {Name: "age", Value: models.NewIntValue(30)},
			"balance": {Name: "balance", Value: models.NewFloatValue(1234.56)},
			"active":  {Name: "active", Value: models.NewBoolValue(true)},
		},
		CreatedAt: time.Now().UTC(),
		UpdatedAt: time.Now().UTC(),
	}

	data, _ := EncodeFastBinaryV2(doc)
	projection := []string{"name"}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := DecodeFastBinaryV2Projected(data, projection)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkLookupFieldByHash measures single field hash lookup
func BenchmarkLookupFieldByHash(b *testing.B) {
	doc := &models.Document{
		DocumentID: "bench-doc",
		Fields: map[string]models.Field{
			"name":    {Name: "name", Value: models.NewStringValue("Test User")},
			"email":   {Name: "email", Value: models.NewStringValue("test@example.com")},
			"age":     {Name: "age", Value: models.NewIntValue(30)},
			"balance": {Name: "balance", Value: models.NewFloatValue(1234.56)},
			"active":  {Name: "active", Value: models.NewBoolValue(true)},
		},
		CreatedAt: time.Now().UTC(),
		UpdatedAt: time.Now().UTC(),
	}

	data, _ := EncodeFastBinaryV2(doc)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := LookupFieldByHashV2(data, "balance")
		if err != nil {
			b.Fatal(err)
		}
	}
}

// TestProjectionHashCache verifies cached projection works correctly
func TestProjectionHashCache(t *testing.T) {
	doc := &models.Document{
		DocumentID: "cache-test",
		Fields: map[string]models.Field{
			"name":     {Name: "name", Value: models.NewStringValue("Cache User")},
			"email":    {Name: "email", Value: models.NewStringValue("cache@test.com")},
			"priority": {Name: "priority", Value: models.NewIntValue(100)},
		},
		CreatedAt: time.Now().UTC(),
		UpdatedAt: time.Now().UTC(),
	}

	data, err := EncodeFastBinaryV2(doc)
	if err != nil {
		t.Fatal(err)
	}

	// Create cache with subset of fields
	cache := NewProjectionHashCache([]string{"name", "priority"})

	// Test HasField
	if !cache.HasField("name") {
		t.Error("Cache should have 'name' field")
	}
	if cache.HasField("nonexistent") {
		t.Error("Cache should not have 'nonexistent' field")
	}

	// Test LookupField
	field, err := cache.LookupField(data, "priority")
	if err != nil {
		t.Fatalf("LookupField failed: %v", err)
	}
	if field == nil || field.Value.IntVal != 100 {
		t.Error("LookupField returned wrong value")
	}

	// Test DeserializeWithCachedProjection
	result, err := cache.DeserializeWithCachedProjection(data)
	if err != nil {
		t.Fatalf("DeserializeWithCachedProjection failed: %v", err)
	}

	if len(result.Fields) != 2 {
		t.Errorf("Expected 2 fields from projection, got %d", len(result.Fields))
	}
	if _, ok := result.Fields["name"]; !ok {
		t.Error("name should be in projected result")
	}
	if _, ok := result.Fields["priority"]; !ok {
		t.Error("priority should be in projected result")
	}
	if _, ok := result.Fields["email"]; ok {
		t.Error("email should NOT be in projected result")
	}
}

// BenchmarkProjectionHashCache measures cached projection lookup
func BenchmarkProjectionHashCache(b *testing.B) {
	doc := &models.Document{
		DocumentID: "bench-doc",
		Fields: map[string]models.Field{
			"name":    {Name: "name", Value: models.NewStringValue("Test User")},
			"email":   {Name: "email", Value: models.NewStringValue("test@example.com")},
			"age":     {Name: "age", Value: models.NewIntValue(30)},
			"balance": {Name: "balance", Value: models.NewFloatValue(1234.56)},
			"active":  {Name: "active", Value: models.NewBoolValue(true)},
		},
		CreatedAt: time.Now().UTC(),
		UpdatedAt: time.Now().UTC(),
	}

	data, _ := EncodeFastBinaryV2(doc)
	cache := NewProjectionHashCache([]string{"name", "balance"})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := cache.DeserializeWithCachedProjection(data)
		if err != nil {
			b.Fatal(err)
		}
	}
}
