// Package helpers provides high-performance utilities for SyndrDB.
//
// This test file validates the BatchEncoder for efficient multi-document
// serialization used in UPDATE operations.
package helpers

import (
	"syndrdb/src/internal/domain/models"
	"testing"
	"time"
)

// TestBatchEncoderRoundtrip verifies batch encode/decode cycle
func TestBatchEncoderRoundtrip(t *testing.T) {
	// Create test documents
	now := time.Now().UTC()
	docs := []*models.Document{
		{
			DocumentID: "doc-001",
			Fields: map[string]models.Field{
				"name": {Name: "name", Value: models.NewStringValue("Alice")},
				"age":  {Name: "age", Value: models.NewIntValue(30)},
			},
			CreatedAt: now,
			UpdatedAt: now,
		},
		{
			DocumentID: "doc-002",
			Fields: map[string]models.Field{
				"name": {Name: "name", Value: models.NewStringValue("Bob")},
				"age":  {Name: "age", Value: models.NewIntValue(25)},
			},
			CreatedAt: now,
			UpdatedAt: now,
		},
		{
			DocumentID: "doc-003",
			Fields: map[string]models.Field{
				"name":   {Name: "name", Value: models.NewStringValue("Charlie")},
				"age":    {Name: "age", Value: models.NewIntValue(35)},
				"active": {Name: "active", Value: models.NewBoolValue(true)},
			},
			CreatedAt: now,
			UpdatedAt: now,
		},
	}

	// Encode batch
	encoder := GetBatchEncoder()
	defer PutBatchEncoder(encoder)

	for _, doc := range docs {
		if err := encoder.AddDocument(doc); err != nil {
			t.Fatalf("AddDocument failed: %v", err)
		}
	}

	if encoder.Count() != 3 {
		t.Errorf("Expected 3 documents, got %d", encoder.Count())
	}

	batchData := encoder.Bytes()
	if len(batchData) == 0 {
		t.Fatal("Batch data is empty")
	}

	// Decode batch
	decoder, err := NewBatchDecoder(batchData)
	if err != nil {
		t.Fatalf("NewBatchDecoder failed: %v", err)
	}

	if decoder.Count() != 3 {
		t.Errorf("Decoded count mismatch: got %d, want 3", decoder.Count())
	}

	// Verify each document
	for i, originalDoc := range docs {
		decoded, err := decoder.GetDocument(i)
		if err != nil {
			t.Fatalf("GetDocument(%d) failed: %v", i, err)
		}

		if decoded.DocumentID != originalDoc.DocumentID {
			t.Errorf("Doc %d DocumentID mismatch: got %s, want %s",
				i, decoded.DocumentID, originalDoc.DocumentID)
		}

		if len(decoded.Fields) != len(originalDoc.Fields) {
			t.Errorf("Doc %d field count mismatch: got %d, want %d",
				i, len(decoded.Fields), len(originalDoc.Fields))
		}
	}
}

// TestBatchEncoderPool verifies pool functionality
func TestBatchEncoderPool(t *testing.T) {
	// Get encoder from pool
	encoder1 := GetBatchEncoder()
	if encoder1 == nil {
		t.Fatal("GetBatchEncoder returned nil")
	}

	// Add a document
	doc := &models.Document{
		DocumentID: "pool-test",
		Fields: map[string]models.Field{
			"test": {Name: "test", Value: models.NewStringValue("value")},
		},
		CreatedAt: time.Now().UTC(),
		UpdatedAt: time.Now().UTC(),
	}
	encoder1.AddDocument(doc)

	if encoder1.Count() != 1 {
		t.Errorf("Expected 1 document, got %d", encoder1.Count())
	}

	// Return to pool
	PutBatchEncoder(encoder1)

	// Get from pool again - should be reset
	encoder2 := GetBatchEncoder()
	if encoder2.Count() != 0 {
		t.Errorf("Pooled encoder not reset: count = %d", encoder2.Count())
	}

	PutBatchEncoder(encoder2)
}

// TestBatchDecoderIteration verifies iteration over batch
func TestBatchDecoderIteration(t *testing.T) {
	encoder := GetBatchEncoder()
	defer PutBatchEncoder(encoder)

	// Add 5 documents
	now := time.Now().UTC()
	for i := 0; i < 5; i++ {
		doc := &models.Document{
			DocumentID: "iter-doc",
			Fields: map[string]models.Field{
				"index": {Name: "index", Value: models.NewIntValue(int64(i))},
			},
			CreatedAt: now,
			UpdatedAt: now,
		}
		encoder.AddDocument(doc)
	}

	batchData := encoder.Bytes()
	decoder, err := NewBatchDecoder(batchData)
	if err != nil {
		t.Fatal(err)
	}

	// Iterate and verify
	processed := 0
	count, err := decoder.IterateDocuments(func(index int, doc *models.Document) bool {
		processed++
		if indexField, ok := doc.Fields["index"]; ok {
			if indexField.Value.IntVal != int64(index) {
				t.Errorf("Index mismatch at %d", index)
			}
		}
		return true // Continue iteration
	})

	if err != nil {
		t.Fatalf("IterateDocuments failed: %v", err)
	}
	if count != 5 || processed != 5 {
		t.Errorf("Expected 5 documents, got count=%d processed=%d", count, processed)
	}

	// Test early termination
	processed = 0
	count, _ = decoder.IterateDocuments(func(index int, doc *models.Document) bool {
		processed++
		return index < 2 // Stop after 3 documents (indices 0, 1, 2)
	})

	if processed != 3 {
		t.Errorf("Early termination failed: processed %d, want 3", processed)
	}
}

// BenchmarkBatchEncode measures batch encoding performance
func BenchmarkBatchEncode(b *testing.B) {
	now := time.Now().UTC()
	docs := make([]*models.Document, 100)
	for i := range docs {
		docs[i] = &models.Document{
			DocumentID: "bench-doc",
			Fields: map[string]models.Field{
				"name":   {Name: "name", Value: models.NewStringValue("Benchmark User")},
				"age":    {Name: "age", Value: models.NewIntValue(int64(i))},
				"active": {Name: "active", Value: models.NewBoolValue(true)},
			},
			CreatedAt: now,
			UpdatedAt: now,
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		encoder := GetBatchEncoder()
		for _, doc := range docs {
			encoder.AddDocument(doc)
		}
		_ = encoder.Bytes()
		PutBatchEncoder(encoder)
	}
}

// BenchmarkBatchDecode measures batch decoding performance
func BenchmarkBatchDecode(b *testing.B) {
	// Prepare batch data
	now := time.Now().UTC()
	encoder := GetBatchEncoder()
	for i := 0; i < 100; i++ {
		doc := &models.Document{
			DocumentID: "bench-doc",
			Fields: map[string]models.Field{
				"name":   {Name: "name", Value: models.NewStringValue("Benchmark User")},
				"age":    {Name: "age", Value: models.NewIntValue(int64(i))},
				"active": {Name: "active", Value: models.NewBoolValue(true)},
			},
			CreatedAt: now,
			UpdatedAt: now,
		}
		encoder.AddDocument(doc)
	}
	batchData := encoder.Bytes()
	PutBatchEncoder(encoder)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		decoder, _ := NewBatchDecoder(batchData)
		for j := 0; j < decoder.Count(); j++ {
			_, _ = decoder.GetDocument(j)
		}
	}
}
