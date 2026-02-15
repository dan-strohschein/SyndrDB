// Package helpers provides high-performance utilities for SyndrDB.
//
// This test file validates the BatchEncoder for efficient multi-document
// serialization used in UPDATE operations. It uses the schema-ordered
// Document.Values model and BundleFieldSchema for encode/decode.
package helpers

import (
	"syndrdb/src/internal/domain/models"
	"testing"
	"time"
)

// testSchema defines field order for batch encoder tests (name, age, active).
var testSchema = models.BuildBundleFieldSchemaFromNames([]string{"name", "age", "active"})

// makeDoc creates a document with Values in testSchema order for testing.
func makeDoc(id, name string, age int64, active bool, now time.Time) *models.Document {
	return &models.Document{
		DocumentID: id,
		Values: []models.FieldValue{
			models.NewStringValue(name),
			models.NewIntValue(age),
			models.NewBoolValue(active),
		},
		CreatedAt: now,
		UpdatedAt: now,
	}
}

// TestBatchEncoderRoundtrip verifies batch encode/decode cycle using Values and schema.
func TestBatchEncoderRoundtrip(t *testing.T) {
	now := time.Now().UTC()
	docs := []*models.Document{
		makeDoc("doc-001", "Alice", 30, false, now),
		makeDoc("doc-002", "Bob", 25, false, now),
		makeDoc("doc-003", "Charlie", 35, true, now),
	}

	encoder := GetBatchEncoder()
	defer PutBatchEncoder(encoder)

	for _, doc := range docs {
		if err := encoder.AddDocument(doc, testSchema); err != nil {
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

	decoder, err := NewBatchDecoder(batchData)
	if err != nil {
		t.Fatalf("NewBatchDecoder failed: %v", err)
	}

	if decoder.Count() != 3 {
		t.Errorf("Decoded count mismatch: got %d, want 3", decoder.Count())
	}

	for i, originalDoc := range docs {
		decoded, err := decoder.GetDocument(i, testSchema)
		if err != nil {
			t.Fatalf("GetDocument(%d) failed: %v", i, err)
		}

		if decoded.DocumentID != originalDoc.DocumentID {
			t.Errorf("Doc %d DocumentID mismatch: got %s, want %s",
				i, decoded.DocumentID, originalDoc.DocumentID)
		}

		if len(decoded.Values) != len(originalDoc.Values) {
			t.Errorf("Doc %d Values length mismatch: got %d, want %d",
				i, len(decoded.Values), len(originalDoc.Values))
		}

		for j := range originalDoc.Values {
			if j >= len(decoded.Values) {
				break
			}
			orig, dec := originalDoc.Values[j], decoded.Values[j]
			if orig.Type != dec.Type || orig.StringVal != dec.StringVal ||
				orig.IntVal != dec.IntVal || orig.BoolVal != dec.BoolVal {
				t.Errorf("Doc %d Values[%d] mismatch: orig=%+v decoded=%+v", i, j, orig, dec)
			}
		}
	}
}

// schemaOneField is used for pool test (single field to minimize setup).
var schemaOneField = models.BuildBundleFieldSchemaFromNames([]string{"test"})

// TestBatchEncoderPool verifies pool functionality.
func TestBatchEncoderPool(t *testing.T) {
	encoder1 := GetBatchEncoder()
	if encoder1 == nil {
		t.Fatal("GetBatchEncoder returned nil")
	}

	doc := &models.Document{
		DocumentID: "pool-test",
		Values:     []models.FieldValue{models.NewStringValue("value")},
		CreatedAt:  time.Now().UTC(),
		UpdatedAt:  time.Now().UTC(),
	}
	if err := encoder1.AddDocument(doc, schemaOneField); err != nil {
		t.Fatalf("AddDocument failed: %v", err)
	}

	if encoder1.Count() != 1 {
		t.Errorf("Expected 1 document, got %d", encoder1.Count())
	}

	PutBatchEncoder(encoder1)

	encoder2 := GetBatchEncoder()
	if encoder2.Count() != 0 {
		t.Errorf("Pooled encoder not reset: count = %d", encoder2.Count())
	}

	PutBatchEncoder(encoder2)
}

// schemaIndex is used for iteration test (single field "index").
var schemaIndex = models.BuildBundleFieldSchemaFromNames([]string{"index"})

// TestBatchDecoderIteration verifies iteration over batch with schema-first API.
func TestBatchDecoderIteration(t *testing.T) {
	encoder := GetBatchEncoder()
	defer PutBatchEncoder(encoder)

	now := time.Now().UTC()
	for i := 0; i < 5; i++ {
		doc := &models.Document{
			DocumentID: "iter-doc",
			Values:     []models.FieldValue{models.NewIntValue(int64(i))},
			CreatedAt:  now,
			UpdatedAt:  now,
		}
		if err := encoder.AddDocument(doc, schemaIndex); err != nil {
			t.Fatalf("AddDocument failed: %v", err)
		}
	}

	batchData := encoder.Bytes()
	decoder, err := NewBatchDecoder(batchData)
	if err != nil {
		t.Fatal(err)
	}

	processed := 0
	count, err := decoder.IterateDocuments(schemaIndex, func(index int, doc *models.Document) bool {
		processed++
		if len(doc.Values) > 0 && doc.Values[0].IntVal != int64(index) {
			t.Errorf("Index mismatch at %d: got %d", index, doc.Values[0].IntVal)
		}
		return true
	})

	if err != nil {
		t.Fatalf("IterateDocuments failed: %v", err)
	}
	if count != 5 || processed != 5 {
		t.Errorf("Expected 5 documents, got count=%d processed=%d", count, processed)
	}

	// Early termination: stop after indices 0, 1, 2 (3 documents)
	processed = 0
	_, _ = decoder.IterateDocuments(schemaIndex, func(index int, doc *models.Document) bool {
		processed++
		return index < 2
	})

	if processed != 3 {
		t.Errorf("Early termination failed: processed %d, want 3", processed)
	}
}

// BenchmarkBatchEncode measures batch encoding performance with schema-ordered Values.
func BenchmarkBatchEncode(b *testing.B) {
	now := time.Now().UTC()
	docs := make([]*models.Document, 100)
	for i := range docs {
		docs[i] = &models.Document{
			DocumentID: "bench-doc",
			Values: []models.FieldValue{
				models.NewStringValue("Benchmark User"),
				models.NewIntValue(int64(i)),
				models.NewBoolValue(true),
			},
			CreatedAt: now,
			UpdatedAt: now,
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		encoder := GetBatchEncoder()
		for _, doc := range docs {
			_ = encoder.AddDocument(doc, testSchema)
		}
		_ = encoder.Bytes()
		PutBatchEncoder(encoder)
	}
}

// BenchmarkBatchDecode measures batch decoding performance with schema.
func BenchmarkBatchDecode(b *testing.B) {
	now := time.Now().UTC()
	encoder := GetBatchEncoder()
	for i := 0; i < 100; i++ {
		doc := &models.Document{
			DocumentID: "bench-doc",
			Values: []models.FieldValue{
				models.NewStringValue("Benchmark User"),
				models.NewIntValue(int64(i)),
				models.NewBoolValue(true),
			},
			CreatedAt: now,
			UpdatedAt: now,
		}
		_ = encoder.AddDocument(doc, testSchema)
	}
	batchData := encoder.Bytes()
	PutBatchEncoder(encoder)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		decoder, _ := NewBatchDecoder(batchData)
		for j := 0; j < decoder.Count(); j++ {
			_, _ = decoder.GetDocument(j, testSchema)
		}
	}
}
