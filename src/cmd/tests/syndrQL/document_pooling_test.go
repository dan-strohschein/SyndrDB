package syndrQL

import (
	"testing"

	"syndrdb/src/internal/domain/document"
	"syndrdb/src/internal/domain/models"
)

// STEP 1: Document Pool Integration - Test pool behavior
// TestDocumentPoolReuse verifies pooled documents are reused and properly cleared
func TestDocumentPoolReuse(t *testing.T) {
	// Get a document from the pool
	doc1 := document.GetPooledDocument()
	if doc1 == nil {
		t.Fatal("GetPooledDocument returned nil")
	}

	// Set some data
	doc1.DocumentID = "test-doc-1"
	doc1.Fields = make(map[string]models.Field)
	doc1.Fields["name"] = models.Field{Name: "name", Value: models.NewStringValue("Test Document")}

	// Return it to the pool
	document.ReturnPooledDocument(doc1)

	// Get another document - should be the same instance (reused)
	doc2 := document.GetPooledDocument()
	if doc2 == nil {
		t.Fatal("GetPooledDocument returned nil on second call")
	}

	// Verify it was cleared
	if doc2.DocumentID != "" {
		t.Errorf("Expected empty DocumentID after pool return, got: %s", doc2.DocumentID)
	}
	if len(doc2.Fields) != 0 {
		t.Errorf("Expected empty Fields map after pool return, got %d fields", len(doc2.Fields))
	}

	// Clean up
	document.ReturnPooledDocument(doc2)
}

// STEP 1: Document Pool Integration - Test batch cleanup
// TestFreeDocuments verifies batch document cleanup
func TestFreeDocuments(t *testing.T) {
	// Create a batch of pooled documents
	docs := make([]*models.Document, 10)
	for i := 0; i < 10; i++ {
		docs[i] = document.GetPooledDocument()
		docs[i].DocumentID = "test-doc"
	}

	// Free them all at once
	document.FreeDocuments(docs)

	// Verify pool still works
	newDoc := document.GetPooledDocument()
	if newDoc == nil {
		t.Fatal("Pool broken after FreeDocuments")
	}
	if newDoc.DocumentID != "" {
		t.Errorf("Expected clean document from pool, got DocumentID: %s", newDoc.DocumentID)
	}

	document.ReturnPooledDocument(newDoc)
}

// STEP 1: Document Pool Integration - Test nil handling
// TestFreeDocumentsNilSafe verifies nil safety
func TestFreeDocumentsNilSafe(t *testing.T) {
	// Should not panic with nil slice
	document.FreeDocuments(nil)

	// Should not panic with empty slice
	document.FreeDocuments([]*models.Document{})

	// Should not panic with nil elements
	docs := []*models.Document{nil, nil, nil}
	document.FreeDocuments(docs)

	// Mixed nil and valid
	validDoc := document.GetPooledDocument()
	validDoc.DocumentID = "test"
	mixed := []*models.Document{nil, validDoc, nil}
	document.FreeDocuments(mixed)

	// Verify pool still works
	testDoc := document.GetPooledDocument()
	if testDoc == nil {
		t.Fatal("Pool broken after nil-safe cleanup")
	}

	document.ReturnPooledDocument(testDoc)
}
