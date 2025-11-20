package sorting

import (
	"fmt"
	"syndrdb/src/internal/domain/models"

	"github.com/google/uuid"
)

/*
TEST HELPERS FOR SORTING PACKAGE

This file provides utility functions for creating test data used across
multiple sorting test files. It follows DRY principles by centralizing
test document creation logic.

Main Functions:
- createTestDocuments: Generate documents with sequential integer values
- createTestDocumentsWithField: Generate documents with custom field values
*/

// createTestDocuments creates a map of test documents with sequential integer values.
// Each document gets a unique DocumentID and a field with the specified name containing
// an int64 value from 0 to count-1.
//
// This is useful for testing sorting correctness and performance with predictable data.
//
// Parameters:
//   - count: Number of documents to create
//   - fieldName: Name of the field to populate with sequential values
//
// Returns:
//   - map[string]*models.Document: Map of documents keyed by DocumentID
//
// Example:
//
//	docs := createTestDocuments(100, "score")
//	// Creates 100 documents with score values 0-99
func createTestDocuments(count int, fieldName string) map[string]*models.Document {
	docs := make(map[string]*models.Document, count)

	for i := 0; i < count; i++ {
		doc := &models.Document{
			DocumentID: uuid.New().String(),
			Fields: map[string]models.Field{
				fieldName: {
					Name:  fieldName,
					Value: models.NewIntValue(int64(i)), // ✅ Use NewIntValue
				},
			},
		}
		docs[doc.DocumentID] = doc
	}

	return docs
}

// createTestDocumentsWithValues creates test documents with specific field values.
// This allows for more controlled testing scenarios like duplicate values, NULL handling,
// and multi-field sorting.
//
// Parameters:
//   - values: Map of field names to slices of values
//   - count: Number of documents to create (must match length of value slices)
//
// Returns:
//   - map[string]*models.Document: Map of documents keyed by DocumentID
//   - error: If value slices have mismatched lengths
//
// Example:
//
//	docs, _ := createTestDocumentsWithValues(map[string][]interface{}{
//	    "category": []interface{}{1, 1, 2, 2, 3},
//	    "score": []interface{}{10, 20, 15, 25, 30},
//	}, 5)
func createTestDocumentsWithValues(
	values map[string][]interface{},
	count int,
) (map[string]*models.Document, error) {
	// Validate all value slices have correct length
	for fieldName, vals := range values {
		if len(vals) != count {
			return nil, fmt.Errorf("field %s has %d values, expected %d",
				fieldName, len(vals), count)
		}
	}

	docs := make(map[string]*models.Document, count)

	for i := 0; i < count; i++ {
		doc := &models.Document{
			DocumentID: uuid.New().String(),
			Fields:     make(map[string]models.Field),
		}

		// Populate all fields for this document
		for fieldName, vals := range values {
			doc.Fields[fieldName] = models.Field{
				Name:  fieldName,
				Value: models.NewInterfaceValue(vals[i]), // ✅ Use NewInterfaceValue
			}
		}

		docs[doc.DocumentID] = doc
	}

	return docs, nil
}

// createTestDocumentsWithStrings creates test documents with string field values.
// This is useful for testing SIMD string sorting and string comparison logic.
//
// Parameters:
//   - count: Number of documents to create
//   - fieldName: Name of the string field
//   - prefix: Prefix for generated strings
//
// Returns:
//   - map[string]*models.Document: Map of documents with string fields
//
// Example:
//
//	docs := createTestDocumentsWithStrings(100, "name", "user")
//	// Creates docs with name values: "user000", "user001", ..., "user099"
func createTestDocumentsWithStrings(
	count int,
	fieldName string,
	prefix string,
) map[string]*models.Document {
	docs := make(map[string]*models.Document, count)

	for i := 0; i < count; i++ {
		doc := &models.Document{
			DocumentID: uuid.New().String(),
			Fields: map[string]models.Field{
				fieldName: {
					Name:  fieldName,
					Value: models.NewStringValue(fmt.Sprintf("%s%03d", prefix, i)), // ✅ Use NewStringValue
				},
			},
		}
		docs[doc.DocumentID] = doc
	}

	return docs
}
