package helpers

import (
	"sort"
	"syndrdb/src/internal/domain/models"
)

// transformDocumentsToFlatFormat converts a map of documents to an array of flattened objects
// This creates the user-friendly format with field names as direct properties
// Documents are returned in deterministic order (sorted by DocumentID) to ensure
// consistent results across multiple queries when no explicit ORDER BY is specified
func TransformDocumentsToFlatFormat(documents map[string]*models.Document) []map[string]interface{} {
	flattenedDocs := make([]map[string]interface{}, 0, len(documents))

	// Sort document IDs for deterministic ordering
	// This ensures queries without ORDER BY return consistent results
	docIDs := make([]string, 0, len(documents))
	for docID := range documents {
		docIDs = append(docIDs, docID)
	}
	sort.Strings(docIDs)

	// Process documents in sorted ID order
	for _, docID := range docIDs {
		doc := documents[docID]
		flatDoc := make(map[string]interface{})

		// Add document metadata
		flatDoc["DocumentID"] = doc.DocumentID
		flatDoc["CreatedAt"] = doc.CreatedAt
		flatDoc["UpdatedAt"] = doc.UpdatedAt

		// Add flattened fields
		for fieldName, field := range doc.Fields {
			flatDoc[fieldName] = field.Value
		}

		flattenedDocs = append(flattenedDocs, flatDoc)
	}

	return flattenedDocs
}
