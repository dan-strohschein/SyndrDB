package helpers

import (
	"syndrdb/src/internal/domain/models"
)

// transformDocumentsToFlatFormat converts a map of documents to an array of flattened objects
// This creates the user-friendly format with field names as direct properties
func TransformDocumentsToFlatFormat(documents map[string]*models.Document) []map[string]interface{} {
	flattenedDocs := make([]map[string]interface{}, 0, len(documents))

	for _, doc := range documents {
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
