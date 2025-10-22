package helpers

import (
	"sort"
	"syndrdb/src/internal/domain/models"
)

// TransformDocumentsToFlatFormat converts a map of documents to an array of flattened objects
// This creates the user-friendly format with field names as direct properties
// Documents are returned in deterministic order (sorted by DocumentID) to ensure
// consistent results across multiple queries when no explicit ORDER BY is specified
//
// Field Projection:
// - If selectedFields is empty or nil, all fields are returned (legacy behavior)
// - If selectedFields is provided, only those fields are included in the output
// - Nested relationship fields (arrays/objects) are always preserved regardless of projection
// - DocumentID, CreatedAt, UpdatedAt metadata are always included
func TransformDocumentsToFlatFormat(documents map[string]*models.Document) []map[string]interface{} {
	return TransformDocumentsToFlatFormatWithProjection(documents, nil)
}

// TransformDocumentsToFlatFormatWithProjection converts documents with optional field projection
// This supports SELECT field1, field2 queries by filtering returned fields
//
// Parameters:
//   - documents: Map of documents to transform
//   - selectedFields: List of fields to include (nil/empty = all fields)
//
// Returns:
//   - Array of flattened document objects with projected fields
//
// Special handling:
//   - Nested relationships (arrays/slices/maps) are always included (JOIN results)
//   - Document metadata (DocumentID, CreatedAt, UpdatedAt) always included
//   - If a selected field doesn't exist in a document, it's omitted from that document
func TransformDocumentsToFlatFormatWithProjection(documents map[string]*models.Document, selectedFields []string) []map[string]interface{} {
	flattenedDocs := make([]map[string]interface{}, 0, len(documents))

	// Build field filter map for O(1) lookup
	var fieldFilter map[string]bool
	hasProjection := len(selectedFields) > 0
	if hasProjection {
		fieldFilter = make(map[string]bool)
		for _, field := range selectedFields {
			fieldFilter[field] = true
		}
	}

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

		// Always include document metadata (not subject to projection)
		flatDoc["DocumentID"] = doc.DocumentID
		flatDoc["CreatedAt"] = doc.CreatedAt
		flatDoc["UpdatedAt"] = doc.UpdatedAt

		// Add fields based on projection
		for fieldName, field := range doc.Fields {
			// Check if this field should be included
			shouldInclude := !hasProjection || fieldFilter[fieldName] || isNestedRelationship(field.Value)

			if shouldInclude {
				flatDoc[fieldName] = field.Value
			}
		}

		flattenedDocs = append(flattenedDocs, flatDoc)
	}

	return flattenedDocs
}

// isNestedRelationship checks if a field value is a nested relationship (from JOIN queries)
// Nested relationships are always included regardless of field projection to preserve
// hierarchical query results (e.g., Authors with their Books)
func isNestedRelationship(value interface{}) bool {
	if value == nil {
		return false
	}

	switch value.(type) {
	case []interface{}: // Array of related documents
		return true
	case []map[string]interface{}: // Array of document maps
		return true
	case map[string]interface{}: // Nested object
		// Check if it looks like a document (has common document fields)
		if docMap, ok := value.(map[string]interface{}); ok {
			_, hasDocID := docMap["DocumentID"]
			_, hasCreatedAt := docMap["CreatedAt"]
			// If it has document-like structure, it's likely a relationship
			return hasDocID || hasCreatedAt
		}
		return false
	default:
		return false
	}
}
