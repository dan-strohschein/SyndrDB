package helpers

import (
	"sort"
	"sync"
	"syndrdb/src/internal/domain/models"
)

// PHASE G: String interning - common field names as constants to avoid allocations
// These strings are used repeatedly across all queries, so interning saves allocations
const (
	FieldDocumentID = "DocumentID"
	FieldCreatedAt  = "CreatedAt"
	FieldUpdatedAt  = "UpdatedAt"
)

// Global pools for result set allocation optimization
// These pools dramatically reduce allocations in query result formatting
var (
	// Pool for individual document maps (100s allocated per query)
	docMapPool = sync.Pool{
		New: func() interface{} {
			// Pre-allocate capacity for typical document:
			// 3 metadata fields (DocumentID, CreatedAt, UpdatedAt) + ~7 user fields
			return make(map[string]interface{}, 10)
		},
	}

	// Pool for document ID slices used during sorting
	docIDSlicePool = sync.Pool{
		New: func() interface{} {
			slice := make([]string, 0, 100) // typical query returns ~100 docs
			return &slice
		},
	}
)

// GetDocMap retrieves a map from the pool for document result formatting
func GetDocMap() map[string]interface{} {
	m := docMapPool.Get().(map[string]interface{})
	// Clear any existing entries (safety check)
	for k := range m {
		delete(m, k)
	}
	return m
}

// PutDocMap returns a map to the pool for reuse
// Only pool maps that aren't too large to prevent memory bloat
func PutDocMap(m map[string]interface{}) {
	if m == nil || len(m) > 50 { // Don't pool oversized maps
		return
	}
	// Clear the map before returning to pool
	for k := range m {
		delete(m, k)
	}
	docMapPool.Put(m)
}

// FreeResultSet returns all maps in a result set to the pool
// MUST be called after JSON marshaling completes to avoid memory leaks
// Safe to call with nil or empty slices
func FreeResultSet(results []map[string]interface{}) {
	if results == nil {
		return
	}
	for _, docMap := range results {
		PutDocMap(docMap)
	}
}

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
	// ✅ PRE-ALLOCATE TO EXACT SIZE - eliminates slice growth allocations!
	flattenedDocs := make([]map[string]interface{}, 0, len(documents))

	// Build field filter map for O(1) lookup
	var fieldFilter map[string]bool
	hasProjection := len(selectedFields) > 0
	if hasProjection {
		fieldFilter = make(map[string]bool, len(selectedFields)) // ✅ Pre-size
		for _, field := range selectedFields {
			fieldFilter[field] = true
		}
	}

	// PHASE F: Sort document IDs for deterministic ordering
	// Pre-allocate with exact capacity to avoid slice growth
	docIDs := make([]string, 0, len(documents)) // ✅ Use append pattern
	for docID := range documents {
		docIDs = append(docIDs, docID)
	}
	sort.Strings(docIDs)

	// Process documents in sorted ID order
	for _, docID := range docIDs {
		doc := documents[docID]
		// Get a map from the pool (PHASE A OPTIMIZATION)
		// This eliminates 100+ allocations per query by reusing maps
		flatDoc := GetDocMap()

		// PHASE G: Always include document metadata using interned string constants
		flatDoc[FieldDocumentID] = doc.DocumentID
		flatDoc[FieldCreatedAt] = doc.CreatedAt
		flatDoc[FieldUpdatedAt] = doc.UpdatedAt

		// Add fields based on projection
		for fieldName, field := range doc.Fields {
			// Check if this field should be included
			// ✅ ZERO-ALLOCATION: Check FieldValue type directly (no boxing)!
			shouldInclude := !hasProjection || fieldFilter[fieldName] || isNestedRelationshipFieldValue(field.Value)

			if shouldInclude {
				// ✅ ZERO-ALLOCATION: Store FieldValue directly (no boxing)!
				// JSON marshaling calls FieldValue.MarshalJSON() automatically
				flatDoc[fieldName] = field.Value
			}
		}

		flattenedDocs = append(flattenedDocs, flatDoc)
	}

	return flattenedDocs
}

// isNestedRelationshipFieldValue checks if a FieldValue contains a nested relationship
// Avoids boxing by checking FieldValue type first
func isNestedRelationshipFieldValue(fv models.FieldValue) bool {
	// Primitive types (String, Int, Float, Bool, Nil) are never relationships
	if fv.Type != models.FieldTypeInterface {
		return false
	}
	// Only complex types stored in InterfaceVal could be relationships
	return isNestedRelationship(fv.InterfaceVal)
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

// TransformDocumentSliceToFlatFormat converts a sorted slice of documents to flat format
// Preserves the order of documents in the slice (used for ORDER BY queries)
//
// This function is similar to TransformDocumentsToFlatFormatWithProjection but:
// - Accepts a slice instead of a map (order is already determined)
// - Does NOT sort by DocumentID (preserves input order)
// - Used when query has ORDER BY to maintain sort order
//
// Parameters:
//   - documents: Slice of documents in desired order (e.g., from ORDER BY + LIMIT)
//   - selectedFields: List of fields to include (nil/empty = all fields)
//
// Returns:
//   - Array of flattened document objects in the same order as input
func TransformDocumentSliceToFlatFormat(documents []*models.Document, selectedFields []string) []map[string]interface{} {
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

	// Process documents in preserved order
	for _, doc := range documents {
		// Get a map from the pool (PHASE A OPTIMIZATION)
		flatDoc := GetDocMap()

		// PHASE G: Always include document metadata using interned string constants
		flatDoc[FieldDocumentID] = doc.DocumentID
		flatDoc[FieldCreatedAt] = doc.CreatedAt
		flatDoc[FieldUpdatedAt] = doc.UpdatedAt

		// Add fields based on projection
		for fieldName, field := range doc.Fields {
			// Check if this field should be included
			// ✅ ZERO-ALLOCATION: Check FieldValue type directly (no boxing)!
			shouldInclude := !hasProjection || fieldFilter[fieldName] || isNestedRelationshipFieldValue(field.Value)

			if shouldInclude {
				// ✅ ZERO-ALLOCATION: Store FieldValue directly (no boxing)!
				// JSON marshaling calls FieldValue.MarshalJSON() automatically
				flatDoc[fieldName] = field.Value
			}
		}

		flattenedDocs = append(flattenedDocs, flatDoc)
	}

	return flattenedDocs
}
