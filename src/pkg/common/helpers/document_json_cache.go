package helpers

import (
	"syndrdb/src/internal/domain/models"
)

// BuildCachedJSON populates doc.CachedJSON with pre-encoded JSON fragments for all
// user-data fields. Each entry is a complete "fieldName":encodedValue fragment.
// Metadata fields (DocumentID, CreatedAt, UpdatedAt) are NOT cached — they're
// always encoded inline because they're simple strings with fast encoding paths.
func BuildCachedJSON(doc *models.Document) {
	if doc == nil || len(doc.Fields) == 0 {
		return
	}
	cache := make(map[string][]byte, len(doc.Fields))
	for fieldName, field := range doc.Fields {
		if fieldName == FieldDocumentID {
			continue // Metadata — always encoded inline
		}
		cache[fieldName] = BuildFieldFragment(fieldName, field.Value)
	}
	doc.CachedJSON = cache
}

// BuildFieldFragment encodes a single "fieldName":value JSON fragment using
// FieldValue.MarshalJSON() which already has optimized paths (strconv fast paths,
// pre-allocated literals, HVJson for complex types).
func BuildFieldFragment(fieldName string, value models.FieldValue) []byte {
	valBytes, err := value.MarshalJSON()
	if err != nil {
		valBytes = []byte("null")
	}
	// Build "fieldName":value fragment
	// Pre-calculate exact size: 2 quotes + fieldName + colon + value bytes
	buf := make([]byte, 0, len(fieldName)+len(valBytes)+3)
	buf = append(buf, '"')
	buf = append(buf, fieldName...)
	buf = append(buf, '"', ':')
	buf = append(buf, valBytes...)
	return buf
}
