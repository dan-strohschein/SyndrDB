package helpers

import (
	"syndrdb/src/internal/domain/models"
)

// BuildCachedJSON populates doc.CachedJSON with pre-encoded JSON fragments for all
// user-data fields in schema order. Each entry is a complete "fieldName":encodedValue fragment.
// Metadata fields (DocumentID, CreatedAt, UpdatedAt) are NOT cached — they're
// always encoded inline. Schema defines the order of doc.Values; if nil, no cache is built.
func BuildCachedJSON(doc *models.Document, schema *models.BundleFieldSchema) {
	if doc == nil || schema == nil || len(doc.Values) == 0 {
		return
	}
	cache := make(map[string][]byte, len(schema.Names))
	for i, fieldName := range schema.Names {
		if fieldName == FieldDocumentID {
			continue
		}
		if i < len(doc.Values) {
			cache[fieldName] = BuildFieldFragment(fieldName, doc.Values[i])
		}
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
