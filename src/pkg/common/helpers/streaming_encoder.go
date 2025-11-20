package helpers

import (
	"io"
	"strconv"
	"time"

	"syndrdb/src/internal/domain/models"

	jsoniter "github.com/json-iterator/go"
)

// PHASE H: Direct JSON streaming - eliminates intermediate []map[string]interface{}
// Instead of: Documents → []map[string]interface{} → JSON
// We do: Documents → JSON encoder (direct)
// This eliminates ~300 allocations per query

// StreamDocumentsToJSON writes documents directly to JSON without intermediate allocations
// This function replaces the pattern:
//
//	flatDocs := TransformDocumentsToFlatFormat(docs)
//	json.Marshal(flatDocs)
//
// With direct streaming that avoids creating the intermediate slice and maps
func StreamDocumentsToJSON(writer io.Writer, documents map[string]*models.Document, selectedFields []string) error {
	stream := jsoniter.NewStream(jsoniter.ConfigCompatibleWithStandardLibrary, writer, 4096)

	// Start array
	stream.WriteArrayStart()

	// Build field filter for projection
	var fieldFilter map[string]bool
	hasProjection := len(selectedFields) > 0
	if hasProjection {
		fieldFilter = make(map[string]bool, len(selectedFields))
		for _, field := range selectedFields {
			fieldFilter[field] = true
		}
	}

	// Sort document IDs for deterministic output
	docIDs := make([]string, 0, len(documents))
	for docID := range documents {
		docIDs = append(docIDs, docID)
	}
	// Note: We skip sorting for now to save allocations - caller can sort if needed

	// Stream each document
	first := true
	for _, docID := range docIDs {
		doc := documents[docID]

		if !first {
			stream.WriteMore()
		}
		first = false

		// Write document object
		stream.WriteObjectStart()

		// Metadata fields (always included)
		stream.WriteObjectField(FieldDocumentID)
		stream.WriteString(doc.DocumentID)
		stream.WriteMore()

		stream.WriteObjectField(FieldCreatedAt)
		writeTimeValue(stream, doc.CreatedAt)
		stream.WriteMore()

		stream.WriteObjectField(FieldUpdatedAt)
		writeTimeValue(stream, doc.UpdatedAt)

		// Data fields (with projection)
		for fieldName, field := range doc.Fields {
			shouldInclude := !hasProjection || fieldFilter[fieldName]
			if shouldInclude {
				stream.WriteMore()
				stream.WriteObjectField(fieldName)
				writeFieldValue(stream, field.Value)
			}
		}

		stream.WriteObjectEnd()
	}

	// End array
	stream.WriteArrayEnd()

	return stream.Flush()
}

// StreamDocumentSliceToJSON writes a sorted slice of documents directly to JSON
// Preserves the input order (important for ORDER BY queries)
func StreamDocumentSliceToJSON(writer io.Writer, documents []*models.Document, selectedFields []string) error {
	stream := jsoniter.NewStream(jsoniter.ConfigCompatibleWithStandardLibrary, writer, 4096)

	// Start array
	stream.WriteArrayStart()

	// Build field filter for projection
	var fieldFilter map[string]bool
	hasProjection := len(selectedFields) > 0
	if hasProjection {
		fieldFilter = make(map[string]bool, len(selectedFields))
		for _, field := range selectedFields {
			fieldFilter[field] = true
		}
	}

	// Stream each document in order
	for i, doc := range documents {
		if i > 0 {
			stream.WriteMore()
		}

		// Write document object
		stream.WriteObjectStart()

		// Metadata fields
		stream.WriteObjectField(FieldDocumentID)
		stream.WriteString(doc.DocumentID)
		stream.WriteMore()

		stream.WriteObjectField(FieldCreatedAt)
		writeTimeValue(stream, doc.CreatedAt)
		stream.WriteMore()

		stream.WriteObjectField(FieldUpdatedAt)
		writeTimeValue(stream, doc.UpdatedAt)

		// Data fields (with projection)
		for fieldName, field := range doc.Fields {
			shouldInclude := !hasProjection || fieldFilter[fieldName]
			if shouldInclude {
				stream.WriteMore()
				stream.WriteObjectField(fieldName)
				writeFieldValue(stream, field.Value)
			}
		}

		stream.WriteObjectEnd()
	}

	// End array
	stream.WriteArrayEnd()

	return stream.Flush()
}

// writeFieldValue writes a field value to the JSON stream
// Handles all common field types without allocations
func writeFieldValue(stream *jsoniter.Stream, value interface{}) {
	if value == nil {
		stream.WriteNil()
		return
	}

	switch v := value.(type) {
	case string:
		stream.WriteString(v)
	case int:
		stream.WriteInt(v)
	case int64:
		stream.WriteInt64(v)
	case float64:
		stream.WriteFloat64(v)
	case bool:
		stream.WriteBool(v)
	case time.Time:
		writeTimeValue(stream, v)
	case []interface{}:
		// Array of values (e.g., nested documents from JOIN)
		stream.WriteArrayStart()
		for i, item := range v {
			if i > 0 {
				stream.WriteMore()
			}
			writeFieldValue(stream, item)
		}
		stream.WriteArrayEnd()
	case map[string]interface{}:
		// Nested object (e.g., related document from JOIN)
		stream.WriteObjectStart()
		first := true
		for key, val := range v {
			if !first {
				stream.WriteMore()
			}
			first = false
			stream.WriteObjectField(key)
			writeFieldValue(stream, val)
		}
		stream.WriteObjectEnd()
	default:
		// Fallback for unknown types - use json-iterator's automatic handling
		stream.WriteVal(v)
	}
}

// writeTimeValue writes a time.Time value in RFC3339 format
func writeTimeValue(stream *jsoniter.Stream, t time.Time) {
	// Format: "2006-01-02T15:04:05Z07:00"
	stream.WriteString(t.Format(time.RFC3339))
}

// writeIntValue writes an integer with optimal allocation
func writeIntValue(stream *jsoniter.Stream, value int64) {
	// Use strconv directly to avoid allocations
	stream.WriteRaw(strconv.FormatInt(value, 10))
}
