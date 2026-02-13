package helpers

import (
	"io"
	"time"

	"syndrdb/src/internal/domain/models"

	hvjson "github.com/dan-strohschein/HVJson/hvjson"
)

// arrayOpen/close/comma are reused byte slices for the outer JSON array envelope.
// The ISE handles each document object at depth 0; we write array structure manually
// because the ISE's structural writers don't auto-insert commas in nested arrays.
var (
	arrayOpen  = []byte{'['}
	arrayClose = []byte{']'}
	comma      = []byte{','}
)

// StreamDocumentsToJSON writes documents directly to JSON without intermediate allocations.
// Uses HVJson IncrementalStreamEncoder with SIMD-accelerated string escaping and
// number formatting, with 64KB incremental flushes to bound memory.
func StreamDocumentsToJSON(writer io.Writer, documents map[string]*models.Document, selectedFields []string) error {
	ise := hvjson.NewIncrementalEncoder(writer, 0) // 64KB default threshold

	// Build field filter for projection
	var fieldFilter map[string]bool
	hasProjection := len(selectedFields) > 0
	if hasProjection {
		fieldFilter = make(map[string]bool, len(selectedFields))
		for _, field := range selectedFields {
			fieldFilter[field] = true
		}
	}

	// Collect document IDs
	docIDs := make([]string, 0, len(documents))
	for docID := range documents {
		docIDs = append(docIDs, docID)
	}

	// Write array envelope manually; ISE handles each object at depth 0
	if _, err := writer.Write(arrayOpen); err != nil {
		return err
	}

	for i, docID := range docIDs {
		doc := documents[docID]
		if i > 0 {
			// Flush ISE buffer before writing comma to ensure correct byte ordering
			if err := ise.Flush(); err != nil {
				return err
			}
			if _, err := writer.Write(comma); err != nil {
				return err
			}
		}
		writeDocumentObject(ise, doc, fieldFilter, hasProjection)
	}

	if err := ise.Flush(); err != nil {
		return err
	}
	_, err := writer.Write(arrayClose)
	return err
}

// StreamDocumentSliceToJSON writes an ordered slice of documents directly to JSON.
// Preserves the input order (important for ORDER BY queries).
func StreamDocumentSliceToJSON(writer io.Writer, documents []*models.Document, selectedFields []string) error {
	ise := hvjson.NewIncrementalEncoder(writer, 0)

	// Build field filter for projection
	var fieldFilter map[string]bool
	hasProjection := len(selectedFields) > 0
	if hasProjection {
		fieldFilter = make(map[string]bool, len(selectedFields))
		for _, field := range selectedFields {
			fieldFilter[field] = true
		}
	}

	if _, err := writer.Write(arrayOpen); err != nil {
		return err
	}

	for i, doc := range documents {
		if i > 0 {
			if err := ise.Flush(); err != nil {
				return err
			}
			if _, err := writer.Write(comma); err != nil {
				return err
			}
		}
		writeDocumentObject(ise, doc, fieldFilter, hasProjection)
	}

	if err := ise.Flush(); err != nil {
		return err
	}
	_, err := writer.Write(arrayClose)
	return err
}

// writeDocumentObject writes a single document as a JSON object using the ISE.
// Uses pre-encoded JSON cache when available (fast path: memcpy of cached fragments).
// Falls back to field-by-field encoding if cache is not populated.
func writeDocumentObject(ise *hvjson.IncrementalStreamEncoder, doc *models.Document, fieldFilter map[string]bool, hasProjection bool) {
	// Lazy cache population for documents loaded from disk
	if doc.CachedJSON == nil && len(doc.Fields) > 0 {
		BuildCachedJSON(doc)
	}

	useCachedPath := doc.CachedJSON != nil

	ise.WriteObjectStart()

	// Metadata fields (always included, always encoded inline — not cached)
	ise.WriteObjectField(FieldDocumentID)
	ise.WriteString(doc.DocumentID)

	ise.WriteObjectField(FieldCreatedAt)
	ise.WriteString(doc.CreatedAt.Format(time.RFC3339))

	ise.WriteObjectField(FieldUpdatedAt)
	ise.WriteString(doc.UpdatedAt.Format(time.RFC3339))

	if useCachedPath {
		// Fast path: write pre-encoded fragments as raw bytes
		for fieldName, fragment := range doc.CachedJSON {
			if !hasProjection || fieldFilter[fieldName] {
				ise.WriteMore()         // comma separator
				ise.WriteRawBytes(fragment) // "name":value as pre-encoded bytes
			}
		}
	} else {
		// Fallback: field-by-field encoding (same as original code)
		for fieldName, field := range doc.Fields {
			if !hasProjection || fieldFilter[fieldName] {
				ise.WriteObjectField(fieldName)
				writeFieldValue(ise, field.Value)
			}
		}
	}

	ise.WriteObjectEnd()
}

// writeFieldValue writes a field value to the ISE stream.
// Handles all SyndrDB field types with typed writes for SIMD acceleration.
// Complex nested types (slices, maps) use WriteValue for correct encoding.
func writeFieldValue(ise *hvjson.IncrementalStreamEncoder, value interface{}) {
	if value == nil {
		ise.WriteNull()
		return
	}

	// Handle models.FieldValue typed union (hot path)
	if fv, ok := value.(models.FieldValue); ok {
		switch fv.Type {
		case models.FieldTypeString:
			ise.WriteString(fv.StringVal)
		case models.FieldTypeInt:
			ise.WriteInt64(fv.IntVal)
		case models.FieldTypeFloat:
			ise.WriteFloat64(fv.FloatVal)
		case models.FieldTypeBool:
			ise.WriteBool(fv.BoolVal)
		case models.FieldTypeDateTime:
			ise.WriteString(fv.DateTimeVal.Format(time.RFC3339))
		case models.FieldTypeDate:
			ise.WriteString(fv.DateVal.Format("2006-01-02"))
		case models.FieldTypeInterface:
			writeFieldValue(ise, fv.InterfaceVal)
		case models.FieldTypeNil:
			ise.WriteNull()
		default:
			ise.WriteNull()
		}
		return
	}

	// Go primitive types
	switch v := value.(type) {
	case string:
		ise.WriteString(v)
	case int:
		ise.WriteInt64(int64(v))
	case int64:
		ise.WriteInt64(v)
	case float64:
		ise.WriteFloat64(v)
	case bool:
		ise.WriteBool(v)
	case time.Time:
		ise.WriteString(v.Format(time.RFC3339))
	default:
		// Complex types ([]interface{}, map[string]interface{}, etc.)
		// use WriteValue which delegates to the full HVJson encoder
		ise.WriteValue(v)
	}
}
