package bundle

import (
	"fmt"
	"strings"
	"syndrdb/src/internal/domain/models"

	"go.uber.org/zap"
)

// Magic value constants for representing NULL states in document fields.
// These special sentinel values allow us to distinguish between different types
// of NULL/missing data while maintaining a consistent field map structure.
//
// Benefits of this approach:
// - Single field lookup operation (no existence check + value check)
// - Field schema remains consistent across all documents
// - Different NULL semantics can be distinguished (explicit NULL vs missing vs deleted)
// - Efficient indexing (magic values can be indexed and queried)
// - GraphQL-compatible nullable field mapping
const (
	// SYNDR_NULL represents an explicit NULL value set by the user or application.
	// This is equivalent to SQL NULL or JSON null - a known absence of value.
	SYNDR_NULL = "::SYNDR_NULL::"

	// SYNDR_MISSING indicates the field was never provided in the source data.
	// This helps distinguish between "field set to null" vs "field not included".
	SYNDR_MISSING = "::SYNDR_MISSING::"

	// SYNDR_DELETED represents a tombstone for a field that was explicitly deleted.
	// Useful for tracking field removal history and soft deletes.
	SYNDR_DELETED = "::SYNDR_DELETED::"

	// SYNDR_DEFAULT indicates the field is using its default value from the schema.
	// This allows distinguishing between "value was provided" vs "using default".
	SYNDR_DEFAULT = "::SYNDR_DEFAULT::"

	// SYNDR_ESCAPED is used to escape user data that looks like a magic value.
	// If a user tries to store "::SYNDR_NULL::", we prefix it with this marker.
	SYNDR_ESCAPED = "::SYNDR_ESCAPED::"
)

// NullType represents the semantic type of a NULL or missing value.
type NullType int

const (
	// HasValue indicates the field contains a real (non-NULL) value.
	HasValue NullType = iota
	// ExplicitNull indicates the field was explicitly set to NULL.
	ExplicitNull
	// MissingField indicates the field was never provided.
	MissingField
	// DeletedField indicates the field was deleted (tombstone).
	DeletedField
	// DefaultValue indicates the field is using a default value.
	DefaultValue
)

// String returns a human-readable representation of the NullType.
func (nt NullType) String() string {
	switch nt {
	case HasValue:
		return "HasValue"
	case ExplicitNull:
		return "ExplicitNull"
	case MissingField:
		return "MissingField"
	case DeletedField:
		return "DeletedField"
	case DefaultValue:
		return "DefaultValue"
	default:
		return "Unknown"
	}
}

// NullHandler provides utilities for managing NULL states in documents using
// magic values. This allows efficient single-operation field access while
// maintaining rich NULL semantics.
type NullHandler struct {
	logger *zap.SugaredLogger
}

// NewNullHandler creates a new NullHandler instance.
func NewNullHandler(logger *zap.SugaredLogger) *NullHandler {
	return &NullHandler{
		logger: logger,
	}
}

// getDocField returns the field value from doc (Values+schema or Data). schema may be nil.
func getDocField(doc *models.Document, fieldName string, schema *models.BundleFieldSchema) (models.FieldValue, bool) {
	if doc == nil {
		return models.FieldValue{}, false
	}
	if schema != nil && len(doc.Values) > 0 {
		return doc.GetFieldValue(schema, fieldName)
	}
	if doc.Data != nil {
		if v, ok := doc.Data[fieldName]; ok {
			return models.NewInterfaceValue(v), true
		}
	}
	return models.FieldValue{}, false
}

// setDocField sets a field value on doc (Values+schema or Data). schema may be nil.
func setDocField(doc *models.Document, fieldName string, fv models.FieldValue, schema *models.BundleFieldSchema) {
	if doc == nil {
		return
	}
	if schema != nil && len(doc.Values) > 0 {
		if idx, ok := schema.NameToIndex[fieldName]; ok && idx >= 0 && idx < len(doc.Values) {
			doc.Values[idx] = fv
		}
		return
	}
	if doc.Data == nil {
		doc.Data = make(map[string]interface{})
	}
	doc.Data[fieldName] = fv.AsInterface()
}

// IsNull checks if a field contains any type of NULL value. schema may be nil (uses doc.Data).
func (nh *NullHandler) IsNull(doc *models.Document, fieldName string, schema *models.BundleFieldSchema) bool {
	fv, exists := getDocField(doc, fieldName, schema)
	if !exists {
		return true
	}
	return nh.IsNullValue(fv.AsInterface())
}

// IsNullValue checks if a raw value is a magic NULL value.
func (nh *NullHandler) IsNullValue(value interface{}) bool {
	if value == nil {
		return true
	}

	strValue, ok := value.(string)
	if !ok {
		return false
	}

	switch strValue {
	case SYNDR_NULL, SYNDR_MISSING, SYNDR_DELETED, SYNDR_DEFAULT:
		return true
	default:
		return false
	}
}

// GetNullType determines the specific type of NULL for a field.
// Returns HasValue if the field contains a real value, or the specific
// NULL type if it's a magic value.
//
// Example:
//
//	nullType := handler.GetNullType(document, "email")
//	switch nullType {
//	case ExplicitNull:
//	    // User set this to null
//	case MissingField:
//	    // Field was never provided
//	case HasValue:
//	    // Field has a real value
//	}
func (nh *NullHandler) GetNullType(doc *models.Document, fieldName string, schema *models.BundleFieldSchema) NullType {
	fv, exists := getDocField(doc, fieldName, schema)
	if !exists {
		return MissingField
	}
	return nh.GetNullTypeFromValue(fv.AsInterface())
}

// GetNullTypeFromValue determines the NULL type from a raw value.
func (nh *NullHandler) GetNullTypeFromValue(value interface{}) NullType {
	if value == nil {
		return ExplicitNull
	}

	strValue, ok := value.(string)
	if !ok {
		return HasValue
	}

	switch strValue {
	case SYNDR_NULL:
		return ExplicitNull
	case SYNDR_MISSING:
		return MissingField
	case SYNDR_DELETED:
		return DeletedField
	case SYNDR_DEFAULT:
		return DefaultValue
	default:
		return HasValue
	}
}

// GetFieldValue retrieves the actual value of a field, converting magic values
// to nil for external API consumption. This provides a clean interface that
// hides the internal magic value representation.
//
// Example:
//
//	value := handler.GetFieldValue(document, "email")
//	// If field contains SYNDR_NULL, returns nil
//	// Otherwise returns the actual value
func (nh *NullHandler) GetFieldValue(doc *models.Document, fieldName string, schema *models.BundleFieldSchema) interface{} {
	fv, exists := getDocField(doc, fieldName, schema)
	if !exists {
		return nil
	}
	if nh.IsNullValue(fv.AsInterface()) {
		return nil
	}
	if strValue, ok := fv.AsString(); ok {
		if strings.HasPrefix(strValue, SYNDR_ESCAPED) {
			return strings.TrimPrefix(strValue, SYNDR_ESCAPED)
		}
	}
	return fv.AsInterface()
}

// SetFieldValue sets a field value, automatically handling NULL values and
// escaping user data that looks like magic values.
//
// Example:
//
//	handler.SetFieldValue(document, "email", "user@example.com")
//	handler.SetFieldValue(document, "phone", nil) // Sets SYNDR_NULL
func (nh *NullHandler) SetFieldValue(doc *models.Document, fieldName string, value interface{}, schema *models.BundleFieldSchema) {
	if value == nil {
		setDocField(doc, fieldName, models.NewStringValue(SYNDR_NULL), schema)
		return
	}
	setDocField(doc, fieldName, models.NewInterfaceValue(nh.EscapeUserValue(value)), schema)
}

// EscapeUserValue escapes user data that looks like a magic value to prevent
// conflicts. If a user tries to store "::SYNDR_NULL::", we prefix it with
// the SYNDR_ESCAPED marker.
//
// Example:
//
//	value := handler.EscapeUserValue("::SYNDR_NULL::")
//	// Returns "::SYNDR_ESCAPED::::SYNDR_NULL::"
func (nh *NullHandler) EscapeUserValue(value interface{}) interface{} {
	strValue, ok := value.(string)
	if !ok {
		return value
	}

	// Check if the value looks like a magic value
	if strings.HasPrefix(strValue, "::SYNDR_") && !strings.HasPrefix(strValue, SYNDR_ESCAPED) {
		return SYNDR_ESCAPED + strValue
	}

	return value
}

// UnescapeValue removes the escape prefix if present, returning the original
// user value.
func (nh *NullHandler) UnescapeValue(value interface{}) interface{} {
	strValue, ok := value.(string)
	if !ok {
		return value
	}

	if strings.HasPrefix(strValue, SYNDR_ESCAPED) {
		return strings.TrimPrefix(strValue, SYNDR_ESCAPED)
	}

	return value
}

func (nh *NullHandler) SetMissingField(doc *models.Document, fieldName string, schema *models.BundleFieldSchema) {
	setDocField(doc, fieldName, models.NewStringValue(SYNDR_MISSING), schema)
}

func (nh *NullHandler) SetDeletedField(doc *models.Document, fieldName string, schema *models.BundleFieldSchema) {
	setDocField(doc, fieldName, models.NewStringValue(SYNDR_DELETED), schema)
}

func (nh *NullHandler) SetDefaultField(doc *models.Document, fieldName string, schema *models.BundleFieldSchema) {
	setDocField(doc, fieldName, models.NewStringValue(SYNDR_DEFAULT), schema)
} // InitializeDocumentFields initializes document fields based on bundle schema.
// For each field definition:
// - If field is required and not provided: return error
// - If field has default value and not provided: set to SYNDR_DEFAULT
// - If field is optional and not provided: set to SYNDR_NULL
//
// This should be called during document creation to ensure proper NULL handling.
//
func (nh *NullHandler) InitializeDocumentFields(doc *models.Document, bundle *models.Bundle, providedFields map[string]bool) error {
	schema := bundle.DocumentStructure.FieldSchema()
	if bundle.DocumentStructure.FieldDefinitions == nil {
		return fmt.Errorf("bundle '%s' has no field definitions", bundle.Name)
	}

	for fieldName, fieldDef := range bundle.DocumentStructure.FieldDefinitions {
		if fieldName == "DocumentID" {
			continue
		}
		if providedFields[fieldName] {
			if fv, exists := getDocField(doc, fieldName, schema); exists {
				setDocField(doc, fieldName, models.NewInterfaceValue(nh.EscapeUserValue(fv.AsInterface())), schema)
			}
			continue
		}
		if fieldDef.IsRequired {
			nh.logger.Warnf("[NULL_HANDLER] Required field '%s' missing during initialization", fieldName)
			return fmt.Errorf("required field '%s' is missing", fieldName)
		}
		if fieldDef.DefaultValue != nil {
			nh.SetDefaultField(doc, fieldName, schema)
			nh.logger.Debugf("[NULL_HANDLER] Field '%s' set to SYNDR_DEFAULT", fieldName)
		} else {
			setDocField(doc, fieldName, models.NewStringValue(SYNDR_NULL), schema)
			nh.logger.Debugf("[NULL_HANDLER] Field '%s' set to SYNDR_NULL (optional, no default)", fieldName)
		}
	}
	return nil
}

// ToJSON converts a document to a JSON-compatible map, replacing magic values
// with standard nil. This provides a clean external API that hides the internal
// magic value representation.
//
// Example:
//
//	jsonData := handler.ToJSON(document)
//	// Fields with SYNDR_NULL become JSON null
//	// Fields with SYNDR_ESCAPED values are unescaped
func (nh *NullHandler) ToJSON(doc *models.Document, schema *models.BundleFieldSchema) map[string]interface{} {
	if doc == nil {
		return nil
	}
	result := make(map[string]interface{})
	result["DocumentID"] = doc.DocumentID
	if schema != nil && len(doc.Values) > 0 {
		for i, name := range schema.Names {
			if name == "DocumentID" {
				continue
			}
			if i < len(doc.Values) {
				if nh.IsNullValue(doc.Values[i].AsInterface()) {
					result[name] = nil
				} else {
					result[name] = nh.UnescapeValue(doc.Values[i].AsInterface())
				}
			}
		}
		return result
	}
	if doc.Data != nil {
		for name, v := range doc.Data {
			if nh.IsNullValue(v) {
				result[name] = nil
			} else {
				result[name] = nh.UnescapeValue(v)
			}
		}
	}
	return result
}

func (nh *NullHandler) FromJSON(jsonData map[string]interface{}, doc *models.Document, schema *models.BundleFieldSchema) {
	for fieldName, value := range jsonData {
		if fieldName == "DocumentID" {
			if s, ok := value.(string); ok {
				doc.DocumentID = s
			}
			continue
		}
		nh.SetFieldValue(doc, fieldName, value, schema)
	}
}

// CompareValues compares two values for equality, considering magic values.
// Two NULL values are considered equal. Escaped values are unescaped before comparison.
//
// Example:
//
//	equal := handler.CompareValues(value1, value2)
//	// SYNDR_NULL == SYNDR_NULL -> true
//	// SYNDR_NULL == SYNDR_MISSING -> false (different NULL semantics)
func (nh *NullHandler) CompareValues(value1, value2 interface{}) bool {
	// Get NULL types
	null1 := nh.GetNullTypeFromValue(value1)
	null2 := nh.GetNullTypeFromValue(value2)

	// If both are NULL, compare the NULL types
	if null1 != HasValue && null2 != HasValue {
		return null1 == null2
	}

	// If one is NULL and other isn't, they're not equal
	if null1 != HasValue || null2 != HasValue {
		return false
	}

	// Both have values - unescape and compare
	unescaped1 := nh.UnescapeValue(value1)
	unescaped2 := nh.UnescapeValue(value2)

	return unescaped1 == unescaped2
}

// GetNullStatistics returns statistics about NULL values in a document.
// Useful for debugging and optimization.
//
// TODO: Integrate with hot key tracker to optimize NULL-heavy fields
// TODO: Add telemetry for NULL access patterns
func (nh *NullHandler) GetNullStatistics(doc *models.Document, schema *models.BundleFieldSchema) map[string]int {
	stats := map[string]int{
		"total_fields":  0,
		"has_value":     0,
		"explicit_null": 0,
		"missing":       0,
		"deleted":       0,
		"default_value": 0,
	}
	if doc == nil {
		return stats
	}
	if schema != nil && len(doc.Values) > 0 {
		stats["total_fields"] = len(doc.Values)
		for _, fv := range doc.Values {
			nullType := nh.GetNullTypeFromValue(fv.AsInterface())
			switch nullType {
			case HasValue:
				stats["has_value"]++
			case ExplicitNull:
				stats["explicit_null"]++
			case MissingField:
				stats["missing"]++
			case DeletedField:
				stats["deleted"]++
			case DefaultValue:
				stats["default_value"]++
			}
		}
		return stats
	}
	if doc.Data != nil {
		stats["total_fields"] = len(doc.Data)
		for _, v := range doc.Data {
			nullType := nh.GetNullTypeFromValue(v)
			switch nullType {
			case HasValue:
				stats["has_value"]++
			case ExplicitNull:
				stats["explicit_null"]++
			case MissingField:
				stats["missing"]++
			case DeletedField:
				stats["deleted"]++
			case DefaultValue:
				stats["default_value"]++
			}
		}
	}
	return stats
}

// ShouldIndexValue determines if a value should be included in an index.
// By default, all values including magic values are indexed to support
// queries like "WHERE field IS NULL".
//
// TODO: Add configuration option to exclude certain NULL types from indexes
// TODO: Optimize index storage for NULL-heavy fields (bitmap compression)
func (nh *NullHandler) ShouldIndexValue(value interface{}) bool {
	// Index all values including magic values
	// This allows queries like: WHERE Email IS NULL
	// or WHERE Email = "::SYNDR_NULL::"
	return true
}

// GetMagicValueForQuery converts a query value to the appropriate magic value
// if the query is asking for NULL values. This allows natural NULL queries
// to work correctly.
//
// Example:
//
//	value := handler.GetMagicValueForQuery("NULL")
//	// Returns SYNDR_NULL
//
// TODO: Integrate with query parser to support "IS NULL" syntax
// TODO: Support "IS NOT NULL" queries
func (nh *NullHandler) GetMagicValueForQuery(queryValue interface{}) interface{} {
	if queryValue == nil {
		return SYNDR_NULL
	}

	strValue, ok := queryValue.(string)
	if !ok {
		return queryValue
	}

	// Convert common NULL representations to SYNDR_NULL
	upperValue := strings.ToUpper(strings.TrimSpace(strValue))
	switch upperValue {
	case "NULL", "SYNDR_NULL", "::SYNDR_NULL::":
		return SYNDR_NULL
	case "MISSING", "SYNDR_MISSING", "::SYNDR_MISSING::":
		return SYNDR_MISSING
	case "DELETED", "SYNDR_DELETED", "::SYNDR_DELETED::":
		return SYNDR_DELETED
	case "DEFAULT", "SYNDR_DEFAULT", "::SYNDR_DEFAULT::":
		return SYNDR_DEFAULT
	default:
		return queryValue
	}
}

// IsNullQueryValue checks if a query value represents NULL.
// This is used by WHERE clause evaluation to detect NULL comparisons.
//
// Example:
//
//	if handler.IsNullQueryValue("NULL") {
//	    // Query is checking for NULL values
//	}
//
// Supports: NULL, null, SYNDR_NULL, ::SYNDR_NULL::
func (nh *NullHandler) IsNullQueryValue(value interface{}) bool {
	if value == nil {
		return true
	}

	strValue, ok := value.(string)
	if !ok {
		return false
	}

	upperValue := strings.ToUpper(strings.TrimSpace(strValue))
	return upperValue == "NULL" ||
		upperValue == "SYNDR_NULL" ||
		upperValue == "::SYNDR_NULL::"
}
