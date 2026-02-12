package syndrQL

import "strings"

// FieldType represents the resolved type of an expression node.
// This is the core of the semantic type system.
type FieldType int

const (
	// FieldTypeUnknown means the type could not be determined (schema-less bundle,
	// dynamic field, or mixed-type field). Comparisons involving Unknown are
	// permitted and deferred to runtime.
	FieldTypeUnknown FieldType = iota

	// Primitive types
	FieldTypeString
	FieldTypeInt
	FieldTypeFloat
	FieldTypeBool
	FieldTypeNull     // The NULL literal; compatible with any type via IS NULL
	FieldTypeDateTime // Full timestamp
	FieldTypeDate     // Date-only

	// Composite / special types
	FieldTypeArray    // Array literal [1, 2, 3] or IN-list
	FieldTypeInterval // INTERVAL expression
	FieldTypeDocument // Nested document / JSON object
)

// String returns a human-readable name for error messages.
func (ft FieldType) String() string {
	switch ft {
	case FieldTypeUnknown:
		return "Unknown"
	case FieldTypeString:
		return "String"
	case FieldTypeInt:
		return "Int"
	case FieldTypeFloat:
		return "Float"
	case FieldTypeBool:
		return "Bool"
	case FieldTypeNull:
		return "Null"
	case FieldTypeDateTime:
		return "DateTime"
	case FieldTypeDate:
		return "Date"
	case FieldTypeArray:
		return "Array"
	case FieldTypeInterval:
		return "Interval"
	case FieldTypeDocument:
		return "Document"
	default:
		return "Unknown"
	}
}

// IsNumeric returns true for Int, Float, and Unknown (Unknown is treated
// as potentially numeric to avoid false negatives in schema-less mode).
func (ft FieldType) IsNumeric() bool {
	return ft == FieldTypeInt || ft == FieldTypeFloat || ft == FieldTypeUnknown
}

// IsComparable returns true if the type supports <, >, <=, >= operators.
func (ft FieldType) IsComparable() bool {
	return ft == FieldTypeInt || ft == FieldTypeFloat || ft == FieldTypeString ||
		ft == FieldTypeDateTime || ft == FieldTypeDate || ft == FieldTypeUnknown
}

// IsCompatibleWith returns true if two types can be compared or combined
// in a binary operation. Unknown is compatible with everything.
func (ft FieldType) IsCompatibleWith(other FieldType) bool {
	if ft == FieldTypeUnknown || other == FieldTypeUnknown {
		return true
	}
	if ft == FieldTypeNull || other == FieldTypeNull {
		return true // NULL is comparable with anything via IS NULL / equality
	}
	// Int and Float are cross-compatible (int promotes to float)
	if (ft == FieldTypeInt || ft == FieldTypeFloat) &&
		(other == FieldTypeInt || other == FieldTypeFloat) {
		return true
	}
	// DateTime and Date are cross-compatible
	if (ft == FieldTypeDateTime || ft == FieldTypeDate) &&
		(other == FieldTypeDateTime || other == FieldTypeDate) {
		return true
	}
	return ft == other
}

// BundleFieldTypeToFieldType converts a bundle schema type string (from
// models.FieldDefinition.Type) to a FieldType.
func BundleFieldTypeToFieldType(schemaType string) FieldType {
	switch strings.ToLower(schemaType) {
	case "string":
		return FieldTypeString
	case "int", "integer":
		return FieldTypeInt
	case "float", "double", "number":
		return FieldTypeFloat
	case "bool", "boolean":
		return FieldTypeBool
	case "datetime", "timestamp":
		return FieldTypeDateTime
	case "date":
		return FieldTypeDate
	case "json", "document", "object":
		return FieldTypeDocument
	case "array":
		return FieldTypeArray
	default:
		return FieldTypeUnknown
	}
}

// GetResolvedType extracts the ResolvedType from any Expression node.
// Returns FieldTypeUnknown if the expression type doesn't carry a ResolvedType.
func GetResolvedType(expr Expression) FieldType {
	if expr == nil {
		return FieldTypeUnknown
	}
	switch e := expr.(type) {
	case *LiteralExpression:
		return e.ResolvedType
	case *IdentifierExpression:
		return e.ResolvedType
	case *BinaryExpression:
		return e.ResolvedType
	case *UnaryExpression:
		return e.ResolvedType
	case *CallExpression:
		return e.ResolvedType
	case *ArrayExpression:
		return e.ResolvedType
	case *GroupedExpression:
		return e.ResolvedType
	case *QualifiedIdentifierExpression:
		return e.ResolvedType
	case *SubqueryExpression:
		return e.ResolvedType
	case *ParameterExpression:
		return e.ResolvedType
	case *IntervalExpression:
		return e.ResolvedType
	case *AtTimeZoneExpression:
		return e.ResolvedType
	default:
		return FieldTypeUnknown
	}
}
