/*
DOCUMENT SORTING SYSTEM

This file implements the document sorting functionality for ORDER BY clauses in SyndrDB.
It provides efficient sorting algorithms that can handle multiple sort fields and directions,
following PostgreSQL-style sorting semantics while working with SyndrDB's document model.

SORTING FEATURES:
1. Multi-field sorting - Sort by multiple fields in order of precedence
2. Mixed directions - Each field can be ASC or DESC independently
3. Type-aware sorting - Proper handling of strings, numbers, booleans, dates
4. NULL handling - NULL values sorted according to SQL standards
5. Stable sorting - Preserves order of equal elements

SORTING ALGORITHMS:
The implementation uses Go's stable sort with custom comparison functions
that handle the multi-field, multi-direction sorting requirements.

TYPE HANDLING:
- Strings: Lexicographic comparison (case-sensitive by default)
- Numbers: Numeric comparison (int, float64)
- Booleans: false < true
- NULL/nil: Sorted last in ASC, first in DESC
- Mixed types: Type precedence ordering

PERFORMANCE:
The sorting is optimized for typical document sizes and uses stable algorithms
to ensure predictable and consistent results across multiple executions.

ERROR HANDLING:
The sorting system provides comprehensive error handling for:
- Missing fields in documents
- Type conversion errors
- Invalid sort field specifications

This implementation follows SyndrDB's comprehensive error handling practices
and integrates seamlessly with the existing query execution pipeline.
*/

package queryparser

import (
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"syndrdb/src/internal/domain/models"

	"go.uber.org/zap"
)

// DocumentSorter handles sorting of document collections based on ORDER BY clauses.
// Schema is required for schema-ordered document Values; set via SetSchema before SortDocuments.
type DocumentSorter struct {
	orderBy *OrderByClause
	logger  *zap.SugaredLogger
	schema  *models.BundleFieldSchema
}

// NewDocumentSorter creates a new document sorter with the given ORDER BY clause.
func NewDocumentSorter(orderBy *OrderByClause, logger *zap.SugaredLogger) *DocumentSorter {
	return &DocumentSorter{
		orderBy: orderBy,
		logger:  logger,
	}
}

// SetSchema sets the bundle field schema for document field access (required for Values-based documents).
func (ds *DocumentSorter) SetSchema(schema *models.BundleFieldSchema) {
	ds.schema = schema
}

// SortDocuments sorts a slice of documents according to the ORDER BY clause
// This function follows SyndrDB comprehensive error handling practices
// Parameters:
//   - documents: Slice of documents to sort
//
// Returns:
//   - error: Any error that occurred during sorting
func (ds *DocumentSorter) SortDocuments(documents []*models.Document) error {
	if ds.orderBy == nil || len(ds.orderBy.Fields) == 0 {
		// No sorting required
		return nil
	}

	ds.logger.Debugf("Sorting %d documents by %d fields", len(documents), len(ds.orderBy.Fields))

	// Use Go's stable sort with custom comparison function
	sort.SliceStable(documents, func(i, j int) bool {
		return ds.compareDocuments(documents[i], documents[j])
	})

	ds.logger.Debugf("Document sorting completed successfully")
	return nil
}

// SortDocumentMap sorts a map of documents and returns a sorted slice
// This function follows SyndrDB comprehensive error handling practices
// Parameters:
//   - documentMap: Map of documents to sort
//
// Returns:
//   - []*models.Document: Sorted slice of documents
//   - error: Any error that occurred during sorting
func (ds *DocumentSorter) SortDocumentMap(documentMap map[string]*models.Document) ([]*models.Document, error) {
	if ds.orderBy == nil || len(ds.orderBy.Fields) == 0 {
		// No sorting required, just convert to slice
		documents := make([]*models.Document, 0, len(documentMap))
		for _, doc := range documentMap {
			documents = append(documents, doc)
		}
		return documents, nil
	}

	// Convert map to slice
	documents := make([]*models.Document, 0, len(documentMap))
	for _, doc := range documentMap {
		documents = append(documents, doc)
	}

	// Sort the slice
	err := ds.SortDocuments(documents)
	if err != nil {
		return nil, fmt.Errorf("failed to sort documents: %w", err)
	}

	return documents, nil
}

// compareDocuments compares two documents according to the ORDER BY fields
// Returns true if document i should come before document j
func (ds *DocumentSorter) compareDocuments(doc1, doc2 *models.Document) bool {
	for _, field := range ds.orderBy.Fields {
		// Get field values from both documents
		value1, exists1 := ds.getFieldValue(doc1, field.FieldName)
		value2, exists2 := ds.getFieldValue(doc2, field.FieldName)

		// Handle missing fields (treat as NULL)
		if !exists1 && !exists2 {
			continue // Both missing, try next field
		}
		if !exists1 {
			// doc1 has NULL, doc2 has value
			return field.Direction == SortDesc // NULL sorts last in ASC, first in DESC
		}
		if !exists2 {
			// doc1 has value, doc2 has NULL
			return field.Direction == SortAsc // Value sorts first in ASC, last in DESC
		}

		// Both documents have values for this field
		comparison := ds.compareValues(value1, value2)

		if comparison != 0 {
			// Values are different, return based on comparison and direction
			if field.Direction == SortAsc {
				return comparison < 0
			} else {
				return comparison > 0
			}
		}

		// Values are equal, continue to next field
	}

	// All fields are equal
	return false
}

// getFieldValue extracts a field value from a document using schema-ordered Values when schema is set.
func (ds *DocumentSorter) getFieldValue(doc *models.Document, fieldName string) (models.FieldValue, bool) {
	if strings.EqualFold(fieldName, "documentid") {
		return models.NewStringValue(doc.DocumentID), true
	}
	if ds.schema != nil && len(doc.Values) > 0 {
		if idx, ok := ds.schema.NameToIndex[fieldName]; ok && idx < len(doc.Values) {
			return doc.Values[idx], true
		}
		// Case-insensitive fallback
		for i, name := range ds.schema.Names {
			if strings.EqualFold(name, fieldName) && i < len(doc.Values) {
				return doc.Values[i], true
			}
		}
		return models.FieldValue{}, false
	}
	if doc.Data != nil {
		if v, ok := doc.Data[fieldName]; ok {
			return models.NewInterfaceValue(v), true
		}
	}
	return models.FieldValue{}, false
}

// compareValues compares two values and returns:
// -1 if value1 < value2
//
//	0 if value1 == value2
//	1 if value1 > value2
func (ds *DocumentSorter) compareValues(value1, value2 interface{}) int {
	// Handle nil values
	if value1 == nil && value2 == nil {
		return 0
	}
	if value1 == nil {
		return -1
	}
	if value2 == nil {
		return 1
	}

	// Check if both values are FieldValue - use zero-allocation type-aware comparison
	fv1, ok1 := value1.(models.FieldValue)
	fv2, ok2 := value2.(models.FieldValue)
	if ok1 && ok2 {
		// Use FieldValue's built-in comparison methods for type-aware, zero-allocation comparison
		if fv1.CompareEqual(fv2) {
			return 0
		} else if fv1.CompareLessThan(fv2) {
			return -1
		} else {
			return 1
		}
	}

	// Try to compare as same types first
	if reflect.TypeOf(value1) == reflect.TypeOf(value2) {
		return ds.compareMatchingTypes(value1, value2)
	}

	// Try numeric comparison if both can be converted to numbers
	if num1, ok1 := ds.toNumber(value1); ok1 {
		if num2, ok2 := ds.toNumber(value2); ok2 {
			if num1 < num2 {
				return -1
			} else if num1 > num2 {
				return 1
			} else {
				return 0
			}
		}
	}

	// Fall back to string comparison
	str1 := ds.toString(value1)
	str2 := ds.toString(value2)

	if str1 < str2 {
		return -1
	} else if str1 > str2 {
		return 1
	} else {
		return 0
	}
}

// compareMatchingTypes compares values of the same type
func (ds *DocumentSorter) compareMatchingTypes(value1, value2 interface{}) int {
	switch v1 := value1.(type) {
	case string:
		v2 := value2.(string)
		if v1 < v2 {
			return -1
		} else if v1 > v2 {
			return 1
		} else {
			return 0
		}

	case int:
		v2 := value2.(int)
		if v1 < v2 {
			return -1
		} else if v1 > v2 {
			return 1
		} else {
			return 0
		}

	case int64:
		v2 := value2.(int64)
		if v1 < v2 {
			return -1
		} else if v1 > v2 {
			return 1
		} else {
			return 0
		}

	case float64:
		v2 := value2.(float64)
		if v1 < v2 {
			return -1
		} else if v1 > v2 {
			return 1
		} else {
			return 0
		}

	case bool:
		v2 := value2.(bool)
		if !v1 && v2 {
			return -1 // false < true
		} else if v1 && !v2 {
			return 1 // true > false
		} else {
			return 0 // same boolean value
		}

	default:
		// Fall back to string comparison for unknown types
		str1 := ds.toString(value1)
		str2 := ds.toString(value2)
		if str1 < str2 {
			return -1
		} else if str1 > str2 {
			return 1
		} else {
			return 0
		}
	}
}

// toNumber attempts to convert a value to float64 for numeric comparison
func (ds *DocumentSorter) toNumber(value interface{}) (float64, bool) {
	switch v := value.(type) {
	case int:
		return float64(v), true
	case int64:
		return float64(v), true
	case float64:
		return v, true
	case float32:
		return float64(v), true
	case string:
		if num, err := strconv.ParseFloat(v, 64); err == nil {
			return num, true
		}
		return 0, false
	default:
		return 0, false
	}
}

// toString converts any value to string for comparison
func (ds *DocumentSorter) toString(value interface{}) string {
	if value == nil {
		return ""
	}

	switch v := value.(type) {
	case string:
		return v
	case int:
		return strconv.Itoa(v)
	case int64:
		return strconv.FormatInt(v, 10)
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64)
	case float32:
		return strconv.FormatFloat(float64(v), 'f', -1, 32)
	case bool:
		return strconv.FormatBool(v)
	default:
		return fmt.Sprintf("%v", v)
	}
}

// SortDocumentsWithOrderBy is a convenience function that parses ORDER BY and sorts documents
// This function follows SyndrDB comprehensive error handling practices
// Parameters:
//   - documents: Documents to sort
//   - orderByClause: ORDER BY clause string (e.g., "name ASC, age DESC")
//   - logger: Logger for debug messages
//
// Returns:
//   - error: Any error that occurred during parsing or sorting
func SortDocumentsWithOrderBy(documents []*models.Document, orderByClause string, logger *zap.SugaredLogger) error {
	if orderByClause == "" {
		// No sorting required
		return nil
	}

	// Parse ORDER BY clause
	orderBy, err := parseOrderByFields(orderByClause, logger)
	if err != nil {
		return fmt.Errorf("failed to parse ORDER BY clause: %w", err)
	}

	// Create sorter and sort documents
	sorter := NewDocumentSorter(orderBy, logger)
	err = sorter.SortDocuments(documents)
	if err != nil {
		return fmt.Errorf("failed to sort documents: %w", err)
	}

	return nil
}

// SortDocumentMapWithOrderBy is a convenience function that sorts a document map
// This function follows SyndrDB comprehensive error handling practices
// Parameters:
//   - documentMap: Document map to sort
//   - orderByClause: ORDER BY clause string
//   - logger: Logger for debug messages
//
// Returns:
//   - []*models.Document: Sorted slice of documents
//   - error: Any error that occurred during parsing or sorting
func SortDocumentMapWithOrderBy(documentMap map[string]*models.Document, orderByClause string, logger *zap.SugaredLogger) ([]*models.Document, error) {
	if orderByClause == "" {
		// No sorting required, just convert to slice
		documents := make([]*models.Document, 0, len(documentMap))
		for _, doc := range documentMap {
			documents = append(documents, doc)
		}
		return documents, nil
	}

	// Parse ORDER BY clause
	orderBy, err := parseOrderByFields(orderByClause, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to parse ORDER BY clause: %w", err)
	}

	// Create sorter and sort documents
	sorter := NewDocumentSorter(orderBy, logger)
	documents, err := sorter.SortDocumentMap(documentMap)
	if err != nil {
		return nil, fmt.Errorf("failed to sort documents: %w", err)
	}

	return documents, nil
}
