package planner

/*
DISTINCT QUERY HELPERS

This file provides helper functions for DISTINCT query deduplication operations.
It implements field extraction, comparison, serialization, and index lookup utilities
used by the DistinctNode execution node.

KEY RESPONSIBILITIES:
- Extract DISTINCT field values from documents (single-field, multi-field, and all-fields)
- Compute deterministic hashes for composite field values
- Compare and check equality of field value slices for sorting and deduplication
- Serialize field values for hash table storage
- Find suitable indexes for index-based DISTINCT optimization

DESIGN PRINCIPLES:
- Single Responsibility: Each function has one clear purpose
- DRY: Serialization logic reused from bloom filter package
- Type Safety: Handles all SyndrDB field types (nil, bool, int64, float64, string)

DISTINCT FIELD HANDLING:
- Empty fields slice: Extract ALL document fields sorted alphabetically (SELECT DISTINCT *)
- Single field: Extract one field value (SELECT DISTINCT status)
- Multiple fields: Extract fields in order (SELECT DISTINCT country, status)

COMPARISON ORDERING:
- nil < bool < numeric (int64/float64) < string
- Within same type: natural ordering (false < true, numbers ascending, strings lexicographic)
*/

import (
	"fmt"
	"hash/fnv"
	"sort"
	"syndrdb/src/internal/domain/models"
	"syndrdb/src/internal/query/bloomfilter"
)

// extractDistinctFields extracts field values from a document for DISTINCT comparison
// If fields slice is empty (SELECT DISTINCT * case), extracts all document fields
// sorted alphabetically by field name for deterministic behavior
//
// Parameters:
//   - doc: Document to extract fields from
//   - fields: Field names to extract (empty = all fields)
//
// Returns:
//   - Slice of field values in order
//   - Error if field extraction fails
func extractDistinctFields(doc *models.Document, fields []string) ([]interface{}, error) {
	if doc == nil {
		return nil, fmt.Errorf("cannot extract fields from nil document")
	}

	// Handle SELECT DISTINCT * case - extract all fields alphabetically
	if len(fields) == 0 {
		// Get all field names sorted alphabetically for deterministic ordering
		fieldNames := make([]string, 0, len(doc.Fields))
		for fieldName := range doc.Fields {
			fieldNames = append(fieldNames, fieldName)
		}
		sort.Strings(fieldNames)

		// Extract values in alphabetical field order
		values := make([]interface{}, 0, len(fieldNames))
		for _, fieldName := range fieldNames {
			field := doc.Fields[fieldName]
			values = append(values, field.Value)
		}
		return values, nil
	}

	// Extract specified fields in order
	values := make([]interface{}, 0, len(fields))
	for _, fieldName := range fields {
		field, exists := doc.Fields[fieldName]
		if !exists {
			// Field doesn't exist - treat as nil for consistency
			values = append(values, nil)
		} else {
			values = append(values, field.Value)
		}
	}

	return values, nil
}

// computeMultiFieldHash computes a deterministic hash for multiple field values
// Uses bloom filter's SerializeValues for consistent serialization, then FNV-1a hashing
//
// Parameters:
//   - values: Field values to hash together
//
// Returns:
//   - 64-bit hash value
//   - Error if serialization fails
func computeMultiFieldHash(values []interface{}) (uint64, error) {
	// Serialize values using bloom filter's length-delimited encoding
	serialized, err := bloomfilter.SerializeValues(values)
	if err != nil {
		return 0, fmt.Errorf("failed to serialize values for hashing: %w", err)
	}

	// Compute FNV-1a hash of serialized bytes
	hasher := fnv.New64a()
	hasher.Write(serialized)
	return hasher.Sum64(), nil
}

// compareDocumentFields compares two field value slices for sorting
// Returns -1 if vals1 < vals2, 0 if equal, 1 if vals1 > vals2
//
// Type ordering: nil < bool < numeric < string
// Within same type: natural ordering
//
// Parameters:
//   - vals1: First value slice
//   - vals2: Second value slice
//
// Returns:
//   - -1, 0, or 1 for sort ordering
func compareDocumentFields(vals1, vals2 []interface{}) int {
	// Compare element by element
	minLen := len(vals1)
	if len(vals2) < minLen {
		minLen = len(vals2)
	}

	for i := 0; i < minLen; i++ {
		cmp := compareValues(vals1[i], vals2[i])
		if cmp != 0 {
			return cmp
		}
	}

	// All elements equal up to minLen, shorter slice is "less than"
	if len(vals1) < len(vals2) {
		return -1
	} else if len(vals1) > len(vals2) {
		return 1
	}
	return 0
}

// compareValues compares two individual values for sorting
// Type ordering: nil < bool < numeric < string
//
// Parameters:
//   - v1: First value
//   - v2: Second value
//
// Returns:
//   - -1, 0, or 1 for sort ordering
func compareValues(v1, v2 interface{}) int {
	// Handle nil values
	if v1 == nil && v2 == nil {
		return 0
	}
	if v1 == nil {
		return -1 // nil is less than everything
	}
	if v2 == nil {
		return 1
	}

	// Type precedence: nil < bool < numeric < string
	type1 := getTypeOrder(v1)
	type2 := getTypeOrder(v2)

	if type1 != type2 {
		if type1 < type2 {
			return -1
		}
		return 1
	}

	// Same type - compare values
	switch val1 := v1.(type) {
	case bool:
		val2 := v2.(bool)
		if val1 == val2 {
			return 0
		}
		if !val1 && val2 { // false < true
			return -1
		}
		return 1

	case int:
		val2, ok := v2.(int)
		if !ok {
			// Try int64
			if val2Int64, ok := v2.(int64); ok {
				if int64(val1) < val2Int64 {
					return -1
				} else if int64(val1) > val2Int64 {
					return 1
				}
				return 0
			}
			return 0
		}
		if val1 < val2 {
			return -1
		} else if val1 > val2 {
			return 1
		}
		return 0

	case int64:
		val2, ok := v2.(int64)
		if !ok {
			// Try int
			if val2Int, ok := v2.(int); ok {
				if val1 < int64(val2Int) {
					return -1
				} else if val1 > int64(val2Int) {
					return 1
				}
				return 0
			}
			return 0
		}
		if val1 < val2 {
			return -1
		} else if val1 > val2 {
			return 1
		}
		return 0

	case float64:
		val2 := v2.(float64)
		if val1 < val2 {
			return -1
		} else if val1 > val2 {
			return 1
		}
		return 0

	case string:
		val2 := v2.(string)
		if val1 < val2 {
			return -1
		} else if val1 > val2 {
			return 1
		}
		return 0

	default:
		// Fallback: convert to string and compare
		str1 := fmt.Sprintf("%v", v1)
		str2 := fmt.Sprintf("%v", v2)
		if str1 < str2 {
			return -1
		} else if str1 > str2 {
			return 1
		}
		return 0
	}
}

// getTypeOrder returns type precedence for comparison
// nil=0, bool=1, numeric=2, string=3, other=4
func getTypeOrder(v interface{}) int {
	if v == nil {
		return 0
	}
	switch v.(type) {
	case bool:
		return 1
	case int, int64, float64:
		return 2
	case string:
		return 3
	default:
		return 4
	}
}

// equalDocumentFields checks if two field value slices are exactly equal
//
// Parameters:
//   - vals1: First value slice
//   - vals2: Second value slice
//
// Returns:
//   - true if all elements are equal, false otherwise
func equalDocumentFields(vals1, vals2 []interface{}) bool {
	if len(vals1) != len(vals2) {
		return false
	}

	for i := range vals1 {
		if !equalValues(vals1[i], vals2[i]) {
			return false
		}
	}

	return true
}

// equalValues checks if two individual values are equal
func equalValues(v1, v2 interface{}) bool {
	// Handle nil
	if v1 == nil && v2 == nil {
		return true
	}
	if v1 == nil || v2 == nil {
		return false
	}

	// Type must match
	switch val1 := v1.(type) {
	case bool:
		val2, ok := v2.(bool)
		return ok && val1 == val2

	case int:
		// Handle int/int64 cross-type equality
		if val2, ok := v2.(int); ok {
			return val1 == val2
		}
		if val2, ok := v2.(int64); ok {
			return int64(val1) == val2
		}
		return false

	case int64:
		// Handle int64/int cross-type equality
		if val2, ok := v2.(int64); ok {
			return val1 == val2
		}
		if val2, ok := v2.(int); ok {
			return val1 == int64(val2)
		}
		return false

	case float64:
		val2, ok := v2.(float64)
		return ok && val1 == val2

	case string:
		val2, ok := v2.(string)
		return ok && val1 == val2

	default:
		// Fallback: convert to string and compare
		return fmt.Sprintf("%v", v1) == fmt.Sprintf("%v", v2)
	}
}

// serializeFieldValues serializes field values into a byte array
// Wraps bloom filter's SerializeValues for code reuse (DRY principle)
//
// Parameters:
//   - values: Field values to serialize
//
// Returns:
//   - Serialized byte array
//   - Error if serialization fails
func serializeFieldValues(values []interface{}) ([]byte, error) {
	return bloomfilter.SerializeValues(values)
}

// findIndexForDistinctFields checks if a suitable index exists for DISTINCT operation
// Only supports single-field DISTINCT in this implementation
// Prefers btree indexes over hash indexes (btree supports efficient iteration)
//
// Parameters:
//   - bundle: Bundle containing index metadata
//   - fields: DISTINCT field names (must be single field)
//
// Returns:
//   - Index reference if suitable index found, nil otherwise
func findIndexForDistinctFields(bundle *models.Bundle, fields []string) *models.IndexReference {
	if bundle == nil || len(fields) != 1 {
		// Multi-field DISTINCT requires composite indexes
		// TODO: I could support multi-column DISTINCT with composite indexes by checking
		// if index field list matches or prefixes DISTINCT field list, and could implement
		// partial index usage where single index on first DISTINCT field reduces candidate
		// set before hash/sort deduplication on remaining fields
		return nil
	}

	fieldName := fields[0]
	var btreeIndex *models.IndexReference
	var hashIndex *models.IndexReference

	// Search for indexes on the DISTINCT field
	for _, indexRef := range bundle.Indexes {
		if len(indexRef.Fields) == 1 && indexRef.Fields[0].Name == fieldName {
			if indexRef.IndexType == "btree" {
				btreeIndex = &indexRef
				break // Prefer btree, stop searching
			} else if indexRef.IndexType == "hash" {
				hashIndex = &indexRef
			}
		}
	}

	// Prefer btree over hash (btree supports ordered iteration for DISTINCT)
	if btreeIndex != nil {
		return btreeIndex
	}

	// Hash indexes don't support efficient DISTINCT iteration, return nil
	// (Could be used for lookups but not for scanning distinct values)
	_ = hashIndex
	return nil
}

// findBTreeIndexForGroupByField returns the name of a B-tree index on the given field, if one exists.
// Used to enable index-ordered scan for single-field GROUP BY (avoids in-memory sort when data is already ordered by the index).
//
// Parameters:
//   - bundle: Bundle containing index metadata
//   - fieldName: Unqualified GROUP BY field name (e.g. "category")
//
// Returns:
//   - indexName and true if a B-tree index on that field exists, else ("", false)
func findBTreeIndexForGroupByField(bundle *models.Bundle, fieldName string) (indexName string, ok bool) {
	if bundle == nil || bundle.Indexes == nil {
		return "", false
	}
	for name, ir := range bundle.Indexes {
		if ir.IndexType == "btree" && len(ir.Fields) == 1 && ir.Fields[0].Name == fieldName {
			return name, true
		}
	}
	return "", false
}
