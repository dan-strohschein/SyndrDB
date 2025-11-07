package graphQL

// filter_types.go
//
// PHASE 9: GraphQL Input Types for WHERE Clause Filtering
//
// This file defines the GraphQL input types used for filtering query results.
// It implements a structured, type-safe approach to building WHERE clauses that
// can be translated into SyndrDB's native query syntax.
//
// DESIGN PHILOSOPHY:
//
// Instead of passing raw SQL-like strings (e.g., "age > 18 AND status = 'active'"),
// we use GraphQL input types to provide:
// - Type safety: Field names and values are validated against the schema
// - Composability: Complex filters built from simple operators
// - IDE support: Autocomplete and validation in GraphQL clients
// - Security: Prevents injection attacks
//
// EXAMPLE USAGE:
//
//   query {
//     users(where: {
//       and: [
//         { age: { gt: 18 } }
//         { status: { eq: "active" } }
//         { or: [
//           { name: { like: "John%" } }
//           { email: { like: "%@example.com" } }
//         ]}
//       ]
//     }) {
//       id
//       name
//     }
//   }
//
// TRANSLATION TO SYNDRDB:
//   The above translates to SyndrDB query:
//   SELECT FROM users WHERE age > 18 AND status = "active" AND (name LIKE "John%" OR email LIKE "%@example.com")
//
// TODO: In Phase 10, I will add:
// - Relationship field filtering (e.g., filter users by their post count)
// - Full-text search operators
// - Geo-spatial operators for location-based filtering
// - Custom scalar type operators (JSON path queries, etc.)

import (
	"fmt"
	"strings"
)

// WhereInput represents the top-level filter input for GraphQL queries
// Supports both direct field filtering and logical operators (AND, OR, NOT)
//
// STRUCTURE:
//   - Fields: Map of field name → field-specific filters
//   - AND: Array of WhereInput (all must match)
//   - OR: Array of WhereInput (at least one must match)
//   - NOT: Single WhereInput (must not match)
//
// EXAMPLES:
//
//	Simple: { age: { gt: 18 } }
//	AND: { and: [{ age: { gt: 18 } }, { status: { eq: "active" } }] }
//	OR: { or: [{ name: { like: "John%" } }, { name: { like: "Jane%" } }] }
//	NOT: { not: { status: { eq: "deleted" } } }
//	Combined: { and: [{ age: { gt: 18 } }, { not: { status: { eq: "deleted" } } }] }
type WhereInput struct {
	// Fields contains field-specific filters
	// Key: field name (e.g., "age", "name", "status")
	// Value: field filter with comparison operators
	Fields map[string]*FieldFilter `json:"fields,omitempty"`

	// AND combines multiple conditions (all must be true)
	// Equivalent to SQL: (condition1 AND condition2 AND ...)
	AND []*WhereInput `json:"and,omitempty"`

	// OR combines multiple conditions (at least one must be true)
	// Equivalent to SQL: (condition1 OR condition2 OR ...)
	OR []*WhereInput `json:"or,omitempty"`

	// NOT negates a condition
	// Equivalent to SQL: NOT (condition)
	NOT *WhereInput `json:"not,omitempty"`
}

// FieldFilter contains comparison operators for a specific field
// Each operator is optional - only specified operators are applied
//
// OPERATOR SEMANTICS:
//
//	eq: Equals (=)
//	ne: Not equals (!=)
//	gt: Greater than (>)
//	gte: Greater than or equal (>=)
//	lt: Less than (<)
//	lte: Less than or equal (<=)
//	in: Value is in array (IN)
//	notIn: Value is not in array (NOT IN)
//	like: Pattern matching with wildcards (LIKE)
//	notLike: Pattern not matching (NOT LIKE)
//	isNull: Field is NULL
//	isNotNull: Field is NOT NULL
//
// TYPE COMPATIBILITY:
//
//	Numeric (int, float): eq, ne, gt, gte, lt, lte, in, notIn, isNull, isNotNull
//	String: eq, ne, in, notIn, like, notLike, isNull, isNotNull
//	Boolean: eq, ne, isNull, isNotNull
//	DateTime: eq, ne, gt, gte, lt, lte, in, notIn, isNull, isNotNull
//
// EXAMPLES:
//
//	Age > 18: { gt: 18 }
//	Name starts with "John": { like: "John%" }
//	Status is active or pending: { in: ["active", "pending"] }
//	Email is not null: { isNotNull: true }
type FieldFilter struct {
	// Equals operator (=)
	// Example: { eq: "active" } → status = "active"
	Eq interface{} `json:"eq,omitempty"`

	// Not equals operator (!=)
	// Example: { ne: "deleted" } → status != "deleted"
	Ne interface{} `json:"ne,omitempty"`

	// Greater than operator (>)
	// Example: { gt: 18 } → age > 18
	Gt interface{} `json:"gt,omitempty"`

	// Greater than or equal operator (>=)
	// Example: { gte: 18 } → age >= 18
	Gte interface{} `json:"gte,omitempty"`

	// Less than operator (<)
	// Example: { lt: 65 } → age < 65
	Lt interface{} `json:"lt,omitempty"`

	// Less than or equal operator (<=)
	// Example: { lte: 65 } → age <= 65
	Lte interface{} `json:"lte,omitempty"`

	// In operator (IN)
	// Example: { in: ["active", "pending"] } → status IN ("active", "pending")
	In []interface{} `json:"in,omitempty"`

	// Not in operator (NOT IN)
	// Example: { notIn: ["deleted", "archived"] } → status NOT IN ("deleted", "archived")
	NotIn []interface{} `json:"notIn,omitempty"`

	// Like operator (LIKE) with wildcard support
	// Wildcards: % (zero or more chars), _ (exactly one char)
	// Example: { like: "John%" } → name LIKE "John%"
	// Example: { like: "%@example.com" } → email LIKE "%@example.com"
	Like *string `json:"like,omitempty"`

	// Not like operator (NOT LIKE)
	// Example: { notLike: "%test%" } → name NOT LIKE "%test%"
	NotLike *string `json:"notLike,omitempty"`

	// Is null check
	// Example: { isNull: true } → email IS NULL
	// Example: { isNull: false } → email IS NOT NULL (equivalent to isNotNull: true)
	IsNull *bool `json:"isNull,omitempty"`

	// Is not null check
	// Example: { isNotNull: true } → email IS NOT NULL
	IsNotNull *bool `json:"isNotNull,omitempty"`
}

// OrderByInput specifies sort order for query results
// Supports single-field and multi-field sorting
//
// EXAMPLES:
//
//	Single field ascending: { field: "name", direction: "ASC" }
//	Single field descending: { field: "createdAt", direction: "DESC" }
//	Multiple fields: [
//	  { field: "status", direction: "ASC" },
//	  { field: "createdAt", direction: "DESC" }
//	]
//
// TRANSLATION:
//
//	ORDER BY status ASC, createdAt DESC
type OrderByInput struct {
	// Field name to sort by
	// Example: "name", "createdAt", "age"
	Field string `json:"field"`

	// Sort direction: "ASC" or "DESC"
	// Default: "ASC" if not specified
	Direction string `json:"direction"`
}

// OrderDirection represents sort direction constants
type OrderDirection string

const (
	// OrderAsc sorts in ascending order (A-Z, 0-9, oldest to newest)
	OrderAsc OrderDirection = "ASC"

	// OrderDesc sorts in descending order (Z-A, 9-0, newest to oldest)
	OrderDesc OrderDirection = "DESC"
)

// ParseWhereInput converts a raw map[string]interface{} from GraphQL args into WhereInput
// PHASE 9: Parse GraphQL where argument into structured filter
//
// This function handles the conversion from untyped GraphQL arguments to our typed
// WhereInput structure. It recursively processes nested AND, OR, NOT operators and
// validates field filters.
//
// ALGORITHM:
// 1. Check for logical operators (and, or, not) at top level
// 2. Process each operator recursively
// 3. Extract field filters from remaining keys
// 4. Validate operator values and types
// 5. Return structured WhereInput
//
// ERROR HANDLING:
//
//	Returns error if:
//	- Logical operator values are not arrays (for AND/OR) or object (for NOT)
//	- Field filter values are not objects
//	- Unknown operators are specified
func ParseWhereInput(raw map[string]interface{}) (*WhereInput, error) {
	if raw == nil || len(raw) == 0 {
		return nil, nil
	}

	where := &WhereInput{
		Fields: make(map[string]*FieldFilter),
	}

	for key, value := range raw {
		switch key {
		case "and", "AND":
			// Parse AND array
			andArray, ok := value.([]interface{})
			if !ok {
				return nil, fmt.Errorf("'and' operator must be an array")
			}

			where.AND = make([]*WhereInput, len(andArray))
			for i, item := range andArray {
				itemMap, ok := item.(map[string]interface{})
				if !ok {
					return nil, fmt.Errorf("'and' array items must be objects")
				}

				parsed, err := ParseWhereInput(itemMap)
				if err != nil {
					return nil, fmt.Errorf("invalid 'and' condition at index %d: %w", i, err)
				}
				where.AND[i] = parsed
			}

		case "or", "OR":
			// Parse OR array
			orArray, ok := value.([]interface{})
			if !ok {
				return nil, fmt.Errorf("'or' operator must be an array")
			}

			where.OR = make([]*WhereInput, len(orArray))
			for i, item := range orArray {
				itemMap, ok := item.(map[string]interface{})
				if !ok {
					return nil, fmt.Errorf("'or' array items must be objects")
				}

				parsed, err := ParseWhereInput(itemMap)
				if err != nil {
					return nil, fmt.Errorf("invalid 'or' condition at index %d: %w", i, err)
				}
				where.OR[i] = parsed
			}

		case "not", "NOT":
			// Parse NOT object
			notMap, ok := value.(map[string]interface{})
			if !ok {
				return nil, fmt.Errorf("'not' operator must be an object")
			}

			parsed, err := ParseWhereInput(notMap)
			if err != nil {
				return nil, fmt.Errorf("invalid 'not' condition: %w", err)
			}
			where.NOT = parsed

		default:
			// Parse field filter
			filterMap, ok := value.(map[string]interface{})
			if !ok {
				return nil, fmt.Errorf("field filter for '%s' must be an object", key)
			}

			fieldFilter, err := ParseFieldFilter(filterMap)
			if err != nil {
				return nil, fmt.Errorf("invalid filter for field '%s': %w", key, err)
			}
			where.Fields[key] = fieldFilter
		}
	}

	return where, nil
}

// ParseFieldFilter converts a raw map into a FieldFilter structure
// Extracts comparison operators and validates their values
func ParseFieldFilter(raw map[string]interface{}) (*FieldFilter, error) {
	filter := &FieldFilter{}

	for operator, value := range raw {
		switch operator {
		case "eq":
			filter.Eq = value

		case "ne":
			filter.Ne = value

		case "gt":
			filter.Gt = value

		case "gte":
			filter.Gte = value

		case "lt":
			filter.Lt = value

		case "lte":
			filter.Lte = value

		case "in":
			// Convert to []interface{}
			inArray, ok := value.([]interface{})
			if !ok {
				return nil, fmt.Errorf("'in' operator value must be an array")
			}
			filter.In = inArray

		case "notIn":
			// Convert to []interface{}
			notInArray, ok := value.([]interface{})
			if !ok {
				return nil, fmt.Errorf("'notIn' operator value must be an array")
			}
			filter.NotIn = notInArray

		case "like":
			likeStr, ok := value.(string)
			if !ok {
				return nil, fmt.Errorf("'like' operator value must be a string")
			}
			filter.Like = &likeStr

		case "notLike":
			notLikeStr, ok := value.(string)
			if !ok {
				return nil, fmt.Errorf("'notLike' operator value must be a string")
			}
			filter.NotLike = &notLikeStr

		case "isNull":
			isNullBool, ok := value.(bool)
			if !ok {
				return nil, fmt.Errorf("'isNull' operator value must be a boolean")
			}
			filter.IsNull = &isNullBool

		case "isNotNull":
			isNotNullBool, ok := value.(bool)
			if !ok {
				return nil, fmt.Errorf("'isNotNull' operator value must be a boolean")
			}
			filter.IsNotNull = &isNotNullBool

		default:
			return nil, fmt.Errorf("unknown filter operator: %s", operator)
		}
	}

	return filter, nil
}

// ParseOrderByInput converts raw map[string]interface{} into OrderByInput
// Supports both single object and array of objects
func ParseOrderByInput(raw interface{}) ([]OrderByInput, error) {
	if raw == nil {
		return nil, nil
	}

	// Check if it's an array
	if rawArray, ok := raw.([]interface{}); ok {
		result := make([]OrderByInput, len(rawArray))
		for i, item := range rawArray {
			itemMap, ok := item.(map[string]interface{})
			if !ok {
				return nil, fmt.Errorf("orderBy array item at index %d must be an object", i)
			}

			orderBy, err := parseSingleOrderBy(itemMap)
			if err != nil {
				return nil, fmt.Errorf("invalid orderBy at index %d: %w", i, err)
			}
			result[i] = *orderBy
		}
		return result, nil
	}

	// Check if it's a single object
	if rawMap, ok := raw.(map[string]interface{}); ok {
		orderBy, err := parseSingleOrderBy(rawMap)
		if err != nil {
			return nil, err
		}
		return []OrderByInput{*orderBy}, nil
	}

	return nil, fmt.Errorf("orderBy must be an object or array of objects")
}

// parseSingleOrderBy parses a single OrderByInput object
func parseSingleOrderBy(raw map[string]interface{}) (*OrderByInput, error) {
	orderBy := &OrderByInput{
		Direction: string(OrderAsc), // Default to ASC
	}

	// Parse field
	field, ok := raw["field"]
	if !ok {
		return nil, fmt.Errorf("orderBy must have 'field' property")
	}

	fieldStr, ok := field.(string)
	if !ok {
		return nil, fmt.Errorf("orderBy 'field' must be a string")
	}
	orderBy.Field = fieldStr

	// Parse direction (optional)
	if direction, ok := raw["direction"]; ok {
		directionStr, ok := direction.(string)
		if !ok {
			return nil, fmt.Errorf("orderBy 'direction' must be a string")
		}

		// Validate direction
		directionUpper := strings.ToUpper(directionStr)
		if directionUpper != string(OrderAsc) && directionUpper != string(OrderDesc) {
			return nil, fmt.Errorf("orderBy 'direction' must be 'ASC' or 'DESC'")
		}

		orderBy.Direction = directionUpper
	}

	return orderBy, nil
}

// IsEmpty checks if a WhereInput has no conditions
// Useful for optimization - skip filtering if where clause is empty
func (w *WhereInput) IsEmpty() bool {
	if w == nil {
		return true
	}

	return len(w.Fields) == 0 && len(w.AND) == 0 && len(w.OR) == 0 && w.NOT == nil
}

// HasLogicalOperators checks if WhereInput uses AND, OR, or NOT
// Useful for query optimization and execution planning
func (w *WhereInput) HasLogicalOperators() bool {
	if w == nil {
		return false
	}

	return len(w.AND) > 0 || len(w.OR) > 0 || w.NOT != nil
}
