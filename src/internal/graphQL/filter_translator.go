package graphQL

// filter_translator.go
//
// PHASE 9: GraphQL Filter to SyndrDB Query Translation
//
// This file implements the translation layer between GraphQL WhereInput types and
// SyndrDB's native WhereClause/WhereGroup structures. It converts type-safe GraphQL
// filters into the internal query representation used by SyndrDB's query engine.
//
// TRANSLATION FLOW:
//
//   GraphQL Query:
//     users(where: { and: [{ age: { gt: 18 } }, { status: { eq: "active" } }] })
//
//   ↓ ParseWhereInput (filter_types.go)
//
//   WhereInput struct:
//     AND: [
//       { Fields: { "age": { Gt: 18 } } }
//       { Fields: { "status": { Eq: "active" } } }
//     ]
//
//   ↓ TranslateWhereInput (this file)
//
//   WhereGroup struct:
//     Logic: "AND"
//     Clauses: [
//       { Field: "age", Operator: ">", Value: 18, Logic: "AND" }
//       { Field: "status", Operator: "=", Value: "active", Logic: "AND" }
//     ]
//
//   ↓ Query Execution
//
//   SyndrDB WHERE clause: age > 18 AND status = "active"
//
// DESIGN PRINCIPLES:
// - Single Responsibility: Only translates filters, doesn't execute queries
// - Open/Closed: Extensible to new operators without modifying existing code
// - DRY: Reuses logic for field filters across all types
//
// TODO: In Phase 10, I will optimize translation for:
// - Index-aware query planning (use indexes when available)
// - Query rewriting for performance (e.g., IN → multiple OR)
// - Predicate pushdown for relationship filters

import (
	"fmt"
	"strings"
	"syndrdb/src/internal/domain/models"
)

// FilterTranslator translates GraphQL filters to SyndrDB WhereGroup structures
type FilterTranslator struct {
	// Future: Add configuration options like:
	// - caseInsensitive: bool (for LIKE queries)
	// - maxComplexity: int (prevent overly complex filters)
}

// NewFilterTranslator creates a new filter translator
func NewFilterTranslator() *FilterTranslator {
	return &FilterTranslator{}
}

// TranslateWhereInput converts GraphQL WhereInput to SyndrDB WhereGroup
// PHASE 9: Main entry point for filter translation
//
// This function recursively processes the WhereInput structure, converting:
// - Field filters → WhereClauses
// - Logical operators (AND, OR, NOT) → WhereGroups
// - Nested conditions → Nested WhereGroups
//
// ALGORITHM:
// 1. Process field filters into WhereClauses
// 2. Process AND conditions (all must match)
// 3. Process OR conditions (at least one must match)
// 4. Process NOT conditions (invert logic)
// 5. Combine into WhereGroup with appropriate logic
//
// RETURNS:
//   - WhereGroup: Structured query condition
//   - error: If translation fails (invalid operators, type mismatches, etc.)
func (ft *FilterTranslator) TranslateWhereInput(where *WhereInput) (*models.WhereGroup, error) {
	if where == nil || where.IsEmpty() {
		return nil, nil
	}

	group := &models.WhereGroup{
		Clauses:   []models.WhereClause{},
		SubGroups: []models.WhereGroup{},
		Logic:     "AND", // Default logic for top-level group
	}

	// Process field filters
	// Each field filter becomes one or more WhereClauses
	for fieldName, fieldFilter := range where.Fields {
		clauses, err := ft.translateFieldFilter(fieldName, fieldFilter)
		if err != nil {
			return nil, fmt.Errorf("failed to translate filter for field '%s': %w", fieldName, err)
		}
		group.Clauses = append(group.Clauses, clauses...)
	}

	// Process AND conditions
	// Each AND condition becomes a subgroup
	if len(where.AND) > 0 {
		for i, andCondition := range where.AND {
			subGroup, err := ft.TranslateWhereInput(andCondition)
			if err != nil {
				return nil, fmt.Errorf("failed to translate AND condition at index %d: %w", i, err)
			}
			if subGroup != nil {
				subGroup.Logic = "AND"
				group.SubGroups = append(group.SubGroups, *subGroup)
			}
		}
	}

	// Process OR conditions
	// Each OR condition becomes a subgroup
	if len(where.OR) > 0 {
		for i, orCondition := range where.OR {
			subGroup, err := ft.TranslateWhereInput(orCondition)
			if err != nil {
				return nil, fmt.Errorf("failed to translate OR condition at index %d: %w", i, err)
			}
			if subGroup != nil {
				subGroup.Logic = "OR"
				group.SubGroups = append(group.SubGroups, *subGroup)
			}
		}
	}

	// Process NOT condition
	// NOT is implemented by inverting operators in the subgroup
	// Example: NOT { age: { gt: 18 } } becomes age <= 18
	if where.NOT != nil {
		notGroup, err := ft.TranslateWhereInput(where.NOT)
		if err != nil {
			return nil, fmt.Errorf("failed to translate NOT condition: %w", err)
		}
		if notGroup != nil {
			// Invert all operators in the NOT group
			invertedGroup, err := ft.invertWhereGroup(notGroup)
			if err != nil {
				return nil, fmt.Errorf("failed to invert NOT condition: %w", err)
			}
			group.SubGroups = append(group.SubGroups, *invertedGroup)
		}
	}

	return group, nil
}

// translateFieldFilter converts a FieldFilter to one or more WhereClauses
// PHASE 9: Per-field filter translation
//
// A single field filter can specify multiple operators:
//
//	{ age: { gt: 18, lt: 65 } } → age > 18 AND age < 65
//
// This function creates one WhereClause per operator specified.
//
// OPERATOR MAPPING:
//
//	eq → "="
//	ne → "!="
//	gt → ">"
//	gte → ">="
//	lt → "<"
//	lte → "<="
//	in → "IN"
//	notIn → "NOT IN"
//	like → "LIKE"
//	notLike → "NOT LIKE"
//	isNull → "IS NULL"
//	isNotNull → "IS NOT NULL"
func (ft *FilterTranslator) translateFieldFilter(fieldName string, filter *FieldFilter) ([]models.WhereClause, error) {
	clauses := []models.WhereClause{}

	// eq operator (=)
	if filter.Eq != nil {
		clauses = append(clauses, models.WhereClause{
			Field:    fieldName,
			Operator: "=",
			Value:    filter.Eq,
			Logic:    "AND",
		})
	}

	// ne operator (!=)
	if filter.Ne != nil {
		clauses = append(clauses, models.WhereClause{
			Field:    fieldName,
			Operator: "!=",
			Value:    filter.Ne,
			Logic:    "AND",
		})
	}

	// gt operator (>)
	if filter.Gt != nil {
		clauses = append(clauses, models.WhereClause{
			Field:    fieldName,
			Operator: ">",
			Value:    filter.Gt,
			Logic:    "AND",
		})
	}

	// gte operator (>=)
	if filter.Gte != nil {
		clauses = append(clauses, models.WhereClause{
			Field:    fieldName,
			Operator: ">=",
			Value:    filter.Gte,
			Logic:    "AND",
		})
	}

	// lt operator (<)
	if filter.Lt != nil {
		clauses = append(clauses, models.WhereClause{
			Field:    fieldName,
			Operator: "<",
			Value:    filter.Lt,
			Logic:    "AND",
		})
	}

	// lte operator (<=)
	if filter.Lte != nil {
		clauses = append(clauses, models.WhereClause{
			Field:    fieldName,
			Operator: "<=",
			Value:    filter.Lte,
			Logic:    "AND",
		})
	}

	// in operator (IN)
	if len(filter.In) > 0 {
		clauses = append(clauses, models.WhereClause{
			Field:    fieldName,
			Operator: "IN",
			Value:    filter.In,
			Logic:    "AND",
		})
	}

	// notIn operator (NOT IN)
	if len(filter.NotIn) > 0 {
		clauses = append(clauses, models.WhereClause{
			Field:    fieldName,
			Operator: "NOT IN",
			Value:    filter.NotIn,
			Logic:    "AND",
		})
	}

	// like operator (LIKE)
	if filter.Like != nil {
		clauses = append(clauses, models.WhereClause{
			Field:    fieldName,
			Operator: "LIKE",
			Value:    *filter.Like,
			Logic:    "AND",
		})
	}

	// notLike operator (NOT LIKE)
	if filter.NotLike != nil {
		clauses = append(clauses, models.WhereClause{
			Field:    fieldName,
			Operator: "NOT LIKE",
			Value:    *filter.NotLike,
			Logic:    "AND",
		})
	}

	// isNull operator
	// Translates to comparison against magic NULL value
	if filter.IsNull != nil {
		if *filter.IsNull {
			clauses = append(clauses, models.WhereClause{
				Field:    fieldName,
				Operator: "IS NULL",
				Value:    nil, // GraphQL uses nil, but evaluateClause checks magic value
				Logic:    "AND",
			})
		} else {
			// isNull: false means NOT NULL
			clauses = append(clauses, models.WhereClause{
				Field:    fieldName,
				Operator: "IS NOT NULL",
				Value:    nil, // GraphQL uses nil, but evaluateClause checks magic value
				Logic:    "AND",
			})
		}
	}

	// isNotNull operator
	// Translates to comparison against magic NULL value (inverted)
	if filter.IsNotNull != nil {
		if *filter.IsNotNull {
			clauses = append(clauses, models.WhereClause{
				Field:    fieldName,
				Operator: "IS NOT NULL",
				Value:    nil, // GraphQL uses nil, but evaluateClause checks magic value
				Logic:    "AND",
			})
		} else {
			// isNotNull: false means IS NULL
			clauses = append(clauses, models.WhereClause{
				Field:    fieldName,
				Operator: "IS NULL",
				Value:    nil, // GraphQL uses nil, but evaluateClause checks magic value
				Logic:    "AND",
			})
		}
	}

	if len(clauses) == 0 {
		return nil, fmt.Errorf("no valid operators specified for field '%s'", fieldName)
	}

	return clauses, nil
}

// invertWhereGroup inverts all operators in a WhereGroup for NOT logic
// PHASE 9: NOT operator implementation via operator inversion
//
// INVERSION RULES:
//
//	= → !=
//	!= → =
//	> → <=
//	>= → <
//	< → >=
//	<= → >
//	IN → NOT IN
//	NOT IN → IN
//	LIKE → NOT LIKE
//	NOT LIKE → LIKE
//	IS NULL → IS NOT NULL
//	IS NOT NULL → IS NULL
//
// LOGICAL OPERATOR INVERSION:
//
//	AND → OR (De Morgan's Law: NOT (A AND B) = NOT A OR NOT B)
//	OR → AND (De Morgan's Law: NOT (A OR B) = NOT A AND NOT B)
//
// EXAMPLE:
//
//	Input: NOT { and: [{ age: { gt: 18 } }, { status: { eq: "active" } }] }
//	Inverted: { or: [{ age: { lte: 18 } }, { status: { ne: "active" } }] }
//	Meaning: age <= 18 OR status != "active"
func (ft *FilterTranslator) invertWhereGroup(group *models.WhereGroup) (*models.WhereGroup, error) {
	inverted := &models.WhereGroup{
		Clauses:   []models.WhereClause{},
		SubGroups: []models.WhereGroup{},
		Logic:     ft.invertLogic(group.Logic),
	}

	// Invert each clause
	for _, clause := range group.Clauses {
		invertedClause, err := ft.invertWhereClause(clause)
		if err != nil {
			return nil, fmt.Errorf("failed to invert clause for field '%s': %w", clause.Field, err)
		}
		invertedClause.Logic = inverted.Logic
		inverted.Clauses = append(inverted.Clauses, invertedClause)
	}

	// Recursively invert subgroups
	for _, subGroup := range group.SubGroups {
		invertedSubGroup, err := ft.invertWhereGroup(&subGroup)
		if err != nil {
			return nil, err
		}
		inverted.SubGroups = append(inverted.SubGroups, *invertedSubGroup)
	}

	return inverted, nil
}

// invertWhereClause inverts a single WhereClause operator
func (ft *FilterTranslator) invertWhereClause(clause models.WhereClause) (models.WhereClause, error) {
	inverted := clause

	switch clause.Operator {
	case "=":
		inverted.Operator = "!="
	case "!=":
		inverted.Operator = "="
	case ">":
		inverted.Operator = "<="
	case ">=":
		inverted.Operator = "<"
	case "<":
		inverted.Operator = ">="
	case "<=":
		inverted.Operator = ">"
	case "IN":
		inverted.Operator = "NOT IN"
	case "NOT IN":
		inverted.Operator = "IN"
	case "LIKE":
		inverted.Operator = "NOT LIKE"
	case "NOT LIKE":
		inverted.Operator = "LIKE"
	case "IS NULL":
		inverted.Operator = "IS NOT NULL"
	case "IS NOT NULL":
		inverted.Operator = "IS NULL"
	default:
		return inverted, fmt.Errorf("unknown operator '%s' cannot be inverted", clause.Operator)
	}

	return inverted, nil
}

// invertLogic inverts logical operator following De Morgan's Laws
func (ft *FilterTranslator) invertLogic(logic string) string {
	switch strings.ToUpper(logic) {
	case "AND":
		return "OR"
	case "OR":
		return "AND"
	default:
		return logic
	}
}

// TranslateOrderBy converts GraphQL OrderByInput to SyndrDB ORDER BY syntax
// PHASE 9: ORDER BY translation
//
// GraphQL: [{ field: "name", direction: "ASC" }, { field: "createdAt", direction: "DESC" }]
// SyndrDB: "name ASC, createdAt DESC"
func (ft *FilterTranslator) TranslateOrderBy(orderBy []OrderByInput) string {
	if len(orderBy) == 0 {
		return ""
	}

	parts := make([]string, len(orderBy))
	for i, order := range orderBy {
		direction := strings.ToUpper(order.Direction)
		if direction != "ASC" && direction != "DESC" {
			direction = "ASC" // Default to ASC if invalid
		}
		parts[i] = fmt.Sprintf("%s %s", order.Field, direction)
	}

	return strings.Join(parts, ", ")
}

// BuildWhereClauseString converts WhereGroup to a string WHERE clause
// PHASE 9: String representation of WHERE clause for query execution
//
// This function is used when the query executor needs a string-based WHERE clause
// instead of the structured WhereGroup representation.
//
// EXAMPLE:
//
//	Input: WhereGroup with clauses [age > 18, status = "active"]
//	Output: "age > 18 AND status = 'active'"
//
// TODO: In Phase 10, I will add:
// - Proper value escaping/quoting for SQL injection prevention
// - Type-aware formatting (dates, strings, numbers)
// - Query parameterization support
func (ft *FilterTranslator) BuildWhereClauseString(group *models.WhereGroup) (string, error) {
	if group == nil {
		return "", nil
	}

	parts := []string{}

	// Build clause strings
	for _, clause := range group.Clauses {
		clauseStr, err := ft.buildClauseString(clause)
		if err != nil {
			return "", err
		}
		parts = append(parts, clauseStr)
	}

	// Build subgroup strings
	for _, subGroup := range group.SubGroups {
		subGroupStr, err := ft.BuildWhereClauseString(&subGroup)
		if err != nil {
			return "", err
		}
		if subGroupStr != "" {
			parts = append(parts, fmt.Sprintf("(%s)", subGroupStr))
		}
	}

	// Join with logic operator
	logic := strings.ToUpper(group.Logic)
	if logic != "AND" && logic != "OR" {
		logic = "AND" // Default to AND
	}

	return strings.Join(parts, " "+logic+" "), nil
}

// buildClauseString converts a single WhereClause to string
func (ft *FilterTranslator) buildClauseString(clause models.WhereClause) (string, error) {
	field := clause.Field
	operator := clause.Operator

	// Handle NULL checks (no value)
	if operator == "IS NULL" || operator == "IS NOT NULL" {
		return fmt.Sprintf("%s %s", field, operator), nil
	}

	// Handle IN and NOT IN (array values)
	if operator == "IN" || operator == "NOT IN" {
		values, ok := clause.Value.([]interface{})
		if !ok {
			return "", fmt.Errorf("IN operator requires array value for field '%s'", field)
		}

		// Format array values
		valueStrs := make([]string, len(values))
		for i, v := range values {
			valueStrs[i] = ft.formatValue(v)
		}

		return fmt.Sprintf("%s %s (%s)", field, operator, strings.Join(valueStrs, ", ")), nil
	}

	// Handle standard operators (single value)
	valueStr := ft.formatValue(clause.Value)
	return fmt.Sprintf("%s %s %s", field, operator, valueStr), nil
}

// formatValue formats a value for inclusion in a WHERE clause string
// Adds quotes for strings, handles numbers and booleans
func (ft *FilterTranslator) formatValue(value interface{}) string {
	if value == nil {
		return "NULL"
	}

	switch v := value.(type) {
	case string:
		// Escape single quotes and wrap in quotes
		escaped := strings.ReplaceAll(v, "'", "''")
		return fmt.Sprintf("'%s'", escaped)
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return fmt.Sprintf("%d", v)
	case float32, float64:
		return fmt.Sprintf("%f", v)
	case bool:
		if v {
			return "true"
		}
		return "false"
	default:
		// Fallback to string representation
		return fmt.Sprintf("'%v'", v)
	}
}
