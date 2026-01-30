/*
GROUP BY PARSER AND EXECUTION SYSTEM

⚠️  DEPRECATION NOTICE ⚠️

This file contains the LEGACY GROUP BY parser implementation.
It has been SUPERSEDED by the new unified parser:

  src/internal/query/queryparser/unified_parser.go

MIGRATION STATUS:
- ✅ New implementation: UnifiedSelectQuery parser (ACTIVE)
- ⚠️  This implementation: Kept for compatibility (DEPRECATED)
- 🗑️  Removal planned: Version 2.0 (2-3 releases from now)

MIGRATION GUIDE:
Instead of calling ParseGroupByQuery directly, use ParseUnifiedSelectQuery:

  OLD (deprecated):
    query, err := queryparser.ParseGroupByQuery(sqlQuery, logger)

  NEW (recommended):
    query, err := queryparser.ParseUnifiedSelectQuery(sqlQuery, logger)
    // query.GroupBy contains the GROUP BY clause
    // query.AggregateFields contains aggregate functions
    // query.HavingClause contains the HAVING clause

WHY DEPRECATED:
- New unified parser handles all query types (simple, JOIN, GROUP BY)
- Better validation and error messages
- Consistent structure across all query types
- Improved ORDER BY validation for GROUP BY queries
- Integration with planner-based execution

NOTE: This code is still functional and will be maintained until removal.
Structures like GroupByClause, AggregateFunction, and HavingClause are still
used by the new parser and will NOT be removed.

---

ORIGINAL IMPLEMENTATION NOTES:

This file implements the parsing and execution logic for GROUP BY clauses in SyndrDB.
It follows PostgreSQL-style GROUP BY algorithms including Hash Aggregate and Sort + GroupAggregate
optimizations for efficient data aggregation.

SUPPORTED GROUP BY SYNTAX:
SELECT field1, COUNT(*), SUM(field2), AVG(field3) FROM "Bundle_Name"
GROUP BY field1 [, field2, ...]
[HAVING condition]
[ORDER BY field1]

AGGREGATION FUNCTIONS SUPPORTED:
1. COUNT(*) - Count all rows in group
2. COUNT(field) - Count non-null values in field
3. SUM(field) - Sum numeric values
4. AVG(field) - Average of numeric values
5. MIN(field) - Minimum value
6. MAX(field) - Maximum value

EXECUTION STRATEGIES:
1. Hash Aggregate - Default for large unsorted data with sufficient memory
2. Sort + GroupAggregate - Used when input is sorted or memory constrained
3. Memory Management - Spills to disk when work_mem exceeded

PARSING STRATEGY:
The parser breaks down the query into components:
- SELECT clause (fields and aggregate functions)
- FROM clause (bundle name)
- WHERE clause (optional pre-aggregation filtering)
- GROUP BY clause (grouping fields)
- HAVING clause (optional post-aggregation filtering)
- ORDER BY clause (optional result ordering)

This implementation follows PostgreSQL's query planning approach with
cost-based optimization between hash and sort strategies.
*/

package queryparser

import (
	"fmt"
	"strings"
	"syndrdb/src/pkg/common/helpers"

	"go.uber.org/zap"
)

// AggregateFunction represents an aggregate function in SELECT clause
type AggregateFunction struct {
	Function string // COUNT, SUM, AVG, MIN, MAX
	Field    string // Field name or "*" for COUNT(*)
	Alias    string // Optional alias for the result column
}

// GroupByClause represents the GROUP BY clause structure
type GroupByClause struct {
	Fields []string // Fields to group by
}

// HavingClause represents the HAVING clause for post-aggregation filtering
type HavingClause struct {
	Condition string // SQL-like condition to filter aggregated results
}

// SelectQueryWithGroupBy represents a complete SELECT query with GROUP BY
type SelectQueryWithGroupBy struct {
	SelectFields      []string            // Non-aggregate fields to select
	AggregateFields   []AggregateFunction // Aggregate functions in SELECT
	FromBundle        string              // Bundle name to select from
	WhereClause       string              // Optional WHERE clause (pre-aggregation)
	GroupBy           *GroupByClause      // GROUP BY clause
	HavingClause      *HavingClause       // Optional HAVING clause (post-aggregation)
	OrderBy           *OrderByClause      // Optional ORDER BY clause
	ExecutionStrategy GroupByStrategy     // Hash vs Sort strategy
}

// GroupByStrategy represents the execution strategy for GROUP BY
type GroupByStrategy int

const (
	HashAggregate GroupByStrategy = iota
	SortGroupAggregate

	NoGroupByAggregate // For Count(*) with ORDER BY without GROUP BY
	AutoStrategy       // Let the system decide
)

// String returns the string representation of GroupByStrategy
func (gbs GroupByStrategy) String() string {
	switch gbs {
	case HashAggregate:
		return "Hash Aggregate"
	case SortGroupAggregate:
		return "Sort + GroupAggregate"
	case AutoStrategy:
		return "Auto Strategy"
	default:
		return "Unknown Strategy"
	}
}

// ParseSelectQueryWithGroupBy parses a complete SELECT query including GROUP BY
//
// Parameters:
//   - query: The complete SELECT query string
//   - logger: Logger for debugging and error reporting
//
// Returns:
//   - *SelectQueryWithGroupBy: The parsed query structure
//   - error: Any parsing error encountered
func ParseSelectQueryWithGroupBy(query string, logger *zap.SugaredLogger) (*SelectQueryWithGroupBy, error) {
	if query == "" {
		return nil, fmt.Errorf("query cannot be empty")
	}

	logger.Debugf("Parsing GROUP BY query: %s", query)

	// Initialize the select query structure
	selectQuery := &SelectQueryWithGroupBy{
		SelectFields:      make([]string, 0, 15),
		AggregateFields:   make([]AggregateFunction, 0, 5),
		ExecutionStrategy: AutoStrategy,
	}

	// Normalize the query (remove extra whitespace)
	normalizedQuery := normalizeQuery(query)

	// Parse SELECT clause (fields and aggregates)
	if err := parseSelectClauseWithAggregates(normalizedQuery, selectQuery, logger); err != nil {
		return nil, fmt.Errorf("error parsing SELECT clause: %v", err)
	}

	// Parse FROM clause
	if err := parseFromClauseForGroupBy(normalizedQuery, selectQuery, logger); err != nil {
		return nil, fmt.Errorf("error parsing FROM clause: %v", err)
	}

	// Parse optional WHERE clause
	if err := parseWhereClauseForGroupBy(normalizedQuery, selectQuery, logger); err != nil {
		return nil, fmt.Errorf("error parsing WHERE clause: %v", err)
	}

	// Parse GROUP BY clause
	if err := parseGroupByClause(normalizedQuery, selectQuery, logger); err != nil {
		return nil, fmt.Errorf("error parsing GROUP BY clause: %v", err)
	}

	// Parse optional HAVING clause
	if err := parseHavingClause(normalizedQuery, selectQuery, logger); err != nil {
		return nil, fmt.Errorf("error parsing HAVING clause: %v", err)
	}

	// Parse optional ORDER BY clause
	if err := parseOrderByClauseForGroupBy(normalizedQuery, selectQuery, logger); err != nil {
		return nil, fmt.Errorf("error parsing ORDER BY clause: %v", err)
	}

	// Validate the query structure
	if err := validateGroupByQuery(selectQuery, logger); err != nil {
		return nil, fmt.Errorf("query validation failed: %v", err)
	}

	// Determine optimal execution strategy
	selectQuery.ExecutionStrategy = determineExecutionStrategy(selectQuery, logger)

	logger.Debugf("Successfully parsed GROUP BY query. Strategy: %s", selectQuery.ExecutionStrategy.String())
	return selectQuery, nil
}

// parseSelectClauseWithAggregates parses SELECT clause containing fields and aggregate functions
func parseSelectClauseWithAggregates(query string, selectQuery *SelectQueryWithGroupBy, logger *zap.SugaredLogger) error {
	// Extract SELECT portion
	upperQuery := strings.ToUpper(query)
	if !strings.HasPrefix(upperQuery, "SELECT ") {
		return fmt.Errorf("query must start with SELECT")
	}

	selectPart := query[7:] // Remove "SELECT "
	fromIndex := strings.Index(strings.ToUpper(selectPart), " FROM ")
	if fromIndex == -1 {
		return fmt.Errorf("SELECT clause must be followed by FROM clause")
	}

	fieldsPart := strings.TrimSpace(selectPart[:fromIndex])
	if fieldsPart == "" {
		return fmt.Errorf("SELECT clause cannot be empty")
	}

	// Parse individual fields and aggregate functions
	return parseSelectFieldsAndAggregates(fieldsPart, selectQuery, logger)
}

// parseSelectFieldsAndAggregates parses the comma-separated list of fields and aggregates
func parseSelectFieldsAndAggregates(fieldsPart string, selectQuery *SelectQueryWithGroupBy, logger *zap.SugaredLogger) error {
	// Split by comma, but be careful about commas inside function calls
	fields := splitSelectFields(fieldsPart)

	for _, field := range fields {
		field = strings.TrimSpace(field)
		if field == "" {
			continue
		}

		// Check if this is an aggregate function
		if isAggregateFunction(field) {
			aggFunc, err := parseAggregateFunction(field, logger)
			if err != nil {
				return fmt.Errorf("error parsing aggregate function '%s': %v", field, err)
			}
			selectQuery.AggregateFields = append(selectQuery.AggregateFields, aggFunc)
			logger.Debugf("Parsed aggregate function: %s(%s)", aggFunc.Function, aggFunc.Field)
		} else {
			// Regular field
			fieldName := strings.Trim(field, `"'`)
			selectQuery.SelectFields = append(selectQuery.SelectFields, fieldName)
			logger.Debugf("Parsed select field: %s", fieldName)
		}
	}

	if len(selectQuery.SelectFields) == 0 && len(selectQuery.AggregateFields) == 0 {
		return fmt.Errorf("no valid fields or aggregates found in SELECT clause")
	}

	return nil
}

// splitSelectFields splits SELECT fields by comma, handling function parentheses
func splitSelectFields(fieldsPart string) []string {
	var fields []string
	var current strings.Builder
	parenDepth := 0

	for _, char := range fieldsPart {
		switch char {
		case '(':
			parenDepth++
			current.WriteRune(char)
		case ')':
			parenDepth--
			current.WriteRune(char)
		case ',':
			if parenDepth == 0 {
				fields = append(fields, current.String())
				current.Reset()
			} else {
				current.WriteRune(char)
			}
		default:
			current.WriteRune(char)
		}
	}

	if current.Len() > 0 {
		fields = append(fields, current.String())
	}

	return fields
}

// isAggregateFunction checks if a field is an aggregate function
func isAggregateFunction(field string) bool {
	upperField := strings.ToUpper(strings.TrimSpace(field))
	return strings.Contains(upperField, "COUNT(") ||
		strings.Contains(upperField, "SUM(") ||
		strings.Contains(upperField, "AVG(") ||
		strings.Contains(upperField, "MIN(") ||
		strings.Contains(upperField, "MAX(")
}

// parseAggregateFunction parses an aggregate function like COUNT(*) or SUM(field)
func parseAggregateFunction(field string, logger *zap.SugaredLogger) (AggregateFunction, error) {
	logger.Debugf("Parsing aggregate function: %s", field)
	// Pattern to match aggregate functions with optional alias
	// Example: COUNT(*) AS count_all, SUM(amount) as total
	pattern := `(?i)(COUNT|SUM|AVG|MIN|MAX)\s*\(\s*([^)]+)\s*\)(?:\s+AS\s+(\w+))?`
	re := helpers.MustCompileCached(pattern)

	matches := re.FindStringSubmatch(strings.TrimSpace(field))
	if len(matches) < 3 {
		return AggregateFunction{}, fmt.Errorf("invalid aggregate function format: %s", field)
	}

	function := strings.ToUpper(matches[1])
	fieldName := strings.TrimSpace(matches[2])
	alias := ""
	if len(matches) > 3 && matches[3] != "" {
		alias = matches[3]
	}

	// Remove quotes from field name
	fieldName = strings.Trim(fieldName, `"'`)

	// Validate function and field
	if function == "COUNT" && fieldName != "*" {
		// For COUNT(field), ensure field name is valid
		if fieldName == "" {
			return AggregateFunction{}, fmt.Errorf("COUNT function requires a field name or *")
		}
	} else if function != "COUNT" {
		// For other functions, field cannot be *
		if fieldName == "*" {
			return AggregateFunction{}, fmt.Errorf("%s function cannot use * as field", function)
		}
		if fieldName == "" {
			return AggregateFunction{}, fmt.Errorf("%s function requires a field name", function)
		}
	}

	return AggregateFunction{
		Function: function,
		Field:    fieldName,
		Alias:    alias,
	}, nil
}

// parseFromClauseForGroupBy parses the FROM clause for GROUP BY queries
func parseFromClauseForGroupBy(query string, selectQuery *SelectQueryWithGroupBy, logger *zap.SugaredLogger) error {
	fromRegex := helpers.MustCompileCached(`FROM\s+"([^"]+)"`)
	matches := fromRegex.FindStringSubmatch(query)

	if len(matches) < 2 {
		return fmt.Errorf("invalid FROM clause: missing bundle name")
	}

	selectQuery.FromBundle = matches[1]
	logger.Debugf("Parsed FROM bundle: %s", selectQuery.FromBundle)
	return nil
}

// parseWhereClauseForGroupBy parses the optional WHERE clause
func parseWhereClauseForGroupBy(query string, selectQuery *SelectQueryWithGroupBy, logger *zap.SugaredLogger) error {
	// Look for WHERE clause before GROUP BY
	whereIndex := strings.Index(strings.ToUpper(query), " WHERE ")
	if whereIndex == -1 {
		selectQuery.WhereClause = ""
		return nil
	}

	// Find the end of WHERE clause (before GROUP BY, HAVING, or ORDER BY)
	afterWhere := query[whereIndex+7:] // +7 for " WHERE "
	upperAfterWhere := strings.ToUpper(afterWhere)

	endIndex := len(afterWhere)
	for _, keyword := range []string{" GROUP BY ", " HAVING ", " ORDER BY "} {
		if idx := strings.Index(upperAfterWhere, keyword); idx != -1 && idx < endIndex {
			endIndex = idx
		}
	}

	whereClause := strings.TrimSpace(afterWhere[:endIndex])
	if whereClause == "" {
		return fmt.Errorf("WHERE clause cannot be empty")
	}

	selectQuery.WhereClause = whereClause
	logger.Debugf("Parsed WHERE clause: %s", whereClause)
	return nil
}

// parseGroupByClause parses the GROUP BY clause
// Following SQL standard (MS SQL, PostgreSQL): GROUP BY is optional when SELECT contains
// only aggregate functions (e.g., SELECT COUNT(*) FROM table), but required when mixing
// regular fields with aggregates (e.g., SELECT field1, COUNT(*) FROM table GROUP BY field1)
func parseGroupByClause(query string, selectQuery *SelectQueryWithGroupBy, logger *zap.SugaredLogger) error {
	groupByIndex := strings.Index(strings.ToUpper(query), " GROUP BY ")
	if groupByIndex == -1 {
		// GROUP BY is missing - check if it's required
		// If SELECT has only aggregates and no regular fields, GROUP BY is optional
		if len(selectQuery.SelectFields) == 0 && len(selectQuery.AggregateFields) > 0 {
			logger.Debugf("GROUP BY clause omitted - query has only aggregate functions (implicit grouping of all rows)")
			// Create an empty GROUP BY to indicate "group all rows"
			selectQuery.GroupBy = &GroupByClause{
				Fields: []string{}, // Empty means group all rows together
			}
			return nil
		}
		// If SELECT has regular fields mixed with aggregates, GROUP BY is required
		return fmt.Errorf("GROUP BY clause is required when SELECT contains non-aggregate fields with aggregate functions")
	}

	// Extract GROUP BY portion
	afterGroupBy := query[groupByIndex+10:] // +10 for " GROUP BY "
	upperAfterGroupBy := strings.ToUpper(afterGroupBy)

	// Find end of GROUP BY clause - stop at HAVING, ORDER BY, LIMIT, or OFFSET
	endIndex := len(afterGroupBy)
	for _, keyword := range []string{" HAVING ", " ORDER BY ", " LIMIT ", " OFFSET "} {
		if idx := strings.Index(upperAfterGroupBy, keyword); idx != -1 && idx < endIndex {
			endIndex = idx
		}
	}

	groupByPart := strings.TrimSpace(afterGroupBy[:endIndex])
	if groupByPart == "" {
		return fmt.Errorf("GROUP BY clause cannot be empty")
	}

	// Parse GROUP BY fields
	fields := strings.Split(groupByPart, ",")
	groupByFields := make([]string, 0, len(fields))

	for _, field := range fields {
		field = strings.TrimSpace(field)
		if field == "" {
			continue
		}
		// Remove quotes
		field = strings.Trim(field, `"'`)
		groupByFields = append(groupByFields, field)
		logger.Debugf("Parsed GROUP BY field: %s", field)
	}

	if len(groupByFields) == 0 {
		return fmt.Errorf("no valid fields found in GROUP BY clause")
	}

	selectQuery.GroupBy = &GroupByClause{
		Fields: groupByFields,
	}

	return nil
}

// parseHavingClause parses the optional HAVING clause
func parseHavingClause(query string, selectQuery *SelectQueryWithGroupBy, logger *zap.SugaredLogger) error {
	havingIndex := strings.Index(strings.ToUpper(query), " HAVING ")
	if havingIndex == -1 {
		selectQuery.HavingClause = nil
		return nil
	}

	// Extract HAVING portion
	afterHaving := query[havingIndex+8:] // +8 for " HAVING "
	upperAfterHaving := strings.ToUpper(afterHaving)

	// Find end of HAVING clause (before ORDER BY, LIMIT, or OFFSET)
	endIndex := len(afterHaving)
	for _, keyword := range []string{" ORDER BY ", " LIMIT ", " OFFSET "} {
		if idx := strings.Index(upperAfterHaving, keyword); idx != -1 && idx < endIndex {
			endIndex = idx
		}
	}

	havingCondition := strings.TrimSpace(afterHaving[:endIndex])
	if havingCondition == "" {
		return fmt.Errorf("HAVING clause cannot be empty")
	}

	selectQuery.HavingClause = &HavingClause{
		Condition: havingCondition,
	}
	logger.Debugf("Parsed HAVING clause: %s", havingCondition)
	return nil
}

// parseOrderByClauseForGroupBy parses the optional ORDER BY clause for GROUP BY queries
func parseOrderByClauseForGroupBy(query string, selectQuery *SelectQueryWithGroupBy, logger *zap.SugaredLogger) error {
	orderByIndex := strings.Index(strings.ToUpper(query), " ORDER BY ")
	if orderByIndex == -1 {
		selectQuery.OrderBy = nil
		return nil
	}

	// Extract ORDER BY portion (stop at LIMIT or OFFSET)
	afterOrderBy := query[orderByIndex+10:] // +10 for " ORDER BY "
	upperAfterOrderBy := strings.ToUpper(afterOrderBy)

	// Find end of ORDER BY clause
	endIndex := len(afterOrderBy)
	for _, keyword := range []string{" LIMIT ", " OFFSET "} {
		if idx := strings.Index(upperAfterOrderBy, keyword); idx != -1 && idx < endIndex {
			endIndex = idx
		}
	}

	orderByPart := strings.TrimSpace(afterOrderBy[:endIndex])
	if orderByPart == "" {
		return fmt.Errorf("ORDER BY clause cannot be empty")
	}

	// Parse ORDER BY fields manually for GROUP BY queries
	orderBy, err := parseOrderByFieldsForGroupBy(orderByPart, logger)
	if err != nil {
		return fmt.Errorf("error parsing ORDER BY clause: %v", err)
	}

	selectQuery.OrderBy = orderBy
	logger.Debugf("Parsed ORDER BY clause with %d fields", len(orderBy.Fields))
	return nil
}

// parseOrderByFieldsForGroupBy parses ORDER BY fields for GROUP BY queries
func parseOrderByFieldsForGroupBy(orderByPart string, logger *zap.SugaredLogger) (*OrderByClause, error) {
	fields := strings.Split(orderByPart, ",")
	orderByFields := make([]OrderByField, 0, len(fields))

	for _, field := range fields {
		field = strings.TrimSpace(field)
		if field == "" {
			continue
		}

		// Parse field name and direction
		parts := strings.Fields(field)
		if len(parts) == 0 {
			continue
		}

		fieldName := strings.Trim(parts[0], `"'`)
		direction := SortAsc // Default to ASC

		if len(parts) > 1 {
			dirStr := strings.ToUpper(parts[1])
			if dirStr == "DESC" {
				direction = SortDesc
			} else if dirStr != "ASC" {
				return nil, fmt.Errorf("invalid sort direction: %s (expected ASC or DESC)", parts[1])
			}
		}

		orderByFields = append(orderByFields, OrderByField{
			FieldName: fieldName,
			Direction: direction,
		})
		logger.Debugf("Parsed ORDER BY field: %s %s", fieldName, direction.String())
	}

	if len(orderByFields) == 0 {
		return nil, fmt.Errorf("no valid fields found in ORDER BY clause")
	}

	return &OrderByClause{
		Fields: orderByFields,
	}, nil
}

// validateGroupByQuery validates the GROUP BY query structure
// Following SQL standard (MS SQL, PostgreSQL):
// 1. If SELECT has only aggregates (no regular fields), GROUP BY is optional
// 2. If SELECT mixes regular fields with aggregates, all regular fields must be in GROUP BY
// 3. Must have at least one aggregate function
func validateGroupByQuery(selectQuery *SelectQueryWithGroupBy, logger *zap.SugaredLogger) error {
	logger.Debugf("Validating GROUP BY query structure")

	// Ensure we have at least one aggregate function
	if len(selectQuery.AggregateFields) == 0 {
		return fmt.Errorf("GROUP BY queries must include at least one aggregate function")
	}

	// If SELECT has only aggregates (no regular fields), validation passes
	// This allows queries like: SELECT COUNT(*) FROM table (no GROUP BY needed)
	if len(selectQuery.SelectFields) == 0 {
		logger.Debugf("Validation passed: Query has only aggregate functions (GROUP BY optional)")
		return nil
	}

	// If SELECT has regular fields, ensure all are in GROUP BY
	// This enforces queries like: SELECT field1, COUNT(*) FROM table GROUP BY field1
	for _, selectField := range selectQuery.SelectFields {
		found := false
		for _, groupByField := range selectQuery.GroupBy.Fields {
			if selectField == groupByField {
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("field '%s' must appear in GROUP BY clause or be used in an aggregate function", selectField)
		}
	}

	logger.Debugf("Validation passed: All non-aggregate fields are in GROUP BY clause")
	return nil
}

// determineExecutionStrategy chooses between Hash and Sort strategies
func determineExecutionStrategy(selectQuery *SelectQueryWithGroupBy, logger *zap.SugaredLogger) GroupByStrategy {
	// For now, use simple heuristics. In a full implementation, this would
	// consider table size, available memory, existing indexes, etc.

	groupByFieldCount := len(selectQuery.GroupBy.Fields)
	aggregateCount := len(selectQuery.AggregateFields)

	// If ORDER BY matches GROUP BY fields, prefer Sort strategy
	if selectQuery.OrderBy != nil && orderByMatchesGroupBy(selectQuery) {
		logger.Debugf("Using Sort strategy: ORDER BY matches GROUP BY")
		return SortGroupAggregate
	}

	// For few GROUP BY fields, use Hash strategy
	if groupByFieldCount <= 3 && aggregateCount <= 5 {
		logger.Debugf("Using Hash strategy: few GROUP BY fields (%d) and aggregates (%d)", groupByFieldCount, aggregateCount)
		return HashAggregate
	}

	if groupByFieldCount == 0 && aggregateCount == 1 {
		// Special case: COUNT(*) but no GROUP BY
		if selectQuery.OrderBy != nil {
			logger.Debugf("Using NoGroupByAggregate strategy: COUNT(*) with no GROUP BY")
			return NoGroupByAggregate
		}
	}

	// Default to Sort strategy for complex queries
	logger.Debugf("Using Sort strategy: complex query with %d GROUP BY fields and %d aggregates", groupByFieldCount, aggregateCount)
	return SortGroupAggregate
}

// orderByMatchesGroupBy checks if ORDER BY fields match GROUP BY fields
func orderByMatchesGroupBy(selectQuery *SelectQueryWithGroupBy) bool {
	if selectQuery.OrderBy == nil || len(selectQuery.OrderBy.Fields) != len(selectQuery.GroupBy.Fields) {
		return false
	}

	for i, orderByField := range selectQuery.OrderBy.Fields {
		if orderByField.FieldName != selectQuery.GroupBy.Fields[i] {
			return false
		}
	}

	return true
}
