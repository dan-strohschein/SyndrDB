/*
UNIFIED QUERY PARSER SYSTEM

This file implements the unified parsing logic for all SELECT query variations in SyndrDB.
It handles parsing of SELECT statements with any combination of clauses, following
PostgreSQL-style syntax while adapting to SyndrDB's bundle-based document architecture.

SUPPORTED UNIFIED SELECT SYNTAX:
1. SELECT [DISTINCT] [TOP N] field1, field2, ... | DOCUMENTS | COUNT(*) FROM "Bundle_Name"
2. [JOIN "Bundle2" ON condition]
3. [WHERE conditions]
4. [GROUP BY field1, field2, ...]
5. [HAVING conditions]
6. [ORDER BY field1 ASC|DESC, ...]
7. [LIMIT N] [OFFSET M]
8. [WITH RELATIONSHIP "name"]

QUERY TYPE DETECTION:
The parser automatically detects query complexity and delegates to specialized
parsers when appropriate, then enhances with additional clauses:
- SIMPLE: Basic SELECT without JOIN or GROUP BY
- JOIN: SELECT with JOIN clauses
- GROUPBY: SELECT with GROUP BY clause
- COMPLEX: Combination of multiple advanced features

COMPOSITION STRATEGY:
This implementation follows the Open/Closed Principle by composing existing
parsers rather than replacing them. It reuses:
- BasicSelectParser for simple queries
- JoinParser for JOIN queries
- GroupByParser for GROUP BY queries
- FilterParser for WHERE clauses
- OrderByParser for ORDER BY clauses
- DocumentSorter for sorting operations

BACKWARD COMPATIBILITY:
All existing parser functions remain functional. The unified parser provides
a superset of functionality while maintaining compatibility with legacy code.

POSTGRESQL EXECUTION ORDER:
The parser structures the query to enable PostgreSQL-style execution:
1. FROM + JOIN (source data)
2. WHERE (pre-filter)
3. GROUP BY (aggregation)
4. HAVING (post-aggregation filter)
5. SELECT (projection)
6. DISTINCT (deduplication)
7. ORDER BY (sorting)
8. LIMIT/OFFSET (pagination)

This implementation follows SyndrDB's comprehensive error handling practices
and integrates seamlessly with the existing query execution pipeline.
*/

package queryparser

import (
	"fmt"
	"strings"
	"syndrdb/src/pkg/common/helpers"

	"go.uber.org/zap"
)

// QueryType represents the complexity level of a SELECT query
type QueryType int

const (
	// SimpleQuery - Basic SELECT without JOIN or GROUP BY
	SimpleQuery QueryType = iota
	// JoinQuery - SELECT with JOIN clauses
	JoinQuery
	// GroupByQuery - SELECT with GROUP BY clause
	GroupByQuery
	// ComplexQuery - Combination of multiple advanced features
	ComplexQuery
)

// String returns the string representation of QueryType
func (qt QueryType) String() string {
	switch qt {
	case SimpleQuery:
		return "SIMPLE"
	case JoinQuery:
		return "JOIN"
	case GroupByQuery:
		return "GROUP BY"
	case ComplexQuery:
		return "COMPLEX"
	default:
		return "UNKNOWN"
	}
}

// UnifiedSelectQuery represents a complete SELECT query with all possible clauses
// This structure composes elements from all specialized parsers:
// - BasicSelectQuery (simple SELECT)
// - SelectJoinQuery (JOIN queries)
// - SelectQueryWithGroupBy (GROUP BY queries)
type UnifiedSelectQuery struct {
	// Query metadata
	QueryType QueryType // Detected query complexity level

	// SELECT clause
	SelectFields    []string            // Fields to select (empty for all fields)
	AggregateFields []AggregateFunction // Aggregate functions (COUNT, SUM, AVG, MIN, MAX)
	IsDistinct      bool                // SELECT DISTINCT flag
	IsCountOnly     bool                // SELECT COUNT(*) flag (no other fields)

	// FROM clause
	FromBundle string // Primary bundle name

	// JOIN clause
	JoinClauses      []JoinClause // JOIN operations (INNER, LEFT, RIGHT, FULL OUTER)
	RelationshipName string       // WITH RELATIONSHIP clause (hierarchical results)

	// WHERE clause - Expression AST from SyndrQL parser
	// WhereExpression contains a syndrQL.Expression interface value
	// Use type assertion: expr := query.WhereExpression.(syndrQL.Expression)
	WhereExpression interface{} // Pre-aggregation filtering (syndrQL.Expression from parser)

	// GROUP BY clause
	GroupBy *GroupByClause // Grouping fields

	// HAVING clause - Expression AST from SyndrQL parser
	// HavingExpression contains a syndrQL.Expression interface value
	// Use type assertion: expr := query.HavingExpression.(syndrQL.Expression)
	HavingExpression interface{} // Post-aggregation filtering (syndrQL.Expression from parser)

	// ORDER BY clause
	OrderBy *OrderByClause // Sorting specification

	// LIMIT/OFFSET clause
	TopCount int // SELECT TOP N (legacy syntax)
	Limit    int // LIMIT N (standard SQL syntax)
	Offset   int // OFFSET M (pagination)
}

// ParseUnifiedSelectQuery parses any SELECT query variation into a unified structure
//
// This function follows SyndrDB comprehensive error handling practices.
// It delegates to specialized parsers for initial parsing, then enhances
// the result with additional clauses detected in the query.
//
// Parameters:
//   - query: The complete SELECT query string
//   - logger: Logger for debugging and error reporting
//
// Returns:
//   - *UnifiedSelectQuery: The parsed query structure
//   - error: Any parsing error encountered
func ParseUnifiedSelectQuery(query string, logger *zap.SugaredLogger) (*UnifiedSelectQuery, error) {
	if query == "" {
		return nil, fmt.Errorf("query cannot be empty")
	}

	query = strings.TrimSpace(query)
	query = strings.TrimSuffix(query, ";") // Remove trailing semicolon

	logger.Debugf("Parsing unified SELECT query: %s", query)

	// Step 1: Detect query type
	queryType := detectQueryType(query, logger)
	logger.Debugf("Detected query type: %s", queryType.String())

	// Step 2: Delegate to appropriate specialized parser
	var unifiedQuery *UnifiedSelectQuery
	var err error

	switch queryType {
	case SimpleQuery:
		unifiedQuery, err = parseAsSimpleQuery(query, logger)
	case JoinQuery:
		unifiedQuery, err = parseAsJoinQuery(query, logger)
	case GroupByQuery:
		unifiedQuery, err = parseAsGroupByQuery(query, logger)
	case ComplexQuery:
		// For complex queries, try GROUP BY first (most comprehensive)
		// If that fails, try JOIN, then fall back to simple
		unifiedQuery, err = parseAsComplexQuery(query, logger)
	default:
		return nil, fmt.Errorf("unknown query type detected")
	}

	if err != nil {
		return nil, fmt.Errorf("failed to parse query: %w", err)
	}

	// Step 3: Store the detected query type
	unifiedQuery.QueryType = queryType

	// Step 4: Parse additional clauses that might not be handled by specialized parsers
	if err := enhanceWithAdditionalClauses(query, unifiedQuery, logger); err != nil {
		return nil, fmt.Errorf("failed to enhance query with additional clauses: %w", err)
	}

	// Step 5: Validate the complete query structure
	if err := validateUnifiedQuery(unifiedQuery, logger); err != nil {
		return nil, fmt.Errorf("query validation failed: %w", err)
	}

	// TODO: I could optimize by caching parsed GROUP BY queries for repeated execution to avoid re-parsing

	logger.Debugf("Successfully parsed unified query: Type=%s, Fields=%d, Joins=%d, HasWhere=%v, HasGroupBy=%v, HasOrderBy=%v",
		unifiedQuery.QueryType.String(),
		len(unifiedQuery.SelectFields)+len(unifiedQuery.AggregateFields),
		len(unifiedQuery.JoinClauses),
		unifiedQuery.WhereExpression != nil,
		unifiedQuery.GroupBy != nil,
		unifiedQuery.OrderBy != nil)

	return unifiedQuery, nil
}

// detectQueryType analyzes the query string to determine its complexity level
func detectQueryType(query string, logger *zap.SugaredLogger) QueryType {
	// Normalize query for detection (remove extra whitespace, newlines)
	// normalizedQuery := strings.ReplaceAll(query, "\n", " ")
	// normalizedQuery = strings.ReplaceAll(normalizedQuery, "\t", " ")
	// normalizedQuery = strings.ReplaceAll(normalizedQuery, "\r", " ")

	normalizedQuery := helpers.NormalizeCommand(query)
	re := helpers.MustCompileCached(`\s+`)
	normalizedQuery = re.ReplaceAllString(normalizedQuery, " ")
	upperQuery := strings.ToUpper(normalizedQuery)

	hasJoin := strings.Contains(upperQuery, " JOIN ")
	hasGroupBy := strings.Contains(upperQuery, " GROUP BY ")

	// Check for aggregate functions in SELECT clause (COUNT, SUM, AVG, MIN, MAX)
	// Extract SELECT clause to avoid false positives from WHERE/HAVING clauses
	selectStart := strings.Index(upperQuery, "SELECT")
	fromStart := strings.Index(upperQuery, " FROM ")
	hasAggregates := false

	if selectStart >= 0 && fromStart > selectStart {
		selectClause := upperQuery[selectStart:fromStart]
		// Match aggregate functions: COUNT, SUM, AVG, MIN, MAX followed by (
		aggregatePattern := `(COUNT|SUM|AVG|MIN|MAX)\s*\(`
		matched := helpers.MustCompileCached(aggregatePattern).MatchString(selectClause)
		if matched {
			hasAggregates = true
			logger.Debugf("Detected aggregate functions in SELECT clause")
		}
	}

	// Complex query: both JOIN and GROUP BY
	if hasJoin && hasGroupBy {
		logger.Debugf("Detected COMPLEX query (JOIN + GROUP BY)")
		return ComplexQuery
	}

	// GROUP BY query (explicit GROUP BY or aggregate functions)
	// Note: Changed from ComplexQuery to GroupByQuery when GROUP BY is present
	if hasGroupBy || hasAggregates {
		logger.Debugf("Detected GROUP BY query (hasGroupBy=%v, hasAggregates=%v)", hasGroupBy, hasAggregates)
		return GroupByQuery
	}

	// JOIN query
	if hasJoin {
		logger.Debugf("Detected JOIN query")
		return JoinQuery
	}

	// Simple query
	logger.Debugf("Detected SIMPLE query")
	return SimpleQuery
}

// parseAsSimpleQuery delegates to BasicSelectParser and converts to unified structure
func parseAsSimpleQuery(query string, logger *zap.SugaredLogger) (*UnifiedSelectQuery, error) {
	basicQuery, err := ParseBasicSelectQuery(query, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to parse as basic query: %w", err)
	}

	return basicToUnified(basicQuery, logger), nil
}

// parseAsJoinQuery delegates to JoinParser and converts to unified structure
func parseAsJoinQuery(query string, logger *zap.SugaredLogger) (*UnifiedSelectQuery, error) {
	joinQuery, err := ParseSelectJoinQuery(query, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to parse as JOIN query: %w", err)
	}

	return joinToUnified(joinQuery, logger), nil
}

// parseAsGroupByQuery delegates to GroupByParser and converts to unified structure
func parseAsGroupByQuery(query string, logger *zap.SugaredLogger) (*UnifiedSelectQuery, error) {
	groupByQuery, err := ParseSelectQueryWithGroupBy(query, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to parse as GROUP BY query: %w", err)
	}

	return groupByToUnified(groupByQuery, logger), nil
}

// parseAsComplexQuery handles queries with multiple advanced features
func parseAsComplexQuery(query string, logger *zap.SugaredLogger) (*UnifiedSelectQuery, error) {
	logger.Debugf("Attempting to parse complex query with multiple features")

	// Strategy: Try GROUP BY parser first (most comprehensive for complex queries)
	groupByQuery, err := ParseSelectQueryWithGroupBy(query, logger)
	if err == nil {
		logger.Debugf("Successfully parsed complex query using GROUP BY parser")
		unified := groupByToUnified(groupByQuery, logger)

		// Now parse JOIN clauses separately if present
		if strings.Contains(strings.ToUpper(query), " JOIN ") {
			if err := parseJoinClausesForUnified(query, unified, logger); err != nil {
				logger.Warnf("Failed to parse JOIN clauses in complex query: %v", err)
			}
		}

		return unified, nil
	}

	// Fallback: Try JOIN parser
	joinQuery, err2 := ParseSelectJoinQuery(query, logger)
	if err2 == nil {
		logger.Debugf("Successfully parsed complex query using JOIN parser")
		unified := joinToUnified(joinQuery, logger)

		// Now parse GROUP BY clauses separately if present
		if strings.Contains(strings.ToUpper(query), " GROUP BY ") {
			if err := parseGroupByClausesForUnified(query, unified, logger); err != nil {
				logger.Warnf("Failed to parse GROUP BY clauses in complex query: %v", err)
			}
		}

		return unified, nil
	}

	// Both parsers failed
	return nil, fmt.Errorf("failed to parse complex query: GROUP BY parser error: %v, JOIN parser error: %v", err, err2)
}

// basicToUnified converts BasicSelectQuery to UnifiedSelectQuery
func basicToUnified(basic *BasicSelectQuery, logger *zap.SugaredLogger) *UnifiedSelectQuery {
	unified := &UnifiedSelectQuery{
		QueryType:       SimpleQuery,
		SelectFields:    basic.SelectFields,
		AggregateFields: make([]AggregateFunction, 0, 15),
		FromBundle:      basic.FromBundle,
		JoinClauses:     make([]JoinClause, 0, 3),
	}

	// Parse WHERE clause if present
	if basic.WhereClause != "" {
		// LEGACY PATH: Old parser doesn't create Expression AST
		// This path should only be hit when UseNewParser=false
		whereGroup, err := ParseWhereClause(basic.WhereClause)
		if err != nil {
			logger.Warnf("Failed to parse WHERE clause in basic query: %v", err)
		} else {
			// Store in WhereExpression as interface{} - legacy WhereGroup type
			unified.WhereExpression = whereGroup
		}
	}

	logger.Debugf("Converted BasicSelectQuery to UnifiedSelectQuery")
	return unified
}

// joinToUnified converts SelectJoinQuery to UnifiedSelectQuery
func joinToUnified(join *SelectJoinQuery, logger *zap.SugaredLogger) *UnifiedSelectQuery {
	unified := &UnifiedSelectQuery{
		QueryType:       JoinQuery,
		SelectFields:    join.SelectFields,
		AggregateFields: make([]AggregateFunction, 0, 15),
		FromBundle:      join.FromBundle,
		JoinClauses:     join.JoinClauses,
		// LEGACY: join.WhereClause is WhereGroup type, store as interface{}
		WhereExpression:  join.WhereClause,
		RelationshipName: join.RelationshipName,
		Limit:            join.Limit,
		Offset:           join.Offset,
	}

	logger.Debugf("Converted SelectJoinQuery to UnifiedSelectQuery")
	return unified
}

// groupByToUnified converts SelectQueryWithGroupBy to UnifiedSelectQuery
func groupByToUnified(groupBy *SelectQueryWithGroupBy, logger *zap.SugaredLogger) *UnifiedSelectQuery {
	unified := &UnifiedSelectQuery{
		QueryType:       GroupByQuery,
		SelectFields:    groupBy.SelectFields,
		AggregateFields: groupBy.AggregateFields,
		FromBundle:      groupBy.FromBundle,
		JoinClauses:     make([]JoinClause, 0, 3),
		GroupBy:         groupBy.GroupBy,
		// LEGACY: groupBy.HavingClause is HavingClause type, store as interface{}
		HavingExpression: groupBy.HavingClause,
		OrderBy:          groupBy.OrderBy,
	}

	// Parse WHERE clause if present
	if groupBy.WhereClause != "" {
		// LEGACY PATH: Old parser doesn't create Expression AST
		// This path should only be hit when UseNewParser=false
		whereGroup, err := ParseWhereClause(groupBy.WhereClause)
		if err != nil {
			logger.Warnf("Failed to parse WHERE clause in GROUP BY query: %v", err)
		} else {
			// Store in WhereExpression as interface{} - legacy WhereGroup type
			unified.WhereExpression = whereGroup
		}
	}

	logger.Debugf("Converted SelectQueryWithGroupBy to UnifiedSelectQuery")
	return unified
}

// enhanceWithAdditionalClauses parses and adds clauses not handled by specialized parsers
func enhanceWithAdditionalClauses(query string, unified *UnifiedSelectQuery, logger *zap.SugaredLogger) error {
	upperQuery := strings.ToUpper(query)

	// Parse DISTINCT flag
	if strings.Contains(upperQuery, "SELECT DISTINCT") {
		unified.IsDistinct = true
		logger.Debugf("Detected DISTINCT flag")
	}

	// Parse TOP clause (SELECT TOP N)
	topPattern := helpers.MustCompileCached(`(?i)SELECT\s+TOP\s+(\d+)`)
	if matches := topPattern.FindStringSubmatch(query); len(matches) > 1 {
		topCount := 0
		fmt.Sscanf(matches[1], "%d", &topCount)
		unified.TopCount = topCount
		logger.Debugf("Detected TOP %d clause", topCount)
	}

	// Parse LIMIT clause (if not already set by specialized parser)
	if unified.Limit == 0 {
		limitPattern := helpers.MustCompileCached(`(?i)\s+LIMIT\s+(\d+)`)
		if matches := limitPattern.FindStringSubmatch(query); len(matches) > 1 {
			limit := 0
			fmt.Sscanf(matches[1], "%d", &limit)
			unified.Limit = limit
			logger.Debugf("Detected LIMIT %d clause", limit)
		}
	}

	// Parse OFFSET clause (if not already set by specialized parser)
	if unified.Offset == 0 {
		offsetPattern := helpers.MustCompileCached(`(?i)\s+OFFSET\s+(\d+)`)
		if matches := offsetPattern.FindStringSubmatch(query); len(matches) > 1 {
			offset := 0
			fmt.Sscanf(matches[1], "%d", &offset)
			unified.Offset = offset
			logger.Debugf("Detected OFFSET %d clause", offset)
		}
	}

	// Detect COUNT(*) only queries
	// NOTE: If GROUP BY has fields, we need multiple rows (one per group), not a single count
	// If GROUP BY is empty or nil, it's COUNT(*) aggregating all rows into one result
	hasGroupByFields := unified.GroupBy != nil && len(unified.GroupBy.Fields) > 0

	if len(unified.AggregateFields) == 1 &&
		unified.AggregateFields[0].Function == "COUNT" &&
		unified.AggregateFields[0].Field == "*" &&
		len(unified.SelectFields) == 0 &&
		!hasGroupByFields { // Don't treat as count-only if GROUP BY has fields
		unified.IsCountOnly = true
		logger.Debugf("Detected COUNT(*) only query (no GROUP BY fields)")
	}

	// Parse ORDER BY if not already set by specialized parser
	if unified.OrderBy == nil && strings.Contains(upperQuery, " ORDER BY ") {
		orderByStr := extractOrderByClause(query)
		if orderByStr != "" {
			orderBy, err := parseOrderByFields(orderByStr, logger)
			if err != nil {
				logger.Warnf("Failed to parse ORDER BY clause: %v", err)
			} else {
				unified.OrderBy = orderBy
				logger.Debugf("Parsed ORDER BY clause with %d fields", len(orderBy.Fields))
			}
		}
	}

	return nil
}

// extractOrderByClause extracts the ORDER BY portion from a query
func extractOrderByClause(query string) string {
	upperQuery := strings.ToUpper(query)
	orderByIndex := strings.Index(upperQuery, " ORDER BY ")
	if orderByIndex == -1 {
		return ""
	}

	// Extract from ORDER BY to end of query (or next major clause)
	orderByPart := query[orderByIndex+10:] // Skip " ORDER BY "

	// Find end of ORDER BY (before LIMIT, OFFSET, or end of query)
	endIndex := len(orderByPart)
	for _, terminator := range []string{" LIMIT ", " OFFSET ", ";"} {
		if idx := strings.Index(strings.ToUpper(orderByPart), terminator); idx != -1 && idx < endIndex {
			endIndex = idx
		}
	}

	return strings.TrimSpace(orderByPart[:endIndex])
}

// parseJoinClausesForUnified parses JOIN clauses and adds them to unified query
func parseJoinClausesForUnified(query string, unified *UnifiedSelectQuery, logger *zap.SugaredLogger) error {
	// Try to parse as JOIN query to extract JOIN clauses
	joinQuery, err := ParseSelectJoinQuery(query, logger)
	if err != nil {
		return fmt.Errorf("failed to parse JOIN clauses: %w", err)
	}

	unified.JoinClauses = joinQuery.JoinClauses
	unified.RelationshipName = joinQuery.RelationshipName

	logger.Debugf("Added %d JOIN clauses to unified query", len(joinQuery.JoinClauses))
	return nil
}

// parseGroupByClausesForUnified parses GROUP BY clauses and adds them to unified query
func parseGroupByClausesForUnified(query string, unified *UnifiedSelectQuery, logger *zap.SugaredLogger) error {
	// Try to parse as GROUP BY query to extract GROUP BY clauses
	groupByQuery, err := ParseSelectQueryWithGroupBy(query, logger)
	if err != nil {
		return fmt.Errorf("failed to parse GROUP BY clauses: %w", err)
	}

	unified.GroupBy = groupByQuery.GroupBy
	// LEGACY: groupByQuery.HavingClause is HavingClause type, store as interface{}
	unified.HavingExpression = groupByQuery.HavingClause
	unified.AggregateFields = groupByQuery.AggregateFields

	logger.Debugf("Added GROUP BY clause with %d fields to unified query", len(groupByQuery.GroupBy.Fields))
	return nil
}

// validateUnifiedQuery performs comprehensive validation on the parsed query
func validateUnifiedQuery(unified *UnifiedSelectQuery, logger *zap.SugaredLogger) error {
	// Validation 1: Must have FROM bundle
	if unified.FromBundle == "" {
		return fmt.Errorf("FROM clause is required")
	}

	// Validation 2: GROUP BY validation
	if unified.GroupBy != nil {
		// All non-aggregate SELECT fields must be in GROUP BY
		for _, field := range unified.SelectFields {
			found := false
			for _, groupField := range unified.GroupBy.Fields {
				if strings.EqualFold(field, groupField) {
					found = true
					break
				}
			}
			if !found {
				return fmt.Errorf("field '%s' must appear in GROUP BY clause or be used in an aggregate function", field)
			}
		}

		// Validation 2b: ORDER BY fields must be in GROUP BY or be aggregate functions
		if unified.OrderBy != nil {
			for _, orderField := range unified.OrderBy.Fields {
				fieldName := orderField.FieldName

				// Check if it's in GROUP BY clause
				foundInGroupBy := false
				for _, groupField := range unified.GroupBy.Fields {
					if strings.EqualFold(fieldName, groupField) {
						foundInGroupBy = true
						break
					}
				}

				if foundInGroupBy {
					continue // Valid - field is in GROUP BY
				}

				// Check if it's an aggregate function
				// Aggregate functions have format: FUNCTION(field) like COUNT(*), AVG(salary), SUM(amount)
				isAggregate := false
				upperFieldName := strings.ToUpper(fieldName)
				aggregateFuncs := []string{"COUNT(", "SUM(", "AVG(", "MIN(", "MAX("}
				for _, aggFunc := range aggregateFuncs {
					if strings.Contains(upperFieldName, aggFunc) {
						isAggregate = true
						break
					}
				}

				if !isAggregate {
					return fmt.Errorf("ORDER BY field '%s' must appear in GROUP BY clause or be an aggregate function", fieldName)
				}

				// TODO: I might want to add more complex validation for nested aggregate functions in the future
			}
		}
	}

	// Validation 3: HAVING without GROUP BY is invalid
	if unified.HavingExpression != nil && unified.GroupBy == nil {
		return fmt.Errorf("HAVING clause requires GROUP BY clause")
	}

	// Validation 4: TOP and LIMIT cannot both be specified
	if unified.TopCount > 0 && unified.Limit > 0 {
		return fmt.Errorf("cannot specify both TOP and LIMIT clauses")
	}

	// Validation 5: Offset requires Limit or TopCount
	if unified.Offset > 0 && unified.Limit == 0 && unified.TopCount == 0 {
		logger.Warnf("OFFSET specified without LIMIT - will apply to all results")
	}

	// Validation 6: DISTINCT with aggregates might not make sense
	if unified.IsDistinct && len(unified.AggregateFields) > 0 {
		logger.Warnf("DISTINCT with aggregate functions may produce unexpected results")
	}

	logger.Debugf("Query validation passed")
	return nil
}

// GetEffectiveLimit returns the effective row limit for the query
// Considers both TOP and LIMIT clauses
func (usq *UnifiedSelectQuery) GetEffectiveLimit() int {
	if usq.TopCount > 0 {
		return usq.TopCount
	}
	return usq.Limit
}

// HasLimit returns true if the query has any form of row limiting
func (usq *UnifiedSelectQuery) HasLimit() bool {
	return usq.TopCount > 0 || usq.Limit > 0
}

// HasJoin returns true if the query includes JOIN clauses
func (usq *UnifiedSelectQuery) HasJoin() bool {
	return len(usq.JoinClauses) > 0
}

// HasGroupBy returns true if the query includes GROUP BY
func (usq *UnifiedSelectQuery) HasGroupBy() bool {
	return usq.GroupBy != nil
}

// HasOrderBy returns true if the query includes ORDER BY
func (usq *UnifiedSelectQuery) HasOrderBy() bool {
	return usq.OrderBy != nil
}

// HasWhere returns true if the query includes WHERE clause
func (usq *UnifiedSelectQuery) HasWhere() bool {
	return usq.WhereExpression != nil
}

// IsAggregateQuery returns true if the query uses aggregate functions
func (usq *UnifiedSelectQuery) IsAggregateQuery() bool {
	return len(usq.AggregateFields) > 0
}

// String returns a human-readable representation of the query structure
func (usq *UnifiedSelectQuery) String() string {
	var parts []string

	parts = append(parts, fmt.Sprintf("Type: %s", usq.QueryType.String()))

	if usq.IsDistinct {
		parts = append(parts, "DISTINCT")
	}

	if usq.IsCountOnly {
		parts = append(parts, "COUNT(*) ONLY")
	}

	parts = append(parts, fmt.Sprintf("FROM: %s", usq.FromBundle))

	if len(usq.SelectFields) > 0 {
		parts = append(parts, fmt.Sprintf("Fields: %v", usq.SelectFields))
	}

	if len(usq.AggregateFields) > 0 {
		parts = append(parts, fmt.Sprintf("Aggregates: %d", len(usq.AggregateFields)))
	}

	if usq.HasJoin() {
		parts = append(parts, fmt.Sprintf("JOINs: %d", len(usq.JoinClauses)))
	}

	if usq.HasWhere() {
		parts = append(parts, "WHERE: yes")
	}

	if usq.HasGroupBy() {
		parts = append(parts, fmt.Sprintf("GROUP BY: %v", usq.GroupBy.Fields))
	}

	if usq.HavingExpression != nil {
		parts = append(parts, "HAVING: yes")
	}

	if usq.HasOrderBy() {
		parts = append(parts, fmt.Sprintf("ORDER BY: %d fields", len(usq.OrderBy.Fields)))
	}

	if usq.HasLimit() {
		parts = append(parts, fmt.Sprintf("LIMIT: %d", usq.GetEffectiveLimit()))
	}

	if usq.Offset > 0 {
		parts = append(parts, fmt.Sprintf("OFFSET: %d", usq.Offset))
	}

	return strings.Join(parts, ", ")
}
