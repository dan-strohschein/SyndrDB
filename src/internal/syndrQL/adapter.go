package syndrQL

import (
	"fmt"
	"strings"
	"syndrdb/src/internal/domain/models"
	"syndrdb/src/internal/query/queryparser"

	"go.uber.org/zap"
)

/*
adapter.go

This file implements the adapter layer that bridges the new SyndrQL parser with the
existing SyndrDB query infrastructure. It converts SelectStatement objects from the
new parser into UnifiedSelectQuery objects expected by the existing query planner.

Key responsibilities:
- Converting SelectStatement to UnifiedSelectQuery
- Mapping Expression AST to WhereGroup structures
- Converting SelectPattern to QueryType
- Preserving all query semantics during translation
- Supporting incremental migration strategy

The adapter follows the Adapter pattern (Gang of Four), allowing two incompatible
interfaces to work together without modifying existing code.

Design Principles:
- Single Responsibility: Each adapter method handles one specific conversion
- Open/Closed: New conversion logic can be added without modifying existing code
- DRY: Reuses ExpressionAdapter for expression conversion
*/

// SelectStatementAdapter adapts SelectStatement to UnifiedSelectQuery
type SelectStatementAdapter struct {
	expressionAdapter *ExpressionAdapter
	logger            *zap.SugaredLogger
}

// NewSelectStatementAdapter creates a new SELECT statement adapter
func NewSelectStatementAdapter(logger *zap.SugaredLogger) *SelectStatementAdapter {
	return &SelectStatementAdapter{
		expressionAdapter: NewExpressionAdapter(logger),
		logger:            logger,
	}
}

// ToUnifiedSelectQuery converts a SelectStatement to a UnifiedSelectQuery
// This is the main entry point for the adapter
func (a *SelectStatementAdapter) ToUnifiedSelectQuery(stmt *SelectStatement) (*queryparser.UnifiedSelectQuery, error) {
	if stmt == nil {
		return nil, fmt.Errorf("cannot convert nil SelectStatement")
	}

	query := &queryparser.UnifiedSelectQuery{
		// Map query type
		QueryType: a.mapSelectPatternToQueryType(stmt.Pattern),

		// Map SELECT clause
		SelectFields:    a.extractSelectFields(stmt),
		IsDistinct:      stmt.Distinct,
		IsAggregateOnly: stmt.IsAggregateOnly(),            // Detect aggregate-only queries (SELECT COUNT(*))
		AggregateFields: []queryparser.AggregateFunction{}, // Populated below

		// Map FROM clause
		FromBundle: stmt.BundleName,

		// Map LIMIT/OFFSET
		TopCount: stmt.TopN,
		Limit:    stmt.Limit,
		Offset:   stmt.Offset,
	}

	// Convert WHERE clause if present - Store Expression directly
	if stmt.WhereClause != nil {
		query.WhereExpression = stmt.WhereClause
	}

	// Convert ORDER BY clause
	if len(stmt.OrderBy) > 0 {
		query.OrderBy = a.convertOrderBy(stmt.OrderBy)
	}

	// Convert GROUP BY clause
	if len(stmt.GroupBy) > 0 {
		query.GroupBy = a.convertGroupBy(stmt.GroupBy)
	}

	// Convert HAVING clause if present - Store Expression directly
	if stmt.Having != nil {
		query.HavingExpression = stmt.Having
	}

	// Convert JOIN clauses
	if len(stmt.JoinClauses) > 0 {
		query.JoinClauses = a.convertJoinClauses(stmt.JoinClauses)
	}

	// Set relationship name for hierarchical JOIN results (WITH RELATIONSHIP clause)
	// This determines the field name used to nest child records in parent documents
	if stmt.RelationshipName != "" {
		query.RelationshipName = stmt.RelationshipName
	}

	// Extract aggregate functions from SelectFields whenever they exist
	// This handles both GROUP BY queries and aggregate-only queries (SELECT COUNT(*))
	if stmt.HasAggregates() {
		a.logger.Debugf("Extracting %d aggregate functions from SELECT clause", stmt.CountAggregates())
		query.AggregateFields = a.extractAggregateFields(stmt)

		// For GROUP BY queries, separate aggregate and non-aggregate fields
		if len(stmt.GroupBy) > 0 {
			query.SelectFields = a.extractNonAggregateFields(stmt)
			a.logger.Debugf("GROUP BY query: %d aggregates, %d non-aggregate fields",
				len(query.AggregateFields), len(query.SelectFields))
		} else if stmt.IsAggregateOnly() {
			// Aggregate-only query (e.g., SELECT COUNT(*) FROM table)
			// Clear SelectFields since all fields are aggregates
			query.SelectFields = []string{}
			a.logger.Debugf("Aggregate-only query: %d aggregates, 0 regular fields",
				len(query.AggregateFields))

			// CRITICAL FIX: Set IsCountOnly flag for COUNT(*) queries without GROUP BY
			// This enables special handling in command_director.go to return a single count value
			// instead of the full synthetic document with metadata fields
			if len(query.AggregateFields) == 1 &&
				query.AggregateFields[0].Function == "COUNT" &&
				query.AggregateFields[0].Field == "*" {
				query.IsCountOnly = true
				a.logger.Debugf("Detected COUNT(*) only query - setting IsCountOnly=true")
			}
		}
	}

	return query, nil
}

// mapSelectPatternToQueryType maps our SelectPattern to existing QueryType
func (a *SelectStatementAdapter) mapSelectPatternToQueryType(pattern SelectPattern) queryparser.QueryType {
	switch pattern {
	case PATTERN_SELECT_ALL, PATTERN_SELECT_FIELDS, PATTERN_SELECT_WHERE_SIMPLE, PATTERN_SELECT_WHERE_COMPLEX:
		return queryparser.SimpleQuery

	case PATTERN_SELECT_JOIN:
		return queryparser.JoinQuery

	case PATTERN_SELECT_GROUPBY:
		return queryparser.GroupByQuery

	case PATTERN_SELECT_AGGREGATE, PATTERN_SELECT_CUSTOM:
		return queryparser.ComplexQuery

	default:
		return queryparser.SimpleQuery // Safe default
	}
}

// extractSelectFields converts SelectField array to string array
func (a *SelectStatementAdapter) extractSelectFields(stmt *SelectStatement) []string {
	if len(stmt.Fields) == 0 {
		// SELECT * - return empty array (convention in existing parser)
		return []string{}
	}

	// Check if this is SELECT * (single field with name "*")
	if len(stmt.Fields) == 1 {
		firstFieldName := a.extractFieldName(stmt.Fields[0].Expression)
		if firstFieldName == "*" {
			// SELECT * - return empty array to signal "all fields"
			return []string{}
		}
	}

	fields := make([]string, 0, len(stmt.Fields))
	for _, field := range stmt.Fields {
		// Extract field name from expression
		fieldName := a.extractFieldName(field.Expression)

		// If there's an alias, use it
		if field.Alias != "" {
			// Format as "field AS alias" for compatibility
			fields = append(fields, fmt.Sprintf("%s AS %s", fieldName, field.Alias))
		} else {
			fields = append(fields, fieldName)
		}
	}

	return fields
}

// extractFieldName extracts a field name from an expression
func (a *SelectStatementAdapter) extractFieldName(expr Expression) string {
	switch expr := expr.(type) {
	case *IdentifierExpression:
		return expr.Name

	case *QualifiedIdentifierExpression:
		// Return just the field name (e.g., "name") instead of the full qualified string (e.g., "products"."name")
		return expr.Field

	case *LiteralExpression:
		// Literal in SELECT (e.g., SELECT 1, 'hello')
		if expr.Value == nil {
			return "NULL"
		}
		return fmt.Sprintf("%v", expr.Value)

	case *CallExpression:
		// Function call (e.g., COUNT(*), SUM(field), F:EXTRACT("YEAR", date))
		// Use the expression's String() method which handles F: prefix and quoted arguments
		return expr.String()

	case *BinaryExpression:
		// Computed expression (e.g., SELECT price * quantity)
		left := a.extractFieldName(expr.Left)
		right := a.extractFieldName(expr.Right)
		return fmt.Sprintf("%s %s %s", left, expr.Operator.String(), right)

	case *GroupedExpression:
		return a.extractFieldName(expr.Expression)

	default:
		// Fallback
		return expr.String()
	}
}

// extractAggregateFields extracts aggregate function calls from SELECT fields
// Returns them as AggregateFunction structures for query planning
func (a *SelectStatementAdapter) extractAggregateFields(stmt *SelectStatement) []queryparser.AggregateFunction {
	aggregates := make([]queryparser.AggregateFunction, 0)

	if len(stmt.Fields) == 0 {
		return aggregates
	}

	for _, field := range stmt.Fields {
		if call, isCall := field.Expression.(*CallExpression); isCall {
			// Check if it's an aggregate function
			funcName := strings.ToUpper(call.Function)
			if funcName == "COUNT" || funcName == "SUM" || funcName == "AVG" || funcName == "MIN" || funcName == "MAX" {
				// Extract field name from arguments
				var fieldArg string
				if len(call.Arguments) == 0 {
					fieldArg = "*" // COUNT()
				} else if len(call.Arguments) == 1 {
					if ident, ok := call.Arguments[0].(*IdentifierExpression); ok {
						if ident.Name == "*" {
							fieldArg = "*" // COUNT(*)
						} else {
							fieldArg = ident.Name
						}
					} else {
						// Complex expression (e.g., SUM(price * quantity))
						fieldArg = call.Arguments[0].String()
					}
				} else {
					// Multiple arguments - use String() representation
					fieldArg = call.Arguments[0].String()
				}

				aggregate := queryparser.AggregateFunction{
					Function: funcName,
					Field:    fieldArg,
					Alias:    field.Alias, // Use alias if provided
				}
				aggregates = append(aggregates, aggregate)
			}
		}
	}

	return aggregates
}

// extractNonAggregateFields extracts non-aggregate fields from SELECT clause
// This returns the regular fields that should be in GROUP BY
func (a *SelectStatementAdapter) extractNonAggregateFields(stmt *SelectStatement) []string {
	fields := make([]string, 0)

	if len(stmt.Fields) == 0 {
		return fields
	}

	for _, field := range stmt.Fields {
		// Skip aggregate functions
		if call, isCall := field.Expression.(*CallExpression); isCall {
			funcName := strings.ToUpper(call.Function)
			if funcName == "COUNT" || funcName == "SUM" || funcName == "AVG" || funcName == "MIN" || funcName == "MAX" {
				continue // Skip aggregates
			}
		}

		// Add non-aggregate field
		fieldName := a.extractFieldName(field.Expression)
		if field.Alias != "" {
			// If there's an alias, we might want to use it
			// But for GROUP BY compatibility, use the original field name
			fields = append(fields, fieldName)
		} else {
			fields = append(fields, fieldName)
		}
	}

	return fields
}

// convertOrderBy converts OrderByField array to OrderByClause
func (a *SelectStatementAdapter) convertOrderBy(orderBy []OrderByField) *queryparser.OrderByClause {
	if len(orderBy) == 0 {
		return nil
	}

	// Build ORDER BY fields
	fields := make([]queryparser.OrderByField, len(orderBy))
	for i, field := range orderBy {
		direction := queryparser.SortAsc
		if field.Descending {
			direction = queryparser.SortDesc
		}

		fields[i] = queryparser.OrderByField{
			FieldName: field.Field,
			Direction: direction,
		}
	}

	return &queryparser.OrderByClause{
		Fields: fields,
	}
}

// convertGroupBy converts GROUP BY fields to GroupByClause
func (a *SelectStatementAdapter) convertGroupBy(groupBy []string) *queryparser.GroupByClause {
	if len(groupBy) == 0 {
		return nil
	}

	return &queryparser.GroupByClause{
		Fields: groupBy,
	}
}

// convertHaving converts HAVING expression to HavingClause
func (a *SelectStatementAdapter) convertHaving(having Expression) (*queryparser.HavingClause, error) {
	if having == nil {
		return nil, nil
	}

	// Convert HAVING expression to string representation
	// Note: HavingClause in existing system stores condition as string, not WhereGroup
	// TODO: I might want to enhance the existing HavingClause to use WhereGroup structure
	// TODO: For now, I'll convert the expression to a string representation
	condition := having.String()

	return &queryparser.HavingClause{
		Condition: condition,
	}, nil
}

// convertJoinClauses converts JOIN clauses from new parser format to legacy format
// Takes the JoinClause array from SelectStatement and converts to queryparser.JoinClause array
// This enables seamless integration with the existing JOIN execution engine
func (a *SelectStatementAdapter) convertJoinClauses(joinClauses []JoinClause) []queryparser.JoinClause {
	if len(joinClauses) == 0 {
		return nil
	}

	// Convert each JOIN clause
	result := make([]queryparser.JoinClause, len(joinClauses))
	for i, jc := range joinClauses {
		// Convert JOIN type
		var joinType queryparser.JoinType
		switch jc.JoinType {
		case InnerJoin:
			joinType = queryparser.InnerJoin
		case LeftJoin:
			joinType = queryparser.LeftJoin
		case RightJoin:
			joinType = queryparser.RightJoin
		case FullOuterJoin:
			joinType = queryparser.FullOuterJoin
		default:
			// Default to INNER JOIN for safety
			joinType = queryparser.InnerJoin
			a.logger.Warnf("Unknown JOIN type %d, defaulting to INNER JOIN", jc.JoinType)
		}

		// Convert JOIN conditions
		conditions := make([]queryparser.JoinCondition, len(jc.JoinConditions))
		for j, cond := range jc.JoinConditions {
			// Parse qualified field names to extract bundle and field
			// Format: "BundleName"."FieldName" or BundleName.FieldName
			leftBundle, leftField := a.parseQualifiedFieldName(cond.LeftField)
			rightBundle, rightField := a.parseQualifiedFieldName(cond.RightField)

			conditions[j] = queryparser.JoinCondition{
				LeftBundle:  leftBundle,
				LeftField:   leftField,
				Operator:    cond.Operator,
				RightBundle: rightBundle,
				RightField:  rightField,
			}
		}

		result[i] = queryparser.JoinClause{
			JoinType:       joinType,
			RightBundle:    jc.RightBundle,
			JoinConditions: conditions,
		}
	}

	return result
}

// parseQualifiedFieldName parses qualified field names into (bundle, field).
// Contract: one part → ("", trimmed name); two parts → (parts[0], parts[1]);
// more than two → (parts[0], strings.Join(parts[1:], ".")). So "A"."B"."C" or A.B.C
// yields bundle "A", field "B.C". Leading/trailing quotes are trimmed from segments.
func (a *SelectStatementAdapter) parseQualifiedFieldName(qualifiedName string) (bundle string, field string) {
	trimmed := strings.Trim(qualifiedName, "\"")
	parts := strings.Split(trimmed, ".")

	switch len(parts) {
	case 1:
		return "", trimmed
	case 2:
		return strings.Trim(parts[0], "\""), strings.Trim(parts[1], "\"")
	default:
		fieldParts := make([]string, len(parts)-1)
		for i, p := range parts[1:] {
			fieldParts[i] = strings.Trim(p, "\"")
		}
		return strings.Trim(parts[0], "\""), strings.Join(fieldParts, ".")
	}
}

// Adapt converts a SelectStatement to a UnifiedSelectQuery. It returns an error on
// failure; there is no string fallback. Prefer ToUnifiedSelectQuery for the same
// behavior; this method exists for API compatibility.
func (a *SelectStatementAdapter) Adapt(stmt *SelectStatement, originalQuery string) (*queryparser.UnifiedSelectQuery, error) {
	return a.ToUnifiedSelectQuery(stmt)
}

// ValidateConversion validates that the converted query is semantically equivalent
// This is useful for testing and debugging during migration
func (a *SelectStatementAdapter) ValidateConversion(stmt *SelectStatement, query *queryparser.UnifiedSelectQuery) error {
	// Validate FROM clause
	if stmt.BundleName != query.FromBundle {
		return fmt.Errorf("FROM bundle mismatch: expected %s, got %s", stmt.BundleName, query.FromBundle)
	}

	// Validate DISTINCT flag
	if stmt.Distinct != query.IsDistinct {
		return fmt.Errorf("DISTINCT flag mismatch: expected %v, got %v", stmt.Distinct, query.IsDistinct)
	}

	// Validate LIMIT
	if stmt.Limit != query.Limit {
		return fmt.Errorf("LIMIT mismatch: expected %d, got %d", stmt.Limit, query.Limit)
	}

	// Validate OFFSET
	if stmt.Offset != query.Offset {
		return fmt.Errorf("OFFSET mismatch: expected %d, got %d", stmt.Offset, query.Offset)
	}

	// Validate WHERE clause presence
	if (stmt.WhereClause != nil) != (query.WhereExpression != nil) {
		return fmt.Errorf("WHERE clause presence mismatch")
	}

	// TODO: I could add more detailed validation here
	// TODO: I might want to compare the actual clause structures for deep validation

	return nil
}

// GetIndexHints extracts index hints from the SelectStatement for query optimization
// This uses the pattern recognition metadata to suggest which indexes to use
func (a *SelectStatementAdapter) GetIndexHints(stmt *SelectStatement) []string {
	if stmt == nil {
		return nil
	}

	// Return pre-computed index hints from pattern recognition
	return stmt.IndexHints
}

// GetComplexity returns the estimated complexity of the query
// This can be used for query prioritization and resource allocation
func (a *SelectStatementAdapter) GetComplexity(stmt *SelectStatement) int {
	if stmt == nil {
		return 0
	}

	return stmt.Complexity
}

// InsertStatementAdapter adapts InsertStatement to DocumentCommand
// Following the adapter pattern used for SELECT statements
type InsertStatementAdapter struct {
	logger *zap.SugaredLogger
}

// NewInsertStatementAdapter creates a new INSERT statement adapter
func NewInsertStatementAdapter(logger *zap.SugaredLogger) *InsertStatementAdapter {
	return &InsertStatementAdapter{
		logger: logger,
	}
}

// ToDocumentCommand converts an InsertStatement to a DocumentCommand
// This maintains compatibility with the existing bundle service interface
func (a *InsertStatementAdapter) ToDocumentCommand(stmt *InsertStatement) (*models.DocumentCommand, error) {
	if stmt == nil {
		return nil, fmt.Errorf("cannot convert nil InsertStatement")
	}

	if stmt.BundleName == "" {
		return nil, fmt.Errorf("bundle name cannot be empty")
	}

	if len(stmt.Fields) == 0 {
		return nil, fmt.Errorf("document must have at least one field")
	}

	// Convert map[string]interface{} to []KeyValue
	// TODO: I should optimize this conversion to avoid allocations for hot path
	keyValues := make([]models.KeyValue, 0, len(stmt.Fields))
	for key, value := range stmt.Fields {
		keyValues = append(keyValues, models.KeyValue{
			Key:   key,
			Value: value,
		})
	}

	return &models.DocumentCommand{
		CommandType: "ADD",
		BundleName:  stmt.BundleName,
		Fields:      keyValues,
	}, nil
}

// UpdateStatementAdapter adapts UpdateStatement to DocumentUpdateCommand
// This adapter handles the conversion of parsed UPDATE statements from the new
// SyndrQL parser into the DocumentUpdateCommand structure expected by the bundle service
type UpdateStatementAdapter struct {
	expressionAdapter *ExpressionAdapter
	logger            *zap.SugaredLogger
}

// NewUpdateStatementAdapter creates a new UPDATE statement adapter
func NewUpdateStatementAdapter(logger *zap.SugaredLogger) *UpdateStatementAdapter {
	return &UpdateStatementAdapter{
		expressionAdapter: NewExpressionAdapter(logger),
		logger:            logger,
	}
}

// ToDocumentUpdateCommand converts an UpdateStatement to a DocumentUpdateCommand
// This maintains compatibility with the existing bundle service interface
func (a *UpdateStatementAdapter) ToDocumentUpdateCommand(stmt *UpdateStatement) (*models.DocumentUpdateCommand, error) {
	if stmt == nil {
		return nil, fmt.Errorf("cannot convert nil UpdateStatement")
	}

	if stmt.BundleName == "" {
		return nil, fmt.Errorf("bundle name cannot be empty in UPDATE statement")
	}

	if len(stmt.Fields) == 0 {
		return nil, fmt.Errorf("UPDATE statement must specify at least one field to update")
	}

	// Convert map[string]interface{} to []KeyValue
	// TODO: I should optimize this conversion to avoid allocations for hot path
	keyValues := make([]models.KeyValue, 0, len(stmt.Fields))
	for key, value := range stmt.Fields {
		keyValues = append(keyValues, models.KeyValue{
			Key:   key,
			Value: value,
		})
	}

	// Convert WHERE clause expression to WhereGroup for compatibility (if present)
	var whereClauseStr string
	if stmt.WhereClause != nil {
		whereGroup, err := a.expressionAdapter.ToWhereGroup(stmt.WhereClause)
		if err != nil {
			return nil, fmt.Errorf("failed to convert WHERE clause: %w", err)
		}

		// Serialize WHERE clause back to string format
		// TODO: I should consider keeping the structured WhereGroup in DocumentUpdateCommand
		// for better performance instead of round-tripping through string serialization
		whereClauseStr = serializeWhereGroupToClauseString(whereGroup)
	}

	return &models.DocumentUpdateCommand{
		BundleName:  stmt.BundleName,
		Fields:      keyValues,
		WhereClause: whereClauseStr,
		Confirmed:   stmt.Confirmed,
	}, nil
}

// serializeWhereGroupToClauseString converts a WhereGroup back to a WHERE clause
// string. Field names are double-quoted for parseability (reserved words, spaces).
// Used by both Update and Delete statement adapters for compatibility with the
// bundle service interface.
func serializeWhereGroupToClauseString(whereGroup *queryparser.WhereGroup) string {
	if whereGroup == nil {
		return ""
	}

	var parts []string

	for _, clause := range whereGroup.Clauses {
		valueStr := fmt.Sprintf("%v", clause.Value)
		if _, ok := clause.Value.(string); ok {
			valueStr = fmt.Sprintf("\"%s\"", clause.Value)
		}
		conditionStr := fmt.Sprintf("\"%s\" %s %s", clause.Field, clause.Operator, valueStr)
		parts = append(parts, conditionStr)
	}

	for _, subGroup := range whereGroup.SubGroups {
		groupStr := serializeWhereGroupToClauseString(&subGroup)
		if groupStr != "" {
			parts = append(parts, "("+groupStr+")")
		}
	}

	if len(parts) == 0 {
		return ""
	}

	operator := " AND "
	if whereGroup.Operator == "OR" {
		operator = " OR "
	}

	return strings.Join(parts, operator)
}

// DeleteStatementAdapter adapts DeleteStatement to DocumentDeleteCommand
// This adapter handles the conversion of parsed DELETE statements from the new
// SyndrQL parser into the DocumentDeleteCommand structure expected by the bundle service
type DeleteStatementAdapter struct {
	expressionAdapter *ExpressionAdapter
	logger            *zap.SugaredLogger
}

// NewDeleteStatementAdapter creates a new DELETE statement adapter
func NewDeleteStatementAdapter(logger *zap.SugaredLogger) *DeleteStatementAdapter {
	return &DeleteStatementAdapter{
		expressionAdapter: NewExpressionAdapter(logger),
		logger:            logger,
	}
}

// ToDocumentDeleteCommand converts a DeleteStatement to a DocumentDeleteCommand
// This maintains compatibility with the existing bundle service interface
func (a *DeleteStatementAdapter) ToDocumentDeleteCommand(stmt *DeleteStatement) (*models.DocumentDeleteCommand, error) {
	if stmt == nil {
		return nil, fmt.Errorf("cannot convert nil DeleteStatement")
	}

	if stmt.BundleName == "" {
		return nil, fmt.Errorf("bundle name cannot be empty in DELETE statement")
	}

	var whereClauseStr string
	if stmt.WhereClause != nil {
		// Convert WHERE clause expression to WhereGroup for compatibility
		whereGroup, err := a.expressionAdapter.ToWhereGroup(stmt.WhereClause)
		if err != nil {
			return nil, fmt.Errorf("failed to convert WHERE clause: %w", err)
		}

		// Serialize WHERE clause back to string format
		// TODO: I should consider keeping the structured WhereGroup in DocumentDeleteCommand
		// for better performance instead of round-tripping through string serialization
		whereClauseStr = serializeWhereGroupToClauseString(whereGroup)
	}

	return &models.DocumentDeleteCommand{
		BundleName:         stmt.BundleName,
		WhereClause:        whereClauseStr,
		Confirmed:          stmt.Confirmed,
		Fields:             nil, // Deprecated - WHERE clause is used instead
		DeletedDocumentIDs: nil, // Will be populated by the bundle service
		RawCommand:         "",  // TODO: I could store the original command for debugging
	}, nil
}

// CreateBundleStatementAdapter adapts CreateBundleStatement to BundleCommand
// This adapter handles the conversion of parsed CREATE BUNDLE statements from the new
// SyndrQL parser into the BundleCommand structure expected by the bundle service
type CreateBundleStatementAdapter struct {
	logger *zap.SugaredLogger
}

// NewCreateBundleStatementAdapter creates a new CREATE BUNDLE statement adapter
func NewCreateBundleStatementAdapter(logger *zap.SugaredLogger) *CreateBundleStatementAdapter {
	return &CreateBundleStatementAdapter{
		logger: logger,
	}
}

// ToBundleCommand converts a CreateBundleStatement to a BundleCommand
// This maintains compatibility with the existing bundle service interface
func (a *CreateBundleStatementAdapter) ToBundleCommand(stmt *CreateBundleStatement) (*models.BundleCommand, error) {
	if stmt == nil {
		return nil, fmt.Errorf("cannot convert nil CreateBundleStatement")
	}

	if stmt.BundleName == "" {
		return nil, fmt.Errorf("bundle name cannot be empty in CREATE BUNDLE statement")
	}

	if len(stmt.Fields) == 0 {
		return nil, fmt.Errorf("CREATE BUNDLE statement must specify at least one field")
	}

	// Convert FieldDefinitionParsed to FieldDefinition
	fields := make([]models.FieldDefinition, 0, len(stmt.Fields))
	for i, parsedField := range stmt.Fields {
		// Validate field properties
		if parsedField.Name == "" {
			return nil, fmt.Errorf("field %d: field name cannot be empty", i+1)
		}

		if parsedField.Type == "" {
			return nil, fmt.Errorf("field %d (%s): field type cannot be empty", i+1, parsedField.Name)
		}

		// Skip fields with type "relationship" - these should be defined via WITH RELATIONSHIP
		// or UPDATE BUNDLE ADD RELATIONSHIP, not as field types
		if strings.ToLower(parsedField.Type) == "relationship" {
			continue
		}

		// Validate field type
		if err := a.validateFieldType(parsedField.Type); err != nil {
			return nil, fmt.Errorf("field %d (%s): %w", i+1, parsedField.Name, err)
		}

		// Validate default value matches field type
		if parsedField.DefaultValue != nil {
			if err := a.validateDefaultValue(parsedField.Type, parsedField.DefaultValue); err != nil {
				return nil, fmt.Errorf("field %d (%s): %w", i+1, parsedField.Name, err)
			}
		}

		fields = append(fields, models.FieldDefinition{
			Name:         parsedField.Name,
			Type:         parsedField.Type,
			IsRequired:   parsedField.IsRequired,
			IsUnique:     parsedField.IsUnique,
			DefaultValue: parsedField.DefaultValue,
		})
	}

	return &models.BundleCommand{
		CommandType: "CREATE",
		BundleName:  stmt.BundleName,
		Fields:      fields,
		Changes:     nil, // Not applicable for CREATE
	}, nil
}

// validateFieldType validates that the field type is supported
func (a *CreateBundleStatementAdapter) validateFieldType(fieldType string) error {
	// Normalize to lowercase for comparison
	normalizedType := strings.ToLower(fieldType)

	// TODO: I should maintain this list in a central location for consistency
	validTypes := map[string]bool{
		"string":   true,
		"int":      true,
		"float":    true,
		"bool":     true,
		"boolean":  true,
		"date":     true,
		"time":     true,
		"datetime": true,
		"blob":     true,
		"json":     true,
	}

	if !validTypes[normalizedType] {
		return fmt.Errorf("unsupported field type: %s", fieldType)
	}

	return nil
}

// validateDefaultValue validates that the default value matches the field type
func (a *CreateBundleStatementAdapter) validateDefaultValue(fieldType string, defaultValue interface{}) error {
	if defaultValue == nil {
		return nil // NULL is always valid
	}

	// Check if default value is an Expression (e.g., F:NOW())
	// Expressions are evaluated at runtime, so we can't validate the type here
	if _, isExpr := defaultValue.(Expression); isExpr {
		return nil // Expression is valid for any type
	}

	normalizedType := strings.ToLower(fieldType)

	switch normalizedType {
	case "string":
		if _, ok := defaultValue.(string); !ok {
			return fmt.Errorf("default value must be a string for type 'string', got %T", defaultValue)
		}

	case "int":
		// Accept both int64 and float64 (if it's a whole number)
		switch v := defaultValue.(type) {
		case int64:
			// Valid
		case float64:
			if v != float64(int64(v)) {
				return fmt.Errorf("default value must be an integer for type 'int', got float with decimal: %v", v)
			}
		default:
			return fmt.Errorf("default value must be an integer for type 'int', got %T", defaultValue)
		}

	case "float":
		// Accept both int64 and float64
		switch defaultValue.(type) {
		case int64, float64:
			// Valid
		default:
			return fmt.Errorf("default value must be a number for type 'float', got %T", defaultValue)
		}

	case "bool", "boolean":
		if _, ok := defaultValue.(bool); !ok {
			return fmt.Errorf("default value must be a boolean for type 'bool', got %T", defaultValue)
		}

	case "date", "time", "datetime":
		// Date/time values are typically stored as strings in the command
		if _, ok := defaultValue.(string); !ok {
			return fmt.Errorf("default value must be a string for type '%s', got %T", fieldType, defaultValue)
		}

	case "json":
		// JSON values can be strings or structured data
		// We'll accept any type here and let the bundle service handle validation
		// TODO: I should add JSON validation here

	case "blob":
		// Blob values are typically base64 encoded strings
		if _, ok := defaultValue.(string); !ok {
			return fmt.Errorf("default value must be a string (base64) for type 'blob', got %T", defaultValue)
		}

	default:
		// Unknown type, skip validation
		a.logger.Warnf("Unknown field type '%s', skipping default value validation", fieldType)
	}

	return nil
}

// UpdateBundleStatementAdapter adapts UpdateBundleStatement to BundleCommand
// This adapter handles the conversion of parsed UPDATE BUNDLE statements from the new
// SyndrQL parser into the BundleCommand structure expected by the bundle service
type UpdateBundleStatementAdapter struct {
	logger *zap.SugaredLogger
}

// NewUpdateBundleStatementAdapter creates a new UPDATE BUNDLE statement adapter
func NewUpdateBundleStatementAdapter(logger *zap.SugaredLogger) *UpdateBundleStatementAdapter {
	return &UpdateBundleStatementAdapter{
		logger: logger,
	}
}

// ToBundleCommand converts an UpdateBundleStatement to a BundleCommand
// This maintains compatibility with the existing bundle service interface
func (a *UpdateBundleStatementAdapter) ToBundleCommand(stmt *UpdateBundleStatement) (*models.BundleCommand, error) {
	if stmt == nil {
		return nil, fmt.Errorf("cannot convert nil UpdateBundleStatement")
	}

	if stmt.BundleName == "" {
		return nil, fmt.Errorf("bundle name cannot be empty in UPDATE BUNDLE statement")
	}

	// Validate that at least one operation is specified
	if stmt.NewBundleName == "" && len(stmt.Fields) == 0 && len(stmt.Relationships) == 0 {
		return nil, fmt.Errorf("UPDATE BUNDLE statement must specify at least one operation: SET NAME, field modifications, or relationships")
	}

	// Convert field modifications to FieldChange structures
	var changes []models.FieldChange
	if len(stmt.Fields) > 0 {
		changes = make([]models.FieldChange, 0, len(stmt.Fields))
		for _, field := range stmt.Fields {
			change, err := a.convertFieldModification(field)
			if err != nil {
				return nil, fmt.Errorf("failed to convert field modification '%s': %w", field.Name, err)
			}
			changes = append(changes, change)
		}
	}

	// Determine if this command has relationship operations
	hasRelationships := len(stmt.Relationships) > 0

	bundleCommand := &models.BundleCommand{
		CommandType:             "UPDATE",
		BundleName:              stmt.BundleName,
		NewBundleName:           stmt.NewBundleName,
		Fields:                  []models.FieldDefinition{}, // Not used for UPDATE
		Changes:                 changes,
		HasRelationshipCommands: hasRelationships,
		HasForceSwitch:          false, // Not applicable for UPDATE
	}

	a.logger.Debugf("Converted UpdateBundleStatement to BundleCommand: Bundle=%s, NewName=%s, FieldChanges=%d, HasRelationships=%v",
		stmt.BundleName, stmt.NewBundleName, len(changes), hasRelationships)

	return bundleCommand, nil
}

// convertFieldModification converts a parsed field modification to a FieldChange
func (a *UpdateBundleStatementAdapter) convertFieldModification(field FieldModificationDefinitionParsed) (models.FieldChange, error) {
	// Map modification type to change type
	changeType := strings.ToUpper(field.ModificationType)

	// Validate modification type
	switch changeType {
	case "ADD", "REMOVE", "MODIFY":
		// Valid types
	default:
		return models.FieldChange{}, fmt.Errorf("invalid modification type: %s (must be ADD, REMOVE, or MODIFY)", field.ModificationType)
	}

	// Determine the new field name
	// For MODIFY operations, use NewName if provided, otherwise keep the same name
	// For ADD operations, NewName is the field name to add
	// For REMOVE operations, NewName is ignored
	newFieldName := field.Name
	if changeType == "MODIFY" && field.NewName != "" {
		newFieldName = field.NewName
	} else if changeType == "ADD" && field.NewName != "" {
		newFieldName = field.NewName
	}

	// Create the new field definition
	newField := models.FieldDefinition{
		Name:         newFieldName,
		Type:         field.Type,
		IsRequired:   field.IsRequired,
		IsUnique:     field.IsUnique,
		DefaultValue: field.DefaultValue,
	}

	return models.FieldChange{
		ChangeType:   changeType,
		OldFieldName: field.Name, // For MODIFY, this is the field being modified; for REMOVE, this is what's removed
		NewField:     newField,   // For ADD and MODIFY, this contains the new/updated definition
	}, nil
}

// ConvertRelationships converts parsed relationships to RelationshipCommand structures
// This is used by the command director to process ADD RELATIONSHIP operations
func (a *UpdateBundleStatementAdapter) ConvertRelationships(stmt *UpdateBundleStatement) ([]*models.RelationshipCommand, error) {
	if len(stmt.Relationships) == 0 {
		return nil, nil
	}

	relationships := make([]*models.RelationshipCommand, 0, len(stmt.Relationships))
	for _, rel := range stmt.Relationships {
		relCmd := &models.RelationshipCommand{
			CommandType:       "CREATE", // ADD RELATIONSHIP is essentially a CREATE operation
			BundleName:        stmt.BundleName,
			Name:              fmt.Sprintf("%s_%s_to_%s_%s", rel.SourceBundle, rel.SourceField, rel.DestinationBundle, rel.DestinationField),
			RelationshipType:  rel.RelationshipType,
			SourceBundle:      rel.SourceBundle,
			SourceField:       rel.SourceField,
			DestinationBundle: rel.DestinationBundle,
			DestinationField:  rel.DestinationField,
		}
		relationships = append(relationships, relCmd)
	}

	a.logger.Debugf("Converted %d relationships for bundle '%s'", len(relationships), stmt.BundleName)
	return relationships, nil
}

// DropBundleStatementAdapter adapts DropBundleStatement to BundleCommand
// This adapter handles the conversion of parsed DROP BUNDLE statements from the new
// SyndrQL parser into the BundleCommand structure expected by the bundle service
type DropBundleStatementAdapter struct {
	logger *zap.SugaredLogger
}

// NewDropBundleStatementAdapter creates a new DROP BUNDLE statement adapter
func NewDropBundleStatementAdapter(logger *zap.SugaredLogger) *DropBundleStatementAdapter {
	return &DropBundleStatementAdapter{
		logger: logger,
	}
}

// ToBundleCommand converts a DropBundleStatement to a BundleCommand
// This maintains compatibility with the existing bundle service interface
func (a *DropBundleStatementAdapter) ToBundleCommand(stmt *DropBundleStatement) (*models.BundleCommand, error) {
	if stmt == nil {
		return nil, fmt.Errorf("cannot convert nil DropBundleStatement")
	}

	if stmt.BundleName == "" {
		return nil, fmt.Errorf("bundle name cannot be empty in DROP BUNDLE statement")
	}

	return &models.BundleCommand{
		CommandType:    "DELETE",
		BundleName:     stmt.BundleName,
		Fields:         nil,                 // Not applicable for DROP
		Changes:        nil,                 // Not applicable for DROP
		HasForceSwitch: stmt.HasForceSwitch, // Pass FORCE flag for deletion validation
	}, nil
}
