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
		SelectFields: a.extractSelectFields(stmt),
		IsDistinct:   stmt.Distinct,
		// TODO: I need to detect IsCountOnly from SelectFields (SELECT COUNT(*) optimization)

		// Map FROM clause
		FromBundle: stmt.BundleName,

		// Map LIMIT/OFFSET
		TopCount: stmt.TopN,
		Limit:    stmt.Limit,
		Offset:   stmt.Offset,
	}

	// Convert WHERE clause if present
	if stmt.WhereClause != nil {
		whereGroup, err := a.expressionAdapter.ToWhereGroup(stmt.WhereClause)
		if err != nil {
			return nil, fmt.Errorf("failed to convert WHERE clause: %w", err)
		}
		query.WhereClause = whereGroup
	}

	// Convert ORDER BY clause
	if len(stmt.OrderBy) > 0 {
		query.OrderBy = a.convertOrderBy(stmt.OrderBy)
	}

	// Convert GROUP BY clause
	if len(stmt.GroupBy) > 0 {
		query.GroupBy = a.convertGroupBy(stmt.GroupBy)
	}

	// Convert HAVING clause
	if stmt.Having != nil {
		havingClause, err := a.convertHaving(stmt.Having)
		if err != nil {
			return nil, fmt.Errorf("failed to convert HAVING clause: %w", err)
		}
		query.HavingClause = havingClause
	}

	// TODO: I need to implement JOIN clause conversion when JOIN parser is complete
	// TODO: I need to implement aggregate field extraction from SelectFields

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

	case *LiteralExpression:
		// Literal in SELECT (e.g., SELECT 1, 'hello')
		if expr.Value == nil {
			return "NULL"
		}
		return fmt.Sprintf("%v", expr.Value)

	case *CallExpression:
		// Function call (e.g., COUNT(*), SUM(field))
		// TODO: I need to extract this to AggregateFields array
		args := make([]string, len(expr.Arguments))
		for i, arg := range expr.Arguments {
			args[i] = a.extractFieldName(arg)
		}
		return fmt.Sprintf("%s(%s)", expr.Function, strings.Join(args, ", "))

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

// AdaptWithFallback attempts to convert SelectStatement, falling back to string parsing on error
// This is useful during the migration period to ensure zero downtime
func (a *SelectStatementAdapter) AdaptWithFallback(stmt *SelectStatement, originalQuery string) (*queryparser.UnifiedSelectQuery, error) {
	// Try to convert using the adapter
	query, err := a.ToUnifiedSelectQuery(stmt)
	if err != nil {
		// Log the conversion failure
		a.logger.Warnf("Failed to convert SelectStatement to UnifiedSelectQuery: %v. Falling back to string parser.", err)

		// Fall back to the original string-based parser
		// TODO: I need to import and use the original ParseUnifiedSelectQuery here
		// For now, return the error
		return nil, fmt.Errorf("adapter conversion failed and fallback not yet implemented: %w", err)
	}

	return query, nil
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
	if (stmt.WhereClause != nil) != (query.WhereClause != nil) {
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

	if stmt.WhereClause == nil {
		return nil, fmt.Errorf("UPDATE statement requires a WHERE clause")
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

	// Convert WHERE clause expression to WhereGroup for compatibility
	whereGroup, err := a.expressionAdapter.ToWhereGroup(stmt.WhereClause)
	if err != nil {
		return nil, fmt.Errorf("failed to convert WHERE clause: %w", err)
	}

	// Serialize WHERE clause back to string format
	// TODO: I should consider keeping the structured WhereGroup in DocumentUpdateCommand
	// for better performance instead of round-tripping through string serialization
	whereClauseStr := a.serializeWhereGroup(whereGroup)

	return &models.DocumentUpdateCommand{
		BundleName:  stmt.BundleName,
		Fields:      keyValues,
		WhereClause: whereClauseStr,
	}, nil
}

// serializeWhereGroup converts a WhereGroup back to a WHERE clause string
// This is necessary for compatibility with the existing bundle service interface
func (a *UpdateStatementAdapter) serializeWhereGroup(whereGroup *queryparser.WhereGroup) string {
	if whereGroup == nil {
		return ""
	}

	var parts []string

	// Serialize clauses
	for _, clause := range whereGroup.Clauses {
		// Format: Field Operator Value
		valueStr := fmt.Sprintf("%v", clause.Value)
		// Quote string values
		if _, ok := clause.Value.(string); ok {
			valueStr = fmt.Sprintf("'%s'", clause.Value)
		}
		conditionStr := fmt.Sprintf("%s %s %s", clause.Field, clause.Operator, valueStr)
		parts = append(parts, conditionStr)
	}

	// Serialize nested subgroups recursively
	// TODO: I will add support for complex nested WHERE clauses when needed
	for _, subGroup := range whereGroup.SubGroups {
		groupStr := a.serializeWhereGroup(&subGroup)
		if groupStr != "" {
			parts = append(parts, "("+groupStr+")")
		}
	}

	// Join with the logical operator (AND/OR)
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

	if stmt.WhereClause == nil {
		return nil, fmt.Errorf("DELETE statement requires a WHERE clause")
	}

	// Convert WHERE clause expression to WhereGroup for compatibility
	whereGroup, err := a.expressionAdapter.ToWhereGroup(stmt.WhereClause)
	if err != nil {
		return nil, fmt.Errorf("failed to convert WHERE clause: %w", err)
	}

	// Serialize WHERE clause back to string format
	// TODO: I should consider keeping the structured WhereGroup in DocumentDeleteCommand
	// for better performance instead of round-tripping through string serialization
	whereClauseStr := a.serializeWhereGroup(whereGroup)

	return &models.DocumentDeleteCommand{
		BundleName:         stmt.BundleName,
		WhereClause:        whereClauseStr,
		Fields:             nil, // Deprecated - WHERE clause is used instead
		DeletedDocumentIDs: nil, // Will be populated by the bundle service
		RawCommand:         "",  // TODO: I could store the original command for debugging
	}, nil
}

// serializeWhereGroup converts a WhereGroup back to a WHERE clause string
// This is necessary for compatibility with the existing bundle service interface
// Reused from UpdateStatementAdapter
func (a *DeleteStatementAdapter) serializeWhereGroup(whereGroup *queryparser.WhereGroup) string {
	if whereGroup == nil {
		return ""
	}

	var parts []string

	// Serialize clauses
	for _, clause := range whereGroup.Clauses {
		// Format: Field Operator Value
		valueStr := fmt.Sprintf("%v", clause.Value)
		// Quote string values
		if _, ok := clause.Value.(string); ok {
			valueStr = fmt.Sprintf("'%s'", clause.Value)
		}
		conditionStr := fmt.Sprintf("%s %s %s", clause.Field, clause.Operator, valueStr)
		parts = append(parts, conditionStr)
	}

	// Serialize nested subgroups recursively
	// TODO: I will add support for complex nested WHERE clauses when needed
	for _, subGroup := range whereGroup.SubGroups {
		groupStr := a.serializeWhereGroup(&subGroup)
		if groupStr != "" {
			parts = append(parts, "("+groupStr+")")
		}
	}

	// Join with the logical operator (AND/OR)
	if len(parts) == 0 {
		return ""
	}

	operator := " AND "
	if whereGroup.Operator == "OR" {
		operator = " OR "
	}

	return strings.Join(parts, operator)
}
