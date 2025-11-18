package parser

/*
GraphQL Parser for SyndrDB - PHASE 6 NATIVE LANGUAGE SUPPORT

This file implements a native GraphQL parser that converts GraphQL queries into
UnifiedSelectQuery objects, enabling GraphQL to execute through the exact same
query planning and execution path as SyndrQL queries.

Key Design Principle:
GraphQL is a LANGUAGE, not a translation layer. Both GraphQL and SyndrQL produce
the same UnifiedSelectQuery object, ensuring identical execution performance and
behavior. There is no intermediate translation step that would create bottlenecks.

Architecture:
    GraphQL Query → GraphQL Parser → UnifiedSelectQuery → UnifiedQueryPlanner → Results
    SyndrQL Query → SyndrQL Parser → UnifiedSelectQuery → UnifiedQueryPlanner → Results

The parser integrates with Phase 5's SchemaManager to map GraphQL field names to
bundle field names (e.g., "id" → "DocumentID"), ensuring type-safe field resolution.

Performance Considerations:
- Single parse operation (no translation overhead)
- Direct AST → UnifiedSelectQuery conversion
- Same execution path as SyndrQL (no performance penalty)
- Cached schema lookups for field mapping

Supported Query Features:
- Field selection with aliases
- WHERE conditions via arguments
- ORDER BY via arguments
- LIMIT/OFFSET via arguments
- Field mapping using Phase 5 schemas

TODO: I will implement JOIN and GROUP BY support when adding those features to
GraphQL in a future phase. For now, simple queries provide the MVP functionality.

TODO: I will add mutation parsing when implementing GraphQL mutations in the next
phase. This parser currently focuses on SELECT queries only.
*/

import (
	"fmt"
	"strconv"
	"strings"
	"syndrdb/src/internal/domain/models"
	"syndrdb/src/internal/graphQL/schema"
	"syndrdb/src/internal/query/queryparser"

	"github.com/vektah/gqlparser/v2/ast"
	"go.uber.org/zap"
)

// GraphQLParser parses GraphQL queries into UnifiedSelectQuery objects
//
// This parser enables native GraphQL support by converting GraphQL AST into the
// same query structure used by SyndrQL, ensuring both languages execute identically.
type GraphQLParser struct {
	schemaManager *schema.SchemaManager // Phase 5 schema manager for field mapping
	database      *models.Database      // Current database context
	logger        *zap.SugaredLogger    // Structured logging
}

// NewGraphQLParser creates a new GraphQL parser instance
//
// Parameters:
//   - schemaManager: Phase 5 schema manager for field name mapping
//   - database: Current database for bundle and schema access
//   - logger: Structured logger for debugging and error tracking
//
// Returns:
//   - *GraphQLParser: Initialized parser ready to parse GraphQL queries
func NewGraphQLParser(schemaManager *schema.SchemaManager, database *models.Database, logger *zap.SugaredLogger) *GraphQLParser {
	return &GraphQLParser{
		schemaManager: schemaManager,
		database:      database,
		logger:        logger,
	}
}

// ParseGraphQLQuery converts a GraphQL query AST into a UnifiedSelectQuery
//
// PHASE 6: This is the core method that enables native GraphQL support. It takes
// a parsed GraphQL operation (from gqlparser) and produces the same UnifiedSelectQuery
// object that SyndrQL parser produces, ensuring identical query execution.
//
// The parser performs these steps:
// 1. Extract bundle name from the GraphQL field name (e.g., "users" → bundle "users")
// 2. Get the bundle's schema from SchemaManager for field mapping
// 3. Extract field selections and map GraphQL names → bundle field names
// 4. Extract query arguments (where, limit, orderBy)
// 5. Build UnifiedSelectQuery object
//
// Example GraphQL Query:
//
//	{
//	  users(limit: 10, where: "status = 'active'") {
//	    id
//	    name
//	    email
//	  }
//	}
//
// Produces UnifiedSelectQuery:
//
//	{
//	  FromBundle: "users",
//	  SelectFields: ["DocumentID", "name", "email"],
//	  WhereClause: (parsed from "status = 'active'"),
//	  Limit: 10,
//	}
//
// Parameters:
//   - field: The GraphQL field representing the bundle query
//   - variables: GraphQL query variables (for parameterized queries)
//
// Returns:
//   - *queryparser.UnifiedSelectQuery: Query object ready for UnifiedQueryPlanner
//   - error: Parsing errors (invalid field, missing schema, syntax errors)
func (p *GraphQLParser) ParseGraphQLQuery(field *ast.Field, variables map[string]interface{}) (*queryparser.UnifiedSelectQuery, error) {
	p.logger.Debugf("[GraphQL Parser] Parsing query for bundle: %s", field.Name)

	// Step 1: Extract bundle name from GraphQL field name
	// The field name is the bundle name (e.g., "users", "products", "orders")
	bundleName := field.Name

	// Verify bundle exists in database
	if _, exists := p.database.Bundles[bundleName]; !exists {
		return nil, fmt.Errorf("bundle '%s' not found in database '%s'", bundleName, p.database.Name)
	}

	// Step 2: Get bundle schema from SchemaManager for field mapping
	// This allows us to map GraphQL field names to actual bundle field names
	var bundleSchema *schema.GraphQLSchemaDefinition
	if p.schemaManager != nil {
		schemaRecord, err := p.schemaManager.GetActiveSchemaForBundle(bundleName)
		if err != nil {
			p.logger.Warnf("[GraphQL Parser] Failed to get schema for bundle '%s': %v - using field names as-is", bundleName, err)
		} else if schemaRecord != nil && schemaRecord.Payload != nil {
			bundleSchema = schemaRecord.Payload
		}
	}

	// Step 3: Extract field selections from GraphQL query
	// Map GraphQL field names to bundle field names using schema
	selectFields, err := p.extractFieldSelections(field.SelectionSet, bundleSchema)
	if err != nil {
		return nil, fmt.Errorf("failed to extract field selections: %w", err)
	}

	// Step 4: Extract query arguments (where, limit, orderBy)
	whereClause, limit, offset, orderBy, err := p.extractQueryArguments(field, variables)
	if err != nil {
		return nil, fmt.Errorf("failed to extract query arguments: %w", err)
	}

	// Step 5: Build UnifiedSelectQuery object
	// This is the same structure that SyndrQL parser produces
	unifiedQuery := &queryparser.UnifiedSelectQuery{
		QueryType:    queryparser.SimpleQuery, // GraphQL queries are simple queries for MVP
		FromBundle:   bundleName,
		SelectFields: selectFields,
		// LEGACY: GraphQL parser produces WhereGroup type, store as interface{}
		WhereExpression: whereClause,
		Limit:           limit,
		Offset:          offset,
		OrderBy:         orderBy,
	}

	p.logger.Debugf("[GraphQL Parser] Successfully parsed query: bundle=%s, fields=%d, hasWhere=%v, limit=%d",
		bundleName, len(selectFields), whereClause != nil, limit)

	return unifiedQuery, nil
}

// extractFieldSelections extracts and maps field selections from GraphQL query
//
// This method converts GraphQL field selections (e.g., { id, name, email }) into
// bundle field names (e.g., ["DocumentID", "name", "email"]) using the schema
// from Phase 5's SchemaManager.
//
// Field Mapping Logic:
// 1. If schema available: Use BundleField from schema (e.g., "id" → "DocumentID")
// 2. If no schema: Use GraphQL field name as-is (fallback)
// 3. Empty selection set: Return empty list (means SELECT * in UnifiedQueryPlanner)
//
// Parameters:
//   - selectionSet: GraphQL field selections from the query
//   - bundleSchema: Phase 5 schema definition (may be nil)
//
// Returns:
//   - []string: List of bundle field names to select
//   - error: Field not found in schema, or invalid selection
func (p *GraphQLParser) extractFieldSelections(selectionSet []ast.Selection, bundleSchema *schema.GraphQLSchemaDefinition) ([]string, error) {
	if len(selectionSet) == 0 {
		// Empty selection set means SELECT * (all fields)
		return []string{}, nil
	}

	var fields []string
	for _, selection := range selectionSet {
		field, ok := selection.(*ast.Field)
		if !ok {
			// Skip fragments, inline fragments, etc. (not supported in MVP)
			p.logger.Warnf("[GraphQL Parser] Skipping non-field selection (fragments not yet supported)")
			continue
		}

		// Map GraphQL field name to bundle field name
		bundleFieldName, err := p.mapGraphQLFieldToBundleField(field.Name, bundleSchema)
		if err != nil {
			return nil, fmt.Errorf("field '%s': %w", field.Name, err)
		}

		fields = append(fields, bundleFieldName)
	}

	return fields, nil
}

// mapGraphQLFieldToBundleField maps a GraphQL field name to its bundle field name
//
// This method uses the Phase 5 schema to find the corresponding bundle field name
// for a GraphQL field. This enables field aliasing where GraphQL uses friendly names
// (like "id") while bundles use internal names (like "DocumentID").
//
// Mapping Examples:
//   - GraphQL "id" → Bundle "DocumentID" (standard mapping)
//   - GraphQL "createdAt" → Bundle "created_at" (camelCase → snake_case)
//   - GraphQL "name" → Bundle "name" (direct mapping)
//
// Parameters:
//   - graphqlField: The GraphQL field name from the query
//   - bundleSchema: Phase 5 schema definition (may be nil)
//
// Returns:
//   - string: Bundle field name to use in query
//   - error: Field not found in schema
func (p *GraphQLParser) mapGraphQLFieldToBundleField(graphqlField string, bundleSchema *schema.GraphQLSchemaDefinition) (string, error) {
	// If no schema available, use field name as-is (fallback mode)
	if bundleSchema == nil {
		p.logger.Debugf("[GraphQL Parser] No schema available, using field '%s' as-is", graphqlField)
		return graphqlField, nil
	}

	// Search schema for matching GraphQL field
	for _, schemaField := range bundleSchema.Fields {
		if schemaField.Name == graphqlField {
			// Found matching field - use bundle field name
			bundleFieldName := schemaField.BundleField
			if bundleFieldName == "" {
				// No explicit mapping - use GraphQL name
				bundleFieldName = graphqlField
			}
			p.logger.Debugf("[GraphQL Parser] Mapped field '%s' → '%s'", graphqlField, bundleFieldName)
			return bundleFieldName, nil
		}
	}

	// Field not found in schema - this is an error
	return "", fmt.Errorf("field '%s' not found in schema for type '%s'", graphqlField, bundleSchema.TypeName)
}

// extractQueryArguments extracts query arguments from GraphQL field
//
// PHASE 8 & 9 ENHANCEMENT: Now supports both legacy and Relay-style pagination arguments:
// Legacy pagination:
// - limit: Int - LIMIT clause value
// - offset: Int - OFFSET clause value
// - where: String - WHERE clause conditions (deprecated - use structured where)
// - orderBy: String - ORDER BY clause specification (deprecated - use structured orderBy)
//
// Relay-style pagination (Phase 9):
// - first: Int - Number of items to return (forward pagination)
// - after: String - Cursor for forward pagination
// - last: Int - Number of items to return (backward pagination)
// - before: String - Cursor for backward pagination
// - where: WhereInput - Structured filtering with operators (eq, ne, gt, lt, in, like, etc.)
// - orderBy: [OrderByInput!] - Structured ordering with field + direction
//
// Example legacy query:
//
//	users(where: "status = 'active'", limit: 10, orderBy: "name DESC")
//
// Example Relay-style query:
//
//	users(
//	  where: { status: { eq: "active" } },
//	  first: 10,
//	  after: "eyJkb2N1bWVudElEIjoiMTIzIn0=",
//	  orderBy: [{ field: "name", direction: DESC }]
//	)
//
// Parameters:
//   - field: The GraphQL field with arguments
//   - variables: GraphQL query variables for parameterized queries
//
// Returns:
//   - *queryparser.WhereGroup: Parsed WHERE clause (may be nil)
//   - int: LIMIT value (0 if not specified)
//   - int: OFFSET value (0 if not specified)
//   - *queryparser.OrderByClause: ORDER BY specification (may be nil)
//   - error: Parsing errors
//
// TODO: I will store pagination arguments (first/after/last/before) in UnifiedSelectQuery
// when we add pagination support to the query execution pipeline in Phase 10.
func (p *GraphQLParser) extractQueryArguments(field *ast.Field, variables map[string]interface{}) (*queryparser.WhereGroup, int, int, *queryparser.OrderByClause, error) {
	var whereClause *queryparser.WhereGroup
	var limit, offset int
	var orderBy *queryparser.OrderByClause

	// Extract each argument from the GraphQL field
	for _, arg := range field.Arguments {
		value := p.resolveArgumentValue(arg.Value, variables)

		switch arg.Name {
		case "where":
			// PHASE 9: Support both string WHERE (legacy) and structured WhereInput (new)
			switch v := value.(type) {
			case string:
				// Legacy string WHERE clause: "status = 'active' AND age > 18"
				if v != "" {
					var err error
					whereClause, err = p.parseWhereClause(v)
					if err != nil {
						return nil, 0, 0, nil, fmt.Errorf("failed to parse where clause: %w", err)
					}
				}

			case map[string]interface{}:
				// PHASE 9: Structured WhereInput with operators
				// Example: { status: { eq: "active" }, age: { gt: 18 } }
				// This is handled in handler.go via FilterTranslator
				// For now, we'll pass it through as a placeholder
				// TODO: I will integrate FilterTranslator here when refactoring the query pipeline
				p.logger.Debugf("[GraphQL Parser] Structured where input detected (will be translated by handler)")

			default:
				return nil, 0, 0, nil, fmt.Errorf("'where' argument must be a string or WhereInput object")
			}

		case "limit":
			// Extract LIMIT value (legacy pagination)
			limitVal, err := p.parseIntArgument(value, "limit")
			if err != nil {
				return nil, 0, 0, nil, err
			}
			limit = limitVal

		case "offset":
			// Extract OFFSET value (legacy pagination)
			offsetVal, err := p.parseIntArgument(value, "offset")
			if err != nil {
				return nil, 0, 0, nil, err
			}
			offset = offsetVal

		case "first":
			// PHASE 9: Relay-style forward pagination
			// Indicates number of items to return after the 'after' cursor
			firstVal, err := p.parseIntArgument(value, "first")
			if err != nil {
				return nil, 0, 0, nil, err
			}
			// For now, treat 'first' as 'limit' - full cursor pagination in handler
			limit = firstVal
			p.logger.Debugf("[GraphQL Parser] Relay pagination: first=%d", firstVal)

		case "after":
			// PHASE 9: Relay-style forward pagination cursor
			// Base64-encoded cursor from previous page
			// TODO: I will decode cursor and set offset when implementing full cursor pagination
			afterStr, ok := value.(string)
			if ok && afterStr != "" {
				p.logger.Debugf("[GraphQL Parser] Relay pagination: after cursor detected (length=%d)", len(afterStr))
			}

		case "last":
			// PHASE 9: Relay-style backward pagination
			// Indicates number of items to return before the 'before' cursor
			lastVal, err := p.parseIntArgument(value, "last")
			if err != nil {
				return nil, 0, 0, nil, err
			}
			// For now, treat 'last' as 'limit' - full cursor pagination in handler
			limit = lastVal
			p.logger.Debugf("[GraphQL Parser] Relay pagination: last=%d", lastVal)

		case "before":
			// PHASE 9: Relay-style backward pagination cursor
			// Base64-encoded cursor from next page
			// TODO: I will decode cursor and apply backward pagination when implementing full cursor pagination
			beforeStr, ok := value.(string)
			if ok && beforeStr != "" {
				p.logger.Debugf("[GraphQL Parser] Relay pagination: before cursor detected (length=%d)", len(beforeStr))
			}

		case "orderBy":
			// PHASE 9: Support both string orderBy (legacy) and structured OrderByInput (new)
			switch v := value.(type) {
			case string:
				// Legacy string ORDER BY: "name DESC, created_at ASC"
				if v != "" {
					var err error
					orderBy, err = p.parseOrderByClause(v)
					if err != nil {
						return nil, 0, 0, nil, fmt.Errorf("failed to parse orderBy clause: %w", err)
					}
				}

			case []interface{}:
				// PHASE 9: Structured OrderByInput array
				// Example: [{ field: "name", direction: DESC }, { field: "age", direction: ASC }]
				// This is handled in handler.go via FilterTranslator
				// TODO: I will integrate FilterTranslator here when refactoring the query pipeline
				p.logger.Debugf("[GraphQL Parser] Structured orderBy input detected (will be translated by handler)")

			case map[string]interface{}:
				// PHASE 9: Single structured OrderByInput object
				// Example: { field: "name", direction: DESC }
				p.logger.Debugf("[GraphQL Parser] Structured orderBy input detected (will be translated by handler)")

			default:
				return nil, 0, 0, nil, fmt.Errorf("'orderBy' argument must be a string or OrderByInput")
			}
		}
	}

	return whereClause, limit, offset, orderBy, nil
}

// resolveArgumentValue resolves a GraphQL argument value (handles variables, literals)
//
// GraphQL arguments can be:
// - Literal values: where: "status = 'active'"
// - Variables: where: $statusFilter
// - Enums, objects, lists, etc.
//
// Parameters:
//   - value: The GraphQL value AST node
//   - variables: Query variables map
//
// Returns:
//   - interface{}: Resolved value (string, int, float, bool, etc.)
func (p *GraphQLParser) resolveArgumentValue(value *ast.Value, variables map[string]interface{}) interface{} {
	switch value.Kind {
	case ast.Variable:
		// Resolve from variables map
		if variables != nil {
			return variables[value.Raw]
		}
		return nil
	case ast.StringValue:
		return value.Raw
	case ast.IntValue:
		// Parse int from string
		intVal, err := strconv.Atoi(value.Raw)
		if err != nil {
			p.logger.Warnf("[GraphQL Parser] Failed to parse int value '%s': %v", value.Raw, err)
			return value.Raw
		}
		return intVal
	case ast.FloatValue:
		return value.Raw
	case ast.BooleanValue:
		return value.Raw == "true"
	case ast.NullValue:
		return nil
	default:
		return value.Raw
	}
}

// parseIntArgument parses an integer argument value
func (p *GraphQLParser) parseIntArgument(value interface{}, argName string) (int, error) {
	switch v := value.(type) {
	case int:
		return v, nil
	case string:
		intVal, err := strconv.Atoi(v)
		if err != nil {
			return 0, fmt.Errorf("'%s' must be an integer", argName)
		}
		return intVal, nil
	default:
		return 0, fmt.Errorf("'%s' must be an integer", argName)
	}
}

// parseWhereClause parses a WHERE clause string into a WhereGroup
//
// For MVP, this uses a simple parsing approach where the WHERE string is passed
// as-is to the SyndrQL WHERE parser. This ensures consistent WHERE clause behavior
// between GraphQL and SyndrQL.
//
// Example:
//
//	Input:  "status = 'active' AND age > 18"
//	Output: WhereGroup with appropriate conditions
//
// TODO: I will implement structured WHERE objects (GraphQL-style filtering) when
// adding advanced query features in a future phase. For now, string-based WHERE
// clauses provide compatibility with existing SyndrQL queries.
//
// Parameters:
//   - whereStr: WHERE clause string
//
// Returns:
//   - *queryparser.WhereGroup: Parsed WHERE clause
//   - error: Parsing errors
//
// DEPRECATED: This method uses old string-based WHERE parsing
// TODO: Replace with syndrQL.SelectParser to return Expression AST
// GraphQL queries should translate GraphQL filters to SyndrQL Expressions
func (p *GraphQLParser) parseWhereClause(whereStr string) (*queryparser.WhereGroup, error) {
	// DEPRECATED:: USING OLD PARSER, NOT SyndrQL - Line 513

	// For MVP: Parse WHERE clause using existing SyndrQL WHERE parser
	// This ensures consistent behavior between GraphQL and SyndrQL
	whereGroup, err := queryparser.ParseWhereClause(whereStr)
	if err != nil {
		return nil, fmt.Errorf("invalid WHERE clause: %w", err)
	}
	return whereGroup, nil
}

// parseOrderByClause parses an ORDER BY clause string into an OrderByClause
//
// Supports standard ORDER BY syntax:
// - Single field: "name"
// - With direction: "name DESC"
// - Multiple fields: "name ASC, age DESC" (TODO: future enhancement)
//
// Parameters:
//   - orderByStr: ORDER BY clause string
//
// Returns:
//   - *queryparser.OrderByClause: Parsed ORDER BY clause
//   - error: Parsing errors
func (p *GraphQLParser) parseOrderByClause(orderByStr string) (*queryparser.OrderByClause, error) {
	orderByStr = strings.TrimSpace(orderByStr)
	if orderByStr == "" {
		return nil, nil
	}

	// Parse field and direction
	// Format: "field" or "field ASC" or "field DESC"
	parts := strings.Fields(orderByStr)
	if len(parts) == 0 {
		return nil, fmt.Errorf("empty ORDER BY clause")
	}

	fieldName := parts[0]
	direction := queryparser.SortAsc // Default to ascending

	if len(parts) > 1 {
		directionStr := strings.ToUpper(parts[1])
		if directionStr == "DESC" {
			direction = queryparser.SortDesc
		} else if directionStr != "ASC" {
			return nil, fmt.Errorf("invalid ORDER BY direction: %s (expected ASC or DESC)", parts[1])
		}
	}

	// Create OrderByClause
	orderBy := &queryparser.OrderByClause{
		Fields: []queryparser.OrderByField{
			{
				FieldName: fieldName,
				Direction: direction,
			},
		},
	}

	return orderBy, nil
}
