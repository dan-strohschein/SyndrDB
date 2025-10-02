package graphQL

/*
GraphQL Handler for SyndrDB

This file implements the GraphQL API handler for SyndrDB. It provides:
- GraphQL query validation and parsing using gqlparser
- Integration with existing SyndrDB command director
- Type-safe GraphQL operations
- Error handling and validation

The handler processes GraphQL queries and translates them into
SyndrDB native query language commands, ensuring consistent
behavior between GraphQL and native interfaces.
*/

import (
	"encoding/json"
	"fmt"
	"strings"
	"syndrdb/src/internal/domain/models"
	"syndrdb/src/internal/server"

	"github.com/vektah/gqlparser/v2"
	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/parser"
	"github.com/vektah/gqlparser/v2/validator"
	"go.uber.org/zap"
)

// GraphQLHandler handles GraphQL requests
type GraphQLHandler struct {
	schema         *ast.Schema
	serviceManager server.ServiceManager
	database       *models.Database
	logger         *zap.SugaredLogger
}

// GraphQLRequest represents a GraphQL request from TCP socket
type GraphQLRequest struct {
	Query         string                 `json:"query"`
	OperationName string                 `json:"operationName,omitempty"`
	Variables     map[string]interface{} `json:"variables,omitempty"`
}

// GraphQLResponse represents a GraphQL HTTP response
type GraphQLResponse struct {
	Data   interface{}    `json:"data,omitempty"`
	Errors []GraphQLError `json:"errors,omitempty"`
}

// GraphQLError represents a GraphQL error
type GraphQLError struct {
	Message    string                 `json:"message"`
	Path       []interface{}          `json:"path,omitempty"`
	Locations  []GraphQLLocation      `json:"locations,omitempty"`
	Extensions map[string]interface{} `json:"extensions,omitempty"`
}

// GraphQLLocation represents an error location in the query
type GraphQLLocation struct {
	Line   int `json:"line"`
	Column int `json:"column"`
}

// NewGraphQLHandler creates a new GraphQL handler
func NewGraphQLHandler(serviceManager server.ServiceManager, database *models.Database, logger *zap.SugaredLogger) (*GraphQLHandler, error) {
	if logger == nil {
		return nil, fmt.Errorf("logger cannot be nil")
	}

	// Load and parse the GraphQL schema
	schema, err := loadSchema()
	if err != nil {
		return nil, fmt.Errorf("failed to load GraphQL schema: %w", err)
	}

	handler := &GraphQLHandler{
		schema:         schema,
		serviceManager: serviceManager,
		database:       database,
		logger:         logger,
	}

	logger.Info("GraphQL handler initialized successfully")
	return handler, nil
}

// loadSchema loads the GraphQL schema from the embedded schema definition
func loadSchema() (*ast.Schema, error) {
	// For now, we'll use a simplified schema string
	// In production, this should load from the schema.graphql file
	schemaString := `
		scalar JSON
		scalar DateTime

		type Query {
			databases: [Database!]!
			database(name: String!): Database
			bundles(database: String!): [Bundle!]!
			bundle(name: String!, database: String!): Bundle
			documents(bundle: String!, where: String, orderBy: String, limit: Int): [Document!]!
			document(id: String!, bundle: String!): Document
		}

		type Mutation {
			createDatabase(name: String!): Database!
			deleteDatabase(name: String!): Boolean!
			createBundle(name: String!, database: String!, fields: String!): Bundle!
			createDocument(bundle: String!, fields: String!): Document!
			updateDocument(id: String!, bundle: String!, fields: String!): Document!
			deleteDocument(bundle: String!, where: String!): Int!
		}

		type Database {
			name: String!
			bundles: [Bundle!]!
			createdAt: DateTime!
		}

		type Bundle {
			name: String!
			database: String!
			documents: [Document!]!
			documentCount: Int!
			createdAt: DateTime!
		}

		type Document {
			id: String!
			bundle: String!
			fields: JSON!
			createdAt: DateTime!
			updatedAt: DateTime!
		}
	`

	schema, err := gqlparser.LoadSchema(&ast.Source{
		Name:    "schema.graphql",
		Input:   schemaString,
		BuiltIn: false,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to parse GraphQL schema: %w", err)
	}

	return schema, nil
}

// ProcessGraphQLCommand processes a GraphQL command received via TCP socket
// The input should be in the format: GRAPHQL::{query: "...", variables: {...}}
func (h *GraphQLHandler) ProcessGraphQLCommand(command string) (interface{}, error) {
	// Remove the GRAPHQL:: prefix
	if !strings.HasPrefix(command, "GRAPHQL::") {
		return nil, fmt.Errorf("invalid GraphQL command format: missing GRAPHQL:: prefix")
	}

	graphqlPayload := strings.TrimPrefix(command, "GRAPHQL::")
	graphqlPayload = strings.TrimSpace(graphqlPayload)

	// Remove trailing semicolon if present (common from client code)
	graphqlPayload = strings.TrimSuffix(graphqlPayload, ";")

	if graphqlPayload == "" {
		return nil, fmt.Errorf("GraphQL query is required")
	}

	var req GraphQLRequest

	// Try to parse as JSON first (for structured requests)
	if strings.HasPrefix(graphqlPayload, "{") {
		if err := json.Unmarshal([]byte(graphqlPayload), &req); err != nil {
			return nil, fmt.Errorf("invalid GraphQL JSON: %v", err)
		}
	} else {
		// Treat as plain query string
		req.Query = graphqlPayload
	}

	if req.Query == "" {
		return nil, fmt.Errorf("GraphQL query is required")
	}

	// Process the GraphQL request
	response := h.processGraphQLRequest(req)

	// Return the response (will be JSON encoded by the command director)
	return response, nil
}

// processGraphQLRequest processes a GraphQL request and returns a response
func (h *GraphQLHandler) processGraphQLRequest(req GraphQLRequest) GraphQLResponse {
	// Parse the GraphQL query
	query, err := parser.ParseQuery(&ast.Source{
		Name:  "request",
		Input: req.Query,
	})
	if err != nil {
		return GraphQLResponse{
			Errors: []GraphQLError{{
				Message: fmt.Sprintf("Query parsing error: %v", err),
			}},
		}
	}

	// Validate the query against the schema
	validationErrors := validator.Validate(h.schema, query)
	if len(validationErrors) > 0 {
		var errors []GraphQLError
		for _, validationError := range validationErrors {
			errors = append(errors, GraphQLError{
				Message: validationError.Message,
				Locations: []GraphQLLocation{{
					Line:   1, // Default line number
					Column: 1, // Default column number
				}},
			})
		}
		return GraphQLResponse{Errors: errors}
	}

	// Execute the query
	data, err := h.executeQuery(query, req.Variables)
	if err != nil {
		return GraphQLResponse{
			Errors: []GraphQLError{{
				Message: fmt.Sprintf("Execution error: %v", err),
			}},
		}
	}

	return GraphQLResponse{Data: data}
}

// executeQuery executes a validated GraphQL query
func (h *GraphQLHandler) executeQuery(query *ast.QueryDocument, variables map[string]interface{}) (interface{}, error) {
	// For now, we'll implement a simple resolver that translates GraphQL to SyndrDB commands
	for _, operation := range query.Operations {
		switch operation.Operation {
		case ast.Query:
			return h.executeQueryOperation(operation, variables)
		case ast.Mutation:
			return h.executeMutationOperation(operation, variables)
		default:
			return nil, fmt.Errorf("unsupported operation type: %s", operation.Operation)
		}
	}

	return nil, fmt.Errorf("no operations found in query")
}

// executeQueryOperation executes a GraphQL query operation
func (h *GraphQLHandler) executeQueryOperation(operation *ast.OperationDefinition, variables map[string]interface{}) (interface{}, error) {
	result := make(map[string]interface{})

	for _, selection := range operation.SelectionSet {
		field, ok := selection.(*ast.Field)
		if !ok {
			continue
		}

		switch field.Name {
		case "databases":
			data, err := h.resolveDatabases() // previously field, variables
			if err != nil {
				return nil, err
			}
			result[field.Alias] = data

		case "database":
			data, err := h.resolveDatabase(field, variables)
			if err != nil {
				return nil, err
			}
			result[field.Alias] = data

		case "bundles":
			data, err := h.resolveBundles(field, variables)
			if err != nil {
				return nil, err
			}
			result[field.Alias] = data

		case "bundle":
			data, err := h.resolveBundle(field, variables)
			if err != nil {
				return nil, err
			}
			result[field.Alias] = data

		case "documents":
			data, err := h.resolveDocuments(field, variables)
			if err != nil {
				return nil, err
			}
			result[field.Alias] = data

		case "document":
			data, err := h.resolveDocument(field, variables)
			if err != nil {
				return nil, err
			}
			result[field.Alias] = data

		default:
			return nil, fmt.Errorf("unknown query field: %s", field.Name)
		}
	}

	return result, nil
}

// executeMutationOperation executes a GraphQL mutation operation
func (h *GraphQLHandler) executeMutationOperation(operation *ast.OperationDefinition, variables map[string]interface{}) (interface{}, error) {
	result := make(map[string]interface{})

	for _, selection := range operation.SelectionSet {
		field, ok := selection.(*ast.Field)
		if !ok {
			continue
		}

		switch field.Name {
		case "createDatabase":
			data, err := h.mutateCreateDatabase(field, variables)
			if err != nil {
				return nil, err
			}
			result[field.Alias] = data

		case "createBundle":
			data, err := h.mutateCreateBundle(field, variables)
			if err != nil {
				return nil, err
			}
			result[field.Alias] = data

		case "createDocument":
			data, err := h.mutateCreateDocument(field, variables)
			if err != nil {
				return nil, err
			}
			result[field.Alias] = data

		case "updateDocument":
			data, err := h.mutateUpdateDocument(field, variables)
			if err != nil {
				return nil, err
			}
			result[field.Alias] = data

		case "deleteDocument":
			data, err := h.mutateDeleteDocument(field, variables)
			if err != nil {
				return nil, err
			}
			result[field.Alias] = data

		default:
			return nil, fmt.Errorf("unknown mutation field: %s", field.Name)
		}
	}

	return result, nil
}

// Note: writeError method removed as we no longer use HTTP responses

// getArgument gets an argument value from a GraphQL field
func (h *GraphQLHandler) getArgument(field *ast.Field, name string, variables map[string]interface{}) (interface{}, bool) {
	for _, arg := range field.Arguments {
		if arg.Name == name {
			return h.resolveValue(arg.Value, variables), true
		}
	}
	return nil, false
}

// resolveValue resolves a GraphQL value (handling variables, literals, etc.)
func (h *GraphQLHandler) resolveValue(value *ast.Value, variables map[string]interface{}) interface{} {
	switch value.Kind {
	case ast.Variable:
		if variables != nil {
			return variables[value.Raw]
		}
		return nil
	case ast.StringValue:
		return value.Raw
	case ast.IntValue:
		return value.Raw
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
