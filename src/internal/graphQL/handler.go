package graphQL

/*
GraphQL Handler for SyndrDB - PHASE 6 IMPLEMENTATION

This file implements the GraphQL query execution and introspection handler for SyndrDB.
It integrates with Phase 5's SchemaManager to dynamically generate GraphQL schemas from
bundle structures and execute queries against bundle data.

Key Features:
- Native protocol integration (GRAPHQL:: prefix commands, not HTTP)
- Dynamic schema generation from bundle definitions (Phase 5 integration)
- GraphQL introspection support (__schema, __type queries)
- Query execution with field mapping and type coercion
- GraphQL-compliant error responses

The handler processes GraphQL queries received through SyndrDB's native protocol
(via Node/TypeScript driver or other native clients) and translates them into
SyndrDB bundle queries, ensuring type-safe field resolution and proper error handling.

IMPORTANT: This implementation focuses on QUERIES and INTROSPECTION only.
Mutations and subscriptions are marked with TODO comments for future implementation.
*/

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"syndrdb/src/internal/domain/document"
	"syndrdb/src/internal/domain/models"
	gqlcontext "syndrdb/src/internal/graphQL/context"
	"syndrdb/src/internal/graphQL/mutations"
	"syndrdb/src/internal/graphQL/optimization"
	graphqlparser "syndrdb/src/internal/graphQL/parser"
	"syndrdb/src/internal/graphQL/schema"
	"syndrdb/src/internal/query/planner"
	"syndrdb/src/internal/query/queryparser"
	"syndrdb/src/internal/server"
	"syndrdb/src/pkg/common/helpers"
	"syndrdb/src/pkg/settings"
	"time"

	"github.com/vektah/gqlparser/v2"
	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/parser"
	"github.com/vektah/gqlparser/v2/validator"
	"go.uber.org/zap"
)

// GraphQLHandler handles GraphQL query execution and introspection
//
// PHASE 6 INTEGRATION: This handler integrates with Phase 5's SchemaManager to:
// 1. Dynamically generate GraphQL schemas from bundle structures
// 2. Execute queries against bundle data with proper field mapping
// 3. Support introspection queries for schema exploration
// 4. Handle errors with GraphQL-compliant error structure
//
// PHASE 11 INTEGRATION: Added mutation support with dedicated modules:
// - MutationParser: Parses GraphQL mutations into DocumentCommand structs
// - MutationExecutor: Executes mutations via BundleService
// - MutationResolver: Resolves mutation response fields
// - InputValidator: Validates mutation inputs at GraphQL layer
//
// The handler is designed for SyndrDB's native protocol, not HTTP endpoints.
// Queries are received as "GRAPHQL::{...}" commands through the TCP socket.
type GraphQLHandler struct {
	schema             *ast.Schema                      // Dynamically generated GraphQL schema
	schemaManager      *schema.SchemaManager            // Phase 5 schema manager for bundle schemas
	serviceManager     server.ServiceManager            // Access to bundle and database services
	database           *models.Database                 // Current database context
	logger             *zap.SugaredLogger               // Structured logging
	securityConfig     *settings.GraphQLSecurityConfig  // GraphQL security configuration
	complexityAnalyzer *optimization.ComplexityAnalyzer // Query complexity analyzer
	rateLimiter        *RateLimiter                     // Per-user rate limiter
	queryMonitor       *QueryMonitor                    // Query metrics monitor

	// Phase 11: Mutation support components
	mutationParser   *mutations.MutationParser   // Parses GraphQL mutations
	mutationExecutor *mutations.MutationExecutor // Executes mutations
	mutationResolver *mutations.MutationResolver // Resolves mutation responses
	inputValidator   *mutations.InputValidator   // Validates mutation inputs

	// Per-request context (set during processGraphQLRequest)
	currentSession  *server.Session // Current request's session (nil for anonymous)
	currentClientIP string          // Current request's client IP
}

// GraphQLRequest represents a GraphQL request from TCP socket
type GraphQLRequest struct {
	Query         string                 `json:"query"`
	OperationName string                 `json:"operationName,omitempty"`
	Variables     map[string]interface{} `json:"variables,omitempty"`

	// Security context fields (populated by server, not from client JSON)
	UserID    string
	Username  string
	ClientIP  string
	SessionID string
	UserRole  string
	IsAdmin   bool
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

// NewGraphQLHandler creates a new GraphQL handler with Phase 5 SchemaManager integration
//
// PHASE 6 INTEGRATION: This constructor now accepts a SchemaManager parameter to enable
// dynamic schema generation from bundle definitions. The GraphQL schema is built on-demand
// from the currently active bundle schemas, allowing schema introspection and query validation
// against live database structures.
//
// Parameters:
//   - serviceManager: Access to bundle and database services for query execution
//   - database: Current database context for schema and data access
//   - schemaManager: Phase 5 schema manager for accessing bundle schemas
//   - logger: Structured logger for operation tracking
//
// Returns:
//   - *GraphQLHandler: Initialized handler ready to process GraphQL queries
//   - error: Initialization errors (schema loading, validation failures)
//
// The handler generates a GraphQL schema by:
// 1. Retrieving all active bundle schemas from SchemaManager
// 2. Converting Phase 5 schema definitions to gqlparser AST format
// 3. Building Query type with fields for each bundle
// 4. Adding introspection support (__schema, __type)
func NewGraphQLHandler(serviceManager server.ServiceManager, database *models.Database, schemaManager *schema.SchemaManager, logger *zap.SugaredLogger, securityConfig *settings.GraphQLSecurityConfig) (*GraphQLHandler, error) {
	if logger == nil {
		return nil, fmt.Errorf("logger cannot be nil")
	}

	if database == nil {
		return nil, fmt.Errorf("database cannot be nil")
	}

	// Use default security config if not provided
	if securityConfig == nil {
		securityConfig = settings.DefaultGraphQLSecurityConfig()
	}

	// PHASE 6: SchemaManager is optional - if nil, we'll generate a basic schema
	// This allows the handler to work even if GraphQL schema generation is disabled
	if schemaManager == nil {
		logger.Warn("SchemaManager is nil - using fallback static schema")
	}

	// PHASE 6: Load and build GraphQL schema dynamically from bundle schemas
	// This replaces the static schema string with live schema generation
	gqlSchema, err := loadSchemaFromBundles(database, schemaManager, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to load GraphQL schema: %w", err)
	}

	// Phase 11: Initialize mutation support components
	mutationParser := mutations.NewMutationParser(logger)
	mutationExecutor := mutations.NewMutationExecutor(serviceManager, database, logger)
	mutationResolver := mutations.NewMutationResolver(serviceManager, database, logger)
	inputValidator := mutations.NewInputValidator(logger)

	// Initialize complexity analyzer with role-based config
	complexityConfig := &optimization.ComplexityConfig{
		MaxDepth:      securityConfig.DepthLimit,
		MaxBreadth:    50,                                          // Fixed breadth limit
		MaxComplexity: securityConfig.AuthenticatedComplexityLimit, // Will be checked per-role
		WarnThreshold: int(float64(securityConfig.AuthenticatedComplexityLimit) * securityConfig.ExpensiveQueryThreshold),
	}
	complexityAnalyzer := optimization.NewComplexityAnalyzer(complexityConfig)

	// Initialize rate limiter
	rateLimiter := NewRateLimiter(securityConfig, logger)

	// Initialize query monitor
	queryMonitor := NewQueryMonitor(securityConfig, logger)

	handler := &GraphQLHandler{
		schema:             gqlSchema,
		schemaManager:      schemaManager,
		serviceManager:     serviceManager,
		database:           database,
		logger:             logger,
		securityConfig:     securityConfig,
		complexityAnalyzer: complexityAnalyzer,
		rateLimiter:        rateLimiter,
		queryMonitor:       queryMonitor,

		// Phase 11: Mutation components
		mutationParser:   mutationParser,
		mutationExecutor: mutationExecutor,
		mutationResolver: mutationResolver,
		inputValidator:   inputValidator,
	}

	logger.Infof("GraphQL handler initialized successfully for database '%s' with mutation support", database.Name)
	return handler, nil
}

// loadSchemaFromBundles dynamically generates a GraphQL schema from bundle definitions
//
// PHASE 6 IMPLEMENTATION: This function integrates with Phase 5's SchemaManager to build
// a GraphQL schema on-demand from the currently active bundle schemas. This enables:
// 1. Dynamic schema generation reflecting live database structure
// 2. Type-safe field resolution based on bundle field definitions
// 3. Introspection queries that return actual schema metadata
// 4. Automatic schema updates when bundles are modified
//
// Parameters:
//   - database: Current database to generate schema for
//   - schemaManager: Phase 5 schema manager (may be nil if GraphQL disabled)
//   - logger: Structured logger
//
// Returns:
//   - *ast.Schema: gqlparser-compatible GraphQL schema
//   - error: Schema generation or parsing errors
//
// Schema Structure:
// - Scalar types: String, Int, Float, Boolean, ID (standard GraphQL scalars)
// - Query type: One field per bundle (users, products, orders, etc.)
// - Type definitions: One type per bundle with fields from schema
// - Introspection: __schema and __type queries for schema exploration
//
// TODO: I will implement Mutation types here when we add mutation support in a future phase.
// For now, mutations are explicitly excluded from the MVP scope per requirements.
func loadSchemaFromBundles(database *models.Database, schemaManager *schema.SchemaManager, logger *zap.SugaredLogger) (*ast.Schema, error) {
	// Start building the schema string with scalar definitions
	// These are standard GraphQL scalars that all schemas need
	schemaString := `
		scalar DateTime
		scalar Date
		
		# Query type with fields for each bundle
		# Each bundle becomes a queryable field returning a list of documents
		type Query {
	`

	// PHASE 6: If SchemaManager is available, generate types from bundle schemas
	// Otherwise fall back to generic document structure
	bundleTypes := make(map[string]*schema.GraphQLSchemaDefinition)

	if schemaManager != nil {
		// Get all active bundle names from the database
		for bundleName := range database.Bundles {
			// Retrieve the active schema for this bundle
			bundleSchema, err := schemaManager.GetActiveSchemaForBundle(bundleName)
			if err != nil {
				logger.Warnf("Failed to get schema for bundle '%s': %v - using generic type", bundleName, err)
				continue
			}

			if bundleSchema != nil && bundleSchema.Payload != nil {
				bundleTypes[bundleName] = bundleSchema.Payload

				// Add query field for this bundle
				// Example: users(limit: Int, offset: Int, where: String, orderBy: String): [User!]!
				typeName := bundleSchema.Payload.TypeName
				schemaString += fmt.Sprintf("\t\t%s(limit: Int, offset: Int, where: String, orderBy: String): [%s!]!\n",
					bundleName, typeName)
			}
		}
	} else {
		// Fallback: Use generic Document type if no schema manager
		logger.Warn("No SchemaManager available - using generic Document type for all bundles")
		for bundleName := range database.Bundles {
			schemaString += fmt.Sprintf("\t\t%s(limit: Int, offset: Int, where: String, orderBy: String): [Document!]!\n", bundleName)
		}
	}

	// Close Query type and add introspection support
	schemaString += "\t}\n\n"

	// PHASE 6: Generate type definitions for each bundle from Phase 5 schemas
	if len(bundleTypes) > 0 {
		for _, bundleSchema := range bundleTypes {
			// Build type definition from schema
			schemaString += fmt.Sprintf("\ttype %s {\n", bundleSchema.TypeName)

			// Add fields from bundle schema
			for _, field := range bundleSchema.Fields {
				schemaString += fmt.Sprintf("\t\t%s: %s\n", field.Name, field.Type)
			}

			schemaString += "\t}\n\n"
		}
	} else {
		// Fallback generic Document type
		schemaString += `
	type Document {
		id: ID!
		fields: String!
	}
`
	}

	// PHASE 11: Generate Mutation type with auto-generated CRUD mutations
	mutationGenerator := mutations.NewMutationGenerator(logger)

	// Add Mutation type with CRUD operations for each bundle
	mutationSchema := mutationGenerator.GenerateMutationSchema(database, schemaManager)
	schemaString += mutationSchema

	// Add Input types for create and update mutations
	inputTypes := mutationGenerator.GenerateInputTypes(database, schemaManager)
	schemaString += inputTypes

	// Add Delete payload types
	deletePayloads := mutationGenerator.GenerateDeletePayloadTypes(database)
	schemaString += deletePayloads

	// Parse the generated schema string into gqlparser AST
	gqlSchema, err := gqlparser.LoadSchema(&ast.Source{
		Name:    "schema.graphql",
		Input:   schemaString,
		BuiltIn: false,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to parse generated GraphQL schema: %w", err)
	}

	logger.Infof("Generated GraphQL schema with %d bundle types and mutation support", len(bundleTypes))
	return gqlSchema, nil
}

// ProcessGraphQLCommand processes a GraphQL command received via TCP socket
// The input should be in the format: GRAPHQL::{query: "...", variables: {...}}
func (h *GraphQLHandler) ProcessGraphQLCommand(command string, session *server.Session, clientIP string) (interface{}, error) {
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

	// Populate security context from session
	if session != nil {
		req.SessionID = session.SessionID
		req.UserID = session.UserID
		req.Username = session.Username
		req.ClientIP = clientIP

		// Get role with caching - ignore error, default to empty string
		role, _ := session.GetRole()
		req.UserRole = role
		req.IsAdmin = session.GetIsAdmin()
	} else {
		// Anonymous user
		req.ClientIP = clientIP
		req.UserRole = "anonymous"
		req.IsAdmin = false
	}

	// Store session for resolvers to access
	h.currentSession = session

	// Process the GraphQL request
	response := h.processGraphQLRequest(req)

	// Clear session after request
	h.currentSession = nil
	h.currentClientIP = ""

	// Return the response (will be JSON encoded by the command director)
	return response, nil
}

// processGraphQLRequest processes a GraphQL request and returns a response
//
// PHASE 10 UPDATE: Creates RequestContext with DataLoaders for N+1 elimination
// SECURITY LAYERS: Orchestrates all 5 security layers with metrics recording
func (h *GraphQLHandler) processGraphQLRequest(req GraphQLRequest) GraphQLResponse {
	// Store request context for resolvers (will be cleared at end of request)
	h.currentClientIP = req.ClientIP
	// Note: currentSession set in ProcessGraphQLCommand if authenticated

	// Layer 5: Initialize query metrics (always collected if monitoring enabled)
	startTime := time.Now()
	queryID := fmt.Sprintf("gql_%d", startTime.UnixNano())

	metric := &QueryMetric{
		QueryID:   queryID,
		Username:  req.Username,
		ClientIP:  req.ClientIP,
		Role:      req.UserRole,
		IsAdmin:   req.IsAdmin,
		Query:     req.Query,
		StartTime: startTime,
		Success:   false, // Will be set to true on success
	}

	// Defer metric recording to capture final state
	defer func() {
		metric.Duration = time.Since(startTime)
		h.queryMonitor.RecordQuery(metric)
	}()

	// PHASE 10: Create request context with DataLoaders
	reqCtx := gqlcontext.NewRequestContext(&h.serviceManager, h.database, h.logger)
	defer reqCtx.Cleanup() // Always cleanup to prevent memory leaks

	// Create Go context with request context
	ctx := gqlcontext.WithRequestContext(context.Background(), reqCtx)

	// Parse the GraphQL query
	query, err := parser.ParseQuery(&ast.Source{
		Name:  "request",
		Input: req.Query,
	})
	if err != nil {
		metric.ErrorCode = "PARSE_ERROR"
		metric.ErrorMessage = err.Error()
		return GraphQLResponse{
			Errors: []GraphQLError{{
				Message: fmt.Sprintf("Query parsing error: %v", err),
			}},
		}
	}

	// Determine operation type and DDL status for metrics
	metric.OperationType = "query"
	for _, operation := range query.Operations {
		if operation.Operation == ast.Mutation {
			metric.OperationType = "mutation"
			metric.IsDDL = h.isDDLMutation(operation)
			break
		}
	}

	// Validate the query against the schema
	validationErrors := validator.Validate(h.schema, query)
	if len(validationErrors) > 0 {
		metric.ErrorCode = "VALIDATION_ERROR"
		metric.ErrorMessage = validationErrors[0].Message
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

	// Layer 1 & 2: Check query complexity and depth (skip for admins)
	if (h.securityConfig.EnableComplexityLimit || h.securityConfig.EnableDepthLimit) && !req.IsAdmin {
		// Determine role-specific complexity limit
		maxComplexity := h.securityConfig.AnonymousComplexityLimit
		if req.UserRole != "anonymous" && req.UserRole != "" {
			maxComplexity = h.securityConfig.AuthenticatedComplexityLimit
		}

		// Update analyzer config with role-specific limits
		h.complexityAnalyzer = optimization.NewComplexityAnalyzer(&optimization.ComplexityConfig{
			MaxDepth:      h.securityConfig.DepthLimit,
			MaxBreadth:    50,
			MaxComplexity: maxComplexity,
			WarnThreshold: int(float64(maxComplexity) * h.securityConfig.ExpensiveQueryThreshold),
		})

		// Analyze query complexity
		complexityResult := h.complexityAnalyzer.AnalyzeQuery(query, h.database)

		// Record complexity metrics
		metric.Complexity = complexityResult.Complexity
		metric.Depth = complexityResult.Depth

		// Log warnings for high complexity
		for _, warning := range complexityResult.Warnings {
			h.logger.Warnw("GraphQL query complexity warning",
				"warning", warning,
				"username", req.Username,
				"clientIP", req.ClientIP,
				"complexity", complexityResult.Complexity,
				"depth", complexityResult.Depth,
			)
		}

		// Reject if exceeds limits
		if !complexityResult.IsAllowed {
			metric.ErrorCode = "COMPLEXITY_LIMIT_EXCEEDED"
			metric.ErrorMessage = complexityResult.Reason
			h.logger.Warnw("GraphQL query rejected - complexity exceeded",
				"reason", complexityResult.Reason,
				"username", req.Username,
				"clientIP", req.ClientIP,
				"role", req.UserRole,
				"complexity", complexityResult.Complexity,
				"depth", complexityResult.Depth,
				"maxComplexity", maxComplexity,
			)
			return GraphQLResponse{
				Errors: []GraphQLError{{
					Message: fmt.Sprintf("Query too complex: %s", complexityResult.Reason),
					Extensions: map[string]interface{}{
						"code":       "COMPLEXITY_LIMIT_EXCEEDED",
						"complexity": complexityResult.Complexity,
						"depth":      complexityResult.Depth,
						"limit":      maxComplexity,
					},
				}},
			}
		}
	}

	// Layer 3: Check rate limiting (skip for admins)
	if h.securityConfig.EnableGraphQLRateLimit && !req.IsAdmin {
		// Determine operation type from query
		operationType := "query"
		isDDL := false

		for _, operation := range query.Operations {
			if operation.Operation == ast.Mutation {
				operationType = "mutation"
				// Check if mutation contains DDL operations (10x cost)
				isDDL = h.isDDLMutation(operation)
			}
		}

		// Calculate token cost for metrics
		cost := h.rateLimiter.getOperationCost(operationType, isDDL)
		metric.TokensCost = cost

		// Check rate limit
		allowed, err := h.rateLimiter.CheckRateLimit(
			req.Username,
			req.ClientIP,
			req.UserRole,
			req.IsAdmin,
			operationType,
			isDDL,
		)

		// Record available tokens for metrics
		bucketStats := h.rateLimiter.GetBucketStats(req.Username, req.ClientIP)
		if bucketStats != nil {
			if tokens, ok := bucketStats["availableTokens"].(float64); ok {
				metric.TokensAvailable = tokens
			}
		}

		if err != nil {
			h.logger.Errorw("Rate limiter error",
				"error", err,
				"username", req.Username,
				"clientIP", req.ClientIP,
			)
		}

		if !allowed {
			metric.ErrorCode = "RATE_LIMIT_EXCEEDED"
			metric.ErrorMessage = "Rate limit exceeded"
			return GraphQLResponse{
				Errors: []GraphQLError{{
					Message: "Rate limit exceeded. Please try again later.",
					Extensions: map[string]interface{}{
						"code":            "RATE_LIMIT_EXCEEDED",
						"availableTokens": bucketStats["availableTokens"],
						"retryAfter":      "60s",
					},
				}},
			}
		}
	}

	// Layer 4: Apply role-aware execution timeout
	if h.securityConfig.EnableQueryTimeout {
		// Determine timeout based on role and operation type
		var timeout time.Duration
		operationType := "query"

		for _, operation := range query.Operations {
			if operation.Operation == ast.Mutation {
				operationType = "mutation"
				break
			}
		}

		// Select timeout based on role
		if req.IsAdmin {
			// Admins get 10-minute timeout (enforced, not unlimited)
			timeout = h.securityConfig.AdminTimeout
		} else if req.UserRole == "anonymous" {
			if operationType == "mutation" {
				timeout = h.securityConfig.AnonymousMutationTimeout
			} else {
				timeout = h.securityConfig.AnonymousQueryTimeout
			}
		} else {
			// Authenticated users
			if operationType == "mutation" {
				timeout = h.securityConfig.AuthenticatedMutationTimeout
			} else {
				timeout = h.securityConfig.AuthenticatedQueryTimeout
			}
		}

		// Create timeout context
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()

		// Log warning at threshold (skip for admins to reduce noise)
		if !req.IsAdmin {
			go func() {
				warningTime := time.Duration(float64(timeout) * h.securityConfig.TimeoutWarningThreshold)
				select {
				case <-time.After(warningTime):
					h.logger.Warnw("GraphQL query approaching timeout",
						"username", req.Username,
						"clientIP", req.ClientIP,
						"role", req.UserRole,
						"timeout", timeout,
						"elapsed", warningTime,
					)
				case <-ctx.Done():
					// Context completed before warning
					return
				}
			}()
		}
	}

	// Execute the query with timeout context
	data, err := h.executeQuery(ctx, query, req.Variables)
	if err != nil {
		// Check if error is due to timeout
		if ctx.Err() == context.DeadlineExceeded {
			metric.ErrorCode = "TIMEOUT_EXCEEDED"
			metric.ErrorMessage = "Query execution timed out"
			h.logger.Warnw("GraphQL query timed out",
				"username", req.Username,
				"clientIP", req.ClientIP,
				"role", req.UserRole,
			)
			return GraphQLResponse{
				Errors: []GraphQLError{{
					Message: "Query execution timed out",
					Extensions: map[string]interface{}{
						"code": "TIMEOUT_EXCEEDED",
					},
				}},
			}
		}

		metric.ErrorCode = "EXECUTION_ERROR"
		metric.ErrorMessage = err.Error()
		return GraphQLResponse{
			Errors: []GraphQLError{{
				Message: fmt.Sprintf("Execution error: %v", err),
			}},
		}
	}

	// Mark as successful
	metric.Success = true

	return GraphQLResponse{Data: data}
} // isDDLMutation checks if a mutation involves DDL operations (CREATE/ALTER/DROP/GRANT/REVOKE)
func (h *GraphQLHandler) isDDLMutation(operation *ast.OperationDefinition) bool {
	if operation.Operation != ast.Mutation {
		return false
	}

	// Check if any mutation field is a DDL operation
	for _, selection := range operation.SelectionSet {
		field, ok := selection.(*ast.Field)
		if !ok {
			continue
		}

		mutationName := strings.ToLower(field.Name)

		// DDL operations: CREATE/ALTER/DROP for databases/bundles
		if strings.Contains(mutationName, "createdatabase") ||
			strings.Contains(mutationName, "alterdatabase") ||
			strings.Contains(mutationName, "dropdatabase") ||
			strings.Contains(mutationName, "createbundle") ||
			strings.Contains(mutationName, "alterbundle") ||
			strings.Contains(mutationName, "dropbundle") ||
			strings.Contains(mutationName, "grant") ||
			strings.Contains(mutationName, "revoke") {
			return true
		}
	}

	return false
}

// executeQuery executes a validated GraphQL query
//
// PHASE 10 UPDATE: Added ctx parameter for DataLoader support
func (h *GraphQLHandler) executeQuery(ctx context.Context, query *ast.QueryDocument, variables map[string]interface{}) (interface{}, error) {
	// For now, we'll implement a simple resolver that translates GraphQL to SyndrDB commands
	for _, operation := range query.Operations {
		switch operation.Operation {
		case ast.Query:
			return h.executeQueryOperation(ctx, operation, variables)
		case ast.Mutation:
			return h.executeMutationOperation(operation, variables)
		default:
			return nil, fmt.Errorf("unsupported operation type: %s", operation.Operation)
		}
	}

	return nil, fmt.Errorf("no operations found in query")
}

// executeQueryOperation executes a GraphQL query operation
//
// PHASE 6: This method handles both regular queries and introspection queries.
// Introspection queries (__schema, __type) are routed to dedicated handlers
// that return schema metadata from the Phase 5 SchemaManager.
//
// PHASE 10 UPDATE: Added ctx parameter for DataLoader support
func (h *GraphQLHandler) executeQueryOperation(ctx context.Context, operation *ast.OperationDefinition, variables map[string]interface{}) (interface{}, error) {
	result := make(map[string]interface{})

	for _, selection := range operation.SelectionSet {
		field, ok := selection.(*ast.Field)
		if !ok {
			continue
		}

		// PHASE 6 INTROSPECTION: Handle GraphQL introspection queries
		// These allow tools like GraphiQL to explore the schema structure
		switch field.Name {
		case "__schema":
			// Return full schema metadata including types, queries, mutations
			data, err := h.resolveSchemaIntrospection(field, variables)
			if err != nil {
				return nil, err
			}
			result[field.Alias] = data

		case "__type":
			// Return metadata for a specific type by name
			data, err := h.resolveTypeIntrospection(field, variables)
			if err != nil {
				return nil, err
			}
			result[field.Alias] = data

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
			// PHASE 6 NATIVE QUERY: Check if field name matches a bundle
			// If yes, treat as native GraphQL query and parse to UnifiedSelectQuery
			if bundle, exists := h.database.Bundles[field.Name]; exists {
				h.logger.Debugf("[GraphQL Native Query] Executing query for bundle: %s", field.Name)

				// Use native GraphQL parser to create UnifiedSelectQuery
				data, err := h.executeNativeBundleQuery(ctx, field, variables, &bundle)
				if err != nil {
					return nil, fmt.Errorf("failed to execute native bundle query for '%s': %w", field.Name, err)
				}
				result[field.Alias] = data
			} else {
				return nil, fmt.Errorf("unknown query field: %s", field.Name)
			}
		}
	}

	return result, nil
}

// executeNativeBundleQuery executes a GraphQL bundle query using the native parser
//
// PHASE 6 NATIVE LANGUAGE: This method implements native GraphQL query execution.
// It converts GraphQL queries into UnifiedSelectQuery objects (same as SyndrQL parser)
// and executes them through the UnifiedQueryPlanner for maximum performance.
//
// PHASE 9 ENHANCEMENT: Now supports Relay-style cursor pagination and structured filtering.
// If pagination arguments (first, after, last, before) are detected, results are wrapped
// in a Connection object with edges, pageInfo, and cursors. If structured filtering is
// detected (WhereInput), it's translated to SyndrDB query format via FilterTranslator.
//
// Architecture:
//
//	GraphQL Query → GraphQL Parser → UnifiedSelectQuery → UnifiedQueryPlanner → Results
//	                                                                              ↓
//	                                   [Phase 9] FilterTranslator + Pagination.CreateConnection
//
// This ensures GraphQL has identical performance to SyndrQL queries since they use
// the exact same execution path after parsing.
//
// Parameters:
//   - field: The GraphQL field representing the bundle query
//   - variables: GraphQL query variables
//   - bundle: The bundle to query against
//
// Returns:
//   - interface{}: Query results formatted as GraphQL response (array or Connection)
//   - error: Parsing or execution errors
//
// PHASE 10 UPDATE: Added ctx parameter for DataLoader support
func (h *GraphQLHandler) executeNativeBundleQuery(ctx context.Context, field *ast.Field, variables map[string]interface{}, bundle *models.Bundle) (interface{}, error) {
	// PHASE 9: Check if this query uses Relay-style pagination or structured filtering
	usePagination := h.hasPaginationArgs(field)
	useStructuredFilter := h.hasStructuredFilterArgs(field)

	h.logger.Debugf("[GraphQL Native Query] Executing query: bundle=%s, usePagination=%v, useStructuredFilter=%v",
		field.Name, usePagination, useStructuredFilter)

	// PHASE 9: If structured filtering detected, pre-process arguments
	// TODO: I will move this into the parser when refactoring the query pipeline
	if useStructuredFilter {
		return h.executeQueryWithStructuredFiltering(ctx, field, variables, bundle)
	}

	// PHASE 9: If pagination detected, use Connection-based response
	if usePagination {
		return h.executeQueryWithPagination(ctx, field, variables, bundle)
	}

	// Legacy execution path (no pagination, no structured filtering)
	return h.executeLegacyBundleQuery(ctx, field, variables, bundle)
}

// executeLegacyBundleQuery executes a GraphQL query using the original Phase 6 logic
//
// This maintains backward compatibility for queries that don't use pagination or
// structured filtering. Results are returned as a simple array of documents.
//
// PHASE 10 UPDATE: Added ctx parameter for DataLoader support
func (h *GraphQLHandler) executeLegacyBundleQuery(ctx context.Context, field *ast.Field, variables map[string]interface{}, bundle *models.Bundle) (interface{}, error) {
	// Create GraphQL parser instance
	gqlParser := graphqlparser.NewGraphQLParser(h.schemaManager, h.database, h.logger)

	// Parse GraphQL query into UnifiedSelectQuery
	// This is where GraphQL becomes a native language - it produces the same
	// query structure as SyndrQL, ensuring identical execution performance
	unifiedQuery, err := gqlParser.ParseGraphQLQuery(field, variables)
	if err != nil {
		return nil, fmt.Errorf("failed to parse GraphQL query: %w", err)
	}

	h.logger.Debugf("[GraphQL Native Query] Parsed to UnifiedSelectQuery: bundle=%s, fields=%d",
		unifiedQuery.FromBundle, len(unifiedQuery.SelectFields))

	// Execute query through UnifiedQueryPlanner
	// This is the SAME execution path that SyndrQL queries use
	results, err := h.executeUnifiedQuery(unifiedQuery, bundle)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}

	// Format results as GraphQL response (pass context for DataLoader)
	return h.formatGraphQLResults(ctx, results, field), nil
}

// hasPaginationArgs checks if the query uses Relay-style pagination arguments
func (h *GraphQLHandler) hasPaginationArgs(field *ast.Field) bool {
	for _, arg := range field.Arguments {
		if arg.Name == "first" || arg.Name == "after" || arg.Name == "last" || arg.Name == "before" {
			return true
		}
	}
	return false
}

// hasStructuredFilterArgs checks if the query uses structured filtering (WhereInput)
func (h *GraphQLHandler) hasStructuredFilterArgs(field *ast.Field) bool {
	for _, arg := range field.Arguments {
		if arg.Name == "where" {
			// Check if where argument is a map (structured) vs string (legacy)
			if arg.Value.Kind == ast.ObjectValue {
				return true
			}
		}
		if arg.Name == "orderBy" {
			// Check if orderBy is array/object (structured) vs string (legacy)
			if arg.Value.Kind == ast.ListValue || arg.Value.Kind == ast.ObjectValue {
				return true
			}
		}
	}
	return false
}

// executeQueryWithPagination executes a query with Relay-style cursor pagination
//
// PHASE 9 IMPLEMENTATION: This method implements Relay Cursor Connections specification.
// It wraps query results in a Connection object with edges, pageInfo, and cursors for
// stable pagination across result sets.
//
// Pagination Arguments:
//   - first: Int - Number of items to return (forward pagination)
//   - after: String - Cursor to start after (forward pagination)
//   - last: Int - Number of items to return (backward pagination)
//   - before: String - Cursor to start before (backward pagination)
//
// Response Structure:
//
//	{
//	  edges: [{ node: Document, cursor: String }],
//	  pageInfo: {
//	    hasNextPage: Boolean,
//	    hasPreviousPage: Boolean,
//	    startCursor: String,
//	    endCursor: String
//	  },
//	  totalCount: Int
//	}
//
// TODO: I will add connection-level aggregates (totalCount with WHERE applied) when
// implementing query optimization in Phase 10.
//
// PHASE 10 UPDATE: Added ctx parameter for DataLoader support
func (h *GraphQLHandler) executeQueryWithPagination(ctx context.Context, field *ast.Field, variables map[string]interface{}, bundle *models.Bundle) (interface{}, error) {
	h.logger.Debugf("[GraphQL Pagination] Executing paginated query for bundle: %s", field.Name)

	// Parse pagination arguments
	paginationArgs := h.parsePaginationArgs(field, variables)

	// Validate pagination arguments (can't use first+last together, etc.)
	// Use a reasonable default max page size (1000)
	if err := ValidatePaginationArgs(paginationArgs, 1000); err != nil {
		return nil, fmt.Errorf("invalid pagination arguments: %w", err)
	}

	// Execute the underlying query (similar to legacy path)
	gqlParser := graphqlparser.NewGraphQLParser(h.schemaManager, h.database, h.logger)
	unifiedQuery, err := gqlParser.ParseGraphQLQuery(field, variables)
	if err != nil {
		return nil, fmt.Errorf("failed to parse GraphQL query: %w", err)
	}

	// Execute query to get raw results
	results, err := h.executeUnifiedQuery(unifiedQuery, bundle)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}

	// Convert []map[string]interface{} to []interface{}
	items := make([]interface{}, len(results))
	for i, r := range results {
		items[i] = r
	}

	// Apply pagination and create Connection response
	// Define a function to extract ID from document
	getID := func(item interface{}) string {
		if doc, ok := item.(map[string]interface{}); ok {
			if id, ok := doc["DocumentID"].(string); ok {
				return id
			}
			// Try "id" field as fallback
			if id, ok := doc["id"].(string); ok {
				return id
			}
		}
		return ""
	}

	connection, err := CreateConnection(items, paginationArgs, getID)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection: %w", err)
	}

	h.logger.Debugf("[GraphQL Pagination] Created connection: edges=%d, hasNextPage=%v, hasPreviousPage=%v",
		len(connection.Edges), connection.PageInfo.HasNextPage, connection.PageInfo.HasPreviousPage)

	return connection, nil
}

// parsePaginationArgs extracts pagination arguments from GraphQL field
func (h *GraphQLHandler) parsePaginationArgs(field *ast.Field, variables map[string]interface{}) PaginationArgs {
	args := PaginationArgs{}

	for _, arg := range field.Arguments {
		value := h.resolveArgumentValue(arg.Value, variables)

		switch arg.Name {
		case "first":
			if intVal, ok := h.parseIntValue(value); ok {
				args.First = &intVal
			}
		case "after":
			if strVal, ok := value.(string); ok {
				args.After = &strVal
			}
		case "last":
			if intVal, ok := h.parseIntValue(value); ok {
				args.Last = &intVal
			}
		case "before":
			if strVal, ok := value.(string); ok {
				args.Before = &strVal
			}
		}
	}

	return args
}

// parseIntValue converts various int representations to int
func (h *GraphQLHandler) parseIntValue(value interface{}) (int, bool) {
	switch v := value.(type) {
	case int:
		return v, true
	case int64:
		return int(v), true
	case float64:
		return int(v), true
	case string:
		// Try to parse string as int
		var i int
		if _, err := fmt.Sscanf(v, "%d", &i); err == nil {
			return i, true
		}
	}
	return 0, false
}

// executeQueryWithStructuredFiltering executes a query with structured WhereInput and OrderByInput
//
// PHASE 9 IMPLEMENTATION: This method translates structured GraphQL filter inputs into
// SyndrDB WhereGroup structures using the FilterTranslator. This enables type-safe filtering
// with operators like eq, ne, gt, lt, in, like, and logical operators AND/OR/NOT.
//
// Filtering Example:
//
//	users(
//	  where: {
//	    AND: [
//	      { status: { eq: "active" } },
//	      { age: { gt: 18 } }
//	    ]
//	  },
//	  orderBy: [
//	    { field: "name", direction: ASC },
//	    { field: "createdAt", direction: DESC }
//	  ]
//	)
//
// TODO: I will optimize filter translation by caching WhereGroup structures for
// common filter patterns when implementing query optimization in Phase 10.
//
// PHASE 10 UPDATE: Added ctx parameter for DataLoader support
func (h *GraphQLHandler) executeQueryWithStructuredFiltering(ctx context.Context, field *ast.Field, variables map[string]interface{}, bundle *models.Bundle) (interface{}, error) {
	h.logger.Debugf("[GraphQL Filtering] Executing query with structured filtering for bundle: %s", field.Name)

	// Parse structured filter arguments
	whereInput, orderByInput, err := h.parseStructuredFilterArgs(field, variables)
	if err != nil {
		return nil, fmt.Errorf("failed to parse structured filter arguments: %w", err)
	}

	// Translate WhereInput to SyndrDB WhereGroup
	translator := NewFilterTranslator()
	whereGroup, err := translator.TranslateWhereInput(whereInput)
	if err != nil {
		return nil, fmt.Errorf("failed to translate where input: %w", err)
	}

	// Translate OrderByInput to ORDER BY clause
	orderByStr := ""
	if len(orderByInput) > 0 {
		orderByStr = translator.TranslateOrderBy(orderByInput)
	}

	h.logger.Debugf("[GraphQL Filtering] Translated filters: whereGroup=%v, orderBy=%s", whereGroup != nil, orderByStr)

	// Execute query with translated filters
	// TODO: I will refactor this to use the parser's extractQueryArguments when
	// adding full FilterTranslator integration to the parser in Phase 10
	gqlParser := graphqlparser.NewGraphQLParser(h.schemaManager, h.database, h.logger)
	unifiedQuery, err := gqlParser.ParseGraphQLQuery(field, variables)
	if err != nil {
		return nil, fmt.Errorf("failed to parse GraphQL query: %w", err)
	}

	// Override WHERE and ORDER BY with translated values
	if whereGroup != nil {
		// Convert models.WhereGroup to queryparser.WhereGroup
		unifiedQuery.WhereExpression = h.convertWhereGroup(whereGroup)
	}
	// TODO: Parse orderByStr back to OrderByClause when refactoring query pipeline

	// Execute query
	results, err := h.executeUnifiedQuery(unifiedQuery, bundle)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}

	return h.formatGraphQLResults(ctx, results, field), nil
}

// convertWhereGroup converts models.WhereGroup to queryparser.WhereGroup
// TODO: I will unify these types when refactoring the models package in Phase 10
func (h *GraphQLHandler) convertWhereGroup(src *models.WhereGroup) *queryparser.WhereGroup {
	if src == nil {
		return nil
	}

	dst := &queryparser.WhereGroup{
		Operator: src.Logic, // models.WhereGroup.Logic → queryparser.WhereGroup.Operator
	}

	// Convert clauses
	for _, clause := range src.Clauses {
		dst.Clauses = append(dst.Clauses, queryparser.WhereClause{
			Field:    clause.Field,
			Operator: clause.Operator,
			Value:    clause.Value,
			Logic:    clause.Logic,
		})
	}

	// Convert subgroups recursively
	for _, subgroup := range src.SubGroups {
		converted := h.convertWhereGroup(&subgroup)
		if converted != nil {
			dst.SubGroups = append(dst.SubGroups, *converted)
		}
	}

	return dst
}

// parseStructuredFilterArgs extracts WhereInput and OrderByInput from GraphQL arguments
func (h *GraphQLHandler) parseStructuredFilterArgs(field *ast.Field, variables map[string]interface{}) (*WhereInput, []OrderByInput, error) {
	var whereInput *WhereInput
	var orderByInput []OrderByInput

	for _, arg := range field.Arguments {
		// Resolve argument value (handles variables)
		value := h.resolveArgumentValue(arg.Value, variables)

		switch arg.Name {
		case "where":
			if whereMap, ok := value.(map[string]interface{}); ok {
				parsed, err := ParseWhereInput(whereMap)
				if err != nil {
					return nil, nil, fmt.Errorf("failed to parse where input: %w", err)
				}
				whereInput = parsed
			}

		case "orderBy":
			parsed, err := ParseOrderByInput(value)
			if err != nil {
				return nil, nil, fmt.Errorf("failed to parse orderBy input: %w", err)
			}
			orderByInput = parsed
		}
	}

	return whereInput, orderByInput, nil
}

// resolveArgumentValue resolves a GraphQL argument value (handles variables)
func (h *GraphQLHandler) resolveArgumentValue(value *ast.Value, variables map[string]interface{}) interface{} {
	if value == nil {
		return nil
	}

	switch value.Kind {
	case ast.Variable:
		// Resolve variable from variables map
		varName := value.Raw
		if val, ok := variables[varName]; ok {
			return val
		}
		return nil

	case ast.IntValue:
		// TODO: Handle int conversion more robustly
		return value.Raw

	case ast.FloatValue:
		return value.Raw

	case ast.StringValue:
		return value.Raw

	case ast.BooleanValue:
		return value.Raw == "true"

	case ast.NullValue:
		return nil

	case ast.ListValue:
		// Convert list of values
		var list []interface{}
		for _, child := range value.Children {
			list = append(list, h.resolveArgumentValue(child.Value, variables))
		}
		return list

	case ast.ObjectValue:
		// Convert object value
		obj := make(map[string]interface{})
		for _, child := range value.Children {
			obj[child.Name] = h.resolveArgumentValue(child.Value, variables)
		}
		return obj

	default:
		return value.Raw
	}
}

// executeUnifiedQuery executes a UnifiedSelectQuery through the query planner
//
// PHASE 6 INTEGRATION: This method integrates the GraphQL parser with the existing
// UnifiedQueryPlanner. Both GraphQL and SyndrQL queries execute through this same
// path, ensuring consistent behavior and performance.
//
// This is the EXACT same execution flow used by SyndrQL queries:
// 1. Create UnifiedQueryPlanner with BundleService
// 2. Create execution plan with cost estimation and index selection
// 3. Execute the plan's root node to retrieve documents
//
// Parameters:
//   - query: The UnifiedSelectQuery from the GraphQL parser
//   - bundle: The bundle to query against (unused, kept for future use)
//
// Returns:
//   - []map[string]interface{}: Query results as document maps
//   - error: Execution errors
func (h *GraphQLHandler) executeUnifiedQuery(query *queryparser.UnifiedSelectQuery, bundle *models.Bundle) ([]map[string]interface{}, error) {
	// Step 1: Use unified query planner from ServiceManager (with plan caching)
	// This is the SAME planner used by SyndrQL queries
	queryPlanner := h.serviceManager.UnifiedPlanner

	// Step 2: Create execution plan
	// The planner analyzes the query, estimates costs, selects indexes,
	// and builds an optimized execution tree
	plan, err := queryPlanner.CreatePlan(query, h.database)
	if err != nil {
		return nil, fmt.Errorf("failed to create execution plan: %w", err)
	}

	h.logger.Debugf("[GraphQL Native Query] Execution plan created: Cost=%.2f, EstimatedRows=%d, IndexesUsed=%v",
		plan.Cost, plan.EstimatedRows, plan.IndexesUsed)

	// Step 3: Execute the plan
	// This executes the root node of the execution tree, which recursively
	// executes all child nodes (scans, filters, joins, sorts, limits, etc.)
	documents, err := plan.RootNode.Execute(context.Background())
	if err != nil {
		return nil, fmt.Errorf("query execution failed: %w", err)
	}

	h.logger.Debugf("[GraphQL Native Query] Executed successfully, returned %d documents", len(documents))

	// Step 4: Transform documents to flattened format
	// This converts map[string]*models.Document to []map[string]interface{}
	// IMPORTANT: If query has ORDER BY + LIMIT, we must preserve sort order!
	// Check if RootNode is a LimitNode with sorted documents
	var flattenedDocs []map[string]interface{}
	if limitNode, isLimitNode := plan.RootNode.(*planner.LimitNode); isLimitNode {
		sortedDocs := limitNode.GetSortedDocuments()
		if sortedDocs != nil {
			// Use slice-based transformation to preserve ORDER BY sort order
			h.logger.Debugf("[GraphQL Native Query] Preserving ORDER BY sort order (%d sorted documents)", len(sortedDocs))
			flattenedDocs = helpers.TransformDocumentSliceToFlatFormat(sortedDocs, query.SelectFields)
		} else {
			// No sorted documents, use map-based transformation (sorts by DocumentID)
			flattenedDocs = helpers.TransformDocumentsToFlatFormatWithProjection(documents, query.SelectFields)
		}
	} else {
		// Not a LimitNode, use map-based transformation
		flattenedDocs = helpers.TransformDocumentsToFlatFormatWithProjection(documents, query.SelectFields)
	}

	return flattenedDocs, nil
}

// formatGraphQLResults formats query results according to GraphQL field selections
//
// PHASE 6: This method formats the raw document results from the query planner
// into the structure expected by GraphQL clients. It handles:
// - Field selection filtering (only return requested fields)
// - Field aliasing (GraphQL aliases in the query)
// - Nested field selections (for future expansion)
//
// PHASE 8 ENHANCEMENT: Now resolves relationship fields automatically.
// When a requested field is a relationship (defined in bundle.Relationships),
// the RelationshipResolver is invoked to fetch related documents.
//
// Parameters:
//   - results: Raw document results from query planner
//   - field: The GraphQL field with selection set
//
// Returns:
//   - interface{}: Formatted results ready for GraphQL response
//
// PHASE 10 UPDATE: Added ctx parameter for DataLoader support
func (h *GraphQLHandler) formatGraphQLResults(ctx context.Context, results []map[string]interface{}, field *ast.Field) interface{} {
	// Extract field names and ast.Field objects from selection set
	var requestedFields []string
	fieldMap := make(map[string]*ast.Field)
	if field.SelectionSet != nil {
		for _, selection := range field.SelectionSet {
			if fieldSel, ok := selection.(*ast.Field); ok {
				requestedFields = append(requestedFields, fieldSel.Name)
				fieldMap[fieldSel.Name] = fieldSel
			}
		}
	}

	// If no specific fields requested, return all fields
	if len(requestedFields) == 0 {
		return results
	}

	// Get bundle for relationship resolution
	bundle, exists := h.database.Bundles[field.Name]
	if !exists {
		h.logger.Warnf("[GraphQL Format] Bundle '%s' not found for relationship resolution", field.Name)
		return h.formatResultsWithoutRelationships(results, requestedFields)
	}

	// PHASE 8: Create relationship resolver for this bundle
	relationshipResolver := NewRelationshipResolver(h.serviceManager, h.logger)

	// Filter and resolve results
	filteredResults := make([]map[string]interface{}, len(results))
	for i, doc := range results {
		filteredDoc := make(map[string]interface{})

		for _, fieldName := range requestedFields {
			// Check if this is a relationship field
			isRelationship := false
			if _, relExists := bundle.Relationships[fieldName]; relExists {
				isRelationship = true
			}

			if isRelationship {
				// PHASE 8: Resolve relationship field
				h.logger.Debugf("[GraphQL Relationship] Resolving relationship '%s' for document", fieldName)

				// STEP 1: Use document pool to reduce allocations
				// TODO: Option C - Implement reference counting for automatic pool return
				// Convert map[string]interface{} to *models.Document
				// Use the Data field for raw document data
				parentDoc := document.GetPooledDocument()
				parentDoc.Fields = make(map[string]models.Field)
				parentDoc.Data = make(map[string]interface{})

				// Copy DocumentID
				if docID, ok := doc["DocumentID"].(string); ok {
					parentDoc.DocumentID = docID
				} else if docID, ok := doc["id"].(string); ok {
					parentDoc.DocumentID = docID
				}

				// Copy all fields to Data
				for k, v := range doc {
					parentDoc.Data[k] = v
				}

				relatedData, isRel, err := relationshipResolver.ResolveRelationship(
					ctx,
					&bundle,
					parentDoc,
					fieldName,
					h.database,
				)

				if err != nil {
					h.logger.Errorf("[GraphQL Relationship] Failed to resolve relationship '%s': %v", fieldName, err)
					filteredDoc[fieldName] = nil
				} else if isRel {
					// Successfully resolved relationship
					filteredDoc[fieldName] = relatedData

					// TODO: I will add nested field selection filtering when implementing
					// deep relationship traversal in Phase 10. For now, related documents
					// return all fields.
					if fieldAst, ok := fieldMap[fieldName]; ok && fieldAst.SelectionSet != nil {
						h.logger.Debugf("[GraphQL Relationship] TODO: Apply nested field selection for '%s'", fieldName)
					}
				} else {
					// Not a relationship or couldn't resolve
					filteredDoc[fieldName] = nil
				}
			} else {
				// Regular field (not a relationship)
				if value, exists := doc[fieldName]; exists {
					filteredDoc[fieldName] = value
				} else if fieldName == "id" && doc["DocumentID"] != nil {
					// Common mapping: "id" → "DocumentID"
					filteredDoc[fieldName] = doc["DocumentID"]
				} else {
					// Field not found - include as null for GraphQL compliance
					filteredDoc[fieldName] = nil
				}
			}
		}

		filteredResults[i] = filteredDoc
	}

	return filteredResults
}

// formatResultsWithoutRelationships is a fallback formatter when relationship resolution is unavailable
func (h *GraphQLHandler) formatResultsWithoutRelationships(results []map[string]interface{}, requestedFields []string) interface{} {
	filteredResults := make([]map[string]interface{}, len(results))
	for i, doc := range results {
		filteredDoc := make(map[string]interface{})
		for _, fieldName := range requestedFields {
			if value, exists := doc[fieldName]; exists {
				filteredDoc[fieldName] = value
			} else if fieldName == "id" && doc["DocumentID"] != nil {
				filteredDoc[fieldName] = doc["DocumentID"]
			} else {
				filteredDoc[fieldName] = nil
			}
		}
		filteredResults[i] = filteredDoc
	}
	return filteredResults
}

// executeMutationOperation executes a GraphQL mutation operation
//
// PHASE 11 IMPLEMENTATION: Complete mutation support with dedicated modules.
// This method now uses the mutation system to handle CRUD operations:
// - MutationParser: Parses GraphQL mutations into DocumentCommand structs
// - InputValidator: Validates mutation inputs at GraphQL layer
// - MutationExecutor: Executes mutations via BundleService (same path as SyndrQL)
// - MutationResolver: Resolves mutation response fields
//
// Supported mutation patterns:
// - create<Bundle>(input: Create<Bundle>Input!): <Bundle>!
// - update<Bundle>(id: ID!, input: Update<Bundle>Input!): <Bundle>!
// - delete<Bundle>(id: ID!): Delete<Bundle>Payload!
//
// All mutations follow the same execution path as SyndrQL commands for consistency.
func (h *GraphQLHandler) executeMutationOperation(operation *ast.OperationDefinition, variables map[string]interface{}) (interface{}, error) {
	result := make(map[string]interface{})

	// TODO: I will add transaction support here when SyndrDB implements full ACID transactions.
	// Multiple mutations in one operation would be wrapped in a transaction:
	// tx := transactionManager.Begin()
	// Execute all mutations within transaction
	// tx.Commit() on success, tx.Rollback() on any failure

	for _, selection := range operation.SelectionSet {
		field, ok := selection.(*ast.Field)
		if !ok {
			continue
		}

		mutationName := field.Name
		h.logger.Debugf("Executing mutation: %s", mutationName)

		// Extract bundle name from mutation name (e.g., createUser → User)
		bundleName, err := h.mutationParser.ExtractBundleNameFromMutation(mutationName)
		if err != nil {
			return nil, fmt.Errorf("invalid mutation name '%s': %w", mutationName, err)
		}

		// Route to appropriate mutation handler based on operation type
		var data interface{}

		if strings.HasPrefix(mutationName, "create") {
			data, err = h.executeCreateMutation(field, bundleName, variables)
		} else if strings.HasPrefix(mutationName, "update") {
			data, err = h.executeUpdateMutation(field, bundleName, variables)
		} else if strings.HasPrefix(mutationName, "delete") {
			data, err = h.executeDeleteMutation(field, bundleName, variables)
		} else {
			// TODO: I will add custom mutation support here when implementing mutation registry.
			// Custom mutations would be looked up in a registry and dispatched to handlers:
			// if handler, ok := mutationRegistry.Get(mutationName); ok {
			//     data, err = handler(field, variables, h.serviceManager)
			// }
			err = fmt.Errorf("unknown mutation pattern: %s", mutationName)
		}

		if err != nil {
			return nil, fmt.Errorf("mutation '%s' failed: %w", mutationName, err)
		}

		// Use field alias if provided, otherwise use field name
		resultKey := field.Alias
		if resultKey == "" {
			resultKey = field.Name
		}
		result[resultKey] = data
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

// ========================================
// PHASE 6 INTROSPECTION SUPPORT
// ========================================
//
// The following methods implement GraphQL introspection queries that allow
// tools like GraphiQL, GraphQL Playground, and IDE extensions to explore
// the schema structure. These queries are part of the GraphQL specification
// and are essential for developer tooling.
//
// Supported Introspection Queries:
// - __schema: Returns full schema metadata (types, queries, mutations)
// - __type(name): Returns metadata for a specific type
//
// The introspection system integrates with Phase 5's SchemaManager to
// return live schema information from bundle definitions.

// resolveSchemaIntrospection handles the __schema introspection query
//
// This query returns comprehensive schema metadata including:
// - All types defined in the schema (bundle types + scalars)
// - Query type with all available query fields
// - Mutation type (currently empty, marked with TODO for future)
// - Directives (standard GraphQL directives)
//
// Example Query:
//
//	{
//	  __schema {
//	    types { name kind }
//	    queryType { name fields { name } }
//	  }
//	}
//
// Parameters:
//   - field: The __schema field with selected subfields
//   - variables: Query variables (typically unused for introspection)
//
// Returns:
//   - interface{}: Schema metadata structure
//   - error: Introspection errors
func (h *GraphQLHandler) resolveSchemaIntrospection(field *ast.Field, variables map[string]interface{}) (interface{}, error) {
	h.logger.Debug("Resolving __schema introspection query")

	// Build schema metadata structure
	schemaMetadata := map[string]interface{}{
		"queryType": map[string]interface{}{
			"name": "Query",
		},
		"mutationType": nil, // TODO: I will add mutation type when implementing mutations
		"types":        h.buildIntrospectionTypes(),
		"directives":   h.buildIntrospectionDirectives(),
	}

	return schemaMetadata, nil
}

// resolveTypeIntrospection handles the __type(name:) introspection query
//
// This query returns metadata for a specific type including:
// - Type name and kind (OBJECT, SCALAR, etc.)
// - Fields with their names and types
// - Description and deprecation status
//
// Example Query:
//
//	{
//	  __type(name: "User") {
//	    name
//	    kind
//	    fields { name type { name kind } }
//	  }
//	}
//
// Parameters:
//   - field: The __type field with name argument and selected subfields
//   - variables: Query variables (may contain type name)
//
// Returns:
//   - interface{}: Type metadata structure or nil if type not found
//   - error: Introspection errors
func (h *GraphQLHandler) resolveTypeIntrospection(field *ast.Field, variables map[string]interface{}) (interface{}, error) {
	// Get the type name argument
	nameArg, ok := h.getArgument(field, "name", variables)
	if !ok {
		return nil, fmt.Errorf("__type requires 'name' argument")
	}

	typeName, ok := nameArg.(string)
	if !ok {
		return nil, fmt.Errorf("type name must be a string")
	}

	h.logger.Debugf("Resolving __type introspection query for type: %s", typeName)

	// Check if this is a scalar type
	if h.isScalarType(typeName) {
		return map[string]interface{}{
			"name": typeName,
			"kind": "SCALAR",
		}, nil
	}

	// Check if this is the Query type
	if typeName == "Query" {
		return h.buildQueryTypeIntrospection(), nil
	}

	// Look for bundle type in SchemaManager
	if h.schemaManager != nil {
		// Search all bundles for matching type name
		for bundleName := range h.database.Bundles {
			bundleSchema, err := h.schemaManager.GetActiveSchemaForBundle(bundleName)
			if err != nil {
				continue
			}

			if bundleSchema != nil && bundleSchema.Payload != nil && bundleSchema.Payload.TypeName == typeName {
				return h.buildBundleTypeIntrospection(bundleSchema.Payload), nil
			}
		}
	}

	// Type not found
	return nil, nil
}

// buildIntrospectionTypes builds the list of all types in the schema
func (h *GraphQLHandler) buildIntrospectionTypes() []interface{} {
	types := []interface{}{
		// Standard GraphQL scalars
		map[string]interface{}{"name": "String", "kind": "SCALAR"},
		map[string]interface{}{"name": "Int", "kind": "SCALAR"},
		map[string]interface{}{"name": "Float", "kind": "SCALAR"},
		map[string]interface{}{"name": "Boolean", "kind": "SCALAR"},
		map[string]interface{}{"name": "ID", "kind": "SCALAR"},
		map[string]interface{}{"name": "DateTime", "kind": "SCALAR"},
		map[string]interface{}{"name": "Date", "kind": "SCALAR"},

		// Query type
		h.buildQueryTypeIntrospection(),
	}

	// Add bundle types from SchemaManager
	if h.schemaManager != nil {
		for bundleName := range h.database.Bundles {
			bundleSchema, err := h.schemaManager.GetActiveSchemaForBundle(bundleName)
			if err != nil {
				continue
			}

			if bundleSchema != nil && bundleSchema.Payload != nil {
				types = append(types, h.buildBundleTypeIntrospection(bundleSchema.Payload))
			}
		}
	}

	return types
}

// buildQueryTypeIntrospection builds introspection data for the Query type
func (h *GraphQLHandler) buildQueryTypeIntrospection() map[string]interface{} {
	fields := []interface{}{}

	// Add fields for each bundle
	for bundleName := range h.database.Bundles {
		var typeName string
		if h.schemaManager != nil {
			bundleSchema, err := h.schemaManager.GetActiveSchemaForBundle(bundleName)
			if err == nil && bundleSchema != nil && bundleSchema.Payload != nil {
				typeName = bundleSchema.Payload.TypeName
			}
		}
		if typeName == "" {
			typeName = "Document" // Fallback
		}

		fields = append(fields, map[string]interface{}{
			"name": bundleName,
			"type": map[string]interface{}{
				"kind":   "LIST",
				"ofType": map[string]interface{}{"name": typeName, "kind": "OBJECT"},
			},
		})
	}

	return map[string]interface{}{
		"name":   "Query",
		"kind":   "OBJECT",
		"fields": fields,
	}
}

// buildBundleTypeIntrospection builds introspection data for a bundle type
func (h *GraphQLHandler) buildBundleTypeIntrospection(schemaDef *schema.GraphQLSchemaDefinition) map[string]interface{} {
	fields := []interface{}{}

	for _, field := range schemaDef.Fields {
		fields = append(fields, map[string]interface{}{
			"name": field.Name,
			"type": h.parseTypeForIntrospection(field.Type),
		})
	}

	return map[string]interface{}{
		"name":   schemaDef.TypeName,
		"kind":   "OBJECT",
		"fields": fields,
	}
}

// parseTypeForIntrospection parses a GraphQL type string for introspection
// Handles: String, String!, [String], [String!]!, etc.
func (h *GraphQLHandler) parseTypeForIntrospection(typeStr string) map[string]interface{} {
	// Check for non-null modifier (!)
	isNonNull := strings.HasSuffix(typeStr, "!")
	if isNonNull {
		typeStr = strings.TrimSuffix(typeStr, "!")
	}

	// Check for list type ([Type])
	isList := strings.HasPrefix(typeStr, "[") && strings.HasSuffix(typeStr, "]")
	if isList {
		typeStr = strings.TrimPrefix(typeStr, "[")
		typeStr = strings.TrimSuffix(typeStr, "]")

		// Recursive parse for list element type
		listType := map[string]interface{}{
			"kind":   "LIST",
			"ofType": h.parseTypeForIntrospection(typeStr),
		}

		if isNonNull {
			return map[string]interface{}{
				"kind":   "NON_NULL",
				"ofType": listType,
			}
		}
		return listType
	}

	// Base type (scalar or object)
	var kind string
	if h.isScalarType(typeStr) {
		kind = "SCALAR"
	} else {
		kind = "OBJECT"
	}

	baseType := map[string]interface{}{
		"name": typeStr,
		"kind": kind,
	}

	if isNonNull {
		return map[string]interface{}{
			"kind":   "NON_NULL",
			"ofType": baseType,
		}
	}
	return baseType
}

// isScalarType checks if a type name is a GraphQL scalar
func (h *GraphQLHandler) isScalarType(typeName string) bool {
	scalars := []string{"String", "Int", "Float", "Boolean", "ID", "DateTime", "Date"}
	for _, scalar := range scalars {
		if typeName == scalar {
			return true
		}
	}
	return false
}

// buildIntrospectionDirectives returns standard GraphQL directives
func (h *GraphQLHandler) buildIntrospectionDirectives() []interface{} {
	return []interface{}{
		map[string]interface{}{
			"name":        "skip",
			"description": "Directs the executor to skip this field or fragment when the `if` argument is true.",
			"locations":   []string{"FIELD", "FRAGMENT_SPREAD", "INLINE_FRAGMENT"},
		},
		map[string]interface{}{
			"name":        "include",
			"description": "Directs the executor to include this field or fragment only when the `if` argument is true.",
			"locations":   []string{"FIELD", "FRAGMENT_SPREAD", "INLINE_FRAGMENT"},
		},
	}
}

// ====================================================================================
// PHASE 11: MUTATION EXECUTION METHODS
// ====================================================================================
// The following methods implement the mutation execution pipeline:
// 1. Validate mutation inputs (GraphQL layer)
// 2. Parse mutation into DocumentCommand
// 3. Execute via MutationExecutor (calls BundleService)
// 4. Resolve response fields via MutationResolver
// ====================================================================================

// executeCreateMutation executes a create mutation (e.g., createUser).
//
// Flow:
//  1. Validate input argument structure
//  2. Parse mutation into DocumentCommand
//  3. Execute create via MutationExecutor
//  4. Fetch created document and resolve response fields
//
// This follows the EXACT same path as SyndrQL's ADD DOCUMENT command.
func (h *GraphQLHandler) executeCreateMutation(field *ast.Field, bundleName string, variables map[string]interface{}) (interface{}, error) {
	h.logger.Debugf("Executing create mutation for bundle '%s'", bundleName)

	// Step 1: Validate input at GraphQL layer
	if err := h.inputValidator.ValidateCreateMutation(field); err != nil {
		return nil, fmt.Errorf("validation failed: %w", err)
	}

	// Step 2: Parse mutation into DocumentCommand
	docCommand, err := h.mutationParser.ParseCreateMutation(field, bundleName, variables)
	if err != nil {
		return nil, fmt.Errorf("parsing failed: %w", err)
	}

	// Step 3: Execute mutation via BundleService (same path as SyndrQL)
	documentID, err := h.mutationExecutor.ExecuteCreate(docCommand)
	if err != nil {
		return nil, fmt.Errorf("execution failed: %w", err)
	}

	// Step 4: Resolve response fields from created document
	response, err := h.mutationResolver.ResolveCreateResponse(documentID, bundleName, field.SelectionSet, variables)
	if err != nil {
		return nil, fmt.Errorf("response resolution failed: %w", err)
	}

	return response, nil
}

// executeUpdateMutation executes an update mutation (e.g., updateUser).
//
// Flow:
//  1. Validate id and input arguments
//  2. Parse mutation into DocumentUpdateCommand
//  3. Execute update via MutationExecutor
//  4. Fetch updated document and resolve response fields
//
// This follows the EXACT same path as SyndrQL's UPDATE DOCUMENTS command.
func (h *GraphQLHandler) executeUpdateMutation(field *ast.Field, bundleName string, variables map[string]interface{}) (interface{}, error) {
	h.logger.Debugf("Executing update mutation for bundle '%s'", bundleName)

	// Step 1: Validate input at GraphQL layer
	if err := h.inputValidator.ValidateUpdateMutation(field); err != nil {
		return nil, fmt.Errorf("validation failed: %w", err)
	}

	// Step 2: Parse mutation into DocumentUpdateCommand
	updateCommand, err := h.mutationParser.ParseUpdateMutation(field, bundleName, variables)
	if err != nil {
		return nil, fmt.Errorf("parsing failed: %w", err)
	}

	// Step 3: Execute mutation via BundleService (same path as SyndrQL)
	if err := h.mutationExecutor.ExecuteUpdate(updateCommand); err != nil {
		return nil, fmt.Errorf("execution failed: %w", err)
	}

	// Step 4: Resolve response fields from updated document
	response, err := h.mutationResolver.ResolveUpdateResponse(updateCommand, bundleName, field.SelectionSet, variables)
	if err != nil {
		return nil, fmt.Errorf("response resolution failed: %w", err)
	}

	return response, nil
}

// executeDeleteMutation executes a delete mutation (e.g., deleteUser).
//
// Flow:
//  1. Validate id argument
//  2. Parse mutation into DocumentDeleteCommand
//  3. Execute delete via MutationExecutor
//  4. Return deletion metadata (success, deletedId, message)
//
// This follows the EXACT same path as SyndrQL's DELETE DOCUMENTS command.
func (h *GraphQLHandler) executeDeleteMutation(field *ast.Field, bundleName string, variables map[string]interface{}) (interface{}, error) {
	h.logger.Debugf("Executing delete mutation for bundle '%s'", bundleName)

	// Step 1: Validate input at GraphQL layer
	if err := h.inputValidator.ValidateDeleteMutation(field); err != nil {
		return nil, fmt.Errorf("validation failed: %w", err)
	}

	// Step 2: Parse mutation into DocumentDeleteCommand
	deleteCommand, err := h.mutationParser.ParseDeleteMutation(field, bundleName, variables)
	if err != nil {
		return nil, fmt.Errorf("parsing failed: %w", err)
	}

	// Step 3: Execute mutation via BundleService (same path as SyndrQL)
	deletedIDs, err := h.mutationExecutor.ExecuteDelete(deleteCommand)
	if err != nil {
		return nil, fmt.Errorf("execution failed: %w", err)
	}

	// Step 4: Resolve response with deletion metadata
	response, err := h.mutationResolver.ResolveDeleteResponse(deletedIDs, bundleName, field.SelectionSet)
	if err != nil {
		return nil, fmt.Errorf("response resolution failed: %w", err)
	}

	return response, nil
}
