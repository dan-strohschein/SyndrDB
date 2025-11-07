# Phase 6 Implementation Plan: GraphQL Query Execution & Introspection

## Overview

Phase 6 implements GraphQL query parsing and execution as a **native SyndrDB protocol command**, not an HTTP endpoint. Queries are sent through SyndrDB's existing command processor using the `GRAPHQL::` prefix.

## Scope

### ✅ In Scope (MVP)
1. **Query Parsing**: Parse GraphQL query syntax
2. **Query Execution**: Resolve fields from bundle data
3. **Schema Introspection**: `__schema`, `__type` queries
4. **Field Resolution**: Map GraphQL fields → bundle fields
5. **Type Coercion**: Convert bundle types → GraphQL types
6. **Error Handling**: GraphQL-compliant error responses

### ❌ Out of Scope (Future)
- Mutations (TODO comments for future)
- Subscriptions (far future, not MVP)
- HTTP endpoints (handled by API gateway)
- Authentication (handled at API layer)

## Architecture

```
Native Protocol Flow:
┌─────────────┐
│ Client App  │
│ (Node/TS)   │
└──────┬──────┘
       │ Native SyndrDB Protocol
       │ GRAPHQL::{ query { ... } }
       ▼
┌─────────────────┐
│ SyndrDB Server  │
│ CommandDirector │◄─── Detects "GRAPHQL::" prefix
└──────┬──────────┘
       │
       ▼
┌──────────────────┐
│ GraphQLProcessor │◄─── Phase 6 implementation
│ (query handler)  │
└──────┬───────────┘
       │
       ├─► Schema Manager (get schema)
       ├─► Bundle Service (get data)
       └─► Response Builder (format result)
```

## Component Design

### 1. GraphQL Query Handler
**File**: `/src/internal/graphQL/handler/query_handler.go`

**Purpose**: Main entry point for GraphQL command processing

**Responsibilities**:
- Parse command string (extract query from `GRAPHQL::...`)
- Route to query executor or introspection handler
- Format responses in GraphQL JSON format
- Handle errors with GraphQL error structure

**Methods**:
```go
// ProcessGraphQLCommand implements GraphQLProcessor interface
// This is the main entry point called by CommandDirector
ProcessGraphQLCommand(command string) (interface{}, error)

// parseGraphQLCommand extracts the GraphQL query from GRAPHQL:: prefix
parseGraphQLCommand(command string) (string, error)

// isIntrospectionQuery checks if query is for __schema or __type
isIntrospectionQuery(query string) bool
```

### 2. GraphQL Query Parser
**File**: `/src/internal/graphQL/parser/query_parser.go`

**Purpose**: Parse GraphQL query syntax into AST

**Responsibilities**:
- Tokenize GraphQL query string
- Build Abstract Syntax Tree (AST)
- Validate query structure
- Extract selections, arguments, fragments

**Structures**:
```go
type QueryDocument struct {
    Operations []Operation
    Fragments  map[string]Fragment
}

type Operation struct {
    Type       string  // "query", "mutation", "subscription"
    Name       string  // Optional operation name
    Selections []Selection
}

type Selection struct {
    Field      string              // Field name
    Alias      string              // Optional alias
    Arguments  map[string]interface{}
    Selections []Selection         // Nested selections
}
```

### 3. Query Executor
**File**: `/src/internal/graphQL/executor/query_executor.go`

**Purpose**: Execute parsed GraphQL queries against bundle data

**Responsibilities**:
- Resolve field selections
- Execute queries against BundleService
- Handle nested object resolution
- Apply filters and arguments
- TODO: Handle mutations (future)
- TODO: Handle subscriptions (far future)

**Key Methods**:
```go
// ExecuteQuery executes a parsed GraphQL query
ExecuteQuery(query *QueryDocument, database *models.Database, schemaManager *schema.SchemaManager) (map[string]interface{}, error)

// resolveField resolves a single field selection
resolveField(field *Selection, bundle *models.Bundle, parentData interface{}) (interface{}, error)

// executeBundleQuery executes query against a bundle (SELECT documents)
executeBundleQuery(bundleName string, selections []Selection, args map[string]interface{}) ([]interface{}, error)
```

### 4. Introspection Handler
**File**: `/src/internal/graphQL/introspection/introspection_handler.go`

**Purpose**: Handle GraphQL introspection queries (`__schema`, `__type`)

**Responsibilities**:
- Return schema metadata
- Return type information
- Support schema exploration tools (GraphiQL, etc.)

**Introspection Queries Supported**:
```graphql
# Get full schema
query { __schema { types { name fields { name type } } } }

# Get specific type
query { __type(name: "User") { name fields { name type } } }

# Get query type
query { __schema { queryType { name } } }
```

### 5. Response Builder
**File**: `/src/internal/graphQL/response/response_builder.go`

**Purpose**: Format execution results into GraphQL JSON response format

**Responsibilities**:
- Build GraphQL-compliant JSON response
- Include `data` and `errors` fields
- Format errors with locations and paths
- Handle partial results

**Response Format**:
```json
{
  "data": {
    "users": [
      { "id": "1", "name": "Alice", "email": "alice@example.com" },
      { "id": "2", "name": "Bob", "email": "bob@example.com" }
    ]
  },
  "errors": [
    {
      "message": "Field 'age' not found in schema",
      "locations": [{ "line": 3, "column": 5 }],
      "path": ["users", 0, "age"]
    }
  ]
}
```

## Implementation Steps

### Step 1: Create Query Handler (Main Entry Point)
- Implement `GraphQLProcessor` interface
- Parse `GRAPHQL::` command format
- Route to executor or introspection
- Format responses

### Step 2: Create Query Parser
- Tokenize GraphQL syntax
- Build AST for selections
- Validate query structure
- Handle aliases and arguments

### Step 3: Create Query Executor
- Resolve field selections from bundles
- Execute queries via BundleService
- Handle type coercion (bundle → GraphQL types)
- Support filtering via arguments

### Step 4: Create Introspection Handler
- Implement `__schema` query
- Implement `__type(name:)` query
- Return schema metadata from SchemaManager

### Step 5: Create Response Builder
- Format successful results
- Format errors with GraphQL structure
- Handle partial results (some fields succeed, some fail)

### Step 6: Integration & Testing
- Connect handler to ServiceManager
- Test with sample queries
- Test introspection queries
- Test error cases

## Query Examples

### Example 1: Simple Query
```graphql
GRAPHQL::{
  users {
    id
    name
    email
  }
}
```

**Execution Flow**:
1. CommandDirector detects `GRAPHQL::` prefix
2. Handler extracts query: `{ users { id name email } }`
3. Parser creates AST with bundle "users" and fields [id, name, email]
4. Executor:
   - Gets schema for "users" bundle
   - Maps GraphQL fields → bundle fields (id → DocumentID)
   - Queries BundleService: `SELECT DocumentID, name, email FROM users`
   - Converts results to GraphQL format
5. Response builder formats JSON response

### Example 2: Query with Arguments
```graphql
GRAPHQL::{
  users(limit: 10, where: { status: "active" }) {
    id
    name
  }
}
```

**Execution Flow**:
1. Parser extracts arguments: `{limit: 10, where: {status: "active"}}`
2. Executor translates to SyndrQL:
   - `SELECT DocumentID, name FROM users WHERE status = "active" LIMIT 10`
3. TODO: Mutations would use similar argument parsing

### Example 3: Introspection
```graphql
GRAPHQL::{
  __type(name: "User") {
    name
    fields {
      name
      type {
        name
        kind
      }
    }
  }
}
```

**Execution Flow**:
1. Handler detects `__type` introspection query
2. Routes to IntrospectionHandler
3. Retrieves schema for "User" type from SchemaManager
4. Formats schema metadata as GraphQL response

## Error Handling

### GraphQL Error Structure
```go
type GraphQLError struct {
    Message   string                 `json:"message"`
    Locations []SourceLocation       `json:"locations,omitempty"`
    Path      []interface{}          `json:"path,omitempty"`
    Extensions map[string]interface{} `json:"extensions,omitempty"`
}

type SourceLocation struct {
    Line   int `json:"line"`
    Column int `json:"column"`
}
```

### Error Cases
1. **Parse Error**: Invalid GraphQL syntax
2. **Validation Error**: Field doesn't exist in schema
3. **Execution Error**: Database query fails
4. **Type Error**: Type mismatch in field resolution

## File Structure

```
src/internal/graphQL/
├── handler/
│   └── query_handler.go          # Main GraphQL command handler
├── parser/
│   ├── query_parser.go            # GraphQL query parser
│   └── ast.go                     # AST structures
├── executor/
│   ├── query_executor.go          # Query execution engine
│   └── field_resolver.go          # Field resolution logic
├── introspection/
│   └── introspection_handler.go   # Schema introspection
├── response/
│   └── response_builder.go        # Format GraphQL responses
└── schema/                         # Phase 5 (already exists)
    ├── schema_manager.go
    ├── schema_generator.go
    └── ...
```

## Testing Plan

### Unit Tests
1. Query Parser tests
   - Parse simple queries
   - Parse nested selections
   - Parse with arguments
   - Parse with aliases
   - Invalid syntax handling

2. Query Executor tests
   - Execute simple queries
   - Execute with field mapping
   - Execute with filters
   - Error handling

3. Introspection tests
   - `__schema` query
   - `__type` query
   - Non-existent types

### Integration Tests
1. End-to-end query execution
2. Query with existing bundle data
3. Introspection against generated schemas
4. Error responses

## Success Criteria

✅ Parse GraphQL queries from `GRAPHQL::` commands  
✅ Execute queries against bundle data  
✅ Return GraphQL-compliant JSON responses  
✅ Support field selection and mapping  
✅ Support introspection queries  
✅ Handle errors with GraphQL error structure  
✅ Integrate with existing CommandDirector  
✅ TODO comments for mutations  
✅ TODO comments for subscriptions  

## Future Work (Post-MVP)

### Mutations (Next Phase)
```graphql
mutation {
  createUser(input: { name: "Alice", email: "alice@example.com" }) {
    id
    name
  }
}
```

### Subscriptions (Far Future)
```graphql
subscription {
  userCreated {
    id
    name
  }
}
```

## Implementation Rules (Phase 1 Standards)

1. **Detailed Comments**: Every file, function, and integration point
2. **Single Responsibility**: Each handler has one clear purpose
3. **Error Handling**: Graceful degradation, never crash
4. **DRY Principles**: Reuse schema manager, don't duplicate logic
5. **Go Idioms**: Proper error wrapping, defer cleanup
6. **Testing**: Comprehensive unit tests for each component
7. **TODO Comments**: First-person TODOs for future work

---

**Ready to implement Phase 6!** 🚀
