
# SyndrDB
![image](/logo.png)

A relational Document DB with a graphQL interface implemented in Golang. Think MongoDB, Postgres, and GraphQL had a baby.

Disclaimer: WIP. The project is getting closer to MVP with an alpha release soon, but it is not there yet. Performance benchmarks and full testing still need to complete.

(+Current progress+):
- SQL Style query language (SyndrQL)
- All basic SQL commands for querying data
- Basic DDL and DML commands in SyndrQL
- Hash Index and B-Tree index filtering
- Postgres-like file storage and retrieval
- Write Ahead Logging for transactions
- Relationships between bundles (0ToMany, 1ToMany, ManyToMany)
- GraphQL Interface
- ACID compliance
- Native Migration (Version tracking) with roll up/roll down
- backup & Restore with varying compression options


## Usage
``` 
Usage of ./syndr:
  -auth
        Enable authentication (on by default)
  -config string
        Path to config file (Not yet implemented)
  -datadir string
        Directory to store data files (default "./datafiles")
  -debug
        Enable debug mode (default true)
  -host string
        Host name or IP address to listen on (default "127.0.0.1")
  -logdir string
        Directory to store log files (default: stdout) (default "./log_files")
  -mode string
        Operation mode (standalone, cluster) (default "standalone")
  -port int
        Port for the HTTP server (default 1776)
  -print
        Print Log Messages to screen (default true)
  -userdebug
        Enable user debug mode
  -verbose
        Enable verbose logging (default true)
  -version string
        Shows version (default "0.0.1alpha")
  -graphql
        Executes the server in http / GraphQL API Mode
  --wal-mode=sync|async
        Whether the Write Ahead Logging is done asynchronously or synchronously      
```
## How to install

TO BE DETERMINED - Right now its just a single executable file with command line options.

## How it works
Updated diagrams Coming soon

## How its built

```go build -o syndr main.go  ```

## How to use it

It supports CRUD commands but not yet Authentication Commands. 

To create a Database:

```sql
 CREATE DATABASE "<Database_Name>";
```

To Create a Bundle:

```sql
CREATE BUNDLE "<BUNDLE_NAME>"
WITH FIELDS (
	{"<FIELDNAME>", <FIELDTYPE>, <ISREQUIRED>, <ISUNIQUE>, <DEFAULTVALUE>},
	{"<FIELDNAME>", <FIELDTYPE>, <ISREQUIRED>, <ISUNIQUE>, <DEFAULTVALUE>}
);
```

Field Types:
* STRING
* INT
* FLOAT
* BOOL
(Coming soon)
* DATETIME

+ ISREQUIRED is a boolean value (TRUE/FALSE) indicating if the value MUST be supplied to be valid
+ ISUNIQUE is a boolean value (TRUE/FALSE) indicating if the value MUST be unique within that field across all of the documents in that bundle
+ DEFAULTVALUE is a value that is automatically added to the field if the ISREQUIRED Flag is set to true and no value is supplied by the user.

### Indexes 

To Create an Index:
```sql
CREATE B-INDEX "INDEX_NAME" ON BUNDLE "BUNDLE_NAME"
WITH FIELDS (
	{"<FIELDNAME>", <ISUNIQUE>},
	{"<FIELDNAME>", <ISUNIQUE>}
)
```
Or, to create a hash index (Note - hash indexes only operate on one field):

```sql
CREATE H-INDEX "INDEX_NAME" ON BUNDLE "BUNDLE_NAME"
WITH FIELDS ({"<FIELDNAME>", <UNIQUE>})
```

### Basic Create, Read, Update, and Delete commands for documents
To add a Document to a bundle:

```sql
ADD DOCUMENT TO BUNDLE "<BUNDLE_NAME>"
WITH  (
    {"<FIELD_NAME>"=<VALUE>},
    ...
);
```

As long as the field type matches the data type of the value supplied.

SyndrDB supports advanced SQL-like query capabilities including field selection, GROUP BY aggregation, and JOIN operations.

### Basic Queries

For basic document retrieval:

```sql
 SELECT DOCUMENTS FROM "<BUNDLE_NAME>";
```

This will return all of the documents in the bundle.

### Field Selection

To select specific fields from documents:

```sql
SELECT field1, field2, field3 FROM "<BUNDLE_NAME>";
```

### Filtering with WHERE Clauses

To filter and get results more accurately, use this format:

```sql
SELECT DOCUMENTS 
FROM "<BUNDLE_NAME>" 
WHERE (
            (<FIELD_NAME> <OPERATOR> <VALUE>) <AND/OR> 
            (<FIELD_NAME> <OPERATOR> <VALUE> <AND/OR> <FIELD_NAME> <OPERATOR> <VALUE>)
      );
```

### GROUP BY Aggregation

For data analysis and aggregation:

```sql
SELECT category, COUNT(*), SUM(price), AVG(price)
FROM "<BUNDLE_NAME>"
GROUP BY category
HAVING COUNT(*) > 5
ORDER BY COUNT(*) DESC;
```

### JOIN Operations

To filter and get results with related bundles use this format:

```sql
SELECT <Field_List | DOCUMENTS | * | TOP N | COUNT(*) > 
FROM "<BUNDLE_NAME>" 
      <JOIN | OUTER JOIN | LEFT JOIN | RIGHT JOIN> "<BUNDLE_NAME_2>
      ON
      "<BUNDLE_NAME>"."<FIELD_NAME"> ==  "<BUNDLE_NAME_2>"."<FIELD_NAME">
WHERE (
            (<FIELD_NAME> <OPERATOR> <VALUE>) <AND/OR> 
            (<FIELD_NAME> <OPERATOR> <VALUE> <AND/OR> <FIELD_NAME> <OPERATOR> <VALUE>)
      );
ORDER BY  "<BUNDLE_NAME>"."<FIELD_NAME"> <ASC | DESC>
```

Currently supported operators are:

* == (equals)
* != (Not equals)
* \> (Greater Than)
* < (Less Than)
* \>= (Greater Than or equal to)
* <= (Less Than or equal to)

- String values are double quoted
- DateTimes are double quoted (**Coming soon**)
- Boolean values are true/false

To Update one or more documents in a bundle:

```sql
UPDATE DOCUMENTS IN BUNDLE "<BUNDLE_NAME>"
      (<FIELD> = <NEW_VALUE>, <FIELD> = <NEW_VALUE> )
WHERE (
            (<FIELD_NAME> <OPERATOR> <VALUE>) <AND/OR> 
            (<FIELD_NAME> <OPERATOR> <VALUE> <AND/OR> <FIELD_NAME> <OPERATOR> <VALUE>)
      );
```

To Delete one or more Documents in a Bundle:

```sql
DELETE DOCUMENTS 
FROM BUNDLE "<BUNDLE_NAME>" 
WHERE (
            (<FIELD_NAME> <OPERATOR> <VALUE>) <AND/OR> 
            (<FIELD_NAME> <OPERATOR> <VALUE> <AND/OR> <FIELD_NAME> <OPERATOR> <VALUE>)
      );

```

## GraphQL API Implementation

SyndrDB now features a comprehensive GraphQL API that provides a modern, web-friendly interface alongside the native TCP protocol. This implementation makes SyndrDB accessible to web applications, mobile apps, and any system that can consume GraphQL endpoints.

### 🎯 **Core Implementation**

**GraphQL Handler**:
- Query validation and parsing using `vektah/gqlparser`
- Comprehensive error handling and reporting
- Variable support for parameterized queries

**GraphQL Resolvers**:
- Complete resolver system for all CRUD operations
- **Query Resolvers**: `databases`, `database`, `bundles`, `bundle`, `documents`, `document`
- **Mutation Resolvers**: `createDatabase`, `createBundle`, `createDocument`, `updateDocument`, `deleteDocument`
- Integration with existing SyndrDB `CommandDirector` for native query execution

**GraphQL Schema**:
- Comprehensive type system: `Database`, `Bundle`, `Document`
- Scalar types: `JSON`, `DateTime`
- Input types for mutations
- Filtering and pagination support

### 🔧 **Server Integration**

**Server**:
- **TCP Server** (port 1776): Original SyndrDB native protocol


**Service Manager Integration**:
- GraphQL handler uses existing ServiceManager
- WAL (Write Ahead Logging) integration maintained
- Same transaction logging for GraphQL operations

**Configuration Support**:
- `-graphql` command line flag to enable/disable
- `EnableGraphQL` setting in configuration

### 🚀 **Usage Examples**

**Starting the server with GraphQL:**
```bash
./syndr -graphql -datadir=./data_files -logdir=./log_files
```

**Query Capabilities:**
```graphql
# List all databases
query { 
  databases { 
    name 
    bundles 
  } 
}

# Get specific database
query { 
  database(name: "TestDB") { 
    name 
    bundles 
  } 
}

# Query documents with filtering
query { 
  documents(bundle: "users", where: "age > 25", limit: 10) {
    id 
    fields
  }
}
```

**Mutation Capabilities:**
```graphql
# Create a new bundle
mutation {
  createBundle(name: "users", database: "TestDB", fields: "{\"name\":\"string\"}") {
    name 
    documentCount
  }
}

# Insert document
mutation {
  createDocument(bundle: "users", fields: "{\"name\":\"John\"}") {
    id 
    fields
  }
}
```

**Testing the GraphQL endpoint:**
```bash
# Health check
curl -X GET "http://127.0.0.1:1777/health"

# GraphQL query
curl -X POST "http://127.0.0.1:1777/graphql" \
  -H "Content-Type: application/json" \
  -d '{"query": "query { databases { name } }"}'
```

### 🔄 **Architecture Benefits**

1. **Consistency**: GraphQL operations use the same command director and business logic as native SyndrDB operations

2. **Performance**: Direct integration with existing storage layer and index systems

3. **Extensibility**: Easy to add new query types and mutations

### ✅ **Error Handling**

- GraphQL-compliant error responses
- Validation error reporting with locations
- Integration with SyndrDB native error messages

### 🎉 **Innovation Delivered**

This implementation makes SyndrDB a truly modern document database by adding:
- **Industry-standard GraphQL API** alongside the native protocol
- **Type-safe queries** with schema validation
- **Real-time integration** with existing SyndrDB infrastructure

The GraphQL API opens up SyndrDB to modern web applications, mobile apps, and any system that can consume GraphQL endpoints, while maintaining full compatibility with existing SyndrDB native clients.

**Result**: SyndrDB now offers both a high-performance native TCP interface AND a modern, web-friendly GraphQL API - making it suitable for a much broader range of applications and use cases! 🚀

## 📚 Advanced Features Documentation

## FULL FEATURE LIST

### Core Database Features
- **Database Management**
  - CREATE DATABASE
  - DROP DATABASE (partially implemented)
  - USE DATABASE
  - SHOW DATABASES
  - SELECT DATABASES (system catalog query)

- **Bundle (Table) Management**
  - CREATE BUNDLE with field definitions (name, type, required, unique, default values)
  - UPDATE BUNDLE (rename, add/remove/modify fields, add relationships)
  - DROP BUNDLE (with FORCE option)
  - SHOW BUNDLES
  - Field types: STRING, INT, FLOAT, BOOL (DATETIME coming soon)

### Data Manipulation (DML)
- **INSERT Operations**
  - ADD DOCUMENT TO BUNDLE with field validation
  - Automatic field validation (required, unique, type checking)
  - Default value assignment

- **SELECT Operations**
  - SELECT * / SELECT DOCUMENTS (all fields)
  - SELECT specific fields (field1, field2, ...)
  - SELECT DISTINCT
  - SELECT TOP N
  - SELECT COUNT(*)
  - Aggregate functions: COUNT(*), SUM(field), AVG(field), MIN(field), MAX(field)
  - Arithmetic expressions in SELECT (e.g., Price * Quantity + Tax)
  - Field aliases (AS alias_name)

- **UPDATE Operations**
  - UPDATE DOCUMENTS IN BUNDLE with WHERE clause
  - Update multiple fields in single statement
  - Conditional updates with complex WHERE clauses

- **DELETE Operations**
  - DELETE DOCUMENTS FROM BUNDLE with WHERE clause
  - Conditional deletion with complex WHERE clauses

### Query Features
- **WHERE Clause Support**
  - Comparison operators: ==, !=, >, <, >=, <=
  - Logical operators: AND, OR
  - Nested conditions with parentheses
  - Complex expression evaluation

- **JOIN Operations**
  - INNER JOIN
  - LEFT JOIN (partially implemented)
  - RIGHT JOIN (partially implemented)
  - OUTER JOIN (partially implemented)
  - Multi-table joins
  - ON clause with field comparisons
  - WITH RELATIONSHIP clause for hierarchical results

- **Aggregation and Grouping**
  - GROUP BY (single and multiple fields)
  - HAVING clause for filtered aggregation
  - Aggregate functions with GROUP BY

- **Sorting and Pagination**
  - ORDER BY (ASC/DESC)
  - Multiple field sorting
  - LIMIT (result count limiting)
  - OFFSET (result skipping)

### Indexing System
- **B-Tree Indexes**
  - CREATE B-INDEX with multiple fields
  - Unique and non-unique indexes
  - Range query optimization
  - Automatic index updates on document changes
  - Index-aware query planning

- **Hash Indexes**
  - CREATE H-INDEX (single field only)
  - Exact match optimization
  - Unique constraint enforcement
  - Fast equality lookups

- **Index Management**
  - Automatic index selection by query planner
  - Batched index updates for performance
  - Index rebuild capability
  - Index cost estimation

### Relationships
- **Relationship Types**
  - 1-to-Many
  - 0-to-Many
  - 1-to-1 (partially implemented)
  - Many-to-Many (partially implemented)

- **Relationship Operations**
  - ADD RELATIONSHIP via UPDATE BUNDLE
  - Foreign key tracking
  - Relationship-aware query execution
  - Hierarchical result formatting (WITH RELATIONSHIP)

### Transaction & Durability
- **Write-Ahead Logging (WAL)**
  - Synchronous WAL mode
  - Asynchronous WAL mode (with async writer)
  - Binary log format
  - LSN (Log Sequence Number) tracking
  - WAL replay for recovery

- **ACID Compliance**
  - Atomicity through WAL
  - Consistency through validation
  - Isolation (partially implemented)
  - Durability through WAL and fsync

- **Transaction Management**
  - Transaction tracking per session
  - BEGIN/COMMIT semantics (via migrations)
  - Rollback support (via migration rollback)

### Migration System
- **Version Control**
  - START MIGRATION ... COMMIT (create versioned migration)
  - Migration description and metadata
  - Automatic version numbering
  - Migration command batching

- **Migration Application**
  - APPLY MIGRATION WITH VERSION
  - FORCE option for risky migrations
  - Migration validation before apply
  - Checksum verification

- **Rollback Support**
  - APPLY ROLLBACK TO VERSION
  - Automatic down-migration generation
  - Rollback validation
  - Point-in-time recovery

- **Migration Tracking**
  - SHOW MIGRATIONS FOR database
  - Migration status tracking
  - Validation reports
  - Migration history

### Backup & Restore
- **Backup Operations**
  - BACKUP DATABASE command
  - Multiple compression formats: gzip, zstd, none
  - Include/exclude indexes option
  - Manifest generation with metadata
  - Checkpoint before backup

- **Restore Operations**
  - RESTORE DATABASE FROM backup
  - Automatic database creation
  - Index restoration
  - Metadata validation
  - Version compatibility checking

### Authentication & Authorization (RBAC)
- **User Management**
  - CREATE USER with password
  - Argon2id password hashing
  - User storage in Primary database
  - Automatic "Data-Reader" role assignment

- **Permission System**
  - GRANT permission TO USER
  - Role-based access control
  - Permission validation on commands
  - User-Permission junction tables

- **Role Management**
  - GRANT ROLE TO USER
  - Pre-defined roles (Data-Reader, etc.)
  - Role-Permission mapping
  - User-Role assignments

### Security Features
- **Authentication Security**
  - Argon2id password hashing with salt
  - Configurable hash parameters
  - Constant-time password comparison
  - Session-based authentication

- **Rate Limiting**
  - Per-IP connection rate limiting
  - Per-user authentication rate limiting
  - Progressive delay on failed attempts
  - Account lockout after threshold
  - Automatic unlock after time period

- **Session Management**
  - Session creation and tracking
  - Session expiration
  - Session activity updates
  - Connection fingerprinting
  - Session hijack detection
  - Active query tracking per session

- **Security Auditing**
  - Security event logging
  - Authentication event tracking
  - Rate limit violation logging
  - Session event tracking
  - Audit trail with rotation

- **Database Locking**
  - LOCK DATABASE command
  - UNLOCK DATABASE command
  - Lock reasons and comments
  - Prevent writes during maintenance

### GraphQL Interface
- **Query Support**
  - Dynamic schema generation from bundles
  - All CRUD operations via GraphQL
  - Introspection queries (__schema, __type)
  - Field selection and filtering
  - Variable support

- **Pagination**
  - Legacy pagination (limit/offset)
  - Relay-style cursor pagination
  - Connection/Edge/PageInfo pattern
  - Forward pagination (first/after)
  - Backward pagination (last/before)

- **Mutations**
  - createDatabase
  - createBundle
  - create<Bundle> (dynamic per bundle)
  - update<Bundle> (dynamic per bundle)
  - delete<Bundle> (dynamic per bundle)
  - Input validation

- **Advanced Features**
  - DataLoader for N+1 query elimination
  - Batched relationship loading
  - Structured filtering (WhereInput)
  - Deep relationship queries
  - Native integration with SyndrQL execution path

### Storage & Performance
- **Storage Format**
  - Postgres-inspired file format
  - Bundle files (.bnd)
  - Separate document files
  - Binary serialization
  - Page-based storage

- **Query Planning & Execution**
  - Cost-based query optimizer
  - Index selection algorithm
  - Execution plan generation
  - Full scan vs index scan decisions
  - JOIN execution strategies

- **Caching & Buffering**
  - Document scanner caching
  - Index page caching
  - Configurable cache sizes
  - LRU eviction (partially implemented)

- **Async Operations**
  - Async WAL writer with ordered queue
  - Worker pool for parallel operations
  - Sequence generator for ordering
  - Consistent reader for async reads
  - Batched index updates

### SyndrQL Parser
- **New Unified Parser**
  - Tokenizer with comprehensive token types
  - Recursive descent parser
  - Expression AST (Abstract Syntax Tree)
  - Pattern detection for hot-path optimization
  - Fast path for common queries

- **Parser Components**
  - SELECT parser with full feature support
  - INSERT parser (ADD DOCUMENT)
  - UPDATE parser with WHERE clauses
  - DELETE parser with WHERE clauses
  - CREATE BUNDLE parser
  - UPDATE BUNDLE parser
  - DROP BUNDLE parser
  - CREATE USER parser
  - GRANT parser (permissions and roles)
  - Migration parser (placeholder)

- **Expression Support**
  - Binary expressions (AND, OR, +, -, *, /, ==, !=, >, <, >=, <=)
  - Unary expressions (NOT, -)
  - Function calls (COUNT, SUM, AVG, MIN, MAX)
  - Identifier expressions
  - Literal expressions (string, int, float, bool)
  - Parenthesized expressions

- **Adapter Layer**
  - Converts new parser AST to legacy query structures
  - SelectStatement to UnifiedSelectQuery
  - Expression to WhereGroup conversion
  - Maintains backward compatibility
  - Feature flag support for gradual migration

### System Features
- **Command Line Interface**
  - TCP socket server (port 1776)
  - Native protocol (SyndrQL commands)
  - Authentication handshake
  - Session management
  - Multi-database support

- **Configuration**
  - Command-line flags for all options
  - Configurable data directory
  - Configurable log directory
  - Debug mode
  - Verbose logging
  - WAL mode selection (sync/async)
  - Authentication enable/disable

- **Logging**
  - Structured logging with Zap
  - Per-session query logging
  - Server event logging
  - WAL operation logging
  - Security audit logging
  - Log file rotation

- **System Management**
  - CHECKPOINT command (flush all data)
  - SHOW SESSIONS (active session listing)
  - SHOW RATE LIMIT (rate limit statistics)
  - Graceful shutdown
  - Connection tracking

### Data Validation
- **Field Validation**
  - Type checking (STRING, INT, FLOAT, BOOL)
  - Required field enforcement
  - Unique constraint checking
  - Default value assignment
  - Foreign key validation (via relationships)

- **Command Validation**
  - Syntax validation
  - Semantic validation
  - Permission validation
  - Migration validation
  - Checksum verification for migrations

### Error Handling
- **Comprehensive Error Types**
  - Syntax errors with line/column info
  - Validation errors with field details
  - Authentication errors
  - Rate limit errors with retry info
  - User lockout errors with unlock time
  - Migration conflict errors

- **Error Reporting**
  - Structured error messages
  - Debug mode for verbose errors
  - User-friendly error messages in production
  - GraphQL-compliant error format

### Testing Infrastructure
- **Test Coverage**
  - Unit tests for parsers
  - Integration tests for queries
  - End-to-end SELECT tests
  - JOIN operation tests
  - Migration validation tests
  - Performance benchmarks

### Features Partially Implemented
- **Transaction isolation levels** - basic transaction tracking exists
- **Stored procedures** - framework exists but not exposed
- **Triggers** - not implemented
- **Views** - documentation exists but not implemented
- **Full-text search** - documentation exists but not implemented
- **Pub/Sub** - documentation exists but not implemented
- **DATETIME field type** - planned but not implemented
- **Cluster mode** - flag exists but not implemented
- **Custom mutations in GraphQL** - registry placeholder exists
- **Complete cursor pagination** - Relay pattern supported but cursors not fully encoded/decoded