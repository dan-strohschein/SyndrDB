# 🔮 SyndrDB GraphQL API

This document covers how GraphQL works in SyndrDB, including query structure, mutations, security features, and unique implementation details.

---

## 📑 Table of Contents

1. [Overview](#overview)
2. [GraphQL Protocol](#graphql-protocol)
3. [Schema System](#schema-system)
4. [Queries](#queries)
5. [Mutations](#mutations)
6. [Filtering](#filtering)
7. [Pagination](#pagination)
8. [Security Layers](#security-layers)
9. [Performance Optimization](#performance-optimization)
10. [Error Handling](#error-handling)
11. [Complete Examples](#complete-examples)
12. [Best Practices](#best-practices)

---

## Overview

SyndrDB implements a **native GraphQL interface** that provides an alternative to the SQL-like query language. Unlike traditional GraphQL implementations that run over HTTP, SyndrDB's GraphQL operates through the **native TCP protocol** with special command prefixes.

### Key Features

🔹 **Native Protocol Integration:** GraphQL queries sent via `GRAPHQL::` prefix through TCP socket  
🔹 **Dynamic Schema Generation:** Schemas automatically generated from bundle structures  
🔹 **Type-Safe Operations:** Full type checking and validation against bundle schemas  
🔹 **Introspection Support:** Complete `__schema` and `__type` queries for schema exploration  
🔹 **Multi-Layer Security:** 5-layer security system with rate limiting and complexity analysis  
🔹 **Relay Pagination:** Industry-standard cursor-based pagination  
🔹 **Advanced Filtering:** Powerful WHERE clauses with logical operators  
🔹 **Mutation Support:** Full CRUD operations with auto-generated input types  

### What Makes SyndrDB GraphQL Unique?

| Feature | Traditional GraphQL | SyndrDB GraphQL |
|---------|-------------------|-----------------|
| **Transport** | HTTP/HTTPS | Native TCP Protocol |
| **Command Format** | HTTP POST with JSON body | `GRAPHQL::{query: "..."}` |
| **Schema** | Static schema files | Dynamic from bundle structures |
| **Security** | Application-level | 5-layer built-in protection |
| **Pagination** | Varies by implementation | Relay-spec compliant cursors |
| **Filtering** | Custom per implementation | Unified WHERE clause system |
| **Integration** | Separate from database | Direct bundle access |

---

## GraphQL Protocol

### Command Format

SyndrDB GraphQL commands use a special prefix over the TCP connection:

```
GRAPHQL::{...JSON payload...}
```

### Request Structure

```json
{
  "query": "GraphQL query string",
  "operationName": "optional operation name",
  "variables": {
    "optional": "variables"
  }
}
```

### Basic Example

```javascript
// Command sent to SyndrDB TCP socket:
GRAPHQL::{
  "query": "{ databases { name } }"
}
```

### With Variables

```javascript
GRAPHQL::{
  "query": "query GetUser($id: String!) { user(id: $id) { name email } }",
  "variables": {
    "id": "user_123"
  }
}
```

### Response Format

GraphQL responses follow the standard GraphQL response structure:

```json
{
  "data": {
    "databases": [
      { "name": "primary" },
      { "name": "myapp" }
    ]
  },
  "errors": null
}
```

### Error Response

```json
{
  "data": null,
  "errors": [
    {
      "message": "Field 'unknownField' doesn't exist on type 'User'",
      "path": ["user", "unknownField"],
      "locations": [
        { "line": 2, "column": 5 }
      ],
      "extensions": {
        "code": "INVALID_FIELD"
      }
    }
  ]
}
```

---

## Schema System

### Dynamic Schema Generation

SyndrDB generates GraphQL schemas **dynamically** from your bundle structures. When you create or modify bundles, the GraphQL schema automatically updates to reflect the changes.

#### How It Works

1. **Bundle Creation:** You create a bundle with fields
2. **Schema Generation:** GraphQL type automatically generated from bundle schema
3. **Query Fields:** Bundle name becomes a queryable field
4. **Type Definitions:** Bundle fields map to GraphQL types

#### Example

**Bundle Definition:**
```sql
CREATE BUNDLE "users" WITH FIELDS (
    {"id", STRING, true, true},
    {"name", STRING, true, false},
    {"email", STRING, true, true},
    {"age", INT, false, false}
);
```

**Generated GraphQL Type:**
```graphql
type User {
  id: String!
  name: String!
  email: String!
  age: Int
}

type Query {
  users(
    limit: Int
    offset: Int
    where: WhereInput
    orderBy: [OrderByInput!]
  ): [User!]!
}
```

### Scalar Types

SyndrDB GraphQL supports these scalar types:

| SyndrDB Type | GraphQL Type | Description |
|--------------|--------------|-------------|
| **STRING** | `String` | Text data |
| **INT** | `Int` | 32-bit integer |
| **FLOAT** | `Float` | Floating-point number |
| **BOOLEAN** | `Boolean` | true/false |
| **DATETIME** | `DateTime` | Custom scalar for timestamps |
| **JSON** | `JSON` | Custom scalar for JSON objects |
| **TEXT** | `String` | Large text data |
| **BLOB** | `String` | Binary data (base64 encoded) |
| **TIMESTAMP** | `DateTime` | Unix timestamp |

### Field Modifiers

| Modifier | GraphQL Symbol | Meaning |
|----------|----------------|---------|
| **Required** | `!` | Field must have a value (not null) |
| **Optional** | (no symbol) | Field can be null |
| **Array** | `[Type]` | Field contains multiple values |
| **Non-null Array** | `[Type!]!` | Array cannot be null, elements cannot be null |

### Introspection

Query the schema itself using introspection:

#### Get Full Schema

```graphql
{
  __schema {
    types {
      name
      kind
      fields {
        name
        type {
          name
          kind
        }
      }
    }
  }
}
```

#### Get Specific Type

```graphql
{
  __type(name: "User") {
    name
    fields {
      name
      type {
        name
        kind
        ofType {
          name
          kind
        }
      }
    }
  }
}
```

#### List All Types

```graphql
{
  __schema {
    queryType {
      name
      fields {
        name
        type {
          name
        }
      }
    }
  }
}
```

---

## Queries

### Basic Query

Retrieve all documents from a bundle:

```graphql
{
  users {
    id
    name
    email
  }
}
```

### Field Selection

Select only the fields you need:

```graphql
{
  users {
    name
    email
  }
}
```

**Benefit:** Reduces data transfer and improves performance

### With Arguments

```graphql
{
  users(limit: 10, offset: 0) {
    id
    name
    email
  }
}
```

### Query Multiple Bundles

```graphql
{
  users {
    id
    name
  }
  products {
    id
    title
    price
  }
}
```

### Aliases

Rename fields in the response:

```graphql
{
  activeUsers: users(where: { status: { eq: "active" } }) {
    id
    fullName: name
    emailAddress: email
  }
  inactiveUsers: users(where: { status: { eq: "inactive" } }) {
    id
    name
  }
}
```

**Response:**
```json
{
  "data": {
    "activeUsers": [...],
    "inactiveUsers": [...]
  }
}
```

### Fragments

Reuse field selections:

```graphql
fragment UserDetails on User {
  id
  name
  email
  createdAt
}

{
  activeUsers: users(where: { status: { eq: "active" } }) {
    ...UserDetails
  }
  adminUsers: users(where: { role: { eq: "admin" } }) {
    ...UserDetails
  }
}
```

### Variables

Parameterize your queries:

```graphql
query GetUsersByStatus($status: String!, $limit: Int) {
  users(where: { status: { eq: $status } }, limit: $limit) {
    id
    name
    status
  }
}
```

**Variables:**
```json
{
  "status": "active",
  "limit": 20
}
```

---

## Mutations

### Create Operations

#### Create Document

```graphql
mutation CreateUser($input: CreateUserInput!) {
  createUser(input: $input) {
    id
    name
    email
    createdAt
  }
}
```

**Variables:**
```json
{
  "input": {
    "name": "Alice Johnson",
    "email": "alice@example.com",
    "age": 28,
    "status": "active"
  }
}
```

#### Create Database

```graphql
mutation {
  createDatabase(input: { name: "analytics" }) {
    name
    createdAt
  }
}
```

#### Create Bundle

```graphql
mutation {
  createBundle(input: {
    name: "products"
    database: "myapp"
    fields: [
      { name: "id", type: STRING, required: true, unique: true }
      { name: "title", type: STRING, required: true }
      { name: "price", type: FLOAT, required: true }
      { name: "inStock", type: BOOLEAN, required: true }
    ]
  }) {
    name
    database
    documentCount
  }
}
```

### Update Operations

```graphql
mutation UpdateUser($id: String!, $input: UpdateUserInput!) {
  updateUser(id: $id, input: $input) {
    id
    name
    email
    updatedAt
  }
}
```

**Variables:**
```json
{
  "id": "user_123",
  "input": {
    "name": "Alice Smith",
    "status": "inactive"
  }
}
```

### Delete Operations

#### Delete Single Document

```graphql
mutation DeleteUser($id: String!) {
  deleteUser(id: $id) {
    success
    deletedCount
  }
}
```

**Variables:**
```json
{
  "id": "user_123"
}
```

#### Delete Multiple Documents

```graphql
mutation DeleteInactiveUsers {
  deleteUsers(where: { status: { eq: "inactive" } }) {
    deletedCount
  }
}
```

### Mutation Response

All mutations return the affected data:

```json
{
  "data": {
    "createUser": {
      "id": "user_456",
      "name": "Alice Johnson",
      "email": "alice@example.com",
      "createdAt": "2024-01-20T10:30:00Z"
    }
  }
}
```

---

## Filtering

SyndrDB GraphQL provides powerful filtering through the `where` argument.

### Filter Input Structure

```graphql
input WhereInput {
  # Field-level filters
  fieldName: FieldFilter
  
  # Logical operators
  and: [WhereInput!]
  or: [WhereInput!]
  not: WhereInput
}

input FieldFilter {
  eq: JSON      # Equals
  ne: JSON      # Not equals
  gt: JSON      # Greater than
  gte: JSON     # Greater than or equal
  lt: JSON      # Less than
  lte: JSON     # Less than or equal
  like: String  # String pattern matching
  in: [JSON!]   # Value in array
  notIn: [JSON!] # Value not in array
  isNull: Boolean # Field is null
}
```

### Simple Filters

#### Equality

```graphql
{
  users(where: { status: { eq: "active" } }) {
    id
    name
  }
}
```

#### Greater Than

```graphql
{
  users(where: { age: { gt: 18 } }) {
    id
    name
    age
  }
}
```

#### String Matching

```graphql
{
  users(where: { email: { like: "%@example.com" } }) {
    id
    name
    email
  }
}
```

#### IN Clause

```graphql
{
  users(where: { status: { in: ["active", "pending"] } }) {
    id
    name
    status
  }
}
```

### Logical Operators

#### AND (All Conditions)

```graphql
{
  users(where: {
    and: [
      { age: { gte: 18 } }
      { status: { eq: "active" } }
      { email: { like: "%@company.com" } }
    ]
  }) {
    id
    name
    age
    email
  }
}
```

#### OR (Any Condition)

```graphql
{
  users(where: {
    or: [
      { status: { eq: "active" } }
      { role: { eq: "admin" } }
    ]
  }) {
    id
    name
    status
    role
  }
}
```

#### NOT (Inverse)

```graphql
{
  users(where: {
    not: {
      status: { eq: "deleted" }
    }
  }) {
    id
    name
    status
  }
}
```

#### Complex Nested Logic

```graphql
{
  users(where: {
    and: [
      {
        or: [
          { age: { gte: 18 } }
          { role: { eq: "admin" } }
        ]
      }
      {
        not: {
          status: { eq: "suspended" }
        }
      }
      {
        email: { like: "%@company.com" }
      }
    ]
  }) {
    id
    name
    age
    role
    status
  }
}
```

**Translation:** Get users who are:
- (18+ OR admin) AND
- NOT suspended AND
- Have company email

### Filter Translation

SyndrDB translates GraphQL filters into native WHERE clauses:

| GraphQL Filter | SyndrDB WHERE Clause |
|----------------|----------------------|
| `{ age: { eq: 25 } }` | `age = 25` |
| `{ age: { gt: 18 } }` | `age > 18` |
| `{ age: { gte: 18 } }` | `age >= 18` |
| `{ age: { lt: 65 } }` | `age < 65` |
| `{ age: { lte: 65 } }` | `age <= 65` |
| `{ age: { ne: 0 } }` | `age != 0` |
| `{ email: { like: "%@company.com" } }` | `email LIKE "%@company.com"` |
| `{ status: { in: ["active", "pending"] } }` | `status IN ("active", "pending")` |
| `{ deleted: { isNull: true } }` | `deleted IS NULL` |

---

## Pagination

SyndrDB implements **Relay-spec cursor-based pagination** for efficient data retrieval.

### Why Cursor-Based?

✅ **Stable:** Results don't skip or duplicate when data changes  
✅ **Efficient:** Scales to large datasets  
✅ **Standard:** Relay spec is industry standard  
✅ **Bidirectional:** Support forward and backward navigation  

### Connection Pattern

Paginated queries return a `Connection` type:

```graphql
{
  users(first: 10) {
    edges {
      node {
        id
        name
        email
      }
      cursor
    }
    pageInfo {
      hasNextPage
      hasPreviousPage
      startCursor
      endCursor
    }
    totalCount
  }
}
```

### Response Structure

```json
{
  "data": {
    "users": {
      "edges": [
        {
          "node": {
            "id": "user_1",
            "name": "Alice",
            "email": "alice@example.com"
          },
          "cursor": "Y3Vyc29yOjE="
        },
        {
          "node": {
            "id": "user_2",
            "name": "Bob",
            "email": "bob@example.com"
          },
          "cursor": "Y3Vyc29yOjI="
        }
      ],
      "pageInfo": {
        "hasNextPage": true,
        "hasPreviousPage": false,
        "startCursor": "Y3Vyc29yOjE=",
        "endCursor": "Y3Vyc29yOjI="
      },
      "totalCount": 150
    }
  }
}
```

### Forward Pagination

Get the **first N** items:

```graphql
{
  users(first: 10) {
    edges {
      node { id name }
      cursor
    }
    pageInfo {
      hasNextPage
      endCursor
    }
  }
}
```

**Get next page:**
```graphql
{
  users(first: 10, after: "Y3Vyc29yOjEw") {
    edges {
      node { id name }
      cursor
    }
    pageInfo {
      hasNextPage
      endCursor
    }
  }
}
```

### Backward Pagination

Get the **last N** items:

```graphql
{
  users(last: 10) {
    edges {
      node { id name }
      cursor
    }
    pageInfo {
      hasPreviousPage
      startCursor
    }
  }
}
```

**Get previous page:**
```graphql
{
  users(last: 10, before: "Y3Vyc29yOjEw") {
    edges {
      node { id name }
      cursor
    }
    pageInfo {
      hasPreviousPage
      startCursor
    }
  }
}
```

### Pagination Arguments

| Argument | Type | Description |
|----------|------|-------------|
| **first** | `Int` | Number of items to fetch from start (forward) |
| **after** | `String` | Cursor to start after (forward) |
| **last** | `Int` | Number of items to fetch from end (backward) |
| **before** | `String` | Cursor to start before (backward) |

### PageInfo Fields

| Field | Type | Description |
|-------|------|-------------|
| **hasNextPage** | `Boolean!` | More items exist after current page |
| **hasPreviousPage** | `Boolean!` | Items exist before current page |
| **startCursor** | `String` | Cursor of first item in page |
| **endCursor** | `String` | Cursor of last item in page |

### Cursor Format

Cursors are **opaque base64-encoded strings**:

```
Encoded: "Y3Vyc29yOjE="
Decoded: "cursor:1"
Format:  base64(documentID:index)
```

⚠️ **Never** manually construct cursors - always use cursors returned by the API.

### Pagination with Filters

Combine pagination with filtering:

```graphql
{
  users(
    first: 20
    where: { status: { eq: "active" } }
    orderBy: [{ field: "createdAt", direction: DESC }]
  ) {
    edges {
      node {
        id
        name
        status
        createdAt
      }
      cursor
    }
    pageInfo {
      hasNextPage
      endCursor
    }
    totalCount
  }
}
```

### Client-Side Pagination Example

```javascript
let cursor = null;
let allUsers = [];

while (true) {
  const query = `
    query GetUsers($cursor: String) {
      users(first: 100, after: $cursor) {
        edges {
          node { id name email }
          cursor
        }
        pageInfo {
          hasNextPage
          endCursor
        }
      }
    }
  `;
  
  const result = await executeGraphQL(query, { cursor });
  
  // Add to collection
  allUsers.push(...result.data.users.edges.map(e => e.node));
  
  // Check if more pages exist
  if (!result.data.users.pageInfo.hasNextPage) {
    break;
  }
  
  // Update cursor for next page
  cursor = result.data.users.pageInfo.endCursor;
}

console.log(`Loaded ${allUsers.length} users`);
```

---

## Security Layers

SyndrDB GraphQL implements **5 layers of security** to protect against abuse and resource exhaustion.

### Security Architecture

```
┌─────────────────────────────────────────┐
│   Layer 5: Query Monitoring             │ ← Metrics & Alerting
├─────────────────────────────────────────┤
│   Layer 4: Query Timeout                │ ← Execution Time Limit
├─────────────────────────────────────────┤
│   Layer 3: Rate Limiting                │ ← Requests Per Minute
├─────────────────────────────────────────┤
│   Layer 2: Depth Limiting               │ ← Nesting Depth
├─────────────────────────────────────────┤
│   Layer 1: Complexity Analysis          │ ← Query Cost Estimation
└─────────────────────────────────────────┘
```

### Layer 1: Complexity Analysis

Prevents expensive queries before execution.

#### How It Works

Each query is assigned a **complexity score** based on:
- Number of fields: 1 point each
- Relationships: 5 points each
- Nesting depth: multiply by 2 per level

#### Example Calculation

```graphql
{
  users {                    # Depth 1, cost 1
    id                       # +1
    name                     # +1
    posts {                  # Depth 2, relationship +5, multiply by 2
      id                     # +1
      title                  # +1
    }
  }
}

# Calculation:
# Level 1: 1 (users) + 1 (id) + 1 (name) = 3
# Level 2: (5 (relationship) + 1 (id) + 1 (title)) × 2 = 14
# Total: 3 + 14 = 17
```

#### Limits by Role

| Role | Max Complexity | Max Depth | Max Breadth |
|------|----------------|-----------|-------------|
| **Anonymous** | 50 | 3 | 25 |
| **Authenticated** | 100 | 5 | 50 |
| **Admin** | Unlimited | Unlimited | Unlimited |

#### Server Configuration

```bash
# Enable complexity limiting (Layer 1)
--enable-complexity-limit=true

# Set limits (in server config)
# These are role-based and configured internally
```

### Layer 2: Depth Limiting

Prevents deeply nested queries that can cause exponential data loading.

#### Max Nesting Depth

```graphql
# ✅ Allowed (depth 3)
{
  users {                    # Depth 1
    posts {                  # Depth 2
      comments {             # Depth 3
        id
        text
      }
    }
  }
}

# ❌ Rejected (depth 6 exceeds limit)
{
  users {                    # Depth 1
    posts {                  # Depth 2
      comments {             # Depth 3
        author {             # Depth 4
          posts {            # Depth 5
            comments {       # Depth 6
              text
            }
          }
        }
      }
    }
  }
}
```

#### Server Configuration

```bash
# Enable depth limiting (Layer 2)
--enable-depth-limit=true
```

### Layer 3: Rate Limiting

Limits requests per user/IP to prevent abuse.

#### Rate Limit Algorithm

SyndrDB uses **Token Bucket** algorithm:

1. **Bucket Capacity:** Maximum tokens (e.g., 60)
2. **Refill Rate:** Tokens per minute (e.g., 60)
3. **Token Cost:** Each query consumes tokens (1-5)

#### Rate Limits by Role

| Role | Tokens/Minute | Bucket Capacity | Query Cost | DDL Cost |
|------|---------------|-----------------|------------|----------|
| **Anonymous** | 30 | 30 | 1 token | 5 tokens |
| **Authenticated** | 60 | 60 | 1 token | 3 tokens |
| **Admin** | Unlimited | N/A | N/A | N/A |

#### Example: Token Consumption

```javascript
// User starts with 60 tokens

// Query 1 (simple query, 1 token)
{ users { id name } }
// Remaining: 59 tokens

// Query 2 (simple query, 1 token)
{ products { id title } }
// Remaining: 58 tokens

// Mutation 1 (DDL, 3 tokens)
mutation { createBundle(...) }
// Remaining: 55 tokens

// After 1 minute: refill 60 tokens
// Remaining: 60 tokens (capped at capacity)
```

#### Rate Limit Response

```json
{
  "data": null,
  "errors": [
    {
      "message": "Rate limit exceeded. Please wait before making more requests.",
      "extensions": {
        "code": "RATE_LIMIT_EXCEEDED",
        "retryAfter": 30
      }
    }
  ]
}
```

#### Server Configuration

```bash
# Enable GraphQL rate limiting (Layer 3)
--enable-graphql-rate-limit=true

# Choose algorithm: token-bucket or time-bucket
--graphql-rate-algorithm=token-bucket
```

### Layer 4: Query Timeout

Limits execution time to prevent long-running queries.

#### Timeout by Role

| Role | Max Execution Time |
|------|-------------------|
| **Anonymous** | 5 seconds |
| **Authenticated** | 30 seconds |
| **Admin** | 60 seconds |

#### Timeout Response

```json
{
  "data": null,
  "errors": [
    {
      "message": "Query execution timeout exceeded (30s)",
      "extensions": {
        "code": "QUERY_TIMEOUT",
        "executionTime": 30.5
      }
    }
  ]
}
```

#### Server Configuration

```bash
# Enable query timeout (Layer 4)
--enable-query-timeout=true
```

### Layer 5: Query Monitoring

Tracks query metrics for alerting and analysis.

#### Monitored Metrics

| Metric | Description |
|--------|-------------|
| **Query Duration** | Execution time in milliseconds |
| **Complexity Score** | Calculated complexity |
| **Query Depth** | Nesting depth |
| **Token Cost** | Rate limit tokens consumed |
| **Success/Failure** | Query result status |
| **User Context** | Username, IP, role |

#### Expensive Query Alerts

Queries exceeding thresholds trigger warnings:

```
⚠️  Expensive GraphQL query detected
    User: alice@company.com
    Duration: 5.2s
    Complexity: 85
    Operation: query GetUserPosts
```

#### Server Configuration

```bash
# Enable query monitoring (Layer 5)
--enable-query-monitoring=true
```

### Security Best Practices

✅ **DO:**
- Use pagination for large result sets
- Request only needed fields
- Use variables for reusable queries
- Monitor expensive query alerts
- Set appropriate depth limits for your use case

❌ **DON'T:**
- Request deeply nested relationships without pagination
- Select all fields when you only need a few
- Bypass security by creating multiple anonymous connections
- Ignore rate limit warnings
- Use inline values instead of variables (less cacheable)

---

## Performance Optimization

### DataLoader Pattern

SyndrDB implements DataLoader to batch and cache database requests.

#### Problem: N+1 Queries

Without batching:
```graphql
{
  users {           # Query 1: Get users
    id
    posts {         # Query 2, 3, 4...: Get posts for EACH user
      title
    }
  }
}

# Result: 1 + N queries (1 for users, N for posts)
```

#### Solution: DataLoader

With batching:
```graphql
{
  users {           # Query 1: Get users
    id
    posts {         # Query 2: Get posts for ALL users in one query
      title
    }
  }
}

# Result: 2 queries total
```

### Query Optimization Strategies

#### 1. Field Selection

```graphql
# ❌ Over-fetching
{
  users {
    id
    name
    email
    phone
    address
    city
    state
    zip
    country
    createdAt
    updatedAt
  }
}

# ✅ Request only what you need
{
  users {
    id
    name
    email
  }
}
```

#### 2. Pagination

```graphql
# ❌ Load all records
{
  users {
    id
    name
  }
}

# ✅ Paginate large result sets
{
  users(first: 50) {
    edges {
      node {
        id
        name
      }
    }
    pageInfo {
      hasNextPage
      endCursor
    }
  }
}
```

#### 3. Filtering at Source

```graphql
# ❌ Fetch all, filter in client
{
  users {
    id
    name
    status  # Filter status = "active" in JavaScript
  }
}

# ✅ Filter in database
{
  users(where: { status: { eq: "active" } }) {
    id
    name
  }
}
```

#### 4. Use Fragments

```graphql
# ❌ Duplicate field selections
{
  activeUsers: users(where: { status: { eq: "active" } }) {
    id
    name
    email
    createdAt
  }
  adminUsers: users(where: { role: { eq: "admin" } }) {
    id
    name
    email
    createdAt
  }
}

# ✅ DRY with fragments
fragment UserFields on User {
  id
  name
  email
  createdAt
}

{
  activeUsers: users(where: { status: { eq: "active" } }) {
    ...UserFields
  }
  adminUsers: users(where: { role: { eq: "admin" } }) {
    ...UserFields
  }
}
```

### Performance Metrics

Use introspection to understand query cost:

```graphql
{
  __schema {
    queryType {
      fields {
        name
        description
        args {
          name
          type {
            name
          }
        }
      }
    }
  }
}
```

---

## Error Handling

### Error Response Structure

GraphQL errors follow a standard format:

```json
{
  "data": null,
  "errors": [
    {
      "message": "Human-readable error message",
      "path": ["fieldPath", "toError"],
      "locations": [
        {
          "line": 2,
          "column": 5
        }
      ],
      "extensions": {
        "code": "ERROR_CODE",
        "additionalInfo": "value"
      }
    }
  ]
}
```

### Common Error Codes

| Code | Meaning | Example |
|------|---------|---------|
| **GRAPHQL_PARSE_FAILED** | Query syntax error | Missing closing brace |
| **GRAPHQL_VALIDATION_FAILED** | Schema validation failed | Unknown field |
| **INVALID_FIELD** | Field doesn't exist | Typo in field name |
| **RATE_LIMIT_EXCEEDED** | Too many requests | Exceeded tokens |
| **QUERY_TIMEOUT** | Execution too slow | Query took > 30s |
| **COMPLEXITY_EXCEEDED** | Query too complex | Complexity > 100 |
| **DEPTH_EXCEEDED** | Query too deep | Depth > 5 |
| **AUTHENTICATION_REQUIRED** | Not authenticated | DDL without auth |
| **PERMISSION_DENIED** | Insufficient permissions | User lacks role |

### Error Examples

#### Parse Error

**Query:**
```graphql
{
  users {
    id
    name
  # Missing closing brace
}
```

**Response:**
```json
{
  "errors": [
    {
      "message": "Syntax Error: Expected Name, found <EOF>",
      "locations": [{ "line": 5, "column": 1 }],
      "extensions": {
        "code": "GRAPHQL_PARSE_FAILED"
      }
    }
  ]
}
```

#### Validation Error

**Query:**
```graphql
{
  users {
    id
    invalidField
  }
}
```

**Response:**
```json
{
  "errors": [
    {
      "message": "Cannot query field 'invalidField' on type 'User'",
      "path": ["users", "invalidField"],
      "locations": [{ "line": 4, "column": 5 }],
      "extensions": {
        "code": "INVALID_FIELD"
      }
    }
  ]
}
```

#### Rate Limit Error

**Response:**
```json
{
  "data": null,
  "errors": [
    {
      "message": "Rate limit exceeded. Please wait before making more requests.",
      "extensions": {
        "code": "RATE_LIMIT_EXCEEDED",
        "retryAfter": 30,
        "availableTokens": 0,
        "capacity": 60
      }
    }
  ]
}
```

#### Complexity Error

**Response:**
```json
{
  "data": null,
  "errors": [
    {
      "message": "Query complexity 125 exceeds maximum 100",
      "extensions": {
        "code": "COMPLEXITY_EXCEEDED",
        "complexity": 125,
        "maxComplexity": 100,
        "depth": 6
      }
    }
  ]
}
```

### Partial Errors

GraphQL allows **partial success**:

```json
{
  "data": {
    "users": [
      { "id": "1", "name": "Alice" },
      { "id": "2", "name": "Bob" }
    ],
    "products": null
  },
  "errors": [
    {
      "message": "Bundle 'products' not found",
      "path": ["products"],
      "extensions": {
        "code": "BUNDLE_NOT_FOUND"
      }
    }
  ]
}
```

**Interpretation:** `users` query succeeded, `products` query failed.

### Error Handling Best Practices

✅ **DO:**
- Check both `data` and `errors` in response
- Log errors for debugging
- Show user-friendly messages (not raw errors)
- Handle rate limits with exponential backoff
- Retry on network errors, not validation errors

❌ **DON'T:**
- Assume `data` is always populated
- Expose raw error messages to end users
- Retry immediately on rate limit errors
- Ignore `extensions` field (contains useful metadata)

---

## Complete Examples

### Example 1: User Management System

```graphql
# Create user
mutation CreateUser {
  createUser(input: {
    name: "Alice Johnson"
    email: "alice@company.com"
    role: "data-writer"
    age: 28
    status: "active"
  }) {
    id
    name
    email
    createdAt
  }
}

# Query active users with pagination
query GetActiveUsers($cursor: String) {
  users(
    first: 20
    after: $cursor
    where: { status: { eq: "active" } }
    orderBy: [{ field: "createdAt", direction: DESC }]
  ) {
    edges {
      node {
        id
        name
        email
        role
        createdAt
      }
      cursor
    }
    pageInfo {
      hasNextPage
      endCursor
    }
    totalCount
  }
}

# Update user
mutation UpdateUser($id: String!) {
  updateUser(id: $id, input: {
    status: "inactive"
  }) {
    id
    status
    updatedAt
  }
}

# Delete user
mutation DeleteUser($id: String!) {
  deleteUser(id: $id) {
    success
    deletedCount
  }
}
```

### Example 2: E-Commerce Queries

```graphql
# Complex product search with filters
query SearchProducts(
  $minPrice: Float!
  $maxPrice: Float!
  $category: String!
  $inStock: Boolean!
  $cursor: String
) {
  products(
    first: 24
    after: $cursor
    where: {
      and: [
        { price: { gte: $minPrice } }
        { price: { lte: $maxPrice } }
        { category: { eq: $category } }
        { inStock: { eq: $inStock } }
      ]
    }
    orderBy: [
      { field: "price", direction: ASC }
    ]
  ) {
    edges {
      node {
        id
        title
        description
        price
        category
        inStock
        imageUrl
      }
      cursor
    }
    pageInfo {
      hasNextPage
      hasPreviousPage
      startCursor
      endCursor
    }
    totalCount
  }
}

# Variables
{
  "minPrice": 10.00,
  "maxPrice": 100.00,
  "category": "electronics",
  "inStock": true,
  "cursor": null
}
```

### Example 3: Analytics Dashboard

```graphql
# Dashboard with multiple queries
query Dashboard {
  # Total users
  totalUsers: users {
    edges { node { id } }
    totalCount
  }
  
  # Active users
  activeUsers: users(where: { status: { eq: "active" } }) {
    totalCount
  }
  
  # New users this week
  newUsers: users(
    where: {
      createdAt: {
        gte: "2024-01-13T00:00:00Z"
      }
    }
  ) {
    edges {
      node {
        id
        name
        email
        createdAt
      }
    }
    totalCount
  }
  
  # Users by role
  admins: users(where: { role: { eq: "admin" } }) {
    totalCount
  }
  writers: users(where: { role: { eq: "data-writer" } }) {
    totalCount
  }
  readers: users(where: { role: { eq: "data-reader" } }) {
    totalCount
  }
}
```

### Example 4: Introspection

```graphql
# Discover available bundles
{
  __schema {
    queryType {
      fields {
        name
        description
      }
    }
  }
}

# Get schema for specific bundle
{
  __type(name: "User") {
    name
    description
    fields {
      name
      type {
        name
        kind
        ofType {
          name
          kind
        }
      }
      description
    }
  }
}
```

---

## Best Practices

### Query Design

✅ **DO:**
- Use fragments for reusable field selections
- Request only fields you need
- Use variables for parameterized queries
- Paginate large result sets
- Filter at database level, not client level
- Use aliases for multiple similar queries
- Name your operations for better debugging

❌ **DON'T:**
- Over-fetch fields "just in case"
- Use inline values instead of variables
- Request deeply nested data without pagination
- Ignore complexity warnings
- Create unnamed queries in production

### Security

✅ **DO:**
- Authenticate users for sensitive operations
- Use role-based access control
- Monitor query complexity
- Set appropriate rate limits
- Log expensive queries
- Validate input data
- Use prepared queries with variables

❌ **DON'T:**
- Expose GraphQL to unauthenticated users for DDL
- Ignore rate limit errors
- Allow unlimited query depth
- Store secrets in queries
- Trust client-provided data

### Performance

✅ **DO:**
- Use pagination for large datasets
- Leverage DataLoader batching
- Cache frequently accessed data
- Monitor query execution times
- Use indexes on filtered fields
- Optimize bundle schemas

❌ **DON'T:**
- Load entire collections without pagination
- Nest relationships beyond 3-4 levels
- Ignore `hasNextPage` in pagination
- Request data multiple times (use fragments)
- Skip query analysis during development

### Error Handling

✅ **DO:**
- Check both `data` and `errors` fields
- Handle partial errors gracefully
- Implement exponential backoff for rate limits
- Log errors for debugging
- Show user-friendly error messages
- Validate input before sending queries

❌ **DON'T:**
- Assume queries always succeed
- Expose raw error messages to users
- Retry immediately on failure
- Ignore error codes and extensions
- Skip client-side validation

### Development Workflow

✅ **DO:**
- Use GraphQL introspection during development
- Test queries in isolation before combining
- Document your queries with descriptions
- Version your schema changes
- Monitor production query patterns
- Use GraphQL tools (GraphiQL, Apollo DevTools)

❌ **DON'T:**
- Skip introspection queries
- Deploy untested complex queries
- Make breaking schema changes without migration
- Ignore query monitoring metrics
- Develop without a GraphQL client

---

## Quick Reference

### Command Syntax

```bash
# Basic query
GRAPHQL::{ "query": "{ users { id name } }" }

# With variables
GRAPHQL::{ 
  "query": "query GetUser($id: String!) { user(id: $id) { name } }",
  "variables": { "id": "user_123" }
}

# Mutation
GRAPHQL::{
  "query": "mutation CreateUser($input: CreateUserInput!) { createUser(input: $input) { id } }",
  "variables": { "input": { "name": "Alice" } }
}
```

### Common Query Patterns

| Pattern | Example |
|---------|---------|
| **Basic Query** | `{ users { id name } }` |
| **With Filter** | `{ users(where: { status: { eq: "active" } }) { id } }` |
| **With Pagination** | `{ users(first: 10, after: "cursor") { edges { node { id } } } }` |
| **Multiple Bundles** | `{ users { id } products { id } }` |
| **With Alias** | `{ activeUsers: users(where: {...}) { id } }` |
| **With Fragment** | `{ users { ...UserFields } } fragment UserFields on User { id name }` |

### Security Limits

| Layer | Anonymous | Authenticated | Admin |
|-------|-----------|---------------|-------|
| **Complexity** | 50 | 100 | ∞ |
| **Depth** | 3 | 5 | ∞ |
| **Rate (req/min)** | 30 | 60 | ∞ |
| **Timeout (sec)** | 5 | 30 | 60 |

### Server Flags

```bash
# Enable GraphQL
--graphql=true

# Security layers
--enable-complexity-limit=true      # Layer 1
--enable-depth-limit=true          # Layer 2
--enable-graphql-rate-limit=true   # Layer 3
--enable-query-timeout=true        # Layer 4
--enable-query-monitoring=true     # Layer 5

# Rate limiting
--graphql-rate-algorithm=token-bucket
```

---

**End of SyndrDB GraphQL Documentation**
