# 📊 SyndrDB Query Results & Output Format

This document describes how SyndrDB formats query results, including standard queries, JOIN operations with `WITH RELATIONSHIP`, and various command responses.

---

## 📑 Table of Contents

1. [Overview](#overview)
2. [Standard Response Structure](#standard-response-structure)
3. [SELECT Query Results](#select-query-results)
4. [JOIN Query Results](#join-query-results)
5. [WITH RELATIONSHIP Results](#with-relationship-results)
6. [Aggregate Query Results](#aggregate-query-results)
7. [DDL Command Results](#ddl-command-results)
8. [DML Command Results](#dml-command-results)
9. [SHOW Command Results](#show-command-results)
10. [Error Responses](#error-responses)
11. [Field Type Representations](#field-type-representations)
12. [Complete Examples](#complete-examples)

---

## Overview

SyndrDB returns all query results as **JSON-formatted responses** over the TCP protocol. Every response includes metadata about the result count, execution time, and the actual data.

### Response Philosophy

🎯 **Consistency:** All responses follow the same base structure  
⚡ **Performance:** Optimized JSON encoding with zero-allocation streaming  
📊 **Metadata:** Every response includes execution metrics  
🔍 **Type Safety:** Field values maintain their original types  

---

## Standard Response Structure

All SyndrDB responses follow this base structure:

### CommandResponse Format

```json
{
  "ResultCount": <number>,
  "Result": <data>,
  "ExecutionTimeMS": <float>
}
```

### Fields

| Field | Type | Description |
|-------|------|-------------|
| **ResultCount** | `int` | Number of items returned |
| **Result** | `interface{}` | The actual result data (type varies by command) |
| **ExecutionTimeMS** | `float64` | Query execution time in milliseconds |

### Example

```json
{
  "ResultCount": 3,
  "Result": [
    {"id": "user_1", "name": "Alice"},
    {"id": "user_2", "name": "Bob"},
    {"id": "user_3", "name": "Charlie"}
  ],
  "ExecutionTimeMS": 2.47
}
```

---

## SELECT Query Results

### Basic SELECT

Query all documents from a bundle:

**Query:**
```sql
SELECT * FROM BUNDLE "users";
```

**Response:**
```json
{
  "ResultCount": 2,
  "Result": [
    {
      "DocumentID": "doc_001",
      "name": "Alice Johnson",
      "email": "alice@example.com",
      "age": 28,
      "status": "active",
      "CreatedAt": "2024-01-15T10:30:00Z",
      "UpdatedAt": "2024-01-15T10:30:00Z"
    },
    {
      "DocumentID": "doc_002",
      "name": "Bob Smith",
      "email": "bob@example.com",
      "age": 35,
      "status": "active",
      "CreatedAt": "2024-01-16T14:22:00Z",
      "UpdatedAt": "2024-01-16T14:22:00Z"
    }
  ],
  "ExecutionTimeMS": 1.23
}
```

### Document Fields

Every document includes these standard fields:

| Field | Type | Description |
|-------|------|-------------|
| **DocumentID** | `string` | Unique document identifier |
| **CreatedAt** | `string` | ISO 8601 timestamp of creation |
| **UpdatedAt** | `string` | ISO 8601 timestamp of last update |
| **...** | `varies` | User-defined fields from bundle schema |

### SELECT with Field Projection

Query specific fields only:

**Query:**
```sql
SELECT name, email FROM BUNDLE "users";
```

**Response:**
```json
{
  "ResultCount": 2,
  "Result": [
    {
      "name": "Alice Johnson",
      "email": "alice@example.com"
    },
    {
      "name": "Bob Smith",
      "email": "bob@example.com"
    }
  ],
  "ExecutionTimeMS": 0.85
}
```

**Note:** When specific fields are selected, standard fields (DocumentID, CreatedAt, UpdatedAt) are **excluded** unless explicitly requested.

### SELECT with WHERE Clause

Query with filtering:

**Query:**
```sql
SELECT * FROM BUNDLE "users" WHERE age > 30;
```

**Response:**
```json
{
  "ResultCount": 1,
  "Result": [
    {
      "DocumentID": "doc_002",
      "name": "Bob Smith",
      "email": "bob@example.com",
      "age": 35,
      "status": "active",
      "CreatedAt": "2024-01-16T14:22:00Z",
      "UpdatedAt": "2024-01-16T14:22:00Z"
    }
  ],
  "ExecutionTimeMS": 1.45
}
```

### SELECT with ORDER BY

Query with sorting:

**Query:**
```sql
SELECT * FROM BUNDLE "users" ORDER BY age DESC;
```

**Response:**
```json
{
  "ResultCount": 2,
  "Result": [
    {
      "DocumentID": "doc_002",
      "name": "Bob Smith",
      "age": 35
    },
    {
      "DocumentID": "doc_001",
      "name": "Alice Johnson",
      "age": 28
    }
  ],
  "ExecutionTimeMS": 1.67
}
```

**Note:** Results are ordered according to the ORDER BY clause before being returned.

### SELECT with LIMIT

Query with result limiting:

**Query:**
```sql
SELECT * FROM BUNDLE "users" LIMIT 1;
```

**Response:**
```json
{
  "ResultCount": 1,
  "Result": [
    {
      "DocumentID": "doc_001",
      "name": "Alice Johnson",
      "email": "alice@example.com"
    }
  ],
  "ExecutionTimeMS": 0.92
}
```

---

## JOIN Query Results

### Basic JOIN (Flat Format)

JOIN without `WITH RELATIONSHIP` returns a **flat format** with separate left and right documents:

**Query:**
```sql
SELECT * FROM BUNDLE "users" 
JOIN BUNDLE "orders" ON users.id = orders.user_id;
```

**Response:**
```json
{
  "ResultCount": 2,
  "Result": [
    {
      "LeftDocument": {
        "DocumentID": "user_001",
        "id": "user_1",
        "name": "Alice Johnson",
        "email": "alice@example.com"
      },
      "RightDocument": {
        "DocumentID": "order_001",
        "id": "order_1",
        "user_id": "user_1",
        "total": 99.99,
        "status": "shipped"
      },
      "JoinKey": "user_1"
    },
    {
      "LeftDocument": {
        "DocumentID": "user_001",
        "id": "user_1",
        "name": "Alice Johnson",
        "email": "alice@example.com"
      },
      "RightDocument": {
        "DocumentID": "order_002",
        "id": "order_2",
        "user_id": "user_1",
        "total": 149.99,
        "status": "delivered"
      },
      "JoinKey": "user_1"
    }
  ],
  "ExecutionTimeMS": 3.56
}
```

### JoinedDocument Structure

| Field | Type | Description |
|-------|------|-------------|
| **LeftDocument** | `object` | Document from left bundle (e.g., users) |
| **RightDocument** | `object` | Document from right bundle (e.g., orders) |
| **JoinKey** | `string` | The value that caused the join match |

### JOIN with Field Selection

**Query:**
```sql
SELECT users.name, orders.total 
FROM BUNDLE "users" 
JOIN BUNDLE "orders" ON users.id = orders.user_id;
```

**Response:**
```json
{
  "ResultCount": 2,
  "Result": [
    {
      "LeftDocument": {
        "name": "Alice Johnson"
      },
      "RightDocument": {
        "total": 99.99
      },
      "JoinKey": "user_1"
    },
    {
      "LeftDocument": {
        "name": "Alice Johnson"
      },
      "RightDocument": {
        "total": 149.99
      },
      "JoinKey": "user_1"
    }
  ],
  "ExecutionTimeMS": 2.89
}
```

---

## WITH RELATIONSHIP Results

When using `WITH RELATIONSHIP`, SyndrDB transforms the flat JOIN results into a **hierarchical structure** where child documents are nested within parent documents.

### 1:Many Relationship

**Relationship Definition:**
```sql
CREATE RELATIONSHIP "UserOrders"
  FROM BUNDLE "users" FIELD "id"
  TO BUNDLE "orders" FIELD "user_id"
  TYPE "1toMany";
```

**Query:**
```sql
SELECT * FROM BUNDLE "users" 
JOIN BUNDLE "orders" ON users.id = orders.user_id
WITH RELATIONSHIP "UserOrders";
```

**Response:**
```json
{
  "ResultCount": 1,
  "Result": [
    {
      "DocumentID": "user_001",
      "id": "user_1",
      "name": "Alice Johnson",
      "email": "alice@example.com",
      "CreatedAt": "2024-01-15T10:30:00Z",
      "UpdatedAt": "2024-01-15T10:30:00Z",
      "orders": [
        {
          "DocumentID": "order_001",
          "id": "order_1",
          "user_id": "user_1",
          "total": 99.99,
          "status": "shipped",
          "CreatedAt": "2024-01-20T09:15:00Z",
          "UpdatedAt": "2024-01-20T09:15:00Z"
        },
        {
          "DocumentID": "order_002",
          "id": "order_2",
          "user_id": "user_1",
          "total": 149.99,
          "status": "delivered",
          "CreatedAt": "2024-01-21T11:30:00Z",
          "UpdatedAt": "2024-01-21T11:30:00Z"
        }
      ]
    }
  ],
  "ExecutionTimeMS": 4.23
}
```

### Hierarchical Structure

🔹 **Parent Document:** Contains all fields from the left bundle (users)  
🔹 **Nested Field:** Named after the right bundle (e.g., `"orders"`)  
🔹 **Child Array:** Contains all matching documents from the right bundle  
🔹 **Result Count:** Number of **parent** documents (not total joined rows)  

### Key Differences: Flat vs Hierarchical

| Aspect | Flat JOIN | WITH RELATIONSHIP |
|--------|-----------|-------------------|
| **Structure** | Array of `{LeftDocument, RightDocument}` pairs | Array of parent documents with nested children |
| **Duplicates** | Parent repeated for each child | Parent appears once with all children nested |
| **Result Count** | Total matched rows | Number of parent documents |
| **Nested Field** | N/A | Child bundle name (lowercase) |
| **Use Case** | SQL-like results | Document/object-oriented results |

### 1:1 Relationship

**Relationship Definition:**
```sql
CREATE RELATIONSHIP "UserProfile"
  FROM BUNDLE "users" FIELD "id"
  TO BUNDLE "profiles" FIELD "user_id"
  TYPE "1to1";
```

**Query:**
```sql
SELECT * FROM BUNDLE "users" 
JOIN BUNDLE "profiles" ON users.id = profiles.user_id
WITH RELATIONSHIP "UserProfile";
```

**Response:**
```json
{
  "ResultCount": 1,
  "Result": [
    {
      "DocumentID": "user_001",
      "id": "user_1",
      "name": "Alice Johnson",
      "email": "alice@example.com",
      "profiles": {
        "DocumentID": "profile_001",
        "user_id": "user_1",
        "bio": "Software engineer",
        "avatar": "https://example.com/avatar1.jpg"
      }
    }
  ],
  "ExecutionTimeMS": 2.15
}
```

**Note:** For `1to1` relationships, the nested field is a **single object**, not an array.

### Many:Many Relationship

**Relationship Definition:**
```sql
CREATE RELATIONSHIP "StudentCourses"
  FROM BUNDLE "students" FIELD "id"
  TO BUNDLE "courses" FIELD "student_id"
  TYPE "ManytoMany";
```

**Query:**
```sql
SELECT * FROM BUNDLE "students" 
JOIN BUNDLE "courses" ON students.id = courses.student_id
WITH RELATIONSHIP "StudentCourses";
```

**Response:**
```json
{
  "ResultCount": 2,
  "Result": [
    {
      "DocumentID": "student_001",
      "id": "stud_1",
      "name": "Alice Johnson",
      "courses": [
        {
          "DocumentID": "course_001",
          "course_id": "cs101",
          "student_id": "stud_1",
          "title": "Introduction to Programming"
        },
        {
          "DocumentID": "course_002",
          "course_id": "math201",
          "student_id": "stud_1",
          "title": "Calculus II"
        }
      ]
    },
    {
      "DocumentID": "student_002",
      "id": "stud_2",
      "name": "Bob Smith",
      "courses": [
        {
          "DocumentID": "course_003",
          "course_id": "cs101",
          "student_id": "stud_2",
          "title": "Introduction to Programming"
        }
      ]
    }
  ],
  "ExecutionTimeMS": 5.67
}
```

### WITH RELATIONSHIP + Field Selection

**Query:**
```sql
SELECT users.name, users.email, orders.total, orders.status
FROM BUNDLE "users" 
JOIN BUNDLE "orders" ON users.id = orders.user_id
WITH RELATIONSHIP "UserOrders";
```

**Response:**
```json
{
  "ResultCount": 1,
  "Result": [
    {
      "DocumentID": "user_001",
      "name": "Alice Johnson",
      "email": "alice@example.com",
      "orders": [
        {
          "DocumentID": "order_001",
          "total": 99.99,
          "status": "shipped"
        },
        {
          "DocumentID": "order_002",
          "total": 149.99,
          "status": "delivered"
        }
      ]
    }
  ],
  "ExecutionTimeMS": 3.89
}
```

**Note:** Field selection applies to both parent and child documents based on the bundle prefix.

### WITH RELATIONSHIP + WHERE Clause

**Query:**
```sql
SELECT * FROM BUNDLE "users" 
JOIN BUNDLE "orders" ON users.id = orders.user_id
WHERE orders.status = "shipped"
WITH RELATIONSHIP "UserOrders";
```

**Response:**
```json
{
  "ResultCount": 1,
  "Result": [
    {
      "DocumentID": "user_001",
      "id": "user_1",
      "name": "Alice Johnson",
      "email": "alice@example.com",
      "orders": [
        {
          "DocumentID": "order_001",
          "id": "order_1",
          "total": 99.99,
          "status": "shipped"
        }
      ]
    }
  ],
  "ExecutionTimeMS": 3.12
}
```

**Note:** WHERE clause filters **before** hierarchical transformation, so only matching child documents are included.

---

## Aggregate Query Results

### COUNT(*)

**Query:**
```sql
SELECT COUNT(*) FROM BUNDLE "users";
```

**Response:**
```json
{
  "ResultCount": 1,
  "Result": {
    "Count": 150
  },
  "ExecutionTimeMS": 0.45
}
```

### COUNT(*) Result Structure

| Field | Type | Value |
|-------|------|-------|
| **ResultCount** | `int` | Always `1` for COUNT queries |
| **Result** | `object` | Contains `{"Count": <number>}` |
| **Count** | `int` | Total number of documents |

### COUNT with WHERE

**Query:**
```sql
SELECT COUNT(*) FROM BUNDLE "users" WHERE status = "active";
```

**Response:**
```json
{
  "ResultCount": 1,
  "Result": {
    "Count": 87
  },
  "ExecutionTimeMS": 1.23
}
```

### GROUP BY with COUNT

**Query:**
```sql
SELECT status, COUNT(*) FROM BUNDLE "users" GROUP BY status;
```

**Response:**
```json
{
  "ResultCount": 3,
  "Result": [
    {
      "status": "active",
      "count_all": 87
    },
    {
      "status": "inactive",
      "count_all": 45
    },
    {
      "status": "suspended",
      "count_all": 18
    }
  ],
  "ExecutionTimeMS": 2.56
}
```

**Note:** Aggregate functions in GROUP BY create fields named `count_all`, `sum_<field>`, `avg_<field>`, etc.

---

## DDL Command Results

### CREATE DATABASE

**Command:**
```sql
CREATE DATABASE "analytics";
```

**Response:**
```json
{
  "ResultCount": 1,
  "Result": {
    "message": "Database 'analytics' created successfully",
    "database_name": "analytics"
  },
  "ExecutionTimeMS": 45.67
}
```

### CREATE BUNDLE

**Command:**
```sql
CREATE BUNDLE "users" WITH FIELDS (
    {"id", STRING, true, true},
    {"name", STRING, true, false}
);
```

**Response:**
```json
{
  "ResultCount": 1,
  "Result": {
    "message": "Bundle 'users' created successfully",
    "bundle_name": "users",
    "field_count": 2
  },
  "ExecutionTimeMS": 23.45
}
```

### CREATE RELATIONSHIP

**Command:**
```sql
CREATE RELATIONSHIP "UserOrders"
  FROM BUNDLE "users" FIELD "id"
  TO BUNDLE "orders" FIELD "user_id"
  TYPE "1toMany";
```

**Response:**
```json
{
  "ResultCount": 1,
  "Result": {
    "message": "Relationship 'UserOrders' created successfully",
    "relationship_name": "UserOrders",
    "type": "1toMany",
    "from_bundle": "users",
    "to_bundle": "orders"
  },
  "ExecutionTimeMS": 12.34
}
```

### CREATE INDEX

**Command:**
```sql
CREATE HASH INDEX "email_idx" ON BUNDLE "users" FIELD "email";
```

**Response:**
```json
{
  "ResultCount": 1,
  "Result": {
    "message": "HASH index 'email_idx' created successfully on bundle 'users' field 'email'",
    "index_name": "email_idx",
    "index_type": "HASH",
    "bundle_name": "users",
    "field_name": "email"
  },
  "ExecutionTimeMS": 156.78
}
```

### DROP BUNDLE

**Command:**
```sql
DROP BUNDLE "temp_data";
```

**Response:**
```json
{
  "ResultCount": 1,
  "Result": {
    "message": "Bundle 'temp_data' dropped successfully"
  },
  "ExecutionTimeMS": 89.23
}
```

---

## DML Command Results

### ADD DOCUMENT

**Command:**
```sql
ADD DOCUMENT TO BUNDLE "users" WITH (
    {"name" = "Alice Johnson"},
    {"email" = "alice@example.com"},
    {"age" = 28}
);
```

**Response:**
```json
{
  "ResultCount": 1,
  "Result": {
    "message": "Document added successfully to bundle 'users'",
    "document_id": "doc_7f3a9b2c",
    "bundle_name": "users"
  },
  "ExecutionTimeMS": 5.67
}
```

### UPDATE DOCUMENTS

**Command:**
```sql
UPDATE DOCUMENTS IN BUNDLE "users" 
(status = "active") 
WHERE age > 18;
```

**Response:**
```json
{
  "ResultCount": 1,
  "Result": {
    "message": "Successfully updated 47 documents in bundle 'users'",
    "updated_count": 47,
    "bundle_name": "users"
  },
  "ExecutionTimeMS": 23.45
}
```

### DELETE DOCUMENTS

**Command:**
```sql
DELETE DOCUMENTS FROM BUNDLE "users" WHERE status = "deleted";
```

**Response:**
```json
{
  "ResultCount": 1,
  "Result": {
    "message": "Successfully deleted 12 documents from bundle 'users'",
    "deleted_ids": [
      "doc_001",
      "doc_005",
      "doc_007",
      "doc_012",
      "doc_015",
      "doc_023",
      "doc_034",
      "doc_045",
      "doc_067",
      "doc_089",
      "doc_091",
      "doc_102"
    ]
  },
  "ExecutionTimeMS": 15.23
}
```

### DELETE DOCUMENTS (No Matches)

**Command:**
```sql
DELETE DOCUMENTS FROM BUNDLE "users" WHERE email = "nonexistent@example.com";
```

**Response:**
```json
{
  "ResultCount": 0,
  "Result": {
    "message": "No documents matched the WHERE clause in bundle 'users'"
  },
  "ExecutionTimeMS": 2.34
}
```

---

## SHOW Command Results

### SHOW DATABASES

**Command:**
```sql
SHOW DATABASES;
```

**Response:**
```json
{
  "ResultCount": 3,
  "Result": [
    "primary",
    "myapp",
    "analytics"
  ],
  "ExecutionTimeMS": 0.45
}
```

### SHOW BUNDLES

**Command:**
```sql
USE "myapp";
SHOW BUNDLES;
```

**Response:**
```json
{
  "ResultCount": 4,
  "Result": [
    {
      "Name": "users",
      "BundleID": "bnd_1234567890",
      "DatabaseName": "myapp",
      "FieldCount": 5,
      "FilePath": "./data_files/myapp/users.bnd"
    },
    {
      "Name": "orders",
      "BundleID": "bnd_0987654321",
      "DatabaseName": "myapp",
      "FieldCount": 8,
      "FilePath": "./data_files/myapp/orders.bnd"
    },
    {
      "Name": "products",
      "BundleID": "bnd_1122334455",
      "DatabaseName": "myapp",
      "FieldCount": 6,
      "FilePath": "./data_files/myapp/products.bnd"
    },
    {
      "Name": "reviews",
      "BundleID": "bnd_5544332211",
      "DatabaseName": "myapp",
      "FieldCount": 4,
      "FilePath": "./data_files/myapp/reviews.bnd"
    }
  ],
  "ExecutionTimeMS": 1.23
}
```

### SHOW BUNDLE

**Command:**
```sql
USE "myapp";
SHOW BUNDLE "users";
```

**Response:**
```json
{
  "ResultCount": 1,
  "Result": {
    "Name": "users",
    "BundleID": "bnd_1234567890",
    "Description": "User accounts",
    "CreatedBy": "admin",
    "CreatedAt": "2024-01-15T10:30:00Z",
    "UpdatedAt": "2024-01-20T14:22:00Z",
    "PageCount": 5,
    "TotalDocuments": 1250,
    "DocumentStructure": {
      "FieldDefinitions": {
        "id": {
          "Name": "id",
          "Type": "STRING",
          "IsRequired": true,
          "IsUnique": true
        },
        "name": {
          "Name": "name",
          "Type": "STRING",
          "IsRequired": true,
          "IsUnique": false
        },
        "email": {
          "Name": "email",
          "Type": "STRING",
          "IsRequired": true,
          "IsUnique": true
        },
        "age": {
          "Name": "age",
          "Type": "INT",
          "IsRequired": false,
          "IsUnique": false
        }
      }
    },
    "Indexes": {
      "email_hash_idx": {
        "IndexName": "email_hash_idx",
        "IndexType": "HASH",
        "FieldName": "email",
        "BundleName": "users"
      }
    },
    "Relationships": {
      "user_orders": {
        "RelationshipName": "user_orders",
        "SourceBundle": "users",
        "SourceField": "id",
        "DestinationBundle": "orders",
        "DestinationField": "user_id",
        "RelationshipType": "1toMany"
      }
    }
  },
  "ExecutionTimeMS": 2.67
}
```

### SHOW USERS

**Command:**
```sql
SHOW USERS;
```

**Response:**
```json
{
  "ResultCount": 3,
  "Result": [
    {
      "DocumentID": "doc_1234567890",
      "UserID": "user_001",
      "Name": "alice",
      "IsActive": true,
      "IsLockedOut": false,
      "FailedLoginAttempts": 0,
      "LockoutExpiresOn": null
    },
    {
      "DocumentID": "doc_0987654321",
      "UserID": "user_002",
      "Name": "bob",
      "IsActive": true,
      "IsLockedOut": false,
      "FailedLoginAttempts": 0,
      "LockoutExpiresOn": null
    },
    {
      "DocumentID": "doc_1122334455",
      "UserID": "user_003",
      "Name": "charlie",
      "IsActive": false,
      "IsLockedOut": true,
      "FailedLoginAttempts": 5,
      "LockoutExpiresOn": "2024-01-20T15:00:00Z"
    }
  ],
  "ExecutionTimeMS": 3.45
}
```

**Note:** `PasswordHash` field is **never** returned for security reasons.

---

## Error Responses

When an error occurs, SyndrDB returns an error message instead of a CommandResponse.

### Syntax Error

**Command:**
```sql
SELECT * FORM BUNDLE "users";  -- Typo: FORM instead of FROM
```

**Response:**
```json
{
  "error": "syntax error: unexpected FORM, expecting FROM",
  "line": 1,
  "column": 10
}
```

### Bundle Not Found

**Command:**
```sql
SELECT * FROM BUNDLE "nonexistent";
```

**Response:**
```json
{
  "error": "error retrieving bundle 'nonexistent': bundle not found"
}
```

### WHERE Clause Error

**Command:**
```sql
SELECT * FROM BUNDLE "users" WHERE age > "thirty";  -- Type mismatch
```

**Response:**
```json
{
  "error": "type mismatch: cannot compare INT field 'age' with STRING value 'thirty'"
}
```

### Permission Denied

**Command:**
```sql
DROP DATABASE "production";  -- User lacks permission
```

**Response:**
```json
{
  "error": "permission denied: user 'bob' does not have permission to drop database 'production'"
}
```

### Constraint Violation

**Command:**
```sql
ADD DOCUMENT TO BUNDLE "users" WITH (
    {"email" = "alice@example.com"}  -- Duplicate unique field
);
```

**Response:**
```json
{
  "error": "unique constraint violation: field 'email' with value 'alice@example.com' already exists in bundle 'users'"
}
```

---

## Field Type Representations

SyndrDB field types are represented in JSON responses as follows:

### Type Mapping Table

| SyndrDB Type | JSON Type | Example Value | Notes |
|--------------|-----------|---------------|-------|
| **STRING** | `string` | `"Hello World"` | UTF-8 encoded text |
| **INT** | `number` | `42` | 64-bit signed integer |
| **FLOAT** | `number` | `3.14159` | 64-bit floating point |
| **BOOLEAN** | `boolean` | `true` or `false` | Boolean value |
| **DATETIME** | `string` | `"2024-01-15T10:30:00Z"` | ISO 8601 format (RFC3339) |
| **TIMESTAMP** | `string` | `"2024-01-15T10:30:00Z"` | ISO 8601 format (RFC3339) |
| **JSON** | `object` or `array` | `{"key": "value"}` | Nested JSON structure |
| **TEXT** | `string` | `"Long text..."` | Large text data |
| **BLOB** | `string` | `"base64data=="` | Base64-encoded binary |

### Example with All Types

**Bundle Schema:**
```sql
CREATE BUNDLE "datatypes" WITH FIELDS (
    {"id", STRING, true, true},
    {"name", STRING, true, false},
    {"count", INT, true, false},
    {"price", FLOAT, true, false},
    {"active", BOOLEAN, true, false},
    {"created", DATETIME, true, false},
    {"metadata", JSON, false, false},
    {"description", TEXT, false, false}
);
```

**Query:**
```sql
SELECT * FROM BUNDLE "datatypes" LIMIT 1;
```

**Response:**
```json
{
  "ResultCount": 1,
  "Result": [
    {
      "DocumentID": "doc_001",
      "id": "item_123",
      "name": "Premium Widget",
      "count": 150,
      "price": 99.99,
      "active": true,
      "created": "2024-01-15T10:30:00Z",
      "metadata": {
        "category": "electronics",
        "tags": ["featured", "bestseller"],
        "ratings": {
          "average": 4.5,
          "count": 234
        }
      },
      "description": "This is a high-quality premium widget designed for professional use...",
      "CreatedAt": "2024-01-15T10:30:00Z",
      "UpdatedAt": "2024-01-20T14:22:00Z"
    }
  ],
  "ExecutionTimeMS": 1.23
}
```

### NULL Values

Fields can be `null` if they are optional and not set:

```json
{
  "DocumentID": "doc_002",
  "id": "item_124",
  "name": "Basic Widget",
  "count": 50,
  "price": 19.99,
  "active": true,
  "created": "2024-01-16T09:00:00Z",
  "metadata": null,
  "description": null
}
```

---

## Complete Examples

### Example 1: E-Commerce Query

**Scenario:** Get all active users with their orders

**Relationship:**
```sql
CREATE RELATIONSHIP "UserOrders"
  FROM BUNDLE "users" FIELD "id"
  TO BUNDLE "orders" FIELD "user_id"
  TYPE "1toMany";
```

**Query:**
```sql
SELECT * FROM BUNDLE "users" 
JOIN BUNDLE "orders" ON users.id = orders.user_id
WHERE users.status = "active"
WITH RELATIONSHIP "UserOrders";
```

**Response:**
```json
{
  "ResultCount": 2,
  "Result": [
    {
      "DocumentID": "user_001",
      "id": "user_1",
      "name": "Alice Johnson",
      "email": "alice@example.com",
      "status": "active",
      "CreatedAt": "2024-01-15T10:30:00Z",
      "UpdatedAt": "2024-01-15T10:30:00Z",
      "orders": [
        {
          "DocumentID": "order_001",
          "id": "order_1",
          "user_id": "user_1",
          "total": 99.99,
          "status": "shipped",
          "items": ["item_1", "item_2"],
          "CreatedAt": "2024-01-20T09:15:00Z",
          "UpdatedAt": "2024-01-20T09:15:00Z"
        },
        {
          "DocumentID": "order_002",
          "id": "order_2",
          "user_id": "user_1",
          "total": 149.99,
          "status": "delivered",
          "items": ["item_3"],
          "CreatedAt": "2024-01-21T11:30:00Z",
          "UpdatedAt": "2024-01-21T11:30:00Z"
        }
      ]
    },
    {
      "DocumentID": "user_002",
      "id": "user_2",
      "name": "Bob Smith",
      "email": "bob@example.com",
      "status": "active",
      "CreatedAt": "2024-01-16T14:22:00Z",
      "UpdatedAt": "2024-01-16T14:22:00Z",
      "orders": [
        {
          "DocumentID": "order_003",
          "id": "order_3",
          "user_id": "user_2",
          "total": 75.50,
          "status": "processing",
          "items": ["item_4", "item_5"],
          "CreatedAt": "2024-01-22T08:45:00Z",
          "UpdatedAt": "2024-01-22T08:45:00Z"
        }
      ]
    }
  ],
  "ExecutionTimeMS": 5.67
}
```

### Example 2: Analytics Query

**Scenario:** Count users by status

**Query:**
```sql
SELECT status, COUNT(*) FROM BUNDLE "users" GROUP BY status;
```

**Response:**
```json
{
  "ResultCount": 3,
  "Result": [
    {
      "status": "active",
      "count_all": 1250
    },
    {
      "status": "inactive",
      "count_all": 345
    },
    {
      "status": "suspended",
      "count_all": 23
    }
  ],
  "ExecutionTimeMS": 12.45
}
```

### Example 3: Complex Filtered Join

**Scenario:** Get high-value orders for premium users

**Query:**
```sql
SELECT users.name, users.email, orders.id, orders.total 
FROM BUNDLE "users" 
JOIN BUNDLE "orders" ON users.id = orders.user_id
WHERE users.tier = "premium" AND orders.total > 100
WITH RELATIONSHIP "UserOrders";
```

**Response:**
```json
{
  "ResultCount": 2,
  "Result": [
    {
      "DocumentID": "user_005",
      "name": "Premium User 1",
      "email": "premium1@example.com",
      "orders": [
        {
          "DocumentID": "order_010",
          "id": "order_10",
          "total": 299.99
        },
        {
          "DocumentID": "order_015",
          "id": "order_15",
          "total": 450.00
        }
      ]
    },
    {
      "DocumentID": "user_007",
      "name": "Premium User 2",
      "email": "premium2@example.com",
      "orders": [
        {
          "DocumentID": "order_012",
          "id": "order_12",
          "total": 199.99
        }
      ]
    }
  ],
  "ExecutionTimeMS": 8.92
}
```

### Example 4: Batch Operations

**Scenario:** Add multiple documents and verify

**Commands:**
```sql
-- Add documents
ADD DOCUMENT TO BUNDLE "users" WITH ({"name" = "Alice"}, {"email" = "alice@example.com"});
ADD DOCUMENT TO BUNDLE "users" WITH ({"name" = "Bob"}, {"email" = "bob@example.com"});
ADD DOCUMENT TO BUNDLE "users" WITH ({"name" = "Charlie"}, {"email" = "charlie@example.com"});

-- Verify
SELECT COUNT(*) FROM BUNDLE "users";
```

**Final Response:**
```json
{
  "ResultCount": 1,
  "Result": {
    "Count": 3
  },
  "ExecutionTimeMS": 0.78
}
```

---

## Best Practices

### Response Handling

✅ **DO:**
- Always check `ResultCount` before accessing `Result`
- Handle empty results gracefully (`ResultCount: 0`)
- Parse `ExecutionTimeMS` for performance monitoring
- Type-check field values when processing JSON
- Handle `null` values in optional fields

❌ **DON'T:**
- Assume `Result` is always an array (it varies by command)
- Ignore `ResultCount` (it indicates actual data returned)
- Parse timestamps without timezone awareness
- Skip error handling for malformed responses

### Performance Considerations

🔹 **Field Projection:** Request only needed fields to reduce response size  
🔹 **LIMIT Clause:** Use LIMIT for large result sets to avoid memory issues  
🔹 **WITH RELATIONSHIP:** Understand that hierarchical transformation adds overhead  
🔹 **Monitoring:** Track `ExecutionTimeMS` to identify slow queries  

### Working with Hierarchical Results

When using `WITH RELATIONSHIP`:

1. **Parent Count:** `ResultCount` reflects **parent documents**, not total rows
2. **Nested Arrays:** Child documents are in an array named after the bundle
3. **Empty Relationships:** Parents without children will have an empty array `[]`
4. **Field Selection:** Applies to both parent and child documents

---

**End of SyndrDB Results & Output Format Documentation**
