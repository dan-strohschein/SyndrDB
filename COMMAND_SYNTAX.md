# SyndrDB Command Syntax Reference

This document contains all possible commands that can be sent to the SyndrDB server from a client.

## Database Management Commands

### CREATE DATABASE
Creates a new database with the specified name.
```
CREATE DATABASE "<DATABASE_NAME>";
```

### DELETE DATABASE  
Deletes an existing database and all its contents.
```
DELETE DATABASE "<DATABASE_NAME>";
```

### SELECT DATABASES
Retrieves information about a specific database.
```
SELECT DATABASES "<DATABASE_NAME>";
```

### USE
Switches the current session context to use a specific database.
```
USE "<DATABASE_NAME>";
```

## Bundle Management Commands

### CREATE BUNDLE
Creates a new bundle with field definitions.
```
CREATE BUNDLE "<BUNDLE_NAME>" 
WITH FIELDS (
    {"<FIELDNAME>", <FIELDTYPE>, <REQUIRED>, <UNIQUE>},
    {"<FIELDNAME>", <FIELDTYPE>, <REQUIRED>, <UNIQUE>}
);
```

### DELETE BUNDLE
Deletes an empty bundle from the database.
```
DELETE BUNDLE "<BUNDLE_NAME>";
```

### UPDATE BUNDLE
Updates bundle structure or metadata.
```
UPDATE BUNDLE "<BUNDLE_NAME>" [update_operations];
```

## Document Management Commands

### ADD DOCUMENT
Adds a new document to a bundle.
```
ADD DOCUMENT TO BUNDLE "<BUNDLE_NAME>" 
WITH ({key=value, key=value});
```

### SELECT DOCUMENTS
Queries documents from a bundle with optional filtering.
```
SELECT DOCUMENTS FROM "<BUNDLE_NAME>";
```
```
SELECT DOCUMENTS FROM "<BUNDLE_NAME>" WHERE <CONDITIONS>;
```
```
SELECT * FROM "<BUNDLE_NAME>";
```

### SELECT TOP DOCUMENTS
Queries a limited number of documents from a bundle, returning the top N documents.
```
SELECT TOP <NUMBER> DOCUMENTS FROM "<BUNDLE_NAME>";
```
```
SELECT TOP <NUMBER> DOCUMENTS FROM "<BUNDLE_NAME>" WHERE <CONDITIONS>;
```
```
SELECT TOP <NUMBER> DOCUMENTS FROM "<BUNDLE_NAME>" ORDER BY <FIELD_NAME> [ASC|DESC];
```
```
SELECT TOP <NUMBER> DOCUMENTS FROM "<BUNDLE_NAME>" WHERE <CONDITIONS> ORDER BY <FIELD_NAME> [ASC|DESC];
```

### SELECT DOCUMENTS with JOIN
Performs JOIN operations between bundles.
```
SELECT DOCUMENTS FROM "<BUNDLE_NAME>" 
JOIN "<OTHER_BUNDLE>" ON <JOIN_CONDITIONS> 
WHERE <CONDITIONS>;
```

### SELECT DOCUMENTS with ORDER BY
Queries documents with result ordering.
```
SELECT DOCUMENTS FROM "<BUNDLE_NAME>" 
WHERE <CONDITIONS> 
ORDER BY <FIELD_NAME> [ASC|DESC];
```

### SELECT DOCUMENTS with GROUP BY
Queries documents with result grouping.
```
SELECT DOCUMENTS FROM "<BUNDLE_NAME>" 
WHERE <CONDITIONS> 
GROUP BY <FIELD_NAME>;
```

### UPDATE DOCUMENTS
Updates existing documents in a bundle.
```
UPDATE DOCUMENTS IN BUNDLE "<BUNDLE_NAME>" 
({key=new_value, key=new_value}) 
WHERE <CONDITIONS>;
```

### DELETE DOCUMENTS
Deletes documents from a bundle based on conditions.
```
DELETE DOCUMENTS FROM BUNDLE "<BUNDLE_NAME>" 
WHERE <CONDITIONS>;
```

## Index Management Commands

### CREATE BTREE INDEX
Creates a B-Tree index on bundle fields.
```
CREATE BTREE INDEX "<INDEX_NAME>" ON BUNDLE "<BUNDLE_NAME>" 
(<FIELD_NAME>, <FIELD_NAME>);
```

### CREATE HASH INDEX  
Creates a Hash index on bundle fields.
```
CREATE HASH INDEX "<INDEX_NAME>" ON BUNDLE "<BUNDLE_NAME>" 
(<FIELD_NAME>);
```

## Information Display Commands

### SHOW DATABASES
Lists all available databases.
```
SHOW DATABASES;
```

### SHOW BUNDLES
Lists all bundles in the current database or specified database.
```
SHOW BUNDLES;
```
```
SHOW BUNDLES FOR "<DATABASE_NAME>";
```

### SHOW BUNDLE
Shows detailed information about a specific bundle.
```
SHOW BUNDLE "<BUNDLE_NAME>";
```

### SHOW USERS
Shows all documents in the Users bundle from the primary database.
```
SHOW USERS;
```

### SHOW RATE LIMIT
Displays current rate limiting information.
```
SHOW RATE LIMIT;
```

## Permission and Security Commands

### GRANT
Grants permissions to users or roles.
```
GRANT <PERMISSION_TYPE> ON <RESOURCE> TO <USER_OR_ROLE>;
```

## Connection Management Commands

### ATTACH
Attaches additional resources or connections.
```
ATTACH <RESOURCE_SPECIFICATION>;
```

### INVALIDATE SESSION
Invalidates the current session or specified session.
```
INVALIDATE SESSION;
```
```
INVALIDATE SESSION "<SESSION_ID>";
```

## Field Types
When defining bundle fields, the following types are supported:
- STRING
- INTEGER  
- FLOAT
- BOOLEAN
- DATE
- TIMESTAMP

## Condition Syntax
WHERE clauses support:
- Equality: `field_name == "value"`
- Inequality: `field_name != "value"`
- Comparisons: `field_name > value`, `field_name < value`, `field_name >= value`, `field_name <= value`
- Logical operators: `AND`, `OR`, `NOT`
- Parentheses for grouping: `(condition1 OR condition2) AND condition3`
- IN operator: `field_name IN (value1, value2, value3)`
- NOT IN operator: `field_name NOT IN (value1, value2, value3)`

### IN and NOT IN Operators

The IN and NOT IN operators allow filtering documents based on whether a field's value matches any value in a specified list.

**Basic Syntax:**
```
SELECT DOCUMENTS FROM "<BUNDLE_NAME>" WHERE "<FIELD_NAME>" IN (value1, value2, value3);
SELECT DOCUMENTS FROM "<BUNDLE_NAME>" WHERE "<FIELD_NAME>" NOT IN (value1, value2, value3);
```

**Examples:**

1. **Simple IN query:**
```
SELECT DOCUMENTS FROM "Users" WHERE "Status" IN ("active", "pending", "verified");
```

2. **Numeric IN query:**
```
SELECT DOCUMENTS FROM "Products" WHERE "CategoryID" IN (1, 2, 5, 10);
```

3. **NOT IN query:**
```
SELECT DOCUMENTS FROM "Orders" WHERE "Status" NOT IN ("cancelled", "refunded");
```

4. **Case-insensitive IN query (using N prefix):**
```
SELECT DOCUMENTS FROM "Users" WHERE "Email" IN N("john@example.com", "jane@example.com");
```

5. **Date IN query:**
```
SELECT DOCUMENTS FROM "Events" WHERE "EventDate" IN ("2025-01-15", "2025-02-20", "2025-03-10");
```

6. **Combined with other conditions:**
```
SELECT DOCUMENTS FROM "Products" WHERE "Status" == "active" AND "CategoryID" IN (1, 2, 3);
```

**Important Features:**

- **Type Consistency**: All values in the IN list must be of the same type (all strings, all numbers, or all dates). Type coercion is not supported.

- **Case Sensitivity**: By default, string comparisons are case-sensitive. Use the `N` prefix for case-insensitive matching:
  ```
  IN N("value1", "value2")  // Case-insensitive
  IN ("value1", "value2")   // Case-sensitive
  ```

- **NULL Handling**: To check for NULL values, use the special `::SYNDR_NULL::` value:
  ```
  WHERE "Email" IN ("john@example.com", "::SYNDR_NULL::")
  ```
  Other NULL magic values: `::SYNDR_MISSING::`, `::SYNDR_DELETED::`, `::SYNDR_DEFAULT::`

- **Automatic Deduplication**: Duplicate values in the IN list are automatically removed. This is logged in debug mode.

- **List Size Limits**: 
  - Maximum 10,000 values per IN list
  - Warnings logged for lists >1,000 values
  - Query throttling may apply to very large IN queries

- **Single-Value Optimization**: IN queries with a single value are automatically optimized to equality operators:
  ```
  WHERE "Status" IN ("active")  // Optimized to: "Status" == "active"
  ```

**Performance Considerations:**

- **Index Usage**: IN queries can utilize hash indexes on the queried field for better performance
- **Hash Set Lookups**: Values are converted to hash sets internally for O(1) lookup time
- **Memory Tracking**: Large IN queries (>100MB memory) trigger warnings
- **Query Throttling**: Large IN queries (>1,000 values) may be subject to concurrent query limits
- **Statistics**: IN query patterns are tracked for optimization (admin-only access)

**Best Practices:**

1. Keep IN lists reasonably sized (<1,000 values) for optimal performance
2. Use indexes on fields frequently queried with IN operators
3. Consider breaking very large IN queries into smaller batches
4. Use case-insensitive matching (N prefix) only when necessary
5. For negation, prefer NOT IN over multiple != conditions

## Notes
- All database and bundle names must be enclosed in double quotes
- Commands are case-insensitive but conventionally written in UPPERCASE
- Commands should end with semicolon (;) but it's optional for most operations
- Field values in document operations use key=value pairs within parentheses
- Multi-line commands are supported with whitespace normalization