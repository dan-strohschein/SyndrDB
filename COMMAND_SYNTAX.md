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

## Notes
- All database and bundle names must be enclosed in double quotes
- Commands are case-insensitive but conventionally written in UPPERCASE
- Commands should end with semicolon (;) but it's optional for most operations
- Field values in document operations use key=value pairs within parentheses
- Multi-line commands are supported with whitespace normalization