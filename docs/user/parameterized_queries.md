# Parameterized Queries

SyndrDB supports PostgreSQL-style parameterized queries with `$1, $2, $3` placeholder syntax. Parameterized queries provide improved security against SQL injection attacks and enable query plan caching for better performance.

## Table of Contents
- [Overview](#overview)
- [Basic Syntax](#basic-syntax)
- [Command Reference](#command-reference)
- [Security Benefits](#security-benefits)
- [Performance Considerations](#performance-considerations)
- [Examples](#examples)
- [Limitations](#limitations)

---

## Overview

Parameterized queries separate SQL logic from data values by using numbered placeholders (`$1`, `$2`, etc.) that are replaced at execution time. This provides two key benefits:

1. **Security**: Parameters are treated as literal values, preventing SQL injection attacks
2. **Performance**: Query plans are cached and reused, reducing parsing overhead

### Architecture

Parameterized queries in SyndrDB consist of three operations:

1. **PREPARE**: Parse and cache a query with placeholders
2. **EXECUTE**: Run the prepared query with specific parameter values
3. **DEALLOCATE**: Remove a prepared statement from the cache

All prepared statements are scoped to a user session and automatically cleaned up when the session expires.

---

## Basic Syntax

### Parameter Placeholders

Parameters use 1-indexed numeric placeholders:
```sql
SELECT * FROM "Authors" WHERE "Name" = $1
SELECT * FROM "Books" WHERE "AuthorID" = $1 AND "Year" > $2
```

**Important Rules:**
- Parameters are numbered sequentially starting from `$1`
- Parameter numbers must be consecutive (no gaps)
- Maximum parameter index is validated during PREPARE
- Field names and bundle names must still be quoted if they contain special characters

### PREPARE Statement

Syntax:
```sql
PREPARE statement_name AS query_with_placeholders
```

The statement name:
- Must be a valid identifier (alphanumeric and underscores only)
- Cannot contain special characters like `-`, `@`, `#`, etc.
- Must be unique within the session
- Is case-sensitive

Example:
```sql
PREPARE get_author AS SELECT * FROM "Authors" WHERE "Name" = $1
```

### EXECUTE Statement

Syntax:
```sql
EXECUTE statement_name
```

Parameters are passed separately using a delimiter-based protocol (see [Protocol Details](#protocol-details) below).

Example with parameters:
```
EXECUTE get_author\x05Strohschein
```

Where `\x05` (ENQ character) separates the command from parameters.

### DEALLOCATE Statement

Syntax:
```sql
DEALLOCATE statement_name
```

Removes a prepared statement from the session cache.

Example:
```sql
DEALLOCATE get_author
```

---

## Command Reference

### PREPARE

**Purpose**: Parse a query and store it in the session cache with placeholders.

**Syntax**:
```sql
PREPARE <statement_name> AS <SELECT_query_with_placeholders>
```

**Parameters**:
- `statement_name`: Unique identifier for this prepared statement
- `SELECT_query_with_placeholders`: Any valid SELECT query containing `$1`, `$2`, etc.

**Returns**: Success message with statement name and parameter count

**Example**:
```sql
PREPARE search_books AS 
  SELECT * FROM "Books" 
  WHERE "Author" = $1 AND "Year" > $2
```

**Response**:
```json
{
  "success": true,
  "message": "Prepared statement 'search_books' created (2 parameters)"
}
```

**Errors**:
- Duplicate statement name in session
- Invalid SQL syntax
- Invalid parameter numbering
- Non-SELECT queries (INSERT/UPDATE/DELETE not supported)

---

### EXECUTE

**Purpose**: Execute a previously prepared statement with specific parameter values.

**Syntax**:
```sql
EXECUTE <statement_name>
```

**Protocol**: Parameters are passed using delimiter-separated format:
```
EXECUTE statement_name\x05param1\x05param2\x05param3
```

Where `\x05` is the ENQ (Enquiry) ASCII control character.

**Parameter Types**: All parameters are passed as strings and automatically converted to appropriate types during evaluation based on the comparison context.

**Returns**: Query results in the same format as regular SELECT queries

**Example**:
```
EXECUTE search_books\x05Strohschein\x052020
```

**Response**:
```json
{
  "success": true,
  "result_count": 5,
  "results": [
    {"DocumentID": "1", "Title": "Book 1", "Author": "Strohschein", "Year": 2021},
    {"DocumentID": "2", "Title": "Book 2", "Author": "Strohschein", "Year": 2022}
  ],
  "execution_time_ms": 15.3
}
```

**Errors**:
- Statement name does not exist
- Wrong number of parameters provided
- Query execution timeout
- Memory limit exceeded

---

### DEALLOCATE

**Purpose**: Remove a prepared statement from the session cache.

**Syntax**:
```sql
DEALLOCATE <statement_name>
```

**Returns**: Success message

**Example**:
```sql
DEALLOCATE search_books
```

**Response**:
```json
{
  "success": true,
  "message": "Prepared statement 'search_books' deallocated"
}
```

**Errors**:
- Statement name does not exist

---

## Protocol Details

### Delimiter-Based Parameter Passing

SyndrDB uses ASCII control character `\x05` (ENQ - Enquiry) as the delimiter between the command and parameters:

```
EXECUTE statement_name\x05param1\x05param2\x05param3
```

### Escape Sequences

If parameter values contain delimiter or control characters, use these escape sequences:

| Character | Escape Sequence |
|-----------|----------------|
| `\x04` (EOT) | `\x04\x04` |
| `\x05` (ENQ) | `\x05\x05` |

**Example with escaped delimiter**:
```
EXECUTE search\x05value_with_\x05\x05_delimiter
```

This passes the parameter value `value_with_\x05_delimiter` (the ENQ character is escaped).

### Client Implementation

Most clients should provide helper functions to automatically format parameterized commands. Example in Go:

```go
// Helper function to escape parameter values
func escapeParameterValue(value string) string {
    value = strings.ReplaceAll(value, "\x04", "\x04\x04") // Escape EOT
    value = strings.ReplaceAll(value, "\x05", "\x05\x05") // Escape ENQ
    return value
}

// Build parameterized command
func buildExecuteCommand(stmtName string, params []string) string {
    var sb strings.Builder
    sb.WriteString("EXECUTE ")
    sb.WriteString(stmtName)
    for _, param := range params {
        sb.WriteString("\x05") // Delimiter
        sb.WriteString(escapeParameterValue(param))
    }
    return sb.String()
}

// Usage
command := buildExecuteCommand("get_author", []string{"Strohschein"})
```

---

## Security Benefits

### SQL Injection Prevention

Parameterized queries prevent SQL injection by treating all parameter values as **literals**, not executable SQL code.

**Vulnerable Code (without parameters)**:
```sql
-- User input: "' OR '1'='1"
SELECT * FROM "Users" WHERE "Name" = '' OR '1'='1'
-- Returns ALL users!
```

**Safe Code (with parameters)**:
```sql
PREPARE get_user AS SELECT * FROM "Users" WHERE "Name" = $1
EXECUTE get_user\x05' OR '1'='1
-- Searches for literal string "' OR '1'='1" - returns no results
```

### Test Results

The implementation includes comprehensive SQL injection tests that verify:

✅ **Injection attempts are neutralized**: `' OR '1'='1`, `'; DROP BUNDLE Users--`, etc. are treated as literal strings  
✅ **Special characters are safe**: Quotes, semicolons, comments are not interpreted as SQL  
✅ **Type safety is maintained**: Parameters are validated against expected types

---

## Performance Considerations

### Query Plan Caching

Prepared statements are parsed once and cached for reuse, providing significant performance benefits:

**Without Prepared Statements**:
```sql
-- Each execution requires full parsing
SELECT * FROM "Authors" WHERE "Name" = 'Strohschein'  -- Parse + Execute
SELECT * FROM "Authors" WHERE "Name" = 'Smith'        -- Parse + Execute
SELECT * FROM "Authors" WHERE "Name" = 'Johnson'      -- Parse + Execute
```

**With Prepared Statements**:
```sql
PREPARE get_author AS SELECT * FROM "Authors" WHERE "Name" = $1  -- Parse once
EXECUTE get_author\x05Strohschein  -- Execute (cached plan)
EXECUTE get_author\x05Smith        -- Execute (cached plan)
EXECUTE get_author\x05Johnson      -- Execute (cached plan)
```

### Cache Architecture

- **Sharded Cache**: 8-shard LRU cache for lock-free parallel access
- **Default Capacity**: 1,000 statements per shard (8,000 total)
- **Lazy Invalidation**: Bundle version tracking invalidates stale plans
- **Statistics Tracking**: Execution count, average execution time per statement

### Memory Limits

Each query execution enforces per-query memory limits:
- Default: 64 MB for regular users, 256 MB for admin users
- Configurable via `query_memory_limit_bytes` and `admin_query_memory_limit_bytes`
- Exceeded limits return `MEMORY_LIMIT_ERROR`

### Timeouts

Query execution respects configurable timeouts:
- Default: 300 seconds (5 minutes) for regular users
- Default: 600 seconds (10 minutes) for admin users
- Configurable via `query_timeout_seconds` and `admin_query_timeout_seconds`
- Exceeded timeouts return `TIMEOUT_ERROR`

---

## Examples

### Example 1: Simple WHERE Clause

```sql
-- Prepare
PREPARE find_author AS SELECT * FROM "Authors" WHERE "Name" = $1

-- Execute with different parameters
EXECUTE find_author\x05Strohschein
EXECUTE find_author\x05Smith
EXECUTE find_author\x05Johnson

-- Clean up
DEALLOCATE find_author
```

### Example 2: Multiple Parameters

```sql
-- Prepare with multiple conditions
PREPARE search_books AS 
  SELECT * FROM "Books" 
  WHERE "Author" = $1 AND "Year" >= $2 AND "Year" <= $3

-- Execute with three parameters
EXECUTE search_books\x05Strohschein\x052020\x052023

-- Results: All books by Strohschein published between 2020-2023
```

### Example 3: Complex Query with JOIN

```sql
-- Prepare a JOIN query
PREPARE author_books AS
  SELECT a."Name" as AuthorName, b."Title" as BookTitle, b."Year"
  FROM "Authors" a
  JOIN "Books" b ON a."DocumentID" = b."AuthorID"
  WHERE a."Name" = $1
  ORDER BY b."Year" DESC

-- Execute
EXECUTE author_books\x05Strohschein

-- Results: All books by the specified author, ordered by year
```

### Example 4: Range Queries

```sql
-- Prepare range query
PREPARE books_by_year_range AS
  SELECT "Title", "Author", "Year"
  FROM "Books"
  WHERE "Year" BETWEEN $1 AND $2
  ORDER BY "Year"

-- Execute with year range
EXECUTE books_by_year_range\x052020\x052023
```

### Example 5: Pattern Matching

```sql
-- Note: LIKE operator not yet supported with parameters
-- Use exact match only for now
PREPARE find_book_title AS
  SELECT * FROM "Books" WHERE "Title" = $1

-- Execute
EXECUTE find_book_title\x05The Great Gatsby
```

---

## Limitations

### Current Limitations

1. **SELECT Only**: Only SELECT queries are supported. INSERT, UPDATE, DELETE are not yet supported with parameters.

2. **No LIKE Patterns**: Pattern matching with LIKE/ILIKE operators is not currently supported with parameters. Use exact equality matches only.

3. **Session Scoped**: Prepared statements are scoped to individual sessions and cannot be shared between sessions.

4. **No Named Parameters**: Only positional parameters (`$1`, `$2`) are supported. Named parameters (`:name`) are not supported.

5. **Parameter Type Inference**: Parameters are passed as strings and converted based on comparison context. Explicit type casting is not yet supported.

6. **No Batch Execution**: Each EXECUTE command runs one query. Batch execution of the same prepared statement with multiple parameter sets is not yet supported.

### Planned Enhancements

The following features are planned for future releases:

- **DML Support**: INSERT, UPDATE, DELETE with parameters
- **Pattern Matching**: LIKE/ILIKE operator support with wildcards
- **Named Parameters**: `:parameter_name` syntax
- **Type Hints**: Explicit type casting syntax (e.g., `$1::INTEGER`)
- **Batch Execution**: Execute prepared statement multiple times with different parameter sets
- **Cross-Session Caching**: Shared prepared statement cache (with appropriate security controls)

---

## Best Practices

### 1. Always Use Prepared Statements for User Input

❌ **Don't**:
```sql
-- Vulnerable to SQL injection
SELECT * FROM "Users" WHERE "Name" = 'userInput'
```

✅ **Do**:
```sql
PREPARE get_user AS SELECT * FROM "Users" WHERE "Name" = $1
EXECUTE get_user\x05userInput
```

### 2. Reuse Prepared Statements

❌ **Don't** prepare the same query multiple times:
```sql
PREPARE get_user1 AS SELECT * FROM "Users" WHERE "Name" = $1
EXECUTE get_user1\x05Alice
PREPARE get_user2 AS SELECT * FROM "Users" WHERE "Name" = $1
EXECUTE get_user2\x05Bob
```

✅ **Do** reuse the same prepared statement:
```sql
PREPARE get_user AS SELECT * FROM "Users" WHERE "Name" = $1
EXECUTE get_user\x05Alice
EXECUTE get_user\x05Bob
```

### 3. Deallocate When Done

If a prepared statement is no longer needed, deallocate it to free cache space:

```sql
PREPARE temp_query AS SELECT * FROM "Books" WHERE "Year" = $1
EXECUTE temp_query\x052023
DEALLOCATE temp_query  -- Free cache space
```

### 4. Use Meaningful Statement Names

✅ **Good names**:
```sql
PREPARE get_author_by_name AS ...
PREPARE search_books_by_year_range AS ...
PREPARE find_recent_orders AS ...
```

❌ **Poor names**:
```sql
PREPARE q1 AS ...
PREPARE temp AS ...
PREPARE stmt AS ...
```

### 5. Monitor Cache Statistics

Session prepared statement cache provides statistics:
- Total prepared statements in cache
- Hit/miss rates
- Average execution times
- Eviction counts

Use these metrics to optimize your query patterns and cache sizing.

---

## Troubleshooting

### Error: "prepared statement does not exist"

**Cause**: Attempting to EXECUTE or DEALLOCATE a statement that hasn't been prepared or has been deallocated.

**Solution**: Ensure PREPARE is called before EXECUTE.

### Error: "wrong number of parameters"

**Cause**: The number of parameters passed to EXECUTE doesn't match the number of placeholders in the prepared query.

**Solution**: Count your `$1`, `$2`, etc. placeholders and ensure you pass the same number of parameters.

### Error: "illegal token"

**Cause**: Invalid characters in statement name (e.g., `get-author@123`).

**Solution**: Use only alphanumeric characters and underscores in statement names.

### Error: "query execution timeout"

**Cause**: Query took longer than the configured timeout limit.

**Solution**: 
- Optimize your query (add indexes, reduce result set)
- Increase timeout limits in server configuration
- Check for slow network or disk I/O

### Error: "query exceeded memory limit"

**Cause**: Query allocated more memory than the per-query limit.

**Solution**:
- Reduce result set size with LIMIT clauses
- Add WHERE conditions to filter data earlier
- Increase memory limits in server configuration

---

## FAQ

**Q: Are prepared statements persisted across server restarts?**  
A: No, prepared statements are session-scoped and exist only in memory. They are cleared when the session expires or the server restarts.

**Q: Can I share prepared statements between different users?**  
A: No, prepared statements are isolated to individual sessions for security reasons.

**Q: How many parameters can a prepared statement have?**  
A: There is no hard limit, but practical limits depend on memory and protocol constraints. Most queries use 1-10 parameters.

**Q: Do prepared statements work with transactions?**  
A: Yes, EXECUTE commands can be used within transactions and follow the same transaction isolation rules as regular queries.

**Q: Can I prepare multiple queries in one statement?**  
A: No, each PREPARE statement can only contain a single SELECT query.

**Q: Do parameters work with subqueries?**  
A: Yes, parameters can be used in subqueries if subquery support is enabled in your SyndrDB configuration.

**Q: How do I pass NULL values as parameters?**  
A: Pass an empty string as the parameter value. The evaluator will interpret it based on the comparison context.

---

## Related Documentation

- [Query Optimization Guide](./query_optimization.md) - Performance tuning for queries
- [Security Best Practices](./security.md) - Comprehensive security guidelines
- [Transaction Management](./transactions.md) - Using prepared statements in transactions
- [Session Management](./sessions.md) - Session lifecycle and configuration

---

## Version History

**v0.1.0** (December 2025)
- Initial implementation of parameterized queries
- PREPARE, EXECUTE, DEALLOCATE commands
- PostgreSQL-style `$1, $2, $3` placeholder syntax
- Session-scoped prepared statement cache
- SQL injection prevention
- Query plan caching and reuse
