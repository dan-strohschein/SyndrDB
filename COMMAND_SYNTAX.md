# SyndrDB Command Syntax Reference

This document contains all possible commands that can be sent to the SyndrDB server from a client.

## Database Management Commands

### CREATE DATABASE
Creates a new database with the specified name.
```
CREATE DATABASE "<DATABASE_NAME>";
```

### DROP DATABASE
Drops an existing database and all its contents.
```
DROP DATABASE "<DATABASE_NAME>";
```

### DELETE DATABASE
Alias for DROP DATABASE.
```
DELETE DATABASE "<DATABASE_NAME>";
```

### RENAME DATABASE
Renames an existing database. Use FORCE to bypass safety checks.
```
RENAME DATABASE "<OLD_NAME>" TO "<NEW_NAME>";
RENAME DATABASE "<OLD_NAME>" TO "<NEW_NAME>" FORCE;
```

### SELECT DATABASES
Lists all databases from the default system catalog.
```
SELECT DATABASES FROM DEFAULT;
```

### USE
Switches the current session context to use a specific database.
```
USE "<DATABASE_NAME>";
```

### ATTACH DATABASE
Attaches a database from a file path.
```
ATTACH DATABASE "<FILE_PATH>" "<DATABASE_NAME>";
```

### UPDATE DATABASE
Updates database metadata.
```
UPDATE DATABASE "<DATABASE_NAME>" SET <field> = <value>;
```

### LOCK / UNLOCK DATABASE
Acquires or releases an exclusive lock on a database.
```
LOCK DATABASE "<DATABASE_NAME>";
UNLOCK DATABASE "<DATABASE_NAME>";
```

## Server Configuration Commands

### SET SYSTEM
Dynamically adjusts server configuration parameters without requiring a restart.

#### Storage Configuration

**Set Bundle File Size Threshold:**
```
SET SYSTEM bundle_file_max_size_mb = 64;
```
Changes the maximum size (in MB) for bundle files before rotation. New value applies to subsequent file rotations. Default: 32MB, Range: 16-512MB.

## Bundle Management Commands

### CREATE BUNDLE
Creates a new bundle with field definitions. Each field definition specifies the name, type, whether it is required, whether it is unique, and an optional default value.
```
CREATE BUNDLE "<BUNDLE_NAME>"
WITH FIELDS (
    {"<FIELDNAME>", <FIELDTYPE>, <REQUIRED>, <UNIQUE>},
    {"<FIELDNAME>", <FIELDTYPE>, <REQUIRED>, <UNIQUE>, <DEFAULT_VALUE>}
);
```

**Example:**
```
CREATE BUNDLE "Users"
WITH FIELDS (
    {"name", STRING, true, false},
    {"age", INTEGER, true, false},
    {"email", STRING, true, true}
);
```

### DROP BUNDLE
Drops a bundle from the database. Bundle must be empty unless FORCE is used.
```
DROP BUNDLE "<BUNDLE_NAME>";
DROP BUNDLE "<BUNDLE_NAME>" FORCE;
```

### DELETE BUNDLE
Alias for DROP BUNDLE.
```
DELETE BUNDLE "<BUNDLE_NAME>";
DELETE BUNDLE "<BUNDLE_NAME>" FORCE;
```

### UPDATE BUNDLE
Updates bundle structure or metadata. Supports multiple operations:

**Rename a bundle:**
```
UPDATE BUNDLE "<BUNDLE_NAME>" RENAME TO "<NEW_NAME>";
```

**Add a field:**
```
UPDATE BUNDLE "<BUNDLE_NAME>" ADD FIELD {"<FIELDNAME>", <FIELDTYPE>, <REQUIRED>, <UNIQUE>};
```

**Change a field:**
```
UPDATE BUNDLE "<BUNDLE_NAME>" CHANGE FIELD "<OLD_FIELD>" TO {"<FIELDNAME>", <FIELDTYPE>, <REQUIRED>, <UNIQUE>};
```

**Remove a field:**
```
UPDATE BUNDLE "<BUNDLE_NAME>" REMOVE FIELD "<FIELD_NAME>";
```

**Add a relationship:**
```
UPDATE BUNDLE "<BUNDLE_NAME>" ADD RELATIONSHIP "<REL_NAME>" TO "<TARGET_BUNDLE>" ON "<FIELD>";
```

**Drop a relationship:**
```
UPDATE BUNDLE "<BUNDLE_NAME>" DROP RELATIONSHIP "<REL_NAME>";
```

## Document Management Commands

### ADD DOCUMENT
Adds a new document to a bundle. Each field is specified as `{field_name = value}` within the WITH clause, separated by commas.
```
ADD DOCUMENT TO BUNDLE "<BUNDLE_NAME>"
WITH ({<FIELD_NAME> = <VALUE>}, {<FIELD_NAME> = <VALUE>}, ...);
```

**Examples:**
```
ADD DOCUMENT TO BUNDLE "Users" WITH ({"name" = "John"}, {"age" = 30}, {"email" = "john@example.com"});
ADD DOCUMENT TO BUNDLE "Products" WITH ({"id" = 1}, {"title" = "Widget"}, {"price" = 99.99});
```

### BULK ADD DOCUMENTS / BULK INSERT INTO
Inserts multiple documents in a single batch operation. All documents are validated upfront with all-or-nothing semantics — if any document fails validation, the entire batch is rejected. The `BULK` keyword prefix is required to explicitly opt in to the optimized batch insert path. Maximum 10,000 documents per batch.

**SyndrDB-native syntax:**
```
BULK ADD DOCUMENTS TO BUNDLE "<BUNDLE_NAME>" WITH (
  ({<FIELD_NAME> = <VALUE>}, {<FIELD_NAME> = <VALUE>}),
  ({<FIELD_NAME> = <VALUE>}, {<FIELD_NAME> = <VALUE>}),
  ...
);
```

**SQL-style alias:**
```
BULK INSERT INTO BUNDLE "<BUNDLE_NAME>" VALUES (
  ({<FIELD_NAME> = <VALUE>}, {<FIELD_NAME> = <VALUE>}),
  ({<FIELD_NAME> = <VALUE>}, {<FIELD_NAME> = <VALUE>}),
  ...
);
```

**Examples:**
```
BULK ADD DOCUMENTS TO BUNDLE "Users" WITH (
  ({"name" = "Alice"}, {"age" = 28}, {"email" = "alice@example.com"}),
  ({"name" = "Bob"}, {"age" = 34}, {"email" = "bob@example.com"}),
  ({"name" = "Charlie"}, {"age" = 42}, {"email" = "charlie@example.com"})
);

BULK INSERT INTO BUNDLE "Products" VALUES (
  ({"sku" = "ABC123"}, {"price" = 19.99}, {"active" = true}),
  ({"sku" = "DEF456"}, {"price" = 29.99}, {"active" = false})
);
```

### SELECT
Queries documents from a bundle. `SELECT DOCUMENTS FROM` is a legacy alias that is normalized to `SELECT * FROM`.
```
SELECT * FROM "<BUNDLE_NAME>";
SELECT <fields> FROM "<BUNDLE_NAME>";
SELECT DOCUMENTS FROM "<BUNDLE_NAME>";
```

#### SELECT with WHERE
```
SELECT * FROM "<BUNDLE_NAME>" WHERE <CONDITIONS>;
```

#### SELECT DISTINCT
Returns only unique rows.
```
SELECT DISTINCT <fields> FROM "<BUNDLE_NAME>";
```

#### SELECT with LIMIT and OFFSET
```
SELECT * FROM "<BUNDLE_NAME>" LIMIT <N>;
SELECT * FROM "<BUNDLE_NAME>" LIMIT <N> OFFSET <M>;
```

#### SELECT TOP
Legacy syntax equivalent to LIMIT. `SELECT TOP N` is normalized internally.
```
SELECT TOP <N> * FROM "<BUNDLE_NAME>";
SELECT TOP <N> DOCUMENTS FROM "<BUNDLE_NAME>";
```

#### SELECT with ORDER BY
Supports ASC (default) and DESC. Multiple fields allowed.
```
SELECT * FROM "<BUNDLE_NAME>" ORDER BY <FIELD> [ASC|DESC];
SELECT * FROM "<BUNDLE_NAME>" ORDER BY <FIELD1> ASC, <FIELD2> DESC;
```

#### SELECT with GROUP BY
```
SELECT <fields>, <aggregates> FROM "<BUNDLE_NAME>" GROUP BY <FIELD1>, <FIELD2>;
```

#### SELECT with HAVING
Filters groups after GROUP BY. Must follow GROUP BY.
```
SELECT <fields>, COUNT(*) FROM "<BUNDLE_NAME>"
GROUP BY <FIELD>
HAVING COUNT(*) > 5;
```

#### Aggregate Functions
Supported in SELECT field lists and HAVING clauses:
- `COUNT(*)` - Count all rows
- `COUNT(<field>)` - Count non-null values
- `SUM(<field>)` - Sum numeric values
- `AVG(<field>)` - Average numeric values
- `MIN(<field>)` - Minimum value
- `MAX(<field>)` - Maximum value

#### Field Aliasing
```
SELECT <field> AS <alias>, COUNT(*) AS "Total" FROM "<BUNDLE_NAME>";
```

#### SELECT with JOIN
Performs JOIN operations between bundles.
```
SELECT * FROM "<BUNDLE_NAME>"
JOIN "<OTHER_BUNDLE>" ON "<BUNDLE_NAME>"."<FIELD>" == "<OTHER_BUNDLE>"."<FIELD>";
```

Multiple JOINs can be chained:
```
SELECT * FROM "Authors"
JOIN "Books" ON "Authors"."ID" == "Books"."AuthorID"
JOIN "Publishers" ON "Books"."PublisherID" == "Publishers"."ID";
```

JOIN types: `JOIN` (inner), `LEFT JOIN`, `RIGHT JOIN`, `FULL OUTER JOIN`.

#### SELECT with FOR UPDATE
Acquires read locks on selected documents for subsequent update within a transaction.
```
SELECT * FROM "<BUNDLE_NAME>" WHERE <CONDITIONS> FOR UPDATE;
```

#### Full SELECT Syntax
```
SELECT [DISTINCT] [TOP <N>] <fields|*>
FROM "<BUNDLE_NAME>"
[JOIN "<OTHER_BUNDLE>" ON <condition>]
[WHERE <conditions>]
[GROUP BY <fields>]
[HAVING <conditions>]
[ORDER BY <fields> [ASC|DESC]]
[LIMIT <N> [OFFSET <M>]]
[FOR UPDATE];
```

### UPDATE DOCUMENTS
Updates existing documents in a bundle. Field assignments use `field = value` format. The `CONFIRMED` keyword is required for bulk updates without a WHERE clause.
```
UPDATE DOCUMENTS IN BUNDLE "<BUNDLE_NAME>"
(<FIELD_NAME> = <VALUE>, <FIELD_NAME> = <VALUE>)
WHERE <CONDITIONS>;
```

**Bulk update (no WHERE) requires CONFIRMED:**
```
UPDATE DOCUMENTS IN BUNDLE "<BUNDLE_NAME>"
(<FIELD_NAME> = <VALUE>)
CONFIRMED;
```

### DELETE DOCUMENTS
Deletes documents from a bundle. Note: there is no `BUNDLE` keyword between `FROM` and the bundle name. The `CONFIRMED` keyword is required for bulk deletes without a WHERE clause.
```
DELETE DOCUMENTS FROM "<BUNDLE_NAME>" WHERE <CONDITIONS>;
```

**Bulk delete (no WHERE) requires CONFIRMED:**
```
DELETE DOCUMENTS FROM "<BUNDLE_NAME>" CONFIRMED;
```

## Transaction Commands

### BEGIN TRANSACTION
Starts a new transaction. All subsequent commands execute within this transaction until COMMIT or ROLLBACK.
```
BEGIN TRANSACTION;
```

### COMMIT
Commits the current transaction, making all changes permanent.
```
COMMIT;
```

### ROLLBACK
Rolls back the current transaction, undoing all changes.
```
ROLLBACK;
```

### SAVEPOINT
Creates a savepoint within the current transaction. Only single-level savepoints are supported.
```
SAVEPOINT "<SAVEPOINT_NAME>";
```

### ROLLBACK TO SAVEPOINT
Rolls back to a previously created savepoint without aborting the entire transaction.
```
ROLLBACK TO SAVEPOINT "<SAVEPOINT_NAME>";
```

## Cursor Commands

Cursors provide a way to iterate through large result sets without loading everything into memory. Cursors follow PostgreSQL semantics and are automatically closed on COMMIT or ROLLBACK.

### DECLARE CURSOR
Creates a named cursor for a SELECT query.
```
DECLARE <cursor_name> CURSOR FOR <SELECT_STATEMENT>;
```

**Example:**
```
DECLARE my_cursor CURSOR FOR SELECT * FROM "Orders" WHERE "Status" == "pending";
```

### FETCH
Retrieves rows from an open cursor.
```
FETCH <N> FROM <cursor_name>;
FETCH ALL FROM <cursor_name>;
FETCH NEXT FROM <cursor_name>;
```

**Notes:**
- `FETCH ALL` caps at 10,000 rows per call to prevent out-of-memory issues.
- `FETCH NEXT` retrieves a single row.

### CLOSE
Closes an open cursor and releases its resources.
```
CLOSE <cursor_name>;
```

## Prepared Statement Commands

Prepared statements allow query parsing to happen once and execution to happen multiple times with different parameters.

### PREPARE
Parses and caches a query for later execution. Parameters use `$1`, `$2`, etc. placeholders.
```
PREPARE <statement_name> AS <SELECT_STATEMENT>;
```

**Example:**
```
PREPARE find_user AS SELECT * FROM "Users" WHERE "email" == $1;
```

### EXECUTE
Executes a previously prepared statement. Parameters are passed via the protocol layer (delimiter `\x05`), not inline in the SQL.
```
EXECUTE <statement_name>;
```

### DEALLOCATE
Removes a prepared statement from the session cache.
```
DEALLOCATE <statement_name>;
```

## Query Analysis Commands

### EXPLAIN
Shows the query execution plan without executing the query. This command helps optimize query performance by revealing:
- Estimated execution cost
- Expected number of rows
- Indexes that will be used
- Execution node tree structure
- Cost calculation formulas

**Current Limitation**: EXPLAIN currently only supports SELECT statements. Support for UPDATE and DELETE statements will be added when they migrate to the query planning system.

```
EXPLAIN <SELECT_STATEMENT>;
```

**Example:**
```
EXPLAIN SELECT * FROM "Authors" WHERE "Name" == "Strohschein";
```

**Output Format:**
```json
{
  "QueryPlan": {
    "QueryType": "SimpleSelect",
    "PlanType": "HashIndexScan -> Filter",
    "Cost": 15.42,
    "EstimatedRows": 3,
    "IndexesUsed": ["authors_name_hash_idx"],
    "MemoryEstimate": 4096,
    "CostFormulas": {
      "HashIndexCost": "1.0 (base) + 0.1 (lookup)",
      "FilterCost": "N * 0.1 (per-row evaluation)"
    },
    "ExecutionTree": {
      "NodeType": "FilterNode",
      "Cost": 15.42,
      "EstimatedRows": 3,
      "MemoryUsage": 2048,
      "Child": {
        "NodeType": "HashIndexScanNode",
        "IndexName": "authors_name_hash_idx",
        "ScanType": "HashIndexScan",
        "SearchKey": "Strohschein",
        "Cost": 1.1,
        "EstimatedRows": 100
      }
    }
  }
}
```

### EXPLAIN ANALYZE
Executes the query AND shows both estimated and actual execution metrics. This provides comprehensive performance analysis including:
- All information from EXPLAIN
- Actual execution time per node
- Actual number of rows returned
- Comparison of estimated vs actual metrics

**Current Limitation**: EXPLAIN ANALYZE currently only supports SELECT statements. Support for UPDATE and DELETE statements will be added when they migrate to the query planning system.

```
EXPLAIN ANALYZE <SELECT_STATEMENT>;
```

**Example:**
```
EXPLAIN ANALYZE SELECT * FROM "Authors" WHERE "Country" == "USA" ORDER BY "Name" LIMIT 10;
```

**Use Cases:**

1. **Query Optimization**: Identify slow operations and missing indexes
   ```
   EXPLAIN SELECT * FROM "Products" WHERE "Price" > 100;
   ```

2. **Index Verification**: Confirm that indexes are being used
   ```
   EXPLAIN SELECT * FROM "Users" WHERE "Email" == "user@example.com";
   ```

3. **JOIN Analysis**: Understand JOIN algorithm selection (NestedLoop, Hash, Merge)
   ```
   EXPLAIN SELECT * FROM "Orders" JOIN "Customers" ON "Orders"."CustomerID" == "Customers"."ID";
   ```

4. **Performance Debugging**: Find bottlenecks with ANALYZE
   ```
   EXPLAIN ANALYZE SELECT * FROM "Logs" WHERE "Timestamp" >= "2024-01-01" ORDER BY "Timestamp" LIMIT 1000;
   ```

5. **Aggregation Analysis**: Review GROUP BY execution strategies
   ```
   EXPLAIN SELECT "Category", COUNT(*) FROM "Products" GROUP BY "Category";
   ```

**Cost Model:**

SyndrDB's query planner estimates costs using the following formulas:

| Operation | Formula | Description |
|-----------|---------|-------------|
| Hash Index Scan | `1.0 + 0.1` | Base cost + lookup cost (O(1)) |
| B-Tree Index Scan | `log2(N) + 0.5` | Tree traversal cost (O(log N)) |
| B-Tree Range Scan | `log2(N) + M` | Tree traversal + range size |
| Full Scan | `N * 1.0` | Linear scan of all documents |
| Filter | `N * 0.1` | Per-row predicate evaluation |
| Sort | `N * log2(N) * 0.1` | Comparison-based sorting |
| Limit | `min(N, LIMIT) * 1.0` | Early termination |
| Hash Aggregation | `N * 0.2` | Hash-based GROUP BY |
| Distinct | `N * 0.15` | Hash-based deduplication |
| Nested Loop JOIN | `M * N` | Cartesian product iteration |
| Hash JOIN | `M + N` | Hash build + probe |
| Merge JOIN | `M*log(M) + N*log(N) + M + N` | Sort both sides + merge |

Where:
- `N` = number of rows
- `M` = number of rows from other table/side
- `LIMIT` = limit value

**Performance Tips:**

1. **Add indexes on WHERE clause fields** to reduce scan costs
2. **Use LIMIT** with ORDER BY to enable Top-N optimization
3. **Filter before JOIN** to reduce join input size
4. **Choose selective predicates** to reduce estimated rows early
5. **Review cost formulas** to understand optimizer decisions
6. **Compare estimated vs actual** (ANALYZE) to identify estimation errors

### ANALYZE
Collects statistics about data distribution in bundles to improve query planning and optimization. Statistics include:
- Row count estimates
- Most common values and their frequencies
- Value distribution histograms (20 buckets)
- NULL value ratios
- Cardinality estimates per field

These statistics enable the query optimizer to:
- Make better cost estimates for query plans
- Choose optimal JOIN algorithms
- Rewrite correlated subqueries into semi-joins
- Improve selectivity estimates for filters

> **Note:** The `ANALYZE BUNDLE` command is implemented internally but is not currently routed through the command director. Statistics collection is triggered automatically via auto-analyze when data changes significantly.

```
ANALYZE BUNDLE "<BUNDLE_NAME>";
```

**Auto-Analyze:**

SyndrDB can automatically analyze bundles when data changes significantly. This is controlled by configuration settings:
- `StatsAutoAnalyzeEnabled`: Enable/disable auto-analyze (default: true)
- `StatsAutoAnalyzeThreshold`: Minimum document changes before auto-analyze (default: 1000)
- `StatsAutoAnalyzeRatio`: Percentage of bundle size that triggers auto-analyze (default: 0.10)

Auto-analyze triggers when: `changes >= max(threshold, bundle_size * ratio)`

**Scheduled Analysis:**

For large bundles, you can configure scheduled analysis to run during off-peak hours:
- `StatsScheduledAnalyzeEnabled`: Enable/disable scheduled analysis (default: false)
- `StatsScheduledAnalyzeTime`: Time to run analysis in HH:MM format (default: "02:00")
- `StatsScheduledAnalyzeMinDocs`: Minimum bundle size for scheduled analysis (default: 100000)

**Statistics Storage:**

Statistics are:
- Versioned (STATS_VERSION=1) with automatic re-analysis on version mismatch
- Compressed using gzip (<5MB) or zstd (>=5MB) when over threshold (default: 1MB)
- Stored in `data_files/statistics/<bundle_name>.stats`
- Automatically refreshed when stale or bundle schema changes

**Sampling Strategy:**

To maintain performance on large bundles, ANALYZE uses adaptive sampling:
- Full scan for bundles with <1,000 documents
- 10% sample for bundles with 1,000-100,000 documents
- 1,000 document sample for bundles with >100,000 documents

## Index Management Commands

### CREATE B-INDEX (B-Tree Index)
Creates a B-Tree index on bundle fields. Supports optional INCLUDE columns for covering indexes and optional WHERE for partial indexes.
```
CREATE B-INDEX "<INDEX_NAME>" ON BUNDLE "<BUNDLE_NAME>"
WITH FIELDS ({"<FIELD_NAME>", <REQUIRED>, <UNIQUE>});
```

**With INCLUDE (covering index):**
```
CREATE B-INDEX "<INDEX_NAME>" ON BUNDLE "<BUNDLE_NAME>"
WITH FIELDS ({"<FIELD_NAME>", <REQUIRED>, <UNIQUE>})
INCLUDE ("<COL1>", "<COL2>");
```

**With WHERE (partial index):**
```
CREATE B-INDEX "<INDEX_NAME>" ON BUNDLE "<BUNDLE_NAME>"
WITH FIELDS ({"<FIELD_NAME>", <REQUIRED>, <UNIQUE>})
WHERE <PREDICATE>;
```

**With Expression (functional index):**
```
CREATE B-INDEX "<INDEX_NAME>" ON BUNDLE "<BUNDLE_NAME>"
WITH EXPRESSION (<EXPRESSION>);
```

### CREATE HASH INDEX
Creates a Hash index on bundle fields. Optimal for equality lookups.
```
CREATE HASH INDEX "<INDEX_NAME>" ON BUNDLE "<BUNDLE_NAME>"
WITH FIELDS ({"<FIELD_NAME>", <REQUIRED>, <UNIQUE>});
```

**With WHERE (partial index):**
```
CREATE HASH INDEX "<INDEX_NAME>" ON BUNDLE "<BUNDLE_NAME>"
WITH FIELDS ({"<FIELD_NAME>", <REQUIRED>, <UNIQUE>})
WHERE <PREDICATE>;
```

### CREATE BRIN INDEX
Creates a BRIN (Block Range INdex) on bundle fields. Efficient for naturally ordered data like timestamps.
```
CREATE BRIN INDEX "<INDEX_NAME>" ON BUNDLE "<BUNDLE_NAME>"
WITH FIELDS ({"<FIELD_NAME>", <REQUIRED>, <UNIQUE>});
```

**With custom pages per range:**
```
CREATE BRIN INDEX "<INDEX_NAME>" ON BUNDLE "<BUNDLE_NAME>"
WITH FIELDS ({"<FIELD_NAME>", <REQUIRED>, <UNIQUE>})
PAGES_PER_RANGE <N>;
```

## View Management Commands

Views provide query abstraction and reusability. SyndrDB supports two types of views:
- **Regular Views**: Virtual tables that rewrite queries dynamically (no data storage)
- **Materialized Views**: Physical snapshots that store query results for fast repeated access

### CREATE VIEW
Creates a regular view that rewrites queries against the view to execute the underlying SELECT statement.

```
CREATE VIEW "<VIEW_NAME>" AS <SELECT_STATEMENT>;
```

**Examples:**
```
CREATE VIEW "ActiveCustomers" AS
  SELECT * FROM "Customers" WHERE "Status" == "Active";
```

```
CREATE VIEW "OrderSummary" AS
  SELECT "CustomerID", COUNT(*) AS "OrderCount", SUM("Total") AS "TotalSpent"
  FROM "Orders"
  GROUP BY "CustomerID";
```

**Characteristics:**
- No data is stored; queries are rewritten at runtime
- Always shows current data from underlying bundles
- Minimal storage overhead (only view definition)
- Cannot be directly updated (read-only)
- Maximum view name length: 128 characters
- Maximum definition length: 64 KB
- View names cannot start with `_mv_` prefix (reserved for materialized views)

**Permissions:**
- Requires SELECT permission on all referenced bundles
- Permissions are cached for 5 minutes for performance
- View queries execute with the permissions of the calling user
- Users need SELECT permission on the view and all underlying bundles

### CREATE MATERIALIZED VIEW
Creates a materialized view that stores the query results as a physical data bundle.

```
CREATE MATERIALIZED VIEW "<VIEW_NAME>" AS <SELECT_STATEMENT>;
```

**Examples:**
```
CREATE MATERIALIZED VIEW "DailySales" AS
  SELECT DATE("OrderDate") AS "Date", SUM("Amount") AS "TotalSales"
  FROM "Orders"
  GROUP BY DATE("OrderDate");
```

**Characteristics:**
- Stores query results in a physical bundle with `_mv_` prefix
- Data is static until manually refreshed
- Fast query performance (reads from stored snapshot)
- Uses storage space proportional to result set size
- Shows stale data warning after 48 hours (configurable via `view_stale_warning_hours`)
- Maximum concurrent views per bundle: 50
- Requires manual refresh to update data

**Storage:**
- Data bundle: `_mv_<view_name>` (hidden from normal bundle listings)
- View definition: Stored in system catalog
- Automatic backup creation during refresh (`.bak` files)

**Refresh Strategy:**
- Manual refresh only (no automatic refresh)
- Refresh acquires database-level exclusive lock (1-minute timeout)
- Old data preserved until new snapshot is successfully populated
- Atomic replacement ensures consistency

### DROP VIEW
Deletes a regular view from the database.
```
DROP VIEW "<VIEW_NAME>";
```

### DROP MATERIALIZED VIEW
Deletes a materialized view and its associated data bundle.
```
DROP MATERIALIZED VIEW "<VIEW_NAME>";
```

### REFRESH MATERIALIZED VIEW
Manually refreshes a materialized view by re-executing its query and replacing the stored data.
```
REFRESH MATERIALIZED VIEW "<VIEW_NAME>";
```

### SHOW VIEWS
Lists all views in the current or specified database.
```
SHOW VIEWS;
SHOW VIEWS IN DATABASE "<DATABASE_NAME>";
```

### DESCRIBE VIEW
Shows detailed metadata and definition for a specific view.
```
DESCRIBE VIEW "<VIEW_NAME>";
```

## User Management Commands

### CREATE USER
Creates a new user with a password.
```
CREATE USER "<USERNAME>" WITH PASSWORD "<PASSWORD>";
```

### ADD USER
Alias for CREATE USER.
```
ADD USER "<USERNAME>" WITH PASSWORD "<PASSWORD>";
```

### UPDATE USER
Updates user credentials. Use FORCE to bypass session termination warnings.
```
UPDATE USER "<USERNAME>" SET PASSWORD = "<NEW_PASSWORD>";
UPDATE USER "<USERNAME>" SET PASSWORD = "<NEW_PASSWORD>" FORCE;
```

### DELETE USER / DROP USER
Removes a user. Use FORCE to terminate active sessions.
```
DELETE USER "<USERNAME>";
DELETE USER "<USERNAME>" FORCE;
DROP USER "<USERNAME>";
DROP USER "<USERNAME>" FORCE;
```

### ATTACH USER TO DATABASE
Associates a user with a specific database.
```
ATTACH USER <username> TO DATABASE <database_name>;
```

> **Note:** Username and database name are NOT quoted in this command, unlike most other commands.

## Role Management Commands

### CREATE ROLE
Creates a new role with an optional description.
```
CREATE ROLE "<ROLE_NAME>";
CREATE ROLE "<ROLE_NAME>" WITH DESCRIPTION "<DESCRIPTION>";
```

### UPDATE ROLE / ALTER ROLE
Updates role metadata. ALTER ROLE is an alias for UPDATE ROLE.
```
UPDATE ROLE "<ROLE_NAME>" SET DESCRIPTION = "<NEW_DESCRIPTION>";
UPDATE ROLE "<ROLE_NAME>" SET DESCRIPTION = "<NEW_DESCRIPTION>" FORCE;
ALTER ROLE "<ROLE_NAME>" SET DESCRIPTION = "<NEW_DESCRIPTION>";
```

### DELETE ROLE / DROP ROLE
Removes a role. Use FORCE to remove role assignments.
```
DELETE ROLE "<ROLE_NAME>";
DELETE ROLE "<ROLE_NAME>" FORCE;
DROP ROLE "<ROLE_NAME>";
DROP ROLE "<ROLE_NAME>" FORCE;
```

## Permission and Security Commands

### GRANT
Grants a permission or role to a user.
```
GRANT "<PERMISSION>" TO USER "<USERNAME>";
GRANT ROLE "<ROLE_NAME>" TO USER "<USERNAME>";
```

### REVOKE
Revokes a permission or role from a user. Use FORCE to terminate active sessions.
```
REVOKE "<PERMISSION>" FROM USER "<USERNAME>";
REVOKE "<PERMISSION>" FROM USER "<USERNAME>" FORCE;
REVOKE ROLE "<ROLE_NAME>" FROM USER "<USERNAME>";
REVOKE ROLE "<ROLE_NAME>" FROM USER "<USERNAME>" FORCE;
```

## Migration Commands

### START MIGRATION
Begins a migration block. Commands within the block are recorded and versioned.
```
START MIGRATION [WITH DESCRIPTION "<DESCRIPTION>"]
<commands>
COMMIT;
```

### APPLY MIGRATION
Applies a recorded migration by version number.
```
APPLY MIGRATION WITH VERSION <NUMBER>;
APPLY MIGRATION WITH VERSION <NUMBER> FORCE;
```

### APPLY ROLLBACK
Rolls back to a specific migration version.
```
APPLY ROLLBACK TO VERSION <NUMBER>;
```

### VALIDATE MIGRATION / VALIDATE ROLLBACK
Validates a migration or rollback without executing it.
```
VALIDATE MIGRATION WITH VERSION <NUMBER>;
VALIDATE ROLLBACK TO VERSION <NUMBER>;
```

### SHOW MIGRATIONS
Lists all migrations for a database.
```
SHOW MIGRATIONS FOR "<DATABASE_NAME>";
```

## Backup and Recovery Commands

### BACKUP
Creates a backup of a database to a specified path.
```
BACKUP DATABASE "<DATABASE_NAME>" TO "<PATH>";
```

### RESTORE
Restores a database from a backup file.
```
RESTORE DATABASE "<DATABASE_NAME>" FROM "<PATH>";
```

### CHECKPOINT
Forces a WAL checkpoint, flushing pending writes to disk.
```
CHECKPOINT;
```

## Information Display Commands

### SHOW DATABASES
Lists all available databases.
```
SHOW DATABASES;
```

### SHOW BUNDLES
Lists all bundles in the current database or a specified database.
```
SHOW BUNDLES;
SHOW BUNDLES FOR "<DATABASE_NAME>";
```

### SHOW BUNDLE
Shows detailed information about a specific bundle.
```
SHOW BUNDLE "<BUNDLE_NAME>";
```

### SHOW USERS
Shows all users in the system.
```
SHOW USERS;
```

### SHOW SESSIONS
Lists all active sessions.
```
SHOW SESSIONS;
```

### SHOW SESSION
Shows information about the current session.
```
SHOW SESSION;
```

### SHOW RATE LIMIT
Displays current rate limiting information.
```
SHOW RATE LIMIT;
```

### SHOW VERSIONS
Shows document version history (MVCC debugging).
```
SHOW VERSIONS;
SHOW VERSIONS FOR "<DOCUMENT_ID>" IN BUNDLE "<BUNDLE_NAME>";
```

### SHOW ACTIVE SNAPSHOTS
Shows active MVCC snapshots.
```
SHOW ACTIVE SNAPSHOTS;
```

### SHOW CONFLICT LOG
Shows recent MVCC conflict log entries.
```
SHOW CONFLICT LOG;
```

## Connection Management Commands

### INVALIDATE SESSION
Invalidates the current session or a specified session.
```
INVALIDATE SESSION;
INVALIDATE SESSION "<SESSION_ID>";
```

## GraphQL Commands

GraphQL queries are sent over the TCP socket with a `GRAPHQL::` prefix.
```
GRAPHQL::{ bundleName(where: "field > value", limit: 5) { field1 field2 } }
GRAPHQL::{"query": "{ bundleName { field1 field2 } }"}
```

Both raw GraphQL query strings and JSON-encoded `{"query": "..."}` payloads are supported after the `GRAPHQL::` prefix.

## Field Types
When defining bundle fields, the following types are supported:
- `STRING`
- `INTEGER`
- `FLOAT`
- `BOOLEAN`
- `DATE`
- `DATETIME` (also accepted as `TIMESTAMP`)

## Condition Syntax
WHERE clauses support:
- Equality: `"field_name" == "value"` (note: `==` for comparison, `=` is assignment)
- Inequality: `"field_name" != "value"`
- Comparisons: `"field_name" > value`, `"field_name" < value`, `"field_name" >= value`, `"field_name" <= value`
- Logical operators: `AND`, `OR`, `NOT`
- Parentheses for grouping: `("condition1" OR "condition2") AND "condition3"`
- IN operator: `"field_name" IN (value1, value2, value3)`
- NOT IN operator: `"field_name" NOT IN (value1, value2, value3)`
- LIKE operator: `"field_name" LIKE "pattern%"`
- NOT LIKE operator: `"field_name" NOT LIKE "pattern%"`
- CONTAINS operator: `"field_name" CONTAINS "substring"`
- IS NULL: `"field_name" IS NULL`
- IS NOT NULL: `"field_name" IS NOT NULL`
- EXISTS (subquery): `EXISTS (SELECT * FROM "bundle" WHERE ...)`

### IN and NOT IN Operators

The IN and NOT IN operators allow filtering documents based on whether a field's value matches any value in a specified list.

**Basic Syntax:**
```
SELECT * FROM "<BUNDLE_NAME>" WHERE "<FIELD_NAME>" IN (value1, value2, value3);
SELECT * FROM "<BUNDLE_NAME>" WHERE "<FIELD_NAME>" NOT IN (value1, value2, value3);
```

**Examples:**

1. **Simple IN query:**
```
SELECT * FROM "Users" WHERE "Status" IN ("active", "pending", "verified");
```

2. **Numeric IN query:**
```
SELECT * FROM "Products" WHERE "CategoryID" IN (1, 2, 5, 10);
```

3. **NOT IN query:**
```
SELECT * FROM "Orders" WHERE "Status" NOT IN ("cancelled", "refunded");
```

4. **Date IN query:**
```
SELECT * FROM "Events" WHERE "EventDate" IN ("2025-01-15", "2025-02-20", "2025-03-10");
```

5. **Combined with other conditions:**
```
SELECT * FROM "Products" WHERE "Status" == "active" AND "CategoryID" IN (1, 2, 3);
```

**Important Features:**

- **Type Consistency**: All values in the IN list must be of the same type (all strings, all numbers, or all dates). Type coercion is not supported.

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

### LIKE and NOT LIKE Operators

The LIKE and NOT LIKE operators enable pattern matching and wildcard searches on string fields. They support two types of wildcards: `%` (matches zero or more characters) and `_` (matches exactly one character).

**Basic Syntax:**
```
SELECT * FROM "<BUNDLE_NAME>" WHERE "<FIELD_NAME>" LIKE "pattern";
SELECT * FROM "<BUNDLE_NAME>" WHERE "<FIELD_NAME>" NOT LIKE "pattern";
```

**Wildcard Characters:**

- **`%` (Percent)**: Matches zero or more characters
  - `"John%"` matches "John", "Johnny", "Johnson"
  - `"%son"` matches "Johnson", "Anderson", "son"
  - `"%middle%"` matches any string containing "middle"

- **`_` (Underscore)**: Matches exactly one character (single Unicode rune)
  - `"J_hn"` matches "John" but not "Johnson"
  - `"___-2024"` matches "ABC-2024", "XYZ-2024"

**Pattern Types:**

1. **Prefix Match** (pattern ends with `%`):
```
SELECT * FROM "Users" WHERE "Name" LIKE "John%";
```

2. **Suffix Match** (pattern starts with `%`):
```
SELECT * FROM "Users" WHERE "Email" LIKE "%@company.com";
```

3. **Contains Match** (pattern starts and ends with `%`):
```
SELECT * FROM "Products" WHERE "Description" LIKE "%premium%";
```

4. **Exact Match** (no wildcards):
```
SELECT * FROM "Users" WHERE "Name" LIKE "John Doe";
```
Equivalent to: `"Name" == "John Doe"`

5. **Complex Patterns** (multiple wildcards):
```
SELECT * FROM "Products" WHERE "Code" LIKE "PRD-___-2024";
SELECT * FROM "Users" WHERE "Name" LIKE "J_hn%";
```

**Escape Sequences:**

Use backslash (`\`) to match literal wildcard characters:

```
SELECT * FROM "Products" WHERE "Discount" LIKE "50\\% off";  // Matches "50% off"
SELECT * FROM "Files" WHERE "Name" LIKE "test\\_file.txt";   // Matches "test_file.txt"
```

**Supported Escape Sequences:**
- `\\%` - Literal `%`
- `\\_` - Literal `_`
- `\\\\` - Literal `\`
- `\\"` - Literal `"`

**Performance Characteristics:**

| Pattern Type | Performance | Index Usage | Notes |
|-------------|-------------|-------------|-------|
| Prefix (`text%`) | Excellent | B-tree index | Fastest for case-sensitive |
| Exact (`text`) | Excellent | Hash/B-tree index | Optimized to `==` |
| Match All (`%`) | Excellent | No scan needed | Always returns true |
| Suffix (`%text`) | Good | Full scan | Requires scanning all documents |
| Contains (`%text%`) | Good | Full scan | Requires scanning all documents |
| Underscore (`_`) | Good | Full scan | Rune-by-rune matching |
| Complex patterns | Good | Full scan | Fail-fast optimization |

### IS NULL and IS NOT NULL

Tests whether a field value is NULL (stored as `::SYNDR_NULL::` internally).
```
SELECT * FROM "Users" WHERE "Email" IS NULL;
SELECT * FROM "Users" WHERE "Email" IS NOT NULL;
```

### CONTAINS Operator

Tests whether a string field contains a substring.
```
SELECT * FROM "Products" WHERE "Description" CONTAINS "premium";
```

### EXISTS (Subqueries)

Tests whether a subquery returns any rows. Currently supports uncorrelated subqueries only.
```
SELECT * FROM "Authors"
WHERE EXISTS (SELECT * FROM "Books" WHERE "Books"."AuthorID" == "Authors"."ID");
```

## Built-in Functions

SyndrDB provides built-in functions using the `F:` prefix:

### Date/Time Functions
- `F:NOW()` - Returns the current timestamp
- `F:EXTRACT(<part> FROM <field>)` - Extracts a part (YEAR, MONTH, DAY, etc.) from a datetime
- `F:DATE_TRUNC(<part>, <field>)` - Truncates a datetime to the specified precision
- `F:DATE_ADD(<field>, <interval>)` - Adds an interval to a datetime
- `F:DATE_SUB(<field>, <interval>)` - Subtracts an interval from a datetime

### String Functions
- `F:UPPER(<field>)` - Converts a string to uppercase
- `F:LOWER(<field>)` - Converts a string to lowercase
- `F:TRIM(<field>)` - Removes leading and trailing whitespace
- `F:LENGTH(<field>)` - Returns the length of a string

## Notes
- All database and bundle names must be enclosed in double quotes
- Commands are case-insensitive but conventionally written in UPPERCASE
- Commands should end with semicolon (;) but it's optional for most operations
- Field values in document operations use `field = value` pairs within curly braces
- Multi-line commands are supported with whitespace normalization
- The `=` operator is for assignment; use `==` for equality comparison in WHERE clauses
- Cursor names and prepared statement names do NOT use quotes
