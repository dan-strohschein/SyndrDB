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

**Output Format (includes all EXPLAIN fields plus execution metrics):**
```json
{
  "QueryPlan": {
    "QueryType": "SimpleSelect",
    "PlanType": "FullScan -> Filter -> Sort -> Limit",
    "Cost": 245.67,
    "EstimatedRows": 10,
    "IndexesUsed": [],
    "MemoryEstimate": 16384,
    "CostFormulas": {
      "FullScanCost": "N * 1.0 (linear scan)",
      "FilterCost": "N * 0.1 (per-row evaluation)",
      "SortCost": "N * log2(N) * 0.1 (quicksort)",
      "LimitCost": "min(N, LIMIT) * 1.0"
    },
    "ExecutionTree": {
      "NodeType": "LimitNode",
      "Limit": 10,
      "Cost": 245.67,
      "EstimatedRows": 10,
      "ActualExecutionTime": 2.34,
      "ActualRowsReturned": 10,
      "Child": {
        "NodeType": "SortNode",
        "SortFields": [{"Field": "Name", "Direction": "ASC"}],
        "Cost": 240.50,
        "EstimatedRows": 50,
        "ActualExecutionTime": 1.89,
        "ActualRowsReturned": 50,
        "Child": {
          "NodeType": "FilterNode",
          "Cost": 120.00,
          "EstimatedRows": 50,
          "ActualExecutionTime": 0.45,
          "ActualRowsReturned": 50,
          "Child": {
            "NodeType": "FullScanNode",
            "BundleName": "Authors",
            "Cost": 100.00,
            "EstimatedRows": 100,
            "ActualExecutionTime": 0.12,
            "ActualRowsReturned": 100
          }
        }
      }
    }
  }
}
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

```
ANALYZE BUNDLE "<BUNDLE_NAME>";
```

**Example:**
```
ANALYZE BUNDLE "Orders";
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
- Compressed using gzip (<5MB) or zstd (≥5MB) when over threshold (default: 1MB)
- Stored in `data_files/statistics/<bundle_name>.stats`
- Automatically refreshed when stale or bundle schema changes

**Sampling Strategy:**

To maintain performance on large bundles, ANALYZE uses adaptive sampling:
- Full scan for bundles with <1,000 documents
- 10% sample for bundles with 1,000-100,000 documents  
- 1,000 document sample for bundles with >100,000 documents

**When to Use ANALYZE:**

1. **After bulk data loads** to refresh statistics
   ```
   ANALYZE BUNDLE "Products";
   ```

2. **Before complex queries** on new or significantly changed data
   ```
   ANALYZE BUNDLE "Orders";
   EXPLAIN SELECT * FROM "Orders" WHERE "Status" == "Pending";
   ```

3. **When query plans seem suboptimal** to provide fresh data distribution info
   ```
   ANALYZE BUNDLE "Customers";
   EXPLAIN ANALYZE SELECT * FROM "Customers" JOIN "Orders" ON "Customers"."ID" == "Orders"."CustomerID";
   ```

4. **Periodically for stable bundles** using scheduled analysis configuration



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

```
CREATE MATERIALIZED VIEW "TopProducts" AS
  SELECT "ProductID", "ProductName", COUNT(*) AS "OrderCount"
  FROM "Orders" 
  JOIN "Products" ON "Orders"."ProductID" == "Products"."ID"
  GROUP BY "ProductID", "ProductName"
  ORDER BY COUNT(*) DESC
  LIMIT 100;
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

**Example:**
```
DROP VIEW "ActiveCustomers";
```

**Behavior:**
- Removes view definition from system catalog
- Does not affect underlying bundles
- Fails if view does not exist
- Cannot be used to drop materialized views (use `DROP MATERIALIZED VIEW`)

### DROP MATERIALIZED VIEW
Deletes a materialized view and its associated data bundle.

```
DROP MATERIALIZED VIEW "<VIEW_NAME>";
```

**Example:**
```
DROP MATERIALIZED VIEW "DailySales";
```

**Behavior:**
- Removes view definition from system catalog
- Deletes the physical data bundle (`_mv_<view_name>`)
- Cannot be undone (data is permanently deleted)
- Fails if view does not exist
- Cannot be used to drop regular views (use `DROP VIEW`)

### REFRESH MATERIALIZED VIEW
Manually refreshes a materialized view by re-executing its query and replacing the stored data.

```
REFRESH MATERIALIZED VIEW "<VIEW_NAME>";
```

**Example:**
```
REFRESH MATERIALIZED VIEW "DailySales";
```

**Behavior:**
- Acquires database-level exclusive lock (1-minute timeout)
- Re-executes the view's SELECT statement
- Creates new data snapshot in temporary storage
- Atomically replaces old data with new snapshot
- Updates `LastRefreshed` timestamp in system catalog
- Resets stale data warning timer

**Performance Considerations:**
- Refresh duration depends on complexity of underlying query
- Large result sets may take significant time to populate
- Exclusive lock blocks other operations on the database during refresh
- Consider scheduling refreshes during low-traffic periods

### SHOW VIEWS
Lists all views in the current or specified database.

```
SHOW VIEWS;
SHOW VIEWS FROM "<DATABASE_NAME>";
```

**Examples:**
```
SHOW VIEWS;
SHOW VIEWS FROM "SalesDB";
```

**Output Format:**
Returns a list of views with metadata:
```json
{
  "Views": [
    {
      "ViewName": "ActiveCustomers",
      "Type": "VIEW",
      "CreatedAt": "2024-01-15T10:30:00Z",
      "CreatedBy": "admin"
    },
    {
      "ViewName": "DailySales", 
      "Type": "MATERIALIZED_VIEW",
      "CreatedAt": "2024-01-15T11:00:00Z",
      "CreatedBy": "admin",
      "LastRefreshed": "2024-01-20T08:00:00Z",
      "IsStale": false
    }
  ]
}
```

**Fields:**
- `ViewName`: Name of the view
- `Type`: Either "VIEW" or "MATERIALIZED_VIEW"
- `CreatedAt`: Timestamp when view was created
- `CreatedBy`: Username of creator
- `LastRefreshed`: (Materialized views only) Timestamp of last refresh
- `IsStale`: (Materialized views only) True if stale warning threshold exceeded

### DESCRIBE VIEW
Shows detailed metadata and definition for a specific view.

```
DESCRIBE VIEW "<VIEW_NAME>";
```

**Example:**
```
DESCRIBE VIEW "ActiveCustomers";
```

**Output Format:**
```json
{
  "ViewName": "ActiveCustomers",
  "DatabaseName": "CustomerDB",
  "Type": "VIEW",
  "Definition": "SELECT * FROM \"Customers\" WHERE \"Status\" == \"Active\"",
  "CreatedAt": "2024-01-15T10:30:00Z",
  "CreatedBy": "admin",
  "ColumnCount": 8,
  "ReferencedBundles": ["Customers"]
}
```

**Fields:**
- `ViewName`: Name of the view
- `DatabaseName`: Database containing the view
- `Type`: Either "VIEW" or "MATERIALIZED_VIEW"
- `Definition`: Full SELECT statement defining the view
- `CreatedAt`: Timestamp when view was created
- `CreatedBy`: Username of creator
- `LastRefreshed`: (Materialized views only) Timestamp of last refresh
- `ColumnCount`: Number of columns in view result
- `ReferencedBundles`: List of bundles referenced in view definition
- `DataBundleName`: (Materialized views only) Name of physical storage bundle (`_mv_<view_name>`)

**Use Cases:**
1. **Understanding View Logic**: Review the SELECT statement defining a view
2. **Dependency Analysis**: Identify which bundles a view depends on
3. **Refresh Status**: Check when a materialized view was last refreshed
4. **Schema Information**: See column count and structure

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

### LIKE and NOT LIKE Operators

The LIKE and NOT LIKE operators enable pattern matching and wildcard searches on string fields. They support two types of wildcards: `%` (matches zero or more characters) and `_` (matches exactly one character).

**Basic Syntax:**
```
SELECT DOCUMENTS FROM "<BUNDLE_NAME>" WHERE "<FIELD_NAME>" LIKE "pattern";
SELECT DOCUMENTS FROM "<BUNDLE_NAME>" WHERE "<FIELD_NAME>" NOT LIKE "pattern";
```

**Wildcard Characters:**

- **`%` (Percent)**: Matches zero or more characters
  - `"John%"` matches "John", "Johnny", "Johnson"
  - `"%son"` matches "Johnson", "Anderson", "son"
  - `"%middle%"` matches any string containing "middle"

- **`_` (Underscore)**: Matches exactly one character (single Unicode rune)
  - `"J_hn"` matches "John" but not "Johnson"
  - `"___-2024"` matches "ABC-2024", "XYZ-2024"
  - Handles Unicode: `"Hello_World"` matches "Hello😊World"

**Pattern Types:**

1. **Prefix Match** (pattern ends with `%`):
```
SELECT DOCUMENTS FROM "Users" WHERE "Name" LIKE "John%";
```
Matches: "John", "Johnny", "John Doe", "Johnson"

2. **Suffix Match** (pattern starts with `%`):
```
SELECT DOCUMENTS FROM "Users" WHERE "Email" LIKE "%@company.com";
```
Matches: "john@company.com", "admin@company.com"

3. **Contains Match** (pattern starts and ends with `%`):
```
SELECT DOCUMENTS FROM "Products" WHERE "Description" LIKE "%premium%";
```
Matches any description containing "premium"

4. **Exact Match** (no wildcards):
```
SELECT DOCUMENTS FROM "Users" WHERE "Name" LIKE "John Doe";
```
Equivalent to: `"Name" == "John Doe"`

5. **Match All** (only `%`):
```
SELECT DOCUMENTS FROM "Users" WHERE "Email" LIKE "%";
```
Matches all non-NULL string values

6. **Complex Patterns** (multiple wildcards):
```
SELECT DOCUMENTS FROM "Products" WHERE "Code" LIKE "PRD-___-2024";
SELECT DOCUMENTS FROM "Users" WHERE "Name" LIKE "J_hn%";
SELECT DOCUMENTS FROM "Messages" WHERE "Text" LIKE "%hello%world%";
```

**Examples:**

1. **Simple prefix search:**
```
SELECT DOCUMENTS FROM "Customers" WHERE "LastName" LIKE "Smith%";
```

2. **Email domain filtering:**
```
SELECT DOCUMENTS FROM "Users" WHERE "Email" LIKE "%@gmail.com";
```

3. **Pattern with underscores:**
```
SELECT DOCUMENTS FROM "Products" WHERE "SKU" LIKE "PRD-____";
```

4. **Case-insensitive matching (N prefix):**
```
SELECT DOCUMENTS FROM "Users" WHERE "Name" LIKE N"john%";
```
Matches: "John", "JOHN", "johnny", "Johnny Doe"

5. **NOT LIKE to exclude patterns:**
```
SELECT DOCUMENTS FROM "Users" WHERE "Email" NOT LIKE "%@spam.com";
```

6. **Combined with other conditions:**
```
SELECT DOCUMENTS FROM "Products" WHERE "Status" == "active" AND "Name" LIKE "%premium%";
```

7. **Multiple LIKE conditions:**
```
SELECT DOCUMENTS FROM "Users" WHERE "FirstName" LIKE "J%" AND "LastName" LIKE "%son";
```

**Escape Sequences:**

Use backslash (`\`) to match literal wildcard characters:

```
SELECT DOCUMENTS FROM "Products" WHERE "Discount" LIKE "50\\% off";  // Matches "50% off"
SELECT DOCUMENTS FROM "Files" WHERE "Name" LIKE "test\\_file.txt";   // Matches "test_file.txt"
SELECT DOCUMENTS FROM "Paths" WHERE "Path" LIKE "C:\\\\Users\\\\%";  // Matches "C:\Users\..." (Windows paths)
```

**Supported Escape Sequences:**
- `\\%` → Literal `%`
- `\\_` → Literal `_`
- `\\\\` → Literal `\`
- `\\"` → Literal `"`

**Important Features:**

- **Case Sensitivity**: String matching is case-sensitive by default. Use the `N` prefix for case-insensitive:
  ```
  LIKE N"john%"     // Case-insensitive
  LIKE "john%"      // Case-sensitive
  ```

- **NULL Handling**: LIKE returns `false` for NULL fields (NOT LIKE returns `true`):
  ```
  WHERE "Email" LIKE "%"           // Excludes NULL emails
  WHERE "Email" NOT LIKE "%"       // Includes NULL emails
  ```

- **Unicode Support**: The `_` wildcard matches a single Unicode rune, not byte:
  ```
  "Hello_World" LIKE "Hello_World"  // Matches emoji: "Hello😊World"
  ```

- **Pattern Validation**: 
  - Maximum 1,000 characters per pattern
  - Trailing unescaped backslash generates error
  - Invalid escape sequences generate error

- **Pattern Normalization**: Consecutive `%` wildcards are automatically collapsed:
  ```
  "Name" LIKE "John%%%Doe"  // Normalized to: "John%Doe"
  ```

**Performance Characteristics:**

| Pattern Type | Performance | Index Usage | Notes |
|-------------|-------------|-------------|-------|
| Prefix (`text%`) | ⚡ **Excellent** | ✅ B-tree index | Fastest for case-sensitive |
| Exact (`text`) | ⚡ **Excellent** | ✅ Hash/B-tree index | Optimized to `==` |
| Match All (`%`) | ⚡ **Excellent** | ❌ No scan needed | Always returns true |
| Suffix (`%text`) | 🔶 **Good** | ❌ Full scan | Requires scanning all documents |
| Contains (`%text%`) | 🔶 **Good** | ❌ Full scan | Requires scanning all documents |
| Underscore (`_`) | 🔶 **Good** | ❌ Full scan | Rune-by-rune matching |
| Complex patterns | 🔶 **Good** | ❌ Full scan | Fail-fast optimization |

**Performance Considerations:**

- **Index Optimization**: Only case-sensitive prefix patterns can utilize B-tree indexes
- **Case-Insensitive Warning**: Using `N` prefix prevents index usage even for prefix patterns
- **Full Scan Warning**: Contains and suffix patterns trigger performance warnings (deduplicated)
- **Fail-Fast Matching**: Complex patterns exit early on first non-match
- **Statistics Tracking**: Query patterns are tracked for optimization (aggregated by field + pattern type)

**Best Practices:**

1. **Prefer Prefix Patterns** when possible for best performance (e.g., `"Name" LIKE "John%"`)
2. **Use Indexes** on fields frequently queried with prefix patterns
3. **Avoid Leading Wildcards** unless necessary (e.g., `"%text"` or `"%text%"`)
4. **Use Case-Sensitive** matching when possible to enable index usage
5. **Keep Patterns Short** (<1,000 characters) for optimal performance
6. **Combine with Other Filters** to reduce scan size before LIKE evaluation
7. **Consider Full-Text Search** for complex text searching requirements (future feature)

**Comparison with SQL Standards:**

SyndrDB LIKE is designed to be compatible with SQL standard LIKE operators while adding SyndrDB-specific optimizations:

- **PostgreSQL Compatible**: Pattern syntax matches PostgreSQL LIKE
- **MySQL Compatible**: Escape sequences work like MySQL LIKE
- **SQL Server Compatible**: Case sensitivity configurable via N prefix
- **Performance**: Competitive with PostgreSQL for indexed prefix patterns

**Migration Examples:**

From PostgreSQL:
```sql
-- PostgreSQL
SELECT * FROM users WHERE name LIKE 'John%';

-- SyndrDB
SELECT DOCUMENTS FROM "users" WHERE "name" LIKE "John%";
```

From MySQL:
```sql
-- MySQL (case-insensitive by default)
SELECT * FROM users WHERE name LIKE 'john%';

-- SyndrDB (use N prefix for case-insensitive)
SELECT DOCUMENTS FROM "users" WHERE "name" LIKE N"john%";
```

## Notes
- All database and bundle names must be enclosed in double quotes
- Commands are case-insensitive but conventionally written in UPPERCASE
- Commands should end with semicolon (;) but it's optional for most operations
- Field values in document operations use key=value pairs within parentheses
- Multi-line commands are supported with whitespace normalization