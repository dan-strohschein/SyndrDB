# 👁️ SyndrDB Views & Materialized Views

This document describes how to use views in SyndrDB, including regular views for query abstraction and materialized views for performance optimization.

---

## 📑 Table of Contents

1. [Overview](#overview)
2. [Regular Views](#regular-views)
3. [Materialized Views](#materialized-views)
4. [View Operations](#view-operations)
5. [Permissions & Security](#permissions--security)
6. [Performance Considerations](#performance-considerations)
7. [Best Practices](#best-practices)
8. [Complete Examples](#complete-examples)

---

## Overview

SyndrDB supports PostgreSQL-style views with two distinct types:

### View Types

**Regular Views (Virtual Tables)**
- Query rewriting mechanism
- No data storage
- Always current data
- Zero storage overhead
- Read-only access

**Materialized Views (Physical Snapshots)**
- Stored query results
- Fast repeated access
- Stale data possible
- Storage overhead
- Manual refresh required

### Key Features

🎯 **Query Abstraction:** Encapsulate complex queries behind simple names  
⚡ **Performance Optimization:** Pre-compute expensive aggregations  
🔒 **Security Layer:** Control data access through view definitions  
📊 **Data Simplification:** Hide complexity from end users  
♻️ **Reusability:** Share common queries across applications  

---

## Regular Views

Regular views are virtual tables defined by a SELECT statement. When you query a view, SyndrDB rewrites the query to execute the underlying SELECT.

### CREATE VIEW

Creates a new regular view.

**Syntax:**
```
CREATE VIEW "<VIEW_NAME>" AS <SELECT_STATEMENT>;
```

**Example 1: Simple Filter View**
```
CREATE VIEW "ActiveCustomers" AS 
  SELECT * FROM "Customers" WHERE "Status" == "Active";
```

**Response:**
```json
{
  "ResultCount": 1,
  "Result": "View 'ActiveCustomers' created successfully",
  "ExecutionTimeMS": 15.23
}
```

**Example 2: Aggregation View**
```
CREATE VIEW "CustomerStats" AS 
  SELECT "CustomerID", 
         COUNT(*) AS "OrderCount",
         SUM("Total") AS "TotalSpent",
         AVG("Total") AS "AvgOrderValue"
  FROM "Orders"
  GROUP BY "CustomerID";
```

**Response:**
```json
{
  "ResultCount": 1,
  "Result": "View 'CustomerStats' created successfully",
  "ExecutionTimeMS": 12.45
}
```

**Example 3: JOIN View**
```
CREATE VIEW "OrderDetails" AS
  SELECT o."OrderID",
         o."OrderDate",
         c."CustomerName",
         c."Email",
         o."Total"
  FROM "Orders" AS o
  JOIN "Customers" AS c ON o."CustomerID" == c."CustomerID";
```

**Response:**
```json
{
  "ResultCount": 1,
  "Result": "View 'OrderDetails' created successfully",
  "ExecutionTimeMS": 18.67
}
```

### Querying Regular Views

Query views just like regular bundles.

**Example:**
```
SELECT * FROM "ActiveCustomers" WHERE "Country" == "USA";
```

**Behind the Scenes:**
SyndrDB rewrites this to:
```
SELECT * FROM "Customers" 
WHERE "Status" == "Active" AND "Country" == "USA";
```

**Response:**
```json
{
  "ResultCount": 42,
  "Result": [
    {
      "CustomerID": "C1001",
      "Name": "John Smith",
      "Email": "john@example.com",
      "Country": "USA",
      "Status": "Active"
    },
    {
      "CustomerID": "C1005",
      "Name": "Jane Doe",
      "Email": "jane@example.com",
      "Country": "USA",
      "Status": "Active"
    }
  ],
  "ExecutionTimeMS": 8.45
}
```

### View Characteristics

| Feature | Regular View | Notes |
|---------|--------------|-------|
| Data Storage | None | Only stores definition |
| Data Freshness | Always current | Executes on every query |
| Query Performance | Variable | Depends on underlying query complexity |
| Storage Cost | Minimal | ~1KB per view definition |
| Update Behavior | Read-only | Cannot INSERT/UPDATE/DELETE into views |
| Max Name Length | 128 characters | Enforced at creation |
| Max Definition | 64 KB | Enforced at creation |
| Reserved Prefix | Cannot start with `_mv_` | Reserved for materialized views |

---

## Materialized Views

Materialized views store the results of a SELECT query as a physical data bundle, providing fast repeated access to expensive query results.

### CREATE MATERIALIZED VIEW

Creates a new materialized view with an initial snapshot.

**Syntax:**
```
CREATE MATERIALIZED VIEW "<VIEW_NAME>" AS <SELECT_STATEMENT>;
```

**Example 1: Daily Sales Aggregation**
```
CREATE MATERIALIZED VIEW "DailySales" AS
  SELECT DATE("OrderDate") AS "Date",
         COUNT(*) AS "OrderCount",
         SUM("Total") AS "TotalRevenue",
         AVG("Total") AS "AvgOrderValue"
  FROM "Orders"
  GROUP BY DATE("OrderDate")
  ORDER BY DATE("OrderDate") DESC;
```

**Response:**
```json
{
  "ResultCount": 1,
  "Result": "Materialized view 'DailySales' created successfully with 365 rows",
  "ExecutionTimeMS": 142.56
}
```

**Example 2: Top Products**
```
CREATE MATERIALIZED VIEW "TopProducts" AS
  SELECT p."ProductID",
         p."ProductName",
         p."Category",
         COUNT(*) AS "OrderCount",
         SUM(o."Quantity") AS "TotalQuantitySold",
         SUM(o."Amount") AS "TotalRevenue"
  FROM "Orders" AS o
  JOIN "Products" AS p ON o."ProductID" == p."ProductID"
  GROUP BY p."ProductID", p."ProductName", p."Category"
  ORDER BY SUM(o."Amount") DESC
  LIMIT 100;
```

**Response:**
```json
{
  "ResultCount": 1,
  "Result": "Materialized view 'TopProducts' created successfully with 100 rows",
  "ExecutionTimeMS": 523.89
}
```

**Example 3: Customer Lifetime Value**
```
CREATE MATERIALIZED VIEW "CustomerLTV" AS
  SELECT c."CustomerID",
         c."CustomerName",
         c."JoinDate",
         COUNT(o."OrderID") AS "TotalOrders",
         SUM(o."Total") AS "LifetimeValue",
         AVG(o."Total") AS "AvgOrderValue",
         MAX(o."OrderDate") AS "LastOrderDate",
         DATEDIFF(NOW(), MAX(o."OrderDate")) AS "DaysSinceLastOrder"
  FROM "Customers" AS c
  LEFT JOIN "Orders" AS o ON c."CustomerID" == o."CustomerID"
  GROUP BY c."CustomerID", c."CustomerName", c."JoinDate";
```

**Response:**
```json
{
  "ResultCount": 1,
  "Result": "Materialized view 'CustomerLTV' created successfully with 15234 rows",
  "ExecutionTimeMS": 1847.23
}
```

### Querying Materialized Views

Query materialized views like regular bundles. Results come from the stored snapshot (fast).

**Example:**
```
SELECT * FROM "DailySales" WHERE "Date" >= "2024-01-01" ORDER BY "TotalRevenue" DESC LIMIT 10;
```

**Response:**
```json
{
  "ResultCount": 10,
  "Result": [
    {
      "Date": "2024-03-15",
      "OrderCount": 1247,
      "TotalRevenue": 523891.45,
      "AvgOrderValue": 420.15
    },
    {
      "Date": "2024-02-14",
      "OrderCount": 1189,
      "TotalRevenue": 498234.67,
      "AvgOrderValue": 419.12
    }
  ],
  "ExecutionTimeMS": 2.34,
  "Warnings": [
    "Materialized view 'DailySales' last refreshed 36 hours ago"
  ]
}
```

### Stale Data Warnings

Materialized views show warnings when data becomes stale (default: 48 hours since last refresh).

**Example Query After 50 Hours:**
```
SELECT * FROM "TopProducts" LIMIT 5;
```

**Response:**
```json
{
  "ResultCount": 5,
  "Result": [...],
  "ExecutionTimeMS": 1.23,
  "Warnings": [
    "⚠️ Materialized view 'TopProducts' is stale (last refreshed 50 hours ago). Consider running: REFRESH MATERIALIZED VIEW \"TopProducts\";"
  ]
}
```

**Configuration:**
Adjust the stale warning threshold in `syndrdb.example.yml`:
```yaml
# Number of hours before showing stale warning (default: 48)
view_stale_warning_hours: 48
```

### Materialized View Storage

Materialized views create a hidden data bundle with the `_mv_` prefix.

**Example:**
- View Name: `"DailySales"`
- Storage Bundle: `"_mv_DailySales"`

**Storage Characteristics:**
- Hidden from `SHOW BUNDLES` output
- Cannot be directly queried (use view name)
- Cannot be manually created (reserved prefix)
- Automatically managed by view system
- Includes automatic `.bak` backups during refresh

---

## View Operations

### DROP VIEW

Deletes a regular view.

**Syntax:**
```
DROP VIEW "<VIEW_NAME>";
```

**Example:**
```
DROP VIEW "ActiveCustomers";
```

**Response:**
```json
{
  "ResultCount": 1,
  "Result": "View 'ActiveCustomers' dropped successfully",
  "ExecutionTimeMS": 8.12
}
```

**Error Example:**
```
DROP VIEW "NonExistentView";
```

**Response:**
```json
{
  "Error": "View 'NonExistentView' does not exist",
  "ExecutionTimeMS": 2.34
}
```

### DROP MATERIALIZED VIEW

Deletes a materialized view and its data bundle.

**Syntax:**
```
DROP MATERIALIZED VIEW "<VIEW_NAME>";
```

**Example:**
```
DROP MATERIALIZED VIEW "DailySales";
```

**Response:**
```json
{
  "ResultCount": 1,
  "Result": "Materialized view 'DailySales' and its data bundle '_mv_DailySales' dropped successfully",
  "ExecutionTimeMS": 45.67
}
```

**Important Notes:**
- Deletes stored data permanently (cannot be undone)
- Removes view definition from system catalog
- Deletes physical storage bundle
- Cannot be used on regular views (use `DROP VIEW`)

### REFRESH MATERIALIZED VIEW

Manually refreshes a materialized view by re-executing its query.

**Syntax:**
```
REFRESH MATERIALIZED VIEW "<VIEW_NAME>";
```

**Example:**
```
REFRESH MATERIALIZED VIEW "DailySales";
```

**Response:**
```json
{
  "ResultCount": 1,
  "Result": "Materialized view 'DailySales' refreshed successfully with 366 rows (previous: 365 rows)",
  "ExecutionTimeMS": 156.89
}
```

**Refresh Behavior:**
1. Acquires database-level exclusive lock (1-minute timeout)
2. Re-executes the view's SELECT statement
3. Creates new snapshot in temporary storage
4. Creates `.bak` backup of old data
5. Atomically replaces old data with new snapshot
6. Updates `LastRefreshed` timestamp
7. Releases lock

**Lock Timeout Example:**
```
REFRESH MATERIALIZED VIEW "CustomerLTV";
```

**Response (if lock timeout):**
```json
{
  "Error": "Failed to acquire lock for refreshing materialized view 'CustomerLTV': timeout after 60 seconds",
  "ExecutionTimeMS": 60012.45
}
```

**Performance Tips:**
- Schedule refreshes during low-traffic periods
- Monitor refresh duration for large views
- Consider impact of exclusive lock on concurrent operations
- Old data preserved if refresh fails

### SHOW VIEWS

Lists all views in the current or specified database.

**Syntax:**
```
SHOW VIEWS;
SHOW VIEWS FROM "<DATABASE_NAME>";
```

**Example 1: Current Database**
```
SHOW VIEWS;
```

**Response:**
```json
{
  "ResultCount": 5,
  "Result": [
    {
      "ViewName": "ActiveCustomers",
      "Type": "VIEW",
      "CreatedAt": "2024-01-15T10:30:00Z",
      "CreatedBy": "admin"
    },
    {
      "ViewName": "CustomerStats",
      "Type": "VIEW",
      "CreatedAt": "2024-01-15T11:00:00Z",
      "CreatedBy": "analytics_user"
    },
    {
      "ViewName": "DailySales",
      "Type": "MATERIALIZED_VIEW",
      "CreatedAt": "2024-01-16T08:00:00Z",
      "CreatedBy": "admin",
      "LastRefreshed": "2024-01-20T08:00:00Z",
      "IsStale": false
    },
    {
      "ViewName": "TopProducts",
      "Type": "MATERIALIZED_VIEW",
      "CreatedAt": "2024-01-16T09:00:00Z",
      "CreatedBy": "admin",
      "LastRefreshed": "2024-01-18T08:00:00Z",
      "IsStale": true
    },
    {
      "ViewName": "CustomerLTV",
      "Type": "MATERIALIZED_VIEW",
      "CreatedAt": "2024-01-17T10:00:00Z",
      "CreatedBy": "analytics_user",
      "LastRefreshed": "2024-01-20T02:00:00Z",
      "IsStale": false
    }
  ],
  "ExecutionTimeMS": 5.67
}
```

**Example 2: Specific Database**
```
SHOW VIEWS FROM "SalesDB";
```

**Response:**
```json
{
  "ResultCount": 2,
  "Result": [
    {
      "ViewName": "QuarterlySales",
      "Type": "MATERIALIZED_VIEW",
      "CreatedAt": "2024-01-10T12:00:00Z",
      "CreatedBy": "admin",
      "LastRefreshed": "2024-01-20T00:00:00Z",
      "IsStale": false
    },
    {
      "ViewName": "RegionalPerformance",
      "Type": "VIEW",
      "CreatedAt": "2024-01-12T14:30:00Z",
      "CreatedBy": "sales_manager"
    }
  ],
  "ExecutionTimeMS": 4.23
}
```

**Field Descriptions:**
- `ViewName`: Name of the view
- `Type`: Either "VIEW" or "MATERIALIZED_VIEW"
- `CreatedAt`: ISO 8601 timestamp of creation
- `CreatedBy`: Username of creator
- `LastRefreshed`: (Materialized views only) ISO 8601 timestamp of last refresh
- `IsStale`: (Materialized views only) True if past stale warning threshold

### DESCRIBE VIEW

Shows detailed metadata and definition for a specific view.

**Syntax:**
```
DESCRIBE VIEW "<VIEW_NAME>";
```

**Example 1: Regular View**
```
DESCRIBE VIEW "ActiveCustomers";
```

**Response:**
```json
{
  "ResultCount": 1,
  "Result": {
    "ViewName": "ActiveCustomers",
    "DatabaseName": "CustomerDB",
    "Type": "VIEW",
    "Definition": "SELECT * FROM \"Customers\" WHERE \"Status\" == \"Active\"",
    "CreatedAt": "2024-01-15T10:30:00Z",
    "CreatedBy": "admin",
    "ColumnCount": 8,
    "ReferencedBundles": ["Customers"]
  },
  "ExecutionTimeMS": 3.45
}
```

**Example 2: Materialized View**
```
DESCRIBE VIEW "DailySales";
```

**Response:**
```json
{
  "ResultCount": 1,
  "Result": {
    "ViewName": "DailySales",
    "DatabaseName": "SalesDB",
    "Type": "MATERIALIZED_VIEW",
    "Definition": "SELECT DATE(\"OrderDate\") AS \"Date\", COUNT(*) AS \"OrderCount\", SUM(\"Total\") AS \"TotalRevenue\", AVG(\"Total\") AS \"AvgOrderValue\" FROM \"Orders\" GROUP BY DATE(\"OrderDate\") ORDER BY DATE(\"OrderDate\") DESC",
    "CreatedAt": "2024-01-16T08:00:00Z",
    "CreatedBy": "admin",
    "LastRefreshed": "2024-01-20T08:00:00Z",
    "ColumnCount": 4,
    "ReferencedBundles": ["Orders"],
    "DataBundleName": "_mv_DailySales"
  },
  "ExecutionTimeMS": 4.12
}
```

**Field Descriptions:**
- `ViewName`: Name of the view
- `DatabaseName`: Database containing the view
- `Type`: "VIEW" or "MATERIALIZED_VIEW"
- `Definition`: Complete SELECT statement
- `CreatedAt`: ISO 8601 timestamp of creation
- `CreatedBy`: Username of creator
- `LastRefreshed`: (Materialized views only) ISO 8601 timestamp of last refresh
- `ColumnCount`: Number of columns in result set
- `ReferencedBundles`: Array of bundles used in definition
- `DataBundleName`: (Materialized views only) Name of storage bundle

---

## Permissions & Security

### Permission Model

Views use a two-tier permission model:

1. **View-Level Permissions**: User needs SELECT permission on the view
2. **Bundle-Level Permissions**: User needs SELECT permission on all underlying bundles

**Example:**
```
-- View definition
CREATE VIEW "SalesReport" AS 
  SELECT * FROM "Orders" JOIN "Customers" ON "Orders"."CustomerID" == "Customers"."CustomerID";

-- Required permissions for user to query "SalesReport":
-- 1. SELECT on view "SalesReport"
-- 2. SELECT on bundle "Orders"  
-- 3. SELECT on bundle "Customers"
```

### Permission Caching

Permissions are cached for 5 minutes to reduce overhead during query rewriting.

**Cache Behavior:**
- First query: Permission check + cache
- Subsequent queries (within 5 minutes): Use cached permissions
- After 5 minutes: Re-validate permissions

**Cache Invalidation:**
- Automatic after 5 minutes
- Manual via permission changes
- View modification or recreation

### Security Best Practices

1. **Principle of Least Privilege**
   ```
   -- Grant view access without exposing full bundle
   GRANT SELECT ON VIEW "PublicCustomerInfo" TO "web_app";
   -- Don't grant: GRANT SELECT ON BUNDLE "Customers" TO "web_app";
   ```

2. **Row-Level Security via Views**
   ```
   CREATE VIEW "MyOrders" AS 
     SELECT * FROM "Orders" WHERE "UserID" == CURRENT_USER();
   ```

3. **Column-Level Security via Views**
   ```
   CREATE VIEW "CustomerBasicInfo" AS 
     SELECT "CustomerID", "Name", "Email" FROM "Customers";
   -- Hides sensitive columns like SSN, CreditCard, etc.
   ```

4. **Audit Trail via Views**
   ```
   CREATE VIEW "RecentActivity" AS
     SELECT *, CURRENT_USER() AS "AccessedBy", NOW() AS "AccessedAt"
     FROM "AuditLog";
   ```

---

## Performance Considerations

### When to Use Regular Views

✅ **Good Use Cases:**
- Query abstraction and simplification
- Security/access control layers
- Frequently changing data
- Low query frequency
- Simple queries with minimal overhead

❌ **Poor Use Cases:**
- Expensive aggregations run frequently
- Complex JOINs executed repeatedly
- Large result sets accessed often
- Performance-critical hot paths

### When to Use Materialized Views

✅ **Good Use Cases:**
- Expensive aggregations (SUM, AVG, COUNT)
- Complex multi-table JOINs
- Large result sets accessed frequently
- Reporting dashboards
- Analytics queries
- Data that changes infrequently

❌ **Poor Use Cases:**
- Real-time data requirements
- Frequently changing source data
- Small result sets
- Simple queries
- Storage-constrained environments

### Performance Comparison

**Scenario: Daily sales aggregation on 10M orders**

| Approach | First Query | Subsequent Queries | Storage | Freshness |
|----------|-------------|-------------------|---------|-----------|
| Direct Query | 2.3s | 2.3s | 0 KB | Real-time |
| Regular View | 2.3s | 2.3s | 1 KB | Real-time |
| Materialized View | 2.3s (initial) | 5ms | 50 KB | Stale (manual refresh) |

**Key Takeaways:**
- Regular views have same performance as direct queries
- Materialized views: Slow initial load, fast subsequent reads
- Materialized views trade storage and freshness for speed

### Query Plan Caching

SyndrDB caches query plans for views to reduce planning overhead.

**Cache Invalidation:**
- View definition changes
- Underlying bundle schema changes
- Statistics updates (ANALYZE)
- Manual cache clear

**Benefits:**
- Faster query execution
- Reduced CPU usage
- Lower latency for repeated queries

**Monitoring:**
```
EXPLAIN SELECT * FROM "CustomerStats";
```

Check for "CachedPlan" indicator in output.

---

## Best Practices

### Naming Conventions

```
-- Good: Descriptive, clear purpose
CREATE VIEW "ActiveCustomersUSA" AS ...;
CREATE MATERIALIZED VIEW "MonthlySalesByRegion" AS ...;

-- Bad: Vague, unclear
CREATE VIEW "v1" AS ...;
CREATE MATERIALIZED VIEW "temp" AS ...;
```

### Definition Organization

```
-- Good: Formatted for readability
CREATE VIEW "OrderSummary" AS
  SELECT o."OrderID",
         o."OrderDate",
         c."CustomerName",
         SUM(i."Amount") AS "TotalAmount"
  FROM "Orders" AS o
  JOIN "Customers" AS c ON o."CustomerID" == c."CustomerID"
  JOIN "OrderItems" AS i ON o."OrderID" == i."OrderID"
  GROUP BY o."OrderID", o."OrderDate", c."CustomerName";

-- Bad: Single line, hard to read
CREATE VIEW "OrderSummary" AS SELECT o."OrderID", o."OrderDate", c."CustomerName", SUM(i."Amount") AS "TotalAmount" FROM "Orders" AS o JOIN "Customers" AS c ON o."CustomerID" == c."CustomerID" JOIN "OrderItems" AS i ON o."OrderID" == i."OrderID" GROUP BY o."OrderID", o."OrderDate", c."CustomerName";
```

### Refresh Scheduling

```
-- Good: Schedule during low-traffic periods
-- Via cron or application scheduler:
02:00 - REFRESH MATERIALIZED VIEW "DailySales";
02:30 - REFRESH MATERIALIZED VIEW "CustomerLTV";
03:00 - REFRESH MATERIALIZED VIEW "TopProducts";

-- Bad: Refreshing during peak hours
12:00 - REFRESH MATERIALIZED VIEW "DailySales";  -- Lunch rush!
```

### Documentation

```
-- Good: Document view purpose and refresh schedule
-- View: DailySales
-- Purpose: Aggregate daily order metrics for reporting dashboard
-- Refresh: Daily at 02:00 UTC via automated job
-- Owner: analytics_team
CREATE MATERIALIZED VIEW "DailySales" AS ...;
```

### Dependency Management

```
-- Good: Track view dependencies
-- DailySales depends on:
--   - Orders bundle
--   - If Orders schema changes, view must be recreated
CREATE MATERIALIZED VIEW "DailySales" AS
  SELECT DATE("OrderDate") AS "Date",
         COUNT(*) AS "OrderCount"
  FROM "Orders"
  GROUP BY DATE("OrderDate");
```

### Error Handling

```
-- Good: Check if view exists before dropping
DESCRIBE VIEW "OldView";
-- If exists, then:
DROP VIEW "OldView";

-- Good: Validate view creation
CREATE VIEW "TestView" AS SELECT * FROM "TestBundle";
DESCRIBE VIEW "TestView";  -- Verify creation
SELECT * FROM "TestView" LIMIT 1;  -- Test query
```

---

## Complete Examples

### Example 1: E-commerce Analytics System

**Setup:**
```
-- Create materialized views for dashboard
CREATE MATERIALIZED VIEW "DailySalesSummary" AS
  SELECT DATE("OrderDate") AS "Date",
         COUNT(*) AS "TotalOrders",
         SUM("Total") AS "Revenue",
         AVG("Total") AS "AvgOrderValue",
         COUNT(DISTINCT "CustomerID") AS "UniqueCustomers"
  FROM "Orders"
  WHERE "Status" == "Completed"
  GROUP BY DATE("OrderDate")
  ORDER BY DATE("OrderDate") DESC;

CREATE MATERIALIZED VIEW "TopSellingProducts" AS
  SELECT p."ProductID",
         p."ProductName",
         p."Category",
         COUNT(*) AS "OrderCount",
         SUM(oi."Quantity") AS "QuantitySold",
         SUM(oi."Amount") AS "Revenue"
  FROM "OrderItems" AS oi
  JOIN "Products" AS p ON oi."ProductID" == p."ProductID"
  GROUP BY p."ProductID", p."ProductName", p."Category"
  ORDER BY SUM(oi."Amount") DESC
  LIMIT 50;

CREATE MATERIALIZED VIEW "CustomerSegments" AS
  SELECT c."CustomerID",
         c."CustomerName",
         CASE
           WHEN COUNT(o."OrderID") >= 10 THEN "VIP"
           WHEN COUNT(o."OrderID") >= 5 THEN "Regular"
           ELSE "Occasional"
         END AS "Segment",
         COUNT(o."OrderID") AS "OrderCount",
         SUM(o."Total") AS "LifetimeValue"
  FROM "Customers" AS c
  LEFT JOIN "Orders" AS o ON c."CustomerID" == o."CustomerID"
  GROUP BY c."CustomerID", c."CustomerName";
```

**Daily Refresh (Scheduled at 02:00):**
```
REFRESH MATERIALIZED VIEW "DailySalesSummary";
REFRESH MATERIALIZED VIEW "TopSellingProducts";
REFRESH MATERIALIZED VIEW "CustomerSegments";
```

**Dashboard Query (Fast - reads from snapshots):**
```
-- Revenue trend
SELECT * FROM "DailySalesSummary" 
WHERE "Date" >= DATEADD(DAY, -30, NOW())
ORDER BY "Date" DESC;

-- Top products
SELECT * FROM "TopSellingProducts" LIMIT 10;

-- Customer distribution
SELECT "Segment", COUNT(*) AS "Count", SUM("LifetimeValue") AS "TotalValue"
FROM "CustomerSegments"
GROUP BY "Segment";
```

### Example 2: Security Layer with Views

**Setup:**
```
-- Hide sensitive customer data
CREATE VIEW "PublicCustomerInfo" AS
  SELECT "CustomerID",
         "CustomerName",
         "Email",
         "Country"
  FROM "Customers";
  -- Excludes: SSN, CreditCardHash, BillingAddress, etc.

-- Row-level security: Users see only their orders
CREATE VIEW "MyOrders" AS
  SELECT *
  FROM "Orders"
  WHERE "CustomerID" == CURRENT_USER_ID();

-- Department-specific view
CREATE VIEW "SalesTeamView" AS
  SELECT o."OrderID",
         o."OrderDate",
         c."CustomerName",
         o."Total",
         o."Status"
  FROM "Orders" AS o
  JOIN "Customers" AS c ON o."CustomerID" == c."CustomerID"
  WHERE o."AssignedSalesRep" == CURRENT_USER();
```

**Grant Permissions:**
```
-- Web app only sees public info
GRANT SELECT ON VIEW "PublicCustomerInfo" TO "web_app_user";

-- Mobile app sees user's own orders
GRANT SELECT ON VIEW "MyOrders" TO "mobile_app_user";

-- Sales team sees their assigned orders
GRANT SELECT ON VIEW "SalesTeamView" TO "sales_team";
```

### Example 3: Complex Reporting View

**Setup:**
```
CREATE MATERIALIZED VIEW "ComprehensiveSalesReport" AS
  SELECT r."Region",
         r."RegionName",
         DATE_TRUNC('month', o."OrderDate") AS "Month",
         p."Category",
         COUNT(DISTINCT o."OrderID") AS "OrderCount",
         COUNT(DISTINCT o."CustomerID") AS "UniqueCustomers",
         SUM(oi."Quantity") AS "TotalQuantity",
         SUM(oi."Amount") AS "Revenue",
         AVG(o."Total") AS "AvgOrderValue",
         SUM(oi."Amount") - SUM(oi."Cost") AS "Profit",
         (SUM(oi."Amount") - SUM(oi."Cost")) / SUM(oi."Amount") AS "ProfitMargin"
  FROM "Orders" AS o
  JOIN "OrderItems" AS oi ON o."OrderID" == oi."OrderID"
  JOIN "Products" AS p ON oi."ProductID" == p."ProductID"
  JOIN "Customers" AS c ON o."CustomerID" == c."CustomerID"
  JOIN "Regions" AS r ON c."RegionID" == r."RegionID"
  WHERE o."Status" == "Completed"
  GROUP BY r."Region", r."RegionName", DATE_TRUNC('month', o."OrderDate"), p."Category"
  ORDER BY DATE_TRUNC('month', o."OrderDate") DESC, r."Region", p."Category";
```

**Benefits:**
- Pre-computed complex JOINs across 5 tables
- Aggregations at multiple levels (region, month, category)
- Fast dashboard queries (milliseconds vs. seconds)
- Consistent calculations across reports

**Monthly Refresh:**
```
-- Refresh at month end
REFRESH MATERIALIZED VIEW "ComprehensiveSalesReport";
```

**Query Examples:**
```
-- Regional performance this year
SELECT "Region", "RegionName", SUM("Revenue") AS "TotalRevenue"
FROM "ComprehensiveSalesReport"
WHERE "Month" >= '2024-01-01'
GROUP BY "Region", "RegionName"
ORDER BY SUM("Revenue") DESC;

-- Category trends
SELECT "Category", "Month", SUM("Revenue") AS "Revenue"
FROM "ComprehensiveSalesReport"
WHERE "Month" >= DATEADD(MONTH, -12, NOW())
GROUP BY "Category", "Month"
ORDER BY "Month" DESC, "Revenue" DESC;
```

---

## Summary

**Views provide:**
- 🎯 Query abstraction and reusability
- ⚡ Performance optimization (materialized views)
- 🔒 Security and access control
- 📊 Data simplification for end users

**Key Differences:**

| Feature | Regular View | Materialized View |
|---------|--------------|-------------------|
| Storage | Definition only (~1KB) | Definition + data (varies) |
| Performance | Same as underlying query | Fast (pre-computed) |
| Freshness | Always current | Stale until refreshed |
| Use Case | Abstraction, security | Performance, analytics |
| Refresh | Automatic (query rewrite) | Manual (REFRESH command) |

**Best Practices:**
1. Use regular views for abstraction and security
2. Use materialized views for expensive, frequently-accessed queries
3. Schedule materialized view refreshes during low-traffic periods
4. Monitor stale warnings and refresh accordingly
5. Document view purpose, dependencies, and refresh schedules
6. Grant minimal permissions (view-level, not bundle-level)
7. Use descriptive names and formatted definitions

For more information, see:
- [COMMAND_SYNTAX.md](../../COMMAND_SYNTAX.md) - Complete syntax reference
- [SyndrDB_Results_Output.md](./SyndrDB_Results_Output.md) - Response format details
- [select_queries.md](./select_queries.md) - Query syntax and examples
