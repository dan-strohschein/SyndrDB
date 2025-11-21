# 🔍 CREATE INDEX - Build Indexes for Query Optimization

## Table of Contents
- [Overview](#overview)
- [Index Types](#index-types)
  - [Hash Index](#hash-index)
  - [B-Tree Index](#btree-index)
- [Basic Syntax](#basic-syntax)
  - [CREATE HASH INDEX](#create-hash-index-syntax)
  - [CREATE B-INDEX](#create-b-index-syntax)
- [Keywords](#keywords)
  - [CREATE](#create)
  - [HASH INDEX](#hash-index-keyword)
  - [B-INDEX](#b-index-keyword)
  - [ON BUNDLE](#on-bundle)
  - [WITH FIELDS](#with-fields)
- [Field Definitions](#field-definitions)
  - [Field Structure](#field-structure)
  - [Field Properties](#field-properties)
- [Examples](#examples)
  - [Hash Index Examples](#hash-index-examples)
  - [B-Tree Index Examples](#btree-index-examples)
  - [Multiple Field Indexes](#multiple-field-indexes)
- [Index Performance](#index-performance)
- [Best Practices](#best-practices)
- [Error Handling](#error-handling)
- [Quick Reference](#quick-reference)

---

## Overview

Indexes in SyndrDB dramatically improve query performance by creating fast lookup structures for specific fields. SyndrDB supports two types of indexes:

1. **Hash Indexes** - O(1) equality lookups (perfect for `WHERE field == value`)
2. **B-Tree Indexes** - O(log n) range queries (perfect for `WHERE field > value`)

📊 **Performance Impact**: B-Tree indexes can provide **10.6x performance improvement** for range queries!

---

## Index Types

### <a name="hash-index"></a>🔐 Hash Index

Hash indexes provide **constant-time O(1)** lookups for exact match queries. They use a hash table structure optimized for equality comparisons.

**Use Cases:**
- Primary key lookups: `WHERE DocumentID == "abc-123"`
- Foreign key relationships: `WHERE user_id == 42`
- Exact string matches: `WHERE status == "active"`
- Boolean flags: `WHERE verified == true`

**Architecture:**
- LSM (Log-Structured Merge) tree implementation
- MemTable for recent writes (read-your-own-writes consistency)
- SSTables (Sorted String Tables) for disk persistence
- Automatic compaction for optimal performance

**Limitations:**
- ❌ Cannot be used for range queries (`>`, `<`, `>=`, `<=`)
- ❌ Cannot be used for pattern matching (`LIKE`)
- ✅ Perfect for equality checks (`==`, `!=`)

---

### <a name="btree-index"></a>🌲 B-Tree Index

B-Tree (Balanced Tree) indexes provide **logarithmic O(log n)** lookups and support both equality and range queries. They maintain sorted data for efficient range scans.

**Use Cases:**
- Range queries: `WHERE age > 18`, `WHERE price < 100`
- Sorting: `ORDER BY created_date`
- Greater/less than: `WHERE salary >= 50000`
- Between operations: `WHERE score >= 70 AND score <= 100`

**Architecture:**
- Self-balancing tree structure
- Nodes contain sorted keys
- Efficient disk I/O patterns
- Support for composite indexes (multiple fields)

**Performance:**
- **Single field lookup**: 1-5 μs (microseconds)
- **Range scan (1000 docs)**: 10.6x faster than full scan
- **Index overhead**: Minimal when not actively used

**Limitations:**
- ❌ Slower than hash indexes for exact matches
- ✅ Only index type that supports range queries
- ✅ Can be used for both equality and range queries

---

## Basic Syntax

### <a name="create-hash-index-syntax"></a>CREATE HASH INDEX

```syndrql
CREATE HASH INDEX "<INDEX_NAME>" ON BUNDLE "<BUNDLE_NAME>" 
WITH FIELDS (
    {"<FIELD_NAME>", <REQUIRED>, <UNIQUE>}
);
```

**Note:** Hash indexes currently support **single field only**.

---

### <a name="create-b-index-syntax"></a>CREATE B-INDEX

```syndrql
CREATE B-INDEX "<INDEX_NAME>" ON BUNDLE "<BUNDLE_NAME>" 
WITH FIELDS (
    {"<FIELD_NAME>", <REQUIRED>, <UNIQUE>}
);
```

**Composite Index Syntax** (future support):
```syndrql
CREATE B-INDEX "<INDEX_NAME>" ON BUNDLE "<BUNDLE_NAME>" 
WITH FIELDS (
    {"<FIELD_NAME_1>", <REQUIRED>, <UNIQUE>},
    {"<FIELD_NAME_2>", <REQUIRED>, <UNIQUE>}
);
```

---

## Keywords

### <a name="create"></a>🔧 CREATE

The `CREATE` keyword initiates index creation. Indexes are built asynchronously and become available immediately for queries.

**Characteristics:**
- Creates persistent index files on disk
- Automatically maintained on INSERT/UPDATE/DELETE
- Index names must be unique within a bundle
- Semicolon is optional

---

### <a name="hash-index-keyword"></a>🔐 HASH INDEX

The `HASH INDEX` keywords specify creation of a hash-based index optimized for equality lookups.

**Syntax:**
```syndrql
CREATE HASH INDEX "<name>" ...
```

**Characteristics:**
- Uses LSM tree architecture
- O(1) lookup performance
- Supports single field only
- Ideal for foreign keys and unique identifiers

---

### <a name="b-index-keyword"></a>🌲 B-INDEX

The `B-INDEX` keyword specifies creation of a B-Tree index optimized for range queries and sorting.

**Syntax:**
```syndrql
CREATE B-INDEX "<name>" ...
```

**Characteristics:**
- Balanced tree structure
- O(log n) lookup performance
- Supports single or multiple fields (composite)
- Required for range queries (`>`, `<`, `>=`, `<=`)

**Note:** The keyword is `B-INDEX` not `BTREE INDEX` or `B-TREE INDEX`.

---

### <a name="on-bundle"></a>📦 ON BUNDLE

The `ON BUNDLE` keywords specify which bundle (table) the index applies to.

**Syntax:**
```syndrql
ON BUNDLE "<BUNDLE_NAME>"
```

**Characteristics:**
- Bundle name must be in double quotes
- Bundle must already exist
- Case-sensitive bundle name matching
- Index files stored in bundle's `indexes/` subdirectory

---

### <a name="with-fields"></a>🏷️ WITH FIELDS

The `WITH FIELDS` clause defines which fields to index and their properties.

**Syntax:**
```syndrql
WITH FIELDS (
    {"<FIELD_NAME>", <REQUIRED>, <UNIQUE>}
)
```

**Characteristics:**
- Field definitions enclosed in curly braces `{}`
- Multiple fields separated by commas (for composite indexes)
- Each field has three components: name, required flag, unique flag
- Field names must be in double quotes

---

## Field Definitions

### <a name="field-structure"></a>Field Structure

Each field definition follows this exact syntax:

```syndrql
{"<FIELD_NAME>", <REQUIRED>, <UNIQUE>}
```

**Components:**
1. **Field Name** (string) - The name of the field to index
2. **Required** (boolean) - Whether the field must exist in all documents
3. **Unique** (boolean) - Whether values must be unique across documents

---

### <a name="field-properties"></a>Field Properties

#### **Field Name**
- Must match a field in the bundle's schema
- Enclosed in double quotes: `"field_name"`
- Case-sensitive
- Can include special characters

**Valid examples:**
```syndrql
"user_id"
"DocumentID"
"created_date"
"field-with-dashes"
```

---

#### **Required Flag**

Controls whether the field must exist in all documents.

| Value | Meaning | Use Case |
|-------|---------|----------|
| `true` | Field must exist in all docs | Primary keys, required fields |
| `false` | Field can be missing in some docs | Optional fields, nullable columns |

**Examples:**
```syndrql
{"DocumentID", true, true}    -- DocumentID is always present
{"middle_name", false, false} -- middle_name is optional
```

---

#### **Unique Flag**

Controls whether field values must be unique across all documents.

| Value | Meaning | Use Case |
|-------|---------|----------|
| `true` | Values must be unique | Primary keys, email addresses |
| `false` | Values can be duplicated | Foreign keys, status fields |

**Examples:**
```syndrql
{"email", true, true}      -- Email must be unique
{"category_id", false, false} -- Multiple docs can have same category
```

**Important:** Unique constraints are enforced at insert/update time.

---

## Examples

### <a name="hash-index-examples"></a>Hash Index Examples

#### **Primary Key Index**
```syndrql
CREATE HASH INDEX "idx_document_id" ON BUNDLE "users" 
WITH FIELDS (
    {"DocumentID", true, true}
);
```
Perfect for: `WHERE DocumentID == "abc-123"`

---

#### **Foreign Key Index**
```syndrql
CREATE HASH INDEX "idx_user_id" ON BUNDLE "orders" 
WITH FIELDS (
    {"user_id", false, false}
);
```
Perfect for: `WHERE user_id == 42`

---

#### **Unique Email Index**
```syndrql
CREATE HASH INDEX "idx_email" ON BUNDLE "users" 
WITH FIELDS (
    {"email", true, true}
);
```
Perfect for: `WHERE email == "user@example.com"`

---

#### **Status Field Index**
```syndrql
CREATE HASH INDEX "idx_status" ON BUNDLE "orders" 
WITH FIELDS (
    {"status", true, false}
);
```
Perfect for: `WHERE status == "pending"`

---

#### **Optional Field Index**
```syndrql
CREATE HASH INDEX "idx_category" ON BUNDLE "products" 
WITH FIELDS (
    {"category", false, false}
);
```
Perfect for: `WHERE category == "electronics"`

---

### <a name="btree-index-examples"></a>B-Tree Index Examples

#### **Age Range Index**
```syndrql
CREATE B-INDEX "idx_age" ON BUNDLE "users" 
WITH FIELDS (
    {"age", false, false}
);
```
Perfect for: 
- `WHERE age > 18`
- `WHERE age >= 21 AND age <= 65`
- `ORDER BY age`

---

#### **Price Range Index**
```syndrql
CREATE B-INDEX "idx_price" ON BUNDLE "products" 
WITH FIELDS (
    {"price", true, false}
);
```
Perfect for:
- `WHERE price < 100`
- `WHERE price >= 50 AND price <= 200`
- `ORDER BY price DESC`

---

#### **Date Range Index**
```syndrql
CREATE B-INDEX "idx_created" ON BUNDLE "orders" 
WITH FIELDS (
    {"created_date", true, false}
);
```
Perfect for:
- `WHERE created_date > "2024-01-01"`
- `WHERE created_date >= "2024-01-01" AND created_date <= "2024-12-31"`
- `ORDER BY created_date DESC`

---

#### **Score Index**
```syndrql
CREATE B-INDEX "idx_score" ON BUNDLE "test_results" 
WITH FIELDS (
    {"score", true, false}
);
```
Perfect for:
- `WHERE score >= 70`
- `WHERE score > 90`
- `ORDER BY score DESC LIMIT 10`

---

### <a name="multiple-field-indexes"></a>Multiple Field Indexes

**Composite B-Tree Index** (future support):
```syndrql
CREATE B-INDEX "idx_user_date" ON BUNDLE "orders" 
WITH FIELDS (
    {"user_id", true, false},
    {"created_date", true, false}
);
```
Perfect for:
- `WHERE user_id == 42 ORDER BY created_date`
- `WHERE user_id == 42 AND created_date > "2024-01-01"`

**Note:** Composite indexes are planned but not yet fully implemented.

---

## Index Performance

### Hash Index Performance

| Operation | Performance | Use Case |
|-----------|-------------|----------|
| Exact match | O(1) - Constant time | `WHERE id == 123` |
| Multiple matches | O(1) per lookup | `WHERE status == "active"` |
| Index creation | O(n) - Linear | One-time cost |
| Insert/Update | O(1) - Constant | MemTable write |

**Benchmark Results:**
- Single lookup: < 1 μs (microsecond)
- 1000 lookups: < 1 ms (millisecond)
- Read-your-own-writes: Immediate consistency

---

### B-Tree Index Performance

| Operation | Performance | Use Case |
|-----------|-------------|----------|
| Exact match | O(log n) | `WHERE id == 123` |
| Range scan | O(log n + k) | `WHERE age > 18` |
| Index creation | O(n log n) | One-time cost |
| Insert/Update | O(log n) | Tree rebalancing |

**Benchmark Results:**
- Single lookup: 1-5 μs
- Range query (1000 docs): **10.6x faster** than full scan
- Sorted retrieval: Native support

**k** = number of results returned

---

### Index Overhead

| Aspect | Impact |
|--------|--------|
| **Disk space** | ~20-30% of data size per index |
| **Memory** | MemTable (hash) or tree nodes (btree) |
| **Insert time** | +5-10% per index |
| **Update time** | +5-10% per index |
| **Idle overhead** | Minimal when not queried |

**Recommendation:** Only create indexes on fields you frequently query.

---

## Best Practices

### ✅ DO:

1. **Index frequently queried fields:**
   ```syndrql
   -- If you often query by status, create an index
   CREATE HASH INDEX "idx_status" ON BUNDLE "orders" 
   WITH FIELDS ({"status", true, false});
   ```

2. **Use hash indexes for exact matches:**
   ```syndrql
   -- For WHERE field == value
   CREATE HASH INDEX "idx_email" ON BUNDLE "users" 
   WITH FIELDS ({"email", true, true});
   ```

3. **Use B-Tree indexes for ranges:**
   ```syndrql
   -- For WHERE field > value
   CREATE B-INDEX "idx_age" ON BUNDLE "users" 
   WITH FIELDS ({"age", false, false});
   ```

4. **Index foreign keys:**
   ```syndrql
   -- Automatic for relationships, but can create manually
   CREATE HASH INDEX "idx_user_fk" ON BUNDLE "orders" 
   WITH FIELDS ({"user_id", false, false});
   ```

5. **Use descriptive index names:**
   ```syndrql
   CREATE HASH INDEX "idx_users_email_unique" ...  -- ✅ Clear purpose
   CREATE B-INDEX "idx_products_price_range" ...    -- ✅ Indicates usage
   ```

6. **Set unique=true for unique constraints:**
   ```syndrql
   -- Enforce email uniqueness at database level
   CREATE HASH INDEX "idx_email" ON BUNDLE "users" 
   WITH FIELDS ({"email", true, true});
   ```

### ❌ DON'T:

1. **Don't over-index:**
   ```syndrql
   -- ❌ Don't index every field
   CREATE HASH INDEX "idx_field1" ...
   CREATE HASH INDEX "idx_field2" ...
   CREATE HASH INDEX "idx_field3" ...
   CREATE HASH INDEX "idx_field4" ...
   CREATE HASH INDEX "idx_field5" ...
   ```

2. **Don't use hash indexes for ranges:**
   ```syndrql
   -- ❌ Wrong index type
   CREATE HASH INDEX "idx_age" ON BUNDLE "users" 
   WITH FIELDS ({"age", false, false});
   -- This won't help: WHERE age > 18
   ```

3. **Don't use B-Tree for simple equality when hash is faster:**
   ```syndrql
   -- ❌ Slower than hash for exact matches
   CREATE B-INDEX "idx_id" ON BUNDLE "users" 
   WITH FIELDS ({"DocumentID", true, true});
   
   -- ✅ Better
   CREATE HASH INDEX "idx_id" ON BUNDLE "users" 
   WITH FIELDS ({"DocumentID", true, true});
   ```

4. **Don't use generic index names:**
   ```syndrql
   CREATE HASH INDEX "index1" ...  -- ❌ Unclear purpose
   CREATE HASH INDEX "idx" ...     -- ❌ Too vague
   ```

5. **Don't index low-cardinality fields (few distinct values):**
   ```syndrql
   -- ❌ Only has 2 values (true/false)
   CREATE HASH INDEX "idx_active" ON BUNDLE "users" 
   WITH FIELDS ({"is_active", true, false});
   ```

6. **Don't create duplicate indexes:**
   ```syndrql
   -- ❌ Already have idx_email, don't create another
   CREATE HASH INDEX "idx_email2" ON BUNDLE "users" 
   WITH FIELDS ({"email", true, true});
   ```

---

## Error Handling

### Common Errors

#### 1. **Index Already Exists**
```
Error: index 'idx_email' already exists in bundle 'users'
```
**Solution:** Use a different index name or drop the existing index first.

---

#### 2. **Bundle Not Found**
```
Error: bundle 'products' not found
```
**Solution:** Verify bundle name (case-sensitive) and ensure it exists.
```syndrql
SHOW BUNDLES;  -- List all bundles
```

---

#### 3. **Field Not in Bundle**
```
Error: field 'email' does not exist in bundle 'users'
```
**Solution:** Check bundle schema and use correct field name.
```syndrql
SHOW BUNDLE "users";  -- View bundle structure
```

---

#### 4. **Invalid Field Definition Syntax**
```
Error: invalid field definitions syntax
```
**Solution:** Ensure proper syntax with curly braces and commas.
```syndrql
-- ❌ Wrong
WITH FIELDS ("email", true, true)

-- ✅ Correct
WITH FIELDS ({"email", true, true})
```

---

#### 5. **Missing WITH FIELDS Clause**
```
Error: invalid CREATE HASH INDEX command syntax
```
**Solution:** Include WITH FIELDS clause.
```syndrql
-- ❌ Wrong
CREATE HASH INDEX "idx_email" ON BUNDLE "users";

-- ✅ Correct
CREATE HASH INDEX "idx_email" ON BUNDLE "users" 
WITH FIELDS ({"email", true, true});
```

---

#### 6. **Invalid Boolean Values**
```
Error: invalid field definition in CREATE HASH INDEX command
```
**Solution:** Use lowercase `true` or `false` (not TRUE/FALSE/1/0).
```syndrql
-- ❌ Wrong
WITH FIELDS ({"email", TRUE, FALSE})

-- ✅ Correct
WITH FIELDS ({"email", true, false})
```

---

#### 7. **Unique Constraint Violation**
```
Error: unique constraint violation - duplicate value 'user@example.com' for field 'email'
```
**Solution:** Remove duplicates before creating unique index, or set unique=false.

---

## Quick Reference

### Hash Index Syntax

```syndrql
CREATE HASH INDEX "<INDEX_NAME>" ON BUNDLE "<BUNDLE_NAME>" 
WITH FIELDS (
    {"<FIELD_NAME>", <REQUIRED>, <UNIQUE>}
);
```

**Example:**
```syndrql
CREATE HASH INDEX "idx_email" ON BUNDLE "users" 
WITH FIELDS ({"email", true, true});
```

---

### B-Tree Index Syntax

```syndrql
CREATE B-INDEX "<INDEX_NAME>" ON BUNDLE "<BUNDLE_NAME>" 
WITH FIELDS (
    {"<FIELD_NAME>", <REQUIRED>, <UNIQUE>}
);
```

**Example:**
```syndrql
CREATE B-INDEX "idx_age" ON BUNDLE "users" 
WITH FIELDS ({"age", false, false});
```

---

### Index Type Decision Matrix

| Query Type | Index Type | Reason |
|------------|------------|--------|
| `WHERE id == 123` | Hash | O(1) exact match |
| `WHERE age > 18` | B-Tree | Range query support |
| `WHERE status == "active"` | Hash | Equality check |
| `WHERE price >= 50 AND price <= 100` | B-Tree | Range query |
| `ORDER BY created_date` | B-Tree | Sorted access |
| `WHERE email == "user@example.com"` | Hash | Unique lookup |
| `WHERE score > 90` | B-Tree | Greater than |

---

### Field Definition Quick Guide

| Property | Type | Values | Purpose |
|----------|------|--------|---------|
| **Field Name** | String | `"field_name"` | Which field to index |
| **Required** | Boolean | `true`, `false` | Must exist in all docs? |
| **Unique** | Boolean | `true`, `false` | Enforce uniqueness? |

**Template:**
```syndrql
{"<field_name>", <required>, <unique>}
```

---

### Common Patterns

| Use Case | Index Configuration |
|----------|-------------------|
| **Primary Key** | `{"DocumentID", true, true}` - Hash |
| **Foreign Key** | `{"user_id", false, false}` - Hash |
| **Unique Email** | `{"email", true, true}` - Hash |
| **Age Range** | `{"age", false, false}` - B-Tree |
| **Price Range** | `{"price", true, false}` - B-Tree |
| **Optional Field** | `{"field", false, false}` - Either |

---

## Related Documentation

- [SELECT Queries](select_queries.md) - Query documents using indexes
- [CREATE BUNDLE](create_bundle.md) - Define bundle schemas
- [UPDATE BUNDLE](update_bundle.md) - Modify bundle structures
- [ADD DOCUMENTS](add_documents.md) - Insert documents that will be indexed

---

**Version:** SyndrDB 1.0  
**Last Updated:** November 2024  
**Status:** ✅ Implemented and tested

---

## Performance Tips

### 🚀 Optimization Strategies

1. **Create indexes before bulk inserts**
   - Indexes are updated on every insert
   - Creating after bulk load is more efficient

2. **Use selective indexes**
   - Index high-cardinality fields (many distinct values)
   - Skip low-cardinality fields (e.g., boolean flags)

3. **Monitor index usage**
   - Remove unused indexes to reduce overhead
   - Check query plans to verify index usage

4. **Consider composite indexes** (when supported)
   - One composite index can replace multiple single-field indexes
   - Order fields by selectivity (most selective first)

5. **Balance read vs write performance**
   - More indexes = faster reads, slower writes
   - Find the optimal number for your workload

---

### 📊 Index Statistics

Track these metrics to evaluate index effectiveness:

| Metric | Good | Bad | Action |
|--------|------|-----|--------|
| **Index hit rate** | > 80% | < 50% | Remove unused indexes |
| **Query speedup** | > 5x | < 2x | Consider different index type |
| **Insert overhead** | < 20% | > 50% | Reduce number of indexes |
| **Disk usage** | < 50% of data | > 100% of data | Optimize or remove |

---

**Happy Indexing! 🎯**
