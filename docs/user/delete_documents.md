# 🗑️ DELETE DOCUMENTS - Remove Documents from Bundles

## Table of Contents
- [Overview](#overview)
- [Basic Syntax](#basic-syntax)
- [Keywords](#keywords)
  - [DELETE](#delete)
  - [DOCUMENTS](#documents)
  - [FROM](#from)
  - [WHERE](#where)
- [WHERE Clause](#where-clause)
  - [Comparison Operators](#comparison-operators)
  - [Logical Operators](#logical-operators)
  - [Parentheses for Grouping](#parentheses-for-grouping)
- [Examples](#examples)
  - [Simple Equality](#simple-equality)
  - [Comparison Operators](#comparison-operators-examples)
  - [Logical Conditions](#logical-conditions)
  - [Complex Filtering](#complex-filtering)
  - [Quoted Field Names](#quoted-field-names)
- [Best Practices](#best-practices)
- [Error Handling](#error-handling)
- [Quick Reference](#quick-reference)

---

## Overview

The `DELETE DOCUMENTS` statement removes one or more documents from a bundle based on specified filtering criteria. This operation is permanent and cannot be undone.

⚠️ **WARNING**: Deleted documents cannot be recovered. Always verify your WHERE clause before executing deletion.

---

## Basic Syntax

```syndrql
DELETE DOCUMENTS FROM "<BUNDLE_NAME>" WHERE <CONDITION>;
```

**Components:**
- `DELETE DOCUMENTS` - Specifies deletion operation
- `FROM "<BUNDLE_NAME>"` - Target bundle name (quoted)
- `WHERE <CONDITION>` - Filter criteria (required)

---

## Keywords

### <a name="delete"></a>🔑 DELETE

The `DELETE` keyword indicates a document removal operation.

**Characteristics:**
- Permanently removes matching documents
- Requires WHERE clause for safety
- Updates bundle metadata (document count)
- Invalidates relevant caches
- Writes tombstone markers to bundle file

**Safety Features:**
- ⚠️ DELETE without WHERE is currently **not supported** to prevent accidental data loss
- All deletions require explicit filtering criteria

---

### <a name="documents"></a>📄 DOCUMENTS

The `DOCUMENTS` keyword specifies that you are deleting documents (as opposed to bundles or other entities).

**Characteristics:**
- Can be singular `DOCUMENT` or plural `DOCUMENTS` (both accepted)
- Must be followed by `FROM` keyword
- Operates on document-level data

---

### <a name="from"></a>📦 FROM

The `FROM` keyword specifies the source bundle containing documents to delete.

**Syntax:**
```syndrql
FROM "<BUNDLE_NAME>"
```

**Characteristics:**
- Bundle name must be in double quotes
- Bundle must exist in the database
- Case-sensitive bundle name matching

**Valid Examples:**
```syndrql
DELETE DOCUMENTS FROM "users" WHERE ...
DELETE DOCUMENTS FROM "order_items" WHERE ...
DELETE DOCUMENTS FROM "UserProfiles" WHERE ...
```

---

### <a name="where"></a>🔍 WHERE

The `WHERE` clause is **required** and defines which documents to delete based on filtering conditions.

**Syntax:**
```syndrql
WHERE <field> <operator> <value>
```

**Characteristics:**
- **Mandatory** - cannot be omitted
- Supports comparison operators: `==`, `!=`, `<`, `>`, `<=`, `>=`
- Supports logical operators: `AND`, `OR`
- Supports parentheses for grouping conditions
- Can reference any field in the document

---

## WHERE Clause

### <a name="comparison-operators"></a>Comparison Operators

SyndrDB supports the following comparison operators in WHERE clauses:

| Operator | Description | Example |
|----------|-------------|---------|
| `==` | Equal to | `WHERE id == 1` |
| `!=` | Not equal to | `WHERE status != "active"` |
| `<` | Less than | `WHERE age < 18` |
| `>` | Greater than | `WHERE price > 100` |
| `<=` | Less than or equal | `WHERE quantity <= 5` |
| `>=` | Greater than or equal | `WHERE score >= 90` |

**Type Support:**
- **Numbers**: `age == 25`, `price > 99.99`
- **Strings**: `name == "John"`, `status != "inactive"`
- **Booleans**: `active == true`, `verified == false`
- **Field Names**: Can be quoted or unquoted

---

### <a name="logical-operators"></a>Logical Operators

Combine multiple conditions using logical operators:

| Operator | Description | Example |
|----------|-------------|---------|
| `AND` | Both conditions must be true | `WHERE age > 18 AND status == "active"` |
| `OR` | At least one condition must be true | `WHERE role == "admin" OR role == "moderator"` |

**Precedence:**
- `AND` has higher precedence than `OR`
- Use parentheses to control evaluation order

---

### <a name="parentheses-for-grouping"></a>Parentheses for Grouping

Use parentheses `()` to group conditions and control evaluation order:

```syndrql
-- Delete users who are either minors OR seniors, but only if inactive
DELETE DOCUMENTS FROM "users" 
WHERE (age < 18 OR age > 65) AND status == "inactive";

-- Delete orders that are pending AND either high priority OR high value
DELETE DOCUMENTS FROM "orders" 
WHERE status == "pending" AND (priority == "high" OR amount > 1000);
```

---

## Examples

### <a name="simple-equality"></a>Simple Equality

**Delete by ID:**
```syndrql
DELETE DOCUMENTS FROM "users" WHERE id == 1;
```

**Delete by string field:**
```syndrql
DELETE DOCUMENTS FROM "users" WHERE name == "John Doe";
```

**Delete by boolean:**
```syndrql
DELETE DOCUMENTS FROM "settings" WHERE enabled == false;
```

**Without semicolon (optional):**
```syndrql
DELETE DOCUMENTS FROM "users" WHERE id == 99
```

---

### <a name="comparison-operators-examples"></a>Comparison Operators

**Not equal:**
```syndrql
DELETE DOCUMENTS FROM "users" WHERE status != "active";
```

**Less than:**
```syndrql
DELETE DOCUMENTS FROM "products" WHERE stock < 10;
```

**Greater than:**
```syndrql
DELETE DOCUMENTS FROM "products" WHERE price > 1000;
```

**Less than or equal:**
```syndrql
DELETE DOCUMENTS FROM "orders" WHERE quantity <= 5;
```

**Greater than or equal:**
```syndrql
DELETE DOCUMENTS FROM "employees" WHERE age >= 65;
```

---

### <a name="logical-conditions"></a>Logical Conditions

**AND operator:**
```syndrql
DELETE DOCUMENTS FROM "users" 
WHERE age > 30 AND status == "inactive";
```

**OR operator:**
```syndrql
DELETE DOCUMENTS FROM "users" 
WHERE role == "guest" OR verified == false;
```

**Multiple AND conditions:**
```syndrql
DELETE DOCUMENTS FROM "products" 
WHERE price > 100 AND stock < 10 AND discontinued == true;
```

**Mixed AND/OR with parentheses:**
```syndrql
DELETE DOCUMENTS FROM "orders" 
WHERE status == "pending" AND (amount > 1000 OR priority == "high");
```

---

### <a name="complex-filtering"></a>Complex Filtering

**Age range with status:**
```syndrql
DELETE DOCUMENTS FROM "users" 
WHERE (age < 18 OR age > 65) AND active == true;
```

**Multi-field product cleanup:**
```syndrql
DELETE DOCUMENTS FROM "products" 
WHERE discontinued == true AND stock == 0 AND price < 5;
```

**Order cancellation cleanup:**
```syndrql
DELETE DOCUMENTS FROM "orders" 
WHERE status == "cancelled" AND created_date < "2024-01-01";
```

**Expired sessions:**
```syndrql
DELETE DOCUMENTS FROM "sessions" 
WHERE expires_at < "2024-11-20" OR active == false;
```

---

### <a name="quoted-field-names"></a>Quoted Field Names

Field names can be quoted if they contain special characters or match keywords:

**DocumentID field (quoted):**
```syndrql
DELETE DOCUMENTS FROM "users" WHERE "DocumentID" == "123e4567-e89b-12d3-a456-426614174000";
```

**Multiple quoted fields:**
```syndrql
DELETE DOCUMENTS FROM "products" 
WHERE "ProductID" == 456 AND "CategoryID" == 789;
```

**Mixed quoted and unquoted:**
```syndrql
DELETE DOCUMENTS FROM "orders" 
WHERE "OrderID" == 1 AND status == "cancelled";
```

**Fields with special characters:**
```syndrql
DELETE DOCUMENTS FROM "data" 
WHERE "field-with-dashes" == "value";
```

---

## Best Practices

### ✅ DO:

1. **Test WHERE clause with SELECT first:**
   ```syndrql
   -- 1. Preview what will be deleted
   SELECT * FROM "users" WHERE status == "inactive";
   
   -- 2. Then delete if correct
   DELETE DOCUMENTS FROM "users" WHERE status == "inactive";
   ```

2. **Use specific conditions:**
   ```syndrql
   -- ✅ Specific deletion
   DELETE DOCUMENTS FROM "logs" WHERE created_date < "2024-01-01";
   ```

3. **Combine multiple conditions for safety:**
   ```syndrql
   -- ✅ Safe multi-condition deletion
   DELETE DOCUMENTS FROM "temp_data" 
   WHERE type == "temporary" AND expires_at < "2024-11-20";
   ```

4. **Delete by unique identifiers when possible:**
   ```syndrql
   -- ✅ Delete specific document
   DELETE DOCUMENTS FROM "users" WHERE DocumentID == "abc-123";
   ```

5. **Use transactions for related deletions:**
   ```syndrql
   -- Delete related documents in order
   DELETE DOCUMENTS FROM "order_items" WHERE order_id == 123;
   DELETE DOCUMENTS FROM "orders" WHERE id == 123;
   ```

### ❌ DON'T:

1. **Don't delete without testing WHERE clause:**
   ```syndrql
   DELETE DOCUMENTS FROM "users" WHERE name = "John";  -- ❌ Test first!
   ```

2. **Don't use broad conditions on production data:**
   ```syndrql
   DELETE DOCUMENTS FROM "customers" WHERE true == true;  -- ❌ Too broad!
   ```

3. **Don't forget referential integrity:**
   ```syndrql
   DELETE DOCUMENTS FROM "users" WHERE id == 1;  -- ❌ Check for related orders first!
   ```

4. **Don't use ambiguous conditions:**
   ```syndrql
   DELETE DOCUMENTS FROM "products" WHERE active;  -- ❌ Not supported - use == true
   ```

---

## Error Handling

### Common Errors

#### 1. **Missing WHERE Clause**
```
Error: expected WHERE after bundle name
```
**Solution:** Always include WHERE clause.
```syndrql
DELETE DOCUMENTS FROM "users" WHERE id == 1;  -- ✅ Correct
```

---

#### 2. **Bundle Not Found**
```
Error: bundle 'xyz' not found
```
**Solution:** Verify bundle name (case-sensitive) exists in database.

---

#### 3. **Invalid WHERE Syntax**
```
Error: invalid WHERE clause expression
```
**Solution:** Check operator syntax and field references.
```syndrql
-- ❌ Wrong
DELETE DOCUMENTS FROM "users" WHERE id = 1;

-- ✅ Correct
DELETE DOCUMENTS FROM "users" WHERE id == 1;
```

---

#### 4. **Empty WHERE Clause**
```
Error: WHERE clause cannot be empty
```
**Solution:** Provide at least one condition.
```syndrql
DELETE DOCUMENTS FROM "users" WHERE status == "inactive";
```

---

#### 5. **Type Mismatch**
```
Error: type mismatch in comparison
```
**Solution:** Ensure value types match field types.
```syndrql
-- ❌ Wrong - age is numeric
DELETE DOCUMENTS FROM "users" WHERE age == "25";

-- ✅ Correct
DELETE DOCUMENTS FROM "users" WHERE age == 25;
```

---

#### 6. **No Documents Matched**
```
Info: 0 documents deleted
```
**Explanation:** No documents matched the WHERE clause criteria. This is not an error, but verify your WHERE clause is correct.

---

#### 7. **Referential Integrity Violation**
```
Error: cannot delete document - referenced by other documents
```
**Solution:** Delete dependent documents first or use cascade delete (if implemented).

---

## Quick Reference

### Basic Syntax Patterns

```syndrql
-- Simple equality
DELETE DOCUMENTS FROM "bundle" WHERE field == value;

-- Comparison
DELETE DOCUMENTS FROM "bundle" WHERE field > value;

-- Logical AND
DELETE DOCUMENTS FROM "bundle" WHERE field1 == value1 AND field2 == value2;

-- Logical OR
DELETE DOCUMENTS FROM "bundle" WHERE field1 == value1 OR field2 == value2;

-- Grouped conditions
DELETE DOCUMENTS FROM "bundle" WHERE (cond1 OR cond2) AND cond3;

-- Quoted fields
DELETE DOCUMENTS FROM "bundle" WHERE "FieldName" == value;
```

### Comparison Operators

| Operator | Usage | Example |
|----------|-------|---------|
| `==` | Equality | `id == 1` |
| `!=` | Inequality | `status != "active"` |
| `<` | Less than | `age < 18` |
| `>` | Greater than | `price > 100` |
| `<=` | Less/equal | `quantity <= 5` |
| `>=` | Greater/equal | `score >= 90` |

### Logical Operators

| Operator | Usage | Example |
|----------|-------|---------|
| `AND` | Both true | `age > 18 AND verified == true` |
| `OR` | Either true | `role == "admin" OR role == "mod"` |

### Value Types

| Type | Example | Notes |
|------|---------|-------|
| **String** | `"John"` | Double quotes |
| **Integer** | `25` | No quotes |
| **Float** | `99.99` | Decimal point |
| **Boolean** | `true`, `false` | Lowercase |
| **NULL** | `NULL` | Uppercase |

---

## Related Documentation

- [ADD DOCUMENTS](add_documents.md) - Insert documents into bundles
- [UPDATE DOCUMENTS](update_documents.md) - Modify existing documents
- [SELECT Queries](select_queries.md) - Query documents from bundles
- [DROP BUNDLE](drop_bundle.md) - Delete entire bundle schema

---

**Version:** SyndrDB 1.0  
**Last Updated:** November 2024  
**Status:** ✅ Implemented and tested
