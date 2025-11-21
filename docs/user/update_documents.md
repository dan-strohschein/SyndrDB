# ✏️ UPDATE DOCUMENTS - Modify Existing Documents

## Table of Contents
- [Overview](#overview)
- [Basic Syntax](#basic-syntax)
- [Keywords](#keywords)
  - [UPDATE](#update)
  - [DOCUMENTS](#documents)
  - [IN BUNDLE](#in-bundle)
  - [Field Updates](#field-updates)
  - [WHERE](#where)
- [Field Update Syntax](#field-update-syntax)
  - [Single Field Update](#single-field-update)
  - [Multiple Field Updates](#multiple-field-updates)
  - [Value Types](#value-types)
- [WHERE Clause](#where-clause)
  - [Comparison Operators](#comparison-operators)
  - [Logical Operators](#logical-operators)
  - [Parentheses for Grouping](#parentheses-for-grouping)
- [Examples](#examples)
  - [Single Field Updates](#single-field-updates)
  - [Multiple Field Updates](#multiple-field-updates-examples)
  - [Different Value Types](#different-value-types)
  - [Complex WHERE Clauses](#complex-where-clauses)
  - [Quoted Field Names](#quoted-field-names)
- [Best Practices](#best-practices)
- [Error Handling](#error-handling)
- [Quick Reference](#quick-reference)

---

## Overview

The `UPDATE DOCUMENTS` statement modifies one or more fields in existing documents within a bundle. Updates are applied to all documents matching the WHERE clause criteria.

⚠️ **WARNING**: Updates are permanent and cannot be undone. Always verify your WHERE clause before executing updates.

---

## Basic Syntax

```syndrql
UPDATE DOCUMENTS IN BUNDLE "<BUNDLE_NAME>" 
(<FIELD> = <VALUE>) 
WHERE <CONDITION>;
```

**Alternative (singular form):**
```syndrql
UPDATE DOCUMENT IN BUNDLE "<BUNDLE_NAME>" 
(<FIELD> = <VALUE>) 
WHERE <CONDITION>;
```

**Components:**
- `UPDATE DOCUMENTS` - Specifies update operation (DOCUMENT or DOCUMENTS)
- `IN BUNDLE "<BUNDLE_NAME>"` - Target bundle name (quoted)
- `(<FIELD> = <VALUE>)` - Field assignments in parentheses
- `WHERE <CONDITION>` - Filter criteria (required)

---

## Keywords

### <a name="update"></a>🔄 UPDATE

The `UPDATE` keyword indicates a document modification operation.

**Characteristics:**
- Modifies existing documents in place
- Requires WHERE clause to specify target documents
- Can update one or multiple fields simultaneously
- Updates bundle metadata (modification timestamps)
- Preserves fields not mentioned in update clause

---

### <a name="documents"></a>📄 DOCUMENTS

The `DOCUMENTS` keyword (or singular `DOCUMENT`) specifies that you are updating documents.

**Characteristics:**
- Both `DOCUMENT` and `DOCUMENTS` are valid
- Must be followed by `IN BUNDLE`
- Operates on document-level data

**Valid Forms:**
```syndrql
UPDATE DOCUMENTS IN BUNDLE ...
UPDATE DOCUMENT IN BUNDLE ...
```

---

### <a name="in-bundle"></a>📦 IN BUNDLE

The `IN BUNDLE` keywords specify the source bundle containing documents to update.

**Syntax:**
```syndrql
IN BUNDLE "<BUNDLE_NAME>"
```

**Characteristics:**
- Bundle name must be in double quotes
- Bundle must exist in the database
- Case-sensitive bundle name matching

**Valid Examples:**
```syndrql
UPDATE DOCUMENTS IN BUNDLE "users" ...
UPDATE DOCUMENTS IN BUNDLE "order_items" ...
UPDATE DOCUMENTS IN BUNDLE "UserProfiles" ...
```

---

### <a name="field-updates"></a>🏷️ Field Updates

Field updates are specified in parentheses with assignment syntax: `field = value`

**Syntax:**
```syndrql
(<FIELD1> = <VALUE1>, <FIELD2> = <VALUE2>, ...)
```

**Characteristics:**
- Enclosed in parentheses `( )`
- Multiple fields separated by commas `,`
- Uses single equals `=` for assignment (not `==`)
- Field names can be quoted or unquoted
- At least one field update required

---

### <a name="where"></a>🔍 WHERE

The `WHERE` clause is **required** and defines which documents to update based on filtering conditions.

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

## Field Update Syntax

### <a name="single-field-update"></a>Single Field Update

Update one field at a time:

```syndrql
UPDATE DOCUMENTS IN BUNDLE "users" 
(name = "John Doe") 
WHERE id = 1;
```

**Notes:**
- Field name: `name` (can be quoted: `"name"`)
- Assignment operator: `=` (single equals)
- Value: `"John Doe"` (string in quotes)

---

### <a name="multiple-field-updates"></a>Multiple Field Updates

Update multiple fields in one statement:

```syndrql
UPDATE DOCUMENTS IN BUNDLE "users" 
(name = "Jane", age = 30, active = true) 
WHERE id = 2;
```

**Notes:**
- Multiple assignments separated by commas
- Each field can have different value type
- All updates applied atomically to matching documents

---

### <a name="value-types"></a>Value Types

SyndrDB supports the following value types in UPDATE statements:

#### **String Values**
```syndrql
UPDATE DOCUMENTS IN BUNDLE "users" 
(name = "Alice", email = "alice@example.com") 
WHERE id = 1;
```
- Must be enclosed in double quotes
- Empty strings allowed: `name = ""`

#### **Numeric Values**

**Integers:**
```syndrql
UPDATE DOCUMENTS IN BUNDLE "products" 
(quantity = 100, stock = 50) 
WHERE sku = "ABC123";
```

**Floats:**
```syndrql
UPDATE DOCUMENTS IN BUNDLE "products" 
(price = 99.99, discount = 0.15) 
WHERE sku = "ABC123";
```

#### **Boolean Values**
```syndrql
UPDATE DOCUMENTS IN BUNDLE "settings" 
(enabled = true, verified = false) 
WHERE name = "feature_x";
```
- Use lowercase: `true` or `false`
- No quotes

#### **NULL Values**
```syndrql
UPDATE DOCUMENTS IN BUNDLE "users" 
(middle_name = NULL, suffix = NULL) 
WHERE id = 3;
```
- Use uppercase: `NULL`
- Removes/clears the field value

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

**Note:** WHERE clause uses `==` for comparison, while field updates use `=` for assignment.

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
-- Update users who are either minors OR seniors, but only if inactive
UPDATE DOCUMENTS IN BUNDLE "users" 
(status = "review") 
WHERE (age < 18 OR age > 65) AND status == "inactive";

-- Update orders that are pending AND either high priority OR high value
UPDATE DOCUMENTS IN BUNDLE "orders" 
(review_flag = true) 
WHERE status == "pending" AND (priority == "high" OR amount > 1000);
```

---

## Examples

### <a name="single-field-updates"></a>Single Field Updates

**Update string field:**
```syndrql
UPDATE DOCUMENTS IN BUNDLE "users" 
(name = "John Doe") 
WHERE id = 1;
```

**Update numeric field:**
```syndrql
UPDATE DOCUMENTS IN BUNDLE "products" 
(price = 99) 
WHERE sku = "ABC123";
```

**Update boolean field:**
```syndrql
UPDATE DOCUMENTS IN BUNDLE "settings" 
(enabled = true) 
WHERE name = "feature_x";
```

**Set field to NULL:**
```syndrql
UPDATE DOCUMENTS IN BUNDLE "users" 
(middle_name = NULL) 
WHERE id = 3;
```

**Without semicolon (optional):**
```syndrql
UPDATE DOCUMENTS IN BUNDLE "users" 
(name = "Bob") 
WHERE id = 4
```

---

### <a name="multiple-field-updates-examples"></a>Multiple Field Updates

**Update multiple fields with different types:**
```syndrql
UPDATE DOCUMENTS IN BUNDLE "users" 
(name = "Jane", age = 30, active = true) 
WHERE id = 2;
```

**Update product details:**
```syndrql
UPDATE DOCUMENTS IN BUNDLE "products" 
(price = 99.99, stock = 150, available = true, last_updated = "2024-11-20") 
WHERE category = "electronics";
```

**Clear optional fields:**
```syndrql
UPDATE DOCUMENTS IN BUNDLE "users" 
(middle_name = NULL, suffix = NULL, nickname = NULL) 
WHERE id = 5;
```

---

### <a name="different-value-types"></a>Different Value Types

**Integer value:**
```syndrql
UPDATE DOCUMENTS IN BUNDLE "products" 
(quantity = 100) 
WHERE sku = "XYZ789";
```

**Float value:**
```syndrql
UPDATE DOCUMENTS IN BUNDLE "products" 
(price = 99.99) 
WHERE sku = "ABC123";
```

**Boolean true:**
```syndrql
UPDATE DOCUMENTS IN BUNDLE "settings" 
(enabled = true) 
WHERE name = "feature_x";
```

**Boolean false:**
```syndrql
UPDATE DOCUMENTS IN BUNDLE "settings" 
(enabled = false) 
WHERE name = "feature_y";
```

**NULL value:**
```syndrql
UPDATE DOCUMENTS IN BUNDLE "users" 
(middle_name = NULL) 
WHERE id = 3;
```

**String with special characters:**
```syndrql
UPDATE DOCUMENTS IN BUNDLE "users" 
(email = "user@example.com", bio = "Software Engineer @ Tech Corp") 
WHERE id = 10;
```

---

### <a name="complex-where-clauses"></a>Complex WHERE Clauses

**Simple equality:**
```syndrql
UPDATE DOCUMENTS IN BUNDLE "users" 
(last_login = "2024-11-20") 
WHERE id = 1;
```

**Comparison operators:**
```syndrql
-- Greater than
UPDATE DOCUMENTS IN BUNDLE "products" 
(clearance = true) 
WHERE price > 1000;

-- Less than
UPDATE DOCUMENTS IN BUNDLE "inventory" 
(reorder_flag = true) 
WHERE stock < 10;

-- Not equal
UPDATE DOCUMENTS IN BUNDLE "users" 
(status = "review") 
WHERE role != "admin";
```

**AND conditions:**
```syndrql
UPDATE DOCUMENTS IN BUNDLE "orders" 
(status = "processing") 
WHERE payment_status == "paid" AND inventory_status == "available";
```

**OR conditions:**
```syndrql
UPDATE DOCUMENTS IN BUNDLE "users" 
(tier = "premium") 
WHERE total_purchases > 1000 OR subscription == "gold";
```

**Complex grouped conditions:**
```syndrql
UPDATE DOCUMENTS IN BUNDLE "products" 
(featured = true) 
WHERE (category == "electronics" OR category == "computers") 
  AND price < 500 
  AND stock > 10;
```

**Age-based status update:**
```syndrql
UPDATE DOCUMENTS IN BUNDLE "users" 
(age_group = "senior") 
WHERE age >= 65 AND status == "active";
```

---

### <a name="quoted-field-names"></a>Quoted Field Names

Field names can be quoted if they contain special characters or match keywords:

**Update using quoted field names:**
```syndrql
UPDATE DOCUMENTS IN BUNDLE "users" 
("FirstName" = "John", "LastName" = "Doe") 
WHERE "UserID" = 123;
```

**Mixed quoted and unquoted fields:**
```syndrql
UPDATE DOCUMENTS IN BUNDLE "products" 
("ProductID" = "NEW-123", name = "Updated Product") 
WHERE id = 456;
```

**Fields with special characters:**
```syndrql
UPDATE DOCUMENTS IN BUNDLE "data" 
("field-with-dashes" = "new-value", "field_with_underscores" = 42) 
WHERE "record-id" == "abc-123";
```

---

## Best Practices

### ✅ DO:

1. **Test WHERE clause with SELECT first:**
   ```syndrql
   -- 1. Preview what will be updated
   SELECT * FROM "users" WHERE status == "pending";
   
   -- 2. Then update if correct
   UPDATE DOCUMENTS IN BUNDLE "users" 
   (status = "active") 
   WHERE status == "pending";
   ```

2. **Use specific WHERE conditions:**
   ```syndrql
   -- ✅ Specific update
   UPDATE DOCUMENTS IN BUNDLE "users" 
   (verified = true) 
   WHERE email == "user@example.com" AND verification_code == "ABC123";
   ```

3. **Update multiple related fields together:**
   ```syndrql
   -- ✅ Atomic multi-field update
   UPDATE DOCUMENTS IN BUNDLE "orders" 
   (status = "shipped", shipped_date = "2024-11-20", tracking = "TRK123") 
   WHERE id = 1001;
   ```

4. **Use appropriate data types:**
   ```syndrql
   -- ✅ Correct types
   UPDATE DOCUMENTS IN BUNDLE "products" 
   (price = 99.99, stock = 100, available = true) 
   WHERE sku = "ABC";
   ```

5. **Update by unique identifiers when possible:**
   ```syndrql
   -- ✅ Update specific document
   UPDATE DOCUMENTS IN BUNDLE "users" 
   (last_login = "2024-11-20") 
   WHERE DocumentID == "abc-123";
   ```

6. **Use NULL to clear optional fields:**
   ```syndrql
   -- ✅ Clear optional data
   UPDATE DOCUMENTS IN BUNDLE "users" 
   (temp_token = NULL, temp_token_expires = NULL) 
   WHERE id = 5;
   ```

### ❌ DON'T:

1. **Don't update without testing WHERE clause:**
   ```syndrql
   -- ❌ Test first!
   UPDATE DOCUMENTS IN BUNDLE "users" (status = "deleted") WHERE active = false;
   ```

2. **Don't use broad WHERE conditions on critical data:**
   ```syndrql
   -- ❌ Too broad!
   UPDATE DOCUMENTS IN BUNDLE "accounts" (balance = 0) WHERE true == true;
   ```

3. **Don't forget field type compatibility:**
   ```syndrql
   -- ❌ Wrong - age should be numeric
   UPDATE DOCUMENTS IN BUNDLE "users" (age = "25") WHERE id = 1;
   
   -- ✅ Correct
   UPDATE DOCUMENTS IN BUNDLE "users" (age = 25) WHERE id = 1;
   ```

4. **Don't use == in field assignments:**
   ```syndrql
   -- ❌ Wrong operator
   UPDATE DOCUMENTS IN BUNDLE "users" (name == "John") WHERE id = 1;
   
   -- ✅ Correct
   UPDATE DOCUMENTS IN BUNDLE "users" (name = "John") WHERE id = 1;
   ```

5. **Don't update without considering related data:**
   ```syndrql
   -- ❌ May break referential integrity
   UPDATE DOCUMENTS IN BUNDLE "users" (id = 999) WHERE id = 1;
   ```

---

## Error Handling

### Common Errors

#### 1. **Missing WHERE Clause**
```
Error: expected keyword 'WHERE', got <token>
```
**Solution:** Always include WHERE clause.
```syndrql
UPDATE DOCUMENTS IN BUNDLE "users" 
(name = "Bob") 
WHERE id = 1;  -- ✅ Correct
```

---

#### 2. **Bundle Not Found**
```
Error: bundle 'xyz' not found
```
**Solution:** Verify bundle name (case-sensitive) exists in database.

---

#### 3. **Empty Field Set**
```
Error: UPDATE statement must specify at least one field to update
```
**Solution:** Provide at least one field assignment.
```syndrql
-- ❌ Wrong
UPDATE DOCUMENTS IN BUNDLE "users" () WHERE id = 1;

-- ✅ Correct
UPDATE DOCUMENTS IN BUNDLE "users" (name = "John") WHERE id = 1;
```

---

#### 4. **Missing Parentheses**
```
Error: expected '(' after bundle name
```
**Solution:** Enclose field assignments in parentheses.
```syndrql
-- ❌ Wrong
UPDATE DOCUMENTS IN BUNDLE "users" name = "Bob" WHERE id = 1;

-- ✅ Correct
UPDATE DOCUMENTS IN BUNDLE "users" (name = "Bob") WHERE id = 1;
```

---

#### 5. **Invalid Field Assignment Syntax**
```
Error: expected '=' after field name
```
**Solution:** Use single equals `=` for assignment (not `==`).
```syndrql
-- ❌ Wrong
UPDATE DOCUMENTS IN BUNDLE "users" (name == "Bob") WHERE id = 1;

-- ✅ Correct
UPDATE DOCUMENTS IN BUNDLE "users" (name = "Bob") WHERE id = 1;
```

---

#### 6. **Invalid WHERE Syntax**
```
Error: failed to parse WHERE expression
```
**Solution:** Check WHERE clause uses `==` for comparison.
```syndrql
-- ❌ Wrong
UPDATE DOCUMENTS IN BUNDLE "users" (name = "Bob") WHERE id = 1;

-- ✅ Correct
UPDATE DOCUMENTS IN BUNDLE "users" (name = "Bob") WHERE id == 1;
```

---

#### 7. **Type Mismatch**
```
Error: type mismatch in field assignment
```
**Solution:** Ensure value types match field definitions.
```syndrql
-- ❌ Wrong - price should be numeric
UPDATE DOCUMENTS IN BUNDLE "products" (price = "99.99") WHERE id = 1;

-- ✅ Correct
UPDATE DOCUMENTS IN BUNDLE "products" (price = 99.99) WHERE id = 1;
```

---

#### 8. **No Documents Matched**
```
Info: 0 documents updated
```
**Explanation:** No documents matched the WHERE clause criteria. This is not an error, but verify your WHERE clause is correct.

---

## Quick Reference

### Basic Syntax Patterns

```syndrql
-- Single field update
UPDATE DOCUMENTS IN BUNDLE "bundle" (field = value) WHERE condition;

-- Multiple fields update
UPDATE DOCUMENTS IN BUNDLE "bundle" 
(field1 = value1, field2 = value2) 
WHERE condition;

-- Different value types
UPDATE DOCUMENTS IN BUNDLE "bundle" 
(str_field = "text", num_field = 42, bool_field = true, null_field = NULL) 
WHERE condition;

-- Complex WHERE
UPDATE DOCUMENTS IN BUNDLE "bundle" 
(field = value) 
WHERE (cond1 OR cond2) AND cond3;
```

### Assignment vs Comparison

| Context | Operator | Example |
|---------|----------|---------|
| **Field Assignment** | `=` | `(name = "John")` |
| **WHERE Comparison** | `==` | `WHERE id == 1` |

### Comparison Operators (WHERE clause)

| Operator | Usage | Example |
|----------|-------|---------|
| `==` | Equality | `WHERE id == 1` |
| `!=` | Inequality | `WHERE status != "active"` |
| `<` | Less than | `WHERE age < 18` |
| `>` | Greater than | `WHERE price > 100` |
| `<=` | Less/equal | `WHERE quantity <= 5` |
| `>=` | Greater/equal | `WHERE score >= 90` |

### Logical Operators

| Operator | Usage | Example |
|----------|-------|---------|
| `AND` | Both true | `WHERE age > 18 AND verified == true` |
| `OR` | Either true | `WHERE role == "admin" OR role == "mod"` |

### Value Types

| Type | Example | Notes |
|------|---------|-------|
| **String** | `"John"` | Double quotes |
| **Integer** | `25` | No quotes |
| **Float** | `99.99` | Decimal point |
| **Boolean** | `true`, `false` | Lowercase, no quotes |
| **NULL** | `NULL` | Uppercase, clears field |

### Field Names

| Type | Example | When to Use |
|------|---------|-------------|
| **Unquoted** | `name` | Standard field names |
| **Quoted** | `"FirstName"` | Special characters, case-sensitive |

---

## Related Documentation

- [ADD DOCUMENTS](add_documents.md) - Insert documents into bundles
- [DELETE DOCUMENTS](delete_documents.md) - Remove documents from bundles
- [SELECT Queries](select_queries.md) - Query documents from bundles
- [CREATE BUNDLE](create_bundle.md) - Define bundle schemas

---

**Version:** SyndrDB 1.0  
**Last Updated:** November 2024  
**Status:** ✅ Implemented and tested
