# 🗑️ DROP BUNDLE - Delete Bundle Schema

## Table of Contents
- [Overview](#overview)
- [Basic Syntax](#basic-syntax)
- [Keywords](#keywords)
  - [DROP](#drop)
  - [BUNDLE](#bundle)
  - [WITH FORCE](#with-force)
- [Bundle Name](#bundle-name)
- [Examples](#examples)
  - [Simple Drop](#simple-drop)
  - [Drop with Force](#drop-with-force-1)
  - [Naming Variations](#naming-variations)
- [Best Practices](#best-practices)
- [Error Handling](#error-handling)
- [Quick Reference](#quick-reference)

---

## Overview

The `DROP BUNDLE` statement permanently deletes a bundle (schema) from the database. This operation removes the bundle definition and, when used with `WITH FORCE`, can also delete all documents and relationships associated with that bundle.

⚠️ **WARNING**: This is a destructive operation that cannot be undone. Always backup your data before dropping bundles.

---

## Basic Syntax

```syndrql
DROP BUNDLE "<BUNDLE_NAME>";
```

**Optional WITH FORCE clause:**
```syndrql
DROP BUNDLE "<BUNDLE_NAME>" WITH FORCE;
```

---

## Keywords

### <a name="drop"></a>🔑 DROP

The `DROP` keyword indicates a deletion operation. In SyndrDB, `DROP` is specifically used for removing bundle schemas.

**Characteristics:**
- Initiates bundle deletion process
- Requires appropriate permissions
- Cannot be undone once executed
- May be blocked if bundle contains documents or relationships (without FORCE)

**Related Commands:**
- `CREATE BUNDLE` - Creates a new bundle schema
- `DELETE DOCUMENTS` - Deletes documents from a bundle

---

### <a name="bundle"></a>📦 BUNDLE

The `BUNDLE` keyword specifies that you are dropping a bundle (table/collection equivalent).

**Characteristics:**
- Must be followed by a quoted bundle name
- Bundle must exist in the database
- Case-sensitive bundle name matching

---

### <a name="with-force"></a>💪 WITH FORCE

The optional `WITH FORCE` clause allows dropping a bundle even if it contains documents or has relationships to other bundles.

**Characteristics:**
- Bypasses safety checks for non-empty bundles
- Deletes all documents in the bundle
- Removes all relationships involving the bundle
- **Use with extreme caution** - this is a destructive operation
- Recommended for migrations and cleanup operations

**Without FORCE:**
```syndrql
-- This will FAIL if bundle has documents or relationships
DROP BUNDLE "old_users";
```

**With FORCE:**
```syndrql
-- This will DELETE everything and drop the bundle
DROP BUNDLE "old_users" WITH FORCE;
```

**Safety Considerations:**
- ⚠️ All documents in the bundle will be permanently deleted
- ⚠️ All relationships (1toMany, 0toMany, ManyToMany) will be removed
- ⚠️ This may break referential integrity in related bundles
- ⚠️ No confirmation prompt - executes immediately
- ✅ Consider backing up data before using FORCE

---

## Bundle Name

Bundle names must be:
- **Enclosed in double quotes**: `"users"`, `"order_items"`
- **Case-sensitive**: `"Users"` and `"users"` are different bundles
- **Non-empty**: Cannot be an empty string

**Valid Examples:**
```syndrql
DROP BUNDLE "users";
DROP BUNDLE "user_profiles";
DROP BUNDLE "user-sessions";
DROP BUNDLE "logs2025";
DROP BUNDLE "OrderItems";
```

**Invalid Examples:**
```syndrql
DROP BUNDLE users;           -- ❌ Missing quotes
DROP BUNDLE "";              -- ❌ Empty bundle name
DROP BUNDLE;                 -- ❌ Missing bundle name
```

---

## Examples

### <a name="simple-drop"></a>Simple Drop

**Drop empty bundle:**
```syndrql
DROP BUNDLE "temp_data";
```

**Drop bundle without semicolon (optional):**
```syndrql
DROP BUNDLE "logs"
```

**Drop with whitespace:**
```syndrql
DROP BUNDLE
    "archived_users";
```

---

### <a name="drop-with-force-1"></a>Drop with Force

**Drop bundle with documents:**
```syndrql
-- Bundle has 1,000 documents - force deletion
DROP BUNDLE "old_orders" WITH FORCE;
```

**Drop bundle with relationships:**
```syndrql
-- Bundle has relationships to other bundles - force removal
DROP BUNDLE "legacy_products" WITH FORCE;
```

**Drop bundle with both documents and relationships:**
```syndrql
-- Complete cleanup of old data structure
DROP BUNDLE "deprecated_schema" WITH FORCE;
```

---

### <a name="naming-variations"></a>Naming Variations

**Bundle with underscores:**
```syndrql
DROP BUNDLE "user_sessions";
```

**Bundle with hyphens:**
```syndrql
DROP BUNDLE "api-keys";
```

**Bundle with numbers:**
```syndrql
DROP BUNDLE "logs_2024";
```

**Mixed case bundle:**
```syndrql
DROP BUNDLE "UserProfiles";
```

---

## Best Practices

### ✅ DO:

1. **Check bundle contents first:**
   ```syndrql
   -- Query documents before dropping
   SELECT COUNT(*) FROM "old_data";
   
   -- Then drop if safe
   DROP BUNDLE "old_data";
   ```

2. **Export data before dropping:**
   ```syndrql
   -- Export important data first
   SELECT * FROM "archive_users";
   
   -- Then drop
   DROP BUNDLE "archive_users" WITH FORCE;
   ```

3. **Use descriptive bundle names:**
   ```syndrql
   DROP BUNDLE "temp_migration_2024_11";  -- ✅ Clear purpose
   ```

4. **Check for relationships:**
   ```syndrql
   -- Verify no other bundles depend on this one
   -- Then drop safely
   DROP BUNDLE "standalone_logs";
   ```

5. **Use transactions for multiple drops:**
   ```syndrql
   -- Drop related bundles in a coordinated way
   DROP BUNDLE "orders" WITH FORCE;
   DROP BUNDLE "order_items" WITH FORCE;
   DROP BUNDLE "order_history" WITH FORCE;
   ```

### ❌ DON'T:

1. **Don't drop production bundles without backup:**
   ```syndrql
   DROP BUNDLE "customers" WITH FORCE;  -- ❌ NO BACKUP!
   ```

2. **Don't use generic names:**
   ```syndrql
   DROP BUNDLE "data";  -- ❌ Too vague
   ```

3. **Don't drop without verifying bundle name:**
   ```syndrql
   DROP BUNDLE "user";  -- ❌ Did you mean "users"?
   ```

4. **Don't chain drops without checking dependencies:**
   ```syndrql
   DROP BUNDLE "products";     -- ❌ May break orders
   DROP BUNDLE "categories";   -- ❌ May break products
   ```

---

## Error Handling

### Common Errors

#### 1. **Bundle Not Found**
```
Error: bundle 'xyz' not found
```
**Solution:** Verify bundle name (case-sensitive) and check it exists.

---

#### 2. **Bundle Not Empty (without FORCE)**
```
Error: cannot drop bundle 'users' because it contains documents or has relationships. Use FORCE to override
```
**Solution:** Either:
- Delete all documents first, then drop bundle
- Use `WITH FORCE` to bypass check (destructive)

```syndrql
-- Option 1: Delete documents first
DELETE DOCUMENTS FROM "users" WHERE 1 == 1;
DROP BUNDLE "users";

-- Option 2: Use FORCE (destructive)
DROP BUNDLE "users" WITH FORCE;
```

---

#### 3. **Missing Bundle Name**
```
Error: expected bundle name: unexpected end of input
```
**Solution:** Provide bundle name in quotes.
```syndrql
DROP BUNDLE "bundle_name";  -- ✅ Correct
```

---

#### 4. **Invalid Syntax**
```
Error: expected BUNDLE, got <token>
```
**Solution:** Check syntax matches pattern:
```syndrql
DROP BUNDLE "name";           -- ✅ Correct
DROP BUNDLE "name" WITH FORCE; -- ✅ Correct
DROP "name";                  -- ❌ Missing BUNDLE keyword
```

---

#### 5. **Permission Denied**
```
Error: insufficient permissions to drop bundle 'users'
```
**Solution:** Ensure you have DROP permissions on the bundle.

---

#### 6. **Referential Integrity Violation**
```
Error: cannot drop bundle 'products' - referenced by bundle 'orders'
```
**Solution:** Drop dependent bundles first or use FORCE to override.

---

## Quick Reference

| Feature | Syntax | Required |
|---------|--------|----------|
| **Basic Drop** | `DROP BUNDLE "name";` | ✅ Yes |
| **Bundle Name** | Quoted string | ✅ Yes |
| **Force Flag** | `WITH FORCE` | ❌ Optional |
| **Semicolon** | `;` | ❌ Optional |

### Operator Support

| Operation | Keyword | Example |
|-----------|---------|---------|
| Drop empty bundle | `DROP BUNDLE` | `DROP BUNDLE "logs";` |
| Drop with documents | `WITH FORCE` | `DROP BUNDLE "users" WITH FORCE;` |
| Drop with relationships | `WITH FORCE` | `DROP BUNDLE "products" WITH FORCE;` |

### Safety Levels

| Method | Safety | Use Case |
|--------|--------|----------|
| `DROP BUNDLE` | 🟢 Safe | Empty bundles only |
| `DROP BUNDLE WITH FORCE` | 🔴 Destructive | Development/migrations |

---

## Related Documentation

- [CREATE BUNDLE](create_bundle.md) - Create new bundle schemas
- [UPDATE BUNDLE](update_bundle.md) - Modify bundle schemas
- [DELETE DOCUMENTS](delete_documents.md) - Delete documents from bundles
- [ADD DOCUMENTS](add_documents.md) - Insert documents into bundles

---

**Version:** SyndrDB 1.0  
**Last Updated:** November 2024  
**Status:** ✅ Implemented and tested
