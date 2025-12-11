# 🔄 UPDATE BUNDLE in SyndrDB

Complete guide to updating bundle schemas and adding relationships in SyndrDB.

---

## 📑 Table of Contents

- [**Basic Syntax**](#-basic-syntax)
- [**UPDATE BUNDLE Operations**](#-update-bundle-operations)
- [**SET NAME - Rename Bundle**](#-set-name---rename-bundle)
- [**SET Fields - Modify Schema**](#-set-fields---modify-schema)
  - [ADD Fields](#add-fields)
  - [MODIFY Fields](#modify-fields)
  - [REMOVE Fields](#remove-fields)
- [**ADD RELATIONSHIP**](#-add-relationship)
  - [Relationship Types](#relationship-types)
  - [Field Mappings](#field-mappings)
  - [Default Values](#default-values)
- [**Complete Examples**](#-complete-examples)
- [**Best Practices**](#-best-practices)
- [**Error Handling**](#-error-handling)

---

## 🎯 Basic Syntax

The `UPDATE BUNDLE` statement modifies existing bundles by renaming them, changing their schema, or adding relationships.

**Standard Syntax:**
```sql
UPDATE BUNDLE "<BUNDLE_NAME>" <OPERATION>;
```

**Available Operations:**
1. **SET NAME** - Rename the bundle
2. **SET (...)** - Add, modify, or remove fields
3. **ADD RELATIONSHIP** - Create relationships between bundles

---

## 🔧 UPDATE BUNDLE Operations

`UPDATE BUNDLE` supports three main operations that can be used independently:

### Operation Types

| Operation | Purpose | Example |
|-----------|---------|---------|
| `SET NAME` | Rename bundle | `UPDATE BUNDLE "users" SET NAME = "customers";` |
| `SET (...)` | Modify fields | `UPDATE BUNDLE "users" SET ({ADD "age" = "int", TRUE, FALSE, 0});` |
| `ADD RELATIONSHIP` | Link bundles | `UPDATE BUNDLE "users" ADD RELATIONSHIP (...);` |

> 💡 **Note:** Each operation is independent and can be executed separately.

---

## 📝 SET NAME - Rename Bundle

Rename an existing bundle.

**Syntax:**
```sql
UPDATE BUNDLE "<OLD_NAME>" SET NAME = "<NEW_NAME>";
```

**Example:**

```sql
UPDATE BUNDLE "users" SET NAME = "customers";
```

**Response:**
```
Bundle renamed from 'users' to 'customers'.
```

**Use Cases:**
- Rebranding/refactoring database schema
- Correcting naming mistakes
- Aligning with new naming conventions

**Important Notes:**
- ⚠️ All existing documents remain intact
- 🔗 Relationships referencing the bundle need manual updates
- 📊 Indexes are preserved with the bundle

---

## 🏗️ SET Fields - Modify Schema

Add, modify, or remove fields in a bundle's schema.

**Syntax:**
```sql
UPDATE BUNDLE "<BUNDLE_NAME>"
SET (
    {<MODIFICATION_TYPE> "<FIELD_NAME>" = "<NEW_FIELD_NAME>", "<TYPE>", <REQUIRED>, <UNIQUE>, <DEFAULT>},
    ...
);
```

**Modification Types:**
- `ADD` - Add a new field
- `MODIFY` - Change an existing field
- `REMOVE` - Remove an existing field

---

### ADD Fields

Add new fields to an existing bundle.

**Syntax:**
```sql
UPDATE BUNDLE "<BUNDLE_NAME>"
SET (
    {ADD "<FIELD_NAME>" = "<FIELD_NAME>", "<TYPE>", <REQUIRED>, <UNIQUE>, <DEFAULT>}
);
```

> 📝 **Note:** For ADD operations, the field name appears twice (current name = new name) for consistency.

**Single Field Example:**

```sql
UPDATE BUNDLE "users"
SET (
    {ADD "age" = "age", "int", FALSE, FALSE, 0}
);
```

**Multiple Fields Example:**

```sql
UPDATE BUNDLE "users"
SET (
    {ADD "age" = "age", "int", FALSE, FALSE, 0},
    {ADD "city" = "city", "string", FALSE, FALSE, ""},
    {ADD "verified" = "verified", "bool", FALSE, FALSE, FALSE}
);
```

**Response:**
```
Bundle 'users' updated successfully. Added 3 field(s).
```

**Use Cases:**
- Adding new fields to existing bundles
- Extending schema without recreating the bundle
- Adding optional tracking fields

---

### MODIFY Fields

Change properties of existing fields.

**Syntax:**
```sql
UPDATE BUNDLE "<BUNDLE_NAME>"
SET (
    {MODIFY "<OLD_FIELD_NAME>" = "<NEW_FIELD_NAME>", "<TYPE>", <REQUIRED>, <UNIQUE>, <DEFAULT>}
);
```

**Rename Field Example:**

```sql
UPDATE BUNDLE "users"
SET (
    {MODIFY "name" = "full_name", "string", TRUE, FALSE, ""}
);
```

**Change Field Properties:**

```sql
UPDATE BUNDLE "users"
SET (
    {MODIFY "email" = "email", "string", TRUE, TRUE, ""}
);
```
*Makes email field both required and unique.*

**Change Data Type:**

```sql
UPDATE BUNDLE "products"
SET (
    {MODIFY "price" = "price", "float", TRUE, FALSE, 0.0}
);
```
*Converts price from int to float.*

**Multiple Modifications:**

```sql
UPDATE BUNDLE "users"
SET (
    {MODIFY "name" = "full_name", "string", TRUE, FALSE, ""},
    {MODIFY "active" = "is_active", "bool", TRUE, FALSE, TRUE}
);
```

**Response:**
```
Bundle 'users' updated successfully. Modified 2 field(s).
```

**Use Cases:**
- Renaming fields for clarity
- Changing field constraints (required, unique)
- Converting field types
- Updating default values

---

### REMOVE Fields

Remove fields from a bundle schema.

**Syntax:**
```sql
UPDATE BUNDLE "<BUNDLE_NAME>"
SET (
    {REMOVE "<FIELD_NAME>" = "", "", FALSE, FALSE, NULL}
);
```

> 📝 **Note:** For REMOVE operations, only the field name is significant. Other values are placeholders.

**Single Field Example:**

```sql
UPDATE BUNDLE "users"
SET (
    {REMOVE "middle_name" = "", "", FALSE, FALSE, NULL}
);
```

**Multiple Fields Example:**

```sql
UPDATE BUNDLE "users"
SET (
    {REMOVE "temp_field" = "", "", FALSE, FALSE, NULL},
    {REMOVE "old_status" = "", "", FALSE, FALSE, NULL}
);
```

**Response:**
```
Bundle 'users' updated successfully. Removed 2 field(s).
```

**Use Cases:**
- Cleaning up deprecated fields
- Removing unused schema elements
- Schema optimization

> ⚠️ **Warning:** Removing fields will delete the data in those fields from all documents!

---

## 🔗 ADD RELATIONSHIP

Create relationships between bundles to enable JOIN queries and foreign key constraints.

**Syntax:**
```sql
UPDATE BUNDLE "<SOURCE_BUNDLE>"
ADD RELATIONSHIP (
    "<RELATIONSHIP_TYPE>",
    "<SOURCE_BUNDLE>",
    "<SOURCE_FIELD>",
    "<DESTINATION_BUNDLE>",
    "<DESTINATION_FIELD>"
);
```

**Parameters:**

| Parameter | Description | Example |
|-----------|-------------|---------|
| Source Bundle | Bundle being updated | `"users"` |
| Relationship Type | Type of relationship | `"1toMany"` |
| Source Bundle (param) | Source bundle name (repeated) | `"users"` |
| Source Field | Field in source bundle | `"DocumentID"` or `"id"` |
| Destination Bundle | Related bundle name | `"orders"` |
| Destination Field | Field in destination bundle | `"user_id"` |

---

### Relationship Types

SyndrDB supports three relationship types:

#### 1. **1toMany (One-to-Many)**

One record in the source bundle relates to many records in the destination bundle.

**Syntax:**
```sql
"1toMany"
```

**Example:**

```sql
UPDATE BUNDLE "users"
ADD RELATIONSHIP (
    "1toMany",
    "users",
    "DocumentID",
    "orders",
    "user_id"
);
```

**Use Cases:**
- One customer → many orders
- One author → many books
- One category → many products

**Diagram:**
```
Users (1) ←→ (Many) Orders
  ├─ user_1 → order_1, order_2, order_3
  ├─ user_2 → order_4, order_5
  └─ user_3 → order_6
```

#### 2. **0toMany (Zero-to-Many)**

Zero or more records in the source bundle relate to many records in the destination bundle.

**Syntax:**
```sql
"0toMany"
```

**Example:**

```sql
UPDATE BUNDLE "categories"
ADD RELATIONSHIP (
    "0toMany",
    "categories",
    "DocumentID",
    "products",
    "category_id"
);
```

**Use Cases:**
- Optional relationships
- Categories that might not have products
- Tags that might not have posts

**Diagram:**
```
Categories (0..N) ←→ (Many) Products
  ├─ category_1 → product_1, product_2
  ├─ category_2 → (no products)
  └─ category_3 → product_3, product_4, product_5
```

#### 3. **ManyToMany**

Many records in the source bundle relate to many records in the destination bundle.

**Syntax:**
```sql
"ManyToMany"
```

**Example:**

```sql
UPDATE BUNDLE "students"
ADD RELATIONSHIP (
    "ManyToMany",
    "students",
    "DocumentID",
    "courses",
    "CourseID"
);
```

**Use Cases:**
- Students ↔ Courses
- Products ↔ Tags
- Users ↔ Roles

**Diagram:**
```
Students (Many) ←→ (Many) Courses
  ├─ student_1 → course_A, course_B
  ├─ student_2 → course_B, course_C
  └─ student_3 → course_A, course_C

  course_A ← student_1, student_3
  course_B ← student_1, student_2
  course_C ← student_2, student_3
```

---

### Field Mappings

Relationship fields connect documents between bundles.

**Standard Mapping:**

```sql
UPDATE BUNDLE "authors"
ADD RELATIONSHIP (
    "1toMany",
    "authors",
    "DocumentID",      -- Primary key in authors
    "books",
    "author_id"        -- Foreign key in books
);
```

**How it works:**
- `DocumentID` in `authors` matches `author_id` in `books`
- Each author can have multiple books
- Each book has one author

**Example Documents:**

**Authors bundle:**
```json
{"DocumentID": "author-123", "name": "Jane Austen"}
```

**Books bundle:**
```json
{"DocumentID": "book-456", "title": "Pride and Prejudice", "author_id": "author-123"}
```

---

### Default Values

If source or destination fields are empty strings, SyndrDB uses intelligent defaults.

**Empty Field Defaults:**

```sql
UPDATE BUNDLE "authors"
ADD RELATIONSHIP (
    "1toMany",
    "authors",
    "",        -- Defaults to "DocumentID"
    "books",
    ""         -- Defaults to "authorsID" (source bundle name + "ID")
);
```

**Equivalent to:**

```sql
UPDATE BUNDLE "authors"
ADD RELATIONSHIP (
    "1toMany",
    "authors",
    "DocumentID",
    "books",
    "authorsID"
);
```

**Default Field Rules:**
- **Source Field:** Defaults to `"DocumentID"`
- **Destination Field:** Defaults to `"<SourceBundleName>ID"`

---

## 🌟 Complete Examples

### Example 1: Add Multiple Fields to User Bundle

```sql
UPDATE BUNDLE "users"
SET (
    {ADD "phone" = "phone", "string", FALSE, FALSE, NULL},
    {ADD "address" = "address", "string", FALSE, FALSE, ""},
    {ADD "city" = "city", "string", FALSE, FALSE, ""},
    {ADD "country" = "country", "string", FALSE, FALSE, "USA"},
    {ADD "postal_code" = "postal_code", "string", FALSE, FALSE, ""},
    {ADD "verified_email" = "verified_email", "bool", FALSE, FALSE, FALSE}
);
```

**Use Case:** Extending user profile with contact and location information.

---

### Example 2: Rename Bundle and Fields

```sql
-- Step 1: Rename fields
UPDATE BUNDLE "users"
SET (
    {MODIFY "name" = "full_name", "string", TRUE, FALSE, ""},
    {MODIFY "active" = "is_active", "bool", TRUE, FALSE, TRUE},
    {MODIFY "created" = "created_at", "datetime", TRUE, FALSE, "2025-01-01T00:00:00Z"}
);

-- Step 2: Rename bundle
UPDATE BUNDLE "users" SET NAME = "customers";
```

**Use Case:** Schema refactoring for better naming conventions.

---

### Example 3: Complete E-commerce Relationship Setup

```sql
-- Create relationship: Customers → Orders
UPDATE BUNDLE "customers"
ADD RELATIONSHIP (
    "1toMany",
    "customers",
    "DocumentID",
    "orders",
    "customer_id"
);

-- Create relationship: Orders → OrderItems
UPDATE BUNDLE "orders"
ADD RELATIONSHIP (
    "1toMany",
    "orders",
    "DocumentID",
    "order_items",
    "order_id"
);

-- Create relationship: Products → OrderItems
UPDATE BUNDLE "products"
ADD RELATIONSHIP (
    "1toMany",
    "products",
    "DocumentID",
    "order_items",
    "product_id"
);
```

**Use Case:** Setting up complete e-commerce relational structure.

**Enables queries like:**
```sql
SELECT * FROM "customers"
JOIN "orders" ON customers.DocumentID = orders.customer_id
JOIN "order_items" ON orders.DocumentID = order_items.order_id
JOIN "products" ON order_items.product_id = products.DocumentID;
```

---

### Example 4: Blog System with Relationships

```sql
-- Authors → Posts (1 to Many)
UPDATE BUNDLE "authors"
ADD RELATIONSHIP (
    "1toMany",
    "authors",
    "DocumentID",
    "posts",
    "author_id"
);

-- Posts → Comments (1 to Many)
UPDATE BUNDLE "posts"
ADD RELATIONSHIP (
    "1toMany",
    "posts",
    "DocumentID",
    "comments",
    "post_id"
);

-- Categories → Posts (0 to Many - posts without category allowed)
UPDATE BUNDLE "categories"
ADD RELATIONSHIP (
    "0toMany",
    "categories",
    "DocumentID",
    "posts",
    "category_id"
);

-- Posts ↔ Tags (Many to Many)
UPDATE BUNDLE "posts"
ADD RELATIONSHIP (
    "ManyToMany",
    "posts",
    "DocumentID",
    "tags",
    "tag_id"
);
```

**Use Case:** Content management system with complex relationships.

---

### Example 5: Using Default Field Names

```sql
-- Short form with defaults
UPDATE BUNDLE "authors"
ADD RELATIONSHIP (
    "1toMany",
    "authors",
    "",          -- Defaults to "DocumentID"
    "books",
    ""           -- Defaults to "authorsID"
);

-- Equivalent long form
UPDATE BUNDLE "authors"
ADD RELATIONSHIP (
    "1toMany",
    "authors",
    "DocumentID",
    "books",
    "authorsID"
);
```

**Use Case:** Quick relationship setup with conventional field names.

---

### Example 6: Combined Operations

```sql
-- Add new field and create relationship
UPDATE BUNDLE "users"
SET (
    {ADD "role_id" = "role_id", "int", TRUE, FALSE, 1}
);

UPDATE BUNDLE "users"
ADD RELATIONSHIP (
    "1toMany",
    "roles",
    "DocumentID",
    "users",
    "role_id"
);
```

**Use Case:** Adding role-based access control to existing user system.

---

### Example 7: Schema Migration

```sql
-- Remove deprecated fields
UPDATE BUNDLE "products"
SET (
    {REMOVE "old_price" = "", "", FALSE, FALSE, NULL},
    {REMOVE "legacy_sku" = "", "", FALSE, FALSE, NULL}
);

-- Add new fields
UPDATE BUNDLE "products"
SET (
    {ADD "current_price" = "current_price", "float", TRUE, FALSE, 0.0},
    {ADD "sale_price" = "sale_price", "float", FALSE, FALSE, NULL},
    {ADD "discount_percent" = "discount_percent", "float", FALSE, FALSE, 0.0}
);
```

**Use Case:** Migrating from old schema to new pricing structure.

---

## ✅ Best Practices

### 1. **Always Quote Bundle Names**

```sql
✅ CORRECT:   UPDATE BUNDLE "users" SET NAME = "customers";
❌ INCORRECT: UPDATE BUNDLE users SET NAME = customers;
```

### 2. **Use Descriptive Field Names**

```sql
✅ GOOD:   {ADD "email_verified" = "email_verified", "bool", FALSE, FALSE, FALSE}
❌ AVOID:  {ADD "flag1" = "flag1", "bool", FALSE, FALSE, FALSE}
```

### 3. **Plan Relationship Types Carefully**

Choose the correct relationship type for your data model:

```sql
✅ CORRECT: 1 customer → many orders (1toMany)
❌ WRONG:   1 order → many customers (doesn't make business sense)
```

### 4. **Use Consistent Field Naming for Relationships**

```sql
✅ GOOD PATTERN:
- Source: "DocumentID"
- Destination: "<bundle_name>_id" or "<bundle_name>ID"

Examples:
- author_id, customer_id, product_id
- authorID, customerID, productID
```

### 5. **Test Relationships with Queries**

After adding relationships, verify with a JOIN query:

```sql
UPDATE BUNDLE "authors" ADD RELATIONSHIP ("authorsbooks" { "1toMany", "authors", "DocumentID", "books", "author_id"});

-- Test it:
SELECT * FROM "authors" JOIN "books" ON authors.DocumentID = books.author_id;
```

### 6. **Document Field Modifications**

Keep track of schema changes:

```sql
-- Good: Add comment explaining why
UPDATE BUNDLE "users"
SET (
    {ADD "gdpr_consent" = "gdpr_consent", "bool", TRUE, FALSE, FALSE}
);
-- Added for GDPR compliance, 2025-01-15
```

### 7. **Avoid Breaking Changes**

When modifying fields:

```sql
✅ SAFE:   {MODIFY "email" = "email", "string", TRUE, TRUE, ""}
           -- Same type, adding unique constraint

❌ RISKY:  {MODIFY "age" = "age", "string", TRUE, FALSE, ""}
           -- Changing int to string might break existing data
```

### 8. **Use Transactions for Multiple Changes**

When making multiple related changes, group them logically:

```sql
-- All related changes together
UPDATE BUNDLE "users"
SET (
    {ADD "role_id" = "role_id", "int", TRUE, FALSE, 1},
    {REMOVE "old_role" = "", "", FALSE, FALSE, NULL}
);

UPDATE BUNDLE "users"
ADD RELATIONSHIP ("1toMany", "roles", "DocumentID", "users", "role_id");
```

---

## ⚠️ Error Handling

### Common Errors and Solutions

#### 1. **Bundle Not Found**

```sql
❌ ERROR:
UPDATE BUNDLE "nonexistent" SET NAME = "new_name";
```

**Error Message:**
```
bundle 'nonexistent' not found
```

**Solution:**
Verify the bundle exists:
```sql
SHOW BUNDLES;
```

---

#### 2. **Invalid Relationship Type**

```sql
❌ ERROR:
UPDATE BUNDLE "users" ADD RELATIONSHIP ("1to1", "users", "id", "orders", "user_id");
```

**Error Message:**
```
invalid relationship type '1to1'. Valid types are: 0toMany, 1toMany, ManyToMany
```

**Solution:**
```sql
✅ CORRECT:
UPDATE BUNDLE "users" ADD RELATIONSHIP ("1toMany", "users", "DocumentID", "orders", "user_id");
```

**Valid Types:**
- `"1toMany"`
- `"0toMany"`
- `"ManyToMany"`

---

#### 3. **Missing Operation**

```sql
❌ ERROR:
UPDATE BUNDLE "users";
```

**Error Message:**
```
UPDATE BUNDLE must specify at least one operation: SET NAME, SET fields, or ADD RELATIONSHIP
```

**Solution:**
```sql
✅ CORRECT:
UPDATE BUNDLE "users" SET NAME = "customers";
```

---

#### 4. **Invalid Modification Type**

```sql
❌ ERROR:
UPDATE BUNDLE "users"
SET (
    {UPDATE "name" = "full_name", "string", TRUE, FALSE, ""}
);
```

**Error Message:**
```
invalid modification type 'UPDATE'. Valid types are: ADD, MODIFY, REMOVE
```

**Solution:**
```sql
✅ CORRECT:
UPDATE BUNDLE "users"
SET (
    {MODIFY "name" = "full_name", "string", TRUE, FALSE, ""}
);
```

**Valid Modification Types:**
- `ADD`
- `MODIFY`
- `REMOVE`

---

#### 5. **Field Already Exists (ADD)**

```sql
❌ ERROR:
UPDATE BUNDLE "users"
SET (
    {ADD "email" = "email", "string", TRUE, FALSE, ""}
);
-- But 'email' field already exists
```

**Error Message:**
```
field 'email' already exists in bundle 'users'
```

**Solution:**
Use `MODIFY` instead:
```sql
✅ CORRECT:
UPDATE BUNDLE "users"
SET (
    {MODIFY "email" = "email", "string", TRUE, TRUE, ""}
);
```

---

#### 6. **Field Not Found (MODIFY/REMOVE)**

```sql
❌ ERROR:
UPDATE BUNDLE "users"
SET (
    {MODIFY "nonexistent" = "new_name", "string", TRUE, FALSE, ""}
);
```

**Error Message:**
```
field 'nonexistent' not found in bundle 'users'
```

**Solution:**
Use `ADD` for new fields:
```sql
✅ CORRECT:
UPDATE BUNDLE "users"
SET (
    {ADD "new_field" = "new_field", "string", TRUE, FALSE, ""}
);
```

---

#### 7. **Missing Required Properties**

```sql
❌ ERROR:
UPDATE BUNDLE "users"
SET (
    {ADD "age" = "age", "int"}
);
```

**Error Message:**
```
field definition must have all properties: modification type, name, new name, type, required, unique, default
```

**Solution:**
```sql
✅ CORRECT (all properties):
UPDATE BUNDLE "users"
SET (
    {ADD "age" = "age", "int", FALSE, FALSE, 0}
);
```

---

#### 8. **Invalid Syntax in ADD RELATIONSHIP**

```sql
❌ ERROR:
UPDATE BUNDLE "users" ADD RELATIONSHIP "1toMany", "users", "id", "orders", "user_id";
```

**Error Message:**
```
expected '(' after RELATIONSHIP
```

**Solution:**
```sql
✅ CORRECT (wrapped in parentheses):
UPDATE BUNDLE "users" ADD RELATIONSHIP ("1toMany", "users", "DocumentID", "orders", "user_id");
```

---

## 🔍 Verification

After updating a bundle, verify the changes:

### Check Bundle Schema

```sql
SHOW BUNDLE "users";
```

**Expected Output:**
```
Bundle: users
Fields:
  - id (int, required, unique)
  - full_name (string, required)        -- renamed from 'name'
  - email (string, required, unique)
  - age (int, optional)                 -- newly added
```

### Check Relationships

```sql
-- Query using the relationship
SELECT * FROM "authors"
JOIN "books" ON authors.DocumentID = books.author_id;
```

---

## 🎓 Quick Reference

| Operation | Syntax | Example |
|-----------|--------|---------|
| **Rename Bundle** | `UPDATE BUNDLE "old" SET NAME = "new"` | `UPDATE BUNDLE "users" SET NAME = "customers"` |
| **ADD Field** | `SET ({ADD "field" = "field", "type", req, uniq, default})` | `{ADD "age" = "age", "int", FALSE, FALSE, 0}` |
| **MODIFY Field** | `SET ({MODIFY "old" = "new", "type", req, uniq, default})` | `{MODIFY "name" = "full_name", "string", TRUE, FALSE, ""}` |
| **REMOVE Field** | `SET ({REMOVE "field" = "", "", FALSE, FALSE, NULL})` | `{REMOVE "old_field" = "", "", FALSE, FALSE, NULL}` |
| **1toMany** | `ADD RELATIONSHIP ("1toMany", src, src_fld, dest, dest_fld)` | `("1toMany", "users", "DocumentID", "orders", "user_id")` |
| **0toMany** | `ADD RELATIONSHIP ("0toMany", src, src_fld, dest, dest_fld)` | `("0toMany", "categories", "DocumentID", "products", "category_id")` |
| **ManyToMany** | `ADD RELATIONSHIP ("ManyToMany", src, src_fld, dest, dest_fld)` | `("ManyToMany", "students", "DocumentID", "courses", "course_id")` |

---

## 🆘 Getting Help

For more information:
- 📖 [SyndrDB Documentation](../README.md)
- 🏗️ [CREATE Bundle](./create_bundle.md)
- 📝 [ADD Documents](./add_documents.md)
- 🔍 [SELECT Queries](./select_queries.md)
- 🗑️ [DELETE Bundle](./delete_bundle.md)

---

*Last updated: November 20, 2025* 🚀
