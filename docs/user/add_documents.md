# 📝 ADD DOCUMENT in SyndrDB

Complete guide to adding documents to bundles in SyndrDB.

---

## 📑 Table of Contents

- [**Basic Syntax**](#-basic-syntax)
- [**ADD DOCUMENT TO BUNDLE**](#-add-document-to-bundle)
  - [Single Field](#single-field)
  - [Multiple Fields](#multiple-fields)
- [**WITH Clause**](#-with-clause)
- [**Field Specifications**](#-field-specifications)
  - [Field-Value Pairs](#field-value-pairs)
  - [Field Names](#field-names)
- [**Data Types**](#-data-types)
  - [String Values](#string-values)
  - [Numeric Values](#numeric-values)
  - [Boolean Values](#boolean-values)
  - [NULL Values](#null-values)
- [**Complete Examples**](#-complete-examples)
- [**Best Practices**](#-best-practices)
- [**Error Handling**](#-error-handling)

---

## 🎯 Basic Syntax

The `ADD DOCUMENT` statement inserts a new document into a bundle (collection).

**Standard Syntax:**
```sql
ADD DOCUMENT TO BUNDLE "<BUNDLE_NAME>" WITH ({<FIELD_NAME> = <VALUE>}, ...);
```

**Components:**
- `ADD DOCUMENT` - Command keywords
- `TO BUNDLE` - Specifies the target bundle
- `"<BUNDLE_NAME>"` - Name of the bundle (quoted string)
- `WITH` - Introduces the field specifications
- `({<FIELD_NAME> = <VALUE>}, ...)` - Field-value pairs in braces

---

## 📦 ADD DOCUMENT TO BUNDLE

### Single Field

Add a document with one field:

```sql
ADD DOCUMENT TO BUNDLE "users" WITH ({"name" = "John Doe"});
```

**Response:**
```json
{
  "DocumentID": "uuid-generated-123",
  "message": "Document added successfully to bundle 'users'."
}
```

### Multiple Fields

Add a document with multiple fields using comma-separated field sets:

```sql
ADD DOCUMENT TO BUNDLE "users" WITH (
    {"name" = "Jane Smith"},
    {"email" = "jane@example.com"},
    {"age" = 30},
    {"active" = true}
);
```

**Response:**
```json
{
  "DocumentID": "uuid-generated-456",
  "message": "Document added successfully to bundle 'users'."
}
```

> 💡 **Tip:** Each field is specified in its own brace set `{field = value}` and separated by commas.

---

## 🔧 WITH Clause

The `WITH` clause specifies the document's field values.

**Syntax:**
```sql
WITH ({<field> = <value>}, {<field> = <value>}, ...)
```

**Requirements:**
- Must follow the bundle name
- Wrapped in parentheses `( )`
- Contains one or more field-value pairs
- Each field-value pair wrapped in braces `{ }`
- Pairs separated by commas

**Examples:**

**Single field:**
```sql
WITH ({"title" = "Sample Document"})
```

**Multiple fields:**
```sql
WITH (
    {"title" = "Sample Document"},
    {"author" = "John Doe"},
    {"published" = true}
)
```

---

## 🏷️ Field Specifications

### Field-Value Pairs

Each field is specified using the format: `{<FIELD_NAME> = <VALUE>}`

**Format Rules:**
- Opening brace `{`
- Field name (string or identifier)
- Equals sign `=`
- Value (string, number, boolean, or NULL)
- Closing brace `}`

**Valid Examples:**

```sql
{"name" = "Alice"}           -- String field name, string value
{"age" = 25}                 -- String field name, integer value
{"price" = 99.99}            -- String field name, float value
{"active" = true}            -- String field name, boolean value
{"middle_name" = NULL}       -- String field name, NULL value
```

### Field Names

Field names can be:
- **Quoted strings** (recommended): `"field_name"`
- **Identifiers** (no quotes): `field_name`

**Examples:**

```sql
-- Quoted field names (recommended)
ADD DOCUMENT TO BUNDLE "products" WITH (
    {"product_name" = "Widget"},
    {"product_id" = 12345}
);

-- Identifier field names (also valid)
ADD DOCUMENT TO BUNDLE "products" WITH (
    {product_name = "Widget"},
    {product_id = 12345}
);
```

> ⚠️ **Best Practice:** Use quoted field names for consistency and to avoid conflicts with reserved keywords.

---

## 📊 Data Types

SyndrDB supports multiple data types for field values.

### String Values

String values must be enclosed in double quotes.

**Syntax:**
```sql
{"field_name" = "string value"}
```

**Examples:**

```sql
-- Simple string
ADD DOCUMENT TO BUNDLE "authors" WITH ({"name" = "Mark Twain"});

-- String with spaces
ADD DOCUMENT TO BUNDLE "books" WITH ({"title" = "The Adventures of Tom Sawyer"});

-- Empty string
ADD DOCUMENT TO BUNDLE "notes" WITH ({"content" = ""});

-- String with special characters
ADD DOCUMENT TO BUNDLE "messages" WITH ({"text" = "Hello, World! 🌍"});
```

**Common String Examples:**

```sql
-- Names
{"first_name" = "Alice"}
{"last_name" = "Johnson"}
{"full_name" = "Alice Marie Johnson"}

-- Email addresses
{"email" = "alice@example.com"}

-- Descriptions
{"description" = "High-quality product with premium features"}

-- Categories
{"category" = "Electronics"}
{"status" = "active"}
```

### Numeric Values

SyndrDB supports both integers and floating-point numbers.

**Integer Values:**

```sql
-- Positive integers
{"age" = 30}
{"quantity" = 100}
{"year" = 2024}

-- Negative integers
{"temperature" = -5}
{"balance" = -150}

-- Zero
{"count" = 0}
```

**Floating-Point Values:**

```sql
-- Decimal numbers
{"price" = 99.99}
{"rating" = 4.5}
{"percentage" = 0.75}

-- Scientific notation (if supported)
{"distance" = 1.5e6}
```

**Complete Example:**

```sql
ADD DOCUMENT TO BUNDLE "products" WITH (
    {"product_id" = 1001},
    {"name" = "Premium Widget"},
    {"price" = 149.99},
    {"stock" = 42},
    {"weight" = 2.5},
    {"discount" = 0.15}
);
```

> 📝 **Note:** The parser automatically detects the numeric type. Integers are stored as `int64` and decimals as `float64`.

### Boolean Values

Boolean fields accept `true` or `false` values (case-sensitive).

**Syntax:**
```sql
{"field_name" = true}
{"field_name" = false}
```

**Examples:**

```sql
-- Active/Inactive flags
ADD DOCUMENT TO BUNDLE "users" WITH (
    {"name" = "Bob Smith"},
    {"active" = true},
    {"verified" = false}
);

-- Feature toggles
ADD DOCUMENT TO BUNDLE "settings" WITH (
    {"dark_mode" = true},
    {"notifications_enabled" = true},
    {"auto_save" = false}
);

-- Status indicators
ADD DOCUMENT TO BUNDLE "orders" WITH (
    {"order_id" = 5001},
    {"paid" = true},
    {"shipped" = false},
    {"delivered" = false}
);
```

> ⚠️ **Important:** Use lowercase `true` and `false`. Uppercase or mixed case will not be recognized as boolean values.

### NULL Values

Use `NULL` to indicate the absence of a value.

**Syntax:**
```sql
{"field_name" = NULL}
```

**Examples:**

```sql
-- Optional fields
ADD DOCUMENT TO BUNDLE "users" WITH (
    {"name" = "Alice"},
    {"middle_name" = NULL},
    {"email" = "alice@example.com"}
);

-- Unset values
ADD DOCUMENT TO BUNDLE "products" WITH (
    {"product_name" = "Widget"},
    {"discount_price" = NULL},
    {"discontinuation_date" = NULL}
);

-- Missing data
ADD DOCUMENT TO BUNDLE "employees" WITH (
    {"employee_id" = 12345},
    {"name" = "John Doe"},
    {"phone" = NULL},
    {"department" = "Sales"}
);
```

> 💡 **Use Case:** NULL is useful for optional fields or when data is not yet available.

---

## 🌟 Complete Examples

### Example 1: User Registration

```sql
ADD DOCUMENT TO BUNDLE "users" WITH (
    {"username" = "alice_smith"},
    {"email" = "alice.smith@example.com"},
    {"age" = 28},
    {"verified" = false},
    {"phone" = NULL},
    {"created_at" = "2024-11-20T10:30:00Z"}
);
```

**Use Case:** Creating a new user account with profile information.

### Example 2: Product Catalog Entry

```sql
ADD DOCUMENT TO BUNDLE "products" WITH (
    {"sku" = "WIDGET-001"},
    {"name" = "Premium Widget Pro"},
    {"description" = "Professional-grade widget with advanced features"},
    {"price" = 299.99},
    {"category" = "Electronics"},
    {"in_stock" = true},
    {"quantity" = 150},
    {"rating" = 4.7},
    {"featured" = true}
);
```

**Use Case:** Adding a product to an e-commerce catalog.

### Example 3: Order Processing

```sql
ADD DOCUMENT TO BUNDLE "orders" WITH (
    {"order_id" = 10001},
    {"customer_email" = "customer@example.com"},
    {"total_amount" = 459.97},
    {"status" = "pending"},
    {"paid" = false},
    {"shipped" = false},
    {"tracking_number" = NULL},
    {"order_date" = "2024-11-20"}
);
```

**Use Case:** Recording a new customer order.

### Example 4: Blog Post

```sql
ADD DOCUMENT TO BUNDLE "posts" WITH (
    {"title" = "Introduction to SyndrDB"},
    {"author" = "Jane Developer"},
    {"content" = "SyndrDB is a powerful document database..."},
    {"published" = true},
    {"views" = 0},
    {"likes" = 0},
    {"created_at" = "2024-11-20T14:22:00Z"},
    {"updated_at" = NULL}
);
```

**Use Case:** Publishing a blog post or article.

### Example 5: Inventory Item

```sql
ADD DOCUMENT TO BUNDLE "inventory" WITH (
    {"item_code" = "INV-2024-1156"},
    {"warehouse" = "Warehouse A"},
    {"location" = "Aisle 12, Shelf B"},
    {"quantity" = 500},
    {"unit_price" = 12.50},
    {"total_value" = 6250.00},
    {"reorder_level" = 100},
    {"needs_reorder" = false}
);
```

**Use Case:** Tracking inventory items in a warehouse.

### Example 6: Minimal Document

```sql
ADD DOCUMENT TO BUNDLE "tags" WITH ({"name" = "important"});
```

**Use Case:** Creating a simple document with just one field.

### Example 7: Settings Configuration

```sql
ADD DOCUMENT TO BUNDLE "app_settings" WITH (
    {"setting_name" = "max_upload_size"},
    {"value" = "10485760"},
    {"unit" = "bytes"},
    {"enabled" = true},
    {"description" = "Maximum file upload size in bytes"}
);
```

**Use Case:** Storing application configuration settings.

---

## ✅ Best Practices

### 1. **Always Quote Bundle Names**

```sql
✅ CORRECT:   ADD DOCUMENT TO BUNDLE "users" WITH ({"name" = "Alice"});
❌ INCORRECT: ADD DOCUMENT TO BUNDLE users WITH ({"name" = "Alice"});
```

### 2. **Use Consistent Field Naming**

```sql
✅ CORRECT:   {"first_name" = "John"}, {"last_name" = "Doe"}
❌ AVOID:     {"firstName" = "John"}, {"last_name" = "Doe"}
```

> 💡 Choose either `snake_case` or `camelCase` and stick with it throughout your schema.

### 3. **Quote String Values**

```sql
✅ CORRECT:   {"status" = "active"}
❌ INCORRECT: {"status" = active}
```

### 4. **Validate Required Fields**

Before inserting, ensure all required fields defined in the bundle schema are included.

```sql
-- If bundle requires: name, email, age
✅ CORRECT:
ADD DOCUMENT TO BUNDLE "users" WITH (
    {"name" = "Alice"},
    {"email" = "alice@example.com"},
    {"age" = 30}
);

❌ INCOMPLETE (missing required field):
ADD DOCUMENT TO BUNDLE "users" WITH (
    {"name" = "Alice"},
    {"email" = "alice@example.com"}
);
```

### 5. **Use NULL for Optional Fields**

```sql
-- Explicitly set optional fields to NULL if not available
ADD DOCUMENT TO BUNDLE "contacts" WITH (
    {"name" = "Bob"},
    {"email" = "bob@example.com"},
    {"phone" = NULL},        -- Optional, not available
    {"company" = NULL}       -- Optional, not available
);
```

### 6. **Semicolon is Optional**

Both formats are valid:

```sql
✅ WITH semicolon:    ADD DOCUMENT TO BUNDLE "users" WITH ({"name" = "Alice"});
✅ WITHOUT semicolon: ADD DOCUMENT TO BUNDLE "users" WITH ({"name" = "Alice"})
```

### 7. **Multi-line Formatting for Readability**

For documents with many fields, use multi-line formatting:

```sql
ADD DOCUMENT TO BUNDLE "employees" WITH (
    {"employee_id" = 12345},
    {"first_name" = "Alice"},
    {"last_name" = "Johnson"},
    {"department" = "Engineering"},
    {"position" = "Senior Developer"},
    {"salary" = 95000},
    {"hire_date" = "2024-01-15"},
    {"active" = true}
);
```

### 8. **Validate Data Types**

Ensure values match expected types:

```sql
✅ CORRECT:   {"age" = 30}          -- Integer
❌ INCORRECT: {"age" = "30"}        -- String (will be stored as string)

✅ CORRECT:   {"active" = true}     -- Boolean
❌ INCORRECT: {"active" = "true"}   -- String
```

---

## ⚠️ Error Handling

### Common Errors and Solutions

#### 1. **Missing Bundle Name**

```sql
❌ ERROR:
ADD DOCUMENT TO BUNDLE WITH ({"name" = "Alice"});
```

**Error Message:**
```
Invalid ADD DOCUMENT command syntax
```

**Solution:**
```sql
✅ CORRECT:
ADD DOCUMENT TO BUNDLE "users" WITH ({"name" = "Alice"});
```

#### 2. **Missing WITH Keyword**

```sql
❌ ERROR:
ADD DOCUMENT TO BUNDLE "users" ({"name" = "Alice"});
```

**Error Message:**
```
expected WITH, got (
```

**Solution:**
```sql
✅ CORRECT:
ADD DOCUMENT TO BUNDLE "users" WITH ({"name" = "Alice"});
```

#### 3. **Missing Field Braces**

```sql
❌ ERROR:
ADD DOCUMENT TO BUNDLE "users" WITH ("name" = "Alice");
```

**Error Message:**
```
expected {, got "name"
```

**Solution:**
```sql
✅ CORRECT:
ADD DOCUMENT TO BUNDLE "users" WITH ({"name" = "Alice"});
```

#### 4. **Empty Field Set**

```sql
❌ ERROR:
ADD DOCUMENT TO BUNDLE "users" WITH ();
```

**Error Message:**
```
expected {, got )
```

**Solution:**
```sql
✅ CORRECT:
ADD DOCUMENT TO BUNDLE "users" WITH ({"name" = "Alice"});
```

#### 5. **Bundle Does Not Exist**

```sql
❌ ERROR:
ADD DOCUMENT TO BUNDLE "nonexistent" WITH ({"name" = "Alice"});
```

**Error Message:**
```
error: bundle 'nonexistent' not found
```

**Solution:**
Create the bundle first:
```sql
CREATE BUNDLE "users" WITH FIELDS (
    {"name", STRING, true, false},
    {"email", STRING, true, true}
);

ADD DOCUMENT TO BUNDLE "users" WITH (
    {"name" = "Alice"},
    {"email" = "alice@example.com"}
);
```

#### 6. **Invalid Syntax - Missing Equals**

```sql
❌ ERROR:
ADD DOCUMENT TO BUNDLE "users" WITH ({"name": "Alice"});
```

**Error Message:**
```
expected '=' after field name, got :
```

**Solution:**
```sql
✅ CORRECT (use = not :):
ADD DOCUMENT TO BUNDLE "users" WITH ({"name" = "Alice"});
```

#### 7. **Schema Validation Errors**

If the bundle has field constraints:

```sql
❌ ERROR (required field missing):
ADD DOCUMENT TO BUNDLE "users" WITH ({"name" = "Alice"});
-- But bundle requires 'email' field
```

**Error Message:**
```
error: required field 'email' is missing
```

**Solution:**
```sql
✅ CORRECT:
ADD DOCUMENT TO BUNDLE "users" WITH (
    {"name" = "Alice"},
    {"email" = "alice@example.com"}
);
```

---

## 🚀 Transaction Support

Documents added with `ADD DOCUMENT` are logged in the Write-Ahead Log (WAL) for durability and crash recovery.

**Transaction Flow:**

1. **Parse** the ADD DOCUMENT command
2. **Validate** bundle exists and schema constraints
3. **Generate** unique document ID
4. **Write** to WAL (if available)
5. **Add** document to bundle
6. **Update** indexes (hash, BTree)
7. **Invalidate** query plan cache
8. **Return** success response

> 💾 **Durability:** All document insertions are persisted to disk via WAL before returning success.

---

## 🔍 Verification

After adding a document, verify it was inserted:

```sql
-- Add document
ADD DOCUMENT TO BUNDLE "users" WITH (
    {"name" = "Alice"},
    {"email" = "alice@example.com"}
);

-- Verify insertion
SELECT * FROM "users" WHERE "name" = "Alice";
```

**Expected Output:**
```json
[
  {
    "id": "uuid-generated-789",
    "name": "Alice",
    "email": "alice@example.com"
  }
]
```

---

## 🎓 Quick Reference

| Component | Syntax | Example |
|-----------|--------|---------|
| **Basic ADD** | `ADD DOCUMENT TO BUNDLE "name" WITH (...)` | `ADD DOCUMENT TO BUNDLE "users" WITH ({"name" = "Alice"})` |
| **String Value** | `{"field" = "value"}` | `{"name" = "John Doe"}` |
| **Integer Value** | `{"field" = 123}` | `{"age" = 30}` |
| **Float Value** | `{"field" = 12.34}` | `{"price" = 99.99}` |
| **Boolean Value** | `{"field" = true/false}` | `{"active" = true}` |
| **NULL Value** | `{"field" = NULL}` | `{"middle_name" = NULL}` |
| **Multiple Fields** | `{...}, {...}, {...}` | `{"name" = "Alice"}, {"age" = 30}` |
| **Semicolon** | Optional `;` at end | `WITH (...);` or `WITH (...)` |

---

## 🔮 Future Features

The following features are planned but not yet implemented:

### Array Values (Coming Soon)
```sql
-- NOT YET SUPPORTED
{"tags" = ["important", "urgent", "todo"]}
```

### Nested Objects (Coming Soon)
```sql
-- NOT YET SUPPORTED
{"address" = {"street" = "123 Main St", "city" = "Boston"}}
```

### Batch Inserts (Coming Soon)
```sql
-- NOT YET SUPPORTED
ADD DOCUMENTS TO BUNDLE "users" WITH [
    ({"name" = "Alice"}),
    ({"name" = "Bob"}),
    ({"name" = "Charlie"})
];
```

> 📌 **Note:** These features are mentioned in the codebase as TODO items and will be added in future releases.

---

## 🆘 Getting Help

For more information:
- 📖 [SyndrDB Documentation](../README.md)
- 🔍 [SELECT Queries](./select_queries.md)
- 🔄 [UPDATE Documents](./update_documents.md)
- 🗑️ [DELETE Documents](./delete_documents.md)
- 🏗️ [Schema Management](./schema_management.md)

---

*Last updated: November 20, 2025* 🚀
