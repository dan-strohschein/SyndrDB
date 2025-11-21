# 🏗️ CREATE BUNDLE in SyndrDB

Complete guide to creating bundles (collections/tables) in SyndrDB.

---

## 📑 Table of Contents

- [**Basic Syntax**](#-basic-syntax)
- [**CREATE BUNDLE**](#-create-bundle)
- [**WITH FIELDS Clause**](#-with-fields-clause)
- [**Field Definitions**](#-field-definitions)
  - [Field Properties](#field-properties)
  - [Field Name](#field-name)
  - [Field Type](#field-type)
  - [Required Flag](#required-flag)
  - [Unique Flag](#unique-flag)
  - [Default Value](#default-value)
- [**Supported Data Types**](#-supported-data-types)
  - [String](#string)
  - [Integer (int)](#integer-int)
  - [Float](#float)
  - [Boolean (bool)](#boolean-bool)
  - [Datetime](#datetime)

- [**Complete Examples**](#-complete-examples)
- [**Best Practices**](#-best-practices)
- [**Error Handling**](#-error-handling)

---

## 🎯 Basic Syntax

The `CREATE BUNDLE` statement creates a new bundle with defined fields and constraints.

**Standard Syntax:**
```sql
CREATE BUNDLE "<BUNDLE_NAME>"
WITH FIELDS (
    {"<FIELD_NAME>", "<FIELD_TYPE>", <IS_REQUIRED>, <IS_UNIQUE>, <DEFAULT_VALUE>},
    ...
);
```

**Components:**
- `CREATE BUNDLE` - Command keywords
- `"<BUNDLE_NAME>"` - Name of the bundle (quoted string)
- `WITH FIELDS` - Introduces the field definitions
- `( {...}, ... )` - Field definitions in braces

---

## 📦 CREATE BUNDLE

### Minimal Bundle

Create a bundle with one field:

```sql
CREATE BUNDLE "users"
WITH FIELDS (
    {"id", "int", TRUE, TRUE, 0}
);
```

**Response:**
```
Bundle 'users' created successfully.
```

### Multiple Fields Bundle

Create a bundle with multiple fields:

```sql
CREATE BUNDLE "products"
WITH FIELDS (
    {"id", "int", TRUE, TRUE, 0},
    {"name", "string", TRUE, FALSE, ""},
    {"price", "float", TRUE, FALSE, 0.0},
    {"active", "bool", FALSE, FALSE, TRUE}
);
```

> 💡 **Tip:** Each field definition is a complete specification enclosed in braces `{ }` and separated by commas.

---

## 🔧 WITH FIELDS Clause

The `WITH FIELDS` clause specifies the bundle's schema.

**Syntax:**
```sql
WITH FIELDS (
    {<field_definition>},
    {<field_definition>},
    ...
)
```

**Requirements:**
- Must follow the bundle name
- Wrapped in parentheses `( )`
- Contains one or more field definitions
- Each field definition wrapped in braces `{ }`
- Field definitions separated by commas
- At least one field is required

**Examples:**

**Single field:**
```sql
WITH FIELDS (
    {"id", "int", TRUE, TRUE, 0}
)
```

**Multiple fields:**
```sql
WITH FIELDS (
    {"id", "int", TRUE, TRUE, 0},
    {"email", "string", TRUE, TRUE, ""},
    {"created_at", "datetime", TRUE, FALSE, NULL}
)
```

---

## 🏷️ Field Definitions

### Field Properties

Each field definition consists of **5 properties** in this exact order:

```sql
{"<FIELD_NAME>", "<FIELD_TYPE>", <IS_REQUIRED>, <IS_UNIQUE>, <DEFAULT_VALUE>}
```

| Position | Property | Type | Description |
|----------|----------|------|-------------|
| 1 | Field Name | String | Name of the field |
| 2 | Field Type | String | Data type of the field |
| 3 | Is Required | Boolean | Whether field is mandatory (`TRUE`/`FALSE`) |
| 4 | Is Unique | Boolean | Whether field must be unique (`TRUE`/`FALSE`) |
| 5 | Default Value | Any | Default value matching field type |

**Example:**
```sql
{"email", "string", TRUE, TRUE, "user@example.com"}
```

### Field Name

Field names identify fields within documents.

**Rules:**
- Must be a quoted string
- Case-sensitive
- Should be descriptive
- Can contain letters, numbers, underscores

**Valid Examples:**
```sql
{"id", ...}
{"user_name", ...}
{"first_name", ...}
{"createdAt", ...}
{"email_address", ...}
```

**Invalid Examples:**
```sql
{id, ...}           -- ❌ Not quoted
{"", ...}           -- ❌ Empty string
```

> 💡 **Best Practice:** Use consistent naming conventions (`snake_case` or `camelCase`) throughout your schema.

### Field Type

Specifies the data type for the field value.

**Syntax:**
```sql
"<type_name>"
```

**Supported Types:**
- `"string"` - Text data
- `"int"` - Integer numbers
- `"float"` - Decimal numbers
- `"bool"` - Boolean (true/false)
- `"datetime"` - Date and time values

**Examples:**
```sql
{"name", "string", TRUE, FALSE, ""}
{"age", "int", TRUE, FALSE, 0}
{"price", "float", TRUE, FALSE, 0.0}
{"active", "bool", FALSE, FALSE, TRUE}
```

> 📝 **Note:** Type names are case-insensitive and normalized to lowercase internally.

### Required Flag

Determines whether the field must have a value.

**Syntax:**
```sql
TRUE  -- Field is required (cannot be NULL or empty)
FALSE -- Field is optional (can be NULL or empty)
```

**Examples:**

**Required field:**
```sql
{"email", "string", TRUE, FALSE, ""}
-- Email must be provided for every document
```

**Optional field:**
```sql
{"middle_name", "string", FALSE, FALSE, NULL}
-- Middle name can be omitted or NULL
```

> ⚠️ **Important:** Use uppercase `TRUE` and `FALSE`. Lowercase will not be recognized.

### Unique Flag

Determines whether the field value must be unique across all documents in the bundle.

**Syntax:**
```sql
TRUE  -- Field value must be unique
FALSE -- Field value can be duplicated
```

**Examples:**

**Unique field:**
```sql
{"email", "string", TRUE, TRUE, ""}
-- No two documents can have the same email
```

**Non-unique field:**
```sql
{"status", "string", TRUE, FALSE, "active"}
-- Multiple documents can have the same status
```

> 💡 **Use Case:** Use `TRUE` for fields like IDs, email addresses, usernames, or any field that should uniquely identify documents.

### Default Value

The value assigned to the field if no value is provided during document insertion.

**Syntax:**
```sql
<value_matching_field_type>
NULL  -- Explicitly no default
```

**Type-Specific Defaults:**

**String:**
```sql
{"name", "string", FALSE, FALSE, "Unknown"}
{"status", "string", TRUE, FALSE, "pending"}
```

**Integer:**
```sql
{"count", "int", FALSE, FALSE, 0}
{"age", "int", FALSE, FALSE, 18}
```

**Float:**
```sql
{"price", "float", TRUE, FALSE, 0.0}
{"rating", "float", FALSE, FALSE, 5.0}
```

**Boolean:**
```sql
{"active", "bool", FALSE, FALSE, TRUE}
{"verified", "bool", FALSE, FALSE, FALSE}
```

**NULL (no default):**
```sql
{"optional_field", "string", FALSE, FALSE, NULL}
```

> 📝 **Note:** Default values must match the field type, otherwise validation will fail.

---

## 📊 Supported Data Types

SyndrDB supports multiple data types for field definitions.

### String

Text data of any length.

**Syntax:**
```sql
"string"
```

**Default Value Examples:**
```sql
""              -- Empty string
"default text"  -- Any text
NULL            -- No default
```

**Complete Example:**
```sql
CREATE BUNDLE "users"
WITH FIELDS (
    {"username", "string", TRUE, TRUE, ""},
    {"bio", "string", FALSE, FALSE, NULL},
    {"status", "string", TRUE, FALSE, "active"}
);
```

### Integer (int)

Whole numbers (positive or negative).

**Syntax:**
```sql
"int"
```

**Default Value Examples:**
```sql
0      -- Zero
42     -- Positive integer
-10    -- Negative integer
NULL   -- No default
```

**Complete Example:**
```sql
CREATE BUNDLE "products"
WITH FIELDS (
    {"id", "int", TRUE, TRUE, 0},
    {"stock", "int", TRUE, FALSE, 0},
    {"min_order", "int", FALSE, FALSE, 1}
);
```

> 📝 **Note:** Integers are stored as `int64` internally.

### Float

Decimal numbers (floating-point).

**Syntax:**
```sql
"float"
```

**Default Value Examples:**
```sql
0.0      -- Zero
99.99    -- Positive decimal
-5.5     -- Negative decimal
NULL     -- No default
```

**Complete Example:**
```sql
CREATE BUNDLE "products"
WITH FIELDS (
    {"price", "float", TRUE, FALSE, 0.0},
    {"discount", "float", FALSE, FALSE, 0.0},
    {"rating", "float", FALSE, FALSE, 5.0}
);
```

> 📝 **Note:** Floats are stored as `float64` internally.

### Boolean (bool)

True or false values.

**Syntax:**
```sql
"bool"
```

**Default Value Examples:**
```sql
TRUE   -- True (boolean)
FALSE  -- False (boolean)
NULL   -- No default
```

**Complete Example:**
```sql
CREATE BUNDLE "settings"
WITH FIELDS (
    {"dark_mode", "bool", FALSE, FALSE, FALSE},
    {"notifications", "bool", FALSE, FALSE, TRUE},
    {"auto_save", "bool", TRUE, FALSE, TRUE}
);
```

> ⚠️ **Important:** Use uppercase `TRUE` and `FALSE` for boolean values.

### Date

Date values (year-month-day).

**Syntax:**
```sql
"date"
```

**Default Value Examples:**
```sql
"2025-01-01"     -- ISO date format
NULL             -- No default
```

**Complete Example:**
```sql
CREATE BUNDLE "events"
WITH FIELDS (
    {"event_date", "date", TRUE, FALSE, "2025-01-01"},
    {"deadline", "date", FALSE, FALSE, NULL}
);
```

> 📅 **Format:** Use ISO 8601 date format: `YYYY-MM-DD`

### Datetime

Date and time values.

**Syntax:**
```sql
"datetime"
```

**Default Value Examples:**
```sql
"2025-01-01T12:00:00Z"    -- ISO datetime format with timezone
NULL                       -- No default
```

**Complete Example:**
```sql
CREATE BUNDLE "logs"
WITH FIELDS (
    {"created_at", "datetime", TRUE, FALSE, "2025-01-01T00:00:00Z"},
    {"updated_at", "datetime", FALSE, FALSE, NULL}
);
```

> 🕐 **Format:** Use ISO 8601 datetime format: `YYYY-MM-DDTHH:MM:SSZ`

### Time

Time values (hours:minutes:seconds).

**Syntax:**
```sql
"time"
```

**Default Value Examples:**
```sql
"14:30:00"    -- 24-hour format
"00:00:00"    -- Midnight
NULL          -- No default
```

**Complete Example:**
```sql
CREATE BUNDLE "schedules"
WITH FIELDS (
    {"start_time", "time", TRUE, FALSE, "09:00:00"},
    {"end_time", "time", TRUE, FALSE, "17:00:00"}
);
```

> ⏰ **Format:** Use 24-hour time format: `HH:MM:SS`

### JSON

JSON objects for structured data.

**Syntax:**
```sql
"json"
```

**Default Value Examples:**
```sql
"{}"      -- Empty JSON object
NULL      -- No default
```

**Complete Example:**
```sql
CREATE BUNDLE "configurations"
WITH FIELDS (
    {"settings", "json", FALSE, FALSE, "{}"},
    {"metadata", "json", FALSE, FALSE, NULL}
);
```

> 💡 **Use Case:** Store complex nested data structures without predefined schema.

### Blob

Binary large objects for storing files or binary data.

**Syntax:**
```sql
"blob"
```

**Default Value Examples:**
```sql
""      -- Empty blob
NULL    -- No default
```

**Complete Example:**
```sql
CREATE BUNDLE "attachments"
WITH FIELDS (
    {"file_data", "blob", FALSE, FALSE, ""},
    {"thumbnail", "blob", FALSE, FALSE, NULL}
);
```

> 📎 **Use Case:** Store images, PDFs, or any binary file data.

---

## 🌟 Complete Examples

### Example 1: User Management System

```sql
CREATE BUNDLE "users"
WITH FIELDS (
    {"id", "int", TRUE, TRUE, 0},
    {"username", "string", TRUE, TRUE, ""},
    {"email", "string", TRUE, TRUE, ""},
    {"password_hash", "string", TRUE, FALSE, ""},
    {"first_name", "string", TRUE, FALSE, ""},
    {"last_name", "string", TRUE, FALSE, ""},
    {"age", "int", FALSE, FALSE, NULL},
    {"is_active", "bool", TRUE, FALSE, TRUE},
    {"is_verified", "bool", TRUE, FALSE, FALSE},
    {"created_at", "datetime", TRUE, FALSE, "2025-01-01T00:00:00Z"},
    {"last_login", "datetime", FALSE, FALSE, NULL}
);
```

**Use Case:** Complete user profile system with authentication and tracking.

### Example 2: E-commerce Product Catalog

```sql
CREATE BUNDLE "products"
WITH FIELDS (
    {"sku", "string", TRUE, TRUE, ""},
    {"name", "string", TRUE, FALSE, ""},
    {"description", "string", FALSE, FALSE, ""},
    {"price", "float", TRUE, FALSE, 0.0},
    {"cost", "float", FALSE, FALSE, 0.0},
    {"stock_quantity", "int", TRUE, FALSE, 0},
    {"min_stock", "int", FALSE, FALSE, 10},
    {"category", "string", TRUE, FALSE, "general"},
    {"is_available", "bool", TRUE, FALSE, TRUE},
    {"is_featured", "bool", FALSE, FALSE, FALSE},
    {"rating", "float", FALSE, FALSE, 0.0},
    {"image_data", "blob", FALSE, FALSE, NULL}
);
```

**Use Case:** Product inventory management with pricing and availability tracking.

### Example 3: Blog Posts

```sql
CREATE BUNDLE "posts"
WITH FIELDS (
    {"id", "int", TRUE, TRUE, 0},
    {"title", "string", TRUE, FALSE, ""},
    {"slug", "string", TRUE, TRUE, ""},
    {"content", "string", TRUE, FALSE, ""},
    {"excerpt", "string", FALSE, FALSE, ""},
    {"author_id", "int", TRUE, FALSE, 0},
    {"category", "string", TRUE, FALSE, "uncategorized"},
    {"is_published", "bool", TRUE, FALSE, FALSE},
    {"view_count", "int", FALSE, FALSE, 0},
    {"published_at", "datetime", FALSE, FALSE, NULL},
    {"created_at", "datetime", TRUE, FALSE, "2025-01-01T00:00:00Z"},
    {"updated_at", "datetime", FALSE, FALSE, NULL},
    {"metadata", "json", FALSE, FALSE, "{}"}
);
```

**Use Case:** Content management system with publishing workflow.

### Example 4: Order Tracking

```sql
CREATE BUNDLE "orders"
WITH FIELDS (
    {"order_id", "int", TRUE, TRUE, 0},
    {"customer_id", "int", TRUE, FALSE, 0},
    {"order_date", "date", TRUE, FALSE, "2025-01-01"},
    {"order_time", "time", TRUE, FALSE, "00:00:00"},
    {"total_amount", "float", TRUE, FALSE, 0.0},
    {"discount", "float", FALSE, FALSE, 0.0},
    {"tax", "float", FALSE, FALSE, 0.0},
    {"status", "string", TRUE, FALSE, "pending"},
    {"is_paid", "bool", TRUE, FALSE, FALSE},
    {"is_shipped", "bool", TRUE, FALSE, FALSE},
    {"tracking_number", "string", FALSE, FALSE, NULL},
    {"notes", "string", FALSE, FALSE, ""}
);
```

**Use Case:** E-commerce order management with payment and shipping status.

### Example 5: Simple Configuration

```sql
CREATE BUNDLE "settings"
WITH FIELDS (
    {"key", "string", TRUE, TRUE, ""},
    {"value", "string", TRUE, FALSE, ""},
    {"description", "string", FALSE, FALSE, ""}
);
```

**Use Case:** Application configuration key-value store.

### Example 6: Compact Format

```sql
CREATE BUNDLE "tags" WITH FIELDS ({"id", "int", TRUE, TRUE, 0}, {"name", "string", TRUE, TRUE, ""});
```

**Use Case:** Single-line format for simple bundles.

---

## ✅ Best Practices

### 1. **Always Quote Bundle Names**

```sql
✅ CORRECT:   CREATE BUNDLE "users" WITH FIELDS (...)
❌ INCORRECT: CREATE BUNDLE users WITH FIELDS (...)
```

### 2. **Always Quote Field Names and Types**

```sql
✅ CORRECT:   {"name", "string", TRUE, FALSE, ""}
❌ INCORRECT: {name, string, TRUE, FALSE, ""}
```

### 3. **Use Uppercase for Booleans**

```sql
✅ CORRECT:   {"active", "bool", TRUE, FALSE, FALSE}
❌ INCORRECT: {"active", "bool", true, false, false}
```

### 4. **Include at Least One Field**

Every bundle must have at least one field definition.

```sql
✅ CORRECT:
CREATE BUNDLE "users" WITH FIELDS (
    {"id", "int", TRUE, TRUE, 0}
);

❌ INCORRECT:
CREATE BUNDLE "users" WITH FIELDS ();
```

### 5. **Use Unique IDs**

Always include a unique identifier field:

```sql
{"id", "int", TRUE, TRUE, 0}
-- or
{"uuid", "string", TRUE, TRUE, ""}
```

### 6. **Choose Appropriate Types**

Match data types to your data:

```sql
✅ CORRECT:   {"age", "int", ...}
❌ AVOID:     {"age", "string", ...}

✅ CORRECT:   {"price", "float", ...}
❌ AVOID:     {"price", "string", ...}
```

### 7. **Use Meaningful Default Values**

Provide sensible defaults:

```sql
✅ GOOD:   {"status", "string", TRUE, FALSE, "pending"}
❌ AVOID:  {"status", "string", TRUE, FALSE, ""}

✅ GOOD:   {"quantity", "int", FALSE, FALSE, 0}
✅ GOOD:   {"optional", "string", FALSE, FALSE, NULL}
```

### 8. **Consistent Naming Convention**

Choose one naming style and stick to it:

```sql
✅ SNAKE_CASE (recommended):
{"first_name", "string", TRUE, FALSE, ""}
{"last_name", "string", TRUE, FALSE, ""}

✅ CAMELCASE (also valid):
{"firstName", "string", TRUE, FALSE, ""}
{"lastName", "string", TRUE, FALSE, ""}

❌ MIXED (avoid):
{"first_name", "string", TRUE, FALSE, ""}
{"lastName", "string", TRUE, FALSE, ""}
```

### 9. **Semicolon is Optional**

Both formats are valid:

```sql
✅ WITH semicolon:    CREATE BUNDLE "users" WITH FIELDS (...);
✅ WITHOUT semicolon: CREATE BUNDLE "users" WITH FIELDS (...)
```

### 10. **Multi-line for Readability**

Format complex bundles with multiple lines:

```sql
✅ READABLE:
CREATE BUNDLE "users"
WITH FIELDS (
    {"id", "int", TRUE, TRUE, 0},
    {"email", "string", TRUE, TRUE, ""},
    {"name", "string", TRUE, FALSE, ""}
);

❌ HARD TO READ:
CREATE BUNDLE "users" WITH FIELDS ({"id", "int", TRUE, TRUE, 0}, {"email", "string", TRUE, TRUE, ""}, {"name", "string", TRUE, FALSE, ""});
```

---

## ⚠️ Error Handling

### Common Errors and Solutions

#### 1. **Missing Bundle Name**

```sql
❌ ERROR:
CREATE BUNDLE WITH FIELDS ({"id", "int", TRUE, TRUE, 0});
```

**Error Message:**
```
invalid CREATE BUNDLE command syntax
```

**Solution:**
```sql
✅ CORRECT:
CREATE BUNDLE "users" WITH FIELDS ({"id", "int", TRUE, TRUE, 0});
```

#### 2. **Missing WITH FIELDS**

```sql
❌ ERROR:
CREATE BUNDLE "users" ({"id", "int", TRUE, TRUE, 0});
```

**Error Message:**
```
WITH FIELDS section not found in CREATE BUNDLE command
```

**Solution:**
```sql
✅ CORRECT:
CREATE BUNDLE "users" WITH FIELDS ({"id", "int", TRUE, TRUE, 0});
```

#### 3. **No Fields Defined**

```sql
❌ ERROR:
CREATE BUNDLE "users" WITH FIELDS ();
```

**Error Message:**
```
CREATE BUNDLE must specify at least one field
```

**Solution:**
```sql
✅ CORRECT:
CREATE BUNDLE "users" WITH FIELDS ({"id", "int", TRUE, TRUE, 0});
```

#### 4. **Field Name Not Quoted**

```sql
❌ ERROR:
CREATE BUNDLE "users" WITH FIELDS ({id, "int", TRUE, TRUE, 0});
```

**Error Message:**
```
field name must be a string
```

**Solution:**
```sql
✅ CORRECT:
CREATE BUNDLE "users" WITH FIELDS ({"id", "int", TRUE, TRUE, 0});
```

#### 5. **Field Type Not Quoted**

```sql
❌ ERROR:
CREATE BUNDLE "users" WITH FIELDS ({"id", int, TRUE, TRUE, 0});
```

**Error Message:**
```
field type must be a string
```

**Solution:**
```sql
✅ CORRECT:
CREATE BUNDLE "users" WITH FIELDS ({"id", "int", TRUE, TRUE, 0});
```

#### 6. **Missing Field Properties**

```sql
❌ ERROR:
CREATE BUNDLE "users" WITH FIELDS ({"id", "int", TRUE});
```

**Error Message:**
```
field definition must have name, type, required, and unique properties
```

**Solution:**
```sql
✅ CORRECT (all 5 properties):
CREATE BUNDLE "users" WITH FIELDS ({"id", "int", TRUE, TRUE, 0});
```

#### 7. **Missing Comma Between Fields**

```sql
❌ ERROR:
CREATE BUNDLE "users" WITH FIELDS (
    {"id", "int", TRUE, TRUE, 0}
    {"name", "string", TRUE, FALSE, ""}
);
```

**Error Message:**
```
expected comma or closing parenthesis
```

**Solution:**
```sql
✅ CORRECT:
CREATE BUNDLE "users" WITH FIELDS (
    {"id", "int", TRUE, TRUE, 0},
    {"name", "string", TRUE, FALSE, ""}
);
```

#### 8. **Invalid Bundle Name Characters**

```sql
❌ ERROR:
CREATE BUNDLE "my bundle!" WITH FIELDS (...);
```

**Error Message:**
```
invalid bundle name 'my bundle!': contains invalid characters
```

**Solution:**
```sql
✅ CORRECT:
CREATE BUNDLE "my_bundle" WITH FIELDS (...);
```

> 💡 **Allowed characters:** Letters, numbers, underscores (`_`), and hyphens (`-`). No spaces or special characters.

#### 9. **Bundle Already Exists**

```sql
❌ ERROR:
CREATE BUNDLE "users" WITH FIELDS (...);
-- Then running again:
CREATE BUNDLE "users" WITH FIELDS (...);
```

**Error Message:**
```
bundle 'users' already exists
```

**Solution:**
Use a different bundle name or delete the existing bundle first.

---

## 🚀 After Creation

Once a bundle is created, you can:

1. **Add documents:**
   ```sql
   ADD DOCUMENT TO BUNDLE "users" WITH ({"name" = "Alice"});
   ```

2. **Query documents:**
   ```sql
   SELECT * FROM "users";
   ```

3. **Create indexes:**
   ```sql
   CREATE HASH INDEX "users_email_idx" ON BUNDLE "users" ("email");
   ```

4. **Add relationships:**
   ```sql
   UPDATE BUNDLE "users" ADD RELATIONSHIP ("1toMany", "users", "id", "orders", "user_id");
   ```

---

## 🔍 Verification

Verify the bundle was created successfully:

```sql
SHOW BUNDLE "users";
```

**Expected Output:**
```
Bundle: users
Fields:
  - id (int, required, unique)
  - email (string, required, unique)
  - name (string, required)
```

---

## 🎓 Quick Reference

| Component | Syntax | Example |
|-----------|--------|---------|
| **Basic CREATE** | `CREATE BUNDLE "name" WITH FIELDS (...)` | `CREATE BUNDLE "users" WITH FIELDS (...)` |
| **Field Definition** | `{"name", "type", req, uniq, default}` | `{"email", "string", TRUE, TRUE, ""}` |
| **String Type** | `"string"` | `{"name", "string", TRUE, FALSE, ""}` |
| **Integer Type** | `"int"` | `{"age", "int", FALSE, FALSE, 0}` |
| **Float Type** | `"float"` | `{"price", "float", TRUE, FALSE, 0.0}` |
| **Boolean Type** | `"bool"` | `{"active", "bool", FALSE, FALSE, TRUE}` |
| **Date Type** | `"date"` | `{"created", "date", TRUE, FALSE, "2025-01-01"}` |
| **Datetime Type** | `"datetime"` | `{"timestamp", "datetime", TRUE, FALSE, "2025-01-01T00:00:00Z"}` |
| **Time Type** | `"time"` | `{"start", "time", TRUE, FALSE, "09:00:00"}` |
| **JSON Type** | `"json"` | `{"data", "json", FALSE, FALSE, "{}"}` |
| **Blob Type** | `"blob"` | `{"file", "blob", FALSE, FALSE, ""}` |
| **Required** | `TRUE` | Required field (cannot be NULL) |
| **Optional** | `FALSE` | Optional field (can be NULL) |
| **Unique** | `TRUE` | Value must be unique across bundle |
| **Non-Unique** | `FALSE` | Value can be duplicated |
| **NULL Default** | `NULL` | No default value |

---

## 🆘 Getting Help

For more information:
- 📖 [SyndrDB Documentation](../README.md)
- 📝 [ADD Documents](./add_documents.md)
- 🔄 [UPDATE Bundle](./update_bundle.md)
- 🗑️ [DELETE Bundle](./delete_bundle.md)
- 🔍 [SELECT Queries](./select_queries.md)

---

*Last updated: November 20, 2025* 🚀
