# 📊 SyndrDB SHOW Commands

This document covers SHOW commands in SyndrDB. These commands retrieve metadata and system information about databases, bundles, users, and sessions.

---

## 📑 Table of Contents

1. [SHOW DATABASES](#show-databases)
2. [SHOW BUNDLES](#show-bundles)
3. [SHOW BUNDLES FOR](#show-bundles-for)
4. [SHOW BUNDLE](#show-bundle)
5. [SHOW USERS](#show-users)
6. [SHOW SESSIONS](#show-sessions)
7. [SHOW SESSION](#show-session)
8. [SHOW MIGRATIONS](#show-migrations)
9. [SHOW RATE LIMIT](#show-rate-limit)
10. [Quick Reference](#quick-reference)

---

## SHOW DATABASES

Lists all databases currently loaded in the SyndrDB system.

### Syntax

```sql
SHOW DATABASES;
```

### Components

This command has no parameters - it always returns all loaded databases.

### Examples

#### Basic Usage

```sql
-- List all databases
SHOW DATABASES;
```

#### Response Format

```json
{
    "ResultCount": 3,
    "Result": [
        "primary",
        "myapp",
        "analytics"
    ]
}
```

### Behavior

1. **Database Retrieval:** Fetches list of databases from DatabaseService (in-memory)
2. **Catalog Verification:** Optionally verifies consistency with catalog (if InternalCatalogService available)
3. **Consistency Check:** Logs warnings if databases are in catalog but not loaded (or vice versa)
4. **Response:** Returns array of database names

### Database Sources

The command returns databases that are:
- ✅ Loaded in memory (DatabaseService)
- ✅ Registered in system catalog (Databases bundle in primary database)

### Catalog Consistency

The command performs optional consistency checks:

| Scenario | Logged Warning |
|----------|----------------|
| Database in catalog but not loaded | "Database 'X' is in catalog but not loaded in memory" |
| Database loaded but not in catalog | "Database 'X' is loaded but not found in catalog" |

These warnings help identify potential data integrity issues.

### Use Cases

#### Check Available Databases

```sql
-- Before switching databases, check what's available
SHOW DATABASES;
-- Response: ["primary", "development", "production"]

-- Now switch to one
USE "production";
```

#### Verify Database Creation

```sql
-- Create a new database
CREATE DATABASE "test_db";

-- Verify it was created
SHOW DATABASES;
-- Response should include "test_db"
```

#### System Health Check

```sql
-- Check database consistency (look for warnings in logs)
SHOW DATABASES;
-- Server logs will show any catalog inconsistencies
```

### Error Cases

This command rarely fails, but potential issues:

| Error | Cause | Solution |
|-------|-------|----------|
| `database service unavailable` | DatabaseService not initialized | Check server startup, verify initialization |
| `catalog service unavailable` | InternalCatalogService not available | Non-critical - command still returns loaded databases |

### Best Practices

✅ **DO:**
- Use before `USE` command to verify database exists
- Use to verify database creation succeeded
- Check logs for consistency warnings

❌ **DON'T:**
- Don't assume all databases in catalog are loaded (some may be detached)
- Don't ignore consistency warnings in production

### Complete Example

```sql
-- Development workflow

-- 1. Check what databases exist
SHOW DATABASES;
-- Response: ["primary"]

-- 2. Create development database
CREATE DATABASE "dev_app";

-- 3. Verify creation
SHOW DATABASES;
-- Response: ["primary", "dev_app"]

-- 4. Switch to new database
USE "dev_app";

-- 5. Create some bundles
CREATE BUNDLE "users" WITH FIELDS (
    {"id", STRING, true, true},
    {"name", STRING, true, false}
);

-- 6. List databases again (same as before)
SHOW DATABASES;
-- Response: ["primary", "dev_app"]
```

---

## SHOW BUNDLES

Lists all bundles in the currently active database.

### Syntax

```sql
SHOW BUNDLES;
```

### Components

This command has no parameters - it operates on the currently selected database.

### Prerequisites

⚠️ **Active Database Required:** You must select a database with `USE` before running this command.

### Examples

#### Basic Usage

```sql
-- First, select a database
USE "myapp";

-- Then list its bundles
SHOW BUNDLES;
```

#### Response Format

```json
{
    "ResultCount": 3,
    "Result": [
        {
            "Name": "users",
            "BundleID": "bnd_1234567890",
            "DatabaseName": "myapp",
            "FieldCount": 5,
            "FilePath": "./data_files/myapp/users.bnd"
        },
        {
            "Name": "orders",
            "BundleID": "bnd_0987654321",
            "DatabaseName": "myapp",
            "FieldCount": 8,
            "FilePath": "./data_files/myapp/orders.bnd"
        },
        {
            "Name": "products",
            "BundleID": "bnd_1122334455",
            "DatabaseName": "myapp",
            "FieldCount": 6,
            "FilePath": "./data_files/myapp/products.bnd"
        }
    ]
}
```

### Behavior

1. **Database Check:** Verifies a database is currently selected
2. **Catalog Retrieval:** Fetches bundles from system catalog (Bundles bundle in primary database)
3. **Filtering:** Returns only bundles for the active database
4. **Response:** Returns array of bundle metadata objects

### Bundle Information

Each bundle in the response includes:

| Field | Description | Example |
|-------|-------------|---------|
| **Name** | Bundle name | `"users"` |
| **BundleID** | Unique bundle identifier | `"bnd_1234567890"` |
| **DatabaseName** | Parent database name | `"myapp"` |
| **FieldCount** | Number of fields in bundle | `5` |
| **FilePath** | Path to bundle file | `"./data_files/myapp/users.bnd"` |

### Use Cases

#### Explore Database Schema

```sql
-- Switch to database
USE "analytics";

-- See what bundles exist
SHOW BUNDLES;
-- Response shows all bundles: events, users, sessions, etc.
```

#### Verify Bundle Creation

```sql
USE "myapp";

-- Before creating bundle
SHOW BUNDLES;
-- Response: ["users", "orders"]

-- Create new bundle
CREATE BUNDLE "products" WITH FIELDS (
    {"id", STRING, true, true},
    {"name", STRING, true, false}
);

-- Verify creation
SHOW BUNDLES;
-- Response now includes "products"
```

#### Count Bundles in Database

```sql
USE "production";
SHOW BUNDLES;
-- Check ResultCount in response: {"ResultCount": 15, "Result": [...]}
```

### Error Cases

| Error | Cause | Solution |
|-------|-------|----------|
| `no database selected` | No active database | Use `USE "database_name"` first |
| `database not found in system` | Database was deleted/detached | Use `SHOW DATABASES` to see available databases |
| `internal catalog service unavailable` | System error | Check server logs, verify primary database |
| `failed to retrieve bundles` | Catalog access error | Check primary database health |

### Best Practices

✅ **DO:**
- Always use `USE` before `SHOW BUNDLES`
- Check bundle names before creating new bundles (avoid duplicates)
- Use to verify bundle creation/deletion succeeded

❌ **DON'T:**
- Don't forget to select a database first
- Don't assume bundle order in response (may vary)

### Complete Example

```sql
-- Explore a database structure

-- 1. Check available databases
SHOW DATABASES;
-- Response: ["primary", "myapp", "analytics"]

-- 2. Switch to application database
USE "myapp";

-- 3. List all bundles
SHOW BUNDLES;
-- Response shows: users, orders, products, invoices

-- 4. Get details on specific bundle
SHOW BUNDLE "users";
-- Response shows field structure, indexes, relationships

-- 5. Work with the bundle
SELECT * FROM BUNDLE "users" WHERE status = "active";
```

---

## SHOW BUNDLES FOR

Lists all bundles in a specific database without switching the active database context.

### Syntax

```sql
SHOW BUNDLES FOR "database_name";
```

### Components

| Component | Description | Required |
|-----------|-------------|----------|
| **database_name** | Name of the database to list bundles for | ✅ Yes |

### Difference from SHOW BUNDLES

| Command | Requires USE? | Changes Context? | Database |
|---------|---------------|------------------|----------|
| `SHOW BUNDLES` | ✅ Yes | No | Current database |
| `SHOW BUNDLES FOR` | ❌ No | No | Specified database |

### Examples

#### Basic Usage

```sql
-- List bundles from another database without switching
SHOW BUNDLES FOR "analytics";
```

#### Compare Multiple Databases

```sql
-- Check bundles in different databases without switching
SHOW BUNDLES FOR "development";
SHOW BUNDLES FOR "staging";
SHOW BUNDLES FOR "production";
```

#### Cross-Database Inspection

```sql
-- Currently working in one database
USE "myapp";
SELECT * FROM BUNDLE "users";

-- Check bundles in another database without switching
SHOW BUNDLES FOR "reporting";
-- Still in "myapp" context after this command
```

#### Response Format

```json
{
    "ResultCount": 2,
    "Result": [
        {
            "Name": "events",
            "BundleID": "bnd_2233445566",
            "DatabaseName": "analytics",
            "FieldCount": 10,
            "FilePath": "./data_files/analytics/events.bnd"
        },
        {
            "Name": "sessions",
            "BundleID": "bnd_3344556677",
            "DatabaseName": "analytics",
            "FieldCount": 7,
            "FilePath": "./data_files/analytics/sessions.bnd"
        }
    ]
}
```

### Behavior

1. **Database Lookup:** Finds database by name (case-sensitive)
2. **Existence Check:** Verifies database exists in loaded databases
3. **Catalog Verification:** Checks database exists in system catalog
4. **Bundle Retrieval:** Fetches all bundles for specified database
5. **Context Preservation:** Active database remains unchanged
6. **Response:** Returns array of bundle metadata

### Database Validation

The command performs two checks:

| Check | Purpose | Error if Fails |
|-------|---------|----------------|
| **Loaded Check** | Database exists in DatabaseService | `database not found in system` |
| **Catalog Check** | Database exists in system catalog | `database not found in catalog` |

Both must pass for the command to succeed.

### Use Cases

#### Database Comparison

```sql
-- Compare schema across environments
SHOW BUNDLES FOR "development";
SHOW BUNDLES FOR "production";

-- Verify both have same bundles
```

#### Migration Planning

```sql
-- Check source database bundles
SHOW BUNDLES FOR "old_app";

-- Check target database bundles  
SHOW BUNDLES FOR "new_app";

-- Plan migration based on bundle differences
```

#### Multi-Database Monitoring

```sql
-- Monitor multiple databases from single context
SHOW BUNDLES FOR "app1";
SHOW BUNDLES FOR "app2";
SHOW BUNDLES FOR "app3";

-- No need to switch between databases
```

#### Schema Verification

```sql
-- Verify database has expected bundles
SHOW BUNDLES FOR "production";
-- Check if "users", "orders", "products" are all present
```

### Error Cases

| Error | Cause | Solution |
|-------|-------|----------|
| `database not found in system` | Database doesn't exist or isn't loaded | Use `SHOW DATABASES` to see available databases |
| `database not found in catalog` | Database not registered in catalog | Check database integrity, may need recreation |
| `failed to parse database name` | Invalid syntax | Use format: `SHOW BUNDLES FOR "name";` |
| `internal catalog service unavailable` | System error | Check primary database and server logs |

### Best Practices

✅ **DO:**
- Use when inspecting multiple databases
- Use for read-only database exploration
- Use in monitoring/reporting scripts
- Verify database name with `SHOW DATABASES` first

❌ **DON'T:**
- Don't confuse with `SHOW BUNDLES` (different behavior)
- Don't assume case-insensitive database names (they're case-sensitive)
- Don't forget quotes around database name

### Complete Example

```sql
-- Multi-environment database inspection

-- 1. Check what databases exist
SHOW DATABASES;
-- Response: ["primary", "dev", "staging", "prod"]

-- 2. Inspect development bundles
SHOW BUNDLES FOR "dev";
-- Response: users, orders, products (FieldCount varies)

-- 3. Compare with staging
SHOW BUNDLES FOR "staging";
-- Response: users, orders, products (should match dev)

-- 4. Check production
SHOW BUNDLES FOR "prod";
-- Response: users, orders, products, reports, analytics

-- 5. Note: Still in whatever database was active before
--    (or no database if none selected)

-- 6. Now switch to work in specific database
USE "dev";
SHOW BUNDLES;
-- Same as: SHOW BUNDLES FOR "dev";
```

---

## SHOW BUNDLE

Displays detailed metadata about a specific bundle including its schema, indexes, relationships, and constraints.

### Syntax

```sql
SHOW BUNDLE "bundle_name";
```

### Components

| Component | Description | Required |
|-----------|-------------|----------|
| **bundle_name** | Name of the bundle to inspect | ✅ Yes |

### Prerequisites

⚠️ **Active Database Required:** You must select a database with `USE` before running this command.

### Examples

#### Basic Usage

```sql
-- First, select a database
USE "myapp";

-- Show details of specific bundle
SHOW BUNDLE "users";
```

#### Response Format

```json
{
    "ResultCount": 1,
    "Result": {
        "Name": "users",
        "BundleID": "bnd_1234567890",
        "Description": "User accounts",
        "CreatedBy": "admin",
        "CreatedAt": "2024-01-15T10:30:00Z",
        "UpdatedAt": "2024-01-20T14:22:00Z",
        "Permissions": [],
        "PageCount": 5,
        "TotalDocuments": 1250,
        "DocumentStructure": {
            "FieldDefinitions": {
                "id": {
                    "Name": "id",
                    "Type": "STRING",
                    "IsRequired": true,
                    "IsUnique": true,
                    "DefaultValue": ""
                },
                "email": {
                    "Name": "email",
                    "Type": "STRING",
                    "IsRequired": true,
                    "IsUnique": true,
                    "DefaultValue": ""
                },
                "name": {
                    "Name": "name",
                    "Type": "STRING",
                    "IsRequired": true,
                    "IsUnique": false,
                    "DefaultValue": ""
                },
                "age": {
                    "Name": "age",
                    "Type": "INT",
                    "IsRequired": false,
                    "IsUnique": false,
                    "DefaultValue": 0
                },
                "created_at": {
                    "Name": "created_at",
                    "Type": "DATETIME",
                    "IsRequired": true,
                    "IsUnique": false,
                    "DefaultValue": "CURRENT_TIMESTAMP"
                }
            }
        },
        "Indexes": {
            "email_hash_idx": {
                "IndexName": "email_hash_idx",
                "IndexType": "HASH",
                "FieldName": "email",
                "BundleName": "users"
            }
        },
        "Relationships": {
            "user_orders": {
                "RelationshipName": "user_orders",
                "SourceBundle": "users",
                "SourceField": "id",
                "DestinationBundle": "orders",
                "DestinationField": "user_id",
                "RelationshipType": "1toMany"
            }
        },
        "Constraints": {}
    }
}
```

### Bundle Metadata

The response includes comprehensive bundle information:

#### Core Information

| Field | Description | Type |
|-------|-------------|------|
| **Name** | Bundle name | String |
| **BundleID** | Unique identifier | String |
| **Description** | Bundle description | String |
| **CreatedBy** | Creator username | String |
| **CreatedAt** | Creation timestamp | DateTime |
| **UpdatedAt** | Last update timestamp | DateTime |
| **PageCount** | Number of data pages | Integer |
| **TotalDocuments** | Total document count | Integer |

#### Document Structure

Complete field schema with:
- Field names
- Data types (STRING, INT, FLOAT, BOOLEAN, DATETIME, JSON, TEXT, BLOB, TIMESTAMP)
- Required/optional flags
- Unique constraints
- Default values

#### Indexes

Hash and B-Tree indexes on bundle fields:
- Index name
- Index type (HASH or BTREE)
- Field name
- Bundle name

#### Relationships

Links to other bundles:
- Relationship name
- Source bundle and field
- Destination bundle and field
- Relationship type (1to1, 1toMany, ManytoMany)

#### Constraints

Field and document-level constraints (if defined)

### Use Cases

#### Explore Bundle Schema

```sql
USE "myapp";

-- See complete bundle structure
SHOW BUNDLE "users";
-- Review field types, constraints, indexes
```

#### Verify Bundle Creation

```sql
USE "myapp";

-- Create bundle
CREATE BUNDLE "products" WITH FIELDS (
    {"id", STRING, true, true},
    {"name", STRING, true, false},
    {"price", FLOAT, true, false}
);

-- Verify structure
SHOW BUNDLE "products";
-- Confirm fields were created correctly
```

#### Check Indexes

```sql
USE "analytics";

-- Check what indexes exist
SHOW BUNDLE "events";
-- Look at "Indexes" section in response
```

#### Review Relationships

```sql
USE "myapp";

-- See bundle relationships
SHOW BUNDLE "users";
-- Check "Relationships" section for links to other bundles
```

#### Document Planning

```sql
USE "myapp";

-- Before adding documents, check schema
SHOW BUNDLE "users";
-- Review required fields, data types, constraints
-- Plan document structure accordingly
```

### Error Cases

| Error | Cause | Solution |
|-------|-------|----------|
| `no database selected` | No active database | Use `USE "database_name"` first |
| `failed to parse bundle name` | Invalid syntax | Use format: `SHOW BUNDLE "name";` |
| `bundle not found` | Bundle doesn't exist | Use `SHOW BUNDLES` to see available bundles |
| `failed to retrieve bundle` | System error | Check bundle integrity, server logs |

### Best Practices

✅ **DO:**
- Use before adding documents (verify schema)
- Use after bundle modifications (verify changes)
- Review indexes to understand query performance
- Check relationships before joining bundles
- Use to document database schema

❌ **DON'T:**
- Don't assume field names (verify with SHOW BUNDLE)
- Don't forget required fields when adding documents
- Don't ignore index information (affects query performance)

### Complete Example

```sql
-- Complete bundle exploration workflow

-- 1. Switch to database
USE "myapp";

-- 2. See what bundles exist
SHOW BUNDLES;
-- Response shows: users, orders, products

-- 3. Inspect users bundle structure
SHOW BUNDLE "users";
-- Review fields:
--   - id: STRING, required, unique
--   - email: STRING, required, unique
--   - name: STRING, required
--   - age: INT, optional
--   - created_at: DATETIME, required
-- Review indexes:
--   - email_hash_idx on email field
-- Review relationships:
--   - user_orders: users.id -> orders.user_id (1toMany)

-- 4. Now add documents with correct schema
ADD DOCUMENT TO BUNDLE "users" WITH VALUES (
    {
        "id": "user_001",
        "email": "alice@example.com",
        "name": "Alice Smith",
        "age": 28,
        "created_at": "2024-01-15T10:30:00Z"
    }
);

-- 5. Query using indexed field (fast)
SELECT * FROM BUNDLE "users" WHERE email = "alice@example.com";
-- Uses email_hash_idx for fast lookup

-- 6. Use relationship to join bundles
SELECT u.name, o.total 
FROM BUNDLE "users" u
JOIN BUNDLE "orders" o ON u.id = o.user_id
WHERE u.email = "alice@example.com";
```

---

## SHOW USERS

Lists all user accounts from the Users bundle in the primary database. Excludes sensitive information like password hashes.

### Syntax

```sql
SHOW USERS;
```

### Components

This command has no parameters - it always returns all users from the primary database.

### Examples

#### Basic Usage

```sql
-- List all users
SHOW USERS;
```

#### Response Format

```json
{
    "ResultCount": 3,
    "Result": [
        {
            "DocumentID": "doc_1234567890",
            "UserID": "user_001",
            "Name": "alice",
            "IsActive": true,
            "IsLockedOut": false,
            "FailedLoginAttempts": 0,
            "LockoutExpiresOn": null
        },
        {
            "DocumentID": "doc_0987654321",
            "UserID": "user_002",
            "Name": "bob",
            "IsActive": true,
            "IsLockedOut": false,
            "FailedLoginAttempts": 0,
            "LockoutExpiresOn": null
        },
        {
            "DocumentID": "doc_1122334455",
            "UserID": "user_003",
            "Name": "charlie",
            "IsActive": false,
            "IsLockedOut": true,
            "FailedLoginAttempts": 5,
            "LockoutExpiresOn": "2024-01-20T15:00:00Z"
        }
    ]
}
```

### User Fields

Each user object includes:

| Field | Description | Type | Example |
|-------|-------------|------|---------|
| **DocumentID** | Document identifier | String | `"doc_1234567890"` |
| **UserID** | Unique user identifier | String | `"user_001"` |
| **Name** | Username | String | `"alice"` |
| **IsActive** | Account active status | Boolean | `true` |
| **IsLockedOut** | Account locked status | Boolean | `false` |
| **FailedLoginAttempts** | Failed login count | Integer | `0` |
| **LockoutExpiresOn** | Lockout expiration time | Timestamp | `null` or datetime |

**Note:** `PasswordHash` is **excluded** from the response for security.

### Behavior

1. **Primary Database Access:** Always queries primary database (where Users bundle resides)
2. **Users Bundle Retrieval:** Fetches Users bundle from primary database
3. **Document Retrieval:** Gets all documents from Users bundle
4. **Field Filtering:** Removes PasswordHash field from response
5. **Response:** Returns array of user objects

### Use Cases

#### List All Users

```sql
-- See all user accounts
SHOW USERS;
-- Review usernames, active status, lockout status
```

#### Check User Exists

```sql
-- Before creating user, check if exists
SHOW USERS;
-- Look for username in result

-- Create if not exists
CREATE USER "new_user" WITH PASSWORD 'SecureP@ss123!';

-- Verify creation
SHOW USERS;
-- Should now include "new_user"
```

#### Monitor Account Lockouts

```sql
-- Check for locked accounts
SHOW USERS;
-- Look for users with IsLockedOut = true
-- Check FailedLoginAttempts count
```

#### Audit Active Accounts

```sql
-- List all users
SHOW USERS;
-- Filter by IsActive in application logic
-- Identify inactive accounts for cleanup
```

### Security Considerations

🔒 **Password Protection:**
- `PasswordHash` field is **never** returned
- Only Argon2id hashes stored in database
- Original passwords cannot be recovered
- Even admins cannot view passwords

🔒 **Sensitive Information:**
The response includes some security-related fields:
- Failed login attempts
- Lockout status
- Lockout expiration

This information is useful for security monitoring but should be protected from unauthorized access.

### Error Cases

| Error | Cause | Solution |
|-------|-------|----------|
| `primary database not found` | Primary database not loaded | Check server initialization, restart server |
| `Users bundle not found` | System bundle missing | Restore primary database from backup |
| `failed to retrieve users` | System error | Check primary database integrity |

### Best Practices

✅ **DO:**
- Use to verify user creation
- Monitor for locked accounts
- Audit user list periodically
- Check for inactive accounts

❌ **DON'T:**
- Don't expect password hashes in response
- Don't rely solely on this for authentication
- Don't expose user list to non-admin users
- Don't use for permission checking (use dedicated permission queries)

### Complete Example

```sql
-- User management workflow

-- 1. Check existing users
SHOW USERS;
-- Response: alice, bob (both active)

-- 2. Create new user
CREATE USER "charlie" WITH PASSWORD 'CharlieP@ss!';

-- 3. Verify creation
SHOW USERS;
-- Response: alice, bob, charlie

-- 4. Grant permissions to new user
GRANT ROLE "Data-Writer" TO USER "charlie";

-- 5. Later, check for locked accounts
SHOW USERS;
-- Look for IsLockedOut = true
-- Example: Charlie has 5 failed attempts, locked until 3pm

-- 6. Check who's active
SHOW USERS;
-- Filter by IsActive = true in application
```

---

## SHOW SESSIONS

Lists all active sessions in the SyndrDB server.

### Syntax

```sql
SHOW SESSIONS;
```

### Status

⚠️ **Partially Implemented:** This command exists but requires SessionManager context from the server, which is not currently available in the CommandDirector.

### Current Behavior

The command returns a placeholder response indicating that session management requires server context.

### Response Format

```json
{
    "ResultCount": 1,
    "Result": "Session management requires server context - use server.SessionManager.GetSessionStats()"
}
```

### Planned Features

When fully implemented, this command will return:
- Active session IDs
- Session usernames
- Connection times
- Last activity timestamps
- IP addresses
- Session states

### Alternative

Currently, session information can be accessed programmatically via:
```go
server.SessionManager.GetSessionStats()
```

### Future Implementation

```sql
-- Planned response format:
SHOW SESSIONS;

-- Expected response:
{
    "ResultCount": 3,
    "Result": [
        {
            "SessionID": "sess_abc123",
            "Username": "alice",
            "ConnectedAt": "2024-01-20T10:00:00Z",
            "LastActivity": "2024-01-20T10:30:00Z",
            "IPAddress": "192.168.1.100",
            "State": "active"
        },
        {
            "SessionID": "sess_def456",
            "Username": "bob",
            "ConnectedAt": "2024-01-20T09:30:00Z",
            "LastActivity": "2024-01-20T10:29:00Z",
            "IPAddress": "192.168.1.101",
            "State": "active"
        },
        {
            "SessionID": "sess_ghi789",
            "Username": "charlie",
            "ConnectedAt": "2024-01-20T08:00:00Z",
            "LastActivity": "2024-01-20T10:28:00Z",
            "IPAddress": "192.168.1.102",
            "State": "idle"
        }
    ]
}
```

---

## SHOW SESSION

Displays information about a specific session by session ID.

### Syntax

```sql
SHOW SESSION session_id;
```

### Status

⚠️ **Partially Implemented:** This command exists but requires SessionManager context from the server, which is not currently available in the CommandDirector.

### Current Behavior

The command returns a placeholder response indicating that session information requires server context.

### Example

```sql
SHOW SESSION sess_abc123;
```

### Response Format

```json
{
    "ResultCount": 1,
    "Result": "Session info for sess_abc123 requires server context - use server.SessionManager.GetSession()"
}
```

### Alternative

Currently, session information can be accessed programmatically via:
```go
server.SessionManager.GetSession(sessionID)
```

### Future Implementation

```sql
-- Planned usage:
SHOW SESSION sess_abc123;

-- Expected response:
{
    "ResultCount": 1,
    "Result": {
        "SessionID": "sess_abc123",
        "Username": "alice",
        "ConnectedAt": "2024-01-20T10:00:00Z",
        "LastActivity": "2024-01-20T10:30:00Z",
        "IPAddress": "192.168.1.100",
        "State": "active",
        "QueriesExecuted": 42,
        "DatabaseContext": "myapp",
        "Permissions": ["Read", "Write"],
        "Roles": ["Data-Writer"]
    }
}
```

---

## SHOW MIGRATIONS

Lists all migrations for a specific database with their version numbers, status, and metadata.

### Syntax

```sql
SHOW MIGRATIONS FOR "database_name" [WHERE Status = "value"];
```

### Components

| Component | Description | Required |
|-----------|-------------|----------|
| **database_name** | Name of database to show migrations for | ✅ Yes |
| [**WHERE clause**](#where-filtering) | Filter migrations by status | ❌ No |

### Prerequisites

⚠️ **Active Database Required:** You must select a database with `USE` before running this command.

### Examples

#### List All Migrations

```sql
USE "production";
SHOW MIGRATIONS FOR "production";
```

#### Filter by Status

```sql
USE "production";

-- Show only pending migrations
SHOW MIGRATIONS FOR "production" WHERE Status = "PENDING";

-- Show only applied migrations
SHOW MIGRATIONS FOR "production" WHERE Status = "APPLIED";

-- Show failed migrations
SHOW MIGRATIONS FOR "production" WHERE Status = "FAILED";
```

### Response Format

```json
{
    "status": "success",
    "database": "production",
    "currentVersion": 5,
    "migrations": [
        {
            "Version": 1,
            "Status": "APPLIED",
            "Description": "Initial schema",
            "CreatedAt": "2024-01-10T10:00:00Z",
            "AppliedAt": "2024-01-10T10:05:00Z",
            "CommandCount": 3
        },
        {
            "Version": 2,
            "Status": "APPLIED",
            "Description": "Add user profiles",
            "CreatedAt": "2024-01-11T14:00:00Z",
            "AppliedAt": "2024-01-11T14:02:00Z",
            "CommandCount": 5
        },
        {
            "Version": 3,
            "Status": "PENDING",
            "Description": "Add product categories",
            "CreatedAt": "2024-01-12T09:00:00Z",
            "CommandCount": 2
        }
    ]
}
```

### Migration States

| State | Description |
|-------|-------------|
| **PENDING** | Migration created but not yet applied |
| **APPLIED** | Migration successfully executed |
| **FAILED** | Migration execution failed |
| **ROLLED_BACK** | Migration was applied then rolled back |

### WHERE Filtering

Filter migrations by status:

```sql
-- Only pending migrations
SHOW MIGRATIONS FOR "myapp" WHERE Status = "PENDING";

-- Only applied migrations
SHOW MIGRATIONS FOR "myapp" WHERE Status = "APPLIED";

-- Failed migrations
SHOW MIGRATIONS FOR "myapp" WHERE Status = "FAILED";
```

### Use Cases

See the [MIGRATION Commands](./MISC_commands.md#show-migrations) section in MISC_commands.md for complete documentation.

---

## SHOW RATE LIMIT

Displays rate limiting statistics and status.

### Syntax

```sql
SHOW RATE LIMIT;
```

### Examples

```sql
SHOW RATE LIMIT;
```

### Response Format

```json
{
    "command": "SHOW RATE LIMIT",
    "message": "Rate limiting statistics",
    "status": "success",
    "data": {
        "note": "Rate limiting is active. Use server logs for detailed statistics.",
        "description": "This command shows rate limiting is enabled and protecting the server from abuse."
    }
}
```

### Behavior

The command confirms that rate limiting is active on the server. Detailed statistics are available in server logs.

### Use Cases

#### Verify Rate Limiting Active

```sql
-- Check if rate limiting is enabled
SHOW RATE LIMIT;
-- Response confirms it's active
```

#### System Health Check

```sql
-- Part of system diagnostics
SHOW RATE LIMIT;
-- Verify protection is in place
```

### Best Practices

✅ **DO:**
- Use to verify rate limiting is active
- Check server logs for detailed statistics
- Include in health check scripts

❌ **DON'T:**
- Don't rely solely on this command for rate limit monitoring
- Don't expect detailed statistics in response (check logs)

---

## Quick Reference

### Database Commands

| Command | Description | Requires USE? | Output |
|---------|-------------|---------------|--------|
| `SHOW DATABASES;` | List all databases | ❌ No | Array of database names |
| `SHOW BUNDLES;` | List bundles in current database | ✅ Yes | Array of bundle metadata |
| `SHOW BUNDLES FOR "db";` | List bundles in specific database | ❌ No | Array of bundle metadata |
| `SHOW BUNDLE "name";` | Show bundle details | ✅ Yes | Bundle schema, indexes, relationships |

### User & Session Commands

| Command | Description | Requires USE? | Status |
|---------|-------------|---------------|--------|
| `SHOW USERS;` | List all users | ❌ No | ✅ Implemented |
| `SHOW SESSIONS;` | List active sessions | ❌ No | ⚠️ Partial |
| `SHOW SESSION id;` | Show session details | ❌ No | ⚠️ Partial |

### System Commands

| Command | Description | Requires USE? | Status |
|---------|-------------|---------------|--------|
| `SHOW MIGRATIONS FOR "db";` | List migrations | ✅ Yes | ✅ Implemented |
| `SHOW RATE LIMIT;` | Show rate limit status | ❌ No | ✅ Implemented |

### Common Workflows

#### Explore New Database

```sql
-- 1. List databases
SHOW DATABASES;

-- 2. Switch to database
USE "myapp";

-- 3. List bundles
SHOW BUNDLES;

-- 4. Inspect bundle structure
SHOW BUNDLE "users";

-- 5. Query data
SELECT * FROM BUNDLE "users";
```

#### Compare Databases

```sql
-- Check bundles in multiple databases
SHOW BUNDLES FOR "development";
SHOW BUNDLES FOR "staging";
SHOW BUNDLES FOR "production";

-- Compare results
```

#### User Administration

```sql
-- List users
SHOW USERS;

-- Create new user
CREATE USER "newuser" WITH PASSWORD 'Pass123!';

-- Verify creation
SHOW USERS;

-- Grant permissions
GRANT ROLE "Data-Writer" TO USER "newuser";
```

### Best Practices Summary

✅ **DO:**
- Use `SHOW DATABASES` before `USE` to verify database exists
- Use `SHOW BUNDLES` to explore database schema
- Use `SHOW BUNDLE` to verify bundle structure before adding documents
- Use `SHOW BUNDLES FOR` for cross-database inspection
- Use `SHOW USERS` to audit user accounts
- Check logs for detailed information (sessions, rate limits)

❌ **DON'T:**
- Don't forget to `USE` database before `SHOW BUNDLES`
- Don't assume database/bundle names are case-insensitive
- Don't expose `SHOW USERS` to non-admin users
- Don't rely on `SHOW SESSIONS` until fully implemented
- Don't ignore catalog consistency warnings

---

**End of SHOW Commands Documentation**
