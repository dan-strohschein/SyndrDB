# 🔧 SyndrDB Miscellaneous Commands

This document covers database management and administrative commands in SyndrDB. These commands handle database creation, context switching, backup/restore operations, migrations, and query analysis.

---

## 📑 Table of Contents

1. [CREATE DATABASE](#create-database)
2. [USE DATABASE](#use-database)
3. [BACKUP DATABASE](#backup-database)
4. [RESTORE DATABASE](#restore-database)
5. [MIGRATION Commands](#migration-commands)
   - [START MIGRATION](#start-migration)
   - [SHOW MIGRATIONS](#show-migrations)
   - [APPLY MIGRATION](#apply-migration)
   - [APPLY ROLLBACK](#apply-rollback)
   - [VALIDATE MIGRATION](#validate-migration)
   - [VALIDATE ROLLBACK](#validate-rollback)
6. [EXPLAIN (Coming Soon)](#explain-coming-soon)

---

## CREATE DATABASE

Creates a new database in the SyndrDB system.

### Syntax

```sql
CREATE DATABASE "database_name"
```

### Components

| Component | Description | Required |
|-----------|-------------|----------|
| **database_name** | Name of the database to create | ✅ Yes |

### Naming Rules

Database names must follow these rules:
- ✅ Start with a letter (a-z, A-Z)
- ✅ Can contain alphanumeric characters (a-z, A-Z, 0-9)
- ✅ Can contain underscores (`_`) and hyphens (`-`)
- ❌ Cannot start with numbers or special characters
- ❌ Cannot contain spaces (use underscores instead)

### Examples

#### ✅ Valid Database Names

```sql
-- Simple database name
CREATE DATABASE "MyApp"

-- Database with underscores
CREATE DATABASE "my_application_db"

-- Database with hyphens
CREATE DATABASE "app-production"

-- Alphanumeric with version
CREATE DATABASE "AppV2"
```

#### ❌ Invalid Database Names

```sql
-- Cannot start with number
CREATE DATABASE "2024_app"  -- Error!

-- Cannot contain spaces
CREATE DATABASE "My App"  -- Error!

-- Must use quotes
CREATE DATABASE MyApp  -- Error! (quotes required)
```

### Permissions

- **Required Permission:** `Admin`
- Only users with Admin permissions can create databases
- When user authentication is implemented, this will be enforced

### Behavior

1. **Validation:** Checks if database name is valid according to naming rules
2. **Existence Check:** Verifies database doesn't already exist
3. **Creation:** Creates database directory structure and metadata
4. **Catalog Registration:** Registers database in system catalog
5. **Confirmation:** Returns success message with database name

### Error Cases

| Error | Cause | Solution |
|-------|-------|----------|
| `database already exists` | Database with that name exists | Use a different name or drop existing database |
| `invalid database name` | Name violates naming rules | Follow naming rules (start with letter, alphanumeric + _ -) |
| `catalog registration failed` | Database created but not registered | Check system catalog integrity |

### Best Practices

✅ **DO:**
- Use descriptive names that reflect the database purpose
- Use consistent naming conventions (e.g., snake_case or kebab-case)
- Include environment in name for multiple deployments (e.g., "app-dev", "app-prod")

❌ **DON'T:**
- Don't use overly long names (keep it under 50 characters)
- Don't use special characters beyond `_` and `-`
- Don't create databases without a clear purpose

### Complete Example

```sql
-- Create a new application database
CREATE DATABASE "user_management_system"
-- Response: Database 'user_management_system' created successfully.

-- Attempt to create duplicate (will fail)
CREATE DATABASE "user_management_system"
-- Error: database 'user_management_system' already exists

-- Create environment-specific databases
CREATE DATABASE "analytics-dev"
CREATE DATABASE "analytics-staging"
CREATE DATABASE "analytics-prod"
```

---

## USE DATABASE

Switches the active database context for subsequent operations. All commands after `USE` will execute against the specified database.

### Syntax

```sql
USE "database_name";
```

### Components

| Component | Description | Required |
|-----------|-------------|----------|
| **database_name** | Name of the database to switch to | ✅ Yes |

### Examples

#### Basic Usage

```sql
-- Switch to a database
USE "MyApp";

-- Now all commands use MyApp database
CREATE BUNDLE "Users" WITH FIELDS (
    {"id", STRING, true, true},
    {"name", STRING, true, false}
);
```

#### Switching Between Databases

```sql
-- Start with one database
USE "analytics-dev";
SELECT * FROM BUNDLE "Events";

-- Switch to another database
USE "analytics-prod";
SELECT * FROM BUNDLE "Events";  -- Now querying prod database
```

### Behavior

1. **Validation:** Checks if database exists in loaded databases
2. **Catalog Check:** Verifies database is registered in system catalog
3. **Context Switch:** Sets the active database for the session
4. **Confirmation:** Returns success message

### Error Cases

| Error | Cause | Solution |
|-------|-------|----------|
| `database not found` | Database doesn't exist or isn't loaded | Use `SHOW DATABASES` to see available databases |
| `catalog verification failed` | Database exists but not in catalog | Check catalog integrity |
| `invalid syntax` | Missing quotes or incorrect format | Use format: `USE "database_name";` |

### Database Context

The `USE` command sets the **session-level** database context:
- All subsequent bundle/document operations use this database
- The context persists until changed by another `USE` command
- Each connection/session maintains its own database context

### Best Practices

✅ **DO:**
- Always use `USE` before working with bundles or documents
- Verify you're in the correct database before destructive operations
- Use `SHOW DATABASES` to list available databases

❌ **DON'T:**
- Don't assume you're in the right database - always verify
- Don't switch databases in the middle of a transaction (when transactions are implemented)

### Complete Example

```sql
-- List available databases
SHOW DATABASES;

-- Switch to development database
USE "app-dev";

-- Create some test data
ADD DOCUMENT TO BUNDLE "Users" 
WITH VALUES ({"id": "1", "name": "Test User"});

-- Switch to production database
USE "app-prod";

-- Now working with production data
SELECT * FROM BUNDLE "Users";
```

---

## BACKUP DATABASE

Creates a compressed backup archive of a database including all bundles, indexes, documents, and metadata.

### Syntax

```sql
BACKUP DATABASE "database_name" TO "backup_path" [WITH options]
```

### Components

| Component | Description | Required |
|-----------|-------------|----------|
| **database_name** | Name of the database to backup | ✅ Yes |
| **backup_path** | Path where backup file will be saved | ✅ Yes |
| [**WITH options**](#backup-options) | Optional backup configuration | ❌ No |

### Backup Path

The backup path can be:
- **Relative:** Saved to configured backup directory (default: `./backups`)
- **Absolute:** Saved to exact path specified
- **Extension:** Automatically adds `.sdb` extension if not provided

### Backup Options

Options are specified in the `WITH` clause:

| Option | Values | Default | Description |
|--------|--------|---------|-------------|
| **COMPRESSION** | `'gzip'`, `'zstd'`, `'none'` | `'gzip'` | Compression algorithm |
| **INCLUDE_INDEXES** | `true`, `false` | `true` | Include index files in backup |

#### COMPRESSION

Controls the compression algorithm used for the backup archive:
- **`gzip`:** Standard compression, good compatibility
- **`zstd`:** Modern compression, better ratio and speed
- **`none`:** No compression (fastest, largest files)

#### INCLUDE_INDEXES

Controls whether index files are included in the backup:
- **`true`:** Backup includes all index files (faster restore)
- **`false`:** Indexes excluded (smaller backup, slower restore)

### Examples

#### Basic Backup

```sql
-- Simple backup with defaults (gzip compression, includes indexes)
BACKUP DATABASE "MyApp" TO "myapp_backup.sdb"
-- Saves to: ./backups/myapp_backup.sdb
```

#### Absolute Path Backup

```sql
-- Backup to specific location
BACKUP DATABASE "analytics" TO "/backups/production/analytics_2024_01_15.sdb"
```

#### Custom Compression

```sql
-- Use zstd compression for better ratio
BACKUP DATABASE "BigData" TO "bigdata_backup.sdb" 
WITH COMPRESSION = 'zstd'

-- No compression for fastest backup
BACKUP DATABASE "SmallDB" TO "small_backup.sdb" 
WITH COMPRESSION = 'none'
```

#### Exclude Indexes

```sql
-- Smaller backup without indexes (will rebuild on restore)
BACKUP DATABASE "TempData" TO "temp_backup.sdb" 
WITH INCLUDE_INDEXES = false
```

#### Multiple Options

```sql
-- Combine multiple options
BACKUP DATABASE "Production" TO "prod_backup.sdb" 
WITH COMPRESSION = 'zstd', INCLUDE_INDEXES = true
```

### Backup Process

The backup operation performs these steps:

1. **Validation:** Checks if database exists
2. **Checkpoint:** Executes checkpoint to ensure data consistency
3. **File Collection:** Gathers all database files (bundles, indexes, documents)
4. **Checksum Calculation:** Computes CRC checksums for integrity validation
5. **Metadata Copy:** Copies Primary database metadata (Databases, Bundles)
6. **Archive Creation:** Creates compressed tar archive (.sdb file)
7. **Validation:** Verifies backup integrity after creation

### Backup Contents

Each backup archive contains:
- **Bundle Files:** All `.bnd` bundle definition files
- **Document Files:** All document data files
- **Index Files:** Hash and B-Tree index files (if `INCLUDE_INDEXES = true`)
- **Metadata:** Primary database Databases and Bundles documents
- **Manifest:** `manifest.json` with metadata and file checksums

### Permissions

- **Required Permission:** `Admin`
- Only users with Admin permissions can create backups
- TODO: This will be enforced when user authentication is implemented

### Error Cases

| Error | Cause | Solution |
|-------|-------|----------|
| `database not found` | Database doesn't exist | Use `SHOW DATABASES` to verify database name |
| `invalid backup path` | Path is invalid or inaccessible | Check path permissions and directory existence |
| `invalid compression type` | Unknown compression value | Use 'gzip', 'zstd', or 'none' |
| `backup failed` | File system error or insufficient space | Check disk space and permissions |

### Best Practices

✅ **DO:**
- Include timestamps in backup filenames (e.g., `db_2024_01_15.sdb`)
- Use `zstd` compression for production backups (best ratio)
- Keep backups in a separate location from data files
- Test restore procedures regularly
- Include indexes for production databases

❌ **DON'T:**
- Don't backup to the same disk as the database
- Don't use `none` compression for large databases
- Don't forget to verify backups periodically
- Don't rely on a single backup - maintain rotation

### Complete Example

```sql
-- Daily backup with timestamp
BACKUP DATABASE "production" TO "prod_2024_01_15.sdb" 
WITH COMPRESSION = 'zstd', INCLUDE_INDEXES = true
-- Response: Database 'production' backed up successfully to './backups/prod_2024_01_15.sdb' in 2.5s

-- Quick backup without indexes (smaller size)
BACKUP DATABASE "development" TO "dev_quick.sdb" 
WITH COMPRESSION = 'gzip', INCLUDE_INDEXES = false

-- Full backup with absolute path
BACKUP DATABASE "analytics" TO "/mnt/backups/analytics_full.sdb" 
WITH COMPRESSION = 'zstd', INCLUDE_INDEXES = true
```

---

## RESTORE DATABASE

Restores a database from a backup archive created by the `BACKUP DATABASE` command.

### Syntax

```sql
RESTORE DATABASE FROM "backup_path" AS "database_name" [WITH options]
```

### Components

| Component | Description | Required |
|-----------|-------------|----------|
| **backup_path** | Path to the backup file (.sdb) | ✅ Yes |
| **database_name** | Name for the restored database | ✅ Yes |
| [**WITH options**](#restore-options) | Optional restore configuration | ❌ No |

### Restore Options

Options are specified in the `WITH` clause:

| Option | Values | Default | Description |
|--------|--------|---------|-------------|
| **FORCE** | `true`, `false` | `false` | Overwrite existing database |

#### FORCE

Controls whether to overwrite an existing database:
- **`false`:** Restore fails if database already exists (safe default)
- **`true`:** Overwrites existing database with backup (destructive)

### Examples

#### Basic Restore

```sql
-- Restore backup to new database
RESTORE DATABASE FROM "myapp_backup.sdb" AS "MyApp"
```

#### Restore with Absolute Path

```sql
-- Restore from specific location
RESTORE DATABASE FROM "/backups/production/analytics_2024_01_15.sdb" AS "analytics"
```

#### Force Overwrite

```sql
-- Overwrite existing database (DANGEROUS!)
RESTORE DATABASE FROM "prod_backup.sdb" AS "production" 
WITH FORCE = true
```

#### Restore to Different Name

```sql
-- Restore production backup to test environment
RESTORE DATABASE FROM "prod_2024_01_15.sdb" AS "production-test"
-- Original database name in backup: "production"
-- Restored as: "production-test"
```

### Restore Process

The restore operation performs these steps:

1. **Validation:** Checks if backup file exists and is readable
2. **Extraction:** Extracts backup archive to temporary directory
3. **Checksum Verification:** Verifies all file CRC checksums for integrity
4. **Compatibility Check:** Ensures backup is compatible with server version
5. **Directory Creation:** Creates database directory structure
6. **File Copy:** Copies all files from backup to database directory
7. **Database Creation:** Creates database in system (LOCKED state)
8. **Metadata Restore:** Restores Primary database metadata
9. **Validation:** Validates restored database structure
10. **Rollback:** If any step fails, entire operation is rolled back

### Database Lock State

⚠️ **IMPORTANT:** After restore, the database is in **LOCKED** state:
- **LOCKED databases are read-only**
- Prevents accidental writes to freshly restored data
- Must manually unlock before making changes
- Use `UNLOCK DATABASE "name"` to enable writes

### Permissions

- **Required Permission:** `Admin`
- Only users with Admin permissions can restore databases
- TODO: This will be enforced when user authentication is implemented

### Error Cases

| Error | Cause | Solution |
|-------|-------|----------|
| `backup file not found` | Backup path doesn't exist | Verify backup file path |
| `database already exists` | Database name is in use | Use different name or `WITH FORCE = true` |
| `checksum verification failed` | Backup file is corrupted | Use different backup file |
| `incompatible backup version` | Backup from different server version | Check backup metadata |
| `restore failed` | File system error | Check disk space and permissions |

### Best Practices

✅ **DO:**
- Always verify backup file exists before restore
- Use `FORCE = false` (default) to prevent accidental overwrites
- Test restores in non-production environment first
- Unlock database only after verifying restore success
- Keep original database if possible (restore to different name)

❌ **DON'T:**
- Don't use `FORCE = true` without verifying you want to overwrite
- Don't restore directly to production without testing
- Don't forget to unlock database after restore
- Don't assume restore will work - always test

### Complete Example

```sql
-- Restore production backup to test environment
RESTORE DATABASE FROM "/backups/prod_2024_01_15.sdb" AS "production-test"
-- Response: Database 'production-test' restored successfully from '/backups/prod_2024_01_15.sdb' in 3.2s. 
--           Database is LOCKED - use UNLOCK DATABASE to enable writes.

-- Verify the restore
USE "production-test";
SHOW BUNDLES;

-- If verification passes, unlock the database
UNLOCK DATABASE "production-test";

-- Now you can use the restored database
ADD DOCUMENT TO BUNDLE "Users" WITH VALUES (...);
```

#### Disaster Recovery Example

```sql
-- Production database is corrupted, restore from last known good backup
RESTORE DATABASE FROM "/backups/prod_2024_01_14.sdb" AS "production-recovery"

-- Verify the data
USE "production-recovery";
SELECT COUNT(*) FROM BUNDLE "Users";
SELECT COUNT(*) FROM BUNDLE "Orders";

-- If data looks good, can rename and use
-- (rename operations TBD in future SyndrDB version)
```

---

## MIGRATION Commands

SyndrDB's migration system provides version control for database schemas. Migrations track schema changes over time, enable rollbacks, and ensure consistency across environments.

### Migration System Overview

The migration system provides:
- 📝 **Version Control:** Track schema changes with version numbers
- ✅ **Validation:** 5-phase validation pipeline before applying migrations
- 🔄 **Rollback:** Revert to previous versions with auto-generated DOWN commands
- 🔒 **Safety:** Strict ordering, checksum verification, performance monitoring
- 📊 **Reporting:** Detailed validation reports and performance metrics

### Migration States

| State | Description |
|-------|-------------|
| **PENDING** | Migration created but not yet applied |
| **APPLIED** | Migration successfully executed |
| **FAILED** | Migration execution failed |
| **ROLLED_BACK** | Migration was applied then rolled back |

### Configuration Settings

| Setting | Default | Description |
|---------|---------|-------------|
| `MaxMigrationCommands` | 1000 | Maximum commands per migration |
| `MigrationPerformanceThreshold` | 1.0s | Performance warning threshold |
| `MigrationTimeoutSeconds` | 300 | Execution timeout (5 minutes) |
| `RequireExplicitDownCommands` | false | Require manual DOWN commands |

---

## START MIGRATION

Creates a new migration with schema change commands. The migration is validated but not applied until explicitly executed with `APPLY MIGRATION`.

### Syntax

```sql
START MIGRATION [WITH DESCRIPTION "description"] 
    command1;
    command2;
    ...
COMMIT
```

### Components

| Component | Description | Required |
|-----------|-------------|----------|
| [**WITH DESCRIPTION**](#description) | Human-readable migration description | ❌ No |
| **commands** | SyndrDB commands to execute | ✅ Yes |
| **COMMIT** | Marks end of migration definition | ✅ Yes |

### Description

Optional description field:
- Maximum 500 characters
- Should describe what the migration does
- Helpful for team communication and audit trails

### Supported Commands

Migrations can contain these commands:
- `CREATE BUNDLE`
- `UPDATE BUNDLE` (rename, add/remove fields, add/remove relationships)
- `DROP BUNDLE`
- `CREATE INDEX`
- `DROP INDEX`
- `ADD DOCUMENT` (for seed data)

### Examples

#### Simple Migration

```sql
-- Create a new bundle
START MIGRATION WITH DESCRIPTION "Add Users bundle"
    CREATE BUNDLE "Users" WITH FIELDS (
        {"id", STRING, true, true},
        {"email", STRING, true, true},
        {"created_at", DATETIME, true, false}
    );
COMMIT
```

#### Migration with Multiple Commands

```sql
-- Add multiple bundles with relationships
START MIGRATION WITH DESCRIPTION "Add e-commerce schema"
    CREATE BUNDLE "Products" WITH FIELDS (
        {"id", STRING, true, true},
        {"name", STRING, true, false},
        {"price", FLOAT, true, false}
    );
    
    CREATE BUNDLE "Orders" WITH FIELDS (
        {"id", STRING, true, true},
        {"user_id", STRING, true, false},
        {"total", FLOAT, true, false}
    );
    
    UPDATE BUNDLE "Orders"
    ADD RELATIONSHIP "product_orders"
    FROM BUNDLE "Orders" TO BUNDLE "Products";
COMMIT
```

#### Migration with Seed Data

```sql
-- Create bundle and add initial data
START MIGRATION WITH DESCRIPTION "Add Roles with defaults"
    CREATE BUNDLE "Roles" WITH FIELDS (
        {"id", STRING, true, true},
        {"name", STRING, true, true},
        {"permissions", JSON, true, false}
    );
    
    ADD DOCUMENT TO BUNDLE "Roles" WITH VALUES (
        {"id": "admin", "name": "Administrator", "permissions": ["*"]}
    );
    
    ADD DOCUMENT TO BUNDLE "Roles" WITH VALUES (
        {"id": "user", "name": "Regular User", "permissions": ["read"]}
    );
COMMIT
```

#### Migration Without Description

```sql
-- Description is optional
START MIGRATION
    UPDATE BUNDLE "Users"
    ADD FIELD {"phone", STRING, false, false};
COMMIT
```

### Behavior

1. **Database Context:** Requires active database (use `USE "database"` first)
2. **Command Parsing:** Splits commands by semicolon
3. **Validation:** Basic syntax validation (full validation with `VALIDATE MIGRATION`)
4. **Version Assignment:** Auto-assigns next version number
5. **State:** Created in `PENDING` state
6. **DOWN Commands:** Auto-generated if possible (or empty if `RequireExplicitDownCommands = true`)

### Error Cases

| Error | Cause | Solution |
|-------|-------|----------|
| `no active database` | No database selected | Use `USE "database"` first |
| `migration must contain at least one command` | Empty command body | Add at least one command |
| `description exceeds 500 character limit` | Description too long | Shorten description |
| `migration service not initialized` | System error | Check server configuration |

### Auto-Generated DOWN Commands

The migration system attempts to auto-generate rollback commands:
- `CREATE BUNDLE` → `DROP BUNDLE`
- `DROP BUNDLE` → `CREATE BUNDLE` (if reversible)
- `ADD FIELD` → `REMOVE FIELD`
- `UPDATE BUNDLE SET NAME` → `UPDATE BUNDLE SET NAME` (reverse)

If commands cannot be reversed automatically:
- DOWN commands list will be empty
- Set `RequireExplicitDownCommands = true` to enforce manual DOWN commands

### Best Practices

✅ **DO:**
- Always include descriptive migration descriptions
- Group related changes in single migration
- Test migrations in development first
- Keep migrations focused and atomic
- Validate before applying (`VALIDATE MIGRATION`)

❌ **DON'T:**
- Don't mix schema changes with large data imports
- Don't create migrations with hundreds of commands (use batching)
- Don't skip migration descriptions for production
- Don't apply migrations without validation

### Complete Example

```sql
-- Select database
USE "production";

-- Create migration for user profile feature
START MIGRATION WITH DESCRIPTION "Add user profiles and preferences"
    -- Add profile bundle
    CREATE BUNDLE "UserProfiles" WITH FIELDS (
        {"user_id", STRING, true, true},
        {"bio", TEXT, false, false},
        {"avatar_url", STRING, false, false},
        {"created_at", DATETIME, true, false}
    );
    
    -- Add preferences bundle
    CREATE BUNDLE "UserPreferences" WITH FIELDS (
        {"user_id", STRING, true, true},
        {"theme", STRING, true, false},
        {"notifications", BOOLEAN, true, false}
    );
    
    -- Link to Users bundle
    UPDATE BUNDLE "Users"
    ADD RELATIONSHIP "user_profile"
    FROM BUNDLE "Users" TO BUNDLE "UserProfiles";
    
    UPDATE BUNDLE "Users"
    ADD RELATIONSHIP "user_preferences"
    FROM BUNDLE "Users" TO BUNDLE "UserPreferences";
COMMIT

-- Response: Migration created successfully
--           {
--               "status": "success",
--               "message": "Migration created successfully",
--               "migration": {
--                   "Version": 1,
--                   "Status": "PENDING",
--                   "Description": "Add user profiles and preferences",
--                   ...
--               }
--           }
```

---

## SHOW MIGRATIONS

Lists all migrations for the current database with their version numbers, status, and metadata.

### Syntax

```sql
SHOW MIGRATIONS FOR "database_name" [WHERE field = "value"]
```

### Components

| Component | Description | Required |
|-----------|-------------|----------|
| **database_name** | Name of database to show migrations for | ✅ Yes |
| [**WHERE clause**](#where-filtering) | Filter migrations by field | ❌ No |

### WHERE Filtering

Filter migrations using a WHERE clause:

```sql
SHOW MIGRATIONS FOR "MyApp" WHERE Status = "PENDING"
SHOW MIGRATIONS FOR "MyApp" WHERE Status = "APPLIED"
```

### Examples

#### List All Migrations

```sql
-- Show all migrations for database
USE "production";
SHOW MIGRATIONS FOR "production"
```

#### Filter by Status

```sql
-- Show only pending migrations
SHOW MIGRATIONS FOR "production" WHERE Status = "PENDING"

-- Show only applied migrations
SHOW MIGRATIONS FOR "production" WHERE Status = "APPLIED"

-- Show failed migrations
SHOW MIGRATIONS FOR "production" WHERE Status = "FAILED"
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

### Migration Fields

Each migration includes:
- **Version:** Sequential version number
- **Status:** PENDING, APPLIED, FAILED, or ROLLED_BACK
- **Description:** Human-readable description
- **CreatedAt:** When migration was created
- **AppliedAt:** When migration was applied (if APPLIED)
- **CommandCount:** Number of commands in migration
- **CreatedBy:** User who created migration

### Behavior

1. **Database Context:** Requires active database
2. **Ordering:** Returns migrations ordered by version (ascending)
3. **Current Version:** Includes current database version in response
4. **Filtering:** Applies WHERE filter if specified

### Error Cases

| Error | Cause | Solution |
|-------|-------|----------|
| `no active database` | No database selected | Use `USE "database"` first |
| `migration service not initialized` | System error | Check server configuration |

### Best Practices

✅ **DO:**
- Review all migrations before applying to production
- Check `currentVersion` to understand database state
- Use WHERE filters to find specific migrations
- Verify migration descriptions match expected changes

❌ **DON'T:**
- Don't assume migrations are in order (always check Version field)
- Don't apply migrations without reviewing commands

### Complete Example

```sql
-- Switch to database
USE "production";

-- Show all migrations
SHOW MIGRATIONS FOR "production";

-- Output shows current version is 2, with 1 pending migration
-- {
--     "currentVersion": 2,
--     "migrations": [
--         {"Version": 1, "Status": "APPLIED", "Description": "Initial schema"},
--         {"Version": 2, "Status": "APPLIED", "Description": "Add user profiles"},
--         {"Version": 3, "Status": "PENDING", "Description": "Add categories"}
--     ]
-- }

-- Show only pending migrations to see what needs to be applied
SHOW MIGRATIONS FOR "production" WHERE Status = "PENDING";
-- Shows only version 3

-- Show only applied migrations for audit trail
SHOW MIGRATIONS FOR "production" WHERE Status = "APPLIED";
-- Shows versions 1 and 2
```

---

## APPLY MIGRATION

Executes a pending migration, applying all its commands to the database. This permanently modifies the database schema.

### Syntax

```sql
APPLY MIGRATION WITH VERSION <number> [FORCE]
```

### Components

| Component | Description | Required |
|-----------|-------------|----------|
| **VERSION number** | Version number of migration to apply | ✅ Yes |
| [**FORCE**](#force-flag) | Skip validation warnings | ❌ No |

### FORCE Flag

The `FORCE` flag controls warning behavior:
- **Without FORCE:** Validation warnings will block execution
- **With FORCE:** Warnings are logged but execution continues

⚠️ **WARNING:** Using `FORCE` can lead to data loss or schema corruption. Only use when you understand the warnings and accept the risks.

### Examples

#### Apply Migration (Safe)

```sql
-- Apply next migration
APPLY MIGRATION WITH VERSION 3
```

#### Apply with Force (Dangerous)

```sql
-- Apply migration even if validation warnings exist
APPLY MIGRATION WITH VERSION 3 FORCE
```

### Execution Process

1. **Validation:** Runs 5-phase validation pipeline
2. **Status Check:** Ensures migration is in PENDING state
3. **Ordering Check:** Verifies sequential application (no gaps)
4. **Command Execution:** Executes each command in order
5. **Checksum Calculation:** Calculates migration checksum
6. **Status Update:** Marks migration as APPLIED
7. **Metadata Update:** Updates database version

### Validation Phases

Before execution, the migration undergoes 5 validation phases:

| Phase | Check | Blocking |
|-------|-------|----------|
| **1. Syntax** | Command syntax is valid | ✅ Yes |
| **2. Semantics** | Commands reference existing objects | ✅ Yes |
| **3. Constraints** | No constraint violations | ✅ Yes |
| **4. Performance** | Estimated execution time | ⚠️ Warning |
| **5. Reversibility** | Can be rolled back | ⚠️ Warning |

### Error Cases

| Error | Cause | Solution |
|-------|-------|----------|
| `no active database` | No database selected | Use `USE "database"` first |
| `migration not found` | Invalid version number | Use `SHOW MIGRATIONS` to see versions |
| `migration not in PENDING state` | Already applied or failed | Check migration status |
| `validation failed` | Syntax, semantic, or constraint errors | Review validation report, fix migration |
| `performance warning` | Estimated slow execution | Review commands, use FORCE if acceptable |
| `not reversible` | Cannot auto-generate DOWN commands | Manually create DOWN commands or use FORCE |

### Sequential Application Enforcement

Migrations **must be applied in order**:
- Cannot skip versions (e.g., can't apply V3 if V2 is pending)
- Prevents schema inconsistencies
- Ensures all environments follow same upgrade path

### Best Practices

✅ **DO:**
- Always validate migration first (`VALIDATE MIGRATION`)
- Review validation report before applying
- Apply migrations in sequence (don't skip versions)
- Test in development/staging before production
- Have rollback plan ready (`VALIDATE ROLLBACK`)
- Monitor performance during application

❌ **DON'T:**
- Don't use FORCE without understanding warnings
- Don't apply migrations during peak traffic
- Don't apply untested migrations to production
- Don't apply multiple migrations simultaneously

### Complete Example

```sql
-- Switch to database
USE "production";

-- Show pending migrations
SHOW MIGRATIONS FOR "production" WHERE Status = "PENDING";
-- Shows version 3 is pending

-- Validate before applying
VALIDATE MIGRATION WITH VERSION 3;
-- Review validation report...
-- {
--     "syntaxPhase": {"passed": true},
--     "semanticPhase": {"passed": true},
--     "constraintPhase": {"passed": true},
--     "performancePhase": {"passed": true, "warnings": []},
--     "reversibilityPhase": {"passed": true}
-- }

-- Validation passed, safe to apply
APPLY MIGRATION WITH VERSION 3;
-- Response: {
--     "status": "success",
--     "message": "Migration version 3 applied successfully",
--     "version": 3,
--     "force": false
-- }

-- Verify migration was applied
SHOW MIGRATIONS FOR "production";
-- Shows version 3 now has Status = "APPLIED"
```

#### Force Application Example (Use with Caution!)

```sql
-- Validate migration
VALIDATE MIGRATION WITH VERSION 4;
-- Report shows performance warning:
-- {
--     "performancePhase": {
--         "passed": false,
--         "warnings": ["Migration estimated to take 3.5s, exceeds threshold of 1.0s"]
--     }
-- }

-- Review warning - it's a large data import, expected to be slow
-- Decision: Acceptable for off-peak application

-- Apply with FORCE to override warning
APPLY MIGRATION WITH VERSION 4 FORCE;
-- Response: {
--     "status": "success",
--     "message": "Migration version 4 applied successfully",
--     "version": 4,
--     "force": true
-- }
```

---

## APPLY ROLLBACK

Reverts the database to a previous migration version by executing DOWN commands in reverse order.

### Syntax

```sql
APPLY ROLLBACK TO VERSION <number>
```

### Components

| Component | Description | Required |
|-----------|-------------|----------|
| **VERSION number** | Target version to rollback to | ✅ Yes |

### Examples

#### Rollback One Version

```sql
-- Current version: 5, rollback to 4
APPLY ROLLBACK TO VERSION 4
```

#### Rollback Multiple Versions

```sql
-- Current version: 5, rollback to 2
APPLY ROLLBACK TO VERSION 2
-- Executes: V5 DOWN, V4 DOWN, V3 DOWN
```

### Rollback Process

1. **Version Check:** Validates target version < current version
2. **Ordering Enforcement:** Ensures strict reverse order (no skipping)
3. **DOWN Command Execution:** Executes DOWN commands from current to target
4. **Status Updates:** Marks rolled-back migrations as ROLLED_BACK
5. **Version Update:** Sets database version to target

### Rollback Order

Rollbacks execute in **strict reverse order**:
```
Current: V5 → Target: V2
Execution: V5 DOWN → V4 DOWN → V3 DOWN
Result: Database at V2
```

Cannot skip versions - must rollback through each version sequentially.

### DOWN Commands

Rollback requires DOWN commands for each migration:
- **Auto-generated:** Most migrations have auto-generated DOWN commands
- **Manual:** Some migrations require explicit DOWN commands
- **Verification:** Use `VALIDATE ROLLBACK` to check if rollback is possible

### Error Cases

| Error | Cause | Solution |
|-------|-------|----------|
| `no active database` | No database selected | Use `USE "database"` first |
| `invalid target version` | Target >= current version | Can only rollback to lower version |
| `missing DOWN commands` | Migration lacks DOWN commands | Manually create DOWN commands |
| `rollback failed` | DOWN command execution failed | Review error, restore from backup |

### Approval Workflow (Future)

🚧 **TODO:** Approval token validation will be added
- Rollbacks are destructive operations
- Will require approval token from admin
- Prevents accidental rollbacks in production

### Best Practices

✅ **DO:**
- Always validate rollback first (`VALIDATE ROLLBACK`)
- Create database backup before rollback
- Test rollback in development first
- Review DOWN commands before executing
- Have forward migration ready if rollback fails
- Document reason for rollback

❌ **DON'T:**
- Don't rollback without backup
- Don't rollback in production without testing
- Don't rollback during active usage
- Don't assume rollback will work - always validate

### Complete Example

```sql
-- Switch to database
USE "production";

-- Check current version
SHOW MIGRATIONS FOR "production";
-- Current version: 5

-- Validate rollback to version 3
VALIDATE ROLLBACK TO VERSION 3;
-- Review validation report...
-- {
--     "canRollback": true,
--     "migrationsToReverse": [5, 4],
--     "downCommands": {
--         "5": ["DROP BUNDLE \"NewFeature\""],
--         "4": ["UPDATE BUNDLE \"Users\" REMOVE FIELD \"phone\""]
--     }
-- }

-- Validation passed, create backup first
BACKUP DATABASE "production" TO "prod_before_rollback.sdb";

-- Execute rollback
APPLY ROLLBACK TO VERSION 3;
-- Response: {
--     "status": "success",
--     "message": "Database rolled back to version 3 successfully",
--     "targetVersion": 3
-- }

-- Verify rollback
SHOW MIGRATIONS FOR "production";
-- Shows versions 4 and 5 now have Status = "ROLLED_BACK"
-- Current version: 3
```

#### Rollback Failure Recovery

```sql
-- Attempt rollback
APPLY ROLLBACK TO VERSION 2;
-- Error: rollback failed: DOWN command failed: cannot drop bundle "Users" - has relationships

-- Rollback failed! Database may be in inconsistent state
-- Recovery: Restore from backup
RESTORE DATABASE FROM "prod_before_rollback.sdb" AS "production-recovery";

-- Verify restored database
USE "production-recovery";
SHOW MIGRATIONS FOR "production-recovery";
-- Back to version 5 (pre-rollback state)
```

---

## VALIDATE MIGRATION

Runs a comprehensive 5-phase validation pipeline on a pending migration **without executing it**. This is a critical safety check before applying migrations.

### Syntax

```sql
VALIDATE MIGRATION WITH VERSION <number>
```

### Components

| Component | Description | Required |
|-----------|-------------|----------|
| **VERSION number** | Version number of migration to validate | ✅ Yes |

### Examples

```sql
-- Validate a pending migration
VALIDATE MIGRATION WITH VERSION 3
```

### Validation Pipeline

The validation runs 5 phases in sequence:

#### Phase 1: Syntax Validation
- **Purpose:** Verify all commands have valid syntax
- **Checks:** Command structure, keywords, field types, operators
- **Blocking:** ✅ Yes - syntax errors prevent execution
- **Example Errors:**
  - Invalid field type
  - Missing required keyword
  - Malformed command

#### Phase 2: Semantic Validation
- **Purpose:** Verify commands reference existing objects
- **Checks:** Bundle existence, field existence, relationship validity
- **Blocking:** ✅ Yes - semantic errors prevent execution
- **Example Errors:**
  - Bundle doesn't exist
  - Field not found in bundle
  - Relationship references missing bundle

#### Phase 3: Constraint Validation
- **Purpose:** Verify no constraint violations
- **Checks:** Unique constraints, required fields, data type compatibility
- **Blocking:** ✅ Yes - constraint violations prevent execution
- **Example Errors:**
  - Unique constraint violation
  - Required field missing
  - Type mismatch

#### Phase 4: Performance Estimation
- **Purpose:** Estimate execution time and resource usage
- **Checks:** Command count, estimated duration, index rebuilds
- **Blocking:** ⚠️ Warning only - can override with FORCE
- **Example Warnings:**
  - Estimated execution time exceeds threshold (> 1.0s default)
  - Large number of commands (> 1000 default)
  - Index rebuild required

#### Phase 5: Reversibility Check
- **Purpose:** Verify migration can be rolled back
- **Checks:** DOWN commands exist or can be auto-generated
- **Blocking:** ⚠️ Warning only - can override with FORCE
- **Example Warnings:**
  - No DOWN commands and cannot auto-generate
  - Irreversible operations (e.g., DROP with data loss)

### Response Format

```json
{
    "status": "success",
    "message": "Migration version 3 validation complete",
    "report": {
        "migrationVersion": 3,
        "validatedAt": "2024-01-15T10:30:00Z",
        "validatedBy": "system",
        
        "syntaxPhase": {
            "passed": true,
            "errors": []
        },
        
        "semanticPhase": {
            "passed": true,
            "errors": []
        },
        
        "constraintPhase": {
            "passed": true,
            "errors": []
        },
        
        "performancePhase": {
            "passed": true,
            "warnings": [],
            "estimatedDuration": "0.5s",
            "commandCount": 5
        },
        
        "reversibilityPhase": {
            "passed": true,
            "warnings": [],
            "downCommandsGenerated": true
        },
        
        "overallResult": "PASS",
        "canApply": true,
        "canApplyWithForce": true
    }
}
```

### Overall Results

| Result | Meaning | Action |
|--------|---------|--------|
| **PASS** | All phases passed, no warnings | Safe to apply |
| **PASS_WITH_WARNINGS** | All blocking phases passed, warnings exist | Can apply with FORCE |
| **FAIL** | One or more blocking phases failed | Fix errors before applying |

### Error Cases

| Error | Cause | Solution |
|-------|-------|----------|
| `no active database` | No database selected | Use `USE "database"` first |
| `migration not found` | Invalid version number | Use `SHOW MIGRATIONS` to see versions |
| `migration not in PENDING state` | Already applied or failed | Only pending migrations can be validated |

### Best Practices

✅ **DO:**
- **Always validate before applying migrations**
- Review entire validation report, not just overall result
- Pay attention to performance warnings for production
- Validate again if migration commands are modified
- Save validation reports for audit trail

❌ **DON'T:**
- Don't skip validation step
- Don't ignore performance warnings for large databases
- Don't assume validation will pass

### Complete Example

```sql
-- Switch to database
USE "production";

-- Create migration
START MIGRATION WITH DESCRIPTION "Add product reviews"
    CREATE BUNDLE "Reviews" WITH FIELDS (
        {"id", STRING, true, true},
        {"product_id", STRING, true, false},
        {"user_id", STRING, true, false},
        {"rating", INTEGER, true, false},
        {"comment", TEXT, false, false},
        {"created_at", DATETIME, true, false}
    );
    
    UPDATE BUNDLE "Products"
    ADD RELATIONSHIP "product_reviews"
    FROM BUNDLE "Products" TO BUNDLE "Reviews";
    
    CREATE INDEX HASH ON BUNDLE "Reviews" WITH FIELD "product_id";
COMMIT

-- Validation returns version 3

-- Validate before applying
VALIDATE MIGRATION WITH VERSION 3;

-- Response shows all phases passed:
-- {
--     "overallResult": "PASS",
--     "canApply": true,
--     "syntaxPhase": {"passed": true},
--     "semanticPhase": {"passed": true},
--     "constraintPhase": {"passed": true},
--     "performancePhase": {
--         "passed": true,
--         "estimatedDuration": "0.3s",
--         "commandCount": 3
--     },
--     "reversibilityPhase": {
--         "passed": true,
--         "downCommandsGenerated": true
--     }
-- }

-- Validation passed - safe to apply
APPLY MIGRATION WITH VERSION 3;
```

#### Validation Failure Example

```sql
-- Create migration with error
START MIGRATION WITH DESCRIPTION "Add user ratings"
    UPDATE BUNDLE "Users"
    ADD FIELD {"rating", INTEGER, true, false};
    
    -- Error: NonexistentBundle doesn't exist
    UPDATE BUNDLE "NonexistentBundle"
    ADD FIELD {"test", STRING, false, false};
COMMIT

-- Validate migration
VALIDATE MIGRATION WITH VERSION 4;

-- Response shows semantic phase failure:
-- {
--     "overallResult": "FAIL",
--     "canApply": false,
--     "syntaxPhase": {"passed": true},
--     "semanticPhase": {
--         "passed": false,
--         "errors": [
--             "Bundle 'NonexistentBundle' does not exist"
--         ]
--     }
-- }

-- Cannot apply - must fix migration first
```

#### Performance Warning Example

```sql
-- Create migration with many commands
START MIGRATION WITH DESCRIPTION "Import 500 seed records"
    ADD DOCUMENT TO BUNDLE "Products" WITH VALUES (...);
    ADD DOCUMENT TO BUNDLE "Products" WITH VALUES (...);
    -- ... 498 more ADD DOCUMENT commands ...
COMMIT

-- Validate migration
VALIDATE MIGRATION WITH VERSION 5;

-- Response shows performance warning:
-- {
--     "overallResult": "PASS_WITH_WARNINGS",
--     "canApply": false,
--     "canApplyWithForce": true,
--     "performancePhase": {
--         "passed": false,
--         "warnings": [
--             "Migration contains 500 commands, exceeds threshold of 1000",
--             "Estimated execution time 2.5s exceeds threshold of 1.0s"
--         ],
--         "estimatedDuration": "2.5s",
--         "commandCount": 500
--     }
-- }

-- Can apply with FORCE if performance is acceptable
APPLY MIGRATION WITH VERSION 5 FORCE;
```

---

## VALIDATE ROLLBACK

Simulates a rollback operation **without executing it** to verify it can be safely performed. This is critical before running `APPLY ROLLBACK`.

### Syntax

```sql
VALIDATE ROLLBACK TO VERSION <number>
```

### Components

| Component | Description | Required |
|-----------|-------------|----------|
| **VERSION number** | Target version to validate rollback to | ✅ Yes |

### Examples

```sql
-- Validate rollback from current version to version 3
VALIDATE ROLLBACK TO VERSION 3
```

### Validation Process

The validation checks:

1. **Version Validity:** Target version is less than current version
2. **Migration Chain:** All migrations from current to target have DOWN commands
3. **Command Safety:** All DOWN commands are valid and executable
4. **Data Impact:** Estimates data loss from rollback
5. **Dependency Check:** Verifies no dependent objects will be orphaned

### Response Format

```json
{
    "status": "success",
    "message": "Rollback to version 3 validation complete",
    "targetVersion": 3,
    "report": {
        "canRollback": true,
        "currentVersion": 5,
        "targetVersion": 3,
        "migrationsToReverse": [5, 4],
        
        "downCommands": {
            "5": [
                "DROP BUNDLE \"NewFeature\"",
                "UPDATE BUNDLE \"Users\" REMOVE FIELD \"preferences\""
            ],
            "4": [
                "UPDATE BUNDLE \"Users\" REMOVE FIELD \"phone\""
            ]
        },
        
        "dataImpact": {
            "bundlesDropped": ["NewFeature"],
            "fieldsRemoved": ["Users.preferences", "Users.phone"],
            "estimatedDataLoss": "Documents in NewFeature bundle will be deleted"
        },
        
        "warnings": [],
        "errors": []
    }
}
```

### Rollback Verification

The report includes:

| Field | Description |
|-------|-------------|
| **canRollback** | Whether rollback is possible |
| **migrationsToReverse** | List of versions that will be rolled back |
| **downCommands** | DOWN commands for each migration |
| **dataImpact** | Estimate of data that will be lost |
| **warnings** | Non-blocking issues |
| **errors** | Blocking issues that prevent rollback |

### Data Impact Analysis

The validation estimates potential data loss:
- **Bundles Dropped:** Bundles that will be deleted
- **Fields Removed:** Fields that will be removed from bundles
- **Relationship Changes:** Relationships that will be removed
- **Index Drops:** Indexes that will be deleted

### Error Cases

| Error | Cause | Solution |
|-------|-------|----------|
| `no active database` | No database selected | Use `USE "database"` first |
| `invalid target version` | Target >= current version | Can only rollback to lower version |
| `missing DOWN commands` | Migration lacks DOWN commands | Manually create DOWN commands |
| `DOWN command validation failed` | DOWN commands are invalid | Review and fix DOWN commands |

### Best Practices

✅ **DO:**
- **Always validate rollback before executing**
- Review data impact section carefully
- Verify DOWN commands match expected changes
- Test rollback in development first
- Create backup before actual rollback

❌ **DON'T:**
- Don't rollback without validation
- Don't ignore data impact warnings
- Don't assume DOWN commands are correct

### Complete Example

```sql
-- Switch to database
USE "production";

-- Check current state
SHOW MIGRATIONS FOR "production";
-- Current version: 5

-- Validate rollback to version 3
VALIDATE ROLLBACK TO VERSION 3;

-- Response shows validation details:
-- {
--     "canRollback": true,
--     "currentVersion": 5,
--     "targetVersion": 3,
--     "migrationsToReverse": [5, 4],
--     
--     "downCommands": {
--         "5": ["DROP BUNDLE \"Features\""],
--         "4": ["UPDATE BUNDLE \"Users\" REMOVE FIELD \"phone\""]
--     },
--     
--     "dataImpact": {
--         "bundlesDropped": ["Features"],
--         "fieldsRemoved": ["Users.phone"],
--         "estimatedDataLoss": "All documents in Features bundle (approx 150 records), phone field data from Users"
--     },
--     
--     "warnings": [
--         "Rollback will delete Features bundle and all its documents"
--     ]
-- }

-- Review data impact - acceptable for this scenario
-- Create backup before rollback
BACKUP DATABASE "production" TO "prod_before_rollback_v3.sdb";

-- Execute rollback
APPLY ROLLBACK TO VERSION 3;
```

#### Validation Failure Example

```sql
-- Validate rollback
VALIDATE ROLLBACK TO VERSION 2;

-- Response shows cannot rollback:
-- {
--     "canRollback": false,
--     "errors": [
--         "Migration version 3 is missing DOWN commands",
--         "Cannot auto-generate DOWN for irreversible operation: DROP BUNDLE \"ImportantData\" WITH FORCE"
--     ]
-- }

-- Rollback not possible - would need to manually create DOWN commands
-- or accept that version 3 cannot be reversed
```

---

## EXPLAIN (Coming Soon)

⚠️ **This feature is not yet implemented.**

The `EXPLAIN` command will provide query execution plans and performance analysis for `SELECT` queries.

### Planned Syntax

```sql
EXPLAIN SELECT * FROM BUNDLE "Users" WHERE age > 25
```

### Planned Features

- 📊 **Execution Plan:** Show how the query will be executed
- 🔍 **Index Usage:** Identify which indexes are used
- ⚡ **Performance Metrics:** Estimate execution time and resource usage
- 💡 **Optimization Suggestions:** Recommend indexes or query improvements

### Example Output (Planned)

```
Query Plan:
  1. Index Scan: hash_index_Users_age
     Filter: age > 25
     Estimated Rows: 1,250
  
  2. Document Fetch: Users bundle
     Estimated I/O: 1,250 reads
  
  3. Result Set: 1,250 documents
  
Performance:
  Estimated Time: 15ms
  Index Used: YES (hash_index_Users_age)
  
Recommendations:
  - Consider B-Tree index on 'age' for range queries
  - Current hash index optimal for equality checks only
```

### Stay Tuned!

This feature is planned for a future release. Follow SyndrDB development for updates.

---

## 📚 Quick Reference

### Database Operations

| Command | Purpose | Permission |
|---------|---------|------------|
| `CREATE DATABASE "name"` | Create new database | Admin |
| `USE "name"` | Switch active database | Any |

### Backup & Restore

| Command | Purpose | Permission |
|---------|---------|------------|
| `BACKUP DATABASE "db" TO "path"` | Create backup | Admin |
| `RESTORE DATABASE FROM "path" AS "db"` | Restore backup | Admin |

### Migrations

| Command | Purpose | State |
|---------|---------|-------|
| `START MIGRATION ... COMMIT` | Create migration | Creates PENDING |
| `SHOW MIGRATIONS FOR "db"` | List migrations | Any |
| `APPLY MIGRATION WITH VERSION n` | Execute migration | PENDING → APPLIED |
| `APPLY ROLLBACK TO VERSION n` | Revert migration | APPLIED → ROLLED_BACK |
| `VALIDATE MIGRATION WITH VERSION n` | Validate before apply | PENDING |
| `VALIDATE ROLLBACK TO VERSION n` | Validate before rollback | Any |

---

## ⚙️ Configuration Settings

| Setting | Default | Description |
|---------|---------|-------------|
| **BackupDir** | `"./backups"` | Default backup directory |
| **BackupCompression** | `"gzip"` | Default compression (gzip/zstd/none) |
| **BackupIncludeIndexes** | `true` | Include indexes in backups |
| **MaxMigrationCommands** | `1000` | Max commands per migration |
| **MigrationPerformanceThreshold** | `1.0s` | Performance warning threshold |
| **MigrationTimeoutSeconds** | `300` | Migration execution timeout |
| **RequireExplicitDownCommands** | `false` | Require manual DOWN commands |

---

## 🎯 Best Practices Summary

### Database Management
- Use descriptive database names with environment suffixes
- Always verify active database before operations (`USE "database"`)
- Maintain separate databases for dev/staging/production

### Backup & Restore
- Create regular backups with timestamps
- Use `zstd` compression for production backups
- Test restore procedures regularly
- Store backups on separate storage from data files
- Verify backups periodically

### Migrations
- **Always validate before applying** (`VALIDATE MIGRATION`)
- Test migrations in development first
- Include descriptive migration descriptions
- Review data impact before rollbacks
- Create backups before applying migrations or rollbacks
- Apply migrations during low-traffic periods
- Keep migrations atomic and focused

---

**End of Miscellaneous Commands Documentation**
