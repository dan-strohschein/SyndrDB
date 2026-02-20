# SyndrDB Migration System

## Overview

SyndrDB provides a native migration system for managing schema changes to your database. Migrations are versioned, sequential, and tracked in the `primary` system database. Each migration contains a set of SyndrQL DDL/DML commands ("up commands") that are executed atomically. Migrations can optionally include rollback commands ("down commands") to reverse changes.

The migration system supports:
- Creating and staging migrations (PENDING status)
- Validating migrations before execution
- Applying migrations with WAL transaction wrapping
- Rolling back to a previous version
- Listing migration history per database

All migration commands require an active database context. Use `USE DATABASE "name"` before running any migration command.

---

## Commands

### START MIGRATION

Creates a new migration record in PENDING status. The migration is **not executed** — it is only registered. You must run `APPLY MIGRATION` separately to execute it.

**Syntax:**
```sql
START MIGRATION [WITH DESCRIPTION "<description>"]
  <command1>;
  <command2>;
  ...
COMMIT
```

**Parameters:**
- `WITH DESCRIPTION "<text>"` — Optional. A human-readable description (max 500 characters). If omitted, a description is auto-generated from the first command in the format: `"{OPERATION} {ENTITY} {NAME} - {TIMESTAMP}"` (e.g., `"CREATE BUNDLE Authors - 2026-01-15 14:30:45"`).
- Commands are separated by semicolons. Semicolons inside quoted strings are handled correctly.
- The migration body is everything between `START MIGRATION` and `COMMIT`.

**Example:**
```sql
USE DATABASE "mydb"

START MIGRATION WITH DESCRIPTION "Add authors bundle and email index"
  CREATE BUNDLE "Authors" WITH FIELDS ({"name", "STRING", true, false, ""}, {"email", "STRING", true, true, ""});
  CREATE HASH INDEX "idx_author_email" ON BUNDLE "Authors" WITH FIELDS ({"email", true, true})
COMMIT
```

**Response:**
```json
{
  "migration_id": "migration-1707912345000000000",
  "version": 1,
  "migration_status": "PENDING",
  "status": "success",
  "message": "Migration created successfully for database 'mydb'"
}
```

**Behavior:**
- Assigns the next sequential version number for the target database.
- Generates a SHA-256 checksum of the up commands, down commands, and description.
- If `CREATE BUNDLE` commands contain `WITH RELATIONSHIP (...)` clauses, they are automatically split into two commands: the `CREATE BUNDLE` (without relationship) and a deferred `UPDATE BUNDLE ... ADD RELATIONSHIP (...)`. This ensures both bundles exist before the relationship is established.
- Attempts to auto-generate down (rollback) commands. This is currently **not implemented** — the reverser is a stub. Migrations are created with empty down commands unless you provide them explicitly.

---

### APPLY MIGRATION WITH VERSION

Executes a PENDING migration by its version number.

**Syntax:**
```sql
APPLY MIGRATION WITH VERSION <number> [FORCE]
```

**Parameters:**
- `<number>` — The version number of the migration to apply.
- `FORCE` — Optional. Overrides data-loss warnings (e.g., DROP BUNDLE on a non-empty bundle).

**Example:**
```sql
APPLY MIGRATION WITH VERSION 1
```

**Response:**
```json
{
  "status": "success",
  "message": "Migration version 1 applied successfully",
  "version": 1,
  "force": false
}
```

**Behavior:**
1. Acquires a fail-fast lock on the database. If another migration is already running, returns an error immediately (no queuing).
2. Validates the migration is in PENDING status. Rejects if already APPLIED.
3. Sets status to IN_PROGRESS.
4. Executes each command sequentially within WAL transactions. Each command gets its own transaction that is committed immediately, so subsequent commands can see the effects of previous commands (e.g., a `CREATE BUNDLE` followed by `CREATE HASH INDEX` on that bundle).
5. If any command fails, the current transaction is rolled back, the migration status is set to FAILED with the error message, and execution stops.
6. On success, sets status to APPLIED and updates the database version.
7. Releases the migration lock.

---

### APPLY ROLLBACK TO VERSION

Rolls back all migrations from the current version down to (but not including) the target version. Migrations are rolled back in strict reverse order.

**Syntax:**
```sql
APPLY ROLLBACK TO VERSION <number>
```

**Parameters:**
- `<number>` — The version to roll back to. Must be less than the current version.

**Example:**
```sql
-- Current version is 3, roll back to version 1
-- This will rollback version 3 first, then version 2
APPLY ROLLBACK TO VERSION 1
```

**Response:**
```json
{
  "status": "success",
  "message": "Database rolled back to version 1 successfully",
  "targetVersion": 1
}
```

**Behavior:**
1. Acquires a fail-fast lock.
2. Validates that `targetVersion < currentVersion`.
3. Iterates from `currentVersion` down to `targetVersion + 1`:
   - Each migration must be in APPLIED status.
   - Each migration must have non-empty down commands. If any migration has no down commands, the entire rollback fails before executing anything.
4. Executes down commands for each migration in a WAL transaction.
5. Sets each rolled-back migration's status to ROLLED_BACK.
6. Updates the database version to the target version.

---

### VALIDATE MIGRATION WITH VERSION

Runs the 5-phase validation pipeline on an existing migration **without executing it**. Stores a validation report in the `primary.MigrationValidationReports` bundle.

**Syntax:**
```sql
VALIDATE MIGRATION WITH VERSION <number>
```

**Example:**
```sql
VALIDATE MIGRATION WITH VERSION 2
```

**Response:**
```json
{
  "status": "success",
  "message": "Migration version 2 validation complete",
  "report": { ... }
}
```

**Validation Phases:**

| Phase | Name | Behavior |
|-------|------|----------|
| 1 | Syntax Validation | Checks that each command starts with a valid keyword (CREATE, DROP, ALTER, ADD, REMOVE, RENAME, UPDATE, DELETE, INSERT, SELECT) and has a minimum number of tokens. **Fail-fast** — stops on first error. |
| 2 | Dependency Validation | Tracks entities created within the migration. Checks that ALTER/DROP/ADD/REMOVE reference bundles that were either created earlier in the migration or exist in the database. **Fail-fast.** |
| 3 | Command Count | Checks that the number of commands does not exceed `MaxCommandsPerMigration` (default: 1000). **Fail-fast.** |
| 4 | Data Loss Detection | Identifies destructive operations (DROP BUNDLE, DELETE, REMOVE FIELD). Queries document counts for impact estimation. Generates warnings — does not block validation. |
| 5 | Performance Impact | Stub — currently returns no warnings. |

A migration is considered valid if phases 1, 2, and 3 all pass. Phase 4 and 5 produce warnings only.

---

### VALIDATE ROLLBACK TO VERSION

Simulates a rollback without executing it. Checks that down commands exist for all affected migrations and validates their syntax.

**Syntax:**
```sql
VALIDATE ROLLBACK TO VERSION <number>
```

**Example:**
```sql
VALIDATE ROLLBACK TO VERSION 0
```

**Response includes a rollback plan:**
```json
{
  "status": "success",
  "message": "Rollback to version 0 validation complete",
  "targetVersion": 0,
  "report": {
    "rollbackPlan": [
      {"version": 3, "canRollback": true, "downCommands": 2, "syntaxValid": true},
      {"version": 2, "canRollback": false, "error": "no down commands available"},
      {"version": 1, "canRollback": true, "downCommands": 1, "syntaxValid": true}
    ]
  }
}
```

---

### SHOW MIGRATIONS

Lists all migrations for the current (or specified) database.

**Syntax:**
```sql
SHOW MIGRATIONS
SHOW MIGRATIONS FOR "<database_name>"
SHOW MIGRATIONS FOR "<database_name>" WHERE <field> = "<value>"
```

**Example:**
```sql
-- Show all migrations for the current database
SHOW MIGRATIONS

-- Show migrations for a specific database
SHOW MIGRATIONS FOR "mydb"

-- Filter by status
SHOW MIGRATIONS FOR "mydb" WHERE Status = "APPLIED"
```

**Response:**
```json
{
  "status": "success",
  "database": "mydb",
  "currentVersion": 3,
  "migrations": [
    {
      "migration_id": "migration-1707912345000000000",
      "version": 1,
      "database_name": "mydb",
      "description": "CREATE BUNDLE Authors - 2026-01-15 14:30:45",
      "status": "APPLIED",
      "created_at": "2026-01-15T14:30:45Z",
      "applied_at": "2026-01-15T14:31:02Z",
      "execution_time_ms": 127
    }
  ]
}
```

**WHERE filtering:** Only simple `field = "value"` equality is supported. The WHERE clause is parsed via regex, not the full SyndrQL parser.

---

## Supported Operations Inside Migrations

The following SyndrQL commands can be used within a `START MIGRATION ... COMMIT` block:

### CREATE BUNDLE

Creates a new bundle (table/collection) with a defined schema.

```sql
CREATE BUNDLE "BundleName" WITH FIELDS (
  {"fieldname", "TYPE", required, unique, default},
  {"fieldname2", "TYPE", required, unique, default}
)
```

Field types: `STRING`, `INT`, `FLOAT`, `BOOL`, `DATETIME`

### DROP BUNDLE

Drops an existing bundle.

```sql
DROP BUNDLE "BundleName"
DROP BUNDLE "BundleName" FORCE
```

Without `FORCE`, fails if the bundle contains documents.

### UPDATE BUNDLE — Add/Remove/Modify Fields

Modifies bundle schema fields.

```sql
UPDATE BUNDLE "BundleName"
SET (
  {"ADD", "FieldName", "TYPE", required, unique, defaultValue},
  {"REMOVE", "FieldName"},
  {"MODIFY", "OldFieldName", "NewFieldName", "TYPE", required, unique, defaultValue}
)
```

- **ADD**: Adds a new field to the schema.
- **REMOVE**: Removes a field from the schema.
- **MODIFY**: Currently **only renames** the field. Changing data types, required/unique constraints, or default values is not yet implemented.

### UPDATE BUNDLE — Rename Bundle

```sql
UPDATE BUNDLE "OldBundleName" SET NAME = "NewBundleName"
```

### UPDATE BUNDLE — Add Relationship

```sql
UPDATE BUNDLE "BundleName" ADD RELATIONSHIP (
  "<RelationshipType>", "<SourceBundle>", "<SourceField>", "<DestBundle>", "<DestField>"
)
```

Relationship types: `1toMany`, `0toMany`, `1to1`, `ManytoMany`

### CREATE HASH INDEX

```sql
CREATE HASH INDEX "IndexName" ON BUNDLE "BundleName" WITH FIELDS ({"field", required, unique})
```

### CREATE B-INDEX (B-Tree)

```sql
CREATE B-INDEX "IndexName" ON BUNDLE "BundleName" WITH FIELDS ({"field", required, unique})
```

### ADD DOCUMENT / INSERT

```sql
ADD DOCUMENT TO BUNDLE "BundleName" WITH ({field=value}, {field2=value2})
```

Field values are automatically type-converted: booleans (`true`/`false`), null, integers, floats, and strings.

### DELETE

```sql
DELETE FROM "BundleName" WHERE DocumentID == 'id'
DELETE DOCUMENTS FROM "BundleName" WHERE field == "value"
```

If the WHERE clause targets `DocumentID`, the document is deleted directly. Otherwise, matching documents are queried first, then deleted individually.

---

## Migration Lifecycle

```
                    START MIGRATION
                          │
                          ▼
                     ┌─────────┐
                     │ PENDING │
                     └────┬────┘
                          │
              ┌───────────┼───────────┐
              ▼           ▼           ▼
     VALIDATE MIGRATION   │    APPLY MIGRATION
     (dry run only)       │           │
                          │           ▼
                          │    ┌─────────────┐
                          │    │ IN_PROGRESS │
                          │    └──────┬──────┘
                          │           │
                          │     ┌─────┴─────┐
                          │     ▼           ▼
                          │ ┌────────┐ ┌────────┐
                          │ │ FAILED │ │APPLIED │
                          │ └────────┘ └───┬────┘
                          │                │
                          │         APPLY ROLLBACK
                          │                │
                          │                ▼
                          │        ┌─────────────┐
                          │        │ ROLLED_BACK │
                          │        └─────────────┘
                          │
                   Can re-APPLY a
                   FAILED migration
                   after fixing issues
```

### Status Values

| Status | Description |
|--------|-------------|
| `PENDING` | Migration created but not yet executed. |
| `IN_PROGRESS` | Migration is currently being executed. |
| `APPLIED` | Migration executed successfully. |
| `FAILED` | Migration execution encountered an error. The error message is stored in the migration record. |
| `ROLLED_BACK` | Migration was previously APPLIED but has been rolled back. |

---

## State Storage

All migration state is stored in the **`primary`** system database across four auto-created bundles:

### primary.Migrations

Stores migration records.

| Field | Type | Description |
|-------|------|-------------|
| MigrationID | STRING (unique) | Generated ID (format: `migration-{UnixNano}`) |
| Version | INT | Sequential version per database |
| DatabaseName | STRING | Target database name |
| Description | STRING | Auto-generated or user-provided |
| UpCommands | STRING | JSON-serialized array of up command strings |
| DownCommands | STRING | JSON-serialized array of down command strings |
| Status | STRING | PENDING, IN_PROGRESS, APPLIED, FAILED, ROLLED_BACK |
| Checksum | STRING | SHA-256 of up + down commands + description |
| AppliedBy | STRING | User who applied the migration |
| CreatedAt | DATETIME | When the migration was created |
| AppliedAt | DATETIME | When the migration was applied (null if not applied) |
| RolledBackAt | DATETIME | When the migration was rolled back (null if not) |
| ExecutionTimeMs | INT | Execution duration in milliseconds |
| ErrorMessage | STRING | Error details if FAILED |
| PerformanceWarnings | STRING | JSON-serialized array of warnings |

### primary.DatabaseVersions

Tracks the current schema version per database.

| Field | Type | Description |
|-------|------|-------------|
| DatabaseName | STRING (unique) | One row per database |
| CurrentVersion | INT | Currently applied version (0 = no migrations) |
| LastUpdated | DATETIME | When the version was last changed |
| LastMigrationID | STRING | ID of the most recently applied migration |

### primary.MigrationLocks

Enforces single-migration-at-a-time per database.

| Field | Type | Description |
|-------|------|-------------|
| DatabaseName | STRING (unique) | Database being migrated |
| LockedAt | DATETIME | When the lock was acquired |
| LockedBy | STRING | Always `"migration_service"` (user identity not yet wired) |
| MigrationVersion | INT | Version being applied |
| MigrationID | STRING | Migration being applied |
| Status | STRING | `"ACTIVE"` while locked |

The lock is always released in a `defer` block, including on panics.

### primary.MigrationValidationReports

Stores validation results from `VALIDATE MIGRATION` and `VALIDATE ROLLBACK`.

| Field | Type | Description |
|-------|------|-------------|
| ReportID | STRING (unique) | Generated report ID |
| MigrationVersion | INT | Version validated (0 for rollback validation) |
| TargetVersion | INT | Rollback target (0 for migration validation) |
| DatabaseName | STRING | Database name |
| ReportType | STRING | `MIGRATION_VALIDATION` or `ROLLBACK_VALIDATION` |
| GeneratedAt | DATETIME | When the report was generated |
| GeneratedBy | STRING | Always `"system"` (user identity not yet wired) |
| ValidationResults | STRING | JSON-serialized validation/rollback plan |
| ReportSizeBytes | INT | Size of the serialized results |
| Status | STRING | `ACTIVE` or `ARCHIVED` |
| ArchivedAt | DATETIME | When soft-deleted (null if active) |
| ExpiresAt | DATETIME | Auto-archive date (default: 30 days from creation) |

Expired reports are soft-deleted by the `ArchiveExpiredReports()` method (Status set to `ARCHIVED`).

---

## Tamper Detection (Checksums)

Each migration has a SHA-256 checksum computed at creation time:

```
Input:  join(UpCommands, ";") + "||" + join(DownCommands, ";") + "||" + Description
Output: 64-character hexadecimal string
```

The `ValidateMigrationChecksum()` function can verify a migration's integrity by recomputing the checksum and comparing it to the stored value. However, this check is **not automatically performed** before `APPLY MIGRATION` — it must be called explicitly.

---

## Configuration

These settings control migration system behavior. They can be set in the YAML config file or via CLI flags.

| Setting | YAML Key | Default | Description |
|---------|----------|---------|-------------|
| MaxMigrationCommands | `max_migration_commands` | 1000 | Maximum commands per migration |
| MigrationPerformanceThreshold | `migration_performance_threshold` | 1.0 (sec) | Performance warning threshold |
| MaxValidationReportSize | `max_validation_report_size` | 10,485,760 (10 MB) | Maximum validation report size in bytes |
| ValidationReportRetentionDays | `validation_report_retention_days` | 30 | Days before reports are auto-archived |
| EnableAutoReverse | `enable_auto_reverse` | true | Attempt to auto-generate down commands |
| RequireExplicitDownCommands | `require_explicit_down_commands` | false | Fail migration creation if down commands can't be generated |
| MigrationTimeoutSeconds | `migration_timeout_seconds` | 300 (5 min) | Migration execution timeout |

---

## Relationship Preprocessing

When a `CREATE BUNDLE` command includes a `WITH RELATIONSHIP (...)` clause, the migration system automatically splits it into two commands:

1. `CREATE BUNDLE "Name" WITH FIELDS (...)` — without the relationship
2. `UPDATE BUNDLE "Name" ADD RELATIONSHIP (...)` — deferred to after all bundle creation commands

This ensures both referenced bundles exist before the relationship is established. The deferred relationship commands are appended to the end of the command list.

**Example input:**
```sql
CREATE BUNDLE "Books" WITH FIELDS ({"title", "STRING", true, false, ""}, {"author_id", "STRING", true, false, ""}) WITH RELATIONSHIP ("1toMany", "Books", "author_id", "Authors", "AuthorID")
```

**Preprocessed output (2 commands):**
```sql
-- Command 1 (executed first):
CREATE BUNDLE "Books" WITH FIELDS ({"title", "STRING", true, false, ""}, {"author_id", "STRING", true, false, ""})

-- Command 2 (deferred to end):
UPDATE BUNDLE "Books" ADD RELATIONSHIP ("1toMany", "Books", "author_id", "Authors", "AuthorID")
```

---

## Concurrency and Locking

- **Fail-fast locking**: Only one migration can run per database at a time. If a lock already exists, the migration command returns an error immediately rather than waiting.
- **Lock release**: Locks are released in a `defer` block to handle panics and errors gracefully.
- **Service-level mutex**: The MigrationService holds a `sync.RWMutex` for in-memory state (version counters). `CreateMigration`, `ApplyMigration`, and `RollbackToVersion` all acquire a write lock.

---

## Limitations

### Rollback Commands Not Auto-Generated

The `MigrationReverser` is a **stub** — `GenerateDownCommands()` always returns an error. When `EnableAutoReverse` is true (default) and `RequireExplicitDownCommands` is false (default), migration creation succeeds but the migration has **no rollback capability**.

To enable rollbacks, you must provide down commands explicitly. There is currently no syntax for specifying down commands through the `START MIGRATION` command — this requires programmatic access via the `MigrationCommand.DownCommands` field.

The planned (but unimplemented) reverse mappings:
- CREATE BUNDLE -> DROP BUNDLE
- ADD FIELD -> DROP FIELD
- CREATE INDEX -> DROP INDEX
- INSERT -> DELETE (requires tracking inserted document IDs)
- UPDATE -> (requires storing before-images)
- DELETE -> (requires storing deleted document data)

### MODIFY Only Renames Fields

The `MODIFY` operation in `UPDATE BUNDLE ... SET ({"MODIFY", ...})` currently **only renames fields**. It does not support:
- Changing field data types (would require data migration across all documents)
- Changing `IsRequired` constraints (would require compliance checking)
- Changing `IsUnique` constraints (would require duplicate checking)
- Changing default values

### ADD Field Bug

There is a known bug in `executeAddFieldOperation()` at `migration_service.go:1324-1364`. The function creates a populated `field` variable with the correct values from the parsed field modification, but passes an empty `newFieldDef` variable to `adapter.AddField()` instead. This means ADD FIELD operations through the migration system will create a field with empty/default properties.

### Shallow Syntax Validation

Phase 1 validation uses a simple whitespace tokenizer, **not** the full SyndrQL parser. It only checks:
- That the first token is a recognized keyword
- Minimum token count for each operation type

It does **not** validate field types, bundle name quoting, expression syntax, or any semantic correctness. Invalid commands will pass validation but fail during execution.

### Incomplete Dependency Validation

Phase 2 dependency validation tracks entities created within the migration but does **not** query the live database. The `// TODO: Query database to check if entity exists` check defaults to `bundleFound = true`. This means:
- Referencing a non-existent bundle will pass validation but fail at execution time.
- No false positives, but also no real dependency checking against existing database state.

### Performance Analysis Is a Stub

Phase 5 (`PerformanceAnalyzer.AnalyzePerformance()`) always returns empty warnings. There is no actual performance estimation.

### No BRIN Index Support

The `executeCreateCommand()` handler recognizes `"hash"` and `"b-index"` but not `"brin"`. BRIN index creation will fail inside a migration.

### WHERE Filtering Limited

- `SHOW MIGRATIONS ... WHERE` uses regex parsing and only supports simple `field = "value"` equality.
- `DELETE` commands inside migrations use `whereClauseToFilter()` which only handles simple equality conditions — no AND, OR, comparison operators, or nested expressions.

### Migration Timeout Not Enforced

`MigrationTimeoutSeconds` is configured but **not actually enforced** during command execution. Migrations can run indefinitely.

### No User Identity Tracking

`AppliedBy` is hardcoded to `"system"`. `LockedBy` is hardcoded to `"migration_service"`. `GeneratedBy` for validation reports is `"system"`. The actual authenticated user is not wired into the migration system.

### Weak UUID Generation

Migration IDs are generated as `fmt.Sprintf("migration-%d", time.Now().UnixNano())` instead of using a proper UUID library. Under high concurrency, this can produce collisions.

### No Migration Branching or Squashing

Only linear sequential versioning is supported. There is no way to:
- Create parallel migration branches
- Squash multiple migrations into one
- Reorder migrations

### Checksum Not Validated on Apply

The SHA-256 tamper-detection checksum is generated at creation time but is **not checked** before `APPLY MIGRATION`. Manually modified migration records will execute without warning.

### Lock System Is Not Queued

If a migration is running, any concurrent migration attempt returns an error immediately. There is no advisory lock queue or retry mechanism.

### No Approval Workflow

There is a TODO for implementing an approval token system for rollbacks (validate -> approve -> apply). Currently any user with database access can apply rollbacks directly.

### Description Only Analyzes First Command

Auto-generated descriptions parse only the first command in the migration. Multi-command migrations may have misleading descriptions.

---

## Complete Example: Migration Workflow

```sql
-- 1. Select the target database
USE DATABASE "ecommerce"

-- 2. Create a migration with two commands
START MIGRATION WITH DESCRIPTION "Add products bundle with price index"
  CREATE BUNDLE "Products" WITH FIELDS (
    {"name", "STRING", true, false, ""},
    {"price", "FLOAT", true, false, "0.0"},
    {"category", "STRING", false, false, ""},
    {"in_stock", "BOOL", false, false, "true"}
  );
  CREATE HASH INDEX "idx_product_category" ON BUNDLE "Products" WITH FIELDS ({"category", false, false})
COMMIT

-- 3. Validate the migration before applying
VALIDATE MIGRATION WITH VERSION 1

-- 4. Apply the migration
APPLY MIGRATION WITH VERSION 1

-- 5. Check migration history
SHOW MIGRATIONS

-- 6. Later: add a new field
START MIGRATION WITH DESCRIPTION "Add SKU field to products"
  UPDATE BUNDLE "Products" SET ({"ADD", "sku", "STRING", false, true, ""})
COMMIT

APPLY MIGRATION WITH VERSION 2

-- 7. Check current state
SHOW MIGRATIONS FOR "ecommerce" WHERE Status = "APPLIED"
```

---

## Source Files

| File | Purpose |
|------|---------|
| `src/internal/domain/migration/migration_service.go` | Core business logic: create, apply, rollback, validate |
| `src/internal/domain/migration/migration_models.go` | Data types: Migration, MigrationCommand, MigrationConfig, ValidationReport |
| `src/internal/domain/migration/migration_validator.go` | 5-phase validation pipeline |
| `src/internal/domain/migration/migration_reverser.go` | Auto-reverse stub (not implemented) |
| `src/internal/domain/migration/migration_checksum.go` | SHA-256 checksum generation and validation |
| `src/internal/domain/migration/migration_description.go` | Auto-generated description from first command |
| `src/internal/domain/migration/migration_performance.go` | Performance analysis stub |
| `src/internal/domain/migration/migration_config.go` | Configuration loading from settings |
| `src/internal/domain/migration/migration_command_parser.go` | Lightweight parsers for INSERT, DELETE, field definitions |
| `src/internal/server/migration_commands.go` | Command handlers (START, APPLY, VALIDATE, SHOW, ROLLBACK) |
| `src/internal/syndrQL/show_migrations_parser.go` | SHOW MIGRATIONS tokenizer-based parser |
| `src/internal/syndrQL/migration_parser.go` | Migration parser stub (not implemented) |
| `src/internal/server/bundle_service_adapter.go` | Adapter bridging migration service to BundleService |
| `src/internal/server/migration_service_adapter.go` | Adapter bridging MigrationService to command interface |
