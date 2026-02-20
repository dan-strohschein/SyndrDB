# SyndrDB Backup and Restore

## Overview

SyndrDB provides native backup and restore functionality for creating full database snapshots and recovering from them. Backups are stored as compressed tar archives containing all database files plus a JSON manifest with metadata and CRC32 checksums for integrity verification.

The backup/restore system involves several related commands:

| Command | Purpose |
|---------|---------|
| `BACKUP DATABASE` | Create a full backup of a database |
| `RESTORE DATABASE` | Restore a database from a backup file |
| `LOCK DATABASE` | Put a database into read-only mode |
| `UNLOCK DATABASE` | Return a locked database to normal operation |
| `CHECKPOINT` | Flush all in-memory data to disk |

---

## BACKUP DATABASE

Creates a full backup of a database by collecting all data files, calculating CRC32 checksums, capturing system catalog metadata, and packaging everything into a compressed tar archive.

### Syntax

```sql
BACKUP DATABASE "dbname" TO "path/to/backup.sdb" [WITH COMPRESSION = 'gzip', INCLUDE_INDEXES = true]
```

### Parameters

| Parameter | Required | Description |
|-----------|----------|-------------|
| `"dbname"` | Yes | Name of the database to back up |
| `"path/to/backup.sdb"` | Yes | Output file path. If relative, joined with `BackupDir` setting. If no extension is provided, `.sdb` is appended automatically. |
| `COMPRESSION` | No | `'gzip'` (default), `'zstd'`, or `'none'` |
| `INCLUDE_INDEXES` | No | `true` (default) or `false` — whether to include `.hidx` and `.btidx` index files |

### Examples

```sql
-- Basic backup with default settings (gzip compression, includes indexes)
BACKUP DATABASE "ecommerce" TO "ecommerce_2026-02-19.sdb"

-- Backup with zstd compression, excluding indexes
BACKUP DATABASE "ecommerce" TO "/backups/ecommerce.sdb" WITH COMPRESSION = 'zstd', INCLUDE_INDEXES = false

-- Uncompressed backup
BACKUP DATABASE "analytics" TO "analytics_full.sdb" WITH COMPRESSION = 'none'
```

### Backup Process (10 Steps)

1. **Validate database** — Confirms the database exists via `DatabaseService.GetDatabaseByName()`.
2. **Execute CHECKPOINT** — Flushes the WAL to disk (if WAL is enabled) to ensure all in-memory data is persisted. Note: bundle page cache and index buffer flushing are not yet implemented.
3. **Collect database files** — Walks the database directory recursively, collecting all files. Skips `.lock` and `.tmp` files. Skips `.hidx` and `.btidx` index files if `INCLUDE_INDEXES` is false.
4. **Build manifest** — Creates a `Manifest` struct with backup metadata.
5. **Create temp directory** — Stages files in a temporary directory under the configured `TempDir` path. The temp directory is always cleaned up on completion (success or failure).
6. **Copy files with CRC** — Copies each file to the temp directory, calculates its CRC32 checksum (IEEE polynomial), records relative path and size in the manifest.
7. **Collect Primary DB metadata** — Reads documents from the `primary` database's `Databases` and `Bundles` bundles that relate to the target database. These contain the system catalog entries (database config, bundle schemas, field definitions) needed to re-register the database on restore.
8. **Write manifest** — Serializes the manifest as pretty-printed JSON and writes it as `manifest.json` in the temp directory root.
9. **Create archive** — Packages the entire temp directory into a tar archive with the chosen compression (gzip, zstd, or none).
10. **Record final size** — Stats the output file to capture the compressed backup size.

### What's Included in a Backup

| Content | Always Included | Conditional |
|---------|----------------|-------------|
| Bundle segment files (`.bnd`) | Yes | — |
| Bundle manifests (`bundle.manifest`) | Yes | — |
| Sorted index files (`.idx`) | Yes | — |
| Hash index files (`.hidx`) | — | Only if `INCLUDE_INDEXES = true` |
| B-Tree index files (`.btidx`) | — | Only if `INCLUDE_INDEXES = true` |
| `manifest.json` (backup metadata) | Yes | — |
| Primary DB catalog documents | Yes | — |
| WAL files | No | — |
| Lock files (`.lock`) | No | — |
| Temp files (`.tmp`) | No | — |

---

## RESTORE DATABASE

Restores a database from a backup archive. The restored database is placed in a **LOCKED** state and must be manually unlocked before it can accept writes.

### Syntax

```sql
RESTORE DATABASE FROM "path/to/backup.sdb" AS "dbname" [WITH FORCE = true]
```

### Parameters

| Parameter | Required | Description |
|-----------|----------|-------------|
| `"path/to/backup.sdb"` | Yes | Path to the backup file |
| `"dbname"` | Yes | Name for the restored database (can differ from the original) |
| `FORCE` | No | `true` to overwrite an existing database with the same name. Default: `false`. |

### Examples

```sql
-- Restore to the same name as the backup
RESTORE DATABASE FROM "/backups/ecommerce_2026-02-19.sdb" AS "ecommerce" WITH FORCE = true

-- Restore to a different name (e.g., for testing)
RESTORE DATABASE FROM "/backups/ecommerce.sdb" AS "ecommerce_staging"
```

### Restore Process (9 Steps)

1. **Extract and validate** — Extracts the backup archive to a temporary directory and reads the `manifest.json`.
2. **Check target database** — If a database with the target name already exists:
   - Without `FORCE`: returns an error.
   - With `FORCE`: deletes the existing database entirely, then proceeds.
3. **Verify CRCs** — Recalculates CRC32 for every file listed in the manifest and compares against stored checksums. If any file fails, the entire restore is aborted.
4. **Check compatibility** — Verifies the backup format version is `"1.0"`. Logs a warning (but does not fail) if the server version in the backup differs from the current server version.
5. **Create database directory** — Creates the target database's directory under the data path.
6. **Copy files** — Copies all files from the temp directory to the database directory, recreating the original directory structure.
7. **Create database in locked state** — Registers the database via `DatabaseService.AddDatabase()`, then immediately locks it via `LockService.LockDatabase()` with reason `"RESTORE"` and comment `"Database restored from backup - verification required"`.
8. **Restore Primary DB metadata** — Inserts the catalog documents from the backup manifest into the `primary` database's `Databases` and `Bundles` bundles. If the target database name differs from the original, the `DatabaseName` and `Name` fields are updated in the documents.
9. **Validate** — Confirms the database exists, is locked, and the file count matches the manifest.

### Rollback on Failure

If any step from 5 onward fails, the restore operation rolls back by:
- Deleting the created database directory (`os.RemoveAll`)
- Deleting the database registration (if step 7 completed) via `DatabaseService.DeleteDatabase()`

The temp directory is always cleaned up regardless of success or failure.

### Post-Restore: Unlocking

After a successful restore, the database is in LOCKED state. You must unlock it before normal operations:

```sql
UNLOCK DATABASE "ecommerce"
```

This safety mechanism prevents accidental writes to a freshly restored database before you've had a chance to verify its contents.

---

## LOCK DATABASE

Places a database into read-only mode. Locked databases:
- Allow **read operations** from admin users only
- **Block all write operations** (even from admins)
- **Block all access** from non-admin users

### Syntax

```sql
LOCK DATABASE "dbname" [FOR "reason"] [COMMENT "comment"]
```

### Parameters

| Parameter | Required | Description |
|-----------|----------|-------------|
| `"dbname"` | Yes | Name of the database to lock |
| `FOR "reason"` | No | Reason for locking. Default: `"manual lock"` |
| `COMMENT "comment"` | No | Additional documentation about the lock |

### Examples

```sql
-- Simple lock
LOCK DATABASE "production"

-- Lock with reason and comment
LOCK DATABASE "production" FOR "backup" COMMENT "scheduled weekly backup"

-- Lock for maintenance
LOCK DATABASE "analytics" FOR "MAINTENANCE" COMMENT "rebuilding indexes"
```

### Lock Metadata

Each lock tracks:

| Field | Description |
|-------|-------------|
| `DatabaseName` | Name of the locked database |
| `LockedAt` | Timestamp when the lock was acquired |
| `LockedBy` | Username who created the lock (currently hardcoded to `"admin"`) |
| `Reason` | Lock reason (MAINTENANCE, BACKUP, RESTORE, MANUAL, or custom string) |
| `Comment` | Optional descriptive comment |

### Access Rules for Locked Databases

| User Type | Read Operations | Write Operations |
|-----------|----------------|-----------------|
| Admin | Allowed | Blocked |
| Non-Admin | Blocked | Blocked |

Access validation is performed by `LockService.ValidateAccess(dbName, isAdmin, isWriteOperation)`.

---

## UNLOCK DATABASE

Removes the lock from a database, returning it to normal operation.

### Syntax

```sql
UNLOCK DATABASE "dbname"
```

### Example

```sql
UNLOCK DATABASE "production"
```

The UNLOCK command does not accept any additional options. If the database is not currently locked, an error is returned.

---

## CHECKPOINT

Flushes all in-memory data to disk. This is called automatically at the start of every backup, but can also be invoked manually.

### Syntax

```sql
CHECKPOINT
```

### What It Flushes

Currently, CHECKPOINT flushes the WAL (Write-Ahead Log) if WAL is enabled. Bundle page cache flushing and index buffer flushing are not yet implemented.

---

## Backup File Format

Backup files are tar archives with optional compression. The file extension is `.sdb` by convention.

### Archive Structure

```
backup.sdb (tar + compression)
├── manifest.json                    # Backup metadata and file checksums
├── bundleName/
│   ├── bundle.manifest              # Bundle storage manifest
│   ├── 000001.bnd                   # Segment file
│   ├── 000002.bnd                   # Segment file
│   ├── sorted_index.idx             # Sorted index
│   ├── indexName.hidx               # Hash index (if INCLUDE_INDEXES)
│   └── indexName.btidx              # B-Tree index (if INCLUDE_INDEXES)
└── anotherBundle/
    └── ...
```

### Manifest Format

The `manifest.json` file is a JSON document with the following structure:

```json
{
  "backup_version": "1.0",
  "timestamp": "2026-02-19T10:30:00Z",
  "database_name": "ecommerce",
  "server_version": "0.9.0",
  "compression": "gzip",
  "includes_indexes": true,
  "files": [
    {
      "path": "orders/bundle.manifest",
      "size_bytes": 4096,
      "crc32": 3456789012,
      "compressed_size": 0
    },
    {
      "path": "orders/000001.bnd",
      "size_bytes": 33554432,
      "crc32": 1234567890,
      "compressed_size": 0
    }
  ],
  "primary_db_documents": [
    {
      "bundle": "Databases",
      "document_id": "abc-123",
      "data": { "Name": "ecommerce", ... }
    },
    {
      "bundle": "Bundles",
      "document_id": "def-456",
      "data": { "DatabaseName": "ecommerce", "Name": "orders", ... }
    }
  ],
  "total_size_bytes": 67108864,
  "compressed_size": 23456789
}
```

| Field | Description |
|-------|-------------|
| `backup_version` | Format version. Currently always `"1.0"`. |
| `timestamp` | When the backup was created. |
| `database_name` | Original database name at backup time. |
| `server_version` | SyndrDB server version that created the backup. |
| `compression` | Compression algorithm used: `"gzip"`, `"zstd"`, or `"none"`. |
| `includes_indexes` | Whether index files are included. |
| `files` | Array of file entries with relative paths, sizes, and CRC32 checksums. |
| `primary_db_documents` | System catalog documents from the `primary` database (database and bundle metadata). |
| `total_size_bytes` | Sum of all uncompressed file sizes. |
| `compressed_size` | Size of the final compressed archive on disk. |

### Integrity Verification

Every file in the backup has a CRC32 checksum (IEEE polynomial) calculated at backup time and recorded in the manifest. During restore, each file's checksum is recalculated and compared. A mismatch indicates file corruption and aborts the restore.

You can validate a backup without restoring it using the `BackupService.ValidateBackup()` API (not currently exposed as a command):
1. Extracts the archive to a temp directory
2. Reads the manifest
3. Verifies CRC32 for every file
4. Cleans up temp directory

### Compression Options

| Algorithm | Extension Convention | Library | Characteristics |
|-----------|---------------------|---------|-----------------|
| gzip | `.sdb` or `.tar.gz` | `compress/gzip` (stdlib) | Good compression, universally supported |
| zstd | `.sdb` or `.tar.zst` | `github.com/klauspost/compress/zstd` | Better compression ratio and speed than gzip |
| none | `.sdb` or `.tar` | — | No compression, fastest backup/restore |

Compression type is auto-detected during extraction based on file extension (`.gz` or `.zst`).

---

## Configuration

These settings control backup and restore behavior. They can be set in the YAML config file or via CLI flags.

| Setting | YAML Key | Default | Description |
|---------|----------|---------|-------------|
| `BackupDir` | `backup_dir` | `"./backups"` | Default directory for backup files. Relative backup paths are resolved against this. |
| `BackupCompression` | `backup_compression` | `"gzip"` | Default compression: `"gzip"`, `"zstd"`, or `"none"`. Can be overridden per-backup via `WITH COMPRESSION`. |
| `BackupIncludeIndexes` | `backup_include_indexes` | `true` | Whether to include index files by default. Can be overridden per-backup via `WITH INCLUDE_INDEXES`. |
| `TempDir` | (system) | OS default | Temporary directory used for staging backup/restore operations. |

---

## Complete Example: Backup and Restore Workflow

```sql
-- 1. Optional: Lock the database to ensure consistency during backup
LOCK DATABASE "production" FOR "backup" COMMENT "daily scheduled backup"

-- 2. Create the backup
BACKUP DATABASE "production" TO "production_2026-02-19.sdb" WITH COMPRESSION = 'zstd'

-- 3. Unlock the database to resume normal operations
UNLOCK DATABASE "production"

-- 4. Later: Restore the backup to a staging environment
RESTORE DATABASE FROM "production_2026-02-19.sdb" AS "staging"

-- 5. Verify the restored data (database is in LOCKED/read-only state)
-- Admin users can run read queries to verify
USE DATABASE "staging"
SELECT * FROM "orders" LIMIT 10

-- 6. Unlock the restored database when satisfied
UNLOCK DATABASE "staging"

-- 7. Overwrite an existing database from backup
RESTORE DATABASE FROM "production_2026-02-19.sdb" AS "staging" WITH FORCE = true
UNLOCK DATABASE "staging"
```

---

## Limitations

### Backup Is Not Hot

The backup process does not acquire a database lock automatically. If writes occur during the backup, the resulting backup may contain a mix of pre-write and post-write data. For consistent backups, manually lock the database first with `LOCK DATABASE`.

### No Incremental Backups

Every backup is a full backup — all database files are copied every time. There is no support for incremental or differential backups based on WAL LSN tracking.

### Checkpoint Is Incomplete

The `CHECKPOINT` command (called automatically at backup start) only flushes the WAL. It does not flush:
- Dirty bundle pages from the page cache
- Index write buffers
- OS-level filesystem buffers (no `fsync` call)

This means recently written data that hasn't been naturally flushed may not appear in the backup.

### Temp Directory Required

Both backup and restore operations extract/stage files to a temporary directory. For large databases, this requires disk space equal to the uncompressed database size in addition to the final archive.

### No Streaming Backup

The entire backup is staged in a temp directory before archiving. There is no streaming mode that writes directly to the archive, which would reduce disk space requirements.

### Locks Are In-Memory Only

Database locks do not survive server restarts. If the server restarts after a restore, the database will be unlocked automatically. There is no persistent lock state on disk.

### No Permission Checks

Both backup and restore operations have TODOs for admin permission checking. Currently, any connected user can execute `BACKUP DATABASE`, `RESTORE DATABASE`, `LOCK DATABASE`, and `UNLOCK DATABASE` commands.

### No Backup Validation Command

The `BackupService.ValidateBackup()` method exists in code but is not exposed as a user-facing command. There is also a `GetBackupInfo()` method to inspect a backup's manifest. Neither is accessible via SyndrQL.

### Server Version Compatibility Is Lenient

If the backup was created by a different server version, the restore logs a warning but does not fail. Only the backup format version (`"1.0"`) is strictly enforced.

### Single-Threaded Operations

Both backup and restore copy files sequentially. There is no parallel file copying or multi-threaded compression.

### No Encryption

Backup files are not encrypted. There is no support for encrypted backups or password-protected archives.

### Restore Requires Target Name

The `RESTORE DATABASE` command requires the `AS "name"` clause. There is no option to auto-use the original database name from the backup.

### No BRIN Index File Handling

The backup file collection skips files by `.hidx` and `.btidx` extensions when indexes are excluded, but does not explicitly reference BRIN index file extensions. BRIN index files may or may not be included depending on their file extension.

---

## Source Files

| File | Purpose |
|------|---------|
| `src/internal/backup/backup_service.go` | Core backup logic: file collection, CRC calculation, archive creation |
| `src/internal/backup/restore_service.go` | Core restore logic: extraction, CRC verification, file placement, validation |
| `src/internal/backup/manifest.go` | Manifest data types, JSON serialization, CRC32 utilities |
| `src/internal/server/backup_operations.go` | `BACKUP DATABASE` command handler and parser |
| `src/internal/server/restore_operations.go` | `RESTORE DATABASE` command handler and parser |
| `src/internal/server/lock_operations.go` | `LOCK DATABASE` / `UNLOCK DATABASE` command handlers |
| `src/internal/server/checkpoint_operations.go` | `CHECKPOINT` command handler |
| `src/internal/lock/lock_service.go` | Lock management service (in-memory lock state, access validation) |
