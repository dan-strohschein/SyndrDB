# SyndrDB Point-in-Time Recovery (PITR) Commands

This document covers Point-in-Time Recovery commands in SyndrDB. These commands manage WAL archiving, named restore points, and targeted recovery to specific moments in time.

---

## Table of Contents

1. [CREATE RESTORE POINT](#create-restore-point)
2. [SHOW RESTORE POINTS](#show-restore-points)
3. [SHOW WAL ARCHIVE](#show-wal-archive)
4. [RESTORE DATABASE ... TO POINT IN TIME](#restore-database-to-point-in-time)
5. [RESTORE DATABASE ... TO LSN](#restore-database-to-lsn)
6. [RESTORE DATABASE ... TO RESTORE POINT](#restore-database-to-restore-point)
7. [Configuration](#configuration)
8. [Quick Reference](#quick-reference)

---

## CREATE RESTORE POINT

Creates a named recovery target at the current WAL position. Restore points record the current LSN (Log Sequence Number) and commit sequence, allowing you to later recover the database to this exact moment.

### Syntax

```sql
CREATE RESTORE POINT "name"
```

### Components

| Component | Description | Required |
|-----------|-------------|----------|
| **name** | Unique name for the restore point | Yes |

### Prerequisites

- WAL must be enabled (`wal_enabled: true`)
- Restore point names must be unique

### Examples

```sql
-- Create a restore point before a migration
CREATE RESTORE POINT "before_migration_v3"

-- Create a daily checkpoint
CREATE RESTORE POINT "daily_2026-02-24"

-- Create a restore point before a risky operation
CREATE RESTORE POINT "pre_bulk_import"
```

### Response

```
Restore point 'before_migration_v3' created at LSN 45678, commit sequence 1234, timestamp 2026-02-24T14:30:00Z
```

### Behavior

1. Captures the current WAL LSN via `WALManager.GetCurrentLSN()`
2. Captures the current commit sequence via `SnapshotManager.GetCurrentSequence()`
3. Logs an `OpRestorePoint` entry to the WAL (persisted across crashes)
4. Persists the restore point to `restore_points.json` in the WAL directory

### Error Cases

| Error | Cause | Solution |
|-------|-------|----------|
| `WAL is not enabled` | WAL disabled in config | Set `wal_enabled: true` in config |
| `restore point name cannot be empty` | No name provided | Provide a quoted name |
| `restore point already exists` | Duplicate name | Use a unique name |

### Permissions

- **Required Permission:** `Write`

### Best Practices

- Create restore points before migrations, bulk imports, or schema changes
- Use descriptive, timestamped names (e.g., `"pre_migration_2026-02-24"`)
- Restore points are lightweight -- create them liberally before risky operations

---

## SHOW RESTORE POINTS

Lists all named restore points with their LSN, commit sequence, and timestamp.

### Syntax

```sql
SHOW RESTORE POINTS
```

### Examples

```sql
SHOW RESTORE POINTS
```

### Response Format

```json
{
    "ResultCount": 2,
    "Result": [
        {
            "Name": "before_migration_v3",
            "LSN": 45678,
            "CommitSequence": 1234,
            "Timestamp": "2026-02-24T14:30:00Z",
            "DatabaseName": ""
        },
        {
            "Name": "daily_2026-02-24",
            "LSN": 50000,
            "CommitSequence": 1300,
            "Timestamp": "2026-02-24T18:00:00Z",
            "DatabaseName": ""
        }
    ]
}
```

### Response Fields

| Field | Description |
|-------|-------------|
| **Name** | Restore point name |
| **LSN** | WAL Log Sequence Number at creation time |
| **CommitSequence** | Global commit sequence at creation time |
| **Timestamp** | When the restore point was created |
| **DatabaseName** | Associated database (if scoped) |

### Error Cases

| Error | Cause | Solution |
|-------|-------|----------|
| `WAL is not enabled` | WAL disabled | Set `wal_enabled: true` |

### Permissions

- **Required Permission:** `Read`

---

## SHOW WAL ARCHIVE

Lists all archived WAL files with their LSN ranges, timestamps, and sizes.

### Syntax

```sql
SHOW WAL ARCHIVE
```

### Prerequisites

- WAL archiving must be enabled (`wal_archive_enabled: true`)

### Examples

```sql
SHOW WAL ARCHIVE
```

### Response Format

```json
{
    "ResultCount": 3,
    "Result": [
        {
            "FileName": "wal_2026-02-24.log",
            "FirstLSN": 10000,
            "LastLSN": 25000,
            "FirstTimestamp": "2026-02-24T00:00:00Z",
            "LastTimestamp": "2026-02-24T08:00:00Z",
            "SizeBytes": 10485760,
            "Compressed": false,
            "ArchivedAt": "2026-02-24T08:05:00Z"
        },
        {
            "FileName": "wal_2026-02-24_2.log",
            "FirstLSN": 25001,
            "LastLSN": 50000,
            "FirstTimestamp": "2026-02-24T08:00:01Z",
            "LastTimestamp": "2026-02-24T16:00:00Z",
            "SizeBytes": 12582912,
            "Compressed": false,
            "ArchivedAt": "2026-02-24T16:05:00Z"
        }
    ]
}
```

### Response Fields

| Field | Description |
|-------|-------------|
| **FileName** | Name of the archived WAL file |
| **FirstLSN** | Lowest LSN in this file |
| **LastLSN** | Highest LSN in this file |
| **FirstTimestamp** | Timestamp of first entry |
| **LastTimestamp** | Timestamp of last entry |
| **SizeBytes** | File size in bytes |
| **Compressed** | Whether the file is compressed |
| **ArchivedAt** | When the file was archived |

### Behavior

- Reads the `wal_archive.manifest` from the configured archive directory
- If archiving is disabled, returns a message indicating so
- If the archive is empty, returns `"WAL archive is empty."`

### Error Cases

| Error | Cause | Solution |
|-------|-------|----------|
| `WAL archiving is not enabled` | Feature disabled | Set `wal_archive_enabled: true` |
| `failed to read WAL archive` | Archive dir missing or corrupt | Check `wal_archive_dir` setting |

### Permissions

- **Required Permission:** `Read`

---

## RESTORE DATABASE TO POINT IN TIME

Restores a database from a base backup and replays archived WAL entries up to a specific timestamp. This is the primary PITR recovery command.

### Syntax

```sql
RESTORE DATABASE FROM "backup_path" AS "database_name" TO POINT IN TIME 'timestamp'
```

### Components

| Component | Description | Required |
|-----------|-------------|----------|
| **backup_path** | Path to the base backup file (.sdb) | Yes |
| **database_name** | Name for the restored database | Yes |
| **timestamp** | RFC3339 target timestamp (e.g., `'2026-02-24T14:30:00Z'`) | Yes |

### Prerequisites

- WAL must be enabled
- The base backup must have PITR metadata (`pitr_enabled: true`, version `"1.1"`)
- Archived WAL files must cover the time range from the backup's end LSN to the target timestamp
- WAL archiving must have been running between the backup and the target time

### Examples

```sql
-- Restore to 2:30 PM today (recover from a bad UPDATE at 3 PM)
RESTORE DATABASE FROM "/backups/prod_2026-02-24.sdb" AS "production_recovered" TO POINT IN TIME '2026-02-24T14:30:00Z'

-- Restore to just before midnight
RESTORE DATABASE FROM "/backups/daily_backup.sdb" AS "analytics_recovery" TO POINT IN TIME '2026-02-23T23:59:59Z'
```

### Response

```
PITR restore complete. Restored to LSN 45000, commit sequence 1200, time 2026-02-24T14:30:00Z. Replayed 5432 operations from 3 WAL files (128 skipped).
```

### Recovery Process

The PITR restore executes in 4 phases:

1. **Validate** -- Checks the backup has PITR metadata and the WAL archive has continuous coverage to the target
2. **Restore base** -- Restores the base backup using the standard `RestoreService.RestoreBackup()` path
3. **Replay WAL** -- Replays archived WAL entries from the backup's end LSN to the target timestamp:
   - Buffers operations per-transaction
   - Only applies transactions that committed before the stop point
   - Discards uncommitted transactions (same as crash recovery)
4. **Finalize** -- Updates commit sequence and transaction counters

### Error Cases

| Error | Cause | Solution |
|-------|-------|----------|
| `WAL is not enabled` | WAL disabled | Enable WAL in config |
| `invalid timestamp` | Malformed RFC3339 timestamp | Use format `'2026-02-24T14:30:00Z'` |
| `backup does not have PITR metadata` | v1.0 backup (pre-PITR) | Take a new backup with PITR enabled |
| `WAL archive gap` | Missing WAL files in chain | Ensure continuous archiving; fill gaps |
| `target beyond archive` | Target timestamp beyond last archived WAL | Can only recover to within archived range |

### Permissions

- **Required Permission:** `Admin`

---

## RESTORE DATABASE TO LSN

Restores a database to a specific WAL Log Sequence Number. Useful when you know the exact LSN boundary.

### Syntax

```sql
RESTORE DATABASE FROM "backup_path" AS "database_name" TO LSN <number>
```

### Components

| Component | Description | Required |
|-----------|-------------|----------|
| **backup_path** | Path to the base backup file (.sdb) | Yes |
| **database_name** | Name for the restored database | Yes |
| **number** | Target LSN (unsigned integer) | Yes |

### Examples

```sql
-- Restore to exact LSN
RESTORE DATABASE FROM "/backups/prod.sdb" AS "prod_recovery" TO LSN 45678

-- Restore to LSN obtained from SHOW WAL ARCHIVE
RESTORE DATABASE FROM "/backups/daily.sdb" AS "analytics_fix" TO LSN 25000
```

### Behavior

Same 4-phase recovery as POINT IN TIME, but the stop condition is based on LSN rather than timestamp. WAL replay stops when entries exceed the target LSN.

### Error Cases

| Error | Cause | Solution |
|-------|-------|----------|
| `invalid LSN value` | Non-numeric or negative LSN | Provide a valid positive integer |
| `target LSN before backup` | Target LSN < backup's end LSN | Target must be after backup |
| `target LSN beyond archive` | Archive doesn't cover target | Check with `SHOW WAL ARCHIVE` |

### Permissions

- **Required Permission:** `Admin`

---

## RESTORE DATABASE TO RESTORE POINT

Restores a database to a previously created named restore point. This is the most user-friendly PITR option -- you create the restore point before a risky operation, then recover to it if needed.

### Syntax

```sql
RESTORE DATABASE FROM "backup_path" AS "database_name" TO RESTORE POINT "name"
```

### Components

| Component | Description | Required |
|-----------|-------------|----------|
| **backup_path** | Path to the base backup file (.sdb) | Yes |
| **database_name** | Name for the restored database | Yes |
| **name** | Name of the restore point (created with CREATE RESTORE POINT) | Yes |

### Examples

```sql
-- Restore to a named restore point
RESTORE DATABASE FROM "/backups/prod.sdb" AS "prod_recovery" TO RESTORE POINT "before_migration_v3"

-- Restore to a daily checkpoint
RESTORE DATABASE FROM "/backups/weekly.sdb" AS "analytics_fix" TO RESTORE POINT "daily_2026-02-24"
```

### Behavior

Same 4-phase recovery, but the stop condition is finding the matching `OpRestorePoint` entry in the WAL. The restore point's LSN is resolved from the restore point metadata.

### Error Cases

| Error | Cause | Solution |
|-------|-------|----------|
| `restore point name cannot be empty` | Missing name | Provide a quoted name |
| `restore point not found` | Name doesn't exist | Check with `SHOW RESTORE POINTS` |
| `restore point before backup` | Point was created before the backup | Use an older backup |

### Permissions

- **Required Permission:** `Admin`

---

## Configuration

These settings control PITR behavior. They can be set in the YAML config file or via CLI flags.

### PITR Settings

| Setting | YAML Key | Default | Description |
|---------|----------|---------|-------------|
| `PITREnabled` | `pitr_enabled` | `false` | Master switch for PITR features. When true, backups include LSN/commit sequence metadata. |
| `WALArchiveEnabled` | `wal_archive_enabled` | `false` | Enable continuous WAL archiving. Required for PITR recovery. |
| `WALArchiveDir` | `wal_archive_dir` | `"./wal_archive"` | Directory where archived WAL files are stored. |
| `WALArchivePollSeconds` | `wal_archive_poll_seconds` | `60` | How often the archiver checks for completed WAL files (seconds). |
| `WALArchiveRetentionDays` | `wal_archive_retention_days` | `30` | How long to keep archived WAL files before cleanup. |
| `WALArchiveCompression` | `wal_archive_compression` | `"none"` | Compression for archived WAL files: `"none"`, `"gzip"`, or `"zstd"`. |

### Prerequisite Settings

| Setting | YAML Key | Required Value | Description |
|---------|----------|----------------|-------------|
| `WALEnabled` | `wal_enabled` | `true` | WAL must be enabled for all PITR features |

### Example Configuration

```yaml
# Enable PITR in config.yaml
wal_enabled: true
pitr_enabled: true
wal_archive_enabled: true
wal_archive_dir: "/var/syndrdb/wal_archive"
wal_archive_poll_seconds: 30
wal_archive_retention_days: 14
wal_archive_compression: "zstd"
```

---

## Quick Reference

### Restore Point Commands

| Command | Purpose | Permission |
|---------|---------|------------|
| `CREATE RESTORE POINT "name"` | Create a named recovery target | Write |
| `SHOW RESTORE POINTS` | List all restore points | Read |

### WAL Archive Commands

| Command | Purpose | Permission |
|---------|---------|------------|
| `SHOW WAL ARCHIVE` | List archived WAL files | Read |

### PITR Restore Commands

| Command | Purpose | Permission |
|---------|---------|------------|
| `RESTORE DATABASE FROM "path" AS "db" TO POINT IN TIME 'ts'` | Recover to timestamp | Admin |
| `RESTORE DATABASE FROM "path" AS "db" TO LSN <n>` | Recover to WAL position | Admin |
| `RESTORE DATABASE FROM "path" AS "db" TO RESTORE POINT "name"` | Recover to named point | Admin |

### Typical PITR Workflow

```sql
-- 1. Enable PITR in server config (wal_enabled, pitr_enabled, wal_archive_enabled)

-- 2. Take a base backup (with PITR metadata)
BACKUP DATABASE "production" TO "prod_base.sdb" WITH COMPRESSION = 'zstd'

-- 3. Create restore points before risky operations
CREATE RESTORE POINT "before_migration"

-- 4. Run the migration...
APPLY MIGRATION WITH VERSION 5

-- 5. If something goes wrong, check available recovery targets
SHOW RESTORE POINTS
SHOW WAL ARCHIVE

-- 6. Recover to just before the migration
RESTORE DATABASE FROM "prod_base.sdb" AS "production_recovered" TO RESTORE POINT "before_migration"

-- 7. Unlock and verify the recovered database
UNLOCK DATABASE "production_recovered"
USE "production_recovered"
SELECT COUNT(*) FROM BUNDLE "Users"
```

### Disaster Recovery Workflow

```sql
-- Scenario: Bad UPDATE wiped data at 3 PM, discovered at 5 PM

-- 1. Check the WAL archive coverage
SHOW WAL ARCHIVE
-- Verify archive covers from backup to 2:55 PM

-- 2. Restore to just before the bad update
RESTORE DATABASE FROM "/backups/prod_morning.sdb" AS "production_fix" TO POINT IN TIME '2026-02-24T14:55:00Z'

-- 3. Unlock and verify
UNLOCK DATABASE "production_fix"
USE "production_fix"
SELECT COUNT(*) FROM BUNDLE "Customers"
-- Data is intact!
```

---

**End of PITR Commands Documentation**
