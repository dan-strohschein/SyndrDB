# SyndrDB Feature Reference

**Version:** Current (as of 2026-02-23)
**Module:** `syndrdb` | **Go version:** 1.24.2 | **License:** BSL 1.1

SyndrDB is a relational document database written in Go that combines MongoDB's flexible document model with PostgreSQL's query planning, ACID transactions, and indexing. This document catalogs every fully implemented feature.

---

## Table of Contents

1. [Query Language (SyndrQL)](#1-query-language-syndrql)
2. [Data Types](#2-data-types)
3. [ACID Compliance & Transactions](#3-acid-compliance--transactions)
4. [MVCC (Multi-Version Concurrency Control)](#4-mvcc-multi-version-concurrency-control)
5. [Write-Ahead Log (WAL)](#5-write-ahead-log-wal)
6. [Storage Engine](#6-storage-engine)
7. [Index System](#7-index-system)
8. [Query Execution & Optimization](#8-query-execution--optimization)
9. [SIMD Acceleration](#9-simd-acceleration)
10. [Concurrency & Parallelism](#10-concurrency--parallelism)
11. [Security & Authentication](#11-security--authentication)
12. [Networking & Wire Protocol](#12-networking--wire-protocol)
13. [GraphQL Support](#13-graphql-support)
14. [Schema Migrations](#14-schema-migrations)
15. [Backup & Restore](#15-backup--restore)
16. [Client Drivers](#16-client-drivers)
17. [CLI Client](#17-cli-client)
18. [Visual Client (SyndrDB Studio)](#18-visual-client-syndrdb-studio)
19. [Operational Features](#19-operational-features)
20. [Configuration](#20-configuration)

---

## 1. Query Language (SyndrQL)

SQL-like query language with document-database extensions.

### 1.1 Data Manipulation Language (DML)

#### SELECT
- `SELECT * FROM "Bundle"` - full document retrieval
- `SELECT field1, field2 FROM "Bundle"` - field projection
- `SELECT DISTINCT field FROM "Bundle"` - unique values
- `SELECT COUNT(*) FROM "Bundle"` - optimized count path
- `WHERE` clause filtering with arbitrary expression trees
- `GROUP BY` with aggregate functions
- `HAVING` for post-aggregation filtering
- `ORDER BY field ASC|DESC` with multi-field sorting
- `LIMIT n OFFSET m` for pagination
- `SELECT DATABASES` - list all databases

#### JOIN Operations
- `INNER JOIN ... ON condition`
- `LEFT JOIN ... ON condition`
- `RIGHT JOIN ... ON condition`
- `FULL OUTER JOIN ... ON condition`
- `WITH RELATIONSHIP` clause for hierarchical result nesting

#### Subqueries (Uncorrelated)
- `WHERE field IN (SELECT ...)`
- `WHERE field NOT IN (SELECT ...)`
- `WHERE EXISTS (SELECT ...)`
- `WHERE NOT EXISTS (SELECT ...)`
- Proper NULL semantics in NOT IN

#### Aggregate Functions
- `COUNT(*)` - count all rows
- `COUNT(field)` - count non-NULL values
- `SUM(field)` - numeric sum
- `AVG(field)` - numeric average
- `MIN(field)` - minimum value
- `MAX(field)` - maximum value

#### INSERT / ADD
- `ADD DOCUMENT TO BUNDLE "Name" WITH ({field = value}, ...)`
- `BULK ADD DOCUMENTS TO BUNDLE "Name" WITH (({f=v}), ({f=v}), ...)`
- `BULK INSERT INTO BUNDLE "Name" VALUES (({f=v}), ({f=v}), ...)`

#### UPDATE
- `UPDATE DOCUMENTS IN BUNDLE "Name" (field = value) WHERE condition`
- `CONFIRMED` keyword required for bulk updates without WHERE (safety guard)

#### DELETE
- `DELETE DOCUMENTS FROM BUNDLE "Name" WHERE condition`
- `CONFIRMED` keyword required for bulk deletes without WHERE (safety guard)

### 1.2 Data Definition Language (DDL)

#### Bundle (Table/Collection) Management
- `CREATE BUNDLE "Name" WITH FIELDS ({field, type, required, unique, default}, ...)`
- `UPDATE BUNDLE "Name"` - field modifications, renames, relationship management
- `DROP BUNDLE "Name" [FORCE]`
- `DELETE BUNDLE "Name"` - alias for DROP

#### View Management (Parsing Complete, Execution Pending)
- `CREATE VIEW "Name" AS SELECT ...` - parsed, handler returns placeholder
- `CREATE MATERIALIZED VIEW "Name" AS SELECT ...` - parsed, handler returns placeholder
- `REFRESH MATERIALIZED VIEW "Name"` - parsed, handler returns placeholder
- `DROP VIEW "Name"` - parsed, handler returns placeholder
- `SHOW VIEWS` - parsed, handler returns placeholder
- `DESCRIBE VIEW "Name"` - parsed, handler returns placeholder

> **Note:** All view commands are fully parsed and routed, but the underlying ViewService execution layer is not yet wired into the ServiceManager. View infrastructure (registry, validator, materializer, store) exists as a framework awaiting integration.

#### Index Management
- `CREATE B-INDEX "idx" ON BUNDLE "b" WITH FIELDS ({field, req, uniq})`
- `CREATE HASH INDEX "idx" ON BUNDLE "b" WITH FIELDS ({field, req, uniq})`
- `CREATE BRIN INDEX "idx" ON BUNDLE "b" WITH FIELDS ({field, req, uniq})`
- `DROP INDEX "idx"`
- Partial indexes (WHERE clause scoping)
- Functional indexes (UPPER, LOWER expressions)
- Covering indexes (INCLUDE clause for index-only scans)

#### Database Management
- `CREATE DATABASE "Name"`
- `DROP DATABASE "Name"` / `DROP DATABASE "Name" WITH FORCE`
  - **Safety checks:** The `primary` system database is unconditionally protected (case-insensitive) and cannot be dropped even with `WITH FORCE`
  - **Non-empty bundle guard:** Without `WITH FORCE`, rejects the drop if any bundle in the database contains documents; the error message reports which bundle and document count, and suggests using `WITH FORCE`
  - **Admin permission required:** When authentication is enabled, only users with the `Admin` permission may drop databases
  - **Active session termination:** Automatically detects and terminates all sessions connected to the target database (grouped by user via `InvalidateUserSessions`)
  - **Cleanup scope:** Flushes write buffers → removes in-memory bundle metadata/indexes/page caches → deletes from `DatabaseService.Databases` map → WAL-logged transaction (records database name, ID, terminated session count, timestamp, admin user) → removes all bundle and database records from the internal catalog → deletes GraphQL schema file → recursively removes the database directory from the filesystem
  - **Partial failure handling:** Two-phase success tracking (catalog + filesystem); if catalog cleanup succeeds but filesystem deletion fails, returns an error with the path for manual cleanup
- `RENAME DATABASE "old" TO "new"`
- `USE "DatabaseName"` - switch active database
- `ATTACH DATABASE "<file_path>" "<database_name>"` - attach external database
  - **Primary protection:** The `primary` system database name is protected (case-insensitive) and cannot be used as an attach alias
  - **Path validation:** Requires an absolute file path; rejects relative paths and path traversal (`..`) attempts
  - **Duplicate guard:** Rejects if a database with the same name already exists in memory or the system catalog
  - **Bundle discovery:** Automatically discovers `.bnd` files in the target directory and registers them in the BundleService, making them immediately queryable via SELECT
  - **WAL-logged:** The attach operation is persisted to WAL for crash recovery
  - **DDL classification:** ATTACH DATABASE is classified as DDL and rejected inside transactions
  - **Admin permission required:** Requires `Admin` permission when authentication is enabled
- `DETACH DATABASE "<database_name>"` - detach database (files preserved on disk)
  - **Primary protection:** The `primary` system database cannot be detached
  - **Session termination:** Automatically terminates all active sessions connected to the target database
  - **Clean teardown:** Flushes write buffers, removes bundles from BundleService in-memory state (page cache, metadata), removes database from catalog
  - **Files preserved:** Unlike DROP DATABASE, DETACH does not delete any files from disk — the database can be re-attached later
  - **WAL-logged:** The detach operation is persisted to WAL
  - **DDL classification:** DETACH DATABASE is classified as DDL and rejected inside transactions
  - **Admin permission required:** Requires `Admin` permission when authentication is enabled
- `ALTER DATABASE` / `UPDATE DATABASE`

#### Bundle Relationships
- `ADD RELATIONSHIP` - define relationship between bundles
- `CREATE RELATIONSHIP` - legacy syntax
- `DROP RELATIONSHIP` - remove relationship

### 1.3 Transaction Commands
- `BEGIN TRANSACTION` - start transaction with MVCC snapshot
- `COMMIT` - commit with conflict detection
- `ROLLBACK` - undo all changes
- `SAVEPOINT "name"` - create named savepoint (single-level)
- `ROLLBACK TO SAVEPOINT "name"` - partial rollback

### 1.4 Cursor Operations
- `DECLARE cursor_name CURSOR FOR SELECT ...`
- `FETCH N FROM cursor_name` - fetch N rows
- `FETCH ALL FROM cursor_name` - fetch all remaining rows
- `FETCH NEXT FROM cursor_name` - fetch single row
- `CLOSE cursor_name`
- Auto-close on COMMIT/ROLLBACK (PostgreSQL semantics)

### 1.5 Prepared Statements
- `PREPARE stmt_name AS SELECT ... WHERE field = $1`
- `EXECUTE stmt_name` - with parameter binding via protocol layer
- `DEALLOCATE stmt_name` - remove prepared statement
- Parameter placeholders: `$1, $2, $3, ...` (1-based indexing)
- Adaptive generic/custom planning based on execution count

### 1.6 RBAC Commands
- `CREATE USER "name" PASSWORD "pwd"`
- `UPDATE USER "name" SET PASSWORD = "new_pwd"`
- `DELETE USER "name"` / `DROP USER "name"`
- `SHOW USERS`
- `CREATE ROLE "name" [WITH DESCRIPTION "desc"]`
- `UPDATE ROLE "name" SET DESCRIPTION = "desc"`
- `DELETE ROLE "name"` / `DROP ROLE "name"`
- `GRANT "permission" TO USER "name"`
- `REVOKE "permission" FROM USER "name"`
- `GRANT ROLE "role" TO USER "name"`
- `REVOKE ROLE "role" FROM USER "name"`

### 1.7 Schema Migration
- `START MIGRATION` - begin schema migration
- `APPLY MIGRATION` - apply with version tracking
- `VALIDATE MIGRATION` - 5-phase validation pipeline
- `APPLY ROLLBACK` - rollback to previous version
- `VALIDATE ROLLBACK` - validate rollback feasibility
- `SHOW MIGRATIONS` - display migration history
- Checksum validation (SHA-256) for tamper detection
- Auto-generation of reverse (down) commands
- Atomic execution via WAL transaction wrapping

### 1.8 Expression Operators

| Category | Operators |
|----------|-----------|
| Comparison | `==`, `!=`, `>`, `>=`, `<`, `<=` |
| Logical | `AND`, `OR`, `NOT` |
| Pattern | `LIKE` (with `%` and `_`), `CONTAINS` |
| Membership | `IN`, `NOT IN` |
| Null | `IS NULL`, `IS NOT NULL` |
| Subquery | `EXISTS`, `NOT EXISTS` |
| Arithmetic | `+`, `-`, `*`, `/`, `%` |

### 1.9 Built-in Functions

| Function | Description |
|----------|-------------|
| `F:NOW()` | Current timestamp (UTC, query-scoped) |
| `F:EXTRACT(unit, datetime)` | Extract date part (YEAR, MONTH, DAY, HOUR, MINUTE, SECOND) |
| `F:DATE_TRUNC(unit, datetime)` | Truncate to precision |
| `F:DATE_ADD(datetime, interval)` | Add interval to date |
| `F:DATE_SUB(datetime, interval)` | Subtract interval from date |
| `F:AGE(datetime1, datetime2)` | Calculate age between dates |
| `F:UPPER(string)` | Convert to uppercase (SIMD-accelerated for ASCII) |
| `F:LOWER(string)` | Convert to lowercase (SIMD-accelerated for ASCII) |
| `F:TRIM(string)` | Remove leading/trailing whitespace |
| `F:LENGTH(string)` | String length |
| `AT TIME ZONE` | Timezone conversion |
| `INTERVAL 'value' UNIT` | Date/time intervals (YEAR, MONTH, DAY, HOUR, MINUTE, SECOND) |

### 1.10 Monitoring & Diagnostic Commands

| Command | Purpose |
|---------|---------|
| `SHOW DATABASES` | List all databases |
| `SHOW BUNDLES [FOR "dbname"]` | List bundles in database |
| `SHOW BUNDLE "name"` | Bundle schema details |
| `SHOW VIEWS` | List views |
| `SHOW SESSIONS` / `SHOW PROCESSLIST` | Active sessions |
| `SHOW SESSION` | Current session info |
| `SHOW USERS` | List database users |
| `SHOW MIGRATIONS FOR "dbname"` | Schema migration history |
| `SHOW RATE LIMIT` | Rate limiting statistics |
| `SHOW SERVER` | Server info/stats |
| `SHOW CACHE` | Cache statistics (page cache, plan cache) |
| `SHOW VERSIONS [FOR "docid"] [IN "bundle"]` | MVCC version history |
| `SHOW ACTIVE SNAPSHOTS` | Active transaction snapshots |
| `SHOW CONFLICT LOG` | OCC conflict history |
| `SHOW BACKUPS` | List backup history |
| `MONITOR SESSIONS [INTERVAL ms]` | Live session streaming |
| `MONITOR SESSION "id" [INTERVAL ms]` | Monitor specific session |
| `STOP MONITOR` | Stop monitoring |
| `INVALIDATE SESSION "id"` | Force disconnect session |
| `EXPLAIN SELECT ...` | Query plan analysis |
| `CHECKPOINT` | Force WAL flush and page sync |
| `LOCK DATABASE "name"` | Acquire database write lock |
| `UNLOCK DATABASE "name"` | Release database lock |
| `BACKUP DATABASE "name" TO "path"` | Create backup |
| `RESTORE DATABASE "name" FROM "path"` | Restore from backup |

---

## 2. Data Types

| Type | Description | Storage |
|------|-------------|---------|
| `STRING` | Text data | Variable-length |
| `INT` | 64-bit signed integer | 8 bytes |
| `FLOAT` | 64-bit floating-point | 8 bytes |
| `BOOL` | Boolean (true/false) | 1 byte |
| `DATE` | Date value | Parsed from datetime strings |
| `DATETIME` | Timestamp with timezone | UTC storage |
| `ARRAY` | Array type | Variable-length |
| `NULL` | Null/undefined | N/A |

### Zero-Allocation Field Type
Documents use a discriminated union `FieldValue` struct instead of `interface{}` boxing, eliminating heap allocations for field access. Each field stores its value in a type-specific slot (StringVal, IntVal, FloatVal, BoolVal, DateTimeVal) with a Type discriminator.

---

## 3. ACID Compliance & Transactions

### 3.0 Transaction Commands & Lifecycle
```sql
BEGIN TRANSACTION          -- Start transaction, capture MVCC snapshot
COMMIT                     -- Validate conflicts, assign commit sequence, persist
ROLLBACK                   -- Undo all changes via WAL replay, release locks
SAVEPOINT "name"           -- Create named savepoint (single-level)
ROLLBACK TO SAVEPOINT "name" -- Partial rollback to savepoint
```

**Transaction lifecycle**:
```
BEGIN → [DML operations] → COMMIT (success) or ROLLBACK (failure)
                         ↳ Auto-rollback on any command error
```

- Transactions support all DML commands (SELECT, INSERT, UPDATE, DELETE, BULK operations)
- DDL commands (CREATE, DROP, ALTER) are rejected inside transactions
- Cursors auto-close on COMMIT/ROLLBACK (PostgreSQL semantics)
- Prepared statements work within transaction scope
- Document-level write locks acquired during UPDATE/DELETE, released on COMMIT/ROLLBACK

### 3.1 Atomicity
- All-or-nothing transaction execution via WAL-based undo/redo
- Before-images stored for rollback (UPDATE, DELETE)
- After-images stored for crash recovery (INSERT, UPDATE)
- Auto-rollback on any command error within a transaction
- Single-level savepoints with ROLLBACK TO SAVEPOINT

### 3.2 Consistency
- Schema validation on every INSERT/UPDATE (type checking, required fields, uniqueness)
- Foreign key enforcement via index relationships
- CONFIRMED keyword safety guard prevents accidental bulk modifications
- DDL rejected inside transactions (DML-only)

### 3.3 Isolation
- **Snapshot Isolation** via MVCC
- Each transaction gets an immutable point-in-time snapshot at BEGIN
- Read-your-own-writes: uncommitted changes visible within the creating transaction
- Active transaction exclusion: changes from concurrent transactions invisible
- Write-write conflict detection at COMMIT (first-writer-wins)

### 3.4 Durability
- Three configurable durability modes:
  - **Strict**: fsync on every operation (zero data loss)
  - **Balanced**: group commit with periodic fsync (<1s loss window)
  - **Performance**: async flush to OS page cache (checkpoint-based durability)
- Binary WAL with CRC32 checksums for integrity
- Crash recovery via checkpoint markers + operation replay

### 3.5 Conflict Detection & Resolution
- **Optimistic Concurrency Control (OCC)** with pessimistic fallback
- Write-write conflict detection: if document modified after snapshot, transaction aborts
- OCC retries with exponential backoff (5ms -> 10ms -> 100ms), falls back to pessimistic locks after 3 failures
- ConflictTracker maintains document-to-commitSequence mapping via sync.Map

### 3.6 Locking
- **Document-level write locks** for UPDATE/DELETE operations
- **Bundle-level locks** for maintenance operations (backup, restore)
- **Database-level locks** for admin operations (read-only enforcement)
- **RCU (Read-Copy-Update)** lock-free path for high-throughput updates
- 30-second deadlock timeout

---

## 4. MVCC (Multi-Version Concurrency Control)

### 4.1 Document Versioning
Every document carries MVCC metadata:
- `CommitSequence` - global sequence number when committed (0 = uncommitted)
- `CreatedByTxID` - transaction ID that created this version
- `DeletedByTxID` - transaction ID that deleted this version
- `SupersededAt` - timestamp when replaced by newer version
- `VersionSequence` - incrementing version counter (1, 2, 3...)

### 4.2 Visibility Rules
1. Read-your-own-writes: visible if `CreatedByTxID` matches active transaction
2. Snapshot boundary: `CommitSequence <= snapshotSequence`
3. Active transaction exclusion: not created by tx in active set
4. Not deleted before snapshot boundary
5. RCU grace period: 100ms window for version visibility (configurable)

### 4.3 Snapshot Manager
- Creates immutable snapshots on BEGIN TRANSACTION
- Captures global commit sequence as visibility boundary
- Tracks active transaction IDs at snapshot time
- Maintains oldest active snapshot for vacuum decisions
- Auto-increments global commit sequence on COMMIT

### 4.4 Dead Version Reclamation (Vacuum)
- `RemoveDeadVersionsFromPage()` - in-memory cleanup of superseded versions
- `isDeadVersion()` - checks: superseded + grace period expired + commitSequence < safeCutoff
- **MVCC GC Worker**: background goroutine runs every 30 seconds
  - Load-aware: pauses when 500+ concurrent queries active
  - Non-blocking: yields CPU with `runtime.Gosched()`
  - Triggers compaction when dead ratio exceeds threshold
- Configurable: `vacuumEnabled`, `vacuumDeadRatioThreshold` (0.3), `vacuumMaxPagesPerCycle` (100)

### 4.5 HOT-like Updates
- `updatesIndexedField()` checks if update touches any indexed field
- Skips hash index update when no indexed field changes
- PostgreSQL Heap-Only Tuple equivalent for SyndrDB

### 4.6 Debugging Commands
- `SHOW VERSIONS` - display document version history
- `SHOW ACTIVE SNAPSHOTS` - active transaction snapshots
- `SHOW CONFLICT LOG` - write conflicts detected

---

## 5. Write-Ahead Log (WAL)

### 5.1 Binary Format
- High-performance binary serialization (3-5x faster than JSON)
- Format: `[4-byte entry length][payload with magic+version+LSN+data]`
- CRC32 checksums for integrity verification
- Backwards compatible: auto-detects and reads old ASCII JSON format

### 5.2 Durability Modes
| Mode | Behavior | Data Loss Window |
|------|----------|-----------------|
| Strict | fsync every operation | None |
| Balanced | Group commit with periodic fsync | <1 second |
| Performance | Async flush to OS page cache | Up to checkpoint interval |

### 5.3 Group Commit (Double-Buffering)
- PostgreSQL-style architecture for reduced fsync contention (~10x reduction)
- Active buffer receives writes; flush buffer syncs to disk concurrently
- Brief mutex hold for buffer append, then released for concurrent writes
- Flush leader performs single fsync for multiple transactions
- Configurable: MaxWaitTime (1ms), MaxGroupSize (100), BufferSizeBytes (64KB)

### 5.4 Platform-Optimized Sync
- Linux: `fdatasync()` (2-3x faster than fsync)
- macOS: `F_FULLFSYNC` for true durability guarantees

### 5.5 File Management
- Automatic rotation by size (default 100MB) and date
- File naming: `YYYY-MM-DD.wal` (daily), `YYYY-MM-DD_HH-MM-SS.wal` (rotated)
- Configurable retention (default 30 days)
- Automatic cleanup on startup

### 5.6 Crash Recovery
- Replay from checkpoint or last known LSN
- Aggregated recovery errors with detailed context (LSN, file, reason)
- Checkpoint markers: `OpCheckpointBegin` -> operations -> `OpCheckpointComplete`
- PostgreSQL-style auto-tuned batching for checkpoints

### 5.7 Async WAL Workers
- Configurable worker count (default 2) and queue size (default 1000)
- Used in async/performance durability modes
- Ordered queue with sequence number processing

---

## 6. Storage Engine

### 6.1 On-Disk Layout
```
database/bundleName/
├── bundle.manifest       # JSON: segment file registry, bloom filters
├── 000001.bnd           # Binary segment file (append-only, BSON-encoded)
├── 000002.bnd           # Rotated at 32MB (configurable)
├── sorted_index.idx     # Sharded sorted index for pageID calculation
└── *.brin               # BRIN index files
```

### 6.2 Binary Segment Files
- Append-only `.bnd` files with BSON encoding
- Magic numbers: `0x42444D44` (metadata), `0x42445047` (document pages)
- File rotation at configurable max size (default 32MB)
- Atomic rotation with fileID increment and manifest update

### 6.3 Manifest Manager
- JSON manifest tracks all segment files per bundle
- O(1) file lookup via `filesByID` map
- Double-checked locking for first-time load (RLock fast path)
- Deferred updates skip fsync for RCU writes
- Bloom filter data persisted per file

### 6.4 Page Cache (64-Shard)
- 64 shards with per-shard RWMutex to eliminate global lock contention
- **Authoritative map**: RWMutex-protected `pages` map per shard
- **Lock-free fast path**: `sync.Map` for concurrent reads without locks
- **Reader view**: immutable snapshots updated atomically after writes
- **COW snapshot cache**: immutable `[]Document` snapshots for GROUP BY
- **LRU eviction**: per-shard max pages with linked-list tracking
- **Compaction**: periodic recreation of sync.Map to reclaim "expunged" entries
- Shard selection: `xxhash(pageKey) & 63` for O(1) distribution

### 6.5 Write Buffer (Double-Buffered, RCU Lock-Free)
- **Active buffer**: receives client writes
- **Back buffer**: background I/O flush
- **RCU lock-free path** (`WriteDirectAtomic`):
  - Atomic offset reservation without mutex contention
  - `pwrite()` direct to reserved offset
  - Minimal mutex only for OS-level pwrite serialization
  - Frozen flag prevents stale writers during file rotation
- **Buffered path**: transaction tracking with discard support on rollback
- Flush strategies: size-based (50% full), time-based (100ms), explicit (on commit)

### 6.6 Segment Compaction
- Background workers (default 3) orchestrated by CompactionScheduler with priority queue
- **Two trigger paths:**
  - **Post-write**: `EvaluateBundle()` called after each write in `AppendDocumentToBundleFileWithTxID`
  - **Periodic**: 60-second background ticker via `periodicCompactionEvaluator()` evaluates all bundles
- **Five trigger strategies** (in `compaction_strategy.go`):
  - **FileCountStrategy**: triggers when segment count >= 10
  - **TotalSizeStrategy**: triggers when total bundle size >= 512MB
  - **TombstoneRatioStrategy**: triggers when tombstone ratio >= 30%
  - **TimeBasedStrategy**: triggers on 1-hour interval
  - **CompositeStrategy**: combines strategies with AND/OR logic
- Process: select files -> merge (keep latest version per key) -> remove tombstones -> atomic replace
- Per-bundle locking prevents concurrent compaction of the same bundle
- MVCC-aware: preserves versions visible to active snapshots
- Graceful shutdown via `CompactionScheduler.Stop()` on server shutdown

### 6.7 Document Pages
- `Documents` map (`map[string]Document`) for key-based access
- `DocumentSlice` (`[]Document`) for scan-optimized sequential access
- Linked list pagination via `NextPageID`/`PreviousPageID`
- ~4096 documents per page

### 6.8 Sharded Caches
- **ShardedBufferCache**: Write buffers per file path (64 shards)
- **ShardedManifestCache**: ManifestManager instances per bundle
- **ShardedProjectionCache**: Projection field lists per bundle
- **ShardedFileReadCache**: File content with LRU eviction
- **ShardedParsedDocsCache**: Pre-parsed documents from files
- **ShardedMergedBundleCache**: Single merge+sort per bundle for multi-file loads
- Singleflight on all caches prevents thundering herd on cache population misses

### 6.9 Bloom Filters
- Per-file bloom filters for negative lookup optimization (~1% false positive rate)
- Serialized as base64 in manifest JSON
- `BuildBloomFilterForDocuments()` / `SerializeBloomFilter()` / `DeserializeBloomFilter()`
- Skips files that definitely don't contain a document

### 6.10 Sorted Index Shards
- 64 shards with B-tree per shard for O(log n) pageID calculation during INSERT
- Hash-based approximation for position in tree
- Atomic per-shard counts for lock-free position calculation

---

## 7. Index System

### 7.1 Hash Index V3 (LSM-Style)
- **Use case**: Equality lookups (`field = value`)
- **Complexity**: O(1) average (MemTable), O(log n) disk
- **Architecture**: LSM stack with MemTable + append-only EntryStorage
- Write path: append to storage -> update MemTable -> check compaction
- Read path: MemTable lookup -> if miss, scan EntryStorage backward
- Delete path: tombstone append -> MemTable update
- 256 buckets for distributed file sizing
- Temporal ordering via global sequence numbers (MVCC)
- Compaction removes tombstones and merges duplicate keys

### 7.2 B-Tree Index V2
- **Use case**: Range queries, ORDER BY, unique constraints
- **Complexity**: O(log n) for search, insert, delete
- **Architecture**: B+ tree with all leaves at same level, linked for range scans
- Fixed 8KB pages with metadata page 0
- LRU page cache for performance
- WAL integration for durability
- Batched sync mode (relies on WAL instead of per-page fsync)
- Automatic node splitting and merging (rebalancing)

### 7.3 BRIN Index (Block Range Index)
- **Use case**: Range queries on naturally ordered data
- **Complexity**: O(ranges) with up to ~99% skip rate
- One entry per 128 pages (configurable PagesPerRange)
- Min/max tracking per range with null flags
- Compact representation (~250 entries for 1M documents)
- JSON persistence to `.brin` files

### 7.4 Index Features
- **Partial indexes**: WHERE clause to limit index scope
- **Functional indexes**: Computed expressions (e.g., LOWER(name))
- **Covering indexes (INCLUDE)**: Additional columns stored for index-only scans
- **HOT-like optimization**: Skip index update when no indexed field changes
- **Index maintenance scheduler**: health tracking, staleness monitoring, rebuild scheduling

### 7.5 Index Statistics
- Per-index tracking: queries per minute, staleness rate, rebuild count
- Health status: IsHealthy, LastFailureReason, LastFailureTime
- Used by cost-based planner for scan strategy selection

---

## 8. Query Execution & Optimization

### 8.1 Query Pipeline
```
SQL string -> Tokenizer -> Parser -> Expression AST -> Semantic Analyzer
-> QueryRouter -> Planner (cost-based) -> ExecutionPlan -> Execute
```

### 8.2 Execution Node Interfaces
- **ExecutionNode**: `Execute(ctx) -> map[string]*Document` (materialized)
- **SliceExecutionNode**: `ExecuteSlice(ctx) -> ([]*Document, []string, error)` (scan-optimized)
- **IteratorNode**: `Init(ctx)`, `Next() -> (*Document, error)`, `Close()` (Volcano pull-based)
- **IterableNode**: `AsIterator() -> IteratorNode`

### 8.3 Execution Nodes
| Node | Purpose |
|------|---------|
| FullScanNode | Bundle-wide scan with projection/predicate pushdown |
| IndexScanNode | Hash/BTree/BRIN index lookup with fallback to full scan |
| BRINScanNode | Block-range skip scan |
| IndexOnlyScanNode | Covers query from index alone (no heap fetch) |
| BTreeOrderedScanNode | Pre-sorted range scan (skips in-memory sort) |
| FilterNode | WHERE expression evaluation (SIMD batch path) |
| AggregationNode | GROUP BY with hash or sort strategy |
| JoinExecutionNode | Hash join with predicate pushdown |
| SortNode | ORDER BY (radix, parallel, SIMD, Top-N heap) |
| LimitNode | LIMIT/OFFSET |

### 8.4 Cost-Based Query Planner
- PostgreSQL-style cost model with I/O and CPU components
- Cost constants: SeqPageCost (1.0), RandomPageCost (4.0), CPUDocCost (0.01), CPUPredicateCost (0.0025)
- Separate cost functions for: FullScan, HashIndexScan, BTreeRangeScan, BTreeOrderedScan, Filter, Sort, Aggregation, Join
- Buffer hit ratio adjustment for cache-aware planning
- Memory estimation with disk spillover penalties

### 8.5 Plan Cache (8-Shard LRU)
- 8 shards with xxhash keys for reduced lock contention
- **Adaptive generic/custom planning** (PostgreSQL-style):
  - First 5 executions: custom plans (parameter-specific)
  - 6th execution: builds generic plan
  - Compares average custom cost vs generic cost (110% threshold)
  - Switches to generic if within acceptable range
- Bundle-scoped lazy invalidation via version bumps
- Write-threshold invalidation (default 1000 writes)
- Graceful stale plan serving for SELECTs during async rebuild

### 8.6 Predicate Pushdown
- PostgreSQL-style WHERE clause analysis for JOIN optimization
- Separates conditions into: left-bundle, right-bundle, cross-bundle, remaining
- FilteredBundleAdapter applies WHERE conditions during document loading
- 100-1000x speedup for selective queries

### 8.7 Projection Pushdown
- FullScanNode.ProjectionFields specifies fields to deserialize
- ScanDocumentChunks applies projection in-place on snapshot copies
- CopyProjectedFromCache copies only projected fields under one RLock
- Streaming GROUP BY with projection pushdown

### 8.8 Sort Implementations
| Algorithm | Use Case | Complexity |
|-----------|----------|------------|
| Radix Sort | Integer fields | O(n) linear time (6.7x faster than quicksort) |
| String Sort | String fields | O(n log n) comparison-based |
| Top-N Heap | LIMIT queries | O(n log k) where k = LIMIT |
| Parallel Radix | Large int datasets | O(n/p) with partition-merge |
| Parallel String | Large string datasets | Parallel partition-merge |
| Parallel Top-N | Large LIMIT queries | Parallel heap-based |

### 8.9 Join Optimization
- Hash join strategy for equi-joins: O(n+m) complexity
- Build/probe side selection (smaller bundle for hash table)
- Bloom filter optimization for probe-side filtering
- SIMD acceleration option for hash/compare operations
- Memory spillover with disk spill manager for out-of-core processing
- Predicate pushdown integration via FilteredBundleAdapter

### 8.10 Streaming Aggregation
- Chunk-based aggregation via ScanDocumentChunks
- Skip session cache for COUNT(*)-only GROUP BY (~15-20ms savings)
- Type-specific group key fast path (avoids fmt.Sprint boxing, ~5-8ms savings for 100k docs)
- Projection pushdown into streaming GROUP BY
- Eliminated double copy in ScanDocumentChunks

### 8.11 Bloom Filter for WHERE Optimization
- Space-efficient probabilistic set membership filter
- Optimal bit array sizing: `m = -n * ln(p) / (ln(2)^2)`
- Optimal hash function count: `k = (m/n) * ln(2)`
- Uses: hash join probe optimization, DISTINCT deduplication (2-3x improvement)
- Zero false negatives guarantee

### 8.12 Large Scan Throttling
- Buffered semaphore limits concurrent full scans to 15
- Reduces GC pressure: 450MB vs 900MB at 30 connections
- Adaptive parallelism: disables per-query parallelism under high concurrency (30+ connections)
- Sequential page iteration under load to avoid goroutine explosion

### 8.13 Bundle Statistics (Cost-Based Planner Input)
- Adaptive sampling: <1k docs = 100%, 1k-100k = 10%, >100k = 1000 docs
- Collected stats: DistinctCount, NullCount, AvgDocumentSize, MostCommonValues (top 10), Histogram (20 buckets)
- Reservoir sampling (10,000-entry buffer)
- Scheduled auto-analyze on bundle changes

### 8.14 Visibility Map
- Per-page all-visible tracking
- Skips per-document `IsVisibleToSnapshot()` checks for fully visible pages
- Significant speedup on stable (non-updated) data

---

## 9. SIMD Acceleration

All SIMD operations use the `syndrdb-simd` library and have automatic fallback paths.

### 9.1 WHERE Clause Batch Evaluation
- `BatchWhereEvaluator` for columnar extraction + SIMD batch processing
- **Supported operators**: `>`, `>=`, `<`, `<=`, `==`, `!=`
- **Supported types**: int64, float64, string, bool
- SIMD functions: `CmpGtInt64`, `CmpGeInt64`, `CmpLtInt64`, `CmpLeInt64`, `CmpEqInt64`, `CmpNeInt64`, `CmpGtFloat64`, `CmpGeFloat64`, `CmpLtFloat64`, `CmpLeFloat64`, `CmpEqFloat64`, `CmpNeFloat64`, `CmpEqString`, `CmpNeString`
- Minimum batch size threshold (100) before activation
- Both map-based and slice-based evaluation paths

### 9.2 Batch LIKE/CONTAINS Evaluation
- `EvaluateLikeBatch()` / `EvaluateLikeBatchSlice()`
- `CompilePatternAuto` pre-compilation for repeated patterns
- SIMD string functions: `CmpContainsString`, `CmpHasPrefixString`, `CmpHasSuffixString`

### 9.3 Compound Predicate Bitmap Operations
- `extractCompoundPredicate()` for `A AND B` / `A OR B` top-level expressions
- Mask SIMD variants: `CmpEqInt64Mask`, `CmpGtFloat64Mask`, etc.
- `EvaluateCompoundBatchSlice()` combines masks via `AndBitmap` / `OrBitmap`
- `BitmaskToBools` for result gathering

### 9.4 SIMD Aggregation
- `updateAggregatesSIMD()` for streaming aggregate-only queries
- Functions: `SumInt64`, `MinInt64`, `MaxInt64` from syndrdb-simd
- `canUseSIMDAggregation()` checks for schema-resolved int64 fields
- Integrated into streaming chunk callback

### 9.5 SIMD String Functions
- `simdToLower()` / `simdToUpper()` for ASCII strings
- Uses `syndrdbsimd.StrToLower` / `StrToUpper`
- `isASCII()` guard with `strings.ToLower` / `strings.ToUpper` fallback for non-ASCII

### 9.6 SIMD Sorting
- Parallel string sorting with SIMD acceleration markers
- Integrated into sort node execution path

---

## 10. Concurrency & Parallelism

### 10.1 Sharding Architecture
| Component | Shards | Purpose |
|-----------|--------|---------|
| Page Cache | 64 | Eliminates global cache lock contention |
| Session Manager | 64 | Lock-free session lookups |
| Write Buffers | 64 | Distributed write buffer access |
| Manifests | 64 | Per-bundle manifest caching |
| Rotation Locks | 64 | Per-bundle file rotation coordination |
| Document Locks | 64 | Row-like concurrency control |
| Write Locks | 64 | Per-bundle write coordination |
| Plan Cache | 8 | Reduced lock contention for plan lookups |
| Rate Limiter | 32 | Distributed rate limit tracking |

### 10.2 Lock-Free Patterns
- `atomic.Pointer[ServiceManager]` - zero-contention singleton access
- `atomic.Pointer[BucketFileHandle]` - lock-free file handle array
- `sync.Map` for: session secondary indexes, scanner registry, fast-path cache reads
- `atomic.Int32` / `atomic.Int64` for: metadata buffer length, session counts, scan counts
- Double-checked locking for manifest creation and cache population

### 10.3 Copy-Outside-Lock Pattern
- Build new snapshots/data structures outside lock
- Take write lock only for brief atomic swap
- Applied in: page cache COW, dead version reclamation, user store encryption

### 10.4 sync.Map Compaction
- Automatic 60-second interval compaction
- Recreates sync.Map to reclaim accumulated "expunged" entries
- Applied to: ConflictTracker, SessionIndexes, HashTableCache, BundleCaches

### 10.5 Worker Pools
- Configurable goroutine pool with ordered queue
- Priority queue based on sequence numbers
- Backpressure management with queue size limits
- Per-pool error callbacks and rejected count tracking

---

## 11. Security & Authentication

### 11.1 Authentication
- **Argon2id password hashing** (OWASP-recommended): Time=1, Memory=64MB, Threads=4, KeyLen=32 bytes
- 16-byte cryptographic salt per user (`crypto/rand`)
- Constant-time comparison via `crypto/subtle.ConstantTimeCompare` (prevents timing attacks)
- Connection string authentication with session binding

### 11.2 User Store Encryption
- AES-256-GCM authenticated encryption for user store at rest
- 12-byte random nonce per encryption
- SHA-256 key derivation from user-provided string
- Atomic file writes (temp file + rename) for crash safety
- File permissions: 0600 (owner read/write only)

### 11.3 TLS/SSL
- TLS 1.2 minimum, TLS 1.3 maximum
- Cipher suites: ECDHE_RSA_WITH_AES_256_GCM_SHA384, ECDHE_RSA_WITH_CHACHA20_POLY1305, ECDHE_ECDSA variants
- Curve preferences: X25519, P256, P384
- Auto-generated self-signed certificates for development (RSA-2048, 1-year validity)
- Optional client certificate verification with CA file support

### 11.4 Role-Based Access Control (RBAC)
- **Permission levels**: Read (SELECT, SHOW, EXPLAIN, MONITOR, USE), Write (ADD, UPDATE, DELETE, BULK), Admin (CREATE, DROP, ALTER, GRANT, BACKUP, RESTORE)
- **Permission gateway**: `classifyCommandPermission()` called before every command execution
- `RequirePermission()` helper with ERR_PERMISSION_DENIED / ERR_AUTH_REQUIRED errors
- System role protection prevents modification of built-in roles
- 5-minute role cache TTL per session

### 11.5 Rate Limiting

#### Authentication Rate Limiting
- Max 5 failed attempts before lockout (15-minute duration)
- 1-hour attempt window
- Progressive delay: 2s -> 4s -> 8s -> 16s -> 32s -> 60s (max)

#### IP-Based Rate Limiting
- Max 20 attempts per IP before 30-minute lockout
- 1-hour attempt window

#### Connection Rate Limiting
- 1,000 requests/minute per IP (default)
- 10 concurrent connections per IP (default)
- 1,000 global concurrent connections (default)
- 32-shard architecture with immutable whitelist set
- 15-minute ban duration, 5-minute cleanup interval

### 11.6 Session Security
- **Session binding**: Client IP + User Agent + SHA-256 cryptographic hash
- **Validation**: `ValidateSessionSecurity()` re-validates binding on each request
- **Secure session ID**: 32-byte cryptographic random value
- **Session cleanup**: cursor closure, transaction cancellation, lock release on termination
- Configurable timeout (default 30 minutes)

### 11.7 Input Validation & Limits

| Limit | Value |
|-------|-------|
| MaxPartialDataSize | 1 MB |
| MaxFieldsPerDocument | 500 |
| ExpressionMaxDepth | 64 levels |
| LikePatternMaxLength | 1,000 characters |
| LikePatternMaxWildcards | 20 per pattern |
| MaxDocumentsPerBulkInsert | 10,000 |
| FieldNameMaxLength | 64 bytes (256 extended) |
| UsernameMaxLength | 64 bytes |
| PasswordMaxLength | 128 bytes |
| InputMaxParameterValueLength | 10,000 bytes |
| InputMaxFieldValueLength | 50,000 bytes |
| InputMaxCommandLength | 10,000 bytes |

### 11.8 LIKE Pattern Security
- Dynamic programming algorithm replaces recursive backtracking (prevents ReDoS)
- O(m*n) complexity where m=pattern length, n=value length
- Pre-calculated pattern types: exact, prefix, suffix, contains, match_all
- Proper escape handling for `%` and `_`

### 11.9 Privileged Field Protection
- DocumentID is read-only (cannot be updated)
- CreatedAt/UpdatedAt are system-managed timestamps
- CommitSequence/VersionSequence are MVCC metadata (not user-modifiable)
- Magic value escaping for `::SYNDR_NULL`, `::SYNDR_DEFAULT`, etc.

### 11.10 Audit Logging
- **Event types**: AUTH_SUCCESS, AUTH_FAILURE, AUTH_LOCKOUT, AUTH_UNLOCK, SESSION_CREATED, SESSION_EXPIRED, SESSION_DESTROYED, SESSION_HIJACK_ATTEMPT, RATE_LIMIT_HIT, IP_BLOCKED, IP_UNBLOCKED, ACCESS_DENIED, PRIVILEGE_ESCALATION, SECURITY_CONFIG_CHANGE
- **Severity levels**: INFO, WARNING, CRITICAL
- **Architecture**: Non-blocking buffered channel (2x capacity) with background processing
- **File management**: 50MB max per file, 100 files retained, automatic rotation
- **Flush**: 5-second periodic flush, 100-event buffer threshold
- **Structured events**: ID, timestamp, type, severity, username, session ID, IP, user agent, details map

### 11.11 Backup Security
- Decompression bomb prevention with `MaxRestoreSizeBytes` limit
- Path traversal prevention: `filepath.Clean()` + reject `..` and absolute paths
- Magic byte format detection (gzip, zstd, uncompressed tar)
- CRC32 checksums for backup file integrity

### 11.12 File Permissions
- Secure directories: 0700 (rwx------)
- Secure files: 0600 (rw-------)
- Default directories: 0755 (rwxr-xr-x)
- Default files: 0644 (rw-r--r--)

---

## 12. Networking & Wire Protocol

### 12.1 TCP Protocol
- Persistent TCP connections with buffered I/O
- TCP_NODELAY enabled (disables Nagle's 40ms batching)
- Command terminator: `\x04` (EOT), escape: `\x04\x04` for literal
- Parameter delimiter: `\x05` (ENQ), escape: `\x05\x05` for literal
- Buffered data channel (8 messages) prevents blocking at 150+ connections

### 12.2 Connection String
```
syndrdb://host:port:database:username:password[:options]
```
Options (6th field, `&`-separated):
- `compress=zstd` - enable zstd response compression
- `pipeline=true` - enable pipeline mode
- `streaming=chunked` - enable chunked streaming protocol

### 12.3 Pipeline Mode
- Batch-process multiple commands without waiting for individual responses
- `READY\n` sentinel after each response for client-side parsing
- Batch-drain: reader goroutine splits on `\x04`, main loop processes all available commands

### 12.4 Streaming Protocol (STREAM:v1)
```
STREAM:v1\n
{"ResultMeta":{"Fields":[...], "Compress":true/false}}\n
CHUNK:<size>\n<JSON array of docs>\n
ZCHUNK:<compressed_size>:<uncompressed_size>\n<zstd_data>\n
...
END:<total_count>,<execution_time_ms>\n
```
- Iterator-based pull model avoids materializing large result sets
- Configurable chunk size (default 256 documents)
- Error frame: `ERR:<json_error>\n` for mid-stream errors

### 12.5 Compression (Zstd)
- SpeedFastest (level 1) for minimal overhead
- Pooled encoders and buffers to reduce allocations
- Legacy format: `ZSTD:<size>\n<compressed_data>\n` for non-chunked responses
- Disable via `SYNDRDB_NO_COMPRESS=1` environment variable

### 12.6 Session Management
- 64-shard storage with per-shard RWMutex
- Lock-free secondary indexes via sync.Map (by username, by connectionID)
- Atomic session counter
- Per-session tracking: queries, locks, transaction state, cursors, prepared statements, role cache

### 12.7 Connection Pool Management
- **Server-side connection pool** with configurable maximum size (default 100 connections)
- **Connection idle timeout:** Default 30 minutes, checked every 5 minutes; idle connections are closed with a notification sent to the client
- **Active connection tracking:** Each `Connection` carries a `LastActive` timestamp updated on creation, authentication, and activity; `GetConnectionPoolStats()` exposes per-connection idle duration
- **Pool limit enforcement:** New connections are rejected with an error message (`ERROR: Server has reached maximum connection limit`) when active count reaches `MaxConnections`
- **Two-tier rate limiting:** Global connection limit (atomic, lock-free) + per-IP connection limit (default 10 per IP) via 32-shard `RateLimiter`; whitelisted IPs (`127.0.0.1`, `::1`) bypass per-IP tracking; temporarily bans IPs that exceed rate thresholds (default 15 minutes)
- **`GetConnectionPoolStats()` API:** Returns `active_count`, `max_connections`, `idle_timeout_seconds`, and a `connections` array with per-connection details (`id`, `last_active_unix`, `idle_seconds`, `database`, `user`, `authorized`)
- **Client-side pooling** is the responsibility of application drivers and SDKs

---

## 13. GraphQL Support

### 13.1 Native Protocol Integration
- Commands prefixed with `GRAPHQL::` over TCP (not HTTP)
- Query and introspection fully implemented
- Mutation support (parser, executor, resolver, input validator)

### 13.2 Dynamic Schema Generation
- SchemaManager generates GraphQL types from bundle field schemas
- Automatic schema reconciliation on bundle DDL changes
- Full `__schema` and `__type` introspection support

### 13.3 Query Execution
- GraphQL queries translated to equivalent SyndrQL SELECT queries
- Uses unified cost-based query planner for execution
- Type coercion via bundle field schema

### 13.4 5-Layer Security Model
1. **Complexity Analysis**: configurable maximum query complexity score
2. **Depth Limiting**: maximum nesting depth enforcement
3. **Per-User Rate Limiting**: token-bucket or time-bucket algorithms
4. **Query Timeout**: per-query timeout enforcement
5. **Query Monitoring**: metrics collection and alerting

### 13.5 Performance Features
- **DataLoader**: batches nested queries to prevent N+1 problems
- **Field Projection**: only requested fields fetched from storage
- **Query Plan Caching**: reuses plans for repeated queries

---

## 14. Schema Migrations

### 14.1 Migration Lifecycle
```
PENDING → IN_PROGRESS → APPLIED → ROLLED_BACK
                ↓ (error)
             FAILED (can re-apply after fixing)
```

### 14.2 Migration Commands
- `START MIGRATION [WITH DESCRIPTION "text"] <commands> COMMIT` - create a new migration
- `APPLY MIGRATION WITH VERSION <n>` - execute a pending migration
- `APPLY ROLLBACK TO VERSION <n>` - rollback migrations in reverse order to target version
- `VALIDATE MIGRATION WITH VERSION <n>` - dry-run validation without execution
- `VALIDATE ROLLBACK TO VERSION <n>` - simulate rollback and verify feasibility
- `SHOW MIGRATIONS [FOR "db_name"] [WHERE field = "value"]` - list migration history

### 14.3 Supported Operations Inside Migrations
- `CREATE BUNDLE` - create new bundle with schema
- `DROP BUNDLE [FORCE]` - drop bundle (FORCE required if non-empty)
- `UPDATE BUNDLE` - add, remove, or modify fields
- `UPDATE BUNDLE ... SET NAME =` - rename bundle
- `UPDATE BUNDLE ... ADD RELATIONSHIP` - add foreign key relationships
- `CREATE HASH INDEX` / `CREATE B-INDEX` - create indexes
- `ADD DOCUMENT` / `INSERT` - insert documents with type conversion
- `DELETE` / `DELETE DOCUMENTS` - delete with WHERE clauses

### 14.4 Validation Pipeline (5 Phases)
1. **Syntax Validation** - keyword and token analysis (fail-fast)
2. **Dependency Validation** - entity creation/reference graph analysis
3. **Command Count Check** - max 1000 commands per migration (configurable)
4. **Data Loss Detection** - warns on DROP BUNDLE, DELETE, REMOVE FIELD
5. **Performance Analysis** - estimates impact of large operations

### 14.5 Safety Features
- **Fail-fast locking**: only one migration per database at a time (no queuing)
- **SHA-256 checksums**: tamper detection for migration integrity
- **Atomic execution**: each command wrapped in WAL transaction
- **Auto-generated descriptions**: derived from first command if not provided
- **FORCE option**: override data-loss warnings when intentional
- **Version tracking**: sequential per-database versioning

### 14.6 Migration Storage
Stored in the `primary` system database across 4 auto-created bundles:

| Bundle | Purpose |
|--------|---------|
| `Migrations` | Migration records (up/down commands, status, checksum, error, timing) |
| `DatabaseVersions` | Current schema version per database |
| `MigrationLocks` | Fail-fast database-level locks |
| `MigrationValidationReports` | Validation/rollback reports with 30-day retention |

### 14.7 Migration Configuration

| Setting | Default | Description |
|---------|---------|-------------|
| MaxMigrationCommands | 1000 | Max commands per migration |
| MigrationTimeoutSeconds | 300 | Execution timeout |
| EnableAutoReverse | true | Auto-generate down commands |
| RequireExplicitDownCommands | false | Fail if reverse can't be generated |

---

## 15. Backup & Restore

### 15.1 Backup

#### Syntax
```sql
BACKUP DATABASE "dbname" TO "path/to/backup.sdb" [WITH COMPRESSION = 'gzip']
```

#### Backup Process
1. Validate database exists
2. Execute CHECKPOINT (flush WAL and in-memory state for consistency)
3. Collect all bundle files (`.bnd` segments, `.manifest`)
4. Collect all index files
5. Calculate CRC32 checksums for every file
6. Collect primary database metadata (Databases and Bundles catalog)
7. Write manifest JSON (file list, checksums, metadata, sizes)
8. Create compressed tar archive (`.sdb` file)
9. Validate backup after creation

#### Compression Options
| Mode | Description |
|------|-------------|
| `gzip` | Standard gzip compression |
| `zstd` | Zstandard (faster, better compression ratio) |
| `none` | Uncompressed tar archive |

#### Backup Options
- **Compression**: gzip, zstd, or none (configurable, default: zstd)
- **Include Indexes**: configurable inclusion of index files (default: true)
- **Output Path**: full path to output `.sdb` file

### 15.2 Restore

#### Syntax
```sql
RESTORE DATABASE "dbname" FROM "path/to/backup.sdb" [FORCE]
```

#### Restore Process
1. Extract and validate backup archive
2. Read and verify manifest
3. Verify all file CRC32 checksums
4. Check server version compatibility
5. Create database directory
6. Copy files (rename if restoring to different database name)
7. Create database in LOCKED state for verification
8. Register in system catalog (`primary.Databases`)
9. Load and register all bundles in catalog
10. Validate restored database integrity

#### Restore Features
- **Database rename**: restore to a different name than the original backup
- **Manifest rewriting**: auto-updates `databaseName` field in bundle manifests for renamed databases
- **FORCE option**: overwrite existing database if it already exists
- **Locked state**: restored databases start locked pending user verification/unlock
- **Catalog registration**: fresh UUID generation prevents overwriting source database

### 15.3 Security
- **Decompression bomb prevention**: enforces `MaxRestoreSizeBytes` limit (default: 50GB)
- **Path traversal prevention**: `filepath.Clean()` + reject `..` and absolute paths
- **Format auto-detection**: magic byte inspection (not file extension) for gzip/zstd
- **CRC32 integrity**: every file verified during restore

### 15.4 Backup Configuration

| Setting | Default | Description |
|---------|---------|-------------|
| BackupCompression | zstd | Default compression method |
| BackupIncludeIndexes | true | Include index files |
| MaxRestoreSizeBytes | 50 GB | Decompression size limit |

---

## 16. Client Drivers

SyndrDB provides official client drivers for all major modern programming languages, communicating over the TCP wire protocol.

### 16.1 Supported Languages

| Language | Driver |
|----------|--------|
| **Go** | Reference implementation (built-in CLI client) |
| **Python** | Official Python driver |
| **JavaScript / Node.js** | Official Node.js driver |
| **TypeScript** | Full TypeScript type definitions included with Node.js driver |
| **Java** | Official Java driver |
| **C# / .NET** | Official .NET driver |
| **Ruby** | Official Ruby driver |
| **Rust** | Official Rust driver |
| **PHP** | Official PHP driver |
| **Swift** | Official Swift driver |

### 16.2 Wire Protocol Support
All drivers implement the SyndrDB TCP wire protocol:
- Connection string parsing: `syndrdb://host:port:database:username:password[:options]`
- Command framing with `\x04` (EOT) terminator and `\x04\x04` escape
- Parameter binding with `\x05` (ENQ) delimiter and `\x05\x05` escape
- Pipeline mode with `READY\n` sentinel framing
- Streaming protocol (STREAM:v1) with `CHUNK` / `ZCHUNK` frames
- Zstd response compression negotiation
- Connection pooling and session management

### 16.3 Common Driver Features
- Connection pooling with configurable pool size
- Automatic reconnection and retry logic
- Prepared statement support with parameter binding
- Transaction management (BEGIN, COMMIT, ROLLBACK, SAVEPOINT)
- Cursor support for large result set streaming
- Streaming query results via iterator/async patterns
- Zstd compression toggle
- TLS/SSL connection support
- Type mapping between SyndrDB types and language-native types

### 16.4 SyndrQL IR Compiler
- TypeScript/Node.js package (`@syndrdb/ir-compiler`)
- Compiles SyndrQL Intermediate Representation to SyndrQL query strings
- Enables programmatic query construction with type safety

---

## 17. CLI Client

### 17.1 Interactive Shell
- Readline-style line editor with raw terminal mode
- Persistent command history stored in `~/.syndrdb_history` (gob-encoded binary, ring buffer)
- Configurable history capacity (default: 250 commands)
- Arrow key navigation (Up/Down for history, Left/Right for cursor)

### 17.2 Keyboard Shortcuts

| Shortcut | Action |
|----------|--------|
| `Ctrl-A` | Move cursor to beginning of line |
| `Ctrl-E` | Move cursor to end of line |
| `Ctrl-K` | Kill text to end of line |
| `Ctrl-U` | Kill text to beginning of line |
| `Ctrl-W` | Delete word backwards |
| `Ctrl-L` | Clear screen and redraw prompt |
| `Ctrl-D` | EOF (exit) or delete character |
| `Ctrl-C` | Graceful SIGINT |
| `Home` / `End` | Jump to start/end of line |
| `Delete` | Delete character at cursor |
| `Backspace` | Delete character before cursor |

### 17.3 Output Formatting
- **Pretty-print JSON** with indentation (default: enabled)
- **Compact JSON** output mode
- **TimeOnly mode**: prefix command with `TimeOnly` to show only `ResultCount` and `ExecutionTimeMS`
- **Streaming response handling**: auto-detects large responses (>= 4096 bytes) and buffers with progress dots

### 17.4 Connection Management
- Full connection string support: `syndrdb://host:port:database:username:password[:options]`
- Zstd compression toggle via `--compress` flag
- Pipeline mode via `--pipeline` flag
- Streaming protocol negotiation

### 17.5 Operating Modes
- **Interactive shell** (default): readline prompt with history
- **Single command mode**: via `-command` argument for scripting
- **Piped input**: auto-detects non-TTY input for script-friendly usage

### 17.6 Async Features
- Non-blocking server message listener in separate goroutine
- Live MONITOR session streaming display
- Large query response buffering with on-the-fly completion detection

### 17.7 Command-Line Flags

| Flag | Default | Description |
|------|---------|-------------|
| `-connection_string` | | Full connection URI |
| `-host` | localhost | Server host |
| `-port` | 1776 | Server port |
| `-database` | testdb | Database name |
| `-username` | user | Username |
| `-password` | password | Password |
| `-pretty_print` | true | Enable JSON indentation |
| `-compress` | false | Enable zstd compression |
| `-pipeline` | false | Enable pipeline mode |
| `-history_size` | 250 | Max history entries |

### 17.8 Session Management
- Graceful signal handling (SIGTERM/SIGINT) with terminal state cleanup
- Exit commands: `exit;` or `quit;` with automatic history persistence

---

## 18. Visual Client (SyndrDB Studio)

SyndrDB Studio is the official graphical client for SyndrDB, providing a full-featured visual interface for database management, query execution, and data exploration.

### 18.1 Query Tools
- **Query Editor**: syntax-highlighted SyndrQL editor with auto-completion
- **Query Results Viewer**: tabular display with sorting, filtering, and pagination
- **Query History**: searchable log of previously executed queries
- **EXPLAIN Visualizer**: graphical query plan visualization with cost breakdown

### 18.2 Data Management
- **Data Browser**: paginated document viewer with in-place editing
- **Document Inspector**: detailed view of individual documents with field types and MVCC metadata
- **Bulk Operations**: visual interface for bulk insert, update, and delete operations
- **Import/Export**: CSV and JSON data import/export tools

### 18.3 Schema Management
- **Schema Designer**: visual bundle (table) creation and modification
- **Field Editor**: add, remove, and modify fields with type selection, constraints, and defaults
- **Relationship Viewer**: visual representation of bundle relationships and foreign keys
- **Index Manager**: create, view, and manage indexes (Hash, B-Tree, BRIN) with visual configuration

### 18.4 Database Administration
- **Connection Manager**: save and organize multiple server connections
- **Session Monitor**: real-time view of active sessions and running queries
- **Server Dashboard**: server statistics, cache performance, and resource utilization
- **User & Role Management**: visual RBAC administration (create users, assign roles, manage permissions)
- **Backup Manager**: visual interface for creating and restoring backups

### 18.5 Migration Tools
- **Migration Editor**: create and edit schema migrations with syntax support
- **Migration History**: visual timeline of applied migrations with version tracking
- **Rollback Interface**: guided rollback workflow with validation preview
- **Diff Viewer**: side-by-side comparison of schema changes

### 18.6 Monitoring & Diagnostics
- **Live Metrics Dashboard**: real-time charts for query throughput, latency, cache hit rates
- **Slow Query Log**: filterable view of slow queries with execution plans
- **MVCC Inspector**: view active snapshots, version history, and conflict logs
- **Index Health**: index utilization statistics, staleness indicators, and maintenance status

---

## 19. Operational Features

### 19.1 Live Monitoring (MONITOR:v1)
```
MONITOR:v1\n
SNAPSHOT:<timestamp>\n
[JSON array of session snapshots]\n
```
- Admin sees all sessions; non-admin sees own sessions only
- Configurable interval: 500ms to 60000ms
- Streaming snapshots pushed asynchronously until STOP MONITOR

### 19.2 Metrics & Telemetry
- GlobalServerMetrics with atomic counters (query count, error count, cache hits/misses, etc.)
- Real-time export via memory-mapped file (`/tmp/syndrdb_metrics.mmap`)
- Zero-copy metrics accessible to external monitoring agents

### 19.3 Server Entry Point
- GC tuning for database workloads
- CLI flag parsing with YAML config file support
- Graceful shutdown handling
- Build: `cd src && go build -o ../bin/server/server cmd/server/main.go`

---

## 20. Configuration

All settings loadable from YAML config file with CLI flag overrides.

### 20.1 Server & Network
| Setting | Default | Description |
|---------|---------|-------------|
| host | 127.0.0.1 | Bind address |
| port | 1776 | Listen port |
| mode | standalone | Server operating mode (standalone) |
| max_connections | 100 | Max concurrent connections |
| max_sessions | 1000 | Max concurrent sessions |
| session_timeout_minutes | 30 | Session idle timeout |
| connection_idle_timeout | 30 | Connection idle timeout |

### 20.2 Storage
| Setting | Default | Description |
|---------|---------|-------------|
| data_dir | (required) | Data directory path |
| bundle_file_max_size_mb | 32 | Segment file rotation threshold |
| bundle_storage_format | binary | Storage format (binary only) |
| max_loaded_document_pages | 500 | Max pages in page cache |

### 20.3 WAL & Durability
| Setting | Default | Description |
|---------|---------|-------------|
| wal_enabled | true | Enable write-ahead log |
| wal_mode | sync | sync or async |
| durability_mode | performance | strict, balanced, or performance |
| use_group_commit | true | Enable double-buffered group commit |
| wal_batch_size | 100 | Operations per flush (balanced mode) |
| wal_max_flush_delay | 100 | Max ms before forced flush |

### 20.4 Query & Execution
| Setting | Default | Description |
|---------|---------|-------------|
| query_timeout_seconds | 300 | Query execution timeout |
| query_max_memory_mb | 25 | Per-query memory limit |
| plan_cache_capacity | 1000/shard | Plan cache entries per shard |

### 20.5 SIMD & Performance
| Setting | Default | Description |
|---------|---------|-------------|
| sort_simd_enabled | true | SIMD-accelerated sorting |
| where_simd_enabled | true | SIMD WHERE evaluation |
| where_batch_simd | true | Batch SIMD WHERE evaluation |
| join_simd_enabled | true | SIMD join acceleration |
| sort_parallel_enabled | true | Parallel sort algorithms |
| sort_parallel_min_size | 10000 | Min docs for parallel sort |

### 20.6 Indexes
| Setting | Default | Description |
|---------|---------|-------------|
| btree_sync_mode | batched | B-tree sync strategy |
| index_maintenance_enabled | true | Background index maintenance |

### 20.7 MVCC & Concurrency
| Setting | Default | Description |
|---------|---------|-------------|
| enable_rcu_writes | (varies) | Lock-free update path |
| rcu_grace_period_ms | 100 | Dead version grace period |
| max_occ_retries | 3 | OCC attempts before pessimistic |
| vacuum_enabled | true | Background dead version reclamation |
| vacuum_dead_ratio_threshold | 0.3 | Trigger compaction at 30% dead |
| vacuum_max_pages_per_cycle | 100 | Pages scanned per GC cycle |

### 20.8 Streaming & Cursors
| Setting | Default | Description |
|---------|---------|-------------|
| streaming_chunk_size | 256 | Documents per streaming chunk |
| max_open_cursors_per_session | 64 | Max cursors per session |
| cursor_idle_timeout_seconds | 300 | Cursor expiration timeout |

### 20.9 Security
| Setting | Default | Description |
|---------|---------|-------------|
| auth_enabled | false | Enable authentication |
| tls_enabled | false | Enable TLS |
| max_group_by_cardinality | (varies) | GROUP BY group limit |
| max_join_result_size | (varies) | Join result size limit |
| require_join_condition | true | Enforce join conditions |
| max_restore_size_bytes | 100GB | Backup restore size limit |

### 20.10 Monitoring
| Setting | Default | Description |
|---------|---------|-------------|
| export_realtime_metrics | true | Enable mmap metrics export |
| monitor_default_interval_ms | 1000 | Default MONITOR interval |
| monitor_min_interval_ms | 500 | Minimum MONITOR interval |
| monitor_max_interval_ms | 60000 | Maximum MONITOR interval |
