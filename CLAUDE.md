# SyndrDB - CLAUDE.md

## What Is SyndrDB?

SyndrDB is a relational document database written in Go. It combines MongoDB's flexible document model with PostgreSQL's query planning, ACID transactions, and indexing. It also provides native GraphQL support. Documents are stored as typed field maps within "bundles" (the equivalent of tables/collections), organized into pages on disk in binary (BSON) segment files.

**Module:** `syndrdb` | **Go version:** 1.24.2 | **License:** BSL 1.1

## Build & Test

```bash
# Build (from project root)
./build.sh              # Builds server + client + test runner
./build.sh test         # Build and run integration tests

# Core unit tests (fast, no server required)
cd src && go test -race ./internal/query/... ./internal/domain/bundle/... ./internal/syndrQL/... ./internal/server/...

# Build manually
cd src && go build -o ../bin/server/server cmd/server/main.go
cd src && go build -o ../bin/client/client cmd/client/main.go
```

**Test locations:**
- Unit tests: colocated `_test.go` files throughout `src/internal/`
- Integration/E2E tests: `src/cmd/tests/` (built as separate binary, has mixed package issues with `go test`)
- Deprecated: `src/tests/homegrown/` (IGNORE)

## Project Structure

```
src/
├── cmd/
│   ├── server/main.go              # Server entry point (GC tuning, flag parsing, YAML config)
│   ├── client/main.go              # Interactive CLI client
│   └── tests/                      # Integration test suite (compiled to bin/tests/test_runner)
├── internal/
│   ├── server/                     # TCP server, sessions, command routing, transactions, cursors
│   ├── domain/
│   │   ├── models/                 # Core types: Document, Bundle, Field, DocumentPage
│   │   ├── bundle/                 # BundleService: CRUD, page cache, MVCC, index scheduling
│   │   ├── document/               # Document factory and pool
│   │   ├── index/                  # Hash (V3 LSM), B-Tree (V2), BRIN indexes
│   │   ├── database/               # Database operations
│   │   ├── compactor/              # Segment file compaction
│   │   ├── statistics/             # Bundle statistics for cost-based planning
│   │   ├── view/                   # View management (materialized + standard)
│   │   └── migration/              # Schema migration system
│   ├── query/
│   │   ├── planner/                # Cost-based query planner, execution nodes, plan cache
│   │   │   ├── sorting/            # Sort implementations (radix, parallel, SIMD, Top-N heap)
│   │   │   └── subquery/           # Subquery optimization
│   │   ├── documentScanner/        # Document scanning with predicate pushdown
│   │   ├── executor/               # Query execution engine
│   │   ├── join_executor/          # Hash join implementation
│   │   ├── queryparser/            # Unified query parser (delegates to syndrQL)
│   │   ├── resolver/               # Field/column resolver
│   │   ├── results/                # Result formatting
│   │   └── bloomfilter/            # Bloom filter for WHERE optimization
│   ├── syndrQL/                    # Query language: lexer, parsers, expressions, evaluator
│   ├── graphQL/                    # Native GraphQL support (schema, resolvers, DataLoader)
│   ├── journal/                    # WAL system, snapshot manager, group commit
│   ├── auth/                       # Authentication, user store, security
│   ├── monitoring/                 # Metrics and monitoring
│   ├── async/                      # Worker pools, ordered queues
│   ├── lock/                       # Lock manager (document-level + bundle-level)
│   ├── storage/
│   │   └── bundlestore/            # Storage engine, manifest manager, write buffer
│   ├── audit/                      # Audit logging
│   └── backup/                     # Backup/restore operations
├── pkg/
│   ├── settings/                   # Global settings singleton (100+ options)
│   ├── constants/                  # Constants
│   ├── common/                     # Helpers, type conversion, streaming encoder
│   ├── errors/                     # SyndrDBError framework
│   ├── mvcc/                       # MVCC types (SnapshotInfo)
│   └── fatal/                      # Fatal error handling
```

## Core Data Model

### Document
```go
type Document struct {
    DocumentID      string              // Unique ID within bundle
    Fields          map[string]Field    // Typed field values (NOT map[string]interface{})
    Data            map[string]interface{} // Raw data for storage
    CreatedAt       time.Time
    UpdatedAt       time.Time
    CommitSequence  uint64              // MVCC: global sequence when committed
    SupersededAt    time.Time           // MVCC: when replaced (zero = current version)
    CreatedByTxID   uint64              // Transaction that created this version
    DeletedByTxID   uint64              // Transaction that deleted this version
    VersionSequence uint64              // Version counter (1, 2, 3...)
}
```

### Field (Zero-Allocation Union Type)
```go
type FieldValue struct {
    Type        FieldValueType  // Discriminator
    StringVal   string
    IntVal      int64
    FloatVal    float64
    BoolVal     bool
    DateTimeVal time.Time
    // ... eliminates interface{} boxing allocations
}
```

### Bundle (= Table/Collection)
- Named container with `DocumentStructure` (schema with field definitions)
- Documents organized into `DocumentPage` (~4096 docs per page)
- Multi-file storage: append-only `.bnd` segment files tracked by `.manifest`
- Supports indexes (hash, B-tree, BRIN), relationships, and constraints

### DocumentPage
- `Documents map[string]Document` - map-based access
- `DocumentSlice []Document` - slice-based access (scan-optimized)
- Linked list via `NextPageID`/`PreviousPageID`
- COW snapshots for concurrent GROUP BY

## Query Language (SyndrQL)

SQL-like with document-database extensions. Key syntax:

```sql
-- DML
SELECT [DISTINCT] fields FROM "BundleName" [WHERE expr] [GROUP BY fields] [HAVING expr] [ORDER BY fields] [LIMIT n OFFSET m]
ADD DOCUMENT TO BUNDLE "BundleName" WITH ({field = value}, ...)
UPDATE DOCUMENTS IN BUNDLE "BundleName" (field = value) [CONFIRMED] [WHERE expr]
DELETE DOCUMENTS FROM BUNDLE "BundleName" [CONFIRMED] [WHERE expr]

-- DDL
CREATE BUNDLE "Name" WITH FIELDS ({"field", TYPE, required, unique, default}, ...)
CREATE [MATERIALIZED] VIEW "Name" AS SELECT ...
CREATE B-INDEX "idx" ON BUNDLE "b" WITH FIELDS ({"field", req, uniq})
CREATE HASH INDEX "idx" ON BUNDLE "b" WITH FIELDS ({"field", req, uniq})
CREATE BRIN INDEX "idx" ON BUNDLE "b" WITH FIELDS ({"field", req, uniq})

-- Transactions
BEGIN TRANSACTION / COMMIT / ROLLBACK
SAVEPOINT "name" / ROLLBACK TO SAVEPOINT "name"

-- Cursors
DECLARE cursor_name CURSOR FOR SELECT ...
FETCH N FROM cursor_name / FETCH ALL FROM cursor_name
CLOSE cursor_name

-- Prepared Statements
PREPARE stmt AS SELECT ... WHERE field = $1
EXECUTE stmt    -- params via protocol layer, not SQL
DEALLOCATE stmt

-- RBAC
CREATE USER "name" PASSWORD "pwd" / GRANT "perm" TO USER "name"
```

**Expression operators:** `==`, `!=`, `>`, `>=`, `<`, `<=`, `AND`, `OR`, `NOT`, `LIKE`, `IN`, `NOT IN`, `IS NULL`, `IS NOT NULL`, `CONTAINS`, `EXISTS`

**Built-in functions:** `F:NOW()`, `F:EXTRACT()`, `F:DATE_TRUNC()`, `F:DATE_ADD()`, `F:DATE_SUB()`, `F:UPPER()`, `F:LOWER()`, `F:TRIM()`, `F:LENGTH()`

**Aggregates:** `COUNT(*)`, `COUNT(field)`, `SUM()`, `AVG()`, `MIN()`, `MAX()`

**Token note:** `*` is `TOKEN_MULTIPLY` (not TOKEN_STAR). `=` is `TOKEN_ASSIGN` (assignment); `==` is `TOKEN_EQ` (comparison).

## Execution Architecture

### Query Pipeline
```
SQL string → Tokenizer → Parser → Expression AST → Semantic Analyzer
→ QueryRouter → Planner (cost-based) → ExecutionPlan → Execute
```

### Execution Node Interfaces
```go
// Materialized (map-based)
ExecutionNode.Execute(ctx) → map[string]*Document

// Slice-based (scan-optimized, avoids map overhead)
SliceExecutionNode.ExecuteSlice(ctx) → ([]*Document, []string, error)

// Iterator (Volcano pull-based, for streaming/cursors)
IteratorNode: Init(ctx), Next() → (*Document, error), Close()  // (nil,nil) = EOF

// Nodes that can produce iterators
IterableNode.AsIterator() → IteratorNode
```

### Node Types
- **FullScanNode** - Bundle-wide scan with projection/predicate pushdown
- **IndexScanNode** - Hash/BTree/BRIN index lookup with fallback to full scan
- **BRINScanNode** - Block-range skip scan
- **IndexOnlyScanNode** - Covers query from index alone
- **BTreeOrderedScanNode** - Pre-sorted range scan (skips in-memory sort)
- **FilterNode** - WHERE expression evaluation
- **AggregationNode** - GROUP BY with hash or sort strategy
- **JoinExecutionNode** - Hash join with predicate pushdown
- **SortNode** - ORDER BY (radix, parallel, SIMD, Top-N heap)
- **LimitNode** - LIMIT/OFFSET

### Plan Cache
- 8-shard LRU with xxhash keys (parameter-independent for generic plans)
- Adaptive generic/custom planning (PostgreSQL-style: custom for first 5 executions, then compare costs)
- Lazy invalidation via version bumps; stale plan serving for SELECTs during rebuild

## Server Architecture

### Wire Protocol
- TCP with `\x04` command terminator (`\x04\x04` escapes literal)
- Connection string: `syndrdb://host:port:database:user:pass[:options]`
- Options: `compress=zstd`, `pipeline=true`, `streaming=chunked`
- Pipeline mode: `READY\n` sentinel after each response
- Streaming: `STREAM:v1\n` header → `CHUNK:<len>\n<data>` / `ZCHUNK:<comp>:<uncomp>\n<data>` → `END:<count>,<timeMS>\n`
- Parameter delimiter: `\x05` (ENQ)

### Command Director
Routes all commands. SELECT path: parse → plan (with cache) → inject MVCC snapshot → execute → stream/materialize result. Large scan throttling via semaphore (max 15 concurrent full scans).

### Session Manager
- 64-shard storage with per-shard RWMutex
- Lock-free secondary indexes via sync.Map (username, connectionID)
- Session binds: clientIP + userAgent fingerprint + cryptographic hash
- Tracks: queries, locks, transaction state, cursors, prepared statements, role cache

### Transactions
- ACID via WAL + undo-based rollback (before-images) + document-level write locks
- MVCC snapshot isolation: snapshot captured at BEGIN, conflict detection before COMMIT
- Single-level savepoints
- Auto-rollback on command errors
- Cursors closed on COMMIT/ROLLBACK (PostgreSQL semantics)

## Storage Engine

### On-Disk Layout
```
database/bundleName/
├── bundle.manifest       # JSON: tracks segment files, doc counts, bloom filters
├── 000001.bnd           # Binary segment file (append-only, BSON-encoded)
├── 000002.bnd           # Rotated when max size reached (default 32MB)
└── sorted_index.idx     # ShardedSortedIndex for pageID calculation
```

### Page Cache (64-shard)
- Dual-map: authoritative `map[string]*DocumentPage` + lock-free `sync.Map` for reads
- COW snapshot cache for GROUP BY (immutable `[]Document` snapshots)
- LRU eviction with configurable max pages
- Reader view: immutable snapshots updated atomically after writes

### Write Buffer
- Double-buffered: active buffer for writes, back buffer for background flush
- RCU lock-free path: atomic offset reservation + pwrite (no mutex contention)

## Index System

| Type | Use Case | Complexity | Implementation |
|------|----------|------------|----------------|
| **Hash V3** | Equality (`field = value`) | O(1) avg | LSM: MemTable + append-only EntryStorage |
| **B-Tree V2** | Range, ORDER BY, unique constraints | O(log n) | B+ tree with linked leaves, WAL, page cache |
| **BRIN** | Range on naturally ordered data | O(ranges) | Block-range min/max summaries (~1 entry per 128 pages) |

All indexes support: partial indexes (WHERE clause), functional indexes (LOWER, UPPER, etc.), INCLUDE for covering queries. HOT-like optimization skips hash index update when no indexed field changes.

## MVCC & WAL

### Visibility Rules
1. Read-your-own-writes: uncommitted docs visible if CreatedByTxID matches
2. Snapshot boundary: CommitSequence <= snapshotSeq
3. Active transaction exclusion: not created by tx in activeTxIDs set
4. Not deleted before snapshot boundary
5. RCU grace period: superseded docs visible for 100ms window

### Dead Version Reclamation
- `RemoveDeadVersionsFromPage()`: in-memory cleanup of versions older than oldest active snapshot
- `isDeadVersion()`: checks superseded + grace period + commitSequence < safeCutoff
- Vacuum: page-level, configurable ratio threshold and max pages per cycle

### WAL System
- Binary format with CRC32 checksums
- Three durability modes: strict (fsync each op), balanced (group commit), performance (async flush)
- Group commit with double-buffering: reduces fsync contention ~10x
- Crash recovery via checkpoint markers + operation replay
- File rotation by date + size (default 100MB max)

## Concurrency Patterns

- **64-shard page cache** with sync.Map fast path (lock-free reads)
- **64-shard session manager** with atomic counters
- **8-shard plan cache** with lazy invalidation
- **32-shard rate limiter** with immutable whitelist
- **atomic.Pointer** for BucketFileManager and ServiceManager (lock-free singletons)
- **sync.Map** for scanner registry, secondary session indexes
- **Double-checked locking** for manifest creation
- **Copy-outside-lock** pattern for page mutations

## Key Dependencies

- `github.com/dan-strohschein/HVJson` - Custom JSON handling
- `github.com/dan-strohschein/syndrdb-simd` - SIMD acceleration (sort, WHERE)
- `github.com/google/btree` - B-tree data structure
- `github.com/cespare/xxhash/v2` - Fast hashing (plan cache, sharding)
- `github.com/klauspost/compress` - zstd compression
- `go.uber.org/zap` - Structured logging
- `go.mongodb.org/mongo-driver` - BSON serialization
- `github.com/vektah/gqlparser/v2` - GraphQL schema parsing

## Settings

Settings singleton at `src/pkg/settings/settings.go` (1000+ lines, 100+ options). Key categories:
- **Server:** host, port, mode (standalone/cluster), maxConnections, TLS
- **Storage:** bundleFileMaxSizeMB (32), bundleStorageFormat (binary only)
- **WAL:** walEnabled, walMode (sync/async), durabilityMode (strict/balanced/performance), groupCommit
- **Cache:** maxLoadedDocumentPages (500), bundleAdapterMaxCachedPages (500)
- **Query:** queryTimeoutSeconds (300), queryMaxMemoryMB (25), planCacheCapacity (1000/shard)
- **Indexes:** bTreeSyncMode (batched), indexMaintenanceEnabled
- **MVCC:** enableRCUWrites, rcuGracePeriodMs (100), maxOCCRetries (3)
- **Vacuum:** vacuumEnabled, vacuumDeadRatioThreshold (0.3), vacuumMaxPagesPerCycle (100)
- **Streaming:** streamingChunkSize (256), maxOpenCursorsPerSession (64)
- **Sort:** sortSIMDEnabled, sortParallelEnabled, sortParallelMinSize (10000)

All settings loadable from YAML config file with CLI flag overrides.

## Common Pitfalls

- `Document.Fields` is `map[string]models.Field` - NOT `map[string]interface{}`
- `*` token is `TOKEN_MULTIPLY`, not TOKEN_STAR
- `=` is assignment (`TOKEN_ASSIGN`), `==` is equality comparison (`TOKEN_EQ`)
- `toFloat64` already exists in `join_nodes.go` - don't redeclare in planner package
- `src/tests/homegrown/` is deprecated - ignore those tests
- `src/cmd/tests/` has mixed package issues - build with `go build cmd/tests/*.go`, not `go test`
- Scanner's `SnapshotPageDocuments()` returns `[]models.Document` copies from COW cache
- `ExecutionPlan.UseIterator` + `IteratorFactory` for pull-based execution path

## Benchmarking

In order to validate this, there is a tool built that you can run. from the /Users/danstrohschein/Documents/CodeProjects/golang/syndrdb-bench/temp directory, you can run the following command:

../bin/benchmark/benchmark -runs 1 -warmup 0 -query-count 1  -data-dir ./data_files -log-dir ./log_files -server-bin ../../SyndrDB/bin/server/server -concurrency 60
