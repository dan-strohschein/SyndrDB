# GraphQL Schema Storage Implementation Specification

**Document Version:** 1.0  
**Created:** 2025-11-06  
**Author:** SyndrDB Architecture Team  
**Target Release:** v0.1.0-alpha  

---

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [Goals & Success Criteria](#2-goals--success-criteria)
3. [Requirements](#3-requirements)
4. [Architectural Overview](#4-architectural-overview)
5. [File Format Specification](#5-file-format-specification)
6. [Implementation Phases](#6-implementation-phases)
7. [Algorithms](#7-algorithms)
8. [API Specifications](#8-api-specifications)
9. [Performance Targets](#9-performance-targets)
10. [Testing Strategy](#10-testing-strategy)
11. [Migration & Deployment](#11-migration--deployment)
12. [Appendices](#12-appendices)

---

## 1. Executive Summary

### 1.1 Problem Statement

SyndrDB requires a GraphQL interface layer that provides modern, web-friendly API access to its document database capabilities. To support this, we need an efficient system for storing, versioning, and retrieving GraphQL schemas that are automatically derived from bundle structures.

### 1.2 Solution Overview

We are implementing a **specialized GraphQL schema storage system** that follows SyndrDB's existing architectural patterns:

- **Append-only writes** for fast updates and WAL compatibility
- **Tombstone markers** for versioning and soft deletes
- **Background compaction** to reclaim space
- **In-memory caching** for zero-latency schema access during query execution
- **Per-database file organization** for isolation and scalability

### 1.3 Core Architectural Principles

1. **Consistency:** Use the same patterns as bundle files and indexes
2. **Performance:** Sub-millisecond schema lookups, fast cold starts
3. **Durability:** Crash-safe with WAL integration
4. **Maintainability:** Self-compacting, automatic cleanup
5. **Compatibility:** Track breaking changes, support schema evolution

---

## 2. Goals & Success Criteria

### 2.1 Primary Goals

| Goal | Description | Success Metric |
|------|-------------|----------------|
| **Fast Cold Start** | Load all schemas quickly on server startup | < 100ms for 100 bundles |
| **Zero-Latency Lookups** | In-memory cache for hot path | < 5μs per schema lookup |
| **Fast Updates** | Append new schema versions quickly | < 2ms per update |
| **Space Efficiency** | Automatic cleanup of old versions | < 10MB per 1000 schemas |
| **Crash Safety** | Survive crashes without corruption | 100% recovery rate |
| **Breaking Change Detection** | Identify incompatible schema changes | 100% detection accuracy |

### 2.2 Non-Goals

- **NOT** a general-purpose document store (use bundles for that)
- **NOT** a distributed consensus system (single-server focus for v1)
- **NOT** a real-time schema migration tool (manual or scripted migrations)
- **NOT** supporting schema queries via GraphQL introspection initially (Phase 6+)

### 2.3 Success Criteria

**Phase 1 Complete:**
- ✅ Schema file can be created, written, and read
- ✅ File format validation passes
- ✅ Basic serialization/deserialization works

**Phase 2 Complete:**
- ✅ Tombstones can be written and read
- ✅ Schema versioning increments correctly
- ✅ Old schemas are properly marked as tombstoned

**Phase 3 Complete:**
- ✅ Compaction reduces file size by 20%+ on test data
- ✅ No data loss during compaction
- ✅ Compaction completes in < 50ms for 1000 schemas

**Phase 4 Complete:**
- ✅ Cold start loads 100 schemas in < 100ms
- ✅ In-memory cache serves lookups in < 5μs
- ✅ Cache invalidation works correctly

**Phase 5 Complete:**
- ✅ Bundle changes trigger schema updates
- ✅ GraphQL queries use cached schemas
- ✅ All integration tests pass

---

## 3. Requirements

### 3.1 Functional Requirements

**FR-1: File Management**
- The system MUST create one GraphQL schema file per database
- Files MUST be named `{database_name}_graphql.bgql`
- Files MUST be stored in the database's data directory
- Files MUST survive server restarts

**FR-2: Schema Operations**
- The system MUST support appending new schema records
- The system MUST support marking schemas as tombstoned
- The system MUST support reading all active schemas
- The system MUST support reading specific schemas by bundle name
- The system MUST increment schema versions automatically

**FR-3: Compaction**
- The system MUST compact files when tombstone ratio exceeds 30%
- Compaction MUST preserve all active schemas
- Compaction MUST retain tombstones within retention window (default: 7 days)
- Compaction MUST be atomic (no partial writes)
- Compaction MUST run in the background (non-blocking)

**FR-4: Caching**
- The system MUST load active schemas into memory on startup
- The system MUST invalidate cache entries when schemas change
- The system MUST support cache preloading for specific bundles

**FR-5: Breaking Change Detection**
- The system MUST detect field removals
- The system MUST detect type changes
- The system MUST detect nullability changes (non-null → null is OK, reverse is breaking)
- The system MUST log breaking changes
- The system MAY reject breaking changes in strict mode

**FR-6: Integration**
- The system MUST trigger schema updates when bundles are created
- The system MUST trigger schema updates when bundle fields change
- The system MUST trigger schema updates when relationships are added
- The system MUST integrate with the GraphQL handler

### 3.2 Non-Functional Requirements

**NFR-1: Performance**
- Cold start: Load 100 schemas in < 100ms
- Schema lookup: < 5μs (in-memory cache)
- Schema update: < 2ms (append operation)
- Compaction: < 50ms for 1000 schemas
- Memory footprint: < 100KB per cached schema

**NFR-2: Durability**
- All writes MUST be fsync'd to disk
- Files MUST be validated on load (checksum)
- System MUST recover from crashes without data loss
- System MUST recover from partial writes

**NFR-3: Scalability**
- Support up to 1,000 bundles per database
- Support up to 100 schema versions per bundle (before compaction)
- Support up to 100 databases

**NFR-4: Maintainability**
- Code MUST follow SyndrDB coding standards
- Code MUST include comprehensive tests
- Code MUST include detailed logging
- Code MUST include error context

**NFR-5: Compatibility**
- File format MUST support versioning
- File format MUST be backward compatible
- Schema changes MUST be tracked in metadata

### 3.3 Compatibility Requirements

**CR-1: GraphQL Compatibility**
- Schema MUST follow GraphQL spec (June 2018 or later)
- Type names MUST be valid GraphQL identifiers
- Field names MUST be valid GraphQL identifiers
- Directives MUST follow GraphQL directive syntax

**CR-2: SyndrDB Integration**
- MUST integrate with existing bundle service
- MUST integrate with existing database service
- MUST use existing logging infrastructure
- MUST use existing serialization formats (BSON preferred)

**CR-3: WAL Integration**
- Schema changes SHOULD be logged to WAL
- WAL replay SHOULD reconstruct schema files
- Schema files SHOULD be recoverable from WAL

---

## 4. Architectural Overview

### 4.1 System Context

```
┌─────────────────────────────────────────────────────────────┐
│                      SyndrDB Server                         │
│                                                             │
│  ┌──────────────┐         ┌──────────────────────────┐    │
│  │   GraphQL    │────────▶│  GraphQL Schema Manager  │    │
│  │   Handler    │         │                          │    │
│  └──────────────┘         │  - Schema Cache          │    │
│         │                 │  - Schema File Loader    │    │
│         │                 │  - Compaction Manager    │    │
│         ▼                 └──────────────────────────┘    │
│  ┌──────────────┐                     │                   │
│  │   Bundle     │                     │                   │
│  │   Service    │◀────────────────────┘                   │
│  └──────────────┘                                          │
│         │                                                  │
│         ▼                                                  │
│  ┌──────────────────────────────────────────────────┐    │
│  │          Disk Storage Layer                       │    │
│  │                                                    │    │
│  │  database1/                                        │    │
│  │  ├── database1_users.bnd                          │    │
│  │  ├── database1_posts.bnd                          │    │
│  │  └── database1_graphql.gql  ◄──── NEW             │    │
│  │                                                    │    │
│  │  database2/                                        │    │
│  │  ├── database2_orders.bnd                         │    │
│  │  └── database2_graphql.gql  ◄──── NEW             │    │
│  └──────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────┘
```

### 4.2 Component Breakdown

**GraphQLSchemaManager**
- Coordinates schema loading, caching, and updates
- Manages lifecycle of schema files
- Provides high-level API for schema operations

**GraphQLSchemaFile**
- Represents a single schema file on disk
- Handles low-level file I/O
- Implements append, tombstone, and compaction operations

**SchemaCache**
- Thread-safe in-memory cache
- Provides fast lookups (< 5μs)
- Handles cache invalidation

**SchemaCompactor**
- Background worker for compaction
- Monitors tombstone ratios
- Executes compaction asynchronously

**SchemaGenerator**
- Converts bundle structure to GraphQL schema
- Analyzes relationships
- Detects breaking changes

### 4.3 Data Flow

**Schema Creation Flow:**
```
Bundle Created
    ↓
BundleService.AddBundle()
    ↓
SchemaGenerator.GenerateSchema()
    ↓
GraphQLSchemaFile.AppendSchema()
    ↓
SchemaCache.Set()
```

**Schema Update Flow:**
```
Bundle Structure Changed
    ↓
BundleService.ApplyFieldChanges()
    ↓
SchemaGenerator.RegenerateSchema()
    ↓
GraphQLSchemaFile.TombstoneOldVersion()
    ↓
GraphQLSchemaFile.AppendNewVersion()
    ↓
SchemaCache.Invalidate()
    ↓
[Next GraphQL query triggers cache reload]
```

**Schema Query Flow:**
```
GraphQL Query Received
    ↓
GraphQLHandler.ProcessQuery()
    ↓
SchemaCache.Get("database:bundle")
    ↓
[Cache Hit] → Use Cached Schema
    ↓
[Cache Miss] → Load from GraphQLSchemaFile
    ↓
Execute Query with Schema
```

---

## 5. File Format Specification

### 5.1 File Structure

```
┌────────────────────────────────────────────────────────────┐
│                     File Header (256 bytes)                 │
├────────────────────────────────────────────────────────────┤
│              Optional: Schema Index (Variable)              │
├────────────────────────────────────────────────────────────┤
│                   Schema Record 1 (Variable)                │
├────────────────────────────────────────────────────────────┤
│                   Schema Record 2 (Variable)                │
├────────────────────────────────────────────────────────────┤
│                          ...                                │
├────────────────────────────────────────────────────────────┤
│                   Schema Record N (Variable)                │
└────────────────────────────────────────────────────────────┘
```

### 5.2 File Header Format (256 bytes)

```go
type FileHeader struct {
    // Magic number: "SNDR" (4 bytes)
    Magic           [4]byte       // Offset 0-3
    
    // Version info (4 bytes)
    FormatVersion   uint16        // Offset 4-5: File format version (current: 1)
    SchemaVersion   uint16        // Offset 6-7: GraphQL schema version supported
    
    // File type (1 byte)
    FileType        byte          // Offset 8: 0x03 = GraphQL Schema File
    
    // Flags (1 byte)
    Flags           byte          // Offset 9:
                                  //   Bit 0: HasSchemaIndex (0=no, 1=yes)
                                  //   Bit 1: CompressedRecords (0=no, 1=yes)
                                  //   Bit 2-7: Reserved
    
    // Padding (2 bytes)
    Reserved1       [2]byte       // Offset 10-11: Reserved for alignment
    
    // Database info (68 bytes)
    DatabaseName    [64]byte      // Offset 12-75: UTF-8, null-terminated
    DatabaseID      [4]byte       // Offset 76-79: Database UUID (first 4 bytes)
    
    // Record counts (24 bytes)
    TotalRecords    int64         // Offset 80-87: Total records ever written
    ActiveRecords   int64         // Offset 88-95: Currently active schemas
    TombstoneCount  int64         // Offset 96-103: Tombstoned records
    
    // Timestamps (24 bytes)
    CreatedAt       int64         // Offset 104-111: Unix timestamp (seconds)
    UpdatedAt       int64         // Offset 112-119: Unix timestamp (seconds)
    LastCompactedAt int64         // Offset 120-127: Unix timestamp (seconds)
    
    // Compaction config (16 bytes)
    CompactionThreshold float64   // Offset 128-135: Tombstone ratio trigger (0.3 = 30%)
    RetentionSeconds    int64     // Offset 136-143: Tombstone retention (604800 = 7 days)
    
    // Schema index info (16 bytes)
    SchemaIndexOffset   int64     // Offset 144-151: Byte offset to schema index
    SchemaIndexSize     int64     // Offset 152-159: Size of schema index in bytes
    
    // File integrity (8 bytes)
    HeaderChecksum  uint32        // Offset 160-163: CRC32 of bytes 0-159
    FileChecksum    uint32        // Offset 164-167: CRC32 of entire file
    
    // Padding to 256 bytes (88 bytes)
    Reserved2       [88]byte      // Offset 168-255: Reserved for future use
}
```

**Field Descriptions:**

- **Magic**: Always "SNDR" (0x534E4452). Used to validate file type.
- **FormatVersion**: File format version. Current: 1. Increment on breaking changes.
- **SchemaVersion**: GraphQL spec version. 1 = June 2018 spec.
- **FileType**: Type discriminator. 0x03 = GraphQL Schema File.
- **Flags**: Bitfield for features.
  - Bit 0: HasSchemaIndex (optional offset index for O(1) lookups)
  - Bit 1: CompressedRecords (future: use zstd compression)
  - Bits 2-7: Reserved
- **DatabaseName**: Name of database this file belongs to (null-terminated UTF-8).
- **DatabaseID**: First 4 bytes of database UUID (for validation).
- **TotalRecords**: Total number of records ever written (including tombstones).
- **ActiveRecords**: Number of currently active (non-tombstoned) schemas.
- **TombstoneCount**: Number of tombstoned records.
- **CreatedAt**: File creation timestamp (Unix seconds).
- **UpdatedAt**: Last modification timestamp.
- **LastCompactedAt**: Last compaction timestamp (0 = never compacted).
- **CompactionThreshold**: Tombstone ratio that triggers compaction (default: 0.3).
- **RetentionSeconds**: How long to keep tombstones before compaction purges them (default: 604800 = 7 days).
- **SchemaIndexOffset**: Byte offset to optional schema index (0 = no index).
- **SchemaIndexSize**: Size of schema index in bytes.
- **HeaderChecksum**: CRC32 of header bytes 0-159 (for header integrity).
- **FileChecksum**: CRC32 of entire file (updated during writes/compaction).

### 5.3 Schema Record Format

Each schema record is variable-length and structured as follows:

```go
type SchemaRecord struct {
    // Record metadata (32 bytes)
    RecordSize      uint32        // Total size of this record (including this field)
    RecordType      byte          // 0x01 = Active Schema, 0x02 = Tombstone
    RecordVersion   byte          // Record format version (current: 1)
    Reserved        [2]byte       // Alignment/future use
    RecordChecksum  uint32        // CRC32 of this record (excluding this field)
    
    // Schema identity (136 bytes)
    SchemaID        [16]byte      // UUID of this specific schema version
    BundleID        [16]byte      // UUID of the bundle this schema represents
    DatabaseName    [64]byte      // Database name (null-terminated)
    BundleName      [64]byte      // Bundle name (null-terminated)
    SchemaVersion   int64         // Version number (1, 2, 3, ...)
    
    // Timestamps (32 bytes)
    CreatedAt       int64         // Unix timestamp (seconds)
    UpdatedAt       int64         // Unix timestamp (seconds)
    TombstonedAt    int64         // Unix timestamp (0 if active)
    RetainUntil     int64         // Unix timestamp (0 = retain forever)
    
    // Metadata (16 bytes)
    BreakingChangeCount uint32    // Number of breaking changes from previous version
    FieldCount          uint32    // Number of fields in this schema
    ResolverCount       uint32    // Number of resolvers in this schema
    PayloadSize         uint32    // Size of serialized payload (BSON/JSON)
    
    // Variable-length payload (BSON-encoded)
    Payload         []byte        // Serialized GraphQLSchemaDefinition
}
```

**Record Layout on Disk:**
```
[4 bytes: RecordSize]
[1 byte: RecordType]
[1 byte: RecordVersion]
[2 bytes: Reserved]
[4 bytes: RecordChecksum]
[16 bytes: SchemaID]
[16 bytes: BundleID]
[64 bytes: DatabaseName]
[64 bytes: BundleName]
[8 bytes: SchemaVersion]
[8 bytes: CreatedAt]
[8 bytes: UpdatedAt]
[8 bytes: TombstonedAt]
[8 bytes: RetainUntil]
[4 bytes: BreakingChangeCount]
[4 bytes: FieldCount]
[4 bytes: ResolverCount]
[4 bytes: PayloadSize]
[N bytes: Payload (BSON)]
```

**Total Fixed Overhead:** 220 bytes per record (before payload)

### 5.4 Schema Payload Format (BSON-Encoded)

The payload is a Binary-serialized `GraphQLSchemaDefinition` structure, using the same fast
binary serialization used by SyndrDB for bundle files:

```go
type GraphQLSchemaDefinition struct {
    // GraphQL Type Info
    TypeName      string                    // "User", "Post", etc.
    Description   string                    // Human-readable description
    
    // Fields
    Fields        []GraphQLField            // List of fields in this type
    
    // Resolvers
    Resolvers     map[string]ResolverConfig // Field name → resolver config
    
    // Directives
    Directives    []string                  // @deprecated, @auth, etc.
    
    // Breaking Changes (from previous version)
    BreakingChanges []BreakingChange
    
    // Deprecation Notices
    DeprecationNotices []DeprecationNotice
}

type GraphQLField struct {
    Name              string        // Field name (must be valid GraphQL identifier)
    Type              string        // GraphQL type: "String!", "Int", "[User!]!", etc.
    BundleField       string        // Corresponding field in bundle
    Description       string        // Field description
    DefaultValue      interface{}   // Default value (if any)
    IsDeprecated      bool          // Marked as deprecated?
    DeprecationReason string        // Why deprecated
    Arguments         []FieldArgument // Field arguments (for queries/mutations)
}

type FieldArgument struct {
    Name         string        // Argument name
    Type         string        // GraphQL type
    Description  string        // Argument description
    DefaultValue interface{}   // Default value
}

type ResolverConfig struct {
    Type              ResolverType      // DIRECT, RELATIONSHIP, SCRIPTED
    
    // For RELATIONSHIP resolvers
    TargetBundle      string            // Target bundle name
    TargetDatabase    string            // Target database (if cross-DB)
    JoinCondition     JoinCondition     // How to join
    Cardinality       string            // "ONE", "MANY"
    
    // For SCRIPTED resolvers
    Script            string            // Resolver script (expr/Lua/SyndrQL)
    CompiledScript    []byte            // Pre-compiled script (if applicable)
    
    // Performance hints
    CacheTTL          int64             // Cache TTL in seconds (0 = no cache)
    UseDataLoader     bool              // Should use DataLoader batching?
}

type ResolverType int

const (
    DirectResolver        ResolverType = 1  // Direct field mapping
    RelationshipResolver  ResolverType = 2  // Join with another bundle
    ScriptedResolver      ResolverType = 3  // Custom script
    ComputedResolver      ResolverType = 4  // Computed field (e.g., fullName from firstName + lastName)
)

type JoinCondition struct {
    LeftField     string   // Field in this bundle
    RightField    string   // Field in target bundle
    JoinType      string   // "INNER", "LEFT", "RIGHT"
}

type BreakingChange struct {
    ChangeType    string   // "FIELD_REMOVED", "TYPE_CHANGED", "MADE_REQUIRED", "ARG_REMOVED"
    FieldName     string   // Field affected
    OldValue      string   // Old value (e.g., old type)
    NewValue      string   // New value (e.g., new type)
    DetectedAt    int64    // Unix timestamp
    Severity      string   // "BREAKING", "DANGEROUS", "DEPRECATED"
}

type DeprecationNotice struct {
    FieldName     string   // Deprecated field
    Reason        string   // Why deprecated
    DeprecatedAt  int64    // Unix timestamp
    RemovalTarget int64    // Expected removal timestamp (0 = unknown)
}
```

**BSON Encoding Benefits:**
- Compact binary format (~30% smaller than JSON)
- Fast serialization/deserialization
- Well-supported in Go 
- Type preservation (no string→number ambiguity)
- Already used in SyndrDB bundle files

### 5.5 Optional: Schema Index Format

For fast lookups without scanning the entire file, an optional schema index can be included:

```go
type SchemaIndex struct {
    IndexVersion   uint16              // Index format version (current: 1)
    EntryCount     uint32              // Number of entries
    Entries        []SchemaIndexEntry  // Array of entries
}

type SchemaIndexEntry struct {
    BundleName     [64]byte   // Bundle name (null-terminated)
    FileOffset     int64      // Byte offset to schema record
    RecordSize     uint32     // Size of schema record
    SchemaVersion  int64      // Version number
    IsTombstone    bool       // Is this entry a tombstone?
}
```

**Index Layout:**
```
[2 bytes: IndexVersion]
[4 bytes: EntryCount]
[82 bytes × N: Entries]
```

**When to Use:**
- Use index if file contains > 100 schemas (for O(1) lookup)
- Don't use index for small files (linear scan is faster)
- Index is rebuilt during compaction

### 5.6 File Validation Rules

**On File Open:**
1. Read header (256 bytes)
2. Validate magic number == "SNDR"
3. Validate FormatVersion <= current supported version
4. Validate FileType == 0x03 (GraphQL Schema)
5. Validate HeaderChecksum (CRC32 of bytes 0-159)
6. Optionally validate FileChecksum (if performance allows)

**On Record Read:**
1. Read RecordSize (4 bytes)
2. Read remaining record (RecordSize - 4 bytes)
3. Validate RecordChecksum (CRC32 of record excluding checksum field)
4. Validate RecordVersion <= current supported version
5. Deserialize Payload (BSON)

**On Compaction:**
1. Validate entire file before compaction
2. Write to temp file with new checksums
3. Validate temp file
4. Atomic rename (temp → actual)
5. Validate actual file

---

## 6. Implementation Phases

### Phase 1: File Structure & Basic I/O (Week 1)

**Objective:** Implement core file I/O operations and file format.

#### 1.1 File Header Implementation

**Task 1.1.1: Define Header Struct**


**Task 1.1.2: Unit Tests for Header**


#### 1.2 Schema Record Implementation

**Task 1.2.1: Define Record Structs**


**Task 1.2.2: Unit Tests for Records**


#### 1.3 GraphQL Schema File Implementation

**Task 1.3.1: Define File Struct**


**Task 1.3.2: Unit Tests for File Operations**


**Phase 1 Deliverables:**
- ✅ File header serialization/deserialization
- ✅ Schema record serialization/deserialization
- ✅ File creation and loading
- ✅ Basic validation
- ✅ Unit tests (90%+ coverage)

**Phase 1 Acceptance Criteria:**
- All unit tests pass
- Files can be created, written, and read back
- Checksums validate correctly
- No memory leaks

---

### Phase 2: Tombstone Management & Versioning (Week 2)

**Objective:** Implement schema versioning with tombstone markers.

#### 2.1 Append Schema Operation

**Task 2.1.1: Implement AppendSchema**
