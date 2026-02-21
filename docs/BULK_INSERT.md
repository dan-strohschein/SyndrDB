# Bulk Insert System

SyndrDB supports inserting multiple documents in a single batch operation via the `BULK` keyword prefix. This enables high-throughput ingestion of thousands of records per second per bundle.

## Syntax

Two equivalent syntaxes are supported. Both require the `BULK` keyword to explicitly opt in to the optimized batch insert path.

### SyndrDB-native syntax

```sql
BULK ADD DOCUMENTS TO BUNDLE "<BUNDLE_NAME>" WITH (
  ({<FIELD> = <VALUE>}, {<FIELD> = <VALUE>}),
  ({<FIELD> = <VALUE>}, {<FIELD> = <VALUE>}),
  ...
);
```

### SQL-style alias

```sql
BULK INSERT INTO BUNDLE "<BUNDLE_NAME>" VALUES (
  ({<FIELD> = <VALUE>}, {<FIELD> = <VALUE>}),
  ({<FIELD> = <VALUE>}, {<FIELD> = <VALUE>}),
  ...
);
```

Each parenthesized group `({f=v}, {f=v})` represents one document. Documents are comma-separated within the outer parentheses.

## Examples

```sql
BULK ADD DOCUMENTS TO BUNDLE "Users" WITH (
  ({"name" = "Alice"}, {"age" = 28}, {"email" = "alice@example.com"}),
  ({"name" = "Bob"}, {"age" = 34}, {"email" = "bob@example.com"}),
  ({"name" = "Charlie"}, {"age" = 42}, {"email" = "charlie@example.com"})
);

BULK INSERT INTO BUNDLE "Products" VALUES (
  ({"sku" = "ABC123"}, {"price" = 19.99}, {"active" = true}),
  ({"sku" = "DEF456"}, {"price" = 29.99}, {"active" = false})
);
```

## Limits

| Limit | Value | Constant |
|-------|-------|----------|
| Max documents per batch | 10,000 | `constants.MaxDocumentsPerBulkInsert` |
| Max fields per document | 500 | `constants.MaxFieldsPerDocument` |

Field name length and field value length limits from `ValidateFieldName` / `ValidateFieldValue` apply per-document.

## Error Semantics

**All-or-nothing.** Every document in the batch is validated upfront before any storage writes occur. If any document fails validation (schema mismatch, unique constraint violation, field limit exceeded), the entire batch is rejected and no documents are inserted.

Validation phases run in order:
1. `processNullValues` -- apply default values for missing/null required fields
2. `validateDocumentFields` -- check types against bundle schema
3. `ValidateUniqueConstraints` -- check unique indexes

## Transaction Support

Bulk inserts work within explicit transactions:

```sql
BEGIN TRANSACTION;
BULK ADD DOCUMENTS TO BUNDLE "Users" WITH (
  ({"name" = "Alice"}, {"age" = 28}),
  ({"name" = "Bob"}, {"age" = 34})
);
COMMIT;
```

Within a transaction:
- Document IDs are pre-allocated and document-level write locks are acquired (sorted to prevent deadlocks)
- MVCC version fields are set (`CreatedByTxID`, `VersionSequence`)
- The batch is logged as a single `OpBatchInsert` WAL entry
- Document locations are tracked in the session's `TransactionBuffer`
- Locks are released on `COMMIT` or `ROLLBACK`

For auto-commit (no explicit transaction), the batch is wrapped in `WALManager.ExecuteWithLogging` which handles WAL logging and commit/rollback automatically.

## Execution Phases

The `BundleService.AddDocumentsToBundle` method processes the batch in eight phases:

| Phase | Description |
|-------|-------------|
| 1. Validate | All documents validated upfront (null processing, schema, unique constraints) |
| 2. Create | Documents created via `documentFactory.NewDocument` (uses document pool) |
| 3. Storage | Each document written via `AddDocumentToBundleFile` (lock-free `WriteDirectAtomic`) |
| 4. Cache | Page cache updated per document via `updatePageCacheWithDocument` (write-through) |
| 5. Metadata | Single `scheduleMetadataUpdate("increment_docs", N)` for the entire batch |
| 6. Statistics | `statsUpdater.IncrementalUpdate` per document per field |
| 7. Indexes | `scheduleIndexUpdate` per document per index (deferred, flushed after return) |
| 8. Invalidation | Single `invalidateBundleCaches` call for the bundle |

Key optimization: bundle lookup, schema fetch, and unique constraint validator creation happen once per batch, not per document.

## WAL Integration

Bulk inserts are logged as a single `OpBatchInsert` WAL entry containing:
- Bundle name
- Document count
- All document data (marshaled via `hvjson.Marshal`)

This is more efficient than N individual `OpInsert` entries and enables atomic replay during crash recovery.

## Permission

The `BULK` command requires `Write` permission (same as `ADD DOCUMENT`).

## Architecture

### Files

| File | Role |
|------|------|
| `src/internal/syndrQL/token.go` | `TOKEN_BULK` keyword |
| `src/internal/syndrQL/insert_parser.go` | `ParseBulkAdd()`, `ParseBulkInsert()`, `parseDocumentList()` |
| `src/internal/syndrQL/adapter.go` | `ToBulkDocumentCommand()` |
| `src/internal/domain/models/models.go` | `BulkDocumentCommand` struct |
| `src/internal/server/parser_integration_insert_V3.go` | `parseBulkCommand()` entry point |
| `src/internal/server/document_operations.go` | `BulkAddDocuments()` command handler |
| `src/internal/server/command_director.go` | Routes `BULK` prefix to handler |
| `src/internal/domain/bundle/bundle_service_document_ops.go` | `AddDocumentsToBundle()`, `AddDocumentsToBundleWithTxID()` |
| `src/internal/journal/journal.go` | `OpBatchInsert` constant |
| `src/internal/journal/wal_manager.go` | `LogBatchInsert()` method |
| `src/pkg/constants/constants.go` | `MaxDocumentsPerBulkInsert` |

### Data Flow

```
Client command
  -> CommandDirector (detects "bulk" prefix)
    -> BulkAddDocuments (command handler)
      -> parseBulkCommand (tokenize + parse)
        -> ParseBulkAdd / ParseBulkInsert
        -> ToBulkDocumentCommand (adapter)
      -> BundleService.AddDocumentsToBundle (8-phase engine)
      -> WALManager.LogBatchInsert (single WAL entry)
      -> ForceFlushIndexUpdates
      -> metrics + plan cache invalidation
```
