# ⚠️ DEPRECATED: hashindexV2 Package

## Status: DEPRECATED - DO NOT USE

This package (`hashindexV2`) has been **DEPRECATED** and replaced by `hashindexV3`.

## Replacement

**Use instead:** `src/internal/domain/index/hashindexV3`

## Why was it deprecated?

### hashindexV2 (Old - Bucket-based)
- Traditional PostgreSQL-style linear hashing
- Bucket-based architecture with overflow pages
- Complex in-place page management
- Requires sophisticated page splitting/merging logic
- More complex crash recovery

### hashindexV3 (New - LSM-based)
- Modern LSM (Log-Structured Merge) architecture
- Append-only writes (simpler and faster)
- MemTable + on-disk entry storage
- Better write performance
- Simpler crash recovery
- More suitable for SyndrDB's append-only document model

## Migration Status

✅ All active code references to `hashindexV2` have been commented out or removed.

The following files still reference the old implementation:
- Comments in `bundle_service.go` (preserved for reference)
- Internal package self-references (will be removed when package is deleted)

## Timeline

- **Phase 11:** hashindexV3 implemented and tested
- **Current:** hashindexV2 marked as deprecated
- **Phase 12+:** Complete removal of hashindexV2 package

## What to do if you're reading this

If you're considering using hash indexes:
1. Import `hashindexV3` instead: `import "syndrdb/src/internal/domain/index/hashindexV3"`
2. Use the LSM-style API: `OpenHashIndexV3()`, `NewHashIndexV3()`, etc.
3. Refer to `hashindexV3/hash_index_api.go` for documentation

## Related Documentation

- `hashindexV3/hash_index_api.go` - Main API documentation
- `docs/hash_index_migration.md` - Migration guide (if exists)
- Architecture decision records in `docs/`

---

Last updated: November 18, 2025
