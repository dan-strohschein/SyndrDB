# Multi-File Storage Integration Status

## What's Already Done ✅

All the **components** for multi-file storage are implemented and working:

1. ✅ **File Rotation** - `AppendDocumentToBundleFileWithTxID` checks file size and creates new segments
2. ✅ **Manifest Management** - Tracks all segment files with metadata
3. ✅ **Multi-File Read Path** - `LoadDocumentPage` merges from multiple files (needs verification)
4. ✅ **Compaction System** - BundleCompactor with merge + tombstone removal
5. ✅ **Worker Pool** - CompactionScheduler with 3 parallel workers
6. ✅ **I/O Throttling** - Token bucket algorithm ready to use
7. ✅ **Bloom Filters** - Serialization/deserialization working
8. ✅ **Integration Tests** - All tests passing

## What's Missing ❌

The system is **wired up but not triggered**. Here's what needs to happen:

### 1. **Compaction Trigger Integration** (CRITICAL)

The `CompactionScheduler.EvaluateAndSchedule()` method exists but is **never called**. This means:
- Files will rotate and accumulate ✅
- Compaction workers are running ✅
- But **no compaction tasks are ever scheduled** ❌

**What needs to happen:**
```go
// After successful writes, trigger compaction evaluation
func (b *BundleStorageEngine) AppendDocumentToBundleFileWithTxID(...) {
    // ... existing write logic ...
    
    // NEW: Trigger compaction evaluation after write
    // This is PostgreSQL autovacuum-style - check triggers after mutations
    go b.evaluateCompactionNeeded(bundle.Database.Name, bundle.Name)
    
    return pageID, nil
}
```

### 2. **Periodic Compaction Evaluation** (OPTIONAL BUT RECOMMENDED)

Even if writes stop, compaction should still run for files that meet trigger criteria.

**What needs to happen:**
```go
// In NewBundleStore(), start a background goroutine
go store.periodicCompactionEvaluator()

// New method
func (b *BundleStorageEngine) periodicCompactionEvaluator() {
    ticker := time.NewTicker(60 * time.Second) // Every minute
    defer ticker.Stop()
    
    for range ticker.C {
        // Evaluate all bundles for compaction
        b.manifestManagersMutex.Lock()
        for key, mgr := range b.manifestManagers {
            dbName, bundleName := parseManagerKey(key)
            go b.compactionScheduler.EvaluateAndSchedule(mgr, dbName, bundleName)
        }
        b.manifestManagersMutex.Unlock()
    }
}
```

### 3. **Verify Multi-File Read Path** (NEEDS TESTING)

The `LoadDocumentPage` function needs to be verified that it:
- Scans ALL segment files (not just the first one)
- Merges documents with last-write-wins
- Respects bloom filter hints to skip files
- Filters tombstones properly

**Location to check:**
- File: `bundle_storage_engine.go`
- Function: `LoadDocumentPage`
- Expected behavior: Iterate through `manifest.Files`, check bloom filters, merge results

### 4. **Bloom Filter Usage in Read Path** (NOT YET IMPLEMENTED)

Bloom filters are serialized in manifests but **not used during reads**.

**What needs to happen:**
```go
func (b *BundleStorageEngine) LoadDocumentPage(...) {
    // ...
    for _, fileInfo := range manifest.Files {
        // NEW: Check bloom filter before reading file
        if fileInfo.BloomFilterData != "" {
            bf, err := DeserializeBloomFilter(
                fileInfo.BloomFilterData,
                fileInfo.BloomFilterSize,
                fileInfo.BloomFilterHashes,
            )
            if err == nil && !bf.MayContain(documentID) {
                continue // Skip this file - document definitely not here
            }
        }
        
        // Read file if bloom filter says "maybe contains"
        // ...
    }
}
```

## Files That Need Changes

### 1. `src/internal/storage/bundlestore/bundle_storage_engine.go`

**Add after line ~1540 (in AppendDocumentToBundleFileWithTxID):**
```go
// Trigger compaction evaluation asynchronously
// Don't block the write path - let workers handle it
go func() {
    if b.compactionScheduler != nil {
        b.compactionScheduler.EvaluateAndSchedule(
            manifestMgr,
            bundle.Database.Name,
            bundle.Name,
        )
    }
}()
```

**Add new method (around line 1600):**
```go
// periodicCompactionEvaluator runs background compaction checks
// PostgreSQL autovacuum-inspired: check all bundles periodically
func (b *BundleStorageEngine) periodicCompactionEvaluator(ctx context.Context) {
    ticker := time.NewTicker(60 * time.Second)
    defer ticker.Stop()
    
    for {
        select {
        case <-ctx.Done():
            return
        case <-ticker.C:
            b.evaluateAllBundlesForCompaction()
        }
    }
}

func (b *BundleStorageEngine) evaluateAllBundlesForCompaction() {
    b.manifestManagersMutex.Lock()
    managers := make(map[string]*ManifestManager)
    for key, mgr := range b.manifestManagers {
        managers[key] = mgr
    }
    b.manifestManagersMutex.Unlock()
    
    for key, mgr := range managers {
        parts := strings.SplitN(key, ":", 2)
        if len(parts) != 2 {
            continue
        }
        dbName, bundleName := parts[0], parts[1]
        
        // Async evaluation - don't block ticker
        go func(m *ManifestManager, db, bundle string) {
            if b.compactionScheduler != nil {
                b.compactionScheduler.EvaluateAndSchedule(m, db, bundle)
            }
        }(mgr, dbName, bundleName)
    }
}
```

**Update NewBundleStore (around line 138):**
```go
store.compactionScheduler.Start()

// NEW: Start periodic compaction evaluator
ctx, cancel := context.WithCancel(context.Background())
store.compactionContext = ctx
store.compactionCancel = cancel
go store.periodicCompactionEvaluator(ctx)
```

**Add to BundleStorageEngine struct (around line 47):**
```go
type BundleStorageEngine struct {
    // ... existing fields ...
    
    // NEW: Context for graceful compaction shutdown
    compactionContext context.Context
    compactionCancel  context.CancelFunc
}
```

### 2. Verify Read Path (CHECK EXISTING CODE)

**In LoadDocumentPage, ensure it:**
1. Gets manifest with `getOrCreateManifestManager()`
2. Iterates through `manifest.Files` (not just legacy single file)
3. Uses bloom filters to skip files
4. Merges results with last-write-wins

**If not implemented, add bloom filter check:**
```go
// Before reading each file in LoadDocumentPage
if fileInfo.BloomFilterData != "" {
    bf, _ := DeserializeBloomFilter(...)
    if bf != nil && !bf.MayContain(documentID) {
        continue // Skip file
    }
}
```

## Testing After Integration

Once the above changes are made, test this workflow:

### Test 1: File Rotation
```bash
# Create bundle with small max file size
SET SYSTEM bundle_file_max_size_mb = 1;

# Insert 100,000 documents (should create multiple files)
INSERT INTO users (id, name, email) VALUES (...);  # x100,000

# Verify multiple files created
ls -lh data_files/testdb/users/
# Should see: 000001.bnd, 000002.bnd, 000003.bnd, etc.

# Check manifest
cat data_files/testdb/users/bundle.manifest
# Should show multiple files with stats
```

### Test 2: Compaction Triggers
```bash
# Delete 30% of documents (tombstone ratio trigger)
DELETE FROM users WHERE id < 30000;

# Wait 60 seconds for periodic evaluator
sleep 60

# Check logs for compaction activity
tail -f log_files/*ServerLog.txt
# Should see: "Compaction triggered" messages

# Verify files were compacted
ls -lh data_files/testdb/users/
# Should have fewer files after compaction
```

### Test 3: Multi-File Reads
```bash
# Query document that might be in any file
SELECT * FROM users WHERE id = '12345';

# Should work correctly regardless of which file contains it
# Check logs to see multiple files being scanned
```

### Test 4: Bloom Filter Optimization
```bash
# Query non-existent document
SELECT * FROM users WHERE id = 'nonexistent';

# Check logs - should see bloom filter skips
# Should NOT see: "Reading all files sequentially"
# Should see: "Skipped N files via bloom filter"
```

## Summary

**Current State:** All components are built, compaction workers are running, but **compaction is never triggered**.

**What You Need to Do:**
1. ✅ Add compaction trigger after writes (5 lines)
2. ✅ Add periodic compaction evaluator (30 lines)
3. ⚠️ Verify multi-file read path works
4. ⚠️ Add bloom filter optimization to reads (10 lines)

**Total Code to Add:** ~50 lines across 1 file

**Once Done:** The system will automatically:
- Rotate files when they hit 32MB
- Trigger compaction when tombstone ratio > 20%
- Merge small files to improve read performance
- Use bloom filters to skip files during queries
- Run compaction workers in parallel (3 workers)
- Throttle I/O to prevent resource starvation

This is what I meant by "Ready for integration with BundleStorageEngine" - all the **hard parts are done**, just need to **wire up the triggers**.
