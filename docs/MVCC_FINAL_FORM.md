# MVCC Implementation Plan for SyndrDB
## Comprehensive Roadmap: Crash Fix to Full Snapshot Isolation

---

## Executive Summary

This plan addresses the immediate concurrent map iteration crash while building toward full MVCC (Multi-Version Concurrency Control) snapshot isolation. The approach leverages SyndrDB's existing infrastructure:
- Multi-file append-only storage ✓
- Tombstone-based deletion ✓
- Background compaction ✓
- Manifest version tracking ✓

**Missing pieces**: Document-level version metadata, transaction-aware writes, snapshot-based read filtering.

**Timeline**: 
- Phase 1 (Immediate): 1-2 days
- Phase 2 (Foundation): 1 week
- Phase 3 (Write Path): 1 week
- Phase 4 (Read Path): 2 weeks
- Phase 5 (Compaction): 1 week
- Phase 6 (Testing): 1 week

**Total Effort**: 6-8 weeks to production-ready MVCC

---

## PHASE 1: Immediate Crash Fix (Map Iteration Safety)
**Goal**: Stop concurrent map crashes without sacrificing 70ns write performance  
**Timeline**: 1-2 days  
**Priority**: CRITICAL

### Task 1.1: Audit All Map Iteration Points
**Objective**: Identify every location where `*bundle.Documents` map is iterated without lock protection

#### Step 1.1.1: Search for Direct Map Iterations
- Use grep/semantic search to find all `range *bundle.Documents` patterns
- Document each location with file path, line number, and calling context
- Note which iterations are in read paths vs write paths
- Identify which functions are called from hot paths (query execution, updates)

#### Step 1.1.2: Analyze Lock Acquisition Patterns
- Review existing `DocumentsMutex.RLock()` and `DocumentsMutex.Lock()` usage
- Map out which functions properly acquire locks before iteration
- Identify gaps where locks should be held but aren't
- Document lock ordering to prevent future deadlocks

#### Step 1.1.3: Measure Iteration Frequency
- Add temporary metrics/logging to count map iteration frequency
- Measure average map size during iterations
- Calculate expected copy overhead for snapshot approach
- Determine if copy-on-read is acceptable for performance

### Task 1.2: Implement Copy-on-Read Pattern
**Objective**: Create lockless map snapshots for safe concurrent iteration

#### Step 1.2.1: Implement Snapshot Helper Function
- Create `snapshotMemtable()` helper in BundleService
- Function acquires read lock, copies map, releases lock immediately
- Return shallow copy of map (documents themselves are immutable once written)
- Add instrumentation to measure lock hold time

#### Step 1.2.2: Replace Live Iterations with Snapshots
- Modify `getAllDocumentsForIndexing()` to use snapshot
- Update `GetDocumentsByFilter()` if it iterates memtable
- Fix any other identified iteration points from audit
- Ensure snapshot is used for merge operations, not live map

#### Step 1.2.3: Add Safety Assertions
- Add debug-mode checks that panic if map iteration occurs without lock
- Implement runtime detection of concurrent access in test environments
- Add metric tracking for map copy operations
- Log warnings if copy operations take longer than expected threshold

### Task 1.3: Verify Fix with Concurrency Testing
**Objective**: Ensure crash is resolved and performance is maintained

#### Step 1.3.1: Create Stress Test Suite
- Build concurrent read/write test with 1000+ goroutines
- Test writes at 70ns target rate while simultaneous reads occur
- Run test for extended duration (30+ minutes) to detect race conditions
- Use `-race` detector to validate concurrent safety

#### Step 1.3.2: Benchmark Performance Impact
- Measure write latency before and after fix
- Verify 70ns average write time is maintained
- Measure read path latency impact from map copying
- Profile memory allocation patterns to ensure no regression

#### Step 1.3.3: Production Validation
- Deploy to staging environment with production-like load
- Monitor for any new concurrent access panics
- Measure query latency at 95th and 99th percentiles
- Validate no deadlocks or lock contention introduced

---

## PHASE 2: Document Version Metadata Foundation
**Goal**: Add MVCC version tracking fields to Document struct and storage format  
**Timeline**: 1 week  
**Priority**: HIGH

### Task 2.1: Extend Document Structure
**Objective**: Add version metadata fields without breaking existing storage

#### Step 2.1.1: Define Version Metadata Fields
- Add `CreatedByTxID uint64` to track creating transaction
- Add `DeletedByTxID uint64` to track deletion (0 = not deleted)
- Add `CommitTimestamp uint64` for visibility determination
- Add `VersionSequence uint64` for ordering within document ID
- Document that 0 values mean "not set" for backward compatibility

#### Step 2.1.2: Update Serialization Format
- Modify fast binary encoder to serialize new fields
- Modify fast binary decoder to read new fields (default to 0 if missing)
- Ensure backward compatibility - old files without fields still load
- Update magic numbers or version markers if needed for format detection

#### Step 2.1.3: Update Storage Engine Integration
- Modify `AppendDocumentToBundleFile` to write version fields
- Modify `parseAppendedDocumentsRange` to read version fields
- Update document validation to check version field consistency
- Add migration helper to set version fields on legacy documents during reads

#### Step 2.1.4: Create Version Field Accessors
- Add helper methods to Document struct for version field access
- Create `IsVisible(snapshotID uint64)` method for visibility checks
- Add `IsDeleted()` method that checks DeletedByTxID
- Implement `GetLatestVersion()` to find newest version in version chain

### Task 2.2: Implement Global Transaction Counter
**Objective**: Create centralized transaction ID allocation for version tracking

#### Step 2.2.1: Create Transaction ID Generator
- Implement atomic uint64 counter starting at 1
- Ensure counter is separate from existing GlobalSequence
- Make counter increment thread-safe using sync/atomic
- Add GetNextTransactionID() method to WALManager or new TransactionManager

#### Step 2.2.2: Persist Transaction Counter State
- Store current counter value in system metadata bundle
- On server startup, recover counter from metadata
- Set counter to max(recovered_value) + 1000 for safety margin
- Log recovery statistics (last transaction ID, source)

#### Step 2.2.3: Integrate with WAL Recovery
- During crash recovery, scan WAL for highest transaction ID referenced
- Compare WAL transaction ID with metadata transaction ID
- Use maximum of both values plus safety margin as starting counter
- Validate no transaction ID reuse across server restarts

### Task 2.3: Track Transaction State
**Objective**: Maintain in-memory registry of active transactions

#### Step 2.3.1: Create Active Transaction Registry
- Implement sync.Map-based registry for concurrent access
- Store transaction metadata: ID, session ID, start timestamp, status
- Define status enum: ACTIVE, COMMITTING, COMMITTED, ABORTED
- Add methods: RegisterTransaction(), DeregisterTransaction(), GetStatus()

#### Step 2.3.2: Integrate with BEGIN TRANSACTION
- When BEGIN executes, allocate new transaction ID from counter
- Register transaction in active registry with ACTIVE status
- Store transaction ID in session context
- Return transaction ID to client for tracking

#### Step 2.3.3: Integrate with COMMIT/ROLLBACK
- On COMMIT, update status to COMMITTING, then COMMITTED
- On ROLLBACK, update status to ABORTED
- Keep committed/aborted transactions in registry briefly (30 seconds)
- Background cleanup removes old transaction metadata periodically

---

## PHASE 3: Transaction-Aware Write Path
**Goal**: Wire transaction IDs through write operations to create versioned documents  
**Timeline**: 1 week  
**Priority**: HIGH

### Task 3.1: Modify Document Insert Path
**Objective**: Store transaction context with every document write

#### Step 3.1.1: Pass Transaction ID Through Call Chain
- Modify `InsertDocument` to accept optional transaction ID parameter
- Pass transaction ID from command executor to bundle service
- Thread transaction ID through to storage engine
- Default to 0 (autocommit) when not in explicit transaction

#### Step 3.1.2: Set Version Fields on Insert
- When creating document, set CreatedByTxID to current transaction ID
- Set CommitTimestamp to 0 (uncommitted)
- Set VersionSequence using monotonic counter
- Leave DeletedByTxID as 0 (not deleted)

#### Step 3.1.3: Write Version Metadata to Disk
- Use existing `AppendDocumentToBundleFileWithTxID` infrastructure
- Ensure version fields are serialized in append-only format
- Update manifest to track min/max transaction IDs per file
- Verify version fields survive flush/reload cycle

### Task 3.2: Implement Document Update as New Version
**Objective**: Updates create new document versions instead of modifying in place

#### Step 3.2.1: Change Update Semantics
- Instead of modifying existing document, insert new version with same DocumentID
- Set new version's CreatedByTxID to current transaction ID
- Set new version's VersionSequence to previous + 1
- Keep old version in storage (compaction will clean up later)

#### Step 3.2.2: Update Memtable Handling
- Memtable can contain multiple versions of same DocumentID
- Use composite key `DocumentID:VersionSequence` for memtable storage
- On flush, write all versions to disk
- Read path must handle multiple versions and choose appropriate one

#### Step 3.2.3: Handle Hash Index Updates
- Hash index must track latest committed version per document ID
- On update, remove old version from index, add new version
- Use version fields to determine which entry is "latest"
- Ensure index consistency during transaction commit

### Task 3.3: Implement Deletion as Tombstone Version
**Objective**: Deletes create tombstone versions instead of removing documents

#### Step 3.3.1: Create Tombstone Version on Delete
- When deleting, create new document version with DeletedByTxID set
- Set CreatedByTxID to transaction that performed delete
- Keep document data intact for potential rollback
- Write tombstone version to append-only storage

#### Step 3.3.2: Update Index Deletion Behavior
- Hash index removes entry but tombstone version remains on disk
- BTree index marks entry as deleted with version information
- Compaction later removes physical storage after no transactions need it
- Ensure tombstone is counted in manifest statistics

#### Step 3.3.3: Handle Delete-Then-Insert of Same ID
- Allow deletion and recreation of same DocumentID within transaction
- Version sequence increments across delete/insert boundary
- Read path must recognize newest version regardless of delete history
- Test edge case of multiple delete/insert cycles in same transaction

### Task 3.4: Implement Commit Timestamp Assignment
**Objective**: Atomically make all transaction versions visible on commit

#### Step 3.4.1: Generate Commit Timestamp
- On COMMIT, allocate commit sequence from global counter
- Commit sequence must be globally unique and monotonically increasing
- Use same counter as transaction IDs or separate sequence
- Ensure commit sequence is durably logged to WAL before proceeding

#### Step 3.4.2: Update All Transaction Versions
- Scan memtable for all documents with CreatedByTxID = committing transaction
- Set CommitTimestamp to commit sequence on all versions
- Flush updated versions to disk in atomic operation
- Update indexes to reflect committed versions

#### Step 3.4.3: Handle Commit Failure Scenarios
- If commit timestamp assignment fails, transaction remains uncommitted
- Implement timeout for commit operations (prevent hanging)
- On commit failure, mark transaction as ABORTED and trigger rollback
- Ensure no partially-committed state visible to other transactions

---

## PHASE 4: Snapshot Isolation for Reads
**Goal**: Implement point-in-time read consistency using transaction snapshots  
**Timeline**: 2 weeks  
**Priority**: MEDIUM

### Task 4.1: Implement Snapshot Manager
**Objective**: Create snapshots representing point-in-time database state

#### Step 4.1.1: Define Snapshot Structure
- Snapshot contains: snapshot sequence, transaction ID, list of active transaction IDs
- Snapshot sequence represents commit boundary (see all commits <= this sequence)
- Active transaction list contains IDs of transactions in-flight at snapshot time
- Snapshot stored in session context for duration of transaction

#### Step 4.1.2: Create Snapshot on BEGIN TRANSACTION
- Capture current global sequence as snapshot boundary
- Query active transaction registry for all ACTIVE transaction IDs
- Store active list in snapshot (these remain invisible even if they commit later)
- Assign snapshot to session and store for all subsequent reads

#### Step 4.1.3: Implement Snapshot Lifecycle Management
- Snapshot created on BEGIN, used for all reads in transaction
- Same snapshot used across multiple queries (repeatable read guarantee)
- Snapshot cleared on COMMIT or ROLLBACK
- For autocommit operations, create snapshot per statement

#### Step 4.1.4: Track Oldest Active Snapshot
- Maintain global tracking of oldest snapshot currently in use
- Used by compaction to determine which versions must be preserved
- Update on snapshot creation and destruction
- Expose via GetOldestActiveSnapshot() API for compaction manager

### Task 4.2: Implement Visibility Rules
**Objective**: Determine which document versions are visible to a given snapshot

#### Step 4.2.1: Define Core Visibility Logic
- Version visible if CommitTimestamp > 0 AND CommitTimestamp <= snapshot sequence
- Version invisible if CreatedByTxID in snapshot's active transaction list
- Version invisible if DeletedByTxID <= snapshot sequence (deleted before snapshot)
- Special case: transaction sees its own uncommitted writes (read-your-own-writes)

#### Step 4.2.2: Implement IsVisibleToSnapshot Method
- Add method to Document struct taking snapshot as parameter
- Return true/false based on visibility rules
- Handle edge cases: deleted versions, uncommitted versions, transaction's own writes
- Optimize for common case (committed, not deleted) with fast path

#### Step 4.2.3: Handle Read-Your-Own-Writes
- Transaction must see documents it created even if CommitTimestamp = 0
- Check if document's CreatedByTxID matches snapshot's transaction ID
- Apply same logic to updates and deletes within transaction
- Test complex scenarios: create, update multiple times, then read

### Task 4.3: Filter Reads Through Snapshot
**Objective**: Apply visibility rules to all read operations

#### Step 4.3.1: Modify Document Scan Operations
- Update `getAllDocumentsForIndexing` to accept optional snapshot parameter
- Filter returned documents through IsVisibleToSnapshot check
- For multi-version documents (same DocumentID), return only visible version
- Maintain performance by filtering early in scan pipeline

#### Step 4.3.2: Update Hash Index Lookups
- Hash index returns all versions of a document ID
- Apply visibility filtering after index lookup
- Return latest visible version to query
- Handle case where document exists but no version is visible

#### Step 4.3.3: Update BTree Index Scans
- BTree scans return documents in key order
- Apply snapshot visibility filter to scan results
- Handle version chains during range scans
- Ensure scan performance doesn't degrade with multiple versions

#### Step 4.3.4: Integrate with Query Execution
- Pass snapshot from session context to query executor
- Thread snapshot through all document access operations
- Ensure JOIN operations use same snapshot consistently
- Test that subqueries see consistent snapshot view

### Task 4.4: Handle Version Chains
**Objective**: Efficiently navigate multiple versions of same document

#### Step 4.4.1: Organize Versions by Sequence
- When scanning storage, group versions by DocumentID
- Sort versions by VersionSequence descending (newest first)
- Return first version that passes visibility check
- Short-circuit scan once visible version found

#### Step 4.4.2: Optimize Common Case (Single Version)
- Fast path when document has only one version
- Skip version chain logic if VersionSequence indicates first version
- Measure performance impact of version chain traversal
- Consider caching latest visible version per document

#### Step 4.4.3: Handle Long Version Chains
- If document has many versions (>10), log warning about update frequency
- Consider compaction hints when excessive versions detected
- Implement version chain length limits if needed
- Test performance with pathological case (1000+ versions of one document)

---

## PHASE 5: MVCC-Aware Compaction
**Goal**: Update compaction to preserve versions needed by active transactions  
**Timeline**: 1 week  
**Priority**: MEDIUM

### Task 5.1: Modify Tombstone Removal Logic
**Objective**: Only remove tombstones not visible to any active transaction

#### Step 5.1.1: Query Oldest Active Snapshot
- Before compaction, get oldest active snapshot sequence from snapshot manager
- If no active transactions, oldest sequence is current global sequence
- Use oldest sequence as version retention boundary
- Log compaction run with retention boundary for debugging

#### Step 5.1.2: Update Tombstone Filter
- Keep tombstones with DeletedByTxID > oldest active snapshot sequence
- Remove only tombstones guaranteed not visible to any transaction
- Preserve tombstones for uncommitted deletes (CommitTimestamp = 0)
- Track how many tombstones kept vs removed in metrics

#### Step 5.1.3: Preserve Deleted But Visible Versions
- If version is deleted but visible to active transaction, keep it
- Version must remain until transaction completes
- Next compaction cycle may remove it if transaction has finished
- Test that long-running read transactions don't block indefinite compaction

### Task 5.2: Implement Version Chain Compaction
**Objective**: Remove old versions no longer needed by any transaction

#### Step 5.2.1: Identify Obsolete Versions
- For documents with multiple versions, keep versions >= oldest active snapshot
- Remove versions with CommitTimestamp < oldest active snapshot AND newer version exists
- Always keep latest committed version regardless of age
- Special case: keep all versions if any transaction hasn't committed

#### Step 5.2.2: Build Version Retention Set
- During compaction merge, build map of DocumentID -> versions to keep
- Apply retention rules based on oldest active snapshot
- Track statistics: versions kept, versions removed, space reclaimed
- Log if excessive versions retained (indicates long-running transaction)

#### Step 5.2.3: Write Compacted Output with Versions
- Write retained versions to new compacted file
- Maintain version ordering (descending VersionSequence)
- Update manifest with version range information
- Verify compacted file has correct version metadata

### Task 5.3: Update Manifest Statistics
**Objective**: Track version-aware metrics in manifest files

#### Step 5.3.1: Add Version Statistics to Manifest
- Track total version count per file (not just document count)
- Track average versions per document
- Track version chain length histogram (1, 2-5, 6-10, 11+)
- Use for compaction priority decisions

#### Step 5.3.2: Track MVCC Compaction Metrics
- Count versions removed during compaction
- Count versions retained due to active snapshots
- Measure space savings from version removal
- Expose metrics via METRICS command or monitoring endpoint

#### Step 5.3.3: Implement Compaction Deferral
- If oldest active snapshot is very old (>1 hour), log warning
- Consider deferring compaction if it would retain most versions anyway
- Implement configurable threshold for compaction effectiveness
- Alert operator if long-running transaction blocks cleanup

---

## PHASE 6: Testing & Production Hardening
**Goal**: Validate correctness, performance, and reliability of MVCC implementation  
**Timeline**: 1 week  
**Priority**: HIGH

### Task 6.1: Correctness Testing
**Objective**: Verify MVCC semantics match expected behavior

#### Step 6.1.1: Test Snapshot Isolation Properties
- Test uncommitted read isolation (T1 inserts, T2 doesn't see)
- Test committed read visibility (T1 inserts and commits, T2 sees)
- Test repeatable read (T1 reads, T2 updates, T1 reads again sees same data)
- Test phantom prevention (T1 counts, T2 inserts, T1 counts again sees same count)

#### Step 6.1.2: Test Write Conflict Scenarios
- Test concurrent updates to same document
- Test delete during concurrent read transaction
- Test insert-delete-insert of same document ID
- Verify last-writer-wins behavior matches expectations

#### Step 6.1.3: Test Transaction Lifecycle
- Test BEGIN, multiple operations, COMMIT
- Test BEGIN, multiple operations, ROLLBACK
- Test transaction timeout and auto-rollback
- Test crash recovery with uncommitted transactions

#### Step 6.1.4: Test Version Chain Correctness
- Create document with 10+ versions across multiple transactions
- Verify each snapshot sees correct version
- Test that old transactions see old versions even after many updates
- Verify version chain navigation returns correct version

### Task 6.2: Concurrency Testing
**Objective**: Validate behavior under high concurrent load

#### Step 6.2.1: Stress Test Concurrent Readers and Writers
- Run 100 reader transactions and 100 writer transactions simultaneously
- Verify no deadlocks, no panics, no data corruption
- Measure read and write latency under contention
- Verify readers see consistent snapshots despite concurrent writes

#### Step 6.2.2: Test Long-Running Transaction Impact
- Start long-running read transaction (5 minutes)
- Run normal workload while long transaction is active
- Verify compaction doesn't break long transaction
- Verify versions retained for long transaction are eventually cleaned up

#### Step 6.2.3: Test Compaction During Active Transactions
- Run compaction while multiple transactions are active
- Verify active transactions still see correct data
- Verify no data loss during compaction
- Test compaction with various transaction lifetime patterns

### Task 6.3: Performance Testing
**Objective**: Validate MVCC overhead is acceptable

#### Step 6.3.1: Benchmark Write Performance
- Measure write latency with version metadata overhead
- Compare to baseline (Phase 1 post-fix performance)
- Target: maintain 70ns average write time
- Test with varying transaction sizes (1 doc, 100 docs, 1000 docs)

#### Step 6.3.2: Benchmark Read Performance
- Measure read latency with snapshot visibility filtering
- Test impact of version chain length on read performance
- Test document scan performance with multi-file storage + versions
- Compare to baseline read performance from Phase 1

#### Step 6.3.3: Benchmark Compaction Performance
- Measure compaction time with version retention logic
- Test compaction with various version chain distributions
- Verify compaction doesn't block reads/writes excessively
- Measure version cleanup effectiveness (% space reclaimed)

#### Step 6.3.4: Test Memory Usage
- Profile memory usage with active transactions
- Measure snapshot structure overhead
- Test memory usage with many concurrent transactions (100+)
- Verify no memory leaks in transaction lifecycle

### Task 6.4: Failure Scenario Testing
**Objective**: Ensure MVCC remains consistent through failures

#### Step 6.4.1: Test Crash During Transaction Commit
- Simulate crash after version creation but before commit timestamp assignment
- Verify uncommitted versions are not visible after recovery
- Verify WAL replay handles partial commits correctly
- Test that recovery doesn't leave orphaned versions

#### Step 6.4.2: Test Crash During Compaction
- Simulate crash in middle of compaction operation
- Verify old files remain intact and usable
- Verify partial compaction output is detected and discarded
- Test that compaction can be retried after crash

#### Step 6.4.3: Test Disk Full Scenarios
- Test behavior when disk fills during version write
- Verify transaction rolls back cleanly on write failure
- Test compaction behavior when disk space is tight
- Ensure system remains operational (read-only) when disk full

### Task 6.5: Migration and Backward Compatibility
**Objective**: Ensure existing data works with MVCC system

#### Step 6.5.1: Test Legacy Data Loading
- Load bundles created before MVCC implementation
- Verify documents without version fields are handled correctly
- Test that version fields default to 0 when missing
- Verify queries work on legacy data

#### Step 6.5.2: Implement Online Migration
- Add migration command to add version fields to existing documents
- Process documents in batches to avoid memory issues
- Update manifest to mark bundles as "MVCC-enabled"
- Test that partially-migrated database works correctly

#### Step 6.5.3: Test Mixed Version Environment
- Test MVCC-enabled bundles alongside legacy bundles
- Verify queries spanning both types work correctly
- Test compaction on legacy bundles (no version logic)
- Document limitations of mixed-mode operation

### Task 6.6: Production Readiness
**Objective**: Prepare MVCC for production deployment

#### Step 6.6.1: Document MVCC Behavior
- Update user documentation with transaction isolation guarantees
- Document version chain limits and recommendations
- Create troubleshooting guide for version-related issues
- Document monitoring metrics and what they mean

#### Step 6.6.2: Create Monitoring and Alerting
- Add Prometheus metrics for MVCC operations
- Create alerts for long version chains
- Create alerts for compaction deferral due to long transactions
- Monitor transaction registry size and alert on leaks

#### Step 6.6.3: Create Operational Tools
- Add admin command to show active transactions
- Add admin command to show version chain statistics
- Add admin command to force version cleanup (emergency)
- Create diagnostic tool to analyze bundle version distribution

#### Step 6.6.4: Performance Tuning Configuration
- Add configuration for snapshot retention time
- Add configuration for version chain length warnings
- Add configuration for compaction effectiveness threshold
- Document recommended settings for different workload types

---

## Success Criteria

### Phase 1 Success
- ✅ Zero concurrent map panics under stress testing
- ✅ Write performance maintained at 70ns average
- ✅ No race conditions detected with `-race` flag

### Phase 2 Success
- ✅ Version fields stored and retrieved correctly
- ✅ Transaction IDs allocated uniquely and monotonically
- ✅ Transaction counter survives restart/crash

### Phase 3 Success
- ✅ All writes create versioned documents
- ✅ Updates create new versions instead of modifying in-place
- ✅ Deletes create tombstone versions
- ✅ Commit assigns timestamp atomically

### Phase 4 Success
- ✅ Snapshots provide repeatable read isolation
- ✅ Read-your-own-writes works correctly
- ✅ Concurrent transactions see consistent data
- ✅ No phantom reads or non-repeatable reads

### Phase 5 Success
- ✅ Compaction preserves versions needed by active transactions
- ✅ Versions cleaned up after transactions complete
- ✅ No data loss during compaction
- ✅ Compaction effectiveness measured and acceptable

### Phase 6 Success
- ✅ All correctness tests pass
- ✅ Performance within 10% of baseline
- ✅ Zero data corruption under failure scenarios
- ✅ Production monitoring and alerting in place

---

## Risk Mitigation

### Risk: Performance Degradation
- **Mitigation**: Benchmark after each phase
- **Fallback**: Feature flag to disable MVCC per-bundle

### Risk: Storage Overhead from Versions
- **Mitigation**: Monitor version chain length
- **Fallback**: Aggressive compaction settings for high-update workloads

### Risk: Long-Running Transaction Blocks Cleanup
- **Mitigation**: Transaction timeout enforcement
- **Fallback**: Admin command to force-abort old transactions

### Risk: Complexity Increases Bugs
- **Mitigation**: Comprehensive test coverage at each phase
- **Fallback**: Incremental rollout per bundle, not system-wide

### Risk: Migration Issues with Existing Data
- **Mitigation**: Online migration with validation
- **Fallback**: Support mixed MVCC/legacy mode indefinitely

---

## Appendix: Existing Infrastructure Leveraged

- ✅ Multi-file append-only storage (bundlestore/)
- ✅ Manifest-based file tracking (manifest_manager.go)
- ✅ Tombstone deletion markers (0xDEADDEAD magic)
- ✅ Background compaction (ghost_cleanup_worker.go)
- ✅ WAL for durability (journal/)
- ✅ Transaction lifecycle management (BEGIN/COMMIT/ROLLBACK)
- ✅ Lock manager for conflict detection (lock_manager.go)
- ✅ Bloom filters for fast lookups (bundlestore/)