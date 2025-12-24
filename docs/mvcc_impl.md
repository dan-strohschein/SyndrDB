# **MVCC Implementation Plan for SyndrDB (LSM-Optimized)**
## **MySQL-Style Simplicity | Read-Optimized | LSM-Native Design**

---

## **EXECUTIVE SUMMARY**

This plan implements Multi-Version Concurrency Control (MVCC) specifically designed for SyndrDB's Log-Structured Merge (LSM) architecture. By leveraging the existing MemTable, SSTable, and compaction infrastructure, this implementation achieves snapshot isolation with minimal overhead and maximum performance.

**Core Principle**:  Readers never block writers, writers never block readers.  LSM's append-only nature and compaction-based garbage collection make MVCC implementation natural and efficient.

**Key Innovation**: Unlike traditional MVCC systems (PostgreSQL's tuple chains, MySQL's undo logs), this design treats the LSM storage layer itself as the version store, eliminating the need for separate version management infrastructure.

---

## **ARCHITECTURAL FOUNDATIONS**

### **Existing LSM Infrastructure to Leverage**

1. **MemTable (In-Memory Write Buffer)**
   - Already caches recent writes for fast reads
   - Already thread-safe with RWMutex
   - Already flushes to disk when capacity reached
   - **MVCC Role**: Store uncommitted versions, provide read-your-own-writes consistency

2. **bundle files (Immutable Sorted String Tables)**
   - Already append-only (no in-place updates)
   - Already store document history naturally
   - Already scanned backward (latest entry wins)
   - **MVCC Role**:  Persistent version storage, historical snapshot source

3. **Compaction (Merge and Cleanup)**
   - Already merges multiple bundle files
   - Already removes obsolete entries
   - Already uses sequence numbers for ordering
   - **MVCC Role**: Garbage collection for old versions

4. **Global Sequence Numbers**
   - Already assigned to every write operation
   - Already monotonically increasing
   - Already used for temporal ordering
   - **MVCC Role**: Transaction commit timestamps, snapshot boundaries

### **What This Plan Adds**

1. **Transaction Lifecycle Management**:  BEGIN, COMMIT, ABORT semantics
2. **Snapshot Isolation**: Each transaction sees consistent point-in-time view
3. **Version Metadata**: Minimal fields added to documents (4 uint64 fields)
4. **Visibility Rules**: Filter LSM read results by transaction snapshot
5. **MVCC-Aware Compaction**: Preserve versions needed by active transactions

### **What This Plan Does NOT Require**

- ❌ Separate version storage structure
- ❌ Version chain traversal logic
- ❌ Dedicated vacuum service
- ❌ Undo log infrastructure
- ❌ Lock tables or lock managers
- ❌ Changes to SSTable file format

---

## **PHASE 1: Transaction Foundation**
**Goal**: Establish transaction lifecycle management and snapshot creation without modifying storage layer

### **Task 1.1: Global Transaction Counter**
Create centralized transaction ID allocation service

#### **Subtask 1.1.1: Atomic Counter Implementation**
- Implement atomic uint64 counter starting at 1
- Store counter value in dedicated field (separate from GlobalSequence)
- Ensure counter increments atomically using sync/atomic package
- Counter must never decrease or reset during server lifetime

#### **Subtask 1.1.2: Counter Persistence Strategy**
- Store current transaction counter value in MemTable header metadata
- When MemTable flushes to SSTable, persist counter value in SSTable header
- On server startup, scan most recent SSTable to recover counter value
- Initialize counter to max(recovered_value) plus 1 to prevent ID reuse

#### **Subtask 1.1.3: Counter Recovery Logic**
- On crash recovery, read WAL to find highest transaction ID referenced
- Set counter to max(SSTable_counter, WAL_counter) plus 1
- Log recovery statistics (recovered counter value, source)
- Validate counter value is monotonically increasing across restarts

---

### **Task 1.2: Active Transaction Registry**
Create in-memory tracking of currently running transactions

#### **Subtask 1.2.1: Registry Data Structure**
- Create sync.Map to store active transactions (key: TransactionID, value: TransactionMetadata)
- TransactionMetadata should contain: TransactionID, SessionID, StartSequence, StartTimestamp, Status
- Status enum values: ACTIVE, COMMITTING, COMMITTED, ABORTED
- Registry must support concurrent access from multiple sessions

#### **Subtask 1.2.2: Transaction Registration**
- On BEGIN TRANSACTION, allocate new TransactionID from global counter
- Capture current GlobalSequence value as transaction start sequence
- Record current wall clock time as start timestamp
- Insert transaction metadata into registry with ACTIVE status
- Return TransactionID to session for use in subsequent operations

#### **Subtask 1.2.3: Transaction Deregistration**
- On COMMIT, update status to COMMITTED and record commit sequence
- On ABORT, update status to ABORTED and record abort timestamp
- Keep completed transaction metadata in registry for configurable duration (default: 60 seconds)
- Implement background cleanup task to remove old completed transactions
- Ensure cleanup only removes transactions older than any active snapshot

#### **Subtask 1.2.4: Registry Query Operations**
- Implement GetActiveTransactions to return list of all ACTIVE transaction IDs
- Implement GetOldestActiveSnapshot to find minimum StartSequence among active transactions
- Implement IsTransactionActive to check if specific transaction ID is still running
- Implement GetTransactionStatus to retrieve current status of any transaction

---

### **Task 1.3: Snapshot Isolation Manager**
Create snapshot capture and visibility determination logic

#### **Subtask 1.3.1: Snapshot Structure Definition**
- Define Snapshot struct containing: SnapshotSequence, TransactionID, ActiveTransactions list
- SnapshotSequence represents the boundary (all committed transactions at or before this sequence are visible)
- TransactionID represents the transaction that owns this snapshot (for read-your-own-writes)
- ActiveTransactions list contains IDs of all transactions that were active when snapshot was taken

#### **Subtask 1.3.2: Snapshot Creation Logic**
- On BEGIN TRANSACTION, capture current GlobalSequence as snapshot boundary
- Query transaction registry for all currently ACTIVE transaction IDs
- Store ActiveTransactions list in snapshot (these will remain invisible even if they commit later)
- Create Snapshot object and store in session context for use during reads
- Ensure snapshot creation is atomic with respect to transaction registry updates

#### **Subtask 1.3.3: Visibility Rule Implementation**
- Rule 1: If document. CommitTimestamp is 0, document is uncommitted (visible only if CreatedByTxID equals snapshot's TransactionID)
- Rule 2: If document.CommitTimestamp is greater than snapshot boundary, document was committed after snapshot (invisible)
- Rule 3: If document.CreatedByTxID is in ActiveTransactions list, document was created by concurrent transaction (invisible even if later committed)
- Rule 4: If document.DeletedByTxID is non-zero and less than or equal to snapshot boundary, document was deleted (invisible)
- Rule 5: If all above checks pass, document is visible
- Create IsVisible function that takes document and snapshot, returns boolean

#### **Subtask 1.3.4: Snapshot Lifecycle Management**
- Store snapshot in session context when transaction begins
- Use same snapshot for all reads within transaction (repeatable read guarantee)
- Clear snapshot from session context when transaction commits or aborts
- For autocommit operations (no explicit transaction), create snapshot per-statement
- Track oldest active snapshot globally for compaction coordination

---

### **Task 1.4: Session Context Integration**
Integrate transaction and snapshot state into existing session management

#### **Subtask 1.4.1: Session Structure Enhancement**
- Add TransactionID field to Session struct (0 if no active transaction)
- Add Snapshot pointer field to Session struct (nil if no active transaction)
- Add IsInTransaction method that returns true if TransactionID is non-zero
- Add GetTransactionID method that returns current transaction ID or 0
- Add GetSnapshot method that returns current snapshot or creates temp snapshot for autocommit

#### **Subtask 1.4.2: BEGIN TRANSACTION Command Handler**
- Validate no transaction is already active in session
- Call transaction registry to allocate new TransactionID
- Create snapshot using snapshot manager
- Store TransactionID and Snapshot in session context
- Log transaction start event to WAL with TransactionID and start sequence
- Return success message with TransactionID to client

#### **Subtask 1.4.3: COMMIT Command Handler**
- Validate transaction is active in session
- Call transaction commit logic (Phase 5, implemented later)
- Remove TransactionID and Snapshot from session context
- Log transaction commit event to WAL
- Return success message with commit statistics to client

#### **Subtask 1.4.4: ROLLBACK Command Handler**
- Validate transaction is active in session
- Call transaction abort logic (Phase 5, implemented later)
- Remove TransactionID and Snapshot from session context
- Log transaction abort event to WAL
- Return success message to client

#### **Subtask 1.4.5: Autocommit Mode Handling**
- For operations without explicit transaction, create temporary single-operation transaction
- Allocate TransactionID, create snapshot, execute operation, commit immediately
- Ensure autocommit transactions are logged to WAL for durability
- Clean up transaction state automatically after autocommit completes

#### **Subtask 1.4.6: Error Handling and Timeout**
- Implement transaction timeout detection (configurable, default:  5 minutes)
- Automatically abort transactions that exceed timeout
- Handle session disconnection by aborting any active transaction
- Prevent DDL operations (CREATE BUNDLE, DROP BUNDLE) inside transactions
- Return clear error messages for transaction violations

---

## **PHASE 2: Document Version Metadata**
**Goal**: Add minimal version tracking fields to documents without changing LSM storage format

### **Task 2.1: Document Structure Extension**
Extend existing Document struct with MVCC metadata fields

#### **Subtask 2.1.1: Add Version Fields to Document Struct**
- Add CreatedByTxID field (uint64) - transaction ID that created this version
- Add CreatedAtSeq field (uint64) - global sequence number when version was created
- Add DeletedByTxID field (uint64) - transaction ID that deleted this version (0 if not deleted)
- Add CommitTimestamp field (uint64) - sequence number when creating transaction committed (0 if uncommitted)
- Ensure fields are positioned after existing fields to maintain backward compatibility
- Document struct size increases by 32 bytes (4 uint64 fields)

#### **Subtask 2.1.2: Default Value Assignment**
- On document creation, set CreatedByTxID to current transaction ID from session
- On document creation, set CreatedAtSeq to atomic increment of GlobalSequence
- On document creation, set DeletedByTxID to 0 (document starts as live)
- On document creation, set CommitTimestamp to 0 (uncommitted until transaction commits)
- For autocommit operations, set CommitTimestamp immediately after operation

#### **Subtask 2.1.3: BSON Serialization Integration**
- Add version fields to BSON encoding logic (already implemented for Documents)
- Ensure MapToDoc function handles new fields when deserializing from disk
- Ensure DocToMap function includes new fields when serializing to disk
- Add field name constants for version metadata (avoid magic strings)
- Maintain backward compatibility by treating missing fields as zero values

#### **Subtask 2.1.4: Backward Compatibility Migration**
- When loading documents without version fields, set CreatedByTxID to 1 (system initialization transaction)
- When loading documents without version fields, set CreatedAtSeq to 1
- When loading documents without version fields, set DeletedByTxID to 0
- When loading documents without version fields, set CommitTimestamp to 1 (assume committed)
- Log migration statistics (number of documents migrated, database name)
- Provide configuration flag to disable migration checks after first startup

---

### **Task 2.2: Bundle Storage Layer Integration**
Integrate version metadata into existing bundle storage operations

#### **Subtask 2.2.1: Document Insertion Version Assignment**
- Modify AddDocumentToBundle to accept transaction context (session parameter)
- Extract TransactionID from session context
- Set version metadata fields on document before adding to MemTable
- Ensure GlobalSequence is incremented atomically for each document inserted
- If transaction is autocommit, set CommitTimestamp immediately

#### **Subtask 2.2.2: MemTable Write Path Modification**
- MemTable Put operation already stores documents by DocumentID (latest write wins)
- No structural changes needed to MemTable storage
- Version metadata travels with document automatically
- MemTable naturally stores uncommitted versions (CommitTimestamp = 0)
- MemTable flush to SSTable preserves version metadata in serialization

#### **Subtask 2.2.3: Update Operation Versioning**
- On UPDATE, create new document version with updated field values
- Set CreatedByTxID and CreatedAtSeq on new version
- Insert new version into MemTable (replaces old version in MemTable due to same DocumentID)
- Old version remains in SSTable until compaction
- Old version becomes invisible once new version commits (visibility rules handle this)

#### **Subtask 2.2.4: Delete Operation Versioning**
- On DELETE, create tombstone version of document
- Set DeletedByTxID to current transaction ID on tombstone
- Set CreatedByTxID and CreatedAtSeq on tombstone
- Insert tombstone into MemTable
- Old live version remains in SSTable until compaction
- Tombstone makes document invisible to new transactions (visibility rules handle this)

---

### **Task 2.3: SSTable Storage Preservation**
Ensure version metadata is preserved in SSTable storage without format changes

#### **Subtask 2.3.1: Flush Operation Verification**
- Verify existing MemTable flush logic serializes version metadata fields
- Verify SSTable file format (BSON) automatically includes new fields
- No changes needed to flush triggers or timing
- Uncommitted versions (CommitTimestamp = 0) should be discarded during flush
- Only committed versions (CommitTimestamp > 0) should be written to SSTable

#### **Subtask 2.3.2: SSTable Read Path Verification**
- Verify existing SSTable scan logic deserializes version metadata fields
- Verify backward scan (newest entry first) still works with version metadata
- No changes needed to SSTable page structure
- Version metadata is read alongside document data in single operation

#### **Subtask 2.3.3: Multiple Version Storage Pattern**
- Since bundle files are immutable, old versions remain in old bundle files
- New versions go into newer bundle files (via MemTable flush)
- Multiple versions of same DocumentID exist across multiple bundle files
- LSM merge-on-read pattern naturally handles version selection
- Compaction (Phase 6) will handle version cleanup

---

## **PHASE 3: LSM Read Path with Snapshot Filtering**
**Goal**:  Modify read operations to filter results by snapshot visibility without blocking

### **Task 3.1: MemTable Read Path Enhancement**
Add snapshot filtering to MemTable lookup operations

#### **Subtask 3.1.1: Point Lookup Modification**
- Modify MemTable Get operation to accept snapshot parameter
- After retrieving document from MemTable map, apply visibility check
- If document is visible to snapshot, return document
- If document is not visible to snapshot, return NotFound (fall through to SSTable scan)
- Maintain existing RLock concurrency pattern (no write locks during reads)

#### **Subtask 3.1.2: Read-Your-Own-Writes Optimization**
- Check if document. CreatedByTxID equals snapshot. TransactionID
- If yes, document is visible regardless of CommitTimestamp (uncommitted but owned by this transaction)
- This enables transaction to read its own uncommitted changes
- No special handling needed, visibility rules handle this automatically

#### **Subtask 3.1.3: MemTable Scan Operations**
- Modify MemTable iteration logic to accept snapshot parameter
- Apply visibility filter to each document during iteration
- Accumulate only visible documents in result set
- Maintain iterator performance (single pass, no backtracking)

---

### **Task 3.2: SSTable Read Path Enhancement**
Add snapshot filtering to SSTable scan operations

#### **Subtask 3.2.1: Backward Scan Modification**
- Modify ScanBackward operation to accept snapshot parameter
- For each document found during backward scan, apply visibility check
- First visible version of each DocumentID is the correct version (LSM property)
- If document is not visible, continue scanning older bundle files
- Stop scanning once visible version is found (optimization)

#### **Subtask 3.2.2: Multiple Version Handling**
- Same DocumentID may appear in multiple bundle files (different versions)
- Scan from newest SSTable to oldest SSTable
- Apply visibility filter to each version encountered
- Return first visible version (newest visible version wins)
- Skip older versions once visible version is found

#### **Subtask 3.2.3: Tombstone Visibility Handling**
- If tombstone version is visible to snapshot, document should not be returned
- Check DeletedByTxID field in visibility rules
- If document was deleted before snapshot boundary, exclude from results
- If document was deleted after snapshot boundary, include in results (deletion not yet visible)

#### **Subtask 3.2.4: Read Performance Optimization**
- Cache visibility check results within single query to avoid repeated calculations
- Store snapshot metadata in thread-local storage for fast access
- Short-circuit visibility checks for autocommit transactions (all committed data visible)
- Use early exit strategy when first visible version is found

---

### **Task 3.3: Document Scanner Integration**
Integrate snapshot filtering into existing document scanner framework

#### **Subtask 3.3.1: Scanner Interface Modification**
- Add snapshot parameter to document scanner factory methods
- Pass snapshot through to MemTable and SSTable read operations
- Ensure all scanner implementations (full scan, index scan, hash scan) support snapshots

#### **Subtask 3.3.2: Full Bundle Scan Filtering**
- Modify GetAllDocumentIDs to accept snapshot parameter
- Apply visibility filter when merging MemTable and SSTable document IDs
- Ensure de-duplication logic considers version visibility
- Maintain scan performance (avoid loading unnecessary versions into memory)

#### **Subtask 3.3.3: Page-Based Scanning Enhancement**
- Apply snapshot filtering when loading document pages from disk
- Filter invisible documents before returning page results
- Adjust page result counts to account for filtered documents
- Handle case where entire page becomes invisible (return empty page, continue scanning)

#### **Subtask 3.3.4: WHERE Clause Integration**
- Apply snapshot filtering before WHERE clause evaluation
- Only evaluate WHERE predicates on visible documents
- Maintain predicate evaluation performance (snapshot filter is fast check)
- Ensure correct result ordering (visible documents only)

---

### **Task 3.4: Range Query Snapshot Consistency**
Ensure range queries see consistent snapshot throughout scan

#### **Subtask 3.4.1: Range Scan Snapshot Capture**
- Capture snapshot once at beginning of range query
- Use same snapshot for all documents scanned in range
- Prevent phantom reads (new inserts during scan are invisible)
- Prevent inconsistent reads (updates during scan use original version)

#### **Subtask 3.4.2: Streaming Result Visibility**
- Apply visibility filter to each document as it streams from storage
- Do not buffer entire range in memory before filtering
- Yield visible documents immediately for query processing
- Maintain scan efficiency (single pass through data)

#### **Subtask 3.4.3: Empty Range Handling**
- If range query returns no visible documents, return empty result set
- Log range query statistics (documents scanned, documents filtered, documents returned)
- Provide query performance metrics for visibility filter overhead

---

## **PHASE 4: Index MVCC Integration**
**Goal**: Make hash and B-tree indexes snapshot-aware for consistent reads

### **Task 4.1: Hash Index Version Metadata**
Add version tracking to hash index entries

#### **Subtask 4.1.1: Hash Index Entry Structure Extension**
- Add CreatedByTxID field to HashIndexEntry struct
- Add CreatedAtSeq field to HashIndexEntry struct
- Add DeletedByTxID field to HashIndexEntry struct
- Add CommitTimestamp field to HashIndexEntry struct
- Maintain existing fields (KeyValue, DocumentID, PageID, Sequence, Deleted)

#### **Subtask 4.1.2: Index Entry Creation**
- When inserting index entry, copy version metadata from source document
- Ensure index entry version matches document version exactly
- Use same TransactionID and sequence numbers as document
- Set CommitTimestamp to 0 for uncommitted transactions

#### **Subtask 4.1.3: Index Entry Serialization**
- Add version fields to hash index entry binary serialization format
- Update header format version number to indicate MVCC support
- Maintain backward compatibility with old index files
- Migrate old index entries by setting CreatedByTxID = 1, CommitTimestamp = 1

---

### **Task 4.2: Hash Index Read Path Filtering**
Apply snapshot visibility to hash index lookups

#### **Subtask 4.2.1: MemTable Index Lookup**
- Modify hash index MemTable Get to accept snapshot parameter
- Apply visibility check to index entry before returning DocumentID
- If entry is not visible, treat as cache miss (fall through to SSTable scan)
- Maintain O(1) lookup performance (visibility check is fast)

#### **Subtask 4.2.2: SSTable Index Scan**
- Modify backward scan to apply visibility filter to each index entry
- Multiple versions of same key may exist in different bundle files
- Return DocumentID from first visible entry encountered
- Skip older entries once visible entry is found

#### **Subtask 4.2.3: Index Result Deduplication**
- When merging MemTable and SSTable index results, apply visibility filter
- De-duplicate DocumentIDs (same document may have multiple index entries)
- Return only unique visible DocumentIDs
- Maintain result ordering (if applicable)

---

### **Task 4.3: Hash Index Write Path Versioning**
Ensure index updates create versioned entries

#### **Subtask 4.3.1: Index Insert Operation**
- On document insert, create new hash index entry with version metadata
- Append entry to index SSTable via EntryStorage
- Update index MemTable with new entry (latest write wins in MemTable)
- Set CreatedByTxID to current transaction ID

#### **Subtask 4.3.2: Index Update Operation**
- On document update, if indexed field value changes, create new index entry
- New entry points to same DocumentID but with new key value
- Set CreatedByTxID on new entry to current transaction ID
- Old index entry remains in SSTable (will be filtered by visibility rules)
- Compaction will clean up old entry eventually

#### **Subtask 4.3.3: Index Delete Operation**
- On document delete, create tombstone entry in index
- Set DeletedByTxID on index entry to current transaction ID
- Keep entry in MemTable with deleted flag set
- Compaction will remove tombstone after no snapshots need it

---

### **Task 4.4: B-Tree Index MVCC Integration**
Apply same versioning pattern to B-tree indexes

#### **Subtask 4.4.1: B-Tree Node Entry Versioning**
- Add version metadata fields to BTreeEntry struct (same as hash index)
- Store version metadata in B-tree leaf nodes alongside key-value pairs
- Internal nodes do not need version metadata (only store keys for navigation)

#### **Subtask 4.4.2: B-Tree Lookup Filtering**
- Modify B-tree Search operation to accept snapshot parameter
- Apply visibility filter when traversing to leaf nodes
- Return first visible entry matching search key
- Handle case where multiple versions exist in same leaf node

#### **Subtask 4.4.3: B-Tree Range Scan Filtering**
- Modify RangeSearch operation to accept snapshot parameter
- Apply visibility filter to each entry in range
- Maintain sorted order (only visible entries)
- Handle leaf node linking (next/prev pointers) with visibility filtering

#### **Subtask 4.4.4: B-Tree Write Operations**
- On insert, create new entry with version metadata
- On update, insert new entry and old entry becomes invisible via visibility rules
- On delete, mark entry with DeletedByTxID
- B-tree node splitting and merging should preserve version metadata

---

### **Task 4.5: Index Compaction Version Cleanup**
Ensure index compaction removes unnecessary old versions

#### **Subtask 4.5.1: Hash Index Compaction Integration**
- Modify hash index compaction to accept oldest active snapshot parameter
- Keep index entries where CreatedAtSeq >= oldest active snapshot
- Remove index entries older than oldest active snapshot
- Keep at least one version of each key (most recent committed version)

#### **Subtask 4.5.2: B-Tree Index Vacuum**
- Implement B-tree maintenance operation to remove old versions
- Remove entries marked with DeletedByTxID older than oldest active snapshot
- Rebalance tree after removing entries (if necessary)
- Maintain tree invariants during version cleanup

---

## **PHASE 5: Transaction Commit and Abort**
**Goal**: Implement commit and abort logic to finalize or discard transaction changes

### **Task 5.1: Commit Path Implementation**
Finalize transaction changes and make them visible to other transactions

#### **Subtask 5.1.1: Commit Sequence Assignment**
- Allocate commit sequence number from GlobalSequence atomically
- Commit sequence becomes the CommitTimestamp for all versions created by this transaction
- Commit sequence must be greater than all previous commit sequences (monotonicity)
- Log commit sequence to WAL for durability

#### **Subtask 5.1.2: MemTable Version Update**
- Acquire write lock on MemTable
- Iterate through all documents in MemTable
- For each document where CreatedByTxID equals committing transaction ID and CommitTimestamp equals 0, set CommitTimestamp to commit sequence
- Release write lock on MemTable
- This makes all uncommitted versions instantly visible to new snapshots

#### **Subtask 5.1.3: Index Version Update**
- Acquire write lock on each index MemTable (hash and B-tree)
- Iterate through all index entries in MemTable
- For each entry where CreatedByTxID equals committing transaction ID and CommitTimestamp equals 0, set CommitTimestamp to commit sequence
- Release write locks on index MemTables
- Index entries become visible simultaneously with documents

#### **Subtask 5.1.4: Transaction Registry Update**
- Update transaction status in registry from ACTIVE to COMMITTED
- Record commit sequence in transaction metadata
- Keep transaction metadata in registry for short duration (for query performance)
- Notify any waiting transactions that may be blocked on this commit

#### **Subtask 5.1.5: WAL Commit Record**
- Write COMMIT record to WAL containing:  TransactionID, CommitSequence, Timestamp
- Flush WAL to disk (fsync) before returning success to client
- COMMIT record is checkpoint for recovery (transaction is durable after this point)
- Failure before WAL flush should rollback transaction

#### **Subtask 5.1.6: Session Cleanup**
- Remove TransactionID from session context
- Remove Snapshot from session context
- Reset session to autocommit mode
- Log commit statistics (duration, documents modified, indexes updated)

---

### **Task 5.2: Abort Path Implementation**
Discard transaction changes and restore state to before transaction began

#### **Subtask 5.2.1: Uncommitted Version Identification**
- Acquire write lock on MemTable
- Iterate through all documents in MemTable
- Identify documents where CreatedByTxID equals aborting transaction ID and CommitTimestamp equals 0
- Collect list of DocumentIDs to remove

#### **Subtask 5.2.2: MemTable Rollback**
- For each uncommitted document version, remove from MemTable
- If document was updated (not inserted), need to find previous version in SSTable
- If document was inserted, simply remove from MemTable (document never existed)
- If document was deleted, restore live version from SSTable to MemTable
- Release write lock on MemTable

#### **Subtask 5.2.3: Index Rollback**
- Acquire write lock on each index MemTable
- Remove all index entries where CreatedByTxID equals aborting transaction ID and CommitTimestamp equals 0
- Restore previous index entries if update changed indexed field
- Release write locks on index MemTables

#### **Subtask 5.2.4: Transaction Registry Update**
- Update transaction status in registry from ACTIVE to ABORTED
- Record abort timestamp in transaction metadata
- Keep transaction metadata briefly for diagnostics
- Notify any waiting transactions

#### **Subtask 5.2.5: WAL Abort Record**
- Write ABORT record to WAL containing: TransactionID, AbortSequence, Timestamp
- Flush WAL to disk for recovery consistency
- ABORT record tells recovery to discard any operations from this transaction

#### **Subtask 5.2.6: Session Cleanup**
- Remove TransactionID from session context
- Remove Snapshot from session context
- Reset session to autocommit mode
- Log abort statistics (reason, duration, documents discarded)

---

### **Task 5.3: Autocommit Transaction Handling**
Implement single-operation transaction semantics for non-explicit transactions

#### **Subtask 5.3.1: Autocommit Detection**
- Before executing any write operation, check if session has active transaction
- If no active transaction, operation is in autocommit mode
- Create temporary transaction context for single operation
- Set autocommit flag in transaction metadata

#### **Subtask 5.3.2: Autocommit Execution**
- Allocate TransactionID and create snapshot for autocommit operation
- Execute operation with transaction context
- Immediately commit transaction after operation completes successfully
- If operation fails, immediately abort transaction

#### **Subtask 5.3.3: Autocommit Optimization**
- For autocommit operations, set CommitTimestamp immediately during write
- Skip MemTable commit phase (version is already marked committed)
- Still write to WAL for durability
- Reduce transaction registry overhead (can skip registration for simple operations)

---

### **Task 5.4: Write-Ahead Log Integration**
Ensure all transaction operations are logged for crash recovery

#### **Subtask 5.4.1: Transaction Begin Logging**
- Write BEGIN record to WAL when transaction starts
- Include:  TransactionID, SessionID, StartSequence, Timestamp
- Flush WAL after BEGIN record (configurable)

#### **Subtask 5.4.2: Operation Logging**
- Existing WAL logging for INSERT, UPDATE, DELETE operations continues
- Add TransactionID field to all operation log records
- Include version metadata in log records (CreatedByTxID, CreatedAtSeq)

#### **Subtask 5.4.3: Commit Logging**
- Write COMMIT record to WAL when transaction commits
- Include: TransactionID, CommitSequence, Timestamp
- Flush WAL synchronously before returning success to client (durability)

#### **Subtask 5.4.4: Abort Logging**
- Write ABORT record to WAL when transaction aborts
- Include: TransactionID, AbortSequence, Timestamp, Reason
- Flush WAL after ABORT record

---

### **Task 5.5: Crash Recovery Integration**
Ensure MVCC state is correctly restored after server crash

#### **Subtask 5.5.1: WAL Replay Strategy**
- On server startup, scan WAL from last checkpoint forward
- Build map of TransactionID to transaction state (BEGIN, COMMIT, ABORT)
- Identify incomplete transactions (BEGIN without COMMIT or ABORT)

#### **Subtask 5.5.2: Committed Transaction Replay**
- For transactions with COMMIT record, replay all operations
- Set CommitTimestamp on replayed documents to recorded commit sequence
- Restore documents to MemTable in transaction order

#### **Subtask 5.5.3: Incomplete Transaction Handling**
- For transactions with BEGIN but no COMMIT or ABORT, assume aborted
- Discard all operations from incomplete transactions
- Log recovery statistics (completed transactions replayed, incomplete transactions discarded)

#### **Subtask 5.5.4: Transaction Counter Recovery**
- Find highest TransactionID in WAL
- Find highest CommitSequence in WAL
- Set transaction counter to max(TransactionID) plus 1
- Set GlobalSequence to max(CommitSequence) plus 1

---

## **PHASE 6: Compaction-Based Garbage Collection**
**Goal**: Use LSM compaction to remove old versions no longer needed by any snapshot

### **Task 6.1: Compaction Strategy Enhancement**
Modify compaction triggers to consider version retention requirements

#### **Subtask 6.1.1: Compaction Trigger Evaluation**
- Existing triggers:  SSTable count threshold, file size threshold, tombstone ratio
- Add new trigger: version accumulation threshold (multiple versions of same document)
- Compaction should run when old versions are unlikely to be needed
- Coordinate compaction timing with transaction registry (avoid compacting during long transactions)

#### **Subtask 6.1.2: Oldest Active Snapshot Tracking**
- Query transaction registry before starting compaction
- Find minimum StartSequence among all active transactions
- Use this as compaction boundary (do not remove versions newer than this)
- If no active transactions, use current GlobalSequence as boundary (all committed versions safe to clean)

#### **Subtask 6.1.3: Compaction Safety Check**
- Before removing any version, verify it is older than oldest active snapshot
- Keep at least one committed version of each document (most recent before boundary)
- Preserve tombstone versions until they are older than oldest active snapshot
- Abort compaction if long-running transaction would block cleanup

---

### **Task 6.2: Bundle File Compaction**
Implement version cleanup during bundle SSTable compaction

#### **Subtask 6.2.1: Multi-Version Collection**
- Scan all bundle files selected for compaction
- Collect all versions of each DocumentID into version list
- Sort version list by CreatedAtSeq (newest first)
- Versions naturally spread across multiple bundle files due to LSM structure

#### **Subtask 6.2.2: Version Retention Logic**
- For each DocumentID, determine which versions to keep:
  - Keep most recent committed version (always)
  - Keep versions where CommitTimestamp is 0 (uncommitted, may still commit)
  - Keep versions where CreatedAtSeq >= oldest active snapshot boundary
  - Keep tombstone versions where DeletedByTxID >= oldest active snapshot boundary
- Discard all other versions (safe to remove)

#### **Subtask 6.2.3: Compacted SSTable Creation**
- Write retained versions to new SSTable in sorted order
- Preserve version metadata in serialized documents
- Multiple versions of same DocumentID may be written (if needed by active snapshots)
- Update SSTable header with compaction statistics

#### **Subtask 6.2.4: Old SSTable Removal**
- After new SSTable is fully written and fsynced, atomically replace old bundle files
- Update bundle metadata to reference new SSTable file path
- Delete old SSTable files from disk
- Log compaction statistics (versions removed, space reclaimed, duration)

---

### **Task 6.3: Hash Index Compaction Integration**
Apply version cleanup to hash index bundle files

#### **Subtask 6.3.1: Index Entry Version Collection**
- Existing hash index compaction already scans all entry files
- Modify to collect all versions of each key-value pair
- Group by KeyValue (not DocumentID, since multiple documents can have same key)

#### **Subtask 6.3.2: Index Entry Retention Logic**
- For each key, keep most recent committed entry pointing to each unique DocumentID
- Keep entries where CommitTimestamp is 0 (uncommitted)
- Keep entries where CreatedAtSeq >= oldest active snapshot
- Keep tombstone entries where DeletedByTxID >= oldest active snapshot
- Discard duplicate entries and old versions safe to remove

#### **Subtask 6.3.3: Compacted Index File Creation**
- Write retained index entries to new compacted file
- Maintain LSM entry storage format (header plus entries)
- Update index header statistics (entry count, deleted count, file size)

#### **Subtask 6.3.4: Old Index File Removal**
- Replace old index files with compacted file atomically
- Delete old files from disk
- Clear index MemTable if compaction is full (includes all recent writes)

---

### **Task 6.4: B-Tree Index Maintenance**
Implement version cleanup for B-tree indexes

#### **Subtask 6.4.1: B-Tree Vacuum Operation**
- Traverse B-tree leaf nodes from left to right
- For each entry, check if version is older than oldest active snapshot
- Mark old versions for removal
- Collect statistics on entries scanned and entries marked

#### **Subtask 6.4.2: B-Tree Entry Removal**
- Remove marked entries from leaf nodes
- Update key counts in internal nodes if necessary
- If leaf node becomes underfull, merge with sibling or rebalance
- Maintain B-tree invariants throughout cleanup

#### **Subtask 6.4.3: B-Tree Rewrite Optimization**
- If significant portion of entries are removed, consider rewriting entire tree
- Build new B-tree from scratch with only retained entries
- Atomically replace old tree file with new tree file
- This is similar to PostgreSQL's REINDEX operation

---

### **Task 6.5: Compaction Coordination**
Ensure compaction does not interfere with active queries

#### **Subtask 6.5.1: Compaction Lock Strategy**
- Existing compaction uses per-bundle locks (BundleOperationLock)
- No changes needed to lock acquisition strategy
- Compaction already acquires write lock on bundle before modifying files
- Read operations use read locks, can proceed concurrently with compaction planning

#### **Subtask 6.5.2: Snapshot Blocking Detection**
- Before starting compaction, check if any long-running transaction exists
- If transaction has been active for more than threshold (e.g., 10 minutes), log warning
- Optionally skip compaction to avoid blocking on long transaction
- Provide monitoring metrics for compaction delays due to active snapshots

#### **Subtask 6.5.3: Incremental Compaction**
- If full compaction would remove too few versions, skip compaction
- Implement heuristic: only compact if can reclaim at least 20% of space
- Track compaction efficiency metrics (space before, space after, time taken)

---

## **PHASE 7: Comprehensive Testing**
**Goal**: Validate correctness, performance, and recovery behavior of MVCC implementation

### **Task 7.1: Functional Correctness Tests**
Verify MVCC semantics are correctly implemented

#### **Subtask 7.1.1: Basic Transaction Test**
- Test:  BEGIN, INSERT document, COMMIT
- Expectation: Document is visible to new transactions after commit
- Validation: Query document after commit, verify it exists and has correct data
- Verify CommitTimestamp is non-zero after commit

#### **Subtask 7.1.2: Uncommitted Read Isolation Test**
- Test: T1 starts, T1 inserts document, T2 starts, T2 queries document
- Expectation: T2 does not see uncommitted document from T1
- Validation: T2 query returns empty result set
- Verify T1 can read its own uncommitted insert (read-your-own-writes)

#### **Subtask 7.1.3: Committed Read Visibility Test**
- Test: T1 starts, T1 inserts document, T1 commits, T2 starts, T2 queries document
- Expectation:  T2 sees document committed by T1
- Validation: T2 query returns document with correct data
- Verify CommitTimestamp is before T2 snapshot boundary

#### **Subtask 7.1.4: Snapshot Isolation Test**
- Test: T1 starts, T2 starts, T2 inserts document, T2 commits, T1 queries document
- Expectation: T1 does not see document inserted after T1's snapshot
- Validation: T1 query returns empty result (document not visible to T1's snapshot)
- Verify T1's snapshot boundary is before T2's commit sequence

#### **Subtask 7.1.5: Repeatable Read Test**
- Test:  T1 starts, T1 queries document, T2 updates document and commits, T1 queries again
- Expectation: T1 sees same data in both queries (repeatable read)
- Validation: Both queries return identical result sets
- Verify T1 is isolated from T2's update

#### **Subtask 7.1.6: Write Conflict Test**
- Test: T1 starts, T2 starts, T1 updates document, T2 updates same document
- Expectation: Second update either blocks (if using locks) or succeeds (first-writer-wins)
- Validation: Verify final state is consistent (one update applied)
- Note: Current plan is first-writer-wins (no blocking), so both updates succeed but only latest is visible

#### **Subtask 7.1.7: Rollback Test**
- Test: T1 starts, T1 inserts document, T1 aborts
- Expectation: Document is not visible to any transaction
- Validation: Query after abort returns empty result
- Verify document is removed from MemTable

#### **Subtask 7.1.8: Autocommit Test**
- Test:  Execute INSERT without BEGIN TRANSACTION
- Expectation: Insert is immediately committed and visible
- Validation:  Subsequent query sees inserted document
- Verify CommitTimestamp is set immediately

---

### **Task 7.2:  Concurrency Correctness Tests**
Verify MVCC works correctly under concurrent load

#### **Subtask 7.2.1: Concurrent Read-Write Test**
- Test: Start 10 reader threads and 10 writer threads operating on same bundle
- Expectation:  Readers see consistent snapshots, writers do not block readers
- Validation: All readers complete without errors, all reads return valid data
- Measure: Read throughput remains high even with concurrent writes

#### **Subtask 7.2.2: Concurrent Write-Write Test**
- Test:  Start 20 writer threads inserting unique documents
- Expectation: All inserts succeed without conflicts
- Validation: After all threads complete, all documents are visible
- Measure: No deadlocks, no crashes, no data corruption

#### **Subtask 7.2.3: Concurrent Update Test**
- Test: Start 10 threads updating same set of documents
- Expectation:  Updates create new versions, no in-place overwrites
- Validation: Final state is consistent (latest version is visible)
- Verify:  Multiple versions exist in bundle files during test

#### **Subtask 7.2.4: Snapshot Consistency Under Load Test**
- Test: Start long-running transaction (holds snapshot), start heavy write load in background
- Expectation: Long transaction continues to see its original snapshot data
- Validation: Long transaction's queries return consistent results throughout
- Verify: Old versions are preserved in bundle files until transaction completes

---

### **Task 7.3: LSM Integration Tests**
Verify MVCC works correctly with LSM operations

#### **Subtask 7.3.1: MemTable Flush During Transaction Test**
- Test: Start transaction, insert 10000 documents (exceeds MemTable capacity), trigger flush, query documents
- Expectation: Transaction can read all inserted documents (read-your-own-writes persists across flush)
- Validation: All queries return inserted documents
- Verify: Uncommitted versions are not written to SSTable during flush

#### **Subtask 7.3.2: SSTable Scan Version Selection Test**
- Test: Create multiple versions of same document across multiple bundle files, query with different snapshots
- Expectation: Each snapshot sees appropriate version based on visibility rules
- Validation: Older snapshot sees older version, newer snapshot sees newer version
- Verify: Backward scan stops at first visible version (optimization works)

#### **Subtask 7.3.3: Compaction During Active Transaction Test**
- Test: Start long-running transaction, trigger compaction in background
- Expectation:  Compaction preserves versions needed by active transaction
- Validation: Transaction can still query old versions after compaction
- Verify:  Compaction does not remove versions newer than transaction's snapshot

#### **Subtask 7.3.4: Compaction Version Cleanup Test**
- Test: Create many versions of documents, commit all transactions, trigger compaction
- Expectation: Compaction removes all but latest version of each document
- Validation: Only one version per document remains in bundle files
- Measure: Space reclaimed by compaction

---

### **Task 7.4: Index MVCC Tests**
Verify indexes are correctly versioned and filtered

#### **Subtask 7.4.1: Hash Index Visibility Test**
- Test: T1 starts, T1 inserts document with indexed field, T2 starts, T2 queries via hash index
- Expectation:  T2 does not find document via index (uncommitted)
- Validation: Hash index lookup returns empty result for T2
- Verify: T1 can find document via index (read-your-own-writes)

#### **Subtask 7.4.2: Hash Index Update Test**
- Test: Insert document with indexed field value "A", update to value "B" within transaction
- Expectation: Both index entries exist until compaction
- Validation: Query before commit via old value returns nothing, query after commit via new value succeeds
- Verify: Old index entry becomes invisible after new entry commits

#### **Subtask 7.4.3: B-Tree Range Query Snapshot Test**
- Test: T1 starts, T2 inserts documents in range, T2 commits, T1 performs range query
- Expectation:  T1 does not see documents inserted after its snapshot
- Validation: T1 range query returns empty result or only pre-existing documents
- Verify: B-tree visibility filtering works correctly

#### **Subtask 7.4.4: Index Compaction Version Cleanup Test**
- Test: Create multiple index entry versions, trigger index compaction
- Expectation:  Old index entries are removed
- Validation: Only latest committed entry per key remains
- Measure: Index file size reduction

---

### **Task 7.5: Recovery and Durability Tests**
Verify MVCC state is correctly restored after crash

#### **Subtask 7.5.1: Crash During Transaction Test**
- Test: Start transaction, insert documents, crash server before commit
- Expectation:  Uncommitted documents are not visible after recovery
- Validation: After restart, query returns empty result (documents discarded)
- Verify: WAL replay identifies incomplete transaction and discards operations

#### **Subtask 7.5.2: Crash After Commit Before Flush Test**
- Test:  Start transaction, insert documents, commit, crash before MemTable flushes
- Expectation:  Committed documents are visible after recovery
- Validation: After restart, query returns committed documents
- Verify: WAL replay restores documents to MemTable with correct CommitTimestamp

#### **Subtask 7.5.3: Crash During Compaction Test**
- Test: Start compaction, crash server mid-compaction
- Expectation: System recovers gracefully, compaction is retried or rolled back
- Validation: After restart, data is consistent (either old or new bundle files, not corrupted)
- Verify: Compaction leaves partial files that are cleaned up on restart

#### **Subtask 7.5.4: Transaction Counter Recovery Test**
- Test: Run transactions, crash server, restart server
- Expectation: Transaction counter resumes from last value (no ID reuse)
- Validation: New transactions receive IDs higher than pre-crash transactions
- Verify: GlobalSequence also resumes correctly

#### **Subtask 7.5.5: Long Transaction Recovery Test**
- Test: Start transaction, insert documents, crash during transaction (no commit)
- Expectation:  Uncommitted writes are discarded
- Validation: After recovery, documents are not visible
- Verify: Transaction registry is cleared on startup (all uncommitted transactions aborted)

---

### **Task 7.6: Performance Tests**
Measure MVCC overhead and ensure acceptable performance

#### **Subtask 7.6.1: Read Performance Baseline**
- Measure: Read throughput (queries per second) without concurrent writes
- Expectation:  MVCC visibility check overhead is minimal (< 5% slowdown)
- Validation: Compare with baseline performance without MVCC
- Test scenarios: Point lookups, full scans, range queries

#### **Subtask 7.6.2: Write Performance Baseline**
- Measure: Write throughput (inserts per second) in autocommit mode
- Expectation: Version metadata overhead is minimal (< 10% slowdown)
- Validation: Compare with baseline performance without MVCC
- Test scenarios: Single inserts, batch inserts, updates, deletes

#### **Subtask 7.6.3: Mixed Workload Performance**
- Measure: Throughput with 80% reads and 20% writes running concurrently
- Expectation:  Reads are not blocked by writes, overall throughput is high
- Validation: Read latency remains low even with concurrent writes
- Compare:  With and without MVCC enabled

#### **Subtask 7.6.4: Transaction Commit Latency**
- Measure: Time to commit transaction with varying number of documents
- Expectation: Commit latency scales linearly with number of documents modified
- Validation: Commit latency is acceptable (< 10ms for 100 documents)
- Test scenarios: Small transactions (1-10 docs), medium (100 docs), large (1000 docs)

#### **Subtask 7.6.5: Compaction Performance**
- Measure: Time to compact bundle with multiple versions per document
- Expectation:  Compaction removes old versions efficiently
- Validation: Compaction completes in reasonable time (< 5 seconds for 10000 documents)
- Measure: Space reclaimed percentage

#### **Subtask 7.6.6: Long Transaction Impact**
- Measure: Memory usage and compaction efficiency with long-running transaction
- Expectation: Long transaction prevents compaction of old versions (expected behavior)
- Validation: System remains stable, memory does not grow unbounded
- Test:  Run transaction for 1 hour while heavy write load continues

#### **Subtask 7.6.7: Snapshot Creation Overhead**
- Measure:  Time to create snapshot (BEGIN TRANSACTION latency)
- Expectation: Snapshot creation is very fast (< 1ms)
- Validation: Snapshot creation does not block other operations
- Test:  Create 1000 transactions concurrently

---

### **Task 7.7: Stress Tests**
Validate system behavior under extreme conditions

#### **Subtask 7.7.1: High Concurrency Stress Test**
- Test: 100 concurrent reader threads, 100 concurrent writer threads
- Duration: 10 minutes of sustained load
- Expectation: No deadlocks, no crashes, no data corruption
- Validation: All operations complete successfully, data is consistent after test
- Monitor: CPU usage, memory usage, disk I/O

#### **Subtask 7.7.2: Version Accumulation Stress Test**
- Test: Update same set of 1000 documents 100 times each (creates 100 versions per document)
- Expectation: System handles version accumulation gracefully
- Validation:  Queries remain performant, compaction reclaims space
- Measure: Query latency trend as versions accumulate

#### **Subtask 7.7.3: Long Transaction Stress Test**
- Test: Start transaction, keep it open for 1 hour while heavy write load continues
- Expectation: Old versions are preserved, system remains stable
- Validation: Long transaction can query old data throughout, no memory exhaustion
- Measure: Memory usage growth, SSTable count growth

#### **Subtask 7.7.4: Rapid Transaction Churn Test**
- Test: Start and commit 10000 transactions as fast as possible
- Expectation:  Transaction ID allocation is fast and correct
- Validation: No ID collisions, all transactions logged to WAL
- Measure:  Transactions per second achieved

#### **Subtask 7.7.5: Compaction Under Load Test**
- Test:  Trigger compaction while heavy read and write load is active
- Expectation: Compaction completes without blocking operations
- Validation: Queries and writes continue during compaction, data remains consistent
- Measure: Compaction impact on query latency (should be minimal)

---

### **Task 7.8: Edge Case Tests**
Validate handling of unusual scenarios

#### **Subtask 7.8.1: Empty Transaction Test**
- Test: BEGIN TRANSACTION, COMMIT (no operations)
- Expectation: Transaction completes successfully with no side effects
- Validation: Transaction is logged to WAL, no documents modified

#### **Subtask 7.8.2: Transaction Timeout Test**
- Test:  BEGIN TRANSACTION, wait for timeout duration, attempt operation
- Expectation: Transaction is automatically aborted after timeout
- Validation: Operation fails with timeout error, transaction is aborted in WAL

#### **Subtask 7.8.3: Nested Transaction Test**
- Test: BEGIN TRANSACTION inside active transaction
- Expectation: Error returned (nested transactions not supported)
- Validation: First transaction remains active, second BEGIN is rejected

#### **Subtask 7.8.4: Commit Without Begin Test**
- Test: COMMIT without active transaction
- Expectation: Error returned (no active transaction)
- Validation: No state changes, clear error message

#### **Subtask 7.8.5: Rollback Without Begin Test**
- Test: ROLLBACK without active transaction
- Expectation: Error returned (no active transaction)
- Validation: No state changes, clear error message

#### **Subtask 7.8.6: Session Disconnect During Transaction Test**
- Test: BEGIN TRANSACTION, disconnect session before commit
- Expectation: Transaction is automatically aborted
- Validation:  Uncommitted changes are discarded, WAL contains ABORT record

#### **Subtask 7.8.7: DDL Inside Transaction Test**
- Test: BEGIN TRANSACTION, CREATE BUNDLE
- Expectation: Error returned (DDL not allowed in transactions)
- Validation: Transaction can continue with DML operations, CREATE BUNDLE is rejected

#### **Subtask 7.8.8: Query Deleted Document Test**
- Test: T1 starts, T2 deletes document, T2 commits, T1 queries document
- Expectation: T1 sees document (not deleted in T1's snapshot)
- Validation: T1 query returns document, T3 (started after T2 commits) does not see document

---

## **IMPLEMENTATION GUIDELINES**

### **Development Order**
1. Implement Phase 1 completely before moving to Phase 2 (foundation is critical)
2. Implement Phase 2 and Phase 3 together (write path and read path are tightly coupled)
3. Implement Phase 4 after Phase 3 is working (indexes depend on document MVCC)
4. Implement Phase 5 after Phase 4 (commit/abort depend on everything working)
5. Implement Phase 6 after Phase 5 (compaction is optimization, not required for correctness)
6. Implement Phase 7 continuously alongside each phase (test early, test often)

### **Testing Strategy**
- Write unit tests for each subtask as implemented
- Write integration tests for each task after all subtasks complete
- Write end-to-end tests for each phase after all tasks complete
- Run all tests before moving to next phase
- Maintain test coverage above 85% for all MVCC code

### **Performance Monitoring**
- Track visibility check performance (should be < 1 microsecond)
- Track snapshot creation performance (should be < 1 millisecond)
- Track commit latency (should be < 10 milliseconds for small transactions)
- Track compaction efficiency (should reclaim > 50% space when run)
- Alert if long-running transaction exists (> 10 minutes)

### **Backward Compatibility**
- Support loading databases without version metadata (migration path)
- Support mixed SSTable versions (old and new format)
- Provide configuration flag to disable MVCC (for testing/comparison)
- Ensure existing queries work without modification

### **Documentation Requirements**
- Document visibility rules clearly for future maintainers
- Document compaction retention policy
- Document transaction timeout behavior
- Document recovery behavior
- Provide troubleshooting guide for common issues

---

## **SUCCESS CRITERIA**

### **Correctness**
- ✅ All functional tests pass (Task 7.1)
- ✅ All concurrency tests pass (Task 7.2)
- ✅ All recovery tests pass (Task 7.5)
- ✅ No data corruption under stress (Task 7.7)

### **Performance**
- ✅ Read performance degradation < 5% (Task 7.6. 1)
- ✅ Write performance degradation < 10% (Task 7.6.2)
- ✅ Commit latency < 10ms for 100 documents (Task 7.6.4)
- ✅ Snapshot creation < 1ms (Task 7.6.7)

### **Stability**
- ✅ No crashes under high concurrency (Task 7.7.1)
- ✅ No deadlocks under any scenario (Task 7.7)
- ✅ Recovery works after any failure (Task 7.5)
- ✅ Memory usage stable with long transactions (Task 7.7.3)

### **Usability**
- ✅ Transaction commands work as documented
- ✅ Error messages are clear and actionable
- ✅ Backward compatibility maintained
- ✅ No breaking changes to existing code

---

## **RISK MITIGATION**

### **Risk:  Version Accumulation**
- Mitigation: Implement compaction early, test with many versions
- Monitoring: Track versions per document metric
- Fallback: Manual compaction trigger command

### **Risk: Long Transaction Blocking Compaction**
- Mitigation: Implement transaction timeout
- Monitoring: Alert on long-running transactions
- Fallback: Provide admin command to abort long transaction

### **Risk: Performance Regression**
- Mitigation:  Extensive performance testing (Task 7.6)
- Monitoring: Track query latency percentiles
- Fallback: Configuration flag to disable MVCC

### **Risk: Recovery Failure**
- Mitigation:  Comprehensive recovery testing (Task 7.5)
- Monitoring: Track WAL replay errors on startup
- Fallback: Restore from backup

---

This completes the fully revised MVCC implementation plan optimized for SyndrDB's LSM architecture. Each phase builds on the previous phase, and each task is broken down into actionable subtasks.  Follow the phases in order, test thoroughly at each stage, and you'll have a production-ready MVCC system. 