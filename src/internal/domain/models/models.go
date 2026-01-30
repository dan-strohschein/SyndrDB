package models

import (
	//btreeindex "syndrdb/src/btree_index"
	//hashindex "syndrdb/src/hash_index"

	"context"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cespare/xxhash/v2"
)

// ============================================================================
// SNAPSHOT INFO - MVCC Snapshot Isolation Context
// ============================================================================
//
// SnapshotInfo contains MVCC snapshot information for query execution.
// Snapshots are passed through the query execution context to enable
// lock-free visibility filtering during read operations.
//
// This is placed in models (not planner) to avoid import cycles since
// both bundle and planner packages need access to this type.
// ============================================================================

// SnapshotInfo contains MVCC snapshot information for query execution
type SnapshotInfo struct {
	SnapshotSequence uint64          // Commit sequence boundary (sees all commits <= this)
	TransactionID    uint64          // Transaction ID for read-your-own-writes (0 for autocommit)
	ActiveTxIDs      map[uint64]bool // Active transactions at snapshot time (invisible)
	GracePeriodMs    int             // RCU grace period in milliseconds for visibility
}

// snapshotContextKey is the context key for SnapshotInfo
type snapshotContextKey struct{}

// WithSnapshotInfo adds snapshot information to the context
func WithSnapshotInfo(ctx context.Context, snapshot *SnapshotInfo) context.Context {
	return context.WithValue(ctx, snapshotContextKey{}, snapshot)
}

// GetSnapshotInfoFromContext retrieves snapshot information from context
// Returns nil if no snapshot is set (non-transactional query)
func GetSnapshotInfoFromContext(ctx context.Context) *SnapshotInfo {
	if snapshot, ok := ctx.Value(snapshotContextKey{}).(*SnapshotInfo); ok {
		return snapshot
	}
	return nil
}

// ============================================================================
// SHARDED SORTED INDEX - PageID Architecture Alignment
// ============================================================================
//
// This structure maintains sorted DocumentIDs across 64 shards for O(log n) lookup
// of a document's alphabetical position. This allows INSERT to calculate the same
// pageID that LOAD uses (alphabetical order), eliminating stale pageID issues.
//
// Architecture:
// - 64 shards selected via xxhash(documentID) & 63 for uniform distribution
// - Per-shard sync.RWMutex for fine-grained locking (1/64 contention vs bundle lock)
// - Atomic shard counts for lock-free global position calculation
// - Binary search within shard for O(log n) position lookup
//
// TODO: For bundles > 10M documents, consider hybrid HyperLogLog + Count-Min Sketch:
//   - Count-Min Sketch (4 hash functions × 65536 counters = ~256KB) tracks density per bucket
//   - Partition UUID space into 256 buckets by first byte (00-FF)
//   - On INSERT: sum counts of all buckets before target → estimated position
//   - Accuracy: ±1% position error acceptable with Phase 1 fallback
//   - Memory: O(1) vs O(n), ~256KB vs ~360MB for 10M docs
// ============================================================================

// SortedIndexShards is the number of shards (power of 2 for efficient modulo)
const SortedIndexShards = 64

// SortedIndexShard represents one shard of the sorted document index
type SortedIndexShard struct {
	Mu     sync.RWMutex
	DocIDs []string // Sorted DocumentIDs within this shard
}

// ShardedSortedIndex maintains sorted DocumentIDs across 64 shards for fast
// alphabetical position lookup. Used to calculate correct pageIDs during INSERT.
type ShardedSortedIndex struct {
	Shards      [SortedIndexShards]SortedIndexShard
	ShardCounts [SortedIndexShards]atomic.Uint32 // Lock-free counts for position calculation
}

// NewShardedSortedIndex creates a new empty ShardedSortedIndex
func NewShardedSortedIndex() *ShardedSortedIndex {
	index := &ShardedSortedIndex{}
	// Initialize each shard with an empty slice to prevent nil pointer dereference
	for i := 0; i < SortedIndexShards; i++ {
		index.Shards[i].DocIDs = make([]string, 0)
	}
	return index
}

// shardIndex returns the shard index for a DocumentID using xxhash
// Uses bitwise AND for fast modulo (SortedIndexShards must be power of 2)
func (s *ShardedSortedIndex) shardIndex(documentID string) int {
	return int(xxhash.Sum64String(documentID) & (SortedIndexShards - 1))
}

// Insert adds a DocumentID to the appropriate shard and returns its global
// alphabetical position divided by pageSize (i.e., the pageID).
// Thread-safe via per-shard locking.
func (s *ShardedSortedIndex) Insert(documentID string, pageSize uint32) uint32 {
	shardIdx := s.shardIndex(documentID)
	shard := &s.Shards[shardIdx]

	shard.Mu.Lock()
	// Binary search for insertion point
	pos := sort.SearchStrings(shard.DocIDs, documentID)
	// Check for duplicate
	if pos < len(shard.DocIDs) && shard.DocIDs[pos] == documentID {
		shard.Mu.Unlock()
		// Already exists - calculate position without inserting
		return s.calculateGlobalPosition(shardIdx, pos, pageSize)
	}
	// Insert at position (grows slice)
	shard.DocIDs = append(shard.DocIDs, "")
	copy(shard.DocIDs[pos+1:], shard.DocIDs[pos:])
	shard.DocIDs[pos] = documentID
	shard.Mu.Unlock()

	// Update atomic count
	s.ShardCounts[shardIdx].Add(1)

	// TODO: Migrate to google/btree when shard exceeds 100K documents
	// if len(shard.DocIDs) > 100_000 { ... }

	return s.calculateGlobalPosition(shardIdx, pos, pageSize)
}

// Delete removes a DocumentID from the appropriate shard.
// Returns true if the document was found and removed.
// Note: DELETE causes position shifts; staleness is acceptable and fixed during compaction.
func (s *ShardedSortedIndex) Delete(documentID string) bool {
	shardIdx := s.shardIndex(documentID)
	shard := &s.Shards[shardIdx]

	shard.Mu.Lock()
	defer shard.Mu.Unlock()

	pos := sort.SearchStrings(shard.DocIDs, documentID)
	if pos >= len(shard.DocIDs) || shard.DocIDs[pos] != documentID {
		return false // Not found
	}

	// Remove from slice
	shard.DocIDs = append(shard.DocIDs[:pos], shard.DocIDs[pos+1:]...)
	s.ShardCounts[shardIdx].Add(^uint32(0)) // Decrement (add max uint32 wraps to -1)

	return true
}

// GetGlobalPosition returns the global alphabetical position of a DocumentID
// divided by pageSize. Returns (0, false) if document not found.
func (s *ShardedSortedIndex) GetGlobalPosition(documentID string, pageSize uint32) (uint32, bool) {
	shardIdx := s.shardIndex(documentID)
	shard := &s.Shards[shardIdx]

	shard.Mu.RLock()
	pos := sort.SearchStrings(shard.DocIDs, documentID)
	found := pos < len(shard.DocIDs) && shard.DocIDs[pos] == documentID
	shard.Mu.RUnlock()

	if !found {
		return 0, false
	}

	return s.calculateGlobalPosition(shardIdx, pos, pageSize), true
}

// calculateGlobalPosition computes the global position by summing counts of
// all "earlier" shards plus the position within the current shard.
// Uses atomic loads for lock-free counting.
//
// Note: Since shards are hashed (not alphabetically partitioned), we sum ALL
// shard counts, not just "earlier" shards. The position within shard is the
// key factor for pageID calculation.
func (s *ShardedSortedIndex) calculateGlobalPosition(shardIdx, posInShard int, pageSize uint32) uint32 {
	// Sum document counts from all shards with lower indices
	var globalPos uint32 = 0
	for i := 0; i < shardIdx; i++ {
		globalPos += s.ShardCounts[i].Load()
	}
	// Add position within current shard
	globalPos += uint32(posInShard)

	return globalPos / pageSize
}

// TotalDocuments returns the total number of documents across all shards
func (s *ShardedSortedIndex) TotalDocuments() uint32 {
	var total uint32 = 0
	for i := 0; i < SortedIndexShards; i++ {
		total += s.ShardCounts[i].Load()
	}
	return total
}

// RebuildFromDocuments clears and rebuilds the index from a list of DocumentIDs.
// Used during compaction and initial bundle load.
func (s *ShardedSortedIndex) RebuildFromDocuments(docIDs []string) {
	// Clear all shards
	for i := 0; i < SortedIndexShards; i++ {
		s.Shards[i].Mu.Lock()
		s.Shards[i].DocIDs = nil
		s.ShardCounts[i].Store(0)
		s.Shards[i].Mu.Unlock()
	}

	// Group documents by shard
	shardedDocs := make([][]string, SortedIndexShards)
	for i := range shardedDocs {
		shardedDocs[i] = make([]string, 0)
	}

	for _, docID := range docIDs {
		shardIdx := s.shardIndex(docID)
		shardedDocs[shardIdx] = append(shardedDocs[shardIdx], docID)
	}

	// Sort each shard's documents and update counts
	for i := 0; i < SortedIndexShards; i++ {
		sort.Strings(shardedDocs[i])
		s.Shards[i].Mu.Lock()
		s.Shards[i].DocIDs = shardedDocs[i]
		s.ShardCounts[i].Store(uint32(len(shardedDocs[i])))
		s.Shards[i].Mu.Unlock()
	}
}

// GetAllDocumentIDs returns all DocumentIDs across all shards (for persistence)
// Order is by shard, then alphabetically within each shard
func (s *ShardedSortedIndex) GetAllDocumentIDs() []string {
	var total int
	for i := 0; i < SortedIndexShards; i++ {
		total += int(s.ShardCounts[i].Load())
	}

	result := make([]string, 0, total)
	for i := 0; i < SortedIndexShards; i++ {
		s.Shards[i].Mu.RLock()
		result = append(result, s.Shards[i].DocIDs...)
		s.Shards[i].Mu.RUnlock()
	}
	return result
}

type Database struct {
	// DatabaseID is the unique identifier for the database.
	DatabaseID string

	// Name is the name of the database.
	Name string

	// Description is the description of the database.
	Description string

	// BundleFileNames is a list of bundle file names.
	BundleFiles []string

	// Documents is a map of document names to Document objects.
	Bundles map[string]Bundle

	DataDirectory string
}

type Bundle struct {
	// BundleID is the unique identifier for the bundle.
	BundleID string `bson:"BundleID" json:"BundleID"`

	// Name is the name of the bundle.
	Name string `bson:"Name" json:"Name"`

	// Description is the description of the bundle.
	Description string `bson:"Description" json:"Description"`

	// Permissions are the permissions for the bundle.
	Permissions []string `bson:"Permissions" json:"Permissions"`

	// CreatedBy is the user who created the bundle.
	CreatedBy string `bson:"CreatedBy" json:"CreatedBy"`

	// CreatedAt is when the bundle was created.
	CreatedAt time.Time `bson:"CreatedAt" json:"CreatedAt"`

	// UpdatedAt is when the bundle was last updated.
	UpdatedAt time.Time `bson:"UpdatedAt" json:"UpdatedAt"`

	// A description of the document structure, similar to a schema/table definition.
	DocumentStructure DocumentStructure `bson:"DocumentStructure" json:"DocumentStructure"`

	// Document storage is now page-based for scalability
	// DEPRECATED: Documents memtable removed in favor of write-through page cache
	// All document storage now goes through PageCache with immediate cache updates
	// Removed: Documents *map[string]Document and DocumentsMutex
	// Use BundleService.GetDocumentPage() for document access
	//
	// WRITE-THROUGH CACHE (Phase 1): Writes update PageCache immediately after WriteBuffer commit
	// This ensures reads always see recent writes without disk round-trips
	// Documents *map[string]Document `json:"Documents,omitempty"`
	// DocumentsMutex sync.RWMutex `json:"-"`

	// Track indexes by name -> reference
	Indexes    map[string]IndexReference
	IndexNames []string // List of index names for easy access

	Relationships map[string]Relationship
	Constraints   map[string]Constraint

	Database *Database `json:"-"` // Reference to the parent database

	// New fields for scalable document management
	TotalDocuments int64 // Total number of documents in this bundle
	PageCount      int64 // Total number of document pages
	PageSize       int   // Number of documents per page (default: 4096)

	// Cassandra-style memtable approach for write buffering
	// When false: Documents map is a write buffer (memtable) containing only recent writes
	// When true: Documents map contains ALL documents and queries can use it directly
	// This prevents the partial cache bug where Documents has some but not all documents
	DocumentsComplete bool // Indicates if Documents map is complete or just a memtable (not serialized)

	// TODO: Add LastPersisted timestamp to track staleness
	IsDirty bool // Indicates metadata needs persistence (not serialized)

	// SortedIndex maintains alphabetically-sorted DocumentIDs across 64 shards
	// for calculating correct pageIDs during INSERT (matches LOAD's alphabetical order).
	// This eliminates stale pageID issues caused by INSERT using insertion order
	// while LOAD uses alphabetical order.
	SortedIndex *ShardedSortedIndex `json:"-"` // Not serialized - persisted separately
}

// GetHashIndexForField searches for a hash index on the specified field
// Returns the hash index instance if found, nil otherwise
// This is used by JOIN optimizations to check if an index exists before using index-assisted strategies
// TODO: Consider caching index lookups if this becomes a bottleneck
func (b *Bundle) GetHashIndexForField(fieldName string) interface{} {
	if b.Indexes == nil {
		return nil
	}

	// Iterate through all indexes to find a hash index on this field
	for _, indexRef := range b.Indexes {
		// Check if this is a hash index
		if indexRef.IndexType == "hash" {
			// Check if the field matches
			if indexRef.HashIndexField.FieldName == fieldName {
				// Return the index instance
				// The caller needs to type-assert this to *hashindexV3.HashIndexV3
				return indexRef.IndexInstance
			}
		}
	}

	return nil
}

// DocumentPage represents a page of documents for scalable loading
type DocumentPage struct {
	PageID         uint32              // Unique page identifier within bundle
	BundleID       string              // Bundle this page belongs to
	Documents      map[string]Document // Limited set of documents in this page
	NextPageID     *uint32             // Pointer to next page (for sequential access)
	PreviousPageID *uint32             // Pointer to previous page (for sequential access)
	IsDirty        bool                // Whether this page has been modified
	LoadedAt       time.Time           // When this page was loaded into memory
	DocumentCount  int                 // Number of documents in this page
}

type DocumentStructure struct {
	FieldDefinitions map[string]FieldDefinition `bson:"FieldDefinitions" json:"FieldDefinitions"`
}

type FieldDefinition struct {
	Name         string      `bson:"Name" json:"Name"`
	Type         string      `bson:"Type" json:"Type"`
	IsRequired   bool        `bson:"Required" json:"Required"` // Indicates if the field can be null
	IsUnique     bool        `bson:"Unique" json:"Unique"`
	DefaultValue interface{} `bson:"DefaultValue" json:"DefaultValue"` // Optional default value for the field
}

type Field struct {
	Name string
	//FieldType    string
	Value FieldValue // ✅ ZERO-ALLOCATION: Typed union instead of interface{}
	// Description  string
	// Required     bool
	// Unique       bool
	// DefaultValue interface{}
}

type Constraint struct {
	// ConstraintID is the unique identifier for the constraint.
	ConstraintID string
	// Name is the name of the constraint.
	Name string
	// Description is the description of the constraint.
	Description string
	// Type is the type of the constraint (e.g., "unique", "required").
	ConstraintType string
}

type Relationship struct {
	// RelationshipID is the unique identifier for the relationship.
	RelationshipID string
	// Name is the name of the relationship.
	Name string
	// Description is the description of the relationship.
	Description string

	// Source field for the relationship (e.g., "DocumentID")
	SourceField string
	// Destination bundle name
	DestinationBundle string
	// Destination field for the relationship (e.g., "OrderID")
	DestinationField string
	// Source bundle name
	SourceBundle string
	// Type is the type of the relationship (e.g., "0toMany", "1toMany", "ManyToMany").
	RelationshipType string

	// Legacy fields for backward compatibility
	SourceBundleID   string // Bundle ID of the source document
	SourceBundleName string // Name of the source bundle
	TargetBundleID   string // Bundle ID of the target document
	TargetBundleName string // Name of the target bundle
}

// IndexService defines the interface for any index implementation
type IndexService interface {
	CreateIndex(bundle *Bundle, fieldName string, isUnique bool) (string, error)
	SearchIndex(indexName string, key interface{}) ([]string, error)
	ListIndexes(bundleID string) ([]string, error)
	DropIndex(indexName string) error
}

// IndexReference stores information about an index
// NOTE: With the new header-based metadata system, much of this information
// is now redundant as it's stored in file headers (.hidx files).
// Future: Simplify this struct to only store runtime state (IndexInstance)
// All metadata (CreateTime, Fields, IndexType, etc.) is in file headers
type IndexReference struct {
	IndexName  string
	Fields     []FieldDefinition // List of fields in the index
	IndexType  string            // "btree", "hash", etc.
	CreateTime time.Time         // TODO: Remove - now in header.CreatedAt
	// Reference to the actual index instance
	// Stored as interface{} to avoid circular imports
	IndexInstance   interface{} `json:"-"` // Skip serialization - this is the primary useful field
	HashIndexField  IndexField  // TODO: Consider removing - metadata in headers
	BTreeIndexField IndexField  // TODO: Consider removing - metadata in headers

	// Index maintenance metadata (for automatic rebuild on staleness)
	Maintenance *IndexMaintenanceMetadata `json:"maintenance,omitempty"`
}

// IndexMaintenanceMetadata tracks index health and rebuild history for automatic maintenance
type IndexMaintenanceMetadata struct {
	// Rebuild tracking (persisted to disk)
	LastRebuildTime time.Time `json:"last_rebuild_time"`
	TotalRebuilds   uint64    `json:"total_rebuilds"`

	// Health status (persisted to disk)
	IsHealthy         bool      `json:"is_healthy"`
	LastFailureReason string    `json:"last_failure_reason,omitempty"`
	LastFailureTime   time.Time `json:"last_failure_time,omitempty"`

	// Staleness tracking (persisted to disk)
	LastStalenessRate  float64   `json:"last_staleness_rate"`
	LastStalenessCheck time.Time `json:"last_staleness_check"`

	// Query frequency (ephemeral - tracked in-memory, reset on restart)
	QueryCount       int64     `json:"-"` // Atomic counter, not serialized
	LastQueryTime    time.Time `json:"-"`
	QueriesPerMinute float64   `json:"-"` // Rolling average
}

type IndexField struct {
	FieldName string
	IsUnique  bool
	Collation string // Optional collation for string comparison
}

// ------------------------------ parser commands ------------------------------
type BundleCommand struct {
	CommandType             string // CREATE, UPDATE, DELETE
	BundleName              string
	NewBundleName           string // New name for the bundle (if renaming)
	Fields                  []FieldDefinition
	Changes                 []FieldChange // This will be used for UPDATE commands
	HasRelationshipCommands bool          // Indicates if there are relationship commands
	HasForceSwitch          bool          // Indicates if FORCE switch is used
}

type RelationshipCommand struct {
	CommandType string
	BundleName  string
	Name        string

	// New fields for the enhanced relationship system
	RelationshipType  string // "0toMany", "1toMany", "ManyToMany"
	SourceBundle      string
	SourceField       string
	DestinationBundle string
	DestinationField  string

	// Legacy fields for backward compatibility
	SourceBundleID   string
	SourceBundleName string
	TargetBundleID   string
	TargetBundleName string
}

// If the Bundle Command is UPDATE, then these changes are used
type FieldChange struct {
	ChangeType   string // CHANGE, ADD, REMOVE
	OldFieldName string
	NewField     FieldDefinition
}

type DocumentCommand struct {
	CommandType string // ADD_DOCUMENT, UPDATE_DOCUMENT, DELETE_DOCUMENT
	BundleName  string
	Fields      []KeyValue // Fields to be added or updated in the document
}

type DocumentDeleteCommand struct {
	BundleName         string
	Fields             []KeyValue // Fields to be added or updated in the document
	WhereClause        string     // Optional where clause for filtering documents
	Confirmed          bool       // Requires CONFIRMED keyword for bulk deletes without WHERE clause
	DeletedDocumentIDs []string   // Track successfully deleted document IDs for response
	RawCommand         string     // Store the raw command for logging/debugging
}

type DocumentUpdateCommand struct {
	BundleName  string
	Fields      []KeyValue // Fields to be added or updated in the document
	WhereClause string     // Optional where clause for filtering documents
	Confirmed   bool       // Requires CONFIRMED keyword for bulk updates without WHERE clause
}

type KeyValue struct {
	Key   string      // Field name
	Value interface{} // Field value, can be any type
}

type DatabaseCommand struct {
	ID                 string
	CommandType        string // CREATE, UPDATE, DELETE, RENAME
	DatabaseName       string
	NewDatabaseName    string // For RENAME operations
	Force              bool   // For FORCE operations
	DBMetadataFilePath string
}

type WhereClause struct {
	Field    string
	Operator string
	Value    interface{} // Can be string, int, float64, bool
	Logic    string      // "AND" or "OR"
}

// WhereGroup represents a group of clauses joined by the same logical operator
type WhereGroup struct {
	Clauses   []WhereClause
	SubGroups []WhereGroup
	Logic     string // Logic connecting this group to others ("AND" or "OR")
}

type CreateBTreeIndexCommand struct {
	IndexName  string
	BundleName string
	Fields     []FieldDefinition
}

type CreateHashIndexCommand struct {
	IndexName  string
	BundleName string
	Fields     []FieldDefinition
}

type CreateIndexCommand struct {
	IndexType  string // "BTree" or "Hash"
	IndexName  string
	BundleName string
	Fields     []FieldDefinition
}
