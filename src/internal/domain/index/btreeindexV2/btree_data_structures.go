package btreeindexV2

/*
BTREE INDEX DATA STRUCTURES

This file implements the core data structures for the BTree index system in SyndrDB.
The implementation follows the B+ tree algorithm used in PostgreSQL, MySQL, and SQL Server,
providing efficient range queries, ordered traversal, and balanced tree operations.

BTREE ALGORITHM OVERVIEW:
B+ trees are balanced tree data structures where:
- All leaf nodes are at the same level
- Internal nodes store keys and pointers to child nodes
- Leaf nodes store key-value pairs and are linked for range queries
- Tree remains balanced through splitting and merging operations
- Provides O(log n) search, insert, and delete operations

DATA STRUCTURE HIERARCHY:
- BTreeIndex: Main index structure managing the entire tree
- BTreeMetadata: Configuration and statistics for the index
- BTreeNode: Represents internal and leaf nodes in the tree
- BTreePage: Page-based storage structure for persistence
- IndexEntry: Individual key-document ID pairs
- BTreeConfig: Configuration parameters for index creation

PAGE-BASED STORAGE:
Following PostgreSQL's approach, the BTree uses fixed-size pages (typically 8KB)
for efficient disk I/O and memory management. Each page contains a header with
metadata and a variable number of index entries depending on key sizes.

CONCURRENCY DESIGN:
The data structures are designed to support concurrent access through read-write
locks at the index level, with future support for page-level locking.
*/

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"
)

// BTreeIndex represents the main BTree index structure
// This is the primary interface for all BTree operations
type BTreeIndex struct {
	FilePath    string             // Path to the index file
	Metadata    *BTreeMetadata     // Index configuration and statistics
	FileManager *BTreeFileManager  // Handles file I/O operations
	PageManager *BTreePageManager  // Manages page caching
	rootPageNum uint32             // Page number of the root node
	mutex       sync.RWMutex       // Thread safety for concurrent access
	isOpen      bool               // Whether the index is currently open
	bundleName  string             // Name of the bundle this index belongs to
	fieldName   string             // Name of the field being indexed
	logger      *zap.SugaredLogger // Logger for debug and error messages

	// Crash recovery optimization: skip validation for recently-validated cached indexes
	// PostgreSQL uses similar approach to avoid redundant validation overhead
	lastValidated time.Time // Timestamp of last successful crash recovery validation
	fromCache     bool      // Whether index was loaded from memory cache vs disk

	// WAL integration for durability and crash recovery
	// Following PostgreSQL's WAL design: log BEFORE modifying index
	// TODO: I could add async WAL writing with batching for higher throughput
	walManager *BTreeWALManager // WAL manager for logging index operations
	walEnabled bool             // Whether WAL is enabled (mandatory for production)
	nextLSN    uint64           // Next Log Sequence Number to assign

	// TODO: Integrate shared WAL into BTreeIndex - add sharedWAL field, log operations before applying in batched mode, delay WritePage fsync when batched
	// URGENT TODO: On index Open(), check for uncommitted WAL entries and replay them to recover from crash. This requires integration with database-level WAL recovery system (implement in subsequent project)
	// IMPORTANT NOTE: Batched mode delays fsync but relies on WAL for crash recovery - durability guaranteed by WAL replay on restart
	// sharedWAL *journal.WriteAheadLog // Shared bundle-level WAL (injected from BundleService)
	// indexName string                 // Index name for WAL logging prefix
	// syncMode string                  // "immediate", "batched", or "scheduled" from settings.BTreeSyncMode
}

func (idx *BTreeIndex) GetRootPageNum() uint32 {
	return idx.rootPageNum
}

// BTreeMetadata contains index configuration and runtime statistics
// This structure is persisted to disk and loaded when the index is opened
type BTreeMetadata struct {
	// File integrity and versioning
	MagicNumber  uint32    // Magic number for file validation (0x42545245 = "BTRE")
	Version      uint32    // File format version for compatibility
	CreatedAt    time.Time // When the index was created
	LastModified time.Time // Last modification timestamp

	// Tree configuration
	PageSize    uint32 // Size of each page in bytes (typically 8192)
	Order       uint32 // Maximum number of keys per node (computed from page size)
	TreeHeight  uint32 // Current height of the tree
	RootPageNum uint32 // Page number of the root node

	// Storage management
	NextPageNum uint32   // Next available page number for allocation
	TotalPages  uint32   // Total number of pages allocated
	FreePages   []uint32 // List of deallocated pages available for reuse

	// Statistics
	TotalRecords  uint64 // Total number of key-value pairs in the index
	TotalNodes    uint32 // Total number of nodes (internal + leaf)
	LeafNodes     uint32 // Number of leaf nodes
	InternalNodes uint32 // Number of internal nodes
	TotalKeys     uint64 // Total number of keys indexed

	// Performance metrics
	SplitCount              uint64  // Number of node splits performed
	MergeCount              uint64  // Number of node merges performed
	SearchCount             uint64  // Number of search operations
	InsertCount             uint64  // Number of insert operations
	DeleteCount             uint64  // Number of delete operations
	TotalKeysFound          uint64  `json:"total_keys_found"`
	TotalNodesVisited       uint64  `json:"total_nodes_visited"`
	AverageSearchEfficiency float64 `json:"average_search_efficiency"`
	MaxNodesVisited         uint32  `json:"max_nodes_visited"`
	TotalNodesDeleted       uint64  `json:"total_nodes_deleted"`
	StructuralChanges       uint64  `json:"structural_changes"`
	AverageNodesDeleted     float64 `json:"average_nodes_deleted"`
	CompactionCount         uint64  // Number of compaction operations performed
	RecordCount             uint64  // Total number of records in the bundle

	// Insertion performance metrics
	TotalNodesCreated   uint64  `json:"total_nodes_created"`
	AverageNodesCreated float64 `json:"average_nodes_created"`
	TreeGrowthEvents    uint64  `json:"tree_growth_events"`

	// Index properties
	IndexName  string // Name of the index (for debugging)
	BundleName string // Name of the bundle this index belongs to
	FieldName  string // Name of the field being indexed
	IsUnique   bool   // Whether the index enforces uniqueness
	AllowNulls bool   // Whether null values are allowed
	DebugMode  bool   // Whether to use ASCII format for debugging

	// Maintenance
	LastCompaction    time.Time // Last time the index was compacted
	LastMaintenance   time.Time // Last maintenance operation performed
	LastValidation    time.Time // Last validation check performed
	FragmentationPct  float64   // Percentage of fragmented space
	CompactionNeeded  bool      // Whether compaction is needed
	MaintenanceNeeded bool      // Whether maintenance is needed
	FillFactor        float64   `json:"fill_factor"`        // Average node utilization (0.0 to 1.0)
	AverageKeyLength  float64   `json:"average_key_length"` // Average length of keys in bytes

	// Lazy deletion tracking (PostgreSQL-style)
	TotalTombstones     uint64  // Total number of tombstones across all nodes
	TombstoneRatio      float64 // Ratio of tombstones to live records (triggers compaction)
	NodesNeedCompaction uint32  // Number of nodes flagged for compaction

	// Performance optimization: throttle expensive maintenance checks
	OperationsSinceLastCheck uint64 // Counter to throttle checkMaintenanceNeeded() calls

	// UNIQUE INDEX MEMORY MANAGEMENT: Estimated memory usage for in-memory loading
	// This is calculated as TotalPages × PageSize × 1.2 (20% overhead for Go structures)
	// Updated on every batch of insert/delete operations and persisted during metadata flush
	EstimatedMemorySizeBytes int64 // Estimated memory usage in bytes for in-memory loading
}

// BTreeNode represents a node in the BTree (internal or leaf)
// Internal nodes store keys and page numbers of child nodes
// Leaf nodes store keys and document IDs, and are linked for range queries
type BTreeNode struct {
	PageNum      uint32     // Page number where this node is stored
	IsLeaf       bool       // Whether this is a leaf node
	KeyCount     uint32     // Number of keys currently in the node
	Keys         [][]byte   // Array of keys (variable length)
	Values       [][]string // For leaf nodes: arrays of document IDs per key
	Children     []uint32   // For internal nodes: page numbers of child nodes
	NextLeaf     uint32     // For leaf nodes: page number of next leaf (0 if none)
	PrevLeaf     uint32     // For leaf nodes: page number of previous leaf (0 if none)
	ParentPage   uint32     // Page number of parent node (0 for root)
	LastModified time.Time  // When this node was last modified
	FreeSpace    uint32     // Available space in bytes for new entries
	Checksum     uint64     // xxHash64 checksum for corruption detection (8 bytes)

	// Lazy deletion support (PostgreSQL-style deferred merging)
	Tombstones      map[string]bool // Map of deleted document IDs (key: key+docID)
	TombstoneCount  uint32          // Number of tombstones in this node
	NeedsCompaction bool            // Flag indicating node should be compacted during VACUUM
}

// BTreePage represents the on-disk format of a BTree node
// This structure handles serialization and deserialization of nodes
type BTreePage struct {
	PageNum      uint32    // Page number identifier
	PageType     uint8     // Type: 1=Internal, 2=Leaf, 3=Metadata
	PageSize     uint32    // Total size of the page
	HeaderSize   uint32    // Size of the page header
	FreeSpace    uint32    // Available space for new entries
	ItemCount    uint32    // Number of items (keys/entries) in the page
	NextPage     uint32    // For leaf pages: next leaf page number
	PrevPage     uint32    // For leaf pages: previous leaf page number
	ParentPage   uint32    // Parent page number
	LastModified time.Time // Last modification timestamp
	Checksum     uint64    // xxHash64 page integrity checksum (8 bytes)
	Data         []byte    // Serialized page data
}

// IndexEntry represents a single key-value pair in the index
// This is the atomic unit of data stored in leaf nodes
type IndexEntry struct {
	Key         []byte    // The indexed key (variable length)
	DocumentIDs []string  // List of document IDs for this key
	KeyLength   uint32    // Length of the key in bytes
	ValueCount  uint32    // Number of document IDs
	Timestamp   time.Time // When this entry was created/modified
}

// BTreeConfig contains configuration parameters for creating a BTree index
// This structure defines the initial parameters and constraints for the index
type BTreeConfig struct {
	BundleName   string // Name of the bundle this index belongs to
	FieldName    string // Name of the field being indexed
	DataDir      string // Directory where index files are stored
	IsUnique     bool   // Whether the index enforces uniqueness
	AllowNulls   bool   // Whether null values are allowed
	DebugMode    bool   // Whether to use ASCII format for debugging
	PageSize     uint32 // Size of each page in bytes
	CacheSize    int    // Number of pages to cache in memory
	InitialPages uint32 // Initial number of pages to allocate
	MaxKeyLength uint32 // Maximum allowed key length in bytes
}

// BTreeStats contains runtime statistics about the index
// This provides insights into index performance and usage patterns
type BTreeStats struct {
	TotalRecords      uint64        // Total number of records in the index
	TotalNodes        uint32        // Total number of nodes
	TreeHeight        uint32        // Height of the tree
	AverageKeyLength  float64       // Average length of keys
	FillFactor        float64       // Percentage of node capacity used
	FragmentationPct  float64       // Percentage of fragmented space
	CacheHitRate      float64       // Cache hit rate percentage
	TotalSearches     uint64        // Total number of search operations
	TotalInserts      uint64        // Total number of insert operations
	TotalDeletes      uint64        // Total number of delete operations
	AverageSearchTime time.Duration // Average search operation time
	LastUpdated       time.Time     // When these statistics were last updated
}

// TreeBalanceResult represents the results of tree balance analysis
// This structure contains comprehensive metrics about tree health and balance characteristics
type TreeBalanceResult struct {
	IsBalanced       bool     `json:"is_balanced"`        // Whether the tree maintains proper balance
	Height           uint32   `json:"height"`             // Maximum depth of the tree
	TotalNodes       uint32   `json:"total_nodes"`        // Total number of nodes in the tree
	FragmentationPct float64  `json:"fragmentation_pct"`  // Percentage of fragmentation
	FillFactor       float64  `json:"fill_factor"`        // Average node utilization (0.0 to 1.0)
	AverageKeyLength float64  `json:"average_key_length"` // Average length of keys in bytes
	Errors           []string `json:"errors"`             // List of balance validation errors
}

// IndexHealthStats represents comprehensive health metrics for an index
// This structure contains all relevant health indicators used for maintenance decisions
type IndexHealthStats struct {
	TotalNodes       uint32   `json:"total_nodes"`        // Total number of nodes in the index
	TreeHeight       uint32   `json:"tree_height"`        // Height of the tree
	FragmentationPct float64  `json:"fragmentation_pct"`  // Percentage of fragmentation
	FillFactor       float64  `json:"fill_factor"`        // Average node utilization (0.0 to 1.0)
	AverageKeyLength float64  `json:"average_key_length"` // Average length of keys in bytes
	IsHealthy        bool     `json:"is_healthy"`         // Overall health status
	Issues           []string `json:"issues"`             // List of health issues found
}

// NewBTreeIndex creates a new BTree index instance
// Parameters:
//   - config: Configuration parameters for the index
//   - fileManager: File manager for I/O operations
//   - pageManager: Page manager for caching
//   - logger: Logger for debug and error messages
//
// Returns:
//   - *BTreeIndex: The created index instance
//   - error: Any error that occurred during creation
func NewBTreeIndex(config *BTreeConfig, fileManager *BTreeFileManager, pageManager *BTreePageManager, logger *zap.SugaredLogger) (*BTreeIndex, error) {
	if config == nil {
		return nil, fmt.Errorf("config cannot be nil")
	}

	if fileManager == nil {
		return nil, fmt.Errorf("fileManager cannot be nil")
	}

	if pageManager == nil {
		return nil, fmt.Errorf("pageManager cannot be nil")
	}

	// Calculate tree order based on page size
	order := calculateTreeOrder(config.PageSize, config.MaxKeyLength)

	metadata := &BTreeMetadata{
		Version:        1,
		CreatedAt:      time.Now(),
		LastModified:   time.Now(),
		PageSize:       config.PageSize,
		Order:          order,
		TreeHeight:     1,
		RootPageNum:    1, // Page 0 is metadata, root starts at page 1
		NextPageNum:    2,
		TotalPages:     2, // Metadata page + root page
		FreePages:      make([]uint32, 0),
		TotalRecords:   0,
		TotalNodes:     1,
		LeafNodes:      1,
		InternalNodes:  0,
		BundleName:     config.BundleName,
		FieldName:      config.FieldName,
		IsUnique:       config.IsUnique,
		AllowNulls:     config.AllowNulls,
		DebugMode:      config.DebugMode,
		LastCompaction: time.Now(),
		// Initialize memory size: 2 pages × PageSize × 1.2 overhead
		EstimatedMemorySizeBytes: int64(float64(2*config.PageSize) * 1.2),
	}

	index := &BTreeIndex{
		FilePath:    fileManager.GetFilePath(),
		Metadata:    metadata,
		FileManager: fileManager,
		PageManager: pageManager,
		rootPageNum: 1,
		mutex:       sync.RWMutex{},
		isOpen:      true,
		bundleName:  config.BundleName,
		fieldName:   config.FieldName,
		logger:      logger,
	}

	// Start background flush worker for automatic dirty page management
	pageManager.StartFlushWorker()
	logger.Debugf("Started flush worker for BTree index '%s'", config.BundleName)

	logger.Debugf("Created new BTree index for bundle '%s' field '%s' with order %d",
		config.BundleName, config.FieldName, order)

	return index, nil
}

// NewBTreeNode creates a new BTree node
// Parameters:
//   - isLeaf: Whether this is a leaf node
//   - pageSize: Size of the page this node will be stored in
//
// Returns:
//   - *BTreeNode: The created node
func NewBTreeNode(isLeaf bool, pageSize uint32) *BTreeNode {
	// Calculate maximum number of keys based on page size
	maxKeys := calculateMaxKeys(pageSize, isLeaf)

	node := &BTreeNode{
		PageNum:      0, // Will be set when node is stored
		IsLeaf:       isLeaf,
		KeyCount:     0,
		Keys:         make([][]byte, 0, maxKeys),
		LastModified: time.Now(),
		FreeSpace:    pageSize - calculateNodeHeaderSize(),
	}

	if isLeaf {
		node.Values = make([][]string, 0, maxKeys)
		node.NextLeaf = 0
		node.PrevLeaf = 0
	} else {
		node.Children = make([]uint32, 0, maxKeys+1) // Internal nodes have one more child than keys
	}

	return node
}

// NewBTreePage creates a new BTree page
// Parameters:
//   - pageNum: Page number identifier
//   - pageSize: Size of the page in bytes
//
// Returns:
//   - *BTreePage: The created page
func NewBTreePage(pageNum uint32, pageSize uint32) *BTreePage {
	headerSize := calculatePageHeaderSize()

	return &BTreePage{
		PageNum:      pageNum,
		PageType:     2, // Default to leaf page
		PageSize:     pageSize,
		HeaderSize:   headerSize,
		FreeSpace:    pageSize - headerSize,
		ItemCount:    0,
		NextPage:     0,
		PrevPage:     0,
		ParentPage:   0,
		LastModified: time.Now(),
		Checksum:     0,
		Data:         make([]byte, 0, pageSize-headerSize),
	}
}

// NewIndexEntry creates a new index entry
// Parameters:
//   - key: The key for this entry
//   - documentID: The document ID associated with this key
//
// Returns:
//   - *IndexEntry: The created entry
func NewIndexEntry(key []byte, documentID string) *IndexEntry {
	return &IndexEntry{
		Key:         append([]byte(nil), key...), // Make a copy of the key
		DocumentIDs: []string{documentID},
		KeyLength:   uint32(len(key)),
		ValueCount:  1,
		Timestamp:   time.Now(),
	}
}

// NewBTreeMetadata creates a new metadata structure with default values
// Parameters:
//   - config: Configuration parameters
//
// Returns:
//   - *BTreeMetadata: The created metadata
func NewBTreeMetadata(config *IndexConfig) *BTreeMetadata {
	order := calculateTreeOrder(config.PageSize, config.MaxKeyLength)

	return &BTreeMetadata{
		MagicNumber:    0x42545245, // "BTRE" - file integrity validation
		Version:        1,
		CreatedAt:      time.Now(),
		LastModified:   time.Now(),
		PageSize:       config.PageSize,
		Order:          order,
		TreeHeight:     1,
		RootPageNum:    1,
		NextPageNum:    2,
		TotalPages:     2,
		FreePages:      make([]uint32, 0),
		TotalRecords:   0,
		TotalNodes:     1,
		LeafNodes:      1,
		InternalNodes:  0,
		BundleName:     config.BundleName,
		FieldName:      config.FieldName,
		IsUnique:       config.IsUnique,
		AllowNulls:     config.AllowNulls,
		DebugMode:      config.DebugMode,
		LastCompaction: time.Now(),
		// Initialize memory size: 2 pages × PageSize × 1.2 overhead
		EstimatedMemorySizeBytes: int64(float64(2*config.PageSize) * 1.2),
	}
}

// Node operation methods

// IsFull checks if the node has reached its maximum capacity
// Returns:
//   - bool: True if the node is full
func (node *BTreeNode) IsFull() bool {
	return node.FreeSpace < calculateMinEntrySize()
}

// UpdateDeletionMetrics updates deletion-specific performance metrics
// This method tracks detailed deletion statistics for performance analysis
// Parameters:
//   - nodesDeleted: Number of nodes that were deleted or merged
//   - structuralChanges: Whether the deletion caused structural changes
func (meta *BTreeMetadata) UpdateDeletionMetrics(nodesDeleted int, structuralChanges bool) {
	// Track total nodes deleted for maintenance planning
	meta.TotalNodesDeleted += uint64(nodesDeleted)

	// Track structural changes for tree health monitoring
	if structuralChanges {
		meta.StructuralChanges++
	}

	// Update average nodes deleted per operation
	if meta.DeleteCount > 0 {
		meta.AverageNodesDeleted = float64(meta.TotalNodesDeleted) / float64(meta.DeleteCount)
	}
}

// CalculateMemorySize calculates the estimated memory usage for in-memory loading
// Uses TotalPages × PageSize × 1.2 to account for Go structure overhead (20%)
// This estimation allows for LRU eviction decisions based on memory budget
// Returns:
//   - int64: Estimated memory usage in bytes
func (meta *BTreeMetadata) CalculateMemorySize() int64 {
	// Base calculation: total pages × page size
	baseSize := int64(meta.TotalPages) * int64(meta.PageSize)

	// Add 20% overhead for Go data structures (slices, maps, pointers, etc.)
	// This accounts for: slice headers, string headers, map buckets, alignment padding
	estimatedSize := int64(float64(baseSize) * 1.2)

	return estimatedSize
}

// UpdateMemorySize recalculates and updates the EstimatedMemorySizeBytes field
// Should be called periodically (e.g., every 100 operations) to keep estimate current
// This method is cheap (just multiplication) so calling it frequently is acceptable
func (meta *BTreeMetadata) UpdateMemorySize() {
	meta.EstimatedMemorySizeBytes = meta.CalculateMemorySize()
}

// CanMerge checks if this node can be merged with a sibling
// Parameters:
//   - sibling: The sibling node to check for merging
//
// Returns:
//   - bool: True if the nodes can be merged
func (node *BTreeNode) CanMerge(sibling *BTreeNode) bool {
	if node.IsLeaf != sibling.IsLeaf {
		return false
	}

	totalKeys := node.KeyCount + sibling.KeyCount
	maxKeys := calculateMaxKeys(8192, node.IsLeaf) // Assume standard page size

	return totalKeys <= uint32(maxKeys)
}

// FindKeyPosition finds the position where a key should be inserted
// Parameters:
//   - key: The key to find the position for
//
// Returns:
//   - int: The position where the key should be inserted
func (node *BTreeNode) FindKeyPosition(key []byte) int {
	// Binary search for the insertion position
	left, right := 0, int(node.KeyCount)

	for left < right {
		mid := (left + right) / 2
		if bytes.Compare(node.Keys[mid], key) < 0 {
			left = mid + 1
		} else {
			right = mid
		}
	}

	return left
}

// Page operation methods

// AddEntry adds an index entry to the page
// Parameters:
//   - entry: The entry to add
//
// Returns:
//   - error: Any error that occurred
func (page *BTreePage) AddEntry(entry *IndexEntry) error {
	entrySize := calculateEntrySize(entry)

	if page.FreeSpace < entrySize {
		return fmt.Errorf("insufficient space in page %d: need %d bytes, have %d",
			page.PageNum, entrySize, page.FreeSpace)
	}

	// Serialize the entry and append to page data
	entryData, err := serializeIndexEntry(entry)
	if err != nil {
		return fmt.Errorf("failed to serialize entry: %w", err)
	}

	page.Data = append(page.Data, entryData...)
	page.ItemCount++
	page.FreeSpace -= entrySize
	page.LastModified = time.Now()

	return nil
}

// RemoveEntry removes an entry with the specified key from the page
// Parameters:
//   - key: The key to remove
//
// Returns:
//   - bool: True if an entry was removed
func (page *BTreePage) RemoveEntry(key []byte) bool {
	// This is a simplified implementation
	// In a real implementation, you would parse the page data to find and remove the entry
	// For now, we'll mark this as a placeholder
	page.LastModified = time.Now()
	return false // Placeholder
}

// SerializeToBytes converts the page to its binary representation
// Returns:
//   - []byte: The serialized page data
//   - error: Any error that occurred during serialization
func (page *BTreePage) SerializeToBytes() ([]byte, error) {
	buf := new(bytes.Buffer)

	// Write page header
	if err := binary.Write(buf, binary.LittleEndian, page.PageNum); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.LittleEndian, page.PageType); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.LittleEndian, page.PageSize); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.LittleEndian, page.FreeSpace); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.LittleEndian, page.ItemCount); err != nil {
		return nil, err
	}

	// Write page data
	buf.Write(page.Data)

	return buf.Bytes(), nil
}

// DeserializeFromBytes populates the page from its binary representation
// Parameters:
//   - data: The binary data to deserialize
//
// Returns:
//   - error: Any error that occurred during deserialization
func (page *BTreePage) DeserializeFromBytes(data []byte) error {
	buf := bytes.NewReader(data)

	// Read page header
	if err := binary.Read(buf, binary.LittleEndian, &page.PageNum); err != nil {
		return err
	}
	if err := binary.Read(buf, binary.LittleEndian, &page.PageType); err != nil {
		return err
	}
	if err := binary.Read(buf, binary.LittleEndian, &page.PageSize); err != nil {
		return err
	}
	if err := binary.Read(buf, binary.LittleEndian, &page.FreeSpace); err != nil {
		return err
	}
	if err := binary.Read(buf, binary.LittleEndian, &page.ItemCount); err != nil {
		return err
	}

	// Read remaining data
	remaining := make([]byte, buf.Len())
	if _, err := buf.Read(remaining); err != nil {
		return err
	}
	page.Data = remaining

	return nil
}

// Metadata operation methods

// IncrementRecordCount safely increments the record count
func (meta *BTreeMetadata) IncrementRecordCount() {
	meta.TotalRecords++
	meta.InsertCount++
	meta.LastModified = time.Now()
}

// DecrementRecordCount safely decrements the record count
func (meta *BTreeMetadata) DecrementRecordCount() {
	if meta.TotalRecords > 0 {
		meta.TotalRecords--
	}
	meta.DeleteCount++
	meta.LastModified = time.Now()
}

// UpdateStatistics updates metadata statistics after an operation
// Parameters:
//   - operation: The type of operation performed ("search", "insert", "delete")
func (meta *BTreeMetadata) UpdateStatistics(operation string) {
	switch operation {
	case "search":
		meta.SearchCount++
	case "insert":
		meta.InsertCount++
	case "delete":
		meta.DeleteCount++
	}
	meta.LastModified = time.Now()
}

// Helper functions for calculations

// calculateTreeOrder computes the maximum number of keys per node based on page size
func calculateTreeOrder(pageSize, maxKeyLength uint32) uint32 {
	headerSize := calculateNodeHeaderSize()
	availableSpace := pageSize - headerSize

	// Estimate space per key (key + pointer + overhead)
	spacePerKey := maxKeyLength + 8 + 16 // key + pointer + overhead

	order := availableSpace / spacePerKey
	if order < 3 {
		order = 3 // Minimum order for a valid B-tree
	}

	return order
}

// calculateMaxKeys computes the maximum number of keys that can fit in a node
func calculateMaxKeys(pageSize uint32, isLeaf bool) int {
	headerSize := calculateNodeHeaderSize()
	availableSpace := pageSize - headerSize

	// Rough estimate: each key takes about 50 bytes on average
	estimatedKeySize := uint32(50)
	if !isLeaf {
		estimatedKeySize += 4 // Additional space for child pointer
	}

	maxKeys := int(availableSpace / estimatedKeySize)
	if maxKeys < 1 {
		maxKeys = 1
	}

	return maxKeys
}

// calculateNodeHeaderSize returns the size of the node header
// func calculateNodeHeaderSize() uint32 {
// 	return 64 // Estimated header size in bytes
// }

// calculatePageHeaderSize returns the size of the page header
func calculatePageHeaderSize() uint32 {
	return 32 // Estimated page header size in bytes
}

// calculateMinEntrySize returns the minimum size needed for an entry
func calculateMinEntrySize() uint32 {
	return 32 // Minimum entry size in bytes
}

// calculateEntrySize calculates the size needed for an index entry
func calculateEntrySize(entry *IndexEntry) uint32 {
	size := uint32(16) // Base overhead
	size += entry.KeyLength

	// Add space for document IDs
	for _, docID := range entry.DocumentIDs {
		size += uint32(len(docID)) + 4 // String length + overhead
	}

	return size
}

// serializeIndexEntry converts an index entry to bytes
func serializeIndexEntry(entry *IndexEntry) ([]byte, error) {
	buf := new(bytes.Buffer)

	// Write key length and key
	if err := binary.Write(buf, binary.LittleEndian, entry.KeyLength); err != nil {
		return nil, err
	}
	buf.Write(entry.Key)

	// Write document ID count
	if err := binary.Write(buf, binary.LittleEndian, entry.ValueCount); err != nil {
		return nil, err
	}

	// Write document IDs
	for _, docID := range entry.DocumentIDs {
		docIDBytes := []byte(docID)
		if err := binary.Write(buf, binary.LittleEndian, uint32(len(docIDBytes))); err != nil {
			return nil, err
		}
		buf.Write(docIDBytes)
	}

	return buf.Bytes(), nil
}

// ========================================================================================
// BTREE NODE HELPER METHODS - Lazy Deletion & Tombstone Management
// Following PostgreSQL's deferred merge approach for optimal delete performance
// ========================================================================================

// makeTombstoneKey creates a unique key for tombstone tracking
// DRY Principle: Centralized key generation for tombstone map
func makeTombstoneKey(key []byte, documentID string) string {
	return string(key) + ":" + documentID
}

// MarkDeleted marks a document ID as deleted using lazy deletion (PostgreSQL-style)
// This is faster than physical deletion and enables deferred merging during VACUUM.
//
// Single Responsibility: Only marks deletion, doesn't restructure tree
//
// TODO: I could add timestamp tracking for tombstones to enable time-based
// compaction strategies (e.g., compact only tombstones older than 1 hour)
func (node *BTreeNode) MarkDeleted(key []byte, documentID string) error {
	if !node.IsLeaf {
		return fmt.Errorf("can only mark deletions in leaf nodes")
	}

	// Initialize tombstones map if needed
	if node.Tombstones == nil {
		node.Tombstones = make(map[string]bool)
	}

	// Create tombstone key and mark as deleted
	tombstoneKey := makeTombstoneKey(key, documentID)
	if !node.Tombstones[tombstoneKey] {
		node.Tombstones[tombstoneKey] = true
		node.TombstoneCount++
		node.LastModified = time.Now()

		// Check if node needs compaction (>50% tombstones - PostgreSQL threshold)
		totalEntries := node.KeyCount + node.TombstoneCount
		if totalEntries > 0 && float64(node.TombstoneCount)/float64(totalEntries) > 0.5 {
			node.NeedsCompaction = true
		}
	}

	return nil
}

// IsDeleted checks if a specific document ID is marked as deleted
// DRY Principle: Centralized deletion check logic
func (node *BTreeNode) IsDeleted(key []byte, documentID string) bool {
	if node.Tombstones == nil {
		return false
	}

	tombstoneKey := makeTombstoneKey(key, documentID)
	return node.Tombstones[tombstoneKey]
}

// GetFragmentationRatio calculates the ratio of tombstones to total entries
// This metric determines when compaction is beneficial
//
// Returns: ratio between 0.0 and 1.0 where higher means more fragmentation
//
// TODO: I could enhance this to consider free space fragmentation in addition
// to tombstone count for a more comprehensive fragmentation metric
func (node *BTreeNode) GetFragmentationRatio() float64 {
	if node.TombstoneCount == 0 {
		return 0.0
	}

	totalEntries := node.KeyCount + node.TombstoneCount
	if totalEntries == 0 {
		return 0.0
	}

	return float64(node.TombstoneCount) / float64(totalEntries)
}

// GetLiveDocumentIDs returns document IDs for a key, excluding tombstones
// Single Responsibility: Filters out deleted entries during reads
//
// This is the READ path filter that hides deleted entries from queries
// without physically removing them from the index
func (node *BTreeNode) GetLiveDocumentIDs(keyIndex int) []string {
	if keyIndex < 0 || keyIndex >= len(node.Values) {
		return []string{}
	}

	if node.Tombstones == nil || node.TombstoneCount == 0 {
		// Fast path: no tombstones, return all IDs
		return node.Values[keyIndex]
	}

	// Filter out tombstones
	key := node.Keys[keyIndex]
	liveIDs := make([]string, 0, len(node.Values[keyIndex]))

	for _, docID := range node.Values[keyIndex] {
		if !node.IsDeleted(key, docID) {
			liveIDs = append(liveIDs, docID)
		}
	}

	return liveIDs
}

// ShouldCompact determines if this node should be compacted
// Open/Closed Principle: Extensible compaction criteria
//
// Returns true if:
// - Tombstone ratio > 50% (PostgreSQL threshold)
// - Explicitly flagged for compaction
//
// TODO: I could add additional criteria:
// - Time since last compaction
// - Absolute tombstone count threshold
// - Free space fragmentation level
func (node *BTreeNode) ShouldCompact() bool {
	return node.NeedsCompaction || node.GetFragmentationRatio() > 0.5
}
