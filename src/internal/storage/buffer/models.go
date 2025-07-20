package buffer

import (
	"sync"
	"time"

	"go.uber.org/zap"
)

// DBPageBuffer represents a single buffer in the buffer pool
type DBPageBuffer struct {
	// Buffer state and lock management
	Mu         sync.RWMutex
	State      int
	RefCount   int
	UsageCount int

	// Buffer identification
	Tag BufferTag
	ID  int

	// Buffer Data
	Data []byte

	// For dirty buffer management
	IsDirty      bool
	LastModified time.Time

	// For clock sweep algorithm
	Referenced bool
}

// BufferTag uniquely identifies a disk page
type BufferTag struct {
	FileID      uint32 // Equivalent to PostgreSQL's RelFileNode
	BlockNumber uint32 // Page number within the file
}

// BufferDescriptor holds metadata about a buffer
type BufferDescriptor struct {
	ID         int       // Buffer ID
	Tag        BufferTag // Identifies which disk page the buffer contains
	State      int       // Buffer state (invalid, valid, dirty)
	RefCount   int       // Number of current users of the buffer
	UsageCount int       // For clock-sweep eviction
	Pinned     bool      // If true, don't evict this buffer
}

// BufferPool manages a collection of buffers
type BufferPool struct {
	MU          sync.Mutex
	Buffers     []*DBPageBuffer
	Descriptors []BufferDescriptor
	HashTable   map[BufferTag]int // Maps BufferTag to buffer index

	// For clock sweep algorithm
	ClockHand int

	// Configuration
	PageSize   int
	MaxBuffers int

	// Stats
	Hits         uint64
	Misses       uint64
	Evictions    uint64
	WriteCount   uint64 // Track total writes
	SyncInterval int    // How often to sync (every N writes)

	// File management
	FileRegistry *FileRegistry

	Logger *zap.SugaredLogger
}

// PageHeader represents the header structure of a database page
type PageHeader struct {
	// LSN of last change (8 bytes in PostgreSQL)
	LSN uint64

	// Checksum (2 bytes)
	Checksum uint16

	// Flags (2 bytes)
	Flags uint16

	// Lower - offset to start of free space (2 bytes)
	Lower uint16

	// Upper - offset to end of free space (2 bytes)
	Upper uint16

	// Special space offset (2 bytes)
	Special uint16

	// PageSize and version info (4 bytes)
	PageSizeVersion uint32

	// XID for pruning (4 bytes)
	PruneXID uint32
}

// REgion: B-Tree Index models

// BTreePage represents a page in the B-tree
type BTreePage struct {
	PageType   int
	PageNum    uint32
	ParentPage uint32
	PrevPage   uint32
	NextPage   uint32
	Level      uint16 // Distance from leaf (0 = leaf)
	NumEntries uint16
	FreeSpace  uint16
	Entries    []BTreeEntry
	IsDirty    bool // Indicates if the page has been modified since last write
}

// BTreeEntry represents a single entry in a B-tree page
type BTreeEntry struct {
	Key   []byte
	Value []byte // For leaf nodes: tuple reference; for internal nodes: child page pointer
	DocID string // Only for leaf entries
	TID   uint64 // Only for leaf entries
}

// BTreeIndex represents a complete B-tree index
type BTreeIndex struct {
	Name       string
	FileName   string
	MetaPage   BTreePage
	RootPage   uint32
	Height     uint16
	TotalPages uint32
	PageSize   uint32
	IndexField IndexField
}

// We need minimal bundle interfaces to avoid circular dependencies
type BundleInfo interface {
	GetBundleID() string
	GetDocumentStructure() DocumentStructureInfo
	GetDocuments() map[string]DocumentInfo
}

type DocumentStructureInfo interface {
	GetFieldDefinition(name string) (FieldDefinitionInfo, bool)
}

type FieldDefinitionInfo interface {
	IsRequired() bool
	IsUnique() bool
	GetType() string
}

type DocumentInfo interface {
	GetField(name string) (interface{}, bool)
	GetID() string
}

// IndexField defines what field from documents will be indexed
type IndexField struct {
	FieldName string
	IsUnique  bool
	Collation string // Optional collation for string comparison
}

// IndexTuple represents a single entry in the index
type IndexTuple struct {
	Key       []byte // Encoded key value (from the indexed field)
	DocID     string // Document ID this entry points to
	BundleID  string // Bundle ID this document belongs to
	TID       uint64 // Tuple identifier (similar to PostgreSQL's TID)
	KeyString string // Human-readable representation of the key (for debugging)
}
