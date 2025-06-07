package hashindex

import (
	"os"
	"sync"
	"time"

	"go.uber.org/zap"
)

// Constants for hash index
const (
	HashPageSize      = 8192 // 8KB pages like PostgreSQL
	MaxFillFactor     = 90   // Maximum fill factor percentage
	DefaultFillFactor = 75   // Default fill factor

	// Page types
	HashMetaPage     = 0
	HashBucketPage   = 1
	HashOverflowPage = 2

	// Initial size - start with 4 buckets like PostgreSQL
	InitialBucketCount = 4
)

// HashIndexMetadata stores global information about the hash index
type HashIndexMetadata struct {
	MaxBucket     uint32    // Maximum bucket number in use
	HighMask      uint32    // Mask to identify which bucket to use
	LowMask       uint32    // Mask for bucket splitting operations
	FillFactor    uint32    // Percentage of page to fill
	NumTuples     uint64    // Total number of entries
	BitmapPages   uint32    // Number of bitmap pages
	OverflowPages uint32    // Number of overflow pages
	IndexField    string    // Name of the indexed field
	IsUnique      bool      // Whether the index enforces uniqueness
	Created       time.Time // When the index was created
}

// HashIndexPage represents a page in the hash index file
type HashIndexPage struct {
	PageType    int
	PageNum     uint32
	NextPage    uint32 // For overflow chains
	ItemCount   uint16
	FreeSpace   uint16
	LastUpdated time.Time
	Items       []HashIndexItem
}

// HashIndexItem represents a single entry in the hash index
type HashIndexItem struct {
	HashValue uint32 // Hash of the key
	Key       []byte // Original key value
	Value     []byte // Value/payload data
	DocID     string // Document ID this entry points to
	TID       uint64 // Tuple identifier
}

// HashIndex manages the hash index operations
type HashIndex struct {
	sync.RWMutex
	filePath     string
	file         *os.File
	metadata     HashIndexMetadata
	pageCache    map[uint32]*HashIndexPage
	cacheSize    int
	maxCacheSize int
	logger       *zap.SugaredLogger
	dirty        bool // Whether metadata has been modified
}

// HashService manages hash index operations at the service level
type HashService struct {
	dataDir       string
	maxMemorySize int64
	logger        *zap.SugaredLogger
}

// IndexTuple represents a single entry in the index result set
type IndexTuple struct {
	Key       []byte // The encoded key value from the indexed field
	DocID     string // Document ID this entry points to
	TID       uint64 // Tuple identifier (similar to PostgreSQL's TID)
	BundleID  string // Optional: Bundle ID this document belongs to
	KeyString string // Optional: Human-readable representation of the key (for debugging)
}

// IndexField defines what field from documents will be indexed
type IndexField struct {
	FieldName string // The name of the document field to index
	IsUnique  bool   // Whether the index should enforce uniqueness
	Collation string // Optional collation for string comparison
}
