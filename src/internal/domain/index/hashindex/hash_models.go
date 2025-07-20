package hashindex

import (
	"os"
	"sync"
	"syndrdb/src/internal/storage/hash"

	"go.uber.org/zap"
)

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

// HashService manages hash index operations at the service level
type HashService struct {
	DataDir       string
	MaxMemorySize int64
	Logger        *zap.SugaredLogger
}

// HashIndex manages the hash index operations
type HashIndex struct {
	sync.RWMutex
	FilePath     string
	File         *os.File
	Metadata     hash.HashIndexMetadata
	PageCache    map[uint32]*hash.HashIndexPage
	CacheSize    int
	MaxCacheSize int
	Logger       *zap.SugaredLogger
	Dirty        bool // Whether metadata has been modified
}
