package documentscanner

import (
	"time"

	"syndrdb/src/internal/domain/models"
)

// CacheInterface defines the contract for caching implementations
// This allows the scanner to work with different cache backends (LRU, TTL, etc.)
type CacheInterface interface {
	// Get retrieves a value from the cache
	Get(key interface{}) (interface{}, bool)

	// Put stores a value in the cache
	Put(key interface{}, value interface{})

	// Contains checks if a key exists in the cache without retrieving it
	Contains(key interface{}) bool

	// Remove removes a key from the cache
	Remove(key interface{})

	// Clear removes all entries from the cache
	Clear()

	// Size returns the current number of cached items
	Size() int
}

// BundleInterface defines the operations needed from a Bundle for document scanning
// This abstracts the Bundle dependency to make the scanner more testable
type BundleInterface interface {
	// GetDocumentIDs returns all document IDs in the bundle
	GetDocumentIDs() []string

	// GetDocument retrieves a document by its ID
	GetDocument(docID string) *models.Document

	// GetAllDocuments returns all documents in the bundle as a map
	GetAllDocuments() map[string]*models.Document

	// GetAllDocumentsWithLimit returns documents up to the specified limit
	// Used for early termination optimization in simple LIMIT-only queries
	// If limit is 0 or negative, behaves the same as GetAllDocuments()
	GetAllDocumentsWithLimit(limit int) map[string]*models.Document

	// GetName returns the bundle name for logging and metrics
	GetName() string

	// GetTotalDocuments returns the total number of documents in the bundle
	GetTotalDocuments() int

	// GetHashIndexForField retrieves the hash index for a specific field
	// Returns nil if no index exists for the field
	// This is used by join executors for index-assisted operations
	// TODO: Extend to support other index types (BTree, etc.) when implemented
	GetHashIndexForField(fieldName string) interface{}

	// HasIndexOnField checks if an index exists for the specified field
	// This is a quick check without loading the index
	// Returns true if any index type exists on this field
	HasIndexOnField(fieldName string) bool
}

// ScanMetrics holds performance and usage metrics for the scanner
// These metrics inform the query planner about scan efficiency and hot keys
type ScanMetrics struct {
	// Query performance metrics
	TotalScans     int64         `json:"total_scans"`
	AverageLatency time.Duration `json:"average_latency"`
	CacheHitRate   float64       `json:"cache_hit_rate"`
	CacheHits      int64         `json:"cache_hits"`

	// Hot key metrics
	HotKeysIdentified int      `json:"hot_keys_identified"`
	TopHotKeys        []string `json:"top_hot_keys"`

	// Memory and batch metrics
	AverageBatchSize  int   `json:"average_batch_size"`
	MemoryPressureGCs int64 `json:"memory_pressure_gcs"`

	// Error metrics
	ScanErrors int64  `json:"scan_errors"`
	LastError  string `json:"last_error,omitempty"`
}

// ScannerConfig holds configuration parameters for the document scanner
// These values control performance characteristics and memory usage
type ScannerConfig struct {
	// Batch processing configuration
	BatchSize  int `json:"batch_size"`  // Documents to load per batch
	MaxBatches int `json:"max_batches"` // Maximum concurrent batches

	// Hot key detection configuration
	HotThreshold  int `json:"hot_threshold"`  // Queries needed to mark key as "hot"
	OptimizeAfter int `json:"optimize_after"` // When to suggest optimization

	// Memory management configuration
	MemoryThreshold int `json:"memory_threshold"` // Results count to trigger GC
	CacheSize       int `json:"cache_size"`       // Maximum cache entries

	// Concurrency configuration
	MaxGoroutines int `json:"max_goroutines"` // Maximum concurrent operations
	ChannelBuffer int `json:"channel_buffer"` // Channel buffer size for batches

	// Metrics configuration
	MetricsEnabled  bool          `json:"metrics_enabled"`  // Whether to collect metrics
	MetricsInterval time.Duration `json:"metrics_interval"` // How often to update metrics
}

// ScanQuery represents a query for scanning documents by key-value pairs
// This encapsulates the search criteria and comparison logic
type ScanQuery struct {
	KeyName       string      `json:"key_name"`       // The key to search for
	Value         interface{} `json:"value"`          // The value to match
	Operator      string      `json:"operator"`       // Comparison operator ("=", ">", "<", etc.)
	CaseSensitive bool        `json:"case_sensitive"` // For string comparisons
}

// ScanResult represents the result of a document scan operation
// This includes both the matching documents and performance metadata
type ScanResult struct {
	Documents    []*models.Document `json:"documents"`     // Matching documents
	DocumentIDs  []string           `json:"document_ids"`  // IDs of matching documents
	ScanLatency  time.Duration      `json:"scan_latency"`  // Time taken for the scan
	CacheHits    int                `json:"cache_hits"`    // Number of cache hits during scan
	BatchesUsed  int                `json:"batches_used"`  // Number of batches processed
	TotalScanned int                `json:"total_scanned"` // Total documents examined
}

// DocumentScannerInterface defines the main contract for document scanning operations
// This is the primary interface that query planners and other components will use
type DocumentScannerInterface interface {
	// ScanForKeyValue performs a scan looking for documents with specific key-value pairs
	ScanForKeyValue(query *ScanQuery) (*ScanResult, error)

	// ScanWithPredicate performs a scan using a custom predicate function
	ScanWithPredicate(predicate func(*models.Document) bool) (*ScanResult, error)

	// ScanAllDocuments performs a full scan using GetAllDocuments() for optimal performance
	// This bypasses the O(n*m) GetDocumentIDs() + GetDocument() loop
	ScanAllDocuments() (*ScanResult, error)

	// GetMetrics returns current scanner performance metrics
	GetMetrics() *ScanMetrics

	// GetHotKeys returns currently identified hot keys
	GetHotKeys() []string

	// IsHotKey checks if a specific key is considered "hot"
	IsHotKey(keyName string) bool

	// GetConfig returns the current scanner configuration
	GetConfig() *ScannerConfig

	// Close performs cleanup and releases resources
	Close() error
}

// DefaultScannerConfig returns a configuration with sensible defaults
// These values are based on PostgreSQL's default settings and Go's memory characteristics
func DefaultScannerConfig() *ScannerConfig {
	return &ScannerConfig{
		// Batch size of 1000 provides good balance between memory usage and I/O efficiency
		// This aligns with PostgreSQL's default work_mem usage patterns
		BatchSize:  1000,
		MaxBatches: 10, // Limit concurrent batches to control memory

		// Hot key thresholds based on typical query patterns
		HotThreshold:  10, // 10 queries to consider a key "hot"
		OptimizeAfter: 50, // Suggest optimization after 50 queries

		// Memory management - trigger GC when result set gets large
		MemoryThreshold: 10000, // GC after 10k results to prevent memory spikes
		CacheSize:       5000,  // Cache up to 5k items

		// Concurrency settings - conservative to avoid goroutine explosion
		MaxGoroutines: 4,  // Match typical CPU cores
		ChannelBuffer: 10, // Buffer 10 batches to smooth processing

		// Metrics enabled by default for query planner feedback
		MetricsEnabled:  true,
		MetricsInterval: time.Second * 30, // Update metrics every 30 seconds
	}
}
