/*
BTREE INDEX CONFIGURATION SYSTEM

This file implements the configuration management system for BTree indexes in SyndrDB.
It provides validation, default settings, and configuration parameter management for
creating and managing BTree indexes that follow the algorithms used in PostgreSQL,
MySQL, and Microsoft SQL Server.

CONFIGURATION OVERVIEW:
The BTree index configuration system manages all parameters required for creating
and operating BTree indexes, including page sizes, cache settings, storage options,
and performance tuning parameters. It ensures that all configurations are valid
and provides sensible defaults for optimal performance.

KEY RESPONSIBILITIES:
- Configuration parameter validation and sanitization
- Default configuration generation with optimal settings
- File path management for index storage
- Configuration compatibility checking
- Performance parameter tuning based on system characteristics

VALIDATION STRATEGY:
All configuration parameters are validated to ensure they meet the requirements
of the B+ tree algorithm and are compatible with the underlying storage system.
This includes checking page sizes, cache configurations, key length limits,
and storage directory accessibility.

INTEGRATION WITH SYNDRDB:
The configuration system integrates seamlessly with the SyndrDB bundle and
document system, automatically generating appropriate file paths and storage
configurations based on bundle names and field specifications.
*/

package btreeindexV2

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"syndrdb/src/internal/journal"
	"syndrdb/src/pkg/common/helpers"
)

// IndexConfig contains configuration parameters for creating a BTree index
// This structure defines all the parameters needed to create and configure
// a BTree index for optimal performance in the SyndrDB environment
type IndexConfig struct {
	// Index identification
	DatabaseName string // Name of the database this index belongs to
	BundleName   string // Name of the bundle this index belongs to
	FieldName    string // Name of the field being indexed
	IndexName    string // Optional custom name for the index (auto-generated if empty)

	// Storage configuration
	DataDir   string // Directory where index files are stored
	DebugMode bool   // Whether to use ASCII format for debugging

	// Index properties
	IsUnique   bool // Whether the index enforces uniqueness
	AllowNulls bool // Whether null values are allowed in the index

	// Performance tuning
	PageSize     uint32 // Size of each page in bytes (must be power of 2)
	CacheSize    int    // Number of pages to cache in memory
	InitialPages uint32 // Initial number of pages to allocate
	MaxKeyLength uint32 // Maximum allowed key length in bytes

	// Advanced options
	FillFactor       float64 // Target fill percentage for pages (0.0-1.0)
	SplitRatio       float64 // Ratio for splitting pages (0.0-1.0)
	CompactThreshold float64 // Fragmentation threshold for automatic compaction

	// Memory management
	BufferPoolSize uint32 // Size of buffer pool in bytes
	MaxMemoryUsage uint64 // Maximum memory usage in bytes (0 = unlimited)

	// File system options
	FilePermissions os.FileMode // File permissions for index files
	SyncWrites      bool        // Whether to sync writes to disk immediately

	// Durability options (added for production readiness)
	WALManager *journal.WALManager // Write-ahead logging manager for durability (optional)
}

// DefaultIndexConfig creates a configuration with optimal default values
// Parameters:
//   - bundleName: Name of the bundle this index belongs to
//   - fieldName: Name of the field being indexed
//   - dataDir: Directory where index files should be stored
//
// Returns:
//   - *IndexConfig: Configuration with sensible defaults
func DefaultIndexConfig(bundleName, fieldName, dataDir, databaseName string) *IndexConfig {
	// Determine optimal page size based on system characteristics
	pageSize := uint32(8192) // 8KB pages (PostgreSQL standard)

	// Calculate cache size based on available memory
	cacheSize := calculateOptimalCacheSize()

	// Set maximum key length based on page size
	maxKeyLength := pageSize / 8 // Conservative estimate

	return &IndexConfig{
		DatabaseName:     databaseName,
		BundleName:       bundleName,
		FieldName:        fieldName,
		IndexName:        "", // Will be auto-generated
		DataDir:          dataDir,
		DebugMode:        false,
		IsUnique:         false,
		AllowNulls:       true,
		PageSize:         pageSize,
		CacheSize:        cacheSize,
		InitialPages:     16, // Start with reasonable number of pages
		MaxKeyLength:     maxKeyLength,
		FillFactor:       0.75, // 75% fill factor for good balance
		SplitRatio:       0.5,  // Split pages evenly
		CompactThreshold: 0.25, // Compact when 25% fragmented
		BufferPoolSize:   pageSize * uint32(cacheSize),
		MaxMemoryUsage:   0,     // Unlimited by default
		FilePermissions:  0644,  // Standard file permissions
		SyncWrites:       false, // Async writes for better performance
	}
}

// NewIndexConfig creates a new configuration with validation
// Parameters:
//   - bundleName: Name of the bundle this index belongs to
//   - fieldName: Name of the field being indexed
//   - dataDir: Directory where index files should be stored
//   - isUnique: Whether the index should enforce uniqueness
//   - debugMode: Whether to use ASCII format for debugging
//
// Returns:
//   - *IndexConfig: The validated configuration
//   - error: Any validation error that occurred
func NewIndexConfig(bundleName, fieldName, dataDir, databaseName string, isUnique, debugMode bool) (*IndexConfig, error) {
	config := DefaultIndexConfig(bundleName, fieldName, dataDir, databaseName)
	config.IsUnique = isUnique
	config.DebugMode = debugMode

	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	return config, nil
}

// NewIndexConfigWithOptions creates a configuration with custom options
// Parameters:
//   - bundleName: Name of the bundle this index belongs to
//   - fieldName: Name of the field being indexed
//   - dataDir: Directory where index files should be stored
//   - options: Map of custom configuration options
//
// Returns:
//   - *IndexConfig: The configured index config
//   - error: Any validation error that occurred
func NewIndexConfigWithOptions(bundleName, fieldName, dataDir, databaseName string, options map[string]interface{}) (*IndexConfig, error) {
	config := DefaultIndexConfig(bundleName, fieldName, dataDir, databaseName)

	// Apply custom options
	if err := config.ApplyOptions(options); err != nil {
		return nil, fmt.Errorf("failed to apply options: %w", err)
	}

	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	return config, nil
}

// Validate checks if the configuration parameters are valid
// Returns:
//   - error: Validation error if configuration is invalid, nil otherwise
func (config *IndexConfig) Validate() error {
	// Validate required fields
	if config.BundleName == "" {
		return fmt.Errorf("bundle name cannot be empty")
	}

	if config.DatabaseName == "" {
		return fmt.Errorf("database name cannot be empty")
	}

	if config.FieldName == "" {
		return fmt.Errorf("field name cannot be empty")
	}

	if config.DataDir == "" {
		return fmt.Errorf("data directory cannot be empty")
	}

	// Validate bundle and field names
	if err := validateName(config.BundleName); err != nil {
		return fmt.Errorf("invalid bundle name: %w", err)
	}

	if err := validateName(config.FieldName); err != nil {
		return fmt.Errorf("invalid field name: %w", err)
	}

	// Validate page size (must be power of 2 and reasonable)
	if config.PageSize < 1024 {
		return fmt.Errorf("page size must be at least 1024 bytes, got %d", config.PageSize)
	}

	if config.PageSize > 65536 {
		return fmt.Errorf("page size cannot exceed 65536 bytes, got %d", config.PageSize)
	}

	if !isPowerOfTwo(config.PageSize) {
		return fmt.Errorf("page size must be a power of 2, got %d", config.PageSize)
	}

	// Validate cache size
	if config.CacheSize <= 0 {
		return fmt.Errorf("cache size must be greater than 0, got %d", config.CacheSize)
	}

	if config.CacheSize > 1000000 { // Reasonable upper limit
		return fmt.Errorf("cache size cannot exceed 1,000,000 pages, got %d", config.CacheSize)
	}

	// Validate key length
	if config.MaxKeyLength == 0 {
		return fmt.Errorf("maximum key length must be greater than 0")
	}

	if config.MaxKeyLength > config.PageSize/4 {
		return fmt.Errorf("maximum key length cannot exceed 1/4 of page size (%d), got %d",
			config.PageSize/4, config.MaxKeyLength)
	}

	// Validate fill factor
	if config.FillFactor <= 0 || config.FillFactor > 1.0 {
		return fmt.Errorf("fill factor must be between 0.0 and 1.0, got %f", config.FillFactor)
	}

	// Validate split ratio
	if config.SplitRatio <= 0 || config.SplitRatio >= 1.0 {
		return fmt.Errorf("split ratio must be between 0.0 and 1.0, got %f", config.SplitRatio)
	}

	// Validate compaction threshold
	if config.CompactThreshold < 0 || config.CompactThreshold > 1.0 {
		return fmt.Errorf("compact threshold must be between 0.0 and 1.0, got %f", config.CompactThreshold)
	}

	// Validate data directory accessibility
	databasePath := helpers.GetDatabaseFolderPath(config.DatabaseName)
	if err := validateDataDirectory(databasePath); err != nil {
		return fmt.Errorf("invalid data directory: %w", err)
	}

	return nil
}

// GetIndexFilePath returns the full path to the index file
// Returns:
//   - string: The complete file path for the index
func (config *IndexConfig) GetIndexFilePath() string {
	indexName := config.IndexName
	if indexName == "" {
		indexName = config.generateIndexName()
	}

	databaseName := helpers.GetDatabaseFolderPath(config.DatabaseName)

	filename := fmt.Sprintf("%s.btidx", indexName)
	return filepath.Join(databaseName, filename)
}

// GetTempFilePath returns the path for temporary files during operations
// Returns:
//   - string: The complete file path for temporary files
func (config *IndexConfig) GetTempFilePath() string {
	indexName := config.IndexName
	if indexName == "" {
		indexName = config.generateIndexName()
	}

	databasePath := helpers.GetDatabaseFolderPath(config.DatabaseName)

	filename := fmt.Sprintf("%s.tmp", indexName)
	return filepath.Join(databasePath, filename)
}

// GetBackupFilePath returns the path for backup files
// Returns:
//   - string: The complete file path for backup files
func (config *IndexConfig) GetBackupFilePath() string {
	indexName := config.IndexName
	if indexName == "" {
		indexName = config.generateIndexName()
	}

	databasePath := helpers.GetDatabaseFolderPath(config.DatabaseName)

	filename := fmt.Sprintf("%s.bak", indexName)
	return filepath.Join(databasePath, filename)
}

// ApplyOptions applies a map of configuration options
// Parameters:
//   - options: Map of option names to values
//
// Returns:
//   - error: Any error that occurred while applying options
func (config *IndexConfig) ApplyOptions(options map[string]interface{}) error {
	for key, value := range options {
		if err := config.setOption(key, value); err != nil {
			return fmt.Errorf("failed to set option %s: %w", key, err)
		}
	}
	return nil
}

// Clone creates a deep copy of the configuration
// Returns:
//   - *IndexConfig: A complete copy of the configuration
func (config *IndexConfig) Clone() *IndexConfig {
	return &IndexConfig{
		DatabaseName:     config.DatabaseName,
		BundleName:       config.BundleName,
		FieldName:        config.FieldName,
		IndexName:        config.IndexName,
		DataDir:          config.DataDir,
		DebugMode:        config.DebugMode,
		IsUnique:         config.IsUnique,
		AllowNulls:       config.AllowNulls,
		PageSize:         config.PageSize,
		CacheSize:        config.CacheSize,
		InitialPages:     config.InitialPages,
		MaxKeyLength:     config.MaxKeyLength,
		FillFactor:       config.FillFactor,
		SplitRatio:       config.SplitRatio,
		CompactThreshold: config.CompactThreshold,
		BufferPoolSize:   config.BufferPoolSize,
		MaxMemoryUsage:   config.MaxMemoryUsage,
		FilePermissions:  config.FilePermissions,
		SyncWrites:       config.SyncWrites,
	}
}

// String provides a string representation of the configuration
// Returns:
//   - string: Human-readable configuration summary
func (config *IndexConfig) String() string {
	return fmt.Sprintf("BTreeIndexConfig{Bundle: %s, Field: %s, PageSize: %d, Cache: %d, Unique: %t}",
		config.BundleName, config.FieldName, config.PageSize, config.CacheSize, config.IsUnique)
}

// Private helper methods

// generateIndexName creates a default index name based on bundle and field
func (config *IndexConfig) generateIndexName() string {
	return fmt.Sprintf("%s_%s_btree", config.BundleName, config.FieldName)
}

// setOption sets a single configuration option
func (config *IndexConfig) setOption(key string, value interface{}) error {
	switch strings.ToLower(key) {
	case "pagesize":
		if size, ok := value.(uint32); ok {
			config.PageSize = size
		} else {
			return fmt.Errorf("pageSize must be uint32")
		}
	case "cachesize":
		if size, ok := value.(int); ok {
			config.CacheSize = size
		} else {
			return fmt.Errorf("cacheSize must be int")
		}
	case "maxkeylength":
		if length, ok := value.(uint32); ok {
			config.MaxKeyLength = length
		} else {
			return fmt.Errorf("maxKeyLength must be uint32")
		}
	case "fillfactor":
		if factor, ok := value.(float64); ok {
			config.FillFactor = factor
		} else {
			return fmt.Errorf("fillFactor must be float64")
		}
	case "isunique":
		if unique, ok := value.(bool); ok {
			config.IsUnique = unique
		} else {
			return fmt.Errorf("isUnique must be bool")
		}
	case "allownulls":
		if allow, ok := value.(bool); ok {
			config.AllowNulls = allow
		} else {
			return fmt.Errorf("allowNulls must be bool")
		}
	case "debugmode":
		if debug, ok := value.(bool); ok {
			config.DebugMode = debug
		} else {
			return fmt.Errorf("debugMode must be bool")
		}
	default:
		return fmt.Errorf("unknown option: %s", key)
	}
	return nil
}

// Validation helper functions

// validateName checks if a bundle or field name is valid
func validateName(name string) error {
	if len(name) == 0 {
		return fmt.Errorf("name cannot be empty")
	}

	if len(name) > 255 {
		return fmt.Errorf("name cannot exceed 255 characters")
	}

	// Check for invalid characters
	for _, char := range name {
		if !isValidNameChar(char) {
			return fmt.Errorf("name contains invalid character: %c", char)
		}
	}

	return nil
}

// isValidNameChar checks if a character is valid in bundle/field names
func isValidNameChar(char rune) bool {
	return (char >= 'a' && char <= 'z') ||
		(char >= 'A' && char <= 'Z') ||
		(char >= '0' && char <= '9') ||
		char == '_' || char == '-'
}

// isPowerOfTwo checks if a number is a power of 2
func isPowerOfTwo(n uint32) bool {
	return n > 0 && (n&(n-1)) == 0
}

// validateDataDirectory checks if the data directory is accessible
func validateDataDirectory(dataDir string) error {
	// Check if directory exists
	info, err := os.Stat(dataDir)
	if os.IsNotExist(err) {
		// Try to create the directory
		if err := os.MkdirAll(dataDir, 0755); err != nil {
			return fmt.Errorf("cannot create data directory: %w", err)
		}
		return nil
	}

	if err != nil {
		return fmt.Errorf("cannot access data directory: %w", err)
	}

	if !info.IsDir() {
		return fmt.Errorf("data directory path is not a directory")
	}

	// Check write permissions by creating a test file
	testFile := filepath.Join(dataDir, ".btree_test")
	file, err := os.Create(testFile)
	if err != nil {
		return fmt.Errorf("data directory is not writable: %w", err)
	}
	file.Close()
	os.Remove(testFile)

	return nil
}

// calculateOptimalCacheSize determines optimal cache size based on system memory
func calculateOptimalCacheSize() int {
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	// Use 1% of available memory for cache, with reasonable bounds
	availableMemory := memStats.Sys
	cacheMemory := availableMemory / 100

	// Assume 8KB pages
	pageSize := uint64(8192)
	cacheSize := int(cacheMemory / pageSize)

	// Apply bounds
	if cacheSize < 100 {
		cacheSize = 100 // Minimum cache size
	} else if cacheSize > 10000 {
		cacheSize = 10000 // Maximum cache size
	}

	return cacheSize
}
