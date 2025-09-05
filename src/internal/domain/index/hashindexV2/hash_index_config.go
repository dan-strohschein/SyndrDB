package hashindexV2

import (
	"fmt"
	"path/filepath"
)

// IndexConfig contains configuration parameters for creating a hash index
type IndexConfig struct {
	BundleName  string  // Name of the bundle this index belongs to
	FieldName   string  // Name of the field being indexed (typically "DocumentID")
	IsUnique    bool    // Whether the index should enforce uniqueness
	DataDir     string  // Directory where index files are stored
	DebugMode   bool    // Whether to use human-readable ASCII format
	InitialSize uint32  // Initial number of buckets (must be power of 2)
	PageSize    uint32  // Size of each page in bytes (typically 8192)
	LoadFactor  float64 // Load factor threshold for bucket splitting (typically 0.75)
	CacheSize   int     // Number of pages to cache in memory
}

// Validate checks if the configuration is valid
func (c *IndexConfig) Validate() error {
	if c.BundleName == "" {
		return fmt.Errorf("bundle name cannot be empty")
	}

	if c.FieldName == "" {
		return fmt.Errorf("field name cannot be empty")
	}

	if c.DataDir == "" {
		return fmt.Errorf("data directory cannot be empty")
	}

	if c.InitialSize == 0 || (c.InitialSize&(c.InitialSize-1)) != 0 {
		return fmt.Errorf("initial size must be a power of 2 and greater than 0")
	}

	if c.PageSize < 1024 {
		return fmt.Errorf("page size must be at least 1024 bytes")
	}

	if c.LoadFactor <= 0 || c.LoadFactor >= 1.0 {
		return fmt.Errorf("load factor must be between 0 and 1.0")
	}

	if c.CacheSize <= 0 {
		return fmt.Errorf("cache size must be greater than 0")
	}

	return nil
}

// GetIndexFilePath returns the full path to the index file
func (c *IndexConfig) GetIndexFilePath() string {
	filename := fmt.Sprintf("%s_%s.hidx", c.BundleName, c.FieldName)
	return filepath.Join(c.DataDir, filename)
}

// DefaultIndexConfig returns a configuration with sensible defaults
func DefaultIndexConfig(bundleName, fieldName, dataDir string) *IndexConfig {
	return &IndexConfig{
		BundleName:  bundleName,
		FieldName:   fieldName,
		IsUnique:    true,
		DataDir:     dataDir,
		DebugMode:   false,
		InitialSize: 16,   // Start with 16 buckets
		PageSize:    8192, // 8KB pages (PostgreSQL-style)
		LoadFactor:  0.75, // Split when 75% full
		CacheSize:   2000, // Cache 2000 pages (handles large bulk operations)
	}
}

// NewIndexConfig creates a new configuration with validation
func NewIndexConfig(bundleName, fieldName, dataDir string, isUnique, debugMode bool) (*IndexConfig, error) {
	config := &IndexConfig{
		BundleName:  bundleName,
		FieldName:   fieldName,
		IsUnique:    isUnique,
		DataDir:     dataDir,
		DebugMode:   debugMode,
		InitialSize: 16,
		PageSize:    8192,
		LoadFactor:  0.75,
		CacheSize:   2000, // Cache 2000 pages (handles large bulk operations)
	}

	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	return config, nil
}
