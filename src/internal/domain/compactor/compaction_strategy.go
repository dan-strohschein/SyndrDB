package compactor

/*
COMPACTION STRATEGY - TRIGGER CONDITIONS FOR LSM COMPACTION

This file defines various strategies for triggering compaction in LSM-style
storage systems. Different strategies optimize for different goals:
- File count strategies: Limit number of files for read performance
- Size strategies: Compact when files grow too large
- Tombstone strategies: Remove deleted entries when they accumulate
- Time strategies: Periodic compaction regardless of other factors

KEY RESPONSIBILITIES:
- Define when compaction should be triggered
- Provide configurable thresholds and limits
- Support multiple trigger conditions (AND/OR logic)
- Enable customization for different workloads

DESIGN PRINCIPLES:
- Single Responsibility: Each strategy focuses on one trigger type
- Composable: Strategies can be combined (AND/OR)
- Configurable: Thresholds can be adjusted at runtime
- Extensible: New strategies can be added easily

STRATEGY TYPES:
1. FileCountStrategy: Compact when file count exceeds threshold
2. TotalSizeStrategy: Compact when total size exceeds threshold
3. TombstoneRatioStrategy: Compact when tombstone % is too high
4. TimeBasedStrategy: Compact after fixed time interval
5. CompositeStrategy: Combine multiple strategies with AND/OR

USAGE EXAMPLES:
- Low latency reads: Use FileCountStrategy (limit to 5-10 files)
- Storage efficiency: Use TombstoneRatioStrategy (remove 30%+ tombstones)
- Predictable performance: Use TimeBasedStrategy (compact every hour)
- Balanced: Combine FileCount OR TombstoneRatio

TODO: Future extensions
- Workload-aware strategies (read-heavy vs write-heavy)
- Adaptive strategies (learn from access patterns)
- Cost-based strategies (estimate compaction benefit)
- Priority-based strategies (compact hot data first)
*/

import (
	"container/list"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// CompactionStrategy defines when compaction should be triggered
type CompactionStrategy interface {
	// ShouldCompact returns true if compaction should be triggered
	ShouldCompact(files []string) bool

	// Name returns the strategy name for logging
	Name() string
}

// FileCountStrategy triggers compaction when file count exceeds threshold
// This is the most common strategy for LSM systems
//
// Benefits:
// - Limits read amplification (fewer files to scan)
// - Predictable read latency
// - Simple to configure
//
// Use when:
// - Read performance is critical
// - File system has limits on open files
// - Consistent latency is required
type FileCountStrategy struct {
	MaxFiles int // Trigger compaction when file count >= MaxFiles
}

// NewFileCountStrategy creates a file count based strategy
func NewFileCountStrategy(maxFiles int) *FileCountStrategy {
	if maxFiles <= 0 {
		maxFiles = 10 // Default to 10 files
	}
	return &FileCountStrategy{MaxFiles: maxFiles}
}

// ShouldCompact returns true if file count exceeds threshold
func (fcs *FileCountStrategy) ShouldCompact(files []string) bool {
	return len(files) >= fcs.MaxFiles
}

// Name returns the strategy name
func (fcs *FileCountStrategy) Name() string {
	return "FileCountStrategy"
}

// TotalSizeStrategy triggers compaction when total size exceeds threshold
// Useful for controlling disk space usage
//
// Benefits:
// - Limits total storage footprint
// - Controls write amplification
// - Prevents unbounded growth
//
// Use when:
// - Disk space is constrained
// - Cost per GB is high
// - Storage limits are strict
type TotalSizeStrategy struct {
	MaxTotalSize int64 // Trigger compaction when total size >= MaxTotalSize (bytes)
}

// NewTotalSizeStrategy creates a size based strategy
func NewTotalSizeStrategy(maxTotalSize int64) *TotalSizeStrategy {
	if maxTotalSize <= 0 {
		maxTotalSize = 1024 * 1024 * 1024 // Default to 1GB
	}
	return &TotalSizeStrategy{MaxTotalSize: maxTotalSize}
}

// ShouldCompact returns true if total size exceeds threshold
func (tss *TotalSizeStrategy) ShouldCompact(files []string) bool {
	var totalSize int64
	for _, filePath := range files {
		info, err := os.Stat(filePath)
		if err == nil {
			totalSize += info.Size()
		}
	}
	return totalSize >= tss.MaxTotalSize
}

// Name returns the strategy name
func (tss *TotalSizeStrategy) Name() string {
	return "TotalSizeStrategy"
}

// ============================================================================
// Tombstone Ratio Strategy with LRU Cache
// ============================================================================

// tombstoneStats tracks tombstone count and total entries for a file
type tombstoneStats struct {
	filePath     string    // Full path to the file
	totalEntries uint64    // Total number of entries in the file
	tombstones   uint64    // Number of tombstone entries
	lastAccessed time.Time // Last time this cache entry was accessed
	fileModTime  time.Time // File modification time when stats were collected
}

// tombstoneLRUCache implements an LRU cache for tombstone statistics
// This prevents rescanning files repeatedly when checking compaction triggers
type tombstoneLRUCache struct {
	maxSize int                      // Maximum number of cached entries
	cache   map[string]*list.Element // Map of filePath -> list element
	lruList *list.List               // Doubly-linked list for LRU ordering
	mutex   sync.RWMutex             // Protects cache access

	// Eviction policy: Remove oldest entries when cache is full
	// Age-based eviction: Remove entries older than 5 minutes
	maxAge time.Duration // Maximum age before eviction (default 5 minutes)

	// Metrics reporting (optional callback to avoid import cycles)
	metricsReporter MetricsReporter
}

// newTombstoneLRUCache creates a new LRU cache for tombstone stats
func newTombstoneLRUCache(maxSize int, metricsReporter MetricsReporter) *tombstoneLRUCache {
	if maxSize <= 0 {
		maxSize = 1000 // Default: cache stats for 1000 files
	}
	return &tombstoneLRUCache{
		maxSize:         maxSize,
		cache:           make(map[string]*list.Element),
		lruList:         list.New(),
		maxAge:          5 * time.Minute, // Age out entries after 5 minutes
		metricsReporter: metricsReporter,
	}
}

// get retrieves tombstone stats from cache
// Returns stats and true if found, nil and false otherwise
func (c *tombstoneLRUCache) get(filePath string) (*tombstoneStats, bool) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	elem, found := c.cache[filePath]
	if !found {
		if c.metricsReporter != nil {
			c.metricsReporter("TombstoneCacheMisses", 1)
		}
		return nil, false
	}

	stats := elem.Value.(*tombstoneStats)

	// Check if entry is stale (file was modified since we cached it)
	fileInfo, err := os.Stat(filePath)
	if err != nil || !fileInfo.ModTime().Equal(stats.fileModTime) {
		// File was modified or deleted, invalidate cache entry
		c.lruList.Remove(elem)
		delete(c.cache, filePath)
		if c.metricsReporter != nil {
			c.metricsReporter("TombstoneCacheMisses", 1)
		}
		return nil, false
	}

	// Check if entry is too old (age-based eviction)
	if time.Since(stats.lastAccessed) > c.maxAge {
		c.lruList.Remove(elem)
		delete(c.cache, filePath)
		if c.metricsReporter != nil {
			c.metricsReporter("TombstoneCacheEvictions", 1)
			c.metricsReporter("TombstoneCacheMisses", 1)
		}
		return nil, false
	}

	// Move to front (most recently used)
	c.lruList.MoveToFront(elem)
	stats.lastAccessed = time.Now()

	if c.metricsReporter != nil {
		c.metricsReporter("TombstoneCacheHits", 1)
	}
	return stats, true
}

// put stores tombstone stats in cache
func (c *tombstoneLRUCache) put(stats *tombstoneStats) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	// Check if entry already exists
	if elem, found := c.cache[stats.filePath]; found {
		// Update existing entry and move to front
		c.lruList.MoveToFront(elem)
		elem.Value = stats
		return
	}

	// Evict oldest entry if cache is full
	if c.lruList.Len() >= c.maxSize {
		oldest := c.lruList.Back()
		if oldest != nil {
			oldStats := oldest.Value.(*tombstoneStats)
			delete(c.cache, oldStats.filePath)
			c.lruList.Remove(oldest)
			if c.metricsReporter != nil {
				c.metricsReporter("TombstoneCacheEvictions", 1)
			}
		}
	}

	// Add new entry to front
	elem := c.lruList.PushFront(stats)
	c.cache[stats.filePath] = elem
}

// Global LRU cache instance for tombstone statistics
// Shared across all TombstoneRatioStrategy instances
// Metrics reporter will be set via SetTombstoneCacheMetricsReporter
var globalTombstoneCache = newTombstoneLRUCache(1000, nil)

// SetTombstoneCacheMetricsReporter sets the metrics reporter for the global tombstone cache
// This must be called during server initialization to enable metric collection
func SetTombstoneCacheMetricsReporter(reporter MetricsReporter) {
	globalTombstoneCache.metricsReporter = reporter
}

// TombstoneRatioStrategy triggers compaction when tombstone ratio is high
// This strategy requires scanning files to count tombstones
//
// Benefits:
// - Reclaims space from deleted data
// - Improves read performance (fewer tombstones to skip)
// - Reduces storage costs
//
// Use when:
// - Delete workloads are heavy
// - Storage efficiency is important
// - Read performance degrades over time
//
// Note: Requires tombstone counting, which is expensive
// Consider caching tombstone counts or sampling
type TombstoneRatioStrategy struct {
	MaxTombstoneRatio float64 // Trigger when tombstones/total >= MaxTombstoneRatio (0.0 to 1.0)

	// LRU cache for tombstone statistics (shared globally)
	// No need to add cache field here - uses globalTombstoneCache
}

// NewTombstoneRatioStrategy creates a tombstone ratio based strategy
func NewTombstoneRatioStrategy(maxRatio float64) *TombstoneRatioStrategy {
	if maxRatio <= 0 || maxRatio > 1.0 {
		maxRatio = 0.3 // Default to 30% tombstones
	}
	return &TombstoneRatioStrategy{MaxTombstoneRatio: maxRatio}
}

// ShouldCompact returns true if tombstone ratio exceeds threshold
// Uses LRU cache to avoid rescanning files repeatedly
func (trs *TombstoneRatioStrategy) ShouldCompact(files []string) bool {
	if len(files) == 0 {
		return false
	}

	var totalEntries uint64 = 0
	var totalTombstones uint64 = 0

	for _, filePath := range files {
		// Try to get from cache first
		if stats, found := globalTombstoneCache.get(filePath); found {
			totalEntries += stats.totalEntries
			totalTombstones += stats.tombstones
			continue
		}

		// Cache miss - scan the file to count tombstones
		if globalTombstoneCache.metricsReporter != nil {
			globalTombstoneCache.metricsReporter("TombstoneScansPerformed", 1)
		}

		entries, tombstones, err := trs.scanFileForTombstones(filePath)
		if err != nil {
			// If we can't scan the file, skip it (file might be deleted/corrupted)
			continue
		}

		totalEntries += entries
		totalTombstones += tombstones

		// Cache the results for future checks
		fileInfo, err := os.Stat(filePath)
		if err == nil {
			stats := &tombstoneStats{
				filePath:     filePath,
				totalEntries: entries,
				tombstones:   tombstones,
				lastAccessed: time.Now(),
				fileModTime:  fileInfo.ModTime(),
			}
			globalTombstoneCache.put(stats)
		}
	}

	if totalEntries == 0 {
		return false
	}

	tombstoneRatio := float64(totalTombstones) / float64(totalEntries)
	return tombstoneRatio >= trs.MaxTombstoneRatio
}

// scanFileForTombstones scans a hash index entry file and counts tombstones
// Returns total entries, tombstone count, and any error
func (trs *TombstoneRatioStrategy) scanFileForTombstones(filePath string) (uint64, uint64, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return 0, 0, err
	}
	defer file.Close()

	// Get file size for efficient scanning
	fileInfo, err := file.Stat()
	if err != nil {
		return 0, 0, err
	}

	// If file is too small to contain even one entry, skip it
	// Minimum entry size: 4 bytes (magic) + 8 (entryID) + 8 (timestamp) + 8 (sequence)
	//                   + 4 (hash) + 4 (bucket) + 1 (deleted flag) + 2 (keyLen) + 2 (docIDLen)
	//                   + 4 (checksum) = 45 bytes minimum
	if fileInfo.Size() < 45 {
		return 0, 0, nil
	}

	var totalEntries uint64 = 0
	var tombstones uint64 = 0

	// Buffer for reading magic numbers (4 bytes each)
	magicBuf := make([]byte, 4)

	for {
		// Read magic number to determine entry type
		n, err := file.Read(magicBuf)
		if err != nil {
			if err.Error() == "EOF" {
				break // End of file
			}
			return 0, 0, err
		}
		if n < 4 {
			break // Incomplete magic number at end of file
		}

		magic := uint32(magicBuf[0]) | uint32(magicBuf[1])<<8 |
			uint32(magicBuf[2])<<16 | uint32(magicBuf[3])<<24

		totalEntries++

		// Check if this is a tombstone entry (0xDEADDEAD)
		if magic == 0xDEADDEAD {
			tombstones++
		}

		// Skip the rest of this entry to get to the next one
		// Read the entry size to know how much to skip
		// Entry structure after magic:
		// - EntryID (8 bytes)
		// - Timestamp (8 bytes)
		// - Sequence (8 bytes)
		// - HashValue (4 bytes)
		// - BucketNum (4 bytes)
		// - Deleted flag (1 byte)
		// - KeyLen (2 bytes)
		// - DocumentIDLen (2 bytes)
		// - KeyValue (variable)
		// - DocumentID (variable)
		// - Checksum (4 bytes)

		// Read KeyLen and DocumentIDLen to know how much variable data to skip
		fixedSizeBuf := make([]byte, 31) // 8+8+8+4+4+1+2+2 = 37 bytes, but we already read 4 for magic
		n, err = file.Read(fixedSizeBuf)
		if err != nil || n < 31 {
			break // Incomplete entry
		}

		// Extract KeyLen (2 bytes at offset 27 from start of entry, offset 23 after magic)
		keyLen := uint16(fixedSizeBuf[23]) | uint16(fixedSizeBuf[24])<<8

		// Extract DocumentIDLen (2 bytes at offset 29 from start of entry, offset 25 after magic)
		docIDLen := uint16(fixedSizeBuf[25]) | uint16(fixedSizeBuf[26])<<8

		// Skip variable-length key, documentID, and checksum (4 bytes)
		skipBytes := int64(keyLen) + int64(docIDLen) + 4
		_, err = file.Seek(skipBytes, 1) // Seek relative to current position
		if err != nil {
			break // Can't seek, probably EOF
		}
	}

	return totalEntries, tombstones, nil
}

// Name returns the strategy name
func (trs *TombstoneRatioStrategy) Name() string {
	return "TombstoneRatioStrategy"
}

// TimeBasedStrategy triggers compaction after a fixed time interval
// Provides predictable compaction behavior
//
// Benefits:
// - Predictable resource usage
// - Can schedule during off-peak hours
// - Simple to understand and configure
//
// Use when:
// - Predictable maintenance windows required
// - Background compaction is acceptable
// - Workload has daily/weekly patterns
type TimeBasedStrategy struct {
	CompactionInterval time.Duration // Trigger every N duration
	lastCompaction     time.Time     // Last compaction time
}

// NewTimeBasedStrategy creates a time based strategy
func NewTimeBasedStrategy(interval time.Duration) *TimeBasedStrategy {
	if interval <= 0 {
		interval = 1 * time.Hour // Default to 1 hour
	}
	return &TimeBasedStrategy{
		CompactionInterval: interval,
		lastCompaction:     time.Now(),
	}
}

// ShouldCompact returns true if enough time has passed
func (tbs *TimeBasedStrategy) ShouldCompact(files []string) bool {
	if len(files) == 0 {
		return false
	}
	return time.Since(tbs.lastCompaction) >= tbs.CompactionInterval
}

// Name returns the strategy name
func (tbs *TimeBasedStrategy) Name() string {
	return "TimeBasedStrategy"
}

// MarkCompacted updates the last compaction time
func (tbs *TimeBasedStrategy) MarkCompacted() {
	tbs.lastCompaction = time.Now()
}

// CompositeStrategy combines multiple strategies with AND or OR logic
// Allows complex compaction policies
//
// Examples:
// - AND: Compact only if file count > 10 AND size > 1GB
// - OR: Compact if file count > 10 OR tombstone ratio > 30%
//
// Use when:
// - Multiple conditions should trigger compaction
// - Complex policies are required
// - Different workloads need different triggers
type CompositeStrategy struct {
	Strategies []CompactionStrategy
	UseAND     bool // true = all must match, false = any can match
}

// NewCompositeStrategy creates a composite strategy
func NewCompositeStrategy(strategies []CompactionStrategy, useAND bool) *CompositeStrategy {
	return &CompositeStrategy{
		Strategies: strategies,
		UseAND:     useAND,
	}
}

// ShouldCompact returns true based on AND/OR logic
func (cs *CompositeStrategy) ShouldCompact(files []string) bool {
	if len(cs.Strategies) == 0 {
		return false
	}

	if cs.UseAND {
		// All strategies must agree
		for _, strategy := range cs.Strategies {
			if !strategy.ShouldCompact(files) {
				return false
			}
		}
		return true
	} else {
		// Any strategy can trigger
		for _, strategy := range cs.Strategies {
			if strategy.ShouldCompact(files) {
				return true
			}
		}
		return false
	}
}

// Name returns the strategy name
func (cs *CompositeStrategy) Name() string {
	logic := "OR"
	if cs.UseAND {
		logic = "AND"
	}
	return "CompositeStrategy(" + logic + ")"
}

// DefaultCompactionStrategy returns a reasonable default strategy
// Compact when file count >= 10 OR total size >= 512MB
func NewDefaultCompactionStrategy() CompactionStrategy {
	return NewCompositeStrategy(
		[]CompactionStrategy{
			NewFileCountStrategy(10),
			NewTotalSizeStrategy(512 * 1024 * 1024), // 512MB
		},
		false, // OR logic
	)
}

// AggressiveCompactionStrategy returns a strategy that compacts frequently
// Compact when file count >= 5 OR size >= 256MB OR tombstone ratio >= 20%
func NewAggressiveCompactionStrategy() CompactionStrategy {
	return NewCompositeStrategy(
		[]CompactionStrategy{
			NewFileCountStrategy(5),
			NewTotalSizeStrategy(256 * 1024 * 1024), // 256MB
			NewTombstoneRatioStrategy(0.2),          // 20%
		},
		false, // OR logic
	)
}

// ConservativeCompactionStrategy returns a strategy that compacts rarely
// Compact only when file count >= 20 AND size >= 1GB
func NewConservativeCompactionStrategy() CompactionStrategy {
	return NewCompositeStrategy(
		[]CompactionStrategy{
			NewFileCountStrategy(20),
			NewTotalSizeStrategy(1024 * 1024 * 1024), // 1GB
		},
		true, // AND logic - both must be true
	)
}

// GetFileSize is a helper function to get file size safely
func GetFileSize(filePath string) int64 {
	info, err := os.Stat(filePath)
	if err != nil {
		return 0
	}
	return info.Size()
}

// GetFilesInDirectory returns all files matching a pattern
// Useful for getting list of files to check for compaction
func GetFilesInDirectory(dirPath string, pattern string) ([]string, error) {
	matches, err := filepath.Glob(filepath.Join(dirPath, pattern))
	if err != nil {
		return nil, err
	}
	return matches, nil
}
