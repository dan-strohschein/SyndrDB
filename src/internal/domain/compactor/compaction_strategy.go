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
	"os"
	"path/filepath"
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

	// TODO: Sprint 3 - Integration with EntryStorage
	// This requires counting tombstones by scanning files
	// For now, this is a placeholder strategy
}

// NewTombstoneRatioStrategy creates a tombstone ratio based strategy
func NewTombstoneRatioStrategy(maxRatio float64) *TombstoneRatioStrategy {
	if maxRatio <= 0 || maxRatio > 1.0 {
		maxRatio = 0.3 // Default to 30% tombstones
	}
	return &TombstoneRatioStrategy{MaxTombstoneRatio: maxRatio}
}

// ShouldCompact returns true if tombstone ratio exceeds threshold
func (trs *TombstoneRatioStrategy) ShouldCompact(files []string) bool {
	// TODO: Sprint 3 - Implement tombstone counting
	// This requires scanning files and counting deleted entries
	//
	// Implementation steps:
	// 1. For each file, scan forward
	// 2. Count total entries and tombstone entries
	// 3. Calculate ratio = tombstones / total
	// 4. Return true if ratio >= MaxTombstoneRatio
	//
	// Optimization: Cache tombstone counts per file
	// Only rescan when file is modified

	// For now, return false (not implemented)
	return false
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
