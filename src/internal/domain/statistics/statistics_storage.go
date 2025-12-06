/*
statistics_storage.go

This file implements persistent storage for bundle statistics with versioning,
tiered compression, and atomic writes.

DESIGN PRINCIPLES:
- Atomic Writes: Statistics updates are atomic to prevent corruption
- Versioned Storage: Automatic migration/re-analysis on version mismatch
- Tiered Compression: Size-based compression strategy (gzip vs zstd)

COMPRESSION STRATEGY:
- Uncompressed: < threshold MB (configurable, default 1MB)
- GZIP compression: threshold to 5MB (fast, moderate compression)
- ZSTD compression: >= 5MB (slower, high compression ratio)

This follows PostgreSQL's approach to statistics persistence with optimizations
for SyndrDB's document-based storage model.
*/

package statistics

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/klauspost/compress/zstd"
	"go.uber.org/zap"
)

const (
	STATS_DIR_NAME        = "statistics"
	STATS_FILE_EXTENSION  = ".stats.json"
	TEMP_FILE_SUFFIX      = ".tmp"
	COMPRESSION_THRESHOLD = 1024 * 1024     // 1MB default
	ZSTD_THRESHOLD        = 5 * 1024 * 1024 // 5MB threshold for zstd
)

// CompressionStrategy defines how statistics are compressed
type CompressionStrategy int

const (
	COMPRESSION_NONE CompressionStrategy = iota
	COMPRESSION_GZIP
	COMPRESSION_ZSTD
)

// StatisticsStorage manages persistent storage of bundle statistics
type StatisticsStorage struct {
	baseDir                string
	compressionThresholdMB int
	logger                 *zap.SugaredLogger
	mu                     sync.RWMutex
	zstdEncoder            *zstd.Encoder
	zstdDecoder            *zstd.Decoder
}

// StorageConfig holds configuration for statistics storage
type StorageConfig struct {
	BaseDir                string
	CompressionThresholdMB int
}

// NewStatisticsStorage creates a new statistics storage manager
func NewStatisticsStorage(config StorageConfig, logger *zap.SugaredLogger) (*StatisticsStorage, error) {
	if config.CompressionThresholdMB == 0 {
		config.CompressionThresholdMB = 1 // Default 1MB
	}

	// Create statistics directory if it doesn't exist
	statsDir := filepath.Join(config.BaseDir, STATS_DIR_NAME)
	if err := os.MkdirAll(statsDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create statistics directory: %w", err)
	}

	// Initialize zstd encoder/decoder
	encoder, err := zstd.NewWriter(nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create zstd encoder: %w", err)
	}

	decoder, err := zstd.NewReader(nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create zstd decoder: %w", err)
	}

	return &StatisticsStorage{
		baseDir:                config.BaseDir,
		compressionThresholdMB: config.CompressionThresholdMB,
		logger:                 logger,
		zstdEncoder:            encoder,
		zstdDecoder:            decoder,
	}, nil
}

// SaveStatistics atomically saves bundle statistics to disk
func (ss *StatisticsStorage) SaveStatistics(stats *BundleStatistics) error {
	if stats == nil {
		return fmt.Errorf("statistics cannot be nil")
	}

	ss.mu.Lock()
	defer ss.mu.Unlock()

	// Marshal to JSON
	jsonData, err := json.MarshalIndent(stats, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal statistics: %w", err)
	}

	uncompressedSize := int64(len(jsonData))
	stats.UncompressedSize = uncompressedSize

	// Determine compression strategy
	strategy := ss.determineCompressionStrategy(uncompressedSize)

	// Compress data
	var dataToWrite []byte
	switch strategy {
	case COMPRESSION_NONE:
		dataToWrite = jsonData
		ss.logger.Infof("Saving statistics for %s (uncompressed, %d bytes)",
			stats.BundleName, len(jsonData))

	case COMPRESSION_GZIP:
		var buf bytes.Buffer
		gzWriter := gzip.NewWriter(&buf)
		if _, err := gzWriter.Write(jsonData); err != nil {
			return fmt.Errorf("failed to gzip compress: %w", err)
		}
		if err := gzWriter.Close(); err != nil {
			return fmt.Errorf("failed to close gzip writer: %w", err)
		}
		dataToWrite = buf.Bytes()
		stats.CompressedSize = int64(len(dataToWrite))
		ss.logger.Infof("Saving statistics for %s (gzip, %d -> %d bytes, %.1f%% ratio)",
			stats.BundleName, uncompressedSize, stats.CompressedSize,
			float64(stats.CompressedSize)/float64(uncompressedSize)*100)

	case COMPRESSION_ZSTD:
		dataToWrite = ss.zstdEncoder.EncodeAll(jsonData, make([]byte, 0, len(jsonData)))
		stats.CompressedSize = int64(len(dataToWrite))
		ss.logger.Infof("Saving statistics for %s (zstd, %d -> %d bytes, %.1f%% ratio)",
			stats.BundleName, uncompressedSize, stats.CompressedSize,
			float64(stats.CompressedSize)/float64(uncompressedSize)*100)
	}

	// Write atomically using temp file + rename
	finalPath := ss.getStatisticsPath(stats.BundleName, strategy)
	tempPath := finalPath + TEMP_FILE_SUFFIX

	if err := os.WriteFile(tempPath, dataToWrite, 0644); err != nil {
		return fmt.Errorf("failed to write temp file: %w", err)
	}

	if err := os.Rename(tempPath, finalPath); err != nil {
		os.Remove(tempPath) // Clean up temp file
		return fmt.Errorf("failed to atomically rename file: %w", err)
	}

	return nil
}

// LoadStatistics loads bundle statistics from disk
func (ss *StatisticsStorage) LoadStatistics(bundleName string) (*BundleStatistics, error) {
	ss.mu.RLock()
	defer ss.mu.RUnlock()

	// Try loading with each compression strategy
	strategies := []CompressionStrategy{COMPRESSION_ZSTD, COMPRESSION_GZIP, COMPRESSION_NONE}

	var lastErr error
	for _, strategy := range strategies {
		path := ss.getStatisticsPath(bundleName, strategy)

		data, err := os.ReadFile(path)
		if err != nil {
			if !os.IsNotExist(err) {
				lastErr = err
			}
			continue
		}

		// Decompress if needed
		var jsonData []byte
		switch strategy {
		case COMPRESSION_NONE:
			jsonData = data

		case COMPRESSION_GZIP:
			gzReader, err := gzip.NewReader(bytes.NewReader(data))
			if err != nil {
				return nil, fmt.Errorf("failed to create gzip reader: %w", err)
			}
			jsonData, err = io.ReadAll(gzReader)
			gzReader.Close()
			if err != nil {
				return nil, fmt.Errorf("failed to decompress gzip: %w", err)
			}

		case COMPRESSION_ZSTD:
			jsonData, err = ss.zstdDecoder.DecodeAll(data, nil)
			if err != nil {
				return nil, fmt.Errorf("failed to decompress zstd: %w", err)
			}
		}

		// Unmarshal statistics
		var stats BundleStatistics
		if err := json.Unmarshal(jsonData, &stats); err != nil {
			return nil, fmt.Errorf("failed to unmarshal statistics: %w", err)
		}

		// Version check
		if stats.StatsVersion != STATS_VERSION {
			ss.logger.Warnf("Statistics version mismatch for %s: found %d, expected %d (re-analysis required)",
				bundleName, stats.StatsVersion, STATS_VERSION)
			return nil, fmt.Errorf("statistics version mismatch: found %d, expected %d",
				stats.StatsVersion, STATS_VERSION)
		}

		ss.logger.Infof("Loaded statistics for %s from %s", bundleName, filepath.Base(path))
		return &stats, nil
	}

	if lastErr != nil {
		return nil, fmt.Errorf("failed to load statistics: %w", lastErr)
	}

	return nil, fmt.Errorf("statistics not found for bundle: %s", bundleName)
}

// DeleteStatistics removes statistics file for a bundle
func (ss *StatisticsStorage) DeleteStatistics(bundleName string) error {
	ss.mu.Lock()
	defer ss.mu.Unlock()

	// Try deleting all possible compression formats
	strategies := []CompressionStrategy{COMPRESSION_NONE, COMPRESSION_GZIP, COMPRESSION_ZSTD}
	deleted := false

	for _, strategy := range strategies {
		path := ss.getStatisticsPath(bundleName, strategy)
		if err := os.Remove(path); err != nil {
			if !os.IsNotExist(err) {
				return fmt.Errorf("failed to delete statistics file: %w", err)
			}
		} else {
			deleted = true
		}
	}

	if !deleted {
		return fmt.Errorf("no statistics file found for bundle: %s", bundleName)
	}

	ss.logger.Infof("Deleted statistics for bundle: %s", bundleName)
	return nil
}

// ExistsStatistics checks if statistics exist for a bundle
func (ss *StatisticsStorage) ExistsStatistics(bundleName string) bool {
	ss.mu.RLock()
	defer ss.mu.RUnlock()

	strategies := []CompressionStrategy{COMPRESSION_ZSTD, COMPRESSION_GZIP, COMPRESSION_NONE}

	for _, strategy := range strategies {
		path := ss.getStatisticsPath(bundleName, strategy)
		if _, err := os.Stat(path); err == nil {
			return true
		}
	}

	return false
}

// ListBundlesWithStatistics returns all bundles that have statistics
func (ss *StatisticsStorage) ListBundlesWithStatistics() ([]string, error) {
	ss.mu.RLock()
	defer ss.mu.RUnlock()

	statsDir := filepath.Join(ss.baseDir, STATS_DIR_NAME)

	entries, err := os.ReadDir(statsDir)
	if err != nil {
		if os.IsNotExist(err) {
			return []string{}, nil
		}
		return nil, fmt.Errorf("failed to read statistics directory: %w", err)
	}

	// Extract bundle names (remove extensions)
	bundleSet := make(map[string]bool)
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		name := entry.Name()
		// Remove compression-specific extensions
		if filepath.Ext(name) == ".json" {
			name = name[:len(name)-5] // Remove .json
		}
		if filepath.Ext(name) == ".gz" {
			name = name[:len(name)-3] // Remove .gz
		}
		if filepath.Ext(name) == ".zst" {
			name = name[:len(name)-4] // Remove .zst
		}
		// Remove .stats
		if filepath.Ext(name) == ".stats" {
			name = name[:len(name)-6]
		}

		if name != "" {
			bundleSet[name] = true
		}
	}

	bundles := make([]string, 0, len(bundleSet))
	for bundle := range bundleSet {
		bundles = append(bundles, bundle)
	}

	return bundles, nil
}

// Close releases resources
func (ss *StatisticsStorage) Close() error {
	if ss.zstdEncoder != nil {
		ss.zstdEncoder.Close()
	}
	if ss.zstdDecoder != nil {
		ss.zstdDecoder.Close()
	}
	return nil
}

// Helper functions

func (ss *StatisticsStorage) determineCompressionStrategy(size int64) CompressionStrategy {
	thresholdBytes := int64(ss.compressionThresholdMB * 1024 * 1024)

	if size < thresholdBytes {
		return COMPRESSION_NONE
	} else if size < ZSTD_THRESHOLD {
		return COMPRESSION_GZIP
	} else {
		return COMPRESSION_ZSTD
	}
}

func (ss *StatisticsStorage) getStatisticsPath(bundleName string, strategy CompressionStrategy) string {
	statsDir := filepath.Join(ss.baseDir, STATS_DIR_NAME)

	var filename string
	switch strategy {
	case COMPRESSION_NONE:
		filename = bundleName + STATS_FILE_EXTENSION
	case COMPRESSION_GZIP:
		filename = bundleName + STATS_FILE_EXTENSION + ".gz"
	case COMPRESSION_ZSTD:
		filename = bundleName + STATS_FILE_EXTENSION + ".zst"
	}

	return filepath.Join(statsDir, filename)
}
