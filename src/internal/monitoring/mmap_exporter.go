// Package monitoring provides real-time metrics export functionality for external monitoring tools.
//
// The memory-mapped exporter writes metrics to a binary file that can be read by external
// monitoring applications without requiring network connections or database queries.
//
// Binary Format (Little Endian):
//   [version:4 bytes][timestamp:8 bytes][count:8 bytes][metrics array]
//   Each metric: [key_length:4 bytes][key:N bytes][value:8 bytes]
//
// Design principles:
// - Lock-free reads via memory-mapped files (mmap)
// - Atomic file updates to prevent partial reads
// - Version header for backward compatibility
// - Small file size (~10-20KB) for fast reads
// - No external dependencies (pure Go)
//
// This file implements the MemoryMappedExporter which exports GlobalServerMetrics
// to a binary file format that external monitoring tools can read in real-time.
// The exporter runs in a background goroutine and updates the file at configurable intervals.
package monitoring

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"syndrdb/src/internal/server"

	"go.uber.org/zap"
)

// MemoryMappedExporter exports metrics to a memory-mapped file for external monitoring
// This allows real-time monitoring tools to read metrics without network overhead
type MemoryMappedExporter struct {
	filePath string             // Path to the memory-mapped metrics file
	interval time.Duration      // How often to update the file
	logger   *zap.SugaredLogger // Logger for debugging

	// Background goroutine control
	stopChan chan struct{}
	wg       sync.WaitGroup
	running  bool
	mu       sync.Mutex
}

// NewMemoryMappedExporter creates a new memory-mapped metrics exporter
//
// Parameters:
//   - filePath: Full path where the metrics file will be written (e.g., "/tmp/syndrdb_metrics.mmap")
//   - interval: How frequently to update the file (default: 1 second)
//   - logger: Logger for debugging and error reporting
//
// Returns:
//   - *MemoryMappedExporter: The exporter instance
//   - error: Error if file path is invalid or permissions are insufficient
func NewMemoryMappedExporter(filePath string, interval time.Duration, logger *zap.SugaredLogger) (*MemoryMappedExporter, error) {
	// Validate file path
	if filePath == "" {
		return nil, fmt.Errorf("file path cannot be empty")
	}

	// Ensure parent directory exists
	dir := filepath.Dir(filePath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create directory %s: %w", dir, err)
	}

	// Default interval if not specified
	if interval <= 0 {
		interval = 1 * time.Second
	}

	return &MemoryMappedExporter{
		filePath: filePath,
		interval: interval,
		logger:   logger,
		stopChan: make(chan struct{}),
		running:  false,
	}, nil
}

// Start begins the background metrics export process
// This method is non-blocking and launches a goroutine to periodically update the metrics file
func (mme *MemoryMappedExporter) Start() error {
	mme.mu.Lock()
	defer mme.mu.Unlock()

	if mme.running {
		return fmt.Errorf("exporter is already running")
	}

	// Perform initial export to verify file access
	if err := mme.exportMetrics(); err != nil {
		return fmt.Errorf("initial metrics export failed: %w", err)
	}

	mme.running = true
	mme.wg.Add(1)

	go mme.exportLoop()

	mme.logger.Infof("Started memory-mapped metrics exporter: %s (interval: %v)", mme.filePath, mme.interval)
	return nil
}

// Stop gracefully stops the metrics export process
// Performs a final metrics export before stopping
func (mme *MemoryMappedExporter) Stop() error {
	mme.mu.Lock()
	if !mme.running {
		mme.mu.Unlock()
		return nil
	}
	mme.running = false
	mme.mu.Unlock()

	// Signal stop and wait for goroutine to finish
	close(mme.stopChan)
	mme.wg.Wait()

	// Final export
	if err := mme.exportMetrics(); err != nil {
		mme.logger.Warnf("Final metrics export failed: %v", err)
		return err
	}

	mme.logger.Info("Stopped memory-mapped metrics exporter")
	return nil
}

// exportLoop runs the periodic metrics export in a background goroutine
func (mme *MemoryMappedExporter) exportLoop() {
	defer mme.wg.Done()

	ticker := time.NewTicker(mme.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := mme.exportMetrics(); err != nil {
				mme.logger.Errorf("Failed to export metrics: %v", err)
			}
		case <-mme.stopChan:
			return
		}
	}
}

// exportMetrics writes the current metrics to the memory-mapped file
// Uses atomic file replacement (write to temp, then rename) to prevent partial reads.
// Uses MetricDescriptors for zero-allocation reads (no map alloc per cycle).
func (mme *MemoryMappedExporter) exportMetrics() error {
	descriptors := server.GetMetricDescriptors()

	// Serialize directly from descriptors to binary format
	data, err := mme.serializeMetricsFromDescriptors(descriptors)
	if err != nil {
		return fmt.Errorf("failed to serialize metrics: %w", err)
	}

	// Atomic file write: write to temp file, then rename
	tempPath := mme.filePath + ".tmp"
	if err := os.WriteFile(tempPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write temp file: %w", err)
	}

	if err := os.Rename(tempPath, mme.filePath); err != nil {
		os.Remove(tempPath) // Clean up temp file on error
		return fmt.Errorf("failed to rename temp file: %w", err)
	}

	return nil
}

// serializeMetricsFromDescriptors writes metrics directly from descriptors to binary format,
// avoiding the intermediate map[string]uint64 allocation.
func (mme *MemoryMappedExporter) serializeMetricsFromDescriptors(descriptors []server.MetricDescriptor) ([]byte, error) {
	// Calculate total size needed
	totalSize := 4 + 8 + 8 // version + timestamp + count
	for i := range descriptors {
		totalSize += 4 + len(descriptors[i].Name) + 8 // key_length + key + value
	}

	// Pre-allocate buffer
	buf := make([]byte, 0, totalSize)

	// Write version
	versionBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(versionBytes, server.MMAP_FORMAT_VERSION)
	buf = append(buf, versionBytes...)

	// Write timestamp
	timestampBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(timestampBytes, uint64(time.Now().Unix()))
	buf = append(buf, timestampBytes...)

	// Write count
	countBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(countBytes, uint64(len(descriptors)))
	buf = append(buf, countBytes...)

	// Write each metric
	keyLenBytes := make([]byte, 4)
	valueBytes := make([]byte, 8)
	for i := range descriptors {
		// Key length
		binary.LittleEndian.PutUint32(keyLenBytes, uint32(len(descriptors[i].Name)))
		buf = append(buf, keyLenBytes...)
		// Key
		buf = append(buf, descriptors[i].Name...)
		// Value (load atomically)
		binary.LittleEndian.PutUint64(valueBytes, descriptors[i].Ptr.Load())
		buf = append(buf, valueBytes...)
	}

	return buf, nil
}

// serializeMetrics converts metrics map to binary format
//
// Binary Format (Little Endian):
//   [version:4 bytes]       - Format version (MMAP_FORMAT_VERSION)
//   [timestamp:8 bytes]     - Unix timestamp in seconds
//   [count:8 bytes]         - Number of metrics
//   [metrics array]         - Variable length array of metrics
//
// Each metric in array:
//   [key_length:4 bytes]    - Length of metric name
//   [key:N bytes]           - Metric name (UTF-8)
//   [value:8 bytes]         - Metric value (uint64)
func (mme *MemoryMappedExporter) serializeMetrics(metrics map[string]uint64) ([]byte, error) {
	// Calculate total size needed
	totalSize := 4 + 8 + 8 // version + timestamp + count
	for key := range metrics {
		totalSize += 4 + len(key) + 8 // key_length + key + value
	}

	// Pre-allocate buffer
	buf := make([]byte, 0, totalSize)

	// Write version (4 bytes)
	versionBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(versionBytes, server.MMAP_FORMAT_VERSION)
	buf = append(buf, versionBytes...)

	// Write timestamp (8 bytes)
	timestampBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(timestampBytes, uint64(time.Now().Unix()))
	buf = append(buf, timestampBytes...)

	// Write metric count (8 bytes)
	countBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(countBytes, uint64(len(metrics)))
	buf = append(buf, countBytes...)

	// Write each metric
	for key, value := range metrics {
		// Write key length (4 bytes)
		keyLenBytes := make([]byte, 4)
		binary.LittleEndian.PutUint32(keyLenBytes, uint32(len(key)))
		buf = append(buf, keyLenBytes...)

		// Write key (variable bytes)
		buf = append(buf, []byte(key)...)

		// Write value (8 bytes)
		valueBytes := make([]byte, 8)
		binary.LittleEndian.PutUint64(valueBytes, value)
		buf = append(buf, valueBytes...)
	}

	return buf, nil
}

// GetFilePath returns the path to the metrics file
func (mme *MemoryMappedExporter) GetFilePath() string {
	return mme.filePath
}

// IsRunning returns whether the exporter is currently running
func (mme *MemoryMappedExporter) IsRunning() bool {
	mme.mu.Lock()
	defer mme.mu.Unlock()
	return mme.running
}
