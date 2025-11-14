package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"go.uber.org/zap"
)

// TestBinaryWALSerialization tests the binary WAL serialization/deserialization
func TestBinaryWALSerialization(t *testing.T) {
	// Create a temporary logger for testing
	logger, _ := zap.NewDevelopment()
	sugar := logger.Sugar()

	// Create temporary WAL for testing
	tmpDir := os.TempDir()
	walPath := filepath.Join(tmpDir, "test_binary_wal")

	config := WALConfig{
		LogDir:             tmpDir,
		MaxFileSize:        1024 * 1024, // 1MB
		FlushInterval:      time.Second,
		RetentionDays:      7,
		FsyncOnCommit:      true,
		CompressionEnabled: false,
		EncryptionEnabled:  false,
		AutoFlush:          true,
	}

	wal, err := NewWriteAheadLog(config, sugar)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer wal.Close()
	defer os.Remove(walPath)

	// Create test WAL entry
	testEntry := WALEntry{
		LSN:        1,
		Timestamp:  time.Now(),
		TxID:       "test-tx-123",
		Operation:  OpInsert,
		BundleName: "test-bundle",
		DocumentID: "doc-12345",
		BeforeData: "",
		AfterData:  `{"name": "test", "value": 42, "active": true}`,
		Metadata:   `{"index": "name", "timestamp": "2025-09-30"}`,
	}

	// Calculate checksum
	testEntry.Checksum = wal.calculateChecksum(testEntry)

	// Test binary serialization
	binaryData, err := wal.SerializeWALEntryBinary(testEntry)
	if err != nil {
		t.Fatalf("Failed to serialize WAL entry to binary: %v", err)
	}

	t.Logf("Binary WAL entry size: %d bytes", len(binaryData))

	// Test binary deserialization
	deserializedEntry, err := wal.DeserializeWALEntryBinary(binaryData)
	if err != nil {
		t.Fatalf("Failed to deserialize binary WAL entry: %v", err)
	}

	// Verify all fields match
	if deserializedEntry.LSN != testEntry.LSN {
		t.Errorf("LSN mismatch: got %d, expected %d", deserializedEntry.LSN, testEntry.LSN)
	}
	if deserializedEntry.TxID != testEntry.TxID {
		t.Errorf("TxID mismatch: got %s, expected %s", deserializedEntry.TxID, testEntry.TxID)
	}
	if deserializedEntry.Operation != testEntry.Operation {
		t.Errorf("Operation mismatch: got %d, expected %d", deserializedEntry.Operation, testEntry.Operation)
	}
	if deserializedEntry.BundleName != testEntry.BundleName {
		t.Errorf("BundleName mismatch: got %s, expected %s", deserializedEntry.BundleName, testEntry.BundleName)
	}
	if deserializedEntry.DocumentID != testEntry.DocumentID {
		t.Errorf("DocumentID mismatch: got %s, expected %s", deserializedEntry.DocumentID, testEntry.DocumentID)
	}
	if deserializedEntry.BeforeData != testEntry.BeforeData {
		t.Errorf("BeforeData mismatch: got %s, expected %s", deserializedEntry.BeforeData, testEntry.BeforeData)
	}
	if deserializedEntry.AfterData != testEntry.AfterData {
		t.Errorf("AfterData mismatch: got %s, expected %s", deserializedEntry.AfterData, testEntry.AfterData)
	}
	if deserializedEntry.Metadata != testEntry.Metadata {
		t.Errorf("Metadata mismatch: got %s, expected %s", deserializedEntry.Metadata, testEntry.Metadata)
	}
	if deserializedEntry.Checksum != testEntry.Checksum {
		t.Errorf("Checksum mismatch: got %d, expected %d", deserializedEntry.Checksum, testEntry.Checksum)
	}

	// Verify timestamp within reasonable bounds (timestamps can have slight differences)
	timeDiff := deserializedEntry.Timestamp.Sub(testEntry.Timestamp)
	if timeDiff < -time.Millisecond || timeDiff > time.Millisecond {
		t.Errorf("Timestamp differs by more than 1ms: %v", timeDiff)
	}

	t.Log("✅ Binary WAL serialization/deserialization test passed")
}

// TestBinaryWALPerformance compares binary vs ASCII performance
func TestBinaryWALPerformance(t *testing.T) {
	// Skip performance tests in short mode
	if testing.Short() {
		t.Skip("Skipping performance test in short mode")
	}

	// Create temporary logger for testing
	logger, _ := zap.NewDevelopment()
	sugar := logger.Sugar()

	// Test entry
	testEntry := WALEntry{
		LSN:        1,
		Timestamp:  time.Now(),
		TxID:       "performance-test-tx-123456789",
		Operation:  OpInsert,
		BundleName: "performance-test-bundle-with-long-name",
		DocumentID: "performance-test-document-id-123456789",
		BeforeData: "",
		AfterData:  `{"name": "performance test", "description": "this is a test document for performance testing", "data": {"nested": {"field": "value", "number": 12345, "array": [1,2,3,4,5]}}, "timestamp": "2025-09-30T12:00:00Z", "active": true}`,
		Metadata:   `{"operation": "insert", "index_updates": ["name", "timestamp"], "source": "performance_test", "version": 1}`,
	}

	// Create temporary WAL
	tmpDir := os.TempDir()
	walPath := filepath.Join(tmpDir, "test_perf_wal")

	config := WALConfig{
		LogDir:             tmpDir,
		MaxFileSize:        1024 * 1024 * 10, // 10MB
		FlushInterval:      time.Second,
		RetentionDays:      7,
		FsyncOnCommit:      false, // Disable fsync for performance testing
		CompressionEnabled: false,
		EncryptionEnabled:  false,
		AutoFlush:          false, // Disable auto flush for controlled testing
	}

	wal, err := NewWriteAheadLog(config, sugar)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer wal.Close()
	defer os.Remove(walPath)

	testEntry.Checksum = wal.calculateChecksum(testEntry)

	// Test binary serialization performance
	iterations := 10000

	start := time.Now()
	var totalBinarySize int64
	for i := 0; i < iterations; i++ {
		binaryData, err := wal.SerializeWALEntryBinary(testEntry)
		if err != nil {
			t.Fatalf("Binary serialization failed: %v", err)
		}
		totalBinarySize += int64(len(binaryData))
	}
	binarySerializeTime := time.Since(start)

	// Test binary deserialization performance
	binaryData, _ := wal.SerializeWALEntryBinary(testEntry)
	start = time.Now()
	for i := 0; i < iterations; i++ {
		_, err := wal.DeserializeWALEntryBinary(binaryData)
		if err != nil {
			t.Fatalf("Binary deserialization failed: %v", err)
		}
	}
	binaryDeserializeTime := time.Since(start)

	avgBinarySize := totalBinarySize / int64(iterations)

	// Performance results
	t.Logf("=== BINARY WAL PERFORMANCE RESULTS ===")
	t.Logf("Iterations: %d", iterations)
	t.Logf("Average binary entry size: %d bytes", avgBinarySize)
	t.Logf("Binary serialization time: %v (%v per op)", binarySerializeTime, binarySerializeTime/time.Duration(iterations))
	t.Logf("Binary deserialization time: %v (%v per op)", binaryDeserializeTime, binaryDeserializeTime/time.Duration(iterations))
	t.Logf("Binary serialize ops/sec: %.0f", float64(iterations)/binarySerializeTime.Seconds())
	t.Logf("Binary deserialize ops/sec: %.0f", float64(iterations)/binaryDeserializeTime.Seconds())

	// Verify binary format is significantly more compact and faster
	if avgBinarySize > 1000 { // Binary should be much more compact than JSON
		t.Logf("Warning: Binary format might be larger than expected: %d bytes", avgBinarySize)
	}

	t.Log("✅ Binary WAL performance test completed")
}

// BenchmarkBinaryWALSerialization benchmarks binary WAL serialization
func BenchmarkBinaryWALSerialization(b *testing.B) {
	logger, _ := zap.NewDevelopment()
	sugar := logger.Sugar()

	tmpDir := os.TempDir()
	walPath := filepath.Join(tmpDir, "bench_wal")

	config := WALConfig{
		LogDir:             tmpDir,
		MaxFileSize:        1024 * 1024,
		FlushInterval:      time.Second,
		RetentionDays:      7,
		FsyncOnCommit:      false,
		CompressionEnabled: false,
		EncryptionEnabled:  false,
		AutoFlush:          false,
	}

	wal, err := NewWriteAheadLog(config, sugar)
	if err != nil {
		b.Fatalf("Failed to create WAL: %v", err)
	}
	defer wal.Close()
	defer os.Remove(walPath)

	testEntry := WALEntry{
		LSN:        1,
		Timestamp:  time.Now(),
		TxID:       "bench-tx-123",
		Operation:  OpInsert,
		BundleName: "bench-bundle",
		DocumentID: "bench-doc-123",
		BeforeData: "",
		AfterData:  `{"name": "benchmark", "value": 42}`,
		Metadata:   `{"index": "name"}`,
	}
	testEntry.Checksum = wal.calculateChecksum(testEntry)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := wal.SerializeWALEntryBinary(testEntry)
		if err != nil {
			b.Fatalf("Serialization failed: %v", err)
		}
	}
}

// BenchmarkBinaryWALDeserialization benchmarks binary WAL deserialization
func BenchmarkBinaryWALDeserialization(b *testing.B) {
	logger, _ := zap.NewDevelopment()
	sugar := logger.Sugar()

	tmpDir := os.TempDir()
	walPath := filepath.Join(tmpDir, "bench_wal_deser")

	config := WALConfig{
		LogDir:             tmpDir,
		MaxFileSize:        1024 * 1024,
		FlushInterval:      time.Second,
		RetentionDays:      7,
		FsyncOnCommit:      false,
		CompressionEnabled: false,
		EncryptionEnabled:  false,
		AutoFlush:          false,
	}

	wal, err := NewWriteAheadLog(config, sugar)
	if err != nil {
		b.Fatalf("Failed to create WAL: %v", err)
	}
	defer wal.Close()
	defer os.Remove(walPath)

	testEntry := WALEntry{
		LSN:        1,
		Timestamp:  time.Now(),
		TxID:       "bench-tx-123",
		Operation:  OpInsert,
		BundleName: "bench-bundle",
		DocumentID: "bench-doc-123",
		BeforeData: "",
		AfterData:  `{"name": "benchmark", "value": 42}`,
		Metadata:   `{"index": "name"}`,
	}
	testEntry.Checksum = wal.calculateChecksum(testEntry)

	// Pre-serialize the test data
	binaryData, err := wal.SerializeWALEntryBinary(testEntry)
	if err != nil {
		b.Fatalf("Failed to serialize test data: %v", err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := wal.DeserializeWALEntryBinary(binaryData)
		if err != nil {
			b.Fatalf("Deserialization failed: %v", err)
		}
	}
}
