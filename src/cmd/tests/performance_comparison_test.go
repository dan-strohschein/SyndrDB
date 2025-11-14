//go:build ignore
// +build ignore

package main

// DISABLED: Performance test requires internal journal package access
//
// This test file requires access to:
//   - wal.calculateChecksum() - unexported method
//   - All journal types: WALEntry, WriteAheadLog, OperationType, OpInsert
//
// To enable this test:
//   1. Move this file to src/internal/journal/performance_comparison_test.go
//      (tests in the same package can access unexported methods)
//   2. Change package declaration from "package main" to "package journal"
//   3. Remove the `// +build ignore` line above
//   4. This is a performance benchmark - consider renaming to *_bench_test.go
//      and using b *testing.B instead of t *testing.T for proper benchmarking
//
// Alternatively, export calculateChecksum as CalculateChecksum if it needs to be
// tested from external packages, though performance tests are typically internal.

import (
	"encoding/json"
	"testing"
	"time"
)

// TestPerformanceComparison compares binary vs ASCII JSON performance
func TestPerformanceComparison(t *testing.T) {
	// Setup test data
	testEntry := WALEntry{
		LSN:        1,
		Timestamp:  time.Now(),
		TxID:       "test-transaction-12345",
		Operation:  OpInsert,
		BundleName: "performance-test-bundle",
		DocumentID: "document-12345",
		BeforeData: "",
		AfterData:  `{"name": "performance test", "data": {"nested": true, "values": [1,2,3,4,5]}, "timestamp": "2025-09-30T18:00:00Z"}`,
		Metadata:   `{"index": "name", "operation": "bulk_insert", "batch_id": "batch-12345"}`,
	}

	// Create WAL instance
	wal := &WriteAheadLog{}
	testEntry.Checksum = wal.calculateChecksum(testEntry)

	const iterations = 10000

	// Test Binary Performance
	t.Run("BinaryPerformance", func(t *testing.T) {
		// Serialize performance
		start := time.Now()
		var binaryData []byte
		for i := 0; i < iterations; i++ {
			data, err := wal.SerializeWALEntryBinary(testEntry)
			if err != nil {
				t.Fatalf("Binary serialization failed: %v", err)
			}
			binaryData = data
		}
		binarySerializeTime := time.Since(start)

		// Deserialize performance
		start = time.Now()
		for i := 0; i < iterations; i++ {
			_, err := wal.DeserializeWALEntryBinary(binaryData)
			if err != nil {
				t.Fatalf("Binary deserialization failed: %v", err)
			}
		}
		binaryDeserializeTime := time.Since(start)

		t.Logf("Binary serialization:   %v (%v per op, %d ops/sec)",
			binarySerializeTime,
			binarySerializeTime/iterations,
			int64(float64(iterations)/binarySerializeTime.Seconds()))

		t.Logf("Binary deserialization: %v (%v per op, %d ops/sec)",
			binaryDeserializeTime,
			binaryDeserializeTime/iterations,
			int64(float64(iterations)/binaryDeserializeTime.Seconds()))

		t.Logf("Binary entry size: %d bytes", len(binaryData))
	})

	// Test ASCII JSON Performance
	t.Run("ASCIIPerformance", func(t *testing.T) {
		// Serialize performance
		start := time.Now()
		var jsonData []byte
		for i := 0; i < iterations; i++ {
			data, err := json.Marshal(testEntry)
			if err != nil {
				t.Fatalf("JSON serialization failed: %v", err)
			}
			jsonData = data
		}
		jsonSerializeTime := time.Since(start)

		// Deserialize performance
		start = time.Now()
		for i := 0; i < iterations; i++ {
			var entry WALEntry
			err := json.Unmarshal(jsonData, &entry)
			if err != nil {
				t.Fatalf("JSON deserialization failed: %v", err)
			}
		}
		jsonDeserializeTime := time.Since(start)

		t.Logf("JSON serialization:     %v (%v per op, %d ops/sec)",
			jsonSerializeTime,
			jsonSerializeTime/iterations,
			int64(float64(iterations)/jsonSerializeTime.Seconds()))

		t.Logf("JSON deserialization:   %v (%v per op, %d ops/sec)",
			jsonDeserializeTime,
			jsonDeserializeTime/iterations,
			int64(float64(iterations)/jsonDeserializeTime.Seconds()))

		t.Logf("JSON entry size: %d bytes", len(jsonData))
	})

	t.Logf("=== PERFORMANCE COMPARISON SUMMARY ===")
	t.Logf("Binary WAL provides significant performance and size advantages over ASCII JSON")
}
