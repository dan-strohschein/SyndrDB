package main

import (
	"os"
	"path/filepath"
	"syndrdb/src/pkg/common"
	"testing"
)

// TestFdatasyncBasic validates basic fdatasync functionality
func TestFdatasyncBasic(t *testing.T) {
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "test.dat")

	file, err := os.Create(filePath)
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}
	defer file.Close()

	// Write some data
	data := []byte("test data")
	if _, err := file.Write(data); err != nil {
		t.Fatalf("Failed to write: %v", err)
	}

	// Test fdatasync
	if err := common.Fdatasync(file); err != nil {
		t.Errorf("Fdatasync failed: %v", err)
	}

	t.Log("✓ Fdatasync works correctly")
}

// BenchmarkFdatasync benchmarks fdatasync performance
func BenchmarkFdatasync(b *testing.B) {
	tmpDir := b.TempDir()
	filePath := filepath.Join(tmpDir, "bench.dat")

	file, err := os.Create(filePath)
	if err != nil {
		b.Fatalf("Failed to create file: %v", err)
	}
	defer file.Close()

	data := make([]byte, 4096)
	if _, err := file.Write(data); err != nil {
		b.Fatalf("Failed to write: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := common.Fdatasync(file); err != nil {
			b.Errorf("Fdatasync failed: %v", err)
		}
	}
}

// BenchmarkStandardSync benchmarks standard Sync performance
func BenchmarkStandardSync(b *testing.B) {
	tmpDir := b.TempDir()
	filePath := filepath.Join(tmpDir, "bench.dat")

	file, err := os.Create(filePath)
	if err != nil {
		b.Fatalf("Failed to create file: %v", err)
	}
	defer file.Close()

	data := make([]byte, 4096)
	if _, err := file.Write(data); err != nil {
		b.Fatalf("Failed to write: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := file.Sync(); err != nil {
			b.Errorf("Sync failed: %v", err)
		}
	}
}
