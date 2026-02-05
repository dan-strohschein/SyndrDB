package hashindexV3

import (
	"math"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"go.uber.org/zap"
)

// TestDeleteGlobalSequenceOverflow verifies that Delete returns an error when
// GlobalSequence would overflow (same behavior as Put).
func TestDeleteGlobalSequenceOverflow(t *testing.T) {
	dir := t.TempDir()
	logger := zap.NewNop().Sugar()
	config := IndexConfig{
		IndexName:        "overflow_test",
		BundleName:       "test",
		DatabaseName:    "testdb",
		FieldName:        "field",
		DataDir:          filepath.Join(dir, "indexes"),
		MemTableMaxSize:  1000,
		CompactionMaxFiles: 10,
		Logger:           logger,
	}
	if err := os.MkdirAll(config.DataDir, 0755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}

	idx, err := NewHashIndexV3(config)
	if err != nil {
		t.Fatalf("NewHashIndexV3: %v", err)
	}
	defer idx.Close()

	// Simulate near-overflow: set GlobalSequence to MaxUint64 so next increment would overflow
	atomic.StoreUint64(&idx.GlobalSequence, math.MaxUint64)

	_, err = idx.Delete("key", 0)
	if err == nil {
		t.Error("Delete expected to return error when GlobalSequence would overflow")
	}
}

// TestPutWithNilCompactorNoPanic verifies that many Puts with compactor nil do not
// spawn goroutines unnecessarily and do not panic (Issue 8).
func TestPutWithNilCompactorNoPanic(t *testing.T) {
	dir := t.TempDir()
	logger := zap.NewNop().Sugar()
	config := IndexConfig{
		IndexName:           "nil_compactor_test",
		BundleName:           "test",
		DatabaseName:         "testdb",
		FieldName:            "field",
		DataDir:              filepath.Join(dir, "indexes"),
		MemTableMaxSize:      10000,
		CompactionMaxFiles:   10,
		Logger:               logger,
	}
	if err := os.MkdirAll(config.DataDir, 0755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}

	idx, err := NewHashIndexV3(config)
	if err != nil {
		t.Fatalf("NewHashIndexV3: %v", err)
	}
	defer idx.Close()

	// compactor is nil by default; do many Puts (e.g. 2500 so we hit the "every 1000" path multiple times)
	for i := 0; i < 2500; i++ {
		err := idx.Put("key", "doc", uint32(i), uint64(i), uint64(i))
		if err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}
}
