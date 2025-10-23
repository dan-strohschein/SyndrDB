package main

import (
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// Test helpers

func createTestConfig(t *testing.T, name string) IndexConfig {
	tempDir := t.TempDir()

	logger, _ := zap.NewDevelopment()

	return IndexConfig{
		IndexName:          fmt.Sprintf("test_idx_%s", name),
		BundleName:         "test_bundle",
		DatabaseName:       "test_db",
		FieldName:          "test_field",
		DataDir:            tempDir,
		MaxFileSize:        1024 * 1024, // 1MB for testing
		WriteBufferSize:    4096,
		MemTableMaxSize:    1000,
		CompactionEnabled:  true,
		CompactionMaxFiles: 5,
		Logger:             logger.Sugar(),
	}
}

// Test: NewHashIndexV3 - Basic Creation

func TestNewHashIndexV3_Success(t *testing.T) {
	config := createTestConfig(t, "new_success")

	idx, err := NewHashIndexV3(config)
	require.NoError(t, err)
	require.NotNil(t, idx)
	defer idx.Close()

	assert.Equal(t, config.IndexName, idx.config.IndexName)
	assert.Equal(t, config.BundleName, idx.config.BundleName)
	assert.True(t, idx.isOpen)
	assert.False(t, idx.closed)
}

func TestNewHashIndexV3_InvalidConfig(t *testing.T) {
	tests := []struct {
		name         string
		modifyConfig func(*IndexConfig)
		expectedErr  string
	}{
		{
			name: "empty index name",
			modifyConfig: func(c *IndexConfig) {
				c.IndexName = ""
			},
			expectedErr: "index name cannot be empty",
		},
		{
			name: "empty bundle name",
			modifyConfig: func(c *IndexConfig) {
				c.BundleName = ""
			},
			expectedErr: "bundle name cannot be empty",
		},
		{
			name: "empty data directory",
			modifyConfig: func(c *IndexConfig) {
				c.DataDir = ""
			},
			expectedErr: "data directory cannot be empty",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := createTestConfig(t, tt.name)
			tt.modifyConfig(&config)

			idx, err := NewHashIndexV3(config)
			assert.Error(t, err)
			assert.Nil(t, idx)
			assert.Contains(t, err.Error(), tt.expectedErr)
		})
	}
}

// Test: Put Operation

func TestPut_Success(t *testing.T) {
	config := createTestConfig(t, "put_success")
	idx, err := NewHashIndexV3(config)
	require.NoError(t, err)
	defer idx.Close()

	err = idx.Put("testKey", "doc123")
	assert.NoError(t, err)

	// Verify entry is in MemTable
	entry, found := idx.memTable.Get("testKey")
	assert.True(t, found)
	assert.Equal(t, "testKey", entry.KeyValue)
	assert.Equal(t, "doc123", entry.DocumentID)
	assert.False(t, entry.Deleted)
}

func TestPut_MultipleEntries(t *testing.T) {
	config := createTestConfig(t, "put_multiple")
	idx, err := NewHashIndexV3(config)
	require.NoError(t, err)
	defer idx.Close()

	// Insert multiple entries
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key%d", i)
		docID := fmt.Sprintf("doc%d", i)
		err := idx.Put(key, docID)
		require.NoError(t, err)
	}

	// Verify all entries
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key%d", i)
		docID := fmt.Sprintf("doc%d", i)

		results, err := idx.Get(key)
		require.NoError(t, err)
		require.Len(t, results, 1)
		assert.Equal(t, docID, results[0])
	}
}

func TestPut_UpdateExisting(t *testing.T) {
	config := createTestConfig(t, "put_update")
	idx, err := NewHashIndexV3(config)
	require.NoError(t, err)
	defer idx.Close()

	// Insert initial value
	err = idx.Put("key1", "doc1")
	require.NoError(t, err)

	// Update with new document ID
	err = idx.Put("key1", "doc2")
	require.NoError(t, err)

	// Verify latest value wins (LSM semantics)
	results, err := idx.Get("key1")
	require.NoError(t, err)
	require.Len(t, results, 1)
	assert.Equal(t, "doc2", results[0])
}

func TestPut_InvalidInput(t *testing.T) {
	config := createTestConfig(t, "put_invalid")
	idx, err := NewHashIndexV3(config)
	require.NoError(t, err)
	defer idx.Close()

	// Empty key
	err = idx.Put("", "doc1")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "key value cannot be empty")

	// Empty document ID
	err = idx.Put("key1", "")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "document ID cannot be empty")
}

func TestPut_AfterClose(t *testing.T) {
	config := createTestConfig(t, "put_after_close")
	idx, err := NewHashIndexV3(config)
	require.NoError(t, err)

	idx.Close()

	err = idx.Put("key1", "doc1")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "index is closed")
}

// Test: Get Operation

func TestGet_Success(t *testing.T) {
	config := createTestConfig(t, "get_success")
	idx, err := NewHashIndexV3(config)
	require.NoError(t, err)
	defer idx.Close()

	// Insert entry
	err = idx.Put("key1", "doc1")
	require.NoError(t, err)

	// Retrieve entry
	results, err := idx.Get("key1")
	require.NoError(t, err)
	require.Len(t, results, 1)
	assert.Equal(t, "doc1", results[0])
}

func TestGet_NotFound(t *testing.T) {
	config := createTestConfig(t, "get_not_found")
	idx, err := NewHashIndexV3(config)
	require.NoError(t, err)
	defer idx.Close()

	// Try to get non-existent key
	results, err := idx.Get("nonexistent")
	require.NoError(t, err)
	assert.Empty(t, results)
}

func TestGet_CacheHit(t *testing.T) {
	config := createTestConfig(t, "get_cache_hit")
	idx, err := NewHashIndexV3(config)
	require.NoError(t, err)
	defer idx.Close()

	// Insert entry
	err = idx.Put("key1", "doc1")
	require.NoError(t, err)

	// First get - should be cache hit
	stats1 := idx.GetStats()
	results, err := idx.Get("key1")
	require.NoError(t, err)
	assert.Len(t, results, 1)

	stats2 := idx.GetStats()
	assert.Equal(t, stats1.CacheHits+1, stats2.CacheHits)
}

func TestGet_CacheMissFromDisk(t *testing.T) {
	config := createTestConfig(t, "get_cache_miss")
	idx, err := NewHashIndexV3(config)
	require.NoError(t, err)

	// Insert and flush
	err = idx.Put("key1", "doc1")
	require.NoError(t, err)
	err = idx.Flush()
	require.NoError(t, err)

	// Close and reopen (clears MemTable)
	err = idx.Close()
	require.NoError(t, err)

	idx, err = OpenHashIndexV3(config)
	require.NoError(t, err)
	defer idx.Close()

	// Clear MemTable to force disk read
	idx.memTable.Clear()

	// Get should read from disk
	stats1 := idx.GetStats()
	results, err := idx.Get("key1")
	require.NoError(t, err)
	require.Len(t, results, 1)
	assert.Equal(t, "doc1", results[0])

	stats2 := idx.GetStats()
	assert.Equal(t, stats1.CacheMisses+1, stats2.CacheMisses)
	assert.Equal(t, stats1.DiskReads+1, stats2.DiskReads)
}

func TestGet_LSMLatestWins(t *testing.T) {
	config := createTestConfig(t, "get_lsm_latest")
	idx, err := NewHashIndexV3(config)
	require.NoError(t, err)
	defer idx.Close()

	// Insert multiple versions
	err = idx.Put("key1", "doc1")
	require.NoError(t, err)

	time.Sleep(1 * time.Millisecond) // Ensure different timestamps

	err = idx.Put("key1", "doc2")
	require.NoError(t, err)

	time.Sleep(1 * time.Millisecond)

	err = idx.Put("key1", "doc3")
	require.NoError(t, err)

	// Get should return latest
	results, err := idx.Get("key1")
	require.NoError(t, err)
	require.Len(t, results, 1)
	assert.Equal(t, "doc3", results[0])
}

func TestGet_InvalidInput(t *testing.T) {
	config := createTestConfig(t, "get_invalid")
	idx, err := NewHashIndexV3(config)
	require.NoError(t, err)
	defer idx.Close()

	results, err := idx.Get("")
	assert.Error(t, err)
	assert.Nil(t, results)
	assert.Contains(t, err.Error(), "key value cannot be empty")
}

// Test: Delete Operation

func TestDelete_Success(t *testing.T) {
	config := createTestConfig(t, "delete_success")
	idx, err := NewHashIndexV3(config)
	require.NoError(t, err)
	defer idx.Close()

	// Insert entry
	err = idx.Put("key1", "doc1")
	require.NoError(t, err)

	// Delete entry
	deleted, err := idx.Delete("key1")
	require.NoError(t, err)
	assert.True(t, deleted)

	// Verify entry is gone
	results, err := idx.Get("key1")
	require.NoError(t, err)
	assert.Empty(t, results)
}

func TestDelete_NotFound(t *testing.T) {
	config := createTestConfig(t, "delete_not_found")
	idx, err := NewHashIndexV3(config)
	require.NoError(t, err)
	defer idx.Close()

	deleted, err := idx.Delete("nonexistent")
	require.NoError(t, err)
	assert.False(t, deleted)
}

func TestDelete_TombstoneCreated(t *testing.T) {
	config := createTestConfig(t, "delete_tombstone")
	idx, err := NewHashIndexV3(config)
	require.NoError(t, err)
	defer idx.Close()

	// Insert and delete
	err = idx.Put("key1", "doc1")
	require.NoError(t, err)

	stats1 := idx.GetStats()

	deleted, err := idx.Delete("key1")
	require.NoError(t, err)
	assert.True(t, deleted)

	// Verify tombstone count increased
	stats2 := idx.GetStats()
	assert.Equal(t, stats1.TombstoneCount+1, stats2.TombstoneCount)
}

func TestDelete_InvalidInput(t *testing.T) {
	config := createTestConfig(t, "delete_invalid")
	idx, err := NewHashIndexV3(config)
	require.NoError(t, err)
	defer idx.Close()

	deleted, err := idx.Delete("")
	assert.Error(t, err)
	assert.False(t, deleted)
	assert.Contains(t, err.Error(), "key value cannot be empty")
}

// Test: Search (Alias for Get)

func TestSearch_Alias(t *testing.T) {
	config := createTestConfig(t, "search_alias")
	idx, err := NewHashIndexV3(config)
	require.NoError(t, err)
	defer idx.Close()

	err = idx.Put("key1", "doc1")
	require.NoError(t, err)

	// Search should work same as Get
	results, err := idx.Search("key1")
	require.NoError(t, err)
	require.Len(t, results, 1)
	assert.Equal(t, "doc1", results[0])
}

// Test: Open Existing Index

func TestOpenHashIndexV3_Success(t *testing.T) {
	config := createTestConfig(t, "open_success")

	// Create and populate index
	idx1, err := NewHashIndexV3(config)
	require.NoError(t, err)

	for i := 0; i < 10; i++ {
		err := idx1.Put(fmt.Sprintf("key%d", i), fmt.Sprintf("doc%d", i))
		require.NoError(t, err)
	}

	err = idx1.Flush()
	require.NoError(t, err)
	err = idx1.Close()
	require.NoError(t, err)

	// Reopen index
	idx2, err := OpenHashIndexV3(config)
	require.NoError(t, err)
	defer idx2.Close()

	// Verify data is accessible
	results, err := idx2.Get("key5")
	require.NoError(t, err)
	require.Len(t, results, 1)
	assert.Equal(t, "doc5", results[0])
}

// Test: Concurrent Operations

func TestConcurrentPuts(t *testing.T) {
	config := createTestConfig(t, "concurrent_puts")
	idx, err := NewHashIndexV3(config)
	require.NoError(t, err)
	defer idx.Close()

	numGoroutines := 10
	entriesPerGoroutine := 100

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	// Concurrent puts
	for g := 0; g < numGoroutines; g++ {
		go func(goroutineID int) {
			defer wg.Done()
			for i := 0; i < entriesPerGoroutine; i++ {
				key := fmt.Sprintf("g%d_key%d", goroutineID, i)
				docID := fmt.Sprintf("g%d_doc%d", goroutineID, i)
				err := idx.Put(key, docID)
				assert.NoError(t, err)
			}
		}(g)
	}

	wg.Wait()

	// Verify all entries
	stats := idx.GetStats()
	assert.Equal(t, uint64(numGoroutines*entriesPerGoroutine), stats.TotalPuts)
}

func TestConcurrentGets(t *testing.T) {
	config := createTestConfig(t, "concurrent_gets")
	idx, err := NewHashIndexV3(config)
	require.NoError(t, err)
	defer idx.Close()

	// Insert test data
	for i := 0; i < 100; i++ {
		err := idx.Put(fmt.Sprintf("key%d", i), fmt.Sprintf("doc%d", i))
		require.NoError(t, err)
	}

	numGoroutines := 20
	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	// Concurrent gets
	for g := 0; g < numGoroutines; g++ {
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				key := fmt.Sprintf("key%d", i)
				results, err := idx.Get(key)
				assert.NoError(t, err)
				assert.Len(t, results, 1)
			}
		}()
	}

	wg.Wait()
}

func TestConcurrentMixedOperations(t *testing.T) {
	config := createTestConfig(t, "concurrent_mixed")
	idx, err := NewHashIndexV3(config)
	require.NoError(t, err)
	defer idx.Close()

	numGoroutines := 15
	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	// Concurrent mixed operations
	for g := 0; g < numGoroutines; g++ {
		go func(goroutineID int) {
			defer wg.Done()

			for i := 0; i < 50; i++ {
				key := fmt.Sprintf("key%d", i)
				docID := fmt.Sprintf("g%d_doc%d", goroutineID, i)

				// Put
				err := idx.Put(key, docID)
				assert.NoError(t, err)

				// Get (might return empty if another goroutine deleted it)
				_, err = idx.Get(key)
				assert.NoError(t, err)

				// Delete some entries
				if i%5 == 0 {
					_, err := idx.Delete(key)
					assert.NoError(t, err)
				}
			}
		}(g)
	}

	wg.Wait()
}

// Test: Statistics

func TestGetStats_Tracking(t *testing.T) {
	config := createTestConfig(t, "stats_tracking")
	idx, err := NewHashIndexV3(config)
	require.NoError(t, err)
	defer idx.Close()

	initialStats := idx.GetStats()

	// Perform operations
	err = idx.Put("key1", "doc1")
	require.NoError(t, err)

	err = idx.Put("key2", "doc2")
	require.NoError(t, err)

	results, err := idx.Get("key1")
	require.NoError(t, err)
	require.Len(t, results, 1)

	deleted, err := idx.Delete("key2")
	require.NoError(t, err)
	require.True(t, deleted)

	finalStats := idx.GetStats()

	assert.Equal(t, initialStats.TotalPuts+2, finalStats.TotalPuts)
	assert.Equal(t, initialStats.TotalGets+2, finalStats.TotalGets) // Get is also called during Delete
	assert.Equal(t, initialStats.TotalDeletes+1, finalStats.TotalDeletes)
	assert.Equal(t, initialStats.TombstoneCount+1, finalStats.TombstoneCount)
}

func TestGetMemTableStats(t *testing.T) {
	config := createTestConfig(t, "memtable_stats")
	idx, err := NewHashIndexV3(config)
	require.NoError(t, err)
	defer idx.Close()

	// Insert entries
	for i := 0; i < 50; i++ {
		err := idx.Put(fmt.Sprintf("key%d", i), fmt.Sprintf("doc%d", i))
		require.NoError(t, err)
	}

	memStats := idx.GetMemTableStats()
	assert.Equal(t, 50, memStats.Size)
	assert.Equal(t, config.MemTableMaxSize, memStats.MaxSize)
	assert.Greater(t, memStats.MemoryUsage, int64(0))
}

// Test: Flush and Close

func TestFlush_Success(t *testing.T) {
	config := createTestConfig(t, "flush_success")
	idx, err := NewHashIndexV3(config)
	require.NoError(t, err)
	defer idx.Close()

	// Insert entries
	for i := 0; i < 10; i++ {
		err := idx.Put(fmt.Sprintf("key%d", i), fmt.Sprintf("doc%d", i))
		require.NoError(t, err)
	}

	// Flush
	err = idx.Flush()
	assert.NoError(t, err)
}

func TestClose_Success(t *testing.T) {
	config := createTestConfig(t, "close_success")
	idx, err := NewHashIndexV3(config)
	require.NoError(t, err)

	err = idx.Put("key1", "doc1")
	require.NoError(t, err)

	err = idx.Close()
	assert.NoError(t, err)
	assert.True(t, idx.closed)
	assert.False(t, idx.isOpen)
}

func TestClose_Idempotent(t *testing.T) {
	config := createTestConfig(t, "close_idempotent")
	idx, err := NewHashIndexV3(config)
	require.NoError(t, err)

	err = idx.Close()
	assert.NoError(t, err)

	// Close again should not error
	err = idx.Close()
	assert.NoError(t, err)
}

// Test: Sequence Numbers

func TestSequenceNumbers_Monotonic(t *testing.T) {
	config := createTestConfig(t, "sequence_monotonic")
	idx, err := NewHashIndexV3(config)
	require.NoError(t, err)
	defer idx.Close()

	// Insert multiple entries
	var sequences []uint64
	for i := 0; i < 10; i++ {
		err := idx.Put(fmt.Sprintf("key%d", i), fmt.Sprintf("doc%d", i))
		require.NoError(t, err)

		entry, found := idx.memTable.Get(fmt.Sprintf("key%d", i))
		require.True(t, found)
		sequences = append(sequences, entry.Sequence)
	}

	// Verify sequences are monotonically increasing
	for i := 1; i < len(sequences); i++ {
		assert.Greater(t, sequences[i], sequences[i-1])
	}
}

// Test: File Path Generation

func TestGetIndexFilePath(t *testing.T) {
	config := IndexConfig{
		BundleName: "test_bundle",
		FieldName:  "test_field",
		DataDir:    "/path/to/data",
	}

	expected := filepath.Join("/path/to/data", "test_bundle_test_field.hidx")
	actual := config.GetIndexFilePath()

	assert.Equal(t, expected, actual)
}

// Benchmark Tests

func BenchmarkPut(b *testing.B) {
	tempDir := b.TempDir()
	logger, _ := zap.NewDevelopment()

	config := IndexConfig{
		IndexName:       "bench_put",
		BundleName:      "bench_bundle",
		DatabaseName:    "bench_db",
		FieldName:       "bench_field",
		DataDir:         tempDir,
		MaxFileSize:     128 * 1024 * 1024, // 128MB
		WriteBufferSize: 64 * 1024,         // 64KB
		MemTableMaxSize: 100000,
		Logger:          logger.Sugar(),
	}

	idx, err := NewHashIndexV3(config)
	require.NoError(b, err)
	defer idx.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key%d", i)
		docID := fmt.Sprintf("doc%d", i)
		idx.Put(key, docID)
	}
}

func BenchmarkGet_CacheHit(b *testing.B) {
	tempDir := b.TempDir()
	logger, _ := zap.NewDevelopment()

	config := IndexConfig{
		IndexName:       "bench_get_hit",
		BundleName:      "bench_bundle",
		DatabaseName:    "bench_db",
		FieldName:       "bench_field",
		DataDir:         tempDir,
		MemTableMaxSize: 100000,
		Logger:          logger.Sugar(),
	}

	idx, err := NewHashIndexV3(config)
	require.NoError(b, err)
	defer idx.Close()

	// Pre-populate
	for i := 0; i < 1000; i++ {
		idx.Put(fmt.Sprintf("key%d", i), fmt.Sprintf("doc%d", i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key%d", i%1000)
		idx.Get(key)
	}
}

func BenchmarkDelete(b *testing.B) {
	tempDir := b.TempDir()
	logger, _ := zap.NewDevelopment()

	config := IndexConfig{
		IndexName:       "bench_delete",
		BundleName:      "bench_bundle",
		DatabaseName:    "bench_db",
		FieldName:       "bench_field",
		DataDir:         tempDir,
		MemTableMaxSize: 100000,
		Logger:          logger.Sugar(),
	}

	idx, err := NewHashIndexV3(config)
	require.NoError(b, err)
	defer idx.Close()

	// Pre-populate
	for i := 0; i < b.N; i++ {
		idx.Put(fmt.Sprintf("key%d", i), fmt.Sprintf("doc%d", i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key%d", i)
		idx.Delete(key)
	}
}

func BenchmarkConcurrentPuts(b *testing.B) {
	tempDir := b.TempDir()
	logger, _ := zap.NewDevelopment()

	config := IndexConfig{
		IndexName:       "bench_concurrent",
		BundleName:      "bench_bundle",
		DatabaseName:    "bench_db",
		FieldName:       "bench_field",
		DataDir:         tempDir,
		MaxFileSize:     128 * 1024 * 1024,
		MemTableMaxSize: 100000,
		Logger:          logger.Sugar(),
	}

	idx, err := NewHashIndexV3(config)
	require.NoError(b, err)
	defer idx.Close()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("key%d", i)
			docID := fmt.Sprintf("doc%d", i)
			idx.Put(key, docID)
			i++
		}
	})
}
