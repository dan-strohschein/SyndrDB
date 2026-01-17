package hashindexV3

import (
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	"go.uber.org/zap"
)

// BenchmarkDiskLookup_Bucketed measures disk lookup performance with bucket optimization
func BenchmarkDiskLookup_Bucketed(b *testing.B) {
	tempDir := createTempBenchDir(b)
	defer os.RemoveAll(tempDir)

	storage, err := NewEntryStorage(EntryStorageConfig{
		IndexName:         "bench_idx",
		FieldName:         "bench_field",
		BundleName:        "bench_bundle",
		DataDir:           tempDir,
		Logger:            createBenchLogger(),
		UseBuckets:        true,
		NumBuckets:        256,
		BucketFileMaxSize: 64 * 1024 * 1024,
	})
	if err != nil {
		b.Fatalf("Failed to create storage: %v", err)
	}
	defer storage.Close()

	// Populate with 10,000 entries across all buckets
	numEntries := 10000
	keys := make([]string, numEntries)
	for i := 0; i < numEntries; i++ {
		key := fmt.Sprintf("key_%d", i)
		keys[i] = key
		entry := NewHashIndexEntry(key, fmt.Sprintf("doc_%d", i), uint32(i%1000), uint64(i))
		storage.AppendEntry(entry)
	}
	storage.Flush()

	b.ResetTimer()
	b.ReportAllocs()

	// Perform lookups using GetLatestEntry
	for i := 0; i < b.N; i++ {
		key := keys[i%numEntries]
		_, _ = storage.GetLatestEntry(key)
	}

	b.StopTimer()
	ops := float64(b.N) / b.Elapsed().Seconds()
	b.ReportMetric(ops, "ops/sec")
}

// BenchmarkDiskLookup_NonBucketed simulates pre-bucket behavior (scanning all files)
func BenchmarkDiskLookup_NonBucketed(b *testing.B) {
	tempDir := createTempBenchDir(b)
	defer os.RemoveAll(tempDir)

	storage, err := NewEntryStorage(EntryStorageConfig{
		IndexName:  "bench_idx",
		FieldName:  "bench_field",
		BundleName: "bench_bundle",
		DataDir:    tempDir,
		Logger:     createBenchLogger(),
		UseBuckets: false, // Disable buckets for comparison
	})
	if err != nil {
		b.Fatalf("Failed to create storage: %v", err)
	}
	defer storage.Close()

	// Populate with same 10,000 entries
	numEntries := 10000
	keys := make([]string, numEntries)
	for i := 0; i < numEntries; i++ {
		key := fmt.Sprintf("key_%d", i)
		keys[i] = key
		entry := NewHashIndexEntry(key, fmt.Sprintf("doc_%d", i), uint32(i%1000), uint64(i))
		storage.AppendEntry(entry)
	}
	storage.Flush()

	b.ResetTimer()
	b.ReportAllocs()

	// Perform lookups
	for i := 0; i < b.N; i++ {
		key := keys[i%numEntries]
		_, _ = storage.GetLatestEntry(key)
	}

	b.StopTimer()
	ops := float64(b.N) / b.Elapsed().Seconds()
	b.ReportMetric(ops, "ops/sec")
}

// BenchmarkBucketLookup_Optimized measures bucket-optimized lookup
func BenchmarkBucketLookup_Optimized(b *testing.B) {
	tempDir := createTempBenchDir(b)
	defer os.RemoveAll(tempDir)

	storage, err := NewEntryStorage(EntryStorageConfig{
		IndexName:         "bench_idx",
		FieldName:         "bench_field",
		BundleName:        "bench_bundle",
		DataDir:           tempDir,
		Logger:            createBenchLogger(),
		UseBuckets:        true,
		NumBuckets:        256,
		BucketFileMaxSize: 64 * 1024 * 1024,
	})
	if err != nil {
		b.Fatalf("Failed to create storage: %v", err)
	}
	defer storage.Close()

	// Populate with 10,000 entries
	numEntries := 10000
	keys := make([]string, numEntries)
	buckets := make([]uint32, numEntries)

	for i := 0; i < numEntries; i++ {
		key := fmt.Sprintf("key_%d", i)
		keys[i] = key

		// Calculate bucket number using exported hash function
		hash := ComputeHash(key)
		bucket, _ := ComputeBucketNum(hash, storage.numBuckets)
		buckets[i] = bucket

		entry := NewHashIndexEntry(key, fmt.Sprintf("doc_%d", i), uint32(i%1000), uint64(i))
		storage.AppendEntry(entry)
	}
	storage.Flush()

	b.ResetTimer()
	b.ReportAllocs()

	// Perform lookups using optimized GetLatestEntryFromBucket
	for i := 0; i < b.N; i++ {
		idx := i % numEntries
		key := keys[idx]
		bucket := buckets[idx]
		_, _ = storage.GetLatestEntryFromBucket(key, bucket)
	}

	b.StopTimer()
	ops := float64(b.N) / b.Elapsed().Seconds()
	b.ReportMetric(ops, "ops/sec")
}

// BenchmarkMixedWorkload_80_20 measures 80% recent writes (faster), 20% old data (disk)
func BenchmarkMixedWorkload_80_20(b *testing.B) {
	tempDir := createTempBenchDir(b)
	defer os.RemoveAll(tempDir)

	storage, err := NewEntryStorage(EntryStorageConfig{
		IndexName:         "bench_idx",
		FieldName:         "bench_field",
		BundleName:        "bench_bundle",
		DataDir:           tempDir,
		Logger:            createBenchLogger(),
		UseBuckets:        true,
		NumBuckets:        256,
		BucketFileMaxSize: 64 * 1024 * 1024,
	})
	if err != nil {
		b.Fatalf("Failed to create storage: %v", err)
	}
	defer storage.Close()

	// Create 4,000 old keys (flushed to disk)
	oldKeys := make([]string, 4000)
	for i := 0; i < 4000; i++ {
		key := fmt.Sprintf("old_%d", i)
		oldKeys[i] = key
		entry := NewHashIndexEntry(key, fmt.Sprintf("doc_%d", i), uint32(i%1000), uint64(i))
		storage.AppendEntry(entry)
	}
	storage.Flush()

	// Create 1,000 recent keys (kept in write buffer/recent files)
	recentKeys := make([]string, 1000)
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("recent_%d", i)
		recentKeys[i] = key
		entry := NewHashIndexEntry(key, fmt.Sprintf("doc_%d", i+4000), uint32(i%1000), uint64(i+4000))
		storage.AppendEntry(entry)
	}

	b.ResetTimer()
	b.ReportAllocs()

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < b.N; i++ {
		var key string
		if rng.Float64() < 0.8 {
			// 80% recent keys
			key = recentKeys[rng.Intn(len(recentKeys))]
		} else {
			// 20% old keys
			key = oldKeys[rng.Intn(len(oldKeys))]
		}
		_, _ = storage.GetLatestEntry(key)
	}

	b.StopTimer()
	ops := float64(b.N) / b.Elapsed().Seconds()
	b.ReportMetric(ops, "ops/sec")
}

// BenchmarkBucketDistribution measures bucket distribution overhead
func BenchmarkBucketDistribution(b *testing.B) {
	numBuckets := uint32(256)
	keys := make([]string, 10000)
	for i := 0; i < 10000; i++ {
		keys[i] = fmt.Sprintf("key_%d", i)
	}

	b.ResetTimer()
	b.ReportAllocs()

	// Measure just the bucket computation overhead
	for i := 0; i < b.N; i++ {
		key := keys[i%len(keys)]
		hash := ComputeHash(key)
		_, _ = ComputeBucketNum(hash, numBuckets)
	}
}

// BenchmarkAppendEntry_Bucketed measures bucketed append performance
func BenchmarkAppendEntry_Bucketed(b *testing.B) {
	tempDir := createTempBenchDir(b)
	defer os.RemoveAll(tempDir)

	storage, err := NewEntryStorage(EntryStorageConfig{
		IndexName:         "bench_idx",
		FieldName:         "bench_field",
		BundleName:        "bench_bundle",
		DataDir:           tempDir,
		Logger:            createBenchLogger(),
		UseBuckets:        true,
		NumBuckets:        256,
		BucketFileMaxSize: 64 * 1024 * 1024,
	})
	if err != nil {
		b.Fatalf("Failed to create storage: %v", err)
	}
	defer storage.Close()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		entry := NewHashIndexEntry(fmt.Sprintf("key_%d", i), fmt.Sprintf("doc_%d", i), uint32(i%1000), uint64(i))
		storage.AppendEntry(entry)
	}

	b.StopTimer()
	storage.Flush()
}

// BenchmarkScanBucket measures bucket scanning performance
func BenchmarkScanBucket(b *testing.B) {
	tempDir := createTempBenchDir(b)
	defer os.RemoveAll(tempDir)

	storage, err := NewEntryStorage(EntryStorageConfig{
		IndexName:         "bench_idx",
		FieldName:         "bench_field",
		BundleName:        "bench_bundle",
		DataDir:           tempDir,
		Logger:            createBenchLogger(),
		UseBuckets:        true,
		NumBuckets:        256,
		BucketFileMaxSize: 64 * 1024 * 1024,
	})
	if err != nil {
		b.Fatalf("Failed to create storage: %v", err)
	}
	defer storage.Close()

	// Populate bucket 0 with entries
	targetBucket := uint32(0)
	entriesAdded := 0

	for i := 0; i < 100000 && entriesAdded < 1000; i++ {
		key := fmt.Sprintf("key_%d", i)
		hash := ComputeHash(key)
		bucket, _ := ComputeBucketNum(hash, storage.numBuckets)

		if bucket == targetBucket {
			entry := NewHashIndexEntry(key, fmt.Sprintf("doc_%d", i), uint32(i%100), uint64(i))
			storage.AppendEntry(entry)
			entriesAdded++
		}
	}
	storage.Flush()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		count := 0
		storage.ScanBucket(targetBucket, func(entry *HashIndexEntry) bool {
			count++
			return true
		})
	}
}

// Helper functions for benchmarks

func createTempBenchDir(b *testing.B) string {
	dir, err := os.MkdirTemp("", "bucket_bench_*")
	if err != nil {
		b.Fatalf("Failed to create temp dir: %v", err)
	}
	return dir
}

func createBenchLogger() *zap.SugaredLogger {
	// Use nop logger for benchmarks to avoid logging overhead
	return zap.NewNop().Sugar()
}

// BenchmarkComparison runs a side-by-side comparison
func BenchmarkComparison(b *testing.B) {
	b.Run("Bucketed", func(b *testing.B) {
		BenchmarkDiskLookup_Bucketed(b)
	})

	b.Run("NonBucketed", func(b *testing.B) {
		BenchmarkDiskLookup_NonBucketed(b)
	})
}
