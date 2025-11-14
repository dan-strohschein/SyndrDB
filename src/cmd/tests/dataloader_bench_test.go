package main

// This file contains benchmark tests for DataLoader performance analysis.
//
// Benchmarks compare:
// - Sequential loading vs batched loading
// - Cache hit rates
// - Concurrent access performance
// - Memory usage
//
// Run with: go test -bench=. -benchmem ./src/internal/graphQL/dataloader/

import (
	"context"
	"fmt"
	"testing"
	"time"

	"syndrdb/src/internal/graphQL/dataloader"
)

// Import dataloader types
type (
	DataLoader       = dataloader.DataLoader
	DataLoaderConfig = dataloader.DataLoaderConfig
)

var NewDataLoader = dataloader.NewDataLoader

// BenchmarkDataLoader_Sequential measures performance of sequential loads
func BenchmarkDataLoader_Sequential(b *testing.B) {
	batchLoadFunc := func(ctx context.Context, keys []string) (map[string]interface{}, error) {
		// Simulate database query (1ms per key)
		time.Sleep(time.Millisecond * time.Duration(len(keys)))
		results := make(map[string]interface{})
		for _, key := range keys {
			results[key] = fmt.Sprintf("value-%s", key)
		}
		return results, nil
	}

	loader := NewDataLoader(batchLoadFunc, nil)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = loader.Load(context.Background(), fmt.Sprintf("key-%d", i%100))
	}
}

// BenchmarkDataLoader_Batched measures performance with batching
func BenchmarkDataLoader_Batched(b *testing.B) {
	batchCount := 0
	batchLoadFunc := func(ctx context.Context, keys []string) (map[string]interface{}, error) {
		batchCount++
		// Simulate single batch query (1ms per batch, not per key)
		time.Sleep(time.Millisecond)
		results := make(map[string]interface{})
		for _, key := range keys {
			results[key] = fmt.Sprintf("value-%s", key)
		}
		return results, nil
	}

	loader := NewDataLoader(batchLoadFunc, nil)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		go func(idx int) {
			_, _ = loader.Load(context.Background(), fmt.Sprintf("key-%d", idx%100))
		}(i)
	}

	time.Sleep(100 * time.Millisecond) // Allow batches to complete
}

// BenchmarkDataLoader_CacheHits measures cache hit performance
func BenchmarkDataLoader_CacheHits(b *testing.B) {
	loadCount := 0
	batchLoadFunc := func(ctx context.Context, keys []string) (map[string]interface{}, error) {
		loadCount++
		results := make(map[string]interface{})
		for _, key := range keys {
			results[key] = fmt.Sprintf("value-%s", key)
		}
		return results, nil
	}

	loader := NewDataLoader(batchLoadFunc, nil)

	// Prime cache with 100 items
	for i := 0; i < 100; i++ {
		loader.Prime(fmt.Sprintf("key-%d", i), fmt.Sprintf("value-key-%d", i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = loader.Load(context.Background(), fmt.Sprintf("key-%d", i%100))
	}
}

// BenchmarkDataLoader_LoadMany measures LoadMany performance
func BenchmarkDataLoader_LoadMany(b *testing.B) {
	batchLoadFunc := func(ctx context.Context, keys []string) (map[string]interface{}, error) {
		time.Sleep(time.Millisecond)
		results := make(map[string]interface{})
		for _, key := range keys {
			results[key] = fmt.Sprintf("value-%s", key)
		}
		return results, nil
	}

	loader := NewDataLoader(batchLoadFunc, nil)

	keys := make([]string, 50)
	for i := 0; i < 50; i++ {
		keys[i] = fmt.Sprintf("key-%d", i)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = loader.LoadMany(context.Background(), keys)
	}
}

// BenchmarkDataLoader_ConcurrentAccess measures concurrent load performance
func BenchmarkDataLoader_ConcurrentAccess(b *testing.B) {
	batchLoadFunc := func(ctx context.Context, keys []string) (map[string]interface{}, error) {
		time.Sleep(time.Millisecond)
		results := make(map[string]interface{})
		for _, key := range keys {
			results[key] = fmt.Sprintf("value-%s", key)
		}
		return results, nil
	}

	loader := NewDataLoader(batchLoadFunc, &DataLoaderConfig{
		BatchWindow:  10 * time.Millisecond,
		MaxBatchSize: 1000,
		EnableCache:  true,
	})

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			_, _ = loader.Load(context.Background(), fmt.Sprintf("key-%d", i%100))
			i++
		}
	})
}

// BenchmarkComparison_WithoutDataLoader simulates N+1 without batching
func BenchmarkComparison_WithoutDataLoader(b *testing.B) {
	// Simulate individual database queries
	queryFunc := func(key string) string {
		time.Sleep(time.Millisecond) // 1ms per query
		return fmt.Sprintf("value-%s", key)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = queryFunc(fmt.Sprintf("key-%d", i%100))
	}
}

// BenchmarkComparison_WithDataLoader simulates the same with DataLoader
func BenchmarkComparison_WithDataLoader(b *testing.B) {
	batchLoadFunc := func(ctx context.Context, keys []string) (map[string]interface{}, error) {
		// Single batch query
		time.Sleep(time.Millisecond)
		results := make(map[string]interface{})
		for _, key := range keys {
			results[key] = fmt.Sprintf("value-%s", key)
		}
		return results, nil
	}

	loader := NewDataLoader(batchLoadFunc, nil)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = loader.Load(context.Background(), fmt.Sprintf("key-%d", i%100))
	}
}

// BenchmarkMemory_DataLoader measures memory usage
func BenchmarkMemory_DataLoader(b *testing.B) {
	batchLoadFunc := func(ctx context.Context, keys []string) (map[string]interface{}, error) {
		results := make(map[string]interface{})
		for _, key := range keys {
			// Simulate document data (1KB per document)
			data := make(map[string]interface{})
			data["id"] = key
			data["data"] = make([]byte, 1024)
			results[key] = data
		}
		return results, nil
	}

	loader := NewDataLoader(batchLoadFunc, nil)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, _ = loader.Load(context.Background(), fmt.Sprintf("key-%d", i))
	}
}

// BenchmarkBatchSize_10 measures performance with small batches
func BenchmarkBatchSize_10(b *testing.B) {
	runBatchSizeBenchmark(b, 10)
}

// BenchmarkBatchSize_100 measures performance with medium batches
func BenchmarkBatchSize_100(b *testing.B) {
	runBatchSizeBenchmark(b, 100)
}

// BenchmarkBatchSize_1000 measures performance with large batches
func BenchmarkBatchSize_1000(b *testing.B) {
	runBatchSizeBenchmark(b, 1000)
}

func runBatchSizeBenchmark(b *testing.B, batchSize int) {
	batchLoadFunc := func(ctx context.Context, keys []string) (map[string]interface{}, error) {
		time.Sleep(time.Millisecond)
		results := make(map[string]interface{})
		for _, key := range keys {
			results[key] = fmt.Sprintf("value-%s", key)
		}
		return results, nil
	}

	loader := NewDataLoader(batchLoadFunc, &DataLoaderConfig{
		BatchWindow:  10 * time.Millisecond,
		MaxBatchSize: batchSize,
		EnableCache:  true,
	})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		go func(idx int) {
			_, _ = loader.Load(context.Background(), fmt.Sprintf("key-%d", idx))
		}(i)
	}

	time.Sleep(100 * time.Millisecond) // Allow batches to complete
}

// Example output interpretation:
//
// BenchmarkDataLoader_Sequential-8         1000    1234567 ns/op    100 B/op    5 allocs/op
// BenchmarkDataLoader_Batched-8           10000     123456 ns/op    100 B/op    5 allocs/op
//
// This shows:
// - Batched is ~10x faster (123456 ns vs 1234567 ns)
// - Same memory usage (100 B/op)
// - Same allocations (5 allocs/op)
// - Therefore: Batching provides 10x performance improvement with no memory overhead
