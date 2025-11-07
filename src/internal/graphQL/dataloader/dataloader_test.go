// Package dataloader implements a generic batching and caching loader for SyndrDB
//
// This file contains unit tests for the DataLoader implementation to ensure
// correct batching, caching, and thread-safe operation.

package dataloader

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

// TestDataLoader_BasicLoad tests basic Load functionality
func TestDataLoader_BasicLoad(t *testing.T) {
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

	value, err := loader.Load(context.Background(), "key1")
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	if value != "value-key1" {
		t.Errorf("Expected 'value-key1', got '%v'", value)
	}

	if loadCount != 1 {
		t.Errorf("Expected 1 batch load, got %d", loadCount)
	}
}

// TestDataLoader_Batching tests that multiple loads are batched
func TestDataLoader_Batching(t *testing.T) {
	loadCount := 0
	var loadedKeys []string
	var mu sync.Mutex

	batchLoadFunc := func(ctx context.Context, keys []string) (map[string]interface{}, error) {
		mu.Lock()
		loadCount++
		loadedKeys = append(loadedKeys, keys...)
		mu.Unlock()

		results := make(map[string]interface{})
		for _, key := range keys {
			results[key] = fmt.Sprintf("value-%s", key)
		}
		return results, nil
	}

	loader := NewDataLoader(batchLoadFunc, &DataLoaderConfig{
		BatchWindow:  50 * time.Millisecond,
		MaxBatchSize: 1000,
		EnableCache:  true,
	})

	// Load multiple keys concurrently
	var wg sync.WaitGroup
	keys := []string{"key1", "key2", "key3", "key4", "key5"}

	for _, key := range keys {
		wg.Add(1)
		go func(k string) {
			defer wg.Done()
			value, err := loader.Load(context.Background(), k)
			if err != nil {
				t.Errorf("Load failed for %s: %v", k, err)
			}
			expected := fmt.Sprintf("value-%s", k)
			if value != expected {
				t.Errorf("Expected '%s', got '%v'", expected, value)
			}
		}(key)
	}

	wg.Wait()

	// Should have batched all loads into 1 batch
	if loadCount != 1 {
		t.Errorf("Expected 1 batch load, got %d", loadCount)
	}

	if len(loadedKeys) != 5 {
		t.Errorf("Expected 5 keys loaded, got %d", len(loadedKeys))
	}
}

// TestDataLoader_Caching tests that values are cached
func TestDataLoader_Caching(t *testing.T) {
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

	// First load - should trigger batch
	value1, err := loader.Load(context.Background(), "key1")
	if err != nil {
		t.Fatalf("First load failed: %v", err)
	}

	// Wait for batch to complete
	time.Sleep(20 * time.Millisecond)

	// Second load - should return from cache
	value2, err := loader.Load(context.Background(), "key1")
	if err != nil {
		t.Fatalf("Second load failed: %v", err)
	}

	if value1 != value2 {
		t.Errorf("Cached value mismatch: %v != %v", value1, value2)
	}

	// Should only have loaded once
	if loadCount != 1 {
		t.Errorf("Expected 1 batch load (cached second time), got %d", loadCount)
	}
}

// TestDataLoader_Prime tests the Prime functionality
func TestDataLoader_Prime(t *testing.T) {
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

	// Prime the loader with a value
	loader.Prime("key1", "primed-value")

	// Load should return primed value without triggering batch
	value, err := loader.Load(context.Background(), "key1")
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	if value != "primed-value" {
		t.Errorf("Expected 'primed-value', got '%v'", value)
	}

	// Should not have triggered any batch loads
	if loadCount != 0 {
		t.Errorf("Expected 0 batch loads (primed), got %d", loadCount)
	}
}

// TestDataLoader_LoadMany tests LoadMany functionality
func TestDataLoader_LoadMany(t *testing.T) {
	loadCount := 0

	batchLoadFunc := func(ctx context.Context, keys []string) (map[string]interface{}, error) {
		loadCount++
		results := make(map[string]interface{})
		for _, key := range keys {
			results[key] = fmt.Sprintf("value-%s", key)
		}
		return results, nil
	}

	loader := NewDataLoader(batchLoadFunc, &DataLoaderConfig{
		BatchWindow:  50 * time.Millisecond,
		MaxBatchSize: 1000,
	})

	keys := []string{"key1", "key2", "key3"}
	results, err := loader.LoadMany(context.Background(), keys)
	if err != nil {
		t.Fatalf("LoadMany failed: %v", err)
	}

	if len(results) != 3 {
		t.Errorf("Expected 3 results, got %d", len(results))
	}

	for _, key := range keys {
		expected := fmt.Sprintf("value-%s", key)
		if results[key] != expected {
			t.Errorf("Expected '%s', got '%v'", expected, results[key])
		}
	}
}

// TestDataLoader_MaxBatchSize tests that batches are limited by MaxBatchSize
func TestDataLoader_MaxBatchSize(t *testing.T) {
	loadCount := 0
	maxBatchSizeSeen := 0
	var mu sync.Mutex

	batchLoadFunc := func(ctx context.Context, keys []string) (map[string]interface{}, error) {
		mu.Lock()
		loadCount++
		batchSize := len(keys)
		if batchSize > maxBatchSizeSeen {
			maxBatchSizeSeen = batchSize
		}
		mu.Unlock()

		results := make(map[string]interface{})
		for _, key := range keys {
			results[key] = fmt.Sprintf("value-%s", key)
		}
		return results, nil
	}

	loader := NewDataLoader(batchLoadFunc, &DataLoaderConfig{
		BatchWindow:  100 * time.Millisecond,
		MaxBatchSize: 10, // Small batch size to test splitting
		EnableCache:  true,
	})

	// Load 25 keys - should split into 3 batches (10, 10, 5)
	var wg sync.WaitGroup
	for i := 0; i < 25; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			key := fmt.Sprintf("key%d", idx)
			_, err := loader.Load(context.Background(), key)
			if err != nil {
				t.Errorf("Load failed for %s: %v", key, err)
			}
		}(i)
	}

	wg.Wait()

	// Check that max batch size was respected
	if maxBatchSizeSeen > 10 {
		t.Errorf("Batch size %d exceeds MaxBatchSize 10", maxBatchSizeSeen)
	}

	// Should have split into multiple batches
	if loadCount < 3 {
		t.Errorf("Expected at least 3 batches (due to MaxBatchSize), got %d", loadCount)
	}
}

// TestDataLoader_ErrorHandling tests error handling
func TestDataLoader_ErrorHandling(t *testing.T) {
	batchLoadFunc := func(ctx context.Context, keys []string) (map[string]interface{}, error) {
		return nil, fmt.Errorf("batch load error")
	}

	loader := NewDataLoader(batchLoadFunc, nil)

	_, err := loader.Load(context.Background(), "key1")
	if err == nil {
		t.Error("Expected error, got nil")
	}

	if err.Error() != "batch load error" {
		t.Errorf("Expected 'batch load error', got '%v'", err)
	}
}

// TestDataLoader_ClearCache tests cache clearing
func TestDataLoader_ClearCache(t *testing.T) {
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

	// First load
	_, err := loader.Load(context.Background(), "key1")
	if err != nil {
		t.Fatalf("First load failed: %v", err)
	}

	time.Sleep(20 * time.Millisecond)

	// Clear cache
	loader.ClearCache()

	// Second load - should trigger new batch since cache was cleared
	_, err = loader.Load(context.Background(), "key1")
	if err != nil {
		t.Fatalf("Second load failed: %v", err)
	}

	time.Sleep(20 * time.Millisecond)

	// Should have loaded twice (cache was cleared)
	if loadCount != 2 {
		t.Errorf("Expected 2 batch loads (cache cleared), got %d", loadCount)
	}
}
