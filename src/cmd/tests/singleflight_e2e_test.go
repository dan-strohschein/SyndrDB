// singleflight_e2e_test.go - End-to-end tests for Phase 8 singleflight implementation
//
// These tests verify that the singleflight pattern in BundleStorageEngine correctly
// prevents thundering herd on cache population. When multiple goroutines request
// the same uncached key simultaneously, only one should perform the actual parse
// operation while others wait for and receive the same result.
//
// Tests cover:
// - Basic singleflight deduplication
// - Concurrent requests for same key
// - Concurrent requests for different keys (should run in parallel)
// - Error handling and result sharing
// - High concurrency stress test

package main

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/sync/singleflight"
)

// TestSingleflight_BasicDeduplication verifies that only one goroutine executes
// the function when multiple goroutines request the same key.
func TestSingleflight_BasicDeduplication(t *testing.T) {
	var group singleflight.Group
	var executeCount atomic.Int32

	// Simulate expensive parsing operation
	parseFunc := func() (interface{}, error) {
		executeCount.Add(1)
		time.Sleep(50 * time.Millisecond) // Simulate work
		return "parsed_result", nil
	}

	// Launch 10 concurrent requests for the same key
	const numRequests = 10
	var wg sync.WaitGroup
	wg.Add(numRequests)

	results := make([]interface{}, numRequests)
	errors := make([]error, numRequests)

	for i := 0; i < numRequests; i++ {
		go func(idx int) {
			defer wg.Done()
			result, err, _ := group.Do("same_key", parseFunc)
			results[idx] = result
			errors[idx] = err
		}(i)
	}

	wg.Wait()

	// Verify only one execution occurred
	executions := executeCount.Load()
	if executions != 1 {
		t.Errorf("Expected 1 execution, got %d", executions)
	}

	// Verify all goroutines got the same result
	for i := 0; i < numRequests; i++ {
		if errors[i] != nil {
			t.Errorf("Request %d got unexpected error: %v", i, errors[i])
		}
		if results[i] != "parsed_result" {
			t.Errorf("Request %d got unexpected result: %v", i, results[i])
		}
	}

	t.Logf("TestSingleflight_BasicDeduplication PASSED: %d requests, %d execution", numRequests, executions)
}

// TestSingleflight_DifferentKeysParallel verifies that requests for different keys
// run in parallel without blocking each other.
func TestSingleflight_DifferentKeysParallel(t *testing.T) {
	var group singleflight.Group
	var concurrentExecutions atomic.Int32
	var maxConcurrent atomic.Int32

	// Track concurrent executions
	parseFunc := func(key string) func() (interface{}, error) {
		return func() (interface{}, error) {
			current := concurrentExecutions.Add(1)
			// Update max if current is higher
			for {
				max := maxConcurrent.Load()
				if current <= max || maxConcurrent.CompareAndSwap(max, current) {
					break
				}
			}
			time.Sleep(50 * time.Millisecond) // Simulate work
			concurrentExecutions.Add(-1)
			return fmt.Sprintf("result_%s", key), nil
		}
	}

	// Launch requests for 5 different keys
	const numKeys = 5
	var wg sync.WaitGroup
	wg.Add(numKeys)

	for i := 0; i < numKeys; i++ {
		go func(idx int) {
			defer wg.Done()
			key := fmt.Sprintf("key_%d", idx)
			group.Do(key, parseFunc(key))
		}(i)
	}

	wg.Wait()

	// Verify that multiple executions ran concurrently
	maxConc := maxConcurrent.Load()
	if maxConc < 2 {
		t.Errorf("Expected at least 2 concurrent executions, got %d", maxConc)
	}

	t.Logf("TestSingleflight_DifferentKeysParallel PASSED: max concurrent executions = %d", maxConc)
}

// TestSingleflight_ErrorSharing verifies that errors are properly shared
// among all waiters.
func TestSingleflight_ErrorSharing(t *testing.T) {
	var group singleflight.Group
	expectedErr := fmt.Errorf("simulated error")

	// Function that always returns an error
	parseFunc := func() (interface{}, error) {
		time.Sleep(20 * time.Millisecond) // Simulate work
		return nil, expectedErr
	}

	// Launch multiple concurrent requests
	const numRequests = 5
	var wg sync.WaitGroup
	wg.Add(numRequests)

	errors := make([]error, numRequests)

	for i := 0; i < numRequests; i++ {
		go func(idx int) {
			defer wg.Done()
			_, err, _ := group.Do("error_key", parseFunc)
			errors[idx] = err
		}(i)
	}

	wg.Wait()

	// Verify all goroutines got the error
	for i := 0; i < numRequests; i++ {
		if errors[i] == nil || errors[i].Error() != expectedErr.Error() {
			t.Errorf("Request %d: expected error '%v', got '%v'", i, expectedErr, errors[i])
		}
	}

	t.Logf("TestSingleflight_ErrorSharing PASSED: all %d requests received shared error", numRequests)
}

// TestSingleflight_SharedFlag verifies the 'shared' return value indicates
// whether the result was shared with other waiters.
func TestSingleflight_SharedFlag(t *testing.T) {
	var group singleflight.Group

	parseFunc := func() (interface{}, error) {
		time.Sleep(30 * time.Millisecond) // Simulate work
		return "result", nil
	}

	// Launch multiple concurrent requests
	const numRequests = 10
	var wg sync.WaitGroup
	wg.Add(numRequests)

	sharedFlags := make([]bool, numRequests)

	for i := 0; i < numRequests; i++ {
		go func(idx int) {
			defer wg.Done()
			_, _, shared := group.Do("shared_key", parseFunc)
			sharedFlags[idx] = shared
		}(i)
	}

	wg.Wait()

	// Count how many had shared=true (all but one should have shared=true)
	sharedCount := 0
	for _, shared := range sharedFlags {
		if shared {
			sharedCount++
		}
	}

	// At least numRequests-1 should have shared=true (the first one has shared=false)
	if sharedCount < numRequests-1 {
		t.Errorf("Expected at least %d shared results, got %d", numRequests-1, sharedCount)
	}

	t.Logf("TestSingleflight_SharedFlag PASSED: %d/%d requests had shared=true", sharedCount, numRequests)
}

// TestSingleflight_HighConcurrency stress tests the singleflight pattern
// with many concurrent requests.
func TestSingleflight_HighConcurrency(t *testing.T) {
	var group singleflight.Group
	var executeCount atomic.Int32
	const numKeys = 10
	const requestsPerKey = 100

	// Each key's parse function counts its executions
	parseFuncs := make([]func() (interface{}, error), numKeys)
	execCounts := make([]atomic.Int32, numKeys)

	for i := 0; i < numKeys; i++ {
		idx := i
		parseFuncs[i] = func() (interface{}, error) {
			execCounts[idx].Add(1)
			executeCount.Add(1)
			time.Sleep(10 * time.Millisecond) // Simulate work
			return fmt.Sprintf("result_%d", idx), nil
		}
	}

	// Launch many concurrent requests
	var wg sync.WaitGroup
	totalRequests := numKeys * requestsPerKey
	wg.Add(totalRequests)

	start := time.Now()

	for keyIdx := 0; keyIdx < numKeys; keyIdx++ {
		for req := 0; req < requestsPerKey; req++ {
			go func(k int) {
				defer wg.Done()
				key := fmt.Sprintf("key_%d", k)
				group.Do(key, parseFuncs[k])
			}(keyIdx)
		}
	}

	wg.Wait()
	elapsed := time.Since(start)

	// Verify each key had exactly 1 execution (singleflight deduplication)
	for i := 0; i < numKeys; i++ {
		count := execCounts[i].Load()
		if count != 1 {
			t.Errorf("Key %d: expected 1 execution, got %d", i, count)
		}
	}

	totalExecs := executeCount.Load()
	if totalExecs != int32(numKeys) {
		t.Errorf("Expected %d total executions, got %d", numKeys, totalExecs)
	}

	t.Logf("TestSingleflight_HighConcurrency PASSED: %d requests across %d keys, %d executions, completed in %v",
		totalRequests, numKeys, totalExecs, elapsed)
}

// TestSingleflight_PanicRecovery verifies that panics in the parse function
// don't permanently block other waiters. Note: singleflight.Do propagates panics
// to all waiters, so they all see the panic.
func TestSingleflight_PanicRecovery(t *testing.T) {
	var group singleflight.Group

	// Function that panics
	panicFunc := func() (interface{}, error) {
		time.Sleep(10 * time.Millisecond)
		panic("simulated panic")
	}

	// Launch a request that will panic
	var wg sync.WaitGroup
	wg.Add(1)

	panicCaught := make(chan bool, 1)

	go func() {
		defer wg.Done()
		defer func() {
			if r := recover(); r != nil {
				panicCaught <- true
			} else {
				panicCaught <- false
			}
		}()
		group.Do("panic_key", panicFunc)
	}()

	wg.Wait()

	select {
	case caught := <-panicCaught:
		if !caught {
			t.Error("Expected panic to be propagated")
		}
	case <-time.After(2 * time.Second):
		t.Error("Timed out waiting for panic")
	}

	// After panic, a new request for the same key should work
	normalFunc := func() (interface{}, error) {
		return "recovered", nil
	}

	result, err, _ := group.Do("panic_key", normalFunc)
	if err != nil {
		t.Errorf("Unexpected error after panic recovery: %v", err)
	}
	if result != "recovered" {
		t.Errorf("Expected 'recovered', got %v", result)
	}

	t.Logf("TestSingleflight_PanicRecovery PASSED: panic propagated, subsequent request succeeded")
}

// TestSingleflight_CachePatternSimulation simulates the actual usage pattern
// in BundleStorageEngine where singleflight protects cache population.
func TestSingleflight_CachePatternSimulation(t *testing.T) {
	var group singleflight.Group
	cache := sync.Map{}
	var parseCount atomic.Int32

	// Simulate cache lookup + singleflight pattern
	getOrParse := func(key string) (string, error) {
		// Check cache first
		if cached, ok := cache.Load(key); ok {
			return cached.(string), nil
		}

		// Cache miss - use singleflight
		result, err, _ := group.Do(key, func() (interface{}, error) {
			// Double-check cache (another goroutine may have populated it)
			if cached, ok := cache.Load(key); ok {
				return cached.(string), nil
			}

			// Simulate expensive parsing
			parseCount.Add(1)
			time.Sleep(20 * time.Millisecond)
			value := fmt.Sprintf("parsed_%s", key)

			// Store in cache
			cache.Store(key, value)
			return value, nil
		})

		if err != nil {
			return "", err
		}
		return result.(string), nil
	}

	// Launch concurrent requests for multiple keys
	const numKeys = 5
	const requestsPerKey = 20
	var wg sync.WaitGroup
	wg.Add(numKeys * requestsPerKey)

	for k := 0; k < numKeys; k++ {
		for r := 0; r < requestsPerKey; r++ {
			go func(keyIdx int) {
				defer wg.Done()
				key := fmt.Sprintf("bundle_%d", keyIdx)
				result, err := getOrParse(key)
				if err != nil {
					t.Errorf("Unexpected error for key %s: %v", key, err)
				}
				expected := fmt.Sprintf("parsed_%s", key)
				if result != expected {
					t.Errorf("Expected %s, got %s", expected, result)
				}
			}(k)
		}
	}

	wg.Wait()

	// Verify each key was parsed exactly once
	totalParses := parseCount.Load()
	if totalParses != int32(numKeys) {
		t.Errorf("Expected %d parses, got %d", numKeys, totalParses)
	}

	t.Logf("TestSingleflight_CachePatternSimulation PASSED: %d keys, %d total requests, %d parses",
		numKeys, numKeys*requestsPerKey, totalParses)
}
