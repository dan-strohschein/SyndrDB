package bloomfilter

import (
	"fmt"
	"testing"
)

func TestBloomFilterBasicOperations(t *testing.T) {
	// Create a Bloom filter for 1000 items with 1% false positive rate
	bf := NewBloomFilter(1000, 0.01)

	// Test empty filter
	if bf.MayContain("item1") {
		t.Error("Empty Bloom filter should return false for any item")
	}

	// Add items
	bf.Add("apple")
	bf.Add("banana")
	bf.Add("cherry")

	// Test that added items are found
	if !bf.MayContain("apple") {
		t.Error("Bloom filter should contain 'apple'")
	}
	if !bf.MayContain("banana") {
		t.Error("Bloom filter should contain 'banana'")
	}
	if !bf.MayContain("cherry") {
		t.Error("Bloom filter should contain 'cherry'")
	}

	// Note: We can't test that non-added items return false
	// because Bloom filters can have false positives
	// But we can test that the FPR is within reasonable bounds
}

func TestBloomFilterFalsePositiveRate(t *testing.T) {
	expectedItems := 10000
	targetFPR := 0.01 // 1%

	bf := NewBloomFilter(expectedItems, targetFPR)

	// Add expected number of items
	for i := 0; i < expectedItems; i++ {
		bf.Add(fmt.Sprintf("item_%d", i))
	}

	// Check false positive rate on non-added items
	falsePositives := 0
	testSize := 10000

	for i := expectedItems; i < expectedItems+testSize; i++ {
		if bf.MayContain(fmt.Sprintf("item_%d", i)) {
			falsePositives++
		}
	}

	actualFPR := float64(falsePositives) / float64(testSize)
	estimatedFPR := bf.EstimatedFalsePositiveRate()

	t.Logf("Target FPR: %.4f, Actual FPR: %.4f, Estimated FPR: %.4f",
		targetFPR, actualFPR, estimatedFPR)

	// Actual FPR should be within 3x of target (due to statistical variance)
	if actualFPR > targetFPR*3 {
		t.Errorf("False positive rate too high: %.4f (target: %.4f)", actualFPR, targetFPR)
	}
}

func TestBloomFilterNoFalseNegatives(t *testing.T) {
	bf := NewBloomFilter(1000, 0.01)

	// Add many items
	items := make([]string, 1000)
	for i := 0; i < 1000; i++ {
		items[i] = fmt.Sprintf("test_item_%d", i)
		bf.Add(items[i])
	}

	// Verify all added items are found (no false negatives)
	for _, item := range items {
		if !bf.MayContain(item) {
			t.Errorf("False negative detected for item: %s", item)
		}
	}
}

func TestBloomFilterClear(t *testing.T) {
	bf := NewBloomFilter(100, 0.01)

	// Add items
	bf.Add("item1")
	bf.Add("item2")

	if !bf.MayContain("item1") {
		t.Error("Item should be in filter before clear")
	}

	// Clear the filter
	bf.Clear()

	// After clear, should not find items (unless false positive, but unlikely with small set)
	stats := bf.GetStats()
	if stats.ItemsAdded != 0 {
		t.Error("Filter should have 0 items after clear")
	}
	if stats.BitsSet != 0 {
		t.Error("Filter should have 0 bits set after clear")
	}
}

func TestBloomFilterStats(t *testing.T) {
	expectedItems := 1000
	bf := NewBloomFilter(expectedItems, 0.01)

	// Add items
	for i := 0; i < expectedItems; i++ {
		bf.Add(fmt.Sprintf("item_%d", i))
	}

	stats := bf.GetStats()

	t.Logf("Bloom Filter Stats:")
	t.Logf("  Size: %d bits", stats.Size)
	t.Logf("  Hash functions: %d", stats.NumHashFunctions)
	t.Logf("  Items added: %d", stats.ItemsAdded)
	t.Logf("  Bits set: %d", stats.BitsSet)
	t.Logf("  Fill rate: %.2f%%", stats.FillRate*100)
	t.Logf("  Estimated FPR: %.4f", stats.EstimatedFPR)
	t.Logf("  Memory used: %d bytes", stats.MemoryUsedBytes)

	if stats.ItemsAdded != uint64(expectedItems) {
		t.Errorf("Expected %d items, got %d", expectedItems, stats.ItemsAdded)
	}

	if stats.FillRate <= 0 || stats.FillRate >= 1 {
		t.Errorf("Fill rate should be between 0 and 1, got %.2f", stats.FillRate)
	}
}

func TestBloomFilterWithSize(t *testing.T) {
	bitSize := uint64(1024 * 8) // 1KB = 8192 bits
	numHash := uint32(5)

	bf := NewBloomFilterWithSize(bitSize, numHash)

	if bf.size != bitSize {
		t.Errorf("Expected size %d, got %d", bitSize, bf.size)
	}
	if bf.numHash != numHash {
		t.Errorf("Expected %d hash functions, got %d", numHash, bf.numHash)
	}

	// Test basic operations
	bf.Add("test")
	if !bf.MayContain("test") {
		t.Error("Should contain added item")
	}
}

func BenchmarkBloomFilterAdd(b *testing.B) {
	bf := NewBloomFilter(b.N, 0.01)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		bf.Add(fmt.Sprintf("item_%d", i))
	}
}

func BenchmarkBloomFilterMayContain(b *testing.B) {
	bf := NewBloomFilter(10000, 0.01)

	// Pre-populate
	for i := 0; i < 10000; i++ {
		bf.Add(fmt.Sprintf("item_%d", i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bf.MayContain(fmt.Sprintf("item_%d", i%10000))
	}
}
