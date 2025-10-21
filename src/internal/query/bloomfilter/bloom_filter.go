package bloomfilter

/*
BLOOM FILTER IMPLEMENTATION

A Bloom filter is a space-efficient probabilistic data structure that tests whether
an element is a member of a set. It can have false positives but never false negatives.

KEY PROPERTIES:
- Space efficient: Uses bit array instead of storing actual keys
- Fast operations: O(k) where k is number of hash functions
- No false negatives: If it says "NOT in set", it's guaranteed correct
- Possible false positives: Might say "in set" when item isn't actually present

USE CASE IN SYNDRDB:
Used in hash join optimization to quickly filter out non-matching rows before
expensive hash table lookups. If Bloom filter says "definitely not in hash table",
we can skip the hash lookup entirely.

MATHEMATICAL FOUNDATION:
- m = size of bit array
- n = number of items inserted
- k = number of hash functions
- False positive rate ≈ (1 - e^(-kn/m))^k
- Optimal k = (m/n) × ln(2) ≈ 0.693 × (m/n)
*/

import (
	"hash/fnv"
	"math"
)

// BloomFilter represents a space-efficient probabilistic set membership filter
type BloomFilter struct {
	bitArray []uint64 // Packed bit array (each uint64 holds 64 bits)
	size     uint64   // Total number of bits in the filter
	numHash  uint32   // Number of hash functions to use
	numItems uint64   // Number of items added (for statistics)
}

// NewBloomFilter creates a new Bloom filter optimized for the expected number of items
// and desired false positive rate
//
// Parameters:
//   - expectedItems: Estimated number of items that will be added
//   - falsePositiveRate: Desired false positive probability (e.g., 0.01 for 1%)
//
// Example:
//
//	bloom := NewBloomFilter(1000000, 0.01)  // 1M items, 1% false positive rate
func NewBloomFilter(expectedItems int, falsePositiveRate float64) *BloomFilter {
	if expectedItems <= 0 {
		expectedItems = 1000 // Default minimum
	}
	if falsePositiveRate <= 0 || falsePositiveRate >= 1 {
		falsePositiveRate = 0.01 // Default to 1%
	}

	// Calculate optimal bit array size
	// m = -n * ln(p) / (ln(2)^2)
	n := float64(expectedItems)
	p := falsePositiveRate
	m := -n * math.Log(p) / (math.Log(2) * math.Log(2))
	size := uint64(math.Ceil(m))

	// Calculate optimal number of hash functions
	// k = (m/n) * ln(2)
	k := (m / n) * math.Log(2)
	numHash := uint32(math.Ceil(k))
	if numHash < 1 {
		numHash = 1
	}
	if numHash > 20 {
		numHash = 20 // Practical upper limit
	}

	// Calculate array size (number of uint64s needed)
	arraySize := (size + 63) / 64 // Round up to nearest uint64

	return &BloomFilter{
		bitArray: make([]uint64, arraySize),
		size:     size,
		numHash:  numHash,
		numItems: 0,
	}
}

// NewBloomFilterWithSize creates a Bloom filter with explicit size and hash count
// Use this when you want manual control over parameters
func NewBloomFilterWithSize(bitSize uint64, numHashFunctions uint32) *BloomFilter {
	if bitSize == 0 {
		bitSize = 1024 * 8 // Default 1KB
	}
	if numHashFunctions == 0 {
		numHashFunctions = 3 // Default 3 hash functions
	}

	arraySize := (bitSize + 63) / 64
	return &BloomFilter{
		bitArray: make([]uint64, arraySize),
		size:     bitSize,
		numHash:  numHashFunctions,
		numItems: 0,
	}
}

// Add inserts an item into the Bloom filter
// After adding, MayContain(item) will return true (but may have false positives)
func (bf *BloomFilter) Add(item string) {
	hash1, hash2 := bf.getHashValues(item)

	for i := uint32(0); i < bf.numHash; i++ {
		// Double hashing technique: h(i) = h1 + i*h2
		hash := (hash1 + uint64(i)*hash2) % bf.size
		bf.setBit(hash)
	}

	bf.numItems++
}

// MayContain checks if an item might be in the set
// Returns:
//   - false: Item is DEFINITELY NOT in the set (100% accurate)
//   - true: Item MIGHT be in the set (could be false positive)
func (bf *BloomFilter) MayContain(item string) bool {
	hash1, hash2 := bf.getHashValues(item)

	for i := uint32(0); i < bf.numHash; i++ {
		hash := (hash1 + uint64(i)*hash2) % bf.size
		if !bf.getBit(hash) {
			// Found a zero bit - definitely NOT in set
			return false
		}
	}

	// All bits were 1 - might be in set (or false positive)
	return true
}

// Clear resets the Bloom filter to empty state
func (bf *BloomFilter) Clear() {
	for i := range bf.bitArray {
		bf.bitArray[i] = 0
	}
	bf.numItems = 0
}

// EstimatedFalsePositiveRate calculates the current false positive probability
// based on how full the filter is
func (bf *BloomFilter) EstimatedFalsePositiveRate() float64 {
	if bf.numItems == 0 {
		return 0
	}

	// Calculate fill rate (proportion of bits set to 1)
	// FPR ≈ (1 - e^(-kn/m))^k
	m := float64(bf.size)
	n := float64(bf.numItems)
	k := float64(bf.numHash)

	exponent := -k * n / m
	fillRate := 1 - math.Exp(exponent)
	fpr := math.Pow(fillRate, k)

	return fpr
}

// GetStats returns statistics about the Bloom filter
func (bf *BloomFilter) GetStats() BloomFilterStats {
	bitsSet := bf.countSetBits()
	fillRate := float64(bitsSet) / float64(bf.size)

	return BloomFilterStats{
		Size:             bf.size,
		NumHashFunctions: bf.numHash,
		ItemsAdded:       bf.numItems,
		BitsSet:          bitsSet,
		FillRate:         fillRate,
		EstimatedFPR:     bf.EstimatedFalsePositiveRate(),
		MemoryUsedBytes:  uint64(len(bf.bitArray) * 8),
	}
}

// BloomFilterStats contains statistics about a Bloom filter
type BloomFilterStats struct {
	Size             uint64  // Total number of bits
	NumHashFunctions uint32  // Number of hash functions used
	ItemsAdded       uint64  // Number of items added
	BitsSet          uint64  // Number of bits currently set to 1
	FillRate         float64 // Proportion of bits set (0.0 to 1.0)
	EstimatedFPR     float64 // Estimated false positive rate
	MemoryUsedBytes  uint64  // Memory used in bytes
}

// getHashValues computes two hash values for double hashing
// Uses FNV-1a hash algorithm for speed
func (bf *BloomFilter) getHashValues(item string) (uint64, uint64) {
	// First hash: FNV-1a
	h1 := fnv.New64a()
	h1.Write([]byte(item))
	hash1 := h1.Sum64()

	// Second hash: FNV-1a with modified seed
	// We XOR with a prime to get a different hash function
	h2 := fnv.New64a()
	h2.Write([]byte(item + "\x00")) // Add null byte as salt
	hash2 := h2.Sum64()

	return hash1, hash2
}

// setBit sets the bit at the given position to 1
func (bf *BloomFilter) setBit(pos uint64) {
	arrayIndex := pos / 64
	bitIndex := pos % 64
	bf.bitArray[arrayIndex] |= (1 << bitIndex)
}

// getBit checks if the bit at the given position is set
func (bf *BloomFilter) getBit(pos uint64) bool {
	arrayIndex := pos / 64
	bitIndex := pos % 64
	return (bf.bitArray[arrayIndex] & (1 << bitIndex)) != 0
}

// countSetBits counts how many bits are currently set to 1
func (bf *BloomFilter) countSetBits() uint64 {
	count := uint64(0)
	for _, word := range bf.bitArray {
		// Count set bits using Brian Kernighan's algorithm
		w := word
		for w != 0 {
			w &= w - 1 // Clear the lowest set bit
			count++
		}
	}
	return count
}

// MemoryUsage returns the memory used by the Bloom filter in bytes
func (bf *BloomFilter) MemoryUsage() int64 {
	return int64(len(bf.bitArray) * 8) // 8 bytes per uint64
}
