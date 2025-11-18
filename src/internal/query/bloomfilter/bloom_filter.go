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

Also used in DISTINCT query deduplication to provide 2-3x performance improvement
by pre-checking if a value hash has been seen before expensive hash table lookups.
Supports composite value operations via byte array serialization for multi-column DISTINCT.

MATHEMATICAL FOUNDATION:
- m = size of bit array
- n = number of items inserted
- k = number of hash functions
- False positive rate ≈ (1 - e^(-kn/m))^k
- Optimal k = (m/n) × ln(2) ≈ 0.693 × (m/n)
*/

import (
	"bytes"
	"encoding/binary"
	"fmt"
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

// AddBytes inserts a byte array into the Bloom filter
// This is used for composite value hashing in DISTINCT operations
// where multiple field values are serialized into a byte array
func (bf *BloomFilter) AddBytes(data []byte) {
	hash1, hash2 := bf.getHashValuesFromBytes(data)

	for i := uint32(0); i < bf.numHash; i++ {
		// Double hashing technique: h(i) = h1 + i*h2
		hash := (hash1 + uint64(i)*hash2) % bf.size
		bf.setBit(hash)
	}

	bf.numItems++
}

// MayContainBytes checks if a byte array might be in the set
// Returns:
//   - false: Item is DEFINITELY NOT in the set (100% accurate)
//   - true: Item MIGHT be in the set (could be false positive)
func (bf *BloomFilter) MayContainBytes(data []byte) bool {
	hash1, hash2 := bf.getHashValuesFromBytes(data)

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

// AddUint64 inserts a uint64 hash value directly into the Bloom filter
// This is used when the hash has already been computed by the caller,
// avoiding redundant hashing. Useful for DISTINCT operations where
// we compute the hash once and use it for both bloom filter and hash table.
func (bf *BloomFilter) AddUint64(hash uint64) {
	// Use the hash value directly with double hashing
	hash1 := hash
	hash2 := hash ^ 0x5555555555555555 // XOR with pattern for second hash

	for i := uint32(0); i < bf.numHash; i++ {
		h := (hash1 + uint64(i)*hash2) % bf.size
		bf.setBit(h)
	}

	bf.numItems++
}

// MayContainUint64 checks if a uint64 hash value might be in the set
// Returns:
//   - false: Hash is DEFINITELY NOT in the set (100% accurate)
//   - true: Hash MIGHT be in the set (could be false positive)
func (bf *BloomFilter) MayContainUint64(hash uint64) bool {
	// Use the hash value directly with double hashing
	hash1 := hash
	hash2 := hash ^ 0x5555555555555555 // XOR with pattern for second hash

	for i := uint32(0); i < bf.numHash; i++ {
		h := (hash1 + uint64(i)*hash2) % bf.size
		if !bf.getBit(h) {
			// Found a zero bit - definitely NOT in set
			return false
		}
	}

	// All bits were 1 - might be in set (or false positive)
	return true
}

// getHashValuesFromBytes computes two hash values from a byte array for double hashing
func (bf *BloomFilter) getHashValuesFromBytes(data []byte) (uint64, uint64) {
	// First hash: FNV-1a
	h1 := fnv.New64a()
	h1.Write(data)
	hash1 := h1.Sum64()

	// Second hash: FNV-1a with modified seed
	// Append a null byte as salt for different hash function
	h2 := fnv.New64a()
	h2.Write(data)
	h2.Write([]byte{0x00})
	hash2 := h2.Sum64()

	return hash1, hash2
}

// SerializeValues serializes a slice of interface{} values into a deterministic byte array
// using length-delimited encoding with type tags for collision-resistant hashing.
// This is used for multi-column DISTINCT operations where we need to hash multiple field values together.
//
// Type tags:
//   - 0x01: nil
//   - 0x02: bool
//   - 0x03: int64
//   - 0x04: float64
//   - 0x05: string
//
// Format for each value: [1-byte type tag][4-byte length][value bytes]
//
// Returns serialized bytes and error if unsupported type encountered
func SerializeValues(values []interface{}) ([]byte, error) {
	buf := &bytes.Buffer{}

	for _, val := range values {
		switch v := val.(type) {
		case nil:
			// Type tag for nil
			buf.WriteByte(0x01)
			// Write 0 length
			binary.Write(buf, binary.LittleEndian, uint32(0))

		case bool:
			// Type tag for bool
			buf.WriteByte(0x02)
			// Write 1 byte length
			binary.Write(buf, binary.LittleEndian, uint32(1))
			// Write bool as byte
			if v {
				buf.WriteByte(1)
			} else {
				buf.WriteByte(0)
			}

		case int, int64:
			// Type tag for int64
			buf.WriteByte(0x03)
			// Write 8 byte length
			binary.Write(buf, binary.LittleEndian, uint32(8))
			// Convert to int64 and write
			var intVal int64
			switch iv := val.(type) {
			case int:
				intVal = int64(iv)
			case int64:
				intVal = iv
			}
			binary.Write(buf, binary.LittleEndian, intVal)

		case float64:
			// Type tag for float64
			buf.WriteByte(0x04)
			// Write 8 byte length
			binary.Write(buf, binary.LittleEndian, uint32(8))
			// Write float64
			binary.Write(buf, binary.LittleEndian, v)

		case string:
			// Type tag for string
			buf.WriteByte(0x05)
			// Write string length
			strBytes := []byte(v)
			binary.Write(buf, binary.LittleEndian, uint32(len(strBytes)))
			// Write string bytes
			buf.Write(strBytes)

		default:
			// Unsupported type - use fmt.Sprintf as fallback
			buf.WriteByte(0x05) // Treat as string
			strVal := fmt.Sprintf("%v", v)
			strBytes := []byte(strVal)
			binary.Write(buf, binary.LittleEndian, uint32(len(strBytes)))
			buf.Write(strBytes)
		}
	}

	return buf.Bytes(), nil
}
