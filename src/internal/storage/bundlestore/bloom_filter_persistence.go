package bundlestore

import (
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"syndrdb/src/internal/query/bloomfilter"
)

/*
BLOOM FILTER PERSISTENCE

Helper functions for serializing/deserializing bloom filters for manifest storage.
Bloom filters are used to quickly determine if a document ID might exist in a file,
reducing unnecessary file reads during document lookups.

SERIALIZATION FORMAT:
- Compact binary representation of the bit array
- Base64 encoded for JSON storage in manifest
- Size and hash count stored separately as metadata

BENEFITS:
- Skip reading files that definitely don't contain a document
- 99%+ accuracy (1% false positive rate typical)
- Space efficient (< 1KB per filter for 10K documents)
*/

// SerializeBloomFilter converts a bloom filter to base64 string for manifest storage
func SerializeBloomFilter(bf *bloomfilter.BloomFilter) (string, uint64, uint32, error) {
	if bf == nil {
		return "", 0, 0, fmt.Errorf("bloom filter is nil")
	}

	stats := bf.GetStats()

	// Get bit array from bloom filter
	bitArray := bf.GetBitArray()

	// Serialize bit array to bytes
	data := SerializeBloomFilterToBytes(bitArray)

	// Encode as base64 for JSON storage
	encoded := base64.StdEncoding.EncodeToString(data)

	return encoded, stats.Size, stats.NumHashFunctions, nil
}

// DeserializeBloomFilter reconstructs a bloom filter from base64 string
func DeserializeBloomFilter(data string, size uint64, numHashes uint32) (*bloomfilter.BloomFilter, error) {
	if data == "" {
		return nil, fmt.Errorf("empty bloom filter data")
	}

	// Decode base64
	bytes, err := base64.StdEncoding.DecodeString(data)
	if err != nil {
		return nil, fmt.Errorf("failed to decode base64: %w", err)
	}

	// Expected size
	expectedSize := int((size + 63) / 64 * 8)
	if len(bytes) != expectedSize {
		return nil, fmt.Errorf("unexpected data size: got %d, expected %d", len(bytes), expectedSize)
	}

	// Convert bytes to bit array
	bitArray, err := DeserializeBloomFilterFromBytes(bytes)
	if err != nil {
		return nil, fmt.Errorf("failed to deserialize bit array: %w", err)
	}

	// Create new bloom filter with specified parameters
	bf := bloomfilter.NewBloomFilterWithSize(size, numHashes)

	// Set the bit array
	if err := bf.SetBitArray(bitArray); err != nil {
		return nil, fmt.Errorf("failed to set bit array: %w", err)
	}

	return bf, nil
}

// BuildBloomFilterForDocuments creates a bloom filter from a list of document IDs
func BuildBloomFilterForDocuments(documentIDs []string, falsePositiveRate float64) *bloomfilter.BloomFilter {
	if len(documentIDs) == 0 {
		return nil
	}

	// Create bloom filter optimized for the number of documents
	bf := bloomfilter.NewBloomFilter(len(documentIDs), falsePositiveRate)

	// Add all document IDs
	for _, docID := range documentIDs {
		bf.Add(docID)
	}

	return bf
}

// EstimateBloomFilterSize calculates the optimal bloom filter size for a given document count
func EstimateBloomFilterSize(docCount int, falsePositiveRate float64) (bitSize uint64, numHashes uint32) {
	if docCount <= 0 {
		return 1024, 3 // Minimum reasonable size
	}

	// Use bloom filter's internal calculation
	bf := bloomfilter.NewBloomFilter(docCount, falsePositiveRate)
	stats := bf.GetStats()

	return stats.Size, stats.NumHashFunctions
}

// SerializeBloomFilterToBytes converts bloom filter bit array to bytes (for future direct access)
func SerializeBloomFilterToBytes(bitArray []uint64) []byte {
	data := make([]byte, len(bitArray)*8)
	for i, val := range bitArray {
		binary.LittleEndian.PutUint64(data[i*8:(i+1)*8], val)
	}
	return data
}

// DeserializeBloomFilterFromBytes converts bytes back to bit array
func DeserializeBloomFilterFromBytes(data []byte) ([]uint64, error) {
	if len(data)%8 != 0 {
		return nil, fmt.Errorf("invalid data length: must be multiple of 8")
	}

	bitArray := make([]uint64, len(data)/8)
	for i := range bitArray {
		bitArray[i] = binary.LittleEndian.Uint64(data[i*8 : (i+1)*8])
	}
	return bitArray, nil
}
