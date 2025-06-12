package hashindex

import (
	//"hash/fnv"
	"unsafe"
)

// hashKey is a function variable that will hold the hash function implementation
// hashKey computes the hash value of a key
var hashKey func(key []byte) uint32

// func hashKey_fnv(key []byte) uint32 {
// 	// Use FNV hash for simplicity in this example
// 	// PostgreSQL uses a more sophisticated hash function
// 	h := fnv.New32a()
// 	h.Write(key)
// 	return h.Sum32()
// }

// computeBucket determines which bucket a hash value belongs to
func (hi *HashIndex) computeBucket(hashValue uint32) uint32 {
	bucket := hashValue & hi.metadata.HighMask

	// Check if this bucket has been split
	if bucket > hi.metadata.MaxBucket {
		// Apply the "linear hashing" algorithm logic
		bucket = hashValue & hi.metadata.LowMask
	}

	return bucket + 1 // Convert to 1-based page numbers
}

// -----------------------  new implementation of the jenkins hash function -----------------------

// jenkinsHash implements the Bob Jenkins hash algorithm from 1997, optimized for database index use
func jenkinsHash(key []byte, seed uint32) uint32 {
	length := len(key)

	// Set up the initial values
	a := uint32(0x9e3779b9) + uint32(length) + seed
	b := uint32(0x9e3779b9) + seed
	c := uint32(0x9e3779b9) + seed

	// Process the key in 12-byte chunks
	i := 0
	for length >= 12 {
		a += uint32(key[i+0]) | (uint32(key[i+1]) << 8) | (uint32(key[i+2]) << 16) | (uint32(key[i+3]) << 24)
		b += uint32(key[i+4]) | (uint32(key[i+5]) << 8) | (uint32(key[i+6]) << 16) | (uint32(key[i+7]) << 24)
		c += uint32(key[i+8]) | (uint32(key[i+9]) << 8) | (uint32(key[i+10]) << 16) | (uint32(key[i+11]) << 24)

		// Mix
		a, b, c = mix(a, b, c)

		i += 12
		length -= 12
	}

	// Handle the last few bytes
	if length > 0 {
		switch length {
		case 11:
			c += uint32(key[i+10]) << 24
			fallthrough
		case 10:
			c += uint32(key[i+9]) << 16
			fallthrough
		case 9:
			c += uint32(key[i+8]) << 8
			fallthrough
		case 8:
			b += uint32(key[i+7]) << 24
			fallthrough
		case 7:
			b += uint32(key[i+6]) << 16
			fallthrough
		case 6:
			b += uint32(key[i+5]) << 8
			fallthrough
		case 5:
			b += uint32(key[i+4])
			fallthrough
		case 4:
			a += uint32(key[i+3]) << 24
			fallthrough
		case 3:
			a += uint32(key[i+2]) << 16
			fallthrough
		case 2:
			a += uint32(key[i+1]) << 8
			fallthrough
		case 1:
			a += uint32(key[i+0])
		}

		// Final mix with remaining bytes
		c ^= b
		c -= (b << 14) | (b >> 18)
		a ^= c
		a -= (c << 11) | (c >> 21)
		b ^= a
		b -= (a << 25) | (a >> 7)
		c ^= b
		c -= (b << 16) | (b >> 16)
		a ^= c
		a -= (c << 4) | (c >> 28)
		b ^= a
		b -= (a << 14) | (a >> 18)
		c ^= b
		c -= (b << 24) | (b >> 8)
	}

	return c
}

// mix mixes 3 32-bit values reversibly
func mix(a, b, c uint32) (uint32, uint32, uint32) {
	a -= b
	a -= c
	a ^= c >> 13
	b -= c
	b -= a
	b ^= a << 8
	c -= a
	c -= b
	c ^= b >> 13
	a -= b
	a -= c
	a ^= c >> 12
	b -= c
	b -= a
	b ^= a << 16
	c -= a
	c -= b
	c ^= b >> 5
	a -= b
	a -= c
	a ^= c >> 3
	b -= c
	b -= a
	b ^= a << 10
	c -= a
	c -= b
	c ^= b >> 15

	return a, b, c
}

// TypedHash provides hash functions optimized for different data types
type TypedHash struct{}

// NewTypedHash creates a new TypedHash
func NewTypedHash() *TypedHash {
	return &TypedHash{}
}

// HashString computes a hash optimized for string data
func (th *TypedHash) HashString(s string, seed uint32) uint32 {
	return jenkinsHash([]byte(s), seed)
}

// HashInt computes a hash optimized for integer data
func (th *TypedHash) HashInt(i int64, seed uint32) uint32 {
	buf := []byte{
		byte(i),
		byte(i >> 8),
		byte(i >> 16),
		byte(i >> 24),
		byte(i >> 32),
		byte(i >> 40),
		byte(i >> 48),
		byte(i >> 56),
	}
	return jenkinsHash(buf, seed)
}

// HashFloat computes a hash optimized for floating point data
func (th *TypedHash) HashFloat(f float64, seed uint32) uint32 {
	// Handle special values
	switch {
	case f == 0:
		return seed // Zero gets special treatment
	case f != f:
		return jenkinsHash([]byte{0xFF, 0xFF, 0xFF, 0xFF}, seed) // NaN
	}

	// Convert float to bits for consistent hashing
	bits := float64Bits(f)
	buf := []byte{
		byte(bits),
		byte(bits >> 8),
		byte(bits >> 16),
		byte(bits >> 24),
		byte(bits >> 32),
		byte(bits >> 40),
		byte(bits >> 48),
		byte(bits >> 56),
	}

	return jenkinsHash(buf, seed)
}

// float64Bits converts a float64 to its IEEE 754 binary representation
func float64Bits(f float64) uint64 {
	return *(*uint64)(unsafe.Pointer(&f))
}

// UpdateHashKey updates the hash function used in the hash index
func UpdateHashKey() {
	// Replace existing hashKey function with the Jenkins hash
	hashKey = func(key []byte) uint32 {
		// Use a fixed seed for consistency
		// TODO In production, use a per-index seed stored in the metadata
		seed := uint32(0x8f43a133) // Random initialization value
		return jenkinsHash(key, seed)
	}
}
