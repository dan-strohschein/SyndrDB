package hashindexV2

/*
HASH FUNCTION IMPLEMENTATION

This file implements the Bob Jenkins hash function (lookup3.c) which is widely
used in database systems for its excellent distribution properties and speed.

ALGORITHM OVERVIEW:
The Jenkins hash function uses a mixing function that processes the input in
12-byte chunks, with special handling for the final partial chunk. It produces
a 32-bit hash value with excellent avalanche properties (small input changes
cause large output changes).

KEY FEATURES:
- Fast execution (processes 12 bytes at a time)
- Excellent distribution properties
- Avalanche effect (1-bit input change affects ~50% of output bits)
- Configurable seed for different hash instances
- Handles arbitrary-length byte sequences

USAGE:
This function is used to map document IDs and other keys to bucket numbers
in the hash index. The same key will always produce the same hash value
when using the same seed.

REFERENCES:
- Original implementation by Bob Jenkins
- Used in PostgreSQL and many other database systems
- Public domain algorithm
*/

// jenkinsHash implements the Bob Jenkins hash function (lookup3.c)
// This is the same hash function used by PostgreSQL for hash indexes
// Parameters:
//   - key: The string key to hash
//   - seed: A seed value to vary the hash function
//
// Returns:
//   - uint32: The computed hash value
func jenkinsHash(key string, seed uint32) uint32 {
	data := []byte(key)
	length := uint32(len(data))

	// Initialize hash values
	a := uint32(0xdeadbeef) + length + seed
	b := a
	c := a

	// Process data in 12-byte chunks
	i := uint32(0)
	for i+12 <= length {
		a += uint32(data[i]) | (uint32(data[i+1]) << 8) | (uint32(data[i+2]) << 16) | (uint32(data[i+3]) << 24)
		b += uint32(data[i+4]) | (uint32(data[i+5]) << 8) | (uint32(data[i+6]) << 16) | (uint32(data[i+7]) << 24)
		c += uint32(data[i+8]) | (uint32(data[i+9]) << 8) | (uint32(data[i+10]) << 16) | (uint32(data[i+11]) << 24)

		a, b, c = jenkinsMix(a, b, c)
		i += 12
	}

	// Handle remaining bytes
	remaining := length - i
	switch remaining {
	case 11:
		c += uint32(data[i+10]) << 24
		fallthrough
	case 10:
		c += uint32(data[i+9]) << 16
		fallthrough
	case 9:
		c += uint32(data[i+8]) << 8
		fallthrough
	case 8:
		b += uint32(data[i+7]) << 24
		fallthrough
	case 7:
		b += uint32(data[i+6]) << 16
		fallthrough
	case 6:
		b += uint32(data[i+5]) << 8
		fallthrough
	case 5:
		b += uint32(data[i+4])
		fallthrough
	case 4:
		a += uint32(data[i+3]) << 24
		fallthrough
	case 3:
		a += uint32(data[i+2]) << 16
		fallthrough
	case 2:
		a += uint32(data[i+1]) << 8
		fallthrough
	case 1:
		a += uint32(data[i])
	}

	// Final mixing
	_, _, c = jenkinsMix(a, b, c)
	return c
}

// jenkinsMix performs the mixing operation for the Jenkins hash function
// This function scrambles the bits to ensure good distribution
// Parameters:
//   - a, b, c: Three 32-bit values to mix
//
// Returns:
//   - Three mixed 32-bit values
func jenkinsMix(a, b, c uint32) (uint32, uint32, uint32) {
	a -= c
	a ^= rotateLeft(c, 4)
	c += b

	b -= a
	b ^= rotateLeft(a, 6)
	a += c

	c -= b
	c ^= rotateLeft(b, 8)
	b += a

	a -= c
	a ^= rotateLeft(c, 16)
	c += b

	b -= a
	b ^= rotateLeft(a, 19)
	a += c

	c -= b
	c ^= rotateLeft(b, 4)
	b += a

	return a, b, c
}

// rotateLeft performs a left rotation on a 32-bit value
// Parameters:
//   - value: The value to rotate
//   - bits: Number of bits to rotate left
//
// Returns:
//   - The rotated value
func rotateLeft(value uint32, bits uint) uint32 {
	return (value << bits) | (value >> (32 - bits))
}

// generateHashSeed creates a cryptographically random seed for the hash function
// This ensures different index instances have different hash distributions
// Returns:
//   - uint32: A random seed value
// func generateHashFunctionSeed() uint32 {
// 	var seedBytes [4]byte
// 	_, err := rand.Read(seedBytes[:])
// 	if err != nil {
// 		// Fallback to time-based seed if crypto/rand fails
// 		return uint32(time.Now().UnixNano())
// 	}

// 	return uint32(seedBytes[0]) |
// 		uint32(seedBytes[1])<<8 |
// 		uint32(seedBytes[2])<<16 |
// 		uint32(seedBytes[3])<<24
// }
