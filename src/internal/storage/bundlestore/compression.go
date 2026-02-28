package bundlestore

import (
	"encoding/binary"
	"fmt"
	"sync"

	"github.com/klauspost/compress/zstd"
)

const (
	CompressionNone byte = 0x00
	CompressionZstd byte = 0x01
)

// Compressed document magic numbers (distinct from uncompressed)
const (
	CompressedDocMagic       uint32 = 0xDEADBEF1 // Compressed document
	CompressedTombstoneMagic uint32 = 0xDEADDEA1 // Compressed tombstone
)

// Encrypted document magic numbers (Enterprise encryption at rest)
const (
	EncryptedDocMagic           uint32 = 0xDEADBE00 // Encrypted uncompressed document
	EncryptedCompressedDocMagic uint32 = 0xDEADBE01 // Encrypted + compressed document
	EncryptedTombstoneMagic     uint32 = 0xDEADBE0D // Encrypted tombstone
	EncryptedCompressedTombMagic uint32 = 0xDEADBE1D // Encrypted + compressed tombstone
)

// Pooled zstd encoder/decoder to avoid allocation per call
var (
	zstdEncoderPool sync.Pool
	zstdDecoderPool sync.Pool
	zstdInitOnce    sync.Once
)

func initZstdPools() {
	zstdInitOnce.Do(func() {
		zstdEncoderPool = sync.Pool{
			New: func() interface{} {
				enc, _ := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedFastest))
				return enc
			},
		}
		zstdDecoderPool = sync.Pool{
			New: func() interface{} {
				dec, _ := zstd.NewReader(nil)
				return dec
			},
		}
	})
}

// CompressDocument compresses document bytes using the specified algorithm.
// Returns the compressed payload: [algo:1][uncompressedSize:4][compressedData]
func CompressDocument(data []byte, algo byte) ([]byte, error) {
	if algo != CompressionZstd {
		return nil, fmt.Errorf("unsupported compression algorithm: %d", algo)
	}

	initZstdPools()

	enc := zstdEncoderPool.Get().(*zstd.Encoder)
	defer zstdEncoderPool.Put(enc)

	compressed := enc.EncodeAll(data, nil)

	// Build payload: [algo:1][uncompressedSize:4][compressedData]
	payload := make([]byte, 1+4+len(compressed))
	payload[0] = algo
	binary.LittleEndian.PutUint32(payload[1:5], uint32(len(data)))
	copy(payload[5:], compressed)

	return payload, nil
}

// DecompressDocument decompresses a compressed document payload.
// Input format: [algo:1][uncompressedSize:4][compressedData]
// Returns the original uncompressed data.
func DecompressDocument(payload []byte) ([]byte, error) {
	if len(payload) < 5 {
		return nil, fmt.Errorf("compressed payload too short: %d bytes", len(payload))
	}

	algo := payload[0]
	uncompressedSize := binary.LittleEndian.Uint32(payload[1:5])
	compressedData := payload[5:]

	switch algo {
	case CompressionZstd:
		initZstdPools()

		dec := zstdDecoderPool.Get().(*zstd.Decoder)
		defer zstdDecoderPool.Put(dec)

		// Pre-allocate output buffer for efficiency
		output := make([]byte, 0, uncompressedSize)
		result, err := dec.DecodeAll(compressedData, output)
		if err != nil {
			return nil, fmt.Errorf("zstd decompression failed: %w", err)
		}
		return result, nil

	default:
		return nil, fmt.Errorf("unsupported compression algorithm: %d", algo)
	}
}
