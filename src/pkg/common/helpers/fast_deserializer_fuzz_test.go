package helpers

import (
	"testing"
)

func FuzzDocumentDeserializer(f *testing.F) {
	// Seed corpus: various byte patterns
	f.Add([]byte{})                    // Empty
	f.Add([]byte{0x00})               // Single zero byte
	f.Add([]byte{0x02})               // V2 marker byte
	f.Add([]byte{0xFF})               // Invalid marker
	f.Add(make([]byte, 4))            // Short data
	f.Add(make([]byte, 64))           // Medium zeros
	f.Add([]byte{0x00, 0x00, 0x00, 0x00, 0x00}) // Length prefix + empty

	// Minimal V1-like structure: 4-byte total length + 2-byte docID length + docID + 4-byte field count
	minV1 := make([]byte, 0, 20)
	// Total length (little-endian uint32): 16
	minV1 = append(minV1, 0x10, 0x00, 0x00, 0x00)
	// DocID length (little-endian uint16): 5
	minV1 = append(minV1, 0x05, 0x00)
	// DocID: "doc01"
	minV1 = append(minV1, 'd', 'o', 'c', '0', '1')
	// Field count (little-endian uint32): 0
	minV1 = append(minV1, 0x00, 0x00, 0x00, 0x00)
	// Padding to match declared length
	minV1 = append(minV1, 0x00)
	f.Add(minV1)

	f.Fuzz(func(t *testing.T, data []byte) {
		// Test DecodeFastBinary (returns map[string]interface{})
		result, err := DecodeFastBinary(data)
		if err != nil {
			// Deserialization error — fine, just no panic
		} else if result == nil {
			// nil result without error is unexpected but not necessarily wrong
		}

		// Test DecodeFastBinaryToDocument (returns *Document)
		doc, err := DecodeFastBinaryToDocument(data, nil)
		if err != nil {
			// Deserialization error — fine, just no panic
		} else if doc != nil {
			// Property: successful decode produces a document
			_ = doc.DocumentID
		}

		// Test DecodeFastBinaryAuto (auto-detects V1/V2)
		docAuto, err := DecodeFastBinaryAuto(data, nil)
		if err != nil {
			// fine
		} else if docAuto != nil {
			_ = docAuto.DocumentID
		}
	})
}
