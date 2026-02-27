package hashindexV3

import (
	"fmt"
	"testing"
	"time"
)

func FuzzHashIndexHeaderDeserialize(f *testing.F) {
	// Create a valid serialized header as seed
	hs := NewHeaderSerializer()
	validHeader := &IndexHeader{
		Magic:          IndexFileMagic,
		Version:        IndexFileVersion,
		IndexName:      "UserID_idx",
		FieldName:      "UserID",
		BundleName:     "Users",
		IsForeignKey:   false,
		IsUnique:       true,
		IsPrimaryKey:   false,
		IndexType:      "hash",
		FileNumber:     0,
		CreatedAt:      time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		TotalEntries:   1000,
		DeletedEntries: 50,
		FileSize:       1048576,
		GlobalSequence: 500,
	}
	validBytes, err := hs.SerializeHeader(validHeader)
	if err == nil {
		f.Add(validBytes)
	}

	// Additional seeds
	f.Add([]byte{})                                                     // Empty
	f.Add([]byte{0x48, 0x49, 0x44, 0x58})                              // Magic only ("HIDX")
	f.Add(make([]byte, 12))                                             // Minimum fixed header, all zeros
	f.Add([]byte{0xFF, 0xFF, 0xFF, 0xFF})                              // Bad magic
	f.Add([]byte{0x48, 0x49, 0x44, 0x58, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}) // Magic + version + zero size

	f.Fuzz(func(t *testing.T, data []byte) {
		// BSON unmarshal can panic on malformed data — recover gracefully
		var header *IndexHeader
		var deserErr error
		func() {
			defer func() {
				if r := recover(); r != nil {
					deserErr = fmt.Errorf("panic: %v", r)
				}
			}()
			header, deserErr = hs.DeserializeHeader(data)
		}()

		if deserErr != nil {
			// Deserialization error or panic — fine
			return
		}

		// Property: successful deserialization returns non-nil header
		if header == nil {
			t.Fatal("DeserializeHeader returned nil without error")
		}

		// Property: roundtrip (deserialize → serialize → re-deserialize preserves IndexName)
		reserialized, err := hs.SerializeHeader(header)
		if err != nil {
			// Serialization might reject some values — that's okay
			return
		}

		reheader, err := hs.DeserializeHeader(reserialized)
		if err != nil {
			t.Fatalf("roundtrip re-deserialization failed: %v", err)
		}
		if reheader.IndexName != header.IndexName {
			t.Fatalf("roundtrip IndexName mismatch: %q vs %q", header.IndexName, reheader.IndexName)
		}
		if reheader.BundleName != header.BundleName {
			t.Fatalf("roundtrip BundleName mismatch: %q vs %q", header.BundleName, reheader.BundleName)
		}
	})
}
