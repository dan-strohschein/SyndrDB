package journal

import (
	"testing"
	"time"
)

func FuzzWALEntryDeserialize(f *testing.F) {
	// Create a valid serialized WAL entry as seed
	wal := &WriteAheadLog{}
	validEntry := WALEntry{
		LSN:        1,
		Timestamp:  time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		TxID:       "tx-001",
		Operation:  OpInsert,
		BundleName: "Users",
		DocumentID: "doc-001",
		BeforeData: "",
		AfterData:  `{"name":"Alice","age":30}`,
		Metadata:   "",
		Checksum:   0,
	}
	validBytes, err := wal.SerializeWALEntryBinary(validEntry)
	if err == nil {
		f.Add(validBytes)
	}

	// Additional seeds
	f.Add([]byte{})                                         // Empty
	f.Add([]byte{0x42, 0x4C, 0x41, 0x57})                  // Magic bytes reversed
	f.Add([]byte{0x57, 0x41, 0x4C, 0x42})                  // Magic only (4 bytes)
	f.Add(make([]byte, 53))                                 // Minimum size, all zeros
	f.Add([]byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF})      // Invalid data
	f.Add([]byte{0x57, 0x41, 0x4C, 0x42, 0x01, 0x00, 0x00, 0x00}) // Magic + version, truncated

	f.Fuzz(func(t *testing.T, data []byte) {
		entry, err := wal.DeserializeWALEntryBinary(data)

		if err != nil {
			// Deserialization error — fine, just no panic
			return
		}

		// Property: successful deserialization returns non-nil entry
		if entry == nil {
			t.Fatal("DeserializeWALEntryBinary returned nil without error")
		}

		// Property: roundtrip (deserialize → serialize → re-deserialize preserves DocumentID)
		reserialized, err := wal.SerializeWALEntryBinary(*entry)
		if err != nil {
			// Serialization might reject some edge-case values — that's okay
			return
		}

		reentry, err := wal.DeserializeWALEntryBinary(reserialized)
		if err != nil {
			t.Fatalf("roundtrip re-deserialization failed: %v", err)
		}
		if reentry.DocumentID != entry.DocumentID {
			t.Fatalf("roundtrip DocumentID mismatch: %q vs %q", entry.DocumentID, reentry.DocumentID)
		}
	})
}
