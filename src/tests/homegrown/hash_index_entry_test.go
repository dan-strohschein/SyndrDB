package homegrown

import (
	"testing"
	"time"
)

// TestNewHashIndexEntry tests creation of a new entry
func TestNewHashIndexEntry(t *testing.T) {
	keyValue := "user@example.com"
	documentID := "550e8400-e29b-41d4-a716-446655440000"
	sequence := uint64(42)

	entry := NewHashIndexEntry(keyValue, documentID, sequence)

	// Verify basic fields
	if entry.KeyValue != keyValue {
		t.Errorf("Expected KeyValue %s, got %s", keyValue, entry.KeyValue)
	}
	if entry.DocumentID != documentID {
		t.Errorf("Expected DocumentID %s, got %s", documentID, entry.DocumentID)
	}
	if entry.Sequence != sequence {
		t.Errorf("Expected Sequence %d, got %d", sequence, entry.Sequence)
	}

	// Verify computed fields
	if entry.HashValue == 0 {
		t.Error("HashValue should not be zero")
	}
	if entry.Checksum == 0 {
		t.Error("Checksum should not be zero")
	}
	if entry.Deleted {
		t.Error("New entry should not be marked as deleted")
	}

	// Verify timestamp is recent
	if time.Since(entry.Timestamp) > time.Second {
		t.Error("Timestamp should be recent")
	}
}

// TestNewTombstoneEntry tests creation of a tombstone entry
func TestNewTombstoneEntry(t *testing.T) {
	keyValue := "user@example.com"
	sequence := uint64(100)

	entry := NewTombstoneEntry(keyValue, sequence)

	// Verify tombstone properties
	if !entry.Deleted {
		t.Error("Tombstone should be marked as deleted")
	}
	if entry.DocumentID != "" {
		t.Error("Tombstone should have empty DocumentID")
	}
	if entry.KeyValue != keyValue {
		t.Errorf("Expected KeyValue %s, got %s", keyValue, entry.KeyValue)
	}
	if entry.Sequence != sequence {
		t.Errorf("Expected Sequence %d, got %d", sequence, entry.Sequence)
	}
}

// TestEntrySerialization tests serialization and deserialization
func TestEntrySerialization(t *testing.T) {
	original := NewHashIndexEntry("test@example.com", "doc-123", 50)
	original.EntryID = 42
	original.BucketNum = 7

	// Serialize
	data, err := original.Serialize()
	if err != nil {
		t.Fatalf("Serialization failed: %v", err)
	}

	// Verify data is not empty
	if len(data) == 0 {
		t.Fatal("Serialized data is empty")
	}

	// Deserialize
	deserialized, bytesRead, err := DeserializeEntry(data)
	if err != nil {
		t.Fatalf("Deserialization failed: %v", err)
	}

	// Verify bytes read
	if bytesRead != len(data) {
		t.Errorf("Expected to read %d bytes, read %d", len(data), bytesRead)
	}

	// Verify all fields match
	if deserialized.EntryID != original.EntryID {
		t.Errorf("EntryID mismatch: expected %d, got %d", original.EntryID, deserialized.EntryID)
	}
	if deserialized.KeyValue != original.KeyValue {
		t.Errorf("KeyValue mismatch: expected %s, got %s", original.KeyValue, deserialized.KeyValue)
	}
	if deserialized.DocumentID != original.DocumentID {
		t.Errorf("DocumentID mismatch: expected %s, got %s", original.DocumentID, deserialized.DocumentID)
	}
	if deserialized.Sequence != original.Sequence {
		t.Errorf("Sequence mismatch: expected %d, got %d", original.Sequence, deserialized.Sequence)
	}
	if deserialized.HashValue != original.HashValue {
		t.Errorf("HashValue mismatch: expected %d, got %d", original.HashValue, deserialized.HashValue)
	}
	if deserialized.BucketNum != original.BucketNum {
		t.Errorf("BucketNum mismatch: expected %d, got %d", original.BucketNum, deserialized.BucketNum)
	}
	if deserialized.Deleted != original.Deleted {
		t.Errorf("Deleted mismatch: expected %v, got %v", original.Deleted, deserialized.Deleted)
	}

	// Verify timestamp is close (within 1ms due to nano precision)
	timeDiff := deserialized.Timestamp.Sub(original.Timestamp)
	if timeDiff < -time.Millisecond || timeDiff > time.Millisecond {
		t.Errorf("Timestamp mismatch: diff %v", timeDiff)
	}
}

// TestTombstoneSerialization tests tombstone serialization
func TestTombstoneSerialization(t *testing.T) {
	original := NewTombstoneEntry("deleted-key", 999)
	original.EntryID = 100

	// Serialize
	data, err := original.Serialize()
	if err != nil {
		t.Fatalf("Serialization failed: %v", err)
	}

	// Verify tombstone magic number is written
	if len(data) < 4 {
		t.Fatal("Serialized data too short")
	}

	// Deserialize
	deserialized, _, err := DeserializeEntry(data)
	if err != nil {
		t.Fatalf("Deserialization failed: %v", err)
	}

	// Verify tombstone flag
	if !deserialized.Deleted {
		t.Error("Deserialized entry should be marked as deleted")
	}
	if deserialized.KeyValue != original.KeyValue {
		t.Errorf("KeyValue mismatch: expected %s, got %s", original.KeyValue, deserialized.KeyValue)
	}
}

// TestSerializationWithLargeData tests serialization with large keys
func TestSerializationWithLargeData(t *testing.T) {
	// Create entry with large key (but within limits)
	largeKey := string(make([]byte, 1000))
	for i := range largeKey {
		largeKey = string(append([]byte(largeKey[:i]), byte('A'+(i%26))))
	}

	entry := NewHashIndexEntry(largeKey, "doc-id", 1)

	// Should serialize successfully
	data, err := entry.Serialize()
	if err != nil {
		t.Fatalf("Serialization failed: %v", err)
	}

	// Should deserialize successfully
	deserialized, _, err := DeserializeEntry(data)
	if err != nil {
		t.Fatalf("Deserialization failed: %v", err)
	}

	if deserialized.KeyValue != entry.KeyValue {
		t.Error("Large key not preserved during serialization")
	}
}

// TestSerializationErrors tests error cases in serialization
func TestSerializationErrors(t *testing.T) {
	// Test key too large
	tooLargeKey := string(make([]byte, MaxKeySize+1))
	entry := NewHashIndexEntry(tooLargeKey, "doc", 1)

	_, err := entry.Serialize()
	if err == nil {
		t.Error("Expected error for oversized key")
	}

	// Test document ID too large
	tooLargeDocID := string(make([]byte, MaxDocumentIDSize+1))
	entry2 := NewHashIndexEntry("key", tooLargeDocID, 1)

	_, err = entry2.Serialize()
	if err == nil {
		t.Error("Expected error for oversized document ID")
	}
}

// TestDeserializationErrors tests error cases in deserialization
func TestDeserializationErrors(t *testing.T) {
	// Test insufficient data
	shortData := make([]byte, 10)
	_, _, err := DeserializeEntry(shortData)
	if err == nil {
		t.Error("Expected error for insufficient data")
	}

	// Test invalid magic number
	invalidData := make([]byte, 100)
	invalidData[0] = 0xFF // Invalid magic
	_, _, err = DeserializeEntry(invalidData)
	if err == nil {
		t.Error("Expected error for invalid magic number")
	}
}

// TestComputeHash tests hash computation consistency
func TestComputeHash(t *testing.T) {
	key1 := "test@example.com"
	key2 := "test@example.com"
	key3 := "different@example.com"

	hash1 := ComputeHash(key1)
	hash2 := ComputeHash(key2)
	hash3 := ComputeHash(key3)

	// Same key should produce same hash
	if hash1 != hash2 {
		t.Error("Same key should produce same hash")
	}

	// Different keys should (probably) produce different hashes
	if hash1 == hash3 {
		t.Error("Different keys produced same hash (unlikely but possible)")
	}

	// Hash should be non-zero
	if hash1 == 0 {
		t.Error("Hash should not be zero")
	}
}

// TestIsNewer tests entry comparison logic
func TestIsNewer(t *testing.T) {
	// Create entries with different sequences
	entry1 := NewHashIndexEntry("key", "doc1", 100)
	entry2 := NewHashIndexEntry("key", "doc2", 200)

	if !entry2.IsNewer(entry1) {
		t.Error("Entry with higher sequence should be newer")
	}

	if entry1.IsNewer(entry2) {
		t.Error("Entry with lower sequence should not be newer")
	}

	// Test timestamp tie-breaker
	time.Sleep(time.Millisecond * 10)
	entry3 := NewHashIndexEntry("key", "doc3", 100) // Same sequence as entry1

	if !entry3.IsNewer(entry1) {
		t.Error("Entry with later timestamp should be newer when sequences match")
	}
}

// TestChecksumValidation tests checksum computation and validation
func TestChecksumValidation(t *testing.T) {
	entry := NewHashIndexEntry("test", "doc", 1)
	originalChecksum := entry.Checksum

	// Serialize and deserialize
	data, err := entry.Serialize()
	if err != nil {
		t.Fatalf("Serialization failed: %v", err)
	}

	deserialized, _, err := DeserializeEntry(data)
	if err != nil {
		t.Fatalf("Deserialization failed: %v", err)
	}

	// Checksum should match
	if deserialized.Checksum != originalChecksum {
		t.Error("Checksum mismatch after deserialization")
	}

	// Corrupt the data and verify checksum detects it
	data[50] ^= 0xFF // Flip some bits
	_, _, err = DeserializeEntry(data)
	if err == nil {
		t.Error("Expected error for corrupted data")
	}
}

// TestEntryString tests string representation
func TestEntryString(t *testing.T) {
	entry := NewHashIndexEntry("key", "doc", 1)
	str := entry.String()

	// Should contain key information
	if len(str) == 0 {
		t.Error("String representation is empty")
	}

	// Tombstone should show different status
	tombstone := NewTombstoneEntry("key", 2)
	tombstoneStr := tombstone.String()

	if tombstoneStr == str {
		t.Error("Tombstone string should differ from normal entry")
	}
}

// BenchmarkEntrySerialization benchmarks entry serialization
func BenchmarkEntrySerialization(b *testing.B) {
	entry := NewHashIndexEntry("benchmark@example.com", "doc-id-12345", 1000)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := entry.Serialize()
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkEntryDeserialization benchmarks entry deserialization
func BenchmarkEntryDeserialization(b *testing.B) {
	entry := NewHashIndexEntry("benchmark@example.com", "doc-id-12345", 1000)
	data, _ := entry.Serialize()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, err := DeserializeEntry(data)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkComputeHash benchmarks hash computation
func BenchmarkComputeHash(b *testing.B) {
	key := "benchmark@example.com"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = ComputeHash(key)
	}
}
