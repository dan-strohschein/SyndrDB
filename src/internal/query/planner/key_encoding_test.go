package planner

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func TestEncodeCompositeKey_TwoFields(t *testing.T) {
	encoder := NewKeyEncoder()

	key, err := encoder.EncodeCompositeKey([]interface{}{"alice", int64(25)})
	if err != nil {
		t.Fatalf("encode failed: %v", err)
	}

	// First component: "alice" (5 bytes) with 4-byte length prefix
	expectedLen1 := uint32(5) // "alice"
	gotLen1 := binary.BigEndian.Uint32(key[0:4])
	if gotLen1 != expectedLen1 {
		t.Errorf("first component length: expected %d, got %d", expectedLen1, gotLen1)
	}
	if string(key[4:9]) != "alice" {
		t.Errorf("first component value: expected 'alice', got '%s'", string(key[4:9]))
	}

	// Second component: "25" (2 bytes for string representation of int64)
	gotLen2 := binary.BigEndian.Uint32(key[9:13])
	expectedLen2 := uint32(2)
	if gotLen2 != expectedLen2 {
		t.Errorf("second component length: expected %d, got %d", expectedLen2, gotLen2)
	}
}

func TestEncodeCompositeKey_NilComponent(t *testing.T) {
	encoder := NewKeyEncoder()

	key, err := encoder.EncodeCompositeKey([]interface{}{"hello", nil, "world"})
	if err != nil {
		t.Fatalf("encode failed: %v", err)
	}

	// First component: "hello"
	gotLen1 := binary.BigEndian.Uint32(key[0:4])
	if gotLen1 != 5 {
		t.Fatalf("first component length: expected 5, got %d", gotLen1)
	}

	// Second component: nil (0-length)
	offset := 4 + int(gotLen1) // past first component
	gotLen2 := binary.BigEndian.Uint32(key[offset : offset+4])
	if gotLen2 != 0 {
		t.Errorf("nil component length: expected 0, got %d", gotLen2)
	}

	// Third component: "world"
	offset += 4
	gotLen3 := binary.BigEndian.Uint32(key[offset : offset+4])
	if gotLen3 != 5 {
		t.Errorf("third component length: expected 5, got %d", gotLen3)
	}
}

func TestEncodeCompositeKey_EqualityMatch(t *testing.T) {
	encoder := NewKeyEncoder()

	// Same inputs should produce identical composite keys (for point queries)
	key1, _ := encoder.EncodeCompositeKey([]interface{}{"alice", int64(25)})
	key2, _ := encoder.EncodeCompositeKey([]interface{}{"alice", int64(25)})

	if !bytes.Equal(key1, key2) {
		t.Error("same inputs should produce identical composite keys")
	}

	// Different inputs should produce different keys
	key3, _ := encoder.EncodeCompositeKey([]interface{}{"alice", int64(30)})
	if bytes.Equal(key1, key3) {
		t.Error("different inputs should produce different composite keys")
	}

	// Same-length strings preserve order: (aaa, 25) < (bbb, 25)
	key4, _ := encoder.EncodeCompositeKey([]interface{}{"aaa", int64(25)})
	key5, _ := encoder.EncodeCompositeKey([]interface{}{"bbb", int64(25)})
	if bytes.Compare(key4, key5) >= 0 {
		t.Error("expected (aaa, 25) < (bbb, 25) in byte order for same-length strings")
	}
}

func TestEncodeCompositeKey_SingleField(t *testing.T) {
	encoder := NewKeyEncoder()

	key, err := encoder.EncodeCompositeKey([]interface{}{"test"})
	if err != nil {
		t.Fatalf("encode failed: %v", err)
	}

	gotLen := binary.BigEndian.Uint32(key[0:4])
	if gotLen != 4 {
		t.Errorf("expected length 4, got %d", gotLen)
	}
	if string(key[4:8]) != "test" {
		t.Errorf("expected 'test', got '%s'", string(key[4:8]))
	}
}
