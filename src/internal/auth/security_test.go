package auth

import (
	"path/filepath"
	"testing"
)

// ---------------------------------------------------------------------------
// Fix 3.1: SlowEqual — constant-time comparison via subtle.ConstantTimeCompare
// ---------------------------------------------------------------------------

func TestSlowEqual_EqualInputs(t *testing.T) {
	if !SlowEqual([]byte("hello"), []byte("hello")) {
		t.Fatal("SlowEqual should return true for identical inputs")
	}
}

func TestSlowEqual_DifferentInputs(t *testing.T) {
	if SlowEqual([]byte("hello"), []byte("world")) {
		t.Fatal("SlowEqual should return false for different inputs of the same length")
	}
}

func TestSlowEqual_DifferentLengths_ReturnsFalse(t *testing.T) {
	if SlowEqual([]byte("short"), []byte("longer string")) {
		t.Fatal("SlowEqual should return false when inputs have different lengths")
	}
}

func TestSlowEqual_EmptyInputs(t *testing.T) {
	if !SlowEqual([]byte{}, []byte{}) {
		t.Fatal("SlowEqual should return true for two empty byte slices")
	}
}

func TestSlowEqual_OneEmpty(t *testing.T) {
	if SlowEqual([]byte("a"), []byte{}) {
		t.Fatal("SlowEqual should return false when one input is empty")
	}
}

// ---------------------------------------------------------------------------
// Fix 3.2: Encryption key derivation — SHA-256 derives a 32-byte key from
// any non-empty input; empty keys are rejected.
// ---------------------------------------------------------------------------

func TestUserStore_ShortKeyDerived(t *testing.T) {
	filePath := filepath.Join(t.TempDir(), "users.enc")
	_, err := NewUserStore(filePath, "short-key!")
	if err != nil {
		t.Fatalf("NewUserStore should accept a short key (derived via sha256), got error: %v", err)
	}
}

func TestUserStore_LongKeyDerived(t *testing.T) {
	filePath := filepath.Join(t.TempDir(), "users.enc")
	longKey := "this-is-a-very-long-encryption-key-that-exceeds-thirty-two-bytes-and-keeps-going-for-a-while-to-test-derivation"
	_, err := NewUserStore(filePath, longKey)
	if err != nil {
		t.Fatalf("NewUserStore should accept a long key (derived via sha256), got error: %v", err)
	}
}

func TestUserStore_EmptyKeyRejected(t *testing.T) {
	filePath := filepath.Join(t.TempDir(), "users.enc")
	_, err := NewUserStore(filePath, "")
	if err == nil {
		t.Fatal("NewUserStore should return an error when the encryption key is empty")
	}
}
