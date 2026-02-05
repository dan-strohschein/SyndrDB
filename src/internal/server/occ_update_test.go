package server

import (
	"errors"
	"fmt"
	"testing"

	bndle "syndrdb/src/internal/domain/bundle"
)

// TestIsRCUWriteConflict_ErrWriteConflictReturnsTrue verifies that
// isRCUWriteConflict returns true for bndle.ErrWriteConflict.
func TestIsRCUWriteConflict_ErrWriteConflictReturnsTrue(t *testing.T) {
	if !isRCUWriteConflict(bndle.ErrWriteConflict) {
		t.Error("expected isRCUWriteConflict(ErrWriteConflict) to be true")
	}
}

// TestIsRCUWriteConflict_NilReturnsFalse verifies that isRCUWriteConflict
// returns false for nil.
func TestIsRCUWriteConflict_NilReturnsFalse(t *testing.T) {
	if isRCUWriteConflict(nil) {
		t.Error("expected isRCUWriteConflict(nil) to be false")
	}
}

// TestIsRCUWriteConflict_OtherErrorReturnsFalse verifies that
// isRCUWriteConflict returns false for a non-write-conflict error.
func TestIsRCUWriteConflict_OtherErrorReturnsFalse(t *testing.T) {
	err := errors.New("some other error")
	if isRCUWriteConflict(err) {
		t.Error("expected isRCUWriteConflict(other error) to be false")
	}
}

// TestIsRCUWriteConflict_WrappedErrWriteConflictReturnsTrue verifies that
// isRCUWriteConflict returns true when the error wraps ErrWriteConflict
// (errors.Is semantics).
func TestIsRCUWriteConflict_WrappedErrWriteConflictReturnsTrue(t *testing.T) {
	wrapped := fmt.Errorf("context: %w", bndle.ErrWriteConflict)
	if !isRCUWriteConflict(wrapped) {
		t.Error("expected isRCUWriteConflict(wrapped ErrWriteConflict) to be true")
	}
}
