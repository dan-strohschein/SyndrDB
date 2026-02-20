package queryparser

import (
	"strings"
	"testing"
	"time"
)

// TestLikePattern_ExponentialBacktrackingPrevented verifies that the DP-based
// LIKE matcher handles pathological patterns in O(n*m) time, preventing ReDoS.
func TestLikePattern_ExponentialBacktrackingPrevented(t *testing.T) {
	// This pattern would cause exponential backtracking in a naive recursive implementation.
	pattern := "%a%a%a%a%a%a%a%a%a%a"
	value := strings.Repeat("b", 100)

	start := time.Now()
	result := matchLikePatternComplex(value, pattern)
	elapsed := time.Since(start)

	if result {
		t.Fatal("expected no match for pattern with no 'a' characters in value")
	}

	if elapsed >= 1*time.Second {
		t.Fatalf("LIKE pattern matching took %v, expected less than 1 second (possible ReDoS)", elapsed)
	}
}

// TestLikePattern_MaxPatternLengthEnforced verifies that patterns exceeding
// LikePatternMaxLength (1000) are rejected.
func TestLikePattern_MaxPatternLengthEnforced(t *testing.T) {
	pattern := strings.Repeat("a", 2000)
	result := matchLikePatternComplex("anything", pattern)

	if result {
		t.Fatal("expected false for pattern exceeding LikePatternMaxLength")
	}
}

// TestLikePattern_MaxWildcardsEnforced verifies that patterns with more than
// LikePatternMaxWildcards (20) percent wildcards are rejected.
func TestLikePattern_MaxWildcardsEnforced(t *testing.T) {
	pattern := strings.Repeat("%", 50)
	result := matchLikePatternComplex("anything", pattern)

	if result {
		t.Fatal("expected false for pattern exceeding LikePatternMaxWildcards")
	}
}

// TestLikePattern_SimplePatternStillWorks verifies that standard LIKE patterns
// with percent wildcards continue to match correctly.
func TestLikePattern_SimplePatternStillWorks(t *testing.T) {
	result := matchLikePatternComplex("say hello world", "%hello%")

	if !result {
		t.Fatal("expected true for '%hello%' matching 'say hello world'")
	}
}

// TestLikePattern_SingleCharWildcard verifies that the underscore wildcard
// matches exactly one character.
func TestLikePattern_SingleCharWildcard(t *testing.T) {
	result := matchLikePatternComplex("abc", "a_c")

	if !result {
		t.Fatal("expected true for 'a_c' matching 'abc'")
	}
}

// TestLikePattern_ExactMatch verifies that a pattern with no wildcards
// matches only the exact string.
func TestLikePattern_ExactMatch(t *testing.T) {
	result := matchLikePatternComplex("hello", "hello")

	if !result {
		t.Fatal("expected true for exact match of 'hello'")
	}
}

// TestLikePattern_NoMatch verifies that non-matching patterns return false.
func TestLikePattern_NoMatch(t *testing.T) {
	result := matchLikePatternComplex("hello", "world")

	if result {
		t.Fatal("expected false for 'world' not matching 'hello'")
	}
}
