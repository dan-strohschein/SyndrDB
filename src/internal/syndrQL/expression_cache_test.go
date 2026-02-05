package syndrQL

import (
	"testing"
)

// TestExpressionCache_HashCollisionReturnsCorrectExpression verifies that when
// two different WHERE strings would produce the same cache key (FNV collision),
// Get returns the correct expression for each string by comparing the stored
// whereClause, not just the hash.
func TestExpressionCache_HashCollisionReturnsCorrectExpression(t *testing.T) {
	cache := NewExpressionCache(10, nil)

	// Parse two different WHERE clauses to get two distinct expressions
	tok1 := NewTokenizer("x == 1")
	tokens1, err := tok1.Tokenize()
	if err != nil {
		t.Fatal(err)
	}
	expr1, err := NewExpressionParser(tokens1, nil).Parse()
	if err != nil {
		t.Fatal(err)
	}

	tok2 := NewTokenizer("y == 2")
	tokens2, err := tok2.Tokenize()
	if err != nil {
		t.Fatal(err)
	}
	expr2, err := NewExpressionParser(tokens2, nil).Parse()
	if err != nil {
		t.Fatal(err)
	}

	// Simulate hash collision: both strings map to the same key
	prev := testHashKeyOverride
	testHashKeyOverride = func(string) uint64 { return 99999 }
	defer func() { testHashKeyOverride = prev }()

	cache.Put("x == 1", expr1, tokens1)
	cache.Put("y == 2", expr2, tokens2)

	// With same key, the second Put evicts the first, so only "y == 2" is in cache
	_, _, found1 := cache.Get("x == 1")
	got2, _, found2 := cache.Get("y == 2")

	// Without whereClause check we would wrongly return expr1 for "x == 1" (same key).
	// We correctly treat "x == 1" as miss after eviction.
	if found1 {
		t.Error("Get(\"x == 1\") should be miss (evicted by second Put with same key)")
	}
	if !found2 {
		t.Error("Get(\"y == 2\") should be a cache hit")
	}
	if found2 && got2 != expr2 {
		t.Error("Get(\"y == 2\") must return the expression for y == 2, not for x == 1")
	}
}
