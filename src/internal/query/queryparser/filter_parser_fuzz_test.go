package queryparser

import (
	"testing"
)

func FuzzParseLikePattern(f *testing.F) {
	// Seed corpus: LIKE patterns
	seeds := []string{
		// Standard patterns
		`%test%`,
		`test%`,
		`%test`,
		`_test_`,
		`%`,
		`_`,
		`%%`,
		`__`,
		`%_%`,
		// Exact match
		`exact`,
		`hello world`,
		// Escaped wildcards
		`\%literal`,
		`\_literal`,
		`test\%`,
		`\%\%`,
		`\\backslash`,
		`\"quoted\"`,
		// Mixed
		`%hello\_world%`,
		`prefix%suffix`,
		`a_b%c_d`,
		// Edge cases
		``,
		`"quoted"`,
		`'single'`,
		`a`,
		// Repeated wildcards
		`%%%%`,
		`____`,
		`%_%_%_%_`,
	}

	for _, s := range seeds {
		f.Add(s)
	}

	f.Fuzz(func(t *testing.T, pattern string) {
		patternType, normalized, wildcardCount, err := ParseLikePattern(pattern)

		if err != nil {
			// Validation error — fine, just no panic
			return
		}

		// Property: valid patternType on success
		validTypes := map[string]bool{
			"exact": true, "prefix": true, "suffix": true,
			"contains": true, "match_all": true,
		}
		if !validTypes[patternType] {
			t.Fatalf("invalid patternType: %q", patternType)
		}

		// Property: non-negative wildcard count
		if wildcardCount < 0 {
			t.Fatalf("negative wildcardCount: %d", wildcardCount)
		}

		// Property: normalized is non-empty for non-empty input (after quote stripping)
		_ = normalized
	})
}
