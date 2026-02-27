package index

import (
	"testing"
)

func FuzzExpressionCompiler(f *testing.F) {
	// Seed corpus: index expressions
	seeds := []string{
		// Simple function calls
		`LOWER("name")`,
		`UPPER("name")`,
		`TRIM("name")`,
		`ABS("balance")`,
		`CEIL("price")`,
		`FLOOR("price")`,
		`ROUND("price")`,
		// Nested functions
		`UPPER(TRIM("name"))`,
		`LOWER(TRIM("email"))`,
		// Arithmetic
		`"price" * "quantity"`,
		`"a" + "b"`,
		`"total" - "discount"`,
		`"a" / "b"`,
		`("price" + "tax") * "quantity"`,
		// COALESCE
		`COALESCE("nickname", "name")`,
		`COALESCE("a", "b", "c")`,
		// CONCAT
		`CONCAT("first", "last")`,
		`CONCAT("a", "b", "c")`,
		// SUBSTRING
		`SUBSTRING("name", 1, 3)`,
		`SUBSTRING("code", 0, 5)`,
		// Date functions
		`YEAR("created_at")`,
		`MONTH("created_at")`,
		// Unary negation
		`-"balance"`,
		// Complex expressions
		`LOWER(CONCAT("first", "last"))`,
		`ABS("price" - "cost")`,
		`ROUND("price" * 1.1)`,
		// Edge cases
		``,
		`(`,
		`)`,
		`LOWER()`,
		`LOWER("a", "b")`,
		`"field"`,
		`42`,
		`NOW()`,
		`AGE()`,
		`+++`,
	}

	for _, s := range seeds {
		f.Add(s)
	}

	f.Fuzz(func(t *testing.T, input string) {
		compiled, err := CompileIndexExpressionAST(input)

		if err != nil {
			// Compilation error — fine, just no panic
			return
		}

		// Property: successful compile returns non-nil with EvalFn and Normalized
		if compiled == nil {
			t.Fatal("CompileIndexExpressionAST returned nil without error")
		}
		if compiled.EvalFn == nil {
			t.Fatal("successful compile produced nil EvalFn")
		}
		if compiled.Normalized == "" {
			t.Fatal("successful compile produced empty Normalized string")
		}

		// Property: EvalFn doesn't panic on empty map
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("EvalFn panicked on empty map: %v", r)
				}
			}()
			_ = compiled.EvalFn(map[string]interface{}{})
		}()

		// Property: normalization is idempotent when re-parsable
		// Some normalized forms (e.g., scientific notation "1e-05") can't be re-parsed,
		// so we only check idempotency when re-compilation succeeds.
		recompiled, err := CompileIndexExpressionAST(compiled.Normalized)
		if err == nil && recompiled.Normalized != compiled.Normalized {
			t.Fatalf("normalization not idempotent: %q → %q", compiled.Normalized, recompiled.Normalized)
		}
	})
}
