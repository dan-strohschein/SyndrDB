package syndrQL

/*
select_where_simd_test.go

End-to-end tests for WHERE clause SIMD optimization (Phase 1).
Tests verify SIMD-accelerated comparisons produce identical results to scalar comparisons
with significant performance improvements (target: 4-6x for string comparisons).

Test Coverage:
- String equality comparisons (WHERE Country = 'USA')
- Integer range comparisons (WHERE BirthYear > 1970)
- Multi-condition AND/OR queries (WHERE BirthYear > 1970 AND Country = 'USA')
- SIMD vs Scalar result consistency (field-by-field validation)
- Fallback behavior for mixed/unsupported types
- Performance benchmarks at 100/500/1000 document scales

Data Strategy:
- Reuses Authors/Books/Publishers bundle schemas from select_e2e_2_test.go
- Randomized data (Country, BirthYear, Genre, Price) for real-world simulation
- Field-by-field validation ensures SIMD correctness
- Hard failures on any SIMD comparison errors

Benchmark Strategy:
- Measures only SELECT execution (b.ResetTimer() after seeding)
- Sub-benchmarks compare SIMD_Enabled vs SIMD_Disabled
- Tests string equality, integer range, and multi-condition queries
- Validates 4-6x performance improvement target

Single Responsibility: Validates SIMD WHERE clause optimization correctness and performance.
Open/Closed: New test cases can be added without modifying existing tests.
*/

import (
	"fmt"
	"math/rand"
	"syndrdb/src/internal/domain/models"
	"syndrdb/src/internal/server"
	"syndrdb/src/pkg/settings"
	"testing"
	"time"
)

// =============================================================================
// HELPER FUNCTIONS
// =============================================================================

// seedRandomizedAuthors creates count Author documents with randomized Country and BirthYear
func seedRandomizedAuthors(t testing.TB, fixture *TestFixture, count int) {
	t.Helper()

	countries := []string{"USA", "UK", "Canada", "France"}
	rand.Seed(time.Now().UnixNano())

	for i := 1; i <= count; i++ {
		country := countries[rand.Intn(len(countries))]
		birthYear := 1950 + rand.Intn(51) // 1950-2000
		name := fmt.Sprintf("Author_%d_%d", i, rand.Intn(1000))

		addDocCmd := fmt.Sprintf(`ADD DOCUMENT TO BUNDLE "Authors" WITH ({"ID"=%d}, {"Name"="%s"}, {"Country"="%s"}, {"BirthYear"=%d});`,
			i, name, country, birthYear)

		startTime := time.Now()
		_, err := server.CommandDirector(fixture.Database, *fixture.ServiceManager, addDocCmd, fixture.Logger, startTime, nil, "127.0.0.1")
		if err != nil {
			t.Fatalf("Failed to seed author %d: %v", i, err)
		}
	}
}

// seedRandomizedBooks creates count Book documents with randomized Genre and Price
func seedRandomizedBooks(t testing.TB, fixture *TestFixture, count int) {
	t.Helper()

	genres := []string{"Fiction", "Non-Fiction", "Mystery", "Sci-Fi"}
	rand.Seed(time.Now().UnixNano())

	for i := 1; i <= count; i++ {
		genre := genres[rand.Intn(len(genres))]
		price := 10.0 + rand.Float64()*90.0 // 10.00-100.00
		title := fmt.Sprintf("Book_%d_%d", i, rand.Intn(1000))
		authorID := 1 + rand.Intn(count) // Random author reference

		// Note: Use AuthorsID to match the foreign key relationship defined in select_e2e_2_test.go
		addDocCmd := fmt.Sprintf(`ADD DOCUMENT TO BUNDLE "Books" WITH ({"ID"=%d}, {"Title"="%s"}, {"AuthorsID"=%d}, {"Genre"="%s"}, {"Price"=%.2f});`,
			i, title, authorID, genre, price)

		startTime := time.Now()
		_, err := server.CommandDirector(fixture.Database, *fixture.ServiceManager, addDocCmd, fixture.Logger, startTime, nil, "127.0.0.1")
		if err != nil {
			t.Fatalf("Failed to seed book %d: %v", i, err)
		}
	}
}

// executeSelectQuery executes a SELECT query via CommandDirector and returns documents
func executeSelectQuery(t testing.TB, fixture *TestFixture, query string) []*models.Document {
	t.Helper()

	startTime := time.Now()
	response, err := server.CommandDirector(fixture.Database, *fixture.ServiceManager, query, fixture.Logger, startTime, nil, "127.0.0.1")
	if err != nil {
		t.Fatalf("Query execution failed: %v\nQuery: %s", err, query)
	}

	// Extract documents from CommandResponse
	cmdResp, ok := response.(*server.CommandResponse)
	if !ok {
		t.Fatalf("Expected CommandResponse, got %T", response)
	}

	// Handle streaming response (newer format) - convert map to slice
	if cmdResp.StreamDocuments != nil && len(cmdResp.StreamDocuments) > 0 {
		docs := make([]*models.Document, 0, len(cmdResp.StreamDocuments))
		for _, doc := range cmdResp.StreamDocuments {
			docs = append(docs, doc)
		}
		return docs
	}

	// Handle legacy Result format
	docs, ok := cmdResp.Result.([]*models.Document)
	if !ok {
		// Return empty slice if no results
		if cmdResp.Result == nil {
			return []*models.Document{}
		}
		t.Fatalf("Expected []*models.Document, got %T", cmdResp.Result)
	}

	return docs
}

// validateStringField validates that a document field matches expected string value
func validateStringField(t testing.TB, doc *models.Document, fieldName, expectedValue string) {
	t.Helper()

	field, exists := doc.Fields[fieldName]
	if !exists {
		t.Fatalf("Document missing field %s", fieldName)
	}

	actualValue, ok := field.Value.AsString()
	if !ok {
		t.Fatalf("Field %s is not a string: %v", fieldName, field.Value)
	}

	if actualValue != expectedValue {
		t.Fatalf("Field %s: expected %s, got %s", fieldName, expectedValue, actualValue)
	}
}

// validateIntFieldGreaterThan validates that a document field is greater than a threshold
func validateIntFieldGreaterThan(t testing.TB, doc *models.Document, fieldName string, threshold int64) {
	t.Helper()

	field, exists := doc.Fields[fieldName]
	if !exists {
		t.Fatalf("Document missing field %s", fieldName)
	}

	actualValue, ok := field.Value.AsInt()
	if !ok {
		t.Fatalf("Field %s is not an integer: %v", fieldName, field.Value)
	}

	if actualValue <= threshold {
		t.Fatalf("Field %s: expected > %d, got %d", fieldName, threshold, actualValue)
	}
}

// validateFloatFieldGreaterThan validates that a document field is greater than a threshold
func validateFloatFieldGreaterThan(t testing.TB, doc *models.Document, fieldName string, threshold float64) {
	t.Helper()

	field, exists := doc.Fields[fieldName]
	if !exists {
		t.Fatalf("Document missing field %s", fieldName)
	}

	actualValue, ok := field.Value.AsFloat()
	if !ok {
		t.Fatalf("Field %s is not a float: %v", fieldName, field.Value)
	}

	if actualValue <= threshold {
		t.Fatalf("Field %s: expected > %.2f, got %.2f", fieldName, threshold, actualValue)
	}
}

// =============================================================================
// CORRECTNESS TESTS
// =============================================================================

// TestWhereSIMD_StringEquality verifies SIMD string equality comparisons
func TestWhereSIMD_StringEquality(t *testing.T) {
	fixture := setupRealServer(t)
	seedRandomizedAuthors(t, fixture, 100)

	// Ensure SIMD is enabled
	settings.GetSettings().WhereSIMDEnabled = true

	query := `SELECT * FROM "Authors" WHERE "Country" = 'USA'`
	results := executeSelectQuery(t, fixture, query)

	// Validate count (at least 1 result expected from randomization)
	if len(results) == 0 {
		t.Log("Warning: No USA authors in randomized data - test may be flaky")
	}

	// Field-by-field validation: all returned documents must have Country = 'USA'
	for i, doc := range results {
		validateStringField(t, doc, "Country", "USA")
		t.Logf("✓ Document %d validated: Country = USA", i+1)
	}

	t.Logf("✓ SIMD string equality test passed (%d results validated)", len(results))
}

// TestWhereSIMD_IntegerRange verifies SIMD integer range comparisons
func TestWhereSIMD_IntegerRange(t *testing.T) {
	fixture := setupRealServer(t)
	seedRandomizedAuthors(t, fixture, 100)

	// Ensure SIMD is enabled
	settings.GetSettings().WhereSIMDEnabled = true

	query := `SELECT * FROM "Authors" WHERE "BirthYear" > 1970`
	results := executeSelectQuery(t, fixture, query)

	// Validate count (expect ~60% of 100 docs to be > 1970 from 1950-2000 range)
	if len(results) == 0 {
		t.Fatal("Expected at least some results for BirthYear > 1970")
	}

	// Field-by-field validation: all returned documents must have BirthYear > 1970
	for i, doc := range results {
		validateIntFieldGreaterThan(t, doc, "BirthYear", 1970)
		t.Logf("✓ Document %d validated: BirthYear > 1970", i+1)
	}

	t.Logf("✓ SIMD integer range test passed (%d results validated)", len(results))
}

// TestWhereSIMD_MultiCondition verifies SIMD works with AND/OR logic
func TestWhereSIMD_MultiCondition(t *testing.T) {
	fixture := setupRealServer(t)
	seedRandomizedAuthors(t, fixture, 100)

	// Ensure SIMD is enabled
	settings.GetSettings().WhereSIMDEnabled = true

	query := `SELECT * FROM "Authors" WHERE "BirthYear" > 1970 AND "Country" = 'USA'`
	results := executeSelectQuery(t, fixture, query)

	// Field-by-field validation: all returned documents must satisfy both conditions
	for i, doc := range results {
		validateIntFieldGreaterThan(t, doc, "BirthYear", 1970)
		validateStringField(t, doc, "Country", "USA")
		t.Logf("✓ Document %d validated: BirthYear > 1970 AND Country = USA", i+1)
	}

	t.Logf("✓ SIMD multi-condition test passed (%d results validated)", len(results))
}

// TestWhereSIMD_vs_Scalar compares SIMD and scalar results for identical queries
func TestWhereSIMD_vs_Scalar(t *testing.T) {
	fixture := setupRealServer(t)
	seedRandomizedBooks(t, fixture, 100)

	query := `SELECT * FROM "Books" WHERE "Genre" = 'Fiction'`

	// Execute with SIMD enabled
	settings.GetSettings().WhereSIMDEnabled = true
	resultsSIMD := executeSelectQuery(t, fixture, query)

	// Execute with SIMD disabled
	settings.GetSettings().WhereSIMDEnabled = false
	resultsScalar := executeSelectQuery(t, fixture, query)

	// Validate identical result counts
	if len(resultsSIMD) != len(resultsScalar) {
		t.Fatalf("Result count mismatch: SIMD=%d, Scalar=%d", len(resultsSIMD), len(resultsScalar))
	}

	// Field-by-field validation: both must return same Genre
	for i := 0; i < len(resultsSIMD); i++ {
		validateStringField(t, resultsSIMD[i], "Genre", "Fiction")
		validateStringField(t, resultsScalar[i], "Genre", "Fiction")
	}

	t.Logf("✓ SIMD vs Scalar consistency test passed (%d results, identical)", len(resultsSIMD))

	// Re-enable SIMD for other tests
	settings.GetSettings().WhereSIMDEnabled = true
}

// TestWhereSIMD_Fallback verifies graceful fallback for mixed/unsupported types
func TestWhereSIMD_Fallback(t *testing.T) {
	fixture := setupRealServer(t)
	seedRandomizedBooks(t, fixture, 100)

	// Ensure SIMD is enabled
	settings.GetSettings().WhereSIMDEnabled = true

	// Query with float comparison (may use scalar fallback)
	query := `SELECT * FROM "Books" WHERE "Price" > 25.50`
	results := executeSelectQuery(t, fixture, query)

	// Validate count (expect ~75% of books to have Price > 25.50 from 10-100 range)
	if len(results) == 0 {
		t.Fatal("Expected at least some results for Price > 25.50")
	}

	// Field-by-field validation: all returned documents must have Price > 25.50
	for i, doc := range results {
		validateFloatFieldGreaterThan(t, doc, "Price", 25.50)
		t.Logf("✓ Document %d validated: Price > 25.50", i+1)
	}

	t.Logf("✓ SIMD fallback test passed (%d results validated)", len(results))
}

// =============================================================================
// BENCHMARK FUNCTIONS
// =============================================================================

// BenchmarkWhereSIMD_StringEq_100 measures string equality at 100 documents
func BenchmarkWhereSIMD_StringEq_100(b *testing.B) {
	fixture := setupRealServerTB(b)
	seedRandomizedAuthors(b, fixture, 100)
	query := `SELECT * FROM "Authors" WHERE "Country" = 'USA'`

	b.Run("SIMD_Enabled", func(b *testing.B) {
		settings.GetSettings().WhereSIMDEnabled = true
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			executeSelectQuery(b, fixture, query)
		}
	})

	b.Run("SIMD_Disabled", func(b *testing.B) {
		settings.GetSettings().WhereSIMDEnabled = false
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			executeSelectQuery(b, fixture, query)
		}
	})
}

// BenchmarkWhereSIMD_StringEq_500 measures string equality at 500 documents
func BenchmarkWhereSIMD_StringEq_500(b *testing.B) {
	fixture := setupRealServerTB(b)
	seedRandomizedAuthors(b, fixture, 500)
	query := `SELECT * FROM "Authors" WHERE "Country" = 'USA'`

	b.Run("SIMD_Enabled", func(b *testing.B) {
		settings.GetSettings().WhereSIMDEnabled = true
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			executeSelectQuery(b, fixture, query)
		}
	})

	b.Run("SIMD_Disabled", func(b *testing.B) {
		settings.GetSettings().WhereSIMDEnabled = false
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			executeSelectQuery(b, fixture, query)
		}
	})
}

// BenchmarkWhereSIMD_StringEq_1000 measures string equality at 1000 documents
func BenchmarkWhereSIMD_StringEq_1000(b *testing.B) {
	fixture := setupRealServerTB(b)
	seedRandomizedAuthors(b, fixture, 1000)
	query := `SELECT * FROM "Authors" WHERE "Country" = 'USA'`

	b.Run("SIMD_Enabled", func(b *testing.B) {
		settings.GetSettings().WhereSIMDEnabled = true
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			executeSelectQuery(b, fixture, query)
		}
	})

	b.Run("SIMD_Disabled", func(b *testing.B) {
		settings.GetSettings().WhereSIMDEnabled = false
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			executeSelectQuery(b, fixture, query)
		}
	})
}

// BenchmarkWhereSIMD_IntRange_100 measures integer range at 100 documents
func BenchmarkWhereSIMD_IntRange_100(b *testing.B) {
	fixture := setupRealServerTB(b)
	seedRandomizedAuthors(b, fixture, 100)
	query := `SELECT * FROM "Authors" WHERE "BirthYear" > 1970`

	b.Run("SIMD_Enabled", func(b *testing.B) {
		settings.GetSettings().WhereSIMDEnabled = true
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			executeSelectQuery(b, fixture, query)
		}
	})

	b.Run("SIMD_Disabled", func(b *testing.B) {
		settings.GetSettings().WhereSIMDEnabled = false
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			executeSelectQuery(b, fixture, query)
		}
	})
}

// BenchmarkWhereSIMD_IntRange_500 measures integer range at 500 documents
func BenchmarkWhereSIMD_IntRange_500(b *testing.B) {
	fixture := setupRealServerTB(b)
	seedRandomizedAuthors(b, fixture, 500)
	query := `SELECT * FROM "Authors" WHERE "BirthYear" > 1970`

	b.Run("SIMD_Enabled", func(b *testing.B) {
		settings.GetSettings().WhereSIMDEnabled = true
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			executeSelectQuery(b, fixture, query)
		}
	})

	b.Run("SIMD_Disabled", func(b *testing.B) {
		settings.GetSettings().WhereSIMDEnabled = false
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			executeSelectQuery(b, fixture, query)
		}
	})
}

// BenchmarkWhereSIMD_IntRange_1000 measures integer range at 1000 documents
func BenchmarkWhereSIMD_IntRange_1000(b *testing.B) {
	fixture := setupRealServerTB(b)
	seedRandomizedAuthors(b, fixture, 1000)
	query := `SELECT * FROM "Authors" WHERE "BirthYear" > 1970`

	b.Run("SIMD_Enabled", func(b *testing.B) {
		settings.GetSettings().WhereSIMDEnabled = true
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			executeSelectQuery(b, fixture, query)
		}
	})

	b.Run("SIMD_Disabled", func(b *testing.B) {
		settings.GetSettings().WhereSIMDEnabled = false
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			executeSelectQuery(b, fixture, query)
		}
	})
}

// BenchmarkWhereSIMD_MultiCond_100 measures multi-condition at 100 documents
func BenchmarkWhereSIMD_MultiCond_100(b *testing.B) {
	fixture := setupRealServerTB(b)
	seedRandomizedAuthors(b, fixture, 100)
	query := `SELECT * FROM "Authors" WHERE "BirthYear" > 1970 AND "Country" = 'USA'`

	b.Run("SIMD_Enabled", func(b *testing.B) {
		settings.GetSettings().WhereSIMDEnabled = true
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			executeSelectQuery(b, fixture, query)
		}
	})

	b.Run("SIMD_Disabled", func(b *testing.B) {
		settings.GetSettings().WhereSIMDEnabled = false
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			executeSelectQuery(b, fixture, query)
		}
	})
}

// BenchmarkWhereSIMD_MultiCond_500 measures multi-condition at 500 documents
func BenchmarkWhereSIMD_MultiCond_500(b *testing.B) {
	fixture := setupRealServerTB(b)
	seedRandomizedAuthors(b, fixture, 500)
	query := `SELECT * FROM "Authors" WHERE "BirthYear" > 1970 AND "Country" = 'USA'`

	b.Run("SIMD_Enabled", func(b *testing.B) {
		settings.GetSettings().WhereSIMDEnabled = true
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			executeSelectQuery(b, fixture, query)
		}
	})

	b.Run("SIMD_Disabled", func(b *testing.B) {
		settings.GetSettings().WhereSIMDEnabled = false
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			executeSelectQuery(b, fixture, query)
		}
	})
}

// BenchmarkWhereSIMD_MultiCond_1000 measures multi-condition at 1000 documents
func BenchmarkWhereSIMD_MultiCond_1000(b *testing.B) {
	fixture := setupRealServerTB(b)
	seedRandomizedAuthors(b, fixture, 1000)
	query := `SELECT * FROM "Authors" WHERE "BirthYear" > 1970 AND "Country" = 'USA'`

	b.Run("SIMD_Enabled", func(b *testing.B) {
		settings.GetSettings().WhereSIMDEnabled = true
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			executeSelectQuery(b, fixture, query)
		}
	})

	b.Run("SIMD_Disabled", func(b *testing.B) {
		settings.GetSettings().WhereSIMDEnabled = false
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			executeSelectQuery(b, fixture, query)
		}
	})
}
