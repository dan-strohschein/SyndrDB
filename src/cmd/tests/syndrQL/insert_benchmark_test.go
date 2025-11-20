package syndrQL

import (
	"fmt"
	"syndrdb/src/internal/server"
	"testing"
	"time"
)

// BenchmarkInsert_Single benchmarks single document INSERT operations
// This directly measures WAL batching impact (Priority 2 optimization)
func BenchmarkInsert_Single(b *testing.B) {
	// Setup once outside the measured loop
	fixture := setupRealServerTB(b)

	// Reset timer to exclude setup time from measurements
	b.ResetTimer()
	b.ReportAllocs()

	// The actual benchmark loop - each iteration inserts one document
	for i := 0; i < b.N; i++ {
		query := fmt.Sprintf(`ADD DOCUMENT TO BUNDLE "Authors" WITH ({"ID"=%d}, {"Name"="%s"}, {"Country"="USA"});`,
			i, fmt.Sprintf("Author_%d", i))

		_, err := server.CommandDirector(
			fixture.Database,
			*fixture.ServiceManager,
			query,
			fixture.Logger,
			time.Now(),
			nil,
			"127.0.0.1",
		)
		if err != nil {
			b.Fatalf("Insert failed: %v", err)
		}
	}
}

// BenchmarkInsert_Batch benchmarks batch document INSERT operations
// Measures throughput when inserting multiple documents sequentially
func BenchmarkInsert_Batch(b *testing.B) {
	// Setup once outside the measured loop
	fixture := setupRealServerTB(b)

	batchSize := 100

	// Reset timer to exclude setup time from measurements
	b.ResetTimer()
	b.ReportAllocs()

	// The actual benchmark loop - each iteration inserts batchSize documents
	for i := 0; i < b.N; i++ {
		for j := 0; j < batchSize; j++ {
			docID := i*batchSize + j
			query := fmt.Sprintf(`ADD DOCUMENT TO BUNDLE "Authors" WITH ({"ID"=%d}, {"Name"="%s"}, {"Country"="USA"});`,
				docID, fmt.Sprintf("Author_%d", docID))

			_, err := server.CommandDirector(
				fixture.Database,
				*fixture.ServiceManager,
				query,
				fixture.Logger,
				time.Now(),
				nil,
				"127.0.0.1",
			)
			if err != nil {
				b.Fatalf("Insert failed: %v", err)
			}
		}
	}
}
