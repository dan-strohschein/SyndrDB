package main

import (
	"testing"
	"time"
)

// BenchmarkSingleInsert measures single document insert performance
func BenchmarkSingleInsert(b *testing.B) {
	b.ReportAllocs() // Report memory allocations

	// Run the benchmark
	for i := 0; i < b.N; i++ {
		start := time.Now()

		// TODO: Replace with your actual insert code
		// Example (you'll need to adapt this):
		// cmd := fmt.Sprintf(`ADD DOCUMENT TO BUNDLE "test" WITH ({"name" = "doc%d"})`, i)
		// _, err := executeCommand(cmd)
		// if err != nil {
		// 	b.Fatalf("Insert failed: %v", err)
		// }

		// PLACEHOLDER - this simulates 15ms insert
		time.Sleep(15 * time.Millisecond)

		elapsed := time.Since(start)
		if elapsed > 50*time.Millisecond {
			b.Logf("Slow insert detected: %v", elapsed)
		}
	}
}
