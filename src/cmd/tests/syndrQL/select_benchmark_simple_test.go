package syndrQL

import (
	"syndrdb/src/internal/server"
	"testing"
	"time"
)

// Simple benchmark - just measure the query execution, not setup
func BenchmarkSelectQuery_100Authors(b *testing.B) {
	// Do setup outside benchmark using testing wrapper
	// We'll create a mini test to do setup, then extract the fixture
	var fixture *TestFixture
	
	testSetup := func(t *testing.T) {
		fixture = setupRealServer(t)
		seedSimpleAuthorsBundle(t, fixture, 100)
	}
	
	// Run setup as a test (hacky but works)
	testing.RunTests(func(_, _ string) (bool, error) { return true, nil },
		[]testing.InternalTest{{Name: "Setup", F: testSetup}})
	
	if fixture == nil {
		b.Fatal("Setup failed")
	}
	
	query := `SELECT * FROM "Authors"`
	
	b.ResetTimer()
	b.ReportAllocs()
	
	for i := 0; i < b.N; i++ {
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
			b.Fatalf("Query failed: %v", err)
		}
	}
	
	// Cleanup
	if fixture.ServiceManager != nil && fixture.ServiceManager.BundleService != nil {
		fixture.ServiceManager.BundleService.Shutdown()
	}
}
