package syndrQL

import (
	"context"
	"fmt"
	"syndrdb/src/internal/server"
	"testing"
	"time"
)

// BenchmarkDateTime_Function_NOW benchmarks the F:NOW() function
func BenchmarkDateTime_Function_NOW(b *testing.B) {
	fixture := setupFullServer(&testing.T{})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	selectCmd := "SELECT F:NOW();"
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		startTime := time.Now()
		_, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, selectCmd, fixture.Logger, startTime, nil, "127.0.0.1")
		if err != nil {
			b.Fatalf("Failed to execute F:NOW(): %v", err)
		}
	}
}

// BenchmarkDateTime_Function_EXTRACT benchmarks the F:EXTRACT() function
func BenchmarkDateTime_Function_EXTRACT(b *testing.B) {
	fixture := setupFullServer(&testing.T{})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	selectCmd := `SELECT F:EXTRACT(YEAR FROM "2024-11-22T15:30:45Z");`
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		startTime := time.Now()
		_, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, selectCmd, fixture.Logger, startTime, nil, "127.0.0.1")
		if err != nil {
			b.Fatalf("Failed to execute F:EXTRACT: %v", err)
		}
	}
}

// BenchmarkDateTime_Function_DATE_TRUNC benchmarks the F:DATE_TRUNC() function
func BenchmarkDateTime_Function_DATE_TRUNC(b *testing.B) {
	fixture := setupFullServer(&testing.T{})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	selectCmd := `SELECT F:DATE_TRUNC(DAY, "2024-11-22T15:30:45Z");`
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		startTime := time.Now()
		_, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, selectCmd, fixture.Logger, startTime, nil, "127.0.0.1")
		if err != nil {
			b.Fatalf("Failed to execute F:DATE_TRUNC: %v", err)
		}
	}
}

// BenchmarkDateTime_Function_DATE_ADD benchmarks the F:DATE_ADD() function
func BenchmarkDateTime_Function_DATE_ADD(b *testing.B) {
	fixture := setupFullServer(&testing.T{})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	selectCmd := `SELECT F:DATE_ADD("2024-11-22T15:30:00Z", INTERVAL '1 DAY');`
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		startTime := time.Now()
		_, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, selectCmd, fixture.Logger, startTime, nil, "127.0.0.1")
		if err != nil {
			b.Fatalf("Failed to execute F:DATE_ADD: %v", err)
		}
	}
}

// BenchmarkDateTime_Function_DATE_SUB benchmarks the F:DATE_SUB() function
func BenchmarkDateTime_Function_DATE_SUB(b *testing.B) {
	fixture := setupFullServer(&testing.T{})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	selectCmd := `SELECT F:DATE_SUB("2024-11-22T15:30:00Z", INTERVAL '1 DAY');`
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		startTime := time.Now()
		_, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, selectCmd, fixture.Logger, startTime, nil, "127.0.0.1")
		if err != nil {
			b.Fatalf("Failed to execute F:DATE_SUB: %v", err)
		}
	}
}

// BenchmarkDateTime_Function_AGE benchmarks the F:AGE() function
func BenchmarkDateTime_Function_AGE(b *testing.B) {
	fixture := setupFullServer(&testing.T{})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	selectCmd := `SELECT F:AGE("2024-11-22T15:30:00Z", "2023-11-22T15:30:00Z");`
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		startTime := time.Now()
		_, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, selectCmd, fixture.Logger, startTime, nil, "127.0.0.1")
		if err != nil {
			b.Fatalf("Failed to execute F:AGE: %v", err)
		}
	}
}

// BenchmarkDateTime_AT_TIME_ZONE benchmarks the AT TIME ZONE operator
func BenchmarkDateTime_AT_TIME_ZONE(b *testing.B) {
	fixture := setupFullServer(&testing.T{})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	selectCmd := `SELECT "2024-11-22T15:30:00Z" AT TIME ZONE 'America/New_York';`
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		startTime := time.Now()
		_, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, selectCmd, fixture.Logger, startTime, nil, "127.0.0.1")
		if err != nil {
			b.Fatalf("Failed to execute AT TIME ZONE: %v", err)
		}
	}
}

// BenchmarkDateTime_DefaultValue_NOW benchmarks F:NOW() as default value in INSERT
func BenchmarkDateTime_DefaultValue_NOW(b *testing.B) {
	fixture := setupFullServer(&testing.T{})
	bundleName := "BenchDefaultNowBundle"
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// Create bundle with F:NOW() as default
	createCmd := fmt.Sprintf(`CREATE BUNDLE "%s" WITH FIELDS (
		{"id", "STRING", true, false},
		{"created_at", "DATETIME", true, false}
	);`, bundleName)

	startTime := time.Now()
	_, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, createCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	if err != nil {
		b.Fatalf("Failed to create bundle: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Insert document WITHOUT providing created_at (uses F:NOW() default)
		insertCmd := fmt.Sprintf(`ADD DOCUMENT TO BUNDLE "%s" WITH ({"id"="bench-%d"});`, bundleName, i)
		startTime := time.Now()
		_, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, insertCmd, fixture.Logger, startTime, nil, "127.0.0.1")
		if err != nil {
			b.Fatalf("Failed to insert document: %v", err)
		}
	}
}
