package syndrQL

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"syndrdb/src/internal/domain/models"
	"syndrdb/src/internal/server"
	"syndrdb/src/pkg/settings"
)

/*
WHERE CLAUSE BLOOM FILTER E2E TESTS
Tests Bloom filter pre-filtering for multi-condition WHERE queries
Expected: 50-70% reduction in evaluation overhead for selective AND conditions
*/

// seedUsers creates count Users with randomized data
func seedUsers(t testing.TB, fixture *TestFixture, count int) {
	t.Helper()
	rand.Seed(time.Now().UnixNano())

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	countries := []string{"USA", "UK", "Canada", "France"}
	statuses := []string{"active", "inactive"}

	for i := 1; i <= count; i++ {
		country := countries[rand.Intn(len(countries))]
		age := 18 + rand.Intn(53)
		status := statuses[rand.Intn(len(statuses))]

		cmd := fmt.Sprintf(
			`ADD DOCUMENT TO BUNDLE "Users" WITH ({"ID"=%d}, {"Name"="User%d"}, {"Country"="%s"}, {"Age"=%d}, {"Status"="%s"});`,
			i, i, country, age, status,
		)

		_, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, cmd, fixture.Logger, time.Now(), nil, "127.0.0.1")
		if err != nil {
			t.Fatalf("Failed to add document: %v", err)
		}
	}
}

// seedUsersWithPrices creates count Users with Price field for range queries
func seedUsersWithPrices(t testing.TB, fixture *TestFixture, count int) {
	t.Helper()
	rand.Seed(time.Now().UnixNano())

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	countries := []string{"USA", "UK", "Canada", "France"}
	statuses := []string{"active", "inactive"}

	for i := 1; i <= count; i++ {
		country := countries[rand.Intn(len(countries))]
		age := 18 + rand.Intn(53)
		status := statuses[rand.Intn(len(statuses))]
		price := 50 + rand.Intn(450) // Price between 50 and 500

		cmd := fmt.Sprintf(
			`ADD DOCUMENT TO BUNDLE "Users" WITH ({"ID"=%d}, {"Name"="User%d"}, {"Country"="%s"}, {"Age"=%d}, {"Status"="%s"}, {"Price"=%d});`,
			i, i, country, age, status, price,
		)

		_, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, cmd, fixture.Logger, time.Now(), nil, "127.0.0.1")
		if err != nil {
			t.Fatalf("Failed to add document: %v", err)
		}
	}
}

// getDocString returns the string value for field from doc.Data.
func getDocString(doc *models.Document, field string) (string, bool) {
	if doc == nil || doc.Data == nil {
		return "", false
	}
	if v, ok := doc.Data[field]; ok && v != nil {
		if s, ok := v.(string); ok {
			return s, true
		}
	}
	return "", false
}

// getDocInt64 returns the int64 value for field from doc.Data.
func getDocInt64(doc *models.Document, field string) (int64, bool) {
	if doc == nil || doc.Data == nil {
		return 0, false
	}
	if v, ok := doc.Data[field]; ok && v != nil {
		switch n := v.(type) {
		case int64:
			return n, true
		case int:
			return int64(n), true
		case float64:
			return int64(n), true
		}
	}
	return 0, false
}

// executeQuery executes SELECT and returns documents
func executeQuery(t testing.TB, fixture *TestFixture, query string) []*models.Document {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	resp, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, query, fixture.Logger, time.Now(), nil, "127.0.0.1")
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	cmdResp := resp.(*server.CommandResponse)
	if len(cmdResp.StreamDocuments) > 0 {
		docs := make([]*models.Document, 0, len(cmdResp.StreamDocuments))
		for _, doc := range cmdResp.StreamDocuments {
			docs = append(docs, doc)
		}
		return docs
	}

	if docs, ok := cmdResp.Result.([]*models.Document); ok {
		return docs
	}

	return []*models.Document{}
}

// TestWhereBloom_MultiCondition tests Bloom with multi-condition AND
func TestWhereBloom_MultiCondition(t *testing.T) {
	fixture := setupFullServer(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	createCmd := `CREATE BUNDLE "Users" WITH FIELDS (
		{"ID", "INT", true, false},
		{"Name", "STRING", true, false},
		{"Country", "STRING", true, false},
		{"Age", "INT", true, false},
		{"Status", "STRING", true, false}
	);`
	_, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, createCmd, fixture.Logger, time.Now(), nil, "127.0.0.1")
	if err != nil {
		t.Fatalf("Failed to create bundle: %v", err)
	}

	seedUsers(t, fixture, 1000)

	results := executeQuery(t, fixture, `SELECT * FROM Users WHERE Country == "USA" AND Age > 30 AND Status == "active"`)

	// Validate all results match (read from doc.Data)
	for _, doc := range results {
		country, _ := getDocString(doc, "Country")
		age, _ := getDocInt64(doc, "Age")
		status, _ := getDocString(doc, "Status")

		if country != "USA" || age <= 30 || status != "active" {
			t.Fatalf("Invalid result: Country=%s, Age=%d, Status=%s", country, age, status)
		}
	}

	t.Logf("✓ Multi-condition test passed: %d results validated", len(results))
}

// TestWhereBloom_vs_NoBloom compares Bloom enabled vs disabled
func TestWhereBloom_vs_NoBloom(t *testing.T) {
	fixture := setupFullServer(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	createCmd := `CREATE BUNDLE "Users" WITH FIELDS (
		{"ID", "INT", true, false},
		{"Name", "STRING", true, false},
		{"Country", "STRING", true, false},
		{"Age", "INT", true, false},
		{"Status", "STRING", true, false}
	);`
	_, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, createCmd, fixture.Logger, time.Now(), nil, "127.0.0.1")
	if err != nil {
		t.Fatalf("Failed to create bundle: %v", err)
	}

	seedUsers(t, fixture, 1000)

	query := `SELECT * FROM Users WHERE Country == "USA" AND Age > 30`

	settings.GetSettings().WhereBloomEnabled = true
	withBloom := executeQuery(t, fixture, query)

	settings.GetSettings().WhereBloomEnabled = false
	withoutBloom := executeQuery(t, fixture, query)
	settings.GetSettings().WhereBloomEnabled = true

	if len(withBloom) != len(withoutBloom) {
		t.Fatalf("Count mismatch: Bloom=%d, NoBloom=%d", len(withBloom), len(withoutBloom))
	}

	t.Logf("✓ Bloom vs NoBloom: Both returned %d results", len(withBloom))
}

// Benchmark_WhereBloom_Enabled measures Bloom-enabled performance at 1000 docs
func Benchmark_WhereBloom_Enabled(b *testing.B) {
	b.StopTimer()
	fixture := setupFullServerTB(b)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	createCmd := `CREATE BUNDLE "Users" WITH FIELDS (
		{"ID", "INT", true, false},
		{"Name", "STRING", true, false},
		{"Country", "STRING", true, false},
		{"Age", "INT", true, false},
		{"Status", "STRING", true, false}
	);`
	server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, createCmd, fixture.Logger, time.Now(), nil, "127.0.0.1")

	// Seed documents
	countries := []string{"USA", "UK", "Canada", "France"}
	statuses := []string{"active", "inactive"}
	for i := 1; i <= 1000; i++ {
		country := countries[rand.Intn(len(countries))]
		age := 18 + rand.Intn(53)
		status := statuses[rand.Intn(len(statuses))]
		cmd := fmt.Sprintf(
			`ADD DOCUMENT TO BUNDLE "Users" WITH ({"ID"=%d}, {"Name"="User%d"}, {"Country"="%s"}, {"Age"=%d}, {"Status"="%s"});`,
			i, i, country, age, status,
		)
		server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, cmd, fixture.Logger, time.Now(), nil, "127.0.0.1")
	}

	settings.GetSettings().WhereBloomEnabled = true
	query := `SELECT * FROM Users WHERE Country == "USA" AND Age > 30 AND Status == "active"`

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, query, fixture.Logger, time.Now(), nil, "127.0.0.1")
	}
}

// Benchmark_WhereBloom_Disabled measures non-Bloom performance at 1000 docs
func Benchmark_WhereBloom_Disabled(b *testing.B) {
	b.StopTimer()
	fixture := setupFullServerTB(b)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	createCmd := `CREATE BUNDLE "Users" WITH FIELDS (
		{"ID", "INT", true, false},
		{"Name", "STRING", true, false},
		{"Country", "STRING", true, false},
		{"Age", "INT", true, false},
		{"Status", "STRING", true, false}
	);`
	server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, createCmd, fixture.Logger, time.Now(), nil, "127.0.0.1")

	// Seed documents
	countries := []string{"USA", "UK", "Canada", "France"}
	statuses := []string{"active", "inactive"}
	for i := 1; i <= 1000; i++ {
		country := countries[rand.Intn(len(countries))]
		age := 18 + rand.Intn(53)
		status := statuses[rand.Intn(len(statuses))]
		cmd := fmt.Sprintf(
			`ADD DOCUMENT TO BUNDLE "Users" WITH ({"ID"=%d}, {"Name"="User%d"}, {"Country"="%s"}, {"Age"=%d}, {"Status"="%s"});`,
			i, i, country, age, status,
		)
		server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, cmd, fixture.Logger, time.Now(), nil, "127.0.0.1")
	}

	settings.GetSettings().WhereBloomEnabled = false
	query := `SELECT * FROM Users WHERE Country == "USA" AND Age > 30 AND Status == "active"`

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, query, fixture.Logger, time.Now(), nil, "127.0.0.1")
	}
	b.StopTimer()
	settings.GetSettings().WhereBloomEnabled = true
}

// Benchmark_RangedWhere benchmarks SELECT with ranged WHERE clause (Price > X AND Price < Y)
func Benchmark_RangedWhere(b *testing.B) {
	b.StopTimer()
	fixture := setupFullServerTB(b)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	createCmd := `CREATE BUNDLE "Users" WITH FIELDS (
		{"ID", "INT", true, false},
		{"Name", "STRING", true, false},
		{"Country", "STRING", true, false},
		{"Age", "INT", true, false},
		{"Status", "STRING", true, false},
		{"Price", "INT", true, false}
	);`
	server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, createCmd, fixture.Logger, time.Now(), nil, "127.0.0.1")

	// Seed 1000 users with Price field
	seedUsersWithPrices(b, fixture, 1000)

	query := `SELECT * FROM Users WHERE Price > 100 AND Price < 400`

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, query, fixture.Logger, time.Now(), nil, "127.0.0.1")
	}
}
