package syndrQL

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"syndrdb/src/internal/server"
	"syndrdb/src/pkg/settings"
)

/*
WHERE CLAUSE INDEXED BENCHMARKS
Measures query performance WITH indexes to compare against non-indexed baseline.
Tests Hash indexes (equality) and B-Tree indexes (ranges) with 2500 documents.
*/

// Benchmark_WhereBloom_Enabled_HashIndexed tests single binary WHERE with Hash index on Country
// Index: Hash index on Country field
// Query: WHERE Country == "USA" AND Age > 30 AND Status == "active"
// Expected: Hash index accelerates Country equality check
func Benchmark_WhereBloom_Enabled_HashIndexed(b *testing.B) {
	b.StopTimer()
	fixture := setupRealServerTB(b)

	// Create bundle
	createCmd := `CREATE BUNDLE "Users" WITH FIELDS (
		{"ID", "INT", true, false, 0},
		{"Name", "STRING", true, false, ""},
		{"Country", "STRING", true, false, ""},
		{"Age", "INT", true, false, 0},
		{"Status", "STRING", true, false, ""}
	);`
	_, err := server.CommandDirector(fixture.Database, *fixture.ServiceManager, createCmd, fixture.Logger, time.Now(), nil, "127.0.0.1")
	if err != nil {
		b.Fatalf("Failed to create bundle: %v", err)
	}

	// Create Hash indexes on Country and Status
	countryIndexCmd := `CREATE H-INDEX "idx_country" ON BUNDLE "Users" WITH FIELDS ({"Country", false, false});`
	_, err = server.CommandDirector(fixture.Database, *fixture.ServiceManager, countryIndexCmd, fixture.Logger, time.Now(), nil, "127.0.0.1")
	if err != nil {
		b.Fatalf("Failed to create Country index: %v", err)
	}

	statusIndexCmd := `CREATE H-INDEX "idx_status" ON BUNDLE "Users" WITH FIELDS ({"Status", false, false});`
	_, err = server.CommandDirector(fixture.Database, *fixture.ServiceManager, statusIndexCmd, fixture.Logger, time.Now(), nil, "127.0.0.1")
	if err != nil {
		b.Fatalf("Failed to create Status index: %v", err)
	}

	// Seed 2500 documents
	countries := []string{"USA", "UK", "Canada", "France"}
	statuses := []string{"active", "inactive"}
	for i := 1; i <= 2500; i++ {
		country := countries[rand.Intn(len(countries))]
		age := 18 + rand.Intn(53)
		status := statuses[rand.Intn(len(statuses))]
		cmd := fmt.Sprintf(
			`ADD DOCUMENT TO BUNDLE "Users" WITH ({"ID"=%d}, {"Name"="User%d"}, {"Country"="%s"}, {"Age"=%d}, {"Status"="%s"});`,
			i, i, country, age, status,
		)
		server.CommandDirector(fixture.Database, *fixture.ServiceManager, cmd, fixture.Logger, time.Now(), nil, "127.0.0.1")
	}

	// Wait for indexes to fully build (200ms should be sufficient for 2500 docs)
	time.Sleep(200 * time.Millisecond)

	settings.GetSettings().WhereBloomEnabled = true
	query := `SELECT * FROM Users WHERE Country == "USA" AND Age > 30 AND Status == "active"`

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		server.CommandDirector(fixture.Database, *fixture.ServiceManager, query, fixture.Logger, time.Now(), nil, "127.0.0.1")
	}
}

// Benchmark_WhereBloom_Disabled_BTreeIndexed tests multiple binary WHERE with B-Tree index on Age
// Index: B-Tree index on Age field
// Query: WHERE Country == "USA" AND Age > 30 AND Status == "active"
// Expected: B-Tree index accelerates Age range check
func Benchmark_WhereBloom_Disabled_BTreeIndexed(b *testing.B) {
	b.StopTimer()
	fixture := setupRealServerTB(b)

	// Create bundle
	createCmd := `CREATE BUNDLE "Users" WITH FIELDS (
		{"ID", "INT", true, false, 0},
		{"Name", "STRING", true, false, ""},
		{"Country", "STRING", true, false, ""},
		{"Age", "INT", true, false, 0},
		{"Status", "STRING", true, false, ""}
	);`
	_, err := server.CommandDirector(fixture.Database, *fixture.ServiceManager, createCmd, fixture.Logger, time.Now(), nil, "127.0.0.1")
	if err != nil {
		b.Fatalf("Failed to create bundle: %v", err)
	}

	// Create B-Tree index on Age
	ageIndexCmd := `CREATE B-INDEX "idx_age" ON BUNDLE "Users" WITH FIELDS ({"Age", false, false});`
	_, err = server.CommandDirector(fixture.Database, *fixture.ServiceManager, ageIndexCmd, fixture.Logger, time.Now(), nil, "127.0.0.1")
	if err != nil {
		b.Fatalf("Failed to create Age index: %v", err)
	}

	// Seed 2500 documents
	countries := []string{"USA", "UK", "Canada", "France"}
	statuses := []string{"active", "inactive"}
	for i := 1; i <= 2500; i++ {
		country := countries[rand.Intn(len(countries))]
		age := 18 + rand.Intn(53)
		status := statuses[rand.Intn(len(statuses))]
		cmd := fmt.Sprintf(
			`ADD DOCUMENT TO BUNDLE "Users" WITH ({"ID"=%d}, {"Name"="User%d"}, {"Country"="%s"}, {"Age"=%d}, {"Status"="%s"});`,
			i, i, country, age, status,
		)
		server.CommandDirector(fixture.Database, *fixture.ServiceManager, cmd, fixture.Logger, time.Now(), nil, "127.0.0.1")
	}

	// Wait for indexes to fully build
	time.Sleep(200 * time.Millisecond)

	settings.GetSettings().WhereBloomEnabled = false
	query := `SELECT * FROM Users WHERE Country == "USA" AND Age > 30 AND Status == "active"`

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		server.CommandDirector(fixture.Database, *fixture.ServiceManager, query, fixture.Logger, time.Now(), nil, "127.0.0.1")
	}
	b.StopTimer()
	settings.GetSettings().WhereBloomEnabled = true
}

// Benchmark_RangedWhere_FullyIndexed tests ranged WHERE with ALL indexes (Hash + B-Tree)
// Indexes: Hash on Country, Hash on Status, B-Tree on Age, B-Tree on Price
// Query: WHERE Price > 100 AND Price < 400
// Expected: Maximum index acceleration with all fields indexed
func Benchmark_RangedWhere_FullyIndexed(b *testing.B) {
	b.StopTimer()
	fixture := setupRealServerTB(b)

	// Create bundle with Price field
	createCmd := `CREATE BUNDLE "Users" WITH FIELDS (
		{"ID", "INT", true, false, 0},
		{"Name", "STRING", true, false, ""},
		{"Country", "STRING", true, false, ""},
		{"Age", "INT", true, false, 0},
		{"Status", "STRING", true, false, ""},
		{"Price", "INT", true, false, 0}
	);`
	_, err := server.CommandDirector(fixture.Database, *fixture.ServiceManager, createCmd, fixture.Logger, time.Now(), nil, "127.0.0.1")
	if err != nil {
		b.Fatalf("Failed to create bundle: %v", err)
	}

	// Create ALL indexes
	countryIndexCmd := `CREATE H-INDEX "idx_country" ON BUNDLE "Users" WITH FIELDS ({"Country", false, false});`
	_, err = server.CommandDirector(fixture.Database, *fixture.ServiceManager, countryIndexCmd, fixture.Logger, time.Now(), nil, "127.0.0.1")
	if err != nil {
		b.Fatalf("Failed to create Country index: %v", err)
	}

	statusIndexCmd := `CREATE H-INDEX "idx_status" ON BUNDLE "Users" WITH FIELDS ({"Status", false, false});`
	_, err = server.CommandDirector(fixture.Database, *fixture.ServiceManager, statusIndexCmd, fixture.Logger, time.Now(), nil, "127.0.0.1")
	if err != nil {
		b.Fatalf("Failed to create Status index: %v", err)
	}

	ageIndexCmd := `CREATE B-INDEX "idx_age" ON BUNDLE "Users" WITH FIELDS ({"Age", false, false});`
	_, err = server.CommandDirector(fixture.Database, *fixture.ServiceManager, ageIndexCmd, fixture.Logger, time.Now(), nil, "127.0.0.1")
	if err != nil {
		b.Fatalf("Failed to create Age index: %v", err)
	}

	priceIndexCmd := `CREATE B-INDEX "idx_price" ON BUNDLE "Users" WITH FIELDS ({"Price", false, false});`
	_, err = server.CommandDirector(fixture.Database, *fixture.ServiceManager, priceIndexCmd, fixture.Logger, time.Now(), nil, "127.0.0.1")
	if err != nil {
		b.Fatalf("Failed to create Price index: %v", err)
	}

	// Seed 2500 documents with Price field
	countries := []string{"USA", "UK", "Canada", "France"}
	statuses := []string{"active", "inactive"}
	for i := 1; i <= 2500; i++ {
		country := countries[rand.Intn(len(countries))]
		age := 18 + rand.Intn(53)
		status := statuses[rand.Intn(len(statuses))]
		price := 50 + rand.Intn(450) // Price between 50 and 500
		cmd := fmt.Sprintf(
			`ADD DOCUMENT TO BUNDLE "Users" WITH ({"ID"=%d}, {"Name"="User%d"}, {"Country"="%s"}, {"Age"=%d}, {"Status"="%s"}, {"Price"=%d});`,
			i, i, country, age, status, price,
		)
		server.CommandDirector(fixture.Database, *fixture.ServiceManager, cmd, fixture.Logger, time.Now(), nil, "127.0.0.1")
	}

	// Wait for ALL indexes to fully build (extra time for 4 indexes)
	time.Sleep(300 * time.Millisecond)

	query := `SELECT * FROM Users WHERE Price > 100 AND Price < 400`

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		server.CommandDirector(fixture.Database, *fixture.ServiceManager, query, fixture.Logger, time.Now(), nil, "127.0.0.1")
	}
}
