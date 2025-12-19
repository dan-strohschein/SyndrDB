package syndrQL

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"syndrdb/src/internal/server"
)

/*
HASH INDEX BUCKET OPTIMIZATION BENCHMARKS
Demonstrates real-world SELECT query performance with bucketed hash indexes.
Tests measure end-to-end query execution including index lookups.
*/

// Benchmark_SelectWithHashIndex_SmallDataset tests hash index performance with 1,000 documents
// Query: SELECT * FROM Products WHERE SKU == "PROD-500"
// Expected: Fast equality lookup using bucketed hash index
func Benchmark_SelectWithHashIndex_SmallDataset(b *testing.B) {
	b.StopTimer()
	fixture := setupRealServerTB(b)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Create bundle with hash-indexed SKU field
	createCmd := `CREATE BUNDLE "Products" WITH FIELDS (
		{"ID", "INT", true, false},
		{"SKU", "STRING", true, false},
		{"Name", "STRING", true, false},
		{"Price", "FLOAT", true, false},
		{"Category", "STRING", true, false}
	);`
	_, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, createCmd, fixture.Logger, time.Now(), nil, "127.0.0.1")
	if err != nil {
		b.Fatalf("Failed to create bundle: %v", err)
	}

	// Create hash index on SKU (primary lookup field)
	skuIndexCmd := `CREATE H-INDEX "idx_sku" ON BUNDLE "Products" WITH FIELDS ({"SKU", false, false});`
	_, err = server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, skuIndexCmd, fixture.Logger, time.Now(), nil, "127.0.0.1")
	if err != nil {
		b.Fatalf("Failed to create SKU index: %v", err)
	}

	// Seed 1,000 products
	categories := []string{"Electronics", "Clothing", "Food", "Books", "Toys"}
	for i := 1; i <= 1000; i++ {
		category := categories[rand.Intn(len(categories))]
		price := 9.99 + float64(rand.Intn(990))
		cmd := fmt.Sprintf(
			`ADD DOCUMENT TO BUNDLE "Products" WITH ({"ID"=%d}, {"SKU"="PROD-%d"}, {"Name"="Product %d"}, {"Price"=%.2f}, {"Category"="%s"});`,
			i, i, i, price, category,
		)
		server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, cmd, fixture.Logger, time.Now(), nil, "127.0.0.1")
	}

	// Wait for index build
	time.Sleep(100 * time.Millisecond)

	// Query for a specific SKU (tests bucketed hash index lookup)
	query := `SELECT * FROM Products WHERE SKU == "PROD-500"`

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		_, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, query, fixture.Logger, time.Now(), nil, "127.0.0.1")
		if err != nil {
			b.Fatalf("Query failed: %v", err)
		}
	}
}

// Benchmark_SelectWithHashIndex_MediumDataset tests hash index with 10,000 documents
// Query: SELECT * FROM Orders WHERE OrderID == "ORD-5000"
// Expected: Consistent O(1) performance regardless of dataset size
func Benchmark_SelectWithHashIndex_MediumDataset(b *testing.B) {
	b.StopTimer()
	fixture := setupRealServerTB(b)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Create bundle
	createCmd := `CREATE BUNDLE "Orders" WITH FIELDS (
		{"ID", "INT", true, false},
		{"OrderID", "STRING", true, false},
		{"CustomerID", "INT", true, false},
		{"Total", "FLOAT", true, false},
		{"Status", "STRING", true, false}
	);`
	_, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, createCmd, fixture.Logger, time.Now(), nil, "127.0.0.1")
	if err != nil {
		b.Fatalf("Failed to create bundle: %v", err)
	}

	// Create hash index on OrderID
	orderIndexCmd := `CREATE H-INDEX "idx_orderid" ON BUNDLE "Orders" WITH FIELDS ({"OrderID", false, false});`
	_, err = server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, orderIndexCmd, fixture.Logger, time.Now(), nil, "127.0.0.1")
	if err != nil {
		b.Fatalf("Failed to create OrderID index: %v", err)
	}

	// Seed 10,000 orders
	statuses := []string{"pending", "shipped", "delivered", "cancelled"}
	for i := 1; i <= 10000; i++ {
		status := statuses[rand.Intn(len(statuses))]
		total := 19.99 + float64(rand.Intn(980))
		customerID := rand.Intn(1000) + 1
		cmd := fmt.Sprintf(
			`ADD DOCUMENT TO BUNDLE "Orders" WITH ({"ID"=%d}, {"OrderID"="ORD-%d"}, {"CustomerID"=%d}, {"Total"=%.2f}, {"Status"="%s"});`,
			i, i, customerID, total, status,
		)
		server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, cmd, fixture.Logger, time.Now(), nil, "127.0.0.1")
	}

	// Wait for index build
	time.Sleep(200 * time.Millisecond)

	query := `SELECT * FROM Orders WHERE OrderID == "ORD-5000"`

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		_, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, query, fixture.Logger, time.Now(), nil, "127.0.0.1")
		if err != nil {
			b.Fatalf("Query failed: %v", err)
		}
	}
}

// Benchmark_SelectWithHashIndex_HotKey tests performance with frequently accessed key
// Query: SELECT * FROM Cache WHERE Key == "hot-key-123" (repeated lookups)
// Expected: Benefits from OS page cache + bucket optimization
func Benchmark_SelectWithHashIndex_HotKey(b *testing.B) {
	b.StopTimer()
	fixture := setupRealServerTB(b)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Create bundle
	createCmd := `CREATE BUNDLE "Cache" WITH FIELDS (
		{"ID", "INT", true, false},
		{"Key", "STRING", true, false},
		{"Value", "STRING", true, false},
		{"TTL", "INT", true, false},
		{"AccessCount", "INT", true, false}
	);`
	_, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, createCmd, fixture.Logger, time.Now(), nil, "127.0.0.1")
	if err != nil {
		b.Fatalf("Failed to create bundle: %v", err)
	}

	// Create hash index on Key
	keyIndexCmd := `CREATE H-INDEX "idx_key" ON BUNDLE "Cache" WITH FIELDS ({"Key", false, false});`
	_, err = server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, keyIndexCmd, fixture.Logger, time.Now(), nil, "127.0.0.1")
	if err != nil {
		b.Fatalf("Failed to create Key index: %v", err)
	}

	// Seed 10,000 cache entries including our hot key
	for i := 1; i <= 10000; i++ {
		var key string
		if i == 5000 {
			key = "hot-key-123" // Our frequently accessed key
		} else {
			key = fmt.Sprintf("key-%d", i)
		}
		value := fmt.Sprintf("cached-value-%d", i)
		ttl := 3600 + rand.Intn(3600)
		cmd := fmt.Sprintf(
			`ADD DOCUMENT TO BUNDLE "Cache" WITH ({"ID"=%d}, {"Key"="%s"}, {"Value"="%s"}, {"TTL"=%d}, {"AccessCount"=0});`,
			i, key, value, ttl,
		)
		server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, cmd, fixture.Logger, time.Now(), nil, "127.0.0.1")
	}

	// Wait for index build
	time.Sleep(200 * time.Millisecond)

	// Repeatedly query the same hot key
	query := `SELECT * FROM Cache WHERE Key == "hot-key-123"`

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		_, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, query, fixture.Logger, time.Now(), nil, "127.0.0.1")
		if err != nil {
			b.Fatalf("Query failed: %v", err)
		}
	}
}

// Benchmark_SelectWithMultipleHashIndexes tests query with multiple indexed equality conditions
// Query: SELECT * FROM Users WHERE Email == "user5000@example.com" AND Status == "active"
// Expected: Both indexes leverage bucketing for fast intersection
func Benchmark_SelectWithMultipleHashIndexes(b *testing.B) {
	b.StopTimer()
	fixture := setupRealServerTB(b)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Create bundle
	createCmd := `CREATE BUNDLE "Users" WITH FIELDS (
		{"ID", "INT", true, false},
		{"Email", "STRING", true, false},
		{"Name", "STRING", true, false},
		{"Status", "STRING", true, false},
		{"RegisteredDate", "STRING", true, false}
	);`
	_, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, createCmd, fixture.Logger, time.Now(), nil, "127.0.0.1")
	if err != nil {
		b.Fatalf("Failed to create bundle: %v", err)
	}

	// Create hash indexes on Email and Status
	emailIndexCmd := `CREATE H-INDEX "idx_email" ON BUNDLE "Users" WITH FIELDS ({"Email", false, false});`
	_, err = server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, emailIndexCmd, fixture.Logger, time.Now(), nil, "127.0.0.1")
	if err != nil {
		b.Fatalf("Failed to create Email index: %v", err)
	}

	statusIndexCmd := `CREATE H-INDEX "idx_status" ON BUNDLE "Users" WITH FIELDS ({"Status", false, false});`
	_, err = server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, statusIndexCmd, fixture.Logger, time.Now(), nil, "127.0.0.1")
	if err != nil {
		b.Fatalf("Failed to create Status index: %v", err)
	}

	// Seed 20,000 users
	statuses := []string{"active", "inactive", "suspended", "pending"}
	for i := 1; i <= 20000; i++ {
		status := statuses[rand.Intn(len(statuses))]
		cmd := fmt.Sprintf(
			`ADD DOCUMENT TO BUNDLE "Users" WITH ({"ID"=%d}, {"Email"="user%d@example.com"}, {"Name"="User %d"}, {"Status"="%s"}, {"RegisteredDate"="2025-11-22"});`,
			i, i, i, status,
		)
		server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, cmd, fixture.Logger, time.Now(), nil, "127.0.0.1")

		if i%5000 == 0 {
			b.Logf("Seeded %d users...", i)
		}
	}

	// Wait for index build
	time.Sleep(300 * time.Millisecond)

	// Query with two indexed equality conditions
	query := `SELECT * FROM Users WHERE Email == "user5000@example.com" AND Status == "active"`

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		_, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, query, fixture.Logger, time.Now(), nil, "127.0.0.1")
		if err != nil {
			b.Fatalf("Query failed: %v", err)
		}
	}
}
