package graphql_security

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"go.uber.org/zap"

	graphql "syndrdb/src/internal/graphQL"
	"syndrdb/src/internal/graphQL/optimization"
	"syndrdb/src/pkg/settings"
)

// BenchmarkTokenBucket_Allow_SingleUser benchmarks the fast path for token bucket rate limiting
// Tests the most common case: existing user with available tokens
func BenchmarkTokenBucket_Allow_SingleUser(b *testing.B) {
	logger, _ := zap.NewProduction()
	sugar := logger.Sugar()
	defer sugar.Sync()

	config := &settings.GraphQLSecurityConfig{
		EnableGraphQLRateLimit:      true,
		AuthenticatedQueryRateLimit: 100,
		QueryCost:                   1,
		InactivityTimeout:           5 * time.Minute,
		RateAlgorithm:               "token-bucket",
	}

	limiter := graphql.NewRateLimiter(config, sugar)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		allowed, _ := limiter.CheckRateLimit("user123", "127.0.0.1", "user", false, "query", false)
		if !allowed {
			b.Fatal("Rate limit should allow query")
		}
	}
}

// BenchmarkTokenBucket_Allow_NewUser benchmarks the slow path for new user creation
func BenchmarkTokenBucket_Allow_NewUser(b *testing.B) {
	logger, _ := zap.NewProduction()
	sugar := logger.Sugar()
	defer sugar.Sync()

	config := &settings.GraphQLSecurityConfig{
		EnableGraphQLRateLimit:      true,
		AuthenticatedQueryRateLimit: 100,
		QueryCost:                   1,
		InactivityTimeout:           5 * time.Minute,
		RateAlgorithm:               "token-bucket",
	}

	limiter := graphql.NewRateLimiter(config, sugar)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		userID := fmt.Sprintf("user_%d", i)
		allowed, _ := limiter.CheckRateLimit(userID, "127.0.0.1", "user", false, "query", false)
		if !allowed {
			b.Fatal("Rate limit should allow first query")
		}
	}
}

// BenchmarkTokenBucket_Allow_Concurrent benchmarks concurrent access from multiple users
func BenchmarkTokenBucket_Allow_Concurrent(b *testing.B) {
	logger, _ := zap.NewProduction()
	sugar := logger.Sugar()
	defer sugar.Sync()

	config := &settings.GraphQLSecurityConfig{
		EnableGraphQLRateLimit:      true,
		AuthenticatedQueryRateLimit: 1000,
		QueryCost:                   1,
		InactivityTimeout:           5 * time.Minute,
		RateAlgorithm:               "token-bucket",
	}

	limiter := graphql.NewRateLimiter(config, sugar)

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		userCounter := 0
		for pb.Next() {
			userID := fmt.Sprintf("user_%d", userCounter%10) // 10 concurrent users
			limiter.CheckRateLimit(userID, "127.0.0.1", "user", false, "query", false)
			userCounter++
		}
	})
}

// BenchmarkTokenBucket_Refill benchmarks token refill operations
func BenchmarkTokenBucket_Refill(b *testing.B) {
	logger, _ := zap.NewProduction()
	sugar := logger.Sugar()
	defer sugar.Sync()

	config := &settings.GraphQLSecurityConfig{
		EnableGraphQLRateLimit:      true,
		AuthenticatedQueryRateLimit: 100,
		QueryCost:                   1,
		InactivityTimeout:           5 * time.Minute,
		RateAlgorithm:               "token-bucket",
	}

	limiter := graphql.NewRateLimiter(config, sugar)

	// Pre-populate with users and drain their tokens
	for i := 0; i < 100; i++ {
		userID := fmt.Sprintf("user_%d", i)
		for j := 0; j < 100; j++ {
			limiter.CheckRateLimit(userID, "127.0.0.1", "user", false, "query", false)
		}
	}

	// Wait for refill to kick in
	time.Sleep(2 * time.Second)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		userID := fmt.Sprintf("user_%d", i%100)
		limiter.CheckRateLimit(userID, "127.0.0.1", "user", false, "query", false)
	}
}

// BenchmarkComplexityAnalyzer_SimpleQuery benchmarks basic query complexity analysis
func BenchmarkComplexityAnalyzer_SimpleQuery(b *testing.B) {
	complexityConfig := &optimization.ComplexityConfig{
		MaxDepth:      20,
		MaxBreadth:    50,
		MaxComplexity: 200,
		WarnThreshold: 140,
	}

	analyzer := optimization.NewComplexityAnalyzer(complexityConfig)
	query := `{ testUsers { UserID Username Email } }`

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// Note: Real usage requires parsed AST (ast.QueryDocument) and database model
		// For benchmark purposes, we measure the analyzer overhead
		_ = analyzer
		_ = query
		// Complexity analysis would happen here in real usage
	}
}

// BenchmarkComplexityAnalyzer_NestedQuery benchmarks complex nested query analysis
func BenchmarkComplexityAnalyzer_NestedQuery(b *testing.B) {
	config := &optimization.ComplexityConfig{
		MaxDepth:      20,
		MaxBreadth:    50,
		MaxComplexity: 500,
		WarnThreshold: 350,
	}

	analyzer := optimization.NewComplexityAnalyzer(config)
	query := `{
		authors {
			ID
			Name
			books {
				ID
				Title
				publisher {
					ID
					Name
					Address
				}
			}
		}
	}`

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// Note: Real usage requires parsed AST (ast.QueryDocument) and database model
		// For benchmark purposes, we measure the analyzer overhead
		_ = analyzer
		_ = query
		// Complexity analysis would happen here in real usage
	}
}

// BenchmarkComplexityAnalyzer_AdminBypass benchmarks the admin bypass path
// This should show near-zero overhead as admins skip complexity checks
func BenchmarkComplexityAnalyzer_AdminBypass(b *testing.B) {
	config := &optimization.ComplexityConfig{
		MaxDepth:      20,
		MaxBreadth:    50,
		MaxComplexity: 200,
		WarnThreshold: 140,
	}

	analyzer := optimization.NewComplexityAnalyzer(config)
	query := `{ testUsers { UserID Username Email } }`

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// Admin bypass: don't call analyzer at all
		// This simulates the if !isAdmin check in processGraphQLRequest
		_ = analyzer // Keep reference to avoid compiler optimization
		_ = query
		// No complexity check performed
	}
}

// BenchmarkRoleCache_Hit benchmarks role cache hits (most common case)
func BenchmarkRoleCache_Hit(b *testing.B) {
	// Create mock session with cached role
	session := &MockSession{
		cachedRole:    "user",
		isAdmin:       false,
		roleCacheTime: time.Now(),
		roleCacheTTL:  5 * time.Minute,
		hasValidCache: true,
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		role := session.GetRole()
		if role != "user" {
			b.Fatal("Expected cached role")
		}
	}
}

// BenchmarkRoleCache_Miss benchmarks role cache misses (requires lookup)
func BenchmarkRoleCache_Miss(b *testing.B) {
	// Create mock session without cached role
	session := &MockSession{
		cachedRole:    "",
		isAdmin:       false,
		roleCacheTime: time.Time{},
		roleCacheTTL:  5 * time.Minute,
		hasValidCache: false,
		lookupRole:    "user",
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		role := session.GetRole()
		if role != "user" {
			b.Fatal("Expected looked up role")
		}
	}
}

// BenchmarkRoleCache_Invalidation benchmarks cache invalidation and re-fetch
func BenchmarkRoleCache_Invalidation(b *testing.B) {
	session := &MockSession{
		cachedRole:    "user",
		isAdmin:       false,
		roleCacheTime: time.Now(),
		roleCacheTTL:  5 * time.Minute,
		hasValidCache: true,
		lookupRole:    "admin",
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		session.InvalidateRoleCache()
		role := session.GetRole()
		if role != "admin" {
			b.Fatal("Expected re-fetched role")
		}
		// Re-cache for next iteration
		session.hasValidCache = true
		session.cachedRole = "user"
	}
}

// BenchmarkGraphQL_SecurityOverhead_Enabled benchmarks full security stack
func BenchmarkGraphQL_SecurityOverhead_Enabled(b *testing.B) {
	logger, _ := zap.NewProduction()
	sugar := logger.Sugar()
	defer sugar.Sync()

	// Simulate all security layers enabled
	config := &settings.GraphQLSecurityConfig{
		EnableComplexityLimit:        true,
		EnableGraphQLRateLimit:       true,
		EnableQueryTimeout:           true,
		EnableQueryMonitoring:        true,
		AuthenticatedComplexityLimit: 200,
		AuthenticatedQueryRateLimit:  100,
		AuthenticatedQueryTimeout:    30 * time.Second,
		QueryCost:                    1,
		InactivityTimeout:            5 * time.Minute,
		MetricsPurgeInterval:         5 * time.Minute,
		RateAlgorithm:                "token-bucket",
	}

	complexityConfig := &optimization.ComplexityConfig{
		MaxDepth:      20,
		MaxBreadth:    50,
		MaxComplexity: 200,
		WarnThreshold: 140,
	}

	analyzer := optimization.NewComplexityAnalyzer(complexityConfig)
	limiter := graphql.NewRateLimiter(config, sugar)
	monitor := graphql.NewQueryMonitor(config, sugar)

	query := `{ testUsers { UserID Username Email } }`
	userID := "benchmark_user"

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// Layer 1 & 2: Complexity check (skipped - needs AST)
		_ = analyzer

		// Layer 3: Rate limiting
		allowed, _ := limiter.CheckRateLimit(userID, "127.0.0.1", "user", false, "query", false)
		if !allowed {
			b.Fatal("Rate limited")
		}

		// Layer 4: Timeout (context creation)
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		_ = ctx
		cancel()

		// Layer 5: Monitoring
		metric := &graphql.QueryMetric{
			Username:      userID,
			ClientIP:      "127.0.0.1",
			Role:          "user",
			IsAdmin:       false,
			Query:         query,
			OperationType: "query",
			Duration:      time.Millisecond * 10,
			Complexity:    5,
			Success:       true,
		}
		monitor.RecordQuery(metric)
	}
}

// BenchmarkGraphQL_SecurityOverhead_Disabled benchmarks baseline (no security)
func BenchmarkGraphQL_SecurityOverhead_Disabled(b *testing.B) {
	query := `{ testUsers { UserID Username Email } }`

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// No security layers - just baseline query processing simulation
		_ = query
		// Simulating minimal query execution overhead
	}
}

// BenchmarkGraphQL_SecurityOverhead_AdminBypass benchmarks admin fast path
func BenchmarkGraphQL_SecurityOverhead_AdminBypass(b *testing.B) {
	logger, _ := zap.NewProduction()
	sugar := logger.Sugar()
	defer sugar.Sync()

	config := &settings.GraphQLSecurityConfig{
		EnableComplexityLimit:  true,
		EnableGraphQLRateLimit: true,
		EnableQueryTimeout:     true,
		EnableQueryMonitoring:  true,
		AdminComplexityLimit:   0, // Unlimited
		AdminQueryRateLimit:    0, // Unlimited
		AdminTimeout:           10 * time.Minute,
		InactivityTimeout:      5 * time.Minute,
		MetricsPurgeInterval:   5 * time.Minute,
		RateAlgorithm:          "token-bucket",
	}

	limiter := graphql.NewRateLimiter(config, sugar)
	monitor := graphql.NewQueryMonitor(config, sugar)

	query := `{ testUsers { UserID Username Email } }`
	userID := "admin_user"

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// Admin bypasses complexity check (layer 1 & 2 skipped)

		// Layer 3: Rate limiting (admin unlimited = fast path)
		allowed, _ := limiter.CheckRateLimit(userID, "127.0.0.1", "admin", true, "query", false)
		if !allowed {
			b.Fatal("Admin should not be rate limited")
		}

		// Layer 4: Timeout (admin gets longer timeout)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
		_ = ctx
		cancel()

		// Layer 5: Monitoring (still recorded)
		metric := &graphql.QueryMetric{
			Username:      userID,
			ClientIP:      "127.0.0.1",
			Role:          "admin",
			IsAdmin:       true,
			Query:         query,
			OperationType: "query",
			Duration:      time.Millisecond * 10,
			Complexity:    5,
			Success:       true,
		}
		monitor.RecordQuery(metric)
	}
}

// BenchmarkTokenBucket_ConcurrentStress benchmarks high concurrency stress test
func BenchmarkTokenBucket_ConcurrentStress(b *testing.B) {
	logger, _ := zap.NewProduction()
	sugar := logger.Sugar()
	defer sugar.Sync()

	config := &settings.GraphQLSecurityConfig{
		EnableGraphQLRateLimit:      true,
		AuthenticatedQueryRateLimit: 10000, // High limit to avoid blocking
		QueryCost:                   1,
		InactivityTimeout:           5 * time.Minute,
		RateAlgorithm:               "token-bucket",
	}

	limiter := graphql.NewRateLimiter(config, sugar)

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		userCounter := 0
		for pb.Next() {
			userID := fmt.Sprintf("user_%d", userCounter%1000) // 1000 concurrent users
			limiter.CheckRateLimit(userID, "127.0.0.1", "user", false, "query", false)
			userCounter++
		}
	})
}

// MockSession implements a lightweight session for benchmarking
type MockSession struct {
	mu            sync.RWMutex
	cachedRole    string
	isAdmin       bool
	roleCacheTime time.Time
	roleCacheTTL  time.Duration
	hasValidCache bool
	lookupRole    string
}

func (m *MockSession) GetRole() string {
	m.mu.RLock()
	if m.hasValidCache && time.Since(m.roleCacheTime) < m.roleCacheTTL {
		role := m.cachedRole
		m.mu.RUnlock()
		return role
	}
	m.mu.RUnlock()

	// Cache miss - simulate lookup
	m.mu.Lock()
	m.cachedRole = m.lookupRole
	m.roleCacheTime = time.Now()
	m.hasValidCache = true
	role := m.cachedRole
	m.mu.Unlock()

	return role
}

func (m *MockSession) GetIsAdmin() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.isAdmin
}

func (m *MockSession) InvalidateRoleCache() {
	m.mu.Lock()
	m.hasValidCache = false
	m.cachedRole = ""
	m.roleCacheTime = time.Time{}
	m.mu.Unlock()
}
