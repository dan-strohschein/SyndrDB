package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"path/filepath"
	defaultdb "syndrdb/src/internal/defaultDB"
	"syndrdb/src/internal/domain/bundle"
	"syndrdb/src/internal/domain/database"
	"syndrdb/src/internal/domain/document"
	"syndrdb/src/internal/server"
	"syndrdb/src/internal/storage/buffer"
	"syndrdb/src/internal/storage/bundlestore"
	"syndrdb/src/internal/storage/databasestore"
	"syndrdb/src/pkg/settings"
	"testing"
	"time"

	"go.uber.org/zap"
)

// ConnectionPoolTestFixture holds test server components
type ConnectionPoolTestFixture struct {
	Server  *server.Server
	TempDir string
	Logger  *zap.SugaredLogger
}

// setupConnectionPoolTest creates a test server with connection pool configured
func setupConnectionPoolTest(t *testing.T) *ConnectionPoolTestFixture {
	t.Helper()
	EnsureTestIsolation(t)

	// Create temp directory
	tempDir := t.TempDir()

	// Setup logger
	loggerConfig := zap.NewDevelopmentConfig()
	loggerConfig.Level = zap.NewAtomicLevelAt(zap.WarnLevel)
	logger, err := loggerConfig.Build()
	if err != nil {
		t.Fatalf("Failed to create logger: %v", err)
	}
	sugar := logger.Sugar()

	// Create settings
	args := &settings.Arguments{
		DataDir:                      filepath.Join(tempDir, "data_files"),
		TempDir:                      filepath.Join(tempDir, "temp_files"),
		LogDir:                       filepath.Join(tempDir, "log_files"),
		MaxConnections:               100,
		ConnectionIdleTimeoutMinutes: 30,
		Host:                         "127.0.0.1",
		Port:                         17761, // Use different port for testing
	}

	// Create directory structure
	os.MkdirAll(args.DataDir, 0755)
	os.MkdirAll(args.TempDir, 0755)
	os.MkdirAll(args.LogDir, 0755)

	// Create services
	databaseStore, err := databasestore.NewDatabaseStore(args.DataDir, sugar)
	if err != nil {
		t.Fatalf("Failed to create database store: %v", err)
	}

	databaseFactory := database.NewDatabaseFactory()
	databaseService := database.NewDatabaseService(databaseStore, databaseFactory, args, sugar)

	fileRegistry, err := buffer.NewFileRegistry(args.DataDir, buffer.SyncInterval, sugar)
	if err != nil {
		t.Fatalf("Failed to create file registry: %v", err)
	}
	bufferPool := buffer.NewBufferPool(1000, buffer.DefaultPageSize, fileRegistry, sugar)

	bundleStore, err := bundlestore.NewBundleStore(args.DataDir, bufferPool, sugar, "binary")
	if err != nil {
		t.Fatalf("Failed to create bundle store: %v", err)
	}

	bundleFactory := bundle.NewBundleFactory()
	documentFactory := document.NewDocumentFactory()
	bundleService := bundle.NewBundleService(bundleStore, bundleFactory, documentFactory, sugar, args)

	catalogService := defaultdb.NewCatalogService(databaseService, bundleService, sugar)
	bundleService.SetCatalogService(catalogService)

	// Create server using InitServer
	srv, err := server.InitServer(args)
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}

	// Start server
	err = srv.Start()
	if err != nil {
		t.Fatalf("Failed to start server: %v", err)
	}

	// Wait for server to be ready
	time.Sleep(200 * time.Millisecond)

	return &ConnectionPoolTestFixture{
		Server:  srv,
		TempDir: tempDir,
		Logger:  sugar,
	}
}

// teardownConnectionPoolTest stops the server and cleans up
func teardownConnectionPoolTest(t *testing.T, fixture *ConnectionPoolTestFixture) {
	t.Helper()
	if fixture.Server != nil {
		fixture.Server.Stop()
	}
}

// TestConnectionPool_E2E_PoolLimitEnforcement tests that connections are rejected when pool is full
func TestConnectionPool_E2E_PoolLimitEnforcement(t *testing.T) {
	fixture := setupConnectionPoolTest(t)
	defer teardownConnectionPoolTest(t, fixture)

	// Override the max connections for testing
	fixture.Server.MaxConnections = 3
	fixture.Server.RateLimiter.SetMaxGlobalConnections(3)

	// Create connections up to the limit
	var connections []net.Conn
	defer func() {
		for _, conn := range connections {
			if conn != nil {
				conn.Close()
			}
		}
	}()

	// Open connections up to the limit
	for i := 0; i < 3; i++ {
		conn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", fixture.Server.Host, fixture.Server.Port))
		if err != nil {
			t.Fatalf("Failed to establish connection %d: %v", i+1, err)
		}
		connections = append(connections, conn)
		time.Sleep(100 * time.Millisecond)
	}

	// Wait for connections to be registered
	time.Sleep(200 * time.Millisecond)

	// Verify all connections are active
	stats := fixture.Server.GetConnectionPoolStats()
	activeCount := stats["active_count"].(int)
	if activeCount != 3 {
		t.Errorf("Expected 3 active connections, got %d", activeCount)
	}

	// Try to create one more connection - should be rejected
	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", fixture.Server.Host, fixture.Server.Port))
	if err != nil {
		t.Fatalf("Failed to dial server: %v", err)
	}
	defer conn.Close()

	// Read the rejection message
	reader := bufio.NewReader(conn)
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	response, err := reader.ReadString('\n')

	// Should receive error message about pool limit
	if err == nil {
		if response[:5] != "ERROR" {
			t.Errorf("Expected ERROR response, got: %s", response)
		}
		t.Logf("Correctly received rejection message: %s", response)
	}

	// Verify active count is still 3
	time.Sleep(100 * time.Millisecond)
	stats = fixture.Server.GetConnectionPoolStats()
	activeCount = stats["active_count"].(int)
	if activeCount != 3 {
		t.Errorf("Expected 3 active connections after rejection, got %d", activeCount)
	}

	t.Logf("Pool limit enforcement test passed")
}

// TestConnectionPool_E2E_GetConnectionPoolStats tests the stats method returns correct information
func TestConnectionPool_E2E_GetConnectionPoolStats(t *testing.T) {
	fixture := setupConnectionPoolTest(t)
	defer teardownConnectionPoolTest(t, fixture)

	// Get initial stats
	stats := fixture.Server.GetConnectionPoolStats()

	// Verify structure
	if _, ok := stats["active_count"]; !ok {
		t.Error("Stats missing 'active_count'")
	}
	if _, ok := stats["max_connections"]; !ok {
		t.Error("Stats missing 'max_connections'")
	}
	if _, ok := stats["idle_timeout_seconds"]; !ok {
		t.Error("Stats missing 'idle_timeout_seconds'")
	}
	if _, ok := stats["connections"]; !ok {
		t.Error("Stats missing 'connections'")
	}

	// Verify max_connections value
	maxConns := stats["max_connections"].(int)
	if maxConns != fixture.Server.MaxConnections {
		t.Errorf("Expected max_connections %d, got %d", fixture.Server.MaxConnections, maxConns)
	}

	// Create a connection
	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", fixture.Server.Host, fixture.Server.Port))
	if err != nil {
		t.Fatalf("Failed to establish connection: %v", err)
	}
	defer conn.Close()

	time.Sleep(200 * time.Millisecond)

	// Get updated stats
	stats = fixture.Server.GetConnectionPoolStats()
	activeCount := stats["active_count"].(int)
	if activeCount != 1 {
		t.Errorf("Expected 1 active connection, got %d", activeCount)
	}

	// Verify connections array
	connections := stats["connections"].([]map[string]interface{})
	if len(connections) != 1 {
		t.Errorf("Expected 1 connection in array, got %d", len(connections))
	}

	// Verify connection details structure
	connInfo := connections[0]
	requiredFields := []string{"id", "last_active_unix", "idle_seconds", "database", "user", "authorized"}
	for _, field := range requiredFields {
		if _, ok := connInfo[field]; !ok {
			t.Errorf("Connection info missing field: %s", field)
		}
	}

	// Verify last_active_unix is a recent timestamp
	lastActiveUnix := connInfo["last_active_unix"].(int64)
	now := time.Now().Unix()
	if now-lastActiveUnix > 5 {
		t.Errorf("last_active_unix seems too old: %d (now: %d)", lastActiveUnix, now)
	}

	t.Logf("GetConnectionPoolStats test passed")
}

// TestConnectionPool_E2E_ConfigValidation tests that configuration values are properly set
func TestConnectionPool_E2E_ConfigValidation(t *testing.T) {
	fixture := setupConnectionPoolTest(t)
	defer teardownConnectionPoolTest(t, fixture)

	// Verify defaults are set
	if fixture.Server.MaxConnections <= 0 {
		t.Error("MaxConnections should be set to a positive value")
	}

	if fixture.Server.ConnectionIdleTimeout <= 0 {
		t.Error("ConnectionIdleTimeout should be set to a positive duration")
	}

	// Verify rate limiter is synced
	rlStats := fixture.Server.RateLimiter.GetStats()
	rlConfig := rlStats["config"].(*server.RateLimitConfig)

	if rlConfig.MaxGlobalConnections != fixture.Server.MaxConnections {
		t.Errorf("RateLimiter MaxGlobalConnections (%d) should match Server MaxConnections (%d)",
			rlConfig.MaxGlobalConnections, fixture.Server.MaxConnections)
	}

	t.Logf("Configuration validation test passed")
	t.Logf("MaxConnections: %d", fixture.Server.MaxConnections)
	t.Logf("ConnectionIdleTimeout: %s", fixture.Server.ConnectionIdleTimeout)
}
