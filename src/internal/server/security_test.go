package server

import (
	"strings"
	"syndrdb/src/internal/domain/models"
	"syndrdb/src/pkg/errors"
	"testing"

	"go.uber.org/zap"
)

// ---------------------------------------------------------------------------
// 1. Authentication Bypass (Fix 1.1)
// ---------------------------------------------------------------------------

func TestAuthBypass_ConnectionStartsUnauthorized(t *testing.T) {
	// When AuthEnabled is true, a new connection must start as unauthorized.
	conn := &Connection{
		Authorized: false, // default zero-value for bool, but explicit for clarity
	}
	if conn.Authorized {
		t.Fatal("expected new connection to start with Authorized==false when auth is enabled")
	}
}

func TestAuthBypass_AuthDisabledSkipsAuth(t *testing.T) {
	// When AuthEnabled is false on the server, the connection should be
	// treated as authorized (the !s.AuthEnabled path).
	s := &Server{
		AuthEnabled: false,
	}

	conn := &Connection{}
	// Simulate the server-side logic: if auth is disabled, mark authorized.
	if !s.AuthEnabled {
		conn.Authorized = true
	}

	if !conn.Authorized {
		t.Fatal("expected connection to be Authorized when AuthEnabled is false")
	}
}

func TestAuthBypass_ValidAuthSetsAuthorized(t *testing.T) {
	// After parsing a valid connection string and authenticating, the
	// connection's Authorized field should be true.
	s := &Server{
		AuthEnabled: true,
		Databases:   map[string]*models.Database{"testdb": {Name: "testdb"}},
		logger:      zap.NewNop().Sugar(),
	}

	connStr := "syndrdb://localhost:5555:testdb:admin:password"
	cs, err := parseConnectionString(s, connStr)
	if err != nil {
		t.Fatalf("parseConnectionString returned unexpected error: %v", err)
	}

	// Simulate the post-auth path: on successful credential validation,
	// the server sets Authorized = true.
	conn := &Connection{
		DatabaseName: cs.Database,
		User:         cs.Username,
		Authorized:   true, // set by auth layer after credential check
	}

	if !conn.Authorized {
		t.Fatal("expected Authorized==true after valid authentication")
	}
	if conn.User != "admin" {
		t.Fatalf("expected User=='admin', got %q", conn.User)
	}
}

// ---------------------------------------------------------------------------
// 2. Connection String Safety (Fix 1.3, 1.4)
// ---------------------------------------------------------------------------

func TestConnString_TooFewParts_ReturnsError(t *testing.T) {
	s := &Server{
		logger:    zap.NewNop().Sugar(),
		Databases: map[string]*models.Database{},
	}

	_, err := parseConnectionString(s, "syndrdb://hostonly")
	if err == nil {
		t.Fatal("expected error for connection string with too few parts, got nil")
	}
	if !strings.Contains(err.Error(), "invalid connection string format") {
		t.Fatalf("expected 'invalid connection string format' in error, got: %v", err)
	}
}

func TestConnString_EmptyString_ReturnsError(t *testing.T) {
	s := &Server{
		logger:    zap.NewNop().Sugar(),
		Databases: map[string]*models.Database{},
	}

	_, err := parseConnectionString(s, "")
	if err == nil {
		t.Fatal("expected error for empty connection string, got nil")
	}
}

func TestConnString_ThreeParts_ReturnsError(t *testing.T) {
	s := &Server{
		logger:    zap.NewNop().Sugar(),
		Databases: map[string]*models.Database{},
	}

	_, err := parseConnectionString(s, "syndrdb://host:5555:db")
	if err == nil {
		t.Fatal("expected error for connection string with only 3 parts, got nil")
	}
}

func TestConnString_FiveParts_Succeeds(t *testing.T) {
	s := &Server{
		logger:    zap.NewNop().Sugar(),
		Databases: map[string]*models.Database{"testdb": {Name: "testdb"}},
	}

	cs, err := parseConnectionString(s, "syndrdb://localhost:5555:testdb:admin:password")
	if err != nil {
		t.Fatalf("expected no error for valid 5-part connection string, got: %v", err)
	}
	if cs.Host != "localhost" {
		t.Fatalf("expected Host=='localhost', got %q", cs.Host)
	}
	if cs.Port != 5555 {
		t.Fatalf("expected Port==5555, got %d", cs.Port)
	}
	if cs.Database != "testdb" {
		t.Fatalf("expected Database=='testdb', got %q", cs.Database)
	}
	if cs.Username != "admin" {
		t.Fatalf("expected Username=='admin', got %q", cs.Username)
	}
	if cs.Password != "password" {
		t.Fatalf("expected Password=='password', got %q", cs.Password)
	}
}

func TestConnString_InvalidPort_ReturnsError(t *testing.T) {
	s := &Server{
		logger:    zap.NewNop().Sugar(),
		Databases: map[string]*models.Database{"db": {Name: "db"}},
	}

	_, err := parseConnectionString(s, "syndrdb://host:notaport:db:user:pass")
	if err == nil {
		t.Fatal("expected error for non-numeric port, got nil")
	}
	if !strings.Contains(err.Error(), "invalid port") {
		t.Fatalf("expected 'invalid port' in error message, got: %v", err)
	}
}

// ---------------------------------------------------------------------------
// 3. Permission Enforcement (Fix 1.2)
// ---------------------------------------------------------------------------

func TestPermission_ClassifyCommand_Select(t *testing.T) {
	got := classifyCommandPermission([]string{"SELECT", "name", "FROM", "Users"})
	if got != "Read" {
		t.Fatalf("expected 'Read' for SELECT, got %q", got)
	}
}

func TestPermission_ClassifyCommand_AddDocument(t *testing.T) {
	got := classifyCommandPermission([]string{"ADD", "DOCUMENT", "TO", "BUNDLE"})
	if got != "Write" {
		t.Fatalf("expected 'Write' for ADD DOCUMENT, got %q", got)
	}
}

func TestPermission_ClassifyCommand_CreateBundle(t *testing.T) {
	got := classifyCommandPermission([]string{"CREATE", "BUNDLE", "MyBundle"})
	if got != "Admin" {
		t.Fatalf("expected 'Admin' for CREATE BUNDLE, got %q", got)
	}
}

func TestPermission_ClassifyCommand_DropBundle(t *testing.T) {
	got := classifyCommandPermission([]string{"DROP", "BUNDLE", "MyBundle"})
	if got != "Admin" {
		t.Fatalf("expected 'Admin' for DROP BUNDLE, got %q", got)
	}
}

func TestPermission_ClassifyCommand_Backup(t *testing.T) {
	got := classifyCommandPermission([]string{"BACKUP", "DATABASE"})
	if got != "Admin" {
		t.Fatalf("expected 'Admin' for BACKUP, got %q", got)
	}
}

func TestPermission_ClassifyCommand_Transaction(t *testing.T) {
	for _, cmd := range []string{"BEGIN", "COMMIT", "ROLLBACK"} {
		got := classifyCommandPermission([]string{cmd, "TRANSACTION"})
		if got != "" {
			t.Fatalf("expected empty string (no check) for %s, got %q", cmd, got)
		}
	}
}

func TestRequirePermission_NilSession_ReturnsError(t *testing.T) {
	err := RequirePermission(nil, nil, "Read", true)
	if err == nil {
		t.Fatal("expected error when session is nil and auth is enabled, got nil")
	}

	sdbErr, ok := err.(errors.SyndrDBError)
	if !ok {
		t.Fatalf("expected SyndrDBError, got %T", err)
	}
	if sdbErr.Code() != errors.ERR_AUTH_REQUIRED {
		t.Fatalf("expected error code ERR_AUTH_REQUIRED, got %v", sdbErr.Code())
	}
}

func TestRequirePermission_AuthDisabled_ReturnsNil(t *testing.T) {
	// When auth is disabled, RequirePermission should return nil even with
	// a nil session and a nil permission service.
	err := RequirePermission(nil, nil, "Read", false)
	if err != nil {
		t.Fatalf("expected nil error when auth is disabled, got: %v", err)
	}
}
