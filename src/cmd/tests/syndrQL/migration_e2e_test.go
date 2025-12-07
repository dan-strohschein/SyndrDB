package syndrQL

import (
	"context"
	"strings"
	"syndrdb/src/internal/server"
	"testing"
	"time"
)

// TestMigrationE2E_CommandRouting tests that migration commands are properly routed
func TestMigrationE2E_CommandRouting(t *testing.T) {
	fixture := setupFullServer(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	tests := []struct {
		name    string
		command string
	}{
		{
			name:    "ShowMigrations",
			command: `SHOW MIGRATIONS FOR DATABASE "testdb";`,
		},
		{
			name:    "ValidateMigration",
			command: `VALIDATE MIGRATION WITH VERSION 1 FOR DATABASE "testdb" BY "test_user";`,
		},
		{
			name:    "ApplyMigration",
			command: `APPLY MIGRATION WITH VERSION 1 FOR DATABASE "testdb";`,
		},
		{
			name:    "ValidateRollback",
			command: `VALIDATE ROLLBACK TO VERSION 0 FOR DATABASE "testdb" BY "test_user";`,
		},
		{
			name:    "ApplyRollback",
			command: `APPLY ROLLBACK TO VERSION 0 FOR DATABASE "testdb";`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			startTime := time.Now()
			_, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, tt.command, fixture.Logger, startTime, nil, "127.0.0.1")

			// We expect errors since migrations don't exist, but we're testing routing
			// The key is that we don't get "unknown command" errors
			if err != nil {
				if strings.Contains(err.Error(), "unknown") && !strings.Contains(err.Error(), "not found") {
					t.Errorf("Command not properly routed: %v", err)
				} else {
					t.Logf("Got expected error (command routed correctly): %v", err)
				}
			}
		})
	}
}

// TestMigrationE2E_ServiceInitialization verifies migration service is initialized
func TestMigrationE2E_ServiceInitialization(t *testing.T) {
	fixture := setupFullServer(t)

	// Check that migration service is not nil
	if fixture.ServiceManager.MigrationService == nil {
		t.Fatal("MigrationService is nil - service not properly initialized")
	}

	t.Log("MigrationService successfully initialized")
}
