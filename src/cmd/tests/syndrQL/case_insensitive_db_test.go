package syndrQL

import (
	"testing"

	db "syndrdb/src/internal/domain/database"

	"go.uber.org/zap"
)

// TestCaseInsensitiveDatabaseCommands verifies that database commands work with any case
func TestCaseInsensitiveDatabaseCommands(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	sugar := logger.Sugar()

	t.Run("CREATE DATABASE case variations", func(t *testing.T) {
		tests := []struct {
			name    string
			command string
			wantDB  string
		}{
			{"uppercase", "CREATE DATABASE \"TestDB\"", "TestDB"},
			{"lowercase", "create database \"TestDB2\"", "TestDB2"},
			{"mixed case", "CrEaTe DaTaBaSe \"TestDB3\"", "TestDB3"},
			{"no quotes uppercase", "CREATE DATABASE MyTestDB", "MyTestDB"},
			{"no quotes lowercase", "create database mytestdb2", "mytestdb2"},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				result, err := db.ParseDatabaseCommand(tt.command, sugar)
				if err != nil {
					t.Errorf("ParseDatabaseCommand() error = %v", err)
					return
				}
				if result.DatabaseName != tt.wantDB {
					t.Errorf("ParseDatabaseCommand() got DatabaseName = %v, want %v", result.DatabaseName, tt.wantDB)
				}
				if result.CommandType != "CREATE" {
					t.Errorf("ParseDatabaseCommand() got CommandType = %v, want CREATE", result.CommandType)
				}
			})
		}
	})

	t.Run("DROP DATABASE case variations", func(t *testing.T) {
		tests := []struct {
			name      string
			command   string
			wantDB    string
			wantForce bool
		}{
			{"uppercase", "DROP DATABASE \"TestDB\" FORCE", "TestDB", true},
			{"lowercase", "drop database \"TestDB2\" force", "TestDB2", true},
			{"mixed case", "DrOp DaTaBaSe \"TestDB3\" FoRcE", "TestDB3", true},
			{"no force", "DROP DATABASE \"TestDB4\"", "TestDB4", false},
			{"no quotes", "drop database MyTestDB", "MyTestDB", false},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				result, err := db.ParseDropDatabaseCommand(tt.command)
				if err != nil {
					t.Errorf("ParseDropDatabaseCommand() error = %v", err)
					return
				}
				if result.DatabaseName != tt.wantDB {
					t.Errorf("ParseDropDatabaseCommand() got DatabaseName = %v, want %v", result.DatabaseName, tt.wantDB)
				}
				if result.Force != tt.wantForce {
					t.Errorf("ParseDropDatabaseCommand() got Force = %v, want %v", result.Force, tt.wantForce)
				}
				if result.CommandType != "DROP" {
					t.Errorf("ParseDropDatabaseCommand() got CommandType = %v, want DROP", result.CommandType)
				}
			})
		}
	})

	t.Run("RENAME DATABASE case variations", func(t *testing.T) {
		tests := []struct {
			name      string
			command   string
			wantOld   string
			wantNew   string
			wantForce bool
		}{
			{"uppercase", "RENAME DATABASE \"OldDB\" TO \"NewDB\"", "OldDB", "NewDB", false},
			{"lowercase", "rename database \"olddb2\" to \"newdb2\"", "olddb2", "newdb2", false},
			{"mixed case", "ReNaMe DaTaBaSe \"Old3\" To \"New3\"", "Old3", "New3", false},
			{"with force", "RENAME DATABASE \"Old4\" TO \"New4\" FORCE", "Old4", "New4", true},
			{"no quotes", "rename database OldDB to NewDB", "OldDB", "NewDB", false},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				result, err := db.ParseRenameDatabaseCommand(tt.command)
				if err != nil {
					t.Errorf("ParseRenameDatabaseCommand() error = %v", err)
					return
				}
				if result.DatabaseName != tt.wantOld {
					t.Errorf("ParseRenameDatabaseCommand() got DatabaseName = %v, want %v", result.DatabaseName, tt.wantOld)
				}
				if result.NewDatabaseName != tt.wantNew {
					t.Errorf("ParseRenameDatabaseCommand() got NewDatabaseName = %v, want %v", result.NewDatabaseName, tt.wantNew)
				}
				if result.Force != tt.wantForce {
					t.Errorf("ParseRenameDatabaseCommand() got Force = %v, want %v", result.Force, tt.wantForce)
				}
				if result.CommandType != "RENAME" {
					t.Errorf("ParseRenameDatabaseCommand() got CommandType = %v, want RENAME", result.CommandType)
				}
			})
		}
	})
}
