package homegrown

import (
	"strings"
	"testing"

	"syndrdb/src/internal/domain/database"
)

func TestParseRenameDatabaseCommand_Success(t *testing.T) {
	command := `RENAME DATABASE "olddb" TO "newdb"`
	
	dbCommand, err := database.ParseRenameDatabaseCommand(command)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	
	if dbCommand.CommandType != "RENAME" {
		t.Errorf("Expected CommandType 'RENAME', got '%s'", dbCommand.CommandType)
	}
	
	if dbCommand.DatabaseName != "olddb" {
		t.Errorf("Expected DatabaseName 'olddb', got '%s'", dbCommand.DatabaseName)
	}
	
	if dbCommand.NewDatabaseName != "newdb" {
		t.Errorf("Expected NewDatabaseName 'newdb', got '%s'", dbCommand.NewDatabaseName)
	}
	
	if dbCommand.Force {
		t.Error("Expected Force to be false")
	}
}

func TestParseRenameDatabaseCommand_WithForce(t *testing.T) {
	command := `RENAME DATABASE "testdb" TO "renameddb" FORCE`
	
	dbCommand, err := database.ParseRenameDatabaseCommand(command)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	
	if !dbCommand.Force {
		t.Error("Expected Force to be true")
	}
}

func TestParseRenameDatabaseCommand_InvalidOldName_StartsWithNumber(t *testing.T) {
	command := `RENAME DATABASE "1invaliddb" TO "validdb"`
	
	_, err := database.ParseRenameDatabaseCommand(command)
	if err == nil {
		t.Fatal("Expected error for invalid old database name, got none")
	}
	
	if !strings.Contains(err.Error(), "invalid old database name") {
		t.Errorf("Expected invalid old name error, got: %v", err)
	}
}

func TestParseRenameDatabaseCommand_SameOldAndNewName(t *testing.T) {
	command := `RENAME DATABASE "samedb" TO "samedb"`
	
	_, err := database.ParseRenameDatabaseCommand(command)
	if err == nil {
		t.Fatal("Expected error for same old and new name, got none")
	}
	
	if !strings.Contains(err.Error(), "new database name cannot be the same as the current name") {
		t.Errorf("Expected same name error, got: %v", err)
	}
}
