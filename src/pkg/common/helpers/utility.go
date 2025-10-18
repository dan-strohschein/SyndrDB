package helpers

import (
	"fmt"
	"strings"
	"syndrdb/src/pkg/settings"
	"time"

	"github.com/google/uuid"
)

// Add this function to generate UUIDs
func GenerateUUID() string {
	return uuid.New().String()
}

// Helper function to properly remove quotes from strings
func StripQuotes(s string) string {
	s = strings.TrimSpace(s)
	if len(s) >= 2 {
		if (s[0] == '"' && s[len(s)-1] == '"') || (s[0] == '\'' && s[len(s)-1] == '\'') {
			return s[1 : len(s)-1]
		}
	}
	return s
}

// timeNow returns the current time as a string
func TimeNow() string {
	return time.Now().Format(time.RFC3339)
}

func ParseBool(value string) bool {
	value = strings.ToLower(strings.TrimSpace(value))
	return value == "true"
}

func CleanFileName(name string) string {
	// Replace characters that might be problematic in filenames
	return strings.ReplaceAll(name, "-", "_")
}

func GetDatabaseFolderPath(databaseName string) string {
	var results string

	args := settings.GetSettings()
	results = fmt.Sprintf("%s/%s/", args.DataDir, databaseName)

	return results
}

func NormalizeCommand(command string) string {
	// Normalize the command to NFC form
	cmd := strings.TrimSpace(command)
	cmd = strings.ReplaceAll(cmd, "\n", " ")
	cmd = strings.ReplaceAll(cmd, "\t", " ")
	cmd = strings.ReplaceAll(cmd, "\r", " ")
	cmd = strings.Join(strings.Fields(cmd), " ")
	return cmd
}
