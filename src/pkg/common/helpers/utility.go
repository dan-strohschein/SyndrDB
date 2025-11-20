package helpers

import (
	"regexp"
	"strings"
	"sync"
	"syndrdb/src/pkg/settings"
	"time"

	"github.com/google/uuid"
)

// STEP 5: Path string interning cache to avoid repeated allocations
var pathCache sync.Map

// InternPath returns a cached version of the path string to reduce allocations
func InternPath(path string) string {
	if cached, ok := pathCache.Load(path); ok {
		return cached.(string)
	}
	pathCache.Store(path, path)
	return path
}

func init() {
	// Pre-populate common path patterns
	commonPaths := []string{
		"data_files",
		"indexes",
		"primary",
		"testdb",
	}
	for _, p := range commonPaths {
		pathCache.Store(p, p)
	}
}

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
	// STEP 5: Check cache first to avoid repeated string allocations
	args := settings.GetSettings()
	cacheKey := args.DataDir + "/" + databaseName + "/"

	return InternPath(cacheKey)
}

func NormalizeCommand(command string) string {
	// Normalize the command to NFC form
	cmd := strings.TrimSpace(command)
	cmd = strings.ReplaceAll(cmd, "\n", " ")
	cmd = strings.ReplaceAll(cmd, "\t", " ")
	cmd = strings.ReplaceAll(cmd, "\r", " ")

	// Protect WITH RELATIONSHIP before normalizing whitespace
	cmd = regexp.MustCompile(`(?i)\s*WITH\s+RELATIONSHIP\s+`).ReplaceAllString(cmd, " WITH_RELATIONSHIP ")

	// Now normalize whitespace
	cmd = strings.Join(strings.Fields(cmd), " ")

	// Restore WITH RELATIONSHIP
	cmd = strings.ReplaceAll(cmd, " WITH_RELATIONSHIP ", " WITH RELATIONSHIP ")

	return cmd
}
