package helpers

import (
	"strings"
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
