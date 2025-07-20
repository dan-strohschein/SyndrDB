package hashindex

import "strings"

// cleanFileName sanitizes a string for use as a filename
func CleanFileName(name string) string {
	// Replace characters that might be problematic in filenames
	return strings.ReplaceAll(name, "-", "_")
}
