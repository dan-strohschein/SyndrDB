package helpers

import "github.com/google/uuid"

// Add this function to generate UUIDs
func GenerateUUID() string {
	return uuid.New().String()
}
