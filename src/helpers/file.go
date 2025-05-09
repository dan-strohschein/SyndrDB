package helpers

import (
	"fmt"
	"log"
	"os"
)

// fileExists checks if a file exists and is not a directory
func FileExists(filename string) bool {
	info, err := os.Stat(filename)
	if err != nil {
		if os.IsNotExist(err) {
			log.Printf("File does not exist: %s\n", filename)
			return false // File does not exist

		}
		fmt.Errorf("Error checking file %s for existence: %s\n", filename, err)
		return false // Some other error occurred
	}

	return !info.IsDir() // Return true if it's not a directory
}
