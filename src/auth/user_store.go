package auth

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// NewUserStore creates a new user store
func NewUserStore(filePath string, encryptionKeyString string) (*UserStore, error) {
	// Create directory if it doesn't exist
	dir := filepath.Dir(filePath)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, fmt.Errorf("failed to create directory: %w", err)
	}

	// Convert encryption key string to bytes (32 bytes for AES-256)
	encryptionKey := []byte(encryptionKeyString)
	if len(encryptionKey) < 32 {
		// Pad the key if it's too short
		paddedKey := make([]byte, 32)
		copy(paddedKey, encryptionKey)
		encryptionKey = paddedKey
	} else if len(encryptionKey) > 32 {
		// Truncate if too long
		encryptionKey = encryptionKey[:32]
	}

	store := &UserStore{
		encryptionKey: encryptionKey,
		filePath:      filePath,
		users:         []User{},
		dirty:         false,
	}

	// Load existing users if the file exists
	if _, err := os.Stat(filePath); err == nil {
		if err := store.Load(); err != nil {
			return nil, fmt.Errorf("failed to load user store: %w", err)
		}
	}

	return store, nil
}

// Save persists the user store to disk
func (s *UserStore) Save() error {
	if !s.dirty {
		return nil // Nothing to save
	}

	// Serialize users to JSON
	data, err := json.Marshal(s.users)
	if err != nil {
		return fmt.Errorf("failed to marshal users: %w", err)
	}

	// Encrypt the data
	encryptedData, err := encrypt(data, s.encryptionKey)
	if err != nil {
		return fmt.Errorf("failed to encrypt data: %w", err)
	}

	// Create a temporary file
	tempFile, err := os.CreateTemp(filepath.Dir(s.filePath), "users-*.tmp")
	if err != nil {
		return fmt.Errorf("failed to create temp file: %w", err)
	}
	tempFilePath := tempFile.Name()

	// Write encrypted data to the temporary file
	if _, err := tempFile.Write(encryptedData); err != nil {
		tempFile.Close()
		os.Remove(tempFilePath)
		return fmt.Errorf("failed to write to temp file: %w", err)
	}

	// Close the file before renaming
	if err := tempFile.Close(); err != nil {
		os.Remove(tempFilePath)
		return fmt.Errorf("failed to close temp file: %w", err)
	}

	// Set restrictive permissions
	if err := os.Chmod(tempFilePath, 0600); err != nil {
		os.Remove(tempFilePath)
		return fmt.Errorf("failed to set file permissions: %w", err)
	}

	// Atomically replace the old file with the new one
	if err := os.Rename(tempFilePath, s.filePath); err != nil {
		os.Remove(tempFilePath)
		return fmt.Errorf("failed to rename temp file: %w", err)
	}

	s.dirty = false
	return nil
}

// Load reads the user store from disk
func (s *UserStore) Load() error {
	// Open the file
	file, err := os.Open(s.filePath)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	// Read encrypted data
	encryptedData, err := io.ReadAll(file)
	if err != nil {
		return fmt.Errorf("failed to read file: %w", err)
	}

	// Decrypt the data
	data, err := decrypt(encryptedData, s.encryptionKey)
	if err != nil {
		return fmt.Errorf("failed to decrypt data: %w", err)
	}

	// Unmarshal the JSON
	var users []User
	if err := json.Unmarshal(data, &users); err != nil {
		return fmt.Errorf("failed to unmarshal users: %w", err)
	}

	s.users = users
	return nil
}
