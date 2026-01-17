package auth

import (
	"encoding/json"
	"io"
	"os"
	"path/filepath"

	"syndrdb/src/pkg/constants"
	"syndrdb/src/pkg/errors"

	"go.uber.org/zap"
)

type UserFactory interface {
	NewUserStruct(name, password string) *NewUser
}

// NewUserStore creates a new user store
func NewUserStore(filePath string, encryptionKeyString string) (*UserStore, error) {
	return NewUserStoreWithRateLimit(filePath, encryptionKeyString, nil, nil)
}

// NewUserStoreWithRateLimit creates a new user store with authentication rate limiting
func NewUserStoreWithRateLimit(filePath string, encryptionKeyString string, logger *zap.SugaredLogger, rateLimitConfig *AuthRateLimitConfig) (*UserStore, error) {
	return NewUserStoreWithAuditor(filePath, encryptionKeyString, logger, rateLimitConfig, nil)
}

// NewUserStoreWithAuditor creates a new user store with authentication rate limiting and audit logging
func NewUserStoreWithAuditor(filePath string, encryptionKeyString string, logger *zap.SugaredLogger, rateLimitConfig *AuthRateLimitConfig, auditor SecurityAuditor) (*UserStore, error) {
	// Create directory if it doesn't exist
	dir := filepath.Dir(filePath)
	if err := os.MkdirAll(dir, constants.DirPermissionsSecure); err != nil {
		return nil, errors.WrapWithMessage(err, errors.ERR_INTERNAL_STORAGE,
			"failed to create user store directory", errors.LayerAuth).WithContext("path", dir)
	}

	// Convert encryption key string to bytes (32 bytes for AES-256)
	encryptionKey := []byte(encryptionKeyString)
	if len(encryptionKey) < constants.EncryptionKeyMinLength {
		// Pad the key if it's too short
		paddedKey := make([]byte, constants.EncryptionKeyMinLength)
		copy(paddedKey, encryptionKey)
		encryptionKey = paddedKey
	} else if len(encryptionKey) > constants.EncryptionKeyMinLength {
		// Truncate if too long
		encryptionKey = encryptionKey[:constants.EncryptionKeyMinLength]
	}

	// Initialize rate limiter if logger is provided
	var rateLimiter *AuthRateLimiter
	if logger != nil {
		rateLimiter = NewAuthRateLimiterWithAuditor(rateLimitConfig, logger, auditor)
	}

	store := &UserStore{
		encryptionKey: encryptionKey,
		filePath:      filePath,
		users:         []User{},
		dirty:         false,
		rateLimiter:   rateLimiter,
		auditor:       auditor,
		logger:        logger,
	}

	// Load existing users if the file exists
	if _, err := os.Stat(filePath); err == nil {
		if err := store.Load(); err != nil {
			return nil, errors.WrapWithMessage(err, errors.ERR_INTERNAL_STORAGE,
				"failed to load user store", errors.LayerAuth).WithContext("path", filePath)
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
		return errors.WrapWithMessage(err, errors.ERR_INTERNAL_STORAGE,
			"failed to marshal users", errors.LayerAuth)
	}

	// Encrypt the data
	encryptedData, err := encrypt(data, s.encryptionKey)
	if err != nil {
		return errors.WrapWithMessage(err, errors.ERR_INTERNAL_STORAGE,
			"failed to encrypt user data", errors.LayerAuth)
	}

	// Create a temporary file
	tempFile, err := os.CreateTemp(filepath.Dir(s.filePath), "users-*.tmp")
	if err != nil {
		return errors.WrapWithMessage(err, errors.ERR_INTERNAL_STORAGE,
			"failed to create temporary file for user store", errors.LayerAuth)
	}
	tempFilePath := tempFile.Name()

	// Write encrypted data to the temporary file
	if _, err := tempFile.Write(encryptedData); err != nil {
		tempFile.Close()
		os.Remove(tempFilePath)
		return errors.WrapWithMessage(err, errors.ERR_INTERNAL_STORAGE,
			"failed to write to temporary file", errors.LayerAuth)
	}

	// Close the file before renaming
	if err := tempFile.Close(); err != nil {
		os.Remove(tempFilePath)
		return errors.WrapWithMessage(err, errors.ERR_INTERNAL_STORAGE,
			"failed to close temporary file", errors.LayerAuth)
	}

	// Set restrictive permissions
	if err := os.Chmod(tempFilePath, constants.FilePermissionsSecure); err != nil {
		os.Remove(tempFilePath)
		return errors.WrapWithMessage(err, errors.ERR_INTERNAL_STORAGE,
			"failed to set file permissions", errors.LayerAuth)
	}

	// Atomically replace the old file with the new one
	if err := os.Rename(tempFilePath, s.filePath); err != nil {
		os.Remove(tempFilePath)
		return errors.WrapWithMessage(err, errors.ERR_INTERNAL_STORAGE,
			"failed to replace user store file atomically", errors.LayerAuth)
	}

	s.dirty = false
	return nil
}

// Load reads the user store from disk
func (s *UserStore) Load() error {
	// Open the file
	file, err := os.Open(s.filePath)
	if err != nil {
		return errors.WrapWithMessage(err, errors.ERR_INTERNAL_STORAGE,
			"failed to open user store file", errors.LayerAuth).WithContext("path", s.filePath)
	}
	defer file.Close()

	// Read encrypted data
	encryptedData, err := io.ReadAll(file)
	if err != nil {
		return errors.WrapWithMessage(err, errors.ERR_INTERNAL_STORAGE,
			"failed to read user store file", errors.LayerAuth)
	}

	// Decrypt the data
	data, err := decrypt(encryptedData, s.encryptionKey)
	if err != nil {
		return errors.WrapWithMessage(err, errors.ERR_INTERNAL_STORAGE,
			"failed to decrypt user data", errors.LayerAuth)
	}

	// Unmarshal the JSON
	var users []User
	if err := json.Unmarshal(data, &users); err != nil {
		return errors.WrapWithMessage(err, errors.ERR_INTERNAL_STORAGE,
			"failed to unmarshal user data", errors.LayerAuth)
	}

	s.users = users
	return nil
}

// GetUserByName retrieves a user by their username
func (store *UserStore) GetUserByName(userName string) (*User, error) {
	for _, user := range store.users {
		if user.Username == userName {
			return &user, nil
		}
	}
	// If user not found, return an error

	return nil, ErrUserNotFound

}

// GetAllUsers retrieves all users from the store
func (store *UserStore) GetAllUsers() ([]*User, error) {
	var userList []*User
	for _, user := range store.users {
		userList = append(userList, &user)
	}
	return userList, nil
}
