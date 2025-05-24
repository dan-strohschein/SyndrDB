package auth

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"errors"
	"io"

	"golang.org/x/crypto/argon2"
)

// Helper function to encrypt data
func encrypt(data, key []byte) ([]byte, error) {
	// Create cipher block
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}

	// Create GCM mode
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}

	// Create nonce
	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, err
	}

	// Encrypt and seal
	ciphertext := gcm.Seal(nonce, nonce, data, nil)
	return ciphertext, nil
}

// Helper function to decrypt data
func decrypt(data, key []byte) ([]byte, error) {
	// Create cipher block
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}

	// Create GCM mode
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}

	// Get nonce size
	nonceSize := gcm.NonceSize()
	if len(data) < nonceSize {
		return nil, errors.New("ciphertext too short")
	}

	// Extract nonce and ciphertext
	nonce, ciphertext := data[:nonceSize], data[nonceSize:]

	// Decrypt
	return gcm.Open(nil, nonce, ciphertext, nil)
}

// Constant-time comparison to prevent timing attacks
func SlowEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}

	var result byte
	for i := 0; i < len(a); i++ {
		result |= a[i] ^ b[i]
	}

	return result == 0
}

// VerifyCredentials checks if the provided credentials are valid
func (s *UserStore) VerifyCredentials(username, password string) (bool, *User, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	for _, storedUser := range s.users {
		if storedUser.Username == username {
			// Hash the password using the same parameters and salt
			hash := argon2.IDKey(
				[]byte(password),
				storedUser.PasswordHash.Salt,
				storedUser.PasswordHash.Time,
				storedUser.PasswordHash.Memory,
				storedUser.PasswordHash.Threads,
				storedUser.PasswordHash.KeyLen,
			)

			// Compare the hashes (constant-time comparison to prevent timing attacks)
			if SlowEqual(hash, storedUser.PasswordHash.Hash) {
				return true, &User{
					UserID:   storedUser.UserID,
					Username: storedUser.Username,
					// Don't include password
				}, nil
			}
			return false, nil, nil
		}
	}

	return false, nil, nil
}
