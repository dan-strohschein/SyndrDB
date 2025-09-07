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

// VerifyCredentials checks if the provided credentials are valid with rate limiting
func (s *UserStore) VerifyCredentials(username, password string) (bool, *User, error) {
	return s.VerifyCredentialsWithIP(username, password, "")
}

// VerifyCredentialsWithIP checks credentials with IP-based rate limiting
func (s *UserStore) VerifyCredentialsWithIP(username, password, clientIP string) (bool, *User, error) {
	// Check rate limiting if enabled
	if s.rateLimiter != nil {
		// Check if user account is locked
		if s.rateLimiter.IsUserLocked(username) {
			isLocked, lockedUntil, attempts := s.rateLimiter.GetUserLockoutInfo(username)
			if isLocked {
				if s.logger != nil {
					s.logger.Warnw("Authentication blocked - user account locked",
						"username", username,
						"ip", clientIP,
						"attempts", attempts,
						"lockedUntil", lockedUntil)
				}
				return false, nil, NewUserLockoutError(username, lockedUntil, attempts)
			}
		}

		// Check if IP is locked
		if clientIP != "" && s.rateLimiter.IsIPLocked(clientIP) {
			isLocked, lockedUntil, attempts := s.rateLimiter.GetIPLockoutInfo(clientIP)
			if isLocked {
				if s.logger != nil {
					s.logger.Warnw("Authentication blocked - IP address locked",
						"username", username,
						"ip", clientIP,
						"attempts", attempts,
						"lockedUntil", lockedUntil)
				}
				return false, nil, NewIPLockoutError(clientIP, lockedUntil, attempts)
			}
		}
	}

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
				// Successful authentication
				if s.rateLimiter != nil {
					s.rateLimiter.RecordSuccessfulAttempt(username, clientIP)
				}

				// Log successful authentication to audit
				if s.auditor != nil {
					details := map[string]interface{}{
						"user_id":     storedUser.UserID,
						"auth_method": "password",
					}
					s.auditor.LogAuthenticationEvent(true, username, clientIP, 0, "", "", details)
				}

				if s.logger != nil {
					s.logger.Infow("Authentication successful",
						"username", username,
						"ip", clientIP)
				}
				return true, &User{
					UserID:   storedUser.UserID,
					Username: storedUser.Username,
					// Don't include password
				}, nil
			}

			// Authentication failed - record and apply progressive delay
			if s.rateLimiter != nil {
				s.rateLimiter.RecordFailedAttempt(username, clientIP)

				// Apply progressive delay after recording the failure
				delay := s.rateLimiter.GetProgressiveDelay(username, clientIP)
				if delay > 0 {
					// Log progressive delay to audit
					if s.auditor != nil {
						details := map[string]interface{}{
							"delay_duration":       delay.String(),
							"consecutive_failures": "progressive_delay",
						}
						s.auditor.LogRateLimitEvent("PROGRESSIVE_DELAY", username, clientIP, 0, details)
					}

					if s.logger != nil {
						s.logger.Infow("Authentication rate limited - progressive delay applied",
							"username", username,
							"ip", clientIP,
							"delay", delay)
					}
					return false, nil, NewDelayError(delay)
				}
			}

			// Log authentication failure for invalid password
			if s.auditor != nil {
				details := map[string]interface{}{
					"failure_reason": "invalid_password",
					"user_exists":    true,
				}
				s.auditor.LogAuthenticationEvent(false, username, clientIP, 0, "", "INVALID_PASSWORD", details)
			}

			if s.logger != nil {
				s.logger.Warnw("Authentication failed - invalid password",
					"username", username,
					"ip", clientIP)
			}
			return false, nil, nil
		}
	}

	// User not found - record failed attempt and apply progressive delay
	if s.rateLimiter != nil {
		s.rateLimiter.RecordFailedAttempt(username, clientIP)

		// Apply progressive delay after recording the failure
		delay := s.rateLimiter.GetProgressiveDelay(username, clientIP)
		if delay > 0 {
			// Log progressive delay audit event
			if s.auditor != nil {
				details := map[string]interface{}{
					"delay_duration":       delay.String(),
					"failure_reason":       "user_not_found",
					"consecutive_failures": "progressive_delay",
				}
				s.auditor.LogRateLimitEvent("PROGRESSIVE_DELAY", username, clientIP, 0, details)
			}

			if s.logger != nil {
				s.logger.Infow("Authentication rate limited - progressive delay applied",
					"username", username,
					"ip", clientIP,
					"delay", delay)
			}
			return false, nil, NewDelayError(delay)
		}
	}

	// Log authentication failure for user not found
	if s.auditor != nil {
		details := map[string]interface{}{
			"failure_reason": "user_not_found",
			"user_exists":    false,
		}
		s.auditor.LogAuthenticationEvent(false, username, clientIP, 0, "", "USER_NOT_FOUND", details)
	}

	if s.logger != nil {
		s.logger.Warnw("Authentication failed - user not found",
			"username", username,
			"ip", clientIP)
	}
	return false, nil, nil
}
