package auth

import (
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"golang.org/x/crypto/argon2"
)

type PasswordHash struct {
	Hash    []byte `json:"hash"`
	Salt    []byte `json:"salt"`
	Method  string `json:"method"`  // "argon2id"
	Time    uint32 `json:"time"`    // time parameter for Argon2
	Memory  uint32 `json:"memory"`  // memory parameter in KiB
	Threads uint8  `json:"threads"` // threads parameter
	KeyLen  uint32 `json:"keylen"`  // length of the hash in bytes
}

type User struct {
	ID             string
	Username       string
	PasswordHash   PasswordHash
	CreatedAt      time.Time
	LastModifiedAt time.Time
}

type NewUser struct {
	ID       string
	Username string
	Password string
}

// UserStore manages secure storage of user credentials
type UserStore struct {
	encryptionKey []byte       // Key used to encrypt the storage file
	filePath      string       // Path to the storage file
	users         []User       // In-memory cache of users
	mu            sync.RWMutex // Mutex for thread safety
	dirty         bool         // Whether the store has unsaved changes
}

type UserPermissions struct {
	UserID      string
	AssetType   string        //Database, Bundle, Document
	Permissions []Permissions //relates the user to the asset for a specific set of permissions
}

type Permissions struct {
	Read  bool
	Write bool
}

// GetUser retrieves a user by username
func (s *UserStore) GetUser(username string) (*User, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	for _, storedUser := range s.users {
		if storedUser.Username == username {
			return &User{
				ID:       storedUser.ID,
				Username: storedUser.Username,
				// Don't include password
			}, nil
		}
	}

	return nil, errors.New("user not found")
}

// ListUsers returns a list of all usernames
func (s *UserStore) ListUsers() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	usernames := make([]string, len(s.users))
	for i, user := range s.users {
		usernames[i] = user.Username
	}

	return usernames
}

// AddUser adds a new user to the store
func (s *UserStore) AddUser(user NewUser) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Check if username already exists
	for _, existingUser := range s.users {
		if existingUser.Username == user.Username {
			return errors.New("username already exists")
		}
	}

	// Generate salt
	salt := make([]byte, 16)
	if _, err := io.ReadFull(rand.Reader, salt); err != nil {
		return fmt.Errorf("failed to generate salt: %w", err)
	}

	// Hash the password using Argon2id
	// Parameters recommended by OWASP:
	// - Time: 1
	// - Memory: 64 * 1024 (64 MB)
	// - Threads: 4
	// - Key length: 32 bytes
	timeParam := uint32(1)
	memory := uint32(64 * 1024)
	threads := uint8(4)
	keyLen := uint32(32)
	hash := argon2.IDKey([]byte(user.Password), salt, timeParam, memory, threads, keyLen)

	// Create stored user
	storedUser := User{
		ID:       user.ID,
		Username: user.Username,
		PasswordHash: PasswordHash{
			Hash:    hash,
			Salt:    salt,
			Method:  "argon2id",
			Time:    timeParam,
			Memory:  memory,
			Threads: threads,
			KeyLen:  keyLen,
		},
		CreatedAt:      time.Now(),
		LastModifiedAt: time.Now(),
	}

	s.users = append(s.users, storedUser)
	s.dirty = true

	// Save the changes
	return s.Save()
}

// UpdateUser updates an existing user
func (s *UserStore) UpdateUser(updatedUser NewUser) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for i, existingUser := range s.users {
		if existingUser.Username == updatedUser.Username {
			// Generate salt
			salt := make([]byte, 16)
			if _, err := io.ReadFull(rand.Reader, salt); err != nil {
				return fmt.Errorf("failed to generate salt: %w", err)
			}

			// Hash the password using Argon2id
			timeParam := uint32(1)
			memory := uint32(64 * 1024)
			threads := uint8(4)
			keyLen := uint32(32)
			hash := argon2.IDKey([]byte(updatedUser.Password), salt, timeParam, memory, threads, keyLen)

			// Update the user
			s.users[i].PasswordHash = PasswordHash{
				Hash:    hash,
				Salt:    salt,
				Method:  "argon2id",
				Time:    timeParam,
				Memory:  memory,
				Threads: threads,
				KeyLen:  keyLen,
			}
			s.users[i].LastModifiedAt = time.Now()
			s.dirty = true

			// Save the changes
			return s.Save()
		}
	}

	return errors.New("user not found")
}

// RemoveUser removes a user from the store
func (s *UserStore) RemoveUser(username string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for i, existingUser := range s.users {
		if existingUser.Username == username {
			// Remove the user
			s.users = append(s.users[:i], s.users[i+1:]...)
			s.dirty = true

			// Save the changes
			return s.Save()
		}
	}

	return errors.New("user not found")
}
