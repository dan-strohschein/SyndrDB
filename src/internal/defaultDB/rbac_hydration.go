package defaultdb

/*
rbac_hydration.go

This file contains RBAC-specific hydration functions that must be called AFTER
the UserService and PermissionService are initialized to properly hash passwords
and set up role-based access control for default users.

The main hydration (HydrateUserPrimaryCatalogs) creates users with plaintext passwords
because it runs before RBAC services are available. This file provides post-initialization
functions to upgrade those users to properly hashed passwords.
*/

import (
	"fmt"
	"syndrdb/src/internal/auth"

	"go.uber.org/zap"
)

// UserServiceInterface defines the minimal interface needed from UserService
// This avoids import cycles while allowing us to work with the service
type UserServiceInterface interface {
	GetUserByUsername(username string) (map[string]interface{}, error)
	ValidateUserCredentials(username, password string) (bool, error)
}

// UpdateDefaultUserPasswords updates the default users (Root, Admin, Reader, Writer)
// with properly hashed passwords using the UserService's UserStore.
// This should be called after UserService is initialized.
func UpdateDefaultUserPasswords(userStore *auth.UserStore, logger *zap.SugaredLogger) error {
	if userStore == nil {
		return fmt.Errorf("userStore is nil - cannot update passwords")
	}

	logger.Info("Updating default user passwords with Argon2id hashing...")

	// Define default users and their passwords
	defaultUsers := map[string]string{
		"Root":   "root",
		"Admin":  "admin123",
		"Reader": "reader123",
		"Writer": "writer123",
	}

	successCount := 0
	for username, password := range defaultUsers {
		// Check if user already exists in UserStore
		existingUser, err := userStore.GetUser(username)

		if existingUser != nil {
			// User exists, update their password
			logger.Infof("Updating password for existing user: %s", username)
			newUser := auth.NewUser{
				UserID:   existingUser.UserID,
				Username: username,
				Password: password,
			}

			err = userStore.UpdateUser(newUser)
			if err != nil {
				logger.Warnf("Failed to update password for user %s: %v", username, err)
				continue
			}
		} else {
			// User doesn't exist, create them
			logger.Infof("Creating new user in UserStore: %s", username)
			newUser := auth.NewUser{
				UserID:   fmt.Sprintf("user-%s", username), // Generate consistent UserID
				Username: username,
				Password: password,
			}

			_, err = userStore.AddUser(newUser)
			if err != nil {
				logger.Warnf("Failed to create user %s in UserStore: %v", username, err)
				continue
			}
		}

		logger.Infof("✓ Updated password for user: %s (Argon2id hashed)", username)
		successCount++
	}

	logger.Infof("Successfully updated passwords for %d/%d default users", successCount, len(defaultUsers))

	if successCount < len(defaultUsers) {
		logger.Warn("Not all default user passwords were updated. Some users may have plaintext passwords.")
		logger.Warn("It is STRONGLY RECOMMENDED to change the Root password immediately after first login.")
	}

	return nil
}

// VerifyDefaultUsersExist checks if the default users (Root, Admin, Reader, Writer)
// exist in the UserStore and logs their status
func VerifyDefaultUsersExist(userStore *auth.UserStore, logger *zap.SugaredLogger) error {
	if userStore == nil {
		return fmt.Errorf("userStore is nil")
	}

	logger.Info("Verifying default users in UserStore...")

	defaultUsers := []string{"Root", "Admin", "Reader", "Writer"}

	for _, username := range defaultUsers {
		user, err := userStore.GetUser(username)
		if err != nil {
			logger.Warnf("Default user %s not found in UserStore: %v", username, err)
			continue
		}

		if user == nil {
			logger.Warnf("Default user %s exists but returned nil", username)
			continue
		}

		logger.Infof("✓ Default user %s found in UserStore", username)
	}

	return nil
}
