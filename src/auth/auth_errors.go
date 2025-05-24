package auth

// Add custom error definitions here
import "errors"

// ErrUserAlreadyExists is returned when a user already exists in the system.
var ErrUserAlreadyExists = errors.New("user already exists")
var ErrUserNotFound = errors.New("user not found")
