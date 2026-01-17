package auth

// Add custom error definitions here
import (
	"fmt"
	"time"

	"syndrdb/src/pkg/errors"
)

// ErrUserAlreadyExists is returned when a user already exists in the system.
// DEPRECATED: Use errors.New(errors.ERR_VALIDATION_CONSTRAINT, ...) instead
var ErrUserAlreadyExists = errors.New(errors.ERR_VALIDATION_CONSTRAINT, "user already exists", errors.LayerAuth)

// ErrUserNotFound is returned when a user is not found.
// DEPRECATED: Use errors.New(errors.ERR_NOT_FOUND_USER, ...) instead
var ErrUserNotFound = errors.New(errors.ERR_NOT_FOUND_USER, "user not found", errors.LayerAuth)

// Authentication rate limiting errors - converted to use error framework
var (
	// ErrUserAccountLocked is returned when a user account is locked due to too many failed attempts
	ErrUserAccountLocked = errors.New(errors.ERR_AUTH_LOCKOUT, 
		"user account is temporarily locked due to too many failed authentication attempts", 
		errors.LayerAuth)
	
	// ErrIPAddressBlocked is returned when an IP address is blocked due to suspicious activity
	ErrIPAddressBlocked = errors.New(errors.ERR_AUTH_LOCKOUT,
		"IP address is temporarily blocked due to suspicious activity",
		errors.LayerAuth)
	
	// ErrTooManyAttempts is returned when rate limiting delays authentication
	ErrTooManyAttempts = errors.New(errors.ERR_AUTH_RATE_LIMIT,
		"too many authentication attempts, please wait before trying again",
		errors.LayerAuth)
)

// AuthLockoutError provides detailed information about lockout status
type AuthLockoutError struct {
	Type        string // "user", "ip", or "delay"
	Username    string
	IP          string
	LockedUntil time.Time
	Attempts    int
	Delay       time.Duration
	Message     string
}

func (e *AuthLockoutError) Error() string {
	if e.Message != "" {
		return e.Message
	}

	switch e.Type {
	case "user":
		return fmt.Sprintf("user account '%s' is locked until %s due to %d failed attempts",
			e.Username, e.LockedUntil.Format("15:04:05"), e.Attempts)
	case "ip":
		return fmt.Sprintf("IP address %s is blocked until %s due to %d failed attempts",
			e.IP, e.LockedUntil.Format("15:04:05"), e.Attempts)
	case "delay":
		return fmt.Sprintf("authentication rate limited, please wait %s before trying again",
			e.Delay.String())
	default:
		return "authentication blocked due to security restrictions"
	}
}

// NewUserLockoutError creates a new user lockout error
func NewUserLockoutError(username string, lockedUntil time.Time, attempts int) *AuthLockoutError {
	return &AuthLockoutError{
		Type:        "user",
		Username:    username,
		LockedUntil: lockedUntil,
		Attempts:    attempts,
	}
}

// NewIPLockoutError creates a new IP lockout error
func NewIPLockoutError(ip string, lockedUntil time.Time, attempts int) *AuthLockoutError {
	return &AuthLockoutError{
		Type:        "ip",
		IP:          ip,
		LockedUntil: lockedUntil,
		Attempts:    attempts,
	}
}

// NewDelayError creates a new progressive delay error
func NewDelayError(delay time.Duration) *AuthLockoutError {
	return &AuthLockoutError{
		Type:  "delay",
		Delay: delay,
	}
}
