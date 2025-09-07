package auth

// Add custom error definitions here
import (
	"errors"
	"fmt"
	"time"
)

// ErrUserAlreadyExists is returned when a user already exists in the system.
var ErrUserAlreadyExists = errors.New("user already exists")
var ErrUserNotFound = errors.New("user not found")

// Authentication rate limiting errors
var (
	ErrUserAccountLocked = errors.New("user account is temporarily locked due to too many failed authentication attempts")
	ErrIPAddressBlocked  = errors.New("IP address is temporarily blocked due to suspicious activity")
	ErrTooManyAttempts   = errors.New("too many authentication attempts, please wait before trying again")
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
