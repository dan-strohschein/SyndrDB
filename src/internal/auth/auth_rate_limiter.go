package auth

import (
	"sync"
	"time"

	"go.uber.org/zap"
)

// SecurityAuditor interface for audit logging
type SecurityAuditor interface {
	LogAuthenticationEvent(success bool, username, ipAddress string, port int, sessionID, errorCode string, details map[string]interface{})
	LogRateLimitEvent(eventType string, username, ipAddress string, port int, details map[string]interface{})
}

// AuthRateLimitConfig holds authentication-specific rate limiting configuration
type AuthRateLimitConfig struct {
	// User account lockout settings
	MaxFailedAttempts   int           // Max failed attempts before user lockout (default: 5)
	UserLockoutDuration time.Duration // User lockout duration (default: 15 minutes)
	AttemptWindow       time.Duration // Time window for counting attempts (default: 1 hour)

	// Progressive delay settings
	EnableProgressiveDelay bool // Enable progressive delays between attempts
	BaseDelaySeconds       int  // Base delay in seconds (default: 2)
	MaxDelaySeconds        int  // Maximum delay in seconds (default: 60)

	// IP-based limits (additional layer)
	MaxAttemptsPerIP  int           // Max failed attempts per IP (default: 20)
	IPLockoutDuration time.Duration // IP lockout duration (default: 30 minutes)

	// Cleanup settings
	CleanupInterval time.Duration // How often to clean old records (default: 5 minutes)
}

// DefaultAuthRateLimitConfig returns default authentication rate limiting configuration
func DefaultAuthRateLimitConfig() *AuthRateLimitConfig {
	return &AuthRateLimitConfig{
		MaxFailedAttempts:      5,
		UserLockoutDuration:    15 * time.Minute,
		AttemptWindow:          1 * time.Hour,
		EnableProgressiveDelay: true,
		BaseDelaySeconds:       2,
		MaxDelaySeconds:        60,
		MaxAttemptsPerIP:       20,
		IPLockoutDuration:      30 * time.Minute,
		CleanupInterval:        5 * time.Minute,
	}
}

// UserAuthTracker tracks authentication attempts for a specific user
type UserAuthTracker struct {
	Username            string
	FailedAttempts      int
	LastAttempt         time.Time
	LockedUntil         time.Time
	AttemptWindow       time.Time
	LastSuccessfulAuth  time.Time
	ConsecutiveFailures int // For progressive delay calculation
}

// IPAuthTracker tracks authentication attempts from a specific IP
type IPAuthTracker struct {
	IP             string
	FailedAttempts int
	LastAttempt    time.Time
	LockedUntil    time.Time
	AttemptWindow  time.Time
}

// AuthRateLimiter provides authentication-specific brute force protection
type AuthRateLimiter struct {
	config       *AuthRateLimitConfig
	userTrackers map[string]*UserAuthTracker
	ipTrackers   map[string]*IPAuthTracker
	mu           sync.RWMutex
	logger       *zap.SugaredLogger
	auditor      SecurityAuditor
	stopCleanup  chan struct{}
	cleanupWG    sync.WaitGroup
}

// NewAuthRateLimiter creates a new authentication rate limiter
func NewAuthRateLimiter(config *AuthRateLimitConfig, logger *zap.SugaredLogger) *AuthRateLimiter {
	return NewAuthRateLimiterWithAuditor(config, logger, nil)
}

// NewAuthRateLimiterWithAuditor creates a new authentication rate limiter with audit logging
func NewAuthRateLimiterWithAuditor(config *AuthRateLimitConfig, logger *zap.SugaredLogger, auditor SecurityAuditor) *AuthRateLimiter {
	if config == nil {
		config = DefaultAuthRateLimitConfig()
	}

	arl := &AuthRateLimiter{
		config:       config,
		userTrackers: make(map[string]*UserAuthTracker),
		ipTrackers:   make(map[string]*IPAuthTracker),
		logger:       logger,
		auditor:      auditor,
		stopCleanup:  make(chan struct{}),
	}

	// Start cleanup routine
	arl.startCleanupRoutine()

	return arl
}

// IsUserLocked checks if a user account is currently locked
func (arl *AuthRateLimiter) IsUserLocked(username string) bool {
	arl.mu.RLock()
	defer arl.mu.RUnlock()

	tracker, exists := arl.userTrackers[username]
	if !exists {
		return false
	}

	now := time.Now()

	// Check if lockout period has expired
	if now.After(tracker.LockedUntil) {
		return false
	}

	return !tracker.LockedUntil.IsZero() && now.Before(tracker.LockedUntil)
}

// IsIPLocked checks if an IP address is currently locked
func (arl *AuthRateLimiter) IsIPLocked(ip string) bool {
	arl.mu.RLock()
	defer arl.mu.RUnlock()

	tracker, exists := arl.ipTrackers[ip]
	if !exists {
		return false
	}

	now := time.Now()

	// Check if lockout period has expired
	if now.After(tracker.LockedUntil) {
		return false
	}

	return !tracker.LockedUntil.IsZero() && now.Before(tracker.LockedUntil)
}

// GetProgressiveDelay calculates the progressive delay for the next attempt
func (arl *AuthRateLimiter) GetProgressiveDelay(username, ip string) time.Duration {
	if !arl.config.EnableProgressiveDelay {
		return 0
	}

	arl.mu.RLock()
	defer arl.mu.RUnlock()

	// Use the higher consecutive failure count between user and IP
	userFailures := 0
	ipFailures := 0

	if tracker, exists := arl.userTrackers[username]; exists {
		userFailures = tracker.ConsecutiveFailures
	}

	if tracker, exists := arl.ipTrackers[ip]; exists {
		ipFailures = tracker.FailedAttempts
	}

	consecutiveFailures := userFailures
	if ipFailures > consecutiveFailures {
		consecutiveFailures = ipFailures
	}

	// No delay for first attempt
	if consecutiveFailures == 0 {
		return 0
	}

	// Calculate progressive delay: base delay * 2^(failures-1)
	// 1st failure: 2s, 2nd: 4s, 3rd: 8s, 4th: 16s, 5th: 32s, 6th+: 60s
	delaySeconds := arl.config.BaseDelaySeconds
	for i := 1; i < consecutiveFailures; i++ {
		delaySeconds *= 2
		if delaySeconds > arl.config.MaxDelaySeconds {
			delaySeconds = arl.config.MaxDelaySeconds
			break
		}
	}

	return time.Duration(delaySeconds) * time.Second
}

// RecordFailedAttempt records a failed authentication attempt
func (arl *AuthRateLimiter) RecordFailedAttempt(username, ip string) {
	arl.mu.Lock()
	defer arl.mu.Unlock()

	now := time.Now()

	// Record user attempt
	arl.recordUserFailedAttempt(username, now)

	// Record IP attempt
	arl.recordIPFailedAttempt(ip, now)

	arl.logger.Warnw("Authentication failure recorded",
		"username", username,
		"ip", ip,
		"userAttempts", arl.userTrackers[username].FailedAttempts,
		"ipAttempts", arl.ipTrackers[ip].FailedAttempts)
}

// recordUserFailedAttempt records a failed attempt for a specific user
func (arl *AuthRateLimiter) recordUserFailedAttempt(username string, now time.Time) {
	tracker, exists := arl.userTrackers[username]
	if !exists {
		tracker = &UserAuthTracker{
			Username:      username,
			AttemptWindow: now,
		}
		arl.userTrackers[username] = tracker
	}

	// Reset attempt window if it's been more than the configured window
	if now.Sub(tracker.AttemptWindow) > arl.config.AttemptWindow {
		tracker.FailedAttempts = 0
		tracker.ConsecutiveFailures = 0
		tracker.AttemptWindow = now
	}

	tracker.FailedAttempts++
	tracker.ConsecutiveFailures++
	tracker.LastAttempt = now

	// Check if user should be locked out
	if tracker.FailedAttempts >= arl.config.MaxFailedAttempts {
		tracker.LockedUntil = now.Add(arl.config.UserLockoutDuration)
		arl.logger.Warnw("User account locked due to excessive failed attempts",
			"username", username,
			"attempts", tracker.FailedAttempts,
			"lockedUntil", tracker.LockedUntil)

		// Log to security audit
		if arl.auditor != nil {
			details := map[string]interface{}{
				"attempts":         tracker.FailedAttempts,
				"locked_until":     tracker.LockedUntil,
				"lockout_duration": arl.config.UserLockoutDuration.String(),
			}
			arl.auditor.LogRateLimitEvent("AUTH_LOCKOUT", username, "", 0, details)
		}
	}
}

// recordIPFailedAttempt records a failed attempt from a specific IP
func (arl *AuthRateLimiter) recordIPFailedAttempt(ip string, now time.Time) {
	tracker, exists := arl.ipTrackers[ip]
	if !exists {
		tracker = &IPAuthTracker{
			IP:            ip,
			AttemptWindow: now,
		}
		arl.ipTrackers[ip] = tracker
	}

	// Reset attempt window if it's been more than the configured window
	if now.Sub(tracker.AttemptWindow) > arl.config.AttemptWindow {
		tracker.FailedAttempts = 0
		tracker.AttemptWindow = now
	}

	tracker.FailedAttempts++
	tracker.LastAttempt = now

	// Check if IP should be locked out
	if tracker.FailedAttempts >= arl.config.MaxAttemptsPerIP {
		tracker.LockedUntil = now.Add(arl.config.IPLockoutDuration)
		arl.logger.Warnw("IP address locked due to excessive failed attempts",
			"ip", ip,
			"attempts", tracker.FailedAttempts,
			"lockedUntil", tracker.LockedUntil)

		// Log to security audit
		if arl.auditor != nil {
			details := map[string]interface{}{
				"attempts":         tracker.FailedAttempts,
				"locked_until":     tracker.LockedUntil,
				"lockout_duration": arl.config.IPLockoutDuration.String(),
			}
			arl.auditor.LogRateLimitEvent("IP_BLOCKED", "", ip, 0, details)
		}
	}
}

// RecordSuccessfulAttempt records a successful authentication and resets counters
func (arl *AuthRateLimiter) RecordSuccessfulAttempt(username, ip string) {
	arl.mu.Lock()
	defer arl.mu.Unlock()

	now := time.Now()

	// Reset user counters
	if tracker, exists := arl.userTrackers[username]; exists {
		tracker.FailedAttempts = 0
		tracker.ConsecutiveFailures = 0
		tracker.LockedUntil = time.Time{} // Clear lockout
		tracker.LastSuccessfulAuth = now
	}

	// Don't reset IP counters completely, but reduce them
	// This prevents rapid switching between users from the same IP
	if tracker, exists := arl.ipTrackers[ip]; exists {
		if tracker.FailedAttempts > 0 {
			tracker.FailedAttempts = tracker.FailedAttempts / 2 // Reduce by half
		}
	}

	arl.logger.Infow("Successful authentication recorded",
		"username", username,
		"ip", ip)
}

// GetUserLockoutInfo returns lockout information for a user
func (arl *AuthRateLimiter) GetUserLockoutInfo(username string) (bool, time.Time, int) {
	arl.mu.RLock()
	defer arl.mu.RUnlock()

	tracker, exists := arl.userTrackers[username]
	if !exists {
		return false, time.Time{}, 0
	}

	isLocked := !tracker.LockedUntil.IsZero() && time.Now().Before(tracker.LockedUntil)
	return isLocked, tracker.LockedUntil, tracker.FailedAttempts
}

// GetIPLockoutInfo returns lockout information for an IP
func (arl *AuthRateLimiter) GetIPLockoutInfo(ip string) (bool, time.Time, int) {
	arl.mu.RLock()
	defer arl.mu.RUnlock()

	tracker, exists := arl.ipTrackers[ip]
	if !exists {
		return false, time.Time{}, 0
	}

	isLocked := !tracker.LockedUntil.IsZero() && time.Now().Before(tracker.LockedUntil)
	return isLocked, tracker.LockedUntil, tracker.FailedAttempts
}

// GetStats returns statistics about authentication attempts
func (arl *AuthRateLimiter) GetStats() map[string]interface{} {
	arl.mu.RLock()
	defer arl.mu.RUnlock()

	lockedUsers := 0
	lockedIPs := 0
	now := time.Now()

	for _, tracker := range arl.userTrackers {
		if !tracker.LockedUntil.IsZero() && now.Before(tracker.LockedUntil) {
			lockedUsers++
		}
	}

	for _, tracker := range arl.ipTrackers {
		if !tracker.LockedUntil.IsZero() && now.Before(tracker.LockedUntil) {
			lockedIPs++
		}
	}

	return map[string]interface{}{
		"total_users_tracked":       len(arl.userTrackers),
		"total_ips_tracked":         len(arl.ipTrackers),
		"locked_users":              lockedUsers,
		"locked_ips":                lockedIPs,
		"max_failed_attempts":       arl.config.MaxFailedAttempts,
		"user_lockout_duration":     arl.config.UserLockoutDuration.String(),
		"progressive_delay_enabled": arl.config.EnableProgressiveDelay,
	}
}

// startCleanupRoutine starts the background cleanup routine
func (arl *AuthRateLimiter) startCleanupRoutine() {
	arl.cleanupWG.Add(1)
	go func() {
		defer arl.cleanupWG.Done()

		ticker := time.NewTicker(arl.config.CleanupInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				arl.cleanupExpiredRecords()
			case <-arl.stopCleanup:
				return
			}
		}
	}()
}

// cleanupExpiredRecords removes old tracking records
func (arl *AuthRateLimiter) cleanupExpiredRecords() {
	arl.mu.Lock()
	defer arl.mu.Unlock()

	now := time.Now()
	cleanupThreshold := arl.config.AttemptWindow * 2 // Keep records for 2x the attempt window

	// Cleanup user trackers
	for username, tracker := range arl.userTrackers {
		if now.Sub(tracker.LastAttempt) > cleanupThreshold &&
			(tracker.LockedUntil.IsZero() || now.After(tracker.LockedUntil)) {
			delete(arl.userTrackers, username)
		}
	}

	// Cleanup IP trackers
	for ip, tracker := range arl.ipTrackers {
		if now.Sub(tracker.LastAttempt) > cleanupThreshold &&
			(tracker.LockedUntil.IsZero() || now.After(tracker.LockedUntil)) {
			delete(arl.ipTrackers, ip)
		}
	}
}

// Stop stops the authentication rate limiter and cleanup routine
func (arl *AuthRateLimiter) Stop() {
	close(arl.stopCleanup)
	arl.cleanupWG.Wait()
}
