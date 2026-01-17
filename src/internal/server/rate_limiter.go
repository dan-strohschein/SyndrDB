package server

import (
	"fmt"
	"net"
	"syndrdb/src/pkg/errors"
	"sync"
	"time"
)

// RateLimitConfig holds rate limiting configuration
type RateLimitConfig struct {
	MaxRequestsPerMinute int           // Maximum requests per minute per IP
	MaxConnectionsPerIP  int           // Maximum concurrent connections per IP
	BanDuration          time.Duration // How long to ban after exceeding limits
	CleanupInterval      time.Duration // How often to clean up old entries
	WhitelistedIPs       []string      // IPs that bypass rate limiting
	MaxGlobalConnections int           // Maximum total concurrent connections
}

// DefaultRateLimitConfig returns default rate limiting configuration
func DefaultRateLimitConfig() *RateLimitConfig {
	return &RateLimitConfig{
		MaxRequestsPerMinute: 1000,
		MaxConnectionsPerIP:  10,
		BanDuration:          time.Minute * 15,
		CleanupInterval:      time.Minute * 5,
		WhitelistedIPs:       []string{"127.0.0.1", "::1"},
		MaxGlobalConnections: 1000,
	}
}

// IPTracker tracks requests and connections per IP
type IPTracker struct {
	IP              string
	RequestCount    int
	RequestWindow   time.Time
	ConnectionCount int
	BannedUntil     time.Time
	LastActivity    time.Time
}

// RateLimiter manages rate limiting for the server
type RateLimiter struct {
	config            *RateLimitConfig
	ipTrackers        map[string]*IPTracker
	globalConnections int
	mutex             sync.RWMutex
	stopCleanup       chan bool
}

// NewRateLimiter creates a new rate limiter
func NewRateLimiter(config *RateLimitConfig) *RateLimiter {
	if config == nil {
		config = DefaultRateLimitConfig()
	}

	rl := &RateLimiter{
		config:      config,
		ipTrackers:  make(map[string]*IPTracker),
		stopCleanup: make(chan bool),
	}

	// Start cleanup goroutine
	go rl.cleanupRoutine()

	return rl
}

// Stop stops the rate limiter and cleanup routine
func (rl *RateLimiter) Stop() {
	close(rl.stopCleanup)
}

// CheckRequest checks if a request from the given IP should be allowed
func (rl *RateLimiter) CheckRequest(ip string) error {
	rl.mutex.Lock()
	defer rl.mutex.Unlock()

	// Check if IP is whitelisted
	if rl.isWhitelisted(ip) {
		return nil
	}

	tracker := rl.getOrCreateTracker(ip)
	now := time.Now()

	// Check if IP is currently banned
	if now.Before(tracker.BannedUntil) {
		return errors.New(errors.ERR_AUTH_RATE_LIMIT,
			fmt.Sprintf("IP %s is temporarily banned until %v", ip, tracker.BannedUntil),
			errors.LayerAuth).WithContext("ip", ip).WithContext("banned_until", tracker.BannedUntil.String())
	}

	// Reset request window if it's been more than a minute
	if now.Sub(tracker.RequestWindow) > time.Minute {
		tracker.RequestCount = 0
		tracker.RequestWindow = now
	}

	// Check request rate limit
	tracker.RequestCount++
	tracker.LastActivity = now

	if tracker.RequestCount > rl.config.MaxRequestsPerMinute {
		tracker.BannedUntil = now.Add(rl.config.BanDuration)
		return errors.New(errors.ERR_AUTH_RATE_LIMIT,
			fmt.Sprintf("IP %s exceeded rate limit (%d requests/min), banned for %v", ip, rl.config.MaxRequestsPerMinute, rl.config.BanDuration),
			errors.LayerAuth).WithContext("ip", ip).WithContext("max_requests_per_minute", fmt.Sprintf("%d", rl.config.MaxRequestsPerMinute))
	}

	return nil
}

// CheckConnection checks if a new connection from the given IP should be allowed
func (rl *RateLimiter) CheckConnection(ip string) error {
	rl.mutex.Lock()
	defer rl.mutex.Unlock()

	// Check global connection limit
	if rl.globalConnections >= rl.config.MaxGlobalConnections {
		return errors.New(errors.ERR_RESOURCE_EXHAUSTED,
			fmt.Sprintf("server has reached maximum global connection limit (%d)", rl.config.MaxGlobalConnections),
			errors.LayerAPI).WithContext("max_global_connections", fmt.Sprintf("%d", rl.config.MaxGlobalConnections))
	}

	// Check if IP is whitelisted
	if rl.isWhitelisted(ip) {
		rl.globalConnections++
		return nil
	}

	tracker := rl.getOrCreateTracker(ip)
	now := time.Now()

	// Check if IP is currently banned
	if now.Before(tracker.BannedUntil) {
		return errors.New(errors.ERR_AUTH_RATE_LIMIT,
			fmt.Sprintf("IP %s is temporarily banned until %v", ip, tracker.BannedUntil),
			errors.LayerAuth).WithContext("ip", ip).WithContext("banned_until", tracker.BannedUntil.String())
	}

	// Check per-IP connection limit
	if tracker.ConnectionCount >= rl.config.MaxConnectionsPerIP {
		return errors.New(errors.ERR_RESOURCE_EXHAUSTED,
			fmt.Sprintf("IP %s has reached maximum connection limit (%d)", ip, rl.config.MaxConnectionsPerIP),
			errors.LayerAPI).WithContext("ip", ip).WithContext("max_connections_per_ip", fmt.Sprintf("%d", rl.config.MaxConnectionsPerIP))
	}

	tracker.ConnectionCount++
	tracker.LastActivity = now
	rl.globalConnections++

	return nil
}

// ReleaseConnection releases a connection for the given IP
func (rl *RateLimiter) ReleaseConnection(ip string) {
	rl.mutex.Lock()
	defer rl.mutex.Unlock()

	if tracker, exists := rl.ipTrackers[ip]; exists {
		if tracker.ConnectionCount > 0 {
			tracker.ConnectionCount--
		}
	}

	if rl.globalConnections > 0 {
		rl.globalConnections--
	}
}

// GetStats returns current rate limiting statistics
func (rl *RateLimiter) GetStats() map[string]interface{} {
	rl.mutex.RLock()
	defer rl.mutex.RUnlock()

	stats := map[string]interface{}{
		"global_connections": rl.globalConnections,
		"tracked_ips":        len(rl.ipTrackers),
		"config":             rl.config,
	}

	// Add per-IP stats
	ipStats := make(map[string]interface{})
	for ip, tracker := range rl.ipTrackers {
		ipStats[ip] = map[string]interface{}{
			"request_count":    tracker.RequestCount,
			"connection_count": tracker.ConnectionCount,
			"banned_until":     tracker.BannedUntil,
			"last_activity":    tracker.LastActivity,
		}
	}
	stats["ip_stats"] = ipStats

	return stats
}

// BanIP manually bans an IP for the configured duration
func (rl *RateLimiter) BanIP(ip string, duration time.Duration) {
	rl.mutex.Lock()
	defer rl.mutex.Unlock()

	tracker := rl.getOrCreateTracker(ip)
	tracker.BannedUntil = time.Now().Add(duration)
}

// UnbanIP manually unbans an IP
func (rl *RateLimiter) UnbanIP(ip string) {
	rl.mutex.Lock()
	defer rl.mutex.Unlock()

	if tracker, exists := rl.ipTrackers[ip]; exists {
		tracker.BannedUntil = time.Time{}
	}
}

// SetMaxGlobalConnections updates the maximum global connection limit
// TODO: I could add validation to ensure new limit >= current active connections
func (rl *RateLimiter) SetMaxGlobalConnections(max int) {
	rl.mutex.Lock()
	defer rl.mutex.Unlock()
	rl.config.MaxGlobalConnections = max
}

// isWhitelisted checks if an IP is in the whitelist
func (rl *RateLimiter) isWhitelisted(ip string) bool {
	for _, whitelistedIP := range rl.config.WhitelistedIPs {
		if ip == whitelistedIP {
			return true
		}
	}
	return false
}

// getOrCreateTracker gets an existing tracker or creates a new one
func (rl *RateLimiter) getOrCreateTracker(ip string) *IPTracker {
	tracker, exists := rl.ipTrackers[ip]
	if !exists {
		tracker = &IPTracker{
			IP:            ip,
			RequestWindow: time.Now(),
			LastActivity:  time.Now(),
		}
		rl.ipTrackers[ip] = tracker
	}
	return tracker
}

// cleanupRoutine periodically removes old inactive entries
func (rl *RateLimiter) cleanupRoutine() {
	ticker := time.NewTicker(rl.config.CleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			rl.cleanup()
		case <-rl.stopCleanup:
			return
		}
	}
}

// cleanup removes old inactive IP trackers
func (rl *RateLimiter) cleanup() {
	rl.mutex.Lock()
	defer rl.mutex.Unlock()

	now := time.Now()
	cutoff := now.Add(-time.Hour) // Remove entries older than 1 hour

	for ip, tracker := range rl.ipTrackers {
		// Remove if inactive for more than 1 hour and not banned and no active connections
		if tracker.LastActivity.Before(cutoff) &&
			now.After(tracker.BannedUntil) &&
			tracker.ConnectionCount == 0 {
			delete(rl.ipTrackers, ip)
		}
	}
}

// ExtractIPFromConn extracts IP address from a network connection
func ExtractIPFromConn(conn net.Conn) string {
	if conn == nil {
		return "unknown"
	}

	addr := conn.RemoteAddr()
	if addr == nil {
		return "unknown"
	}

	host, _, err := net.SplitHostPort(addr.String())
	if err != nil {
		return addr.String()
	}

	return host
}

// ExtractConnectionFingerprint creates a fingerprint for the connection
// Since we don't have HTTP headers, we'll use connection properties
func ExtractConnectionFingerprint(conn net.Conn) string {
	if conn == nil {
		return "unknown_connection"
	}

	// Create fingerprint based on connection properties
	remoteAddr := "unknown"
	localAddr := "unknown"

	if conn.RemoteAddr() != nil {
		remoteAddr = conn.RemoteAddr().String()
		networkType := conn.RemoteAddr().Network()

		// Create a basic fingerprint - in production, you might want to include
		// more sophisticated connection properties or protocol-specific information
		fingerprint := fmt.Sprintf("net:%s|remote:%s|local:%s",
			networkType, remoteAddr, localAddr)

		return fingerprint
	}

	if conn.LocalAddr() != nil {
		localAddr = conn.LocalAddr().String()
	}

	// Fallback fingerprint
	return fmt.Sprintf("net:unknown|remote:%s|local:%s", remoteAddr, localAddr)
}
