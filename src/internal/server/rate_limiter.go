package server

import (
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"syndrdb/src/pkg/errors"
	"syndrdb/src/pkg/fatal"
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

const numRateLimitShards = 32

// rateLimitShard holds a subset of IP trackers behind its own mutex.
type rateLimitShard struct {
	mu       sync.Mutex
	trackers map[string]*IPTracker
}

// RateLimiter manages rate limiting for the server
type RateLimiter struct {
	config            *RateLimitConfig
	shards            [numRateLimitShards]rateLimitShard
	globalConnections atomic.Int32
	whitelistSet      map[string]bool // immutable after construction — safe to read without lock
	stopCleanup       chan bool
}

// NewRateLimiter creates a new rate limiter
func NewRateLimiter(config *RateLimitConfig) *RateLimiter {
	if config == nil {
		config = DefaultRateLimitConfig()
	}

	// Build immutable whitelist set for O(1) lock-free lookups
	wset := make(map[string]bool, len(config.WhitelistedIPs))
	for _, ip := range config.WhitelistedIPs {
		wset[ip] = true
	}

	rl := &RateLimiter{
		config:       config,
		whitelistSet: wset,
		stopCleanup:  make(chan bool),
	}

	// Initialize shard maps
	for i := range rl.shards {
		rl.shards[i].trackers = make(map[string]*IPTracker)
	}

	// Start cleanup goroutine
	go rl.cleanupRoutine()

	return rl
}

// Stop stops the rate limiter and cleanup routine
func (rl *RateLimiter) Stop() {
	close(rl.stopCleanup)
}

// shardFor returns the shard index for an IP using FNV-1a-like hash.
func shardFor(ip string) uint32 {
	var h uint32 = 2166136261
	for i := 0; i < len(ip); i++ {
		h ^= uint32(ip[i])
		h *= 16777619
	}
	return h % numRateLimitShards
}

// CheckRequest checks if a request from the given IP should be allowed
func (rl *RateLimiter) CheckRequest(ip string) error {
	// Lock-free fast path: whitelisted IPs bypass rate limiting entirely.
	// whitelistSet is immutable after construction — no synchronization needed.
	if rl.whitelistSet[ip] {
		return nil
	}

	shard := &rl.shards[shardFor(ip)]
	shard.mu.Lock()
	defer shard.mu.Unlock()

	tracker := getOrCreateTrackerInShard(shard, ip)
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
	// Check global connection limit (atomic — no lock needed)
	if int(rl.globalConnections.Load()) >= rl.config.MaxGlobalConnections {
		return errors.New(errors.ERR_RESOURCE_EXHAUSTED,
			fmt.Sprintf("server has reached maximum global connection limit (%d)", rl.config.MaxGlobalConnections),
			errors.LayerAPI).WithContext("max_global_connections", fmt.Sprintf("%d", rl.config.MaxGlobalConnections))
	}

	// Lock-free fast path for whitelisted IPs
	if rl.whitelistSet[ip] {
		rl.globalConnections.Add(1)
		return nil
	}

	shard := &rl.shards[shardFor(ip)]
	shard.mu.Lock()
	defer shard.mu.Unlock()

	tracker := getOrCreateTrackerInShard(shard, ip)
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
	rl.globalConnections.Add(1)

	return nil
}

// ReleaseConnection releases a connection for the given IP
func (rl *RateLimiter) ReleaseConnection(ip string) {
	// Whitelisted IPs only tracked via global counter
	if rl.whitelistSet[ip] {
		rl.globalConnections.Add(-1)
		return
	}

	shard := &rl.shards[shardFor(ip)]
	shard.mu.Lock()
	if tracker, exists := shard.trackers[ip]; exists {
		if tracker.ConnectionCount > 0 {
			tracker.ConnectionCount--
		}
	}
	shard.mu.Unlock()

	rl.globalConnections.Add(-1)
}

// GetStats returns current rate limiting statistics
func (rl *RateLimiter) GetStats() map[string]interface{} {
	ipStats := make(map[string]interface{})
	for i := range rl.shards {
		shard := &rl.shards[i]
		shard.mu.Lock()
		for ip, tracker := range shard.trackers {
			ipStats[ip] = map[string]interface{}{
				"request_count":    tracker.RequestCount,
				"connection_count": tracker.ConnectionCount,
				"banned_until":     tracker.BannedUntil,
				"last_activity":    tracker.LastActivity,
			}
		}
		shard.mu.Unlock()
	}

	return map[string]interface{}{
		"global_connections": int(rl.globalConnections.Load()),
		"tracked_ips":        len(ipStats),
		"config":             rl.config,
		"ip_stats":           ipStats,
	}
}

// BanIP manually bans an IP for the configured duration
func (rl *RateLimiter) BanIP(ip string, duration time.Duration) {
	shard := &rl.shards[shardFor(ip)]
	shard.mu.Lock()
	defer shard.mu.Unlock()

	tracker := getOrCreateTrackerInShard(shard, ip)
	tracker.BannedUntil = time.Now().Add(duration)
}

// UnbanIP manually unbans an IP
func (rl *RateLimiter) UnbanIP(ip string) {
	shard := &rl.shards[shardFor(ip)]
	shard.mu.Lock()
	defer shard.mu.Unlock()

	if tracker, exists := shard.trackers[ip]; exists {
		tracker.BannedUntil = time.Time{}
	}
}

// SetMaxGlobalConnections updates the maximum global connection limit
func (rl *RateLimiter) SetMaxGlobalConnections(max int) {
	rl.config.MaxGlobalConnections = max
}

// getOrCreateTrackerInShard gets or creates a tracker within a shard (caller must hold shard.mu).
func getOrCreateTrackerInShard(shard *rateLimitShard, ip string) *IPTracker {
	tracker, exists := shard.trackers[ip]
	if !exists {
		tracker = &IPTracker{
			IP:            ip,
			RequestWindow: time.Now(),
			LastActivity:  time.Now(),
		}
		shard.trackers[ip] = tracker
	}
	return tracker
}

// cleanupRoutine periodically removes old inactive entries
func (rl *RateLimiter) cleanupRoutine() {
	defer func() {
		if r := recover(); r != nil {
			fatal.LogFatalAndExit(r)
		}
	}()
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
	now := time.Now()
	cutoff := now.Add(-time.Hour)

	for i := range rl.shards {
		shard := &rl.shards[i]
		shard.mu.Lock()
		for ip, tracker := range shard.trackers {
			if tracker.LastActivity.Before(cutoff) &&
				now.After(tracker.BannedUntil) &&
				tracker.ConnectionCount == 0 {
				delete(shard.trackers, ip)
			}
		}
		shard.mu.Unlock()
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
