package server

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"sync"
	"syndrdb/src/internal/domain/models"
	"syndrdb/src/internal/journal"
	"syndrdb/src/internal/storage/buffer"
	"syndrdb/src/internal/syndrQL"
	"syndrdb/src/pkg/common/helpers"
	"syndrdb/src/pkg/constants"
	"syndrdb/src/pkg/errors"
	"syndrdb/src/pkg/fatal"
	"syndrdb/src/pkg/settings"
	"time"

	"go.uber.org/zap"
)

// #region agent log
const debugLogPathSession = "/Users/danstrohschein/Documents/CodeProjects/golang/SyndrDB/.cursor/debug.log"

func debugLogSessionCleanup(sessionID string, txStatus TransactionStatus, txID string, docLockCount, bundleLockCount int) {
	txIDShort := ""
	if len(txID) > 12 {
		txIDShort = txID[:12]
	}
	sessIDShort := ""
	if len(sessionID) > 12 {
		sessIDShort = sessionID[:12]
	}
	entry := map[string]interface{}{"timestamp": time.Now().UnixMilli(), "hypothesisId": "A", "location": "session_manager.go:cleanupSession", "message": "session_cleanup_start", "data": map[string]interface{}{"sessionID": sessIDShort, "txStatus": txStatus.String(), "txID": txIDShort, "docLocks": docLockCount, "bundleLocks": bundleLockCount}}
	if b, err := json.Marshal(entry); err == nil {
		if f, err := os.OpenFile(debugLogPathSession, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644); err == nil {
			f.Write(append(b, '\n'))
			f.Close()
		}
	}
}

func debugLogSessionLockRelease(sessionID string, method string, lockCount int) {
	sessIDShort := ""
	if len(sessionID) > 12 {
		sessIDShort = sessionID[:12]
	}
	entry := map[string]interface{}{"timestamp": time.Now().UnixMilli(), "hypothesisId": "A", "location": "session_manager.go:cleanupSession", "message": "session_lock_release", "data": map[string]interface{}{"sessionID": sessIDShort, "method": method, "lockCount": lockCount}}
	if b, err := json.Marshal(entry); err == nil {
		if f, err := os.OpenFile(debugLogPathSession, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644); err == nil {
			f.Write(append(b, '\n'))
			f.Close()
		}
	}
}

// #endregion

// TransactionStatus represents the status of a multi-statement transaction
type TransactionStatus int

const (
	TransactionStatusInactive  TransactionStatus = iota // No active transaction
	TransactionStatusActive                             // Transaction active, operations being buffered
	TransactionStatusCommitted                          // Transaction successfully committed
	TransactionStatusAborted                            // Transaction aborted/rolled back
)

func (ts TransactionStatus) String() string {
	switch ts {
	case TransactionStatusInactive:
		return "INACTIVE"
	case TransactionStatusActive:
		return "ACTIVE"
	case TransactionStatusCommitted:
		return "COMMITTED"
	case TransactionStatusAborted:
		return "ABORTED"
	default:
		return "UNKNOWN"
	}
}

// Savepoint represents a savepoint within a transaction
type Savepoint struct {
	Name string // Alphanumeric name of the savepoint
	LSN  uint64 // WAL LSN at savepoint creation
}

// SessionState represents the current state of a session
type SessionState int

const (
	SessionStateActive SessionState = iota
	SessionStateIdle
	SessionStateExecuting
	SessionStateError
	SessionStateExpired
	SessionStateTerminated
)

func (s SessionState) String() string {
	switch s {
	case SessionStateActive:
		return "ACTIVE"
	case SessionStateIdle:
		return "IDLE"
	case SessionStateExecuting:
		return "EXECUTING"
	case SessionStateError:
		return "ERROR"
	case SessionStateExpired:
		return "EXPIRED"
	case SessionStateTerminated:
		return "TERMINATED"
	default:
		return "UNKNOWN"
	}
}

// QueryInfo represents information about a query
type QueryInfo struct {
	QueryID      string
	Query        string
	StartTime    time.Time
	EndTime      *time.Time
	Status       string // "EXECUTING", "COMPLETED", "FAILED"
	AffectedRows int
	Error        error
}

// LockInfo represents a lock held by the session
type LockInfo struct {
	LockID       string
	LockType     string // "DOCUMENT", "BUNDLE", "TABLE"
	ResourceName string
	LockMode     string // "READ", "WRITE", "EXCLUSIVE"
	AcquiredAt   time.Time
}

// Session represents a user session with comprehensive state tracking
type Session struct {
	SessionID          string
	UserID             string
	Username           string
	DatabaseName       string
	Database           *models.Database
	DatabaseFolderPath string // STEP 4: Cached database folder path (avoid repeated filepath.Join)
	ConnectionID       string // Associated connection ID

	// Security binding to prevent session hijacking
	ClientIP         string // IP address bound to this session
	UserAgent        string // User agent fingerprint for additional validation
	IPValidationHash string // Hash of IP+session for integrity checking

	// State tracking
	State        SessionState
	CreatedAt    time.Time
	LastActivity time.Time
	ExpiresAt    time.Time

	// Query tracking
	LastSuccessfulQuery *QueryInfo
	CurrentQuery        *QueryInfo
	QueryHistory        []*QueryInfo

	// Lock management
	DocumentLocks map[string]*LockInfo // documentID -> LockInfo
	BundleLocks   map[string]*LockInfo // bundleName -> LockInfo

	// Error tracking
	LastError         error
	ErrorCount        int
	ConsecutiveErrors int

	// Resource management
	BufferPool         *buffer.BufferPool
	TempFiles          []string
	ActiveTransactions map[string]context.CancelFunc // txID -> cancel function

	// Transaction state (for multi-statement transactions)
	TransactionActive    bool              // Whether a transaction is currently active
	ActiveTransactionID  string            // WAL transaction ID for the active transaction
	TransactionStartLSN  uint64            // LSN when transaction started (for rollback)
	TransactionStartTime time.Time         // When the transaction started (for idle timeout)
	PendingOperations    []string          // Buffered commands within the transaction
	CurrentSavepoint     *Savepoint        // Single-level savepoint (nil if no savepoint set)
	TransactionStatus    TransactionStatus // Current status of the transaction
	// PHASE 2: MVCC - TransactionBuffer tracks document locations for commit sequence assignment
	TransactionBuffer *TransactionBuffer // Tracks document locations written in this transaction

	// PHASE 1: MVCC - Snapshot created on BEGIN TRANSACTION, stored for read-path visibility
	MVCCSnapshot *journal.Snapshot

	// Prepared statement cache (session-scoped)
	PreparedStatements *syndrQL.ShardedPreparedStatementCache // Session-isolated prepared statement cache

	// Session configuration
	Timeout         time.Duration
	MaxQueryHistory int

	// Role caching for GraphQL security (5-minute TTL)
	CachedRole        string             // Cached user role string
	IsAdmin           bool               // Cached admin status
	RoleCacheTime     time.Time          // When the role was cached
	RoleCacheTTL      time.Duration      // TTL for role cache (default: 5 minutes)
	PermissionService *PermissionService // Reference to permission service for role lookup

	// Synchronization
	mu     sync.RWMutex
	Logger *zap.SugaredLogger
}

// SessionManager manages all active sessions
type SessionManager struct {
	sessions           map[string]*Session   // sessionID -> Session
	userSessions       map[string][]*Session // username -> list of sessions (already exists!)
	sessionsByUser     map[string][]*Session // username -> list of sessions (alias for consistency)
	connectionSessions map[string]*Session   // connectionID -> Session
	mu                 sync.RWMutex
	logger             *zap.SugaredLogger

	// Configuration
	defaultTimeout  time.Duration
	maxSessions     int
	cleanupInterval time.Duration

	// Cleanup
	stopCleanup chan struct{}
	cleanupWG   sync.WaitGroup

	// Temp file cleanup queue (async, non-blocking)
	tempFileCleanupQueue  chan []string
	tempFileCleanupWG     sync.WaitGroup
	stopTempFileCleanup   chan struct{}
}

// NewSessionManager creates a new session manager
func NewSessionManager(logger *zap.SugaredLogger, defaultTimeout time.Duration, maxSessions int) *SessionManager {
	sm := &SessionManager{
		sessions:            make(map[string]*Session),
		userSessions:        make(map[string][]*Session),
		sessionsByUser:      make(map[string][]*Session),
		connectionSessions:  make(map[string]*Session),
		logger:              logger,
		defaultTimeout:      defaultTimeout,
		maxSessions:         maxSessions,
		cleanupInterval:     time.Minute * 5, // Cleanup every 5 minutes
		stopCleanup:         make(chan struct{}),
		tempFileCleanupQueue: make(chan []string, 100), // Buffered channel for async cleanup
		stopTempFileCleanup:  make(chan struct{}),
	} // Start cleanup routine
	sm.startCleanupRoutine()
	sm.startTempFileCleanupWorker()

	return sm
}

// generateSecureSessionID generates a cryptographically secure session ID
func generateSecureSessionID() string {
	// Generate 32 random bytes (256 bits of entropy)
	bytes := make([]byte, 32)
	if _, err := rand.Read(bytes); err != nil {
		// Fallback to time-based ID if crypto/rand fails
		return fmt.Sprintf("sess_fallback_%d", time.Now().UnixNano())
	}

	// Encode as hexadecimal string
	return fmt.Sprintf("sess_%s", hex.EncodeToString(bytes))
}

// generateIPValidationHash creates a hash for IP address validation
func generateIPValidationHash(sessionID, clientIP, userAgent string) string {
	hasher := sha256.New()
	hasher.Write([]byte(sessionID))
	hasher.Write([]byte(clientIP))
	hasher.Write([]byte(userAgent))
	hasher.Write([]byte(time.Now().Format("2006-01-02"))) // Add date salt for daily rotation
	return hex.EncodeToString(hasher.Sum(nil))
}

// validateSessionBinding validates IP and user agent binding for session security
func validateSessionBinding(session *Session, clientIP, userAgent string) error {
	if session == nil {
		return errors.New(errors.ERR_VALIDATION_FIELD,
			"session is nil", errors.LayerAPI)
	}

	// Check IP address binding
	if session.ClientIP != clientIP {
		return errors.New(errors.ERR_AUTH_SESSION_EXPIRED,
			fmt.Sprintf("session IP mismatch: expected %s, got %s", session.ClientIP, clientIP),
			errors.LayerAuth).WithContext("expected_ip", session.ClientIP).WithContext("actual_ip", clientIP)
	}

	// Check user agent binding (allow some variance for browser updates)
	if session.UserAgent != userAgent {
		// Calculate similarity score for user agent (basic check for major differences)
		if !isUserAgentSimilar(session.UserAgent, userAgent) {
			return errors.New(errors.ERR_AUTH_SESSION_EXPIRED,
				"session user agent mismatch: significant difference detected",
				errors.LayerAuth)
		}
	}

	// Validate IP hash for integrity
	expectedHash := generateIPValidationHash(session.SessionID, clientIP, userAgent)
	if session.IPValidationHash != expectedHash {
		// Re-generate hash with current date in case it's a date change
		currentHash := generateIPValidationHash(session.SessionID, clientIP, session.UserAgent)
		if session.IPValidationHash != currentHash {
			return errors.New(errors.ERR_AUTH_SESSION_EXPIRED,
				"session validation hash mismatch: potential tampering detected",
				errors.LayerAuth)
		}
	}

	return nil
}

// isUserAgentSimilar checks if user agents are similar enough (basic similarity check)
func isUserAgentSimilar(original, current string) bool {
	if original == current {
		return true
	}

	// Basic similarity check - allow minor version differences
	// Extract major components and compare
	originalLen := len(original)
	currentLen := len(current)

	// If length difference is too large, consider them different
	if originalLen > 0 && currentLen > 0 {
		lengthDiff := originalLen - currentLen
		if lengthDiff < 0 {
			lengthDiff = -lengthDiff
		}

		// Allow up to 30% length difference for minor version updates
		maxDiff := originalLen * 30 / 100
		if lengthDiff <= maxDiff {
			return true
		}
	}

	return false
}

// CreateSession creates a new session for a user with IP binding and user-agent fingerprinting
func (sm *SessionManager) CreateSession(username, userID, databaseName string, database *models.Database, connectionID string, timeout time.Duration, clientIP, userAgent string) (*Session, error) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// Check if we've hit the max sessions limit
	if len(sm.sessions) >= sm.maxSessions {
		return nil, errors.New(errors.ERR_RESOURCE_EXHAUSTED,
			fmt.Sprintf("maximum number of sessions (%d) reached", sm.maxSessions),
			errors.LayerAPI).WithContext("max_sessions", fmt.Sprintf("%d", sm.maxSessions))
	}

	// Generate secure session ID
	sessionID := generateSecureSessionID()

	if timeout <= 0 {
		timeout = sm.defaultTimeout
	}

	// Generate IP validation hash for integrity checking
	ipValidationHash := generateIPValidationHash(sessionID, clientIP, userAgent)

	// Initialize prepared statement cache if enabled
	var preparedStmtCache *syndrQL.ShardedPreparedStatementCache
	settingsArgs := settings.GetSettings()
	if settingsArgs.PreparedStatementCacheEnabled {
		preparedStmtCache = syndrQL.NewShardedPreparedStatementCache(
			settingsArgs.PreparedStatementCacheCapacity,
			sm.logger.With("sessionID", sessionID, "component", "prepared_stmt_cache"),
		)
	}

	session := &Session{
		SessionID:          sessionID,
		UserID:             userID,
		Username:           username,
		DatabaseName:       databaseName,
		Database:           database,
		DatabaseFolderPath: helpers.GetDatabaseFolderPath(databaseName), // STEP 4: Cache on creation
		ConnectionID:       connectionID,
		ClientIP:           clientIP,
		UserAgent:          userAgent,
		IPValidationHash:   ipValidationHash,
		State:              SessionStateActive,
		CreatedAt:          time.Now(),
		LastActivity:       time.Now(),
		ExpiresAt:          time.Now().Add(timeout),
		QueryHistory:       make([]*QueryInfo, 0, 100),
		DocumentLocks:      make(map[string]*LockInfo),
		BundleLocks:        make(map[string]*LockInfo),
		ActiveTransactions: make(map[string]context.CancelFunc),
		TempFiles:          make([]string, 0, 10),
		Timeout:            timeout,
		MaxQueryHistory:    100, // Keep last 100 queries
		Logger:             sm.logger.With("sessionID", sessionID, "username", username, "clientIP", clientIP),

		// Initialize prepared statement cache
		PreparedStatements: preparedStmtCache,

		// Initialize role caching fields
		RoleCacheTTL:      5 * time.Minute, // 5-minute TTL for role cache
		CachedRole:        "",              // Will be populated on first GetRole() call
		IsAdmin:           false,           // Will be set by GetRole()
		RoleCacheTime:     time.Time{},     // Zero time means cache is empty
		PermissionService: nil,             // Will be set by server initialization
	}

	// Store session
	sm.sessions[sessionID] = session
	sm.connectionSessions[connectionID] = session

	// METRICS: Track session creation
	metrics := GetGlobalServerMetrics()
	metrics.SessionsCreated.Add(1)
	metrics.SessionsActive.Add(1)

	// Add to user sessions
	if sm.userSessions[username] == nil {
		sm.userSessions[username] = make([]*Session, 0, 5)
	}
	sm.userSessions[username] = append(sm.userSessions[username], session)

	session.Logger.Infow("Session created with IP binding",
		"sessionID", sessionID,
		"username", username,
		"database", databaseName,
		"clientIP", clientIP,
		"userAgent", func() string {
			if len(userAgent) > constants.UserAgentMaxLength {
				return userAgent[:constants.UserAgentMaxLength]
			}
			return userAgent
		}(), // Truncate user agent for logging
		"timeout", timeout,
		"expiresAt", session.ExpiresAt)

	return session, nil
}

// GetSession retrieves a session by ID
func (sm *SessionManager) GetSession(sessionID string) (*Session, bool) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	session, exists := sm.sessions[sessionID]
	return session, exists
}

// GetSessionByConnection retrieves a session by connection ID
func (sm *SessionManager) GetSessionByConnection(connectionID string) (*Session, bool) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	session, exists := sm.connectionSessions[connectionID]
	return session, exists
}

// GetUserSessions retrieves all sessions for a user
func (sm *SessionManager) GetUserSessions(username string) []*Session {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	sessions := sm.userSessions[username]
	if sessions == nil {
		return []*Session{}
	}

	// Return a copy to avoid race conditions
	result := make([]*Session, len(sessions))
	copy(result, sessions)
	return result
}

// GetActiveSessionsByTxID returns a map of active transaction IDs to session IDs
// This is used by the lock manager's CleanupOrphanedLocks to determine which
// transactions are still active and should not have their locks released
func (sm *SessionManager) GetActiveSessionsByTxID() map[string]string {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	activeTxMap := make(map[string]string)

	// Iterate through all sessions and collect active transactions
	for _, session := range sm.sessions {
		if session.IsInTransaction() {
			activeTxMap[session.ActiveTransactionID] = session.SessionID
		}
	}

	return activeTxMap
}

// InvalidateSession invalidates a specific session
func (sm *SessionManager) InvalidateSession(sessionID string) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	session, exists := sm.sessions[sessionID]
	if !exists {
		return errors.New(errors.ERR_AUTH_SESSION_EXPIRED,
			fmt.Sprintf("session %s not found", sessionID),
			errors.LayerAuth).WithContext("session_id", sessionID)
	}

	// METRICS: Track session termination
	metrics := GetGlobalServerMetrics()
	metrics.SessionsTerminated.Add(1)
	metrics.SessionsActive.Add(^uint64(0)) // Atomic decrement

	return sm.cleanupSession(session)
}

// InvalidateUserSessions invalidates all sessions for a user
func (sm *SessionManager) InvalidateUserSessions(username string) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	sessions := sm.userSessions[username]
	if sessions == nil {
		return nil
	}

	var lastError error
	for _, session := range sessions {
		if err := sm.cleanupSession(session); err != nil {
			lastError = err
		}
	}

	return lastError
}

// TerminateUserSessions forcefully terminates all active sessions for a user
// Sends a graceful disconnect message to each client before closing the connection
// This method is used when FORCE option is specified in RBAC commands
// Parameters:
//   - username: The username whose sessions should be terminated
//   - connectionMap: Map of connectionID to Connection objects (from Server.ActiveConnections)
//
// Returns:
//   - count: Number of sessions terminated
//   - error: Any error that occurred during termination
//
// TODO: I can add session termination event broadcast for monitoring systems
// TODO: I can add configurable termination grace period before forced disconnect
func (sm *SessionManager) TerminateUserSessions(username string, connectionMap map[string]*Connection) (int, error) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	sessions := sm.userSessions[username]
	if sessions == nil {
		return 0, nil
	}

	terminatedCount := 0
	var lastError error

	for _, session := range sessions {
		// Get the connection for this session
		if conn, exists := connectionMap[session.ConnectionID]; exists && conn != nil {
			// Send graceful disconnect message to client before closing
			if conn.Writer != nil {
				sm.logger.Infow("Sending termination message to client",
					"username", username,
					"sessionID", session.SessionID,
					"connectionID", session.ConnectionID)

				// Send error message using the same format as sendError
				response := map[string]interface{}{
					"status":  "error",
					"message": "The administrator forcefully removed your session from the server",
				}
				if jsonResponse, err := json.Marshal(response); err == nil {
					conn.Writer.WriteString(string(jsonResponse) + "\n")
					conn.Writer.Flush()
				}
			}

			// Close the connection
			if conn.Conn != nil {
				conn.Conn.Close()
			}
		}

		// Cleanup the session
		if err := sm.cleanupSession(session); err != nil {
			sm.logger.Warnw("Error cleaning up terminated session",
				"username", username,
				"sessionID", session.SessionID,
				"error", err)
			lastError = err
		} else {
			terminatedCount++
		}
	}

	sm.logger.Infow("Terminated user sessions",
		"username", username,
		"count", terminatedCount)

	return terminatedCount, lastError
}

// UpdateActivity updates the last activity time for a session with security validation
func (sm *SessionManager) UpdateActivity(sessionID, clientIP, userAgent string) error {
	sm.mu.RLock()
	session, exists := sm.sessions[sessionID]
	sm.mu.RUnlock()

	if !exists {
		return errors.New(errors.ERR_AUTH_SESSION_EXPIRED,
			fmt.Sprintf("session %s not found", sessionID),
			errors.LayerAuth).WithContext("session_id", sessionID)
	}

	// Validate session binding before updating activity
	if err := validateSessionBinding(session, clientIP, userAgent); err != nil {
		session.Logger.Warnw("Session security validation failed during activity update",
			"error", err,
			"clientIP", clientIP,
			"expectedIP", session.ClientIP,
			"userAgent", userAgent[:func() int {
				if len(userAgent) > constants.UserAgentTruncateLength {
					return constants.UserAgentTruncateLength
				}
				return len(userAgent)
			}()])
		return errors.WrapWithMessage(err, errors.ERR_VALIDATION_FIELD,
			"session security validation failed", errors.LayerAuth)
	}

	session.mu.Lock()
	defer session.mu.Unlock()

	session.LastActivity = time.Now()
	session.ExpiresAt = time.Now().Add(session.Timeout)

	return nil
}

// SetDatabaseContext changes the database context for an existing session
func (sm *SessionManager) SetDatabaseContext(sessionID string, databaseName string, database *models.Database) error {
	sm.mu.RLock()
	session, exists := sm.sessions[sessionID]
	sm.mu.RUnlock()

	if !exists {
		return errors.New(errors.ERR_AUTH_SESSION_EXPIRED,
			fmt.Sprintf("session %s not found", sessionID),
			errors.LayerAuth).WithContext("session_id", sessionID)
	}

	session.mu.Lock()
	defer session.mu.Unlock()

	// Store previous database for logging
	previousDatabase := session.DatabaseName

	// Update the database context
	session.DatabaseName = databaseName
	session.Database = database
	session.DatabaseFolderPath = helpers.GetDatabaseFolderPath(databaseName) // STEP 4: Update cache

	// Update activity time when database context changes
	session.LastActivity = time.Now()
	session.ExpiresAt = time.Now().Add(session.Timeout)

	session.Logger.Infow("Database context changed",
		"previousDatabase", previousDatabase,
		"newDatabase", databaseName)

	return nil
}

// ValidateSessionSecurity validates session security binding
func (sm *SessionManager) ValidateSessionSecurity(sessionID, clientIP, userAgent string) error {
	sm.mu.RLock()
	session, exists := sm.sessions[sessionID]
	sm.mu.RUnlock()

	if !exists {
		return errors.New(errors.ERR_AUTH_SESSION_EXPIRED,
			fmt.Sprintf("session %s not found", sessionID),
			errors.LayerAuth).WithContext("session_id", sessionID)
	}

	return validateSessionBinding(session, clientIP, userAgent)
}

// cleanupSession performs cleanup for a session (must be called with sm.mu held)
func (sm *SessionManager) cleanupSession(session *Session) error {
	session.mu.Lock()
	defer session.mu.Unlock()

	// #region agent log
	debugLogSessionCleanup(session.SessionID, session.TransactionStatus, session.ActiveTransactionID, len(session.DocumentLocks), len(session.BundleLocks))
	// #endregion

	session.Logger.Infow("Cleaning up session", "state", session.State.String())

	// Set state to terminated
	session.State = SessionStateTerminated

	// Cancel active transactions
	for txID, cancelFunc := range session.ActiveTransactions {
		session.Logger.Infow("Canceling active transaction", "transactionID", txID)
		cancelFunc()
	}
	session.ActiveTransactions = make(map[string]context.CancelFunc)

	// Release all locks
	for lockID, lockInfo := range session.DocumentLocks {
		session.Logger.Infow("Releasing document lock", "lockID", lockID, "resource", lockInfo.ResourceName)
	}
	session.DocumentLocks = make(map[string]*LockInfo)

	for lockID, lockInfo := range session.BundleLocks {
		session.Logger.Infow("Releasing bundle lock", "lockID", lockID, "resource", lockInfo.ResourceName)
	}
	session.BundleLocks = make(map[string]*LockInfo)

	// #region agent log
	// NOTE: This is a BUG - the code above only clears session's internal tracking maps
	// but does NOT call LockManager.ReleaseLocksForSession() to actually release locks!
	debugLogSessionLockRelease(session.SessionID, "internal_maps_only_NOT_lock_manager", 0)
	// #endregion

	// Clean up temp files (async, non-blocking)
	// Copy temp files slice to avoid race conditions and allow safe async processing
	tempFiles := make([]string, len(session.TempFiles))
	copy(tempFiles, session.TempFiles)
	session.TempFiles = []string{} // Clear the slice immediately

	// Enqueue files for async deletion (non-blocking)
	if len(tempFiles) > 0 {
		session.Logger.Infow("Enqueueing temp files for async cleanup", "count", len(tempFiles))
		select {
		case sm.tempFileCleanupQueue <- tempFiles:
			// Successfully enqueued
		default:
			// Queue is full - log warning but don't block
			// This prevents session cleanup from hanging if cleanup worker is overloaded
			session.Logger.Warnw("Temp file cleanup queue is full, files will not be cleaned up",
				"count", len(tempFiles),
				"files", tempFiles)
		}
	}

	// Clean up buffer pool if exists
	if session.BufferPool != nil {
		session.Logger.Info("Releasing buffer pool resources")
		
		// Flush any dirty buffers to ensure data integrity
		if err := session.BufferPool.FlushAllDirty(); err != nil {
			session.Logger.Warnw("Failed to flush dirty buffers during session cleanup",
				"error", err)
			// Continue with cleanup even if flush fails
		}

		// Attempt to clear the buffer pool (graceful - won't fail if buffers are in use)
		if err := session.BufferPool.ClearBufferPool(); err != nil {
			// If buffers are still in use, log warning but don't block session cleanup
			// This can happen if other goroutines are still accessing buffers
			session.Logger.Debugw("Could not fully clear buffer pool (buffers may still be in use)",
				"error", err)
		}

		session.BufferPool = nil
	}

	// Remove from session manager maps
	delete(sm.sessions, session.SessionID)
	delete(sm.connectionSessions, session.ConnectionID)

	// Remove from user sessions
	userSessions := sm.userSessions[session.Username]
	for i, s := range userSessions {
		if s.SessionID == session.SessionID {
			sm.userSessions[session.Username] = append(userSessions[:i], userSessions[i+1:]...)
			break
		}
	}

	// Clean up empty user session list
	if len(sm.userSessions[session.Username]) == 0 {
		delete(sm.userSessions, session.Username)
	}

	session.Logger.Info("Session cleanup completed")
	return nil
}

// startCleanupRoutine starts the background cleanup routine
func (sm *SessionManager) startCleanupRoutine() {
	sm.cleanupWG.Add(1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				fatal.LogFatalAndExit(r)
			}
		}()
		defer sm.cleanupWG.Done()

		ticker := time.NewTicker(sm.cleanupInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				sm.cleanupExpiredSessions()
			case <-sm.stopCleanup:
				return
			}
		}
	}()
}

// startTempFileCleanupWorker starts the background worker for async temp file cleanup
// This worker processes temp file deletion requests in batches to avoid blocking session cleanup
func (sm *SessionManager) startTempFileCleanupWorker() {
	sm.tempFileCleanupWG.Add(1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				sm.logger.Errorw("Temp file cleanup worker panicked, restarting", "error", r)
				// Restart the worker on panic
				sm.startTempFileCleanupWorker()
			}
			sm.tempFileCleanupWG.Done()
		}()

		batchSize := 20 // Process up to 20 files per batch
		maxRetries := 3
		retryDelays := []time.Duration{
			constants.RetryDelayShort * time.Millisecond,
			constants.RetryDelayMedium * time.Millisecond,
			constants.RetryDelayLong * time.Millisecond,
		}

		for {
			select {
			case files := <-sm.tempFileCleanupQueue:
				// Process files in batches
				for i := 0; i < len(files); i += batchSize {
					end := i + batchSize
					if end > len(files) {
						end = len(files)
					}
					batch := files[i:end]

					// Delete files in this batch
					for _, filePath := range batch {
						err := sm.deleteTempFileWithRetry(filePath, maxRetries, retryDelays)
						if err != nil {
							sm.logger.Warnw("Failed to delete temp file after retries",
								"file", filePath,
								"retries", maxRetries,
								"error", err)
						} else {
							sm.logger.Debugw("Successfully deleted temp file", "file", filePath)
						}
					}
				}

			case <-sm.stopTempFileCleanup:
				// Process any remaining files in queue before stopping
				for {
					select {
					case files := <-sm.tempFileCleanupQueue:
						for _, filePath := range files {
							_ = sm.deleteTempFileWithRetry(filePath, maxRetries, retryDelays)
						}
					default:
						return
					}
				}
			}
		}
	}()
}

// deleteTempFileWithRetry attempts to delete a temp file with exponential backoff retry logic
func (sm *SessionManager) deleteTempFileWithRetry(filePath string, maxRetries int, retryDelays []time.Duration) error {
	var lastErr error
	for attempt := 0; attempt < maxRetries; attempt++ {
		err := os.Remove(filePath)
		if err == nil {
			return nil // Success
		}

		lastErr = err

		// If file doesn't exist, consider it a success (already deleted)
		if os.IsNotExist(err) {
			return nil
		}

		// Wait before retry (except on last attempt)
		if attempt < maxRetries-1 {
			delay := retryDelays[attempt]
			if delay > 0 {
				time.Sleep(delay)
			}
		}
	}

	return errors.WrapWithMessage(lastErr, errors.ERR_INTERNAL_STORAGE,
		fmt.Sprintf("failed to delete temp file after %d attempts", maxRetries),
		errors.LayerAPI).WithContext("max_retries", fmt.Sprintf("%d", maxRetries))
}

// cleanupExpiredSessions removes expired sessions
func (sm *SessionManager) cleanupExpiredSessions() {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	now := time.Now()
	expiredSessions := make([]*Session, 0, 20)

	for _, session := range sm.sessions {
		if now.After(session.ExpiresAt) {
			session.mu.Lock()
			session.State = SessionStateExpired
			session.mu.Unlock()
			expiredSessions = append(expiredSessions, session)
		}
	}

	for _, session := range expiredSessions {
		sm.logger.Infow("Cleaning up expired session",
			"sessionID", session.SessionID,
			"username", session.Username,
			"expiredAt", session.ExpiresAt)
		sm.cleanupSession(session)
	}

	if len(expiredSessions) > 0 {
		sm.logger.Infow("Cleaned up expired sessions", "count", len(expiredSessions))
	}
}

// Stop stops the session manager and cleans up all sessions
func (sm *SessionManager) Stop() {
	// Stop cleanup routines
	close(sm.stopCleanup)
	close(sm.stopTempFileCleanup)

	// Wait for both cleanup workers to finish
	sm.cleanupWG.Wait()
	sm.tempFileCleanupWG.Wait()

	sm.mu.Lock()
	defer sm.mu.Unlock()

	// Clean up all remaining sessions
	for _, session := range sm.sessions {
		sm.cleanupSession(session)
	}
}

// GetSessionStats returns statistics about sessions
func (sm *SessionManager) GetSessionStats() map[string]interface{} {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	// PHASE 3: Use pooled map to reduce allocations
	stats := GetResponseMap()
	stats["total_sessions"] = len(sm.sessions)
	stats["active_users"] = len(sm.userSessions)
	stats["max_sessions"] = sm.maxSessions
	stats["default_timeout"] = sm.defaultTimeout.String()
	stats["cleanup_interval"] = sm.cleanupInterval.String()

	// Count sessions by state
	stateCounts := make(map[string]int)
	for _, session := range sm.sessions {
		session.mu.RLock()
		state := session.State.String()
		session.mu.RUnlock()
		stateCounts[state]++
	}
	stats["sessions_by_state"] = stateCounts

	return stats
}

// Session methods for managing state and resources

// StartQuery starts a new query execution
func (s *Session) StartQuery(queryID, query string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.State = SessionStateExecuting
	s.LastActivity = time.Now()
	s.ExpiresAt = time.Now().Add(s.Timeout)

	s.CurrentQuery = &QueryInfo{
		QueryID:   queryID,
		Query:     query,
		StartTime: time.Now(),
		Status:    "EXECUTING",
	}

	s.Logger.Debugf("Started query execution '%s'", query)
}

// CompleteQuery marks the current query as completed
func (s *Session) CompleteQuery(affectedRows int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.CurrentQuery == nil {
		return
	}

	now := time.Now()
	s.CurrentQuery.EndTime = &now
	s.CurrentQuery.Status = "COMPLETED"
	s.CurrentQuery.AffectedRows = affectedRows

	// Move to successful query and history
	s.LastSuccessfulQuery = s.CurrentQuery
	s.addToHistory(s.CurrentQuery)

	s.CurrentQuery = nil
	s.State = SessionStateActive
	s.LastActivity = now
	s.ExpiresAt = now.Add(s.Timeout)

	// Reset consecutive error count on success
	s.ConsecutiveErrors = 0

	// s.Logger.Infow("Completed query execution",
	//
	//	"queryID", s.LastSuccessfulQuery.QueryID,
	//	"affectedRows", affectedRows,
	//	"duration", s.LastSuccessfulQuery.EndTime.Sub(s.LastSuccessfulQuery.StartTime))
}

// FailQuery marks the current query as failed
func (s *Session) FailQuery(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.CurrentQuery == nil {
		return
	}

	now := time.Now()
	s.CurrentQuery.EndTime = &now
	s.CurrentQuery.Status = "FAILED"
	s.CurrentQuery.Error = err

	// Update error tracking
	s.LastError = err
	s.ErrorCount++
	s.ConsecutiveErrors++

	// Save queryID before setting CurrentQuery to nil
	queryID := s.CurrentQuery.QueryID
	s.addToHistory(s.CurrentQuery)
	s.CurrentQuery = nil

	// Set state based on error count
	if s.ConsecutiveErrors >= 5 {
		s.State = SessionStateError
		s.Logger.Errorw("Session entered error state due to consecutive failures",
			"consecutiveErrors", s.ConsecutiveErrors)
	} else {
		s.State = SessionStateActive
	}

	s.LastActivity = now
	s.ExpiresAt = now.Add(s.Timeout)

	s.Logger.Errorw("Query execution failed",
		"queryID", queryID,
		"error", err,
		"consecutiveErrors", s.ConsecutiveErrors)
}

// addToHistory adds a query to the history (must be called with lock held)
func (s *Session) addToHistory(query *QueryInfo) {
	s.QueryHistory = append(s.QueryHistory, query)

	// Keep only the last MaxQueryHistory queries
	if len(s.QueryHistory) > s.MaxQueryHistory {
		s.QueryHistory = s.QueryHistory[len(s.QueryHistory)-s.MaxQueryHistory:]
	}
}

// AcquireDocumentLock acquires a lock on a document
func (s *Session) AcquireDocumentLock(documentID, lockMode string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	lockID := fmt.Sprintf("doc_%s_%s", documentID, lockMode)

	// Check if we already have this lock
	if _, exists := s.DocumentLocks[lockID]; exists {
		return errors.New(errors.ERR_INTERNAL_LOCK,
			fmt.Sprintf("document lock already held: %s", documentID),
			errors.LayerTransaction).WithContext("document_id", documentID)
	}

	lockInfo := &LockInfo{
		LockID:       lockID,
		LockType:     "DOCUMENT",
		ResourceName: documentID,
		LockMode:     lockMode,
		AcquiredAt:   time.Now(),
	}

	s.DocumentLocks[lockID] = lockInfo
	s.Logger.Infow("Acquired document lock",
		"documentID", documentID,
		"lockMode", lockMode,
		"lockID", lockID)

	return nil
}

// ReleaseDocumentLock releases a lock on a document
func (s *Session) ReleaseDocumentLock(documentID, lockMode string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	lockID := fmt.Sprintf("doc_%s_%s", documentID, lockMode)

	if _, exists := s.DocumentLocks[lockID]; !exists {
		return errors.New(errors.ERR_INTERNAL_LOCK,
			fmt.Sprintf("document lock not held: %s", documentID),
			errors.LayerTransaction).WithContext("document_id", documentID)
	}

	delete(s.DocumentLocks, lockID)
	s.Logger.Infow("Released document lock",
		"documentID", documentID,
		"lockMode", lockMode,
		"lockID", lockID)

	return nil
}

// AcquireBundleLock acquires a lock on a bundle
func (s *Session) AcquireBundleLock(bundleName, lockMode string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	lockID := fmt.Sprintf("bundle_%s_%s", bundleName, lockMode)

	// Check if we already have this lock
	if _, exists := s.BundleLocks[lockID]; exists {
		return errors.New(errors.ERR_INTERNAL_LOCK,
			fmt.Sprintf("bundle lock already held: %s", bundleName),
			errors.LayerTransaction).WithContext("bundle", bundleName)
	}

	lockInfo := &LockInfo{
		LockID:       lockID,
		LockType:     "BUNDLE",
		ResourceName: bundleName,
		LockMode:     lockMode,
		AcquiredAt:   time.Now(),
	}

	s.BundleLocks[lockID] = lockInfo
	s.Logger.Infow("Acquired bundle lock",
		"bundleName", bundleName,
		"lockMode", lockMode,
		"lockID", lockID)

	return nil
}

// ReleaseBundleLock releases a lock on a bundle
func (s *Session) ReleaseBundleLock(bundleName, lockMode string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	lockID := fmt.Sprintf("bundle_%s_%s", bundleName, lockMode)

	if _, exists := s.BundleLocks[lockID]; !exists {
		return errors.New(errors.ERR_INTERNAL_LOCK,
			fmt.Sprintf("bundle lock not held: %s", bundleName),
			errors.LayerTransaction).WithContext("bundle", bundleName)
	}

	delete(s.BundleLocks, lockID)
	s.Logger.Infow("Released bundle lock",
		"bundleName", bundleName,
		"lockMode", lockMode,
		"lockID", lockID)

	return nil
}

// AddTransaction adds a transaction to the session
func (s *Session) AddTransaction(txID string, cancelFunc context.CancelFunc) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.ActiveTransactions[txID] = cancelFunc
	s.Logger.Infow("Added transaction to session", "transactionID", txID)
}

// RemoveTransaction removes a transaction from the session
func (s *Session) RemoveTransaction(txID string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.ActiveTransactions[txID]; exists {
		delete(s.ActiveTransactions, txID)
		s.Logger.Infow("Removed transaction from session", "transactionID", txID)
	}
}

// AddTempFile adds a temporary file to the session
func (s *Session) AddTempFile(filePath string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.TempFiles = append(s.TempFiles, filePath)
	s.Logger.Infow("Added temp file to session", "file", filePath)
}

// GetSessionInfo returns current session information
func (s *Session) GetSessionInfo() map[string]interface{} {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// PHASE 3: Use pooled map to reduce allocations
	info := GetResponseMap()
	info["sessionID"] = s.SessionID
	info["username"] = s.Username
	info["database"] = s.DatabaseName
	info["clientIP"] = s.ClientIP
	info["userAgent"] = func() string {
		if len(s.UserAgent) > constants.UserAgentMaxLength {
			return s.UserAgent[:constants.UserAgentMaxLength] + "..."
		}
		return s.UserAgent
	}()
	info["state"] = s.State.String()
	info["createdAt"] = s.CreatedAt
	info["lastActivity"] = s.LastActivity
	info["expiresAt"] = s.ExpiresAt
	info["errorCount"] = s.ErrorCount
	info["consecutiveErrors"] = s.ConsecutiveErrors
	info["documentLocks"] = len(s.DocumentLocks)
	info["bundleLocks"] = len(s.BundleLocks)
	info["activeTransactions"] = len(s.ActiveTransactions)
	info["tempFiles"] = len(s.TempFiles)
	info["queryHistoryCount"] = len(s.QueryHistory)

	if s.CurrentQuery != nil {
		// PHASE 3: Use pooled map for nested query info
		currentQuery := GetResponseMap()
		currentQuery["queryID"] = s.CurrentQuery.QueryID
		currentQuery["query"] = s.CurrentQuery.Query
		currentQuery["startTime"] = s.CurrentQuery.StartTime
		currentQuery["status"] = s.CurrentQuery.Status
		info["currentQuery"] = currentQuery
	}

	if s.LastSuccessfulQuery != nil {
		// PHASE 3: Use pooled map for last successful query info
		lastQuery := GetResponseMap()
		lastQuery["queryID"] = s.LastSuccessfulQuery.QueryID
		lastQuery["query"] = s.LastSuccessfulQuery.Query
		lastQuery["affectedRows"] = s.LastSuccessfulQuery.AffectedRows
		lastQuery["endTime"] = s.LastSuccessfulQuery.EndTime
		info["lastSuccessfulQuery"] = lastQuery
	}

	if s.LastError != nil {
		info["lastError"] = s.LastError.Error()
	}

	return info
}

// GetRole returns the cached role for this session, fetching from PermissionService if cache is expired
// This implements the lazy role caching pattern with 5-minute TTL
func (s *Session) GetRole() (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Check if role cache is still valid
	if !s.RoleCacheTime.IsZero() && time.Since(s.RoleCacheTime) < s.RoleCacheTTL {
		// Cache is still fresh, return cached role
		return s.CachedRole, nil
	}

	// Cache expired or empty, fetch role from PermissionService
	if s.PermissionService == nil {
		// No permission service available, default to "anonymous"
		s.CachedRole = "anonymous"
		s.IsAdmin = false
		s.RoleCacheTime = time.Now()
		return s.CachedRole, nil
	}

	// Query permission service to determine if user is admin
	isAdmin, err := s.PermissionService.UserHasPermission(s.Username, "Administrator")
	if err != nil {
		// Error checking permissions, but don't fail - default to authenticated non-admin
		if s.Username != "" {
			s.CachedRole = "authenticated"
			s.IsAdmin = false
		} else {
			s.CachedRole = "anonymous"
			s.IsAdmin = false
		}
		s.RoleCacheTime = time.Now()
		return s.CachedRole, err
	}

	// Set role based on admin status
	if isAdmin {
		s.CachedRole = "admin"
		s.IsAdmin = true
	} else if s.Username != "" {
		s.CachedRole = "authenticated"
		s.IsAdmin = false
	} else {
		s.CachedRole = "anonymous"
		s.IsAdmin = false
	}

	// Update cache timestamp
	s.RoleCacheTime = time.Now()

	return s.CachedRole, nil
}

// GetIsAdmin returns the cached admin status for this session
func (s *Session) GetIsAdmin() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// If cache is expired, trigger a role refresh (but don't block on it)
	if s.RoleCacheTime.IsZero() || time.Since(s.RoleCacheTime) >= s.RoleCacheTTL {
		// Cache is stale, but return current value and let next GetRole() refresh it
		// This prevents blocking during IsAdmin checks
		return s.IsAdmin
	}

	return s.IsAdmin
}

// InvalidateRoleCache clears the role cache, forcing a refresh on next GetRole() call
// This is called when GRANT/REVOKE commands modify user permissions
func (s *Session) InvalidateRoleCache() {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Clear cache timestamp to force refresh
	s.RoleCacheTime = time.Time{} // Zero value
	s.CachedRole = ""
	s.IsAdmin = false
}

// GetSessionsByUser returns all active sessions for a given username
func (sm *SessionManager) GetSessionsByUser(username string) []*Session {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	// Return sessions from userSessions map (which is already maintained)
	sessions := sm.userSessions[username]

	// Return a copy to prevent external modification
	result := make([]*Session, len(sessions))
	copy(result, sessions)

	return result
}

// Transaction Management Methods for Session

// IsInTransaction returns true if the session has an active transaction
func (s *Session) IsInTransaction() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.TransactionActive && s.TransactionStatus == TransactionStatusActive
}

// GetActiveTransactionID returns the current transaction ID (empty string if not in transaction)
func (s *Session) GetActiveTransactionID() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.ActiveTransactionID
}

// IsIdleExpired checks if the transaction has exceeded the idle timeout
func (s *Session) IsIdleExpired(timeout time.Duration) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if !s.TransactionActive {
		return false
	}

	return time.Since(s.TransactionStartTime) > timeout
}

// BeginTransaction initializes transaction state for the session.
// snapshot is the MVCC snapshot for this transaction (created on BEGIN); may be nil for non-MVCC paths.
func (s *Session) BeginTransaction(txID string, startLSN uint64, snapshot *journal.Snapshot) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.TransactionActive = true
	s.ActiveTransactionID = txID
	s.TransactionStartLSN = startLSN
	s.TransactionStartTime = time.Now()
	s.PendingOperations = make([]string, 0)
	s.CurrentSavepoint = nil
	s.TransactionStatus = TransactionStatusActive
	s.MVCCSnapshot = snapshot
	// PHASE 2: MVCC - Initialize transaction buffer for document location tracking
	s.TransactionBuffer = NewTransactionBuffer()
}

// GetMVCCSnapshot returns the MVCC snapshot for this session's active transaction, or nil.
func (s *Session) GetMVCCSnapshot() *journal.Snapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.MVCCSnapshot
}

// CommitTransaction marks the transaction as committed and clears state
func (s *Session) CommitTransaction() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.TransactionStatus = TransactionStatusCommitted
	s.clearTransactionState()
}

// AbortTransaction marks the transaction as aborted and clears state
func (s *Session) AbortTransaction() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.TransactionStatus = TransactionStatusAborted
	s.clearTransactionState()
}

// clearTransactionState resets all transaction-related fields (must hold mu.Lock)
func (s *Session) clearTransactionState() {
	s.TransactionActive = false
	s.ActiveTransactionID = ""
	s.TransactionStartLSN = 0
	s.TransactionStartTime = time.Time{}
	s.PendingOperations = nil
	s.CurrentSavepoint = nil
	s.MVCCSnapshot = nil
	// PHASE 2: MVCC - Clear transaction buffer
	if s.TransactionBuffer != nil {
		s.TransactionBuffer.Clear()
		s.TransactionBuffer = nil
	}
}

// AddPendingOperation adds a command to the transaction's pending operations buffer
func (s *Session) AddPendingOperation(command string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.PendingOperations == nil {
		s.PendingOperations = make([]string, 0)
	}
	s.PendingOperations = append(s.PendingOperations, command)
}

// SetSavepoint sets a savepoint for the transaction (replaces any existing savepoint)
func (s *Session) SetSavepoint(name string, lsn uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.CurrentSavepoint = &Savepoint{
		Name: name,
		LSN:  lsn,
	}
}

// GetSavepoint returns the current savepoint, or nil if none exists
func (s *Session) GetSavepoint() *Savepoint {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.CurrentSavepoint
}

// ClearSavepoint removes the current savepoint
func (s *Session) ClearSavepoint() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.CurrentSavepoint = nil
}

// GetTransactionInfo returns current transaction information for debugging/monitoring
func (s *Session) GetTransactionInfo() map[string]interface{} {
	s.mu.RLock()
	defer s.mu.RUnlock()

	info := map[string]interface{}{
		"active":            s.TransactionActive,
		"status":            s.TransactionStatus.String(),
		"transaction_id":    s.ActiveTransactionID,
		"start_lsn":         s.TransactionStartLSN,
		"start_time":        s.TransactionStartTime,
		"pending_ops_count": len(s.PendingOperations),
	}

	if s.CurrentSavepoint != nil {
		info["savepoint_name"] = s.CurrentSavepoint.Name
		info["savepoint_lsn"] = s.CurrentSavepoint.LSN
	}

	return info
}
