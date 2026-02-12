package server

import (
	"context"
	"fmt"
	"sync"
	"syndrdb/src/internal/domain/models"
	"syndrdb/src/internal/query/planner"
	"syndrdb/src/pkg/errors"
	"time"

	"go.uber.org/zap"
)

// CursorState represents a server-side cursor holding a suspended iterator.
type CursorState struct {
	Name         string
	Iterator     planner.IteratorNode // Suspended execution state
	Fields       []string             // Projected field names
	CreatedAt    time.Time
	LastFetched  time.Time
	TotalFetched int  // Total documents fetched so far
	Exhausted    bool // True when iterator returned (nil, nil) EOF
	Cancel       context.CancelFunc

	mu sync.Mutex // Protects concurrent FETCH calls on same cursor
}

// Fetch pulls up to n documents from the suspended iterator.
// Returns the documents and whether more data remains.
func (cs *CursorState) Fetch(n int) ([]*models.Document, bool, error) {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	if cs.Exhausted {
		return nil, false, nil
	}

	docs := make([]*models.Document, 0, n)
	for i := 0; i < n; i++ {
		doc, err := cs.Iterator.Next()
		if err != nil {
			return docs, false, err
		}
		// (nil, nil) = EOF
		if doc == nil {
			cs.Exhausted = true
			break
		}
		docs = append(docs, doc)
		cs.TotalFetched++
	}

	cs.LastFetched = time.Now()
	return docs, !cs.Exhausted, nil
}

// Close releases the iterator and cancels the query context.
func (cs *CursorState) Close() {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	if cs.Iterator != nil {
		cs.Iterator.Close()
		cs.Iterator = nil
	}
	if cs.Cancel != nil {
		cs.Cancel()
		cs.Cancel = nil
	}
	cs.Exhausted = true
}

// CursorManager manages all cursors for a session.
type CursorManager struct {
	cursors    map[string]*CursorState
	maxCursors int
	mu         sync.RWMutex
	logger     *zap.SugaredLogger
}

// NewCursorManager creates a cursor manager with a maximum cursor limit.
func NewCursorManager(maxCursors int, logger *zap.SugaredLogger) *CursorManager {
	return &CursorManager{
		cursors:    make(map[string]*CursorState),
		maxCursors: maxCursors,
		logger:     logger,
	}
}

// Declare creates a new cursor with a suspended iterator.
func (cm *CursorManager) Declare(
	name string,
	iterator planner.IteratorNode,
	fields []string,
	cancel context.CancelFunc,
) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if _, exists := cm.cursors[name]; exists {
		return errors.New(errors.ERR_VALIDATION_CONSTRAINT,
			fmt.Sprintf("cursor '%s' already exists", name),
			errors.LayerTransaction)
	}

	if len(cm.cursors) >= cm.maxCursors {
		return errors.New(errors.ERR_RESOURCE_EXHAUSTED,
			fmt.Sprintf("maximum cursors (%d) reached for session", cm.maxCursors),
			errors.LayerTransaction)
	}

	cm.cursors[name] = &CursorState{
		Name:        name,
		Iterator:    iterator,
		Fields:      fields,
		CreatedAt:   time.Now(),
		LastFetched: time.Now(),
		Cancel:      cancel,
	}

	cm.logger.Debugf("DECLARE CURSOR: name=%s", name)
	return nil
}

// Fetch retrieves up to n documents from the named cursor.
func (cm *CursorManager) Fetch(name string, n int) ([]*models.Document, []string, bool, error) {
	cm.mu.RLock()
	cursor, exists := cm.cursors[name]
	cm.mu.RUnlock()

	if !exists {
		return nil, nil, false, errors.New(errors.ERR_NOT_FOUND_INDEX,
			fmt.Sprintf("cursor '%s' not found", name),
			errors.LayerTransaction)
	}

	docs, hasMore, err := cursor.Fetch(n)
	return docs, cursor.Fields, hasMore, err
}

// Close closes a named cursor and frees its resources.
func (cm *CursorManager) Close(name string) error {
	cm.mu.Lock()
	cursor, exists := cm.cursors[name]
	if !exists {
		cm.mu.Unlock()
		return errors.New(errors.ERR_NOT_FOUND_INDEX,
			fmt.Sprintf("cursor '%s' not found", name),
			errors.LayerTransaction)
	}
	delete(cm.cursors, name)
	cm.mu.Unlock()

	cursor.Close()
	cm.logger.Debugf("CLOSE CURSOR: name=%s, total_fetched=%d", name, cursor.TotalFetched)
	return nil
}

// CloseAll closes all open cursors. Called on session cleanup,
// transaction commit, or transaction rollback.
func (cm *CursorManager) CloseAll() {
	cm.mu.Lock()
	cursors := cm.cursors
	cm.cursors = make(map[string]*CursorState)
	cm.mu.Unlock()

	for name, cursor := range cursors {
		cursor.Close()
		cm.logger.Debugf("CLOSE CURSOR (cleanup): name=%s, total_fetched=%d", name, cursor.TotalFetched)
	}
}

// CleanupExpired closes cursors that have not been fetched within the timeout.
func (cm *CursorManager) CleanupExpired(timeout time.Duration) int {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	now := time.Now()
	expired := 0
	for name, cursor := range cm.cursors {
		if now.Sub(cursor.LastFetched) > timeout {
			cursor.Close()
			delete(cm.cursors, name)
			expired++
			cm.logger.Infof("CURSOR EXPIRED: name=%s, idle=%v", name, now.Sub(cursor.LastFetched))
		}
	}
	return expired
}

// Count returns the number of open cursors.
func (cm *CursorManager) Count() int {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	return len(cm.cursors)
}
