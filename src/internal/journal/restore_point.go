package journal

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// RestorePoint represents a named recovery target for PITR.
type RestorePoint struct {
	Name         string    `json:"name"`
	LSN          uint64    `json:"lsn"`
	CommitSeq    uint64    `json:"commit_seq"`
	Timestamp    time.Time `json:"timestamp"`
	DatabaseName string    `json:"database_name,omitempty"`
}

// RestorePointManager manages named restore points.
// Restore points are persisted to a JSON file and also recorded in the WAL
// via OpRestorePoint entries for PITR replay.
type RestorePointManager struct {
	mu       sync.RWMutex
	points   []RestorePoint
	filePath string // Path to restore_points.json
}

// NewRestorePointManager creates a new restore point manager.
// filePath is the path to the persistence file (e.g., "<logdir>/wal/restore_points.json").
func NewRestorePointManager(filePath string) *RestorePointManager {
	rpm := &RestorePointManager{
		points:   make([]RestorePoint, 0),
		filePath: filePath,
	}
	// Load existing points from disk (best-effort)
	_ = rpm.Load()
	return rpm
}

// Create adds a new named restore point with the given LSN and commit sequence.
func (rpm *RestorePointManager) Create(name string, lsn uint64, commitSeq uint64) (*RestorePoint, error) {
	rpm.mu.Lock()
	defer rpm.mu.Unlock()

	// Check for duplicate names
	for _, p := range rpm.points {
		if p.Name == name {
			return nil, fmt.Errorf("restore point '%s' already exists", name)
		}
	}

	rp := RestorePoint{
		Name:      name,
		LSN:       lsn,
		CommitSeq: commitSeq,
		Timestamp: time.Now(),
	}

	rpm.points = append(rpm.points, rp)

	// Persist to disk
	if err := rpm.persistLocked(); err != nil {
		// Remove the point we just added on persist failure
		rpm.points = rpm.points[:len(rpm.points)-1]
		return nil, fmt.Errorf("failed to persist restore point: %w", err)
	}

	return &rp, nil
}

// List returns all restore points, ordered by creation time.
func (rpm *RestorePointManager) List() []RestorePoint {
	rpm.mu.RLock()
	defer rpm.mu.RUnlock()

	result := make([]RestorePoint, len(rpm.points))
	copy(result, rpm.points)
	return result
}

// Get returns a restore point by name, or nil if not found.
func (rpm *RestorePointManager) Get(name string) *RestorePoint {
	rpm.mu.RLock()
	defer rpm.mu.RUnlock()

	for i := range rpm.points {
		if rpm.points[i].Name == name {
			rp := rpm.points[i]
			return &rp
		}
	}
	return nil
}

// Delete removes a restore point by name.
func (rpm *RestorePointManager) Delete(name string) error {
	rpm.mu.Lock()
	defer rpm.mu.Unlock()

	found := false
	filtered := make([]RestorePoint, 0, len(rpm.points))
	for _, p := range rpm.points {
		if p.Name == name {
			found = true
			continue
		}
		filtered = append(filtered, p)
	}
	if !found {
		return fmt.Errorf("restore point '%s' not found", name)
	}

	rpm.points = filtered
	return rpm.persistLocked()
}

// Persist writes the current restore points to disk.
func (rpm *RestorePointManager) Persist() error {
	rpm.mu.RLock()
	defer rpm.mu.RUnlock()
	return rpm.persistLocked()
}

func (rpm *RestorePointManager) persistLocked() error {
	if rpm.filePath == "" {
		return nil
	}

	// Ensure parent directory exists
	if err := os.MkdirAll(filepath.Dir(rpm.filePath), 0755); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	data, err := json.MarshalIndent(rpm.points, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal failed: %w", err)
	}

	return os.WriteFile(rpm.filePath, data, 0644)
}

// Load reads restore points from disk.
func (rpm *RestorePointManager) Load() error {
	if rpm.filePath == "" {
		return nil
	}

	data, err := os.ReadFile(rpm.filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("read failed: %w", err)
	}

	var points []RestorePoint
	if err := json.Unmarshal(data, &points); err != nil {
		return fmt.Errorf("unmarshal failed: %w", err)
	}

	rpm.mu.Lock()
	rpm.points = points
	rpm.mu.Unlock()

	return nil
}
