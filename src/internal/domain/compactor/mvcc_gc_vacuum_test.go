package compactor

import (
	"sync/atomic"
	"testing"
	"time"

	"syndrdb/src/internal/domain/models"

	"go.uber.org/zap"
)

func TestNewMVCCGCWorker_VacuumDefaults(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	sugar := logger.Sugar()

	var queryCount atomic.Uint64
	worker := NewMVCCGCWorker(MVCCGCConfig{
		Interval:              30 * time.Second,
		BatchSize:             10,
		PauseThreshold:        500,
		MaxVersionsThreshold:  5,
		MinVersionAge:         1 * time.Hour,
		DataDir:               "/tmp/test",
		ActiveQueryCount:      &queryCount,
		Logger:                sugar,
		// VacuumEnabled not set (zero value = false)
		// VacuumDeadRatioThreshold not set (zero value = 0)
		// VacuumMaxPagesPerCycle not set (zero value = 0)
	})

	// VacuumEnabled defaults to false (Go zero value)
	if worker.vacuumEnabled {
		t.Error("vacuum should default to false when not explicitly set")
	}

	// Dead ratio threshold should default to 0.3
	if worker.vacuumDeadRatioThreshold != 0.3 {
		t.Errorf("expected vacuum dead ratio threshold 0.3, got %f", worker.vacuumDeadRatioThreshold)
	}

	// Max pages per cycle should default to 100
	if worker.vacuumMaxPagesPerCycle != 100 {
		t.Errorf("expected vacuum max pages per cycle 100, got %d", worker.vacuumMaxPagesPerCycle)
	}
}

func TestNewMVCCGCWorker_VacuumExplicitConfig(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	sugar := logger.Sugar()

	var queryCount atomic.Uint64
	worker := NewMVCCGCWorker(MVCCGCConfig{
		Interval:                 30 * time.Second,
		BatchSize:                10,
		PauseThreshold:           500,
		MaxVersionsThreshold:     5,
		MinVersionAge:            1 * time.Hour,
		DataDir:                  "/tmp/test",
		ActiveQueryCount:         &queryCount,
		Logger:                   sugar,
		VacuumEnabled:            true,
		VacuumDeadRatioThreshold: 0.5,
		VacuumMaxPagesPerCycle:   200,
	})

	if !worker.vacuumEnabled {
		t.Error("vacuum should be enabled when explicitly set")
	}

	if worker.vacuumDeadRatioThreshold != 0.5 {
		t.Errorf("expected vacuum dead ratio threshold 0.5, got %f", worker.vacuumDeadRatioThreshold)
	}

	if worker.vacuumMaxPagesPerCycle != 200 {
		t.Errorf("expected vacuum max pages per cycle 200, got %d", worker.vacuumMaxPagesPerCycle)
	}
}

func TestCanCleanupDocument_CurrentVersion(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	sugar := logger.Sugar()

	var queryCount atomic.Uint64
	worker := NewMVCCGCWorker(MVCCGCConfig{
		Interval:             30 * time.Second,
		BatchSize:            10,
		PauseThreshold:       500,
		MaxVersionsThreshold: 5,
		MinVersionAge:        1 * time.Hour,
		DataDir:              "/tmp/test",
		ActiveQueryCount:     &queryCount,
		Logger:               sugar,
	})

	// Test: current version should not be cleaned up
	doc := &models.Document{
		DocumentID:      "doc1",
		CommitSequence:  50,
		SupersededAt:    time.Time{}, // Zero = current version
		VersionSequence: 1,
	}

	if worker.canCleanupDocument(doc, 100) {
		t.Error("current version should not be cleaned up")
	}
}

func TestCanCleanupDocument_SupersededPastGrace(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	sugar := logger.Sugar()

	var queryCount atomic.Uint64
	worker := NewMVCCGCWorker(MVCCGCConfig{
		Interval:             30 * time.Second,
		BatchSize:            10,
		PauseThreshold:       500,
		MaxVersionsThreshold: 5,
		MinVersionAge:        1 * time.Hour,
		DataDir:              "/tmp/test",
		ActiveQueryCount:     &queryCount,
		Logger:               sugar,
	})

	// Test: superseded version past grace period
	doc := &models.Document{
		DocumentID:      "doc1",
		CommitSequence:  50,
		SupersededAt:    time.Now().Add(-200 * time.Millisecond), // Past 100ms grace
		VersionSequence: 1,
	}

	if !worker.canCleanupDocument(doc, 100) {
		t.Error("superseded version past grace period should be cleaned up")
	}
}

func TestCanCleanupDocument_NilDocument(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	sugar := logger.Sugar()

	var queryCount atomic.Uint64
	worker := NewMVCCGCWorker(MVCCGCConfig{
		Interval:             30 * time.Second,
		BatchSize:            10,
		PauseThreshold:       500,
		MaxVersionsThreshold: 5,
		MinVersionAge:        1 * time.Hour,
		DataDir:              "/tmp/test",
		ActiveQueryCount:     &queryCount,
		Logger:               sugar,
	})

	if worker.canCleanupDocument(nil, 100) {
		t.Error("nil document should not be cleaned up")
	}
}

func TestShouldCompactByAge_NoVersions(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	sugar := logger.Sugar()

	var queryCount atomic.Uint64
	worker := NewMVCCGCWorker(MVCCGCConfig{
		Interval:             30 * time.Second,
		BatchSize:            10,
		PauseThreshold:       500,
		MaxVersionsThreshold: 5,
		MinVersionAge:        1 * time.Hour,
		DataDir:              "/tmp/test",
		ActiveQueryCount:     &queryCount,
		Logger:               sugar,
	})

	if worker.shouldCompactByAge(100, nil) {
		t.Error("should not compact with no versions")
	}

	if worker.shouldCompactByAge(100, []*models.Document{}) {
		t.Error("should not compact with empty versions slice")
	}
}
