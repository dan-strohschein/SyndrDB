package homegrown

import (
	"time"

	"github.com/fatih/color"
	"go.uber.org/zap"
)

// WALTestUseCase defines the structure for a WAL test scenario
type WALTestUseCase struct {
	Name          string
	Description   string
	Category      string
	SetupFunc     func() error
	ExecuteFunc   func() error
	ValidateFunc  func() error
	CleanupFunc   func() error
	ExpectSuccess bool
	Tags          []string
	Timeout       time.Duration
}

// WALTestResult represents the result of a single WAL test execution
type WALTestResult struct {
	UseCase   WALTestUseCase
	Success   bool
	Duration  time.Duration
	Error     error
	StartTime time.Time
	EndTime   time.Time
	Details   string
}

// Color functions for console output
var (
	HighlightGreen  = color.New(color.FgGreen, color.Bold).SprintFunc()
	HighlightRed    = color.New(color.FgRed, color.Bold).SprintFunc()
	HighlightYellow = color.New(color.FgYellow, color.Bold).SprintFunc()
	HighlightBlue   = color.New(color.FgBlue, color.Bold).SprintFunc()
	HighlightCyan   = color.New(color.FgCyan, color.Bold).SprintFunc()
	Normal          = color.New(color.Reset).SprintFunc()
)

// ColorLogger is the global logger instance
var ColorLogger *zap.SugaredLogger

// UseCase defines the interface that all test use cases must implement
type UseCase interface {
	GetName() string
	GetDescription() string
	GetCategory() string
	GetExpectSuccess() bool
	Setup() error
	Execute() error
	Validate() error
	Cleanup() error
}
