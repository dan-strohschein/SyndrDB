package server

import (
	"fmt"
	"os"
	"runtime/trace"
	"sync"
	"time"

	"go.uber.org/zap"
)

// TraceHelper manages execution tracing for performance debugging
type TraceHelper struct {
	enabled     bool
	traceFile   *os.File
	logger      *zap.SugaredLogger
	startTime   time.Time
	mutex       sync.Mutex
	taskStack   []trace.Task
	regionStack []trace.Region
}

var globalTraceHelper *TraceHelper
var traceOnce sync.Once

// InitTrace initializes the global trace helper
func InitTrace(logger *zap.SugaredLogger, enabled bool) error {
	var err error
	traceOnce.Do(func() {
		globalTraceHelper = &TraceHelper{
			enabled:     enabled,
			logger:      logger,
			taskStack:   make([]trace.Task, 0, 10),
			regionStack: make([]trace.Region, 0, 20),
		}

		if enabled {
			// Create trace file with timestamp
			timestamp := time.Now().Format("20060102_150405")
			filename := fmt.Sprintf("trace_add_document_%s.out", timestamp)
			globalTraceHelper.traceFile, err = os.Create(filename)
			if err != nil {
				logger.Errorf("Failed to create trace file: %v", err)
				return
			}

			err = trace.Start(globalTraceHelper.traceFile)
			if err != nil {
				logger.Errorf("Failed to start trace: %v", err)
				globalTraceHelper.traceFile.Close()
				return
			}

			logger.Infof("Execution tracing enabled. Output: %s", filename)
			logger.Infof("After running, analyze with: go tool trace %s", filename)
		}
	})
	return err
}

// StopTrace stops the global trace
func StopTrace() {
	if globalTraceHelper != nil && globalTraceHelper.enabled {
		trace.Stop()
		if globalTraceHelper.traceFile != nil {
			globalTraceHelper.traceFile.Close()
			globalTraceHelper.logger.Infof("Trace file closed: %s", globalTraceHelper.traceFile.Name())
		}
	}
}

// TraceRegion represents a traced code region with timing
type TraceRegion struct {
	name      string
	startTime time.Time
	task      *trace.Task
	region    *trace.Region
	logger    *zap.SugaredLogger
}

// StartRegion starts a new traced region
func StartRegion(name string, logger *zap.SugaredLogger) *TraceRegion {
	if globalTraceHelper == nil || !globalTraceHelper.enabled {
		return &TraceRegion{
			name:      name,
			startTime: time.Now(),
			logger:    logger,
		}
	}

	region := &TraceRegion{
		name:      name,
		startTime: time.Now(),
		logger:    logger,
	}

	// Start trace region
	region.region = trace.StartRegion(nil, name)

	return region
}

// End ends the traced region and logs timing
func (tr *TraceRegion) End() {
	duration := time.Since(tr.startTime)

	// End trace region
	if tr.region != nil {
		tr.region.End()
	}

	// Log timing - highlight slow operations
	if duration > 10*time.Millisecond {
		tr.logger.Warnf("⚠️  SLOW: %s took %v (>10ms)", tr.name, duration)
	} else if duration > 1*time.Millisecond {
		tr.logger.Infof("📊 %s took %v", tr.name, duration)
	} else if duration > 100*time.Microsecond {
		tr.logger.Debugf("✓ %s took %v", tr.name, duration)
	} else {
		tr.logger.Debugf("⚡ %s took %v (fast)", tr.name, duration)
	}
}

// EndWithData ends the region and logs additional data
func (tr *TraceRegion) EndWithData(data map[string]interface{}) {
	duration := time.Since(tr.startTime)

	if tr.region != nil {
		tr.region.End()
	}

	// Build data string
	dataStr := ""
	for k, v := range data {
		dataStr += fmt.Sprintf("%s=%v ", k, v)
	}

	if duration > 10*time.Millisecond {
		tr.logger.Infof("⚠️  SLOW: %s took %v | %s", tr.name, duration, dataStr)
	} else if duration > 1*time.Millisecond {
		tr.logger.Infof("📊 %s took %v | %s", tr.name, duration, dataStr)
	} else {
		tr.logger.Debugf("✓ %s took %v | %s", tr.name, duration, dataStr)
	}
}

// LogMilestone logs a timing checkpoint within a region
func (tr *TraceRegion) LogMilestone(milestone string) {
	elapsed := time.Since(tr.startTime)
	tr.logger.Debugf("  ├─ %s at +%v", milestone, elapsed)
}

// IsEnabled returns whether tracing is enabled
func IsTraceEnabled() bool {
	return globalTraceHelper != nil && globalTraceHelper.enabled
}
