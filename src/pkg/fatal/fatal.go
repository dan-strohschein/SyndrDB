// Package fatal provides panic capture and logging to fatal_errors.log
// so that fatal errors are persisted before the process exits.
package fatal

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime/debug"
	"time"

	"syndrdb/src/pkg/settings"
)

const logFileName = "fatal_errors.log"

// LogFatal writes the panic value and full stack trace to fatal_errors.log,
// then returns. The caller should re-panic after calling this so the process
// still exits. Directory is resolved from settings: LogDir, TempDir, DataDir, then ".".
// Uses only os/fmt to avoid depending on log/zap which might not be safe during panic.
func LogFatal(panicVal interface{}) {
	stack := debug.Stack()
	writeFatalLog(panicVal, stack)
}

func writeFatalLog(panicVal interface{}, stack []byte) {
	dir := "."
	if s := settings.GetSettings(); s != nil {
		if s.LogDir != "" {
			dir = s.LogDir
		} else if s.TempDir != "" {
			dir = s.TempDir
		} else if s.DataDir != "" {
			dir = s.DataDir
		}
	}
	_ = os.MkdirAll(dir, 0755)
	path := filepath.Join(dir, logFileName)
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[fatal] could not open %s: %v\n", path, err)
		fmt.Fprintf(os.Stderr, "panic: %v\n%s", panicVal, stack)
		return
	}
	defer f.Close()
	ts := time.Now().Format("2006-01-02 15:04:05.000 -0700 MST")
	_, _ = fmt.Fprintf(f, "\n--- FATAL %s ---\npanic: %v\n\n%s\n", ts, panicVal, stack)
	_ = f.Sync()
}
