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

// LogFatal writes the panic value and stack to fatal_errors.log.
// Directory is tried in order: LogDir, TempDir, DataDir, ".", os.TempDir().
// Returns the path written to, or "" on failure. Uses only os/fmt.
func LogFatal(panicVal interface{}, stack []byte) (path string) {
	dir := resolveLogDir()
	_ = os.MkdirAll(dir, 0755)
	path = filepath.Join(dir, logFileName)
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		// Fallback: try os.TempDir() if configured dirs failed
		if dir != os.TempDir() {
			path = filepath.Join(os.TempDir(), logFileName)
			f, err = os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
		}
		if err != nil {
			return ""
		}
	}
	defer f.Close()
	ts := time.Now().Format("2006-01-02 15:04:05.000 -0700 MST")
	_, _ = fmt.Fprintf(f, "\n--- FATAL %s ---\npanic: %v\n\n%s\n", ts, panicVal, stack)
	_ = f.Sync()
	return path
}

func resolveLogDir() string {
	var dir string
	func() {
		defer func() { _ = recover(); dir = "." }()
		s := settings.GetSettings()
		if s != nil {
			if s.LogDir != "" {
				dir = s.LogDir
				return
			}
			if s.TempDir != "" {
				dir = s.TempDir
				return
			}
			if s.DataDir != "" {
				dir = s.DataDir
				return
			}
		}
		dir = "."
	}()
	if dir == "" {
		dir = "."
	}
	return dir
}

// LogFatalAndExit writes the panic to fatal_errors.log, prints only the
// panicking goroutine's stack to stderr (so the runtime's full goroutine
// dump is never run), then exits with 1. Call this from a recover handler
// instead of re-panicking to avoid the noise from other goroutines.
func LogFatalAndExit(panicVal interface{}) {
	stack := debug.Stack()
	path := LogFatal(panicVal, stack)
	if path == "" {
		path = logFileName + " (write failed, check cwd or temp dir)"
	}
	fmt.Fprintf(os.Stderr, "Fatal panic: %v\nLogged to %s\n\n%s", panicVal, path, stack)
	os.Exit(1)
}
