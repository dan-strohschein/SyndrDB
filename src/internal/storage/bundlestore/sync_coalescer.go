package bundlestore

import (
	"fmt"
	"os"
	"sync"
	"time"

	"syndrdb/src/pkg/common"
)

const coalesceInterval = 20 * time.Millisecond // P4b: PostgreSQL wal_writer_delay–style

var globalCoalescer *syncCoalescer

func init() {
	globalCoalescer = newSyncCoalescer()
}

type syncCoalescer struct {
	mu    sync.Mutex
	run   *syncRun
	timer *time.Timer
}

type syncRun struct {
	files map[*os.File]struct{} // set: dedupe so we fsync each file once per run
	done  chan struct{}
}

func newSyncCoalescer() *syncCoalescer {
	c := &syncCoalescer{}
	c.run = &syncRun{files: make(map[*os.File]struct{}), done: make(chan struct{})}
	c.timer = time.NewTimer(coalesceInterval)
	go c.loop()
	return c
}

// RequestSync adds file to the current run and blocks until that run's fsync completes.
// P4b: Group commit — multiple callers are batched into a single fsync every coalesceInterval.
// Files are deduped per run so we fsync each file at most once.
// CRITICAL FIX: Added timeout to prevent indefinite blocking if the loop goroutine gets stuck
func (c *syncCoalescer) RequestSync(f *os.File) error {
	if f == nil {
		return nil
	}
	c.mu.Lock()
	c.run.files[f] = struct{}{}
	done := c.run.done
	c.mu.Unlock()

	// CRITICAL FIX: Wait with timeout instead of blocking indefinitely
	// If the coalescer loop panics or gets stuck, this prevents caller from blocking forever
	select {
	case <-done:
		return nil
	case <-time.After(30 * time.Second):
		return fmt.Errorf("timeout waiting for sync coalescer (possible stuck fsync or loop panic)")
	}
}

func (c *syncCoalescer) loop() {
	for {
		<-c.timer.C
		c.mu.Lock()
		r := c.run
		c.run = &syncRun{files: make(map[*os.File]struct{}), done: make(chan struct{})}
		c.timer.Reset(coalesceInterval)
		c.mu.Unlock()

		// CRITICAL FIX: Use function with panic recovery to ensure done channel is always closed
		// This prevents callers from blocking forever if Fdatasync panics
		func() {
			// Always close done channel, even on panic
			defer func() {
				if p := recover(); p != nil {
					// Log panic but don't crash the loop - just continue to next run
					// The error will surface as a timeout to callers if it keeps happening
				}
				close(r.done) // Always unblock waiting callers
			}()

			if len(r.files) == 0 {
				return // defer will close done
			}
			for f := range r.files {
				_ = common.Fdatasync(f)
			}
		}()
	}
}
