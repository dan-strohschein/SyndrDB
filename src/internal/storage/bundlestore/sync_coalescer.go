package bundlestore

import (
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
func (c *syncCoalescer) RequestSync(f *os.File) error {
	if f == nil {
		return nil
	}
	c.mu.Lock()
	c.run.files[f] = struct{}{}
	done := c.run.done
	c.mu.Unlock()
	<-done
	return nil
}

func (c *syncCoalescer) loop() {
	for {
		<-c.timer.C
		c.mu.Lock()
		r := c.run
		c.run = &syncRun{files: make(map[*os.File]struct{}), done: make(chan struct{})}
		c.timer.Reset(coalesceInterval)
		c.mu.Unlock()
		if len(r.files) == 0 {
			close(r.done)
			continue
		}
		for f := range r.files {
			_ = common.Fdatasync(f)
		}
		close(r.done)
	}
}
