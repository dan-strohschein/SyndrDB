# SyndrDB Performance Tracing Guide

## 🎯 Purpose
This guide explains how to capture and analyze execution traces to diagnose performance bottlenecks in ADD DOCUMENT operations (or any operation).

## 📊 What You'll See

The trace will show you:
- **Exact function call timing** - where every microsecond goes
- **Goroutine scheduling delays** - if threads are waiting for CPU time
- **Lock contention** - if operations are blocked waiting for locks
- **Syscalls and I/O blocking** - disk writes, network operations
- **GC pauses** - garbage collection impact
- **Memory allocations** - allocation hot paths

## 🚀 Quick Start

### Option 1: Manual Test (Most Detailed)

1. **Start server with tracing enabled:**
   ```bash
   ENABLE_TRACE=true ./bin/server
   ```

2. **Send your test command via client:**
   ```bash
   echo 'ADD DOCUMENT TO BUNDLE "users" WITH ({"name" = "Gloria Robertson"}, {"email" = "tiffany98@example.org"}, {"created_at" = "2024-07-02T20:38:15.975875"}, {"last_login" = "2025-12-21T20:38:07.447450"});' | ./bin/client
   ```

3. **Stop the server** (Ctrl+C or SIGTERM)

4. **Analyze the trace:**
   ```bash
   go tool trace trace_add_document_*.out
   ```

### Option 2: Automated Script

```bash
./trace_test.sh
```

This script will:
- Build the server
- Start it with tracing enabled
- Send the ADD DOCUMENT command
- Stop the server gracefully
- Tell you how to view the trace

## 📈 Analyzing the Trace

When you run `go tool trace trace_add_document_*.out`, a web browser will open with several views:

### 1. **View Trace** (Most Important for Your Use Case)
- Shows a timeline of all operations
- **Look for**:
  - Long horizontal spans = slow operations
  - Gaps between operations = waiting/blocking
  - Red/yellow regions = GC pauses

**What to find:**
- Zoom in on the ADD DOCUMENT operation
- Look for spans labeled:
  - `AddDocument.TOTAL` - overall time
  - `AddDocument.ParseCommand` - parsing overhead
  - `AddDocument.GetBundleByName` - bundle lookup
  - `AddDocument.WAL.ExecuteWithLogging` - WAL logging
  - `AddDocument.BundleService.AddDocumentToBundle` - actual insert

### 2. **Goroutine Analysis**
- Click on any goroutine to see:
  - **Execution time** (actual CPU work)
  - **Network wait** (blocked on I/O)
  - **Sync block** (waiting for locks)
  - **Blocking syscalls** (disk writes, etc.)
  - **GC assist** (helping garbage collector)

**What this reveals:**
- If "Sync block" is large → lock contention problem
- If "Network/Syscall" is large → I/O bottleneck
- If "GC assist" is large → too many allocations

### 3. **Network/Synchronization Blocking Profile**
- Shows time spent waiting for:
  - Mutexes (locks)
  - Channels
  - Network I/O
  - Syscalls

**This is KEY for finding your 70-100ms bottleneck!**

### 4. **Minimum Mutator Utilization** (MMU)
- Shows how much time goroutines spend doing useful work vs blocked
- Low MMU = lots of blocking (bad for performance)

## 🔍 Expected Findings for Your 70-100ms Issue

Based on the code analysis, you should see ONE of these:

### Scenario A: Lock Contention
- **Symptom**: Large "Sync block" time in goroutine analysis
- **Timeline**: Gaps between operations, goroutines waiting
- **Fix**: Reduce lock scope or switch to lock-free structures

### Scenario B: Index Flush Synchronization
- **Symptom**: `flushIndexUpdates` span is very long despite being "deferred"
- **Timeline**: Shows synchronous flush happening during insert
- **Fix**: Truly defer the flush (fix the batching logic)

### Scenario C: Hidden GetDocument Calls
- **Symptom**: Many `GetDocument` spans during validation
- **Timeline**: Multiple document lookups happening
- **Fix**: Cache or batch document existence checks

### Scenario D: Scheduler Delay
- **Symptom**: Goroutines ready to run but not scheduled for milliseconds
- **Timeline**: Gaps where nothing is executing (scheduler overhead)
- **Fix**: Reduce goroutine creation or use worker pools

### Scenario E: Syscall Blocking
- **Symptom**: Large "Syscall" time despite fast fsync
- **Timeline**: Blocked on disk writes longer than expected
- **Fix**: Check if other syscalls (like file opens) are slow

## 📝 What the Logs Will Show

In addition to the trace file, the server logs will show timing like:

```
⚡ AddDocument.ParseCommand took 234µs (fast)
✓ AddDocument.GetBundleByName took 1.2ms
⚠️  SLOW: AddDocument.BundleService.AddDocumentToBundle took 67ms (>10ms)
   ├─ processNullValues at +123µs
   ├─ validateDocumentFields at +456µs
   ├─ ValidateUniqueConstraints at +65ms  ← FOUND IT!
📊 AddDocument.TOTAL took 70ms
```

The ⚠️  markers highlight anything >10ms for quick identification.

## 🎯 Next Steps After Finding the Bottleneck

Once you identify the slow operation:

1. **Add more granular tracing** to that specific function
2. **Profile just that function** with `pprof`
3. **Implement the fix** (we already have solutions ready)
4. **Re-run the trace** to verify improvement

## 💡 Pro Tips

- **Run multiple times**: First run might include cold start costs
- **Compare with fast operation**: Trace a SELECT to see baseline performance
- **Zoom in**: The timeline can zoom down to microsecond resolution
- **Check all goroutines**: Sometimes slow operation is in a background goroutine
- **Look for repeated patterns**: Same operation happening multiple times unexpectedly

## 🐛 Troubleshooting

**"Trace file is huge"**:
- Trace only captures while server is running
- Stop server quickly after test
- Trace files can be 10-50MB for short captures (this is normal)

**"Browser won't open"**:
```bash
go tool trace -http=:8080 trace_add_document_*.out
```
Then manually open http://localhost:8080

**"No spans showing up"**:
- Make sure ENABLE_TRACE=true was set
- Check that server actually processed the ADD DOCUMENT command
- Look in server logs for "Execution tracing ENABLED" message

## 📚 References

- [Go Execution Tracer Official Docs](https://go.dev/blog/execution-tracing)
- [Profiling Go Programs](https://go.dev/blog/pprof)
- [Understanding Go Runtime Metrics](https://go.dev/doc/diagnostics)
