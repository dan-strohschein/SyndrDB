# Configuration: Timeouts and Limits

This document describes the configurable timeouts and limits in SyndrDB. These settings allow you to tune the system for different workloads and environments.

## Overview

SyndrDB provides several configurable timeouts and limits to optimize performance and resource usage. All settings can be configured via the `syndrdb.yml` configuration file or command-line flags.

## Concurrency & Locking Configuration

### lock_timeout_seconds

**Default:** 30 seconds  
**YAML Key:** `lock_timeout_seconds`  
**CLI Flag:** `-lock-timeout`

Timeout for lock acquisition in seconds. Used by document-level locks during transactions.

- **When to increase:** Complex multi-statement transactions that may hold locks longer, or when processing large batches of documents.
- **When to decrease:** To fail fast on deadlocks or stuck operations, or in high-contention environments where quick timeout is preferred.

**Example:**
```yaml
# Allow 60 seconds for lock acquisition (useful for bulk operations)
lock_timeout_seconds: 60
```

### max_worker_pools

**Default:** 10  
**YAML Key:** `max_worker_pools`  
**CLI Flag:** `-max-worker-pools`

Maximum number of worker pools that can be created for async operations. Each worker pool handles a specific type of asynchronous work (e.g., WAL writes, index updates).

- **When to increase:** If you need many different async operation types running concurrently, or in high-throughput environments with diverse operation types.
- **When to decrease:** To limit resource usage on constrained systems, or when async operations are minimal.

**Example:**
```yaml
# Allow up to 20 worker pools for complex async workloads
max_worker_pools: 20
```

### worker_pool_stop_timeout_seconds

**Default:** 30 seconds  
**YAML Key:** `worker_pool_stop_timeout_seconds`  
**CLI Flag:** `-worker-pool-stop-timeout`

Timeout for stopping worker pools in seconds. When shutting down, worker pools have this much time to finish current tasks gracefully.

- **When to increase:** If operations take longer to complete during shutdown (e.g., large batch writes, complex index updates).
- **When to decrease:** To force faster shutdown (may interrupt operations), useful in environments where quick restarts are needed.

**Example:**
```yaml
# Allow 60 seconds for graceful shutdown (useful for large batch operations)
worker_pool_stop_timeout_seconds: 60
```

## Tuning Guidelines

### High-Throughput Systems

For systems with high write throughput:
```yaml
lock_timeout_seconds: 60          # Allow longer lock holds for complex operations
max_worker_pools: 20              # Support more concurrent async operations
worker_pool_stop_timeout_seconds: 60  # Graceful shutdown for large batches
```

### Low-Latency Systems

For systems prioritizing low latency:
```yaml
lock_timeout_seconds: 15          # Fail fast on deadlocks
max_worker_pools: 5               # Limit resource usage
worker_pool_stop_timeout_seconds: 15  # Quick shutdown
```

### Resource-Constrained Systems

For systems with limited resources:
```yaml
lock_timeout_seconds: 30          # Default
max_worker_pools: 5               # Reduce concurrent pools
worker_pool_stop_timeout_seconds: 20  # Faster cleanup
```

## Monitoring

Monitor the following metrics to tune these values:

- **Lock acquisition failures:** If `lock_timeout_seconds` is too low, you'll see frequent timeout errors.
- **Worker pool creation failures:** If `max_worker_pools` is too low, you'll see "maximum worker pools reached" errors.
- **Shutdown duration:** Monitor server shutdown time - if `worker_pool_stop_timeout_seconds` is too low, you may see incomplete operations.

## Related Configuration

These timeout settings work in conjunction with:

- **Query Timeouts:** `query_timeout_seconds` and `admin_query_timeout_seconds`
- **Session Timeouts:** `session_timeout_minutes`
- **Connection Timeouts:** `connection_idle_timeout`
- **WAL Configuration:** `wal_batch_size`, `wal_max_flush_delay`

See `syndrdb.example.yml` for all available configuration options.
