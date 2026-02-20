# Session Management & Live Monitoring

## Connecting & Disconnecting

SyndrDB uses a TCP-based wire protocol. Clients connect by opening a TCP socket and sending a connection string:

```
syndrdb://host:port:database:username:password[:options]
```

**Options** (colon-separated, `key=value` pairs joined with `&`):
- `compress=zstd` — enable zstd response compression
- `pipeline=true` — enable pipeline mode (READY sentinel framing)
- `streaming=chunked` — enable chunked streaming protocol (STREAM:v1)

### Session Lifecycle

1. **TCP connect** — client opens a socket to the server
2. **Authentication** — client sends the connection string; server validates credentials and creates a `Session`
3. **Command loop** — client sends commands terminated by `\x04` (EOT); server processes and responds
4. **Disconnect** — client closes the socket or sends `exit;`/`quit;`; server cleans up the session

Sessions expire after a configurable idle timeout (default: 5 minutes of inactivity). Active monitors keep the session alive.

## How the Connection Works

### Command/Response Cycle

Each command is terminated by `\x04`. The server processes the command and sends a JSON response terminated by `\n`. For multi-statement batches, commands are concatenated with `\x04` separators.

### Pipeline Mode

When `pipeline=true` is negotiated, the server sends a `READY\n` sentinel after each response. This lets clients unambiguously determine response boundaries in batch streams.

### Streaming Mode

When `streaming=chunked` is negotiated, large SELECT results use the `STREAM:v1` chunked protocol:
```
STREAM:v1\n
CHUNK:<len>\n<data>
CHUNK:<len>\n<data>
END:<count>,<timeMS>\n
```

## MONITOR SESSIONS

The `MONITOR SESSIONS` command provides a live, continuously-updating view of all active sessions. It is the database equivalent of `top` or `htop`.

### Syntax

```sql
MONITOR SESSIONS [INTERVAL <ms>]
```

- **INTERVAL**: Snapshot frequency in milliseconds (default: 1000ms)
  - Minimum: 100ms (server-enforced)
  - Maximum: 60000ms (server-enforced)

### Example

```sql
MONITOR SESSIONS INTERVAL 500;
```

### Stopping

```sql
STOP MONITOR;
```

The monitor also stops automatically if the client disconnects or the connection times out.

## MONITOR SESSION

Monitor a single session in detail, with additional fields like connection ID, expiration, and error count.

### Syntax

```sql
MONITOR SESSION "<session_id>" [INTERVAL <ms>]
```

### Example

```sql
MONITOR SESSION "abc123" INTERVAL 2000;
```

## Wire Protocol: MONITOR:v1

The MONITOR command uses a dedicated wire protocol that extends the existing TCP framing:

```
Server sends:  MONITOR:v1\n
               {"type":"sessions","interval_ms":500,"fields":[...]}\n
               SNAPSHOT:<unix_ms>\n
               [{"session_id":"abc","username":"admin","state":"IDLE",...}]\n
               SNAPSHOT:<unix_ms>\n
               [{"session_id":"abc","username":"admin","state":"EXECUTING",...}]\n
               ...
               END:monitor_stopped\n
```

### Frame Types

| Frame | Format | Description |
|-------|--------|-------------|
| Header | `MONITOR:v1\n` | Protocol version identifier |
| Metadata | `{...}\n` | JSON object with monitor type, interval, and field list |
| Snapshot | `SNAPSHOT:<unix_ms>\n` followed by JSON data | Full session state at the given timestamp |
| End | `END:monitor_stopped\n` | Monitor terminated (via STOP MONITOR or disconnect) |

Each `SNAPSHOT` is a **full replacement** (not a delta). The JSON payload is an array of session objects for `MONITOR SESSIONS`, or a single object for `MONITOR SESSION`.

## Data Format

### MONITOR SESSIONS Fields

| Field | Type | Description |
|-------|------|-------------|
| `session_id` | string | Unique session identifier |
| `username` | string | Authenticated username |
| `database` | string | Current database name |
| `state` | string | Session state (IDLE, EXECUTING, etc.) |
| `client_ip` | string | Client IP address |
| `created_at` | string | Session creation time (RFC3339) |
| `last_activity` | string | Last activity time (RFC3339) |
| `current_query` | string | Currently executing query (if any) |
| `query_duration_ms` | int | Elapsed time of current query in ms |
| `transaction_id` | string | Active transaction ID (if any) |
| `last_completed_query` | object | Most recently completed query (see below) |

### MONITOR SESSION Additional Fields

| Field | Type | Description |
|-------|------|-------------|
| `connection_id` | string | TCP connection identifier |
| `expires_at` | string | Session expiration time (RFC3339) |
| `error_count` | int | Total errors in this session |
| `query_history_len` | int | Number of queries in history |
| `current_query_status` | string | Status of current query |
| `transaction_status` | string | Transaction state (if in transaction) |
| `last_error` | string | Most recent error message |
| `last_completed_query` | object | Most recently completed query (see below) |
| `query_history` | array | Last 10 completed queries (most recent last) |

### Query Info Object

Both `last_completed_query` and each entry in `query_history` share the same shape:

| Field | Type | Description |
|-------|------|-------------|
| `query` | string | SQL text |
| `status` | string | `"COMPLETED"` or `"FAILED"` |
| `start_time` | string | Query start time (RFC3339) |
| `end_time` | string | Query end time (RFC3339) |
| `duration_ms` | int | Execution time in milliseconds |
| `affected_rows` | int | Rows affected (omitted if 0) |
| `error` | string | Error message (only for failed queries) |

## Security & Visibility

### Permission Requirements

The MONITOR command requires **Read** permission (same as SHOW commands).

### Admin vs Non-Admin Visibility

| Caller | MONITOR SESSIONS | MONITOR SESSION "<id>" |
|--------|-----------------|----------------------|
| **Admin** | Sees all sessions | Sees any session |
| **Non-admin** | Sees sessions with same username | Sees session only if same username |
| **Auth disabled** | Sees all sessions (treated as admin) | Sees any session |

Non-admin users can see all of their own sessions across multiple connections (username-based filtering), which is useful for debugging connection pools and multi-session workflows.

### Configuration

Monitor behavior is controlled by server settings (configurable via YAML or CLI flags):

| Setting | Default | Description |
|---------|---------|-------------|
| `monitor_default_interval_ms` | 1000 | Default snapshot interval |
| `monitor_min_interval_ms` | 100 | Minimum allowed interval |
| `monitor_max_interval_ms` | 60000 | Maximum allowed interval |

### Concurrency

The monitor goroutine writes directly to the connection's `bufio.Writer`, protected by a `sync.Mutex` (`writeMu`) shared with the main command loop. This ensures monitor snapshots and command responses never interleave on the wire.
