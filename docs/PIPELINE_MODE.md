# SyndrDB Pipeline Mode

Pipeline mode allows clients to send multiple commands in a single network write and receive all responses in order, reducing round-trip latency for batch operations.

## Protocol Overview

SyndrDB's wire protocol uses `\x04` (ASCII EOT) as a command terminator. In pipeline mode, the client sends multiple commands concatenated with `\x04` separators in a single TCP write. The server processes them in order and sends each response before processing the next command.

## Enabling Pipeline Mode

Add `pipeline=true` to the connection string options (6th colon-separated field):

```
syndrdb://host:port:database:username:password:pipeline=true
```

If you also need compression:

```
syndrdb://host:port:database:username:password:compress=zstd&pipeline=true
```

## READY Sentinel

When pipeline mode is enabled, the server sends a `READY\n` line after each response. This allows the client to unambiguously determine where one response ends and the next begins, even when responses vary in format (plain JSON, ZSTD-compressed, streaming, or error).

### Response Framing

```
<response 1 - JSON line or ZSTD-compressed block>\n
READY\n
<response 2 - JSON line or ZSTD-compressed block>\n
READY\n
<response 3 - JSON line or ZSTD-compressed block>\n
READY\n
```

### Without Pipeline Mode

Without `pipeline=true`, the server does NOT send `READY` sentinels. Responses are still sent in order, but the client must know the expected response format to determine boundaries. This is the default and is backwards-compatible with existing clients.

## Sending Pipeline Commands

Concatenate commands with `\x04` terminators and send them in a single TCP write:

```
INSERT INTO users {"name":"Alice","age":30}\x04INSERT INTO users {"name":"Bob","age":25}\x04SELECT * FROM users;\x04
```

The server's reader goroutine splits on `\x04` and queues each command. The main processing loop drains all queued commands and processes them as a batch.

### Important Rules

1. **Ordering is guaranteed.** Responses are always in the same order as commands were sent.
2. **No request IDs needed.** The Nth response corresponds to the Nth command.
3. **Each command must be complete.** Partial commands (without `\x04` terminator) are buffered until the terminator arrives.

## Error Handling

If command N fails, the error response is sent as the Nth response. Subsequent commands (N+1, N+2, ...) continue processing normally. The pipeline does NOT implicitly abort on error.

**For atomicity**, wrap the pipeline in a transaction:

```
BEGIN TRANSACTION\x04INSERT INTO users {"name":"Alice"}\x04INSERT INTO users {"name":"Bob"}\x04COMMIT\x04
```

If any command within the transaction fails, the transaction auto-rolls back per SyndrDB's standard transaction semantics. Subsequent DML commands in the pipeline will fail because there is no active transaction.

**Connection-level errors** (session expired, server shutdown) abort the pipeline and close the connection.

## Response Formats

Each response in the pipeline can be one of:

### Standard JSON Response
```
{"ResultCount":5,"Result":[...],"ExecutionTimeMS":1.23}\n
READY\n
```

### ZSTD-Compressed Response (when `compress=zstd` is also set)
```
ZSTD:<compressed_length>\n
<compressed_bytes>\n
READY\n
```

### Error Response
```
{"Error":"error message","ErrorCode":"ERR_XXX"}\n
READY\n
```

### Success Message (e.g., for DDL commands)
```
{"Message":"Authentication successful - Session: abc123"}\n
READY\n
```

## Implementation Guide for Third-Party Clients

### Pseudocode: Sending Pipeline

```python
def send_pipeline(socket, commands):
    """Send multiple commands as a single pipeline batch."""
    payload = ""
    for cmd in commands:
        payload += cmd + "\x04"
    socket.send(payload.encode())
    return len(commands)
```

### Pseudocode: Receiving Pipeline Responses

```python
def receive_pipeline_responses(reader, count, pipeline_mode=True):
    """Read exactly `count` responses from the server."""
    responses = []
    for i in range(count):
        if pipeline_mode:
            # Read lines until READY sentinel
            response_lines = []
            while True:
                line = reader.readline()
                if line.strip() == "READY":
                    break

                # Handle ZSTD-compressed response
                if line.startswith("ZSTD:"):
                    comp_len = int(line[5:].strip())
                    compressed = reader.read(comp_len)
                    reader.read(1)  # trailing \n
                    decompressed = zstd_decompress(compressed)
                    response_lines.append(decompressed)
                    continue

                response_lines.append(line)

            responses.append("".join(response_lines).strip())
        else:
            # Non-pipeline: each response is a single line
            line = reader.readline()
            if line.startswith("ZSTD:"):
                # Handle ZSTD same as above
                ...
            responses.append(line.strip())

    return responses
```

### Go Client Example

```go
client := internal.NewClient(host, port, db, user, pass)
client.PipelineMode = true
client.Connect()

// Send 3 commands as a pipeline
n, err := client.SendPipelineCommands([]string{
    "INSERT INTO users {\"name\":\"Alice\",\"age\":30}",
    "INSERT INTO users {\"name\":\"Bob\",\"age\":25}",
    "SELECT * FROM users;",
})

// Read all 3 responses
responses, err := client.ReceivePipelineResponses(n)
for i, resp := range responses {
    fmt.Printf("Response %d: %s\n", i+1, resp)
}
```

## Performance Characteristics

- **Reduced round-trips**: N commands require 1 TCP write + N sequential reads instead of N write/read pairs.
- **Server-side batching**: The server drains all buffered commands from its internal channel before returning to the select loop, minimizing context switches.
- **No added latency for single commands**: Sending one command at a time works identically to non-pipeline mode. The batch-drain simply finds no additional commands in the channel.
- **Backwards compatible**: Existing clients that send one command at a time see no behavioral change. The READY sentinel is only sent when `pipeline=true` is in the connection options.

## SyndrDB Official Client Flags

The official SyndrDB CLI client supports:

```
--pipeline    Enable pipeline mode (READY sentinel framing for batch commands)
--compress    Enable zstd response compression
```

These can be combined with the connection string:

```bash
syndrdb-client --connection_string "syndrdb://localhost:1776:mydb:user:pass" --pipeline --compress
```
