# Transaction Behavior (MULTI/EXEC)

This document describes Jellyfish transaction behavior as implemented in `internal/handler/handler.go`.

## Scope

- Applies to `MULTI`, `EXEC`, and `DISCARD`.
- Transactions are **per-connection** and are not shared across clients.
- Only commands received on a given connection are queued/executed for that connection.

## State Model

Each client connection maintains its own transaction state:

- `inTx` (bool): Whether the connection is inside a transaction.
- `txQueue` ([]RESP Value): The queued commands for the current transaction.

There is **no global transaction state**. Concurrent clients do not affect each other’s `inTx` or `txQueue`.

## Command Semantics

### MULTI
- If `inTx` is `false`, the server enters transaction mode for that connection.
- If `inTx` is already `true`, returns an error: `ERR MULTI calls can not be nested`.
- Response: `+OK` on success.

### Queued Commands
- While `inTx` is `true`, all non-control commands are queued.
- Response for each queued command: `+QUEUED`.
- Commands are not executed until `EXEC`.

### DISCARD
- If `inTx` is `false`, returns error: `ERR DISCARD without MULTI`.
- If `inTx` is `true`, clears the queue and exits transaction mode.
- Response: `+OK` on success.

### EXEC
- If `inTx` is `false`, returns error: `ERR EXEC without MULTI`.
- If `inTx` is `true`, executes all queued commands atomically:
  - The store is locked once for the entire execution.
  - Each queued command is executed in order.
  - Responses are returned as an array of per-command responses.
- Transaction state is cleared after execution.

## Atomicity and Isolation

- **Atomicity:** All queued commands are executed under a single store lock, so no other commands interleave during `EXEC`.
- **Isolation:** There is **no isolation** between `MULTI` and `EXEC`. Other clients can read/write between those calls.

## Error Handling

- Command parsing errors or invalid argument counts are surfaced at execution time when queued commands run.
- Errors are returned in the response array at the corresponding command position.

## Examples

Example sequence on a single connection:

```
MULTI
SET a 1
SET b 2
EXEC
```

Responses:

- `MULTI` => `+OK`
- `SET a 1` => `+QUEUED`
- `SET b 2` => `+QUEUED`
- `EXEC` => `*2\r\n+OK\r\n+OK\r\n`

Concurrent clients:

- Client A enters `MULTI` and queues commands.
- Client B is **not** affected and continues to execute commands normally.

## Related Tests

- `TestHandler_TransactionIsolation`
- `TestHandler_TransactionQueueOrder`
