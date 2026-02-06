# Jellyfish

```
          .-""-.
        /       \
       |  O   O  |
        \  ___  /
    .-"""-'   '-"""-.
   /  /\    ^    /\  \
  |  | .\  /_\  /. |  |
   \  \/  /   \  \/  /
    '-.__/     \__.-'
      |  \     /  |
      |   |   |   |
       \  |   |  /
        '-'   '-'
```

Jellyfish is an in-memory key-value store that speaks the Redis protocol. It supports basic string operations, TTLs, ACID transactions, and vector storage with cosine similarity search.

Everything is built from scratch in Go with zero external dependencies.

## Getting started

Build and run:

```bash
go build -o jellyfish .
./jellyfish
```

Or just:

```bash
go run .
```

The server starts on port `6379`. You can talk to it with `redis-cli` or any Redis client:

```bash
redis-cli
```

## Commands

**Key-value basics:**

```
SET mykey hello
GET mykey          # "hello"
DEL mykey
EXPIRE mykey 60    # expire in 60 seconds
TTL mykey          # seconds remaining (-1 = no expiry, -2 = doesn't exist)
```

**Transactions:**

```
MULTI              # start a transaction
SET a 1
SET b 2
EXEC               # execute atomically
```

Use `DISCARD` to cancel a transaction.

**Vector storage and search:**

```
TSET vec1 0.1 0.2 0.3
TSET vec2 0.4 0.5 0.6
TGET vec1                # [0.1, 0.2, 0.3]
VSEARCH 0.1 0.2 0.3 2   # find 2 nearest vectors by cosine distance
```

**Misc:**

```
PING               # PONG
ECHO hello         # hello
```

## Persistence

Write operations are logged to an append-only file (`database.aof`). When the server restarts, it replays the log to restore state.

## Running tests

```bash
go test ./... -v
```
