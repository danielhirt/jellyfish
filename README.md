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

Jellyfish is an in-memory key-value store that speaks the Redis protocol. It supports strings, hash maps, TTLs, transactions (MULTI/EXEC), and vector storage with cosine similarity search.

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
Transactions are per-connection and execute atomically at `EXEC`. There is no isolation across clients between `MULTI` and `EXEC`.

**Vector storage and search:**

```
TSET vec1 0.1 0.2 0.3
TSET vec2 0.4 0.5 0.6
TGET vec1                # [0.1, 0.2, 0.3]
VSEARCH 0.1 0.2 0.3 2   # find 2 nearest vectors by cosine distance
```

**Hash maps:**

```
HSET user name Alice age 30   # set fields (returns number of new fields added)
HGET user name                # "Alice"
HGETALL user                  # ["name", "Alice", "age", "30"]
HEXISTS user name             # 1
HLEN user                     # 2
HDEL user age                 # 1
```

**Misc:**

```
PING               # PONG
ECHO hello         # hello
```

## Protocol

Jellyfish expects RESP arrays of bulk strings for requests. Null bulk values (`$-1`) are accepted.

## Persistence

Write operations are logged to an append-only file (`database.aof`). When the server restarts, it replays the log to restore state.
If an AOF write fails, the command returns an error.

## Running tests

```bash
go test ./... -v
```
