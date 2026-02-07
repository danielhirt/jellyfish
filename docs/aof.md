# Append-Only File (AOF)

This document describes Jellyfish AOF behavior.

## Overview

- Write operations are logged to `database.aof` as RESP commands.
- The file is opened in append mode, so writes always go to the end of the file.
- On startup, the server replays the AOF to restore state.

## Write Semantics

- When a command requires persistence, the server attempts to write it to the AOF.
- If the AOF write fails, the command returns an error (`ERR AOF write failed`).
- The server does not currently expose a fsync policy; writes rely on the OS buffer.

## Replay

- The AOF reader scans the file from the beginning and replays commands in order.
- Replay uses the same command execution path as normal operation, without double-logging.
