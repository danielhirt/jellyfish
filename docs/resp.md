# RESP Support

This document describes the RESP subset supported by Jellyfish.

## Requests

Jellyfish expects requests in RESP array form where each element is a bulk string.

Example:

```
*2
$4
ECHO
$5
hello

```

Null bulk values (`$-1`) are accepted and parsed as a null value.

## Responses

Responses can be any of the following RESP types:

- Simple string (`+OK`)
- Error (`-ERR ...`)
- Integer (`:1`)
- Bulk string (`$5\r\nhello\r\n`)
- Null bulk (`$-1`)
- Array (`*N ...`)

## Notes

- Inline commands are not supported.
- The current reader parses only arrays and bulk strings for requests.
