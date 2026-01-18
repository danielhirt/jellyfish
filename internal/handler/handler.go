package handler

import (
	"fmt"
	"io"
	"jellyfish/internal/aof"
	"jellyfish/internal/resp"
	"jellyfish/internal/store"
	"net"
	"strconv"
	"strings"
)

type Handler struct {
	store *store.Store
	aof   *aof.Aof
}

func New(s *store.Store, aof *aof.Aof) *Handler {
	return &Handler{store: s, aof: aof}
}

func (h *Handler) Handle(conn net.Conn) {
	defer conn.Close()

	r := resp.NewReader(conn)
	w := resp.NewWriter(conn)

	for {
		value, err := r.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			fmt.Println("error reading from client:", err)
			return
		}

		if value.Type != "array" || len(value.Array) == 0 {
			continue
		}

		// Pass the full value to execute for AOF logging
		h.Execute(value, w)
	}
}

// Execute processes a RESP command.
// If w is nil, no response is written (useful for AOF replay).
func (h *Handler) Execute(value resp.Value, w *resp.Writer) {
	command := strings.ToUpper(value.Array[0].Bulk)
	args := value.Array[1:]

	switch command {
	case "PING":
		if w != nil {
			w.Write(resp.Value{Type: "string", Str: "PONG"})
		}

	case "ECHO":
		if len(args) > 0 {
			if w != nil {
				w.Write(resp.Value{Type: "bulk", Bulk: args[0].Bulk})
			}
		} else {
			if w != nil {
				w.Write(resp.Value{Type: "error", Str: "ERR wrong number of arguments for 'echo' command"})
			}
		}

	case "SET":
		if len(args) != 2 {
			if w != nil {
				w.Write(resp.Value{Type: "error", Str: "ERR wrong number of arguments for 'set' command"})
			}
			return
		}

		if h.aof != nil {
			h.aof.Write(value)
		}

		key := args[0].Bulk
		val := args[1].Bulk
		h.store.Set(key, val)
		if w != nil {
			w.Write(resp.Value{Type: "string", Str: "OK"})
		}

	case "GET":
		if len(args) != 1 {
			if w != nil {
				w.Write(resp.Value{Type: "error", Str: "ERR wrong number of arguments for 'get' command"})
			}
			return
		}
		key := args[0].Bulk
		val, ok := h.store.Get(key)
		if w != nil {
			if !ok {
				w.Write(resp.Value{Type: "null"})
			} else {
				w.Write(resp.Value{Type: "bulk", Bulk: val})
			}
		}

	case "DEL":
		if len(args) != 1 {
			if w != nil {
				w.Write(resp.Value{Type: "error", Str: "ERR wrong number of arguments for 'del' command"})
			}
			return
		}

		if h.aof != nil {
			h.aof.Write(value)
		}

		key := args[0].Bulk
		deleted := h.store.Del(key)
		if w != nil {
			if deleted {
				w.Write(resp.Value{Type: "integer", Num: 1})
			} else {
				w.Write(resp.Value{Type: "integer", Num: 0})
			}
		}

	case "EXPIRE":
		if len(args) != 2 {
			if w != nil {
				w.Write(resp.Value{Type: "error", Str: "ERR wrong number of arguments for 'expire' command"})
			}
			return
		}

		key := args[0].Bulk
		// Validate integer before logging
		seconds, err := strconv.Atoi(args[1].Bulk)
		if err != nil {
			if w != nil {
				w.Write(resp.Value{Type: "error", Str: "ERR value is not an integer or out of range"})
			}
			return
		}

		if h.aof != nil {
			h.aof.Write(value)
		}

		ok := h.store.Expire(key, seconds)
		if w != nil {
			if ok {
				w.Write(resp.Value{Type: "integer", Num: 1})
			} else {
				w.Write(resp.Value{Type: "integer", Num: 0})
			}
		}

	case "TTL":
		if len(args) != 1 {
			if w != nil {
				w.Write(resp.Value{Type: "error", Str: "ERR wrong number of arguments for 'ttl' command"})
			}
			return
		}
		key := args[0].Bulk
		ttl := h.store.TTL(key)
		if w != nil {
			w.Write(resp.Value{Type: "integer", Num: ttl})
		}

	default:
		if w != nil {
			w.Write(resp.Value{Type: "error", Str: fmt.Sprintf("ERR unknown command '%s'", command)})
		}
	}
}
