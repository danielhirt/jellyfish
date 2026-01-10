package handler

import (
	"fmt"
	"io"
	"jellyfish/internal/resp"
	"jellyfish/internal/store"
	"net"
	"strconv"
	"strings"
)

type Handler struct {
	store *store.Store
}

func New(s *store.Store) *Handler {
	return &Handler{store: s}
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

		command := strings.ToUpper(value.Array[0].Bulk)
		args := value.Array[1:]

		h.execute(command, args, w)
	}
}

func (h *Handler) execute(cmd string, args []resp.Value, w *resp.Writer) {
	switch cmd {
	case "PING":
		w.Write(resp.Value{Type: "string", Str: "PONG"})

	case "ECHO":
		if len(args) > 0 {
			w.Write(resp.Value{Type: "bulk", Bulk: args[0].Bulk})
		} else {
			w.Write(resp.Value{Type: "error", Str: "ERR wrong number of arguments for 'echo' command"})
		}

	case "SET":
		if len(args) != 2 {
			w.Write(resp.Value{Type: "error", Str: "ERR wrong number of arguments for 'set' command"})
			return
		}
		key := args[0].Bulk
		val := args[1].Bulk
		h.store.Set(key, val)
		w.Write(resp.Value{Type: "string", Str: "OK"})

	case "GET":
		if len(args) != 1 {
			w.Write(resp.Value{Type: "error", Str: "ERR wrong number of arguments for 'get' command"})
			return
		}
		key := args[0].Bulk
		val, ok := h.store.Get(key)
		if !ok {
			w.Write(resp.Value{Type: "null"})
		} else {
			w.Write(resp.Value{Type: "bulk", Bulk: val})
		}

	case "DEL":
		if len(args) != 1 {
			w.Write(resp.Value{Type: "error", Str: "ERR wrong number of arguments for 'del' command"})
			return
		}
		key := args[0].Bulk
		deleted := h.store.Del(key)
		if deleted {
			w.Write(resp.Value{Type: "integer", Num: 1})
		} else {
			w.Write(resp.Value{Type: "integer", Num: 0})
		}

	case "EXPIRE":
		if len(args) != 2 {
			w.Write(resp.Value{Type: "error", Str: "ERR wrong number of arguments for 'expire' command"})
			return
		}
		key := args[0].Bulk
		seconds, err := strconv.Atoi(args[1].Bulk)
		if err != nil {
			w.Write(resp.Value{Type: "error", Str: "ERR value is not an integer or out of range"})
			return
		}

		ok := h.store.Expire(key, seconds)
		if ok {
			w.Write(resp.Value{Type: "integer", Num: 1})
		} else {
			w.Write(resp.Value{Type: "integer", Num: 0})
		}

	case "TTL":
		if len(args) != 1 {
			w.Write(resp.Value{Type: "error", Str: "ERR wrong number of arguments for 'ttl' command"})
			return
		}
		key := args[0].Bulk
		ttl := h.store.TTL(key)
		w.Write(resp.Value{Type: "integer", Num: ttl})

	default:
		w.Write(resp.Value{Type: "error", Str: fmt.Sprintf("ERR unknown command '%s'", cmd)})
	}
}
