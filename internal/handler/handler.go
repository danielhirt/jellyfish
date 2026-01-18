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
	store   *store.Store
	aof     *aof.Aof
	inTx    bool
	txQueue []resp.Value
}

func New(s *store.Store, aof *aof.Aof) *Handler {
	return &Handler{
		store:   s,
		aof:     aof,
		inTx:    false,
		txQueue: make([]resp.Value, 0),
	}
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

		h.handleCommand(value, w)
	}
}

func (h *Handler) handleCommand(value resp.Value, w *resp.Writer) {
	command := strings.ToUpper(value.Array[0].Bulk)

	// Handle Transaction Control Commands
	if command == "MULTI" {
		if h.inTx {
			w.Write(resp.Value{Type: "error", Str: "ERR MULTI calls can not be nested"})
			return
		}
		h.inTx = true
		h.txQueue = make([]resp.Value, 0)
		w.Write(resp.Value{Type: "string", Str: "OK"})
		return
	}

	if command == "DISCARD" {
		if !h.inTx {
			w.Write(resp.Value{Type: "error", Str: "ERR DISCARD without MULTI"})
			return
		}
		h.inTx = false
		h.txQueue = nil
		w.Write(resp.Value{Type: "string", Str: "OK"})
		return
	}

	if command == "EXEC" {
		if !h.inTx {
			w.Write(resp.Value{Type: "error", Str: "ERR EXEC without MULTI"})
			return
		}

		h.execTx(w)
		return
	}

	// Queue commands if in transaction
	if h.inTx {
		h.txQueue = append(h.txQueue, value)
		w.Write(resp.Value{Type: "string", Str: "QUEUED"})
		return
	}

	// Normal execution
	h.Execute(value, w)
}

func (h *Handler) execTx(w *resp.Writer) {
	// Atomically execute all commands
	h.store.Lock()
	defer h.store.Unlock()

	responses := make([]resp.Value, len(h.txQueue))

	// Temporarily disable AOF writing in the loop to batch it or handle it cleanly?
	// Redis logs the individual commands as they execute or the block.
	// For simplicity, we execute them and they will be logged if logic permits.
	// But Execute() currently does self-locking AND logging.
	// We need 'ExecuteWithoutLock' logic.

	for i, cmdValue := range h.txQueue {
		// We need to capture the output.
		// Since our Execute/ExecuteWithoutLock logic writes to a Writer,
		// we need a temporary buffer writer or refactor Execute.
		// Refactoring Execute to return (resp.Value, error) is cleaner than passing a Writer.
		// But for now, let's use a specialized method for the Tx loop.

		responses[i] = h.executeWithoutLock(cmdValue)
	}

	// Clear transaction state
	h.inTx = false
	h.txQueue = nil

	// Write array response
	w.Write(resp.Value{Type: "array", Array: responses})
}

// executeWithoutLock executes a command assuming the store is ALREADY locked.
// It returns the response value instead of writing it.
func (h *Handler) executeWithoutLock(value resp.Value) resp.Value {
	command := strings.ToUpper(value.Array[0].Bulk)
	args := value.Array[1:]

	switch command {
	case "PING":
		return resp.Value{Type: "string", Str: "PONG"}

	case "ECHO":
		if len(args) > 0 {
			return resp.Value{Type: "bulk", Bulk: args[0].Bulk}
		}
		return resp.Value{Type: "error", Str: "ERR wrong number of arguments for 'echo' command"}

	case "SET":
		if len(args) != 2 {
			return resp.Value{Type: "error", Str: "ERR wrong number of arguments for 'set' command"}
		}

		if h.aof != nil {
			h.aof.Write(value)
		}

		h.store.SetWithoutLock(args[0].Bulk, args[1].Bulk)
		return resp.Value{Type: "string", Str: "OK"}

	case "GET":
		if len(args) != 1 {
			return resp.Value{Type: "error", Str: "ERR wrong number of arguments for 'get' command"}
		}
		val, ok := h.store.GetWithoutLock(args[0].Bulk)
		if !ok {
			return resp.Value{Type: "null"}
		}
		return resp.Value{Type: "bulk", Bulk: val}

	case "DEL":
		if len(args) != 1 {
			return resp.Value{Type: "error", Str: "ERR wrong number of arguments for 'del' command"}
		}
		if h.aof != nil {
			h.aof.Write(value)
		}
		deleted := h.store.DelWithoutLock(args[0].Bulk)
		if deleted {
			return resp.Value{Type: "integer", Num: 1}
		}
		return resp.Value{Type: "integer", Num: 0}

	case "EXPIRE":
		if len(args) != 2 {
			return resp.Value{Type: "error", Str: "ERR wrong number of arguments for 'expire' command"}
		}
		seconds, err := strconv.Atoi(args[1].Bulk)
		if err != nil {
			return resp.Value{Type: "error", Str: "ERR value is not an integer or out of range"}
		}
		if h.aof != nil {
			h.aof.Write(value)
		}
		ok := h.store.ExpireWithoutLock(args[0].Bulk, seconds)
		if ok {
			return resp.Value{Type: "integer", Num: 1}
		}
		return resp.Value{Type: "integer", Num: 0}

	case "TTL":
		if len(args) != 1 {
			return resp.Value{Type: "error", Str: "ERR wrong number of arguments for 'ttl' command"}
		}
		ttl := h.store.TTLWithoutLock(args[0].Bulk)
		return resp.Value{Type: "integer", Num: ttl}

	default:
		return resp.Value{Type: "error", Str: fmt.Sprintf("ERR unknown command '%s'", command)}
	}
}

// Execute processes a RESP command in immediate mode (locks per command).
// If w is nil, no response is written.
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
