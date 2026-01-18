package handler

import (
	"fmt"
	"io"
	"jellyfish/internal/aof"
	"jellyfish/internal/resp"
	"jellyfish/internal/store"
	"math"
	"net"
	"sort"
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

	for i, cmdValue := range h.txQueue {
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

	case "TSET":
		// TSET key v1 v2 v3 ...
		if len(args) < 2 {
			return resp.Value{Type: "error", Str: "ERR wrong number of arguments for 'tset' command"}
		}
		key := args[0].Bulk
		vec := make([]float32, 0, len(args)-1)
		for _, arg := range args[1:] {
			val, err := strconv.ParseFloat(arg.Bulk, 32)
			if err != nil {
				return resp.Value{Type: "error", Str: "ERR invalid float value"}
			}
			vec = append(vec, float32(val))
		}

		if h.aof != nil {
			h.aof.Write(value)
		}

		h.store.SetVectorWithoutLock(key, vec)
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

	case "TGET":
		if len(args) != 1 {
			return resp.Value{Type: "error", Str: "ERR wrong number of arguments for 'tget' command"}
		}
		vec, ok := h.store.GetVectorWithoutLock(args[0].Bulk)
		if !ok {
			return resp.Value{Type: "null"}
		}

		// Convert []float32 to []resp.Value
		vals := make([]resp.Value, len(vec))
		for i, v := range vec {
			vals[i] = resp.Value{Type: "bulk", Bulk: fmt.Sprintf("%g", v)}
		}
		return resp.Value{Type: "array", Array: vals}

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

	case "TSET":
		// TSET key v1 v2 v3 ...
		if len(args) < 2 {
			if w != nil {
				w.Write(resp.Value{Type: "error", Str: "ERR wrong number of arguments for 'tset' command"})
			}
			return
		}
		key := args[0].Bulk
		vec := make([]float32, 0, len(args)-1)
		for _, arg := range args[1:] {
			val, err := strconv.ParseFloat(arg.Bulk, 32)
			if err != nil {
				if w != nil {
					w.Write(resp.Value{Type: "error", Str: "ERR invalid float value"})
				}
				return
			}
			vec = append(vec, float32(val))
		}

		if h.aof != nil {
			h.aof.Write(value)
		}

		h.store.SetVector(key, vec)
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

	case "TGET":
		if len(args) != 1 {
			if w != nil {
				w.Write(resp.Value{Type: "error", Str: "ERR wrong number of arguments for 'tget' command"})
			}
			return
		}
		vec, ok := h.store.GetVector(args[0].Bulk)
		if w != nil {
			if !ok {
				w.Write(resp.Value{Type: "null"})
			} else {
				vals := make([]resp.Value, len(vec))
				for i, v := range vec {
					vals[i] = resp.Value{Type: "bulk", Bulk: fmt.Sprintf("%g", v)}
				}
				w.Write(resp.Value{Type: "array", Array: vals})
			}
		}

	case "VSEARCH":
		// VSEARCH q1 q2 ... k
		if len(args) < 2 {
			if w != nil {
				w.Write(resp.Value{Type: "error", Str: "ERR wrong number of arguments for 'vsearch' command"})
			}
			return
		}

		// Last argument is K
		kStr := args[len(args)-1].Bulk
		k, err := strconv.Atoi(kStr)
		if err != nil {
			if w != nil {
				w.Write(resp.Value{Type: "error", Str: "ERR invalid K value"})
			}
			return
		}

		// Parse query vector
		queryVec := make([]float32, 0, len(args)-2)
		for _, arg := range args[:len(args)-1] {
			val, err := strconv.ParseFloat(arg.Bulk, 32)
			if err != nil {
				if w != nil {
					w.Write(resp.Value{Type: "error", Str: "ERR invalid float value"})
				}
				return
			}
			queryVec = append(queryVec, float32(val))
		}

		// Perform linear search
		// Note: GetAllVectors() locks RLock inside
		candidates := h.store.GetAllVectors()

		type result struct {
			key   string
			score float64
		}
		results := make([]result, 0, len(candidates))

		for key, vec := range candidates {
			if len(vec) != len(queryVec) {
				continue // Skip dimension mismatch
			}
			dist := cosineDistance(queryVec, vec)
			results = append(results, result{key: key, score: dist})
		}

		// Sort by distance (ascending)
		sort.Slice(results, func(i, j int) bool {
			return results[i].score < results[j].score
		})

		// Return top K keys
		if k > len(results) {
			k = len(results)
		}

		respArr := make([]resp.Value, k)
		for i := 0; i < k; i++ {
			respArr[i] = resp.Value{Type: "bulk", Bulk: results[i].key}
		}

		if w != nil {
			w.Write(resp.Value{Type: "array", Array: respArr})
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

// cosineDistance calculates 1 - CosineSimilarity. Lower is closer.
func cosineDistance(a, b []float32) float64 {
	var dot, magA, magB float64
	for i := 0; i < len(a); i++ {
		dot += float64(a[i]) * float64(b[i])
		magA += float64(a[i]) * float64(a[i])
		magB += float64(b[i]) * float64(b[i])
	}
	if magA == 0 || magB == 0 {
		return 1.0 // Maximum distance if zero vector
	}
	similarity := dot / (math.Sqrt(magA) * math.Sqrt(magB))
	return 1.0 - similarity
}
