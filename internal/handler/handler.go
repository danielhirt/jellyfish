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
	store *store.Store
	aof   *aof.Aof
}

const aofWriteError = "ERR AOF write failed"

func New(s *store.Store, aof *aof.Aof) *Handler {
	return &Handler{
		store: s,
		aof:   aof,
	}
}

type session struct {
	inTx    bool
	txQueue []resp.Value
}

func (h *Handler) Handle(conn net.Conn) {
	defer conn.Close()

	r := resp.NewReader(conn)
	w := resp.NewWriter(conn)
	sess := &session{
		inTx:    false,
		txQueue: make([]resp.Value, 0),
	}

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

		h.handleCommand(value, w, sess)
	}
}

func (h *Handler) handleCommand(value resp.Value, w *resp.Writer, sess *session) {
	command := strings.ToUpper(value.Array[0].Bulk)

	// Handle Transaction Control Commands
	if command == "MULTI" {
		if sess.inTx {
			w.Write(resp.Value{Type: "error", Str: "ERR MULTI calls can not be nested"})
			return
		}
		sess.inTx = true
		sess.txQueue = make([]resp.Value, 0)
		w.Write(resp.Value{Type: "string", Str: "OK"})
		return
	}

	if command == "DISCARD" {
		if !sess.inTx {
			w.Write(resp.Value{Type: "error", Str: "ERR DISCARD without MULTI"})
			return
		}
		sess.inTx = false
		sess.txQueue = nil
		w.Write(resp.Value{Type: "string", Str: "OK"})
		return
	}

	if command == "EXEC" {
		if !sess.inTx {
			w.Write(resp.Value{Type: "error", Str: "ERR EXEC without MULTI"})
			return
		}

		h.execTx(w, sess)
		return
	}

	// Queue commands if in transaction
	if sess.inTx {
		sess.txQueue = append(sess.txQueue, value)
		w.Write(resp.Value{Type: "string", Str: "QUEUED"})
		return
	}

	// Normal execution
	h.Execute(value, w)
}

func (h *Handler) execTx(w *resp.Writer, sess *session) {
	// Atomically execute all commands
	h.store.Lock()
	defer h.store.Unlock()

	responses := make([]resp.Value, len(sess.txQueue))

	for i, cmdValue := range sess.txQueue {
		responses[i] = h.executeWithoutLock(cmdValue)
	}

	// Clear transaction state
	sess.inTx = false
	sess.txQueue = nil

	// Write array response
	w.Write(resp.Value{Type: "array", Array: responses})
}

func (h *Handler) writeAOF(value resp.Value) error {
	if h.aof == nil {
		return nil
	}
	return h.aof.Write(value)
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

		if err := h.writeAOF(value); err != nil {
			return resp.Value{Type: "error", Str: aofWriteError}
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

		if err := h.writeAOF(value); err != nil {
			return resp.Value{Type: "error", Str: aofWriteError}
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
		if err := h.writeAOF(value); err != nil {
			return resp.Value{Type: "error", Str: aofWriteError}
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
		if err := h.writeAOF(value); err != nil {
			return resp.Value{Type: "error", Str: aofWriteError}
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

	case "HSET":
		if len(args) < 3 || len(args)%2 == 0 {
			return resp.Value{Type: "error", Str: "ERR wrong number of arguments for 'hset' command"}
		}
		key := args[0].Bulk
		fields := make(map[string]string, (len(args)-1)/2)
		for i := 1; i < len(args); i += 2 {
			fields[args[i].Bulk] = args[i+1].Bulk
		}
		added := h.store.HSetWithoutLock(key, fields)
		if added == -1 {
			return resp.Value{Type: "error", Str: "WRONGTYPE Operation against a key holding the wrong kind of value"}
		}
		if err := h.writeAOF(value); err != nil {
			return resp.Value{Type: "error", Str: aofWriteError}
		}
		return resp.Value{Type: "integer", Num: added}

	case "HGET":
		if len(args) != 2 {
			return resp.Value{Type: "error", Str: "ERR wrong number of arguments for 'hget' command"}
		}
		val, found, typeOk := h.store.HGetWithoutLock(args[0].Bulk, args[1].Bulk)
		if !typeOk {
			return resp.Value{Type: "error", Str: "WRONGTYPE Operation against a key holding the wrong kind of value"}
		}
		if !found {
			return resp.Value{Type: "null"}
		}
		return resp.Value{Type: "bulk", Bulk: val}

	case "HDEL":
		if len(args) < 2 {
			return resp.Value{Type: "error", Str: "ERR wrong number of arguments for 'hdel' command"}
		}
		key := args[0].Bulk
		fields := make([]string, len(args)-1)
		for i, a := range args[1:] {
			fields[i] = a.Bulk
		}
		removed := h.store.HDelWithoutLock(key, fields)
		if removed == -1 {
			return resp.Value{Type: "error", Str: "WRONGTYPE Operation against a key holding the wrong kind of value"}
		}
		if removed > 0 {
			if err := h.writeAOF(value); err != nil {
				return resp.Value{Type: "error", Str: aofWriteError}
			}
		}
		return resp.Value{Type: "integer", Num: removed}

	case "HGETALL":
		if len(args) != 1 {
			return resp.Value{Type: "error", Str: "ERR wrong number of arguments for 'hgetall' command"}
		}
		m, typeOk := h.store.HGetAllWithoutLock(args[0].Bulk)
		if !typeOk {
			return resp.Value{Type: "error", Str: "WRONGTYPE Operation against a key holding the wrong kind of value"}
		}
		if m == nil {
			return resp.Value{Type: "array", Array: []resp.Value{}}
		}
		arr := make([]resp.Value, 0, len(m)*2)
		for k, v := range m {
			arr = append(arr, resp.Value{Type: "bulk", Bulk: k}, resp.Value{Type: "bulk", Bulk: v})
		}
		return resp.Value{Type: "array", Array: arr}

	case "HEXISTS":
		if len(args) != 2 {
			return resp.Value{Type: "error", Str: "ERR wrong number of arguments for 'hexists' command"}
		}
		exists, typeOk := h.store.HExistsWithoutLock(args[0].Bulk, args[1].Bulk)
		if !typeOk {
			return resp.Value{Type: "error", Str: "WRONGTYPE Operation against a key holding the wrong kind of value"}
		}
		if exists {
			return resp.Value{Type: "integer", Num: 1}
		}
		return resp.Value{Type: "integer", Num: 0}

	case "HLEN":
		if len(args) != 1 {
			return resp.Value{Type: "error", Str: "ERR wrong number of arguments for 'hlen' command"}
		}
		length := h.store.HLenWithoutLock(args[0].Bulk)
		if length == -1 {
			return resp.Value{Type: "error", Str: "WRONGTYPE Operation against a key holding the wrong kind of value"}
		}
		return resp.Value{Type: "integer", Num: length}

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

		if err := h.writeAOF(value); err != nil {
			if w != nil {
				w.Write(resp.Value{Type: "error", Str: aofWriteError})
			}
			return
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

		if err := h.writeAOF(value); err != nil {
			if w != nil {
				w.Write(resp.Value{Type: "error", Str: aofWriteError})
			}
			return
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

		if err := h.writeAOF(value); err != nil {
			if w != nil {
				w.Write(resp.Value{Type: "error", Str: aofWriteError})
			}
			return
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

		if err := h.writeAOF(value); err != nil {
			if w != nil {
				w.Write(resp.Value{Type: "error", Str: aofWriteError})
			}
			return
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

	case "HSET":
		if len(args) < 3 || len(args)%2 == 0 {
			if w != nil {
				w.Write(resp.Value{Type: "error", Str: "ERR wrong number of arguments for 'hset' command"})
			}
			return
		}
		key := args[0].Bulk
		fields := make(map[string]string, (len(args)-1)/2)
		for i := 1; i < len(args); i += 2 {
			fields[args[i].Bulk] = args[i+1].Bulk
		}
		added := h.store.HSet(key, fields)
		if added == -1 {
			if w != nil {
				w.Write(resp.Value{Type: "error", Str: "WRONGTYPE Operation against a key holding the wrong kind of value"})
			}
			return
		}
		if err := h.writeAOF(value); err != nil {
			if w != nil {
				w.Write(resp.Value{Type: "error", Str: aofWriteError})
			}
			return
		}
		if w != nil {
			w.Write(resp.Value{Type: "integer", Num: added})
		}

	case "HGET":
		if len(args) != 2 {
			if w != nil {
				w.Write(resp.Value{Type: "error", Str: "ERR wrong number of arguments for 'hget' command"})
			}
			return
		}
		val, found, typeOk := h.store.HGet(args[0].Bulk, args[1].Bulk)
		if w != nil {
			if !typeOk {
				w.Write(resp.Value{Type: "error", Str: "WRONGTYPE Operation against a key holding the wrong kind of value"})
			} else if !found {
				w.Write(resp.Value{Type: "null"})
			} else {
				w.Write(resp.Value{Type: "bulk", Bulk: val})
			}
		}

	case "HDEL":
		if len(args) < 2 {
			if w != nil {
				w.Write(resp.Value{Type: "error", Str: "ERR wrong number of arguments for 'hdel' command"})
			}
			return
		}
		key := args[0].Bulk
		fields := make([]string, len(args)-1)
		for i, a := range args[1:] {
			fields[i] = a.Bulk
		}
		removed := h.store.HDel(key, fields)
		if removed == -1 {
			if w != nil {
				w.Write(resp.Value{Type: "error", Str: "WRONGTYPE Operation against a key holding the wrong kind of value"})
			}
			return
		}
		if removed > 0 {
			if err := h.writeAOF(value); err != nil {
				if w != nil {
					w.Write(resp.Value{Type: "error", Str: aofWriteError})
				}
				return
			}
		}
		if w != nil {
			w.Write(resp.Value{Type: "integer", Num: removed})
		}

	case "HGETALL":
		if len(args) != 1 {
			if w != nil {
				w.Write(resp.Value{Type: "error", Str: "ERR wrong number of arguments for 'hgetall' command"})
			}
			return
		}
		m, typeOk := h.store.HGetAll(args[0].Bulk)
		if w != nil {
			if !typeOk {
				w.Write(resp.Value{Type: "error", Str: "WRONGTYPE Operation against a key holding the wrong kind of value"})
			} else if m == nil {
				w.Write(resp.Value{Type: "array", Array: []resp.Value{}})
			} else {
				arr := make([]resp.Value, 0, len(m)*2)
				for k, v := range m {
					arr = append(arr, resp.Value{Type: "bulk", Bulk: k}, resp.Value{Type: "bulk", Bulk: v})
				}
				w.Write(resp.Value{Type: "array", Array: arr})
			}
		}

	case "HEXISTS":
		if len(args) != 2 {
			if w != nil {
				w.Write(resp.Value{Type: "error", Str: "ERR wrong number of arguments for 'hexists' command"})
			}
			return
		}
		exists, typeOk := h.store.HExists(args[0].Bulk, args[1].Bulk)
		if w != nil {
			if !typeOk {
				w.Write(resp.Value{Type: "error", Str: "WRONGTYPE Operation against a key holding the wrong kind of value"})
			} else if exists {
				w.Write(resp.Value{Type: "integer", Num: 1})
			} else {
				w.Write(resp.Value{Type: "integer", Num: 0})
			}
		}

	case "HLEN":
		if len(args) != 1 {
			if w != nil {
				w.Write(resp.Value{Type: "error", Str: "ERR wrong number of arguments for 'hlen' command"})
			}
			return
		}
		length := h.store.HLen(args[0].Bulk)
		if w != nil {
			if length == -1 {
				w.Write(resp.Value{Type: "error", Str: "WRONGTYPE Operation against a key holding the wrong kind of value"})
			} else {
				w.Write(resp.Value{Type: "integer", Num: length})
			}
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
	for i := range a {
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
