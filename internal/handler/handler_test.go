package handler

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"testing"

	"jellyfish/internal/resp"
	"jellyfish/internal/store"
)

type respValue struct {
	Type  string
	Str   string
	Num   int
	Bulk  string
	Array []respValue
}

func readLine(r *bufio.Reader) (string, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return "", err
	}
	if len(line) < 2 || !strings.HasSuffix(line, "\r\n") {
		return "", fmt.Errorf("malformed line: %q", line)
	}
	return strings.TrimSuffix(line, "\r\n"), nil
}

func readRespValue(r *bufio.Reader) (respValue, error) {
	b, err := r.ReadByte()
	if err != nil {
		return respValue{}, err
	}

	switch b {
	case '+':
		line, err := readLine(r)
		if err != nil {
			return respValue{}, err
		}
		return respValue{Type: "string", Str: line}, nil
	case '-':
		line, err := readLine(r)
		if err != nil {
			return respValue{}, err
		}
		return respValue{Type: "error", Str: line}, nil
	case ':':
		line, err := readLine(r)
		if err != nil {
			return respValue{}, err
		}
		n, err := strconv.Atoi(line)
		if err != nil {
			return respValue{}, err
		}
		return respValue{Type: "integer", Num: n}, nil
	case '$':
		line, err := readLine(r)
		if err != nil {
			return respValue{}, err
		}
		n, err := strconv.Atoi(line)
		if err != nil {
			return respValue{}, err
		}
		if n == -1 {
			return respValue{Type: "null"}, nil
		}
		buf := make([]byte, n)
		if _, err := io.ReadFull(r, buf); err != nil {
			return respValue{}, err
		}
		if _, err := readLine(r); err != nil {
			return respValue{}, err
		}
		return respValue{Type: "bulk", Bulk: string(buf)}, nil
	case '*':
		line, err := readLine(r)
		if err != nil {
			return respValue{}, err
		}
		n, err := strconv.Atoi(line)
		if err != nil {
			return respValue{}, err
		}
		arr := make([]respValue, n)
		for i := 0; i < n; i++ {
			v, err := readRespValue(r)
			if err != nil {
				return respValue{}, err
			}
			arr[i] = v
		}
		return respValue{Type: "array", Array: arr}, nil
	default:
		return respValue{}, fmt.Errorf("unknown RESP type: %q", string(b))
	}
}

func writeCommand(w *resp.Writer, args ...string) error {
	arr := make([]resp.Value, len(args))
	for i, arg := range args {
		arr[i] = resp.Value{Type: "bulk", Bulk: arg}
	}
	return w.Write(resp.Value{Type: "array", Array: arr})
}

func TestHandler_Transactions(t *testing.T) {
	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{
			name: "TransactionIsolation",
			run: func(t *testing.T) {
				s := store.New()
				h := New(s, nil)

				server1, client1 := net.Pipe()
				server2, client2 := net.Pipe()
				defer client1.Close()
				defer client2.Close()
				defer server1.Close()
				defer server2.Close()

				go h.Handle(server1)
				go h.Handle(server2)

				r1 := bufio.NewReader(client1)
				w1 := resp.NewWriter(client1)
				r2 := bufio.NewReader(client2)
				w2 := resp.NewWriter(client2)

				if err := writeCommand(w1, "MULTI"); err != nil {
					t.Fatalf("write MULTI: %v", err)
				}
				v, err := readRespValue(r1)
				if err != nil {
					t.Fatalf("read MULTI response: %v", err)
				}
				if v.Type != "string" || v.Str != "OK" {
					t.Fatalf("MULTI response = %#v, want string OK", v)
				}

				if err := writeCommand(w2, "SET", "other", "2"); err != nil {
					t.Fatalf("write SET from conn2: %v", err)
				}
				v, err = readRespValue(r2)
				if err != nil {
					t.Fatalf("read conn2 SET response: %v", err)
				}
				if v.Type != "string" || v.Str != "OK" {
					t.Fatalf("conn2 SET response = %#v, want string OK", v)
				}

				if err := writeCommand(w1, "SET", "a", "1"); err != nil {
					t.Fatalf("write SET in tx: %v", err)
				}
				v, err = readRespValue(r1)
				if err != nil {
					t.Fatalf("read QUEUED response: %v", err)
				}
				if v.Type != "string" || v.Str != "QUEUED" {
					t.Fatalf("tx SET response = %#v, want string QUEUED", v)
				}

				if err := writeCommand(w1, "EXEC"); err != nil {
					t.Fatalf("write EXEC: %v", err)
				}
				v, err = readRespValue(r1)
				if err != nil {
					t.Fatalf("read EXEC response: %v", err)
				}
				if v.Type != "array" || len(v.Array) != 1 || v.Array[0].Type != "string" || v.Array[0].Str != "OK" {
					t.Fatalf("EXEC response = %#v, want array with OK", v)
				}

				if err := writeCommand(w2, "GET", "a"); err != nil {
					t.Fatalf("write GET from conn2: %v", err)
				}
				v, err = readRespValue(r2)
				if err != nil {
					t.Fatalf("read conn2 GET response: %v", err)
				}
				if v.Type != "bulk" || v.Bulk != "1" {
					t.Fatalf("conn2 GET response = %#v, want bulk 1", v)
				}
			},
		},
		{
			name: "TransactionQueueOrder",
			run: func(t *testing.T) {
				s := store.New()
				h := New(s, nil)

				server, client := net.Pipe()
				defer client.Close()
				defer server.Close()

				go h.Handle(server)

				r := bufio.NewReader(client)
				w := resp.NewWriter(client)

				if err := writeCommand(w, "MULTI"); err != nil {
					t.Fatalf("write MULTI: %v", err)
				}
				v, err := readRespValue(r)
				if err != nil {
					t.Fatalf("read MULTI response: %v", err)
				}
				if v.Type != "string" || v.Str != "OK" {
					t.Fatalf("MULTI response = %#v, want string OK", v)
				}

				for i, cmd := range [][]string{{"SET", "a", "1"}, {"SET", "b", "2"}} {
					if err := writeCommand(w, cmd...); err != nil {
						t.Fatalf("write SET %d: %v", i, err)
					}
					v, err = readRespValue(r)
					if err != nil {
						t.Fatalf("read QUEUED response %d: %v", i, err)
					}
					if v.Type != "string" || v.Str != "QUEUED" {
						t.Fatalf("queued response %d = %#v, want string QUEUED", i, v)
					}
				}

				if err := writeCommand(w, "EXEC"); err != nil {
					t.Fatalf("write EXEC: %v", err)
				}
				v, err = readRespValue(r)
				if err != nil {
					t.Fatalf("read EXEC response: %v", err)
				}
				if v.Type != "array" || len(v.Array) != 2 {
					t.Fatalf("EXEC response = %#v, want array len 2", v)
				}
				if v.Array[0].Type != "string" || v.Array[0].Str != "OK" {
					t.Fatalf("EXEC response[0] = %#v, want string OK", v.Array[0])
				}
				if v.Array[1].Type != "string" || v.Array[1].Str != "OK" {
					t.Fatalf("EXEC response[1] = %#v, want string OK", v.Array[1])
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.run(t)
		})
	}
}
