package resp

import (
	"bytes"
	"io"
	"strings"
	"testing"
)

func TestReader_Read(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    Value
		wantErr bool
	}{
		{
			name:  "Simple PING",
			input: "*1\r\n$4\r\nPING\r\n",
			want: Value{
				Type: "array",
				Array: []Value{
					{Type: "bulk", Bulk: "PING"},
				},
			},
			wantErr: false,
		},
		{
			name:  "ECHO hello",
			input: "*2\r\n$4\r\nECHO\r\n$5\r\nhello\r\n",
			want: Value{
				Type: "array",
				Array: []Value{
					{Type: "bulk", Bulk: "ECHO"},
					{Type: "bulk", Bulk: "hello"},
				},
			},
			wantErr: false,
		},
		{
			name:  "Null bulk",
			input: "*2\r\n$4\r\nECHO\r\n$-1\r\n",
			want: Value{
				Type: "array",
				Array: []Value{
					{Type: "bulk", Bulk: "ECHO"},
					{Type: "null"},
				},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := NewReader(bytes.NewBufferString(tt.input))
			got, err := r.Read()
			if (err != nil) != tt.wantErr {
				t.Errorf("Reader.Read() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			// Basic deep comparison logic
			if got.Type != tt.want.Type {
				t.Errorf("got type %v, want %v", got.Type, tt.want.Type)
			}
			if len(got.Array) != len(tt.want.Array) {
				t.Errorf("got array len %v, want %v", len(got.Array), len(tt.want.Array))
			}
			for i := range got.Array {
				if got.Array[i].Type != tt.want.Array[i].Type {
					t.Errorf("got array[%d] type %v, want %v", i, got.Array[i].Type, tt.want.Array[i].Type)
				}
				if got.Array[i].Bulk != tt.want.Array[i].Bulk {
					t.Errorf("got array[%d] %v, want %v", i, got.Array[i].Bulk, tt.want.Array[i].Bulk)
				}
			}
		})
	}
}

type slowReader struct {
	r io.Reader
	n int
}

func (s *slowReader) Read(p []byte) (int, error) {
	if len(p) > s.n {
		p = p[:s.n]
	}
	return s.r.Read(p)
}

func TestReader_Read_Chunked(t *testing.T) {
	input := "*2\r\n$4\r\nECHO\r\n$5\r\nhello\r\n"
	r := NewReader(&slowReader{r: strings.NewReader(input), n: 1})
	got, err := r.Read()
	if err != nil {
		t.Fatalf("Reader.Read() error = %v", err)
	}
	if got.Type != "array" {
		t.Fatalf("got type %v, want array", got.Type)
	}
	if len(got.Array) != 2 {
		t.Fatalf("got array len %v, want 2", len(got.Array))
	}
	if got.Array[0].Type != "bulk" || got.Array[0].Bulk != "ECHO" {
		t.Fatalf("got array[0] = %#v, want bulk ECHO", got.Array[0])
	}
	if got.Array[1].Type != "bulk" || got.Array[1].Bulk != "hello" {
		t.Fatalf("got array[1] = %#v, want bulk hello", got.Array[1])
	}
}

func TestWriter_Write(t *testing.T) {
	tests := []struct {
		name    string
		value   Value
		want    string
		wantErr bool
	}{
		{
			name: "Simple String",
			value: Value{
				Type: "string",
				Str:  "OK",
			},
			want: "+OK\r\n",
		},
		{
			name: "Bulk String",
			value: Value{
				Type: "bulk",
				Bulk: "hello",
			},
			want: "$5\r\nhello\r\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var b bytes.Buffer
			w := NewWriter(&b)
			err := w.Write(tt.value)
			if (err != nil) != tt.wantErr {
				t.Errorf("Writer.Write() error = %v, wantErr %v", err, tt.wantErr)
			}
			if b.String() != tt.want {
				t.Errorf("got %q, want %q", b.String(), tt.want)
			}
		})
	}
}
