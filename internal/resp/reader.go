package resp

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
)

type Reader struct {
	reader *bufio.Reader
}

func NewReader(rd io.Reader) *Reader {
	return &Reader{reader: bufio.NewReader(rd)}
}

func (r *Reader) ReadLine() (line []byte, n int, err error) {
	for {
		b, err := r.reader.ReadByte()
		if err != nil {
			return nil, 0, err
		}
		n += 1
		line = append(line, b)
		if len(line) >= 2 && line[len(line)-2] == '\r' {
			break
		}
	}
	return line[:len(line)-2], n, nil
}

func (r *Reader) ReadInteger() (x int, n int, err error) {
	line, n, err := r.ReadLine()
	if err != nil {
		return 0, 0, err
	}
	i64, err := strconv.ParseInt(string(line), 10, 64)
	if err != nil {
		return 0, n, err
	}
	return int(i64), n, nil
}

func (r *Reader) Read() (Value, error) {
	_type, err := r.reader.ReadByte()
	if err != nil {
		return Value{}, err
	}

	switch _type {
	case ARRAY:
		return r.readArray()
	case BULK:
		return r.readBulk()
	default:
		fmt.Printf("Unknown type: %v\n", string(_type))
		return Value{}, fmt.Errorf("unknown type: %v", string(_type))
	}
}

func (r *Reader) readArray() (Value, error) {
	v := Value{}
	v.Type = "array"

	// read length of array
	len, _, err := r.ReadInteger()
	if err != nil {
		return v, err
	}

	// foreach line, read valid RESP
	v.Array = make([]Value, len)
	for i := range len {
		val, err := r.Read()
		if err != nil {
			return v, err
		}
		v.Array[i] = val
	}

	return v, nil
}

func (r *Reader) readBulk() (Value, error) {
	v := Value{}
	v.Type = "bulk"

	len, _, err := r.ReadInteger()
	if err != nil {
		return v, err
	}

	if len == -1 {
		v.Type = "null"
		return v, nil
	}
	if len < -1 {
		return v, fmt.Errorf("invalid bulk length: %d", len)
	}

	bulk := make([]byte, len)
	if _, err := io.ReadFull(r.reader, bulk); err != nil {
		return v, err
	}
	v.Bulk = string(bulk)

	// Read the trailing CRLF
	if _, _, err := r.ReadLine(); err != nil {
		return v, err
	}

	return v, nil
}
