package resp

import (
	"io"
	"strconv"
)

type Writer struct {
	writer io.Writer
}

func NewWriter(w io.Writer) *Writer {
	return &Writer{writer: w}
}

func (w *Writer) Write(v Value) error {
	var bytes []byte

	switch v.Type {
	case "array":
		bytes = v.marshalArray()
	case "bulk":
		bytes = v.marshalBulk()
	case "string":
		bytes = v.marshalString()
	case "integer":
		bytes = v.marshalInteger()
	case "null":
		bytes = v.marshalNull()
	case "error":
		bytes = v.marshalError()
	default:
		// Default to string for unknown types for now, or could return error
		bytes = []byte{}
	}

	_, err := w.writer.Write(bytes)
	return err
}

func (v Value) marshalString() []byte {
	var bytes []byte
	bytes = append(bytes, STRING)
	bytes = append(bytes, v.Str...)
	bytes = append(bytes, '\r', '\n')
	return bytes
}

func (v Value) marshalBulk() []byte {
	var bytes []byte
	bytes = append(bytes, BULK)
	bytes = append(bytes, strconv.Itoa(len(v.Bulk))...)
	bytes = append(bytes, '\r', '\n')
	bytes = append(bytes, v.Bulk...)
	bytes = append(bytes, '\r', '\n')
	return bytes
}

func (v Value) marshalArray() []byte {
	len := len(v.Array)
	var bytes []byte
	bytes = append(bytes, ARRAY)
	bytes = append(bytes, strconv.Itoa(len)...)
	bytes = append(bytes, '\r', '\n')

	for i := 0; i < len; i++ {
		bytes = append(bytes, v.Array[i].marshal()...)
	}

	return bytes
}

func (v Value) marshalError() []byte {
	var bytes []byte
	bytes = append(bytes, ERROR)
	bytes = append(bytes, v.Str...)
	bytes = append(bytes, '\r', '\n')
	return bytes
}

func (v Value) marshalNull() []byte {
	return []byte("$-1\r\n")
}

func (v Value) marshalInteger() []byte {
	var bytes []byte
	bytes = append(bytes, INTEGER)
	bytes = append(bytes, strconv.Itoa(v.Num)...)
	bytes = append(bytes, '\r', '\n')
	return bytes
}

// Helper for recursive array marshalling
func (v Value) marshal() []byte {
	switch v.Type {
	case "array":
		return v.marshalArray()
	case "bulk":
		return v.marshalBulk()
	case "string":
		return v.marshalString()
	case "integer":
		return v.marshalInteger()
	case "null":
		return v.marshalNull()
	case "error":
		return v.marshalError()
	default:
		return []byte{}
	}
}
