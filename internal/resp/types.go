package resp

// Protocol data types
const (
	STRING  = '+'
	ERROR   = '-'
	INTEGER = ':'
	BULK    = '$'
	ARRAY   = '*'
)

// Value represents the data structure of a RESP message
type Value struct {
	Type  string
	Str   string
	Num   int
	Bulk  string
	Array []Value
}
