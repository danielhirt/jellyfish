package aof

import (
	"io"
	"jellyfish/internal/resp"
	"os"
	"sync"
)

type Aof struct {
	file *os.File
	rd   *resp.Reader
	mu   sync.Mutex
}

func New(path string) (*Aof, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}

	return &Aof{
		file: f,
		rd:   resp.NewReader(f),
	}, nil
}

func (aof *Aof) Close() error {
	aof.mu.Lock()
	defer aof.mu.Unlock()
	return aof.file.Close()
}

func (aof *Aof) Write(v resp.Value) error {
	aof.mu.Lock()
	defer aof.mu.Unlock()

	w := resp.NewWriter(aof.file)
	if err := w.Write(v); err != nil {
		return err
	}

	return nil
}

// Read reads all commands from the AOF file and calls the callback for each one.
// This is used for replaying the log on startup.
func (aof *Aof) Read(fn func(value resp.Value)) error {
	aof.mu.Lock()
	defer aof.mu.Unlock()

	// Seek to start
	aof.file.Seek(0, 0)

	reader := resp.NewReader(aof.file)

	for {
		value, err := reader.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		fn(value)
	}

	return nil
}

// Sync forces a flush to disk
func (aof *Aof) Sync() error {
	aof.mu.Lock()
	defer aof.mu.Unlock()
	return aof.file.Sync()
}
