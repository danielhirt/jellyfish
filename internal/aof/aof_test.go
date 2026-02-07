package aof

import (
	"jellyfish/internal/resp"
	"os"
	"testing"
)

func TestAof_WriteRead(t *testing.T) {
	// Create a temporary file
	f, err := os.CreateTemp("", "jellyfish_test_*.aof")
	if err != nil {
		t.Fatal(err)
	}
	tmpName := f.Name()
	f.Close()
	defer os.Remove(tmpName)

	// Open AOF
	aof, err := New(tmpName)
	if err != nil {
		t.Fatalf("Failed to open AOF: %v", err)
	}

	// Write commands
	cmd1 := resp.Value{Type: "array", Array: []resp.Value{
		{Type: "bulk", Bulk: "SET"},
		{Type: "bulk", Bulk: "key1"},
		{Type: "bulk", Bulk: "value1"},
	}}

	cmd2 := resp.Value{Type: "array", Array: []resp.Value{
		{Type: "bulk", Bulk: "DEL"},
		{Type: "bulk", Bulk: "key1"},
	}}

	if err := aof.Write(cmd1); err != nil {
		t.Errorf("Write cmd1 failed: %v", err)
	}
	if err := aof.Write(cmd2); err != nil {
		t.Errorf("Write cmd2 failed: %v", err)
	}

	// Sync and Close to ensure flush
	aof.Sync()
	aof.Close()

	// Re-open for reading
	aof2, err := New(tmpName)
	if err != nil {
		t.Fatalf("Failed to re-open AOF: %v", err)
	}
	defer aof2.Close()

	var readCmds []resp.Value
	err = aof2.Read(func(v resp.Value) {
		readCmds = append(readCmds, v)
	})
	if err != nil {
		t.Errorf("Read failed: %v", err)
	}

	// Verify
	if len(readCmds) != 2 {
		t.Fatalf("Expected 2 commands, got %d", len(readCmds))
	}

	// Verify content of cmd1
	if readCmds[0].Array[0].Bulk != "SET" || readCmds[0].Array[1].Bulk != "key1" {
		t.Errorf("First command mismatch: %v", readCmds[0])
	}

	// Verify content of cmd2
	if readCmds[1].Array[0].Bulk != "DEL" {
		t.Errorf("Second command mismatch: %v", readCmds[1])
	}
}

func TestAof_AppendOrder(t *testing.T) {
	// Create a temporary file
	f, err := os.CreateTemp("", "jellyfish_test_*.aof")
	if err != nil {
		t.Fatal(err)
	}
	tmpName := f.Name()
	f.Close()
	defer os.Remove(tmpName)

	// Open AOF
	aof, err := New(tmpName)
	if err != nil {
		t.Fatalf("Failed to open AOF: %v", err)
	}
	defer aof.Close()

	cmd1 := resp.Value{Type: "array", Array: []resp.Value{
		{Type: "bulk", Bulk: "SET"},
		{Type: "bulk", Bulk: "key1"},
		{Type: "bulk", Bulk: "value1"},
	}}

	cmd2 := resp.Value{Type: "array", Array: []resp.Value{
		{Type: "bulk", Bulk: "DEL"},
		{Type: "bulk", Bulk: "key1"},
	}}

	if err := aof.Write(cmd1); err != nil {
		t.Fatalf("Write cmd1 failed: %v", err)
	}

	// Seek to start to ensure subsequent writes still append.
	if _, err := aof.file.Seek(0, 0); err != nil {
		t.Fatalf("Seek failed: %v", err)
	}

	if err := aof.Write(cmd2); err != nil {
		t.Fatalf("Write cmd2 failed: %v", err)
	}

	// Re-open for reading
	aof2, err := New(tmpName)
	if err != nil {
		t.Fatalf("Failed to re-open AOF: %v", err)
	}
	defer aof2.Close()

	var readCmds []resp.Value
	err = aof2.Read(func(v resp.Value) {
		readCmds = append(readCmds, v)
	})
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	if len(readCmds) != 2 {
		t.Fatalf("Expected 2 commands, got %d", len(readCmds))
	}
	if readCmds[0].Array[0].Bulk != "SET" {
		t.Fatalf("First command mismatch: %v", readCmds[0])
	}
	if readCmds[1].Array[0].Bulk != "DEL" {
		t.Fatalf("Second command mismatch: %v", readCmds[1])
	}
}
