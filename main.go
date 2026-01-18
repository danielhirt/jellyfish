package main

import (
	"fmt"
	"jellyfish/internal/aof"
	"jellyfish/internal/handler"
	"jellyfish/internal/resp"
	"jellyfish/internal/store"
	"net"
)

func main() {
	fmt.Println("Listening on port :6379")

	l, err := net.Listen("tcp", ":6379")
	if err != nil {
		fmt.Println(err)
		return
	}

	// Initialize the shared store
	kv := store.New()

	// Initialize AOF
	aof, err := aof.New("database.aof")
	if err != nil {
		fmt.Println(err)
		return
	}
	defer aof.Close()

	// Replay AOF
	aof.Read(func(value resp.Value) {
		// Create a temporary handler with nil AOF to avoid double logging
		h := handler.New(kv, nil)
		h.Execute(value, nil)
	})

	// Initialize the handler with the store and AOF
	h := handler.New(kv, aof)

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println(err)
			continue
		}
		go h.Handle(conn)
	}
}
