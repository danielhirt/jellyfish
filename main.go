package main

import (
	"fmt"
	"jellyfish/internal/handler"
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

	// Initialize the handler with the store
	h := handler.New(kv)

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println(err)
			continue
		}
		go h.Handle(conn)
	}
}
