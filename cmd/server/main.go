package main

import (
	"log"
	"net/http"

	"collab-crdt/internal/sync"
)

func main() {
	server := sync.NewServer()
	http.HandleFunc("/ws", server.HandleWebSocket)

	log.Println("Starting Collaborative CRDT Relay Server on :8080...")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		log.Fatalf("Server failed to start: %v", err)
	}
}
