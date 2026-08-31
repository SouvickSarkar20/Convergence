package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"strings"

	"collab-crdt/internal/sync"
)

func main() {
	portFlag := flag.String("port", "8080", "Port to listen on")
	peersFlag := flag.String("peers", "", "Comma-separated peer WebSocket URLs (e.g. ws://localhost:8080/peer)")
	flag.Parse()

	server := sync.NewServer()

	// Client endpoint
	http.HandleFunc("/ws", server.HandleWebSocket)
	// Peer syncing endpoint
	http.HandleFunc("/peer", server.HandlePeer)

	// Connect to configured seed peers
	if *peersFlag != "" {
		peers := strings.Split(*peersFlag, ",")
		for _, peer := range peers {
			peer = strings.TrimSpace(peer)
			if peer != "" {
				// Dial peers in background to prevent blocking server bind
				go server.ConnectToPeer(peer)
			}
		}
	}

	addr := fmt.Sprintf(":%s", *portFlag)
	log.Printf("Starting Collaborative CRDT Relay Server on %s...\n", addr)
	if err := http.ListenAndServe(addr, nil); err != nil {
		log.Fatalf("Server failed to start: %v", err)
	}
}
