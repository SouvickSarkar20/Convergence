package main

import (
	"context"
	"log"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"time"

	collabsync "collab-crdt/internal/sync"
)

func main() {
	log.Println("Starting CRDT Convergence Simulation...")

	// 1. Start background WebSocket Relay Server using httptest
	relayServer := collabsync.NewServer()
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", relayServer.HandleWebSocket)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	// Convert HTTP URL to WebSocket protocol (ws://)
	wsURL := "ws" + strings.TrimPrefix(ts.URL, "http") + "/ws"
	log.Printf("Local server initialized at: %s", wsURL)

	docID := "shared-crdt-doc"
	clientIDs := []string{"Client_Alpha", "Client_Beta", "Client_Gamma"}
	var clients []*collabsync.SyncClient

	// 2. Connect client instances to the server
	for _, id := range clientIDs {
		client := collabsync.NewSyncClient(wsURL, docID, id)
		if err := client.Connect(); err != nil {
			log.Fatalf("Failed to connect client %s: %v", id, err)
		}
		defer client.Disconnect()
		clients = append(clients, client)
		log.Printf("Connected client replica: %s", id)
	}

	// 3. Run Simulated Typists concurrently for 3 seconds
	log.Println("Launching concurrent simulated typists...")
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	for _, client := range clients {
		wg.Add(1)
		// Run each typist in its own goroutine
		go func(c *collabsync.SyncClient) {
			defer wg.Done()
			runTypist(ctx, c)
		}(client)
	}

	// Wait for the timeout to elapse and all typing goroutines to return
	wg.Wait()
	log.Println("All typists have paused typing. Waiting for synchronization to complete...")

	// 4. Wait for all pending messages to propagate and be processed by clients
	time.Sleep(1 * time.Second)

	// 5. Assert final convergence
	log.Println("--- Final Document Content States ---")
	diverged := false
	expectedText := clients[0].Doc.ToString()

	for _, c := range clients {
		text := c.Doc.ToString()
		log.Printf("[%s] length: %d | content: %q", c.Doc.GetSiteID(), len(text), text)
		if text != expectedText {
			diverged = true
		}
	}
	log.Println("-------------------------------------")

	if diverged {
		log.Fatalf("CRITICAL: Convergence failure! Client document states have diverged.")
	} else {
		log.Println("SUCCESS: All client replicas successfully converged to the exact same text representation!")
	}
}

// runTypist performs a loop of random insert and delete operations on a client.
func runTypist(ctx context.Context, c *collabsync.SyncClient) {
	letters := []rune("abcdefghijklmnopqrstuvwxyz")
	
	// Create a local PRNG seeded with current nanoseconds to prevent identical typing sequences
	prng := rand.New(rand.NewSource(time.Now().UnixNano()))

	// Type a character or perform a delete every 50ms - 150ms
	ticker := time.NewTicker(time.Duration(50+prng.Intn(101)) * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			content := c.Doc.GetContent()
			length := len(content)

			// If document is empty, we must insert. 
			// Otherwise, 80% chance of inserting, 20% chance of deleting.
			if length == 0 || prng.Float32() < 0.8 {
				// Random offset between 0 and length (inclusive)
				offset := 0
				if length > 0 {
					offset = prng.Intn(length + 1)
				}
				char := letters[prng.Intn(len(letters))]
				if err := c.LocalInsert(offset, char); err != nil {
					log.Printf("[%s] Insert error: %v", c.Doc.GetSiteID(), err)
				} else {
					log.Printf("[%s] + %q at %d", c.Doc.GetSiteID(), string(char), offset)
				}
			} else {
				// Random offset between 0 and length-1 (inclusive)
				offset := prng.Intn(length)
				if err := c.LocalDelete(offset); err != nil {
					log.Printf("[%s] Delete error: %v", c.Doc.GetSiteID(), err)
				} else {
					log.Printf("[%s] - delete at %d", c.Doc.GetSiteID(), offset)
				}
			}
		}
	}
}
