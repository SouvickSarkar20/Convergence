package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"time"

	collabsync "collab-crdt/internal/sync"
)

func main() {
	urlFlag := flag.String("url", "ws://localhost:8080/ws", "WebSocket server URL")
	docFlag := flag.String("doc", "shared-doc", "Document ID")
	siteFlag := flag.String("site", "client-site", "Site ID")
	durationFlag := flag.Int("duration", 3, "Duration to run in seconds")

	flag.Parse()

	log.Printf("[%s] Starting client targeting %s (doc: %s)...", *siteFlag, *urlFlag, *docFlag)
	client := collabsync.NewSyncClient(*urlFlag, *docFlag, *siteFlag)
	if err := client.Connect(); err != nil {
		log.Fatalf("[%s] Connect failed: %v", *siteFlag, err)
	}
	defer client.Disconnect()

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(*durationFlag)*time.Second)
	defer cancel()

	// Run simulated typing loop
	runTypist(ctx, client)

	// Wait for any remaining in-transit operations to synchronize
	log.Printf("[%s] Finished typing. Waiting for network sync...", *siteFlag)
	time.Sleep(3 * time.Second)

	// Output the final state in a format the test harness can parse
	finalText := client.Doc.ToString()
	fmt.Printf("RESULT:%s:%s\n", *siteFlag, finalText)
}

func runTypist(ctx context.Context, c *collabsync.SyncClient) {
	letters := []rune("abcdefghijklmnopqrstuvwxyz")
	prng := rand.New(rand.NewSource(time.Now().UnixNano()))
	ticker := time.NewTicker(time.Duration(50+prng.Intn(101)) * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			content := c.Doc.GetContent()
			length := len(content)

			// 80% chance of inserting, 20% chance of deleting
			if length == 0 || prng.Float32() < 0.8 {
				offset := 0
				if length > 0 {
					offset = prng.Intn(length + 1)
				}
				char := letters[prng.Intn(len(letters))]
				if err := c.LocalInsert(offset, char); err != nil {
					log.Printf("[%s] Insert error: %v", c.Doc.GetSiteID(), err)
				}
			} else {
				offset := prng.Intn(length)
				if err := c.LocalDelete(offset); err != nil {
					log.Printf("[%s] Delete error: %v", c.Doc.GetSiteID(), err)
				}
			}
		}
	}
}
