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
	deleteFlag := flag.Float64("delete", 0.2, "Probability of deletion operations (0.0 to 1.0)")

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
	runTypist(ctx, client, *deleteFlag)

	// Wait for any remaining in-transit operations to synchronize
	log.Printf("[%s] Finished typing. Waiting for network sync...", *siteFlag)
	time.Sleep(3 * time.Second)

	// Output the final state in a format the test harness can parse
	finalText := client.Doc.ToString()
	fmt.Printf("RESULT:%s:%s\n", *siteFlag, finalText)

	// Output execution performance and memory footprint metrics
	fmt.Printf("METRICS:%s:%d:%d:%d:%d\n",
		*siteFlag,
		client.Doc.GetNodesCount(),
		len(finalText),
		client.GetLastLocalEditTime(),
		client.GetLastUpdateTime(),
	)

	client.Doc.PrintDebugList()
}

func runTypist(ctx context.Context, c *collabsync.SyncClient, deleteRatio float64) {
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

			// If document is empty or random roll is >= deleteRatio, perform insert
			if length == 0 || prng.Float64() >= deleteRatio {
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
