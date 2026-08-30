package sync

import (
	"encoding/json"
	"log"
	"net/url"
	"sync"

	"collab-crdt/internal/crdt"
	"github.com/gorilla/websocket"
)

// SyncClient represents a client connection to the relay server.
// It wraps a local CRDT Document and synchronizes its state with the server.
type SyncClient struct {
	Doc       *crdt.Document
	conn      *websocket.Conn
	send      chan []byte
	done      chan struct{}
	closeOnce sync.Once
	serverURL string
	docID     string
	siteID    string
}

// NewSyncClient instantiates a new SyncClient with an empty CRDT Document.
func NewSyncClient(serverURL string, docID string, siteID string) *SyncClient {
	return &SyncClient{
		Doc:       crdt.NewDocument(siteID),
		send:      make(chan []byte, 256),
		done:      make(chan struct{}),
		serverURL: serverURL,
		docID:     docID,
		siteID:    siteID,
	}
}

// Connect establishes the WebSocket connection, starts message pumps, and joins the room.
func (c *SyncClient) Connect() error {
	u, err := url.Parse(c.serverURL)
	if err != nil {
		return err
	}

	conn, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	if err != nil {
		return err
	}
	c.conn = conn

	// Start message processing loops in separate goroutines
	go c.writePump()
	go c.readPump()

	// Immediately send MsgSync to join the room session
	syncMsg := Message{
		Type:  MsgSync,
		DocID: c.docID,
	}
	syncBytes, err := json.Marshal(syncMsg)
	if err != nil {
		c.Disconnect()
		return err
	}

	select {
	case c.send <- syncBytes:
	case <-c.done:
	}

	return nil
}

// Disconnect cleans up client connection resources safely.
func (c *SyncClient) Disconnect() {
	c.closeOnce.Do(func() {
		close(c.done)
		if c.conn != nil {
			c.conn.Close()
		}
	})
}

// LocalInsert applies an insert locally and transmits the operation to the server.
func (c *SyncClient) LocalInsert(offset int, value rune) error {
	op, err := c.Doc.LocalInsert(offset, value)
	if err != nil {
		return err
	}

	msg := Message{
		Type:  MsgOp,
		DocID: c.docID,
		Op:    &op,
	}
	msgBytes, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	select {
	case c.send <- msgBytes:
	case <-c.done:
	}
	return nil
}

// LocalDelete applies a delete locally and transmits the operation to the server.
func (c *SyncClient) LocalDelete(offset int) error {
	op, err := c.Doc.LocalDelete(offset)
	if err != nil {
		return err
	}

	msg := Message{
		Type:  MsgOp,
		DocID: c.docID,
		Op:    &op,
	}
	msgBytes, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	select {
	case c.send <- msgBytes:
	case <-c.done:
	}
	return nil
}

// writePump handles outbound network messages sequentially from the send channel.
func (c *SyncClient) writePump() {
	defer c.Disconnect()
	for {
		select {
		case msg := <-c.send:
			err := c.conn.WriteMessage(websocket.TextMessage, msg)
			if err != nil {
				return
			}
		case <-c.done:
			return
		}
	}
}

// readPump handles inbound network messages and applies them to the local CRDT document.
func (c *SyncClient) readPump() {
	defer c.Disconnect()
	for {
		_, messageBytes, err := c.conn.ReadMessage()
		if err != nil {
			return
		}

		var msg Message
		if err := json.Unmarshal(messageBytes, &msg); err != nil {
			log.Printf("[%s] failed to unmarshal message: %v", c.siteID, err)
			continue
		}

		switch msg.Type {
		case MsgInit:
			// Hydrate the local document history log
			for _, op := range msg.History {
				c.Doc.Apply(op)
			}
		case MsgOp:
			if msg.Op != nil {
				c.Doc.Apply(*msg.Op)
			}
		}
	}
}
