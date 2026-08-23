package sync

import (
	"encoding/json"
	"log"
	"net/http"
	"sync"

	"collab-crdt/internal/crdt"
	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true // Allow all origins for simplicity in development
	},
}

// MessageType defines the type of message sent over WebSockets.
type MessageType string

const (
	MsgSync MessageType = "sync" // Client wants to join a document room
	MsgInit MessageType = "init" // Server sends document history to the joining client
	MsgOp   MessageType = "op"   // Operation (insert/delete) being relayed
)

// Message defines the JSON structure of our WebSocket messages.
type Message struct {
	Type    MessageType `json:"type"`
	DocID   string      `json:"doc_id"`
	Op      *crdt.Op    `json:"op,omitempty"`      // Present if MsgOp
	History []crdt.Op   `json:"history,omitempty"` // Present if MsgInit
}

// Client represents a single client WebSocket connection.
type Client struct {
	conn  *websocket.Conn
	send  chan []byte
	done  chan struct{}
	once  sync.Once
	docID string
}

// NewClient instantiates a client with a buffered send channel.
func NewClient(conn *websocket.Conn) *Client {
	return &Client{
		conn: conn,
		send: make(chan []byte, 256), // Buffer to handle brief bursts of operations
		done: make(chan struct{}),
	}
}

// Close gracefully tears down the client connection exactly once.
func (c *Client) Close() {
	c.once.Do(func() {
		close(c.done)
		c.conn.Close()
	})
}

// Session represents a document room containing all active clients and the operation log.
type Session struct {
	sync.Mutex
	docID   string
	history []crdt.Op
	clients map[*Client]bool
}

// Server manages all active document sessions.
type Server struct {
	sync.RWMutex
	sessions map[string]*Session
}

// NewServer creates a new instance of the Relay Server.
func NewServer() *Server {
	return &Server{
		sessions: make(map[string]*Session),
	}
}

// HandleWebSocket upgrades the HTTP request to a WebSocket connection and registers the client.
func (s *Server) HandleWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("failed to upgrade connection: %v", err)
		return
	}

	client := NewClient(conn)

	// Spawn write pump to handle outbound messages in a single dedicated goroutine.
	go client.writePump()

	// Handle inbound messages synchronously on the main thread upgrading the connection.
	client.readPump(s)
}

// register adds a client to a document session and sends them the operation log.
func (s *Server) register(client *Client, docID string) {
	s.Lock()
	session, exists := s.sessions[docID]
	if !exists {
		session = &Session{
			docID:   docID,
			history: make([]crdt.Op, 0),
			clients: make(map[*Client]bool),
		}
		s.sessions[docID] = session
	}
	s.Unlock()

	session.Lock()
	client.docID = docID
	session.clients[client] = true

	// Copy history to send to client
	historyCopy := make([]crdt.Op, len(session.history))
	copy(historyCopy, session.history)
	session.Unlock()

	// Send MsgInit to catch the client up with the current document state
	initMsg := Message{
		Type:    MsgInit,
		DocID:   docID,
		History: historyCopy,
	}
	initBytes, err := json.Marshal(initMsg)
	if err == nil {
		select {
		case client.send <- initBytes:
		case <-client.done:
		}
	}
}

// unregister removes a client from their active session.
func (s *Server) unregister(client *Client) {
	if client.docID == "" {
		return
	}

	s.RLock()
	session, exists := s.sessions[client.docID]
	s.RUnlock()

	if exists {
		session.Lock()
		delete(session.clients, client)
		session.Unlock()
	}
}

// broadcastOp appends the operation to the document history and broadcasts it to all other clients.
func (s *Server) broadcastOp(sender *Client, docID string, op crdt.Op) {
	s.RLock()
	session, exists := s.sessions[docID]
	s.RUnlock()

	if !exists {
		return
	}

	session.Lock()
	// Append operation to the in-memory causal log (history)
	session.history = append(session.history, op)

	msg := Message{
		Type:  MsgOp,
		DocID: docID,
		Op:    &op,
	}
	msgBytes, err := json.Marshal(msg)
	if err != nil {
		session.Unlock()
		log.Printf("failed to marshal operation: %v", err)
		return
	}

	// Relay the operation to all other connected clients
	for client := range session.clients {
		if client != sender {
			select {
			case client.send <- msgBytes:
			case <-client.done:
				// Client is disconnecting, ignore
			default:
				// Client buffer is full (slow consumer). Close connection to protect server.
				log.Printf("slow client detected, closing connection")
				client.Close()
			}
		}
	}
	session.Unlock()
}

// readPump reads messages from the WebSocket connection and routes them to the server.
func (c *Client) readPump(server *Server) {
	defer func() {
		server.unregister(c)
		c.Close()
	}()

	for {
		_, messageBytes, err := c.conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				log.Printf("unexpected read error: %v", err)
			}
			break
		}

		var msg Message
		if err := json.Unmarshal(messageBytes, &msg); err != nil {
			log.Printf("failed to unmarshal message: %v", err)
			continue
		}

		switch msg.Type {
		case MsgSync:
			server.register(c, msg.DocID)
		case MsgOp:
			if msg.Op != nil {
				server.broadcastOp(c, msg.DocID, *msg.Op)
			}
		}
	}
}

// writePump writes messages from the send channel to the WebSocket connection.
func (c *Client) writePump() {
	defer c.Close()

	for {
		select {
		case message := <-c.send:
			err := c.conn.WriteMessage(websocket.TextMessage, message)
			if err != nil {
				return
			}
		case <-c.done:
			return
		}
	}
}
