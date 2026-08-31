package sync

import (
	"encoding/json"
	"log"
	"net/http"
	"sync"
	"time"

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
	MsgSync     MessageType = "sync"      // Client wants to join a document room
	MsgInit     MessageType = "init"      // Server sends document history to the joining client
	MsgOp       MessageType = "op"        // Operation (insert/delete) being relayed
	MsgPeerSync MessageType = "peer-sync" // Symmetrical handshake for initial peer history syncing
)

// Message defines the JSON structure of our WebSocket messages.
type Message struct {
	Type       MessageType          `json:"type"`
	DocID      string               `json:"doc_id"`
	Op         *crdt.Op             `json:"op,omitempty"`
	History    []crdt.Op            `json:"history,omitempty"`
	SyncAll    map[string][]crdt.Op `json:"sync_all,omitempty"`
	IsResponse bool                 `json:"is_response,omitempty"`
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
		send: make(chan []byte, 256),
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

// Peer represents a WebSocket connection to another relay server in the network.
type Peer struct {
	conn *websocket.Conn
	send chan []byte
	done chan struct{}
	once sync.Once
}

// NewPeer instantiates a peer connection.
func NewPeer(conn *websocket.Conn) *Peer {
	return &Peer{
		conn: conn,
		send: make(chan []byte, 256),
		done: make(chan struct{}),
	}
}

// Close gracefully tears down the peer connection.
func (p *Peer) Close() {
	p.once.Do(func() {
		close(p.done)
		p.conn.Close()
	})
}

// Session represents a document room containing all active clients and the operation log.
type Session struct {
	sync.Mutex
	docID   string
	history []crdt.Op
	clients map[*Client]bool
}

// Server manages all active document sessions and sibling server peers.
type Server struct {
	sync.RWMutex
	sessions map[string]*Session
	peers    map[*Peer]bool
	peersMu  sync.RWMutex
}

// NewServer creates a new instance of the Relay Server.
func NewServer() *Server {
	return &Server{
		sessions: make(map[string]*Session),
		peers:    make(map[*Peer]bool),
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
	go client.writePump()
	client.readPump(s)
}

// HandlePeer upgrades the HTTP request to register an incoming peer server.
func (s *Server) HandlePeer(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("failed to upgrade peer connection: %v", err)
		return
	}

	peer := NewPeer(conn)
	s.registerPeer(peer)

	go peer.writePump()
	peer.readPump(s)
}

func (s *Server) registerPeer(peer *Peer) {
	s.peersMu.Lock()
	s.peers[peer] = true
	s.peersMu.Unlock()
	log.Printf("[P2P] Peer registered successfully")
}

func (s *Server) unregisterPeer(peer *Peer) {
	s.peersMu.Lock()
	delete(s.peers, peer)
	s.peersMu.Unlock()
	log.Printf("[P2P] Peer unregistered successfully")
}

// ConnectToPeer dials a sibling peer URL and registers it, retrying if the connection fails.
func (s *Server) ConnectToPeer(peerURL string) {
	log.Printf("[P2P] Dialing peer server: %s ...", peerURL)
	
	var conn *websocket.Conn
	var err error
	maxRetries := 10
	
	for i := 0; i < maxRetries; i++ {
		conn, _, err = websocket.DefaultDialer.Dial(peerURL, nil)
		if err == nil {
			break
		}
		log.Printf("[P2P] Attempt %d/%d failed: %v. Retrying in 1 second...", i+1, maxRetries, err)
		time.Sleep(1 * time.Second)
	}

	if err != nil {
		log.Printf("[P2P] Failed to dial peer %s after %d retries: %v", peerURL, maxRetries, err)
		return
	}

	peer := NewPeer(conn)
	s.registerPeer(peer)

	go peer.writePump()
	
	// Send initial MsgPeerSync handshake to align histories
	s.sendPeerSync(peer, false)

	go peer.readPump(s)
}

// sendPeerSync packages all local document history logs to synchronize with a peer.
func (s *Server) sendPeerSync(peer *Peer, isResponse bool) {
	s.RLock()
	syncAll := make(map[string][]crdt.Op)
	for docID, session := range s.sessions {
		session.Lock()
		historyCopy := make([]crdt.Op, len(session.history))
		copy(historyCopy, session.history)
		session.Unlock()
		syncAll[docID] = historyCopy
	}
	s.RUnlock()

	msg := Message{
		Type:       MsgPeerSync,
		SyncAll:    syncAll,
		IsResponse: isResponse,
	}
	msgBytes, err := json.Marshal(msg)
	if err != nil {
		log.Printf("failed to marshal peer sync message: %v", err)
		return
	}

	select {
	case peer.send <- msgBytes:
	case <-peer.done:
	}
}

// mergePeerSync integrates operation logs received from a peer during handshake.
func (s *Server) mergePeerSync(sourcePeer *Peer, syncAll map[string][]crdt.Op, isResponse bool) {
	log.Printf("[P2P] Merging histories from peer (isResponse: %v)...", isResponse)
	s.Lock()
	for docID, history := range syncAll {
		session, exists := s.sessions[docID]
		if !exists {
			session = &Session{
				docID:   docID,
				history: make([]crdt.Op, 0),
				clients: make(map[*Client]bool),
			}
			s.sessions[docID] = session
		}

		session.Lock()
		type opKey struct {
			id   crdt.ID
			kind crdt.OpType
		}
		existingOps := make(map[opKey]bool)
		for _, op := range session.history {
			existingOps[opKey{id: op.ID, kind: op.Type}] = true
		}

		var newOps []crdt.Op
		for _, op := range history {
			if !existingOps[opKey{id: op.ID, kind: op.Type}] {
				session.history = append(session.history, op)
				newOps = append(newOps, op)
			}
		}
		session.Unlock()

		if len(newOps) > 0 {
			session.Lock()
			for _, op := range newOps {
				msg := Message{
					Type:  MsgOp,
					DocID: docID,
					Op:    &op,
				}
				msgBytes, err := json.Marshal(msg)
				if err == nil {
					for client := range session.clients {
						select {
						case client.send <- msgBytes:
						case <-client.done:
						default:
						}
					}
				}
			}
			session.Unlock()
		}
	}
	s.Unlock()

	if !isResponse {
		s.sendPeerSync(sourcePeer, true)
	}
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

	historyCopy := make([]crdt.Op, len(session.history))
	copy(historyCopy, session.history)
	session.Unlock()

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

// broadcastOp appends the operation to the document history, relays it to other local clients, and gossips to all peers.
func (s *Server) broadcastOp(sender *Client, docID string, op crdt.Op) {
	s.RLock()
	session, exists := s.sessions[docID]
	s.RUnlock()

	if !exists {
		return
	}

	session.Lock()
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

	for client := range session.clients {
		if client != sender {
			select {
			case client.send <- msgBytes:
			case <-client.done:
			default:
				log.Printf("slow client detected, closing connection")
				client.Close()
			}
		}
	}
	session.Unlock()

	// Gossip operation to all connected peer servers
	s.peersMu.RLock()
	peerCount := len(s.peers)
	log.Printf("[P2P] Gossiping op %v to %d peers", op.ID, peerCount)
	for peer := range s.peers {
		select {
		case peer.send <- msgBytes:
		case <-peer.done:
		default:
			log.Printf("[P2P] Peer send queue full")
		}
	}
	s.peersMu.RUnlock()
}

// broadcastOpFromPeer processes an operation received from a peer, writes to history, alerts local clients, and forwards to other peers (avoiding loops).
func (s *Server) broadcastOpFromPeer(sourcePeer *Peer, docID string, op crdt.Op) {
	log.Printf("[P2P] Received op %v from peer for doc %s", op.ID, docID)
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
	// Idempotency / loop check: ignore if we already have it in history
	for _, existingOp := range session.history {
		if existingOp.ID == op.ID && existingOp.Type == op.Type {
			session.Unlock()
			log.Printf("[P2P] Op %v (%s) already exists in history. Ignoring.", op.ID, op.Type)
			return
		}
	}
	session.history = append(session.history, op)

	msg := Message{
		Type:  MsgOp,
		DocID: docID,
		Op:    &op,
	}
	msgBytes, err := json.Marshal(msg)
	if err != nil {
		session.Unlock()
		return
	}

	clientCount := len(session.clients)
	log.Printf("[P2P] Relaying op %v to %d local clients", op.ID, clientCount)
	for client := range session.clients {
		select {
		case client.send <- msgBytes:
		case <-client.done:
		default:
		}
	}
	session.Unlock()

	// Gossip to all other peer servers except the one we received it from
	s.peersMu.RLock()
	for peer := range s.peers {
		if peer != sourcePeer {
			select {
			case peer.send <- msgBytes:
			case <-peer.done:
			default:
			}
		}
	}
	s.peersMu.RUnlock()
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

// readPump reads messages from the WebSocket connection and routes them to the peer server manager.
func (p *Peer) readPump(server *Server) {
	defer func() {
		server.unregisterPeer(p)
		p.Close()
	}()

	for {
		_, messageBytes, err := p.conn.ReadMessage()
		if err != nil {
			break
		}

		var msg Message
		if err := json.Unmarshal(messageBytes, &msg); err != nil {
			log.Printf("[P2P] Failed to unmarshal peer message: %v", err)
			continue
		}

		switch msg.Type {
		case MsgPeerSync:
			server.mergePeerSync(p, msg.SyncAll, msg.IsResponse)
		case MsgOp:
			if msg.Op != nil {
				server.broadcastOpFromPeer(p, msg.DocID, *msg.Op)
			}
		}
	}
}

// writePump writes messages from the send channel to the WebSocket connection.
func (p *Peer) writePump() {
	defer p.Close()

	for {
		select {
		case message := <-p.send:
			err := p.conn.WriteMessage(websocket.TextMessage, message)
			if err != nil {
				return
			}
		case <-p.done:
			return
		}
	}
}
