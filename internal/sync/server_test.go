package sync

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"collab-crdt/internal/crdt"
	"github.com/gorilla/websocket"
)

// TestServerSyncAndRelay tests connecting two clients, syncing them, and checking
// that operations sent by client A are broadcast to client B.
func TestServerSyncAndRelay(t *testing.T) {
	// 1. Start the HTTP test server with our WebSocket handler
	server := NewServer()
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", server.HandleWebSocket)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	// Convert http URL to ws URL
	wsURL := "ws" + strings.TrimPrefix(ts.URL, "http") + "/ws"

	// 2. Connect Client A
	connA, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("Client A failed to connect: %v", err)
	}
	defer connA.Close()

	// 3. Connect Client B
	connB, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("Client B failed to connect: %v", err)
	}
	defer connB.Close()

	// 4. Client A syncs with room "doc1"
	syncMsgA := Message{
		Type:  MsgSync,
		DocID: "doc1",
	}
	bytesA, _ := json.Marshal(syncMsgA)
	if err := connA.WriteMessage(websocket.TextMessage, bytesA); err != nil {
		t.Fatalf("Client A failed to send MsgSync: %v", err)
	}

	// Client A should receive MsgInit (empty history)
	var responseA Message
	if err := connA.ReadJSON(&responseA); err != nil {
		t.Fatalf("Client A failed to read MsgInit: %v", err)
	}
	if responseA.Type != MsgInit || len(responseA.History) != 0 {
		t.Errorf("Expected empty MsgInit for Client A, got: %+v", responseA)
	}

	// 5. Client B syncs with room "doc1"
	syncMsgB := Message{
		Type:  MsgSync,
		DocID: "doc1",
	}
	bytesB, _ := json.Marshal(syncMsgB)
	if err := connB.WriteMessage(websocket.TextMessage, bytesB); err != nil {
		t.Fatalf("Client B failed to send MsgSync: %v", err)
	}

	// Client B should receive MsgInit (empty history)
	var responseB Message
	if err := connB.ReadJSON(&responseB); err != nil {
		t.Fatalf("Client B failed to read MsgInit: %v", err)
	}
	if responseB.Type != MsgInit {
		t.Errorf("Expected MsgInit for Client B, got: %+v", responseB)
	}

	// 6. Client A sends an insert operation
	op := crdt.Op{
		Type:     crdt.OpInsert,
		ID:       crdt.ID{Timestamp: 1, SiteID: "siteA"},
		ParentID: crdt.StartID,
		Value:    'H',
	}
	opMsg := Message{
		Type:  MsgOp,
		DocID: "doc1",
		Op:    &op,
	}
	bytesOp, _ := json.Marshal(opMsg)
	if err := connA.WriteMessage(websocket.TextMessage, bytesOp); err != nil {
		t.Fatalf("Client A failed to send MsgOp: %v", err)
	}

	// 7. Verify Client B receives the broadcasted operation
	connB.SetReadDeadline(time.Now().Add(2 * time.Second))
	var relayedMsg Message
	if err := connB.ReadJSON(&relayedMsg); err != nil {
		t.Fatalf("Client B failed to receive relayed op: %v", err)
	}

	if relayedMsg.Type != MsgOp || relayedMsg.Op == nil || relayedMsg.Op.Value != 'H' {
		t.Errorf("Expected relayed MsgOp for Client B with 'H', got: %+v", relayedMsg)
	}

	// 8. Connect Client C and verify it receives the operation history
	connC, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("Client C failed to connect: %v", err)
	}
	defer connC.Close()

	syncMsgC := Message{
		Type:  MsgSync,
		DocID: "doc1",
	}
	bytesC, _ := json.Marshal(syncMsgC)
	_ = connC.WriteMessage(websocket.TextMessage, bytesC)

	var responseC Message
	if err := connC.ReadJSON(&responseC); err != nil {
		t.Fatalf("Client C failed to read MsgInit: %v", err)
	}

	if responseC.Type != MsgInit || len(responseC.History) != 1 || responseC.History[0].Value != 'H' {
		t.Errorf("Expected MsgInit with history length 1 ('H') for Client C, got: %+v", responseC)
	}
}
