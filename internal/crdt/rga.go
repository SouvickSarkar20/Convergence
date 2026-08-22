package crdt

import (
	"fmt"
	"strings"
	"sync"
)

// ID represents a unique identifier for an RGA element, combining a Lamport clock and a Site ID.
type ID struct {
	Timestamp int64  `json:"timestamp"`
	SiteID    string `json:"site_id"`
}

// Compare returns 1 if a > b, -1 if a < b, and 0 if a == b.
// Priority is given first to the Timestamp, and then to the SiteID as a tie-breaker.
func (a ID) Compare(b ID) int {
	if a.Timestamp > b.Timestamp {
		return 1
	}
	if a.Timestamp < b.Timestamp {
		return -1
	}
	if a.SiteID > b.SiteID {
		return 1
	}
	if a.SiteID < b.SiteID {
		return -1
	}
	return 0
}

// StartID is a sentinel ID representing the beginning of the document.
var StartID = ID{Timestamp: 0, SiteID: ""}

// OpType defines the types of collaborative operations supported.
type OpType string

const (
	OpInsert OpType = "insert"
	OpDelete OpType = "delete"
)

// Op represents a CRDT update operation to be replicated over the network.
type Op struct {
	Type     OpType `json:"type"`
	ID       ID     `json:"id"`
	ParentID ID     `json:"parent_id,omitempty"` // Used for inserts
	Value    rune   `json:"value,omitempty"`     // Used for inserts
}

// Node represents a single character node in the RGA linked list.
type Node struct {
	ID       ID
	ParentID ID
	Value    rune
	Deleted  bool
	Next     *Node
	Prev     *Node
}

// Document manages the RGA CRDT document state with thread-safe operations.
type Document struct {
	sync.RWMutex
	nodes          map[ID]*Node
	head           *Node
	tail           *Node
	clock          int64
	siteID         string
	pendingInserts map[ID][]Op
	pendingDeletes map[ID]bool
}

// NewDocument instantiates a new empty RGA Document.
func NewDocument(siteID string) *Document {
	// Initialize with a sentinel start node.
	head := &Node{
		ID:      StartID,
		Deleted: true, // Sentinel is always hidden/deleted
	}
	nodes := make(map[ID]*Node)
	nodes[StartID] = head

	return &Document{
		nodes:          nodes,
		head:           head,
		tail:           head,
		clock:          0,
		siteID:         siteID,
		pendingInserts: make(map[ID][]Op),
		pendingDeletes: make(map[ID]bool),
	}
}

// GetSiteID returns the site ID of this replica.
func (doc *Document) GetSiteID() string {
	return doc.siteID
}

// GetClock returns the current Lamport clock value.
func (doc *Document) GetClock() int64 {
	doc.RLock()
	defer doc.RUnlock()
	return doc.clock
}

// LocalInsert inserts a character at a visible 0-based offset.
func (doc *Document) LocalInsert(offset int, value rune) (Op, error) {
	doc.Lock()
	defer doc.Unlock()

	parentID, err := doc.getNodeIDAtOffsetUnlocked(offset)
	if err != nil {
		return Op{}, err
	}

	doc.clock++
	newID := ID{
		Timestamp: doc.clock,
		SiteID:    doc.siteID,
	}

	doc.applyInsert(newID, parentID, value)

	return Op{
		Type:     OpInsert,
		ID:       newID,
		ParentID: parentID,
		Value:    value,
	}, nil
}

// LocalDelete deletes a character at a visible 0-based offset.
func (doc *Document) LocalDelete(offset int) (Op, error) {
	doc.Lock()
	defer doc.Unlock()

	// The character to delete is at offset+1 in terms of visible count.
	id, err := doc.getNodeIDAtOffsetUnlocked(offset + 1)
	if err != nil {
		return Op{}, err
	}

	doc.applyDelete(id)

	return Op{
		Type: OpDelete,
		ID:   id,
	}, nil
}

// Apply applies a remote operation to the local document replica.
func (doc *Document) Apply(op Op) bool {
	doc.Lock()
	defer doc.Unlock()

	switch op.Type {
	case OpInsert:
		return doc.applyInsert(op.ID, op.ParentID, op.Value)
	case OpDelete:
		return doc.applyDelete(op.ID)
	}
	return false
}

// applyInsert inserts a new node into the RGA list. This is the core RGA integration algorithm.
func (doc *Document) applyInsert(id ID, parentID ID, value rune) bool {
	if _, exists := doc.nodes[id]; exists {
		return false // Already applied (idempotency)
	}

	parent, exists := doc.nodes[parentID]
	if !exists {
		// Parent not yet delivered. Buffer this operation.
		doc.pendingInserts[parentID] = append(doc.pendingInserts[parentID], Op{
			Type:     OpInsert,
			ID:       id,
			ParentID: parentID,
			Value:    value,
		})
		return false
	}

	// Advance local Lamport clock to maintain causality
	if id.Timestamp > doc.clock {
		doc.clock = id.Timestamp
	}

	newNode := &Node{
		ID:       id,
		ParentID: parentID,
		Value:    value,
		Deleted:  false,
	}

	// RGA Shift Rule Insertion logic:
	// We want to insert newNode after parent. However, other concurrent edits
	// might have also been inserted after parent.
	// We scan the siblings (nodes inserted after parent) and their descendants,
	// skipping those with a higher unique ID priority.
	insertPos := parent
	curr := parent.Next

	for curr != nil {
		if curr.ParentID == parentID {
			// Sibling node: compare IDs directly.
			if curr.ID.Compare(id) > 0 {
				insertPos = curr
				curr = curr.Next
			} else {
				// Sibling has lower priority. We stop and insert here.
				break
			}
		} else {
			// Non-sibling node: find which sibling ancestor it belongs to.
			siblingAncestor := curr
			for siblingAncestor != nil && siblingAncestor.ID != StartID && siblingAncestor.ParentID != parentID {
				siblingAncestor = doc.nodes[siblingAncestor.ParentID]
			}
			if siblingAncestor != nil && siblingAncestor.ID != StartID && siblingAncestor.ID.Compare(id) > 0 {
				// Descends from a higher-priority sibling: skip it.
				insertPos = curr
				curr = curr.Next
			} else {
				// Either walked out of parent's subtree or sibling ancestor has lower priority.
				break
			}
		}
	}

	// Perform the insertion in the doubly linked list.
	next := insertPos.Next
	insertPos.Next = newNode
	newNode.Prev = insertPos
	newNode.Next = next
	if next != nil {
		next.Prev = newNode
	} else {
		doc.tail = newNode
	}

	doc.nodes[id] = newNode

	// Apply pending delete if a delete operation arrived early.
	if doc.pendingDeletes[id] {
		newNode.Deleted = true
		delete(doc.pendingDeletes, id)
	}

	// Trigger cascade updates for any operations waiting on this new node as their parent.
	doc.applyPending(id)

	return true
}

// applyDelete sets the deleted flag on a node.
func (doc *Document) applyDelete(id ID) bool {
	node, exists := doc.nodes[id]
	if !exists {
		// Node not yet inserted. Buffer the delete.
		doc.pendingDeletes[id] = true
		return false
	}
	if node.Deleted {
		return false // Already deleted
	}
	node.Deleted = true
	return true
}

// applyPending recursively applies buffered insert operations once their parent is available.
func (doc *Document) applyPending(parentID ID) {
	ops, exists := doc.pendingInserts[parentID]
	if !exists {
		return
	}
	delete(doc.pendingInserts, parentID)

	for _, op := range ops {
		doc.applyInsert(op.ID, op.ParentID, op.Value)
	}
}

// ToString reconstructs the visible document text.
func (doc *Document) ToString() string {
	doc.RLock()
	defer doc.RUnlock()

	var sb strings.Builder
	curr := doc.head.Next
	for curr != nil {
		if !curr.Deleted {
			sb.WriteRune(curr.Value)
		}
		curr = curr.Next
	}
	return sb.String()
}

// GetContent returns the visible characters in the document.
func (doc *Document) GetContent() []rune {
	doc.RLock()
	defer doc.RUnlock()

	var content []rune
	curr := doc.head.Next
	for curr != nil {
		if !curr.Deleted {
			content = append(content, curr.Value)
		}
		curr = curr.Next
	}
	return content
}

// getNodeIDAtOffsetUnlocked returns the Node ID at the given visible 1-based offset.
// Assumes the caller holds the appropriate lock.
func (doc *Document) getNodeIDAtOffsetUnlocked(offset int) (ID, error) {
	if offset == 0 {
		return StartID, nil
	}

	count := 0
	curr := doc.head.Next
	for curr != nil {
		if !curr.Deleted {
			count++
			if count == offset {
				return curr.ID, nil
			}
		}
		curr = curr.Next
	}
	return StartID, fmt.Errorf("offset %d out of bounds (length %d)", offset, count)
}

// GetNodesCount returns the total number of nodes in the RGA tree, including tombstones.
func (doc *Document) GetNodesCount() int {
	doc.RLock()
	defer doc.RUnlock()
	return len(doc.nodes)
}
