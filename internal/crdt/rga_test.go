package crdt

import (
	"math/rand"
	"testing"
)

// TestBasicOperations tests simple sequential insertions and deletions.
func TestBasicOperations(t *testing.T) {
	doc := NewDocument("siteA")

	// Insert "hello"
	ops := make([]Op, 5)
	var err error
	for i, char := range "hello" {
		ops[i], err = doc.LocalInsert(i, char)
		if err != nil {
			t.Fatalf("Failed local insert at index %d: %v", i, err)
		}
	}

	if doc.ToString() != "hello" {
		t.Errorf("Expected 'hello', got '%s'", doc.ToString())
	}

	// Delete "e" (offset 1 in "hello")
	delOp, err := doc.LocalDelete(1)
	if err != nil {
		t.Fatalf("Failed local delete: %v", err)
	}

	if doc.ToString() != "hllo" {
		t.Errorf("Expected 'hllo', got '%s'", doc.ToString())
	}

	// Apply the same delete again (idempotency)
	applied := doc.Apply(delOp)
	if applied {
		t.Errorf("Expected re-applying delete to return false (idempotent)")
	}
}

// TestConcurrentTieBreaking tests that two concurrent inserts at the same position
// resolve to the same order across different sites.
func TestConcurrentTieBreaking(t *testing.T) {
	// Start with doc "a"
	docA := NewDocument("siteA")
	op1, _ := docA.LocalInsert(0, 'a')

	// Set up second site B
	docB := NewDocument("siteB")
	docB.Apply(op1)

	// Concurrent edits:
	// siteA inserts 'b' after 'a'
	opA, err := docA.LocalInsert(1, 'b')
	if err != nil {
		t.Fatal(err)
	}

	// siteB inserts 'c' after 'a'
	opB, err := docB.LocalInsert(1, 'c')
	if err != nil {
		t.Fatal(err)
	}

	// Sync: Apply opB to siteA, and opA to siteB
	docA.Apply(opB)
	docB.Apply(opA)

	// Both should converge to the exact same document string
	strA := docA.ToString()
	strB := docB.ToString()

	if strA != strB {
		t.Errorf("Divergent states! siteA: '%s', siteB: '%s'", strA, strB)
	}

	// Since SiteID "siteB" > "siteA", and timestamps are both 2,
	// 'c' (siteB) should be ordered before 'b' (siteA) in a descending order,
	// meaning it is placed first after 'a'.
	// So we expect "acb"
	expected := "acb"
	if strA != expected {
		t.Errorf("Expected tie-break to order as '%s', got '%s'", expected, strA)
	}
}

// TestDelayedOperations buffers inserts when the parent node is missing,
// and applies them once the parent arrives.
func TestDelayedOperations(t *testing.T) {
	doc := NewDocument("siteA")

	// op1: insert 'x' at root
	op1 := Op{
		Type:     OpInsert,
		ID:       ID{Timestamp: 1, SiteID: "siteB"},
		ParentID: StartID,
		Value:    'x',
	}

	// op2: insert 'y' after 'x'
	op2 := Op{
		Type:     OpInsert,
		ID:       ID{Timestamp: 2, SiteID: "siteB"},
		ParentID: ID{Timestamp: 1, SiteID: "siteB"},
		Value:    'y',
	}

	// op3: insert 'z' after 'y'
	op3 := Op{
		Type:     OpInsert,
		ID:       ID{Timestamp: 3, SiteID: "siteB"},
		ParentID: ID{Timestamp: 2, SiteID: "siteB"},
		Value:    'z',
	}

	// Apply in reverse order (delayed delivery)
	doc.Apply(op3) // Pending on y
	doc.Apply(op2) // Pending on x
	if doc.ToString() != "" {
		t.Errorf("Expected empty string since parent chain is missing, got '%s'", doc.ToString())
	}

	doc.Apply(op1) // Root element arrives, triggering resolution

	expected := "xyz"
	if doc.ToString() != expected {
		t.Errorf("Expected document to resolve to '%s', got '%s'", expected, doc.ToString())
	}
}

// TestDeleteBeforeInsert tests that a delete received before the insert marks
// the node as deleted immediately upon insertion.
func TestDeleteBeforeInsert(t *testing.T) {
	doc := NewDocument("siteA")

	targetID := ID{Timestamp: 1, SiteID: "siteB"}

	// Delete arrived early
	delOp := Op{
		Type: OpDelete,
		ID:   targetID,
	}
	doc.Apply(delOp)

	// Insert arrives later
	insOp := Op{
		Type:     OpInsert,
		ID:       targetID,
		ParentID: StartID,
		Value:    'w',
	}
	doc.Apply(insOp)

	if doc.ToString() != "" {
		t.Errorf("Expected empty document as character should be deleted, got '%s'", doc.ToString())
	}

	// Total node count should still be 2 (Sentinel + 'w' tombstone)
	if doc.GetNodesCount() != 2 {
		t.Errorf("Expected 2 nodes (sentinel + tombstone), got %d", doc.GetNodesCount())
	}
}

// TestCommutativityProperty runs property-based testing using random permutations of operations.
func TestCommutativityProperty(t *testing.T) {
	rng := rand.New(rand.NewSource(42))

	// Generate a pool of concurrent edits starting from an initial document "root"
	docSource := NewDocument("source")
	var ops []Op

	// Insert base string
	for i, char := range "START" {
		op, _ := docSource.LocalInsert(i, char)
		ops = append(ops, op)
	}

	// Now branch out to simulate multiple sites performing concurrent operations
	sites := []string{"siteX", "siteY", "siteZ"}
	for i := 0; i < 20; i++ {
		// Pick a random site
		site := sites[rng.Intn(len(sites))]
		
		// To simulate concurrent client states, we apply a random prefix of current ops to a temp doc
		tempDoc := NewDocument(site)
		for _, op := range ops {
			if rng.Float32() < 0.8 { // 80% chance of knowing each previous op
				tempDoc.Apply(op)
			}
		}

		// Perform an insert or delete
		contentLen := len(tempDoc.GetContent())
		if contentLen > 0 && rng.Float32() < 0.3 {
			// Delete
			offset := rng.Intn(contentLen)
			op, err := tempDoc.LocalDelete(offset)
			if err == nil {
				ops = append(ops, op)
			}
		} else {
			// Insert
			offset := rng.Intn(contentLen + 1)
			char := rune('A' + rng.Intn(26))
			op, err := tempDoc.LocalInsert(offset, char)
			if err == nil {
				ops = append(ops, op)
			}
		}
	}

	// We now have a list of all operations.
	// Apply these operations in 5 different random orderings to 5 separate replicas.
	replicaCount := 5
	replicas := make([]*Document, replicaCount)
	for i := 0; i < replicaCount; i++ {
		replicas[i] = NewDocument("replica")
	}

	for i := 0; i < replicaCount; i++ {
		// Permute the operations
		permOps := make([]Op, len(ops))
		copy(permOps, ops)
		rng.Shuffle(len(permOps), func(j, k int) {
			permOps[j], permOps[k] = permOps[k], permOps[j]
		})

		// Apply all operations to this replica
		for _, op := range permOps {
			replicas[i].Apply(op)
		}
	}

	// Assert that ALL replicas ended up in the exact same state
	baseString := replicas[0].ToString()
	for i := 1; i < replicaCount; i++ {
		repString := replicas[i].ToString()
		if baseString != repString {
			t.Errorf("Replica %d did not converge! Replica 0: '%s', Replica %d: '%s'", i, baseString, i, repString)
		}
	}
}
