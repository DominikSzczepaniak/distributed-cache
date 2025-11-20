package sharding

import (
	"fmt"
	"sync"
	"testing"
)

// TestNewShardManager verifies correct initialization
func TestNewShardManager(t *testing.T) {
	pt := NewPartitionTable()
	partitioner := NewPartitioner()
	nodeID := NodeID(1)

	sm := NewShardManager(nodeID, pt, partitioner)

	if sm == nil {
		t.Fatal("NewShardManager returned nil")
	}
	if sm.nodeID != nodeID {
		t.Errorf("Expected nodeID %d, got %d", nodeID, sm.nodeID)
	}
	if sm.partitionTable != pt {
		t.Error("Partition table not properly assigned")
	}
	if sm.partitioner != partitioner {
		t.Error("Partitioner not properly assigned")
	}
	if sm.peerAddresses == nil {
		t.Error("peerAddresses map not initialized")
	}
}

// TestValidateKey_LocalKey verifies correct behavior when key belongs to local node
func TestValidateKey_LocalKey(t *testing.T) {
	pt := NewPartitionTable()
	partitioner := NewPartitioner()
	nodeID := NodeID(1)

	// Assign partition 0 to node 1
	pt.Assign(0, nodeID)

	sm := NewShardManager(nodeID, pt, partitioner)

	// Find a key that hashes to partition 0
	testKey := "test"
	if partitioner.HashKey(testKey) != 0 {
		// Find a key that actually maps to partition 0
		for i := 0; i < 10000; i++ {
			testKey = fmt.Sprintf("key%d", i)
			if partitioner.HashKey(testKey) == 0 {
				break
			}
		}
	}

	err := sm.ValidateKey(testKey)
	if err != nil {
		t.Errorf("Expected nil error for local key, got %v", err)
	}

	// Verify metrics
	metrics := sm.GetMetrics()
	if metrics["local_hits"] != 1 {
		t.Errorf("Expected 1 local hit, got %d", metrics["local_hits"])
	}
	if metrics["redirects"] != 0 {
		t.Errorf("Expected 0 redirects, got %d", metrics["redirects"])
	}
}

// TestValidateKey_RemoteKey verifies redirect behavior when key belongs to different node
func TestValidateKey_RemoteKey(t *testing.T) {
	pt := NewPartitionTable()
	partitioner := NewPartitioner()
	currentNode := NodeID(1)
	correctNode := NodeID(2)
	correctAddr := "http://localhost:10001"

	// Assign partition 0 to node 2 (not current node)
	pt.Assign(0, correctNode)

	sm := NewShardManager(currentNode, pt, partitioner)
	sm.UpdatePeerAddress(correctNode, correctAddr)

	// Find a key that hashes to partition 0
	testKey := "test"
	for i := 0; i < 10000; i++ {
		testKey = fmt.Sprintf("key%d", i)
		if partitioner.HashKey(testKey) == 0 {
			break
		}
	}

	err := sm.ValidateKey(testKey)
	if err == nil {
		t.Fatal("Expected error for remote key, got nil")
	}

	// Verify it's a WrongNodeError
	wrongNodeErr, ok := err.(*WrongNodeError)
	if !ok {
		t.Fatalf("Expected WrongNodeError, got %T", err)
	}

	if wrongNodeErr.CurrentNode != currentNode {
		t.Errorf("Expected current node %d, got %d", currentNode, wrongNodeErr.CurrentNode)
	}
	if wrongNodeErr.CorrectNode != correctNode {
		t.Errorf("Expected correct node %d, got %d", correctNode, wrongNodeErr.CorrectNode)
	}
	if wrongNodeErr.CorrectAddr != correctAddr {
		t.Errorf("Expected address %s, got %s", correctAddr, wrongNodeErr.CorrectAddr)
	}
	if wrongNodeErr.PartitionID != 0 {
		t.Errorf("Expected partition 0, got %d", wrongNodeErr.PartitionID)
	}

	// Verify metrics
	metrics := sm.GetMetrics()
	if metrics["redirects"] != 1 {
		t.Errorf("Expected 1 redirect, got %d", metrics["redirects"])
	}
	if metrics["local_hits"] != 0 {
		t.Errorf("Expected 0 local hits, got %d", metrics["local_hits"])
	}
}

// TestValidateKey_UnassignedPartition verifies error when partition has no owner
func TestValidateKey_UnassignedPartition(t *testing.T) {
	pt := NewPartitionTable()
	partitioner := NewPartitioner()
	nodeID := NodeID(1)

	sm := NewShardManager(nodeID, pt, partitioner)

	// All partitions are unassigned
	testKey := "test"
	err := sm.ValidateKey(testKey)
	if err == nil {
		t.Fatal("Expected error for unassigned partition, got nil")
	}

	expectedMsg := fmt.Sprintf("no owner assigned for partition %d", partitioner.HashKey(testKey))
	if err.Error() != expectedMsg {
		t.Errorf("Expected error '%s', got '%s'", expectedMsg, err.Error())
	}

	// Verify metrics
	metrics := sm.GetMetrics()
	if metrics["errors"] != 1 {
		t.Errorf("Expected 1 error, got %d", metrics["errors"])
	}
}

// TestValidateKey_MissingPeerAddress verifies error when peer address is not registered
func TestValidateKey_MissingPeerAddress(t *testing.T) {
	pt := NewPartitionTable()
	partitioner := NewPartitioner()
	currentNode := NodeID(1)
	correctNode := NodeID(2)

	// Assign partition 0 to node 2
	pt.Assign(0, correctNode)

	sm := NewShardManager(currentNode, pt, partitioner)
	// Note: NOT registering peer address for node 2

	// Find a key that hashes to partition 0
	testKey := "test"
	for i := 0; i < 10000; i++ {
		testKey = fmt.Sprintf("key%d", i)
		if partitioner.HashKey(testKey) == 0 {
			break
		}
	}

	err := sm.ValidateKey(testKey)
	if err == nil {
		t.Fatal("Expected error for missing peer address, got nil")
	}

	expectedMsg := fmt.Sprintf("no address found for node %d", correctNode)
	if err.Error() != expectedMsg {
		t.Errorf("Expected error '%s', got '%s'", expectedMsg, err.Error())
	}

	// Verify metrics (should count as error AND redirect)
	metrics := sm.GetMetrics()
	if metrics["redirects"] != 1 {
		t.Errorf("Expected 1 redirect, got %d", metrics["redirects"])
	}
	if metrics["errors"] != 1 {
		t.Errorf("Expected 1 error, got %d", metrics["errors"])
	}
}

// TestGetNodeForKey verifies correct node ID lookup
func TestGetNodeForKey(t *testing.T) {
	pt := NewPartitionTable()
	partitioner := NewPartitioner()
	nodeID := NodeID(1)
	targetNode := NodeID(5)

	// Assign partition 100 to node 5
	pt.Assign(100, targetNode)

	sm := NewShardManager(nodeID, pt, partitioner)

	// Find a key that hashes to partition 100
	var testKey string
	for i := 0; i < 100000; i++ {
		testKey = fmt.Sprintf("key%d", i)
		if partitioner.HashKey(testKey) == 100 {
			break
		}
	}

	resultNode, err := sm.GetNodeForKey(testKey)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if resultNode != targetNode {
		t.Errorf("Expected node %d, got %d", targetNode, resultNode)
	}
}

// TestGetNodeForKey_UnassignedPartition verifies error for unassigned partition
func TestGetNodeForKey_UnassignedPartition(t *testing.T) {
	pt := NewPartitionTable()
	partitioner := NewPartitioner()
	nodeID := NodeID(1)

	sm := NewShardManager(nodeID, pt, partitioner)

	testKey := "test"
	_, err := sm.GetNodeForKey(testKey)
	if err == nil {
		t.Fatal("Expected error for unassigned partition, got nil")
	}

	expectedMsg := fmt.Sprintf("no owner assigned for partition %d", partitioner.HashKey(testKey))
	if err.Error() != expectedMsg {
		t.Errorf("Expected error '%s', got '%s'", expectedMsg, err.Error())
	}
}

// TestIsLocalKey verifies boolean helper method
func TestIsLocalKey(t *testing.T) {
	pt := NewPartitionTable()
	partitioner := NewPartitioner()
	nodeID := NodeID(1)

	pt.Assign(0, nodeID)
	pt.Assign(1, NodeID(2))

	sm := NewShardManager(nodeID, pt, partitioner)

	// Find keys for partitions 0 and 1
	var localKey, remoteKey string
	foundLocal, foundRemote := false, false

	for i := 0; i < 100000 && (!foundLocal || !foundRemote); i++ {
		testKey := fmt.Sprintf("key%d", i)
		pid := partitioner.HashKey(testKey)
		if pid == 0 && !foundLocal {
			localKey = testKey
			foundLocal = true
		}
		if pid == 1 && !foundRemote {
			remoteKey = testKey
			foundRemote = true
		}
	}

	if !foundLocal || !foundRemote {
		t.Fatal("Could not find test keys for both partitions")
	}

	// Test local key
	if !sm.IsLocalKey(localKey) {
		t.Error("Expected IsLocalKey=true for local key")
	}

	// Test remote key
	sm.UpdatePeerAddress(NodeID(2), "http://localhost:10001")
	if sm.IsLocalKey(remoteKey) {
		t.Error("Expected IsLocalKey=false for remote key")
	}
}

// TestUpdatePeerAddress verifies peer address management
func TestUpdatePeerAddress(t *testing.T) {
	pt := NewPartitionTable()
	partitioner := NewPartitioner()
	nodeID := NodeID(1)

	sm := NewShardManager(nodeID, pt, partitioner)

	peerID := NodeID(2)
	peerAddr := "http://localhost:10001"

	sm.UpdatePeerAddress(peerID, peerAddr)

	addr, exists := sm.GetNodeAddress(peerID)
	if !exists {
		t.Fatal("Peer address not found after update")
	}
	if addr != peerAddr {
		t.Errorf("Expected address %s, got %s", peerAddr, addr)
	}
}

// TestGetNodeAddress verifies address retrieval
func TestGetNodeAddress(t *testing.T) {
	pt := NewPartitionTable()
	partitioner := NewPartitioner()
	nodeID := NodeID(1)

	sm := NewShardManager(nodeID, pt, partitioner)

	// Test missing address
	_, exists := sm.GetNodeAddress(NodeID(99))
	if exists {
		t.Error("Expected false for missing peer address")
	}

	// Test existing address
	peerID := NodeID(2)
	peerAddr := "http://localhost:10001"
	sm.UpdatePeerAddress(peerID, peerAddr)

	addr, exists := sm.GetNodeAddress(peerID)
	if !exists {
		t.Fatal("Expected true for existing peer address")
	}
	if addr != peerAddr {
		t.Errorf("Expected address %s, got %s", peerAddr, addr)
	}
}

// TestShardManagerConcurrency tests thread-safety under concurrent access
func TestShardManagerConcurrency(t *testing.T) {
	pt := NewPartitionTable()
	partitioner := NewPartitioner()
	nodeID := NodeID(1)

	// Assign some partitions
	for i := 0; i < 100; i++ {
		pt.Assign(PartitionID(i), NodeID(i%3))
	}

	sm := NewShardManager(nodeID, pt, partitioner)

	// Register peer addresses
	for i := 0; i < 3; i++ {
		sm.UpdatePeerAddress(NodeID(i), fmt.Sprintf("http://localhost:1000%d", i))
	}

	// Concurrent validation
	var wg sync.WaitGroup
	errors := make(chan error, 100)

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			testKey := fmt.Sprintf("concurrent_key_%d", id)
			_ = sm.ValidateKey(testKey)
			// Verify methods are callable without panics
		}(i)
	}

	wg.Wait()
	close(errors)

	// Check no errors occurred
	for err := range errors {
		if err != nil {
			t.Errorf("Concurrent access error: %v", err)
		}
	}

	// Verify metrics are consistent (all operations completed)
	metrics := sm.GetMetrics()
	totalOps := metrics["local_hits"] + metrics["redirects"] + metrics["errors"]
	if totalOps != 100 {
		t.Errorf("Expected 100 total operations, got %d", totalOps)
	}
}

// TestShardManagerConcurrentPeerUpdates tests concurrent peer address updates
func TestShardManagerConcurrentPeerUpdates(t *testing.T) {
	pt := NewPartitionTable()
	partitioner := NewPartitioner()
	nodeID := NodeID(1)

	sm := NewShardManager(nodeID, pt, partitioner)

	var wg sync.WaitGroup
	peerCount := 10
	updateCount := 100

	// Concurrent peer address updates
	for i := 0; i < peerCount; i++ {
		wg.Add(1)
		go func(peerID int) {
			defer wg.Done()
			for j := 0; j < updateCount; j++ {
				addr := fmt.Sprintf("http://localhost:%d", 10000+peerID*100+j)
				sm.UpdatePeerAddress(NodeID(peerID), addr)
			}
		}(i)
	}

	wg.Wait()

	// Verify all peers have addresses
	for i := 0; i < peerCount; i++ {
		_, exists := sm.GetNodeAddress(NodeID(i))
		if !exists {
			t.Errorf("Peer %d address not found after concurrent updates", i)
		}
	}
}

// TestGetMetrics verifies metrics tracking
func TestGetMetrics(t *testing.T) {
	pt := NewPartitionTable()
	partitioner := NewPartitioner()
	nodeID := NodeID(1)

	// Setup: node 1 owns partition 0, node 2 owns partition 1
	pt.Assign(0, nodeID)
	pt.Assign(1, NodeID(2))

	sm := NewShardManager(nodeID, pt, partitioner)
	sm.UpdatePeerAddress(NodeID(2), "http://localhost:10001")

	// Find test keys
	var localKey, remoteKey string
	for i := 0; i < 100000; i++ {
		testKey := fmt.Sprintf("key%d", i)
		pid := partitioner.HashKey(testKey)
		if pid == 0 && localKey == "" {
			localKey = testKey
		}
		if pid == 1 && remoteKey == "" {
			remoteKey = testKey
		}
		if localKey != "" && remoteKey != "" {
			break
		}
	}

	// Execute operations
	sm.ValidateKey(localKey)  // Should be local hit
	sm.ValidateKey(remoteKey) // Should be redirect

	metrics := sm.GetMetrics()
	if metrics["local_hits"] != 1 {
		t.Errorf("Expected 1 local hit, got %d", metrics["local_hits"])
	}
	if metrics["redirects"] != 1 {
		t.Errorf("Expected 1 redirect, got %d", metrics["redirects"])
	}
	if metrics["errors"] != 0 {
		t.Errorf("Expected 0 errors, got %d", metrics["errors"])
	}
}

// TestOnPartitionTableUpdate verifies update notification
func TestOnPartitionTableUpdate(t *testing.T) {
	pt := NewPartitionTable()
	partitioner := NewPartitioner()
	nodeID := NodeID(1)

	sm := NewShardManager(nodeID, pt, partitioner)

	// Update partition table
	pt.Assign(0, nodeID)
	pt.Assign(1, nodeID)

	// Call update notification (just for logging/monitoring)
	sm.OnPartitionTableUpdate() // Should not panic

	// Verify shard manager sees the updates (pointer sharing)
	owner, exists := sm.partitionTable.GetOwner(0)
	if !exists || owner != nodeID {
		t.Error("Partition table update not reflected in shard manager")
	}
}

// TestShardDistribution verifies keys distribute across partitions
func TestShardDistribution(t *testing.T) {
	pt := NewPartitionTable()
	partitioner := NewPartitioner()
	nodeID := NodeID(1)

	// Assign first 1000 partitions to node 1, rest to node 2
	for i := 0; i < 1000; i++ {
		pt.Assign(PartitionID(i), nodeID)
	}
	for i := 1000; i < TOTAL_PARTITIONS; i++ {
		pt.Assign(PartitionID(i), NodeID(2))
	}

	sm := NewShardManager(nodeID, pt, partitioner)
	sm.UpdatePeerAddress(NodeID(2), "http://localhost:10001")

	// Generate 10,000 keys and count local vs remote
	localCount := 0
	for i := 0; i < 10000; i++ {
		testKey := fmt.Sprintf("distribution_test_%d", i)
		if sm.IsLocalKey(testKey) {
			localCount++
		}
	}

	// Expect roughly 6% local keys (1000/16384 ≈ 6.1%)
	// Allow 50% variance: 3% to 9%
	expectedMin := 300
	expectedMax := 900
	if localCount < expectedMin || localCount > expectedMax {
		t.Errorf("Expected local keys between %d and %d, got %d",
			expectedMin, expectedMax, localCount)
	}
}
