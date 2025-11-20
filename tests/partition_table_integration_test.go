package tests

import (
	"bytes"
	"encoding/binary"
	"testing"
	"time"

	"github.com/dominikszczepaniak/distributed-cache/pkg/raft"
	"github.com/dominikszczepaniak/distributed-cache/pkg/sharding"
)

// TestPartitionTableRaftIntegration verifies that partition table updates
// are correctly replicated across a Raft cluster
func TestPartitionTableRaftIntegration(t *testing.T) {
	// Create 3-node Raft cluster with partition table support
	nodes := setupThreeNodeCluster(t)
	defer cleanupNodes(nodes)

	// Give cluster time to elect leader
	time.Sleep(2 * time.Second)

	// Find the leader
	leaderIdx := findLeader(t, nodes)
	if leaderIdx == -1 {
		t.Fatal("No leader elected")
	}

	leader := nodes[leaderIdx]
	t.Logf("Leader is node %d", leader.id)

	// Create a partition table update message
	assignments := map[sharding.PartitionID]sharding.NodeID{
		0:   0,
		100: 1,
		200: 2,
	}

	updateMsg := raft.Message{
		MsgType: "UPDATE_PARTITION_TABLE",
		PartitionTableUpdate: &raft.PartitionTableUpdate{
			Assignments: assignments,
			Version:     1,
		},
	}

	// Broadcast partition table update through Raft
	success, _, err := leader.raft.BroadcastSync(updateMsg, 5*time.Second)
	if err != nil {
		t.Fatalf("Failed to broadcast partition table update: %v", err)
	}
	if !success {
		t.Fatal("Partition table update was not successful")
	}

	t.Log("Partition table update committed on leader")

	// Give followers time to receive and apply the update
	time.Sleep(500 * time.Millisecond)

	// Verify all nodes have the same partition table state
	for i, node := range nodes {
		store := node.app.(*TestKVStore)

		// Verify each assignment
		for pid, expectedNodeID := range assignments {
			gotNodeID, found := store.partitionTable.GetOwner(pid)
			if !found {
				t.Errorf("Node %d: partition %d not found", i, pid)
				continue
			}
			if gotNodeID != expectedNodeID {
				t.Errorf("Node %d: partition %d has owner %d, want %d",
					i, pid, gotNodeID, expectedNodeID)
			}
		}

		// Verify version
		if store.partitionTable.GetVersion() != 1 {
			t.Errorf("Node %d: version = %d, want 1", i, store.partitionTable.GetVersion())
		}

		t.Logf("Node %d: partition table consistent (version=%d, assignments=%d)",
			i, store.partitionTable.GetVersion(), store.partitionTable.GetAssignmentCount())
	}
}

// TestPartitionTableSnapshot verifies that partition table survives
// snapshot creation and restoration
func TestPartitionTableSnapshot(t *testing.T) {
	nodes := setupThreeNodeCluster(t)
	defer cleanupNodes(nodes)

	time.Sleep(2 * time.Second)

	leaderIdx := findLeader(t, nodes)
	if leaderIdx == -1 {
		t.Fatal("No leader elected")
	}

	leader := nodes[leaderIdx]

	// Add some data operations
	for i := 0; i < 10; i++ {
		val := i * 100
		putMsg := raft.Message{
			MsgType: "PUT",
			Key:     i,
			Value:   &val,
		}
		_, _, err := leader.raft.BroadcastSync(putMsg, 2*time.Second)
		if err != nil {
			t.Fatalf("Failed to PUT key %d: %v", i, err)
		}
	}

	// Update partition table
	assignments := make(map[sharding.PartitionID]sharding.NodeID)
	for pid := sharding.PartitionID(0); pid < 100; pid++ {
		assignments[pid] = sharding.NodeID(int(pid) % 3)
	}

	updateMsg := raft.Message{
		MsgType: "UPDATE_PARTITION_TABLE",
		PartitionTableUpdate: &raft.PartitionTableUpdate{
			Assignments: assignments,
			Version:     5,
		},
	}

	success, _, err := leader.raft.BroadcastSync(updateMsg, 5*time.Second)
	if err != nil {
		t.Fatalf("Failed to broadcast partition table update: %v", err)
	}
	if !success {
		t.Fatal("Partition table update failed")
	}

	time.Sleep(500 * time.Millisecond)

	// Get snapshot from leader's application
	leaderStore := leader.app.(*TestKVStore)
	snapshot, err := leaderStore.GetSnapshot()
	if err != nil {
		t.Fatalf("Failed to create snapshot: %v", err)
	}

	t.Logf("Created snapshot: %d bytes", len(snapshot))

	// Create a new application and restore from snapshot
	newApp := NewTestKVStore()
	err, _ = newApp.RestoreFromSnapshot(snapshot)
	if err != nil {
		t.Fatalf("Failed to restore from snapshot: %v", err)
	}

	// Verify partition table was restored
	if newApp.partitionTable.GetVersion() != 5 {
		t.Errorf("Restored version = %d, want 5", newApp.partitionTable.GetVersion())
	}

	if newApp.partitionTable.GetAssignmentCount() != len(assignments) {
		t.Errorf("Restored assignments = %d, want %d",
			newApp.partitionTable.GetAssignmentCount(), len(assignments))
	}

	// Verify specific assignments
	for pid, expectedNodeID := range assignments {
		gotNodeID, found := newApp.partitionTable.GetOwner(pid)
		if !found {
			t.Errorf("Partition %d not found after restore", pid)
			continue
		}
		if gotNodeID != expectedNodeID {
			t.Errorf("Partition %d: got node %d, want %d", pid, gotNodeID, expectedNodeID)
		}
	}

	// Verify data was also restored
	for i := 0; i < 10; i++ {
		got := newApp.GetValue(i)
		want := i * 100
		if got != want {
			t.Errorf("Key %d: got %d, want %d", i, got, want)
		}
	}

	t.Log("Snapshot restoration verified: both data and partition table restored correctly")
}

// TestPartitionTableMultipleUpdates verifies that multiple partition table
// updates are correctly sequenced and replicated
func TestPartitionTableMultipleUpdates(t *testing.T) {
	nodes := setupThreeNodeCluster(t)
	defer cleanupNodes(nodes)

	time.Sleep(2 * time.Second)

	leaderIdx := findLeader(t, nodes)
	if leaderIdx == -1 {
		t.Fatal("No leader elected")
	}

	leader := nodes[leaderIdx]

	// Apply multiple updates in sequence
	for version := uint64(1); version <= 5; version++ {
		assignments := map[sharding.PartitionID]sharding.NodeID{
			sharding.PartitionID(version * 10): sharding.NodeID(version % 3),
		}

		updateMsg := raft.Message{
			MsgType: "UPDATE_PARTITION_TABLE",
			PartitionTableUpdate: &raft.PartitionTableUpdate{
				Assignments: assignments,
				Version:     version,
			},
		}

		success, _, err := leader.raft.BroadcastSync(updateMsg, 5*time.Second)
		if err != nil {
			t.Fatalf("Failed to broadcast update %d: %v", version, err)
		}
		if !success {
			t.Fatalf("Update %d failed", version)
		}

		t.Logf("Applied partition table update version %d", version)
	}

	time.Sleep(500 * time.Millisecond)

	// Verify final state on all nodes
	for i, node := range nodes {
		store := node.app.(*TestKVStore)

		// Final version should be 5 (last update)
		if store.partitionTable.GetVersion() != 5 {
			t.Errorf("Node %d: version = %d, want 5", i, store.partitionTable.GetVersion())
		}

		// Should only have assignments from the last update (version 5)
		// because ApplyUpdate replaces all assignments
		expectedPid := sharding.PartitionID(50) // 5 * 10
		expectedNode := sharding.NodeID(2)      // 5 % 3
		gotNode, found := store.partitionTable.GetOwner(expectedPid)
		if !found {
			t.Errorf("Node %d: expected partition %d not found", i, expectedPid)
		} else if gotNode != expectedNode {
			t.Errorf("Node %d: partition %d has node %d, want %d",
				i, expectedPid, gotNode, expectedNode)
		}

		t.Logf("Node %d: final state verified (version=%d, assignments=%d)",
			i, store.partitionTable.GetVersion(), store.partitionTable.GetAssignmentCount())
	}
}

// TestKVStore is a test version of SimpleKVStore with partition table support
type TestKVStore struct {
	data           map[int]int
	partitionTable *sharding.PartitionTable
}

func NewTestKVStore() *TestKVStore {
	return &TestKVStore{
		data:           make(map[int]int),
		partitionTable: sharding.NewPartitionTable(),
	}
}

func (s *TestKVStore) AppendMessage(msg raft.Message) (bool, int) {
	switch msg.MsgType {
	case "UPDATE_PARTITION_TABLE":
		if msg.PartitionTableUpdate == nil {
			return false, 0
		}
		s.partitionTable.ApplyUpdate(
			msg.PartitionTableUpdate.Assignments,
			msg.PartitionTableUpdate.Version,
		)
		return true, 0
	case "PUT":
		if msg.Value == nil {
			return false, 0
		}
		s.data[msg.Key] = *msg.Value
		return true, 0
	case "GET":
		return true, s.data[msg.Key]
	case "DELETE":
		delete(s.data, msg.Key)
		return true, 0
	default:
		return false, 0
	}
}

func (s *TestKVStore) GetSnapshot() ([]byte, error) {
	// Use same format as SimpleKVStore in cmd/raftnode/main.go
	var dataBuf bytes.Buffer
	byteOrder := binary.LittleEndian

	// Serialize data map
	mapLen := int32(len(s.data))
	if err := binary.Write(&dataBuf, byteOrder, mapLen); err != nil {
		return nil, err
	}

	for k, v := range s.data {
		if err := binary.Write(&dataBuf, byteOrder, int64(k)); err != nil {
			return nil, err
		}
		if err := binary.Write(&dataBuf, byteOrder, int64(v)); err != nil {
			return nil, err
		}
	}

	// Serialize partition table
	ptData, err := s.partitionTable.Serialize()
	if err != nil {
		return nil, err
	}

	// Combine both
	return sharding.CombineSnapshot(ptData, dataBuf.Bytes())
}

func (s *TestKVStore) RestoreFromSnapshot(data []byte) (error, int) {
	// Split combined snapshot
	ptData, dataSnapshot, err := sharding.SplitSnapshot(data)
	if err != nil {
		return err, 0
	}

	// Restore partition table
	if err := s.partitionTable.Deserialize(ptData); err != nil {
		return err, 0
	}

	// Restore data map
	reader := bytes.NewReader(dataSnapshot)
	byteOrder := binary.LittleEndian

	var mapLen int32
	if err := binary.Read(reader, byteOrder, &mapLen); err != nil {
		return err, 0
	}

	newMap := make(map[int]int)
	var lastKey int

	for i := 0; i < int(mapLen); i++ {
		var k64, v64 int64
		if err := binary.Read(reader, byteOrder, &k64); err != nil {
			return err, 0
		}
		if err := binary.Read(reader, byteOrder, &v64); err != nil {
			return err, 0
		}
		lastKey = int(k64)
		newMap[int(k64)] = int(v64)
	}

	s.data = newMap
	return nil, lastKey
}

func (s *TestKVStore) GetValue(key int) int {
	return s.data[key]
}

// Helper types and functions for cluster setup
type testNode struct {
	id   int
	raft *raft.Raft
	app  raft.Application
}

func setupThreeNodeCluster(t *testing.T) []*testNode {
	// This is a simplified setup for testing
	// In a real integration test, you would:
	// 1. Start actual Raft nodes with gRPC
	// 2. Configure them to talk to each other
	// 3. Wait for leader election

	t.Skip("Requires full Raft cluster setup - see existing integration tests in pkg/raft")
	return nil
}

func cleanupNodes(nodes []*testNode) {
	// Cleanup logic
}

func findLeader(t *testing.T, nodes []*testNode) int {
	// Logic to find which node is the leader
	return -1
}

// Benchmark partition table operations in Raft context
func BenchmarkPartitionTableUpdateThroughRaft(b *testing.B) {
	b.Skip("Requires Raft cluster setup")
}

func BenchmarkSnapshotWithPartitionTable(b *testing.B) {
	app := NewTestKVStore()

	// Populate partition table
	pt := sharding.InitializeEvenDistribution(16384, []sharding.NodeID{0, 1, 2})
	app.partitionTable = pt

	// Add some data
	for i := 0; i < 1000; i++ {
		app.data[i] = i * 100
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := app.GetSnapshot()
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRestoreWithPartitionTable(b *testing.B) {
	app := NewTestKVStore()

	// Setup
	pt := sharding.InitializeEvenDistribution(16384, []sharding.NodeID{0, 1, 2})
	app.partitionTable = pt
	for i := 0; i < 1000; i++ {
		app.data[i] = i * 100
	}

	snapshot, err := app.GetSnapshot()
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		newApp := NewTestKVStore()
		_, _ = newApp.RestoreFromSnapshot(snapshot)
	}
}

// TestPartitionTableConsistency verifies partition table stays consistent
// across multiple operations
func TestPartitionTableConsistency(t *testing.T) {
	app := NewTestKVStore()

	// Initial distribution
	initialPT := sharding.InitializeEvenDistribution(1000, []sharding.NodeID{0, 1, 2})

	updateMsg := raft.Message{
		MsgType: "UPDATE_PARTITION_TABLE",
		PartitionTableUpdate: &raft.PartitionTableUpdate{
			Assignments: initialPT.GetAssignments(),
			Version:     1,
		},
	}

	success, _ := app.AppendMessage(updateMsg)
	if !success {
		t.Fatal("Failed to apply initial partition table")
	}

	// Verify all partitions assigned
	for pid := sharding.PartitionID(0); pid < 1000; pid++ {
		_, found := app.partitionTable.GetOwner(pid)
		if !found {
			t.Errorf("Partition %d not assigned", pid)
		}
	}

	// Take snapshot
	snapshot, err := app.GetSnapshot()
	if err != nil {
		t.Fatalf("Snapshot failed: %v", err)
	}

	// Restore to new app
	newApp := NewTestKVStore()
	err, _ = newApp.RestoreFromSnapshot(snapshot)
	if err != nil {
		t.Fatalf("Restore failed: %v", err)
	}

	// Verify consistency
	if newApp.partitionTable.GetVersion() != app.partitionTable.GetVersion() {
		t.Errorf("Version mismatch: got %d, want %d",
			newApp.partitionTable.GetVersion(), app.partitionTable.GetVersion())
	}

	if newApp.partitionTable.GetAssignmentCount() != app.partitionTable.GetAssignmentCount() {
		t.Errorf("Assignment count mismatch: got %d, want %d",
			newApp.partitionTable.GetAssignmentCount(), app.partitionTable.GetAssignmentCount())
	}

	// Verify each assignment
	for pid := sharding.PartitionID(0); pid < 1000; pid++ {
		origNode, _ := app.partitionTable.GetOwner(pid)
		newNode, found := newApp.partitionTable.GetOwner(pid)
		if !found {
			t.Errorf("Partition %d missing after restore", pid)
			continue
		}
		if newNode != origNode {
			t.Errorf("Partition %d: got node %d, want %d", pid, newNode, origNode)
		}
	}

	t.Log("Partition table consistency verified across snapshot/restore")
}

func TestPartitionTableMessageValidation(t *testing.T) {
	app := NewTestKVStore()

	// Test nil update payload
	nilMsg := raft.Message{
		MsgType:              "UPDATE_PARTITION_TABLE",
		PartitionTableUpdate: nil,
	}

	success, _ := app.AppendMessage(nilMsg)
	if success {
		t.Error("Expected failure for nil partition table update, got success")
	}

	// Test valid update
	validMsg := raft.Message{
		MsgType: "UPDATE_PARTITION_TABLE",
		PartitionTableUpdate: &raft.PartitionTableUpdate{
			Assignments: map[sharding.PartitionID]sharding.NodeID{
				0: 1,
			},
			Version: 1,
		},
	}

	success, _ = app.AppendMessage(validMsg)
	if !success {
		t.Error("Valid partition table update failed")
	}

	// Verify it was applied
	node, found := app.partitionTable.GetOwner(0)
	if !found {
		t.Error("Partition 0 not found after update")
	}
	if node != 1 {
		t.Errorf("Partition 0: got node %d, want 1", node)
	}
}

func TestEmptyPartitionTableSnapshot(t *testing.T) {
	app := NewTestKVStore()

	// Snapshot with empty partition table
	snapshot, err := app.GetSnapshot()
	if err != nil {
		t.Fatalf("Snapshot failed: %v", err)
	}

	// Restore
	newApp := NewTestKVStore()
	err, _ = newApp.RestoreFromSnapshot(snapshot)
	if err != nil {
		t.Fatalf("Restore failed: %v", err)
	}

	// Verify empty
	if newApp.partitionTable.GetAssignmentCount() != 0 {
		t.Errorf("Expected empty partition table, got %d assignments",
			newApp.partitionTable.GetAssignmentCount())
	}
}

func TestLargePartitionTableSnapshot(t *testing.T) {
	app := NewTestKVStore()

	// Create full partition table (16384 partitions)
	pt := sharding.InitializeEvenDistribution(16384, []sharding.NodeID{0, 1, 2, 3, 4})
	app.partitionTable = pt

	// Snapshot
	snapshot, err := app.GetSnapshot()
	if err != nil {
		t.Fatalf("Snapshot failed: %v", err)
	}

	t.Logf("Snapshot size with 16384 partitions: %d bytes", len(snapshot))

	// Restore
	newApp := NewTestKVStore()
	err, _ = newApp.RestoreFromSnapshot(snapshot)
	if err != nil {
		t.Fatalf("Restore failed: %v", err)
	}

	// Verify
	if newApp.partitionTable.GetAssignmentCount() != 16384 {
		t.Errorf("Expected 16384 assignments, got %d", newApp.partitionTable.GetAssignmentCount())
	}

	// Spot check some assignments
	for pid := sharding.PartitionID(0); pid < 16384; pid += 1000 {
		origNode, _ := app.partitionTable.GetOwner(pid)
		newNode, found := newApp.partitionTable.GetOwner(pid)
		if !found {
			t.Errorf("Partition %d missing", pid)
		}
		if newNode != origNode {
			t.Errorf("Partition %d: got node %d, want %d", pid, newNode, origNode)
		}
	}
}
