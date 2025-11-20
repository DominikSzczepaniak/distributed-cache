package sharding

import (
	"sync"
	"testing"
)

func TestNewPartitionTable(t *testing.T) {
	pt := NewPartitionTable()
	if pt == nil {
		t.Fatal("NewPartitionTable returned nil")
	}
	if pt.GetAssignmentCount() != 0 {
		t.Errorf("Expected empty partition table, got %d assignments", pt.GetAssignmentCount())
	}
	if pt.GetVersion() != 0 {
		t.Errorf("Expected version 0, got %d", pt.GetVersion())
	}
}

func TestAssignAndGetOwner(t *testing.T) {
	pt := NewPartitionTable()

	// Test assignment
	pt.Assign(0, 1)
	pt.Assign(100, 2)
	pt.Assign(16383, 3)

	// Test retrieval
	tests := []struct {
		partition PartitionID
		wantNode  NodeID
		wantFound bool
	}{
		{0, 1, true},
		{100, 2, true},
		{16383, 3, true},
		{999, -1, false}, // Not assigned
	}

	for _, tt := range tests {
		gotNode, gotFound := pt.GetOwner(tt.partition)
		if gotFound != tt.wantFound {
			t.Errorf("GetOwner(%d): found = %v, want %v", tt.partition, gotFound, tt.wantFound)
		}
		if gotFound && gotNode != tt.wantNode {
			t.Errorf("GetOwner(%d): node = %d, want %d", tt.partition, gotNode, tt.wantNode)
		}
	}
}

func TestAssignRange(t *testing.T) {
	pt := NewPartitionTable()

	// Assign range of partitions to node 1
	err := pt.AssignRange(0, 99, 1)
	if err != nil {
		t.Fatalf("AssignRange failed: %v", err)
	}

	// Verify all partitions in range
	for pid := PartitionID(0); pid <= 99; pid++ {
		node, found := pt.GetOwner(pid)
		if !found {
			t.Errorf("Partition %d not assigned", pid)
		}
		if node != 1 {
			t.Errorf("Partition %d: got node %d, want 1", pid, node)
		}
	}

	// Verify partition outside range is not assigned
	_, found := pt.GetOwner(100)
	if found {
		t.Error("Partition 100 should not be assigned")
	}
}

func TestAssignRangeInvalid(t *testing.T) {
	pt := NewPartitionTable()

	// Invalid range: start > end
	err := pt.AssignRange(100, 50, 1)
	if err == nil {
		t.Error("Expected error for invalid range, got nil")
	}
}

func TestGetAssignments(t *testing.T) {
	pt := NewPartitionTable()

	// Assign some partitions
	pt.Assign(0, 1)
	pt.Assign(100, 2)
	pt.Assign(200, 3)

	assignments := pt.GetAssignments()

	// Verify copy is correct
	if len(assignments) != 3 {
		t.Errorf("Expected 3 assignments, got %d", len(assignments))
	}

	// Verify values
	expected := map[PartitionID]NodeID{
		0:   1,
		100: 2,
		200: 3,
	}

	for pid, expectedNode := range expected {
		if gotNode, ok := assignments[pid]; !ok {
			t.Errorf("Partition %d missing in assignments", pid)
		} else if gotNode != expectedNode {
			t.Errorf("Partition %d: got node %d, want %d", pid, gotNode, expectedNode)
		}
	}

	// Verify it's a copy (modifying returned map doesn't affect original)
	assignments[999] = 99
	if _, found := pt.GetOwner(999); found {
		t.Error("Modifying returned assignments map affected original")
	}
}

func TestGetNodePartitions(t *testing.T) {
	pt := NewPartitionTable()

	// Assign partitions to different nodes
	pt.Assign(0, 1)
	pt.Assign(10, 1)
	pt.Assign(20, 1)
	pt.Assign(100, 2)
	pt.Assign(200, 2)
	pt.Assign(300, 3)

	// Test node 1's partitions
	node1Partitions := pt.GetNodePartitions(1)
	if len(node1Partitions) != 3 {
		t.Errorf("Node 1: expected 3 partitions, got %d", len(node1Partitions))
	}

	// Verify all partitions belong to node 1
	for _, pid := range node1Partitions {
		node, _ := pt.GetOwner(pid)
		if node != 1 {
			t.Errorf("Partition %d should belong to node 1, got node %d", pid, node)
		}
	}

	// Test node 2's partitions
	node2Partitions := pt.GetNodePartitions(2)
	if len(node2Partitions) != 2 {
		t.Errorf("Node 2: expected 2 partitions, got %d", len(node2Partitions))
	}

	// Test non-existent node
	node99Partitions := pt.GetNodePartitions(99)
	if len(node99Partitions) != 0 {
		t.Errorf("Node 99: expected 0 partitions, got %d", len(node99Partitions))
	}
}

func TestVersionTracking(t *testing.T) {
	pt := NewPartitionTable()

	initialVersion := pt.GetVersion()
	if initialVersion != 0 {
		t.Errorf("Initial version: got %d, want 0", initialVersion)
	}

	// Single assignment increments version
	pt.Assign(0, 1)
	if pt.GetVersion() != 1 {
		t.Errorf("After Assign: got version %d, want 1", pt.GetVersion())
	}

	// Range assignment increments version
	pt.AssignRange(10, 20, 2)
	if pt.GetVersion() != 2 {
		t.Errorf("After AssignRange: got version %d, want 2", pt.GetVersion())
	}

	// Clear increments version
	pt.Clear()
	if pt.GetVersion() != 3 {
		t.Errorf("After Clear: got version %d, want 3", pt.GetVersion())
	}
}

func TestApplyUpdate(t *testing.T) {
	pt := NewPartitionTable()

	// Initial assignments
	pt.Assign(0, 1)
	pt.Assign(100, 2)

	// Apply update that replaces all assignments
	newAssignments := map[PartitionID]NodeID{
		50:  3,
		150: 4,
		250: 5,
	}

	pt.ApplyUpdate(newAssignments, 10)

	// Verify old assignments are gone
	if _, found := pt.GetOwner(0); found {
		t.Error("Old assignment for partition 0 should be removed")
	}
	if _, found := pt.GetOwner(100); found {
		t.Error("Old assignment for partition 100 should be removed")
	}

	// Verify new assignments exist
	for pid, expectedNode := range newAssignments {
		node, found := pt.GetOwner(pid)
		if !found {
			t.Errorf("Partition %d should be assigned", pid)
		}
		if node != expectedNode {
			t.Errorf("Partition %d: got node %d, want %d", pid, node, expectedNode)
		}
	}

	// Verify version
	if pt.GetVersion() != 10 {
		t.Errorf("Version: got %d, want 10", pt.GetVersion())
	}
}

func TestClear(t *testing.T) {
	pt := NewPartitionTable()

	// Add some assignments
	pt.Assign(0, 1)
	pt.Assign(100, 2)
	pt.Assign(200, 3)

	// Clear
	pt.Clear()

	// Verify all assignments removed
	if pt.GetAssignmentCount() != 0 {
		t.Errorf("After Clear: expected 0 assignments, got %d", pt.GetAssignmentCount())
	}

	// Verify partitions are unassigned
	if _, found := pt.GetOwner(0); found {
		t.Error("Partition 0 should be unassigned after Clear")
	}
}

func TestClone(t *testing.T) {
	pt := NewPartitionTable()

	// Populate original
	pt.Assign(0, 1)
	pt.Assign(100, 2)
	pt.Assign(200, 3)

	// Clone
	clone := pt.Clone()

	// Verify clone has same data
	if clone.GetAssignmentCount() != pt.GetAssignmentCount() {
		t.Errorf("Clone: expected %d assignments, got %d",
			pt.GetAssignmentCount(), clone.GetAssignmentCount())
	}

	if clone.GetVersion() != pt.GetVersion() {
		t.Errorf("Clone: expected version %d, got %d",
			pt.GetVersion(), clone.GetVersion())
	}

	// Verify all assignments match
	for pid := PartitionID(0); pid < 300; pid++ {
		origNode, origFound := pt.GetOwner(pid)
		cloneNode, cloneFound := clone.GetOwner(pid)

		if origFound != cloneFound {
			t.Errorf("Partition %d: found mismatch (orig=%v, clone=%v)",
				pid, origFound, cloneFound)
		}
		if origFound && origNode != cloneNode {
			t.Errorf("Partition %d: node mismatch (orig=%d, clone=%d)",
				pid, origNode, cloneNode)
		}
	}

	// Verify they are independent (modifying clone doesn't affect original)
	clone.Assign(999, 99)
	if _, found := pt.GetOwner(999); found {
		t.Error("Modifying clone affected original")
	}
}

func TestInitializeEvenDistribution(t *testing.T) {
	tests := []struct {
		name             string
		totalPartitions  uint16
		nodeIDs          []NodeID
		wantAssignments  int
		wantMaxVariance  int // Maximum difference in partitions between nodes
	}{
		{
			name:            "3 nodes, 9 partitions",
			totalPartitions: 9,
			nodeIDs:         []NodeID{0, 1, 2},
			wantAssignments: 9,
			wantMaxVariance: 0, // 3, 3, 3 - perfectly even
		},
		{
			name:            "3 nodes, 10 partitions",
			totalPartitions: 10,
			nodeIDs:         []NodeID{0, 1, 2},
			wantAssignments: 10,
			wantMaxVariance: 1, // 4, 3, 3 - remainder distributed
		},
		{
			name:            "Single node",
			totalPartitions: 100,
			nodeIDs:         []NodeID{0},
			wantAssignments: 100,
			wantMaxVariance: 0,
		},
		{
			name:            "Empty nodes",
			totalPartitions: 100,
			nodeIDs:         []NodeID{},
			wantAssignments: 0,
			wantMaxVariance: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pt := InitializeEvenDistribution(tt.totalPartitions, tt.nodeIDs)

			// Verify total assignments
			if pt.GetAssignmentCount() != tt.wantAssignments {
				t.Errorf("Expected %d assignments, got %d",
					tt.wantAssignments, pt.GetAssignmentCount())
			}

			// Verify all partitions are assigned
			for pid := PartitionID(0); pid < PartitionID(tt.totalPartitions); pid++ {
				if _, found := pt.GetOwner(pid); !found && len(tt.nodeIDs) > 0 {
					t.Errorf("Partition %d is not assigned", pid)
				}
			}

			// Verify distribution is even
			if len(tt.nodeIDs) > 0 {
				nodeCounts := make(map[NodeID]int)
				for pid := PartitionID(0); pid < PartitionID(tt.totalPartitions); pid++ {
					node, _ := pt.GetOwner(pid)
					nodeCounts[node]++
				}

				// Find min and max partition counts
				minCount, maxCount := int(tt.totalPartitions), 0
				for _, count := range nodeCounts {
					if count < minCount {
						minCount = count
					}
					if count > maxCount {
						maxCount = count
					}
				}

				variance := maxCount - minCount
				if variance > tt.wantMaxVariance {
					t.Errorf("Distribution variance too high: got %d, want max %d",
						variance, tt.wantMaxVariance)
				}
			}

			// Verify version is set
			if len(tt.nodeIDs) > 0 && pt.GetVersion() != 1 {
				t.Errorf("Expected version 1, got %d", pt.GetVersion())
			}
		})
	}
}

func TestConcurrentAccess(t *testing.T) {
	pt := NewPartitionTable()

	// Initialize with some data
	pt.AssignRange(0, 999, 1)

	var wg sync.WaitGroup
	goroutines := 100
	operationsPerGoroutine := 100

	// Concurrent readers
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < operationsPerGoroutine; j++ {
				pid := PartitionID(j % 1000)
				pt.GetOwner(pid)
				pt.GetAssignments()
				pt.GetNodePartitions(1)
			}
		}(i)
	}

	// Concurrent writers
	for i := 0; i < goroutines/10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < operationsPerGoroutine; j++ {
				pid := PartitionID((id*1000 + j) % 1000)
				pt.Assign(pid, NodeID(id%3))
			}
		}(i)
	}

	wg.Wait()

	// Verify table is still consistent
	if pt.GetAssignmentCount() == 0 {
		t.Error("Partition table is empty after concurrent operations")
	}
}

func TestSerializationRoundTrip(t *testing.T) {
	pt := NewPartitionTable()

	// Populate with various assignments
	pt.Assign(0, 1)
	pt.Assign(100, 2)
	pt.Assign(1000, 3)
	pt.Assign(16383, 4)

	// Serialize
	data, err := pt.Serialize()
	if err != nil {
		t.Fatalf("Serialize failed: %v", err)
	}

	// Deserialize into new table
	pt2 := NewPartitionTable()
	if err := pt2.Deserialize(data); err != nil {
		t.Fatalf("Deserialize failed: %v", err)
	}

	// Verify they match
	if pt2.GetAssignmentCount() != pt.GetAssignmentCount() {
		t.Errorf("Assignment count mismatch: got %d, want %d",
			pt2.GetAssignmentCount(), pt.GetAssignmentCount())
	}

	if pt2.GetVersion() != pt.GetVersion() {
		t.Errorf("Version mismatch: got %d, want %d",
			pt2.GetVersion(), pt.GetVersion())
	}

	// Verify all assignments
	assignments := pt.GetAssignments()
	for pid, node := range assignments {
		node2, found := pt2.GetOwner(pid)
		if !found {
			t.Errorf("Partition %d missing after deserialization", pid)
		}
		if node2 != node {
			t.Errorf("Partition %d: got node %d, want %d", pid, node2, node)
		}
	}
}

func TestDeserializeEmpty(t *testing.T) {
	pt := NewPartitionTable()
	pt.Assign(0, 1) // Add something first

	// Deserialize empty data
	err := pt.Deserialize([]byte{})
	if err != nil {
		t.Fatalf("Deserialize empty data failed: %v", err)
	}

	// Verify table is empty
	if pt.GetAssignmentCount() != 0 {
		t.Errorf("Expected empty table, got %d assignments", pt.GetAssignmentCount())
	}
	if pt.GetVersion() != 0 {
		t.Errorf("Expected version 0, got %d", pt.GetVersion())
	}
}

func BenchmarkAssign(b *testing.B) {
	pt := NewPartitionTable()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		pt.Assign(PartitionID(i%16384), NodeID(i%10))
	}
}

func BenchmarkGetOwner(b *testing.B) {
	pt := NewPartitionTable()
	pt.AssignRange(0, 16383, 1)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		pt.GetOwner(PartitionID(i % 16384))
	}
}

func BenchmarkGetAssignments(b *testing.B) {
	pt := NewPartitionTable()
	pt.AssignRange(0, 16383, 1)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		pt.GetAssignments()
	}
}

func BenchmarkSerialize(b *testing.B) {
	pt := InitializeEvenDistribution(16384, []NodeID{0, 1, 2})
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := pt.Serialize()
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDeserialize(b *testing.B) {
	pt := InitializeEvenDistribution(16384, []NodeID{0, 1, 2})
	data, err := pt.Serialize()
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pt2 := NewPartitionTable()
		if err := pt2.Deserialize(data); err != nil {
			b.Fatal(err)
		}
	}
}
