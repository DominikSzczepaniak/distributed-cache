package sharding

import (
	"bytes"
	"testing"
)

func TestSerializeDeserialize(t *testing.T) {
	pt := NewPartitionTable()

	// Add various assignments
	pt.Assign(0, 1)
	pt.Assign(100, 2)
	pt.Assign(1000, 3)
	pt.Assign(16383, 4)

	// Serialize
	data, err := pt.Serialize()
	if err != nil {
		t.Fatalf("Serialize failed: %v", err)
	}

	// Verify data is not empty
	if len(data) == 0 {
		t.Fatal("Serialized data is empty")
	}

	// Deserialize
	pt2 := NewPartitionTable()
	if err := pt2.Deserialize(data); err != nil {
		t.Fatalf("Deserialize failed: %v", err)
	}

	// Verify match
	if pt2.GetVersion() != pt.GetVersion() {
		t.Errorf("Version mismatch: got %d, want %d", pt2.GetVersion(), pt.GetVersion())
	}

	if pt2.GetAssignmentCount() != pt.GetAssignmentCount() {
		t.Errorf("Assignment count mismatch: got %d, want %d",
			pt2.GetAssignmentCount(), pt.GetAssignmentCount())
	}

	// Verify each assignment
	for pid, expectedNode := range pt.GetAssignments() {
		gotNode, found := pt2.GetOwner(pid)
		if !found {
			t.Errorf("Partition %d missing after deserialization", pid)
		}
		if gotNode != expectedNode {
			t.Errorf("Partition %d: got node %d, want %d", pid, gotNode, expectedNode)
		}
	}
}

func TestSerializeEmptyTable(t *testing.T) {
	pt := NewPartitionTable()

	// Serialize empty table
	data, err := pt.Serialize()
	if err != nil {
		t.Fatalf("Serialize failed: %v", err)
	}

	// Deserialize
	pt2 := NewPartitionTable()
	if err := pt2.Deserialize(data); err != nil {
		t.Fatalf("Deserialize failed: %v", err)
	}

	// Verify empty
	if pt2.GetAssignmentCount() != 0 {
		t.Errorf("Expected empty table, got %d assignments", pt2.GetAssignmentCount())
	}
}

func TestDeserializeEmptyData(t *testing.T) {
	pt := NewPartitionTable()
	pt.Assign(0, 1) // Add something first

	// Deserialize empty byte array
	err := pt.Deserialize([]byte{})
	if err != nil {
		t.Fatalf("Deserialize empty data failed: %v", err)
	}

	// Should result in empty table
	if pt.GetAssignmentCount() != 0 {
		t.Errorf("Expected empty table, got %d assignments", pt.GetAssignmentCount())
	}
	if pt.GetVersion() != 0 {
		t.Errorf("Expected version 0, got %d", pt.GetVersion())
	}
}

func TestSerializeWithMagic(t *testing.T) {
	pt := NewPartitionTable()
	pt.Assign(0, 1)
	pt.Assign(100, 2)

	// Serialize with magic
	data, err := pt.SerializeWithMagic()
	if err != nil {
		t.Fatalf("SerializeWithMagic failed: %v", err)
	}

	// Verify magic bytes at the beginning
	if len(data) < len(MAGIC) {
		t.Fatalf("Serialized data too short: %d bytes", len(data))
	}

	magic := string(data[:len(MAGIC)])
	if magic != MAGIC {
		t.Errorf("Magic bytes mismatch: got %s, want %s", magic, MAGIC)
	}

	// Deserialize with magic
	pt2 := NewPartitionTable()
	if err := pt2.DeserializeWithMagic(data); err != nil {
		t.Fatalf("DeserializeWithMagic failed: %v", err)
	}

	// Verify contents
	if pt2.GetAssignmentCount() != pt.GetAssignmentCount() {
		t.Errorf("Assignment count mismatch: got %d, want %d",
			pt2.GetAssignmentCount(), pt.GetAssignmentCount())
	}
}

func TestDeserializeWithMagicInvalid(t *testing.T) {
	pt := NewPartitionTable()

	// Test with wrong magic
	invalidData := []byte("XXXX" + string(make([]byte, 100)))
	err := pt.DeserializeWithMagic(invalidData)
	if err == nil {
		t.Error("Expected error for invalid magic bytes, got nil")
	}

	// Test with data too short
	shortData := []byte("DC")
	err = pt.DeserializeWithMagic(shortData)
	if err == nil {
		t.Error("Expected error for short data, got nil")
	}
}

func TestCombineSnapshot(t *testing.T) {
	// Create partition table data
	pt := NewPartitionTable()
	pt.Assign(0, 1)
	pt.Assign(100, 2)
	ptData, err := pt.Serialize()
	if err != nil {
		t.Fatalf("Failed to serialize partition table: %v", err)
	}

	// Create dummy data snapshot
	dataSnapshot := []byte("this is some data snapshot content")

	// Combine
	combined, err := CombineSnapshot(ptData, dataSnapshot)
	if err != nil {
		t.Fatalf("CombineSnapshot failed: %v", err)
	}

	// Verify combined data is larger than both components
	if len(combined) < len(ptData)+len(dataSnapshot) {
		t.Error("Combined snapshot is smaller than components")
	}

	// Verify magic bytes
	if len(combined) < 4 {
		t.Fatal("Combined snapshot too short")
	}
	magic := string(combined[:4])
	if magic != "DCSH" {
		t.Errorf("Magic bytes mismatch: got %s, want DCSH", magic)
	}
}

func TestSplitSnapshot(t *testing.T) {
	// Create partition table
	pt := NewPartitionTable()
	pt.Assign(0, 1)
	pt.Assign(100, 2)
	pt.Assign(1000, 3)
	ptData, err := pt.Serialize()
	if err != nil {
		t.Fatalf("Failed to serialize partition table: %v", err)
	}

	// Create data snapshot
	originalDataSnapshot := []byte("this is the original data snapshot with some content")

	// Combine
	combined, err := CombineSnapshot(ptData, originalDataSnapshot)
	if err != nil {
		t.Fatalf("CombineSnapshot failed: %v", err)
	}

	// Split
	splitPtData, splitDataSnapshot, err := SplitSnapshot(combined)
	if err != nil {
		t.Fatalf("SplitSnapshot failed: %v", err)
	}

	// Verify partition table data matches
	if !bytes.Equal(splitPtData, ptData) {
		t.Error("Partition table data mismatch after split")
	}

	// Verify data snapshot matches
	if !bytes.Equal(splitDataSnapshot, originalDataSnapshot) {
		t.Errorf("Data snapshot mismatch after split:\ngot:  %s\nwant: %s",
			string(splitDataSnapshot), string(originalDataSnapshot))
	}

	// Verify partition table can be deserialized
	pt2 := NewPartitionTable()
	if err := pt2.Deserialize(splitPtData); err != nil {
		t.Fatalf("Failed to deserialize split partition table: %v", err)
	}

	if pt2.GetAssignmentCount() != pt.GetAssignmentCount() {
		t.Errorf("Assignment count mismatch: got %d, want %d",
			pt2.GetAssignmentCount(), pt.GetAssignmentCount())
	}
}

func TestSplitSnapshotInvalid(t *testing.T) {
	tests := []struct {
		name string
		data []byte
	}{
		{
			name: "too short",
			data: []byte("DC"),
		},
		{
			name: "invalid magic",
			data: []byte("XXXX" + string(make([]byte, 100))),
		},
		{
			name: "empty",
			data: []byte{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, err := SplitSnapshot(tt.data)
			if err == nil {
				t.Errorf("Expected error for %s, got nil", tt.name)
			}
		})
	}
}

func TestCombineSplitRoundTrip(t *testing.T) {
	// Create large partition table (realistic size)
	pt := InitializeEvenDistribution(16384, []NodeID{0, 1, 2})
	ptData, err := pt.Serialize()
	if err != nil {
		t.Fatalf("Failed to serialize partition table: %v", err)
	}

	// Create larger data snapshot
	dataSnapshot := make([]byte, 1024*1024) // 1MB
	for i := range dataSnapshot {
		dataSnapshot[i] = byte(i % 256)
	}

	// Combine
	combined, err := CombineSnapshot(ptData, dataSnapshot)
	if err != nil {
		t.Fatalf("CombineSnapshot failed: %v", err)
	}

	t.Logf("Combined snapshot size: %d bytes (PT: %d, Data: %d)",
		len(combined), len(ptData), len(dataSnapshot))

	// Split
	splitPtData, splitDataSnapshot, err := SplitSnapshot(combined)
	if err != nil {
		t.Fatalf("SplitSnapshot failed: %v", err)
	}

	// Verify exact match
	if !bytes.Equal(splitPtData, ptData) {
		t.Error("Partition table data corrupted in round-trip")
	}

	if !bytes.Equal(splitDataSnapshot, dataSnapshot) {
		t.Error("Data snapshot corrupted in round-trip")
	}

	// Verify partition table integrity
	pt2 := NewPartitionTable()
	if err := pt2.Deserialize(splitPtData); err != nil {
		t.Fatalf("Failed to deserialize partition table: %v", err)
	}

	if pt2.GetAssignmentCount() != pt.GetAssignmentCount() {
		t.Errorf("Assignment count mismatch: got %d, want %d",
			pt2.GetAssignmentCount(), pt.GetAssignmentCount())
	}
}

func TestSerializationSize(t *testing.T) {
	// Test various table sizes
	tests := []struct {
		name            string
		partitions      uint16
		nodes           []NodeID
		maxExpectedSize int
	}{
		{
			name:            "empty",
			partitions:      0,
			nodes:           []NodeID{},
			maxExpectedSize: 100, // Just version + count
		},
		{
			name:            "small (100 partitions)",
			partitions:      100,
			nodes:           []NodeID{0, 1, 2},
			maxExpectedSize: 1020, // 8 + 4 + 100*(2+4+4) = 1012 bytes (with backup nodes)
		},
		{
			name:            "full (16384 partitions)",
			partitions:      16384,
			nodes:           []NodeID{0, 1, 2},
			maxExpectedSize: 165000, // 8 + 4 + 16384*(2+4+4) = ~164KB (with backup nodes)
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pt := InitializeEvenDistribution(tt.partitions, tt.nodes)
			data, err := pt.Serialize()
			if err != nil {
				t.Fatalf("Serialize failed: %v", err)
			}

			if len(data) > tt.maxExpectedSize {
				t.Errorf("Serialized size too large: got %d bytes, want <= %d",
					len(data), tt.maxExpectedSize)
			}

			t.Logf("Serialized %d partitions: %d bytes", tt.partitions, len(data))
		})
	}
}

func BenchmarkCombineSnapshot(b *testing.B) {
	pt := InitializeEvenDistribution(16384, []NodeID{0, 1, 2})
	ptData, _ := pt.Serialize()
	dataSnapshot := make([]byte, 1024*1024) // 1MB

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := CombineSnapshot(ptData, dataSnapshot)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSplitSnapshot(b *testing.B) {
	pt := InitializeEvenDistribution(16384, []NodeID{0, 1, 2})
	ptData, _ := pt.Serialize()
	dataSnapshot := make([]byte, 1024*1024)
	combined, _ := CombineSnapshot(ptData, dataSnapshot)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, err := SplitSnapshot(combined)
		if err != nil {
			b.Fatal(err)
		}
	}
}
