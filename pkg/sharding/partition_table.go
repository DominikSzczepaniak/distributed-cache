package sharding

import (
	"fmt"
	"sync"
)

// PartitionTable manages the mapping of partitions to nodes
// It is part of the Raft state machine and must be replicated
type PartitionTable struct {
	mu          sync.RWMutex
	assignments map[PartitionID]NodeID
	version     uint64 // For optimistic concurrency control and change tracking
}

// NewPartitionTable creates an empty partition table
func NewPartitionTable() *PartitionTable {
	return &PartitionTable{
		assignments: make(map[PartitionID]NodeID),
		version:     0,
	}
}

// Assign sets the owner of a partition
// Thread-safe for concurrent access
func (pt *PartitionTable) Assign(partitionID PartitionID, nodeID NodeID) {
	pt.mu.Lock()
	defer pt.mu.Unlock()

	pt.assignments[partitionID] = nodeID
	pt.version++
}

// GetOwner returns the node that owns a partition
// Returns (nodeID, true) if found, (-1, false) if unassigned
// Thread-safe for concurrent access
func (pt *PartitionTable) GetOwner(partitionID PartitionID) (NodeID, bool) {
	pt.mu.RLock()
	defer pt.mu.RUnlock()

	nodeID, exists := pt.assignments[partitionID]
	return nodeID, exists
}

// GetAssignments returns a copy of all partition assignments
// Thread-safe for concurrent access
func (pt *PartitionTable) GetAssignments() map[PartitionID]NodeID {
	pt.mu.RLock()
	defer pt.mu.RUnlock()

	// Return a copy to prevent external mutation
	copy := make(map[PartitionID]NodeID, len(pt.assignments))
	for k, v := range pt.assignments {
		copy[k] = v
	}
	return copy
}

// GetVersion returns the current version of the partition table
func (pt *PartitionTable) GetVersion() uint64 {
	pt.mu.RLock()
	defer pt.mu.RUnlock()
	return pt.version
}

// AssignRange assigns a continuous range of partitions to a node
// This is useful for initial distribution or bulk rebalancing
// Thread-safe for concurrent access
func (pt *PartitionTable) AssignRange(startPartition, endPartition PartitionID, nodeID NodeID) error {
	if startPartition > endPartition {
		return fmt.Errorf("invalid range: start %d > end %d", startPartition, endPartition)
	}

	pt.mu.Lock()
	defer pt.mu.Unlock()

	for pid := startPartition; pid <= endPartition; pid++ {
		pt.assignments[pid] = nodeID
	}
	pt.version++

	return nil
}

// GetNodePartitions returns all partition IDs owned by a specific node
// Thread-safe for concurrent access
func (pt *PartitionTable) GetNodePartitions(nodeID NodeID) []PartitionID {
	pt.mu.RLock()
	defer pt.mu.RUnlock()

	partitions := make([]PartitionID, 0)
	for pid, owner := range pt.assignments {
		if owner == nodeID {
			partitions = append(partitions, pid)
		}
	}
	return partitions
}

// GetAssignmentCount returns the number of assigned partitions
func (pt *PartitionTable) GetAssignmentCount() int {
	pt.mu.RLock()
	defer pt.mu.RUnlock()
	return len(pt.assignments)
}

// Clear removes all assignments (used for testing)
func (pt *PartitionTable) Clear() {
	pt.mu.Lock()
	defer pt.mu.Unlock()

	pt.assignments = make(map[PartitionID]NodeID)
	pt.version++
}

// ApplyUpdate applies a bulk update to the partition table
// This is used when receiving updates from Raft
// Thread-safe for concurrent access
func (pt *PartitionTable) ApplyUpdate(assignments map[PartitionID]NodeID, version uint64) {
	pt.mu.Lock()
	defer pt.mu.Unlock()

	// Replace all assignments atomically
	pt.assignments = make(map[PartitionID]NodeID, len(assignments))
	for k, v := range assignments {
		pt.assignments[k] = v
	}
	pt.version = version
}

// Clone creates a deep copy of the partition table
func (pt *PartitionTable) Clone() *PartitionTable {
	pt.mu.RLock()
	defer pt.mu.RUnlock()

	clone := &PartitionTable{
		assignments: make(map[PartitionID]NodeID, len(pt.assignments)),
		version:     pt.version,
	}

	for k, v := range pt.assignments {
		clone.assignments[k] = v
	}

	return clone
}

// InitializeEvenDistribution creates an initial partition table with
// partitions distributed evenly across the specified nodes
func InitializeEvenDistribution(totalPartitions uint16, nodeIDs []NodeID) *PartitionTable {
	if len(nodeIDs) == 0 {
		return NewPartitionTable()
	}

	pt := NewPartitionTable()
	totalNodes := len(nodeIDs)
	partitionsPerNode := int(totalPartitions) / totalNodes
	remainder := int(totalPartitions) % totalNodes

	currentPartition := PartitionID(0)
	for i, nodeID := range nodeIDs {
		count := partitionsPerNode
		if i < remainder {
			count++ // Distribute remainder evenly
		}

		for j := 0; j < count; j++ {
			pt.assignments[currentPartition] = nodeID
			currentPartition++
		}
	}

	pt.version = 1 // Initial version
	return pt
}
