package sharding

import (
	"fmt"
	"sync"
)

// PartitionTable manages the mapping of partitions to nodes
// It is part of the Raft state machine and must be replicated
type PartitionTable struct {
	mu          sync.RWMutex
	assignments map[PartitionID]*PartitionEntry
	version     uint64 // For optimistic concurrency control and change tracking
}

// NewPartitionTable creates an empty partition table
func NewPartitionTable() *PartitionTable {
	return &PartitionTable{
		assignments: make(map[PartitionID]*PartitionEntry),
		version:     0,
	}
}

// Assign sets the owner of a partition (backward compatibility - sets primary only)
// Thread-safe for concurrent access
func (pt *PartitionTable) Assign(partitionID PartitionID, nodeID NodeID) {
	pt.mu.Lock()
	defer pt.mu.Unlock()

	entry, exists := pt.assignments[partitionID]
	if !exists {
		entry = &PartitionEntry{
			PartitionID:  partitionID,
			PrimaryNode:  nodeID,
			BackupNode:   -1, // No backup
			Version:      pt.version + 1,
		}
		pt.assignments[partitionID] = entry
	} else {
		entry.PrimaryNode = nodeID
		entry.Version = pt.version + 1
	}
	pt.version++
}

// GetOwner returns the node that owns a partition (returns primary node)
// Returns (nodeID, true) if found, (-1, false) if unassigned
// Thread-safe for concurrent access
// Kept for backward compatibility
func (pt *PartitionTable) GetOwner(partitionID PartitionID) (NodeID, bool) {
	pt.mu.RLock()
	defer pt.mu.RUnlock()

	entry, exists := pt.assignments[partitionID]
	if !exists {
		return -1, false
	}
	return entry.PrimaryNode, true
}

// GetPrimary returns the primary node for a partition
// Returns (nodeID, true) if found, (-1, false) if unassigned
// Thread-safe for concurrent access
func (pt *PartitionTable) GetPrimary(partitionID PartitionID) (NodeID, bool) {
	return pt.GetOwner(partitionID) // Same as GetOwner
}

// GetBackup returns the backup node for a partition
// Returns (nodeID, true) if found, (-1, false) if unassigned or no backup
// Thread-safe for concurrent access
func (pt *PartitionTable) GetBackup(partitionID PartitionID) (NodeID, bool) {
	pt.mu.RLock()
	defer pt.mu.RUnlock()

	entry, exists := pt.assignments[partitionID]
	if !exists {
		return -1, false
	}
	return entry.BackupNode, true
}

// GetReplicas returns both primary and backup nodes for a partition
// Returns (primary, backup, true) if found, (-1, -1, false) if unassigned
// Thread-safe for concurrent access
func (pt *PartitionTable) GetReplicas(partitionID PartitionID) (primary NodeID, backup NodeID, ok bool) {
	pt.mu.RLock()
	defer pt.mu.RUnlock()

	entry, exists := pt.assignments[partitionID]
	if !exists {
		return -1, -1, false
	}
	return entry.PrimaryNode, entry.BackupNode, true
}

// SetReplicas atomically sets both primary and backup for a partition
// Thread-safe for concurrent access
func (pt *PartitionTable) SetReplicas(partitionID PartitionID, primary NodeID, backup NodeID) {
	pt.mu.Lock()
	defer pt.mu.Unlock()

	pt.assignments[partitionID] = &PartitionEntry{
		PartitionID:  partitionID,
		PrimaryNode:  primary,
		BackupNode:   backup,
		Version:      pt.version + 1,
	}
	pt.version++
}

// GetAssignments returns a copy of all partition assignments (primary nodes only for backward compatibility)
// Thread-safe for concurrent access
func (pt *PartitionTable) GetAssignments() map[PartitionID]NodeID {
	pt.mu.RLock()
	defer pt.mu.RUnlock()

	// Return a copy to prevent external mutation
	copy := make(map[PartitionID]NodeID, len(pt.assignments))
	for k, v := range pt.assignments {
		copy[k] = v.PrimaryNode
	}
	return copy
}

// GetVersion returns the current version of the partition table
func (pt *PartitionTable) GetVersion() uint64 {
	pt.mu.RLock()
	defer pt.mu.RUnlock()
	return pt.version
}

// AssignRange assigns a continuous range of partitions to a node (sets primary only)
// This is useful for initial distribution or bulk rebalancing
// Thread-safe for concurrent access
func (pt *PartitionTable) AssignRange(startPartition, endPartition PartitionID, nodeID NodeID) error {
	if startPartition > endPartition {
		return fmt.Errorf("invalid range: start %d > end %d", startPartition, endPartition)
	}

	pt.mu.Lock()
	defer pt.mu.Unlock()

	for pid := startPartition; pid <= endPartition; pid++ {
		entry, exists := pt.assignments[pid]
		if !exists {
			pt.assignments[pid] = &PartitionEntry{
				PartitionID:  pid,
				PrimaryNode:  nodeID,
				BackupNode:   -1,
				Version:      pt.version + 1,
			}
		} else {
			entry.PrimaryNode = nodeID
			entry.Version = pt.version + 1
		}
	}
	pt.version++

	return nil
}

// GetNodePartitions returns all partition IDs owned by a specific node (as primary)
// Thread-safe for concurrent access
func (pt *PartitionTable) GetNodePartitions(nodeID NodeID) []PartitionID {
	pt.mu.RLock()
	defer pt.mu.RUnlock()

	partitions := make([]PartitionID, 0)
	for pid, entry := range pt.assignments {
		if entry.PrimaryNode == nodeID {
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

	pt.assignments = make(map[PartitionID]*PartitionEntry)
	pt.version++
}

// ApplyUpdate applies a bulk update to the partition table
// This is used when receiving updates from Raft
// Thread-safe for concurrent access
func (pt *PartitionTable) ApplyUpdate(assignments map[PartitionID]NodeID, version uint64) {
	pt.mu.Lock()
	defer pt.mu.Unlock()

	// Replace all assignments atomically (convert old format to new)
	pt.assignments = make(map[PartitionID]*PartitionEntry, len(assignments))
	for pid, nodeID := range assignments {
		pt.assignments[pid] = &PartitionEntry{
			PartitionID:  pid,
			PrimaryNode:  nodeID,
			BackupNode:   -1, // No backup in old format
			Version:      version,
		}
	}
	pt.version = version
}

// Clone creates a deep copy of the partition table
func (pt *PartitionTable) Clone() *PartitionTable {
	pt.mu.RLock()
	defer pt.mu.RUnlock()

	clone := &PartitionTable{
		assignments: make(map[PartitionID]*PartitionEntry, len(pt.assignments)),
		version:     pt.version,
	}

	for pid, entry := range pt.assignments {
		clone.assignments[pid] = &PartitionEntry{
			PartitionID:  entry.PartitionID,
			PrimaryNode:  entry.PrimaryNode,
			BackupNode:   entry.BackupNode,
			Version:      entry.Version,
		}
	}

	return clone
}

// InitializeEvenDistribution creates an initial partition table with
// partitions distributed evenly across the specified nodes
// Each partition gets a primary and backup node in round-robin fashion
func InitializeEvenDistribution(totalPartitions uint16, nodeIDs []NodeID) *PartitionTable {
	if len(nodeIDs) == 0 {
		return NewPartitionTable()
	}

	pt := NewPartitionTable()
	totalNodes := len(nodeIDs)

	// Simple round-robin assignment for both primary and backup
	for pid := PartitionID(0); pid < PartitionID(totalPartitions); pid++ {
		// Primary assignment: round-robin
		primaryIndex := int(pid) % totalNodes
		primaryNode := nodeIDs[primaryIndex]

		// Backup assignment: next node in ring
		backupIndex := (primaryIndex + 1) % totalNodes
		backupNode := nodeIDs[backupIndex]

		pt.assignments[pid] = &PartitionEntry{
			PartitionID:  pid,
			PrimaryNode:  primaryNode,
			BackupNode:   backupNode,
			Version:      1,
		}
	}

	pt.version = 1 // Initial version
	return pt
}
