package sharding

import (
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
)

// ShardManager validates whether this node should handle a given key
// It maintains a local cache of the partition table synchronized with Raft state
type ShardManager struct {
	nodeID         NodeID
	partitionTable *PartitionTable
	partitioner    *Partitioner
	peerAddresses  map[NodeID]string
	mu             sync.RWMutex // Protects peerAddresses

	// Metrics (atomic counters)
	localHits uint64
	redirects uint64
	errors    uint64
}

// NewShardManager creates a new shard manager for this node
func NewShardManager(nodeID NodeID, partitionTable *PartitionTable, partitioner *Partitioner) *ShardManager {
	return &ShardManager{
		nodeID:         nodeID,
		partitionTable: partitionTable,
		partitioner:    partitioner,
		peerAddresses:  make(map[NodeID]string),
	}
}

// ValidateKey checks if this node should handle the given key
// Returns nil if this node owns the key
// Returns WrongNodeError with redirect info if another node owns it
func (sm *ShardManager) ValidateKey(key string) error {
	partitionID := sm.partitioner.HashKey(key)
	ownerID, exists := sm.partitionTable.GetOwner(partitionID)

	if !exists {
		atomic.AddUint64(&sm.errors, 1)
		return fmt.Errorf("no owner assigned for partition %d", partitionID)
	}

	if ownerID == sm.nodeID {
		atomic.AddUint64(&sm.localHits, 1)
		return nil // This node owns the key
	}

	// Wrong node - return redirect error
	sm.mu.RLock()
	ownerAddr := sm.peerAddresses[ownerID]
	sm.mu.RUnlock()

	atomic.AddUint64(&sm.redirects, 1)

	if ownerAddr == "" {
		atomic.AddUint64(&sm.errors, 1)
		return fmt.Errorf("no address found for node %d", ownerID)
	}

	return NewWrongNodeError(key, partitionID, sm.nodeID, ownerID, ownerAddr)
}

// UpdatePeerAddress updates the address for a peer node
// This should be called during initialization to populate the peer registry
func (sm *ShardManager) UpdatePeerAddress(nodeID NodeID, address string) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.peerAddresses[nodeID] = address
	slog.Info(fmt.Sprintf("ShardManager: Updated peer address for node %d: %s", nodeID, address))
}

// GetNodeForKey returns the node ID that should handle the key
func (sm *ShardManager) GetNodeForKey(key string) (NodeID, error) {
	partitionID := sm.partitioner.HashKey(key)
	ownerID, exists := sm.partitionTable.GetOwner(partitionID)
	if !exists {
		return -1, fmt.Errorf("no owner assigned for partition %d", partitionID)
	}
	return ownerID, nil
}

// IsLocalKey returns true if this node should handle the key
func (sm *ShardManager) IsLocalKey(key string) bool {
	return sm.ValidateKey(key) == nil
}

// GetNodeAddress returns the address for a given node ID
func (sm *ShardManager) GetNodeAddress(nodeID NodeID) (string, bool) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	addr, exists := sm.peerAddresses[nodeID]
	return addr, exists
}

// GetMetrics returns current metrics for monitoring
func (sm *ShardManager) GetMetrics() map[string]uint64 {
	return map[string]uint64{
		"local_hits": atomic.LoadUint64(&sm.localHits),
		"redirects":  atomic.LoadUint64(&sm.redirects),
		"errors":     atomic.LoadUint64(&sm.errors),
	}
}

// OnPartitionTableUpdate is called when the partition table changes
// The partition table is a shared pointer, so updates are automatic
// This method exists for monitoring/logging purposes
func (sm *ShardManager) OnPartitionTableUpdate() {
	version := sm.partitionTable.GetVersion()
	assignmentCount := sm.partitionTable.GetAssignmentCount()
	slog.Info(fmt.Sprintf("ShardManager: Partition table updated to version %d (%d assignments)",
		version, assignmentCount))
}
