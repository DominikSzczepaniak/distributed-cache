package datanode

import (
	"sync"

	"github.com/dominikszczepaniak/distributed-cache/pkg/metadata"
)

// StateManager holds the local view of the cluster topology
type StateManager struct {
	mu     sync.RWMutex
	config *metadata.ClusterConfig
}

// NewStateManager creates a new StateManager with an empty config
func NewStateManager() *StateManager {
	return &StateManager{
		config: &metadata.ClusterConfig{
			Epoch:  0,
			Nodes:  make(map[string]metadata.NodeMetadata),
			Shards: make(map[int]metadata.ShardMetadata),
		},
	}
}

// Update atomically updates the cluster configuration
func (s *StateManager) Update(config *metadata.ClusterConfig) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.config = config
}

// Get returns a copy of the current configuration
// Note: In a high-perf scenario, we might return a pointer and treat it as immutable,
// but for safety here we'll just return the pointer since we replace the whole struct on update.
// The caller should NOT modify the returned config.
func (s *StateManager) Get() *metadata.ClusterConfig {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.config
}

// GetEpoch returns the current epoch
func (s *StateManager) GetEpoch() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.config.Epoch
}
