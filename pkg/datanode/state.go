package datanode

import (
	"sync"

	"github.com/dominikszczepaniak/distributed-cache/pkg/metadata"
)

// StateManager maintains a local, consistent view of the cluster topology on each DataNode.
type StateManager struct {
	mu     sync.RWMutex
	config *metadata.ClusterConfig
}

// NewStateManager initializes an empty cluster configuration cache.
func NewStateManager() *StateManager {
	return &StateManager{
		config: &metadata.ClusterConfig{
			Epoch:  0,
			Nodes:  make(map[string]metadata.NodeMetadata),
			Shards: make(map[int]metadata.ShardMetadata),
		},
	}
}

// Update replaces the local topology view with a new one from the controller.
func (s *StateManager) Update(config *metadata.ClusterConfig) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.config = config
}

// Get returns the current cluster configuration.
func (s *StateManager) Get() *metadata.ClusterConfig {
	s.mu.RLock()
	defer s.mu.RUnlock()
	// Caller should NOT modify the returned config
	return s.config
}

// GetEpoch returns the version of the current local cluster configuration.
func (s *StateManager) GetEpoch() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.config.Epoch
}

// GetReplicaURLs returns the API addresses of all nodes serving as replicas for a specific shard.
func (s *StateManager) GetReplicaURLs(shardID int) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	shard, exists := s.config.Shards[shardID]
	if !exists {
		return nil
	}

	var urls []string
	for _, replicaID := range shard.ReplicaIDs {
		if node, ok := s.config.Nodes[replicaID]; ok {
			urls = append(urls, "http://"+node.Address)
		}
	}
	return urls
}
