package datanode

import (
	"sync"

	"github.com/dominikszczepaniak/distributed-cache/pkg/metadata"
)

type StateManager struct {
	mu     sync.RWMutex
	config *metadata.ClusterConfig
}

func NewStateManager() *StateManager {
	return &StateManager{
		config: &metadata.ClusterConfig{
			Epoch:  0,
			Nodes:  make(map[string]metadata.NodeMetadata),
			Shards: make(map[int]metadata.ShardMetadata),
		},
	}
}

func (s *StateManager) Update(config *metadata.ClusterConfig) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.config = config
}

func (s *StateManager) Get() *metadata.ClusterConfig {
	s.mu.RLock()
	defer s.mu.RUnlock()
	// Caller should NOT modify the returned config
	return s.config
}

func (s *StateManager) GetEpoch() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.config.Epoch
}

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
