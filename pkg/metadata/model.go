package metadata

import (
	"time"
)

type NodeStatus string

const (
	StatusActive  NodeStatus = "ACTIVE"
	StatusDead    NodeStatus = "DEAD"
	StatusJoining NodeStatus = "JOINING"
)

type ShardMetadata struct {
	ID         int      `json:"id"`
	PrimaryID  string   `json:"primary_id"`
	ReplicaIDs []string `json:"replica_ids"`
}

type NodeMetadata struct {
	ID            string     `json:"id"`
	Address       string     `json:"address"`
	Status        NodeStatus `json:"status"`
	LastHeartbeat time.Time  `json:"last_heartbeat"`
}

type ClusterConfig struct {
	Epoch       uint64                  `json:"epoch"`
	TotalShards int                     `json:"total_shards"`
	Nodes       map[string]NodeMetadata `json:"nodes"`
	Shards      map[int]ShardMetadata   `json:"shards"`
}

func NewClusterConfig(numShards int) *ClusterConfig {
	config := &ClusterConfig{
		Epoch:       0,
		TotalShards: numShards,
		Nodes:       make(map[string]NodeMetadata),
		Shards:      make(map[int]ShardMetadata),
	}

	for i := 0; i < numShards; i++ {
		config.Shards[i] = ShardMetadata{
			ID:         i,
			PrimaryID:  "",
			ReplicaIDs: []string{},
		}
	}
	return config
}
