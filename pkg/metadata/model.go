package metadata

import (
	"time"
)

// NodeStatus represents the operational state of a DataNode (Active or Dead).
type NodeStatus string

const (
	// StatusActive indicates the node is fully operational and participating in the cluster.
	StatusActive NodeStatus = "ACTIVE"
	// StatusDead indicates the node has missed too many heartbeats and is considered failed.
	StatusDead NodeStatus = "DEAD"
	// StatusJoining indicates the node is currently performing an initial data sync.
	StatusJoining NodeStatus = "JOINING"
)

// ShardStatus represents the availability of a specific data shard.
type ShardStatus string

const (
	// ShardStatusActive indicates the shard is available for both reads and writes.
	ShardStatusActive ShardStatus = "ACTIVE"
	// ShardStatusMigrating indicates the shard is currently being moved between nodes.
	ShardStatusMigrating ShardStatus = "MIGRATING"
	// ShardStatusLocked indicates the shard is temporarily locked during the final phase of migration.
	ShardStatusLocked ShardStatus = "LOCKED"
)

// ShardMetadata describes the ownership and replication state of a single shard.
type ShardMetadata struct {
	ID         int         `json:"id"`
	PrimaryID  string      `json:"primary_id"`
	ReplicaIDs []string    `json:"replica_ids"`
	Status     ShardStatus `json:"status"`
}

// NodeMetadata tracks the network address and health of a cluster node.
type NodeMetadata struct {
	// ID is the unique identifier for the node.
	ID string `json:"id"`
	// Address is the network endpoint (IP:Port) for the node's API.
	Address string `json:"address"`
	// Status is the current health state of the node.
	Status NodeStatus `json:"status"`
	// LastHeartbeat records the last time the node reported to the controller.
	LastHeartbeat time.Time `json:"last_heartbeat"`
}

// ClusterConfig is the global state of the cluster, including nodes, shards, and the current epoch.
type ClusterConfig struct {
	Epoch       uint64                  `json:"epoch"`
	TotalShards int                     `json:"total_shards"`
	Nodes       map[string]NodeMetadata `json:"nodes"`
	Shards      map[int]ShardMetadata   `json:"shards"`
}

// NewClusterConfig initializes a default cluster configuration with the specified number of shards.
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
			Status:     ShardStatusActive,
		}
	}
	return config
}
