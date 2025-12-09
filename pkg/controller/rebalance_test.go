package controller

import (
	"testing"
	"time"

	"github.com/dominikszczepaniak/distributed-cache/pkg/metadata"
	"github.com/dominikszczepaniak/distributed-cache/pkg/raft"
)

func TestController_Rebalance(t *testing.T) {
	c := NewController(&raft.Raft{}, 10*time.Second)

	c.config.Nodes["nodeA"] = metadata.NodeMetadata{ID: "nodeA", Address: "127.0.0.1:8001", Status: metadata.StatusActive}
	c.config.Nodes["nodeB"] = metadata.NodeMetadata{ID: "nodeB", Address: "127.0.0.1:8002", Status: metadata.StatusActive}
	for i := 0; i < 10; i++ {
		c.config.Shards[i] = metadata.ShardMetadata{ID: i, PrimaryID: "", Status: metadata.ShardStatusActive}
	}

	c.Rebalance()

	config := c.GetConfig()
	assignedShards := 0
	for _, shard := range config.Shards {
		if shard.PrimaryID != "" {
			assignedShards++
		}
		if len(shard.ReplicaIDs) != 1 {
			t.Errorf("Shard %d: expected 1 replica, got %d", shard.ID, len(shard.ReplicaIDs))
		}
		if len(shard.ReplicaIDs) > 0 && shard.ReplicaIDs[0] == shard.PrimaryID {
			t.Errorf("Shard %d: replica should be different from primary", shard.ID)
		}
	}

	if assignedShards != 10 {
		t.Errorf("Expected all 10 shards to be assigned, got %d", assignedShards)
	}

	if config.Epoch <= 0 {
		t.Errorf("Expected epoch > 0, got %d", config.Epoch)
	}
}
