package metadata

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRebalance_PromoteReplica(t *testing.T) {
	config := &ClusterConfig{
		Epoch: 1,
		Nodes: map[string]NodeMetadata{
			"node1": {ID: "node1", Status: StatusActive},
			"node2": {ID: "node2", Status: StatusActive},
		},
		Shards: map[int]ShardMetadata{
			0: {ID: 0, PrimaryID: "node1", ReplicaIDs: []string{"node2"}},
		},
	}

	newConfig := Rebalance(config, "node1")

	assert.Equal(t, uint64(2), newConfig.Epoch)
	assert.Equal(t, StatusDead, newConfig.Nodes["node1"].Status)
	assert.Equal(t, "node2", newConfig.Shards[0].PrimaryID)
	assert.Empty(t, newConfig.Shards[0].ReplicaIDs)
}

func TestRebalance_NoReplicas(t *testing.T) {
	config := &ClusterConfig{
		Epoch: 1,
		Nodes: map[string]NodeMetadata{
			"node1": {ID: "node1", Status: StatusActive},
		},
		Shards: map[int]ShardMetadata{
			0: {ID: 0, PrimaryID: "node1", ReplicaIDs: []string{}},
		},
	}

	newConfig := Rebalance(config, "node1")

	assert.Equal(t, uint64(2), newConfig.Epoch)
	assert.Equal(t, StatusDead, newConfig.Nodes["node1"].Status)
	assert.Equal(t, "", newConfig.Shards[0].PrimaryID) // Unavailable
}

func TestRebalance_RemoveDeadReplica(t *testing.T) {
	config := &ClusterConfig{
		Epoch: 1,
		Nodes: map[string]NodeMetadata{
			"node1": {ID: "node1", Status: StatusActive},
			"node2": {ID: "node2", Status: StatusActive},
		},
		Shards: map[int]ShardMetadata{
			0: {ID: 0, PrimaryID: "node1", ReplicaIDs: []string{"node2"}},
		},
	}

	newConfig := Rebalance(config, "node2")

	assert.Equal(t, uint64(2), newConfig.Epoch)
	assert.Equal(t, StatusDead, newConfig.Nodes["node2"].Status)
	assert.Equal(t, "node1", newConfig.Shards[0].PrimaryID)
	assert.Empty(t, newConfig.Shards[0].ReplicaIDs)
}
