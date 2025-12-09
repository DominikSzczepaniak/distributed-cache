package datanode

import (
	"testing"

	"github.com/dominikszczepaniak/distributed-cache/pkg/metadata"
	"github.com/stretchr/testify/assert"
)

func TestStateManager_GetReplicaURLs(t *testing.T) {
	sm := NewStateManager()

	config := &metadata.ClusterConfig{
		Epoch:       1,
		TotalShards: 1,
		Nodes: map[string]metadata.NodeMetadata{
			"node-1": {ID: "node-1", Address: "node-1:9000", Status: metadata.StatusActive},
			"node-2": {ID: "node-2", Address: "node-2:9000", Status: metadata.StatusActive},
		},
		Shards: map[int]metadata.ShardMetadata{
			0: {ID: 0, PrimaryID: "node-1", ReplicaIDs: []string{"node-2"}},
		},
	}
	sm.Update(config)

	urls := sm.GetReplicaURLs(0)

	assert.Len(t, urls, 1)
	assert.Equal(t, "http://node-2:9000", urls[0])
}

func TestStateManager_GetReplicaURLs_NoReplicas(t *testing.T) {
	sm := NewStateManager()

	config := &metadata.ClusterConfig{
		Epoch:       1,
		TotalShards: 1,
		Nodes: map[string]metadata.NodeMetadata{
			"node-1": {ID: "node-1", Address: "node-1:9000", Status: metadata.StatusActive},
		},
		Shards: map[int]metadata.ShardMetadata{
			0: {ID: 0, PrimaryID: "node-1", ReplicaIDs: []string{}},
		},
	}
	sm.Update(config)

	urls := sm.GetReplicaURLs(0)

	assert.Empty(t, urls)
}

func TestStateManager_GetReplicaURLs_ShardNotFound(t *testing.T) {
	sm := NewStateManager()

	urls := sm.GetReplicaURLs(999)

	assert.Nil(t, urls)
}
