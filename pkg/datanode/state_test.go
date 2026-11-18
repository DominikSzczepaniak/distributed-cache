package datanode

import (
	"testing"

	"github.com/dominikszczepaniak/distributed-cache/pkg/metadata"
	"github.com/stretchr/testify/assert"
)

func TestStateManager_GetReplicaURLs(t *testing.T) {
	nodes := map[string]metadata.NodeMetadata{
		"node-1": {ID: "node-1", Address: "node-1:9000", Status: metadata.StatusActive},
		"node-2": {ID: "node-2", Address: "node-2:9000", Status: metadata.StatusActive},
		"node-3": {ID: "node-3", Address: "node-3:9000", Status: metadata.StatusActive},
	}
	config := &metadata.ClusterConfig{
		Epoch:       1,
		TotalShards: 4,
		Nodes:       nodes,
		Shards: map[int]metadata.ShardMetadata{
			0: {ID: 0, PrimaryID: "node-1", ReplicaIDs: []string{"node-2"}},
			1: {ID: 1, PrimaryID: "node-1", ReplicaIDs: []string{}},
			2: {ID: 2, PrimaryID: "node-1", ReplicaIDs: []string{"node-3", "node-2"}},
			3: {ID: 3, PrimaryID: "node-1", ReplicaIDs: []string{"ghost", "node-2"}},
		},
	}

	tests := []struct {
		name    string
		shardID int
		want    []string
	}{
		{"single replica", 0, []string{"http://node-2:9000"}},
		{"no replicas", 1, nil},
		{"multiple replicas keep config order", 2, []string{"http://node-3:9000", "http://node-2:9000"}},
		{"replica missing from node table is skipped", 3, []string{"http://node-2:9000"}},
		{"shard not found", 999, nil},
		{"negative shard", -1, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sm := NewStateManager()
			sm.Update(config)
			got := sm.GetReplicaURLs(tt.shardID)
			if len(tt.want) == 0 {
				assert.Empty(t, got)
				return
			}
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestStateManager_UpdateAndEpoch(t *testing.T) {
	sm := NewStateManager()
	assert.Equal(t, uint64(0), sm.GetEpoch())
	cfg := &metadata.ClusterConfig{Epoch: 7}
	sm.Update(cfg)
	assert.Equal(t, uint64(7), sm.GetEpoch())
	assert.Same(t, cfg, sm.Get())
}
