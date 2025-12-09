package controller

import (
	"testing"
	"time"

	"github.com/dominikszczepaniak/distributed-cache/pkg/metadata"
	"github.com/stretchr/testify/assert"
)

func TestController_HandleNodeFailure(t *testing.T) {
	c := NewController(nil, 100*time.Millisecond)

	c.config.Nodes["node1"] = metadata.NodeMetadata{ID: "node1", Status: metadata.StatusActive}
	c.config.Shards[0] = metadata.ShardMetadata{ID: 0, PrimaryID: "node1", ReplicaIDs: []string{}}

	c.HandleNodeFailure("node1")

	c.mu.RLock()
	defer c.mu.RUnlock()

	assert.Equal(t, metadata.StatusDead, c.config.Nodes["node1"].Status)
	assert.Equal(t, "", c.config.Shards[0].PrimaryID)
	assert.Equal(t, uint64(1), c.config.Epoch)
}

func TestController_Heartbeat(t *testing.T) {
	c := NewController(nil, 1*time.Second)

	c.Heartbeat("node1")

	c.reaper.mu.RLock()
	_, ok := c.reaper.lastSeen["node1"]
	c.reaper.mu.RUnlock()

	assert.True(t, ok)
}
