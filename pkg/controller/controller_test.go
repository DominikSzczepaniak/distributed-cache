package controller

import (
	"testing"
	"time"

	"github.com/dominikszczepaniak/distributed-cache/pkg/metadata"
	"github.com/dominikszczepaniak/distributed-cache/pkg/raft"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

// Heartbeat: a node that keeps heartbeating must never be declared dead, a node
// that goes silent for longer than the grace period must be, exactly once, and
// its shard must fail over to the replica.
func TestController_Heartbeat_KeepsNodeAliveUntilSilence(t *testing.T) {
	clk := newTestClock()
	c := newControllerWithClock(&raft.Raft{}, 10*time.Second, clk.Now)
	c.config.Nodes["n1"] = metadata.NodeMetadata{ID: "n1", Status: metadata.StatusActive}
	c.config.Nodes["n2"] = metadata.NodeMetadata{ID: "n2", Status: metadata.StatusActive}
	c.config.Shards[0] = metadata.ShardMetadata{ID: 0, PrimaryID: "n1", ReplicaIDs: []string{"n2"}, Status: metadata.ShardStatusActive}

	// Both nodes heartbeat every 4s for 60s (6 grace periods).
	for i := 0; i < 15; i++ {
		c.Heartbeat("n1")
		c.Heartbeat("n2")
		clk.Advance(4 * time.Second)
		c.reaper.checkLiveness()
	}
	time.Sleep(50 * time.Millisecond) // give a (wrongly) spawned onDeath goroutine time to run
	cfg := c.GetConfig()
	assert.Equal(t, metadata.StatusActive, cfg.Nodes["n1"].Status, "heartbeating node must stay alive")
	assert.Equal(t, "n1", cfg.Shards[0].PrimaryID)
	assert.Equal(t, uint64(0), cfg.Epoch, "no topology change while all nodes are healthy")

	// n1 goes silent; n2 keeps heartbeating.
	for i := 0; i < 3; i++ {
		clk.Advance(4 * time.Second)
		c.Heartbeat("n2")
		c.reaper.checkLiveness()
	}

	require.Eventually(t, func() bool {
		return c.GetConfig().Nodes["n1"].Status == metadata.StatusDead
	}, 2*time.Second, 5*time.Millisecond, "silent node must be declared dead after grace period")

	cfg = c.GetConfig()
	assert.Equal(t, metadata.StatusActive, cfg.Nodes["n2"].Status, "heartbeating peer unaffected")
	assert.Equal(t, "n2", cfg.Shards[0].PrimaryID, "replica promoted")
	epochAfterDeath := cfg.Epoch
	assert.Greater(t, epochAfterDeath, uint64(0))

	// Dead nodes are reported once, not on every tick.
	for i := 0; i < 5; i++ {
		clk.Advance(time.Minute)
		c.Heartbeat("n2")
		c.reaper.checkLiveness()
	}
	time.Sleep(50 * time.Millisecond)
	assert.Equal(t, epochAfterDeath, c.GetConfig().Epoch, "no repeated failure handling for the same node")
}

// Real reaper loop (1s tick, real clock): silence -> dead without any manual checkLiveness call.
func TestController_Heartbeat_RealReaperLoopDeclaresDeath(t *testing.T) {
	c := NewController(&raft.Raft{}, 100*time.Millisecond)
	c.config.Nodes["n1"] = metadata.NodeMetadata{ID: "n1", Status: metadata.StatusActive}

	c.Heartbeat("n1")
	assert.Equal(t, metadata.StatusActive, c.GetConfig().Nodes["n1"].Status, "alive right after heartbeat")

	require.Eventually(t, func() bool {
		return c.GetConfig().Nodes["n1"].Status == metadata.StatusDead
	}, 5*time.Second, 50*time.Millisecond)
}
