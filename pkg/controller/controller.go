package controller

import (
	"log/slog"
	"sync"
	"time"

	"github.com/dominikszczepaniak/distributed-cache/pkg/metadata"
	"github.com/dominikszczepaniak/distributed-cache/pkg/raft"
)

type Controller struct {
	mu     sync.RWMutex
	raft   *raft.Raft
	reaper *Reaper
	config *metadata.ClusterConfig
}

func NewController(r *raft.Raft, gracePeriod time.Duration) *Controller {
	c := &Controller{
		raft:   r,
		config: metadata.NewClusterConfig(10), // Default 10 shards, should be loaded from Raft
	}

	c.reaper = NewReaper(gracePeriod, c.HandleNodeFailure)
	go c.reaper.Run()

	return c
}

func (c *Controller) HandleNodeFailure(nodeID string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	slog.Warn("Node failure detected", "nodeID", nodeID)

	// Rebalance topology
	newConfig := metadata.Rebalance(c.config, nodeID)

	// Propose new config to Raft
	// TODO: This should be a Raft command. For now we just update local state
	// and assume Raft will handle the consensus part later.
	// In a real implementation:
	// cmd := NewUpdateTopologyCommand(newConfig)
	// c.raft.BroadcastSync(cmd)

	c.config = newConfig
	slog.Info("Topology rebalanced", "epoch", newConfig.Epoch)
}

func (c *Controller) GetConfig() *metadata.ClusterConfig {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.config
}

func (c *Controller) Heartbeat(nodeID string) {
	c.reaper.Track(nodeID)
}

// SetTopology forces a specific cluster configuration (for testing/debugging)
func (c *Controller) SetTopology(config *metadata.ClusterConfig) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.config = config
	slog.Info("Topology manually set", "epoch", config.Epoch)
}
