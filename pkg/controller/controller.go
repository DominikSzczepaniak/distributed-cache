package controller

import (
	"fmt"
	"log/slog"
	"net/http"
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

// MoveShard orchestrates the migration of a shard from its current primary to a target node
func (c *Controller) MoveShard(shardID int, targetNodeID string) error {
	c.mu.Lock()
	config := c.config
	shard, exists := config.Shards[shardID]
	if !exists {
		c.mu.Unlock()
		return fmt.Errorf("shard %d not found", shardID)
	}
	sourceNodeID := shard.PrimaryID
	sourceNode, ok1 := config.Nodes[sourceNodeID]
	targetNode, ok2 := config.Nodes[targetNodeID]
	c.mu.Unlock()

	if !ok1 || !ok2 {
		return fmt.Errorf("source or target node not found")
	}

	slog.Info("Starting migration", "shard", shardID, "source", sourceNodeID, "target", targetNodeID)

	// Phase 1: Copy (Background)
	// Tell Target to pull from Source
	if err := c.triggerPull(targetNode.Address, sourceNode.Address, shardID); err != nil {
		return fmt.Errorf("phase 1 copy failed: %w", err)
	}

	// Phase 2: Freeze (Critical Section)
	c.mu.Lock()
	// Update status to Locked
	newConfig := *c.config // Shallow copy
	// Deep copy shards map
	newShards := make(map[int]metadata.ShardMetadata)
	for k, v := range c.config.Shards {
		newShards[k] = v
	}
	newConfig.Shards = newShards

	shardMeta := newConfig.Shards[shardID]
	shardMeta.Status = metadata.ShardStatusLocked
	newConfig.Shards[shardID] = shardMeta
	newConfig.Epoch++
	c.config = &newConfig
	c.mu.Unlock()
	slog.Info("Phase 2: Shard Locked", "shard", shardID, "epoch", newConfig.Epoch)

	// Phase 3: Catchup & Switch
	// Pull again to get diffs
	if err := c.triggerPull(targetNode.Address, sourceNode.Address, shardID); err != nil {
		// TODO: Rollback? For now just error out, system stays locked (safe but unavailable)
		return fmt.Errorf("phase 3 catchup failed: %w", err)
	}

	c.mu.Lock()
	newConfig2 := *c.config
	newShards2 := make(map[int]metadata.ShardMetadata)
	for k, v := range c.config.Shards {
		newShards2[k] = v
	}
	newConfig2.Shards = newShards2

	shardMeta2 := newConfig2.Shards[shardID]
	shardMeta2.PrimaryID = targetNodeID
	shardMeta2.Status = metadata.ShardStatusActive
	newConfig2.Shards[shardID] = shardMeta2
	newConfig2.Epoch++
	c.config = &newConfig2
	c.mu.Unlock()
	slog.Info("Phase 3: Migration Complete", "shard", shardID, "new_primary", targetNodeID, "epoch", newConfig2.Epoch)

	return nil
}

func (c *Controller) triggerPull(targetAddr, sourceAddr string, shardID int) error {
	// Target needs to pull from Source
	// POST http://target/internal/pull?source=http://source&shard=ID
	url := fmt.Sprintf("http://%s/internal/pull?source=http://%s&shard=%d", targetAddr, sourceAddr, shardID)
	resp, err := http.Post(url, "application/json", nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("target returned status %d", resp.StatusCode)
	}
	return nil
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

	// Trigger Rebalance to assign unassigned shards to active nodes
	go c.Rebalance()
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

// RegisterNode adds a new node to the cluster and triggers rebalancing
func (c *Controller) RegisterNode(nodeID, address string) {
	c.mu.Lock()
	// Check if node already exists
	if _, exists := c.config.Nodes[nodeID]; exists {
		c.mu.Unlock()
		slog.Info("Node already registered", "nodeID", nodeID)
		return
	}

	// Add node
	newConfig := *c.config
	newNodes := make(map[string]metadata.NodeMetadata)
	for k, v := range c.config.Nodes {
		newNodes[k] = v
	}
	newNodes[nodeID] = metadata.NodeMetadata{
		ID:            nodeID,
		Address:       address,
		Status:        metadata.StatusActive,
		LastHeartbeat: time.Now(),
	}
	newConfig.Nodes = newNodes
	newConfig.Epoch++
	c.config = &newConfig
	c.mu.Unlock()

	slog.Info("Node registered", "nodeID", nodeID, "address", address)

	// Trigger Rebalance
	go c.Rebalance()
}

// Rebalance attempts to balance shards across all active nodes
func (c *Controller) Rebalance() {
	c.mu.RLock()
	config := c.config
	activeNodes := []string{}
	for id, node := range config.Nodes {
		if node.Status == metadata.StatusActive {
			activeNodes = append(activeNodes, id)
		}
	}
	totalShards := len(config.Shards)
	c.mu.RUnlock()

	if len(activeNodes) == 0 {
		return
	}

	targetPerNode := totalShards / len(activeNodes)
	if targetPerNode == 0 {
		targetPerNode = 1 // Avoid 0 if more nodes than shards
	}

	// Identify Rich and Poor nodes
	// We need to know how many shards each node has
	shardsPerNode := make(map[string][]int)
	for _, nodeID := range activeNodes {
		shardsPerNode[nodeID] = []int{}
	}

	// First, handle unassigned shards
	unassignedShards := []int{}
	c.mu.RLock()
	for _, shard := range config.Shards {
		if shard.PrimaryID == "" {
			unassignedShards = append(unassignedShards, shard.ID)
		} else {
			shardsPerNode[shard.PrimaryID] = append(shardsPerNode[shard.PrimaryID], shard.ID)
		}
	}
	c.mu.RUnlock()

	if len(unassignedShards) > 0 {
		c.mu.Lock()
		newConfig := *c.config
		newShards := make(map[int]metadata.ShardMetadata)
		for k, v := range c.config.Shards {
			newShards[k] = v
		}
		newConfig.Shards = newShards

		for i, shardID := range unassignedShards {
			targetNodeID := activeNodes[i%len(activeNodes)]
			shardMeta := newConfig.Shards[shardID]
			shardMeta.PrimaryID = targetNodeID
			shardMeta.Status = metadata.ShardStatusActive
			newConfig.Shards[shardID] = shardMeta

			// Update local count for subsequent rebalancing
			shardsPerNode[targetNodeID] = append(shardsPerNode[targetNodeID], shardID)
			slog.Info("Assigned unassigned shard", "shard", shardID, "node", targetNodeID)
		}
		newConfig.Epoch++
		c.config = &newConfig
		c.mu.Unlock()

		// Refresh config for next step
		config = c.config
	}

	// Move shards from Rich to Poor
	for _, richNodeID := range activeNodes {
		shards := shardsPerNode[richNodeID]
		if len(shards) > targetPerNode {
			// This node is Rich
			excess := len(shards) - targetPerNode
			for i := 0; i < excess; i++ {
				// Find a Poor node
				var targetNodeID string
				for _, poorNodeID := range activeNodes {
					if len(shardsPerNode[poorNodeID]) < targetPerNode {
						targetNodeID = poorNodeID
						break
					}
				}

				if targetNodeID != "" {
					// Move shard
					shardID := shards[i]
					slog.Info("Rebalancing: Moving shard", "shard", shardID, "from", richNodeID, "to", targetNodeID)
					if err := c.MoveShard(shardID, targetNodeID); err != nil {
						slog.Error("Rebalancing failed", "shard", shardID, "err", err)
					} else {
						// Update local counts to reflect move
						shardsPerNode[targetNodeID] = append(shardsPerNode[targetNodeID], shardID)
						// We don't remove from richNodeID list because we just iterate excess times
					}
				}
			}
		}
	}
}
