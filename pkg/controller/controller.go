package controller

import (
	"encoding/json"
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

// applyUpdateTopology updates the local state from a Raft command
func (c *Controller) applyUpdateTopology(config *metadata.ClusterConfig) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.config = config
	slog.Info("Topology updated via Raft", "epoch", config.Epoch)
}

// applyRegisterNode updates the local state (internal use by FSM)
func (c *Controller) applyRegisterNode(nodeID, address string) {
	c.mu.Lock()
	// Check if node already exists
	if _, exists := c.config.Nodes[nodeID]; exists {
		c.mu.Unlock()
		slog.Info("Node already registered (apply)", "nodeID", nodeID)
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

	slog.Info("Node registered (apply)", "nodeID", nodeID, "address", address)

	// Trigger Rebalance (only on leader? or everyone? Rebalance initiates moves, so only leader should do it)
	// But applyRegisterNode runs on followers too.
	// Followers shouldn't trigger Rebalance.
	// We can check if we are leader before triggering Rebalance.
	// NOTE: Use IsLeaderUnsafe() because this is called from within Raft's appendEntries
	// which holds the Raft mutex. Using IsLeader() would cause a deadlock.
	if c.raft.IsLeaderUnsafe() {
		go c.Rebalance()
	}
}

// RegisterNode broadcasts a RegisterNode command to the cluster
func (c *Controller) RegisterNode(nodeID, address string) error {
	payload := RegisterNodePayload{
		NodeID:  nodeID,
		Address: address,
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	cmd := Command{
		Type:    CmdRegisterNode,
		Payload: data,
	}
	cmdBytes, err := json.Marshal(cmd)
	if err != nil {
		return err
	}

	msg := raft.Message{
		MsgType: raft.CommandMsg,
		Data:    cmdBytes,
	}

	success, _, err := c.raft.BroadcastSync(msg, 5*time.Second)
	if err != nil {
		return err
	}
	if !success {
		return fmt.Errorf("failed to register node: consensus not reached")
	}
	return nil
}

// Rebalance attempts to balance shards across all active nodes
// It assigns primaries and replicas to unassigned shards
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
		slog.Info("Rebalance: No active nodes")
		return
	}

	slog.Info("Rebalance: Starting", "activeNodes", len(activeNodes), "totalShards", totalShards)

	// Check for unassigned or under-replicated shards
	needsUpdate := false
	c.mu.RLock()
	for _, shard := range config.Shards {
		if shard.PrimaryID == "" {
			needsUpdate = true
			break
		}
		// Check if we need more replicas (want at least min(3, len(activeNodes)-1) replicas)
		desiredReplicas := len(activeNodes) - 1
		if desiredReplicas > 3 {
			desiredReplicas = 3
		}
		if len(shard.ReplicaIDs) < desiredReplicas {
			needsUpdate = true
			break
		}
	}
	c.mu.RUnlock()

	if !needsUpdate {
		slog.Info("Rebalance: No changes needed")
		return
	}

	c.mu.Lock()
	newConfig := *c.config
	newShards := make(map[int]metadata.ShardMetadata)
	for k, v := range c.config.Shards {
		newShards[k] = v
	}
	newConfig.Shards = newShards

	// Assign primaries and replicas for each shard
	nodeIndex := 0
	for shardID, shard := range newConfig.Shards {
		// Assign primary if not set
		if shard.PrimaryID == "" {
			shard.PrimaryID = activeNodes[nodeIndex%len(activeNodes)]
			shard.Status = metadata.ShardStatusActive
			slog.Info("Rebalance: Assigned primary", "shard", shardID, "primary", shard.PrimaryID)
			nodeIndex++
		}

		// Assign replicas (all other active nodes, up to 3 replicas)
		desiredReplicas := len(activeNodes) - 1
		if desiredReplicas > 3 {
			desiredReplicas = 3
		}

		if len(shard.ReplicaIDs) < desiredReplicas {
			// Build new replica list
			existingReplicas := make(map[string]bool)
			for _, r := range shard.ReplicaIDs {
				existingReplicas[r] = true
			}

			newReplicas := make([]string, 0, desiredReplicas)
			// Keep existing replicas that are still active
			for _, r := range shard.ReplicaIDs {
				if _, exists := config.Nodes[r]; exists && config.Nodes[r].Status == metadata.StatusActive {
					newReplicas = append(newReplicas, r)
				}
			}

			// Add more replicas from active nodes
			for _, nodeID := range activeNodes {
				if len(newReplicas) >= desiredReplicas {
					break
				}
				// Skip if this is the primary or already a replica
				if nodeID == shard.PrimaryID || existingReplicas[nodeID] {
					continue
				}
				newReplicas = append(newReplicas, nodeID)
				slog.Info("Rebalance: Added replica", "shard", shardID, "replica", nodeID)
			}
			shard.ReplicaIDs = newReplicas
		}

		newConfig.Shards[shardID] = shard
	}

	newConfig.Epoch++
	c.config = &newConfig
	c.mu.Unlock()

	slog.Info("Rebalance: Complete", "epoch", newConfig.Epoch)
}
