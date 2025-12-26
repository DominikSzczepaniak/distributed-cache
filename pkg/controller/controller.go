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

// Controller is the central coordinator of the distributed cache cluster.
// It manages topology, node health, and consistency via Raft.
type Controller struct {
	mu     sync.RWMutex
	raft   *raft.Raft
	reaper *Reaper
	config *metadata.ClusterConfig
}

// NewController initializes a new cluster coordinator with Raft and a failure detection reaper.
func NewController(r *raft.Raft, gracePeriod time.Duration) *Controller {
	c := &Controller{
		raft:   r,
		config: metadata.NewClusterConfig(10),
	}

	c.reaper = NewReaper(gracePeriod, c.HandleNodeFailure)
	go c.reaper.Run()

	return c
}

// MoveShard orchestrates a multi-phase migration of a shard from one node to another.
// Phase 1: Trigger a data pull on the target node from the source node (non-blocking).
// Phase 2: Lock the shard in the topology (StatusLocked) and increment the Epoch. This prevents new writes.
// Phase 3: Trigger a final catch-up pull on the target node to ensure all data is migrated.
// Phase 4: Update the topology to set the target node as the new primary and unlock the shard (StatusActive).
// MoveShard manually triggers a migration of a shard to a different node.
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

	if err := c.triggerPull(targetNode.Address, sourceNode.Address, shardID); err != nil {
		return fmt.Errorf("phase 1 copy failed: %w", err)
	}

	c.mu.Lock()
	newConfig := *c.config
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

// HandleNodeFailure is called when the Reaper detects that a node has missed its heartbeat deadline.
// It marks the node as failed and triggers a cluster-wide rebalance to reassign its shards.
// HandleNodeFailure is called when the reaper detects a node as non-responsive.
func (c *Controller) HandleNodeFailure(nodeID string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	slog.Warn("Node failure detected", "nodeID", nodeID)

	newConfig := metadata.Rebalance(c.config, nodeID)

	// TODO: This should be a Raft command. For now we just update local state
	// and assume Raft will handle the consensus part later.
	// Maybe?:
	// cmd := NewUpdateTopologyCommand(newConfig)
	// c.raft.BroadcastSync(cmd)

	c.config = newConfig
	slog.Info("Topology rebalanced", "epoch", newConfig.Epoch)

	go c.Rebalance()
}

// GetConfig returns the current global view of the cluster configuration.
func (c *Controller) GetConfig() *metadata.ClusterConfig {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.config
}

// Heartbeat records a node's presence to prevent it from being reaped.
func (c *Controller) Heartbeat(nodeID string) {
	c.reaper.Track(nodeID)
}

// SetTopology forces a specific cluster configuration (useful for testing and bootstrapping).
func (c *Controller) SetTopology(config *metadata.ClusterConfig) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.config = config
	slog.Info("Topology manually set", "epoch", config.Epoch)
}

func (c *Controller) applyUpdateTopology(config *metadata.ClusterConfig) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.config = config
	slog.Info("Topology updated via Raft", "epoch", config.Epoch)
}

func (c *Controller) applyRegisterNode(nodeID, address string) {
	c.mu.Lock()
	if _, exists := c.config.Nodes[nodeID]; exists {
		c.mu.Unlock()
		slog.Info("Node already registered (apply)", "nodeID", nodeID)
		return
	}

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

	// NOTE: We need to use IsLeaderUnsafe() because this is called from within Raft's
	// appendEntries which holds the Raft mutex. Using IsLeader() would cause a deadlock.
	if c.raft.IsLeaderUnsafe() {
		go c.Rebalance()
	}
}

// RegisterNode adding a new node to the cluster metadata.
// It broadcasts a CmdRegisterNode message via Raft to ensure all controller replicas agree on the new membership.
// Once consensus is reached, the node is added to the topology.
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

// replicaSyncTask represents a pending data synchronization from primary to replica
type replicaSyncTask struct {
	shardID   int
	primaryID string
	replicaID string
}

// Rebalance ensures that all shards are assigned a primary and the desired number of replicas.
// It attempts to distribute shards evenly across all active nodes and broadcasts updates via Raft.
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

	needsUpdate := false
	c.mu.RLock()
	for _, shard := range config.Shards {
		if shard.PrimaryID == "" {
			needsUpdate = true
			break
		}

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

	var syncTasks []replicaSyncTask

	c.mu.Lock()
	newConfig := *c.config
	newShards := make(map[int]metadata.ShardMetadata)
	for k, v := range c.config.Shards {
		newShards[k] = v
	}
	newConfig.Shards = newShards

	nodeIndex := 0
	for shardID, shard := range newConfig.Shards {
		if shard.PrimaryID == "" {
			shard.PrimaryID = activeNodes[nodeIndex%len(activeNodes)]
			shard.Status = metadata.ShardStatusActive
			slog.Info("Rebalance: Assigned primary", "shard", shardID, "primary", shard.PrimaryID)
			nodeIndex++
		}

		desiredReplicas := len(activeNodes) - 1
		if desiredReplicas > 3 {
			desiredReplicas = 3
		}

		if len(shard.ReplicaIDs) < desiredReplicas {
			existingReplicas := make(map[string]bool)
			for _, r := range shard.ReplicaIDs {
				existingReplicas[r] = true
			}

			newReplicas := make([]string, 0, desiredReplicas)
			for _, r := range shard.ReplicaIDs {
				if _, exists := config.Nodes[r]; exists && config.Nodes[r].Status == metadata.StatusActive {
					newReplicas = append(newReplicas, r)
				}
			}

			for _, nodeID := range activeNodes {
				if len(newReplicas) >= desiredReplicas {
					break
				}
				if nodeID == shard.PrimaryID || existingReplicas[nodeID] {
					continue
				}
				newReplicas = append(newReplicas, nodeID)
				slog.Info("Rebalance: Added replica", "shard", shardID, "replica", nodeID)

				syncTasks = append(syncTasks, replicaSyncTask{
					shardID:   shardID,
					primaryID: shard.PrimaryID,
					replicaID: nodeID,
				})
			}
			shard.ReplicaIDs = newReplicas
		}

		newConfig.Shards[shardID] = shard
	}

	newConfig.Epoch++
	c.config = &newConfig
	c.mu.Unlock()

	slog.Info("Rebalance: Complete (local)", "epoch", newConfig.Epoch)

	if err := c.BroadcastTopologyUpdate(&newConfig); err != nil {
		slog.Error("Rebalance: Failed to broadcast topology update", "err", err)
	} else {
		slog.Info("Rebalance: Topology broadcasted via Raft", "epoch", newConfig.Epoch)
	}

	if len(syncTasks) > 0 {
		go c.syncNewReplicas(syncTasks, &newConfig)
	}
}

func (c *Controller) syncNewReplicas(tasks []replicaSyncTask, config *metadata.ClusterConfig) {
	for _, task := range tasks {
		primaryNode, ok1 := config.Nodes[task.primaryID]
		replicaNode, ok2 := config.Nodes[task.replicaID]
		if !ok1 || !ok2 {
			slog.Warn("Sync: Node not found", "primary", task.primaryID, "replica", task.replicaID)
			continue
		}

		slog.Info("Sync: Starting data pull", "shard", task.shardID, "from", task.primaryID, "to", task.replicaID)

		if err := c.triggerPull(replicaNode.Address, primaryNode.Address, task.shardID); err != nil {
			slog.Error("Sync: Failed to pull data to replica", "shard", task.shardID, "replica", task.replicaID, "err", err)
		} else {
			slog.Info("Sync: Data pull complete", "shard", task.shardID, "replica", task.replicaID)
		}
	}
}

// BroadcastTopologyUpdate sends the current cluster configuration to all nodes via Raft.
func (c *Controller) BroadcastTopologyUpdate(config *metadata.ClusterConfig) error {
	payload := UpdateTopologyPayload{
		Config: *config,
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	cmd := Command{
		Type:    CmdUpdateTopology,
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
		return fmt.Errorf("failed to broadcast topology: consensus not reached")
	}
	return nil
}

// NoOp broadcasts a NoOp command through Raft to verify cluster consensus and health.
func (c *Controller) NoOp() error {
	cmd := Command{
		Type:    CmdNoOp,
		Payload: nil,
	}
	cmdBytes, _ := json.Marshal(cmd)

	msg := raft.Message{
		MsgType: raft.CommandMsg,
		Data:    cmdBytes,
	}

	success, _, err := c.raft.BroadcastSync(msg, 5*time.Second)
	if err != nil {
		return err
	}
	if !success {
		return fmt.Errorf("consensus not reached")
	}
	return nil
}
