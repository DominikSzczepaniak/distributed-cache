package replication

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/dominikszczepaniak/distributed-cache/pkg/raft/raftpb"
	"github.com/dominikszczepaniak/distributed-cache/pkg/sharding"
)

// Client handles synchronous replication from primary to backup nodes
// Includes circuit breaker pattern for fault tolerance
type Client struct {
	nodeID      sharding.NodeID
	raftClients map[sharding.NodeID]raftpb.RaftClient
	timeout     time.Duration
	mu          sync.RWMutex

	// Circuit breaker state
	failures    map[sharding.NodeID]int
	circuitOpen map[sharding.NodeID]bool
}

// NewClient creates a new replication client
func NewClient(nodeID sharding.NodeID, timeout time.Duration) *Client {
	return &Client{
		nodeID:      nodeID,
		raftClients: make(map[sharding.NodeID]raftpb.RaftClient),
		timeout:     timeout,
		failures:    make(map[sharding.NodeID]int),
		circuitOpen: make(map[sharding.NodeID]bool),
	}
}

// RegisterPeer adds a gRPC client for a peer node
func (c *Client) RegisterPeer(nodeID sharding.NodeID, client raftpb.RaftClient) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.raftClients[nodeID] = client
	slog.Info(fmt.Sprintf("ReplicationClient: Registered peer node %d", nodeID))
}

// Replicate sends a synchronous replication request to backup node
func (c *Client) Replicate(ctx context.Context, backupNodeID sharding.NodeID, key, value int) error {
	// Check circuit breaker
	c.mu.RLock()
	if c.circuitOpen[backupNodeID] {
		c.mu.RUnlock()
		return fmt.Errorf("circuit breaker open for node %d", backupNodeID)
	}
	client, exists := c.raftClients[backupNodeID]
	c.mu.RUnlock()

	if !exists {
		return fmt.Errorf("no client registered for backup node %d", backupNodeID)
	}

	// Create replication request
	req := &raftpb.ReplicateRequest{
		Key:       int32(key),
		Value:     int32(value),
		Operation: "PUT",
	}

	// Call with timeout
	ctxWithTimeout, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	resp, err := client.Replicate(ctxWithTimeout, req)
	if err != nil {
		c.recordFailure(backupNodeID)
		return fmt.Errorf("replication to node %d failed: %w", backupNodeID, err)
	}

	if !resp.Success {
		c.recordFailure(backupNodeID)
		return fmt.Errorf("backup node %d rejected: %s", backupNodeID, resp.Error)
	}

	// Success - reset failure count
	c.resetFailures(backupNodeID)
	return nil
}

// DeleteReplicate sends a synchronous delete replication to backup
func (c *Client) DeleteReplicate(ctx context.Context, backupNodeID sharding.NodeID, key int) error {
	// Check circuit breaker
	c.mu.RLock()
	if c.circuitOpen[backupNodeID] {
		c.mu.RUnlock()
		return fmt.Errorf("circuit breaker open for node %d", backupNodeID)
	}
	client, exists := c.raftClients[backupNodeID]
	c.mu.RUnlock()

	if !exists {
		return fmt.Errorf("no client registered for backup node %d", backupNodeID)
	}

	req := &raftpb.ReplicateRequest{
		Key:       int32(key),
		Operation: "DELETE",
	}

	ctxWithTimeout, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	resp, err := client.Replicate(ctxWithTimeout, req)
	if err != nil {
		c.recordFailure(backupNodeID)
		return fmt.Errorf("delete replication to node %d failed: %w", backupNodeID, err)
	}

	if !resp.Success {
		c.recordFailure(backupNodeID)
		return fmt.Errorf("backup node %d rejected delete: %s", backupNodeID, resp.Error)
	}

	c.resetFailures(backupNodeID)
	return nil
}

func (c *Client) recordFailure(nodeID sharding.NodeID) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.failures[nodeID]++
	if c.failures[nodeID] >= 3 {
		c.circuitOpen[nodeID] = true
		slog.Warn(fmt.Sprintf("Circuit breaker opened for node %d after %d failures", nodeID, c.failures[nodeID]))
	}
}

func (c *Client) resetFailures(nodeID sharding.NodeID) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.failures[nodeID] = 0
	c.circuitOpen[nodeID] = false
}

// ResetCircuitBreaker manually resets circuit breaker for a node
// Called after Raft reconfiguration assigns new backup
func (c *Client) ResetCircuitBreaker(nodeID sharding.NodeID) {
	c.resetFailures(nodeID)
	slog.Info(fmt.Sprintf("Circuit breaker manually reset for node %d", nodeID))
}
