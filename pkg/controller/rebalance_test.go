package controller

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/dominikszczepaniak/distributed-cache/pkg/metadata"
	"github.com/dominikszczepaniak/distributed-cache/pkg/raft"
)

func TestController_Rebalance(t *testing.T) {
	// Mock DataNodes
	// We need them to respond to Pull requests during MoveShard
	nodeAServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/internal/export" {
			json.NewEncoder(w).Encode(map[string]string{"k": "v"})
		}
	}))
	defer nodeAServer.Close()

	nodeBServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/internal/pull" {
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer nodeBServer.Close()

	// Setup Controller
	c := NewController(&raft.Raft{}, 10*time.Second)

	// Setup Initial Topology: 10 Shards on Node A
	c.config.Nodes["nodeA"] = metadata.NodeMetadata{ID: "nodeA", Address: nodeAServer.Listener.Addr().String(), Status: metadata.StatusActive}
	for i := 0; i < 10; i++ {
		c.config.Shards[i] = metadata.ShardMetadata{ID: i, PrimaryID: "nodeA", Status: metadata.ShardStatusActive}
	}

	// Register Node B
	// This should trigger Rebalance in a goroutine, but for testing we might want to call it synchronously or wait.
	// RegisterNode calls `go c.Rebalance()`.
	// Let's call RegisterNode, then wait a bit, or call Rebalance manually to be deterministic.
	// Calling Rebalance manually is safer for unit test.

	c.config.Nodes["nodeB"] = metadata.NodeMetadata{ID: "nodeB", Address: nodeBServer.Listener.Addr().String(), Status: metadata.StatusActive}

	// Trigger Rebalance
	c.Rebalance()

	// Verify
	config := c.GetConfig()
	shardsA := 0
	shardsB := 0
	for _, shard := range config.Shards {
		if shard.PrimaryID == "nodeA" {
			shardsA++
		} else if shard.PrimaryID == "nodeB" {
			shardsB++
		}
	}

	if shardsA != 5 || shardsB != 5 {
		t.Errorf("Expected 5 shards each, got A:%d, B:%d", shardsA, shardsB)
	}
}
