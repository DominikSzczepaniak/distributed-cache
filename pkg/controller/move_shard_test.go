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

func TestController_MoveShard(t *testing.T) {
	// Mock DataNodes
	sourceServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/internal/export" {
			// Return some data
			json.NewEncoder(w).Encode(map[string]string{"k": "v"})
		}
	}))
	defer sourceServer.Close()

	targetServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/internal/pull" {
			// Simulate pull success
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer targetServer.Close()

	// Setup Controller
	// We need a dummy Raft or nil if not used in MoveShard (it's not used yet, just local config update)
	c := NewController(&raft.Raft{}, 10*time.Second)

	// Setup Initial Topology
	// Node A (Source), Node B (Target)
	// Shard 0 on Node A
	c.config.Nodes["nodeA"] = metadata.NodeMetadata{ID: "nodeA", Address: sourceServer.Listener.Addr().String(), Status: metadata.StatusActive}
	c.config.Nodes["nodeB"] = metadata.NodeMetadata{ID: "nodeB", Address: targetServer.Listener.Addr().String(), Status: metadata.StatusActive}
	c.config.Shards[0] = metadata.ShardMetadata{ID: 0, PrimaryID: "nodeA", Status: metadata.ShardStatusActive}
	c.config.Epoch = 1

	// Execute MoveShard
	err := c.MoveShard(0, "nodeB")
	if err != nil {
		t.Fatalf("MoveShard failed: %v", err)
	}

	// Verify Final State
	config := c.GetConfig()
	shard := config.Shards[0]

	if shard.PrimaryID != "nodeB" {
		t.Errorf("Expected PrimaryID nodeB, got %s", shard.PrimaryID)
	}
	if shard.Status != metadata.ShardStatusActive {
		t.Errorf("Expected Status ACTIVE, got %s", shard.Status)
	}
	if config.Epoch <= 1 {
		t.Errorf("Expected Epoch > 1, got %d", config.Epoch)
	}
}
