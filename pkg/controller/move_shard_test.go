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
	sourceServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/internal/export" {
			json.NewEncoder(w).Encode(map[string]string{"k": "v"})
		}
	}))
	defer sourceServer.Close()

	targetServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/internal/pull" {
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer targetServer.Close()

	c := NewController(&raft.Raft{}, 10*time.Second)

	c.config.Nodes["nodeA"] = metadata.NodeMetadata{ID: "nodeA", Address: sourceServer.Listener.Addr().String(), Status: metadata.StatusActive}
	c.config.Nodes["nodeB"] = metadata.NodeMetadata{ID: "nodeB", Address: targetServer.Listener.Addr().String(), Status: metadata.StatusActive}
	c.config.Shards[0] = metadata.ShardMetadata{ID: 0, PrimaryID: "nodeA", Status: metadata.ShardStatusActive}
	c.config.Epoch = 1

	err := c.MoveShard(0, "nodeB")
	if err != nil {
		t.Fatalf("MoveShard failed: %v", err)
	}

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
