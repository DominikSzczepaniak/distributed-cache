package client

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/dominikszczepaniak/distributed-cache/pkg/metadata"
)

func TestSmartClient_MigrationHandling(t *testing.T) {
	// Setup Mock Controller
	controller := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Return topology pointing to DataNode
		config := metadata.ClusterConfig{
			Epoch:       1,
			TotalShards: 1,
			Nodes: map[string]metadata.NodeMetadata{
				"node1": {ID: "node1", Address: "127.0.0.1:9999", Status: metadata.StatusActive}, // Address will be updated
			},
			Shards: map[int]metadata.ShardMetadata{
				0: {ID: 0, PrimaryID: "node1", Status: metadata.ShardStatusActive},
			},
		}
		json.NewEncoder(w).Encode(config)
	}))
	defer controller.Close()

	// Setup Mock DataNode
	attempts := 0
	dataNode := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		if attempts == 1 {
			// First attempt: Locked (Migration in progress)
			w.WriteHeader(http.StatusLocked)
			return
		}
		// Second attempt: Success
		w.WriteHeader(http.StatusOK)
	}))
	defer dataNode.Close()

	// Update Controller to point to actual DataNode address
	// We can't easily update the running controller handler logic without complex setup.
	// Instead, let's just make the client think the node address is the mock server address.
	// But the client fetches topology from controller.
	// So we need the controller to return the correct address.

	// Re-create Controller with correct DataNode address
	controller.Close()
	controller = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		config := metadata.ClusterConfig{
			Epoch:       1,
			TotalShards: 1,
			Nodes: map[string]metadata.NodeMetadata{
				"node1": {ID: "node1", Address: dataNode.Listener.Addr().String(), Status: metadata.StatusActive},
			},
			Shards: map[int]metadata.ShardMetadata{
				0: {ID: 0, PrimaryID: "node1", Status: metadata.ShardStatusActive},
			},
		}
		json.NewEncoder(w).Encode(config)
	}))
	defer controller.Close()

	// Create Client
	client, err := NewSmartClient([]string{controller.URL})
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}

	// Test Put
	err = client.Put("key1", "val1")
	if err != nil {
		t.Errorf("Put failed: %v", err)
	}

	if attempts < 2 {
		t.Errorf("Expected at least 2 attempts (1 retry), got %d", attempts)
	}
}
