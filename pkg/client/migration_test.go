package client

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/dominikszczepaniak/distributed-cache/pkg/metadata"
)

func TestSmartClient_MigrationHandling(t *testing.T) {
	controller := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		config := metadata.ClusterConfig{
			Epoch:       1,
			TotalShards: 1,
			Nodes: map[string]metadata.NodeMetadata{
				"node1": {ID: "node1", Address: "127.0.0.1:9999", Status: metadata.StatusActive},
			},
			Shards: map[int]metadata.ShardMetadata{
				0: {ID: 0, PrimaryID: "node1", Status: metadata.ShardStatusActive},
			},
		}
		json.NewEncoder(w).Encode(config)
	}))
	defer controller.Close()

	attempts := 0
	dataNode := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		if attempts == 1 {
			w.WriteHeader(http.StatusLocked)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer dataNode.Close()

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

	client, err := NewSmartClient([]string{controller.URL})
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}

	err = client.Put("key1", "val1")
	if err != nil {
		t.Errorf("Put failed: %v", err)
	}

	if attempts < 2 {
		t.Errorf("Expected at least 2 attempts (1 retry), got %d", attempts)
	}
}
