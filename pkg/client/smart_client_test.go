package client

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/dominikszczepaniak/distributed-cache/pkg/metadata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSmartClient_FetchTopology(t *testing.T) {
	config := metadata.ClusterConfig{
		Epoch:       5,
		TotalShards: 2,
		Nodes: map[string]metadata.NodeMetadata{
			"node-1": {ID: "node-1", Address: "node-1:9000", Status: metadata.StatusActive},
			"node-2": {ID: "node-2", Address: "node-2:9000", Status: metadata.StatusActive},
		},
		Shards: map[int]metadata.ShardMetadata{
			0: {ID: 0, PrimaryID: "node-1", ReplicaIDs: []string{"node-2"}},
			1: {ID: 1, PrimaryID: "node-2", ReplicaIDs: []string{"node-1"}},
		},
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/topology" {
			json.NewEncoder(w).Encode(config)
		} else {
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	client, err := NewSmartClient([]string{server.URL})
	require.NoError(t, err)

	assert.Equal(t, uint64(5), client.GetEpoch())
	assert.Equal(t, 2, client.GetConfig().TotalShards)
}

func TestSmartClient_FetchTopology_Failover(t *testing.T) {
	config := metadata.ClusterConfig{Epoch: 10}

	// First controller fails
	failServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "Internal Error", http.StatusInternalServerError)
	}))
	defer failServer.Close()

	// Second controller succeeds
	goodServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(config)
	}))
	defer goodServer.Close()

	client, err := NewSmartClient([]string{failServer.URL, goodServer.URL})
	require.NoError(t, err)

	assert.Equal(t, uint64(10), client.GetEpoch())
}

func TestSmartClient_NoControllers(t *testing.T) {
	_, err := NewSmartClient([]string{})
	assert.Error(t, err)
}

func TestSmartClient_Route(t *testing.T) {
	config := metadata.ClusterConfig{
		Epoch:       5,
		TotalShards: 2,
		Nodes: map[string]metadata.NodeMetadata{
			"node-1": {ID: "node-1", Address: "node-1:9000", Status: metadata.StatusActive},
			"node-2": {ID: "node-2", Address: "node-2:9000", Status: metadata.StatusActive},
		},
		Shards: map[int]metadata.ShardMetadata{
			0: {ID: 0, PrimaryID: "node-1", ReplicaIDs: []string{"node-2"}},
			1: {ID: 1, PrimaryID: "node-2", ReplicaIDs: []string{"node-1"}},
		},
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(config)
	}))
	defer server.Close()

	client, err := NewSmartClient([]string{server.URL})
	require.NoError(t, err)

	// Route a key
	result, err := client.Route("test-key")
	require.NoError(t, err)

	assert.Equal(t, uint64(5), result.Epoch)
	assert.Contains(t, result.TargetURL, ":9000")
	assert.GreaterOrEqual(t, result.ShardID, 0)
	assert.Less(t, result.ShardID, 2)
}

func TestSmartClient_Put_Success(t *testing.T) {
	// DataNode mock
	datanodeMock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer datanodeMock.Close()

	datanodeAddr := datanodeMock.URL[7:] // Remove "http://"

	config := metadata.ClusterConfig{
		Epoch:       1,
		TotalShards: 1,
		Nodes: map[string]metadata.NodeMetadata{
			"node-1": {ID: "node-1", Address: datanodeAddr, Status: metadata.StatusActive},
		},
		Shards: map[int]metadata.ShardMetadata{
			0: {ID: 0, PrimaryID: "node-1", ReplicaIDs: []string{}},
		},
	}

	controllerMock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(config)
	}))
	defer controllerMock.Close()

	client, err := NewSmartClient([]string{controllerMock.URL})
	require.NoError(t, err)

	err = client.Put("key1", "value1")
	assert.NoError(t, err)
}

func TestSmartClient_Get_Success(t *testing.T) {
	// DataNode mock
	datanodeMock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("test-value"))
	}))
	defer datanodeMock.Close()

	datanodeAddr := datanodeMock.URL[7:]

	config := metadata.ClusterConfig{
		Epoch:       1,
		TotalShards: 1,
		Nodes: map[string]metadata.NodeMetadata{
			"node-1": {ID: "node-1", Address: datanodeAddr, Status: metadata.StatusActive},
		},
		Shards: map[int]metadata.ShardMetadata{
			0: {ID: 0, PrimaryID: "node-1", ReplicaIDs: []string{}},
		},
	}

	controllerMock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(config)
	}))
	defer controllerMock.Close()

	client, err := NewSmartClient([]string{controllerMock.URL})
	require.NoError(t, err)

	val, err := client.Get("key1")
	assert.NoError(t, err)
	assert.Equal(t, "test-value", val)
}
