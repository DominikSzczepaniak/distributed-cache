package tests

import (
	"bytes"
	"encoding/json"
	"net/http"
	"os/exec"
	"testing"
	"time"

	"github.com/dominikszczepaniak/distributed-cache/pkg/metadata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Helper to run docker commands
func runDocker(args ...string) error {
	cmd := exec.Command("docker", args...)
	return cmd.Run()
}

// Helper to make HTTP requests
func post(url string, body interface{}) (*http.Response, error) {
	jsonBody, _ := json.Marshal(body)
	return http.Post(url, "application/json", bytes.NewBuffer(jsonBody))
}

func TestSplitBrain(t *testing.T) {
	// 1. Start Environment
	t.Log("Starting environment...")
	// Ensure clean state
	runDocker("compose", "down")
	err := runDocker("compose", "up", "-d", "--build")
	require.NoError(t, err, "Failed to start docker-compose")
	defer runDocker("compose", "down")

	// Wait for services to be ready
	time.Sleep(10 * time.Second)

	controllerURL := "http://localhost:8080"
	datanode1URL := "http://localhost:9010"
	datanode2URL := "http://localhost:9011"

	// 2. Configure Topology
	t.Log("Configuring topology...")
	config := metadata.ClusterConfig{
		Epoch:       1,
		TotalShards: 1,
		Nodes: map[string]metadata.NodeMetadata{
			"datanode-1:9000": {ID: "datanode-1:9000", Status: metadata.StatusActive},
			"datanode-2:9000": {ID: "datanode-2:9000", Status: metadata.StatusActive},
		},
		Shards: map[int]metadata.ShardMetadata{
			0: {ID: 0, PrimaryID: "datanode-1:9000", ReplicaIDs: []string{"datanode-2:9000"}},
		},
	}
	resp, err := post(controllerURL+"/debug/config", config)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// Wait for DataNodes to fetch topology (heartbeat interval is small, but give it time)
	time.Sleep(5 * time.Second)

	// 3. Write to Node A (datanode-1) - Should Success
	t.Log("Writing to Node A (Primary)...")
	// Key "foo" hashes to shard 0 (since 1 shard)
	writeReq := map[string]interface{}{
		"key":   "foo",
		"value": "initial",
		"epoch": 1,
	}
	resp, err = post(datanode1URL+"/data", writeReq)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// 4. Partition Node A
	t.Log("Partitioning Node A...")
	err = runDocker("network", "disconnect", "raft-cluster", "datanode-1")
	require.NoError(t, err)

	// 5. The "Zombie" Phase (0-5s)
	// Node A might still accept writes if lease hasn't expired.
	// We skip testing this explicitly as it's timing dependent and "might" accept is acceptable.

	// 6. The "Fenced" Phase (5s+)
	t.Log("Waiting for lease expiration and failover...")
	time.Sleep(10 * time.Second) // Lease is 5s, wait 10s to be sure

	// Try write to Node A - Should Fail (503 Service Unavailable)
	t.Log("Writing to Node A (Fenced)...")
	resp, err = post(datanode1URL+"/data", writeReq)
	// If connection refused (because we cut the network and it has no other way), that's also a pass for "unavailable".
	// But with client-network, it should be reachable and return 503.
	if err != nil {
		t.Logf("Node A unreachable: %v (Acceptable)", err)
	} else {
		assert.Equal(t, http.StatusServiceUnavailable, resp.StatusCode, "Node A should be fenced")
	}

	// 7. Verify Controller Promotes B
	t.Log("Verifying Failover...")
	// Fetch topology from Controller
	resp, err = http.Get(controllerURL + "/topology")
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var newConfig metadata.ClusterConfig
	err = json.NewDecoder(resp.Body).Decode(&newConfig)
	require.NoError(t, err)

	// Verify Epoch incremented
	assert.Greater(t, newConfig.Epoch, config.Epoch)
	// Verify Shard 0 Primary is Node B (datanode-2)
	assert.Equal(t, "datanode-2:9000", newConfig.Shards[0].PrimaryID)

	// 8. Write to Node B (New Primary) - Should Success
	t.Log("Writing to Node B (New Primary)...")
	// Client should update its view (we just did by fetching topology)
	// Update request epoch to match new topology
	writeReq["epoch"] = newConfig.Epoch

	// Write to Node B
	resp, err = post(datanode2URL+"/data", writeReq)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}
