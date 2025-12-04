package tests

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"os/exec"
	"testing"
	"time"

	"github.com/dominikszczepaniak/distributed-cache/pkg/metadata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Helper to run docker commands
func runDockerCmd(args ...string) error {
	cmd := exec.Command("docker", args...)
	return cmd.Run()
}

// Helper to make HTTP POST requests
func httpPost(url string, body interface{}) (*http.Response, error) {
	jsonBody, _ := json.Marshal(body)
	return http.Post(url, "application/json", bytes.NewBuffer(jsonBody))
}

// Helper to make HTTP GET requests
func httpGet(url string) (string, int, error) {
	resp, err := http.Get(url)
	if err != nil {
		return "", 0, err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return string(body), resp.StatusCode, nil
}

func TestDataSafety_DataSurvivesCrash(t *testing.T) {
	// 1. Start Environment
	t.Log("Starting environment...")
	runDockerCmd("compose", "down")
	err := runDockerCmd("compose", "up", "-d", "--build")
	require.NoError(t, err, "Failed to start docker-compose")
	defer runDockerCmd("compose", "down")

	time.Sleep(10 * time.Second)

	controllerURL := "http://localhost:8080"
	datanode1URL := "http://localhost:9010"
	datanode2URL := "http://localhost:9011"

	// 2. Configure Topology: Node A = Primary, Node B = Replica
	t.Log("Configuring topology...")
	config := metadata.ClusterConfig{
		Epoch:       1,
		TotalShards: 1,
		Nodes: map[string]metadata.NodeMetadata{
			"datanode-1:9000": {ID: "datanode-1:9000", Address: "datanode-1:9000", Status: metadata.StatusActive},
			"datanode-2:9000": {ID: "datanode-2:9000", Address: "datanode-2:9000", Status: metadata.StatusActive},
		},
		Shards: map[int]metadata.ShardMetadata{
			0: {ID: 0, PrimaryID: "datanode-1:9000", ReplicaIDs: []string{"datanode-2:9000"}},
		},
	}
	resp, err := httpPost(controllerURL+"/debug/config", config)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	time.Sleep(5 * time.Second)

	// 3. Write to Node A (Primary)
	t.Log("Writing key '100' to Node A (Primary)...")
	writeReq := map[string]interface{}{
		"key":   "100",
		"value": "test-value",
		"epoch": 1,
	}
	resp, err = httpPost(datanode1URL+"/data", writeReq)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// 4. Crash Node A
	t.Log("Stopping Node A...")
	err = runDockerCmd("stop", "datanode-1")
	require.NoError(t, err)

	// 5. Wait for Failover
	t.Log("Waiting for failover...")
	time.Sleep(10 * time.Second)

	// 6. Verify Failover (B is now Primary)
	t.Log("Verifying failover...")
	resp, err = http.Get(controllerURL + "/topology")
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var newConfig metadata.ClusterConfig
	err = json.NewDecoder(resp.Body).Decode(&newConfig)
	require.NoError(t, err)
	resp.Body.Close()

	assert.Equal(t, "datanode-2:9000", newConfig.Shards[0].PrimaryID)

	// 7. Read from Node B (New Primary)
	t.Log("Reading key '100' from Node B (New Primary)...")
	body, statusCode, err := httpGet(datanode2URL + "/data?key=100")
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, statusCode)
	assert.Equal(t, "test-value", body)
}

func TestDataSafety_WritesFailWithoutReplica(t *testing.T) {
	// 1. Start Environment
	t.Log("Starting environment...")
	runDockerCmd("compose", "down")
	err := runDockerCmd("compose", "up", "-d", "--build")
	require.NoError(t, err, "Failed to start docker-compose")
	defer runDockerCmd("compose", "down")

	time.Sleep(10 * time.Second)

	controllerURL := "http://localhost:8080"
	datanode1URL := "http://localhost:9010"

	// 2. Configure Topology: Node A = Primary, Node B = Replica
	t.Log("Configuring topology...")
	config := metadata.ClusterConfig{
		Epoch:       1,
		TotalShards: 1,
		Nodes: map[string]metadata.NodeMetadata{
			"datanode-1:9000": {ID: "datanode-1:9000", Address: "datanode-1:9000", Status: metadata.StatusActive},
			"datanode-2:9000": {ID: "datanode-2:9000", Address: "datanode-2:9000", Status: metadata.StatusActive},
		},
		Shards: map[int]metadata.ShardMetadata{
			0: {ID: 0, PrimaryID: "datanode-1:9000", ReplicaIDs: []string{"datanode-2:9000"}},
		},
	}
	resp, err := httpPost(controllerURL+"/debug/config", config)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	time.Sleep(5 * time.Second)

	// 3. Stop Node B (Replica)
	t.Log("Stopping Node B (Replica)...")
	err = runDockerCmd("stop", "datanode-2")
	require.NoError(t, err)

	time.Sleep(2 * time.Second)

	// 4. Write to Node A - Should Fail (500 Replication Failed)
	t.Log("Writing to Node A (without Replica)...")
	writeReq := map[string]interface{}{
		"key":   "unsafe-key",
		"value": "unsafe-value",
		"epoch": 1,
	}
	resp, err = httpPost(datanode1URL+"/data", writeReq)
	require.NoError(t, err)
	assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)
}
