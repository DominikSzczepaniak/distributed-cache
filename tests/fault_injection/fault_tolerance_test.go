package fault_injection

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type PutRequest struct {
	Key   int `json:"key"`
	Value int `json:"value"`
}

type GetResponse struct {
	Key   int  `json:"key"`
	Value int  `json:"value"`
	Found bool `json:"found"`
}

type LeaderResponse struct {
	IsLeader bool `json:"is_leader"`
	LeaderID int  `json:"leader_id"`
}

var nodeAPIAddresses = map[string]string{
	"raft-node-0": "http://localhost:8080",
	"raft-node-1": "http://localhost:8081",
	"raft-node-2": "http://localhost:8082",
}

const raftNetwork = "raft-cluster"

func TestMain(m *testing.M) {
	if err := startCluster(); err != nil {
		fmt.Printf("Failed to start cluster: %v\n", err)
		m.Run()
		return
	}

	exitCode := m.Run()

	if err := stopCluster(); err != nil {
		fmt.Printf("Failed to stop cluster: %v\n", err)
	}

	_ = exitCode
}

func TestLeaderFailureAndRecovery(t *testing.T) {
	require.NoError(t, waitForClusterReady(t), "Cluster should be ready before test")

	leader, err := findLeader(t)
	require.NoError(t, err, "Should be able to find the initial leader")
	t.Logf("Initial leader is %s", leader)

	t.Logf("Stopping leader node: %s", leader)
	require.NoError(t, stopNode(leader), "Should be able to stop the leader")

	var newLeader string
	require.Eventually(t, func() bool {
		newLeader, err = findLeader(t, leader)
		return err == nil && newLeader != ""
	}, 30*time.Second, 2*time.Second, "A new leader should be elected")
	t.Logf("New leader elected: %s", newLeader)

	testKey, testValue := 101, 1001
	t.Logf("Writing '%d=%d' to new leader %s", testKey, testValue, newLeader)
	require.NoError(t, setKeyValue(t, nodeAPIAddresses[newLeader], testKey, testValue), "Should be able to write to the new leader")

	t.Logf("Restarting original leader node: %s", leader)
	require.NoError(t, startNode(leader), "Should be able to restart the original leader")

	t.Logf("Verifying data synchronization on restarted node %s", leader)
	require.Eventually(t, func() bool {
		value, err := getKeyValue(t, nodeAPIAddresses[leader], testKey)
		return err == nil && value == testValue
	}, 30*time.Second, 2*time.Second, "Restarted node should sync and have the new value")

	t.Log("TestLeaderFailureAndRecovery successful!")
}

func runCommand(name string, args ...string) error {
	cmd := exec.Command(name, args...)
	// var out bytes.Buffer
	// var stderr bytes.Buffer
	// cmd.Stdout = &out
	// cmd.Stderr = &stderr
	cmd.Stdout = io.Discard
	cmd.Stderr = io.Discard

	if err := cmd.Run(); err != nil {
		// return fmt.Errorf("command '%s %s' failed: %w\nSTDOUT:\n%s\nSTDERR:\n%s", name, strings.Join(args, " "), err, out.String(), stderr.String())
		return fmt.Errorf("command '%s %s' failed: %w", name, strings.Join(args, " "), err)
	}
	return nil
}

func startCluster() error {
	fmt.Println("Starting cluster...")
	return runCommand("docker", "compose", "up", "-d", "--build")
}

func stopCluster() error {
	fmt.Println("Stopping cluster...")
	return runCommand("docker", "compose", "down", "-v")
}

func stopNode(nodeID string) error {
	return runCommand("docker", "compose", "stop", nodeID)
}

func startNode(nodeID string) error {
	return runCommand("docker", "compose", "start", nodeID)
}

func disconnectNode(nodeID string) error {
	return runCommand("docker", "network", "disconnect", raftNetwork, nodeID)
}

func connectNode(nodeID string) error {
	return runCommand("docker", "network", "connect", raftNetwork, nodeID)
}

func findLeader(t *testing.T, excludeNodes ...string) (string, error) {
	t.Helper()
	client := &http.Client{Timeout: 2 * time.Second}
	excludeSet := make(map[string]struct{})
	for _, node := range excludeNodes {
		excludeSet[node] = struct{}{}
	}

	for nodeID, addr := range nodeAPIAddresses {
		if _, excluded := excludeSet[nodeID]; excluded {
			continue
		}
		resp, err := client.Get(addr + "/leader")
		if err != nil {
			continue // Node might be down, try next one
		}
		defer resp.Body.Close()

		if resp.StatusCode == http.StatusOK {
			body, err := io.ReadAll(resp.Body)
			if err != nil {
				continue
			}
			var leaderResp LeaderResponse
			if err := json.Unmarshal(body, &leaderResp); err != nil {
				continue
			}
			if leaderResp.IsLeader {
				return nodeID, nil
			}
		}
	}
	return "", fmt.Errorf("no leader found")
}

func setKeyValue(t *testing.T, addr string, key int, value int) error {
	t.Helper()
	client := &http.Client{Timeout: 5 * time.Second}
	requestBody := PutRequest{Key: key, Value: value}
	data, err := json.Marshal(requestBody)
	if err != nil {
		return err
	}

	req, err := http.NewRequest("POST", fmt.Sprintf("%s/kv", addr), bytes.NewBuffer(data))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("failed to set key-value, status: %s, body: %s", resp.Status, string(body))
	}
	return nil
}

func getKeyValue(t *testing.T, addr string, key int) (int, error) {
	t.Helper()
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get(fmt.Sprintf("%s/kv/%d", addr, key))
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return 0, fmt.Errorf("failed to get key, status: %s, body: %s", resp.Status, string(body))
	}

	var result GetResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return 0, err
	}

	if !result.Found {
		return 0, fmt.Errorf("key %d not found", key)
	}

	return result.Value, nil
}

func waitForClusterReady(t *testing.T) error {
	t.Helper()
	var err error
	require.Eventually(t, func() bool {
		_, err = findLeader(t)
		return err == nil
	}, 20*time.Second, 1*time.Second, "timed out waiting for a leader to be elected")

	if err == nil {
		t.Log("Cluster is ready, leader found.")
	}
	return err
}
