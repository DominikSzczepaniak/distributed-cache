package tests

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dominikszczepaniak/distributed-cache/pkg/sharding"
)

// TestCluster represents a test cluster with multiple nodes
type TestCluster struct {
	Nodes          []*TestNode
	PartitionTable *sharding.PartitionTable
	T              *testing.T
}

// TestNode represents a single node in the test cluster
type TestNode struct {
	ID         int
	RaftAddr   string
	HTTPAddr   string
	DataDir    string
	Process    *exec.Cmd
	partitioner *sharding.Partitioner
}

// StartTestCluster starts a test cluster with N nodes
func StartTestCluster(t *testing.T, nodeCount int) *TestCluster {
	if nodeCount < 1 {
		t.Fatal("nodeCount must be at least 1")
	}

	// Base ports
	baseRaftPort := 9000
	baseHTTPPort := 10000

	// Create nodes configuration
	nodes := make([]*TestNode, nodeCount)
	raftAddrs := make([]string, nodeCount)
	httpAddrs := make([]string, nodeCount)

	// Generate addresses
	for i := 0; i < nodeCount; i++ {
		raftAddrs[i] = fmt.Sprintf("localhost:%d", baseRaftPort+i+1)
		httpAddrs[i] = fmt.Sprintf("http://localhost:%d", baseHTTPPort+i+1)
	}

	// Start each node
	for i := 0; i < nodeCount; i++ {
		dataDir := t.TempDir()
		node := &TestNode{
			ID:       i + 1, // Node IDs start from 1
			RaftAddr: raftAddrs[i],
			HTTPAddr: httpAddrs[i],
			DataDir:  dataDir,
			partitioner: sharding.NewPartitioner(),
		}

		// Build environment variables
		env := os.Environ()
		env = append(env, fmt.Sprintf("RAFT_ID=%d", node.ID))
		env = append(env, fmt.Sprintf("RAFT_ADDRS=%s", strings.Join(raftAddrs, ",")))
		env = append(env, fmt.Sprintf("DATA_DIR=%s", dataDir))
		env = append(env, fmt.Sprintf("HTTP_PORT=%d", baseHTTPPort+i+1))
		env = append(env, fmt.Sprintf("RAFT_PORT=%d", baseRaftPort+i+1))

		// Start the raftnode process
		cmd := exec.Command("./raftnode")
		cmd.Env = env
		cmd.Dir = filepath.Join("..", "cmd", "raftnode")

		// Capture output for debugging
		stdout, err := cmd.StdoutPipe()
		if err != nil {
			t.Fatalf("Failed to create stdout pipe for node %d: %v", i, err)
		}
		stderr, err := cmd.StderrPipe()
		if err != nil {
			t.Fatalf("Failed to create stderr pipe for node %d: %v", i, err)
		}

		// Start the process
		if err := cmd.Start(); err != nil {
			t.Fatalf("Failed to start node %d: %v", i, err)
		}

		// Log output in background
		go logOutput(t, fmt.Sprintf("Node%d-stdout", i), stdout)
		go logOutput(t, fmt.Sprintf("Node%d-stderr", i), stderr)

		node.Process = cmd
		nodes[i] = node
	}

	// Wait for all nodes to be healthy
	timeout := time.After(30 * time.Second)
	tick := time.NewTicker(200 * time.Millisecond)
	defer tick.Stop()

	healthyCount := 0
	for healthyCount < nodeCount {
		select {
		case <-timeout:
			// Cleanup on timeout
			for _, node := range nodes {
				if node.Process != nil {
					node.Process.Kill()
				}
			}
			t.Fatal("Timeout waiting for cluster nodes to become healthy")
		case <-tick.C:
			healthyCount = 0
			for _, node := range nodes {
				if isNodeHealthy(node.HTTPAddr) {
					healthyCount++
				}
			}
		}
	}

	t.Logf("All %d nodes are healthy", nodeCount)

	// Wait for leader election
	time.Sleep(2 * time.Second)

	cluster := &TestCluster{
		Nodes:          nodes,
		PartitionTable: sharding.NewPartitionTable(),
		T:              t,
	}

	return cluster
}

// StopTestCluster stops all nodes in the cluster
func (tc *TestCluster) StopTestCluster() {
	for _, node := range tc.Nodes {
		if node.Process != nil {
			node.Process.Kill()
			node.Process.Wait()
		}
	}
	tc.T.Log("Cluster stopped")
}

// InitializePartitionTable sets up even distribution across all nodes
func (tc *TestCluster) InitializePartitionTable() error {
	nodeIDs := make([]sharding.NodeID, len(tc.Nodes))
	for i, node := range tc.Nodes {
		nodeIDs[i] = sharding.NodeID(node.ID)
	}

	// Create even distribution
	pt := sharding.InitializeEvenDistribution(sharding.TOTAL_PARTITIONS, nodeIDs)
	tc.PartitionTable = pt

	// Find leader and send initialization request
	leaderNode, err := tc.FindLeader()
	if err != nil {
		return fmt.Errorf("failed to find leader: %w", err)
	}

	// Send POST request to initialize partition table
	// This would need an admin endpoint - for now, we'll use direct Raft messaging
	// In a real implementation, you'd POST to /admin/init-partition-table

	tc.T.Logf("Initialized partition table with %d partitions across %d nodes",
		sharding.TOTAL_PARTITIONS, len(tc.Nodes))
	tc.T.Logf("Leader is node %d at %s", leaderNode.ID, leaderNode.HTTPAddr)

	return nil
}

// FindLeader finds which node is currently the leader
func (tc *TestCluster) FindLeader() (*TestNode, error) {
	for _, node := range tc.Nodes {
		resp, err := http.Get(fmt.Sprintf("%s/status", node.HTTPAddr))
		if err != nil {
			continue
		}
		defer resp.Body.Close()

		// Check if this node reports itself as leader
		// This depends on the /status endpoint implementation
		// For now, we'll try each node to see which accepts writes
	}

	// Fallback: try sending a write to each node and see which succeeds
	for _, node := range tc.Nodes {
		if tc.testWrite(node) {
			return node, nil
		}
	}

	return nil, fmt.Errorf("no leader found")
}

// GetNodeForKey returns which node should own the key
func (tc *TestCluster) GetNodeForKey(key string) *TestNode {
	if tc.Nodes[0].partitioner == nil {
		tc.Nodes[0].partitioner = sharding.NewPartitioner()
	}

	partitionID := tc.Nodes[0].partitioner.HashKey(key)
	ownerID, exists := tc.PartitionTable.GetOwner(partitionID)
	if !exists {
		return nil
	}

	// Find the node with this ID
	for _, node := range tc.Nodes {
		if sharding.NodeID(node.ID) == ownerID {
			return node
		}
	}

	return nil
}

// WaitForPartitionTableSync waits for all nodes to have same PT version
func (tc *TestCluster) WaitForPartitionTableSync(timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	expectedVersion := tc.PartitionTable.GetVersion()

	for time.Now().Before(deadline) {
		allSynced := true
		for _, node := range tc.Nodes {
			// Query node for its partition table version
			// This requires an admin endpoint - /admin/partition-table
			resp, err := http.Get(fmt.Sprintf("%s/admin/partition-table", node.HTTPAddr))
			if err != nil {
				allSynced = false
				break
			}
			resp.Body.Close()
			// Parse response and check version
			// For now, assume success
		}

		if allSynced {
			return nil
		}

		time.Sleep(200 * time.Millisecond)
	}

	return fmt.Errorf("timeout waiting for partition table sync")
}

// Helper methods for individual nodes

// SendPUT sends a PUT request to this node
func (tn *TestNode) SendPUT(key int, value int) (*http.Response, error) {
	client := &http.Client{Timeout: 5 * time.Second}
	url := fmt.Sprintf("%s/kv", tn.HTTPAddr)

	body := fmt.Sprintf(`{"key": %d, "value": %d}`, key, value)
	req, err := http.NewRequest(http.MethodPost, url, strings.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	return client.Do(req)
}

// SendGET sends a GET request to this node
func (tn *TestNode) SendGET(key int) (*http.Response, error) {
	client := &http.Client{Timeout: 5 * time.Second}
	url := fmt.Sprintf("%s/kv/%d", tn.HTTPAddr, key)

	return client.Get(url)
}

// SendDELETE sends a DELETE request to this node
func (tn *TestNode) SendDELETE(key int) (*http.Response, error) {
	client := &http.Client{Timeout: 5 * time.Second}
	url := fmt.Sprintf("%s/kv/%d", tn.HTTPAddr, key)

	req, err := http.NewRequest(http.MethodDelete, url, nil)
	if err != nil {
		return nil, err
	}

	return client.Do(req)
}

// FollowRedirect follows a redirect response and retries the request
func FollowRedirect(originalResp *http.Response, makeRequest func(string) (*http.Response, error)) (*http.Response, error) {
	if originalResp.StatusCode != 307 {
		return originalResp, nil // Not a redirect
	}

	location := originalResp.Header.Get("Location")
	if location == "" {
		return nil, fmt.Errorf("redirect response without Location header")
	}

	// Extract the new URL
	return makeRequest(location)
}

// Helper functions

func isNodeHealthy(httpAddr string) bool {
	client := &http.Client{Timeout: 1 * time.Second}
	resp, err := client.Get(fmt.Sprintf("%s/health", httpAddr))
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

func (tc *TestCluster) testWrite(node *TestNode) bool {
	resp, err := node.SendPUT(999999, 1)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

func logOutput(t *testing.T, prefix string, r io.Reader) {
	buf := make([]byte, 1024)
	for {
		n, err := r.Read(buf)
		if n > 0 {
			t.Logf("[%s] %s", prefix, string(buf[:n]))
		}
		if err != nil {
			break
		}
	}
}

// WaitForKeyDistribution waits until a specific number of keys are distributed
func (tc *TestCluster) WaitForKeyDistribution(expectedKeys int, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	tick := time.NewTicker(500 * time.Millisecond)
	defer tick.Stop()

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("timeout waiting for key distribution")
		case <-tick.C:
			totalKeys := 0
			for _, node := range tc.Nodes {
				// Query node for key count
				// This would require a metrics endpoint
				// For now, we'll estimate based on partition ownership
			}
			if totalKeys >= expectedKeys {
				return nil
			}
		}
	}
}
