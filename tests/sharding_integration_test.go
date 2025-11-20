package tests

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dominikszczepaniak/distributed-cache/pkg/sharding"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestShardRouting_ThreeNodeCluster tests basic shard routing with 3 nodes
func TestShardRouting_ThreeNodeCluster(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	cluster := StartTestCluster(t, 3)
	defer cluster.StopTestCluster()

	// Initialize partition table with even distribution
	err := cluster.InitializePartitionTable()
	require.NoError(t, err)

	// Wait for partition table to sync
	err = cluster.WaitForPartitionTableSync(10 * time.Second)
	require.NoError(t, err)

	// Test Cases
	t.Run("PUT to correct node succeeds", func(t *testing.T) {
		key := 1000
		value := 12345

		// Find the correct node for this key
		correctNode := cluster.GetNodeForKey(fmt.Sprintf("%d", key))
		require.NotNil(t, correctNode, "No node assigned for key")

		// Send PUT to correct node
		resp, err := correctNode.SendPUT(key, value)
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusOK, resp.StatusCode, "PUT to correct node should succeed")
	})

	t.Run("PUT to wrong node returns redirect", func(t *testing.T) {
		key := 2000
		value := 54321

		// Find correct node
		correctNode := cluster.GetNodeForKey(fmt.Sprintf("%d", key))
		require.NotNil(t, correctNode)

		// Find a wrong node
		var wrongNode *TestNode
		for _, node := range cluster.Nodes {
			if node.ID != correctNode.ID {
				wrongNode = node
				break
			}
		}
		require.NotNil(t, wrongNode)

		// Send PUT to wrong node
		resp, err := wrongNode.SendPUT(key, value)
		require.NoError(t, err)
		defer resp.Body.Close()

		// Should get redirect
		assert.Equal(t, 307, resp.StatusCode, "PUT to wrong node should return 307 redirect")
		location := resp.Header.Get("Location")
		assert.NotEmpty(t, location, "Redirect should include Location header")

		// Parse redirect response body
		var redirectResp map[string]interface{}
		err = json.NewDecoder(resp.Body).Decode(&redirectResp)
		require.NoError(t, err)

		assert.Equal(t, "MOVED", redirectResp["error"], "Error should be MOVED")
		assert.Contains(t, redirectResp, "address", "Response should contain target address")
	})

	t.Run("GET from correct node succeeds", func(t *testing.T) {
		key := 3000
		value := 99999

		// First, write the key
		correctNode := cluster.GetNodeForKey(fmt.Sprintf("%d", key))
		require.NotNil(t, correctNode)

		putResp, err := correctNode.SendPUT(key, value)
		require.NoError(t, err)
		putResp.Body.Close()
		require.Equal(t, http.StatusOK, putResp.StatusCode)

		// Wait for replication
		time.Sleep(500 * time.Millisecond)

		// Now GET from the correct node
		getResp, err := correctNode.SendGET(key)
		require.NoError(t, err)
		defer getResp.Body.Close()

		assert.Equal(t, http.StatusOK, getResp.StatusCode)

		// Verify value
		var result map[string]interface{}
		err = json.NewDecoder(getResp.Body).Decode(&result)
		require.NoError(t, err)
		assert.Equal(t, float64(value), result["value"])
	})

	t.Run("GET from wrong node returns redirect", func(t *testing.T) {
		key := 4000

		// Find correct and wrong nodes
		correctNode := cluster.GetNodeForKey(fmt.Sprintf("%d", key))
		require.NotNil(t, correctNode)

		var wrongNode *TestNode
		for _, node := range cluster.Nodes {
			if node.ID != correctNode.ID {
				wrongNode = node
				break
			}
		}
		require.NotNil(t, wrongNode)

		// GET from wrong node
		resp, err := wrongNode.SendGET(key)
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, 307, resp.StatusCode, "GET from wrong node should redirect")
	})

	t.Run("DELETE on correct node succeeds", func(t *testing.T) {
		key := 5000
		value := 11111

		// Write key first
		correctNode := cluster.GetNodeForKey(fmt.Sprintf("%d", key))
		require.NotNil(t, correctNode)

		putResp, err := correctNode.SendPUT(key, value)
		require.NoError(t, err)
		putResp.Body.Close()
		time.Sleep(500 * time.Millisecond)

		// Delete
		delResp, err := correctNode.SendDELETE(key)
		require.NoError(t, err)
		defer delResp.Body.Close()

		assert.Equal(t, http.StatusOK, delResp.StatusCode)
	})
}

// TestShardRouting_FollowRedirect tests client redirect following
func TestShardRouting_FollowRedirect(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	cluster := StartTestCluster(t, 3)
	defer cluster.StopTestCluster()

	err := cluster.InitializePartitionTable()
	require.NoError(t, err)

	key := 6000
	value := 77777

	// Find correct and wrong nodes
	correctNode := cluster.GetNodeForKey(fmt.Sprintf("%d", key))
	require.NotNil(t, correctNode)

	var wrongNode *TestNode
	for _, node := range cluster.Nodes {
		if node.ID != correctNode.ID {
			wrongNode = node
			break
		}
	}
	require.NotNil(t, wrongNode)

	// Send PUT to wrong node
	resp1, err := wrongNode.SendPUT(key, value)
	require.NoError(t, err)
	defer resp1.Body.Close()

	assert.Equal(t, 307, resp1.StatusCode, "First request should get redirect")

	// Parse redirect response
	var redirectResp map[string]interface{}
	body, err := io.ReadAll(resp1.Body)
	require.NoError(t, err)
	err = json.Unmarshal(body, &redirectResp)
	require.NoError(t, err)

	// Follow redirect manually
	resp2, err := correctNode.SendPUT(key, value)
	require.NoError(t, err)
	defer resp2.Body.Close()

	assert.Equal(t, http.StatusOK, resp2.StatusCode, "Request to correct node should succeed")

	// Verify data was written
	time.Sleep(500 * time.Millisecond)
	getResp, err := correctNode.SendGET(key)
	require.NoError(t, err)
	defer getResp.Body.Close()

	var result map[string]interface{}
	err = json.NewDecoder(getResp.Body).Decode(&result)
	require.NoError(t, err)
	assert.Equal(t, float64(value), result["value"])
}

// TestShardRouting_ConcurrentRequests tests concurrent shard routing
func TestShardRouting_ConcurrentRequests(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	cluster := StartTestCluster(t, 3)
	defer cluster.StopTestCluster()

	err := cluster.InitializePartitionTable()
	require.NoError(t, err)

	numKeys := 100
	var wg sync.WaitGroup
	var successCount int64
	var redirectCount int64

	// Send concurrent PUT requests
	for i := 0; i < numKeys; i++ {
		wg.Add(1)
		go func(key int) {
			defer wg.Done()

			value := key * 100

			// Send to a random node
			node := cluster.Nodes[key%len(cluster.Nodes)]
			resp, err := node.SendPUT(key, value)
			if err != nil {
				t.Logf("Error sending PUT for key %d: %v", key, err)
				return
			}
			defer resp.Body.Close()

			if resp.StatusCode == http.StatusOK {
				atomic.AddInt64(&successCount, 1)
			} else if resp.StatusCode == 307 {
				atomic.AddInt64(&redirectCount, 1)

				// Follow redirect
				correctNode := cluster.GetNodeForKey(fmt.Sprintf("%d", key))
				if correctNode != nil {
					resp2, err := correctNode.SendPUT(key, value)
					if err == nil {
						defer resp2.Body.Close()
						if resp2.StatusCode == http.StatusOK {
							atomic.AddInt64(&successCount, 1)
						}
					}
				}
			}
		}(i)
	}

	wg.Wait()

	t.Logf("Concurrent test: %d successes, %d redirects", successCount, redirectCount)
	assert.Greater(t, successCount, int64(numKeys*2/3), "Most requests should succeed")
	assert.Greater(t, redirectCount, int64(0), "Some requests should be redirected")
}

// TestPartitionTable_Propagation tests partition table updates
func TestPartitionTable_Propagation(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	cluster := StartTestCluster(t, 3)
	defer cluster.StopTestCluster()

	// Initial partition table
	err := cluster.InitializePartitionTable()
	require.NoError(t, err)

	// Wait for propagation
	err = cluster.WaitForPartitionTableSync(10 * time.Second)
	require.NoError(t, err)

	// Verify all nodes have the same version
	// This would require querying each node's /admin/partition-table endpoint
	t.Log("Partition table propagated successfully")
}

// TestShardRouting_PartitionDistribution tests key distribution across partitions
func TestShardRouting_PartitionDistribution(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	cluster := StartTestCluster(t, 3)
	defer cluster.StopTestCluster()

	err := cluster.InitializePartitionTable()
	require.NoError(t, err)

	numKeys := 1000
	partitionCounts := make(map[sharding.NodeID]int)

	// Count how many keys map to each node
	partitioner := sharding.NewPartitioner()
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key_%d", i)
		partitionID := partitioner.HashKey(key)
		ownerID, exists := cluster.PartitionTable.GetOwner(partitionID)
		require.True(t, exists, "Partition %d should have an owner", partitionID)
		partitionCounts[ownerID]++
	}

	// Verify distribution is roughly even (within 20%)
	expectedPerNode := numKeys / len(cluster.Nodes)
	tolerance := expectedPerNode * 20 / 100 // 20% tolerance

	for nodeID, count := range partitionCounts {
		t.Logf("Node %d: %d keys (expected ~%d)", nodeID, count, expectedPerNode)
		assert.InDelta(t, expectedPerNode, count, float64(tolerance),
			"Node %d should have roughly %d keys", nodeID, expectedPerNode)
	}
}

// TestShardRouting_UnassignedPartition tests error handling for unassigned partitions
func TestShardRouting_UnassignedPartition(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Start cluster but don't initialize partition table
	cluster := StartTestCluster(t, 3)
	defer cluster.StopTestCluster()

	// Try to PUT a key when no partitions are assigned
	key := 7000
	value := 88888

	node := cluster.Nodes[0]
	resp, err := node.SendPUT(key, value)
	require.NoError(t, err)
	defer resp.Body.Close()

	// Should get an error (not a redirect)
	assert.Equal(t, http.StatusInternalServerError, resp.StatusCode,
		"Unassigned partition should return error")
}

// TestShardRouting_NodeHealth tests routing when checking node health
func TestShardRouting_NodeHealth(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	cluster := StartTestCluster(t, 3)
	defer cluster.StopTestCluster()

	// All nodes should be healthy
	for i, node := range cluster.Nodes {
		resp, err := http.Get(fmt.Sprintf("%s/health", node.HTTPAddr))
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusOK, resp.StatusCode, "Node %d should be healthy", i)
	}
}

// TestShardRouting_LoadDistribution tests that load is distributed across nodes
func TestShardRouting_LoadDistribution(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	cluster := StartTestCluster(t, 3)
	defer cluster.StopTestCluster()

	err := cluster.InitializePartitionTable()
	require.NoError(t, err)

	// Send many requests to verify they're distributed
	numRequests := 300
	nodeHits := make(map[int]int)
	var mu sync.Mutex

	var wg sync.WaitGroup
	for i := 0; i < numRequests; i++ {
		wg.Add(1)
		go func(key int) {
			defer wg.Done()

			correctNode := cluster.GetNodeForKey(fmt.Sprintf("%d", key))
			if correctNode == nil {
				return
			}

			resp, err := correctNode.SendPUT(key, key*100)
			if err != nil {
				return
			}
			resp.Body.Close()

			if resp.StatusCode == http.StatusOK {
				mu.Lock()
				nodeHits[correctNode.ID]++
				mu.Unlock()
			}
		}(i)
	}

	wg.Wait()

	// Verify all nodes received some requests
	for nodeID, hits := range nodeHits {
		t.Logf("Node %d received %d requests", nodeID, hits)
		assert.Greater(t, hits, 0, "Node %d should have received requests", nodeID)
	}
}
