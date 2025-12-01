package tests

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/dominikszczepaniak/distributed-cache/pkg/raft/raftpb"
	"github.com/dominikszczepaniak/distributed-cache/pkg/sharding"
	"github.com/dominikszczepaniak/distributed-cache/pkg/worker"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// TestWorkerStandalonePutGet tests basic PUT and GET operations
func TestWorkerStandalonePutGet(t *testing.T) {
	// Create standalone worker
	store := worker.NewWorkerStore(0)
	partitionTable := sharding.NewPartitionTable()

	// Assign all partitions to this worker for standalone mode
	for i := 0; i < sharding.TOTAL_PARTITIONS; i++ {
		partitionTable.SetReplicas(sharding.PartitionID(i), 0, -1)
	}

	partitioner := sharding.NewPartitioner()
	shardManager := sharding.NewShardManager(0, partitionTable, partitioner)

	apiServer := worker.NewAPIServer(store, ":17000", shardManager)

	go apiServer.Start()
	defer apiServer.Stop(context.Background())

	time.Sleep(100 * time.Millisecond) // Wait for server to start

	// PUT request
	putReq := map[string]int{"key": 42, "value": 100}
	body, _ := json.Marshal(putReq)
	resp, err := http.Post("http://localhost:17000/kv", "application/json", bytes.NewBuffer(body))
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// GET request
	resp, err = http.Get("http://localhost:17000/kv/42")
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var getResp map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&getResp)
	assert.Equal(t, float64(100), getResp["value"])
}

// TestWorkerStandaloneDelete tests DELETE operations
func TestWorkerStandaloneDelete(t *testing.T) {
	store := worker.NewWorkerStore(0)
	partitionTable := sharding.NewPartitionTable()

	// Assign all partitions to this worker
	for i := 0; i < sharding.TOTAL_PARTITIONS; i++ {
		partitionTable.SetReplicas(sharding.PartitionID(i), 0, -1)
	}

	partitioner := sharding.NewPartitioner()
	shardManager := sharding.NewShardManager(0, partitionTable, partitioner)

	apiServer := worker.NewAPIServer(store, ":17001", shardManager)

	go apiServer.Start()
	defer apiServer.Stop(context.Background())

	time.Sleep(100 * time.Millisecond)

	// PUT
	putReq := map[string]int{"key": 42, "value": 100}
	body, _ := json.Marshal(putReq)
	http.Post("http://localhost:17001/kv", "application/json", bytes.NewBuffer(body))

	// DELETE
	req, _ := http.NewRequest(http.MethodDelete, "http://localhost:17001/kv/42", nil)
	client := &http.Client{}
	resp, err := client.Do(req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusNoContent, resp.StatusCode)

	// GET should return 404
	resp, _ = http.Get("http://localhost:17001/kv/42")
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

// TestWorkerPartitionValidation tests that worker rejects keys it doesn't own
func TestWorkerPartitionValidation(t *testing.T) {
	store := worker.NewWorkerStore(0)
	partitionTable := sharding.NewPartitionTable()
	partitioner := sharding.NewPartitioner()

	// Only assign some partitions to this worker
	// Assign partitions 0-100 to worker 0
	for i := 0; i <= 100; i++ {
		partitionTable.SetReplicas(sharding.PartitionID(i), 0, -1)
	}
	// Assign partitions 101+ to worker 1
	for i := 101; i < sharding.TOTAL_PARTITIONS; i++ {
		partitionTable.SetReplicas(sharding.PartitionID(i), 1, -1)
	}

	shardManager := sharding.NewShardManager(0, partitionTable, partitioner)
	shardManager.UpdatePeerAddress(1, "http://worker-1:7000")

	apiServer := worker.NewAPIServer(store, ":17002", shardManager)

	go apiServer.Start()
	defer apiServer.Stop(context.Background())

	time.Sleep(100 * time.Millisecond)

	// Find a key that hashes to partition > 100 (owned by worker 1)
	var wrongKey int
	for key := 1000; key < 10000; key++ {
		keyStr := fmt.Sprintf("%d", key)
		partitionID := partitioner.HashKey(keyStr)
		if partitionID > 100 {
			wrongKey = key
			break
		}
	}

	// Try to PUT a key this worker doesn't own
	putReq := map[string]int{"key": wrongKey, "value": 999}
	body, _ := json.Marshal(putReq)

	// Create HTTP client that doesn't follow redirects
	client := &http.Client{
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse // Don't follow redirects
		},
	}

	req, _ := http.NewRequest(http.MethodPost, "http://localhost:17002/kv", bytes.NewBuffer(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	require.NoError(t, err)

	// Should get redirect (307)
	assert.Equal(t, http.StatusTemporaryRedirect, resp.StatusCode)

	var respBody map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&respBody)
	assert.Equal(t, "WRONG_NODE", respBody["error"])
}

// TestWorkerReplicationHandler tests gRPC replication handler
func TestWorkerReplicationHandler(t *testing.T) {
	store := worker.NewWorkerStore(1)
	handler := worker.NewReplicationHandler(store)

	// Start gRPC server
	grpcServer := grpc.NewServer()
	raftpb.RegisterRaftServer(grpcServer, handler)

	listener, err := net.Listen("tcp", ":17100")
	require.NoError(t, err)

	go grpcServer.Serve(listener)
	defer grpcServer.Stop()

	time.Sleep(100 * time.Millisecond)

	// Create gRPC client
	conn, err := grpc.Dial("localhost:17100", grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()

	client := raftpb.NewRaftClient(conn)

	// Test PUT replication
	putReq := &raftpb.ReplicateRequest{
		Key:       42,
		Value:     100,
		Operation: "PUT",
	}

	resp, err := client.Replicate(context.Background(), putReq)
	require.NoError(t, err)
	assert.True(t, resp.Success)

	// Verify data was stored
	value, exists := store.Get(42)
	assert.True(t, exists)
	assert.Equal(t, 100, value)

	// Test DELETE replication
	delReq := &raftpb.ReplicateRequest{
		Key:       42,
		Operation: "DELETE",
	}

	resp, err = client.Replicate(context.Background(), delReq)
	require.NoError(t, err)
	assert.True(t, resp.Success)

	// Verify data was deleted
	_, exists = store.Get(42)
	assert.False(t, exists)
}

// TestWorkerConcurrentOperations tests concurrent access with race detector
func TestWorkerConcurrentOperations(t *testing.T) {
	store := worker.NewWorkerStore(0)
	partitionTable := sharding.NewPartitionTable()

	// Assign all partitions to this worker
	for i := 0; i < sharding.TOTAL_PARTITIONS; i++ {
		partitionTable.SetReplicas(sharding.PartitionID(i), 0, -1)
	}

	partitioner := sharding.NewPartitioner()
	shardManager := sharding.NewShardManager(0, partitionTable, partitioner)

	apiServer := worker.NewAPIServer(store, ":17003", shardManager)

	go apiServer.Start()
	defer apiServer.Stop(context.Background())

	time.Sleep(100 * time.Millisecond)

	// Spawn 10 concurrent goroutines doing PUT operations
	done := make(chan bool, 10)
	for i := 0; i < 10; i++ {
		go func(id int) {
			for j := 0; j < 10; j++ {
				key := id*100 + j
				value := id*1000 + j
				putReq := map[string]int{"key": key, "value": value}
				body, _ := json.Marshal(putReq)
				http.Post("http://localhost:17003/kv", "application/json", bytes.NewBuffer(body))
			}
			done <- true
		}(i)
	}

	// Wait for all goroutines to complete
	for i := 0; i < 10; i++ {
		<-done
	}

	// Verify stats
	stats := store.GetStats()
	keyCount := stats["key_count"].(int)
	assert.Equal(t, 100, keyCount) // 10 goroutines * 10 keys each
}

// TestWorkerHealthAndStats tests health and stats endpoints
func TestWorkerHealthAndStats(t *testing.T) {
	store := worker.NewWorkerStore(0)
	partitionTable := sharding.NewPartitionTable()
	for i := 0; i < sharding.TOTAL_PARTITIONS; i++ {
		partitionTable.SetReplicas(sharding.PartitionID(i), 0, -1)
	}
	partitioner := sharding.NewPartitioner()
	shardManager := sharding.NewShardManager(0, partitionTable, partitioner)

	apiServer := worker.NewAPIServer(store, ":17004", shardManager)

	go apiServer.Start()
	defer apiServer.Stop(context.Background())

	time.Sleep(100 * time.Millisecond)

	// Test health endpoint
	resp, err := http.Get("http://localhost:17004/health")
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var healthResp map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&healthResp)
	assert.Equal(t, "healthy", healthResp["status"])

	// Add some data
	putReq := map[string]int{"key": 42, "value": 100}
	body, _ := json.Marshal(putReq)
	http.Post("http://localhost:17004/kv", "application/json", bytes.NewBuffer(body))

	// Test stats endpoint
	resp, err = http.Get("http://localhost:17004/stats")
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var statsResp map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&statsResp)
	assert.Equal(t, float64(0), statsResp["worker_id"])
	assert.Equal(t, float64(1), statsResp["key_count"])
}
