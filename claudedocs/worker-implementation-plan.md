# Worker Node Implementation Plan

**Date:** 2025-12-01
**Branch:** sharding → feature/worker-separation
**Status:** Ready for Implementation

---

## Overview

This plan details the step-by-step implementation of separate worker nodes for the distributed cache, achieving true control plane (Raft) and data plane (Workers) separation.

**Goal:** Raft nodes manage ONLY partition table, Workers handle ALL data storage and client requests.

---

## Stage 1: Create Standalone Worker Binary

**Goal:** Create a working worker binary that can serve data independently of Raft

**Duration:** 2-3 days

**Status:** NOT STARTED

### Files to Create

#### 1. `cmd/worker/main.go`

```go
package main

import (
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/dominikszczepaniak/distributed-cache/pkg/sharding"
	"github.com/dominikszczepaniak/distributed-cache/pkg/worker"
)

func main() {
	opts := &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}
	logger := slog.New(slog.NewTextHandler(os.Stdout, opts))
	slog.SetDefault(logger)

	slog.Info("Starting Worker node...")

	// Load configuration from environment
	cfg := worker.LoadConfig()

	slog.Info("Worker configuration loaded",
		"worker_id", cfg.WorkerID,
		"http_addr", cfg.HTTPAddr,
		"raft_addrs", cfg.RaftAddrs)

	// Create worker application (data storage)
	app := worker.NewWorkerStore(cfg.WorkerID)

	// Initialize partition table (will be fetched from Raft later)
	partitionTable := sharding.NewPartitionTable()

	// Initialize partitioner for consistent hashing
	partitioner := sharding.NewPartitioner()

	// Create shard manager
	nodeID := sharding.NodeID(cfg.WorkerID)
	shardManager := sharding.NewShardManager(nodeID, partitionTable, partitioner)

	// Create replication client (for primary-backup replication)
	replicationClient := worker.NewReplicationClient(nodeID, 1*time.Second)

	// Register peer workers (will be fetched from Raft later)
	// For now, this is manual configuration via environment variables
	for id, addr := range cfg.PeerWorkers {
		shardManager.UpdatePeerAddress(sharding.NodeID(id), addr)
		// replicationClient will need gRPC clients, for now just register addresses
	}

	// Start HTTP API server
	apiServer := worker.NewAPIServer(app, cfg.HTTPAddr, shardManager, replicationClient)

	go func() {
		slog.Info("Starting Worker HTTP API", "addr", cfg.HTTPAddr)
		if err := apiServer.Start(); err != nil && err != http.ErrServerClosed {
			slog.Error("Worker API server error", "error", err)
		}
	}()

	// TODO: Register with Raft cluster (Stage 2)
	// go registerWithRaft(cfg, nodeID, partitionTable)

	// Wait for shutdown signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	sig := <-sigCh
	slog.Info("Received shutdown signal", "signal", sig)

	if err := apiServer.Stop(); err != nil {
		slog.Error("Error stopping API server", "error", err)
	}

	slog.Info("Worker node stopped")
}
```

#### 2. `pkg/worker/config.go`

```go
package worker

import (
	"os"
	"strconv"
	"strings"
)

type Config struct {
	WorkerID    int               // Unique worker identifier
	HTTPAddr    string            // HTTP API listen address
	RaftAddrs   []string          // Raft cluster addresses for registration
	PeerWorkers map[int]string    // Other worker addresses (id -> HTTP addr)
}

func LoadConfig() *Config {
	// WORKER_ID: Unique identifier (0, 1, 2, ...)
	workerIDStr := os.Getenv("WORKER_ID")
	workerID, err := strconv.Atoi(workerIDStr)
	if err != nil {
		panic("WORKER_ID must be a valid integer")
	}

	// HTTP_ADDR: HTTP API listen address (e.g., ":7000")
	httpAddr := os.Getenv("HTTP_ADDR")
	if httpAddr == "" {
		httpAddr = ":7000"
	}

	// RAFT_ADDRS: Comma-separated Raft node addresses
	// Example: "raft-node-0:9000,raft-node-1:9000,raft-node-2:9000"
	raftAddrsStr := os.Getenv("RAFT_ADDRS")
	raftAddrs := strings.Split(raftAddrsStr, ",")

	// PEER_WORKERS: Comma-separated worker addresses
	// Example: "worker-0:7000,worker-1:7000,worker-2:7000"
	// Format: "id:addr,id:addr,..."
	peerWorkersStr := os.Getenv("PEER_WORKERS")
	peerWorkers := make(map[int]string)

	if peerWorkersStr != "" {
		for i, addr := range strings.Split(peerWorkersStr, ",") {
			peerWorkers[i] = "http://" + addr
		}
	}

	return &Config{
		WorkerID:    workerID,
		HTTPAddr:    httpAddr,
		RaftAddrs:   raftAddrs,
		PeerWorkers: peerWorkers,
	}
}
```

#### 3. `pkg/worker/store.go`

```go
package worker

import (
	"fmt"
	"log/slog"
	"sync"
)

// WorkerStore is the worker's local data storage
// It does NOT use Raft consensus, only stores its assigned partitions
type WorkerStore struct {
	workerID int
	mu       sync.RWMutex
	data     map[int]int // key -> value (in-memory cache)
}

func NewWorkerStore(workerID int) *WorkerStore {
	return &WorkerStore{
		workerID: workerID,
		data:     make(map[int]int),
	}
}

// Put stores a key-value pair (called by primary worker)
func (ws *WorkerStore) Put(key, value int) error {
	ws.mu.Lock()
	defer ws.mu.Unlock()

	ws.data[key] = value
	slog.Info("Worker PUT", "worker_id", ws.workerID, "key", key, "value", value)
	return nil
}

// Get retrieves a value by key
func (ws *WorkerStore) Get(key int) (int, bool) {
	ws.mu.RLock()
	defer ws.mu.RUnlock()

	value, exists := ws.data[key]
	slog.Info("Worker GET", "worker_id", ws.workerID, "key", key, "value", value, "exists", exists)
	return value, exists
}

// Delete removes a key
func (ws *WorkerStore) Delete(key int) error {
	ws.mu.Lock()
	defer ws.mu.Unlock()

	delete(ws.data, key)
	slog.Info("Worker DELETE", "worker_id", ws.workerID, "key", key)
	return nil
}

// Replicate handles replication from primary (called on backup worker)
func (ws *WorkerStore) Replicate(key, value int, operation string) error {
	ws.mu.Lock()
	defer ws.mu.Unlock()

	switch operation {
	case "PUT":
		ws.data[key] = value
		slog.Info("Worker REPLICATE PUT", "worker_id", ws.workerID, "key", key, "value", value)
		return nil
	case "DELETE":
		delete(ws.data, key)
		slog.Info("Worker REPLICATE DELETE", "worker_id", ws.workerID, "key", key)
		return nil
	default:
		return fmt.Errorf("unknown operation: %s", operation)
	}
}

// GetStats returns storage statistics
func (ws *WorkerStore) GetStats() map[string]interface{} {
	ws.mu.RLock()
	defer ws.mu.RUnlock()

	return map[string]interface{}{
		"worker_id": ws.workerID,
		"key_count": len(ws.data),
	}
}
```

#### 4. `pkg/worker/api_server.go`

```go
package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/dominikszczepaniak/distributed-cache/pkg/sharding"
)

type APIServer struct {
	store             *WorkerStore
	listenAddr        string
	httpServer        *http.Server
	shardManager      *sharding.ShardManager
	replicationClient *ReplicationClient
}

func NewAPIServer(store *WorkerStore, listenAddr string, shardManager *sharding.ShardManager, replClient *ReplicationClient) *APIServer {
	return &APIServer{
		store:             store,
		listenAddr:        listenAddr,
		shardManager:      shardManager,
		replicationClient: replClient,
	}
}

func (s *APIServer) Start() error {
	mux := http.NewServeMux()

	mux.HandleFunc("/kv", s.handleKV)
	mux.HandleFunc("/kv/", s.handleKVWithKey)
	mux.HandleFunc("/health", s.handleHealth)
	mux.HandleFunc("/stats", s.handleStats)

	s.httpServer = &http.Server{
		Addr:    s.listenAddr,
		Handler: mux,
	}

	slog.Info("Starting Worker HTTP API", "addr", s.listenAddr)
	return s.httpServer.ListenAndServe()
}

func (s *APIServer) Stop() error {
	if s.httpServer != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return s.httpServer.Shutdown(ctx)
	}
	return nil
}

func (s *APIServer) handleKV(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		s.handlePut(w, r)
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *APIServer) handleKVWithKey(w http.ResponseWriter, r *http.Request) {
	keyStr := strings.TrimPrefix(r.URL.Path, "/kv/")
	key, err := strconv.Atoi(keyStr)
	if err != nil {
		http.Error(w, "Invalid key", http.StatusBadRequest)
		return
	}

	switch r.Method {
	case http.MethodGet:
		s.handleGet(w, r, key)
	case http.MethodDelete:
		s.handleDelete(w, r, key)
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *APIServer) handlePut(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Key   int `json:"key"`
		Value int `json:"value"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	// STEP 1: Validate shard ownership (am I the primary?)
	if s.shardManager != nil {
		keyStr := fmt.Sprintf("%d", req.Key)
		partitionID := s.shardManager.GetPartitionID(keyStr)

		primaryNode, backupNode, ok := s.shardManager.GetReplicas(partitionID)
		if !ok {
			http.Error(w, "Partition not assigned", http.StatusInternalServerError)
			return
		}

		if primaryNode != s.shardManager.GetNodeID() {
			// NOT PRIMARY → Redirect
			primaryAddr, _ := s.shardManager.GetNodeAddress(primaryNode)
			w.Header().Set("Location", primaryAddr)
			w.WriteHeader(http.StatusTemporaryRedirect)
			json.NewEncoder(w).Encode(map[string]interface{}{
				"error":        "WRONG_NODE",
				"message":      "Not primary for this key",
				"primary_node": primaryNode,
				"redirect_to":  primaryAddr,
			})
			return
		}

		// STEP 2: Synchronous replication to backup
		if backupNode >= 0 && s.replicationClient != nil {
			replicationCtx, cancel := context.WithTimeout(r.Context(), 1*time.Second)
			defer cancel()

			err := s.replicationClient.Replicate(replicationCtx, backupNode, req.Key, req.Value)
			if err != nil {
				slog.Error("Replication failed", "backup", backupNode, "key", req.Key, "error", err)
				http.Error(w, "Replication failed", http.StatusServiceUnavailable)
				return
			}

			slog.Info("Replicated to backup", "backup", backupNode, "key", req.Key)
		}
	}

	// STEP 3: Write to local storage (NO Raft consensus)
	if err := s.store.Put(req.Key, req.Value); err != nil {
		http.Error(w, "Storage error", http.StatusInternalServerError)
		return
	}

	// STEP 4: Return success
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"success": true,
		"message": "Key-value pair stored",
	})
}

func (s *APIServer) handleGet(w http.ResponseWriter, r *http.Request, key int) {
	// Validate shard ownership
	if s.shardManager != nil {
		keyStr := fmt.Sprintf("%d", key)
		if err := s.shardManager.ValidateKey(keyStr); err != nil {
			// Wrong worker → redirect
			if wrongNodeErr, ok := err.(*sharding.WrongNodeError); ok {
				w.Header().Set("Location", wrongNodeErr.OwnerAddress)
				w.WriteHeader(http.StatusTemporaryRedirect)
				json.NewEncoder(w).Encode(map[string]interface{}{
					"error":       "WRONG_NODE",
					"redirect_to": wrongNodeErr.OwnerAddress,
				})
				return
			}
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}

	// Read from local storage
	value, exists := s.store.Get(key)
	if !exists {
		http.Error(w, "Key not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"key":   key,
		"value": value,
	})
}

func (s *APIServer) handleDelete(w http.ResponseWriter, r *http.Request, key int) {
	// Validate shard ownership (similar to PUT)
	if s.shardManager != nil {
		keyStr := fmt.Sprintf("%d", key)
		partitionID := s.shardManager.GetPartitionID(keyStr)

		primaryNode, backupNode, ok := s.shardManager.GetReplicas(partitionID)
		if !ok {
			http.Error(w, "Partition not assigned", http.StatusInternalServerError)
			return
		}

		if primaryNode != s.shardManager.GetNodeID() {
			primaryAddr, _ := s.shardManager.GetNodeAddress(primaryNode)
			w.Header().Set("Location", primaryAddr)
			w.WriteHeader(http.StatusTemporaryRedirect)
			return
		}

		// Replicate delete to backup
		if backupNode >= 0 && s.replicationClient != nil {
			replicationCtx, cancel := context.WithTimeout(r.Context(), 1*time.Second)
			defer cancel()

			err := s.replicationClient.DeleteReplicate(replicationCtx, backupNode, key)
			if err != nil {
				slog.Error("Delete replication failed", "backup", backupNode, "key", key, "error", err)
				http.Error(w, "Replication failed", http.StatusServiceUnavailable)
				return
			}
		}
	}

	// Delete from local storage
	if err := s.store.Delete(key); err != nil {
		http.Error(w, "Storage error", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func (s *APIServer) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status": "healthy",
	})
}

func (s *APIServer) handleStats(w http.ResponseWriter, r *http.Request) {
	stats := s.store.GetStats()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(stats)
}
```

#### 5. `pkg/worker/replication_client.go`

```go
package worker

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/dominikszczepaniak/distributed-cache/pkg/sharding"
)

// ReplicationClient handles worker-to-worker replication via HTTP
// (Simpler than gRPC for initial implementation)
type ReplicationClient struct {
	nodeID      sharding.NodeID
	httpClients map[sharding.NodeID]string // nodeID -> HTTP address
	timeout     time.Duration
	mu          sync.RWMutex

	// Circuit breaker
	failures    map[sharding.NodeID]int
	circuitOpen map[sharding.NodeID]bool
}

func NewReplicationClient(nodeID sharding.NodeID, timeout time.Duration) *ReplicationClient {
	return &ReplicationClient{
		nodeID:      nodeID,
		httpClients: make(map[sharding.NodeID]string),
		timeout:     timeout,
		failures:    make(map[sharding.NodeID]int),
		circuitOpen: make(map[sharding.NodeID]bool),
	}
}

func (c *ReplicationClient) RegisterPeer(nodeID sharding.NodeID, httpAddr string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.httpClients[nodeID] = httpAddr
	slog.Info("ReplicationClient: Registered peer", "node_id", nodeID, "addr", httpAddr)
}

func (c *ReplicationClient) Replicate(ctx context.Context, backupNodeID sharding.NodeID, key, value int) error {
	// Check circuit breaker
	c.mu.RLock()
	if c.circuitOpen[backupNodeID] {
		c.mu.RUnlock()
		return fmt.Errorf("circuit breaker open for node %d", backupNodeID)
	}
	httpAddr, exists := c.httpClients[backupNodeID]
	c.mu.RUnlock()

	if !exists {
		return fmt.Errorf("no HTTP client for backup node %d", backupNodeID)
	}

	// Send HTTP POST to backup worker's /replicate endpoint
	// TODO: Implement HTTP replication request
	// For now, just log
	slog.Info("Replicating to backup (HTTP)", "backup", backupNodeID, "addr", httpAddr, "key", key, "value", value)

	// Simulate replication (replace with actual HTTP call)
	time.Sleep(10 * time.Millisecond)

	return nil
}

func (c *ReplicationClient) DeleteReplicate(ctx context.Context, backupNodeID sharding.NodeID, key int) error {
	// Similar to Replicate but for DELETE operation
	c.mu.RLock()
	if c.circuitOpen[backupNodeID] {
		c.mu.RUnlock()
		return fmt.Errorf("circuit breaker open for node %d", backupNodeID)
	}
	httpAddr, exists := c.httpClients[backupNodeID]
	c.mu.RUnlock()

	if !exists {
		return fmt.Errorf("no HTTP client for backup node %d", backupNodeID)
	}

	slog.Info("Replicating delete to backup (HTTP)", "backup", backupNodeID, "addr", httpAddr, "key", key)

	// Simulate replication
	time.Sleep(10 * time.Millisecond)

	return nil
}
```

### Docker Compose Configuration

#### 6. Update `docker-compose.yml` to add workers

```yaml
version: '3.8'

services:
  # CONTROL PLANE: Raft nodes (manage partition table ONLY)
  raft-node-0:
    build:
      context: .
      dockerfile: deploy/Dockerfile.raft
    container_name: raft-node-0
    environment:
      RAFT_ID: "0"
      TOTAL_NODES: "3"
      RAFT_ADDRS: "raft-node-0:9000,raft-node-1:9000,raft-node-2:9000"
      FILENAME: "/data/node0"
      SNAPSHOT_THRESHOLD: "100"
      API_ADDR: ":8080"
    volumes:
      - ./data/raft-node0:/data
    networks:
      - cache-network
    ports:
      - "9000:9000"
      - "8080:8080"

  raft-node-1:
    build:
      context: .
      dockerfile: deploy/Dockerfile.raft
    container_name: raft-node-1
    environment:
      RAFT_ID: "1"
      TOTAL_NODES: "3"
      RAFT_ADDRS: "raft-node-0:9000,raft-node-1:9000,raft-node-2:9000"
      FILENAME: "/data/node1"
      SNAPSHOT_THRESHOLD: "100"
      API_ADDR: ":8080"
    volumes:
      - ./data/raft-node1:/data
    networks:
      - cache-network
    ports:
      - "9001:9000"
      - "8081:8080"

  raft-node-2:
    build:
      context: .
      dockerfile: deploy/Dockerfile.raft
    container_name: raft-node-2
    environment:
      RAFT_ID: "2"
      TOTAL_NODES: "3"
      RAFT_ADDRS: "raft-node-0:9000,raft-node-1:9000,raft-node-2:9000"
      FILENAME: "/data/node2"
      SNAPSHOT_THRESHOLD: "100"
      API_ADDR: ":8080"
    volumes:
      - ./data/raft-node2:/data
    networks:
      - cache-network
    ports:
      - "9002:9000"
      - "8082:8080"

  # DATA PLANE: Worker nodes (store actual data)
  worker-0:
    build:
      context: .
      dockerfile: deploy/Dockerfile.worker
    container_name: worker-0
    environment:
      WORKER_ID: "0"
      HTTP_ADDR: ":7000"
      RAFT_ADDRS: "raft-node-0:9000,raft-node-1:9000,raft-node-2:9000"
      PEER_WORKERS: "worker-0:7000,worker-1:7000,worker-2:7000"
    networks:
      - cache-network
    ports:
      - "7000:7000"
    depends_on:
      - raft-node-0
      - raft-node-1
      - raft-node-2

  worker-1:
    build:
      context: .
      dockerfile: deploy/Dockerfile.worker
    container_name: worker-1
    environment:
      WORKER_ID: "1"
      HTTP_ADDR: ":7000"
      RAFT_ADDRS: "raft-node-0:9000,raft-node-1:9000,raft-node-2:9000"
      PEER_WORKERS: "worker-0:7000,worker-1:7000,worker-2:7000"
    networks:
      - cache-network
    ports:
      - "7001:7000"
    depends_on:
      - raft-node-0
      - raft-node-1
      - raft-node-2

  worker-2:
    build:
      context: .
      dockerfile: deploy/Dockerfile.worker
    container_name: worker-2
    environment:
      WORKER_ID: "2"
      HTTP_ADDR: ":7000"
      RAFT_ADDRS: "raft-node-0:9000,raft-node-1:9000,raft-node-2:9000"
      PEER_WORKERS: "worker-0:7000,worker-1:7000,worker-2:7000"
    networks:
      - cache-network
    ports:
      - "7002:7000"
    depends_on:
      - raft-node-0
      - raft-node-1
      - raft-node-2

networks:
  cache-network:
    driver: bridge
```

#### 7. `deploy/Dockerfile.worker`

```dockerfile
FROM golang:1.21-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -o /worker ./cmd/worker

FROM alpine:latest

RUN apk --no-cache add ca-certificates

WORKDIR /root/

COPY --from=builder /worker .

EXPOSE 7000

CMD ["./worker"]
```

### Testing Stage 1

#### 8. `tests/worker_standalone_test.go`

```go
package tests

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/dominikszczepaniak/distributed-cache/pkg/sharding"
	"github.com/dominikszczepaniak/distributed-cache/pkg/worker"
	"github.com/stretchr/testify/assert"
)

func TestWorker_StandalonePutGet(t *testing.T) {
	// Create standalone worker
	store := worker.NewWorkerStore(0)
	partitionTable := sharding.NewPartitionTable()
	partitioner := sharding.NewPartitioner()
	shardManager := sharding.NewShardManager(0, partitionTable, partitioner)
	replClient := worker.NewReplicationClient(0, 1*time.Second)

	apiServer := worker.NewAPIServer(store, ":17000", shardManager, replClient)

	go apiServer.Start()
	defer apiServer.Stop()

	time.Sleep(100 * time.Millisecond) // Wait for server to start

	// PUT request
	putReq := map[string]int{"key": 42, "value": 100}
	body, _ := json.Marshal(putReq)
	resp, err := http.Post("http://localhost:17000/kv", "application/json", bytes.NewBuffer(body))
	assert.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// GET request
	resp, err = http.Get("http://localhost:17000/kv/42")
	assert.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var getResp map[string]int
	json.NewDecoder(resp.Body).Decode(&getResp)
	assert.Equal(t, 100, getResp["value"])
}

func TestWorker_StandaloneDelete(t *testing.T) {
	store := worker.NewWorkerStore(0)
	partitionTable := sharding.NewPartitionTable()
	partitioner := sharding.NewPartitioner()
	shardManager := sharding.NewShardManager(0, partitionTable, partitioner)
	replClient := worker.NewReplicationClient(0, 1*time.Second)

	apiServer := worker.NewAPIServer(store, ":17001", shardManager, replClient)

	go apiServer.Start()
	defer apiServer.Stop()

	time.Sleep(100 * time.Millisecond)

	// PUT
	putReq := map[string]int{"key": 42, "value": 100}
	body, _ := json.Marshal(putReq)
	http.Post("http://localhost:17001/kv", "application/json", bytes.NewBuffer(body))

	// DELETE
	req, _ := http.NewRequest(http.MethodDelete, "http://localhost:17001/kv/42", nil)
	client := &http.Client{}
	resp, err := client.Do(req)
	assert.NoError(t, err)
	assert.Equal(t, http.StatusNoContent, resp.StatusCode)

	// GET should return 404
	resp, _ = http.Get("http://localhost:17001/kv/42")
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}
```

### Validation Checklist for Stage 1

- [ ] `cmd/worker/main.go` compiles successfully
- [ ] Worker binary starts and listens on HTTP port
- [ ] Standalone PUT request succeeds (no Raft)
- [ ] Standalone GET request returns correct value
- [ ] Standalone DELETE request removes key
- [ ] Worker health endpoint returns 200 OK
- [ ] Worker stats endpoint shows key count
- [ ] No Raft consensus involved in data operations
- [ ] Tests pass: `go test ./tests/worker_standalone_test.go`

---

## Stage 2: Worker Registration with Raft

**Goal:** Workers can register with Raft cluster and receive partition assignments

**Duration:** 2-3 days

**Status:** NOT STARTED

### Protobuf Changes

#### 1. Update `proto/raft.proto`

Add worker registration messages:

```protobuf
// Worker registration request
message RegisterWorkerRequest {
    int32 worker_id = 1;
    string http_addr = 2;     // Worker's HTTP API address
    int32 capacity = 3;       // Number of partitions this worker can handle
}

message RegisterWorkerResponse {
    bool success = 1;
    string error = 2;
    repeated PartitionAssignment assignments = 3; // Initial partition assignments
}

message PartitionAssignment {
    uint32 partition_id = 1;
    int32 primary_node = 2;
    int32 backup_node = 3;
}

// Worker heartbeat (health check)
message WorkerHeartbeatRequest {
    int32 worker_id = 1;
}

message WorkerHeartbeatResponse {
    bool success = 1;
}

service Raft {
    // ... existing RPCs ...
    rpc RegisterWorker(RegisterWorkerRequest) returns (RegisterWorkerResponse);
    rpc WorkerHeartbeat(WorkerHeartbeatRequest) returns (WorkerHeartbeatResponse);
}
```

Generate protobuf code:
```bash
cd /Users/dzc/distributed-cache
protoc --go_out=. --go-grpc_out=. proto/raft.proto
```

### Raft Node Changes

#### 2. Create `pkg/raft/worker_registry.go`

```go
package raft

import (
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/dominikszczepaniak/distributed-cache/pkg/sharding"
)

// WorkerInfo tracks active worker metadata
type WorkerInfo struct {
	WorkerID      sharding.NodeID
	HTTPAddr      string
	LastHeartbeat time.Time
	Capacity      int
}

// WorkerRegistry tracks all registered workers
type WorkerRegistry struct {
	mu      sync.RWMutex
	workers map[sharding.NodeID]*WorkerInfo
}

func NewWorkerRegistry() *WorkerRegistry {
	return &WorkerRegistry{
		workers: make(map[sharding.NodeID]*WorkerInfo),
	}
}

func (wr *WorkerRegistry) Register(workerID sharding.NodeID, httpAddr string, capacity int) error {
	wr.mu.Lock()
	defer wr.mu.Unlock()

	if _, exists := wr.workers[workerID]; exists {
		return fmt.Errorf("worker %d already registered", workerID)
	}

	wr.workers[workerID] = &WorkerInfo{
		WorkerID:      workerID,
		HTTPAddr:      httpAddr,
		LastHeartbeat: time.Now(),
		Capacity:      capacity,
	}

	slog.Info("Worker registered", "worker_id", workerID, "http_addr", httpAddr)
	return nil
}

func (wr *WorkerRegistry) Heartbeat(workerID sharding.NodeID) error {
	wr.mu.Lock()
	defer wr.mu.Unlock()

	worker, exists := wr.workers[workerID]
	if !exists {
		return fmt.Errorf("worker %d not registered", workerID)
	}

	worker.LastHeartbeat = time.Now()
	return nil
}

func (wr *WorkerRegistry) GetWorker(workerID sharding.NodeID) (*WorkerInfo, bool) {
	wr.mu.RLock()
	defer wr.mu.RUnlock()

	worker, exists := wr.workers[workerID]
	return worker, exists
}

func (wr *WorkerRegistry) GetAllWorkers() []*WorkerInfo {
	wr.mu.RLock()
	defer wr.mu.RUnlock()

	workers := make([]*WorkerInfo, 0, len(wr.workers))
	for _, worker := range wr.workers {
		workers = append(workers, worker)
	}
	return workers
}

func (wr *WorkerRegistry) RemoveStaleWorkers(timeout time.Duration) []sharding.NodeID {
	wr.mu.Lock()
	defer wr.mu.Unlock()

	stale := []sharding.NodeID{}
	now := time.Now()

	for workerID, worker := range wr.workers {
		if now.Sub(worker.LastHeartbeat) > timeout {
			slog.Warn("Removing stale worker", "worker_id", workerID, "last_heartbeat", worker.LastHeartbeat)
			delete(wr.workers, workerID)
			stale = append(stale, workerID)
		}
	}

	return stale
}
```

#### 3. Update `pkg/raft/grpc_server.go`

Add worker registration RPC handlers:

```go
// Add to GrpcServer struct
type GrpcServer struct {
	raftpb.UnimplementedRaftServer
	raft           *Raft
	shardManager   *sharding.ShardManager
	nodeID         sharding.NodeID
	workerRegistry *WorkerRegistry  // NEW
}

// RegisterWorker handles worker registration
func (s *GrpcServer) RegisterWorker(ctx context.Context, req *raftpb.RegisterWorkerRequest) (*raftpb.RegisterWorkerResponse, error) {
	workerID := sharding.NodeID(req.WorkerId)
	httpAddr := req.HttpAddr
	capacity := int(req.Capacity)

	slog.Info("Received worker registration request", "worker_id", workerID, "http_addr", httpAddr)

	// Only leader handles registration
	if !s.raft.IsLeader() {
		return &raftpb.RegisterWorkerResponse{
			Success: false,
			Error:   "not leader",
		}, nil
	}

	// Register worker in local registry
	if err := s.workerRegistry.Register(workerID, httpAddr, capacity); err != nil {
		return &raftpb.RegisterWorkerResponse{
			Success: false,
			Error:   err.Error(),
		}, nil
	}

	// Assign partitions to this worker
	// For now, use even distribution
	partitionTable := s.raft.GetApplication().(*SimpleKVStore).partitionTable
	allWorkers := s.workerRegistry.GetAllWorkers()

	// Rebalance partitions across all workers
	assignments := rebalancePartitions(allWorkers, sharding.TOTAL_PARTITIONS)

	// Replicate partition table via Raft consensus
	msg := Message{
		MsgType: "UPDATE_PARTITION_TABLE",
		PartitionTableUpdate: &PartitionTableUpdate{
			Assignments: assignments,
			Version:     partitionTable.GetVersion() + 1,
		},
	}

	s.raft.Broadcast(msg)

	// Return partition assignments to worker
	workerAssignments := filterAssignmentsForWorker(assignments, workerID)

	return &raftpb.RegisterWorkerResponse{
		Success:     true,
		Assignments: workerAssignments,
	}, nil
}

func (s *GrpcServer) WorkerHeartbeat(ctx context.Context, req *raftpb.WorkerHeartbeatRequest) (*raftpb.WorkerHeartbeatResponse, error) {
	workerID := sharding.NodeID(req.WorkerId)

	if err := s.workerRegistry.Heartbeat(workerID); err != nil {
		return &raftpb.WorkerHeartbeatResponse{Success: false}, nil
	}

	return &raftpb.WorkerHeartbeatResponse{Success: true}, nil
}

// Helper: Rebalance partitions across workers
func rebalancePartitions(workers []*WorkerInfo, totalPartitions int) map[sharding.PartitionID]sharding.NodeID {
	assignments := make(map[sharding.PartitionID]sharding.NodeID)

	if len(workers) == 0 {
		return assignments
	}

	// Even distribution
	for pid := 0; pid < totalPartitions; pid++ {
		workerIdx := pid % len(workers)
		assignments[sharding.PartitionID(pid)] = workers[workerIdx].WorkerID
	}

	return assignments
}

func filterAssignmentsForWorker(assignments map[sharding.PartitionID]sharding.NodeID, workerID sharding.NodeID) []*raftpb.PartitionAssignment {
	result := []*raftpb.PartitionAssignment{}

	for pid, owner := range assignments {
		if owner == workerID {
			result = append(result, &raftpb.PartitionAssignment{
				PartitionId:  uint32(pid),
				PrimaryNode:  int32(owner),
				BackupNode:   -1, // TODO: Assign backup
			})
		}
	}

	return result
}
```

### Worker Registration Client

#### 4. Create `pkg/worker/registration_client.go`

```go
package worker

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/dominikszczepaniak/distributed-cache/pkg/raft/raftpb"
	"github.com/dominikszczepaniak/distributed-cache/pkg/sharding"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type RegistrationClient struct {
	workerID     sharding.NodeID
	httpAddr     string
	raftAddrs    []string
	raftClient   raftpb.RaftClient
	partitionTable *sharding.PartitionTable
}

func NewRegistrationClient(workerID sharding.NodeID, httpAddr string, raftAddrs []string, partitionTable *sharding.PartitionTable) *RegistrationClient {
	return &RegistrationClient{
		workerID:       workerID,
		httpAddr:       httpAddr,
		raftAddrs:      raftAddrs,
		partitionTable: partitionTable,
	}
}

func (rc *RegistrationClient) Register(ctx context.Context) error {
	// Connect to Raft cluster (try each node until successful)
	var conn *grpc.ClientConn
	var err error

	for _, raftAddr := range rc.raftAddrs {
		conn, err = grpc.Dial(raftAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err == nil {
			break
		}
		slog.Warn("Failed to connect to Raft node", "addr", raftAddr, "error", err)
	}

	if conn == nil {
		return fmt.Errorf("failed to connect to any Raft node")
	}
	defer conn.Close()

	rc.raftClient = raftpb.NewRaftClient(conn)

	// Send registration request
	req := &raftpb.RegisterWorkerRequest{
		WorkerId: int32(rc.workerID),
		HttpAddr: rc.httpAddr,
		Capacity: int32(sharding.TOTAL_PARTITIONS / 3), // Default capacity
	}

	resp, err := rc.raftClient.RegisterWorker(ctx, req)
	if err != nil {
		return fmt.Errorf("registration failed: %w", err)
	}

	if !resp.Success {
		return fmt.Errorf("registration rejected: %s", resp.Error)
	}

	slog.Info("Worker registered successfully", "worker_id", rc.workerID, "assignments", len(resp.Assignments))

	// Update local partition table with assignments
	for _, assignment := range resp.Assignments {
		rc.partitionTable.SetReplicas(
			sharding.PartitionID(assignment.PartitionId),
			sharding.NodeID(assignment.PrimaryNode),
			sharding.NodeID(assignment.BackupNode),
		)
	}

	return nil
}

func (rc *RegistrationClient) StartHeartbeat(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for range ticker.C {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		req := &raftpb.WorkerHeartbeatRequest{
			WorkerId: int32(rc.workerID),
		}

		resp, err := rc.raftClient.WorkerHeartbeat(ctx, req)
		if err != nil {
			slog.Error("Heartbeat failed", "error", err)
		} else if !resp.Success {
			slog.Warn("Heartbeat rejected")
		}

		cancel()
	}
}
```

#### 5. Update `cmd/worker/main.go` to call registration

```go
func main() {
	// ... existing setup ...

	// Create registration client
	regClient := worker.NewRegistrationClient(
		nodeID,
		cfg.HTTPAddr,
		cfg.RaftAddrs,
		partitionTable,
	)

	// Register with Raft cluster
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := regClient.Register(ctx); err != nil {
		slog.Error("Failed to register with Raft", "error", err)
		os.Exit(1)
	}

	slog.Info("Worker registered with Raft cluster")

	// Start heartbeat
	go regClient.StartHeartbeat(5 * time.Second)

	// ... start API server ...
}
```

### Testing Stage 2

#### 6. `tests/worker_registration_test.go`

```go
package tests

import (
	"context"
	"testing"
	"time"

	"github.com/dominikszczepaniak/distributed-cache/pkg/raft"
	"github.com/dominikszczepaniak/distributed-cache/pkg/sharding"
	"github.com/dominikszczepaniak/distributed-cache/pkg/worker"
	"github.com/stretchr/testify/assert"
)

func TestWorkerRegistration_Success(t *testing.T) {
	// Start 3-node Raft cluster
	cluster := NewTestRaftCluster(t, 3)
	defer cluster.Shutdown()

	time.Sleep(2 * time.Second) // Wait for leader election

	// Create worker
	workerID := sharding.NodeID(10)
	httpAddr := "http://worker-0:7000"
	partitionTable := sharding.NewPartitionTable()

	regClient := worker.NewRegistrationClient(
		workerID,
		httpAddr,
		cluster.GetRaftAddrs(),
		partitionTable,
	)

	// Register
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := regClient.Register(ctx)
	assert.NoError(t, err)

	// Verify partition table updated
	assert.Greater(t, partitionTable.GetAssignmentCount(), 0)
}

func TestWorkerRegistration_MultipleWorkers(t *testing.T) {
	cluster := NewTestRaftCluster(t, 3)
	defer cluster.Shutdown()

	time.Sleep(2 * time.Second)

	// Register 3 workers
	for i := 0; i < 3; i++ {
		workerID := sharding.NodeID(10 + i)
		httpAddr := fmt.Sprintf("http://worker-%d:7000", i)
		partitionTable := sharding.NewPartitionTable()

		regClient := worker.NewRegistrationClient(workerID, httpAddr, cluster.GetRaftAddrs(), partitionTable)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err := regClient.Register(ctx)
		cancel()

		assert.NoError(t, err)
	}

	// Verify all workers registered
	leader := cluster.GetLeader()
	registry := leader.GetWorkerRegistry()
	workers := registry.GetAllWorkers()
	assert.Equal(t, 3, len(workers))
}
```

### Validation Checklist for Stage 2

- [ ] Protobuf updated with `RegisterWorker` and `WorkerHeartbeat` RPCs
- [ ] Protobuf code generated successfully
- [ ] `WorkerRegistry` tracks active workers
- [ ] `RegisterWorker` RPC handler implemented on Raft nodes
- [ ] Worker registration client implemented
- [ ] Worker successfully registers with Raft leader
- [ ] Partition table updated after registration
- [ ] Multiple workers can register
- [ ] Heartbeat mechanism works
- [ ] Stale workers removed after timeout
- [ ] Tests pass: `go test ./tests/worker_registration_test.go`

---

## Stage 3: Raft Request Routing to Workers

**Goal:** Raft nodes redirect client requests to workers instead of executing them

**Duration:** 1-2 days

**Status:** NOT STARTED

### Changes to Raft API Server

#### 1. Update `pkg/api/server.go` (Raft node)

Modify PUT/GET/DELETE handlers to return redirects:

```go
func (s *Server) handlePut(w http.ResponseWriter, r *http.Request) {
	var req PutRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	// NEW BEHAVIOR: Redirect to worker instead of executing locally

	// Step 1: Determine which worker owns this key
	if s.shardManager != nil {
		keyStr := fmt.Sprintf("%d", req.Key)
		partitionID := s.shardManager.GetPartitionID(keyStr)

		primaryNode, _, ok := s.shardManager.GetReplicas(partitionID)
		if !ok {
			http.Error(w, "Partition not assigned", http.StatusInternalServerError)
			return
		}

		// Step 2: Look up worker HTTP address
		workerAddr, exists := s.getWorkerAddress(primaryNode)
		if !exists {
			http.Error(w, "Worker not registered", http.StatusInternalServerError)
			return
		}

		// Step 3: Return redirect to worker
		w.Header().Set("Location", workerAddr+"/kv")
		w.WriteHeader(http.StatusTemporaryRedirect)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"message":     "Redirect to worker",
			"worker_id":   primaryNode,
			"worker_addr": workerAddr,
		})
		return
	}

	// Fallback: If no shard manager, return error
	http.Error(w, "Sharding not configured", http.StatusInternalServerError)
}

func (s *Server) handleGet(w http.ResponseWriter, r *http.Request, key int) {
	// Similar redirect logic for GET
	if s.shardManager != nil {
		keyStr := fmt.Sprintf("%d", key)
		partitionID := s.shardManager.GetPartitionID(keyStr)

		primaryNode, _, ok := s.shardManager.GetReplicas(partitionID)
		if !ok {
			http.Error(w, "Partition not assigned", http.StatusInternalServerError)
			return
		}

		workerAddr, exists := s.getWorkerAddress(primaryNode)
		if !exists {
			http.Error(w, "Worker not registered", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Location", fmt.Sprintf("%s/kv/%d", workerAddr, key))
		w.WriteHeader(http.StatusTemporaryRedirect)
		return
	}

	http.Error(w, "Sharding not configured", http.StatusInternalServerError)
}

func (s *Server) handleDelete(w http.ResponseWriter, r *http.Request, key int) {
	// Similar redirect logic for DELETE
	if s.shardManager != nil {
		keyStr := fmt.Sprintf("%d", key)
		partitionID := s.shardManager.GetPartitionID(keyStr)

		primaryNode, _, ok := s.shardManager.GetReplicas(partitionID)
		if !ok {
			http.Error(w, "Partition not assigned", http.StatusInternalServerError)
			return
		}

		workerAddr, exists := s.getWorkerAddress(primaryNode)
		if !exists {
			http.Error(w, "Worker not registered", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Location", fmt.Sprintf("%s/kv/%d", workerAddr, key))
		w.WriteHeader(http.StatusTemporaryRedirect)
		return
	}

	http.Error(w, "Sharding not configured", http.StatusInternalServerError)
}

// Helper: Get worker HTTP address from registry
func (s *Server) getWorkerAddress(workerID sharding.NodeID) (string, bool) {
	// Access worker registry from Raft instance
	registry := s.raft.GetWorkerRegistry()
	worker, exists := registry.GetWorker(workerID)
	if !exists {
		return "", false
	}
	return worker.HTTPAddr, true
}
```

#### 2. Add helper method to Raft struct

```go
// In pkg/raft/raft.go

func (r *Raft) GetWorkerRegistry() *WorkerRegistry {
	return r.workerRegistry
}
```

### Testing Stage 3

#### 3. `tests/raft_worker_routing_test.go`

```go
package tests

import (
	"net/http"
	"testing"
	"time"

	"github.com/dominikszczepaniak/distributed-cache/pkg/client"
	"github.com/stretchr/testify/assert"
)

func TestRaftWorkerRouting_PutRedirect(t *testing.T) {
	// Start 3 Raft nodes + 3 workers
	raftCluster := NewTestRaftCluster(t, 3)
	defer raftCluster.Shutdown()

	workerCluster := NewTestWorkerCluster(t, 3, raftCluster.GetRaftAddrs())
	defer workerCluster.Shutdown()

	time.Sleep(3 * time.Second) // Wait for registration

	// Send PUT to Raft node (should redirect to worker)
	c := client.NewClient([]string{raftCluster.Nodes[0].HTTPAddr}, client.DefaultConfig())

	// Client should follow redirect automatically
	err := c.Put(42, 100)
	assert.NoError(t, err)

	// Verify data is on worker, NOT on Raft node
	// TODO: Add verification logic
}

func TestRaftWorkerRouting_GetRedirect(t *testing.T) {
	// Similar test for GET
	raftCluster := NewTestRaftCluster(t, 3)
	defer raftCluster.Shutdown()

	workerCluster := NewTestWorkerCluster(t, 3, raftCluster.GetRaftAddrs())
	defer workerCluster.Shutdown()

	time.Sleep(3 * time.Second)

	// PUT via worker
	workerAddr := workerCluster.Workers[0].HTTPAddr
	http.Post(workerAddr+"/kv", "application/json", bytes.NewBufferString(`{"key":42,"value":100}`))

	// GET via Raft (should redirect)
	c := client.NewClient([]string{raftCluster.Nodes[0].HTTPAddr}, client.DefaultConfig())
	value, err := c.Get(42)
	assert.NoError(t, err)
	assert.Equal(t, 100, value)
}
```

### Validation Checklist for Stage 3

- [ ] Raft node PUT handler returns 307 Temporary Redirect
- [ ] Raft node GET handler returns 307 Temporary Redirect
- [ ] Raft node DELETE handler returns 307 Temporary Redirect
- [ ] Redirect points to correct worker HTTP address
- [ ] Client follows redirect successfully
- [ ] Data is stored on worker, NOT on Raft node
- [ ] Raft nodes no longer execute data operations
- [ ] Tests pass: `go test ./tests/raft_worker_routing_test.go`

---

## Stage 4: Remove Data Storage from Raft Nodes (FINAL)

**Goal:** Raft nodes store ONLY partition table, no data

**Duration:** 1 day

**Status:** NOT STARTED

### Changes

#### 1. Update `cmd/raftnode/main.go`

Remove data storage from `SimpleKVStore`:

```go
type SimpleKVStore struct {
	mu             sync.RWMutex
	// REMOVED: data map[int]int
	partitionTable *sharding.PartitionTable
}

func NewSimpleKVStore() *SimpleKVStore {
	return &SimpleKVStore{
		// REMOVED: data: make(map[int]int),
		partitionTable: sharding.NewPartitionTable(),
	}
}

func (s *SimpleKVStore) AppendMessage(msg raft.Message) (bool, int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	switch msg.MsgType {
	case "UPDATE_PARTITION_TABLE":
		if msg.PartitionTableUpdate == nil {
			slog.Warn("Received UPDATE_PARTITION_TABLE with nil payload")
			return false, 0
		}
		s.partitionTable.ApplyUpdate(
			msg.PartitionTableUpdate.Assignments,
			msg.PartitionTableUpdate.Version,
		)
		slog.Info("Updated partition table", "version", msg.PartitionTableUpdate.Version)
		return true, 0

	// REMOVED: PUT, GET, DELETE cases

	default:
		slog.Warn("Unknown message type", "type", msg.MsgType)
		return false, 0
	}
}

func (s *SimpleKVStore) GetSnapshot() ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// REMOVED: Data map serialization
	// Only serialize partition table

	partitionTableData, err := s.partitionTable.Serialize()
	if err != nil {
		return nil, fmt.Errorf("failed to serialize partition table: %w", err)
	}

	slog.Info("Created snapshot", "assignments", s.partitionTable.GetAssignmentCount())

	return partitionTableData, nil
}

func (s *SimpleKVStore) RestoreFromSnapshot(data []byte) (error, int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// REMOVED: Data map deserialization
	// Only deserialize partition table

	if err := s.partitionTable.Deserialize(data); err != nil {
		return fmt.Errorf("failed to deserialize partition table: %w", err), 0
	}

	slog.Info("Restored snapshot", "assignments", s.partitionTable.GetAssignmentCount())

	return nil, 0
}
```

### Testing Stage 4

#### 2. `tests/raft_partition_table_only_test.go`

```go
package tests

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestRaftNode_PartitionTableOnly(t *testing.T) {
	// Start 3 Raft nodes (no data storage)
	cluster := NewTestRaftCluster(t, 3)
	defer cluster.Shutdown()

	time.Sleep(2 * time.Second) // Wait for leader election

	// Verify Raft nodes have partition table
	leader := cluster.GetLeader()
	app := leader.App.(*SimpleKVStore)
	assert.Greater(t, app.partitionTable.GetAssignmentCount(), 0)

	// Verify Raft nodes do NOT have data storage
	// (data field removed)

	// Snapshot should only contain partition table
	snapshot, err := app.GetSnapshot()
	assert.NoError(t, err)
	assert.Greater(t, len(snapshot), 0)

	// Snapshot size should be small (only partition table, no data)
	// For 16,384 partitions: ~160 KB
	assert.Less(t, len(snapshot), 200*1024) // Less than 200 KB
}
```

### Validation Checklist for Stage 4

- [ ] `SimpleKVStore.data` field removed
- [ ] Raft `AppendMessage` only handles `UPDATE_PARTITION_TABLE`
- [ ] PUT/GET/DELETE message types removed from Raft
- [ ] Snapshot serialization only includes partition table
- [ ] Snapshot size < 200 KB (no data, only partition table)
- [ ] Raft cluster operates normally with no data storage
- [ ] Worker registration still works
- [ ] Partition table replication still works
- [ ] Tests pass: `go test ./tests/raft_partition_table_only_test.go`

---

## End-to-End Integration Test

### Final Test: Complete System

```go
package tests

import (
	"testing"
	"time"

	"github.com/dominikszczepaniak/distributed-cache/pkg/client"
	"github.com/stretchr/testify/assert"
)

func TestCompleteSystem_RaftAndWorkers(t *testing.T) {
	// Start 3 Raft nodes (control plane)
	raftCluster := NewTestRaftCluster(t, 3)
	defer raftCluster.Shutdown()

	time.Sleep(2 * time.Second) // Wait for leader election

	// Start 3 workers (data plane)
	workerCluster := NewTestWorkerCluster(t, 3, raftCluster.GetRaftAddrs())
	defer workerCluster.Shutdown()

	time.Sleep(3 * time.Second) // Wait for registration

	// Create client (connects to Raft node)
	c := client.NewClient([]string{raftCluster.Nodes[0].HTTPAddr}, client.DefaultConfig())

	// Write 100 keys
	for i := 0; i < 100; i++ {
		err := c.Put(i, i*10)
		assert.NoError(t, err)
	}

	// Read 100 keys
	for i := 0; i < 100; i++ {
		value, err := c.Get(i)
		assert.NoError(t, err)
		assert.Equal(t, i*10, value)
	}

	// Verify data is on workers, NOT on Raft nodes
	for _, raftNode := range raftCluster.Nodes {
		app := raftNode.App.(*SimpleKVStore)
		// Raft nodes should have NO data (data field removed)
		// Only partition table
		assert.Greater(t, app.partitionTable.GetAssignmentCount(), 0)
	}

	// Verify data distribution across workers
	totalKeys := 0
	for _, worker := range workerCluster.Workers {
		stats := worker.Store.GetStats()
		keyCount := stats["key_count"].(int)
		totalKeys += keyCount
		slog.Info("Worker stats", "worker_id", worker.WorkerID, "key_count", keyCount)
	}

	assert.Equal(t, 100, totalKeys) // All keys accounted for
}

func TestCompleteSystem_WorkerFailure(t *testing.T) {
	raftCluster := NewTestRaftCluster(t, 3)
	defer raftCluster.Shutdown()

	workerCluster := NewTestWorkerCluster(t, 3, raftCluster.GetRaftAddrs())
	defer workerCluster.Shutdown()

	time.Sleep(3 * time.Second)

	c := client.NewClient([]string{raftCluster.Nodes[0].HTTPAddr}, client.DefaultConfig())

	// Write key
	c.Put(42, 100)

	// Determine which worker is primary for key 42
	// Kill that worker
	// ... (implementation details)

	// Read from backup (should succeed)
	value, err := c.Get(42)
	assert.NoError(t, err)
	assert.Equal(t, 100, value)
}
```

---

## Summary of All Stages

| Stage | Goal | Duration | Status |
|-------|------|----------|--------|
| 0 | Current State Validation | - | ✅ COMPLETE |
| 1 | Create Worker Binary | 2-3 days | NOT STARTED |
| 2 | Worker Registration | 2-3 days | NOT STARTED |
| 3 | Raft Request Routing | 1-2 days | NOT STARTED |
| 4 | Remove Raft Data Storage | 1 day | NOT STARTED |

**Total Estimated Time:** 6-9 days

**End State:**
- Raft nodes manage ONLY partition table (control plane)
- Workers handle ALL data storage and client requests (data plane)
- Horizontal scaling of workers independent of Raft
- Primary-backup replication between workers
- Clean architectural separation

---

## Next Immediate Steps

1. Create feature branch: `git checkout -b feature/worker-separation`
2. Start with Stage 1: Create worker binary
3. Test standalone worker (no Raft integration)
4. Proceed to Stage 2 once Stage 1 tests pass

Let me know when you're ready to start implementation!
