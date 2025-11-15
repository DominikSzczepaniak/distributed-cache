# Distributed Cache Development Plan

## Project Overview

Build a distributed cache system with **consistent hashing** and **automatic request routing** on top of the existing Raft-based consensus implementation.

### Current Architecture
- **Raft consensus**: Nodes use Raft protocol for leader election and log replication
- **Simple KV store**: Each node maintains a complete replica of all data
- **HTTP API**: REST endpoints for PUT, GET, DELETE operations
- **Leader forwarding**: Non-leader nodes forward writes to the leader

### Target Architecture
- **Sharded data**: Data distributed across nodes using consistent hashing
- **Fixed shard space**: 1024 virtual shards for predictable redistribution
- **Automatic routing**: Clients/coordinators send requests to the correct shard owner
- **Minimal data movement**: Adding nodes only requires partial shard redistribution
- **Raft per shard**: Each shard has its own Raft cluster for fault tolerance (optional: can start with single-owner shards)

### Critical Architectural Constraints

#### 1. Node Isolation
**Nodes do NOT communicate directly with each other**. Only Raft servers (running inside nodes) communicate:
- Nodes are isolated entities with no knowledge of other nodes
- All inter-node communication happens **exclusively through Raft protocol**
- No direct HTTP/gRPC connections between nodes
- Request forwarding and data migration must use Raft messages
- Cluster topology is shared via Raft log replication

#### 2. Configuration-Driven Design
All system parameters must be **easily configurable** via configuration files:
- Number of virtual shards (default: 1024, configurable)
- Number of nodes in cluster
- Migration batch size
- Health check intervals
- Timeout values
- All tuning parameters externalized to config

**Configuration file location**: `config/distributed_cache.yaml` or environment variables

---

## Configuration System

### Stage 0: Configuration Infrastructure (Prerequisite)
**Goal**: Establish configuration management before implementing features

#### 0.1 Configuration File Structure
**Files to create**:
- `config/distributed_cache.yaml` (default config)
- `pkg/config/config.go` (config loader)
- `pkg/config/config_test.go`

**Configuration schema**:
```yaml
cluster:
  # Cluster identification
  node_id: 0                    # This node's ID (env: NODE_ID)
  total_nodes: 3                # Expected cluster size (env: TOTAL_NODES)

sharding:
  # Sharding parameters
  total_shards: 1024            # Number of virtual shards (env: TOTAL_SHARDS)
  hash_algorithm: "fnv"         # Hash function: fnv, crc32, murmur3

migration:
  # Data migration settings
  batch_size: 100               # Keys per migration batch (env: MIGRATION_BATCH_SIZE)
  batch_delay_ms: 10            # Delay between batches (env: MIGRATION_BATCH_DELAY_MS)
  timeout_seconds: 300          # Total migration timeout (env: MIGRATION_TIMEOUT_SEC)

raft:
  # Raft server configuration (existing)
  raft_id: 0                    # Raft instance ID (env: RAFT_ID)
  raft_addr: "localhost:5000"   # Raft RPC address (env: RAFT_ADDR)
  raft_addrs:                   # All Raft server addresses (env: RAFT_ADDRS)
    - "localhost:5000"
    - "localhost:5001"
    - "localhost:5002"
  data_dir: "./data"            # Persistence directory (env: RAFT_DATA_DIR)

api:
  # HTTP API configuration
  api_addr: ":8080"             # HTTP API listen address (env: API_ADDR)
  request_timeout_ms: 5000      # Request timeout (env: API_REQUEST_TIMEOUT_MS)

monitoring:
  # Health and metrics
  health_check_interval_sec: 5  # Health check frequency (env: HEALTH_CHECK_INTERVAL_SEC)
  metrics_enabled: true         # Enable Prometheus metrics (env: METRICS_ENABLED)
```

**Go configuration structure**:
```go
type Config struct {
    Cluster    ClusterConfig
    Sharding   ShardingConfig
    Migration  MigrationConfig
    Raft       RaftConfig
    API        APIConfig
    Monitoring MonitoringConfig
}

type ClusterConfig struct {
    NodeID     int
    TotalNodes int
}

type ShardingConfig struct {
    TotalShards   int
    HashAlgorithm string
}

type MigrationConfig struct {
    BatchSize      int
    BatchDelayMs   int
    TimeoutSeconds int
}

// ... other config structs
```

**Config loading priority**:
1. Environment variables (highest priority)
2. Config file (`--config` flag or default `config/distributed_cache.yaml`)
3. Hardcoded defaults (lowest priority)

**Key functions**:
- `LoadConfig(configPath string) (*Config, error)`: Load and validate configuration
- `LoadFromEnv() (*Config, error)`: Load from environment variables only
- `Validate() error`: Validate configuration values
- `MergeConfigs(base, override *Config) *Config`: Merge config sources

**Validation rules**:
- `TotalShards` must be power of 2 (for efficient modulo via bitwise AND)
- `NodeID` must be in range [0, TotalNodes)
- `TotalNodes` must be <= `TotalShards`
- Raft addresses must be valid and unique
- All timeout values > 0

**Deliverables**:
- [x] Configuration file schema and defaults
- [x] Config loader with validation
- [x] Environment variable support
- [x] Unit tests for config loading and validation

---

## Development Stages

### Stage 1: Consistent Hashing Infrastructure
**Goal**: Implement the core consistent hashing mechanism with 1024 fixed virtual shards

#### 1.1 Hash Ring Implementation
**Files to create**:
- `pkg/sharding/hash_ring.go`
- `pkg/sharding/hash_ring_test.go`

**Components**:
```go
type HashRing struct {
    totalShards int              // Fixed at 1024
    nodes       map[int]NodeInfo // nodeID -> NodeInfo
    shardMap    []int            // shard -> nodeID mapping
}

type NodeInfo struct {
    NodeID      int
    Address     string
    ShardRanges []ShardRange
}

type ShardRange struct {
    Start int // inclusive
    End   int // exclusive
}
```

**Key functions**:
- `NewHashRing(totalShards int)`: Initialize with 1024 shards
- `AddNode(nodeID int, nodeCount int)`: Redistribute shards when node joins
- `RemoveNode(nodeID int)`: Redistribute shards when node leaves
- `GetNodeForKey(key int) int`: Hash key to shard, lookup owning node
- `GetShardsForNode(nodeID int) []int`: Get all shards owned by a node
- `GetShardForKey(key int) int`: Hash function: `shard = hash(key) % 1024`

**Hash function considerations**:
- Use `hash/fnv` or `hash/crc32` for deterministic hashing
- Ensure even distribution across 1024 shards
- Same key always maps to same shard

**Distribution logic**:
```
Initial state (2 nodes):
  Node 0: shards [0-511]    (512 shards)
  Node 1: shards [512-1023] (512 shards)

After adding Node 2 (3 nodes total):
  Node 0: shards [0-341]     (342 shards)
  Node 1: shards [342-682]   (341 shards)
  Node 2: shards [683-1023]  (341 shards)

After adding Node 3 (4 nodes total):
  Node 0: shards [0-255]     (256 shards)
  Node 1: shards [256-511]   (256 shards)
  Node 2: shards [512-767]   (256 shards)
  Node 3: shards [768-1023]  (256 shards)
```

**Algorithm for redistribution**:
```
shardsPerNode = 1024 / nodeCount
remainder = 1024 % nodeCount

for i := 0 to nodeCount-1:
    start = i * shardsPerNode + min(i, remainder)
    end = start + shardsPerNode + (1 if i < remainder else 0)
    assign shards [start, end) to node i
```

**Tests**:
- Distribution is balanced across nodes
- Same key always maps to same shard
- Adding/removing nodes maintains consistency
- Edge cases: 1 node, 1024 nodes, prime number of nodes

**Deliverables**:
- [x] HashRing implementation with fixed 1024 shards
- [x] Unit tests with >90% coverage
- [x] Benchmarks for key lookup performance

---

### Stage 2: Cluster Configuration and Membership
**Goal**: Track cluster topology and manage node membership

#### 2.1 Cluster Manager
**Files to create**:
- `pkg/cluster/manager.go`
- `pkg/cluster/membership.go`
- `pkg/cluster/manager_test.go`

**Components**:
```go
type ClusterManager struct {
    mu          sync.RWMutex
    localNodeID int
    hashRing    *sharding.HashRing
    nodes       map[int]*NodeMetadata
    raft        *raft.Raft // For consensus on membership changes
}

type NodeMetadata struct {
    NodeID      int
    RaftAddr    string
    APIAddr     string
    Status      NodeStatus // Active, Joining, Leaving, Down
    JoinedAt    time.Time
    LastSeen    time.Time
}

type NodeStatus int
const (
    NodeActive NodeStatus = iota
    NodeJoining
    NodeLeaving
    NodeDown
)
```

**Key functions**:
- `NewClusterManager(nodeID int, initialNodes []NodeMetadata)`: Initialize cluster
- `AddNode(node NodeMetadata)`: Add new node to cluster (triggers rebalance)
- `RemoveNode(nodeID int)`: Remove node from cluster (triggers rebalance)
- `GetNodeInfo(nodeID int) *NodeMetadata`: Get node metadata
- `GetAllNodes() []NodeMetadata`: List all cluster nodes
- `GetNodeForKey(key int) *NodeMetadata`: Route key to owning node
- `IsLocalShard(key int) bool`: Check if key belongs to local node

**Membership protocol**:
1. New node broadcasts join request to cluster
2. Leader receives join, proposes membership change via Raft
3. Once committed, hash ring is updated on all nodes
4. Data migration initiated (Stage 3)
5. New node status changes from Joining → Active

**Configuration storage**:
- Store cluster membership in Raft log (consensus required)
- Each node maintains local copy of hash ring
- Periodic health checks update NodeMetadata.LastSeen

**Tests**:
- Node join/leave updates cluster state correctly
- Concurrent membership changes handled safely
- Failure detection and node marking as Down

**Deliverables**:
- [x] ClusterManager with membership tracking
- [x] Integration with Raft for membership consensus
- [x] Unit and integration tests

---

### Stage 3: Shard-Aware Cache Layer
**Goal**: Implement cache that only stores data for owned shards

#### 3.1 Sharded KV Store
**Files to modify/create**:
- `pkg/cache/distributed_cache.go` (new)
- `pkg/cache/distributed_cache_test.go` (new)
- Modify `cmd/raftnode/main.go` to use distributed cache

**Components**:
```go
type DistributedCache struct {
    mu            sync.RWMutex
    localNodeID   int
    clusterMgr    *cluster.ClusterManager
    shardData     map[int]*ShardedCache  // shardID -> local cache
    ownedShards   map[int]bool           // shardID -> owned?
}

type ShardStorage struct {
    shardID   int
    cache     *ShardedCache  // Reuse existing ShardedCache
}
```

**Key functions**:
- `NewDistributedCache(nodeID int, clusterMgr *cluster.ClusterManager)`
- `Put(key, value int) error`: Check ownership, store if local, forward if remote
- `Get(key int) (int, bool, error)`: Check ownership, retrieve if local, forward if remote
- `Delete(key int) error`: Check ownership, delete if local, forward if remote
- `AddShards(shardIDs []int)`: Take ownership of new shards (for rebalancing)
- `RemoveShards(shardIDs []int, targetNode int)`: Release shard ownership, migrate data
- `GetOwnedShards() []int`: Return list of owned shard IDs

**Ownership check flow**:
```go
func (dc *DistributedCache) Put(key, value int) error {
    shard := dc.clusterMgr.GetShardForKey(key)
    ownerNode := dc.clusterMgr.GetNodeForKey(key)

    if ownerNode.NodeID == dc.localNodeID {
        // Local shard - store directly via local cache
        dc.shardData[shard].Put(key, value)
        return nil
    } else {
        // Remote shard - return error indicating wrong node
        // Client must retry with correct node
        return ErrWrongNode{
            RequestedKey: key,
            OwnerNodeID:  ownerNode.NodeID,
        }
    }
}
```

**Important: No Inter-Node Forwarding**
- Nodes do NOT forward requests to each other
- If a node receives a request for a non-owned shard, it returns `ErrWrongNode`
- Clients must handle routing to correct node (see Stage 6)
- Alternative: API layer can return HTTP 307 redirect to correct node's API address

**Integration with Raft**:
- Each node runs one Raft instance (existing architecture)
- Raft is used for:
  1. **Cluster membership consensus**: Adding/removing nodes
  2. **Topology distribution**: All nodes receive same shard mapping
  3. **Data migration coordination**: Migration commands sent via Raft log
- Nodes only accept writes for shards they own
- All inter-node communication happens through Raft protocol only

**Tests**:
- Put/Get/Delete correctly route based on shard ownership
- Non-owned keys return error or forward
- Shard ownership changes reflected in storage

**Deliverables**:
- [x] DistributedCache implementation
- [x] Integration with ClusterManager
- [x] Unit tests for routing logic

---

### Stage 4: Request Routing and API Response Handling
**Goal**: Enable clients to discover correct nodes for keys

**Important**: This stage does NOT implement inter-node forwarding. Nodes remain isolated.

#### 4.1 Topology Exposure API
**Files to create**:
- `pkg/api/topology.go` (new endpoints)
- `pkg/api/errors.go` (routing errors)

**New HTTP endpoints**:
```
GET /cluster/topology     - Get shard-to-node mapping
GET /cluster/node/{key}   - Get owning node for a specific key
GET /cluster/shards       - Get all shard assignments
```

**Response structures**:
```go
type TopologyResponse struct {
    TotalShards int                     `json:"total_shards"`
    ShardMap    map[int]int             `json:"shard_map"`  // shard -> nodeID
    Nodes       map[int]*NodeInfo       `json:"nodes"`      // nodeID -> info
    Version     int64                   `json:"version"`
}

type NodeInfo struct {
    NodeID      int      `json:"node_id"`
    APIAddr     string   `json:"api_addr"`
    OwnedShards []int    `json:"owned_shards"`
    Status      string   `json:"status"`
}

type KeyRouteResponse struct {
    Key         int      `json:"key"`
    Shard       int      `json:"shard"`
    OwnerNodeID int      `json:"owner_node_id"`
    OwnerAPIAddr string  `json:"owner_api_addr"`
}
```

#### 4.2 API Layer Updates for Wrong Node Handling
**Files to modify**:
- `pkg/api/server.go` (add ownership checks)
- `pkg/api/types.go` (add error responses)

**Changes**:
1. Inject `ClusterManager` into API server
2. Before processing request, check shard ownership
3. If local shard: process normally
4. If remote shard: return error with correct node info

**Wrong node handling - Option A (Error Response)**:
```go
// In handlePut
func (s *Server) handlePut(w http.ResponseWriter, r *http.Request) {
    var req PutRequest
    json.NewDecoder(r.Body).Decode(&req)

    // Check if this node owns the shard
    shard := s.clusterMgr.GetShardForKey(req.Key)
    ownerNode := s.clusterMgr.GetNodeForShard(shard)

    if ownerNode.NodeID != s.localNodeID {
        // Return error with correct node information
        w.WriteHeader(http.StatusMisdirectedRequest) // 421
        json.NewEncoder(w).Encode(ErrorResponse{
            Error:        "wrong_node",
            Message:      "This node does not own the requested shard",
            CorrectNode:  ownerNode.NodeID,
            CorrectAddr:  ownerNode.APIAddr,
            Shard:        shard,
            Key:          req.Key,
        })
        return
    }

    // Local shard - process via Raft as before
    // ... existing Raft logic ...
}
```

**Wrong node handling - Option B (HTTP Redirect)**:
```go
if ownerNode.NodeID != s.localNodeID {
    // HTTP 307 Temporary Redirect to correct node
    redirectURL := fmt.Sprintf("http://%s/kv", ownerNode.APIAddr)
    w.Header().Set("Location", redirectURL)
    w.Header().Set("X-Correct-Node-ID", strconv.Itoa(ownerNode.NodeID))
    w.Header().Set("X-Shard-ID", strconv.Itoa(shard))
    w.WriteHeader(http.StatusTemporaryRedirect)
    return
}
```

**Recommendation**: Use Option A (error response) for explicit client control. Option B (redirect) for simpler clients.

#### 4.3 Cluster Topology Distribution via Raft
**How nodes learn topology**:
1. Membership changes proposed via Raft (AddNode, RemoveNode messages)
2. Leader commits topology change to Raft log
3. All followers receive and apply topology update
4. Each node independently calculates shard ownership
5. No direct node-to-node communication required

**Raft message types to add**:
```go
const (
    // Existing: PUT, GET, DELETE
    MSG_ADD_NODE    = "ADD_NODE"
    MSG_REMOVE_NODE = "REMOVE_NODE"
    MSG_MIGRATE_SHARD = "MIGRATE_SHARD"
)

type AddNodeMessage struct {
    NodeID   int
    APIAddr  string
    RaftAddr string
}
```

**Tests**:
- Requests to correct node processed successfully
- Requests to wrong node return appropriate error/redirect
- Topology endpoint returns accurate shard mapping
- Clients can discover correct nodes via API

**Deliverables**:
- [x] Topology exposure endpoints
- [x] Wrong node error handling in API layer
- [x] Raft-based topology distribution
- [x] Integration tests for routing discovery

---

### Stage 5: Data Migration and Rebalancing
**Goal**: Move data between nodes when cluster topology changes

#### 5.1 Data Migration Manager
**Files to create**:
- `pkg/migration/manager.go`
- `pkg/migration/transfer.go`
- `pkg/migration/manager_test.go`

**Components**:
```go
type MigrationManager struct {
    mu             sync.RWMutex
    cache          *cache.DistributedCache
    clusterMgr     *cluster.ClusterManager
    activeMigrations map[int]*MigrationTask  // shardID -> task
}

type MigrationTask struct {
    ShardID       int
    SourceNode    int
    TargetNode    int
    Status        MigrationStatus
    Progress      float64  // 0.0 to 1.0
    StartTime     time.Time
    EstCompletion time.Time
}

type MigrationStatus int
const (
    MigrationPending MigrationStatus = iota
    MigrationInProgress
    MigrationCompleted
    MigrationFailed
)
```

**Key functions**:
- `StartMigration(shardID, sourceNode, targetNode int)`: Initiate shard migration
- `GetShardData(shardID int) map[int]int`: Extract all key-value pairs for a shard
- `TransferShardData(targetNode int, shardID int, data map[int]int)`: Send data to target
- `ReceiveShardData(shardID int, data map[int]int)`: Accept incoming shard data
- `CompleteMigration(shardID int)`: Finalize migration, update ownership
- `CancelMigration(shardID int)`: Abort migration on failure

**Migration protocol via Raft**:
```
1. Cluster topology change detected (new node joins)
2. Leader calculates new shard distribution
3. Leader proposes topology change via Raft (committed to all nodes)
4. For each shard that needs to move:
   a. Source node (current owner) extracts all keys for shard
   b. Source node sends MIGRATE_SHARD message via Raft with batch data
   c. Raft replicates migration message to all nodes
   d. Target node receives committed migration message, stores data
   e. All nodes update their local shard ownership maps
   f. Source node deletes migrated data after confirming commit
5. New node status: Joining → Active
```

**Key insight**: Migration data flows through Raft log
- Migration messages are special Raft log entries containing key-value batches
- All nodes receive migration messages (via Raft replication)
- Only target node actually stores the data
- Source node cleans up after seeing committed migration
- No direct node-to-node data transfer required

**Raft message for migration**:
```go
type MigrateShardMessage struct {
    ShardID    int
    SourceNode int
    TargetNode int
    Batch      map[int]int  // Key-value pairs to migrate
    BatchNum   int          // Current batch number
    TotalBatches int        // Total batches for this shard
    Final      bool         // Last batch for this shard
}
```

**Consistency during migration**:
- **Reads**: Source serves reads until migration committed
- **Writes**: Source accepts writes until migration committed
- **After commit**: Target becomes authoritative, source rejects with ErrWrongNode
- **Raft ensures**: All nodes agree on ownership transition atomically

**Optimization - Incremental migration via Raft**:
```go
// Migrate in batches through Raft log
func (mm *MigrationManager) MigrateShardIncremental(shardID int, batchSize int) {
    data := mm.cache.GetShardData(shardID)

    // Split into batches
    batches := splitIntoBatches(data, batchSize)

    for i, batch := range batches {
        // Send batch via Raft consensus
        msg := raft.Message{
            MsgType: "MIGRATE_SHARD",
            MigrationData: &MigrateShardMessage{
                ShardID:      shardID,
                SourceNode:   mm.localNodeID,
                TargetNode:   targetNodeID,
                Batch:        batch,
                BatchNum:     i + 1,
                TotalBatches: len(batches),
                Final:        i == len(batches)-1,
            },
        }

        // Broadcast via Raft (blocks until committed)
        mm.raft.Broadcast(msg)

        // Progress automatically tracked via committed log index
    }
}
```

**Handling migration messages in Application**:
```go
func (app *DistributedCache) AppendMessage(msg raft.Message) (bool, int) {
    switch msg.MsgType {
    case "MIGRATE_SHARD":
        migration := msg.MigrationData

        // Only target node stores the data
        if migration.TargetNode == app.localNodeID {
            app.AddShardData(migration.ShardID, migration.Batch)
        }

        // Only source node cleans up (after final batch)
        if migration.SourceNode == app.localNodeID && migration.Final {
            app.RemoveShardData(migration.ShardID)
        }

        // All nodes update ownership (after final batch)
        if migration.Final {
            app.UpdateShardOwnership(migration.ShardID, migration.TargetNode)
        }

        return true, 0

    // ... existing PUT, GET, DELETE cases
    }
}
```

**Tests**:
- Data correctly migrated from source to target
- No data loss during migration
- Concurrent reads/writes handled correctly
- Migration rollback on failure

**Deliverables**:
- [x] MigrationManager implementation
- [x] Incremental migration with progress tracking
- [x] Integration tests with multi-node cluster

---

### Stage 6: Client Library with Smart Routing
**Goal**: Client library that discovers topology and routes requests to correct nodes

**Important**: Clients CAN connect to nodes' HTTP APIs. The isolation constraint only applies to node-to-node communication, not client-to-node.

#### 6.1 Go Client Library
**Files to create**:
- `pkg/client/client.go`
- `pkg/client/topology.go`
- `pkg/client/client_test.go`

**Components**:
```go
type Client struct {
    mu          sync.RWMutex
    topology    *ClusterTopology
    httpClients map[int]*http.Client  // nodeID -> HTTP client
    bootstrapNodes []string            // Seed nodes for topology discovery
}

type ClusterTopology struct {
    Nodes       map[int]*NodeInfo     // nodeID -> node info
    ShardMap    map[int]int           // shard -> nodeID
    TotalShards int
    Version     int64                 // Topology version
}
```

**Key functions**:
- `NewClient(bootstrapNodes []string)`: Initialize with seed node addresses
- `Put(key, value int) error`: Route directly to owning node's HTTP API
- `Get(key int) (int, bool, error)`: Route directly to owning node's HTTP API
- `Delete(key int) error`: Route directly to owning node's HTTP API
- `RefreshTopology()`: Fetch latest cluster topology from any node
- `handleWrongNodeError(err error)`: Handle misdirected requests gracefully

**Smart routing flow**:
```go
func (c *Client) Put(key, value int) error {
    // 1. Calculate shard from key (same hash function as nodes)
    c.mu.RLock()
    shard := hash(key) % c.topology.TotalShards

    // 2. Lookup owning node from local topology cache
    nodeID := c.topology.ShardMap[shard]
    node := c.topology.Nodes[nodeID]
    c.mu.RUnlock()

    // 3. Send HTTP request directly to owning node's API
    httpClient := c.httpClients[nodeID]
    req := &PutRequest{Key: key, Value: value}

    resp, err := httpClient.Post(node.APIAddr + "/kv", req)

    // 4. Handle wrong node errors (stale topology)
    if resp.StatusCode == 421 { // Misdirected Request
        var errResp ErrorResponse
        json.Unmarshal(respBody, &errResp)

        // Topology is stale, refresh and retry
        c.RefreshTopology()
        return c.Put(key, value) // Retry once
    }

    return handleResponse(resp, err)
}
```

**Topology discovery**:
```go
func (c *Client) RefreshTopology() error {
    // Try each bootstrap node until one responds
    for _, nodeAddr := range c.bootstrapNodes {
        resp, err := http.Get(fmt.Sprintf("http://%s/cluster/topology", nodeAddr))
        if err != nil {
            continue // Try next node
        }

        var topology TopologyResponse
        json.NewDecoder(resp.Body).Decode(&topology)

        c.mu.Lock()
        c.topology.Nodes = topology.Nodes
        c.topology.ShardMap = topology.ShardMap
        c.topology.TotalShards = topology.TotalShards
        c.topology.Version = topology.Version
        c.mu.Unlock()

        return nil
    }

    return errors.New("failed to refresh topology from any node")
}
```

**Error handling for wrong nodes**:
- Client maintains local topology cache
- On 421 Misdirected Request: refresh topology and retry
- On network error: try another replica (if replication enabled)
- Exponential backoff for repeated failures

**Topology refresh strategies**:
- **On init**: Fetch topology when client starts
- **On error**: Refresh when receiving wrong node errors (421)
- **Periodic**: Optional background refresh every N seconds
- **Manual**: Expose `RefreshTopology()` for explicit updates

**Implement on-init + on-error first, periodic refresh later**

**Example usage**:
```go
// Initialize client with seed nodes
client := NewClient([]string{
    "node0.cache.local:8080",
    "node1.cache.local:8080",
    "node2.cache.local:8080",
})

// Client automatically routes to correct node
err := client.Put(42, 100)
if err != nil {
    log.Fatal(err)
}

value, found, err := client.Get(42)
```

**Tests**:
- Client correctly routes to owning nodes
- Topology refresh on wrong node errors
- Retry logic works for transient failures
- Client handles node failures gracefully
- Concurrent requests from multiple clients

**Deliverables**:
- [x] Go client library with smart routing
- [x] Topology discovery and caching
- [x] Wrong node error handling with retry
- [x] Example usage and documentation

---

### Stage 7: Monitoring and Observability
**Goal**: Track cluster health, shard distribution, and performance

#### 7.1 Metrics and Health Endpoints
**Files to create**:
- `pkg/metrics/collector.go`
- `pkg/api/monitoring.go`

**Metrics to track**:
```go
type ClusterMetrics struct {
    // Node-level
    NodeStatus         NodeStatus
    OwnedShards        int
    TotalKeys          int
    MemoryUsage        int64

    // Request metrics
    RequestsPerSecond  float64
    LatencyP50         time.Duration
    LatencyP99         time.Duration
    ErrorRate          float64

    // Migration metrics
    ActiveMigrations   int
    MigrationProgress  map[int]float64  // shardID -> progress

    // Shard distribution
    ShardBalance       float64  // Coefficient of variation
}
```

**New HTTP endpoints**:
```
GET /metrics              - Prometheus-compatible metrics
GET /cluster/status       - Overall cluster health
GET /cluster/topology     - Current shard distribution
GET /cluster/migrations   - Active migration status
GET /shard/{id}/stats     - Per-shard statistics
```

**Health checks**:
- Node liveness: Heartbeat every 5 seconds
- Shard health: Keys count, replication status
- Cluster balance: Shard distribution variance

**Deliverables**:
- [x] Metrics collection and exposition
- [x] Health check endpoints
- [x] Dashboard-ready JSON responses

---

### Stage 8: End-to-End Testing and Validation
**Goal**: Comprehensive testing of the distributed cache system

#### 8.1 Integration Tests
**Files to create**:
- `tests/e2e/cluster_test.go`
- `tests/e2e/rebalance_test.go`
- `tests/e2e/failover_test.go`

**Test scenarios**:

**Basic functionality**:
1. Start 2-node cluster
2. Insert 10,000 keys
3. Verify keys distributed ~50/50
4. Read all keys successfully
5. Delete random keys
6. Verify deletions

**Rebalancing**:
1. Start 2-node cluster
2. Insert 10,000 keys
3. Add 3rd node
4. Verify ~33/33/33 distribution
5. Verify all keys still accessible
6. Verify no data loss

**Node failure**:
1. Start 3-node cluster with replication
2. Insert keys
3. Kill one node
4. Verify remaining nodes serve traffic
5. Verify no data loss (if replicated)

**Concurrent operations**:
1. Multiple clients issuing requests
2. Node joins/leaves during traffic
3. Verify consistency and availability

**Performance benchmarks**:
- Throughput: requests/second under load
- Latency: p50, p95, p99 for Get/Put
- Scalability: Linear improvement with nodes

**Deliverables**:
- [x] Comprehensive integration test suite
- [x] Performance benchmarks
- [x] Failure scenario tests
- [x] Documentation of test results

---

## Implementation Order Summary

```
Stage 0: Configuration System (1 day) **PREREQUISITE**
  ├─ YAML configuration schema
  ├─ Environment variable support
  ├─ Config validation
  └─ Integration with existing Raft config

Stage 1: Consistent Hashing (1-2 days)
  ├─ Hash ring with configurable shards (default: 1024)
  ├─ Key-to-shard mapping
  └─ Shard distribution algorithm

Stage 2: Cluster Management (2-3 days)
  ├─ Node membership tracking
  ├─ Raft consensus for topology changes
  └─ Health monitoring

Stage 3: Sharded Cache (2-3 days)
  ├─ Distributed cache layer
  ├─ Shard ownership logic
  ├─ Integration with Raft Application interface
  └─ No inter-node forwarding (return errors instead)

Stage 4: Topology API & Wrong Node Handling (2-3 days)
  ├─ Topology exposure endpoints
  ├─ Wrong node error responses (421 status)
  ├─ Raft-based topology distribution
  └─ No direct node-to-node communication

Stage 5: Data Migration via Raft (3-4 days)
  ├─ Migration manager
  ├─ Raft-based data transfer (no direct node-to-node)
  ├─ Incremental migration through Raft log
  └─ Atomic ownership updates

Stage 6: Client Library (2-3 days)
  ├─ Smart client with topology awareness
  ├─ Direct HTTP routing to correct nodes
  ├─ Topology refresh on wrong node errors
  └─ Example usage

Stage 7: Monitoring (1-2 days)
  ├─ Metrics collection
  ├─ Health endpoints
  └─ Observability

Stage 8: Testing (2-3 days)
  ├─ Integration tests
  ├─ Performance benchmarks
  └─ Failure scenarios
```

**Total estimated time: 16-24 days**

---

## Key Design Decisions

### 1. Configuration-Driven Architecture
**Pros**:
- Easy tuning without code changes
- Environment-specific configurations
- Supports both YAML and environment variables
- Validation catches errors early

**Cons**:
- Need to maintain config schema
- Additional validation logic

**Decision**: Use YAML config files with environment variable overrides. All key parameters (shard count, batch sizes, timeouts) externalized to config.

### 2. Node Isolation with Raft-Only Communication
**Pros**:
- Leverages existing Raft infrastructure
- No additional network connections required
- All topology changes are consensus-based
- Simpler security model (only Raft ports exposed)
- Migration data benefits from Raft durability

**Cons**:
- Raft log grows with migration data (mitigated by snapshots)
- All communication serialized through Raft protocol
- Cannot use optimized direct data transfer

**Decision**: **All inter-node communication exclusively through Raft**. Nodes do not connect to each other. Clients connect to node HTTP APIs for data operations.

### 3. Configurable Shard Count (Default: 1024)
**Pros**:
- Predictable redistribution math (when power of 2)
- Fast rebalancing (only affected shards move)
- Configurable for different cluster sizes
- Efficient modulo via bitwise AND

**Cons**:
- Maximum nodes limited by shard count
- Coarse granularity for small clusters

**Decision**: Default to 1024 shards (configurable). Validate that shard count is power of 2 for efficiency.

### 4. Single Raft Instance per Node
**Pros**:
- Simpler implementation
- Reuses existing Raft code
- Lower overhead
- Proven architecture

**Cons**:
- Leader bottleneck for writes
- No per-shard isolation

**Decision**: Start with single Raft per node. Can evolve to per-shard Raft groups later if needed.

### 5. Client-Side Routing (No Server-Side Forwarding)
**Pros**:
- Single-hop requests (lower latency)
- No server-side forwarding complexity
- Nodes remain isolated
- Better scalability

**Cons**:
- Clients need topology awareness
- More complex client library
- Clients must handle wrong node errors

**Decision**: Client-side routing only. Nodes return 421 errors for misrouted requests. Clients fetch topology and route correctly.

### 6. Migration via Raft Log
**Pros**:
- No direct node-to-node connections
- Atomic ownership transitions
- Migration data is durable (Raft guarantees)
- All nodes see migration progress

**Cons**:
- Raft log size increases during migration
- Migration bandwidth limited by Raft throughput

**Decision**: Send migration batches through Raft log. Use incremental batching to manage log size. Rely on Raft snapshots to compact migrated data.

---

## Success Criteria

✅ **Functional**:
- Keys correctly distributed across nodes via hashing
- Requests automatically routed to correct nodes
- Adding nodes triggers rebalancing with minimal data movement
- No data loss during rebalancing
- All CRUD operations work correctly

✅ **Performance**:
- Throughput scales linearly with node count
- Latency remains consistent under load
- Rebalancing completes in < 1 minute for 10,000 keys

✅ **Reliability**:
- >99% availability during normal operation
- Graceful degradation on node failures
- No split-brain scenarios

✅ **Observability**:
- Clear metrics on shard distribution
- Migration progress tracking
- Request routing visibility

---

## Future Enhancements (Out of Scope for Initial Implementation)

1. **Replication**: Multiple replicas per shard for fault tolerance
2. **Dynamic shard count**: Increase shards as cluster grows
3. **Multi-datacenter**: Geographic distribution and WAN optimization
4. **Compression**: Reduce memory footprint for large values
5. **TTL and eviction**: Cache expiration policies
6. **Query API**: Range scans, filters, secondary indices
7. **Transactions**: Multi-key atomic operations
8. **Streaming**: Real-time change feeds

---

## Getting Started

### Prerequisites
- Go 1.24+
- Existing Raft implementation working
- Understanding of consistent hashing concepts

### Next Steps
1. Review this plan and align on approach
2. Start with Stage 1: Implement hash ring
3. Write tests for each component before moving to next stage
4. Integrate incrementally with existing codebase
5. Validate end-to-end after Stage 4

### Questions to Resolve Before Starting
- [ ] Do we need replication (multiple replicas per shard)?
- [ ] Target performance SLOs (latency, throughput)?
- [ ] Expected cluster size (affects design choices)?
- [ ] Initial shard count (1024 recommended, must be power of 2)?
- [ ] Migration batch size for optimal Raft log management?

---

## Architecture Summary

### ✅ What This Plan Implements

1. **Consistent Hashing**: Keys distributed across 1024 virtual shards (configurable)
2. **Automatic Routing**: Clients discover topology and route to correct nodes
3. **Minimal Data Movement**: Only affected shards migrate when nodes join/leave
4. **Configuration-Driven**: All parameters in YAML config or environment variables
5. **Raft-Based Coordination**: Topology changes and migrations via Raft consensus

### 🔒 Critical Constraints

1. **Node Isolation**: Nodes do NOT communicate directly with each other
2. **Raft-Only Communication**: All inter-node communication through Raft protocol
3. **No Node-to-Node Forwarding**: Nodes return errors for misrouted requests
4. **Client-Side Routing**: Clients fetch topology and route to correct nodes
5. **Migration via Raft**: Data migration flows through Raft log, not direct transfers

### 🎯 Communication Patterns

```
✅ ALLOWED:
- Client → Node HTTP API (GET /kv/123)
- Client → Any Node (GET /cluster/topology)
- Node → Raft Protocol → Other Nodes (consensus, log replication)
- Raft Protocol → Node (migration batches in log)

❌ NOT ALLOWED:
- Node → Node HTTP API (no direct forwarding)
- Node → Node gRPC (no direct data transfer)
- Node → Node Custom Protocol (Raft only)
```

### 📦 Data Flow Example

```
Client wants to PUT key=42, value=100:

1. Client calculates: shard = hash(42) % 1024 = 137
2. Client queries topology: shard 137 → Node 1
3. Client sends HTTP PUT to Node 1's API
4. Node 1 checks: "Do I own shard 137?" → Yes
5. Node 1 processes via Raft (existing flow)
6. Raft replicates to followers
7. Client receives success response

If Client sent to wrong node (Node 0):
1. Node 0 checks: "Do I own shard 137?" → No
2. Node 0 returns 421 error with Node 1's address
3. Client updates topology cache
4. Client retries to Node 1
```
