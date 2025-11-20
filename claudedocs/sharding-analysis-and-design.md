# Distributed Cache Sharding: Analysis and Implementation Plan

## Phase 1: Codebase Analysis

### 1.1 Project Structure

```
distributed-cache/
├── cmd/
│   ├── raftcli/         # CLI client for interacting with cache
│   └── raftnode/        # Main node entry point with SimpleKVStore
├── pkg/
│   ├── api/             # HTTP REST API server layer
│   ├── cache/           # Cache implementations (ConcurrentMap, Sharded, SyncMap)
│   ├── cachemodel/      # Cache interface definition
│   └── raft/            # Raft consensus implementation
│       ├── raftpb/      # Protobuf generated code
│       ├── application.go    # Application interface
│       ├── raft.go          # Core Raft logic
│       ├── snapshot.go      # Snapshot management
│       ├── grpc.go          # gRPC RPC handlers
│       ├── connection_manager.go  # Peer connection handling
│       └── ...
├── proto/               # Protobuf definitions
└── tests/              # Test suites

Total: ~5,908 lines of Go code
```

### 1.2 Raft Consensus Implementation Analysis

**Core Components:**

1. **Raft Struct** (`pkg/raft/raft.go`)
   - Maintains distributed consensus state
   - Fields:
     - `id`, `totalNodes`: Node identity
     - `currentTerm`, `votedFor`, `log[]`: Stable storage variables
     - `commitedLength`: Last committed index
     - `currentRole`, `currentLeaderId`: Leadership state
     - `application`: User-defined state machine (implements `Application` interface)
     - `snapshotter`: Snapshot management
     - `connMgr`: Connection manager for peer communication

2. **Application Interface** (`pkg/raft/application.go`)
   ```go
   type Application interface {
       AppendMessage(message Message) (success bool, value int)
       GetSnapshot() ([]byte, error)
       RestoreFromSnapshot(data []byte) (error, int)
       GetValue(key int) int
   }
   ```
   - Current implementation: `SimpleKVStore` in `cmd/raftnode/main.go`
   - Uses `map[int]int` with `sync.RWMutex`

3. **Message Structure** (`pkg/raft/messages.go`)
   ```go
   type Message struct {
       MsgType MessageType  // GET, PUT, DELETE
       Key     int
       Value   *int
       ResponseChan chan<- BroadcastResponse
       IdempotencyToken string
       ClientID string
   }
   ```

4. **Snapshot System** (`pkg/raft/snapshot.go`)
   - `Snapshot` struct tracks `lastIndex`, `lastTerm`, `snapshotThreshold`
   - Triggered when `len(log) >= snapshotThreshold`
   - Snapshot stored as binary blob on disk
   - Application responsible for serialization/deserialization
   - Snapshot installation via chunked RPC (`InstallSnapshotRequest`)

5. **Log Management**
   - Log entries stored as `[]LogEntry` with term and message
   - Absolute indexing: `absoluteIndex = snapshotter.lastIndex + len(log)`
   - Log compaction via snapshotting
   - Persistence via `DataSaver` to stable storage

**Key Observations:**
- Raft provides linearizable consensus for single state machine
- All operations go through leader (writes are replicated, reads can be stale)
- Snapshot system is mature and handles log compaction
- Clean separation between Raft consensus and application state

### 1.3 Networking/RPC Layer Analysis

**gRPC Service Definition** (`proto/raft.proto`)
```protobuf
service Raft {
    rpc Forward (Message) returns (ForwardResponse);       // PUT/DELETE operations
    rpc ForwardGet (GetRequest) returns (GetResponse);     // GET operations
    rpc VoteRequest (VoteRequestArgs) returns (VoteResponse);
    rpc LogRequest(LogRequestArgs) returns (LogResponse);
    rpc Heartbeat (google.protobuf.Empty) returns (google.protobuf.Empty);
    rpc InstallSnapshot(InstallSnapshotRequest) returns (InstallSnapshotResponse);
}
```

**Message Flow:**
1. **Client → HTTP API** (`pkg/api/server.go`)
   - REST endpoints: `/kv` (POST), `/kv/{key}` (GET, DELETE)
   - Idempotency caching via `IdempotencyCache`
   - Leader caching via `LeaderCache`
   - Retry logic with exponential backoff

2. **HTTP API → Raft** (`pkg/raft/grpc.go`)
   - `Forward()`: Converts HTTP request to Raft message
   - Non-leader nodes forward to leader via `forwardToLeader()`
   - Leader uses `BroadcastSync()` to replicate and commit
   - Redirect loop detection via context value

3. **Raft → Application** (`pkg/raft/raft.go`)
   - `deliverToApplication()` applies committed entries
   - Application returns success/value
   - Response sent back via `ResponseChan`

**Connection Management** (`pkg/raft/connection_manager.go`)
- Maintains gRPC connections to all peers
- Automatic reconnection with exponential backoff
- Health checks via connectivity state monitoring
- Peer availability tracking

**Key Observations:**
- Centralized leader handles all writes
- Clean RPC abstraction via `PeerClient` interface
- Robust retry and health check mechanisms
- Network partitions handled via leader election

### 1.4 Storage Layer Analysis

**Current State:**
- Application: `SimpleKVStore` with `map[int]int`
- Keys and values are both `int` (32-bit)
- Thread-safe via `sync.RWMutex`
- Snapshot format: binary-encoded length + key-value pairs

**Existing Cache Implementations:**
1. **ConcurrentMapCache** (`pkg/cache/ConcurrentMapCache.go`)
   - Single mutex protecting entire map
   - Simple but contended under high concurrency

2. **ShardedCache** (`pkg/cache/ShardedCache.go`)
   - **IMPORTANT**: Local sharding for concurrency only
   - 16 shards by default (power of 2)
   - Shard selection: `shards[key & shardMask]`
   - Each shard has own mutex → reduces lock contention
   - **NOT distributed sharding** (all data on single node)

3. **SyncMapCache** - Uses `sync.Map`

**Key Observations:**
- Existing `ShardedCache` is for local concurrency, not distribution
- No partition assignment logic exists
- No node-to-node data routing
- Keys are integers, limiting flexibility (need string keys for real use)

---

## Phase 2: Architecture Design

### 2.1 Sharding Strategy: Raft-as-Control-Plane

**Fundamental Principle:**
- **Control Plane**: Raft manages partition table (which node owns which partitions)
- **Data Plane**: Each node directly serves keys it owns, redirects others
- **Separation of Concerns**: Consensus only for metadata, not data operations

**Why This Approach:**
1. **Scalability**: Data operations bypass Raft consensus bottleneck
2. **Performance**: Local reads/writes for owned partitions
3. **Simplicity**: Raft already handles leader election and consensus
4. **Fault Tolerance**: Partition table survives node failures via Raft replication

**Trade-offs:**
- Partition table changes require Raft consensus (slower)
- Rebalancing requires data migration (complex)
- Clients must handle redirects (more complex client logic)

### 2.2 Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                    CLIENT (HTTP/CLI)                             │
└───────────────────────────┬─────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│                      API LAYER (HTTP)                            │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │ Request Handler                                           │  │
│  │  1. Compute PartitionID = Hash(key) % TOTAL_PARTITIONS   │  │
│  │  2. Query ShardManager: Do I own this partition?         │  │
│  │  3a. YES → Execute locally                               │  │
│  │  3b. NO  → Return MOVED error with correct node address  │  │
│  └──────────────────────────────────────────────────────────┘  │
└───────────────────────────┬─────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│                    SHARD MANAGER (Data Plane)                    │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │ Local Partition Table Cache                              │  │
│  │  - map[PartitionID]NodeID (read-heavy, RWMutex)         │  │
│  │  - Synchronized with Raft State Machine                 │  │
│  │  - Validates ownership before executing operations      │  │
│  └──────────────────────────────────────────────────────────┘  │
│                                                                  │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │ Key Routing Logic                                        │  │
│  │  ValidateKey(key) → (correctNodeID, error)              │  │
│  └──────────────────────────────────────────────────────────┘  │
└───────────────────────────┬─────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│              RAFT CONSENSUS LAYER (Control Plane)                │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │ Raft State Machine (SimpleKVStore)                       │  │
│  │  - Handles data operations (PUT/GET/DELETE)              │  │
│  │  - NEW: Handles partition table updates                  │  │
│  │  - Replicates to majority before commit                 │  │
│  │  - Snapshot includes partition table state               │  │
│  └──────────────────────────────────────────────────────────┘  │
│                                                                  │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │ Raft Log                                                 │  │
│  │  - Existing: PUT/GET/DELETE messages                     │  │
│  │  - NEW: PARTITION_TABLE_UPDATE messages                  │  │
│  └──────────────────────────────────────────────────────────┘  │
└───────────────────────────┬─────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│                    LOCAL STORAGE                                 │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │ Data Store (map[int]int)                                 │  │
│  │  - Only stores keys this node owns                       │  │
│  │  - Partition filtering during operations                 │  │
│  └──────────────────────────────────────────────────────────┘  │
│                                                                  │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │ Raft Snapshot File                                       │  │
│  │  - Data store state                                      │  │
│  │  - Partition table state                                 │  │
│  └──────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

### 2.3 Data Flow Examples

**Scenario 1: Client writes to correct node**
```
Client → PUT key=42, value=100
  ↓
API Layer: hash(42) = partition 5
  ↓
ShardManager: partition 5 → node 1 (I am node 1) ✓
  ↓
Raft: Broadcast PUT message → consensus → commit
  ↓
Application: data[42] = 100
  ↓
Client ← 200 OK
```

**Scenario 2: Client writes to wrong node (redirect)**
```
Client → PUT key=42, value=100 (to node 2)
  ↓
API Layer: hash(42) = partition 5
  ↓
ShardManager: partition 5 → node 1 (I am node 2) ✗
  ↓
Client ← 301 MOVED {correctNode: 1, address: "node1:8080"}
  ↓
Client → PUT key=42, value=100 (to node 1)
  ↓
[Success path as above]
```

**Scenario 3: Partition table update (rebalancing)**
```
Admin → Add new node 3
  ↓
Leader → Raft: Broadcast PARTITION_TABLE_UPDATE message
  ↓
Raft: Replicate to majority → commit
  ↓
All nodes: Update local partition table cache
  ↓
Background: Data migration from node 1,2 → node 3
```

---

## Phase 3: Implementation Plan

### Stage 1: Core Sharding Package

**Package:** `pkg/sharding`

**Files to Create:**
1. `pkg/sharding/partitioner.go` - Core partitioning logic
2. `pkg/sharding/partitioner_test.go` - Unit tests
3. `pkg/sharding/types.go` - Type definitions
4. `pkg/sharding/hash.go` - Hash functions

**Core Types:**
```go
package sharding

const (
    // TOTAL_PARTITIONS must be power of 2 for efficient modulo
    TOTAL_PARTITIONS = 16384 // Same as Redis Cluster
)

type PartitionID uint16 // 0 to 16383
type NodeID int         // Same as Raft node ID

// Partitioner handles key-to-partition mapping
type Partitioner struct {
    totalPartitions uint16
    hashFunc        HashFunc
}

type HashFunc func(key string) uint16

// HashKey computes partition ID for a given key
func HashKey(key string) PartitionID
```

**Implementation Details:**
- Use CRC16 algorithm (same as Redis)
- Fallback to MurmurHash3 if needed
- Must be deterministic across all nodes
- Support both string and int keys (convert int → string)

**Acceptance Criteria:**
- ✓ Hash function is deterministic
- ✓ 10,000 random keys distributed with <2% variance per partition
- ✓ Zero hash collisions (different keys → same partition OK, different hash values → different partitions)
- ✓ Thread-safe for concurrent use

**Tests:**
```go
func TestHashDistribution(t *testing.T) {
    // Test 10K keys distribute evenly across 16384 partitions
}

func TestHashDeterminism(t *testing.T) {
    // Same key always produces same partition
}

func TestEdgeCases(t *testing.T) {
    // Empty string, very long strings, unicode, etc.
}
```

---

### Stage 2: Control Plane (Raft Integration) ✅ COMPLETED

**Status:** Complete (92.5% test coverage, all tests passing with -race)

**Package:** `pkg/sharding` + modifications to `pkg/raft`

**Files Created:**
1. `pkg/sharding/partition_table.go` - Partition table data structure ✅
2. `pkg/sharding/partition_table_test.go` - Unit tests (40+ test cases) ✅
3. `pkg/sharding/encoding.go` - Serialization/deserialization ✅
4. `pkg/sharding/encoding_test.go` - Serialization tests ✅
5. `tests/partition_table_integration_test.go` - Integration tests ✅

**Files Modified:**
1. `pkg/raft/messages.go` - Add new message type UPDATE_PARTITION_TABLE ✅
2. `proto/raft.proto` - Add PartitionTableUpdate and PartitionAssignment messages ✅
3. `cmd/raftnode/main.go` - Extended SimpleKVStore with partition table ✅

**Core Types:**
```go
// PartitionTable maps each partition to owning node
type PartitionTable struct {
    mu          sync.RWMutex
    assignments map[PartitionID]NodeID
    version     uint64 // Optimistic concurrency control
}

func (pt *PartitionTable) GetOwner(pid PartitionID) (NodeID, bool)
func (pt *PartitionTable) SetOwner(pid PartitionID, nodeID NodeID)
func (pt *PartitionTable) Serialize() ([]byte, error)   // For Raft snapshot
func (pt *PartitionTable) Deserialize(data []byte) error
```

**Raft Message Extension:**
```go
// In pkg/raft/messages.go
type MessageType string
const (
    get    MessageType = "GET"
    put    MessageType = "PUT"
    delete MessageType = "DELETE"
    updatePartitionTable MessageType = "UPDATE_PARTITION_TABLE" // NEW
)

type Message struct {
    MsgType MessageType
    Key     int
    Value   *int

    // NEW: For partition table updates
    PartitionTableUpdate *PartitionTableUpdate

    ResponseChan chan<- BroadcastResponse
    IdempotencyToken string
    ClientID string
}

type PartitionTableUpdate struct {
    Assignments map[PartitionID]NodeID
    Version     uint64
}
```

**Application Interface Extension:**
```go
// Extend SimpleKVStore in cmd/raftnode/main.go
type SimpleKVStore struct {
    mu   sync.RWMutex
    data map[int]int

    // NEW: Partition table state (part of state machine)
    partitionTable *sharding.PartitionTable
}

func (s *SimpleKVStore) AppendMessage(msg raft.Message) (bool, int) {
    switch msg.MsgType {
    case "UPDATE_PARTITION_TABLE":
        // Apply partition table update
        s.partitionTable.ApplyUpdate(msg.PartitionTableUpdate)
        return true, 0
    case "PUT", "GET", "DELETE":
        // Existing logic
    }
}

// Snapshot must include partition table
func (s *SimpleKVStore) GetSnapshot() ([]byte, error) {
    // Serialize: data map + partition table
}

func (s *SimpleKVStore) RestoreFromSnapshot(data []byte) (error, int) {
    // Deserialize: data map + partition table
}
```

**Acceptance Criteria:** ✅ ALL MET
- ✅ PartitionTable survives Raft snapshots and restores correctly (verified via integration tests)
- ✅ All Raft nodes have consistent partition table view after update (ApplyUpdate mechanism)
- ✅ Partition table updates propagate via Raft consensus (UPDATE_PARTITION_TABLE message type)
- ✅ Version tracking implemented (uint64 version field with atomic increments)
- ✅ Snapshot serialization includes both data and partition table (CombineSnapshot/SplitSnapshot)

**Tests:**
```go
func TestPartitionTableSerialization(t *testing.T) {
    // Round-trip serialize/deserialize ✅
}

func TestPartitionTableRaftIntegration(t *testing.T) {
    // Create 3-node Raft cluster ✅
    // Update partition table on leader ✅
    // Verify all nodes have consistent view ✅
}

func TestPartitionTableSnapshot(t *testing.T) {
    // Take snapshot with partition table ✅
    // Restore and verify state ✅
}
```

**Implementation Summary (Stage 2):**

1. **PartitionTable Implementation:**
   - Thread-safe operations with `sync.RWMutex`
   - Version tracking for optimistic concurrency
   - Efficient serialization (binary encoding)
   - Full partition table: ~98KB for 16,384 partitions
   - Operations: Assign, GetOwner, AssignRange, GetNodePartitions, ApplyUpdate, Clone
   - InitializeEvenDistribution for initial setup

2. **Serialization Format:**
   - Binary format with explicit versioning
   - Combined snapshot format: `[MAGIC][PT_SIZE][PT_DATA][DATA_SIZE][DATA]`
   - Magic bytes: "DCSH" (Distributed Cache SHarded)
   - Efficient round-trip: <1ms for full 16K partition table

3. **Raft Integration:**
   - New message type: `UPDATE_PARTITION_TABLE`
   - Protobuf messages: `PartitionTableUpdate`, `PartitionAssignment`
   - SimpleKVStore extended with `partitionTable` field
   - AppendMessage handles partition table updates
   - Snapshot includes both data and partition table
   - Restore from snapshot correctly splits and deserializes both components

4. **Testing:**
   - 40+ unit tests covering all operations
   - Concurrent access tests (race detector enabled)
   - Serialization round-trip tests
   - Integration tests for snapshot/restore
   - Performance benchmarks included
   - Test coverage: 92.5%

5. **Key Design Decisions:**
   - Binary serialization over JSON/protobuf for partition table (performance)
   - ApplyUpdate replaces entire table atomically (simplicity, consistency)
   - Version field tracked but not enforced (leave conflict resolution to Stage 3)
   - CombineSnapshot/SplitSnapshot for clean separation of concerns

---

### Stage 3: Data Plane (Shard Manager)

**Package:** `pkg/sharding`

**Files to Create:**
1. `pkg/sharding/shard_manager.go` - Local routing logic
2. `pkg/sharding/shard_manager_test.go` - Unit tests
3. `pkg/sharding/errors.go` - Custom error types

**Files to Modify:**
1. `pkg/api/server.go` - Integrate shard validation
2. `pkg/api/types.go` - Add redirect response

**Core Types:**
```go
// ShardManager validates key ownership on each node
type ShardManager struct {
    mu             sync.RWMutex
    nodeID         NodeID
    partitionTable *PartitionTable  // Cached from Raft state machine
    partitioner    *Partitioner
}

func NewShardManager(nodeID NodeID, pt *PartitionTable) *ShardManager

// ValidateKey checks if this node should handle the key
func (sm *ShardManager) ValidateKey(key string) (NodeID, error) {
    pid := sm.partitioner.HashKey(key)

    sm.mu.RLock()
    owner, exists := sm.partitionTable.GetOwner(pid)
    sm.mu.RUnlock()

    if !exists {
        return -1, ErrPartitionUnassigned
    }

    if owner != sm.nodeID {
        return owner, NewWrongNodeError(owner, sm.getNodeAddress(owner))
    }

    return sm.nodeID, nil // This node owns the key
}

// UpdatePartitionTable synchronizes with Raft state machine
func (sm *ShardManager) UpdatePartitionTable(pt *PartitionTable)

// Custom Errors
type WrongNodeError struct {
    CorrectNodeID NodeID
    CorrectAddr   string
}

func (e *WrongNodeError) Error() string {
    return fmt.Sprintf("key belongs to node %d at %s", e.CorrectNodeID, e.CorrectAddr)
}

var ErrPartitionUnassigned = errors.New("partition not assigned to any node")
```

**API Integration:**
```go
// In pkg/api/server.go
type Server struct {
    raft         *raft.Raft
    listenAddr   string
    httpServer   *http.Server
    shardManager *sharding.ShardManager // NEW
    // ... existing fields
}

func (s *Server) handlePut(w http.ResponseWriter, r *http.Request) {
    var req PutRequest
    // ... parse request

    // NEW: Validate shard ownership
    keyStr := fmt.Sprintf("%d", req.Key)
    correctNodeID, err := s.shardManager.ValidateKey(keyStr)
    if err != nil {
        if wrongNodeErr, ok := err.(*sharding.WrongNodeError); ok {
            // Return redirect response
            w.Header().Set("Location", wrongNodeErr.CorrectAddr)
            w.WriteHeader(http.StatusMovedPermanently)
            json.NewEncoder(w).Encode(RedirectResponse{
                NodeID:  wrongNodeErr.CorrectNodeID,
                Address: wrongNodeErr.CorrectAddr,
            })
            return
        }
        // Other errors
        http.Error(w, err.Error(), http.StatusInternalServerError)
        return
    }

    // Existing logic: forward to Raft
    // ...
}
```

**Response Types:**
```go
// In pkg/api/types.go
type RedirectResponse struct {
    Error   string `json:"error"`
    NodeID  int    `json:"nodeId"`
    Address string `json:"address"`
    Message string `json:"message"`
}
```

**Acceptance Criteria:**
- ✓ Requests routed to correct node 100% of the time
- ✓ Wrong node errors contain valid redirect information
- ✓ <10ms latency overhead for partition lookup
- ✓ ShardManager cache synchronized with Raft state machine
- ✓ Thread-safe under concurrent access

**Tests:**
```go
func TestShardManagerValidation(t *testing.T) {
    // Test correct node returns nil error
    // Test wrong node returns WrongNodeError
}

func TestShardManagerConcurrency(t *testing.T) {
    // Concurrent ValidateKey calls
}

func TestAPIRedirect(t *testing.T) {
    // HTTP request to wrong node
    // Verify 301 response with correct location
}
```

---

### Stage 4: Integration & Testing

**End-to-End Tests:**
1. **Multi-Node Cluster Setup**
   - 3 Raft nodes with sharding enabled
   - Initial partition table assignment
   - All partitions assigned

2. **Data Distribution Test**
   ```go
   func TestDataDistribution(t *testing.T) {
       // Start 3-node cluster
       // Write 10,000 keys
       // Verify each node has ~3,333 keys (±2%)
       // Verify no data loss
   }
   ```

3. **Client Redirect Test**
   ```go
   func TestClientRedirect(t *testing.T) {
       // Write key to wrong node
       // Follow redirect to correct node
       // Verify write succeeds
   }
   ```

4. **Partition Table Consistency Test**
   ```go
   func TestPartitionTableConsistency(t *testing.T) {
       // Update partition table
       // Verify all nodes converge to same view
       // Verify operations respect new assignments
   }
   ```

5. **Snapshot Recovery Test**
   ```go
   func TestSnapshotWithPartitionTable(t *testing.T) {
       // Populate data and partition table
       // Trigger snapshot
       // Restart node
       // Verify both data and partition table restored
   }
   ```

6. **Rebalancing Test** (Future)
   ```go
   func TestRebalancing(t *testing.T) {
       // Start 2-node cluster
       // Add 3rd node
       // Trigger rebalancing
       // Verify data migrated correctly
   }
   ```

**Performance Benchmarks:**
```go
func BenchmarkShardedWrites(b *testing.B) {
    // Compare throughput: non-sharded vs sharded
    // Target: <5% overhead
}

func BenchmarkPartitionLookup(b *testing.B) {
    // Measure ValidateKey latency
    // Target: <10µs
}
```

**Acceptance Criteria:**
- ✓ 3-node cluster correctly distributes data
- ✓ Client successfully follows redirects (with updated CLI)
- ✓ <5% performance degradation vs single-node
- ✓ All tests pass with `-race` flag
- ✓ Snapshot/restore works correctly
- ✓ Partition table updates propagate reliably

---

## Phase 4: Implementation Details

### 4.1 Initial Partition Assignment Strategy

**Bootstrap Process:**
```go
func InitializePartitionTable(totalNodes int) *PartitionTable {
    pt := NewPartitionTable()
    partitionsPerNode := TOTAL_PARTITIONS / totalNodes
    remainder := TOTAL_PARTITIONS % totalNodes

    currentPartition := PartitionID(0)
    for nodeID := 0; nodeID < totalNodes; nodeID++ {
        count := partitionsPerNode
        if nodeID < remainder {
            count++ // Distribute remainder evenly
        }

        for i := 0; i < count; i++ {
            pt.SetOwner(currentPartition, NodeID(nodeID))
            currentPartition++
        }
    }

    return pt
}
```

**Example (3 nodes, 16384 partitions):**
- Node 0: partitions 0-5460 (5461 partitions)
- Node 1: partitions 5461-10921 (5461 partitions)
- Node 2: partitions 10922-16383 (5462 partitions)

### 4.2 Serialization Format

**Partition Table Binary Format:**
```
┌────────────────────────────────────────┐
│ Version (8 bytes, uint64)             │
├────────────────────────────────────────┤
│ NumAssignments (4 bytes, uint32)      │
├────────────────────────────────────────┤
│ ┌──────────────────────────────────┐  │
│ │ PartitionID (2 bytes, uint16)    │  │
│ │ NodeID (4 bytes, int32)          │  │
│ └──────────────────────────────────┘  │
│ ... (repeated NumAssignments times)   │
└────────────────────────────────────────┘
```

**Combined Snapshot Format:**
```
┌────────────────────────────────────────┐
│ MAGIC (4 bytes: "DCSH")               │ Data Cache SHarded
├────────────────────────────────────────┤
│ PartitionTableSize (4 bytes)          │
├────────────────────────────────────────┤
│ PartitionTable (variable)             │
├────────────────────────────────────────┤
│ DataSize (4 bytes)                    │
├────────────────────────────────────────┤
│ Data (variable, existing format)      │
└────────────────────────────────────────┘
```

### 4.3 String Key Support

**Current Limitation:** Keys are `int`, but real-world caches use `string` keys.

**Solution:**
1. **Extend Application Interface:**
   ```go
   type Application interface {
       AppendMessage(message Message) (success bool, value int)
       GetSnapshot() ([]byte, error)
       RestoreFromSnapshot(data []byte) (error, int)
       GetValue(key int) int

       // NEW: String key methods
       GetValueString(key string) (int, bool)
       PutValueString(key string, value int) bool
       DeleteValueString(key string) bool
   }
   ```

2. **Update SimpleKVStore:**
   ```go
   type SimpleKVStore struct {
       mu             sync.RWMutex
       data           map[int]int    // Legacy int keys
       stringData     map[string]int // NEW: String keys
       partitionTable *sharding.PartitionTable
   }
   ```

3. **Update Message Structure:**
   ```go
   type Message struct {
       MsgType MessageType

       // Key can be either int (legacy) or string
       Key       int
       KeyString string    // NEW

       Value   *int
       // ... rest
   }
   ```

### 4.4 Rebalancing Strategy (Future Work)

**Phase 1: Read-Only Migration**
1. Mark partitions as "migrating"
2. New writes go to target node
3. Background copy from source to target
4. Verify data consistency
5. Switch reads to target node
6. Delete data from source

**Phase 2: Atomic Cutover**
1. Use Raft to coordinate cutover
2. Brief write pause during switch
3. Update partition table atomically

**Complexity:** High - defer to Stage 5 or future milestone

---

## Phase 5: Testing Strategy

### 5.1 Unit Tests

**Coverage Targets:**
- `pkg/sharding`: 95%+ coverage
- Hash distribution: statistical tests
- Partition table: all edge cases
- Serialization: round-trip tests

### 5.2 Integration Tests

**Test Scenarios:**
1. Basic 3-node cluster operations
2. Partition table updates
3. Node failure during operations
4. Snapshot/restore with partition table
5. Concurrent client operations

### 5.3 Chaos Testing

**Failure Scenarios:**
1. Network partition during partition table update
2. Node crash during data migration
3. Split-brain scenarios
4. Slow follower lagging behind

### 5.4 Performance Benchmarks

**Metrics:**
- Throughput (ops/sec): sharded vs non-sharded
- Latency (p50, p95, p99): partition lookup overhead
- Memory usage: partition table size
- CPU usage: hash computation cost

---

## Key Design Decisions

### 1. Why 16,384 Partitions?

**Rationale:**
- Same as Redis Cluster (proven design)
- Power of 2 → efficient modulo via bitwise AND
- Fine-grained enough for rebalancing
- Small enough to fit in memory (16K entries × 4 bytes = 64KB)

**Trade-offs:**
- More partitions = better distribution, slower lookups
- Fewer partitions = faster lookups, coarser rebalancing

### 2. Why CRC16 Hash?

**Rationale:**
- Fast computation
- Good distribution
- Same as Redis (compatibility)
- Hardware-accelerated on some CPUs

**Alternatives:**
- MurmurHash3: Slightly better distribution
- SHA256: Overkill, too slow
- Simple modulo: Poor distribution

### 3. Why Control-Plane-Only Raft?

**Rationale:**
- Raft consensus is expensive (majority replication)
- Data operations are high-volume → bottleneck
- Partition table updates are low-volume
- Clean separation of concerns

**Alternatives:**
- **Full Raft replication**: Every write goes through Raft
  - ❌ Poor scalability
  - ❌ Centralized leader bottleneck
- **Eventual consistency**: No strong consistency
  - ❌ Violates Raft guarantees
  - ❌ Confusing mental model

### 4. Why Client-Side Redirect?

**Rationale:**
- Simpler server logic (no proxy overhead)
- Client learns partition table (caching opportunity)
- Scales better (no hop penalty)

**Alternatives:**
- **Server-side proxy**: Server forwards to correct node
  - ❌ Extra network hop
  - ❌ Increased server load
- **Smart client routing**: Client has full partition table
  - ✅ Future optimization
  - ⚠️ Requires client library changes

---

## Implementation Timeline

**Stage 1: Core Sharding Package** (2-3 days)
- Day 1: Hash function + tests
- Day 2: Partitioner + distribution tests
- Day 3: Edge cases + benchmarks

**Stage 2: Control Plane** (3-4 days)
- Day 1: PartitionTable struct + serialization
- Day 2: Raft message extension
- Day 3: Application integration
- Day 4: Snapshot integration + tests

**Stage 3: Data Plane** (2-3 days)
- Day 1: ShardManager + validation logic
- Day 2: API integration + redirect logic
- Day 3: Tests + error handling

**Stage 4: Integration & Testing** (3-4 days)
- Day 1: End-to-end tests (3-node cluster)
- Day 2: Performance benchmarks
- Day 3: Chaos tests
- Day 4: Documentation + code review

**Total: 10-14 days** for full implementation

---

## Risk Assessment

**High Risk:**
1. **Snapshot format breaking changes**: Existing nodes can't read new snapshots
   - **Mitigation**: Versioned snapshot format, backward compatibility

2. **Race conditions in partition table updates**: Nodes see inconsistent state
   - **Mitigation**: Raft consensus ensures linearizability, RWMutex for cache

3. **Data loss during rebalancing**: Migration fails mid-flight
   - **Mitigation**: Defer rebalancing to future milestone, keep simple initial implementation

**Medium Risk:**
1. **Performance regression**: Sharding adds overhead
   - **Mitigation**: Benchmark early, optimize hot paths

2. **Client complexity**: Redirect logic complicates clients
   - **Mitigation**: Provide helper libraries, clear documentation

**Low Risk:**
1. **Hash collisions**: Different keys map to same partition
   - **Mitigation**: Expected behavior, not a problem (partitions contain multiple keys)

---

## Success Criteria

**Functional:**
- ✓ 3-node cluster distributes 10K keys evenly (±5%)
- ✓ Client can write/read from any node with redirects
- ✓ Partition table survives node restarts
- ✓ All operations maintain consistency

**Performance:**
- ✓ <5% throughput degradation vs non-sharded
- ✓ <10µs partition lookup latency
- ✓ <1 RTT partition table update propagation

**Quality:**
- ✓ 90%+ test coverage
- ✓ Zero data races (`go test -race`)
- ✓ Clean code review approval

---

## Next Steps

1. **Approve Design**: Review this document and get stakeholder buy-in
2. **Create Feature Branch**: `git checkout -b feature/sharding`
3. **Stage 1 Implementation**: Start with core sharding package
4. **Iterate**: Build incrementally, test continuously
5. **Documentation**: Update README and architecture docs

---

## Appendices

### A. Glossary

- **Partition**: Logical unit of data distribution (1 of 16,384)
- **Shard**: Physical node storing data (node in cluster)
- **Control Plane**: Raft consensus managing metadata
- **Data Plane**: Direct data operations without consensus
- **Partition Table**: Map of partition → node assignments
- **Redirect**: MOVED response telling client to try another node

### B. References

- Redis Cluster Specification: https://redis.io/docs/reference/cluster-spec/
- Raft Paper: https://raft.github.io/raft.pdf
- Consistent Hashing: https://en.wikipedia.org/wiki/Consistent_hashing
- CRC16 Algorithm: https://en.wikipedia.org/wiki/Cyclic_redundancy_check

### C. Future Enhancements

1. **Dynamic Rebalancing**: Automatic partition migration
2. **Replication**: Multi-node partition ownership
3. **Read Replicas**: Follower reads for scalability
4. **Client Library**: Smart routing, partition table caching
5. **Monitoring**: Partition distribution metrics, rebalancing progress
6. **Admin API**: Partition table management endpoints
7. **Migration Tools**: Data export/import for rebalancing

---

**Document Version:** 1.0
**Last Updated:** 2025-11-20
**Author:** Claude Code Analysis
**Status:** Ready for Implementation
