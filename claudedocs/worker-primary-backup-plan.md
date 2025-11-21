# Worker Node Primary-Backup Implementation Plan

## Current State Analysis

### Existing Architecture (Stages 1-4 Complete)

**Control Plane (Raft Cluster):**
- 3-node Raft cluster managing topology via `PartitionTable`
- Partition table stored in Raft state machine (survives snapshots)
- 16,384 partitions distributed across nodes
- Current structure: `map[PartitionID]NodeID` (single owner per partition)

**Data Plane (Worker Nodes):**
- `SimpleKVStore` in `/cmd/raftnode/main.go` with `map[int]int` storage
- HTTP API in `/pkg/api/server.go` with PUT/GET/DELETE handlers
- `ShardManager` validates key ownership and redirects wrong-node requests
- Write flow: Client → HTTP API → ShardManager validates → Raft consensus → All nodes replicate

**Current Write Path Problem:**
Every write goes through full Raft consensus (N-way replication), which:
- Creates a bottleneck at the leader
- Does not scale with cluster size
- High latency for write operations

**Current Partition Table Structure:**
```go
type PartitionTable struct {
    mu          sync.RWMutex
    assignments map[PartitionID]NodeID  // Single owner only
    version     uint64
}
```

**What's Missing for Primary-Backup:**
1. Backup node tracking in partition table (currently only primary)
2. Synchronous replication RPC between primary and backup
3. Write path that replicates to backup BEFORE confirming to client
4. Replication handler on backup nodes
5. Failure detection and backup reassignment

---

## Proposed Architecture

### Modified Partition Table Structure

**Goal:** Track both primary and backup nodes per partition

```go
// NEW: Partition entry with primary + backup
type PartitionEntry struct {
    PartitionID  PartitionID
    PrimaryNode  NodeID
    BackupNode   NodeID  // NEW: Single backup node
    Version      uint64  // Per-entry version for tracking
}

// MODIFIED: Partition table now stores PartitionEntry
type PartitionTable struct {
    mu          sync.RWMutex
    assignments map[PartitionID]*PartitionEntry  // Changed from NodeID to *PartitionEntry
    version     uint64
}

// NEW METHODS
func (pt *PartitionTable) GetPrimary(partitionID PartitionID) (NodeID, bool)
func (pt *PartitionTable) GetBackup(partitionID PartitionID) (NodeID, bool)
func (pt *PartitionTable) GetReplicas(partitionID PartitionID) (primary NodeID, backup NodeID, ok bool)
func (pt *PartitionTable) SetReplicas(partitionID PartitionID, primary NodeID, backup NodeID)
```

---

### Worker Struct Design

**Current SimpleKVStore (in `/cmd/raftnode/main.go`):**
```go
type SimpleKVStore struct {
    mu             sync.RWMutex
    data           map[int]int
    partitionTable *sharding.PartitionTable
}
```

**Proposed Enhancement (No new struct, enhance existing):**

The Worker Node functionality will be integrated into the existing components:
- `SimpleKVStore`: Already has data storage and partition table
- `ShardManager`: Handles routing and validation
- `API Server`: Add replication logic to PUT/DELETE handlers
- **NEW `ReplicationClient`**: Handles synchronous replication to backup

```go
// NEW: Replication client for primary → backup communication
// Location: /pkg/replication/client.go
package replication

type Client struct {
    nodeID        sharding.NodeID
    raftClients   map[sharding.NodeID]raftpb.RaftClient  // Reuse existing gRPC connections
    timeout       time.Duration  // Default: 1 second
    mu            sync.RWMutex

    // Circuit breaker state
    failures      map[sharding.NodeID]int
    circuitOpen   map[sharding.NodeID]bool
}

func NewClient(nodeID sharding.NodeID, timeout time.Duration) *Client {
    return &Client{
        nodeID:      nodeID,
        raftClients: make(map[sharding.NodeID]raftpb.RaftClient),
        timeout:     timeout,
        failures:    make(map[sharding.NodeID]int),
        circuitOpen: make(map[sharding.NodeID]bool),
    }
}

// RegisterPeer adds a gRPC client for a backup node
func (c *Client) RegisterPeer(nodeID sharding.NodeID, client raftpb.RaftClient)

// Replicate sends a synchronous replication request to backup
func (c *Client) Replicate(ctx context.Context, backupNodeID sharding.NodeID, key, value int) error

// DeleteReplicate sends a synchronous delete replication to backup
func (c *Client) DeleteReplicate(ctx context.Context, backupNodeID sharding.NodeID, key int) error
```

**API Server Enhancement:**
```go
// MODIFIED: pkg/api/server.go
type Server struct {
    raft               *raft.Raft
    listenAddr         string
    httpServer         *http.Server
    retrier            *Retrier
    idempotencyCache   *IdempotencyCache
    leaderCache        *LeaderCache
    shardManager       *sharding.ShardManager
    replicationClient  *replication.Client  // NEW: For primary→backup replication
}
```

---

### HandleSet Pseudo-code (Primary Node)

**Location:** `/pkg/api/server.go` - Modify `handlePut()`

```go
func (s *Server) handlePut(w http.ResponseWriter, r *http.Request) {
    // [1] PARSE REQUEST
    var req PutRequest
    if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
        http.Error(w, "Invalid request body", http.StatusBadRequest)
        return
    }

    // [2] SHARD VALIDATION - Am I the Primary?
    if s.shardManager != nil {
        keyStr := fmt.Sprintf("%d", req.Key)
        partitionID := s.shardManager.GetPartitionID(keyStr)

        // NEW: Check if we are PRIMARY (not just owner)
        primaryNode, backupNode, ok := s.shardManager.GetReplicas(partitionID)
        if !ok {
            http.Error(w, "Partition not assigned", http.StatusInternalServerError)
            return
        }

        if primaryNode != s.shardManager.GetNodeID() {
            // NOT PRIMARY → Return redirect to primary
            primaryAddr, _ := s.shardManager.GetNodeAddress(primaryNode)
            w.Header().Set("Location", primaryAddr)
            w.WriteHeader(http.StatusTemporaryRedirect)
            json.NewEncoder(w).Encode(RedirectResponse{
                Error:   "WRONG_NODE",
                Message: "This node is not primary, redirect to primary",
                NodeID:  fmt.Sprintf("%d", primaryNode),
                Address: primaryAddr,
            })
            return
        }

        // [3] SYNCHRONOUS REPLICATION TO BACKUP (NEW)
        if backupNode >= 0 && s.replicationClient != nil {
            ctx, cancel := context.WithTimeout(r.Context(), 1*time.Second)
            defer cancel()

            err := s.replicationClient.Replicate(ctx, backupNode, req.Key, req.Value)
            if err != nil {
                slog.Error(fmt.Sprintf("Replication to backup %d failed: %v", backupNode, err))

                // OPTION A: Fail the write (strong consistency)
                http.Error(w, "Replication failed", http.StatusServiceUnavailable)
                return

                // OPTION B: Trigger reconfiguration and retry (future enhancement)
                // s.triggerBackupReconfiguration(partitionID, backupNode)
            }

            slog.Info(fmt.Sprintf("Replicated PUT key=%d to backup node %d", req.Key, backupNode))
        }
    }

    // [4] WRITE TO LOCAL RAFT (Existing logic continues)
    // This ensures the write is committed to the Raft log for durability
    // But we've already replicated to backup synchronously above

    idempotencyToken := r.Header.Get("Idempotency-Key")
    if idempotencyToken == "" {
        idempotencyToken = s.generateIdempotencyToken(&req, getClientID(r))
    }

    // [Existing idempotency cache check...]

    var success bool
    var broadcastErr error

    retryFunc := func(ctx context.Context) error {
        msg := &raftpb.Message{
            Type:             raftpb.Message_PUT,
            Key:              int32(req.Key),
            Value:            wrapperspb.Int32(int32(req.Value)),
            IdempotencyToken: idempotencyToken,
            ClientId:         getClientID(r),
        }

        resp, err := s.raft.Forward(ctx, msg)
        if err != nil {
            broadcastErr = err
            return err
        }

        success = resp.Success
        return nil
    }

    ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
    defer cancel()

    err := s.retrier.ExecuteWithRetry(ctx, retryFunc)

    if err != nil {
        // [Existing error handling...]
        return
    }

    // [5] RETURN SUCCESS - Only after BOTH backup replication AND Raft commit
    s.idempotencyCache.Set(idempotencyToken, raft.BroadcastResponse{
        Success: success,
        Value:   0,
        Error:   nil,
    })

    resp := PutResponse{
        Success: success,
        Message: "Key-value pair stored and replicated successfully",
    }

    w.Header().Set("Content-Type", "application/json")
    json.NewEncoder(w).Encode(resp)
}
```

**Key Design Decisions:**

1. **Replication BEFORE Raft Commit:**
   - Primary replicates to backup first
   - If replication fails, write is rejected (no partial writes)
   - Strong consistency guarantee

2. **Timeout:** 1 second for replication (aggressive failure detection)

3. **Failure Handling:**
   - If backup unreachable: Return 503 Service Unavailable
   - Client can retry (idempotent operations)
   - Future: Trigger automatic backup reassignment via Raft

4. **Security:** ShardManager already validates primary ownership

---

### HandleREPLICATE Pseudo-code (Backup Node)

**Location:** NEW gRPC handler in `/pkg/raft/grpc_server.go`

**Step 1: Add Replicate RPC to protobuf**

Modify `/proto/raft.proto`:
```protobuf
message ReplicateRequest {
    int32 key = 1;
    int32 value = 2;
    string operation = 3;  // "PUT" or "DELETE"
    uint64 version = 4;    // For future ordering/deduplication
}

message ReplicateResponse {
    bool success = 1;
    string error = 2;
}

service Raft {
    // ... existing RPCs ...
    rpc Replicate(ReplicateRequest) returns (ReplicateResponse);
}
```

**Step 2: Implement gRPC handler**

In `/pkg/raft/grpc_server.go`:
```go
func (s *GrpcServer) Replicate(ctx context.Context, req *raftpb.ReplicateRequest) (*raftpb.ReplicateResponse, error) {
    // [1] SECURITY CHECK - Ensure caller is current primary for this partition
    // This requires ShardManager reference in GrpcServer
    key := int(req.Key)
    keyStr := fmt.Sprintf("%d", key)
    partitionID := s.shardManager.GetPartitionID(keyStr)

    primaryNode, backupNode, ok := s.shardManager.GetReplicas(partitionID)
    if !ok {
        return &raftpb.ReplicateResponse{
            Success: false,
            Error:   "partition not assigned",
        }, nil
    }

    // Security: Verify I am the BACKUP for this partition
    if backupNode != s.nodeID {
        slog.Warn(fmt.Sprintf("Received REPLICATE for partition %d but I am not backup (primary=%d, backup=%d, me=%d)",
            partitionID, primaryNode, backupNode, s.nodeID))
        return &raftpb.ReplicateResponse{
            Success: false,
            Error:   "not backup node for this partition",
        }, nil
    }

    // [2] WRITE TO LOCAL STORAGE (in-memory)
    app := s.raft.GetApplication()
    kvStore, ok := app.(*SimpleKVStore)
    if !ok {
        return &raftpb.ReplicateResponse{
            Success: false,
            Error:   "invalid application type",
        }, nil
    }

    switch req.Operation {
    case "PUT":
        // Direct write to map (bypass Raft - this is replication)
        kvStore.mu.Lock()
        kvStore.data[key] = int(req.Value)
        kvStore.mu.Unlock()

        slog.Info(fmt.Sprintf("REPLICATE PUT key=%d value=%d", key, req.Value))

        return &raftpb.ReplicateResponse{Success: true}, nil

    case "DELETE":
        kvStore.mu.Lock()
        delete(kvStore.data, key)
        kvStore.mu.Unlock()

        slog.Info(fmt.Sprintf("REPLICATE DELETE key=%d", key))

        return &raftpb.ReplicateResponse{Success: true}, nil

    default:
        return &raftpb.ReplicateResponse{
            Success: false,
            Error:   "unknown operation",
        }, nil
    }
}
```

**Key Security Considerations:**

1. **Verify Backup Role:** Only accept replication if this node is the designated backup
2. **Partition Ownership:** Check partition table to validate replication authority
3. **No Raft Consensus:** Backup writes directly to memory (trusts primary)
4. **Idempotency:** Same key-value can be replicated multiple times safely

---

## Implementation Stages

### Stage 1: Extend Partition Table for Primary+Backup

**Goal:** Modify `PartitionTable` to track both primary and backup nodes per partition

**Duration:** 1-2 days

#### Files to Change

**1. `/pkg/sharding/types.go`**
```go
// ADD: New PartitionEntry struct
type PartitionEntry struct {
    PartitionID  PartitionID
    PrimaryNode  NodeID
    BackupNode   NodeID  // -1 if no backup assigned
    Version      uint64
}
```

**2. `/pkg/sharding/partition_table.go`**

Changes needed:
```go
// CHANGE: From map[PartitionID]NodeID to map[PartitionID]*PartitionEntry
type PartitionTable struct {
    mu          sync.RWMutex
    assignments map[PartitionID]*PartitionEntry  // CHANGED
    version     uint64
}

// MODIFY: GetOwner → keep for backward compatibility, returns primary
func (pt *PartitionTable) GetOwner(partitionID PartitionID) (NodeID, bool) {
    pt.mu.RLock()
    defer pt.mu.RUnlock()

    entry, exists := pt.assignments[partitionID]
    if !exists {
        return -1, false
    }
    return entry.PrimaryNode, true
}

// NEW: GetPrimary (explicit name)
func (pt *PartitionTable) GetPrimary(partitionID PartitionID) (NodeID, bool) {
    return pt.GetOwner(partitionID)  // Same as GetOwner
}

// NEW: GetBackup
func (pt *PartitionTable) GetBackup(partitionID PartitionID) (NodeID, bool) {
    pt.mu.RLock()
    defer pt.mu.RUnlock()

    entry, exists := pt.assignments[partitionID]
    if !exists {
        return -1, false
    }
    return entry.BackupNode, true
}

// NEW: GetReplicas (returns both)
func (pt *PartitionTable) GetReplicas(partitionID PartitionID) (primary NodeID, backup NodeID, ok bool) {
    pt.mu.RLock()
    defer pt.mu.RUnlock()

    entry, exists := pt.assignments[partitionID]
    if !exists {
        return -1, -1, false
    }
    return entry.PrimaryNode, entry.BackupNode, true
}

// NEW: SetReplicas (atomic update)
func (pt *PartitionTable) SetReplicas(partitionID PartitionID, primary NodeID, backup NodeID) {
    pt.mu.Lock()
    defer pt.mu.Unlock()

    pt.assignments[partitionID] = &PartitionEntry{
        PartitionID:  partitionID,
        PrimaryNode:  primary,
        BackupNode:   backup,
        Version:      pt.version + 1,
    }
    pt.version++
}

// MODIFY: InitializeEvenDistribution to assign backups
func InitializeEvenDistribution(totalNodes int) *PartitionTable {
    pt := NewPartitionTable()
    partitionsPerNode := TOTAL_PARTITIONS / totalNodes

    for pid := PartitionID(0); pid < TOTAL_PARTITIONS; pid++ {
        // Round-robin primary assignment
        primaryNode := NodeID(int(pid) % totalNodes)

        // Backup is next node in ring (wrap around)
        backupNode := NodeID((int(pid) + 1) % totalNodes)

        pt.SetReplicas(pid, primaryNode, backupNode)
    }

    return pt
}
```

**Example Distribution (3 nodes, 16384 partitions):**
- Partition 0: Primary=Node0, Backup=Node1
- Partition 1: Primary=Node1, Backup=Node2
- Partition 2: Primary=Node2, Backup=Node0
- Partition 3: Primary=Node0, Backup=Node1
- ... (repeating pattern)

**3. `/pkg/sharding/encoding.go`**

Update serialization to include backup node:
```go
func (pt *PartitionTable) Serialize() ([]byte, error) {
    pt.mu.RLock()
    defer pt.mu.RUnlock()

    var buf bytes.Buffer

    // Write version
    if err := binary.Write(&buf, binary.LittleEndian, pt.version); err != nil {
        return nil, err
    }

    // Write number of assignments
    numAssignments := uint32(len(pt.assignments))
    if err := binary.Write(&buf, binary.LittleEndian, numAssignments); err != nil {
        return nil, err
    }

    // Write each assignment (now includes backup)
    for pid, entry := range pt.assignments {
        // PartitionID (2 bytes)
        if err := binary.Write(&buf, binary.LittleEndian, uint16(pid)); err != nil {
            return nil, err
        }
        // PrimaryNode (4 bytes)
        if err := binary.Write(&buf, binary.LittleEndian, int32(entry.PrimaryNode)); err != nil {
            return nil, err
        }
        // BackupNode (4 bytes) - NEW
        if err := binary.Write(&buf, binary.LittleEndian, int32(entry.BackupNode)); err != nil {
            return nil, err
        }
    }

    return buf.Bytes(), nil
}

func (pt *PartitionTable) Deserialize(data []byte) error {
    reader := bytes.NewReader(data)

    // Read version
    if err := binary.Read(reader, binary.LittleEndian, &pt.version); err != nil {
        return err
    }

    // Read number of assignments
    var numAssignments uint32
    if err := binary.Read(reader, binary.LittleEndian, &numAssignments); err != nil {
        return err
    }

    pt.assignments = make(map[PartitionID]*PartitionEntry)

    // Read each assignment
    for i := uint32(0); i < numAssignments; i++ {
        var pid uint16
        var primaryNode int32
        var backupNode int32

        if err := binary.Read(reader, binary.LittleEndian, &pid); err != nil {
            return err
        }
        if err := binary.Read(reader, binary.LittleEndian, &primaryNode); err != nil {
            return err
        }
        if err := binary.Read(reader, binary.LittleEndian, &backupNode); err != nil {
            return err
        }

        pt.assignments[PartitionID(pid)] = &PartitionEntry{
            PartitionID:  PartitionID(pid),
            PrimaryNode:  NodeID(primaryNode),
            BackupNode:   NodeID(backupNode),
            Version:      pt.version,
        }
    }

    return nil
}
```

**4. `/pkg/sharding/shard_manager.go`**

Add helper methods:
```go
// NEW: GetPartitionID returns partition for a key
func (sm *ShardManager) GetPartitionID(key string) PartitionID {
    return sm.partitioner.HashKey(key)
}

// NEW: GetReplicas returns primary and backup for a key
func (sm *ShardManager) GetReplicas(partitionID PartitionID) (primary NodeID, backup NodeID, ok bool) {
    return sm.partitionTable.GetReplicas(partitionID)
}

// NEW: GetNodeID returns this node's ID
func (sm *ShardManager) GetNodeID() NodeID {
    return sm.nodeID
}
```

**5. Update Tests**

Modify all existing tests in `/pkg/sharding/partition_table_test.go`:
- Test `GetPrimary()`, `GetBackup()`, `GetReplicas()`
- Test `InitializeEvenDistribution()` assigns backups correctly
- Test serialization round-trip with backup nodes
- Test concurrent access to new methods

Add new tests:
```go
func TestPartitionTableBackupAssignment(t *testing.T) {
    pt := InitializeEvenDistribution(3)

    // Verify every partition has primary and backup
    for pid := PartitionID(0); pid < TOTAL_PARTITIONS; pid++ {
        primary, backup, ok := pt.GetReplicas(pid)
        assert.True(t, ok)
        assert.NotEqual(t, primary, backup)  // Primary and backup must differ
        assert.GreaterOrEqual(t, int(primary), 0)
        assert.GreaterOrEqual(t, int(backup), 0)
    }
}

func TestPartitionTableBalancedDistribution(t *testing.T) {
    pt := InitializeEvenDistribution(3)

    primaryCounts := make(map[NodeID]int)
    backupCounts := make(map[NodeID]int)

    for pid := PartitionID(0); pid < TOTAL_PARTITIONS; pid++ {
        primary, backup, _ := pt.GetReplicas(pid)
        primaryCounts[primary]++
        backupCounts[backup]++
    }

    // Each node should be primary for ~5461 partitions (16384/3)
    for nodeID := NodeID(0); nodeID < 3; nodeID++ {
        assert.InDelta(t, 5461, primaryCounts[nodeID], 10)  // Allow ±10 variance
        assert.InDelta(t, 5461, backupCounts[nodeID], 10)
    }
}
```

#### Validation Checklist

- [ ] `PartitionEntry` struct added with `PrimaryNode` and `BackupNode`
- [ ] `PartitionTable.assignments` changed to `map[PartitionID]*PartitionEntry`
- [ ] `GetPrimary()`, `GetBackup()`, `GetReplicas()` methods implemented
- [ ] `SetReplicas()` method for atomic updates
- [ ] `InitializeEvenDistribution()` assigns backups in round-robin
- [ ] Serialization includes backup node (binary format)
- [ ] Deserialization correctly reconstructs `PartitionEntry`
- [ ] All existing tests updated and passing
- [ ] New tests for backup assignment and distribution
- [ ] Run with `-race` flag: `go test -race ./pkg/sharding/...`
- [ ] Test coverage >90%

**Snapshot Size Impact:**
- Old format: 2 bytes (PartitionID) + 4 bytes (NodeID) = 6 bytes per entry
- New format: 2 bytes (PartitionID) + 4 bytes (Primary) + 4 bytes (Backup) = 10 bytes per entry
- Total for 16,384 partitions: 10 * 16,384 = 163,840 bytes (~160 KB)
- Acceptable overhead for fault tolerance

---

### Stage 2: Replication RPC Protocol

**Goal:** Implement gRPC protocol for synchronous primary → backup replication

**Duration:** 2 days

#### Files to Create

**1. `/pkg/replication/client.go`**

Full implementation:
```go
package replication

import (
    "context"
    "fmt"
    "log/slog"
    "sync"
    "time"

    "github.com/dominikszczepaniak/distributed-cache/pkg/raft/raftpb"
    "github.com/dominikszczepaniak/distributed-cache/pkg/sharding"
)

type Client struct {
    nodeID      sharding.NodeID
    raftClients map[sharding.NodeID]raftpb.RaftClient
    timeout     time.Duration
    mu          sync.RWMutex

    // Circuit breaker
    failures    map[sharding.NodeID]int
    circuitOpen map[sharding.NodeID]bool
}

func NewClient(nodeID sharding.NodeID, timeout time.Duration) *Client {
    return &Client{
        nodeID:      nodeID,
        raftClients: make(map[sharding.NodeID]raftpb.RaftClient),
        timeout:     timeout,
        failures:    make(map[sharding.NodeID]int),
        circuitOpen: make(map[sharding.NodeID]bool),
    }
}

func (c *Client) RegisterPeer(nodeID sharding.NodeID, client raftpb.RaftClient) {
    c.mu.Lock()
    defer c.mu.Unlock()
    c.raftClients[nodeID] = client
    slog.Info(fmt.Sprintf("ReplicationClient: Registered peer node %d", nodeID))
}

func (c *Client) Replicate(ctx context.Context, backupNodeID sharding.NodeID, key, value int) error {
    // Check circuit breaker
    c.mu.RLock()
    if c.circuitOpen[backupNodeID] {
        c.mu.RUnlock()
        return fmt.Errorf("circuit breaker open for node %d", backupNodeID)
    }
    client, exists := c.raftClients[backupNodeID]
    c.mu.RUnlock()

    if !exists {
        return fmt.Errorf("no client registered for backup node %d", backupNodeID)
    }

    // Create replication request
    req := &raftpb.ReplicateRequest{
        Key:       int32(key),
        Value:     int32(value),
        Operation: "PUT",
    }

    // Call with timeout
    ctxWithTimeout, cancel := context.WithTimeout(ctx, c.timeout)
    defer cancel()

    resp, err := client.Replicate(ctxWithTimeout, req)
    if err != nil {
        c.recordFailure(backupNodeID)
        return fmt.Errorf("replication to node %d failed: %w", backupNodeID, err)
    }

    if !resp.Success {
        c.recordFailure(backupNodeID)
        return fmt.Errorf("backup node %d rejected: %s", backupNodeID, resp.Error)
    }

    // Success - reset failure count
    c.resetFailures(backupNodeID)
    return nil
}

func (c *Client) DeleteReplicate(ctx context.Context, backupNodeID sharding.NodeID, key int) error {
    // Check circuit breaker
    c.mu.RLock()
    if c.circuitOpen[backupNodeID] {
        c.mu.RUnlock()
        return fmt.Errorf("circuit breaker open for node %d", backupNodeID)
    }
    client, exists := c.raftClients[backupNodeID]
    c.mu.RUnlock()

    if !exists {
        return fmt.Errorf("no client registered for backup node %d", backupNodeID)
    }

    req := &raftpb.ReplicateRequest{
        Key:       int32(key),
        Operation: "DELETE",
    }

    ctxWithTimeout, cancel := context.WithTimeout(ctx, c.timeout)
    defer cancel()

    resp, err := client.Replicate(ctxWithTimeout, req)
    if err != nil {
        c.recordFailure(backupNodeID)
        return fmt.Errorf("delete replication to node %d failed: %w", backupNodeID, err)
    }

    if !resp.Success {
        c.recordFailure(backupNodeID)
        return fmt.Errorf("backup node %d rejected delete: %s", backupNodeID, resp.Error)
    }

    c.resetFailures(backupNodeID)
    return nil
}

func (c *Client) recordFailure(nodeID sharding.NodeID) {
    c.mu.Lock()
    defer c.mu.Unlock()

    c.failures[nodeID]++
    if c.failures[nodeID] >= 3 {
        c.circuitOpen[nodeID] = true
        slog.Warn(fmt.Sprintf("Circuit breaker opened for node %d after %d failures", nodeID, c.failures[nodeID]))
    }
}

func (c *Client) resetFailures(nodeID sharding.NodeID) {
    c.mu.Lock()
    defer c.mu.Unlock()

    c.failures[nodeID] = 0
    c.circuitOpen[nodeID] = false
}

// ResetCircuitBreaker manually resets circuit breaker for a node
// Called after Raft reconfiguration assigns new backup
func (c *Client) ResetCircuitBreaker(nodeID sharding.NodeID) {
    c.resetFailures(nodeID)
    slog.Info(fmt.Sprintf("Circuit breaker manually reset for node %d", nodeID))
}
```

**2. `/pkg/replication/errors.go`**

```go
package replication

import "fmt"

type ReplicationError struct {
    BackupNodeID int
    Operation    string
    Cause        error
}

func (e *ReplicationError) Error() string {
    return fmt.Sprintf("replication to backup node %d failed (%s): %v", e.BackupNodeID, e.Operation, e.Cause)
}

type CircuitBreakerOpenError struct {
    NodeID int
}

func (e *CircuitBreakerOpenError) Error() string {
    return fmt.Sprintf("circuit breaker open for node %d", e.NodeID)
}
```

**3. `/pkg/replication/client_test.go`**

```go
package replication

import (
    "context"
    "testing"
    "time"

    "github.com/dominikszczepaniak/distributed-cache/pkg/raft/raftpb"
    "github.com/dominikszczepaniak/distributed-cache/pkg/sharding"
    "github.com/stretchr/testify/assert"
    "github.com/stretchr/testify/mock"
)

// Mock gRPC client
type MockRaftClient struct {
    mock.Mock
}

func (m *MockRaftClient) Replicate(ctx context.Context, req *raftpb.ReplicateRequest, opts ...interface{}) (*raftpb.ReplicateResponse, error) {
    args := m.Called(ctx, req)
    return args.Get(0).(*raftpb.ReplicateResponse), args.Error(1)
}

func TestReplicationClient_Success(t *testing.T) {
    client := NewClient(sharding.NodeID(0), 1*time.Second)

    mockClient := new(MockRaftClient)
    mockClient.On("Replicate", mock.Anything, mock.Anything).Return(&raftpb.ReplicateResponse{Success: true}, nil)

    client.RegisterPeer(sharding.NodeID(1), mockClient)

    err := client.Replicate(context.Background(), sharding.NodeID(1), 42, 100)
    assert.NoError(t, err)
}

func TestReplicationClient_CircuitBreaker(t *testing.T) {
    client := NewClient(sharding.NodeID(0), 1*time.Second)

    mockClient := new(MockRaftClient)
    mockClient.On("Replicate", mock.Anything, mock.Anything).Return(&raftpb.ReplicateResponse{Success: false, Error: "timeout"}, fmt.Errorf("timeout"))

    client.RegisterPeer(sharding.NodeID(1), mockClient)

    // Fail 3 times
    for i := 0; i < 3; i++ {
        client.Replicate(context.Background(), sharding.NodeID(1), 42, 100)
    }

    // Circuit should be open
    err := client.Replicate(context.Background(), sharding.NodeID(1), 42, 100)
    assert.Error(t, err)
    assert.Contains(t, err.Error(), "circuit breaker open")
}
```

#### Files to Modify

**1. `/proto/raft.proto`**

Add to the end of the file (before `service Raft`):
```protobuf
// Replication messages (primary → backup)
message ReplicateRequest {
    int32 key = 1;
    int32 value = 2;           // Only for PUT operations
    string operation = 3;      // "PUT" or "DELETE"
    uint64 version = 4;        // Future: for ordering/deduplication
}

message ReplicateResponse {
    bool success = 1;
    string error = 2;          // Error message if success = false
}
```

Add to `service Raft`:
```protobuf
service Raft {
    // ... existing RPCs ...
    rpc Replicate(ReplicateRequest) returns (ReplicateResponse);  // NEW
}
```

**2. Generate protobuf code**

Run:
```bash
cd /Users/dzc/distributed-cache
protoc --go_out=. --go-grpc_out=. proto/raft.proto
```

**3. `/pkg/raft/grpc_server.go`**

Add the Replicate RPC handler:
```go
// Add to GrpcServer struct (if not already present)
type GrpcServer struct {
    raftpb.UnimplementedRaftServer
    raft         *Raft
    shardManager *sharding.ShardManager  // NEW: Need for validation
    nodeID       sharding.NodeID         // NEW: This node's ID
}

// NEW: Replicate RPC handler (backup node receives this)
func (s *GrpcServer) Replicate(ctx context.Context, req *raftpb.ReplicateRequest) (*raftpb.ReplicateResponse, error) {
    key := int(req.Key)
    keyStr := fmt.Sprintf("%d", key)

    // [1] SECURITY CHECK - Verify I am the backup for this partition
    partitionID := s.shardManager.GetPartitionID(keyStr)
    primaryNode, backupNode, ok := s.shardManager.GetReplicas(partitionID)

    if !ok {
        slog.Warn(fmt.Sprintf("REPLICATE: Partition %d not assigned", partitionID))
        return &raftpb.ReplicateResponse{
            Success: false,
            Error:   "partition not assigned",
        }, nil
    }

    if backupNode != s.nodeID {
        slog.Warn(fmt.Sprintf("REPLICATE: Not backup for partition %d (primary=%d, backup=%d, me=%d)",
            partitionID, primaryNode, backupNode, s.nodeID))
        return &raftpb.ReplicateResponse{
            Success: false,
            Error:   "not backup node for this partition",
        }, nil
    }

    // [2] WRITE TO LOCAL STORAGE
    app := s.raft.GetApplication()
    kvStore, ok := app.(*SimpleKVStore)
    if !ok {
        return &raftpb.ReplicateResponse{
            Success: false,
            Error:   "invalid application type",
        }, nil
    }

    switch req.Operation {
    case "PUT":
        value := int(req.Value)
        kvStore.mu.Lock()
        kvStore.data[key] = value
        kvStore.mu.Unlock()

        slog.Info(fmt.Sprintf("REPLICATE PUT key=%d value=%d", key, value))
        return &raftpb.ReplicateResponse{Success: true}, nil

    case "DELETE":
        kvStore.mu.Lock()
        delete(kvStore.data, key)
        kvStore.mu.Unlock()

        slog.Info(fmt.Sprintf("REPLICATE DELETE key=%d", key))
        return &raftpb.ReplicateResponse{Success: true}, nil

    default:
        return &raftpb.ReplicateResponse{
            Success: false,
            Error:   "unknown operation: " + req.Operation,
        }, nil
    }
}
```

**Note:** You may need to modify `GrpcServer` constructor to accept `shardManager` and `nodeID`.

**4. `/cmd/raftnode/main.go`**

Wire up the replication client and pass dependencies to GrpcServer:
```go
func main() {
    // ... existing setup ...

    app := NewSimpleKVStore()
    r := raft.NewRaft(app, cfg)

    // Initialize ShardManager
    partitioner := sharding.NewPartitioner()
    nodeID := sharding.NodeID(cfg.GetRaftID())
    shardManager := sharding.NewShardManager(nodeID, app.partitionTable, partitioner)

    // Register peer addresses
    raftAddrs := cfg.GetRaftAddrs()
    for i, raftAddr := range raftAddrs {
        peerNodeID := sharding.NodeID(i)
        httpAddr := convertRaftAddrToHTTP(raftAddr)
        shardManager.UpdatePeerAddress(peerNodeID, httpAddr)
    }

    // NEW: Initialize ReplicationClient
    replicationClient := replication.NewClient(nodeID, 1*time.Second)

    // NEW: Register Raft clients with ReplicationClient
    // This requires accessing Raft's connection manager
    connMgr := r.GetConnectionManager()  // Need to add this getter to Raft
    for peerNodeID, peerClient := range connMgr.GetAllClients() {
        replicationClient.RegisterPeer(sharding.NodeID(peerNodeID), peerClient)
    }

    // Start API server with replication client
    apiServer := api.NewServer(r, apiAddr, shardManager, replicationClient)  // Modified signature

    // ... rest of main ...
}
```

**Note:** You may need to add `GetConnectionManager()` method to Raft struct, and `GetAllClients()` to ConnectionManager.

#### Validation Checklist

- [ ] `ReplicateRequest` and `ReplicateResponse` messages added to protobuf
- [ ] Protobuf code generated successfully
- [ ] `Replicate` RPC added to gRPC service
- [ ] `replication.Client` package created with circuit breaker
- [ ] `Replicate()` gRPC handler implemented with security checks
- [ ] `GrpcServer` receives `shardManager` and `nodeID` for validation
- [ ] Replication client wired up in `main.go`
- [ ] Unit tests for `replication.Client` (mock gRPC client)
- [ ] Test timeout handling (1s)
- [ ] Test circuit breaker (3 failures → open)
- [ ] Run with `-race` flag
- [ ] Test coverage >80%

---

### Stage 3: Synchronous Write Path Implementation

**Goal:** Modify PUT and DELETE handlers to replicate to backup before confirming to client

**Duration:** 2-3 days

#### Files to Modify

**1. `/pkg/api/server.go`**

Update `Server` struct:
```go
type Server struct {
    raft               *raft.Raft
    listenAddr         string
    httpServer         *http.Server
    retrier            *Retrier
    idempotencyCache   *IdempotencyCache
    leaderCache        *LeaderCache
    shardManager       *sharding.ShardManager
    replicationClient  *replication.Client  // NEW
}

// Modified constructor
func NewServer(r *raft.Raft, listenAddr string, shardManager *sharding.ShardManager, replClient *replication.Client) *Server {
    return &Server{
        raft:              r,
        listenAddr:        listenAddr,
        retrier:           NewRetrier(DefaultRetryConfigs["PUT"]),
        idempotencyCache:  NewIdempotencyCache(5 * time.Minute),
        leaderCache:       NewLeaderCache(1 * time.Second),
        shardManager:      shardManager,
        replicationClient: replClient,  // NEW
    }
}
```

**Modify `handlePut`:**

Insert replication logic BEFORE Raft forwarding:
```go
func (s *Server) handlePut(w http.ResponseWriter, r *http.Request) {
    var req PutRequest
    if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
        http.Error(w, "Invalid request body", http.StatusBadRequest)
        return
    }

    // STEP 1: SHARD VALIDATION - Am I the Primary?
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
            json.NewEncoder(w).Encode(RedirectResponse{
                Error:       "WRONG_NODE",
                Message:     "Not primary for this key, redirect to primary",
                NodeID:      fmt.Sprintf("%d", primaryNode),
                Address:     primaryAddr,
                PartitionID: uint16(partitionID),
            })
            return
        }

        // STEP 2: SYNCHRONOUS REPLICATION TO BACKUP (NEW)
        if backupNode >= 0 && s.replicationClient != nil {
            replicationCtx, replicationCancel := context.WithTimeout(r.Context(), 1*time.Second)
            defer replicationCancel()

            err := s.replicationClient.Replicate(replicationCtx, backupNode, req.Key, req.Value)
            if err != nil {
                slog.Error(fmt.Sprintf("Replication to backup %d failed for key %d: %v", backupNode, req.Key, err))

                // FAIL THE WRITE - Strong consistency guarantee
                http.Error(w, fmt.Sprintf("Replication failed: %v", err), http.StatusServiceUnavailable)
                return
            }

            slog.Info(fmt.Sprintf("Successfully replicated PUT key=%d to backup node %d", req.Key, backupNode))
        }
    }

    // STEP 3: EXISTING LOGIC - Idempotency check and Raft forward
    idempotencyToken := r.Header.Get("Idempotency-Key")
    if idempotencyToken == "" {
        idempotencyToken = s.generateIdempotencyToken(&req, getClientID(r))
    }

    if cachedResp, found := s.idempotencyCache.Get(idempotencyToken); found {
        // ... existing cached response logic ...
    }

    var success bool
    var broadcastErr error

    retryFunc := func(ctx context.Context) error {
        msg := &raftpb.Message{
            Type:             raftpb.Message_PUT,
            Key:              int32(req.Key),
            Value:            wrapperspb.Int32(int32(req.Value)),
            IdempotencyToken: idempotencyToken,
            ClientId:         getClientID(r),
        }

        resp, err := s.raft.Forward(ctx, msg)
        if err != nil {
            broadcastErr = err
            return err
        }

        success = resp.Success
        return nil
    }

    ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
    defer cancel()

    err := s.retrier.ExecuteWithRetry(ctx, retryFunc)

    if err != nil {
        // ... existing error handling ...
        return
    }

    // Cache and respond
    s.idempotencyCache.Set(idempotencyToken, raft.BroadcastResponse{
        Success: success,
        Value:   0,
        Error:   nil,
    })

    resp := PutResponse{
        Success: success,
        Message: "Key-value pair stored and replicated successfully",
    }

    w.Header().Set("Content-Type", "application/json")
    json.NewEncoder(w).Encode(resp)
}
```

**Modify `handleDelete`:**

Similar replication logic:
```go
func (s *Server) handleDelete(w http.ResponseWriter, r *http.Request, key int) {
    // STEP 1: SHARD VALIDATION
    if s.shardManager != nil {
        keyStr := fmt.Sprintf("%d", key)
        partitionID := s.shardManager.GetPartitionID(keyStr)

        primaryNode, backupNode, ok := s.shardManager.GetReplicas(partitionID)
        if !ok {
            http.Error(w, "Partition not assigned", http.StatusInternalServerError)
            return
        }

        if primaryNode != s.shardManager.GetNodeID() {
            // Redirect to primary
            primaryAddr, _ := s.shardManager.GetNodeAddress(primaryNode)
            w.Header().Set("Location", primaryAddr)
            w.WriteHeader(http.StatusTemporaryRedirect)
            json.NewEncoder(w).Encode(RedirectResponse{
                Error:       "WRONG_NODE",
                Message:     "Not primary for this key",
                NodeID:      fmt.Sprintf("%d", primaryNode),
                Address:     primaryAddr,
                PartitionID: uint16(partitionID),
            })
            return
        }

        // STEP 2: SYNCHRONOUS DELETE REPLICATION (NEW)
        if backupNode >= 0 && s.replicationClient != nil {
            replicationCtx, replicationCancel := context.WithTimeout(r.Context(), 1*time.Second)
            defer replicationCancel()

            err := s.replicationClient.DeleteReplicate(replicationCtx, backupNode, key)
            if err != nil {
                slog.Error(fmt.Sprintf("Delete replication to backup %d failed for key %d: %v", backupNode, key, err))
                http.Error(w, fmt.Sprintf("Replication failed: %v", err), http.StatusServiceUnavailable)
                return
            }

            slog.Info(fmt.Sprintf("Successfully replicated DELETE key=%d to backup node %d", key, backupNode))
        }
    }

    // STEP 3: EXISTING LOGIC - Idempotency and Raft forward
    idempotencyToken := r.Header.Get("Idempotency-Key")
    // ... rest of existing handleDelete logic ...
}
```

**2. `/pkg/sharding/shard_manager.go`**

Add helper methods if not already present:
```go
func (sm *ShardManager) GetPartitionID(key string) PartitionID {
    return sm.partitioner.HashKey(key)
}

func (sm *ShardManager) GetReplicas(partitionID PartitionID) (primary NodeID, backup NodeID, ok bool) {
    return sm.partitionTable.GetReplicas(partitionID)
}

func (sm *ShardManager) GetNodeID() NodeID {
    return sm.nodeID
}
```

#### Integration Testing

**Create `/tests/replication_integration_test.go`:**

```go
package tests

import (
    "testing"
    "time"

    "github.com/dominikszczepaniak/distributed-cache/pkg/client"
    "github.com/dominikszczepaniak/distributed-cache/pkg/sharding"
    "github.com/stretchr/testify/assert"
)

func TestReplication_PrimaryBackupWrite(t *testing.T) {
    // Start 3-node cluster
    cluster := NewTestCluster(t, 3)
    defer cluster.Shutdown()

    // Wait for partition table initialization
    time.Sleep(2 * time.Second)

    // Write to primary
    c := client.NewClient([]string{cluster.Nodes[0].HTTPAddr}, client.DefaultConfig())
    err := c.Put(42, 100)
    assert.NoError(t, err)

    // Determine which node is backup for key=42
    partitioner := sharding.NewPartitioner()
    partitionID := partitioner.HashKey("42")

    // Query partition table from leader
    pt := cluster.Leader.App.(*SimpleKVStore).partitionTable
    primary, backup, ok := pt.GetReplicas(partitionID)
    assert.True(t, ok)

    // Verify backup has the data
    backupNode := cluster.Nodes[int(backup)]
    backupValue := backupNode.App.(*SimpleKVStore).GetValue(42)
    assert.Equal(t, 100, backupValue)

    t.Logf("Key 42: Primary=%d, Backup=%d, Value on backup=%d", primary, backup, backupValue)
}

func TestReplication_BackupFailure(t *testing.T) {
    cluster := NewTestCluster(t, 3)
    defer cluster.Shutdown()

    time.Sleep(2 * time.Second)

    // Determine backup for key=42
    partitioner := sharding.NewPartitioner()
    partitionID := partitioner.HashKey("42")
    pt := cluster.Leader.App.(*SimpleKVStore).partitionTable
    _, backup, _ := pt.GetReplicas(partitionID)

    // Kill backup node
    cluster.StopNode(int(backup))

    // Write should fail (503 Service Unavailable)
    c := client.NewClient([]string{cluster.Nodes[0].HTTPAddr}, client.DefaultConfig())
    err := c.Put(42, 100)
    assert.Error(t, err)
    assert.Contains(t, err.Error(), "503")
}
```

#### Validation Checklist

- [ ] `Server` struct has `replicationClient` field
- [ ] `NewServer()` signature updated to accept `replicationClient`
- [ ] `handlePut()` replicates to backup before Raft forward
- [ ] `handleDelete()` replicates to backup before Raft forward
- [ ] Replication timeout is 1 second
- [ ] Failed replication returns 503 Service Unavailable
- [ ] Primary validation enforced (redirect if not primary)
- [ ] Integration test: write to primary → verify backup has data
- [ ] Integration test: backup down → write fails with 503
- [ ] Run with `-race` flag
- [ ] All existing tests still pass

**Write Latency Target:** <30ms for successful writes (measured in integration tests)

---

### Stage 4: Failure Handling and Reconfiguration (Future Enhancement)

**Goal:** Automatically detect backup failures and trigger Raft reconfiguration

**Duration:** 2-3 days

**Status:** DEFERRED to future milestone

This stage would include:
1. Backup failure detection via circuit breaker (already implemented)
2. Trigger Raft `UPDATE_PARTITION_TABLE` to reassign backup
3. Retry write with new backup after reconfiguration
4. Health monitoring and proactive backup checks

**Complexity:** High - involves coordinating Raft consensus with data plane operations

**Recommendation:** Start with Stage 3 (writes fail on backup failure), then implement automatic reconfiguration in a separate milestone after validating Stage 3.

---

## Dependencies & Considerations

### Concurrency Concerns

**1. Partition Table Access:**
- `PartitionTable` uses `sync.RWMutex` for thread-safe reads/writes
- Read operations (GetPrimary, GetBackup) use `RLock()`
- Write operations (SetReplicas) use `Lock()`
- No deadlock risk: all locks are acquired/released within method scope

**2. SimpleKVStore Data Map:**
- Already protected by `sync.RWMutex`
- Replication handler acquires `Lock()` for writes
- No change to existing locking strategy

**3. ReplicationClient:**
- Uses `sync.RWMutex` to protect client registry and circuit breaker state
- No blocking operations while holding locks
- Circuit breaker updates are non-blocking

**4. Goroutine Safety:**
- All handlers are goroutine-safe (each HTTP request is a separate goroutine)
- No shared mutable state without synchronization

### Network Configuration

**Timeout Values:**
- Replication timeout: **1 second** (aggressive failure detection)
- Total write timeout: **15 seconds** (allows for retries)
- Health check interval: **5 seconds** (existing ConnectionManager)

**gRPC Connection Reuse:**
- Reuse existing Raft gRPC connections for replication
- No additional connection overhead

### Error Handling Patterns

**1. Replication Failure:**
```go
if err := s.replicationClient.Replicate(ctx, backupNode, key, value); err != nil {
    // Log error with context
    slog.Error(fmt.Sprintf("Replication failed: partition=%d, backup=%d, error=%v", partitionID, backupNode, err))

    // Return 503 Service Unavailable
    http.Error(w, "Replication failed", http.StatusServiceUnavailable)
    return
}
```

**2. Circuit Breaker Open:**
```go
if c.circuitOpen[backupNodeID] {
    return fmt.Errorf("circuit breaker open for node %d", backupNodeID)
}
```

**3. Not Backup Node (Security Check):**
```go
if backupNode != s.nodeID {
    return &raftpb.ReplicateResponse{
        Success: false,
        Error:   "not backup node for this partition",
    }, nil
}
```

### Partition Hashing Strategy

**Already Implemented:**
- MurmurHash3 via `/pkg/sharding/partitioner.go`
- Deterministic: same key → same partition on all nodes
- 16,384 partitions (power of 2 for efficient modulo)

**Distribution:**
- Primary assignment: `partition % totalNodes`
- Backup assignment: `(partition + 1) % totalNodes` (next node in ring)

**Example (3 nodes):**
- Partition 0: Primary=0, Backup=1
- Partition 1: Primary=1, Backup=2
- Partition 2: Primary=2, Backup=0
- Partition 3: Primary=0, Backup=1

**Load Balancing:**
- Each node is primary for ~5,461 partitions (16,384 / 3)
- Each node is backup for ~5,461 partitions
- Even distribution ensures no hot spots

---

## Testing Strategy

### Unit Tests

**1. Partition Table (`/pkg/sharding/partition_table_test.go`):**
- Test `GetPrimary()`, `GetBackup()`, `GetReplicas()`
- Test `SetReplicas()` atomic updates
- Test `InitializeEvenDistribution()` for balanced assignment
- Test serialization/deserialization with backup nodes
- Test concurrent access (run with `-race`)

**2. Replication Client (`/pkg/replication/client_test.go`):**
- Test successful replication (mock gRPC client)
- Test timeout handling
- Test circuit breaker (3 failures → open)
- Test circuit breaker reset

**3. API Server (`/pkg/api/server_test.go`):**
- Test primary validation redirect
- Test replication failure returns 503
- Test successful write with replication

### Integration Tests

**Location:** `/tests/replication_integration_test.go`

**Scenarios:**

**1. Basic Replication:**
```go
func TestReplication_WriteToBackup(t *testing.T) {
    // Start 3-node cluster
    // Write key=42, value=100 to primary
    // Verify backup has key=42, value=100
    // Verify other nodes don't have key=42
}
```

**2. Backup Failure:**
```go
func TestReplication_BackupDown(t *testing.T) {
    // Start 3-node cluster
    // Kill backup node
    // Write should fail with 503
    // Verify primary does not have the key (write was rejected)
}
```

**3. Read After Write:**
```go
func TestReplication_ReadAfterWrite(t *testing.T) {
    // Write to primary
    // Kill primary
    // Read from backup (should have data)
}
```

**4. Concurrent Writes:**
```go
func TestReplication_ConcurrentWrites(t *testing.T) {
    // Start cluster
    // Write 1000 keys concurrently
    // Verify all keys replicated to backups
    // No race conditions
}
```

### Performance Benchmarks

**Location:** `/tests/replication_benchmark_test.go`

```go
func BenchmarkWrite_WithReplication(b *testing.B) {
    // Setup: 3-node cluster with replication enabled
    // Measure: Latency of PUT with synchronous replication
    // Target: <30ms P99 latency
}

func BenchmarkWrite_WithoutReplication(b *testing.B) {
    // Baseline: Measure write latency without replication
    // Compare: Overhead of replication
    // Target: <50% degradation
}

func BenchmarkReplication_Throughput(b *testing.B) {
    // Measure: Writes per second with replication
    // Target: >1000 writes/sec per node
}
```

### Chaos Tests (Future)

**Location:** `/tests/chaos_replication_test.go`

- Random node kills during writes
- Network partition between primary and backup
- Slow backup (artificial latency)
- Verify no data loss after recovery

---

## Future Enhancements

### 1. Automatic Backup Reconfiguration

**Current:** Writes fail when backup is down (503 error)

**Future:** Automatically reassign backup via Raft:
```go
func (s *Server) reconfigureBackup(partitionID sharding.PartitionID, failedBackup sharding.NodeID) error {
    // 1. Choose new backup from available nodes
    // 2. Create PartitionTableUpdate message
    // 3. Propose to Raft
    // 4. Wait for commit
    // 5. Reset circuit breaker
    // 6. Retry replication
}
```

### 2. Data Synchronization

**Current:** Backup gets data via synchronous replication during writes

**Problem:** If backup crashes and recovers, it's missing data

**Future:** Implement background sync:
- On backup node startup, compare partition table
- For each partition where this node is backup:
  - Request full key-value snapshot from primary
  - Apply snapshot to local storage
- Mark node as "ready" after sync complete

### 3. Read from Backup (Stale Reads)

**Current:** All reads go to primary (strong consistency)

**Future:** Allow reads from backup with staleness flag:
```go
GET /kv/42?allow_stale=true
```
- Client can opt-in to lower latency at cost of potential staleness
- Useful for read-heavy workloads

### 4. Multiple Backups

**Current:** 1 backup per partition (survives 1 node failure)

**Future:** 2 backups per partition (survives 2 node failures):
```go
type PartitionEntry struct {
    PartitionID   PartitionID
    PrimaryNode   NodeID
    BackupNodes   []NodeID  // Array instead of single backup
}
```
- Replication becomes 1-to-N (primary → all backups)
- Quorum acknowledgment (e.g., 1 out of 2 backups)

### 5. Async Replication Mode

**Current:** Synchronous replication (blocks client until backup ACKs)

**Future:** Configurable async mode:
```go
PUT /kv?replication=async
```
- Primary responds immediately, replicates in background
- Higher throughput, lower latency
- Risk: data loss if primary crashes before replication
- Trade-off: availability vs. consistency

---

## Success Metrics

### Functional Requirements

- [ ] Every write replicates to backup before confirming to client
- [ ] Zero data loss on single node failure (verified by tests)
- [ ] Writes fail gracefully when backup is unavailable (503 error)
- [ ] Primary-only writes (no backup) still work (backward compatibility)
- [ ] All operations maintain partition table consistency

### Performance Requirements

- [ ] **Write Latency:** P99 < 30ms with replication (local network)
- [ ] **Overhead:** < 50% degradation vs. no replication
- [ ] **Throughput:** > 1000 writes/sec per node
- [ ] **Replication Timeout:** < 1 second for failure detection

### Quality Requirements

- [ ] **Test Coverage:** > 85% across all modified packages
- [ ] **Race Detector:** All tests pass with `-race` flag
- [ ] **Integration Tests:** 100% of failure scenarios covered
- [ ] **Documentation:** Complete inline comments and README

---

## Future You Instructions

### How to Implement Stage 1

**Day 1: Modify PartitionTable**

1. Open `/pkg/sharding/types.go`
   - Add `PartitionEntry` struct as shown above

2. Open `/pkg/sharding/partition_table.go`
   - Change `assignments` field type to `map[PartitionID]*PartitionEntry`
   - Implement `GetPrimary()`, `GetBackup()`, `GetReplicas()`, `SetReplicas()`
   - Modify `InitializeEvenDistribution()` to assign backups

3. Open `/pkg/sharding/encoding.go`
   - Update `Serialize()` to write backup node (4 extra bytes per entry)
   - Update `Deserialize()` to read backup node

4. Run tests:
   ```bash
   go test -v ./pkg/sharding/partition_table_test.go
   ```

5. Fix failing tests (update assertions to use new methods)

**Day 2: Update Tests**

1. Open `/pkg/sharding/partition_table_test.go`
   - Update all existing tests to use `GetPrimary()` instead of `GetOwner()`
   - Add `TestPartitionTableBackupAssignment()`
   - Add `TestPartitionTableBalancedDistribution()`

2. Run with race detector:
   ```bash
   go test -race -v ./pkg/sharding/...
   ```

3. Verify coverage:
   ```bash
   go test -cover ./pkg/sharding/...
   ```

**Validation:** All tests pass, coverage > 90%

---

### How to Implement Stage 2

**Day 1: Protobuf and gRPC**

1. Open `/proto/raft.proto`
   - Add `ReplicateRequest` and `ReplicateResponse` messages
   - Add `Replicate` RPC to service

2. Generate code:
   ```bash
   cd /Users/dzc/distributed-cache
   protoc --go_out=. --go-grpc_out=. proto/raft.proto
   ```

3. Verify generated files in `/pkg/raft/raftpb/`

**Day 2: Replication Client**

1. Create `/pkg/replication/client.go`
   - Copy full implementation from Stage 2 section above
   - Implement `Replicate()`, `DeleteReplicate()`, circuit breaker

2. Create `/pkg/replication/errors.go`
   - Define error types

3. Create `/pkg/replication/client_test.go`
   - Write unit tests with mock gRPC client

**Day 3: gRPC Handler**

1. Open `/pkg/raft/grpc_server.go`
   - Add `Replicate()` method to `GrpcServer`
   - Add security checks (verify backup role)
   - Write directly to `SimpleKVStore.data` map

2. Open `/cmd/raftnode/main.go`
   - Initialize `ReplicationClient`
   - Register Raft peers with replication client
   - Pass replication client to API server

**Validation:** Unit tests pass, manual test with gRPC client

---

### How to Implement Stage 3

**Day 1: Modify API Server**

1. Open `/pkg/api/server.go`
   - Add `replicationClient` field to `Server` struct
   - Update `NewServer()` signature

2. Modify `handlePut()`:
   - Add replication logic after shard validation
   - Call `replicationClient.Replicate()` before Raft forward
   - Return 503 on failure

3. Modify `handleDelete()`:
   - Add replication logic similar to PUT

**Day 2: Integration Tests**

1. Create `/tests/replication_integration_test.go`
   - Implement `TestReplication_PrimaryBackupWrite()`
   - Implement `TestReplication_BackupFailure()`

2. Run integration tests:
   ```bash
   go test -v ./tests/replication_integration_test.go
   ```

**Day 3: Performance Benchmarks**

1. Create `/tests/replication_benchmark_test.go`
   - Measure write latency with replication
   - Compare with baseline (no replication)
   - Verify < 30ms P99, < 50% overhead

**Validation:** Integration tests pass, performance targets met

---

## Appendix: Serialization Format

### PartitionEntry Serialization (10 bytes per entry)

```
┌─────────────────────────────────────────┐
│ PartitionID (2 bytes, uint16)          │  0-1
├─────────────────────────────────────────┤
│ PrimaryNode (4 bytes, int32)           │  2-5
├─────────────────────────────────────────┤
│ BackupNode (4 bytes, int32)            │  6-9
└─────────────────────────────────────────┘
```

### Full Partition Table Snapshot

```
┌─────────────────────────────────────────┐
│ Version (8 bytes, uint64)              │  Header
├─────────────────────────────────────────┤
│ NumAssignments (4 bytes, uint32)       │  Header
├─────────────────────────────────────────┤
│ PartitionEntry #0 (10 bytes)           │  Entry 0
├─────────────────────────────────────────┤
│ PartitionEntry #1 (10 bytes)           │  Entry 1
├─────────────────────────────────────────┤
│ ... (NumAssignments entries)           │  Entries 2-16383
└─────────────────────────────────────────┘
```

**Total size:** 12 bytes (header) + (10 bytes × 16,384 entries) = **163,852 bytes (~160 KB)**

---

## Conclusion

This plan provides a complete, stage-by-stage implementation guide for adding Primary-Backup replication to the Worker Nodes. The design:

1. **Extends the existing Partition Table** to track backup nodes
2. **Adds synchronous replication** via gRPC between primary and backup
3. **Guarantees strong consistency** by failing writes when backup is unavailable
4. **Integrates cleanly** with the existing Raft control plane architecture

**Key Benefits:**
- Zero data loss on single node failure
- < 30ms write latency (2x faster than full Raft consensus)
- Clean separation: Control Plane (Raft) manages topology, Data Plane (Workers) handle data

**Implementation Complexity:** Medium
**Estimated Timeline:** 5-7 days for Stages 1-3

**Next Steps:**
1. Review this plan with stakeholders
2. Create feature branch: `git checkout -b feature/primary-backup-replication`
3. Start with Stage 1 (Partition Table extension)
4. Progress through stages with continuous testing

---

**Document Version:** 1.0
**Created:** 2025-11-21
**Status:** Ready for Implementation
