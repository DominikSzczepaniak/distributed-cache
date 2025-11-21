# Synchronous Replication Implementation Plan

## Executive Summary

This plan outlines the implementation of **synchronous replication** for the distributed cache to achieve **strong consistency** and **fault tolerance**. The system will use a hybrid architecture:

- **Raft Control Plane**: Manages cluster topology (who owns what)
- **Synchronous Replication Data Plane**: Ensures writes survive node failures

**Key Goal**: Zero data loss on single node failure with <30ms write latency.

---

## Current State Analysis

### Existing Components (From Stages 1-4)

✅ **Sharding Package** (`/pkg/sharding/`):
- Partitioner with MurmurHash3 (16,384 partitions)
- PartitionTable with Raft integration
- ShardManager for request routing
- 92.4% test coverage

✅ **Raft Consensus** (`/pkg/raft/`):
- Full Raft implementation with leader election
- ConnectionManager with health checking
- Snapshot support with partition table

✅ **API Layer** (`/pkg/api/`):
- PUT/GET/DELETE handlers with shard validation
- HTTP 307 redirects for wrong-node requests
- Idempotency and retry logic

✅ **Networking**:
- gRPC service with Forward/ForwardGet RPCs
- protobuf message definitions
- Connection pooling and health monitoring

### Current Partition Table Structure

```go
// Current: Single node per partition
type PartitionTable struct {
    mu          sync.RWMutex
    assignments map[PartitionID]NodeID  // Only primary
    version     uint64
}
```

### Current Write Path Flow

```
Client → HTTP API → ShardManager.ValidateKey()
                 ↓
         Is this node primary? → Yes → Raft.Forward()
                                      ↓
                                  Raft replicates to ALL nodes
                                      ↓
                                  All nodes store data
                                      ↓
                                  Return success
```

**Problem**: Every write goes through Raft consensus (expensive, doesn't scale).

### Target Write Path Flow

```
Client → HTTP API → ShardManager.ValidateKey()
                 ↓
         Is this node primary? → Yes → 1. Write to local memory
                                       2. Send REPLICATE RPC to backup
                                       3. Wait for ACK from backup
                                       4. Return success to client
                                      ↓
                         Backup down? → Request Raft reconfiguration
```

**Benefit**: Only 1 synchronous network call (primary→backup), not N-way Raft consensus.

---

## Architecture Design

### New Partition Table Structure

```go
// New: Primary + Backup per partition
type PartitionEntry struct {
    PartitionID  PartitionID
    PrimaryNode  NodeID
    BackupNode   NodeID
    Version      uint64
}

type PartitionTable struct {
    mu          sync.RWMutex
    assignments map[PartitionID]*PartitionEntry  // Changed
    version     uint64
}

// New methods
func (pt *PartitionTable) GetPrimary(partitionID PartitionID) (NodeID, bool)
func (pt *PartitionTable) GetBackup(partitionID PartitionID) (NodeID, bool)
func (pt *PartitionTable) GetReplicas(partitionID PartitionID) (primary NodeID, backup NodeID, ok bool)
```

### Synchronous Replication Flow

```
┌─────────┐                     ┌─────────┐                     ┌─────────┐
│ Client  │                     │ Primary │                     │ Backup  │
└────┬────┘                     └────┬────┘                     └────┬────┘
     │                               │                               │
     │ PUT key=user:123, val=Alice   │                               │
     ├──────────────────────────────>│                               │
     │                               │ 1. Lock key (optional)        │
     │                               │ 2. Write to local memory      │
     │                               │                               │
     │                               │ REPLICATE(key, value)         │
     │                               ├──────────────────────────────>│
     │                               │                               │ 3. Store in memory
     │                               │                               │ 4. Send ACK
     │                               │<──────────────────────────────┤
     │                               │                               │
     │                               │ 5. Unlock key                 │
     │ 200 OK                        │                               │
     │<──────────────────────────────┤                               │
     │                               │                               │
```

### Failure Scenario: Backup Down

```
┌─────────┐                     ┌─────────┐                     ┌─────────┐
│ Client  │                     │ Primary │                     │ Raft    │
└────┬────┘                     └────┬────┘                     └────┬────┘
     │                               │                               │
     │ PUT key=user:123              │                               │
     ├──────────────────────────────>│                               │
     │                               │ 1. Write to local memory      │
     │                               │ 2. REPLICATE to backup        │
     │                               │    (timeout after 1s)         │
     │                               │                               │
     │                               │ 3. Detect backup failure      │
     │                               │                               │
     │                               │ UPDATE_PARTITION_TABLE        │
     │                               │ (reassign backup to new node) │
     │                               ├──────────────────────────────>│
     │                               │                               │
     │                               │ Replicated & Committed        │
     │                               │<──────────────────────────────┤
     │                               │                               │
     │                               │ 4. Retry replication          │
     │ 200 OK (or 503 if timeout)    │    to new backup              │
     │<──────────────────────────────┤                               │
     │                               │                               │
```

---

## Implementation Stages

### Stage 1: Partition Table Extension

**Objective**: Extend partition table to track primary + backup nodes

#### Tasks

- [ ] Modify `PartitionTable` struct to use `map[PartitionID]*PartitionEntry`
- [ ] Add `GetPrimary()`, `GetBackup()`, `GetReplicas()` methods
- [ ] Update `Serialize/Deserialize` in `/pkg/sharding/encoding.go`
- [ ] Modify `InitializeEvenDistribution` to assign backups
- [ ] Update `ShardManager` to use `GetPrimary()` instead of `GetOwner()`
- [ ] Update all tests to use new structure
- [ ] Write new tests for backup assignment logic

#### Files to Modify

- `/pkg/sharding/partition_table.go` - Core struct changes
- `/pkg/sharding/encoding.go` - Serialization updates
- `/pkg/sharding/shard_manager.go` - Use GetPrimary() instead of GetOwner()
- `/pkg/sharding/partition_table_test.go` - Update tests
- `/pkg/sharding/encoding_test.go` - Test new serialization
- `/cmd/raftnode/main.go` - Update initialization

#### Acceptance Criteria

- ✅ PartitionTable stores both primary and backup for each partition
- ✅ Serialization round-trip works correctly (snapshot survives restart)
- ✅ `GetPrimary()` and `GetBackup()` return correct nodes
- ✅ Initialization assigns backups evenly across nodes
- ✅ All existing tests still pass
- ✅ New tests cover backup assignment logic
- ✅ Test coverage >90%

#### Success Definition

- Partition table survives Raft snapshots with primary+backup info
- Each of 1024 partitions has exactly 1 primary and 1 backup
- Distribution is balanced: each node is primary for ~341 partitions (1024/3)
- Distribution is balanced: each node is backup for ~341 partitions

**Estimated Effort**: 2-3 days

---

### Stage 2: Replication RPC Protocol

**Objective**: Implement RPC protocol for synchronous replication between primary and backup

#### Tasks

- [ ] Add `ReplicateRequest` and `ReplicateResponse` messages to `proto/raft.proto`
- [ ] Add `Replicate` RPC method to Raft service
- [ ] Generate protobuf code: `make proto` or `protoc --go_out=. proto/raft.proto`
- [ ] Create `/pkg/replication/` package
- [ ] Implement `ReplicationClient` wrapper for sending replications
- [ ] Implement backup node handler (receives and stores replicated data)
- [ ] Add timeout configuration (default 1s for replication)
- [ ] Write unit tests for RPC handlers

#### New Protobuf Messages

```protobuf
// Add to proto/raft.proto

message ReplicateRequest {
    int32 key = 1;
    int32 value = 2;
    string operation = 3;  // "PUT" or "DELETE"
    uint64 version = 4;    // For ordering/deduplication
}

message ReplicateResponse {
    bool success = 1;
    string error = 2;
}

service Raft {
    // ... existing methods ...
    rpc Replicate(ReplicateRequest) returns (ReplicateResponse);
}
```

#### New Replication Client

```go
// /pkg/replication/client.go

package replication

import (
    "context"
    "time"
    "github.com/dominikszczepaniak/distributed-cache/pkg/raft/raftpb"
)

type Client struct {
    timeout time.Duration
    peers   []raftpb.RaftClient  // Reuse existing gRPC clients
}

func NewClient(peers []raftpb.RaftClient, timeout time.Duration) *Client {
    return &Client{
        timeout: timeout,
        peers:   peers,
    }
}

func (c *Client) Replicate(ctx context.Context, backupNodeID int, key, value int) error {
    ctx, cancel := context.WithTimeout(ctx, c.timeout)
    defer cancel()

    req := &raftpb.ReplicateRequest{
        Key:       int32(key),
        Value:     int32(value),
        Operation: "PUT",
    }

    resp, err := c.peers[backupNodeID].Replicate(ctx, req)
    if err != nil {
        return fmt.Errorf("replication failed: %w", err)
    }

    if !resp.Success {
        return fmt.Errorf("backup rejected: %s", resp.Error)
    }

    return nil
}
```

#### Backup Node Handler

```go
// Integrate into cmd/raftnode/main.go SimpleKVStore

func (s *SimpleKVStore) HandleReplicate(req *raftpb.ReplicateRequest) (*raftpb.ReplicateResponse, error) {
    s.mu.Lock()
    defer s.mu.Unlock()

    switch req.Operation {
    case "PUT":
        s.data[int(req.Key)] = int(req.Value)
        slog.Info(fmt.Sprintf("REPLICATE PUT key=%d value=%d", req.Key, req.Value))
        return &raftpb.ReplicateResponse{Success: true}, nil

    case "DELETE":
        delete(s.data, int(req.Key))
        slog.Info(fmt.Sprintf("REPLICATE DELETE key=%d", req.Key))
        return &raftpb.ReplicateResponse{Success: true}, nil

    default:
        return &raftpb.ReplicateResponse{
            Success: false,
            Error:   "unknown operation",
        }, nil
    }
}
```

#### Files to Create/Modify

**New Files**:
- `/pkg/replication/client.go` - Replication client
- `/pkg/replication/errors.go` - Replication-specific errors
- `/pkg/replication/client_test.go` - Unit tests

**Modified Files**:
- `/proto/raft.proto` - Add Replicate RPC
- `/pkg/raft/grpc_server.go` - Implement Replicate RPC handler
- `/cmd/raftnode/main.go` - Add HandleReplicate to SimpleKVStore

#### Acceptance Criteria

- ✅ Protobuf messages defined with all required fields
- ✅ gRPC `Replicate` service method implemented
- ✅ Backup node receives and stores replicated data
- ✅ ACK is sent back to primary
- ✅ Timeout handling works correctly (1s default)
- ✅ Network errors are detected and returned
- ✅ Test coverage >90%

#### Success Definition

- Primary can send replication request to backup
- Backup acknowledges within timeout (1s)
- Network errors return proper error codes
- <10ms replication latency in local network environment
- Replication requests are idempotent (duplicate requests safe)

**Estimated Effort**: 2-3 days

---

### Stage 3: Synchronous Write Path Implementation

**Objective**: Modify `handlePut` to implement synchronous replication before returning success

#### Tasks

- [ ] Create `ReplicationClient` instance in `Server` struct
- [ ] Modify `/pkg/api/server.go` `handlePut` to add replication logic
- [ ] Add pre-replication: write to local memory first
- [ ] Add replication call to backup node
- [ ] Add synchronous wait for ACK with timeout
- [ ] Add rollback logic if replication fails (optional - or just fail the write)
- [ ] Add key-level locking for atomicity (optional)
- [ ] Modify `handleDelete` similarly
- [ ] Write integration tests for replication flow

#### Modified handlePut Logic

```go
func (s *Server) handlePut(w http.ResponseWriter, r *http.Request) {
    var req PutRequest
    if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
        http.Error(w, "Invalid request body", http.StatusBadRequest)
        return
    }

    // STEP 1: SHARD VALIDATION (existing)
    if s.shardManager != nil {
        keyStr := fmt.Sprintf("%d", req.Key)
        if err := s.shardManager.ValidateKey(keyStr); err != nil {
            // Handle redirect (existing code)
            return
        }
    }

    // STEP 2: WRITE TO LOCAL MEMORY (PRIMARY)
    // Note: We write BEFORE replication, but this is in-memory only
    // If replication fails, we could rollback or just fail the request

    // For now: simple approach - write locally after successful replication

    // STEP 3: SYNCHRONOUS REPLICATION TO BACKUP
    partitionID := s.shardManager.GetPartitionID(fmt.Sprintf("%d", req.Key))
    backupNodeID, hasBackup := s.shardManager.GetBackupNode(partitionID)

    if hasBackup {
        ctx, cancel := context.WithTimeout(r.Context(), 1*time.Second)
        defer cancel()

        err := s.replicationClient.Replicate(ctx, backupNodeID, req.Key, req.Value)
        if err != nil {
            // BACKUP FAILURE DETECTED
            slog.Error(fmt.Sprintf("Replication to backup %d failed: %v", backupNodeID, err))

            // Option A: Fail the write immediately
            http.Error(w, "Replication failed", http.StatusServiceUnavailable)
            return

            // Option B: Trigger reconfiguration and retry (Stage 4)
        }
    }

    // STEP 4: WRITE TO LOCAL RAFT (for consensus on this operation)
    // This ensures leader knows about the write and can coordinate
    msg := &raftpb.Message{
        Type:  raftpb.Message_PUT,
        Key:   int32(req.Key),
        Value: wrapperspb.Int32(int32(req.Value)),
        // ... idempotency fields ...
    }

    resp, err := s.raft.Forward(ctx, msg)
    if err != nil {
        http.Error(w, err.Error(), http.StatusInternalServerError)
        return
    }

    // STEP 5: RETURN SUCCESS
    // Only return OK after BOTH: replication to backup AND local write succeeded
    w.Header().Set("Content-Type", "application/json")
    json.NewEncoder(w).Encode(PutResponse{
        Success: resp.Success,
        Message: "Key-value pair stored and replicated successfully",
    })
}
```

#### Files to Modify

- `/pkg/api/server.go` - Modify `handlePut` and `handleDelete`
- `/pkg/api/types.go` - Add replication-related response fields (if needed)
- `/pkg/sharding/shard_manager.go` - Add `GetBackupNode()` and `GetPartitionID()` helpers
- `/cmd/raftnode/main.go` - Wire up ReplicationClient

#### Acceptance Criteria

- ✅ Primary writes locally only after backup ACKs (or: writes locally, then replicates, then confirms)
- ✅ Replication request sent to correct backup node
- ✅ Write returns success ONLY after backup ACK received
- ✅ Failed replication returns 503 error to client
- ✅ Timeout on replication returns 503 (or triggers reconfiguration)
- ✅ No data inconsistency between primary and backup (verified by tests)
- ✅ Test coverage >85%

#### Success Definition

- 100% of successful writes are replicated to backup before client receives OK
- Zero data loss on single node failure (verified by integration tests)
- Client never sees success for unreplicated writes
- <30ms write latency with replication (in local network)
- Writes fail gracefully when backup is unavailable

**Estimated Effort**: 3-4 days

---

### Stage 4: Failure Handling and Reconfiguration

**Objective**: Handle backup node failures gracefully with automatic reconfiguration

#### Tasks

- [ ] Implement backup timeout detection (already in Stage 3)
- [ ] Create reconfiguration request function
- [ ] Trigger Raft `UPDATE_PARTITION_TABLE` when backup fails
- [ ] Implement backup reassignment logic (choose new backup from available nodes)
- [ ] Add circuit breaker pattern for known-dead backups
- [ ] Add health monitoring for backup nodes
- [ ] Write failure scenario tests (kill backup, verify system recovers)

#### Backup Failure Detection

```go
// In handlePut, when replication fails:

if err := s.replicationClient.Replicate(ctx, backupNodeID, req.Key, req.Value); err != nil {
    slog.Error(fmt.Sprintf("Backup node %d failed: %v", backupNodeID, err))

    // Trigger reconfiguration
    if err := s.requestBackupReconfiguration(partitionID, backupNodeID); err != nil {
        slog.Error(fmt.Sprintf("Reconfiguration request failed: %v", err))
    }

    // Return error to client (write blocked until new backup assigned)
    http.Error(w, "Backup node unavailable, reconfiguration in progress", http.StatusServiceUnavailable)
    return
}
```

#### Reconfiguration Logic

```go
func (s *Server) requestBackupReconfiguration(partitionID sharding.PartitionID, failedBackupID sharding.NodeID) error {
    // 1. Choose new backup node (excluding primary and failed backup)
    availableNodes := s.getAvailableNodes()
    primaryNode, _ := s.shardManager.GetPrimaryNode(partitionID)

    var newBackup sharding.NodeID
    for _, nodeID := range availableNodes {
        if nodeID != primaryNode && nodeID != failedBackupID {
            newBackup = nodeID
            break
        }
    }

    if newBackup == 0 {
        return fmt.Errorf("no available nodes for backup reassignment")
    }

    // 2. Create partition table update
    update := &raftpb.PartitionTableUpdate{
        Assignments: []&raftpb.PartitionAssignment{
            {
                PartitionId: uint32(partitionID),
                PrimaryNode: int32(primaryNode),
                BackupNode:  int32(newBackup),  // NEW BACKUP
            },
        },
        Version: s.shardManager.GetPartitionTableVersion() + 1,
    }

    // 3. Propose to Raft
    msg := &raftpb.Message{
        Type:            raftpb.Message_UPDATE_PARTITION_TABLE,
        PartitionUpdate: update,
    }

    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()

    _, err := s.raft.Forward(ctx, msg)
    return err
}
```

#### Circuit Breaker for Dead Backups

```go
// In ReplicationClient

type Client struct {
    timeout        time.Duration
    peers          []raftpb.RaftClient
    failureCount   map[int]int      // Track consecutive failures
    circuitOpen    map[int]bool     // Circuit breaker state
}

func (c *Client) Replicate(ctx context.Context, backupNodeID int, key, value int) error {
    // Check circuit breaker
    if c.circuitOpen[backupNodeID] {
        return fmt.Errorf("circuit breaker open for node %d", backupNodeID)
    }

    // ... existing replication logic ...

    if err != nil {
        c.failureCount[backupNodeID]++
        if c.failureCount[backupNodeID] >= 3 {
            c.circuitOpen[backupNodeID] = true
            slog.Warn(fmt.Sprintf("Circuit breaker opened for node %d", backupNodeID))
        }
        return err
    }

    // Reset on success
    c.failureCount[backupNodeID] = 0
    return nil
}
```

#### Files to Create/Modify

**New Files**:
- `/pkg/replication/circuit_breaker.go` - Circuit breaker logic
- `/pkg/replication/reconfiguration.go` - Reconfiguration helpers
- `/tests/failure_scenarios_test.go` - Failure tests

**Modified Files**:
- `/pkg/api/server.go` - Add reconfiguration trigger
- `/pkg/replication/client.go` - Add circuit breaker
- `/cmd/raftnode/main.go` - Wire up health monitoring

#### Acceptance Criteria

- ✅ Backup timeout detected within 1-2s
- ✅ Raft reconfiguration triggered automatically on backup failure
- ✅ Writes block until new backup assigned (or return 503)
- ✅ System remains available with N-1 nodes
- ✅ Circuit breaker prevents repeated calls to dead backup
- ✅ Test coverage includes all failure modes:
  - Backup crashes during write
  - Backup is slow (timeout)
  - Backup network partition
  - Multiple backup failures

#### Success Definition

- System detects dead backup within 2s
- Reconfiguration completes within 5s
- Zero data loss during failover
- Writes resume automatically after reconfiguration
- System handles cascading failures (multiple backups down)

**Estimated Effort**: 3-4 days

---

### Stage 5: Read Path Validation

**Objective**: Ensure reads only from primary nodes for linearizability

#### Tasks

- [ ] Modify `handleGet` to validate primary ownership
- [ ] Return 307 redirect if client contacts backup
- [ ] Add consistency validation (read-after-write)
- [ ] Write tests for read path validation

#### Modified handleGet

```go
func (s *Server) handleGet(w http.ResponseWriter, r *http.Request, key int) {
    // VALIDATE PRIMARY OWNERSHIP
    keyStr := fmt.Sprintf("%d", key)
    partitionID := s.shardManager.GetPartitionID(keyStr)
    primaryNode, _ := s.shardManager.GetPrimaryNode(partitionID)

    if primaryNode != s.shardManager.GetNodeID() {
        // This node is BACKUP or WRONG NODE - redirect to primary
        primaryAddr := s.shardManager.GetNodeAddress(primaryNode)

        w.Header().Set("Location", primaryAddr)
        w.Header().Set("Content-Type", "application/json")
        w.WriteHeader(http.StatusTemporaryRedirect)
        json.NewEncoder(w).Encode(RedirectResponse{
            Error:   "NOT_PRIMARY",
            Message: "This node is backup, redirect to primary",
            NodeID:  fmt.Sprintf("%d", primaryNode),
            Address: primaryAddr,
        })
        return
    }

    // ... existing GET logic (read from local memory) ...
}
```

#### Files to Modify

- `/pkg/api/server.go` - Modify `handleGet`
- `/pkg/sharding/shard_manager.go` - Add `GetPrimaryNode()` helper

#### Acceptance Criteria

- ✅ GET requests to backup nodes return 307 redirect to primary
- ✅ GET requests to primary nodes succeed and read from local memory
- ✅ Read-after-write consistency guaranteed (write to primary, read from primary)
- ✅ Redirects contain correct primary address
- ✅ Test coverage >90%

#### Success Definition

- 100% of reads go to primary node (enforced by redirect)
- Read-after-write consistency within 0ms (same node, no replication lag)
- Proper redirects for misrouted reads
- Stale reads from backup are prevented

**Estimated Effort**: 1-2 days

---

### Stage 6: Testing and Validation

**Objective**: Comprehensive testing and performance validation

#### Tasks

- [ ] Write unit tests for all replication components
- [ ] Integration tests with node failures:
  - Kill primary, verify backup becomes new primary (requires leader election logic)
  - Kill backup, verify reconfiguration
  - Kill both, verify data loss detection
- [ ] Consistency tests:
  - Write to primary, kill primary, read from new primary
  - Verify primary and backup have identical data
- [ ] Performance benchmarks:
  - Measure replication overhead
  - Compare with/without replication latency
  - Load testing with concurrent writes
- [ ] Failover testing:
  - Random node kills (chaos testing)
  - Network partition scenarios
- [ ] Documentation updates:
  - Usage guide for replication
  - Operational procedures
  - Troubleshooting guide

#### Test Scenarios

**Consistency Test**:
```go
func TestStrongConsistency_PrimaryFailover(t *testing.T) {
    // 1. Start 3-node cluster
    // 2. Write key=1, value=100 to primary (node 0)
    // 3. Verify backup (node 1) has key=1, value=100
    // 4. Kill primary (node 0)
    // 5. Promote backup to new primary via Raft reconfiguration
    // 6. Read key=1 from new primary
    // 7. Verify value=100 (no data loss)
}
```

**Performance Benchmark**:
```go
func BenchmarkWriteWithReplication(b *testing.B) {
    // Setup: 3-node cluster with replication
    // Measure: Latency of PUT with synchronous replication
    // Compare: vs PUT without replication (baseline)
    // Target: <30ms write latency, <50% overhead
}
```

**Chaos Test**:
```go
func TestChaos_RandomNodeKills(t *testing.T) {
    // 1. Start cluster, write 1000 keys
    // 2. Randomly kill nodes (20% chance per second)
    // 3. Continue writing keys
    // 4. After 60s, stop kills
    // 5. Verify all written keys are still accessible
    // 6. Verify no data loss
}
```

#### Files to Create

- `/tests/replication_integration_test.go` - Integration tests
- `/tests/consistency_test.go` - Consistency validation
- `/tests/replication_benchmark_test.go` - Performance tests
- `/tests/chaos_test.go` - Chaos engineering tests
- `/claudedocs/replication-usage-guide.md` - User documentation
- `/claudedocs/replication-operations.md` - Operational guide

#### Acceptance Criteria

- ✅ >90% test coverage overall
- ✅ Zero data loss in all failure scenarios
- ✅ Replication overhead <50% vs no replication
- ✅ System survives 1 node failure (out of 3)
- ✅ All tests pass with `-race` flag
- ✅ Performance benchmarks documented
- ✅ Operational procedures documented

#### Success Definition

- Strong consistency validated (no stale reads, no lost writes)
- <30ms write latency with replication (local network)
- System recovers from failures automatically within 5s
- Documentation complete and clear
- Production-ready quality

**Estimated Effort**: 3-4 days

---

## Total Implementation Timeline

| Stage | Estimated Effort | Dependencies |
|-------|------------------|--------------|
| Stage 1: Partition Table Extension | 2-3 days | None |
| Stage 2: Replication RPC Protocol | 2-3 days | Stage 1 |
| Stage 3: Synchronous Write Path | 3-4 days | Stage 1, Stage 2 |
| Stage 4: Failure Handling | 3-4 days | Stage 3 |
| Stage 5: Read Path Validation | 1-2 days | Stage 1 |
| Stage 6: Testing & Validation | 3-4 days | All stages |

**Total: 14-20 days** (approximately 3-4 weeks)

---

## File Modifications Required

### New Files (8)

1. `/pkg/replication/client.go` - Replication client
2. `/pkg/replication/errors.go` - Error types
3. `/pkg/replication/circuit_breaker.go` - Circuit breaker
4. `/pkg/replication/reconfiguration.go` - Reconfiguration helpers
5. `/pkg/replication/client_test.go` - Unit tests
6. `/tests/replication_integration_test.go` - Integration tests
7. `/tests/consistency_test.go` - Consistency tests
8. `/tests/chaos_test.go` - Chaos tests

### Modified Files (7)

1. `/pkg/sharding/partition_table.go` - Add primary+backup structure
2. `/pkg/sharding/encoding.go` - Update serialization
3. `/pkg/sharding/shard_manager.go` - Add GetPrimary/GetBackup helpers
4. `/proto/raft.proto` - Add Replicate RPC
5. `/pkg/api/server.go` - Modify PUT/GET handlers
6. `/pkg/raft/grpc_server.go` - Add Replicate RPC handler
7. `/cmd/raftnode/main.go` - Wire up replication, add HandleReplicate

---

## Risk Assessment

### High Risk Areas

1. **Replication Deadlocks**: Primary waits for backup, backup waits for lock
   - **Mitigation**: Use timeouts, deadlock detection, careful lock ordering

2. **Backup Failure Detection Too Slow**: Data loss window
   - **Mitigation**: Aggressive timeouts (1s), health monitoring, circuit breakers

3. **Data Inconsistency During Reconfiguration**: Split-brain scenarios
   - **Mitigation**: Raft consensus for topology changes, version numbers, CAS operations

4. **Performance Degradation >50%**: Unacceptable latency
   - **Mitigation**: Async replication option, batching, pipelining

### Medium Risk Areas

1. **Complex Rollback Logic**: Hard to implement correctly
   - **Mitigation**: Keep it simple - fail the write if replication fails

2. **Network Partition Handling**: Split-brain with primary/backup
   - **Mitigation**: Rely on Raft for partition detection and leader election

3. **Memory Usage**: Duplicate data on primary and backup
   - **Mitigation**: Expected and acceptable for fault tolerance

---

## Success Metrics

### Consistency Metrics
- ✅ **Zero data loss** on single node failure (verified by tests)
- ✅ **Linearizability**: All reads see latest write
- ✅ **No stale reads**: Reads always from primary

### Performance Metrics
- ✅ **<30ms write latency** with replication (P99)
- ✅ **<50% overhead** vs no replication
- ✅ **>1000 writes/sec** per node

### Availability Metrics
- ✅ **System operational** with N-1 nodes (2 out of 3)
- ✅ **Automatic recovery** within 5s after failure
- ✅ **Zero manual intervention** for common failures

### Quality Metrics
- ✅ **>90% test coverage** across all components
- ✅ **Zero race conditions** (all tests pass with `-race`)
- ✅ **Complete documentation** (usage + operations)

---

## Open Questions (User Input Required)

### Question 1: Write Behavior on Backup Failure

**What should happen if backup is down during a write?**

**Option A**: **Block the write** until backup is reassigned (strong consistency)
- Pros: Guarantees replication, no data loss
- Cons: Writes fail during reconfiguration (~5s)

**Option B**: **Allow write to primary only** (degraded mode)
- Pros: Writes continue, high availability
- Cons: Data loss if primary fails before new backup assigned

**Option C**: **Configurable** via parameter
- Pros: Flexibility
- Cons: Complexity

**Recommendation**: Option A (block and fail with 503) - prioritize consistency.

---

### Question 2: Replication Timeout

**What is acceptable replication timeout?**

- **1 second** (aggressive, fast failure detection)
- **2 seconds** (balanced)
- **5 seconds** (conservative)

**Recommendation**: 1 second for local network, configurable via environment variable.

---

### Question 3: Number of Backups Per Partition

**How many backups per partition?**

- **1 backup** (as specified, survives 1 node failure)
- **2 backups** (survives 2 node failures, more overhead)

**Recommendation**: 1 backup as specified, can extend to 2 backups in future.

---

### Question 4: Reads from Backup

**Should reads be allowed from backup nodes (stale reads)?**

**Option A**: **Primary only** (linearizability, as specified)
- Pros: Strong consistency
- Cons: Primary is bottleneck for reads

**Option B**: **Primary or backup with staleness flag** (eventual consistency)
- Pros: Load balancing, higher read throughput
- Cons: Stale reads possible

**Recommendation**: Option A (primary only) for strong consistency, can add stale reads later.

---

## Appendix: Architecture Diagrams

### Data Flow: Successful Write

```
Client                  Primary Node             Backup Node            Raft Cluster
  |                          |                         |                      |
  |--PUT key=1, val=100----->|                         |                      |
  |                          |                         |                      |
  |                          |--Lock key=1 (optional)  |                      |
  |                          |                         |                      |
  |                          |--Write val=100 to RAM   |                      |
  |                          |                         |                      |
  |                          |--REPLICATE(1, 100)----->|                      |
  |                          |                         |--Write val=100       |
  |                          |                         |   to RAM             |
  |                          |<-------ACK--------------|                      |
  |                          |                         |                      |
  |                          |--Unlock key=1           |                      |
  |                          |                         |                      |
  |<------200 OK-------------|                         |                      |
  |                          |                         |                      |
```

### Data Flow: Backup Failure

```
Client                  Primary Node             Backup Node            Raft Cluster
  |                          |                         |  (DOWN)              |
  |--PUT key=1, val=100----->|                         |                      |
  |                          |                         |                      |
  |                          |--Write val=100 to RAM   |                      |
  |                          |                         |                      |
  |                          |--REPLICATE(1, 100)----->X  (timeout after 1s)  |
  |                          |                         |                      |
  |                          |--Detect failure         |                      |
  |                          |                         |                      |
  |                          |--UPDATE_PARTITION_TABLE----------------------->|
  |                          |  (reassign backup)      |                      |
  |                          |                         |                      |
  |                          |<-----Committed----------------------------------|
  |                          |                         |                      |
  |<------503 Unavailable----|                         |                      |
  | (or retry with new       |                         |                      |
  |  backup if implemented)  |                         |                      |
```

---

## Next Steps

1. **User Review**: Review this plan and answer open questions
2. **Stage 1 Start**: Begin implementation with partition table extension
3. **Incremental Delivery**: Complete each stage with testing before moving to next
4. **Continuous Integration**: Ensure all existing tests pass at each stage
5. **Documentation**: Update docs as each stage completes

**Plan Status**: ✅ READY FOR REVIEW

**Created**: 2025-11-21
**Author**: golang-developer agent
**Version**: 1.0
