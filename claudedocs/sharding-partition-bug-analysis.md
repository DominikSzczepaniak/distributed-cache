# Sharding Partition Bug Analysis

## Problem Statement

**Error**: `Shard validation error: no owner assigned for partition 11371`

**Root Cause**: The partition table is not being initialized when nodes start up, resulting in an empty partition table that cannot route any keys.

## Technical Analysis

### Current Architecture

1. **Partition Table Initialization** (`partition_table.go:235-266`)
   - `InitializeEvenDistribution()` creates partition assignments across nodes
   - Distributes 16,384 partitions evenly using round-robin
   - Assigns both primary and backup nodes for replication

2. **Shard Manager Validation** (`shard_manager.go:38-45`)
   - `ValidateKey()` checks partition ownership before processing requests
   - Returns error if partition has no owner assigned
   - This is where the 500 error originates

3. **Node Startup** (`main.go:200-209`)
   - Creates empty `PartitionTable` via `NewPartitionTable()`
   - Passes empty partition table to `ShardManager`
   - **CRITICAL**: No initialization of partition assignments

### The Bug

**Location**: `cmd/raftnode/main.go:32-33`

```go
func NewSimpleKVStore() *SimpleKVStore {
    return &SimpleKVStore{
        data:           make(map[int]int),
        partitionTable: sharding.NewPartitionTable(), // ❌ EMPTY TABLE
    }
}
```

**What Happens**:
1. Node starts → creates empty partition table (0 assignments)
2. Client sends request → ShardManager validates key
3. Partition lookup → `GetOwner()` returns `(nodeID=-1, exists=false)`
4. Error returned: `"no owner assigned for partition X"`

### Why This Happens

The partition table has **two lifecycle phases**:

1. **Creation Phase** (current implementation)
   - `NewPartitionTable()` creates empty map
   - Zero partition assignments
   - Cannot route any keys

2. **Initialization Phase** (MISSING)
   - `InitializeEvenDistribution()` should be called
   - Assigns all 16,384 partitions to nodes
   - Enables key routing

**The initialization phase is never executed during node startup.**

## Impact Analysis

### Severity: 🔴 CRITICAL - Prevents All Operations

**Affected Operations**:
- ✅ GET requests → 500 error (partition validation fails)
- ✅ PUT requests → 500 error (partition validation fails)
- ✅ DELETE requests → 500 error (partition validation fails)

**Cluster State**:
- Raft consensus → ✅ Working (leader election succeeds)
- Node health → ✅ Working (nodes report healthy)
- Partition routing → ❌ BROKEN (no assignments)

### Reproduction

```bash
# Start 3-node cluster
docker compose up -d

# Wait for nodes to start
sleep 5

# Attempt any operation
curl -X PUT http://localhost:8080/cache/1 \
  -H "Content-Type: application/json" \
  -d '{"value": 42}'

# Result: {"error": "Shard validation error: no owner assigned for partition XXXX"}
```

## Solution Design

### Option 1: Auto-Initialize on Startup (RECOMMENDED)

**Approach**: Automatically initialize partition table when first node starts

**Implementation**:

```go
// cmd/raftnode/main.go
func main() {
    // ... existing setup ...

    app := NewSimpleKVStore()
    r := raft.NewRaft(app, cfg)

    // Auto-initialize partition table after Raft starts
    if isLeader() {
        time.Sleep(2 * time.Second) // Wait for cluster formation
        initializePartitionTable(app, cfg)
    }
}

func initializePartitionTable(app *SimpleKVStore, cfg *raft.Config) {
    // Get all node IDs from config
    nodeIDs := make([]sharding.NodeID, cfg.GetTotalNodes())
    for i := 0; i < cfg.GetTotalNodes(); i++ {
        nodeIDs[i] = sharding.NodeID(i)
    }

    // Create partition distribution
    pt := sharding.InitializeEvenDistribution(sharding.TOTAL_PARTITIONS, nodeIDs)

    // Propose to Raft for replication
    ptUpdate := &raft.PartitionTableUpdate{
        Assignments: pt.GetAssignments(),
        Version:     pt.GetVersion(),
    }

    msg := raft.Message{
        MsgType:              "UPDATE_PARTITION_TABLE",
        PartitionTableUpdate: ptUpdate,
    }

    app.AppendMessage(msg) // This will replicate to all nodes
}
```

**Pros**:
- ✅ No manual intervention required
- ✅ Works for any cluster size
- ✅ Survives restarts (persisted via Raft log)
- ✅ Consistent across all nodes

**Cons**:
- ⚠️ Requires leader detection
- ⚠️ Adds startup complexity

### Option 2: Admin Endpoint Initialization

**Approach**: Use existing `/admin/init-partition-table` endpoint

**Implementation**:

```bash
# After cluster starts
curl -X POST http://localhost:8080/admin/init-partition-table \
  -H "Content-Type: application/json" \
  -d '{"node_ids": [0, 1, 2]}'
```

**Pros**:
- ✅ Explicit control over initialization
- ✅ Can re-initialize if needed
- ✅ Simple implementation

**Cons**:
- ❌ Manual step required
- ❌ Easy to forget
- ❌ Not automated in docker-compose

### Option 3: Bootstrap Script

**Approach**: Add initialization script to docker-compose

**Implementation**:

```yaml
# docker-compose.yml
services:
  bootstrap:
    image: curlimages/curl:latest
    depends_on:
      - raft-node-0
      - raft-node-1
      - raft-node-2
    command: >
      sh -c "
        sleep 5 &&
        curl -X POST http://raft-node-0:8080/admin/init-partition-table \
          -H 'Content-Type: application/json' \
          -d '{\"node_ids\": [0, 1, 2]}'
      "
    networks:
      - raft-network
```

**Pros**:
- ✅ Automated with docker-compose
- ✅ No code changes required
- ✅ Clear separation of concerns

**Cons**:
- ⚠️ Docker-compose specific
- ⚠️ Doesn't work for manual deployments
- ⚠️ Timing dependencies

## Recommended Solution

**Use Option 1 (Auto-Initialize on Startup)** with the following design:

### Implementation Plan

1. **Add Initialization Logic to main.go**
   - Detect if partition table is empty
   - Auto-initialize on first leader election
   - Use Raft to replicate across cluster

2. **Make it Idempotent**
   - Check if already initialized before initializing
   - Use partition table version to prevent re-initialization

3. **Add Configuration Flag**
   - `AUTO_INIT_PARTITIONS=true` (default)
   - Allow disabling for manual control

### Code Changes Required

**File: `cmd/raftnode/main.go`**

```go
// Add after Raft initialization
func autoInitializePartitions(r *raft.Raft, app *SimpleKVStore, cfg *raft.Config) {
    // Only initialize if table is empty
    if app.partitionTable.GetAssignmentCount() > 0 {
        slog.Info("Partition table already initialized, skipping")
        return
    }

    // Wait for leader election
    maxWait := 10 * time.Second
    start := time.Now()
    for time.Since(start) < maxWait {
        if r.IsLeader() {
            slog.Info("Leader elected, initializing partition table")
            break
        }
        time.Sleep(100 * time.Millisecond)
    }

    if !r.IsLeader() {
        slog.Info("Not leader, waiting for partition table from leader")
        return
    }

    // Create node list from config
    totalNodes := cfg.GetTotalNodes()
    nodeIDs := make([]sharding.NodeID, totalNodes)
    for i := 0; i < totalNodes; i++ {
        nodeIDs[i] = sharding.NodeID(i)
    }

    // Initialize partition distribution
    pt := sharding.InitializeEvenDistribution(sharding.TOTAL_PARTITIONS, nodeIDs)

    slog.Info(fmt.Sprintf("Initialized partition table with %d partitions across %d nodes",
        sharding.TOTAL_PARTITIONS, totalNodes))

    // Replicate via Raft
    msg := raft.Message{
        MsgType: "UPDATE_PARTITION_TABLE",
        PartitionTableUpdate: &raft.PartitionTableUpdate{
            Assignments: pt.GetAssignments(),
            Version:     pt.GetVersion(),
        },
    }

    if _, err := r.ProposeMessage(msg); err != nil {
        slog.Error(fmt.Sprintf("Failed to propose partition table update: %v", err))
    }
}
```

### Testing Strategy

1. **Unit Tests**
   - Test auto-initialization logic
   - Verify idempotency
   - Test with different cluster sizes

2. **Integration Tests**
   - Start cluster and verify auto-initialization
   - Test with 1, 3, 5 nodes
   - Verify all partitions assigned

3. **Regression Tests**
   - Verify existing functionality still works
   - Test snapshot restore with partition table
   - Test leader failover

## Verification

After implementing the fix, verify with:

```bash
# Start cluster
docker compose up -d

# Wait for initialization
sleep 5

# Check partition table status
curl http://localhost:8080/admin/partition-table

# Should show:
# {
#   "version": 1,
#   "total_partitions": 16384,
#   "assignment_count": 16384,
#   "node_stats": {
#     "0": 5461,
#     "1": 5461,
#     "2": 5462
#   }
# }

# Test actual operation
curl -X PUT http://localhost:8080/cache/1 \
  -H "Content-Type: application/json" \
  -d '{"value": 42}'

# Should return: {"success": true}
```

## Additional Considerations

### Dynamic Cluster Sizing

The current issue is exacerbated when `TOTAL_NODES` differs from actual running nodes:

**Example**:
- `TOTAL_NODES=5` in config
- Only 3 nodes actually running
- Partitions assigned to nodes 3 and 4 → unreachable

**Solution**:
- Auto-detect running nodes during initialization
- Use actual peer count from Raft, not config value

### Partition Rebalancing

Future enhancement: Support adding/removing nodes dynamically

**Requirements**:
- Rebalance partitions when nodes join/leave
- Migrate data during rebalancing
- Minimize disruption during migration

## Timeline

1. **Immediate** (1 hour): Implement Option 3 (bootstrap script) as workaround
2. **Short-term** (1 day): Implement Option 1 (auto-initialize) as permanent fix
3. **Medium-term** (1 week): Add dynamic node detection
4. **Long-term** (future): Implement partition rebalancing

## Conclusion

The root cause is clear: **partition table is never initialized during node startup**. The fix is straightforward: auto-initialize the partition table when the first leader is elected. This ensures all partitions are assigned before accepting client requests.

**Priority**: 🔴 CRITICAL - Blocks all cluster operations
**Complexity**: 🟡 MEDIUM - Requires Raft integration
**Risk**: 🟢 LOW - Well-defined scope and clear implementation path
