# Current Architecture vs Goal Analysis

**Date:** 2025-12-01
**Branch:** sharding

## Executive Summary

**CRITICAL FINDING:** Worker nodes are NOT implemented. The current architecture has Raft nodes storing BOTH partition table AND actual cache data, which contradicts the goal of separation between control plane and data plane.

---

## Current Architecture (What EXISTS)

### Control Plane + Data Plane MERGED
- **Single binary:** `cmd/raftnode/main.go`
- **Raft cluster:** 3 nodes running Raft consensus
- **Storage:** `SimpleKVStore` with `map[int]int data` field (lines 23-27)
- **Partition table:** Stored IN Raft state machine alongside data

### What Raft Nodes Currently Do
1. **Control Plane (CORRECT):**
   - Manage partition table via Raft consensus
   - 16,384 partitions with PRIMARY + BACKUP tracking (`PartitionEntry` exists)
   - Partition table replication via Raft snapshots

2. **Data Plane (INCORRECT - should be workers):**
   - Store actual key-value data in `SimpleKVStore.data`
   - Handle PUT/GET/DELETE via HTTP API
   - Replicate data writes via Raft consensus (N-way replication)
   - Primary-backup replication implemented in `/pkg/replication/client.go`

### Current Write Flow
```
Client → HTTP API (any Raft node)
       ↓
   ShardManager validates primary ownership
       ↓
   [PRIMARY NODE] Replicate to BACKUP (synchronous gRPC)
       ↓
   Raft.Forward() → Raft consensus (all nodes)
       ↓
   SimpleKVStore.AppendMessage() → write to local map[int]int
       ↓
   Return success to client
```

### Current Components (GOOD)

**✅ Already Implemented:**
1. `PartitionEntry` struct with `PrimaryNode` + `BackupNode` (Stage 1 COMPLETE)
2. `ReplicationClient` with circuit breaker (Stage 2 COMPLETE)
3. Primary-backup replication RPC (`Replicate` gRPC handler) (Stage 2 COMPLETE)
4. Synchronous write path with backup replication (Stage 3 COMPLETE)
5. ShardManager for partition validation
6. Consistent hashing (MurmurHash3, 16,384 partitions)

**❌ NOT Implemented:**
1. Separate worker binary (`cmd/worker/main.go` does not exist)
2. Worker registration with Raft cluster
3. Data-only workers (no Raft consensus on workers)
4. Horizontal scaling of workers independent of Raft cluster

---

## Goal Architecture (What SHOULD EXIST)

### Control Plane (Raft Nodes)
**Purpose:** Manage topology ONLY, no data storage

**Responsibilities:**
- Maintain partition table (which worker owns which partitions)
- Raft consensus for partition table updates
- Handle worker registration/deregistration
- Detect worker failures
- Trigger partition reassignments

**What changes:**
- Remove `data map[int]int` from `SimpleKVStore`
- Keep only partition table in Raft state machine
- Raft nodes do NOT handle client PUT/GET/DELETE requests

### Data Plane (Worker Nodes)
**Purpose:** Store and serve actual cache data

**Responsibilities:**
- Store subset of partitions (sharded data)
- Handle client PUT/GET/DELETE requests
- Primary-backup replication between workers
- Register with Raft cluster on startup
- Report health to Raft cluster

**New components needed:**
- `cmd/worker/main.go` binary
- Worker-to-Raft registration protocol
- Raft-to-Worker routing (Raft redirects clients to correct worker)

---

## Gap Analysis

### Missing Components

#### 1. Worker Binary (`cmd/worker/main.go`)
**What it needs:**
- HTTP API server (reuse `/pkg/api/server.go` logic)
- Local data storage (`map[int]int` or better cache implementation)
- Replication client for primary-backup replication
- Registration client to announce itself to Raft cluster
- Health reporting

**Dependencies:**
- Raft cluster addresses (environment variables)
- Worker ID (unique identifier)
- HTTP listen address
- Partition assignment (fetched from Raft cluster)

#### 2. Worker Registration Protocol
**What's needed:**
- gRPC endpoint on Raft nodes: `RegisterWorker(workerID, httpAddr)`
- Raft leader assigns partitions to new worker
- Raft broadcasts updated partition table to all nodes
- Worker fetches partition table after registration

**Flow:**
```
Worker startup
    ↓
Connect to Raft cluster (any node)
    ↓
Call RegisterWorker(workerID, httpAddr)
    ↓
Raft leader assigns partitions (rebalance)
    ↓
Raft replicates new partition table via consensus
    ↓
Worker receives partition assignment
    ↓
Worker is ready to serve requests
```

#### 3. Client Request Routing
**Current:** Client connects to any Raft node, gets redirected if wrong node

**Goal:** Client connects to Raft node, Raft redirects to WORKER

**Changes needed:**
- Raft HTTP API responds with `307 Temporary Redirect` to worker address
- Raft nodes do NOT execute PUT/GET/DELETE themselves
- Raft nodes ONLY manage partition table

#### 4. Data Migration Strategy
**Problem:** Current Raft nodes have data in `SimpleKVStore.data`

**Migration options:**

**Option A: Clean Start (RECOMMENDED)**
- Deploy workers alongside Raft nodes
- Stop accepting writes to Raft nodes
- Workers start empty
- Gradually migrate data from Raft nodes to workers
- Once complete, remove data storage from Raft nodes

**Option B: Dual Mode**
- Raft nodes run in "compatibility mode" (keep data storage)
- Deploy workers in parallel
- Clients gradually shift to workers
- Eventually deprecate Raft data storage

**Recommendation:** Option A (clean architecture, simpler reasoning)

---

## Implementation Stages (Revised)

### Stage 0: Current State Validation (ALREADY DONE)
**Status:** ✅ COMPLETE
- Partition table tracks primary + backup
- Replication client implemented
- Synchronous replication working
- ShardManager validates ownership

### Stage 1: Create Worker Binary (NEW)
**Goal:** Standalone worker that can serve data without Raft

**Duration:** 2-3 days

**Files to create:**
- `cmd/worker/main.go` - Worker entry point
- `pkg/worker/store.go` - Worker-specific data storage
- `pkg/worker/config.go` - Worker configuration

**What worker does:**
- Starts HTTP API server on dedicated port
- Stores partition data in local map (no Raft)
- Implements PUT/GET/DELETE handlers
- Performs primary-backup replication to other workers
- Registers with Raft cluster on startup

**Testing:**
- Standalone worker serves requests
- Primary-backup replication between workers
- No Raft consensus on worker data writes

### Stage 2: Worker Registration Protocol (NEW)
**Goal:** Workers can register with Raft cluster and receive partition assignments

**Duration:** 2-3 days

**Files to modify:**
- `proto/raft.proto` - Add `RegisterWorker` RPC
- `pkg/raft/grpc_server.go` - Implement `RegisterWorker` handler
- `pkg/raft/worker_registry.go` (NEW) - Track active workers
- `cmd/worker/main.go` - Call `RegisterWorker` on startup

**Flow:**
1. Worker starts, connects to Raft leader
2. Calls `RegisterWorker(workerID, httpAddr, capacity)`
3. Raft leader updates partition table (assign partitions to worker)
4. Raft replicates partition table via consensus
5. Worker receives partition assignment
6. Worker ready to serve

**Testing:**
- Worker registration succeeds
- Partition table updated with worker address
- Multiple workers can register
- Partition rebalancing on new worker join

### Stage 3: Raft Request Routing (NEW)
**Goal:** Raft nodes redirect client requests to workers

**Duration:** 1-2 days

**Files to modify:**
- `pkg/api/server.go` - Change PUT/GET/DELETE to return redirects
- Raft nodes no longer execute data operations themselves

**Current:**
```go
func (s *Server) handlePut(w http.ResponseWriter, r *http.Request) {
    // Validate shard ownership
    // Replicate to backup
    // Write to local Raft state
}
```

**New:**
```go
func (s *Server) handlePut(w http.ResponseWriter, r *http.Request) {
    // Validate shard ownership
    // Look up worker address from partition table
    // Return 307 Temporary Redirect to worker
}
```

**Testing:**
- Client request to Raft node returns redirect
- Redirect points to correct worker
- Client follows redirect successfully

### Stage 4: Data Migration (NEW)
**Goal:** Migrate existing data from Raft nodes to workers

**Duration:** 2-3 days (if needed)

**Options:**

**Option A: Clean Start**
- Accept data loss (acceptable for cache)
- Workers start empty
- Clients gradually populate cache

**Option B: Snapshot Export**
- Export data from Raft snapshots
- Import into workers during startup
- Verify consistency

**Recommendation:** Option A (simpler, aligns with cache semantics)

### Stage 5: Remove Data Storage from Raft (NEW)
**Goal:** Raft nodes store ONLY partition table, no data

**Duration:** 1 day

**Files to modify:**
- `cmd/raftnode/main.go` - Remove `data map[int]int` from `SimpleKVStore`
- Raft AppendMessage only handles `UPDATE_PARTITION_TABLE`
- Remove PUT/GET/DELETE message types

**Testing:**
- Raft cluster operates with no data storage
- Partition table replication still works
- Worker registration still works

---

## Key Architectural Decisions

### Decision 1: Worker Discovery
**Question:** How do workers find Raft cluster?

**Answer:** Environment variable `RAFT_ADDRS` (same as current Raft nodes)

**Rationale:** Reuse existing configuration pattern

### Decision 2: Worker ID Assignment
**Question:** How are worker IDs assigned?

**Answer:** Workers self-assign UUID, Raft cluster validates uniqueness

**Rationale:** Simplifies deployment, Raft is source of truth

### Decision 3: Partition Assignment Strategy
**Question:** How are partitions assigned to workers?

**Answer:** Even distribution (same as current `InitializeEvenDistribution`)

**Rationale:** Balanced load, simple rebalancing on worker join/leave

### Decision 4: Worker Failure Detection
**Question:** How does Raft detect worker failures?

**Answer:** Workers send periodic heartbeats (every 5 seconds)

**Rationale:** Aligns with existing Raft health check interval

### Decision 5: Data Consistency on Worker Failure
**Question:** What happens when primary worker fails?

**Answer:** Raft promotes backup to primary, assigns new backup

**Rationale:** Leverage existing primary-backup replication

---

## Deployment Topology

### Current (3 Raft nodes)
```
┌────────────────────────────────────────┐
│  Raft Node 0 (localhost:9000)         │
│  - Raft consensus                      │
│  - Partition table                     │
│  - DATA STORAGE (map[int]int)          │ ← WRONG
│  - HTTP API (localhost:8080)           │
└────────────────────────────────────────┘

┌────────────────────────────────────────┐
│  Raft Node 1 (localhost:9001)         │
│  - Raft consensus                      │
│  - Partition table                     │
│  - DATA STORAGE (map[int]int)          │ ← WRONG
│  - HTTP API (localhost:8081)           │
└────────────────────────────────────────┘

┌────────────────────────────────────────┐
│  Raft Node 2 (localhost:9002)         │
│  - Raft consensus                      │
│  - Partition table                     │
│  - DATA STORAGE (map[int]int)          │ ← WRONG
│  - HTTP API (localhost:8082)           │
└────────────────────────────────────────┘
```

### Goal (3 Raft nodes + N workers)
```
CONTROL PLANE (Raft Cluster)
┌────────────────────────────────────────┐
│  Raft Node 0 (localhost:9000)         │
│  - Raft consensus                      │
│  - Partition table ONLY                │ ← CORRECT
│  - Worker registry                     │
│  - HTTP API (redirects to workers)     │
└────────────────────────────────────────┘

┌────────────────────────────────────────┐
│  Raft Node 1 (localhost:9001)         │
│  - Raft consensus                      │
│  - Partition table ONLY                │ ← CORRECT
│  - Worker registry                     │
│  - HTTP API (redirects to workers)     │
└────────────────────────────────────────┘

┌────────────────────────────────────────┐
│  Raft Node 2 (localhost:9002)         │
│  - Raft consensus                      │
│  - Partition table ONLY                │ ← CORRECT
│  - Worker registry                     │
│  - HTTP API (redirects to workers)     │
└────────────────────────────────────────┘

DATA PLANE (Worker Nodes)
┌────────────────────────────────────────┐
│  Worker 0 (localhost:7000)             │
│  - DATA STORAGE (partitions 0-5460)    │ ← CORRECT
│  - Primary-backup replication          │
│  - HTTP API (data operations)          │
└────────────────────────────────────────┘

┌────────────────────────────────────────┐
│  Worker 1 (localhost:7001)             │
│  - DATA STORAGE (partitions 5461-10921)│ ← CORRECT
│  - Primary-backup replication          │
│  - HTTP API (data operations)          │
└────────────────────────────────────────┘

┌────────────────────────────────────────┐
│  Worker 2 (localhost:7002)             │
│  - DATA STORAGE (partitions 10922-16383)│ ← CORRECT
│  - Primary-backup replication          │
│  - HTTP API (data operations)          │
└────────────────────────────────────────┘
```

---

## Next Steps

### Immediate Actions (Recommended Order)

1. **Create worker binary** (Stage 1)
   - Start with standalone worker
   - No Raft integration yet
   - Manual partition assignment for testing

2. **Implement worker registration** (Stage 2)
   - Add RegisterWorker RPC
   - Raft assigns partitions
   - Workers fetch partition table

3. **Modify Raft routing** (Stage 3)
   - Raft redirects to workers
   - Stop executing data operations on Raft nodes

4. **Clean data from Raft** (Stage 5)
   - Remove data storage from Raft nodes
   - Partition table only

### Testing Strategy

**Stage 1 Tests:**
- Worker serves GET/PUT/DELETE
- Primary-backup replication between workers
- No Raft involvement

**Stage 2 Tests:**
- Worker registers with Raft
- Partition assignment correct
- Multiple workers register successfully

**Stage 3 Tests:**
- Client request to Raft → redirect to worker
- Client follows redirect → success
- Wrong worker → redirect to correct worker

**End-to-End Tests:**
- 3 Raft nodes + 3 workers
- Client writes data
- Primary worker fails
- Backup promoted
- Data still accessible

---

## Complexity Assessment

### Low Complexity
- Creating worker binary (reuse existing code)
- Worker HTTP API (copy from Raft node)
- Standalone worker testing

### Medium Complexity
- Worker registration protocol
- Partition assignment logic
- Raft-to-worker redirect logic

### High Complexity
- Data migration (if required)
- Partition rebalancing on worker join/leave
- Failure recovery with backup promotion

---

## Estimated Timeline

**Conservative Estimate:**
- Stage 1 (Worker binary): 3 days
- Stage 2 (Registration): 3 days
- Stage 3 (Raft routing): 2 days
- Stage 4 (Data migration): Skip (clean start)
- Stage 5 (Clean Raft): 1 day

**Total: 9 days** (assuming no major blockers)

**Aggressive Estimate:**
- Stage 1: 2 days
- Stage 2: 2 days
- Stage 3: 1 day
- Stage 5: 1 day

**Total: 6 days** (with clean start, no migration)

---

## Risk Assessment

### High Risk
- **Partition rebalancing bugs** → Data loss or unavailability
- **Worker registration race conditions** → Duplicate partition assignments
- **Raft-worker routing errors** → Clients can't find data

### Medium Risk
- **Primary-backup replication failures** → Data inconsistency
- **Worker failure detection delays** → Temporary unavailability
- **Network partition between Raft and workers** → Split brain

### Low Risk
- Worker binary creation (well-understood problem)
- HTTP API reuse (existing code works)
- Testing strategy (good existing test infrastructure)

---

## Conclusion

**Current State:**
- Raft nodes are doing BOTH control plane (partition table) AND data plane (data storage)
- Primary-backup replication is implemented but within Raft cluster
- Worker separation does NOT exist

**Required Changes:**
- Create separate worker binary
- Implement worker registration with Raft
- Change Raft nodes to redirect clients to workers
- Remove data storage from Raft nodes

**Outcome:**
- Clean separation: Raft = topology, Workers = data
- Horizontal scaling of workers independent of Raft
- Simplified Raft cluster (no data, smaller snapshots)
- True distributed cache with sharding and replication
