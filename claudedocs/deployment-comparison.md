# Deployment Comparison: Current vs Goal

**Date:** 2025-12-01

---

## Current Deployment (WRONG Architecture)

### Architecture Diagram

```
┌─────────────────────────────────────────────────┐
│ RAFT NODE 0 (localhost:9000, HTTP :8080)      │
│ ┌─────────────────────────────────────────┐   │
│ │ Control Plane: Partition Table          │   │
│ │ - Raft consensus                        │   │
│ │ - 16,384 partitions                     │   │
│ └─────────────────────────────────────────┘   │
│ ┌─────────────────────────────────────────┐   │
│ │ Data Plane: map[int]int ❌ WRONG       │   │
│ │ - Stores ALL keys (full replication)    │   │
│ │ - PUT/GET/DELETE handlers               │   │
│ └─────────────────────────────────────────┘   │
└─────────────────────────────────────────────────┘
                      ↓
                 Raft consensus
                      ↓
┌─────────────────────────────────────────────────┐
│ RAFT NODE 1 (localhost:9001, HTTP :8081)      │
│ - Partition Table + Data Storage ❌            │
└─────────────────────────────────────────────────┘
                      ↓
                 Raft consensus
                      ↓
┌─────────────────────────────────────────────────┐
│ RAFT NODE 2 (localhost:9002, HTTP :8082)      │
│ - Partition Table + Data Storage ❌            │
└─────────────────────────────────────────────────┘
```

### Problems

1. **No separation:** Raft nodes do BOTH control and data plane
2. **Full replication:** Every key replicated to all 3 Raft nodes (N-way)
3. **No sharding:** Cannot scale data storage independently
4. **Raft bottleneck:** All writes go through Raft consensus (slow)
5. **Large snapshots:** Snapshots include BOTH partition table AND data

### Current Data Flow

```
Client
  ↓
PUT /kv {key: 42, value: 100}
  ↓
Raft Node 0 HTTP API (:8080)
  ↓
ShardManager: "Am I primary for key 42?" → YES
  ↓
ReplicationClient: Replicate to backup (Node 1)
  ↓
Raft.Forward() → Raft consensus (all nodes)
  ↓
SimpleKVStore.AppendMessage("PUT")
  ↓
Write to map[int]int ❌ WRONG (should be on worker)
  ↓
Return success to client
```

---

## Goal Deployment (CORRECT Architecture)

### Architecture Diagram

```
┌───────────────── CONTROL PLANE ─────────────────┐
│                                                  │
│  ┌────────────────────────────────────────┐    │
│  │ RAFT NODE 0 (localhost:9000)          │    │
│  │ - Raft consensus ONLY                  │    │
│  │ - Partition table (160 KB)             │    │
│  │ - Worker registry                      │    │
│  │ - HTTP redirects to workers            │    │
│  └────────────────────────────────────────┘    │
│                     ↕                            │
│              Raft consensus                     │
│                     ↕                            │
│  ┌────────────────────────────────────────┐    │
│  │ RAFT NODE 1 (localhost:9001)          │    │
│  │ - Raft consensus ONLY                  │    │
│  └────────────────────────────────────────┘    │
│                     ↕                            │
│  ┌────────────────────────────────────────┐    │
│  │ RAFT NODE 2 (localhost:9002)          │    │
│  │ - Raft consensus ONLY                  │    │
│  └────────────────────────────────────────┘    │
│                                                  │
└──────────────────────────────────────────────────┘
                      ↓
           Partition table updates
                      ↓
┌───────────────── DATA PLANE ────────────────────┐
│                                                  │
│  ┌────────────────────────────────────────┐    │
│  │ WORKER 0 (localhost:7000)              │    │
│  │ - Partitions 0-5460 (1/3 of data) ✅   │    │
│  │ - Primary-backup replication           │    │
│  │ - HTTP API: PUT/GET/DELETE             │    │
│  │ - NO Raft consensus on data            │    │
│  └────────────────────────────────────────┘    │
│                     ↕                            │
│         Primary-backup replication              │
│                     ↕                            │
│  ┌────────────────────────────────────────┐    │
│  │ WORKER 1 (localhost:7001)              │    │
│  │ - Partitions 5461-10921 (1/3) ✅       │    │
│  └────────────────────────────────────────┘    │
│                     ↕                            │
│  ┌────────────────────────────────────────┐    │
│  │ WORKER 2 (localhost:7002)              │    │
│  │ - Partitions 10922-16383 (1/3) ✅      │    │
│  └────────────────────────────────────────┘    │
│                                                  │
└──────────────────────────────────────────────────┘
```

### Benefits

1. **Clean separation:** Raft = topology, Workers = data
2. **Sharding:** Each worker stores 1/3 of data (scalable)
3. **Independent scaling:** Add more workers without touching Raft
4. **Fast writes:** No Raft consensus on data (only primary-backup)
5. **Small Raft snapshots:** Only partition table (~160 KB)

### Goal Data Flow

```
Client
  ↓
PUT /kv {key: 42, value: 100}
  ↓
Raft Node 0 HTTP API (:8080)
  ↓
ShardManager: "Which worker owns key 42?" → Worker 1
  ↓
307 Temporary Redirect → http://worker-1:7000/kv
  ↓
────────────────────────────────────────────
  ↓
Worker 1 HTTP API (:7001)
  ↓
ShardManager: "Am I primary?" → YES
  ↓
ReplicationClient: Replicate to backup (Worker 2)
  ↓
Worker 2 receives replication → Write to local map
  ↓
Worker 1 writes to local map ✅ CORRECT (no Raft)
  ↓
Return success to client
```

---

## Side-by-Side Comparison

| Aspect | Current (WRONG) | Goal (CORRECT) |
|--------|----------------|----------------|
| **Raft Node Role** | Control + Data | Control ONLY |
| **Raft Node Storage** | Partition table + Data | Partition table ONLY |
| **Data Storage** | All Raft nodes (full replication) | Workers (sharded) |
| **Write Path** | Raft consensus (N-way) | Primary-backup (2-way) |
| **Scaling** | Add Raft nodes (complex) | Add workers (simple) |
| **Snapshot Size** | Large (data + table) | Small (table only) |
| **Client Request** | Raft node executes | Raft node redirects |
| **Partition Count** | 16,384 (exists) | 16,384 (same) |
| **Partitions per Node** | All 16,384 (replicated) | 5,461 per worker (sharded) |

---

## File Structure Comparison

### Current (WRONG)

```
/Users/dzc/distributed-cache/
├── cmd/
│   ├── raftnode/main.go        ← Does BOTH control + data ❌
│   └── raftcli/main.go
├── pkg/
│   ├── raft/                   ← Raft consensus
│   ├── sharding/               ← Partition table ✅
│   ├── replication/            ← Replication client ✅
│   └── api/server.go           ← Executes data ops ❌
└── docker-compose.yml          ← 3 Raft nodes only
```

### Goal (CORRECT)

```
/Users/dzc/distributed-cache/
├── cmd/
│   ├── raftnode/main.go        ← Control plane ONLY ✅
│   ├── worker/main.go          ← Data plane (NEW) ✅
│   └── raftcli/main.go
├── pkg/
│   ├── raft/                   ← Raft consensus
│   │   └── worker_registry.go ← Track workers (NEW) ✅
│   ├── worker/                 ← Worker logic (NEW) ✅
│   │   ├── config.go
│   │   ├── store.go
│   │   ├── api_server.go
│   │   ├── replication_client.go
│   │   └── registration_client.go
│   ├── sharding/               ← Partition table
│   ├── replication/            ← Replication client
│   └── api/server.go           ← Redirects to workers ✅
├── proto/raft.proto            ← Add RegisterWorker RPC ✅
└── docker-compose.yml          ← 3 Raft + 3 Workers ✅
```

---

## Docker Compose Comparison

### Current (WRONG)

```yaml
services:
  raft-node-0:
    container_name: raft-node-0
    environment:
      RAFT_ID: "0"
      TOTAL_NODES: "3"
      API_ADDR: ":8080"  # Handles data operations ❌
    ports:
      - "9000:9000"  # Raft gRPC
      - "8080:8080"  # HTTP API (data ops) ❌

  # ... raft-node-1, raft-node-2 same ...
```

### Goal (CORRECT)

```yaml
services:
  # CONTROL PLANE
  raft-node-0:
    container_name: raft-node-0
    environment:
      RAFT_ID: "0"
      TOTAL_NODES: "3"
      API_ADDR: ":8080"  # Redirects to workers ✅
    ports:
      - "9000:9000"  # Raft gRPC
      - "8080:8080"  # HTTP API (redirects) ✅

  # DATA PLANE
  worker-0:
    container_name: worker-0
    environment:
      WORKER_ID: "0"
      HTTP_ADDR: ":7000"
      RAFT_ADDRS: "raft-node-0:9000,raft-node-1:9000,raft-node-2:9000"
      PEER_WORKERS: "worker-0:7000,worker-1:7000,worker-2:7000"
    ports:
      - "7000:7000"  # Worker HTTP API ✅
    depends_on:
      - raft-node-0
      - raft-node-1
      - raft-node-2

  # ... worker-1, worker-2 same ...
```

---

## Request Flow Comparison

### Current: Client → Raft (WRONG)

```
1. Client sends PUT to http://localhost:8080/kv
2. Raft node validates shard ownership
3. Raft node replicates to backup (via Raft gRPC)
4. Raft node writes to Raft state machine
5. Raft consensus commits to ALL nodes
6. Raft node returns success

Problem: Every write goes through Raft consensus (slow, bottleneck)
```

### Goal: Client → Raft → Worker (CORRECT)

```
1. Client sends PUT to http://localhost:8080/kv
2. Raft node looks up partition owner (Worker 1)
3. Raft node returns 307 Redirect → http://worker-1:7001/kv
4. Client follows redirect to Worker 1
5. Worker 1 validates primary ownership
6. Worker 1 replicates to Worker 2 (backup, via HTTP)
7. Worker 1 writes to local map (NO Raft consensus)
8. Worker 1 returns success

Benefit: Fast writes (no Raft consensus), scalable workers
```

---

## Partition Distribution

### Current (WRONG): Full Replication

```
Raft Node 0: Partitions 0-16383 (ALL)
Raft Node 1: Partitions 0-16383 (ALL)
Raft Node 2: Partitions 0-16383 (ALL)

Total partitions stored: 16,384 × 3 = 49,152
Data replication factor: 3×
Storage efficiency: 33% (wasteful)
```

### Goal (CORRECT): Sharded + Primary-Backup

```
Worker 0 (Primary):   Partitions 0-5460    (1/3)
Worker 1 (Primary):   Partitions 5461-10921  (1/3)
Worker 2 (Primary):   Partitions 10922-16383 (1/3)

Worker 0 (Backup):    Partitions for Worker 2
Worker 1 (Backup):    Partitions for Worker 0
Worker 2 (Backup):    Partitions for Worker 1

Total partitions stored: 16,384 × 2 = 32,768 (primary + backup)
Data replication factor: 2× (efficient)
Storage efficiency: 50% (better)
```

---

## Scalability Comparison

### Current (WRONG): Cannot Scale

```
To add more data capacity:
1. Add new Raft node → Requires Raft reconfiguration (complex)
2. All nodes still replicate ALL data (no sharding)
3. Snapshot size grows with data (large snapshots)
4. More Raft nodes = slower consensus (worse performance)

Conclusion: Does NOT scale
```

### Goal (CORRECT): Horizontal Scaling

```
To add more data capacity:
1. Start new worker → docker-compose up worker-3
2. Worker registers with Raft cluster
3. Raft rebalances partitions across workers
4. New worker starts serving its partitions
5. Raft cluster unchanged (3 nodes always)

Conclusion: Linear scalability
```

---

## Testing Strategy

### Current System Tests

```bash
# Test Raft cluster with data storage (current)
docker-compose up -d
curl -X POST http://localhost:8080/kv -d '{"key":42,"value":100}'
curl http://localhost:8080/kv/42

# Problem: Data is on Raft nodes (wrong)
```

### Goal System Tests

```bash
# Test Raft + Workers (goal)
docker-compose up -d  # Start 3 Raft + 3 Workers

# Step 1: Verify workers registered
curl http://localhost:8080/admin/workers
# Expected: {"workers": [0, 1, 2], "total": 3}

# Step 2: PUT via Raft (should redirect)
curl -L -X POST http://localhost:8080/kv -d '{"key":42,"value":100}'
# Expected: Redirect to worker, then success

# Step 3: GET via Raft (should redirect)
curl -L http://localhost:8080/kv/42
# Expected: Redirect to worker, then {"key":42,"value":100}

# Step 4: Direct worker access (should work)
curl -X POST http://localhost:7000/kv -d '{"key":42,"value":100}'
curl http://localhost:7000/kv/42

# Step 5: Verify data is on workers, NOT Raft
# Raft snapshot should be < 200 KB (only partition table)
```

---

## Migration Path

### Option A: Clean Start (RECOMMENDED)

```
1. Deploy workers alongside current Raft cluster
2. Stop accepting writes to Raft nodes (maintenance mode)
3. Workers start with empty data
4. Clients gradually populate cache via workers
5. Remove data storage from Raft nodes (Stage 4)
6. Accept data loss (acceptable for cache)

Timeline: 1-2 days
Risk: Low (clean separation)
Data loss: Acceptable (cache semantics)
```

### Option B: Data Export/Import (COMPLEX)

```
1. Export data from Raft snapshots (manual)
2. Convert to worker format
3. Import into workers during startup
4. Verify consistency
5. Remove data from Raft

Timeline: 3-5 days
Risk: Medium (data migration bugs)
Data loss: None
Complexity: High
```

**Recommendation:** Option A (clean start) for simplicity

---

## Success Criteria

### Stage 1: Worker Binary

- [ ] Worker binary compiles
- [ ] Worker starts and listens on HTTP
- [ ] Standalone PUT/GET/DELETE work (no Raft)
- [ ] Worker health endpoint returns 200

### Stage 2: Worker Registration

- [ ] Worker registers with Raft leader
- [ ] Partition table updated with worker address
- [ ] Multiple workers can register
- [ ] Worker heartbeat mechanism works

### Stage 3: Raft Routing

- [ ] Client PUT to Raft → 307 redirect to worker
- [ ] Client GET to Raft → 307 redirect to worker
- [ ] Client follows redirect successfully
- [ ] Data stored on worker, NOT Raft

### Stage 4: Raft Data Removal

- [ ] Raft snapshot < 200 KB (only partition table)
- [ ] Raft nodes have NO data storage
- [ ] Raft cluster operates normally
- [ ] Worker registration still works

### End-to-End

- [ ] Client can PUT/GET/DELETE via Raft redirects
- [ ] Data sharded across workers (1/3 each)
- [ ] Primary-backup replication works
- [ ] Worker failure → backup promoted
- [ ] Raft cluster manages topology only

---

## Conclusion

**Current State:**
- Raft nodes do BOTH control plane (partition table) AND data plane (data storage)
- Cannot scale data storage independently
- All writes go through slow Raft consensus

**Goal State:**
- Raft nodes manage ONLY partition table (control plane)
- Workers handle ALL data storage (data plane)
- Horizontal scaling by adding workers
- Fast writes (no Raft consensus on data)

**Implementation Plan:**
1. Create worker binary (Stage 1)
2. Implement worker registration (Stage 2)
3. Modify Raft to redirect requests (Stage 3)
4. Remove data from Raft nodes (Stage 4)

**Timeline:** 6-9 days

Ready to start implementation!
