# Worker Node Implementation Roadmap

**Date:** 2025-12-01
**Branch:** sharding → feature/worker-separation
**Status:** Ready for Implementation

---

## Quick Summary

### What's Missing?

**Workers are NOT implemented.** Current Raft nodes store BOTH partition table AND data, which contradicts the goal of control/data plane separation.

### What Needs to Be Built?

1. **Worker binary** (`cmd/worker/main.go`) - Separate data plane nodes
2. **Worker registration** - Workers register with Raft cluster
3. **Raft routing** - Raft redirects client requests to workers
4. **Data removal from Raft** - Raft stores ONLY partition table

---

## Current State ✅

### Already Implemented (Good Foundation)

| Component | Status | Location |
|-----------|--------|----------|
| Partition Table with Primary+Backup | ✅ COMPLETE | `/pkg/sharding/types.go` |
| Replication Client | ✅ COMPLETE | `/pkg/replication/client.go` |
| Primary-Backup Replication RPC | ✅ COMPLETE | Raft gRPC handler |
| Synchronous Write Path | ✅ COMPLETE | `/pkg/api/server.go` |
| ShardManager | ✅ COMPLETE | `/pkg/sharding/shard_manager.go` |
| Consistent Hashing | ✅ COMPLETE | MurmurHash3, 16,384 partitions |

### Current Problem

```
┌─────────────────────────────────────┐
│ Raft Node (WRONG)                  │
│ ┌─────────────────────────────┐   │
│ │ Partition Table ✅          │   │
│ └─────────────────────────────┘   │
│ ┌─────────────────────────────┐   │
│ │ Data Storage ❌ (should be  │   │
│ │ on workers, not Raft)       │   │
│ └─────────────────────────────┘   │
└─────────────────────────────────────┘
```

---

## Implementation Stages

### Stage 1: Create Worker Binary
**Goal:** Standalone worker that can serve data without Raft

**Duration:** 2-3 days

**Files to Create:**
- `cmd/worker/main.go` - Worker entry point
- `pkg/worker/config.go` - Worker configuration
- `pkg/worker/store.go` - Worker data storage
- `pkg/worker/api_server.go` - Worker HTTP API
- `pkg/worker/replication_client.go` - Worker-to-worker replication
- `deploy/Dockerfile.worker` - Worker container

**Key Features:**
- HTTP API for PUT/GET/DELETE
- Primary-backup replication between workers
- No Raft consensus on data writes
- Standalone operation (manual partition assignment for testing)

**Test:**
```bash
go test ./tests/worker_standalone_test.go
```

**Validation:**
- [ ] Worker binary compiles
- [ ] Worker starts on HTTP port
- [ ] PUT/GET/DELETE work without Raft
- [ ] Primary-backup replication works

---

### Stage 2: Worker Registration
**Goal:** Workers register with Raft and receive partition assignments

**Duration:** 2-3 days

**Files to Create:**
- `pkg/raft/worker_registry.go` - Track active workers
- `pkg/worker/registration_client.go` - Worker registration logic

**Files to Modify:**
- `proto/raft.proto` - Add `RegisterWorker` and `WorkerHeartbeat` RPCs
- `pkg/raft/grpc_server.go` - Implement RPC handlers
- `cmd/worker/main.go` - Call registration on startup

**Key Features:**
- Workers announce themselves to Raft cluster
- Raft leader assigns partitions to workers
- Workers receive partition table via RPC
- Periodic heartbeats for health monitoring

**Test:**
```bash
go test ./tests/worker_registration_test.go
```

**Validation:**
- [ ] Worker registers successfully
- [ ] Partition table updated on Raft
- [ ] Multiple workers can register
- [ ] Heartbeat mechanism works
- [ ] Stale workers removed after timeout

---

### Stage 3: Raft Request Routing
**Goal:** Raft nodes redirect client requests to workers

**Duration:** 1-2 days

**Files to Modify:**
- `pkg/api/server.go` (Raft node) - Change PUT/GET/DELETE to return redirects

**Changes:**
```go
// OLD: Raft executes data operations
func (s *Server) handlePut(w http.ResponseWriter, r *http.Request) {
    // ... validate shard ownership ...
    // ... replicate to backup ...
    // ... write to Raft state machine ... ❌
}

// NEW: Raft redirects to worker
func (s *Server) handlePut(w http.ResponseWriter, r *http.Request) {
    // ... determine worker address ...
    w.Header().Set("Location", workerAddr)
    w.WriteHeader(http.StatusTemporaryRedirect) ✅
}
```

**Test:**
```bash
go test ./tests/raft_worker_routing_test.go
```

**Validation:**
- [ ] PUT to Raft → 307 redirect to worker
- [ ] GET to Raft → 307 redirect to worker
- [ ] DELETE to Raft → 307 redirect to worker
- [ ] Client follows redirect successfully
- [ ] Data stored on worker, NOT Raft

---

### Stage 4: Remove Data from Raft
**Goal:** Raft nodes store ONLY partition table

**Duration:** 1 day

**Files to Modify:**
- `cmd/raftnode/main.go` - Remove `data map[int]int` from `SimpleKVStore`

**Changes:**
```go
// OLD: Raft stores partition table + data
type SimpleKVStore struct {
    mu             sync.RWMutex
    data           map[int]int           ❌ REMOVE
    partitionTable *sharding.PartitionTable
}

// NEW: Raft stores ONLY partition table
type SimpleKVStore struct {
    mu             sync.RWMutex
    partitionTable *sharding.PartitionTable ✅
}
```

**Test:**
```bash
go test ./tests/raft_partition_table_only_test.go
```

**Validation:**
- [ ] `data` field removed from `SimpleKVStore`
- [ ] Raft snapshot < 200 KB (only partition table)
- [ ] Raft cluster operates normally
- [ ] Worker registration still works

---

## File Structure Changes

### New Files (Create)

```
/Users/dzc/distributed-cache/
├── cmd/
│   └── worker/
│       └── main.go                    ← NEW: Worker binary
├── pkg/
│   ├── raft/
│   │   └── worker_registry.go         ← NEW: Track workers
│   └── worker/                         ← NEW: Worker package
│       ├── config.go
│       ├── store.go
│       ├── api_server.go
│       ├── replication_client.go
│       └── registration_client.go
├── deploy/
│   └── Dockerfile.worker              ← NEW: Worker container
└── tests/
    ├── worker_standalone_test.go      ← NEW
    ├── worker_registration_test.go    ← NEW
    ├── raft_worker_routing_test.go    ← NEW
    └── raft_partition_table_only_test.go ← NEW
```

### Modified Files

```
/Users/dzc/distributed-cache/
├── proto/
│   └── raft.proto                     ← MODIFY: Add RegisterWorker RPC
├── pkg/
│   ├── raft/
│   │   └── grpc_server.go             ← MODIFY: Add RPC handlers
│   └── api/
│       └── server.go                  ← MODIFY: Redirect to workers
├── cmd/
│   └── raftnode/
│       └── main.go                    ← MODIFY: Remove data storage
└── docker-compose.yml                 ← MODIFY: Add workers
```

---

## Docker Compose Changes

### Current (3 Raft nodes only)

```yaml
services:
  raft-node-0:
    # ... config ...
  raft-node-1:
    # ... config ...
  raft-node-2:
    # ... config ...
```

### Goal (3 Raft + 3 Workers)

```yaml
services:
  # CONTROL PLANE
  raft-node-0:
    # ... config ...
  raft-node-1:
    # ... config ...
  raft-node-2:
    # ... config ...

  # DATA PLANE (NEW)
  worker-0:
    build:
      dockerfile: deploy/Dockerfile.worker
    environment:
      WORKER_ID: "0"
      HTTP_ADDR: ":7000"
      RAFT_ADDRS: "raft-node-0:9000,..."
    ports:
      - "7000:7000"

  worker-1:
    # ... similar ...

  worker-2:
    # ... similar ...
```

---

## Testing Strategy

### Stage 1: Worker Standalone

```bash
# Start standalone worker
./worker

# Test PUT
curl -X POST http://localhost:7000/kv -d '{"key":42,"value":100}'

# Test GET
curl http://localhost:7000/kv/42

# Test DELETE
curl -X DELETE http://localhost:7000/kv/42
```

### Stage 2: Worker Registration

```bash
# Start Raft cluster
docker-compose up -d raft-node-0 raft-node-1 raft-node-2

# Start worker (should auto-register)
docker-compose up -d worker-0

# Check registration
curl http://localhost:8080/admin/workers
# Expected: {"workers": [0], "total": 1}

# Start more workers
docker-compose up -d worker-1 worker-2

# Check partition distribution
curl http://localhost:8080/admin/partitions
```

### Stage 3: Raft Routing

```bash
# Start full system
docker-compose up -d

# PUT via Raft (should redirect to worker)
curl -L -X POST http://localhost:8080/kv -d '{"key":42,"value":100}'

# GET via Raft (should redirect to worker)
curl -L http://localhost:8080/kv/42

# Verify data on worker, NOT Raft
# (Check Raft snapshot size < 200 KB)
```

### End-to-End Tests

```bash
# Run all tests
go test ./tests/...

# Run with race detector
go test -race ./tests/...

# Run integration tests
go test -v ./tests/integration_test.go
```

---

## Timeline

| Stage | Task | Duration | Dependencies |
|-------|------|----------|--------------|
| 1 | Create worker binary | 2-3 days | None |
| 2 | Worker registration | 2-3 days | Stage 1 |
| 3 | Raft routing | 1-2 days | Stage 2 |
| 4 | Remove Raft data | 1 day | Stage 3 |

**Total:** 6-9 days (conservative estimate)

---

## Success Metrics

### Functional Requirements

- [ ] Workers serve data independently of Raft
- [ ] Workers register with Raft cluster
- [ ] Partition table updated on worker join
- [ ] Raft redirects client requests to workers
- [ ] Data stored ONLY on workers, NOT Raft
- [ ] Primary-backup replication between workers
- [ ] Worker failure detection and backup promotion

### Performance Requirements

- [ ] Write latency < 30ms (worker-to-worker replication)
- [ ] Raft snapshot < 200 KB (partition table only)
- [ ] Worker registration < 5 seconds
- [ ] Partition rebalancing < 10 seconds

### Quality Requirements

- [ ] All tests pass with `-race` flag
- [ ] Test coverage > 85% for worker package
- [ ] Integration tests cover all failure scenarios
- [ ] Documentation complete (inline comments + README)

---

## Common Issues and Solutions

### Issue 1: Worker Registration Fails

**Symptom:** Worker cannot connect to Raft cluster

**Solution:**
- Check `RAFT_ADDRS` environment variable
- Verify Raft cluster is running
- Check network connectivity
- Review Raft leader election status

### Issue 2: Partition Assignment Wrong

**Symptom:** Worker receives wrong partitions

**Solution:**
- Check partition table version
- Verify Raft consensus committed update
- Review partition rebalancing logic
- Check for stale partition table

### Issue 3: Replication Timeout

**Symptom:** Primary-backup replication fails

**Solution:**
- Increase replication timeout (default 1s)
- Check backup worker health
- Review circuit breaker state
- Verify network latency

### Issue 4: Raft Redirect Loop

**Symptom:** Client keeps getting redirected

**Solution:**
- Verify worker HTTP address in registry
- Check partition table consistency
- Review ShardManager logic
- Ensure worker is registered

---

## Migration Checklist

### Pre-Migration

- [ ] Backup current Raft snapshots
- [ ] Document current partition distribution
- [ ] Verify all tests pass on current system
- [ ] Review resource requirements (CPU, memory)

### During Migration

- [ ] Deploy workers alongside Raft nodes
- [ ] Verify workers register successfully
- [ ] Test redirect flow (Raft → Worker)
- [ ] Gradually shift traffic to workers
- [ ] Monitor worker health and performance

### Post-Migration

- [ ] Remove data storage from Raft nodes
- [ ] Verify Raft snapshot size reduced
- [ ] Test worker failure scenarios
- [ ] Update client configuration (if needed)
- [ ] Document new architecture

---

## Next Immediate Steps

### 1. Create Feature Branch

```bash
cd /Users/dzc/distributed-cache
git checkout -b feature/worker-separation
git push -u origin feature/worker-separation
```

### 2. Start with Stage 1

```bash
# Create worker package directory
mkdir -p pkg/worker
mkdir -p cmd/worker

# Create worker binary entry point
touch cmd/worker/main.go
```

### 3. Implement Worker Store

```bash
# Create worker data storage
touch pkg/worker/store.go
touch pkg/worker/config.go
touch pkg/worker/api_server.go
```

### 4. Test Standalone Worker

```bash
# Build worker binary
go build -o bin/worker ./cmd/worker

# Run worker
./bin/worker

# Test API
curl -X POST http://localhost:7000/kv -d '{"key":42,"value":100}'
curl http://localhost:7000/kv/42
```

---

## Key Design Decisions

### Decision 1: HTTP vs gRPC for Worker-to-Worker

**Choice:** HTTP (simple, reuse existing API)
**Rationale:** Faster implementation, easier debugging
**Alternative:** gRPC (better performance, but more complex)

### Decision 2: Worker ID Assignment

**Choice:** Manual assignment via environment variable
**Rationale:** Simple, deterministic
**Alternative:** Auto-generated UUID (more flexible)

### Decision 3: Partition Rebalancing

**Choice:** Even distribution (same as current)
**Rationale:** Balanced load, simple logic
**Alternative:** Consistent hashing (better for dynamic scaling)

### Decision 4: Data Migration

**Choice:** Clean start (accept data loss)
**Rationale:** Simpler, aligns with cache semantics
**Alternative:** Snapshot export/import (preserves data)

---

## Documentation References

| Document | Purpose |
|----------|---------|
| `current-architecture-vs-goal.md` | Detailed architecture analysis |
| `worker-implementation-plan.md` | Complete implementation guide with code |
| `deployment-comparison.md` | Side-by-side deployment comparison |
| `worker-primary-backup-plan.md` | Original primary-backup plan (outdated) |

---

## Contact and Support

**Questions?** Review these documents in order:

1. `current-architecture-vs-goal.md` - Understand what's missing
2. `deployment-comparison.md` - See current vs goal
3. `worker-implementation-plan.md` - Get detailed code examples
4. This document (IMPLEMENTATION_ROADMAP.md) - Quick reference

**Ready to implement!** 🚀
