# Stale Leader Detection Fix - Implementation Plan

## Problem Summary

**Test:** `./scripts/test_network_partition.sh` hangs at GET request after network partition heal

**Symptom:** GET request to previously-partitioned leader (node 2) hangs indefinitely

**Root Cause:** Stale leader problem - node 2 believes it's still leader after network reconnection

## Detailed Analysis

### Timeline of Events

```
1. Node 2 is leader with full cluster connectivity
2. Network partition: node 2 isolated from cluster
3. Nodes 0 and 1 elect new leader (node 0)
4. Write key=303 to new leader (node 0) - succeeds
5. Network heals: node 2 reconnects to cluster
6. GET request to node 2 for key=303
   ❌ HANGS: Node 2 thinks it's leader, tries to serve from stale state
```

### Why Current Fix Doesn't Work

The fix in commit `49705bf` added `isPeerAvailable()` checks:

```go
// pkg/raft/grpc.go:91-116
func (r *Raft) ForwardGet(ctx context.Context, req *raftpb.GetRequest) {
    isLeader, leaderID := r.getLeaderData()

    if !isLeader {  // ❌ Node 2 thinks isLeader=true!
        // This branch NEVER executes on stale leader
        if !r.isPeerAvailable(leaderID) {
            return nil, fmt.Errorf("leader %d is not available", leaderID)
        }
        // ... forward to leader
    }

    // ❌ Stale leader executes this path
    value := r.application.GetValue(int(req.Key))
    return &raftpb.GetResponse{...}
}
```

**The Problem:**
- `isPeerAvailable()` check is only in the `if !isLeader` branch
- Stale leader never enters that branch because `isLeader=true`
- Stale leader serves reads from outdated state without verifying leadership

### Core Issues

1. **No Quorum Verification for Reads**
   - Leader serves GET without checking if it can reach quorum
   - Stale leader doesn't know it lost leadership

2. **Slow Leadership Detection**
   - Health check interval: 10 seconds (default)
   - Election timeout must expire for stale leader to step down
   - Test script times out before detection completes

3. **Split-Brain Risk**
   - Two nodes can believe they're leader simultaneously
   - Network partition → reconnect creates temporary dual-leader state

## Solution Options

### Option A: Quorum-Based Read Verification (Recommended)

**Concept:** Leader must verify it can reach quorum before serving reads

**Pros:**
- Guarantees linearizable reads
- Fast failure detection (no waiting for health checks)
- Follows Raft paper recommendations
- Prevents stale reads

**Cons:**
- Adds network round-trip for every GET
- Increased read latency (~50-100ms)

**Implementation Complexity:** Medium

---

### Option B: Read Index Protocol

**Concept:** Leader verifies leadership by checking log replication state

**Pros:**
- More efficient than full quorum check
- Still provides linearizability guarantees
- Batch multiple reads in single verification

**Cons:**
- More complex implementation
- Still adds latency to reads

**Implementation Complexity:** High

---

### Option C: Leadership Lease with Fast Failover

**Concept:** Leader maintains time-based lease, must renew before expiry

**Pros:**
- Fast local reads (no network round-trip)
- Strong consistency guarantees with clock sync
- Industry standard (used by etcd, CockroachDB)

**Cons:**
- Requires synchronized clocks
- Complex lease management
- Clock skew can cause split-brain

**Implementation Complexity:** High

---

### Option D: Immediate Step-Down on Reconnection (Quick Fix)

**Concept:** When previously-unavailable peers become available, verify leadership

**Pros:**
- Minimal code changes
- Fast to implement
- Solves immediate test failure

**Cons:**
- Still has window where stale reads possible
- Doesn't fully solve split-brain
- Band-aid solution, not comprehensive

**Implementation Complexity:** Low

## Recommended Approach

**Phase 1 (Immediate):** Option D - Step-down on peer reconnection
**Phase 2 (Follow-up):** Option A - Quorum verification for reads

### Why This Approach?

1. **Quick win:** Fix the test script failure immediately
2. **Incremental improvement:** Build toward proper solution
3. **Risk management:** Small changes, easy to validate
4. **Performance consideration:** Defer read latency increase until properly benchmarked

## Implementation Plan

### Stage 1: Add Leadership Verification Hook

**Goal:** Detect when previously-unavailable peers reconnect

**Files to modify:**
- `pkg/raft/connection_manager.go`
- `pkg/raft/raft.go`

**Changes:**

1. Add callback mechanism in ConnectionManager:
```go
type PeerReconnectCallback func(peerID int)

type ConnectionManager struct {
    // ... existing fields
    onPeerReconnect PeerReconnectCallback
}

func (cm *ConnectionManager) SetPeerReconnectCallback(cb PeerReconnectCallback) {
    cm.onPeerReconnect = cb
}
```

2. Trigger callback in `performHealthCheck()`:
```go
func (cm *ConnectionManager) performHealthCheck() {
    for i := 0; i < cm.totalNodes; i++ {
        // ... existing health check logic

        if !cm.peerAvailable[i].Load() && (state == connectivity.Ready || state == connectivity.Idle) {
            slog.Info(fmt.Sprintf("Node %d: Peer %d reconnected", cm.selfID, i))
            cm.peerAvailable[i].Store(true)

            // NEW: Trigger reconnect callback
            if cm.onPeerReconnect != nil {
                cm.onPeerReconnect(i)
            }
        }
    }
}
```

### Stage 2: Implement Leadership Verification

**Goal:** Leader verifies it can reach quorum when critical peers reconnect

**Files to modify:**
- `pkg/raft/raft.go`

**Changes:**

1. Add leadership verification method:
```go
// verifyLeadership checks if this node can still reach quorum
// Returns true if leadership is confirmed, false otherwise
func (r *Raft) verifyLeadership() bool {
    r.mu.RLock()
    isLeader := r.currentRole == Leader
    r.mu.RUnlock()

    if !isLeader {
        return false
    }

    // Count available peers
    availableCount := r.getAvailablePeerCount()
    quorum := (r.totalNodes / 2) + 1

    // Self + available peers must form quorum
    if availableCount + 1 < quorum {
        slog.Warn(fmt.Sprintf("Node %d: Cannot reach quorum (%d/%d available), stepping down",
            r.id, availableCount+1, quorum))
        r.stepDownAsLeader()
        return false
    }

    return true
}

func (r *Raft) stepDownAsLeader() {
    r.mu.Lock()
    defer r.mu.Unlock()

    if r.currentRole == Leader {
        slog.Info(fmt.Sprintf("Node %d: Stepping down from leader role", r.id))
        r.currentRole = Follower
        r.currentLeaderId = -1
        r.votedFor = -1
    }
}
```

2. Register callback in Raft initialization:
```go
func NewRaft(...) *Raft {
    // ... existing initialization

    if r.connMgr != nil {
        r.connMgr.SetPeerReconnectCallback(func(peerID int) {
            r.handlePeerReconnect(peerID)
        })
    }

    return r
}

func (r *Raft) handlePeerReconnect(peerID int) {
    r.mu.RLock()
    isLeader := r.currentRole == Leader
    r.mu.RUnlock()

    if isLeader {
        slog.Info(fmt.Sprintf("Node %d: Peer %d reconnected, verifying leadership", r.id, peerID))
        r.verifyLeadership()
    }
}
```

### Stage 3: Add Quorum Check to ForwardGet

**Goal:** Leader verifies quorum before serving reads

**Files to modify:**
- `pkg/raft/grpc.go`

**Changes:**

```go
func (r *Raft) ForwardGet(ctx context.Context, req *raftpb.GetRequest) (*raftpb.GetResponse, error) {
    slog.Info(fmt.Sprintf("Received ForwardGet on node %d for key %d", r.id, req.Key))

    isLeader, leaderID := r.getLeaderData()

    if !isLeader {
        if leaderID < 0 {
            return nil, fmt.Errorf("no leader known")
        }

        if !r.isPeerAvailable(leaderID) {
            return nil, fmt.Errorf("leader %d is not available", leaderID)
        }

        if ctx.Value(forwardHopKey{}) != nil {
            return nil, fmt.Errorf("redirect loop detected")
        }

        ctx = context.WithValue(ctx, forwardHopKey{}, true)
        peer := r.getPeer(leaderID)
        if peer == nil {
            return nil, fmt.Errorf("no peer for leader %d", leaderID)
        }
        return peer.ForwardGet(ctx, req)
    }

    // NEW: Leader must verify it can reach quorum before serving read
    if !r.verifyLeadership() {
        return nil, fmt.Errorf("node %d lost leadership, cannot serve read", r.id)
    }

    // Serve the read
    value := r.application.GetValue(int(req.Key))
    return &raftpb.GetResponse{
        Key:   req.Key,
        Value: int32(value),
        Found: true,
    }, nil
}
```

### Stage 4: Enhanced Testing

**Goal:** Verify fix works and doesn't break other scenarios

**Test Scenarios:**

1. **Network Partition Recovery** (existing test)
   ```bash
   ./scripts/test_network_partition.sh
   ```
   Expected: GET succeeds after partition heal

2. **Normal Operation** (verify no regression)
   ```bash
   # Start cluster
   docker compose up -d

   # Write to any node
   curl -X POST -d '{"key": 1, "value": 100}' http://localhost:8080/kv

   # Read from all nodes
   curl http://localhost:8080/kv/1  # Should succeed
   curl http://localhost:8081/kv/1  # Should succeed
   curl http://localhost:8082/kv/1  # Should succeed
   ```

3. **Leader Failure** (ensure election still works)
   ```bash
   # Start cluster, identify leader
   # Kill leader container
   docker stop raft-node-X

   # Verify new leader elected
   # Write should succeed to remaining nodes
   ```

4. **Quorum Loss** (verify proper error handling)
   ```bash
   # Stop 2 of 3 nodes
   docker stop raft-node-1 raft-node-2

   # Writes to remaining node should fail (no quorum)
   curl -X POST -d '{"key": 1, "value": 100}' http://localhost:8080/kv
   # Expected: Error (cannot reach quorum)
   ```

## Testing Strategy

### Pre-Implementation Validation
1. ✅ Confirm test fails with current code
2. ✅ Capture detailed logs showing stale leader behavior
3. ✅ Document exact hang point

### During Implementation
1. Implement Stage 1 → test callback fires on reconnection
2. Implement Stage 2 → test leader steps down when quorum lost
3. Implement Stage 3 → test reads fail on stale leader
4. Integration test → full network partition scenario

### Post-Implementation Validation
1. Network partition test passes
2. No regressions in normal operation
3. Leader election still works
4. Proper error handling on quorum loss

## Rollback Plan

If issues arise:

1. **Stage 3 problems:** Remove quorum check from ForwardGet, keep stages 1-2
2. **Stage 2 problems:** Remove leadership verification, keep stage 1 callback
3. **Stage 1 problems:** Revert all changes, original behavior restored

Each stage is independently testable and can be rolled back without breaking prior stages.

## Success Criteria

1. ✅ `./scripts/test_network_partition.sh` completes successfully
2. ✅ No stale reads served after network partition
3. ✅ Leader correctly steps down when losing quorum
4. ✅ Normal read/write operations unaffected
5. ✅ No new deadlocks introduced

## Performance Considerations

**Current state:**
- GET latency: ~10-20ms (local read)

**After Stage 3:**
- GET latency: ~10-20ms (quorum check is local, just counting available peers)
- Only adds microseconds (atomic load of peer availability flags)

**No network round-trip required** because:
- We're checking `peerAvailable[]` flags (local atomic bools)
- Not doing actual network calls to verify
- Health check loop maintains availability state

## Future Enhancements

After this fix is validated:

1. **Read Index Protocol:** More sophisticated quorum verification
2. **Leadership Leases:** Reduce overhead for high-frequency reads
3. **Follower Reads:** Allow stale reads with bounded staleness
4. **Metrics:** Track leadership transitions and step-downs

## Timeline Estimate

- **Stage 1:** 30 minutes (callback mechanism)
- **Stage 2:** 45 minutes (leadership verification)
- **Stage 3:** 30 minutes (quorum check in ForwardGet)
- **Testing:** 1 hour (comprehensive validation)

**Total:** ~3 hours for complete implementation and testing

## References

- Raft Paper Section 8: Client Interaction and Linearizable Reads
- Original issue: Network partition test hangs on GET
- Related commit: `49705bf` (incomplete fix attempt)
- Test script: `./scripts/test_network_partition.sh`
