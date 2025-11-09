# Network Partition Test Regression Diagnosis

**Date**: 2025-11-09
**Test**: `./scripts/test_network_partition.sh`
**Status**: FAILING (previously passed once)
**Commit**: 49705bf (fix/network-partition-deadlock branch)

## Executive Summary

The network partition test is consistently failing because **node-0 (the partitioned former leader) never steps down to follower after being reconnected to the cluster**. This prevents it from accepting log entries from the new leader (node-1), causing state synchronization to fail indefinitely.

## Test Execution Flow

1. ✅ Cluster starts, node-0 becomes leader
2. ✅ Node-0 is isolated from network
3. ✅ Node-1 wins election and becomes new leader (term 2)
4. ✅ Key=303, value=3003 is written to node-1
5. ✅ Node-0 is reconnected to network
6. ❌ **FAILURE**: Node-0 never synchronizes the new data (30 attempts timeout)

## Root Cause Analysis

### Problem: Stale Leader Not Stepping Down

**Node-0 state after reconnection:**
- Still thinks it is the leader (term 1)
- Cannot serve requests because it can't reach majority of followers
- Cannot accept log entries from node-1 (the actual leader in term 2)
- Enters a zombie state: neither leading nor following

**Node-1 state after taking over:**
- Became leader with term 2
- Successfully committed key=303
- Continuously fails to replicate to node-0 with timeouts:
  ```
  Node 1: LogRequest to follower 0 failed: rpc error: code = DeadlineExceeded
  ```

### Evidence from Logs

**Node-0 logs (stale leader):**
```
time=2025-11-09T12:25:39.182Z level=INFO msg="Node 0 became leader with 2/2 peers available"
time=2025-11-09T12:25:44.487Z level=WARN msg="Node 0: LogRequest to follower 1 failed: context deadline exceeded"
time=2025-11-09T12:25:44.487Z level=WARN msg="Node 0: LogRequest to follower 2 failed: context deadline exceeded"
time=2025-11-09T12:25:45.207Z level=INFO msg="Received ForwardGet on node 0 for key 303"
```

**Key observations:**
- Node-0 became leader at term 1
- Lost connection to followers after partition
- Received GET requests for key 303 but has no data (not synchronized)
- **No logs showing it stepped down to follower after reconnection**

**Node-1 logs (new leader):**
```
time=2025-11-09T12:25:44.354Z level=INFO msg="Node 1 became leader with 2/2 peers available"
time=2025-11-09T12:25:45.143Z level=INFO msg="Received Forward on node 1"
time=2025-11-09T12:28:35.611Z level=WARN msg="Node 1: LogRequest to follower 0 failed: context deadline exceeded"
[... continuous timeout warnings ...]
```

**Key observations:**
- Node-1 became leader at term 2
- Successfully processed write request
- Cannot replicate to node-0 (timeouts)

### Why Node-0 Doesn't Step Down

The issue is in `/Users/dzc/distributed-cache/pkg/raft/grpc.go` `LogRequest` function (lines 154-218):

```go
func (r *Raft) LogRequest(ctx context.Context, in *raftpb.LogRequestArgs) (*raftpb.LogResponse, error) {
    leaderId, term, prevIndex, prevTerm, commitLength, suffix := convertLogRequestArgs(in)

    r.mu.Lock()
    if r.currentTerm < term {
        r.currentTerm = term
        r.votedFor = -1
        r.raftElector.ResetTimer()
    }
    if r.currentTerm == term {
        r.currentRole = Follower           // ✅ Should set to Follower
        if r.id != leaderId {
            r.currentLeaderId = leaderId    // ✅ Should update leader
        } else {
            r.currentLeaderId = -1
        }
    }
    // ... rest of logic
}
```

**The problem**: Node-1's LogRequest calls to node-0 are **timing out before reaching the node**, so node-0 never receives the LogRequest that would trigger the step-down logic.

### Why LogRequests Timeout

Looking at `/Users/dzc/distributed-cache/pkg/raft/replicator.go` lines 94-102:

```go
ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
defer cancel()

resp, err := peer.LogRequest(ctx, args)

if err != nil {
    slog.Warn(fmt.Sprintf("Node %d: LogRequest to follower %d failed: %v",
        rep.parent.id, rep.followerId, err))
    return
}
```

**The timeout is too short (500ms)** when node-0 is:
1. Stuck in some blocking operation
2. Has full RPC queue from previous failed attempts
3. Is experiencing network latency after reconnection

### Secondary Issue: Potential Deadlock

When we tried to query node-0's `/status` endpoint, **the request hung indefinitely**. This indicates a mutex deadlock or blocking operation.

Looking at `/Users/dzc/distributed-cache/pkg/api/server.go` line 326-339:

```go
func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
    status := s.raft.GetStatus()  // Calls GetStatus()
    // ... encode response
}
```

And `/Users/dzc/distributed-cache/pkg/raft/raft.go` lines 413-424:

```go
func (r *Raft) GetStatus() ClusterStatus {
    r.mu.RLock()          // Acquires read lock
    defer r.mu.RUnlock()

    return ClusterStatus{
        NodeID:     r.id,
        Role:       string(r.currentRole),
        Term:       r.currentTerm,
        LeaderID:   r.currentLeaderId,
        TotalNodes: len(r.peers) + 1,
    }
}
```

**The /status endpoint hangs**, which means:
- Either `r.mu.RLock()` is blocked waiting for a write lock
- Or there's a goroutine holding the write lock indefinitely

## Why It Worked Once

The test may have passed once due to **timing race conditions**:
- Node-0 might have received a successful LogRequest before the partition fully healed
- Network timing was favorable (faster reconnection)
- Election timing was different (node-1 won election faster)
- First run had no stale gRPC connection state

## Current Behavior vs Expected Behavior

### Current (Broken)
1. Node-0 becomes leader (term 1)
2. Node-0 partitioned
3. Node-1 becomes leader (term 2)
4. Node-0 reconnected
5. **Node-0 stays as stale leader, never receives LogRequest**
6. State never synchronizes

### Expected (Correct)
1. Node-0 becomes leader (term 1)
2. Node-0 partitioned
3. Node-1 becomes leader (term 2)
4. Node-0 reconnected
5. **Node-1 sends LogRequest to node-0 with term 2**
6. **Node-0 receives LogRequest, sees higher term, steps down to follower**
7. **Node-0 accepts log entries from node-1**
8. State synchronizes successfully

## Solutions to Consider

### Option 1: Passive Step-Down via Heartbeat (Recommended)

Add heartbeat-based leader detection. If a leader cannot reach majority for N consecutive heartbeats, step down:

```go
// In heartbeat loop
if r.currentRole == Leader {
    if consecutiveFailures >= maxFailures {
        r.mu.Lock()
        r.currentRole = Follower
        r.currentLeaderId = -1
        r.mu.Unlock()
    }
}
```

### Option 2: Increase RPC Timeout

Increase the LogRequest timeout from 500ms to 2-3 seconds to allow network stabilization:

```go
// In replicator.go
ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
```

### Option 3: Active Peer Discovery After Reconnection

When a partitioned node reconnects, actively query peers for current term/leader:

```go
func (r *Raft) detectNetworkReconnection() {
    // On reconnection, send VoteRequest with term+1
    // If rejected with higher term, step down immediately
}
```

### Option 4: Combination Approach (Best)

Combine multiple strategies:
1. Heartbeat-based step-down for stale leaders
2. Increased RPC timeout (1-2 seconds)
3. Active leader validation on reconnection
4. Fix any mutex deadlocks in status/monitoring paths

## Immediate Next Steps

1. **Fix the deadlock** in GetStatus or related mutex operations
2. **Implement heartbeat-based step-down** to handle stale leader scenarios
3. **Increase LogRequest timeout** to 2 seconds for network instability
4. **Add comprehensive logging** around leadership transitions
5. **Validate the fix** with multiple test runs (not just one)

## Relevant Files

- `/Users/dzc/distributed-cache/pkg/raft/grpc.go` - LogRequest handling (lines 154-218)
- `/Users/dzc/distributed-cache/pkg/raft/replicator.go` - Replication logic (lines 65-126)
- `/Users/dzc/distributed-cache/pkg/raft/raft.go` - GetStatus deadlock (lines 413-424)
- `/Users/dzc/distributed-cache/pkg/api/server.go` - HTTP status handler (lines 326-339)
- `/Users/dzc/distributed-cache/scripts/test_network_partition.sh` - Test script

## Test Failure Pattern

```
Attempt 1-30: Checking for key=303 on raft-node-0...
--- 🔥 FAILURE: Timed out waiting for state reconciliation on 'raft-node-0'. ---
```

Every single attempt times out because node-0 never accepts the log entry containing key=303.
