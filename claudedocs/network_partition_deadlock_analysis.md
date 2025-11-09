# Network Partition Deadlock Analysis

**Date**: 2025-11-09
**Test**: `./scripts/test_network_partition.sh`
**Status**: DEADLOCK CONFIRMED
**Symptom**: Node-1 (partitioned leader) becomes completely unresponsive, unable to serve /status requests

## Executive Summary

The network partition test reveals a **critical deadlock** where the partitioned former leader (node-1) becomes completely unresponsive after reconnection. This is NOT a simple locking issue - it's a complex interaction between:

1. HTTP request handling attempting to acquire `r.mu.RLock()`
2. Multiple replicator goroutines holding or waiting for `r.mu.Lock()`
3. `conn.GetState()` calls that can block indefinitely during network instability

## Test Execution Observations

### Test Flow
1. Node-1 starts as leader (term 1)
2. Node-1 is isolated from network
3. Node-2 wins election, becomes leader (term 2)
4. Key=303 written to node-2
5. Node-1 reconnected to network
6. **Node-1 becomes completely unresponsive**
7. `/status` endpoint hangs indefinitely
8. State never synchronizes

### Evidence

**Node-0 (Follower)**: Operating normally
```
[LOCK] Acquired lock for Raft.mu in LogRequest
[LOCK] Released lock for Raft.mu in LogRequest (logOk)
[LOCK] Acquired lock for Raft.mu in appendEntries
[LOCK] Released lock for Raft.mu in appendEntries
```

**Node-1 (Partitioned Leader)**: DEADLOCKED
```bash
$ curl --max-time 2 http://localhost:8081/status
# Hangs for 2+ seconds, times out
```

**Last logged lock on node-1**:
```
[LOCK] Acquired lock for parent.mu in replicate (process response)
# NO CORRESPONDING RELEASE LOG
```

**Health check continues**: ConnectionManager locks are still working
```
[LOCK] Acquired lock for ConnectionManager.mu in performHealthCheck (update lastContact)
[LOCK] Released lock for ConnectionManager.mu in performHealthCheck (update lastContact)
```

## Root Cause Analysis

### Primary Deadlock: Lock Starvation

The deadlock occurs due to **lock starvation** where:

1. **Multiple Replicator Goroutines** (2 replicators, one per follower):
   - Each runs `replicate()` every 50ms (heartbeat interval)
   - Each acquires `r.mu.Lock()` to process responses
   - Location: `/Users/dzc/distributed-cache/pkg/raft/replicator.go:107-112`

2. **HTTP Request Handler** trying to serve `/status`:
   - Needs `r.mu.RLock()` in `getLeaderData()`
   - Location: `/Users/dzc/distributed-cache/pkg/raft/utils.go:130`
   - Cannot acquire because replicators hold/wait for write lock

3. **The Starvation Pattern**:
   ```
   Time 0ms:   Replicator-1 acquires r.mu.Lock()
   Time 20ms:  Replicator-2 waits for r.mu.Lock()
   Time 25ms:  HTTP /status arrives, waits for r.mu.RLock()
   Time 50ms:  Replicator-1 releases, Replicator-2 acquires immediately
   Time 50ms:  Replicator-1 attempts to re-acquire (new heartbeat cycle)
   Time 100ms: Pattern repeats - readers never get a chance
   ```

### Secondary Issue: Blocking conn.GetState()

**Location**: `/Users/dzc/distributed-cache/pkg/raft/connection_manager.go:196`

The `performHealthCheck()` function calls `conn.GetState()` which:
- Can block for several seconds during network instability
- Happens while holding `cm.mu.RLock()` (lines 181-185)
- Not the primary deadlock, but exacerbates resource contention

```go
// connection_manager.go:175-196
func (cm *ConnectionManager) performHealthCheck() {
    for i := 0; i < cm.totalNodes; i++ {
        // ...
        cm.mu.RLock()  // Line 181
        conn := cm.conns[i]
        cm.mu.RUnlock()  // Line 184

        if conn == nil {
            continue
        }

        state := conn.GetState()  // Line 196 - CAN BLOCK FOR SECONDS

        switch state {
            // ... state handling
        }
    }
}
```

### Tertiary Issue: Excessive Lock Contention from Replicators

**Location**: `/Users/dzc/distributed-cache/pkg/raft/replicator.go:72-132`

Each replicator runs a tight loop every 50ms:

```go
// replicator.go:42-55
func (rep *Replicator) run() {
    heartbeat := time.NewTicker(50 * time.Millisecond)  // 20 times per second
    defer heartbeat.Stop()

    for {
        select {
        case <-rep.stopCh:
            return
        case <-rep.signalCh:
            rep.replicate()  // Acquires r.mu.Lock()
        case <-heartbeat.C:
            rep.replicate()  // Acquires r.mu.Lock() every 50ms
        }
    }
}
```

With 2 replicators, this means:
- 40 lock acquisitions per second (2 replicators × 20 Hz)
- Read locks (like `getLeaderData()`) can starve

### The Perfect Storm

When node-1 is reconnected but still thinks it's a leader:

1. **Replicators still running** (because it hasn't stepped down yet)
2. **Attempting to replicate to followers** every 50ms
3. **Each replication acquires `r.mu.Lock()`** to process responses
4. **Network is unstable**, causing gRPC calls to timeout
5. **Lock contention is maximum**, readers cannot acquire RLock
6. **Test script tries to query `/status`**, but cannot get RLock
7. **Deadlock/livelock**: System appears frozen

## Detailed Lock Acquisition Patterns

### Normal Operation (No Contention)
```
HTTP /status → getLeaderData() → r.mu.RLock() → read → r.mu.RUnlock() → return
```

### During Deadlock (High Contention)
```
Replicator-1 → replicate() → r.mu.Lock() → process → (50ms later) → r.mu.Unlock()
Replicator-2 → replicate() → r.mu.Lock() [WAITING] → process → (50ms later) → r.mu.Unlock()
HTTP /status → getLeaderData() → r.mu.RLock() [WAITING INDEFINITELY] → never acquired
```

###Goroutine Hierarchy During Deadlock

```
Main Goroutine:
  └─ HTTP Server
      └─ handleStatus() → GetStatus() → getLeaderData()
          └─ r.mu.RLock() [BLOCKED - WAITING FOR READERS]

Raft Goroutines:
  ├─ Replicator-0 (to node-0)
  │   └─ replicate() every 50ms
  │       └─ r.mu.Lock() → process response → r.mu.Unlock()
  │           [CYCLING RAPIDLY - PREVENTING READERS]
  │
  ├─ Replicator-2 (to node-2)
  │   └─ replicate() every 50ms
  │       └─ r.mu.Lock() → process response → r.mu.Unlock()
  │           [CYCLING RAPIDLY - PREVENTING READERS]
  │
  └─ Health Check Loop
      └─ performHealthCheck() every 1s
          └─ conn.GetState() [CAN BLOCK FOR SECONDS]
```

## Why Previous Fix Didn't Prevent This

The previous fix in `health_check_callback_deadlock_fix.md` addressed a **different deadlock**:
- Made peer reconnect callback asynchronous
- Prevented health check loop from blocking

But that callback **no longer exists in current code**:
```bash
$ grep -r "RegisterPeerReconnectCallback" pkg/raft/
# No results found
```

The current deadlock is caused by:
1. **Replicator lock contention** (new/different issue)
2. **Partitioned leader not stepping down** (architectural issue)
3. **Reader starvation under high write lock pressure** (sync.RWMutex behavior)

## Why Node-1 Doesn't Step Down

Node-1 remains stuck as a "zombie leader" because:

1. **LogRequest timeout is too short** (500ms)
   - Location: `/Users/dzc/distributed-cache/pkg/raft/replicator.go:96`
   - Network instability after reconnection causes timeouts
   - Node-1 never receives LogRequest from node-2 with higher term

2. **No heartbeat-based step-down logic**
   - A leader that can't reach majority should step down
   - Currently, leaders stay leaders until told otherwise

3. **Replicators keep running**
   - Even when node-1 can't reach majority, replicators continue
   - Causes ongoing lock contention

## Key Insights

### Insight 1: RWMutex Doesn't Guarantee Reader Priority

Go's `sync.RWMutex` does NOT prevent writer starvation of readers. From Go documentation:

> "If a goroutine holds a RLock, a Lock call will block until all readers have released the lock."

But when writers are continuously re-acquiring (every 50ms):
- New Lock() requests queue up
- New RLock() requests wait behind queued Lock() requests
- Result: **readers can starve indefinitely**

### Insight 2: Network Partition Creates Lock Pressure Spike

When node-1 reconnects:
- Network is unstable (connections recovering)
- RPC calls timeout frequently
- Replicators retry aggressively (every 50ms)
- Lock acquisition rate spikes
- Read operations (like /status) cannot get through

### Insight 3: The Test Reveals Production Risk

This deadlock scenario is **highly likely in production**:
- Network partitions are common in distributed systems
- Leaders that lose quorum will experience this
- Any read operation can hang indefinitely
- System appears unresponsive even though internal goroutines work

## Lock Acquisition Chains

### Chain 1: HTTP /status Request (BLOCKED)
```
HTTP Request (goroutine 1)
  → handleStatus() [pkg/api/server.go:326]
      → r.GetStatus() [pkg/raft/raft.go:440]
          → r.mu.RLock() [pkg/raft/raft.go:441]  ← WAITING INDEFINITELY
              Never acquires because writers cycle continuously
```

### Chain 2: Replicator Processing (CYCLING)
```
Replicator Goroutine (goroutine 2)
  → replicate() [pkg/raft/replicator.go:65]
      → peer.LogRequest() [pkg/raft/replicator.go:99]
          → r.mu.Lock() [pkg/raft/replicator.go:107]  ← ACQUIRED
              → Process response, update state
              → r.mu.Unlock() [pkg/raft/replicator.go:110]
              → Sleep 50ms
              → REPEAT (20 times per second)
```

### Chain 3: Health Check (INDEPENDENT)
```
Health Check Goroutine (goroutine 3)
  → performHealthCheck() [pkg/raft/connection_manager.go:175]
      → cm.mu.RLock() [connection_manager.go:181]
          → read conn
          → cm.mu.RUnlock() [connection_manager.go:184]
          → conn.GetState() [connection_manager.go:196]  ← CAN BLOCK
              → May take seconds during network instability
              → But doesn't hold Raft.mu, so not part of primary deadlock
```

## Related Files and Line Numbers

### Primary Deadlock Locations

**Replicator (Write Lock Contention)**:
- `/Users/dzc/distributed-cache/pkg/raft/replicator.go:42-55` - Tight 50ms loop
- `/Users/dzc/distributed-cache/pkg/raft/replicator.go:107-112` - Lock acquisition in process response

**HTTP Handler (Read Lock Starvation)**:
- `/Users/dzc/distributed-cache/pkg/api/server.go:326-339` - handleStatus
- `/Users/dzc/distributed-cache/pkg/raft/raft.go:440-455` - GetStatus
- `/Users/dzc/distributed-cache/pkg/raft/utils.go:128-137` - getLeaderData

**Leadership Transition**:
- `/Users/dzc/distributed-cache/pkg/raft/grpc.go:142-209` - LogRequest handling
- `/Users/dzc/distributed-cache/pkg/raft/utils.go:63-86` - becomeFollower
- `/Users/dzc/distributed-cache/pkg/raft/utils.go:88-117` - becomeLeader

### Secondary Issue Locations

**Blocking conn.GetState()**:
- `/Users/dzc/distributed-cache/pkg/raft/connection_manager.go:175-226` - performHealthCheck
- `/Users/dzc/distributed-cache/pkg/raft/connection_manager.go:196` - Blocking GetState call

**Timeout Configuration**:
- `/Users/dzc/distributed-cache/pkg/raft/replicator.go:96` - 500ms LogRequest timeout

## Staged Fix Plan

### Stage 1: Immediate Deadlock Relief (Low Risk)

**Goal**: Reduce lock contention to allow read operations

**Changes**:

1. **Increase replication heartbeat interval**
   - File: `/Users/dzc/distributed-cache/pkg/raft/replicator.go:42`
   - Change: `50 * time.Millisecond` → `100 * time.Millisecond`
   - Impact: Reduces lock acquisitions from 40/sec to 20/sec
   - Risk: Slightly slower replication (still acceptable)

2. **Increase LogRequest timeout for network stability**
   - File: `/Users/dzc/distributed-cache/pkg/raft/replicator.go:96`
   - Change: `500*time.Millisecond` → `2*time.Second`
   - Impact: Allows partitioned nodes to receive term updates
   - Risk: Slower failure detection (but more reliable)

**Testing**:
```bash
./scripts/test_network_partition.sh
# Should complete successfully
# /status should respond within 1 second
```

**Expected Outcome**: Deadlock frequency reduced by 50%, but not eliminated

---

### Stage 2: Architectural Fix - Leader Step-Down (Medium Risk)

**Goal**: Prevent zombie leaders that can't reach quorum

**Changes**:

1. **Add heartbeat failure counter per replicator**
   - File: `/Users/dzc/distributed-cache/pkg/raft/replicator.go`
   - Add: Track consecutive failures in Replicator struct

2. **Implement quorum-based step-down**
   - File: `/Users/dzc/distributed-cache/pkg/raft/raft.go`
   - Add: New method `checkQuorumAndStepDown()`
   - Logic: If majority of replicators fail for N consecutive heartbeats, step down

3. **Call step-down check from replication failure**
   - File: `/Users/dzc/distributed-cache/pkg/raft/replicator.go:101-104`
   - Add: On LogRequest failure, increment counter and check quorum

**Pseudocode**:
```go
// replicator.go
type Replicator struct {
    consecutiveFailures int
    maxConsecutiveFailures int  // e.g., 5 failures = 250ms at 50ms interval
}

func (rep *Replicator) replicate() {
    // ... existing code ...
    if err != nil {
        rep.consecutiveFailures++
        if rep.consecutiveFailures >= rep.maxConsecutiveFailures {
            rep.parent.signalReplicationFailure(rep.followerId)
        }
        return
    }
    rep.consecutiveFailures = 0  // Reset on success
}

// raft.go
func (r *Raft) signalReplicationFailure(followerID int) {
    r.mu.Lock()
    defer r.mu.Unlock()

    if r.currentRole != Leader {
        return
    }

    failedCount := 0
    for i, rep := range r.replicators {
        if i == r.id || rep == nil {
            continue
        }
        if rep.consecutiveFailures >= rep.maxConsecutiveFailures {
            failedCount++
        }
    }

    majority := (r.totalNodes + 1) / 2
    if failedCount >= majority {
        slog.Warn(fmt.Sprintf("Node %d stepping down: cannot reach majority", r.id))
        r.becomeFollower(r.currentTerm)
    }
}
```

**Testing**:
```bash
# Test 1: Normal partition recovery
./scripts/test_network_partition.sh

# Test 2: Leader isolation
# - Start cluster
# - Identify leader
# - Isolate leader
# - Verify it steps down within 500ms
# - Verify new leader elected
```

**Expected Outcome**: Zombie leaders eliminated, partitioned nodes step down gracefully

---

### Stage 3: Lock Contention Reduction (Higher Risk)

**Goal**: Reduce frequency of lock acquisitions in critical paths

**Option 3A: Reduce Lock Scope in replicate()** (RECOMMENDED)

Minimize time holding `r.mu.Lock()` by extracting state before RPC:

```go
// replicator.go:65-132
func (rep *Replicator) replicate() {
    if !rep.parent.isPeerAvailable(rep.followerId) {
        return
    }

    // Check snapshot need without holding lock during RPC
    rep.parent.mu.RLock()
    nextIndex := rep.parent.sentLengths[rep.followerId]
    needsSnapshot := nextIndex <= rep.parent.snapshotter.lastIndex
    currentTerm := rep.parent.currentTerm
    currentRole := rep.parent.currentRole
    rep.parent.mu.RUnlock()

    if needsSnapshot {
        rep.parent.sendInstallSnapshotRPC(rep.followerId)
        return
    }

    // Make RPC without holding any locks
    args := rep.parent.prepareLogRequestArgs(rep.followerId)
    ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
    defer cancel()

    peer := rep.parent.getPeer(rep.followerId)
    if peer == nil {
        return
    }

    resp, err := peer.LogRequest(ctx, args)  // NO LOCK HELD DURING RPC

    if err != nil {
        rep.consecutiveFailures++
        return
    }
    rep.consecutiveFailures = 0

    // Only acquire lock for state updates (minimal critical section)
    rep.parent.mu.Lock()

    // Re-check term and role after acquiring lock
    if resp.CurrentTerm > int32(rep.parent.currentTerm) {
        rep.parent.becomeFollower(int(resp.CurrentTerm))
        rep.parent.mu.Unlock()
        return
    }

    if rep.parent.currentRole == Leader && int(resp.CurrentTerm) == rep.parent.currentTerm {
        if resp.Success {
            match := int(resp.Ack)
            rep.parent.sentLengths[rep.followerId] = match + 1
            rep.parent.ackedLengths[rep.followerId] = match
            rep.parent.commitLogEntries()
        } else {
            floor := rep.parent.snapshotter.lastIndex + 1
            if rep.parent.sentLengths[rep.followerId] > floor {
                rep.parent.sentLengths[rep.followerId]--
            }
        }
    }

    rep.parent.mu.Unlock()
}
```

**Benefits**:
- Lock held for ~1-5ms (state update only)
- NOT held for ~500-2000ms (RPC duration)
- Dramatic reduction in lock contention

**Risks**:
- State may change between RLock release and Lock acquisition
- Need careful validation of state consistency
- Potential race conditions if not implemented carefully

**Option 3B: Use Separate Locks for Different State**

Split `r.mu` into multiple locks for different concerns:
- `r.logMu` for log operations
- `r.stateMu` for role/term/leader state
- `r.replicationMu` for sentLengths/ackedLengths

**Risks**: HIGH - Requires extensive refactoring and careful deadlock prevention

**Recommendation**: Start with Option 3A

**Testing**:
```bash
# Run test 100 times to catch race conditions
for i in {1..100}; do
    echo "Run $i"
    ./scripts/test_network_partition.sh || exit 1
done

# Run with race detector
go test -race -run TestNetworkPartition ./pkg/raft/
```

**Expected Outcome**: Lock contention reduced by 95%, reads complete in <10ms

---

### Stage 4: Health Check Optimization (Low Risk, Optional)

**Goal**: Prevent `conn.GetState()` from blocking health checks

**Changes**:

1. **Make GetState() call async with timeout**
   - File: `/Users/dzc/distributed-cache/pkg/raft/connection_manager.go:196`
   - Wrap in goroutine with timeout channel

```go
func (cm *ConnectionManager) performHealthCheck() {
    for i := 0; i < cm.totalNodes; i++ {
        if i == cm.selfID {
            continue
        }

        cm.mu.RLock()
        conn := cm.conns[i]
        cm.mu.RUnlock()

        if conn == nil {
            if cm.peerAvailable[i].Load() {
                cm.peerAvailable[i].Store(false)
            }
            continue
        }

        // Non-blocking state check with timeout
        stateChan := make(chan connectivity.State, 1)
        go func() {
            stateChan <- conn.GetState()
        }()

        select {
        case state := <-stateChan:
            // Handle state as before
            switch state {
            case connectivity.Ready, connectivity.Idle:
                if !cm.peerAvailable[i].Load() {
                    cm.peerAvailable[i].Store(true)
                }
                cm.mu.Lock()
                cm.lastContact[i] = time.Now()
                cm.mu.Unlock()
            case connectivity.TransientFailure, connectivity.Shutdown:
                if cm.peerAvailable[i].Load() {
                    cm.peerAvailable[i].Store(false)
                }
                if state == connectivity.Shutdown {
                    cm.reconnectPeer(i)
                }
            }
        case <-time.After(100 * time.Millisecond):
            // GetState() taking too long, skip this peer
            slog.Debug(fmt.Sprintf("Node %d: GetState() timeout for peer %d", cm.selfID, i))
        }
    }
}
```

**Testing**:
```bash
# Simulate slow GetState()
# Should not block other health checks
./scripts/test_network_partition.sh
```

**Expected Outcome**: Health checks complete reliably even with slow connections

---

### Stage 5: Comprehensive Testing (Validation)

**Test Matrix**:

| Test Case | Description | Success Criteria |
|-----------|-------------|------------------|
| Basic partition | Original test script | Completes in <10s |
| Leader isolation | Isolate current leader | Steps down within 500ms |
| Follower isolation | Isolate follower | No impact on cluster |
| Multiple partitions | Isolate majority | No deadlocks, minority steps down |
| Partition healing | Rapid connect/disconnect | State synchronizes correctly |
| High load partition | Partition under write load | No data loss, clean recovery |
| /status during partition | Query all nodes during partition | All respond within 1s |
| Concurrent operations | Reads/writes during partition | Operations complete or fail cleanly |

**Chaos Testing**:
```bash
# Random partition test
for i in {1..50}; do
    # Start cluster
    # Random partition timing
    # Random write operations
    # Verify consistency
done
```

**Performance Benchmarks**:
- /status response time: <10ms (99th percentile)
- Lock contention ratio: <5% of time
- Replication latency: <100ms (normal operation)

---

## Implementation Risk Assessment

| Stage | Risk Level | Confidence | Effort |
|-------|------------|------------|--------|
| Stage 1: Immediate Relief | LOW | HIGH | 5 min |
| Stage 2: Leader Step-Down | MEDIUM | MEDIUM | 2-3 hours |
| Stage 3: Lock Reduction | MEDIUM-HIGH | MEDIUM | 4-6 hours |
| Stage 4: Health Check | LOW | HIGH | 1 hour |
| Stage 5: Testing | LOW | HIGH | 4-8 hours |

**Total Estimated Effort**: 12-18 hours for complete solution

---

## Recommended Implementation Sequence

### Week 1: Quick Wins
1. Implement Stage 1 (immediate relief) - 5 minutes
2. Test basic partition scenario - 30 minutes
3. Implement Stage 2 (leader step-down) - 3 hours
4. Comprehensive testing of stages 1+2 - 2 hours

### Week 2: Structural Improvements
1. Implement Stage 3A (lock scope reduction) - 6 hours
2. Extensive race detection testing - 4 hours
3. Implement Stage 4 (health check optimization) - 1 hour
4. Full chaos testing - 4 hours

### Week 3: Validation
1. Complete test matrix - 4 hours
2. Performance benchmarking - 2 hours
3. Documentation updates - 2 hours

---

## Prevention Strategies for Future

### Code Review Checklist
- [ ] No locks held during RPC calls
- [ ] Lock scope minimized to critical sections only
- [ ] Read operations use RLock, never Lock
- [ ] Timeouts on all blocking operations
- [ ] Leader validates quorum before processing writes

### Monitoring Metrics
- Lock contention ratio (should be <5%)
- Lock hold duration (should be <1ms for reads, <10ms for writes)
- Replication failure rate per follower
- Time-since-last-successful-replication per follower
- /status endpoint response time (should be <10ms p99)

### Testing Standards
- All partition tests must run with `-race` flag
- Chaos tests should run in CI on every PR
- Performance regression tests for lock contention
- Simulate slow networks in tests

---

## Conclusion

This deadlock is a **classic distributed systems problem**:

1. **Lock starvation** under high write pressure
2. **Zombie leaders** that don't step down when losing quorum
3. **Network instability** amplifying concurrency issues

The staged fix plan addresses all three root causes while managing risk through incremental improvements and comprehensive testing. The fixes are designed to be:

- **Independent**: Each stage delivers value on its own
- **Testable**: Clear success criteria for each stage
- **Safe**: Low-risk changes first, higher-risk changes well-tested
- **Comprehensive**: Addresses both symptoms and root causes

**Immediate Action**: Implement Stage 1 and Stage 2 to restore basic functionality. This provides 80% of the benefit with 20% of the risk.
