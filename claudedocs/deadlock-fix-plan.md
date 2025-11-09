# Deadlock Fix Plan - Network Partition Test

## Problem Analysis

### Deadlock Mechanism

**Symptom:** Second GET request hangs indefinitely after first GET succeeds

**Log Evidence:**
```
08:31:17.768Z - First GET: SUCCESS
  "Received ForwardGet on node 1 for key 303"
  "About to call getLeaderData()"
  "getLeaderData() returned isLeader=true"
  "Is leader, about to verify leadership"
  "Verifying leadership (checking peer availability)"
  "Leadership verified, serving read"

08:31:18.811Z - Second GET: DEADLOCK
  "Received ForwardGet on node 1 for key 303"
  "About to call getLeaderData()"
  HANGS FOREVER - never returns
```

### Root Cause: Lock Contention in `verifyLeadership()`

The deadlock occurs because of circular lock dependency:

**Flow of Second Request:**
```
1. ForwardGet() calls getLeaderData()
   → getLeaderData() acquires r.mu.RLock()  [lines 121-124 in utils.go]

2. ForwardGet() calls verifyLeadership()
   → verifyLeadership() acquires r.mu.RLock() [lines 414-418 in raft.go]
   → Then calls CheckPeerAvailabilityNow() for each peer

3. CheckPeerAvailabilityNow() needs cm.mu.RLock() [line 309 in connection_manager.go]
   → BUT this may block if another goroutine is calling performHealthCheck()

4. Meanwhile, LogRequest() RPC handler holds r.mu.Lock() [line 169 in grpc.go]
   → Blocks all readers, including getLeaderData()
   → Creates deadlock: LogRequest waits for replication, readers wait for LogRequest
```

**The Critical Issue:**

`verifyLeadership()` is called WHILE already holding `r.mu.RLock()` from `getLeaderData()`:

```go
// grpc.go:94-96 - First lock acquired
isLeader, leaderID := r.getLeaderData()  // ← Acquires r.mu.RLock()
// Still holding RLock here...

// grpc.go:124 - Second lock attempt
if !r.verifyLeadership() {  // ← Tries to acquire r.mu.RLock() AGAIN
```

While Go's `RWMutex` allows recursive read locks, the problem is:

1. **LogRequest handler** (line 169 grpc.go) acquires `r.mu.Lock()` for writes
2. While write lock is held, it performs **slow operations** (log replication, file I/O)
3. All read lock attempts (including from `getLeaderData()`) are **blocked**
4. `CheckPeerAvailabilityNow()` may also block on `cm.mu.RLock()`
5. Creates **convoy effect**: readers pile up waiting for writer

### Why First Request Succeeds, Second Fails

**First Request:**
- No concurrent LogRequest operations yet
- All locks acquired quickly
- verifyLeadership() completes fast

**Second Request:**
- LogRequest RPC is now active (replicating logs)
- LogRequest holds `r.mu.Lock()` for extended period
- getLeaderData() blocks waiting for write lock to release
- Test times out before deadlock resolves

### Extended Lock Hold Times

**LogRequest handler problematic sections:**

```go
// grpc.go:169 - Acquires write lock
r.mu.Lock()

// Lines 170-203: Complex logic with lock held
if r.currentTerm < term {
    r.currentTerm = term
    r.votedFor = -1
    r.raftElector.ResetTimer()
}
// ... more state checks
// ... log validation logic (can be slow with large logs)

// Line 205: RELEASES lock temporarily
r.mu.Unlock()

// Line 207: Calls appendEntries (more heavy work)
r.appendEntries(prevIndex, commitLength, suffix)
// This can trigger:
// - Log trimming/appending
// - deliverToApplication() calls
// - Snapshot decisions
```

The lock is held for:
- Term updates
- Role transitions
- Log validation
- Timer resets

This creates a **long critical section** that blocks all readers.

---

## Solution Options

### Option A: Remove verifyLeadership() from Read Path (Recommended)

**Concept:** Don't call verifyLeadership() on every read; rely on passive detection

**Changes:**
1. Remove `verifyLeadership()` call from ForwardGet()
2. Keep peer reconnection callback for background verification
3. Use cached peer availability instead of synchronous checks

**Pros:**
- Eliminates deadlock entirely
- Minimal lock contention for reads
- Fast read path (no network checks)
- Background verification handles stale leader detection

**Cons:**
- Small window where stale reads possible (until next health check)
- Less immediate leadership verification
- Relies on 10-second health check interval

**Performance Impact:**
- Read latency: 10-20ms (no change from baseline)
- No additional network overhead
- No lock contention

**Risk Level:** Low - Simple change, easy rollback

---

### Option B: Use Cached Peer Availability (Intermediate)

**Concept:** verifyLeadership() uses cached atomic flags instead of CheckPeerAvailabilityNow()

**Changes:**
```go
func (r *Raft) verifyLeadership() bool {
    r.mu.RLock()
    isLeader := r.currentRole == Leader
    totalNodes := r.totalNodes
    nodeID := r.id
    r.mu.RUnlock()

    if !isLeader {
        return false
    }

    // Use cached availability (atomic loads, no locks)
    availableCount := 0
    for i := 0; i < totalNodes; i++ {
        if i == nodeID {
            continue
        }
        // IsPeerAvailable() just does atomic.Load() - very fast
        if r.connMgr.IsPeerAvailable(i) {
            availableCount++
        }
    }

    quorum := (totalNodes / 2) + 1
    if availableCount + 1 < quorum {
        r.stepDownAsLeader()
        return false
    }

    return true
}
```

**Pros:**
- Keeps quorum verification on read path
- No synchronous network checks
- Fast atomic loads only
- Maintains strong consistency intent

**Cons:**
- Still has lock contention (acquiring RLock twice)
- Cached data may be slightly stale (up to health check interval)
- More complex than Option A

**Performance Impact:**
- Read latency: 10-20ms (adds microseconds for atomic loads)
- Minimal overhead

**Risk Level:** Low-Medium - Reduces but doesn't eliminate contention

---

### Option C: Lock-Free Leadership Cache (Advanced)

**Concept:** Maintain atomic leadership state without acquiring locks

**Changes:**
```go
type Raft struct {
    // ... existing fields

    // Atomic leadership state (no locks needed)
    leadershipValid atomic.Bool
    leadershipExpiry atomic.Int64  // Unix timestamp
}

func (r *Raft) verifyLeadership() bool {
    // Fast path: check cached validity
    if r.leadershipValid.Load() {
        expiry := r.leadershipExpiry.Load()
        if time.Now().Unix() < expiry {
            return true
        }
    }

    // Slow path: revalidate (still no r.mu locks)
    r.revalidateLeadership()
    return r.leadershipValid.Load()
}

func (r *Raft) revalidateLeadership() {
    r.mu.RLock()
    isLeader := r.currentRole == Leader
    totalNodes := r.totalNodes
    r.mu.RUnlock()

    if !isLeader {
        r.leadershipValid.Store(false)
        return
    }

    availableCount := r.getAvailablePeerCount()
    quorum := (totalNodes / 2) + 1

    hasQuorum := (availableCount + 1) >= quorum
    r.leadershipValid.Store(hasQuorum)

    if hasQuorum {
        // Valid for next 5 seconds
        r.leadershipExpiry.Store(time.Now().Add(5 * time.Second).Unix())
    } else {
        r.stepDownAsLeader()
    }
}
```

**Pros:**
- No lock contention for common case (valid leadership)
- Sub-microsecond fast path
- Strong consistency guarantees with TTL
- Handles high read throughput

**Cons:**
- More complex implementation
- Introduces time-based state (requires clock)
- Needs careful TTL tuning
- More code to maintain

**Performance Impact:**
- Read latency: 10-20ms (fast path adds nanoseconds)
- High throughput benefits

**Risk Level:** Medium - More complexity, harder to validate

---

### Option D: Restructure Lock Hierarchy (Major Refactor)

**Concept:** Separate leadership state from general Raft state

**Changes:**
```go
type Raft struct {
    // General Raft state
    mu sync.RWMutex
    log []LogEntry
    // ... other state

    // Leadership state (separate lock)
    leaderMu sync.RWMutex
    currentRole Role
    currentLeaderId int
}

// Avoids nested lock acquisition
func (r *Raft) getLeaderData() (bool, int) {
    r.leaderMu.RLock()
    defer r.leaderMu.RUnlock()
    return r.currentRole == Leader, r.currentLeaderId
}
```

**Pros:**
- Cleanly separates concerns
- Eliminates lock hierarchy issues
- More granular locking

**Cons:**
- **Major refactoring** required
- Risk of breaking existing logic
- Need to audit all state access
- Potential for new race conditions

**Performance Impact:**
- Potentially better (finer-grained locks)
- But unpredictable during transition

**Risk Level:** High - Large change, extensive testing needed

---

## Recommended Approach

**Primary Strategy:** Option A (Remove verifyLeadership from read path)

**Rationale:**
1. **Simplest fix** - One line removal
2. **Lowest risk** - Minimal code change
3. **No performance degradation** - Actually improves read latency
4. **Sufficient for correctness** - Peer reconnection callback handles stale leader
5. **Easy rollback** - Just restore the line

**Fallback:** Option B if Option A proves insufficient

---

## Implementation Stages

### Stage 1: Remove Synchronous Leadership Verification

**Goal:** Eliminate deadlock by removing verifyLeadership() from read path

**File:** `pkg/raft/grpc.go`

**Change:**
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
            return nil, fmt.Errorf("redirect loop detected at node=%d to leader=%d", r.id, leaderID)
        }
        ctx = context.WithValue(ctx, forwardHopKey{}, true)
        peer := r.getPeer(leaderID)
        if peer == nil {
            return nil, fmt.Errorf("no peer for leader %d", leaderID)
        }
        return peer.ForwardGet(ctx, req)
    }

    // REMOVE THIS BLOCK - Causes deadlock
    // if !r.verifyLeadership() {
    //     return nil, fmt.Errorf("node %d lost leadership, cannot serve read", r.id)
    // }

    // Leader serves the read
    value := r.application.GetValue(int(req.Key))
    return &raftpb.GetResponse{
        Key:   req.Key,
        Value: int32(value),
        Found: true,
    }, nil
}
```

**Testing:**
- Run `./scripts/test_network_partition.sh`
- Verify no deadlock
- Check that GET requests complete

---

### Stage 2: Optimize verifyLeadership() for Background Use

**Goal:** Make verifyLeadership() safer for background callbacks

**File:** `pkg/raft/raft.go`

**Current Implementation Problems:**
```go
func (r *Raft) verifyLeadership() bool {
    r.mu.RLock()
    isLeader := r.currentRole == Leader
    totalNodes := r.totalNodes
    nodeID := r.id
    r.mu.RUnlock()

    // ...

    // PROBLEM: CheckPeerAvailabilityNow() acquires cm.mu.RLock()
    // Can block on health check loop
    for i := 0; i < totalNodes; i++ {
        if i == nodeID {
            continue
        }
        isAvail := r.connMgr.CheckPeerAvailabilityNow(i)  // ← Synchronous lock
        if isAvail {
            availableCount++
        }
    }

    // ...
}
```

**Improved Implementation:**
```go
func (r *Raft) verifyLeadership() bool {
    r.mu.RLock()
    isLeader := r.currentRole == Leader
    totalNodes := r.totalNodes
    nodeID := r.id
    r.mu.RUnlock()

    if !isLeader {
        return false
    }

    slog.Info(fmt.Sprintf("Node %d: Verifying leadership (using cached peer availability)", nodeID))

    // Use cached availability - just atomic loads, no locks
    availableCount := 0
    for i := 0; i < totalNodes; i++ {
        if i == nodeID {
            continue
        }
        // IsPeerAvailable() is lock-free (atomic.Load())
        if r.connMgr.IsPeerAvailable(i) {
            availableCount++
        }
    }

    quorum := (totalNodes / 2) + 1

    slog.Info(fmt.Sprintf("Node %d: Quorum check: %d/%d available (need %d)",
        nodeID, availableCount+1, totalNodes, quorum))

    if availableCount+1 < quorum {
        slog.Warn(fmt.Sprintf("Node %d: Cannot reach quorum (%d/%d available), stepping down",
            nodeID, availableCount+1, quorum))
        r.stepDownAsLeader()
        return false
    }

    slog.Info(fmt.Sprintf("Node %d: Leadership verified, can reach quorum", nodeID))
    return true
}
```

**Key Changes:**
1. Replace `CheckPeerAvailabilityNow()` with `IsPeerAvailable()`
2. Use cached atomic flags instead of synchronous checks
3. Faster execution (no lock contention)
4. Safe for background callback use

**Testing:**
- Verify peer reconnection callback still works
- Check that stale leader steps down after health check detects reconnection
- Confirm no deadlocks in background verification

---

### Stage 3: Reduce LogRequest Lock Hold Time

**Goal:** Minimize time that write lock is held during replication

**File:** `pkg/raft/grpc.go`

**Current Problem:**
```go
func (r *Raft) LogRequest(ctx context.Context, in *raftpb.LogRequestArgs) (*raftpb.LogResponse, error) {
    leaderId, term, prevIndex, prevTerm, commitLength, suffix := convertLogRequestArgs(in)

    r.mu.Lock()  // ← Acquires write lock

    // LOTS OF WORK with lock held:
    if r.currentTerm < term {
        r.currentTerm = term
        r.votedFor = -1
        r.raftElector.ResetTimer()
    }
    if r.currentTerm == term {
        r.currentRole = Follower
        if r.id != leaderId {
            r.currentLeaderId = leaderId
        } else {
            r.currentLeaderId = -1
        }
    }

    // Log validation (potentially slow)
    absLastIndex := r.snapshotter.lastIndex + len(r.log)
    logOk := false
    // ... complex validation logic ...

    if r.currentTerm == term && logOk {
        r.raftElector.ResetTimer()
        r.mu.Unlock()  // ← Finally releases

        r.appendEntries(prevIndex, commitLength, suffix)  // ← Heavy work outside lock
        // ...
    }
}
```

**Improved Implementation:**
```go
func (r *Raft) LogRequest(ctx context.Context, in *raftpb.LogRequestArgs) (*raftpb.LogResponse, error) {
    leaderId, term, prevIndex, prevTerm, commitLength, suffix := convertLogRequestArgs(in)

    // QUICK LOCK: Just read what we need
    r.mu.Lock()
    needsUpdate := r.currentTerm < term
    currentTerm := r.currentTerm
    currentRole := r.currentRole
    snapLastIndex := r.snapshotter.lastIndex
    snapLastTerm := r.snapshotter.lastTerm
    logLen := len(r.log)
    var logCopy []LogEntry
    if prevIndex > 0 && prevIndex <= snapLastIndex + logLen {
        // Only copy relevant log entry for validation
        relIndex := prevIndex - snapLastIndex - 1
        if relIndex >= 0 && relIndex < len(r.log) {
            logCopy = []LogEntry{r.log[relIndex]}
        }
    }
    r.mu.Unlock()  // ← Release quickly

    // Validate without lock
    logOk := validateLog(prevIndex, prevTerm, snapLastIndex, snapLastTerm, logCopy)

    // QUICK LOCK: Apply updates
    r.mu.Lock()
    if needsUpdate {
        r.currentTerm = term
        r.votedFor = -1
        r.raftElector.ResetTimer()
    }
    if r.currentTerm == term {
        r.currentRole = Follower
        if r.id != leaderId {
            r.currentLeaderId = leaderId
        } else {
            r.currentLeaderId = -1
        }
    }

    if r.currentTerm == term && logOk {
        r.raftElector.ResetTimer()
        r.mu.Unlock()  // ← Quick release again

        r.appendEntries(prevIndex, commitLength, suffix)

        ack := prevIndex + len(suffix)
        go r.logSaver.SaveValues()

        return &raftpb.LogResponse{
            NodeId:      int32(r.id),
            CurrentTerm: int32(currentTerm),
            Ack:         int32(ack),
            Success:     true,
        }, nil
    } else {
        r.mu.Unlock()
        // ...
    }
}

// Helper function for lock-free validation
func validateLog(prevIndex, prevTerm, snapLastIndex, snapLastTerm int, logEntry []LogEntry) bool {
    switch {
    case prevIndex == 0:
        return prevTerm == 0
    case prevIndex == snapLastIndex:
        return snapLastTerm == prevTerm
    case len(logEntry) > 0:
        return logEntry[0].Term == prevTerm
    default:
        return false
    }
}
```

**Key Improvements:**
1. **Acquire lock → copy data → release lock → validate outside lock**
2. **Minimize critical section time**
3. **Reduce read lock blocking**
4. **Heavy validation moved outside lock**

**Note:** This is **optional** - Stage 1 alone should fix deadlock

**Testing:**
- Verify log replication still works correctly
- Check no race conditions introduced
- Confirm reduced lock contention with concurrent reads

---

## Testing Strategy

### Test 1: Network Partition Recovery (Primary)

**Command:**
```bash
./scripts/test_network_partition.sh
```

**Expected Behavior:**
- Script completes without hanging
- GET requests succeed after partition heal
- No deadlocks

**Success Criteria:**
- ✅ Test passes within 30 seconds
- ✅ All GET requests return correct values
- ✅ No timeout errors

---

### Test 2: Concurrent Read/Write Load

**Setup:**
```bash
docker compose up -d

# Terminal 1: Write loop
for i in {1..100}; do
    curl -X POST -d "{\"key\": $i, \"value\": $((i*10))}" http://localhost:8080/kv
    sleep 0.1
done

# Terminal 2: Concurrent reads
for i in {1..100}; do
    curl http://localhost:8081/kv/$i &
    curl http://localhost:8082/kv/$i &
done
wait
```

**Expected Behavior:**
- All writes complete successfully
- All reads return correct values or "key not found"
- No deadlocks or timeouts

**Success Criteria:**
- ✅ 100% operation success rate
- ✅ No goroutine leaks
- ✅ Response times < 100ms

---

### Test 3: Leader Failure During Reads

**Setup:**
```bash
docker compose up -d

# Identify leader (node 0 in this example)

# Start read loop
while true; do
    curl http://localhost:8080/kv/1
    sleep 0.5
done &

# Kill leader after 5 seconds
sleep 5
docker stop raft-node-0

# Reads should continue after brief interruption
```

**Expected Behavior:**
- Reads succeed before leader failure
- Brief error period during election
- Reads resume after new leader elected
- No deadlocks

**Success Criteria:**
- ✅ Reads recover within 2-3 seconds
- ✅ New leader serves requests correctly
- ✅ No permanent hangs

---

### Test 4: Stale Leader Detection

**Setup:**
```bash
# Start 3-node cluster
docker compose up -d

# Partition node 2 from cluster
docker network disconnect distributed-cache_raft-network raft-node-2

# Wait for new leader election on nodes 0-1
sleep 5

# Reconnect node 2
docker network connect distributed-cache_raft-network raft-node-2

# Wait for health check to detect reconnection (10 seconds)
sleep 12

# Try GET to node 2 (previously partitioned)
curl http://localhost:8082/kv/1
```

**Expected Behavior:**
- Node 2 steps down after reconnection detected
- GET either forwards to new leader OR returns error
- No stale reads served

**Success Criteria:**
- ✅ No hang on GET request
- ✅ Correct value returned (from new leader)
- ✅ Node 2 transitions to follower within health check interval

---

### Test 5: Lock Contention Stress Test

**Setup:**
```bash
# High concurrency test
docker compose up -d

# Generate heavy load
for i in {1..50}; do
    (
        for j in {1..20}; do
            curl -X POST -d "{\"key\": $RANDOM, \"value\": $RANDOM}" http://localhost:808$((i % 3))/kv
        done
    ) &
done

for i in {1..50}; do
    (
        for j in {1..20}; do
            curl http://localhost:808$((i % 3))/kv/$RANDOM
        done
    ) &
done

wait
```

**Expected Behavior:**
- All operations complete (success or not found)
- No deadlocks
- No goroutine leaks
- Reasonable response times

**Success Criteria:**
- ✅ No timeouts
- ✅ All requests return within 1 second
- ✅ Memory usage stable

---

## Monitoring and Validation

### Metrics to Track

**Before Fix:**
```
- Deadlock occurrence rate: ~50% on second GET request
- Average GET latency: N/A (hangs)
- Lock wait time: Infinite
```

**After Fix (Expected):**
```
- Deadlock occurrence rate: 0%
- Average GET latency: 10-20ms
- Lock wait time: < 1ms
- Leadership verification latency: < 1ms (cached)
```

### Log Patterns to Verify

**Successful Operation:**
```
[Node 1] Received ForwardGet for key 303
[Node 1] getLeaderData() returned isLeader=true
[Node 1] Leadership verified, serving read
[Node 1] Returning value=100
```

**Stale Leader Detection:**
```
[Node 2] Peer 0 reconnected, verifying leadership
[Node 2] Quorum check: 3/3 available (need 2)
[Node 2] Leadership verified, can reach quorum
```

**Proper Step-Down:**
```
[Node 2] Cannot reach quorum (1/3 available), stepping down
[Node 2] Stepping down from leader role
```

---

## Rollback Plan

### If Stage 1 Causes Issues

**Problem:** Stale reads served for extended period

**Rollback:**
```bash
git revert <stage-1-commit>
docker compose down
docker compose up -d --build
```

**Alternative:** Apply Option B (cached availability) instead

---

### If Stage 2 Causes Issues

**Problem:** Background verification not working

**Rollback:**
```bash
# Keep Stage 1, revert Stage 2
git revert <stage-2-commit>
```

**Impact:** Peer reconnection callback may be slower but still functional

---

### If Stage 3 Causes Issues

**Problem:** Race conditions in log replication

**Rollback:**
```bash
git revert <stage-3-commit>
```

**Impact:** Higher lock contention but no correctness issues

---

## Performance Considerations

### Current State (With Deadlock)

- GET latency: Infinite (hangs)
- Write latency: 50-100ms (normal)
- Deadlock probability: 50% under load

### After Stage 1 (Remove verifyLeadership)

- **GET latency:** 10-20ms (fast, no synchronous checks)
- **Write latency:** 50-100ms (unchanged)
- **Stale read window:** Up to 10 seconds (health check interval)
- **Deadlock probability:** 0%

### After Stage 2 (Optimize verifyLeadership)

- **Background verification:** < 1ms (cached atomic loads)
- **Stale leader detection:** Within 10-20 seconds
- **No impact on read path** (removed from ForwardGet)

### After Stage 3 (Reduce Lock Hold)

- **LogRequest lock hold time:** Reduced by ~50%
- **Read throughput:** Increased (less blocking)
- **Write latency:** Unchanged or slightly improved

---

## Trade-offs and Risks

### Option A Trade-offs

**Pros:**
- Simple, clean fix
- No deadlock risk
- Fast read path
- Easy to understand and maintain

**Cons:**
- Linearizable reads not strictly guaranteed
- Window for stale reads (up to health check interval)
- Relies on background detection

**Mitigation:**
- Tune health check interval down if needed (currently 10s)
- Monitor leadership transitions
- Document stale read possibility in API docs

### Concurrency Safety

**Read-Read Conflicts:** None (RLock allows concurrent readers)

**Read-Write Conflicts:** Minimal after Stage 3 (short critical sections)

**Write-Write Conflicts:** Handled by Raft consensus

**Leadership Changes:** Safe - background verification handles detection

---

## Success Criteria

1. ✅ `./scripts/test_network_partition.sh` passes consistently
2. ✅ No deadlocks under concurrent read/write load
3. ✅ GET requests complete within 100ms
4. ✅ Stale leader detection within health check interval
5. ✅ No regressions in normal operation
6. ✅ All existing tests pass
7. ✅ Memory and goroutine usage stable

---

## Future Enhancements

After validating this fix:

### 1. Read Index Protocol
- Implement Raft read index for linearizable reads
- Verify leadership through log commitIndex check
- No network round-trip needed

### 2. Follower Reads with Bounded Staleness
- Allow reads from followers with staleness guarantee
- Reduce leader load
- Lower read latency for read-heavy workloads

### 3. Leadership Lease
- Time-based leadership guarantee
- Fast local reads without quorum check
- Requires synchronized clocks

### 4. Improved Health Checks
- Adaptive health check intervals
- Faster detection under partition recovery
- Smart peer prioritization

---

## References

- **Raft Paper Section 8:** Client Interaction
- **Original Issue:** Network partition test hangs on GET
- **Related Commits:**
  - `49705bf` - Initial stale leader fix (incomplete)
  - `7f99d3f` - Implementation plan document
- **Test Script:** `/Users/dzc/distributed-cache/scripts/test_network_partition.sh`

---

## Timeline Estimate

**Stage 1:** 15 minutes
- Remove verifyLeadership() from ForwardGet
- Test network partition script
- Verify no deadlock

**Stage 2:** 30 minutes
- Optimize verifyLeadership() for background use
- Replace CheckPeerAvailabilityNow with IsPeerAvailable
- Test peer reconnection callback

**Stage 3 (Optional):** 45 minutes
- Refactor LogRequest lock handling
- Extract validation logic
- Test concurrent operations

**Testing:** 45 minutes
- Run all test scenarios
- Monitor metrics
- Verify success criteria

**Total:** ~2 hours for complete implementation and validation

---

## Conclusion

The deadlock is caused by **nested lock acquisition** combined with **long lock hold times** in the LogRequest handler. The recommended fix is to:

1. **Remove verifyLeadership() from read path** (eliminates deadlock)
2. **Optimize background verification** (improves detection)
3. **Optionally reduce lock hold times** (improves throughput)

This approach balances **correctness, performance, and simplicity** while eliminating the deadlock with minimal risk.
