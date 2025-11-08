# Network Partition Deadlock Fix - Implementation Plan

**Date:** 2025-11-08
**Issue:** GET requests hang indefinitely on reconnected nodes after network partition
**Severity:** Critical - causes production hangs in network partition scenarios
**Affected Test:** `scripts/test_network_partition.sh`

---

## Table of Contents
1. [Problem Overview](#problem-overview)
2. [Root Cause Analysis](#root-cause-analysis)
3. [Affected Code Locations](#affected-code-locations)
4. [Proposed Solution](#proposed-solution)
5. [Implementation Plan](#implementation-plan)
6. [Testing Strategy](#testing-strategy)
7. [Risk Assessment](#risk-assessment)
8. [Rollback Plan](#rollback-plan)
9. [Future Improvements](#future-improvements)

---

## Problem Overview

### Symptom
When executing `scripts/test_network_partition.sh`, the GET request to retrieve key 303 from a reconnected node hangs indefinitely:

```bash
# Test sequence:
1. Start 3-node cluster
2. Identify initial leader
3. Isolate leader from network (docker network disconnect)
4. New leader elected among remaining nodes
5. Write key=303, value=3003 to new leader
6. Reconnect original leader (docker network connect)
7. GET request to original leader for key 303 → HANGS HERE
```

### Expected Behavior
The GET request should either:
- Return the value (after state synchronization)
- Return an error (if leader unavailable)
- Timeout within 2 seconds (context deadline)

### Actual Behavior
- GET request blocks indefinitely
- No timeout occurs
- No error is returned
- Client hangs waiting for response

---

## Root Cause Analysis

### Technical Root Cause
**Stale gRPC Connection Usage Without Availability Check**

The issue occurs due to the following sequence:

#### 1. Network Partition Phase
```
Original Leader (Node 0)
├─ Network disconnected via docker network disconnect
├─ gRPC connections to peers enter TransientFailure state
├─ Cannot send heartbeats to followers
├─ Cannot receive votes for re-election
└─ Eventually recognizes it's no longer leader (via VoteRequest from new term)
```

#### 2. Network Reconnection Phase
```
Original Leader Reconnected
├─ Docker network connect restores network
├─ gRPC connections still reference OLD broken connections
├─ Connection manager hasn't detected reconnection yet
│  └─ Health check runs at healthCheckInterval (periodic)
├─ Connections not yet re-established
└─ Peer availability flags still show unavailable/stale state
```

#### 3. GET Request Handling Phase
```
GET /kv/303 arrives at reconnected node
│
├─ HTTP Handler: server.go:192 handleGet()
│  └─ Context timeout: 2 seconds (line 205)
│
├─ Raft Layer: grpc.go:84 ForwardGet()
│  ├─ getLeaderData() → isLeader=false, leaderID=1 (new leader)
│  ├─ No check: isPeerAvailable(leaderID) ← MISSING!
│  ├─ getPeer(leaderID) → returns PeerClient with STALE connection
│  └─ peer.ForwardGet(ctx, req) → BLOCKS on broken connection
│     └─ gRPC call attempts to use broken TCP connection
│        └─ TCP stack waits for ACK that never comes
│           └─ Context cancellation doesn't properly interrupt ← HANG
```

### Why Context Timeout Doesn't Work

The gRPC connection is in a state where:
1. **Connection Appears Valid** - gRPC client exists in memory
2. **Underlying TCP Broken** - Network partition left TCP in limbo state
3. **Context Not Propagated** - TCP layer doesn't honor gRPC context deadline
4. **No Circuit Breaker** - No fast-fail mechanism for known-bad connections

---

## Affected Code Locations

### Primary Issues

#### 1. `pkg/raft/grpc.go:84-112` - ForwardGet Method
```go
func (r *Raft) ForwardGet(ctx context.Context, req *raftpb.GetRequest) (*raftpb.GetResponse, error) {
    slog.Info(fmt.Sprintf("Received ForwardGet on node %d for key %d", r.id, req.Key))

    isLeader, leaderID := r.getLeaderData()

    if !isLeader {
        if leaderID < 0 {
            return nil, fmt.Errorf("no leader known")
        }
        // ❌ MISSING: Peer availability check
        // ❌ SHOULD BE: if !r.isPeerAvailable(leaderID) { return error }

        if ctx.Value(forwardHopKey{}) != nil {
            return nil, fmt.Errorf("redirect loop detected at node=%d to leader=%d", r.id, leaderID)
        }
        ctx = context.WithValue(ctx, forwardHopKey{}, true)
        slog.Info(fmt.Sprintf("Node %d forwards ForwardGet to leader %d", r.id, leaderID))
        peer := r.getPeer(leaderID)
        if peer == nil {
            return nil, fmt.Errorf("no peer for leader %d", leaderID)
        }
        return peer.ForwardGet(ctx, req)  // ← BLOCKS HERE
    }

    // Leader serves the read
    value := r.application.GetValue(int(req.Key))
    return &raftpb.GetResponse{
        Key:   req.Key,
        Value: int32(value),
        Found: true,
    }, nil
}
```

**Problem:** Line 98-102 use peer without checking availability

#### 2. `pkg/raft/grpc.go:30-81` - Forward Method
```go
func (r *Raft) Forward(ctx context.Context, msg *raftpb.Message) (*raftpb.ForwardResponse, error) {
    // ... message conversion ...

    isLeader, leaderID := r.getLeaderData()

    if !isLeader {
        if leaderID < 0 {
            return nil, fmt.Errorf("no leader known")
        }
        if leaderID == r.id {
            r.mu.Lock()
            if r.currentRole != Leader && r.currentLeaderId == r.id {
                r.currentLeaderId = -1
            }
            r.mu.Unlock()
            return nil, fmt.Errorf("no leader known")
        }
        if ctx.Value(forwardHopKey{}) != nil {
            return nil, fmt.Errorf("redirect loop detected at node=%d to leader=%d", r.id, leaderID)
        }
        ctx = context.WithValue(ctx, forwardHopKey{}, true)
        slog.Info(fmt.Sprintf("Node %d forwards Forward call to %d", r.id, leaderID))

        // ❌ MISSING: Peer availability check
        // ❌ SHOULD BE: if !r.isPeerAvailable(leaderID) { return error }

        peer := r.getPeer(leaderID)
        if peer == nil {
            return nil, fmt.Errorf("no peer for node %d", r.id)
        }
        return peer.Forward(ctx, msg)  // ← CAN ALSO BLOCK
    }

    // Leader: use BroadcastSync to wait for commit
    success, value, err := r.BroadcastSync(internal, 5*time.Second)
    // ...
}
```

**Problem:** Line 64-68 use peer without checking availability (same issue as ForwardGet)

### Correct Pattern (For Reference)

#### `pkg/raft/election.go:113-123` - sendVoteRequest Method
```go
func (r *Raft) sendVoteRequest(data VoteRequestData, nodeId int) {
    // ✅ CORRECT: Check availability first
    if !r.isPeerAvailable(nodeId) {
        slog.Debug(fmt.Sprintf("Node %d: Skipping vote request to unavailable peer %d", r.id, nodeId))
        return
    }

    peer := r.getPeer(nodeId)
    if peer == nil {
        slog.Debug(fmt.Sprintf("Node %d: No peer client for node %d", r.id, nodeId))
        return
    }
    // ... proceed with request ...
}
```

**This is the pattern we should follow in Forward and ForwardGet**

---

## Proposed Solution

### Solution Overview
Add peer availability checks before forwarding requests to prevent using stale/broken connections.

### Design Principles
1. **Fail Fast** - Return error immediately if leader unavailable
2. **Clear Errors** - Provide actionable error messages
3. **Retry Support** - Allow upper layers to retry with backoff
4. **Consistency** - Apply same pattern to both Forward and ForwardGet

### Detailed Changes

#### Change 1: Fix ForwardGet Method

**File:** `pkg/raft/grpc.go`
**Function:** `ForwardGet`
**Lines:** 84-112

**Before:**
```go
if !isLeader {
    if leaderID < 0 {
        return nil, fmt.Errorf("no leader known")
    }
    if ctx.Value(forwardHopKey{}) != nil {
        return nil, fmt.Errorf("redirect loop detected at node=%d to leader=%d", r.id, leaderID)
    }
    ctx = context.WithValue(ctx, forwardHopKey{}, true)
    slog.Info(fmt.Sprintf("Node %d forwards ForwardGet to leader %d", r.id, leaderID))
    peer := r.getPeer(leaderID)
    if peer == nil {
        return nil, fmt.Errorf("no peer for leader %d", leaderID)
    }
    return peer.ForwardGet(ctx, req)
}
```

**After:**
```go
if !isLeader {
    if leaderID < 0 {
        return nil, fmt.Errorf("no leader known")
    }

    // Check peer availability before attempting forward
    if !r.isPeerAvailable(leaderID) {
        slog.Warn(fmt.Sprintf("Node %d: Leader %d is not available, cannot forward GET request", r.id, leaderID))
        return nil, fmt.Errorf("leader %d is not available", leaderID)
    }

    if ctx.Value(forwardHopKey{}) != nil {
        return nil, fmt.Errorf("redirect loop detected at node=%d to leader=%d", r.id, leaderID)
    }
    ctx = context.WithValue(ctx, forwardHopKey{}, true)
    slog.Info(fmt.Sprintf("Node %d forwards ForwardGet to leader %d", r.id, leaderID))
    peer := r.getPeer(leaderID)
    if peer == nil {
        return nil, fmt.Errorf("no peer for leader %d", leaderID)
    }
    return peer.ForwardGet(ctx, req)
}
```

**Rationale:**
- Prevents hanging on stale connections
- Returns clear error to HTTP layer
- Allows retry logic in API server to handle unavailability
- Matches pattern used in sendVoteRequest

#### Change 2: Fix Forward Method

**File:** `pkg/raft/grpc.go`
**Function:** `Forward`
**Lines:** 30-81

**Before:**
```go
if !isLeader {
    if leaderID < 0 {
        return nil, fmt.Errorf("no leader known")
    }
    if leaderID == r.id {
        r.mu.Lock()
        if r.currentRole != Leader && r.currentLeaderId == r.id {
            r.currentLeaderId = -1
        }
        r.mu.Unlock()
        return nil, fmt.Errorf("no leader known")
    }
    if ctx.Value(forwardHopKey{}) != nil {
        return nil, fmt.Errorf("redirect loop detected at node=%d to leader=%d", r.id, leaderID)
    }
    ctx = context.WithValue(ctx, forwardHopKey{}, true)
    slog.Info(fmt.Sprintf("Node %d forwards Forward call to %d", r.id, leaderID))
    peer := r.getPeer(leaderID)
    if peer == nil {
        return nil, fmt.Errorf("no peer for node %d", r.id)
    }
    return peer.Forward(ctx, msg)
}
```

**After:**
```go
if !isLeader {
    if leaderID < 0 {
        return nil, fmt.Errorf("no leader known")
    }
    if leaderID == r.id {
        r.mu.Lock()
        if r.currentRole != Leader && r.currentLeaderId == r.id {
            r.currentLeaderId = -1
        }
        r.mu.Unlock()
        return nil, fmt.Errorf("no leader known")
    }

    // Check peer availability before attempting forward
    if !r.isPeerAvailable(leaderID) {
        slog.Warn(fmt.Sprintf("Node %d: Leader %d is not available, cannot forward request", r.id, leaderID))
        return nil, fmt.Errorf("leader %d is not available", leaderID)
    }

    if ctx.Value(forwardHopKey{}) != nil {
        return nil, fmt.Errorf("redirect loop detected at node=%d to leader=%d", r.id, leaderID)
    }
    ctx = context.WithValue(ctx, forwardHopKey{}, true)
    slog.Info(fmt.Sprintf("Node %d forwards Forward call to %d", r.id, leaderID))
    peer := r.getPeer(leaderID)
    if peer == nil {
        return nil, fmt.Errorf("no peer for node %d", r.id)
    }
    return peer.Forward(ctx, msg)
}
```

**Rationale:**
- Same protection for PUT/DELETE operations
- Consistent error handling across all forwarding operations
- Prevents deadlock in write path as well

---

## Implementation Plan

### Phase 1: Code Changes (Estimated: 30 minutes)

#### Step 1: Backup Current Code
```bash
git status  # Ensure clean working directory
git checkout -b fix/network-partition-deadlock
```

#### Step 2: Implement ForwardGet Fix
1. Open `pkg/raft/grpc.go`
2. Locate `ForwardGet` function (line 84)
3. Add availability check after `leaderID < 0` check (around line 90)
4. Add appropriate logging for debugging
5. Save file

#### Step 3: Implement Forward Fix
1. Same file: `pkg/raft/grpc.go`
2. Locate `Forward` function (line 30)
3. Add availability check after self-reference check (around line 58)
4. Add appropriate logging for debugging
5. Save file

#### Step 4: Code Review Checklist
- [ ] Both methods have availability check
- [ ] Checks occur BEFORE getPeer() call
- [ ] Error messages are clear and actionable
- [ ] Logging is at appropriate level (Warn)
- [ ] No syntax errors
- [ ] Consistent with sendVoteRequest pattern

### Phase 2: Testing (Estimated: 1 hour)

#### Step 1: Unit Test Preparation
Create test case in `pkg/raft/grpc_test.go` (new file or add to existing):

```go
func TestForwardGetWithUnavailablePeer(t *testing.T) {
    // Setup: Create Raft node with unavailable peer
    // Execute: Call ForwardGet
    // Verify: Returns error "leader X is not available"
    // Verify: Does not hang
}

func TestForwardWithUnavailablePeer(t *testing.T) {
    // Setup: Create Raft node with unavailable peer
    // Execute: Call Forward
    // Verify: Returns error "leader X is not available"
    // Verify: Does not hang
}
```

#### Step 2: Integration Test - Network Partition
```bash
# Run the failing test
./scripts/test_network_partition.sh

# Expected results AFTER fix:
# - No hang on GET request (line 104)
# - Either: Returns error initially, then succeeds after sync
# - Or: Succeeds after retry (depending on timing)
# - Test should complete within reasonable time (~30-60 seconds)
```

#### Step 3: Regression Testing
```bash
# Run all existing tests
cd pkg/raft
go test -v ./...

# Run integration tests
cd ../../tests/integration
go test -v ./...

# Verify no regressions in:
# - Normal GET operations
# - Normal PUT/DELETE operations
# - Leader election
# - Log replication
```

#### Step 4: Manual Testing Scenarios

**Scenario 1: Network Partition Recovery**
```bash
1. Start 3-node cluster
2. Write some data to leader
3. Partition leader (docker network disconnect)
4. Verify new leader elected
5. Write new data to new leader
6. Reconnect original leader (docker network connect)
7. Immediately send GET to reconnected node
   Expected: Error or retry, no hang
8. Wait 5 seconds (for connection recovery)
9. Send GET again
   Expected: Success with current data
```

**Scenario 2: Cascading Failures**
```bash
1. Start 3-node cluster
2. Partition 2 nodes simultaneously
3. Remaining node has no quorum
4. Send GET to isolated nodes
   Expected: "no leader known" error, no hang
5. Reconnect all nodes
6. Verify cluster recovers
```

### Phase 3: Documentation (Estimated: 20 minutes)

#### Update CHANGELOG.md
```markdown
## [Unreleased]

### Fixed
- Network partition deadlock: GET/PUT/DELETE requests no longer hang indefinitely
  when forwarding to unavailable leader after network reconnection
- Added peer availability checks in Forward() and ForwardGet() methods
- Improved error messages for leader unavailability scenarios
```

#### Update README or docs (if applicable)
- Document expected behavior during network partitions
- Document retry recommendations for clients
- Document recovery time expectations

---

## Testing Strategy

### Test Pyramid

#### Level 1: Unit Tests (Fast, Isolated)
```go
// Test availability check logic
- TestForwardGetWithUnavailablePeer
- TestForwardWithUnavailablePeer
- TestForwardGetWithAvailablePeer (regression)
- TestForwardWithAvailablePeer (regression)

// Test error messages
- TestForwardGetErrorMessages
- TestForwardErrorMessages

// Test edge cases
- TestForwardGetWithNegativeLeaderID
- TestForwardWithSelfReference
```

**Success Criteria:**
- All tests pass
- Code coverage > 80% for modified functions
- No new race conditions detected

#### Level 2: Integration Tests (Slower, Multi-Component)
```bash
# Existing test that should now pass
./scripts/test_network_partition.sh

# Additional scenarios
./scripts/test_multiple_partitions.sh (if exists)

# Go integration tests
go test -v ./tests/integration/fault_tolerance_test.go
```

**Success Criteria:**
- `test_network_partition.sh` completes successfully
- No hangs within 2x expected completion time
- All data eventually consistent after partition heals

#### Level 3: Manual/Exploratory Tests (Comprehensive)

**Test Cases:**

1. **Immediate Reconnection**
   - Partition node for 1 second
   - Reconnect immediately
   - Send request
   - Expected: Error or fast retry

2. **Delayed Reconnection**
   - Partition node for 30 seconds
   - Reconnect
   - Wait 10 seconds
   - Send request
   - Expected: Success (connections recovered)

3. **Concurrent Requests During Partition**
   - Partition leader
   - Send 100 concurrent GET requests
   - Expected: All return error, none hang

4. **Write During Partition**
   - Partition node
   - Try PUT to partitioned node
   - Expected: Error, no data loss

**Success Criteria:**
- Zero hangs observed
- All errors are actionable
- Data consistency maintained
- Recovery time < 30 seconds after reconnection

### Performance Testing

#### Before/After Comparison

**Metrics to Measure:**
- Normal operation latency (should be unchanged)
- Error case latency (should be <2 seconds, was infinite)
- Connection recovery time (should be unchanged)
- CPU usage during partition (should be unchanged)

**Benchmarks:**
```bash
# Run before fix
go test -bench=. -benchmem ./pkg/raft > before.txt

# Apply fix

# Run after fix
go test -bench=. -benchmem ./pkg/raft > after.txt

# Compare
diff before.txt after.txt
```

**Expected:**
- No significant performance degradation (<5%)
- Availability check overhead negligible (simple atomic bool read)

---

## Risk Assessment

### High Risks (Likelihood: Medium, Impact: High)

#### Risk 1: False Negative Availability Check
**Description:** `isPeerAvailable()` returns false even when peer is actually reachable

**Mitigation:**
- Review `connection_manager.go` health check logic
- Add defensive logging around availability checks
- Monitor false negative rate in production

**Detection:**
- Increased "leader not available" errors in logs
- Requests failing when they shouldn't

**Rollback Trigger:**
- Error rate > 5% during normal operation

#### Risk 2: Race Condition in Availability Check
**Description:** Peer becomes unavailable between check and usage

**Mitigation:**
- This is acceptable - gRPC will handle it with context timeout
- The check is best-effort to avoid known-bad connections
- Context deadline still protects against hangs

**Detection:**
- Occasional timeout errors (acceptable)

**Acceptance:**
- This is the correct trade-off - better than always hanging

### Medium Risks (Likelihood: Low, Impact: Medium)

#### Risk 3: Changed Error Semantics
**Description:** Callers might not handle new error type properly

**Mitigation:**
- Review all call sites of Forward() and ForwardGet()
- Ensure API layer retry logic handles new error
- Add integration tests for error propagation

**Detection:**
- API returns 500 instead of retrying
- Client sees different error messages

**Rollback Trigger:**
- Breaking existing client contracts

### Low Risks (Likelihood: Low, Impact: Low)

#### Risk 4: Logging Verbosity
**Description:** Too many "leader not available" warnings

**Mitigation:**
- Use slog.Warn (appropriate level)
- Can adjust later if needed

**Detection:**
- Log volume increase

**Remediation:**
- Change to Debug level or add rate limiting

---

## Rollback Plan

### Rollback Decision Criteria
Rollback if ANY of the following occur within 24 hours:

1. **Correctness Issues:**
   - Data inconsistency detected
   - Lost writes
   - Incorrect reads

2. **Availability Issues:**
   - Normal operation error rate > 5%
   - False positive unavailability > 1%
   - Cluster cannot elect leader

3. **Performance Issues:**
   - Request latency increase > 20%
   - CPU usage increase > 15%
   - Memory leak detected

### Rollback Procedure

#### Step 1: Immediate Rollback (5 minutes)
```bash
# Revert the commit
git log --oneline -5  # Find commit hash
git revert <commit-hash>

# Or reset if not pushed
git reset --hard HEAD~1

# Rebuild and redeploy
go build ./cmd/raftnode
# Deploy via your process
```

#### Step 2: Verify Rollback (10 minutes)
```bash
# Run network partition test with old code
./scripts/test_network_partition.sh
# Should fail with hang (expected old behavior)

# Run normal operations
go test -v ./pkg/raft
# Should pass
```

#### Step 3: Post-Rollback Analysis (1 hour)
- Review logs for root cause
- Identify what was missed in testing
- Update this plan with lessons learned
- Schedule fix retry with updated approach

---

## Future Improvements

### Short-term (Next Sprint)

#### 1. Enhanced Connection Recovery
**Problem:** Current health check is periodic, slow to detect reconnection

**Solution:**
- Add event-driven connection state monitoring
- React immediately to connectivity changes
- Implement exponential backoff for reconnection

**Implementation:**
```go
// In connection_manager.go
func (cm *ConnectionManager) monitorConnection(peerID int) {
    conn := cm.conns[peerID]
    conn.WaitForStateChange(cm.ctx, conn.GetState())
    // React immediately to state changes
}
```

#### 2. Circuit Breaker Pattern
**Problem:** Repeated failed requests to unavailable peer

**Solution:**
- Implement circuit breaker for peer connections
- Fast-fail after N consecutive failures
- Auto-recover with exponential backoff

**Reference:** github.com/sony/gobreaker

#### 3. Request Hedging
**Problem:** Single unavailable peer can delay response

**Solution:**
- Send request to leader + backup simultaneously
- Use first successful response
- Reduces tail latency during instability

### Medium-term (Next Quarter)

#### 1. Read Your Writes Consistency
**Problem:** Reconnected node serves stale data

**Solution:**
- Track last applied index per read
- Ensure read index >= write index
- Implement read index protocol from Raft paper

#### 2. Lease-based Reads
**Problem:** All reads require leader forwarding

**Solution:**
- Leader grants time-based read leases to followers
- Followers serve reads locally with lease
- Reduces read latency and leader load

#### 3. Quorum Reads
**Problem:** Leader failure during read

**Solution:**
- Option to read from majority quorum
- Higher latency but higher availability
- Configurable per-request

### Long-term (Next Year)

#### 1. Multi-Raft Groups
- Horizontal scaling with multiple Raft groups
- Leader distribution across nodes
- Reduced blast radius of network partitions

#### 2. Byzantine Fault Tolerance
- Protect against malicious nodes
- Cryptographic verification
- Cross-datacenter deployments

#### 3. Dynamic Membership
- Add/remove nodes without downtime
- Automatic rebalancing
- Zero-downtime upgrades

---

## Appendix

### A. Related Code Files

**Modified Files:**
- `pkg/raft/grpc.go` - Forward and ForwardGet methods

**Related Files (Review but no changes):**
- `pkg/raft/connection_manager.go` - Availability tracking
- `pkg/raft/utils.go` - isPeerAvailable implementation
- `pkg/api/server.go` - HTTP handlers calling ForwardGet
- `pkg/api/retry.go` - Retry logic that may handle new errors

**Test Files:**
- `scripts/test_network_partition.sh` - Main test that currently fails
- `pkg/raft/integration_test.go` - Integration tests
- `tests/integration/fault_tolerance_test.go` - Fault tolerance tests

### B. Debugging Guide

#### If Test Still Hangs After Fix

**Step 1: Verify Fix Applied**
```bash
grep -A 5 "isPeerAvailable" pkg/raft/grpc.go
# Should see two occurrences: in Forward and ForwardGet
```

**Step 2: Enable Debug Logging**
```bash
# Set log level to debug
export RAFT_LOG_LEVEL=debug
./scripts/test_network_partition.sh
```

**Step 3: Attach Debugger**
```bash
# Find hanging process
ps aux | grep raftnode

# Attach delve
dlv attach <pid>

# Check goroutines
(dlv) goroutines
(dlv) goroutine <id>  # For blocked goroutine
(dlv) bt  # Backtrace
```

**Step 4: Check Connection State**
```bash
# In container
docker exec -it raft-node-0 /bin/sh

# Check network
ping raft-node-1
netstat -an | grep 9090  # gRPC port

# Check gRPC health
grpc-health-probe -addr=raft-node-1:9090
```

#### Common Issues and Solutions

**Issue:** Still hangs despite availability check
**Solution:** Check if health check is running (healthCheckInterval > 0)

**Issue:** Returns error but should succeed
**Solution:** Increase wait time for connection recovery

**Issue:** Connection never recovers
**Solution:** Check reconnection logic in ConnectionManager

### C. Performance Benchmarks

#### Expected Performance Characteristics

**Before Fix:**
- Normal GET latency: ~1-5ms
- Network partition GET: Infinite hang
- CPU during hang: ~0% (blocked)
- Memory during hang: Stable (no leak but resources held)

**After Fix:**
- Normal GET latency: ~1-5ms (unchanged)
- Network partition GET: ~2s (context timeout) or <100ms (fast fail if leader known unavailable)
- CPU during partition: ~0% (no busy wait)
- Memory during partition: Stable

**Availability Check Overhead:**
- Cost: 1 atomic bool read
- Latency: <1µs
- Impact: Negligible (<0.1% overhead)

### D. Contact and Escalation

**Code Owner:** Distributed Cache Team
**Reviewers Required:** 2 (including 1 senior engineer)
**Testing Sign-off:** QA team for integration tests
**Deploy Approval:** Tech lead or higher

**Escalation Path:**
1. Team discussion (Slack/meeting)
2. Tech lead review
3. Engineering manager decision
4. If impacts production: Immediate rollback + incident review

---

## Checklist

### Pre-Implementation
- [ ] Plan reviewed by team
- [ ] Risk assessment accepted
- [ ] Testing strategy approved
- [ ] Rollback plan validated
- [ ] Timeline agreed upon

### Implementation
- [ ] Feature branch created
- [ ] ForwardGet fix implemented
- [ ] Forward fix implemented
- [ ] Code self-reviewed
- [ ] Unit tests written
- [ ] Unit tests passing
- [ ] Integration tests passing
- [ ] Manual testing completed
- [ ] Documentation updated
- [ ] Code review completed
- [ ] All review comments addressed

### Deployment
- [ ] Changes merged to main
- [ ] Regression tests passing
- [ ] Performance benchmarks acceptable
- [ ] Staging deployment successful
- [ ] Staging verification complete
- [ ] Production deployment plan ready
- [ ] Rollback plan ready
- [ ] Monitoring alerts configured

### Post-Deployment
- [ ] Production deployment successful
- [ ] Network partition test passing
- [ ] No error rate increase
- [ ] No performance degradation
- [ ] No rollback required (24 hours)
- [ ] Incident documentation complete
- [ ] Lessons learned documented
- [ ] Future improvements planned

---

**Plan Version:** 1.0
**Last Updated:** 2025-11-08
**Status:** Ready for Implementation
