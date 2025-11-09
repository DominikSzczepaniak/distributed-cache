# Health Check Callback Deadlock Fix

**Date**: 2025-11-09
**Issue**: Network partition test hangs due to synchronous callback blocking health check loop
**Solution**: Make peer reconnect callback asynchronous

## Root Cause Analysis

### The Problem

The health check loop in `ConnectionManager.performHealthCheck()` was calling the peer reconnect callback **synchronously**, which created a blocking chain:

```
performHealthCheck() (goroutine blocked)
  └─> callback(i) [SYNCHRONOUS]
       └─> handlePeerReconnect(peerID)
            └─> verifyLeadership()
                 └─> for each peer: CheckPeerAvailabilityNow(i)
                      └─> conn.GetState() [CAN BLOCK FOR SECONDS]
```

### Why This Causes Deadlock/Livelock

1. **Health Check Blocked**: The health check goroutine is stuck waiting for the callback chain to complete
2. **Multiple Blocking Calls**: `verifyLeadership()` calls `CheckPeerAvailabilityNow()` for ALL peers, each of which can block on `conn.GetState()`
3. **During Network Partition**: These gRPC connection state checks can hang for seconds when connections are unstable
4. **Cascading Effect**: While the health check is blocked, other components that depend on timely health updates are starved
5. **Lock Contention**: Although locks are properly paired, the blocking behavior creates effective lock contention

### Specific Scenario in Network Partition Test

1. Original leader (node-0) is isolated from the network
2. New leader (node-1) is elected
3. Node-0 is reconnected to the network
4. Health check detects node-0 is available → triggers callback **synchronously**
5. Callback calls `verifyLeadership()` which checks ALL peers' availability
6. During unstable network state, `conn.GetState()` blocks for extended periods
7. Health check goroutine is stuck, preventing further health monitoring
8. System enters a degraded state where operations hang

## The Fix

**File**: `pkg/raft/connection_manager.go:212`

**Change**:
```go
// Before (BLOCKING):
if callback != nil {
    callback(i)  // ← Blocks health check goroutine
}

// After (NON-BLOCKING):
if callback != nil {
    go callback(i)  // ← Asynchronous execution
}
```

### Why This Works

1. **Non-Blocking Health Check**: The health check loop continues immediately
2. **Concurrent Processing**: Peer reconnection verification happens in parallel
3. **No Cascading Delays**: Even if `verifyLeadership()` takes seconds, it doesn't block the health monitor
4. **Better Fault Tolerance**: Health check continues working even during network instability

### Trade-offs and Considerations

**Pros**:
- ✅ Health check loop never blocks
- ✅ Better performance under network stress
- ✅ Prevents cascading failures
- ✅ Simple one-line fix

**Cons**:
- ⚠️ Callback now executes without waiting for completion
- ⚠️ Multiple callbacks could fire concurrently for different peers
- ⚠️ Need to ensure `handlePeerReconnect()` and `verifyLeadership()` are goroutine-safe

**Safety Analysis**:
- `handlePeerReconnect()`: Uses proper locking (`r.mu.RLock()` → read state → `r.mu.RUnlock()`)
- `verifyLeadership()`: Uses proper locking and only calls into ConnectionManager methods
- `CheckPeerAvailabilityNow()`: Uses proper locking (`cm.mu.RLock()` → read conn → `cm.mu.RUnlock()`)
- All methods are already designed to be called concurrently from different goroutines

## Testing

### Before Fix
```bash
$ ./scripts/test_network_partition.sh
# Hangs indefinitely during state reconciliation phase
# Health check goroutine stuck in callback chain
```

### After Fix
```bash
$ ./scripts/test_network_partition.sh
# Expected: Completes successfully
# Health check continues operating during reconnection
# State reconciles within timeout period
```

## Related Files

- `pkg/raft/connection_manager.go:212` - Main fix location
- `pkg/raft/raft.go:472` - `handlePeerReconnect()` implementation
- `pkg/raft/raft.go:413` - `verifyLeadership()` implementation
- `pkg/raft/connection_manager.go:299` - `CheckPeerAvailabilityNow()` implementation

## Prevention

**Principle**: Never call potentially blocking callbacks synchronously from within a periodic monitor/health check loop.

**Pattern**:
```go
// ❌ BAD: Synchronous callback
if callback != nil {
    callback(data)  // Blocks if callback takes time
}

// ✅ GOOD: Asynchronous callback
if callback != nil {
    go callback(data)  // Continues immediately
}
```

## Verification Checklist

- [x] Identified blocking call chain
- [x] Verified all involved methods are goroutine-safe
- [x] Made callback asynchronous
- [ ] Run network partition test multiple times
- [ ] Verify no race conditions introduced
- [ ] Check for any callback ordering dependencies
