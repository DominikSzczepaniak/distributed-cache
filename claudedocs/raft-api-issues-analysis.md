# Raft API Issues - Root Cause Analysis

**Date**: 2025-11-08
**Component**: API Server (`pkg/api/server.go`)
**Related**: Raft Core (`pkg/raft/raft.go`, `pkg/raft/grpc.go`)

## Executive Summary

Two critical issues identified in the Raft API implementation:

1. **GET requests fail on non-leader nodes** - incorrectly returns errors instead of forwarding
2. **PUT/DELETE requests hang indefinitely** - never receive responses despite operation success

Both issues stem from misunderstanding how Raft's automatic forwarding works and incorrect response handling patterns.

---

## Issue #1: GET Requests Fail on Non-Leader Nodes

### Current Behavior (INCORRECT)
**Location**: `pkg/api/server.go:185-234` (`handleGet` function)

```go
func (s *Server) handleGet(w http.ResponseWriter, r *http.Request, key int) {
    // ...
    if !stale {
        // Linearizable read: verify leadership first
        if !s.raft.IsLeader() {
            // ❌ PROBLEM: Returns error or redirect instead of letting Raft forward
            http.Error(w, fmt.Sprintf("Not leader. Leader is node %d", leaderID),
                       http.StatusServiceUnavailable)
            return
        }
        // Leader serves read
        value = s.raft.GetApplication().GetValue(key)
    }
}
```

### Root Cause
The API layer **manually checks leadership** and returns errors for non-leader nodes. This bypasses Raft's built-in `Forward()` mechanism that automatically routes requests to the leader.

### Expected Behavior
- Non-leader nodes should **use Raft's Forward() mechanism** under the hood
- Raft's `Forward()` function (`pkg/raft/grpc.go:29-72`) already handles:
  - Detecting if node is not leader
  - Finding current leader
  - Forwarding to leader via gRPC
  - Preventing redirect loops
  - Returning results to caller

### Why This Matters
- **Current**: GET on non-leader → immediate error (503 Service Unavailable)
- **Expected**: GET on non-leader → Raft forwards to leader → returns data seamlessly
- **User Impact**: Clients must manually discover leader and retry, breaking abstraction

---

## Issue #2: PUT/DELETE Requests Hang Indefinitely

### Current Behavior (INCORRECT)
**Location**: `pkg/api/server.go:102-182` (PUT), `pkg/api/server.go:236-312` (DELETE)

```go
func (s *Server) handlePut(w http.ResponseWriter, r *http.Request) {
    // ...
    retryFunc := func(ctx context.Context) error {
        msg := raft.Message{
            MsgType: "PUT",
            Key:     req.Key,
            Value:   &req.Value,
            // ...
        }

        // ❌ PROBLEM: BroadcastSync expects response via ResponseChan
        success, _, err = s.raft.BroadcastSync(msg, 5*time.Second)
        return err
    }

    err := s.retrier.ExecuteWithRetry(ctx, retryFunc)
    // Hangs here waiting for timeout...
}
```

### Root Cause Analysis

#### Step 1: API calls `BroadcastSync`
```go
// pkg/raft/raft.go:170-182
func (r *Raft) BroadcastSync(message Message, timeout time.Duration) (bool, int, error) {
    responseChan := make(chan BroadcastResponse, 1)
    message.ResponseChan = responseChan  // ✅ Creates response channel

    r.Broadcast(message)  // Calls Broadcast with ResponseChan

    select {
    case resp := <-responseChan:  // ❌ WAITING HERE FOREVER
        return resp.Success, resp.Value, resp.Error
    case <-time.After(timeout):
        return false, 0, fmt.Errorf("timeout")
    }
}
```

#### Step 2: `Broadcast` forwards to leader (if not leader)
```go
// pkg/raft/raft.go:134-167
func (r *Raft) Broadcast(message Message) {
    isLeader, leaderID := r.getLeaderData()

    if !isLeader {
        peer := r.getPeer(leaderID)
        r.forwardToLeader(message, peer)  // ❌ LOSES ResponseChan
        return
    }

    // Leader: appends to log, triggers replication
    r.log = append(r.log, LogEntry{Message: message, Term: r.currentTerm})
    // Signals replicators...
}
```

#### Step 3: `forwardToLeader` loses the response channel
```go
// pkg/raft/raft.go:114-132
func (r *Raft) forwardToLeader(message Message, leader PeerClient) {
    msg := &raftpb.Message{
        Type:  toProtoMsgType(message.MsgType),
        Key:   int32(message.Key),
        Value: wrapperspb.Int32(int32(*message.Value)),
        // ❌ PROBLEM: ResponseChan is NOT serialized in protobuf
    }

    leader.Forward(ctx, msg)  // Fire-and-forget gRPC call
    // ❌ No mechanism to send response back to original caller
}
```

#### Step 4: Leader processes via `Forward()` gRPC handler
```go
// pkg/raft/grpc.go:29-72
func (r *Raft) Forward(ctx context.Context, msg *raftpb.Message) (*raftpb.Null, error) {
    // Converts protobuf → internal Message
    internal := Message{
        MsgType: MessageType(msg.Type.String()),
        Key:     int(msg.Key),
        Value:   val,
        // ❌ ResponseChan is nil here (not in protobuf)
    }

    r.Broadcast(internal)  // Appends to log
    return &raftpb.Null{}, nil  // ❌ Returns empty response
}
```

#### Step 5: Eventually committed, but response never sent
```go
// pkg/raft/raft.go:184-202
func (r *Raft) deliverToApplication(message Message) (success bool, value int) {
    success, value = r.application.AppendMessage(message)

    // ✅ Tries to send response
    if message.ResponseChan != nil {
        message.ResponseChan <- BroadcastResponse{...}
    }
    // ❌ But ResponseChan is nil (lost during forwarding)
}
```

### Why Requests Hang
1. **On Leader**: ResponseChan works ✅ → immediate response
2. **On Follower**:
   - `BroadcastSync` waits on ResponseChan
   - `forwardToLeader` sends gRPC `Forward()` call
   - Leader's `Forward()` handler creates **new Message without ResponseChan**
   - Leader commits entry successfully
   - `deliverToApplication` checks `ResponseChan` → finds it nil → doesn't send response
   - Original caller **waits forever** until 5s timeout

### Observable Symptoms
- ✅ **Operation succeeds**: Data is committed to Raft log and applied
- ❌ **Response never arrives**: Client times out after 5 seconds
- ❌ **Retries triggered**: Idempotency cache prevents duplicates but still slow
- 🔴 **User Experience**: "Operation successful but appeared to fail"

---

## Architectural Insights

### Raft's Design Intent
The `Forward()` gRPC method (`pkg/raft/grpc.go:29`) is designed to:
- Accept messages on any node
- Automatically route to leader if needed
- Handle the operation and return result
- **Not** propagate response channels across network boundaries

### API Layer Responsibility
The API should:
- For **reads**: Use `Forward()` for linearizable reads OR local reads with `?stale=true`
- For **writes**: Use `Forward()` gRPC call directly, await its response
- **Not** use `BroadcastSync` from non-leader nodes (loses response channel)

---

## Summary Table

| Issue | Location | Root Cause | Impact |
|-------|----------|------------|--------|
| **GET fails on followers** | `server.go:198-219` | Manual leadership check blocks request | Clients get 503 errors |
| **PUT/DEL hang** | `server.go:143`, `raft.go:142` | ResponseChan lost during forwarding | 5s timeout, poor UX |

---

## Next Steps
See `raft-api-fix-plan.md` for detailed implementation plan.
