# Raft API Fix Plan - Implementation Strategy

**Date**: 2025-11-08
**Status**: 🟡 PENDING USER APPROVAL
**Dependencies**: See `raft-api-issues-analysis.md` for root cause analysis

---

## Overview

This plan addresses two critical issues:
1. **GET requests fail on non-leader nodes** → Fix: Use Raft's Forward() for reads
2. **PUT/DELETE hang on followers** → Fix: Use Forward() gRPC directly, not BroadcastSync

**Core Principle**: Let Raft's `Forward()` mechanism handle routing automatically.

---

## Implementation Stages

### Stage 1: Fix GET Request Handling ⚡ HIGH PRIORITY
**Estimated Time**: 30 minutes
**Risk Level**: 🟢 Low (read-only operations)

#### 1.1 Create GET Forward Handler in Raft Core
**File**: `pkg/raft/grpc.go`
**Action**: Add new `ForwardGet()` gRPC method

```go
// New gRPC method for read operations
func (r *Raft) ForwardGet(ctx context.Context, req *raftpb.GetRequest) (*raftpb.GetResponse, error) {
    // Check if leader
    isLeader, leaderID := r.getLeaderData()

    if !isLeader {
        // Forward to leader (with loop detection)
        if ctx.Value(forwardHopKey{}) != nil {
            return nil, fmt.Errorf("redirect loop detected")
        }
        ctx = context.WithValue(ctx, forwardHopKey{}, true)

        peer := r.getPeer(leaderID)
        if peer == nil {
            return nil, fmt.Errorf("no leader available")
        }

        return peer.ForwardGet(ctx, req)
    }

    // Leader serves the read
    value := r.application.GetValue(int(req.Key))
    return &raftpb.GetResponse{
        Key:   req.Key,
        Value: int32(value),
        Found: true, // TODO: Add existence check to Application interface
    }, nil
}
```

#### 1.2 Update Protobuf Definition
**File**: `pkg/raft/raftpb/raft.proto`
**Action**: Add GetRequest/GetResponse messages and ForwardGet RPC

```protobuf
message GetRequest {
    int32 key = 1;
}

message GetResponse {
    int32 key = 1;
    int32 value = 2;
    bool found = 3;
}

service Raft {
    // ... existing methods
    rpc ForwardGet(GetRequest) returns (GetResponse);
}
```

#### 1.3 Regenerate Protobuf Code
**Command**:
```bash
cd pkg/raft/raftpb
protoc --go_out=. --go-grpc_out=. raft.proto
```

#### 1.4 Update API Server GET Handler
**File**: `pkg/api/server.go`
**Action**: Replace manual leadership check with Forward() call

**BEFORE** (lines 185-234):
```go
func (s *Server) handleGet(w http.ResponseWriter, r *http.Request, key int) {
    stale := r.URL.Query().Get("stale") == "true"

    if stale {
        value = s.raft.GetApplication().GetValue(key)
    } else {
        if !s.raft.IsLeader() {  // ❌ REMOVE THIS
            http.Error(w, "Not leader", http.StatusServiceUnavailable)
            return
        }
        value = s.raft.GetApplication().GetValue(key)
    }
}
```

**AFTER**:
```go
func (s *Server) handleGet(w http.ResponseWriter, r *http.Request, key int) {
    stale := r.URL.Query().Get("stale") == "true"

    var value int
    var found bool

    if stale {
        // Stale reads: local application state
        value = s.raft.GetApplication().GetValue(key)
        found = true
    } else {
        // Linearizable reads: use Raft Forward()
        ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
        defer cancel()

        resp, err := s.raft.ForwardGet(ctx, &raftpb.GetRequest{
            Key: int32(key),
        })

        if err != nil {
            if ctx.Err() == context.DeadlineExceeded {
                http.Error(w, "Request timeout", http.StatusGatewayTimeout)
            } else {
                http.Error(w, err.Error(), http.StatusInternalServerError)
            }
            return
        }

        value = int(resp.Value)
        found = resp.Found
    }

    // Send response (same as before)
    resp := GetResponse{Key: key, Value: value, Found: found}
    w.Header().Set("Content-Type", "application/json")
    json.NewEncoder(w).Encode(resp)
}
```

#### 1.5 Implement ForwardGet in Transport Layer
**File**: `pkg/raft/transport.go`
**Action**: Add ForwardGet to GRPCPeerClient and PeerClient interface

```go
type PeerClient interface {
    // ... existing methods
    ForwardGet(ctx context.Context, req *raftpb.GetRequest) (*raftpb.GetResponse, error)
}

func (g *GRPCPeerClient) ForwardGet(ctx context.Context, req *raftpb.GetRequest) (*raftpb.GetResponse, error) {
    return g.client.ForwardGet(ctx, req)
}
```

#### 1.6 Update Test Mocks
**File**: `pkg/raft/tests_setup.go`
**Action**: Add ForwardGet to mock implementations

---

### Stage 2: Fix PUT/DELETE Request Handling ⚡ HIGH PRIORITY
**Estimated Time**: 45 minutes
**Risk Level**: 🟡 Medium (write operations, need careful testing)

#### 2.1 Create Unified Write Forward Handler in Raft Core
**File**: `pkg/raft/grpc.go`
**Action**: Enhance existing `Forward()` to return operation result

**Current signature**:
```go
func (r *Raft) Forward(ctx context.Context, msg *raftpb.Message) (*raftpb.Null, error)
```

**New approach**: Modify `Forward()` to wait for commit and return result

```go
func (r *Raft) Forward(ctx context.Context, msg *raftpb.Message) (*raftpb.ForwardResponse, error) {
    slog.Info(fmt.Sprintf("Received Forward on node %d", r.id))

    // Convert protobuf to internal message
    var val *int
    if msg.Value != nil {
        tmp := int(msg.Value.Value)
        val = &tmp
    }

    internal := Message{
        MsgType:          MessageType(msg.Type.String()),
        Key:              int(msg.Key),
        Value:            val,
        IdempotencyToken: msg.IdempotencyToken,
        ClientID:         msg.ClientId,
    }

    // Check if leader
    isLeader, leaderID := r.getLeaderData()

    if !isLeader {
        // Forward to actual leader
        if leaderID < 0 {
            return nil, fmt.Errorf("no leader known")
        }
        if ctx.Value(forwardHopKey{}) != nil {
            return nil, fmt.Errorf("redirect loop detected")
        }
        ctx = context.WithValue(ctx, forwardHopKey{}, true)

        peer := r.getPeer(leaderID)
        if peer == nil {
            return nil, fmt.Errorf("no peer for node %d", leaderID)
        }

        return peer.Forward(ctx, msg)
    }

    // Leader: Broadcast and wait for commit
    success, value, err := r.BroadcastSync(internal, 5*time.Second)
    if err != nil {
        return nil, err
    }

    return &raftpb.ForwardResponse{
        Success: success,
        Value:   int32(value),
    }, nil
}
```

#### 2.2 Update Protobuf Definition
**File**: `pkg/raft/raftpb/raft.proto`
**Action**: Add response message and update Forward RPC

```protobuf
message ForwardResponse {
    bool success = 1;
    int32 value = 2;
}

message Message {
    MessageType type = 1;
    int32 key = 2;
    google.protobuf.Int32Value value = 3;
    string idempotency_token = 4;  // ADD THIS
    string client_id = 5;           // ADD THIS
}

service Raft {
    // ... existing methods
    rpc Forward(Message) returns (ForwardResponse);  // Changed from Null
}
```

#### 2.3 Regenerate Protobuf Code
**Command**: Same as Stage 1.3

#### 2.4 Update API Server PUT Handler
**File**: `pkg/api/server.go`
**Action**: Replace `BroadcastSync` with `Forward()` gRPC call

**BEFORE** (lines 129-151):
```go
retryFunc := func(ctx context.Context) error {
    msg := raft.Message{
        MsgType:          "PUT",
        Key:              req.Key,
        Value:            &req.Value,
        IdempotencyToken: idempotencyToken,
        ClientID:         getClientID(r),
    }

    var err error
    success, _, err = s.raft.BroadcastSync(msg, 5*time.Second)  // ❌ PROBLEM
    broadcastErr = err
    return err
}
```

**AFTER**:
```go
retryFunc := func(ctx context.Context) error {
    msg := &raftpb.Message{
        Type:             raftpb.MessageType_PUT,
        Key:              int32(req.Key),
        Value:            wrapperspb.Int32(int32(req.Value)),
        IdempotencyToken: idempotencyToken,
        ClientId:         getClientID(r),
    }

    // Use Forward() gRPC - works on any node
    resp, err := s.raft.Forward(ctx, msg)
    if err != nil {
        broadcastErr = err
        return err
    }

    success = resp.Success
    return nil
}
```

#### 2.5 Update API Server DELETE Handler
**File**: `pkg/api/server.go`
**Action**: Same pattern as PUT - replace `BroadcastSync` with `Forward()`

**BEFORE** (lines 264-277):
```go
retryFunc := func(ctx context.Context) error {
    msg := raft.Message{
        MsgType:          "DELETE",
        Key:              key,
        Value:            nil,
        IdempotencyToken: idempotencyToken,
        ClientID:         getClientID(r),
    }

    var err error
    success, _, err = s.raft.BroadcastSync(msg, 5*time.Second)  // ❌ PROBLEM
    broadcastErr = err
    return err
}
```

**AFTER**:
```go
retryFunc := func(ctx context.Context) error {
    msg := &raftpb.Message{
        Type:             raftpb.MessageType_DELETE,
        Key:              int32(key),
        Value:            nil,
        IdempotencyToken: idempotencyToken,
        ClientId:         getClientID(r),
    }

    // Use Forward() gRPC - works on any node
    resp, err := s.raft.Forward(ctx, msg)
    if err != nil {
        broadcastErr = err
        return err
    }

    success = resp.Success
    return nil
}
```

#### 2.6 Update Transport Layer
**File**: `pkg/raft/transport.go`
**Action**: Update Forward() signature in PeerClient interface

```go
type PeerClient interface {
    // ... existing methods
    Forward(ctx context.Context, msg *raftpb.Message) (*raftpb.ForwardResponse, error)  // Changed return type
    ForwardGet(ctx context.Context, req *raftpb.GetRequest) (*raftpb.GetResponse, error)
}

func (g *GRPCPeerClient) Forward(ctx context.Context, msg *raftpb.Message) (*raftpb.ForwardResponse, error) {
    return g.client.Forward(ctx, msg)
}
```

#### 2.7 Update Test Mocks
**File**: `pkg/raft/tests_setup.go`
**Action**: Update mock Forward() signatures

---

### Stage 3: Testing & Validation ✅ CRITICAL
**Estimated Time**: 1 hour
**Risk Level**: 🟢 Low (validation only)

#### 3.1 Unit Tests
**File**: `pkg/api/server_test.go` (create if doesn't exist)

Test scenarios:
- ✅ GET on leader returns data
- ✅ GET on follower forwards and returns data
- ✅ GET with `?stale=true` reads local state
- ✅ PUT on leader commits successfully
- ✅ PUT on follower forwards and commits
- ✅ DELETE on leader commits successfully
- ✅ DELETE on follower forwards and commits
- ✅ Idempotency works across forwarding
- ⚠️ Forward loop detection works
- ⚠️ Timeout handling for unavailable leader

#### 3.2 Integration Tests
**Command**: Use existing `cmd/raftcli` for manual testing

Test workflow:
```bash
# Start 3-node cluster
docker-compose up -d

# Identify leader
curl http://localhost:8001/leader
curl http://localhost:8002/leader
curl http://localhost:8003/leader

# Test GET on follower (should work now)
curl http://localhost:8002/kv/42  # Follower node
# Expected: 200 OK with data (not 503)

# Test PUT on follower (should not hang)
time curl -X POST http://localhost:8002/kv \
  -H "Content-Type: application/json" \
  -d '{"key": 42, "value": 100}'
# Expected: Response in <1s (not 5s timeout)

# Verify data consistency
curl http://localhost:8001/kv/42
curl http://localhost:8002/kv/42
curl http://localhost:8003/kv/42
# Expected: All return same value
```

#### 3.3 Performance Validation
**Metrics to check**:
- PUT/DELETE latency on followers: Should be <500ms (down from 5s timeout)
- GET latency on followers: Should be <200ms
- No increase in leader load (same number of commits)

#### 3.4 Regression Testing
- Ensure existing functionality still works:
  - Leader election
  - Log replication
  - Snapshot handling
  - Idempotency caching

---

### Stage 4: Cleanup & Documentation 📚
**Estimated Time**: 30 minutes
**Risk Level**: 🟢 None

#### 4.1 Remove Dead Code (Optional)
Consider deprecating/removing:
- `BroadcastSync()` if only used by API layer
- Manual leadership checks in API handlers
- Leader address caching (if no longer needed)

**Decision**: Keep for backward compatibility, mark as deprecated

#### 4.2 Update Documentation
**File**: `README.md` or `docs/api.md`

Document the change:
```markdown
## API Behavior

### Read Operations (GET)
- **Linearizable reads** (default): Automatically forwarded to leader for consistency
- **Stale reads** (`?stale=true`): Served from local node, may be slightly outdated

### Write Operations (PUT/DELETE)
- Automatically forwarded to leader if sent to follower
- Response received after successful commit (typically <500ms)
- Idempotency ensured via Idempotency-Key header or content-based token
```

#### 4.3 Update Comments
Add clarifying comments in code:
```go
// handleGet uses Raft's Forward() mechanism to ensure linearizable reads
// by automatically routing requests to the leader. Stale reads can bypass
// this for performance at the cost of potential inconsistency.
```

---

## Risk Mitigation

### Protobuf Changes
**Risk**: Breaking changes in .proto file
**Mitigation**:
- Use field numbers >10 for new fields (avoid conflicts)
- Keep existing RPC signatures compatible where possible
- Version the protobuf package if needed

### Response Timeout Changes
**Risk**: Clients may have hardcoded 5s timeout expectations
**Mitigation**:
- Keep timeout configurable
- Log warnings if operations take >1s
- Monitor P99 latency in production

### Idempotency Cache
**Risk**: Cache misses after forwarding due to different token generation
**Mitigation**:
- Ensure IdempotencyToken is preserved in Message protobuf
- Test cache hit rate before/after changes

---

## Rollback Plan

If issues discovered:

1. **Immediate**: Revert `pkg/api/server.go` changes
   - Restore old GET handler with manual leader check
   - Restore old PUT/DELETE with BroadcastSync

2. **Quick**: Revert protobuf changes
   - Restore `raft.proto` from git
   - Regenerate: `protoc --go_out=. --go-grpc_out=. raft.proto`
   - Revert `grpc.go` Forward() signature

3. **Complete**: `git revert <commit-hash>`

---

## Success Criteria

### Must Have ✅
- [ ] GET requests succeed on all nodes (leader + followers)
- [ ] PUT/DELETE requests complete in <1s on followers
- [ ] No hanging requests or timeouts
- [ ] All integration tests pass
- [ ] No data loss or consistency issues

### Should Have 🎯
- [ ] Improved user experience (no manual leader discovery)
- [ ] Reduced latency for follower writes (5s → <500ms)
- [ ] Clear error messages for failure cases
- [ ] Comprehensive test coverage

### Nice to Have 🌟
- [ ] Monitoring/metrics for forwarding rate
- [ ] Performance benchmarks documented
- [ ] API documentation updated

---

## Timeline Estimate

| Stage | Duration | Dependencies |
|-------|----------|--------------|
| Stage 1: Fix GET | 30 min | None |
| Stage 2: Fix PUT/DELETE | 45 min | Stage 1 (protobuf) |
| Stage 3: Testing | 60 min | Stage 1+2 |
| Stage 4: Cleanup | 30 min | Stage 3 |
| **Total** | **~3 hours** | Sequential |

---

## Next Steps

1. **User Review**: Review this plan, provide feedback
2. **Approval**: Confirm approach before implementation
3. **Implementation**: Execute stages sequentially
4. **Validation**: Run full test suite
5. **Deployment**: Roll out to staging, then production

---

## Questions for User

Before proceeding, please confirm:

1. ✅ **Approach**: Is the Forward()-based solution acceptable?
2. ✅ **Protobuf changes**: OK to modify .proto and regenerate code?
3. ✅ **Testing**: Do you have integration tests, or create new ones?
4. ✅ **Timeline**: Is 3-hour estimate acceptable?
5. ❓ **Scope**: Any other API endpoints need similar fixes?

**Status**: 🟡 AWAITING USER APPROVAL
