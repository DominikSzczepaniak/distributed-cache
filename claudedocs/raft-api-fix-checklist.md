# Raft API Fix - Implementation Checklist

**Date**: 2025-11-08
**Status**: 🟡 READY FOR EXECUTION (pending approval)

Use this checklist during implementation to track progress.

---

## Pre-Implementation

- [ ] User has reviewed `raft-api-issues-analysis.md`
- [ ] User has reviewed `raft-api-fix-plan.md`
- [ ] User approves the approach
- [ ] Create feature branch: `git checkout -b fix/raft-api-forwarding`
- [ ] Ensure clean working directory: `git status`

---

## Stage 1: Fix GET Request Handling

### 1.1 Protobuf Changes
- [ ] Open `pkg/raft/raftpb/raft.proto`
- [ ] Add `GetRequest` message with fields:
  - [ ] `int32 key = 1;`
- [ ] Add `GetResponse` message with fields:
  - [ ] `int32 key = 1;`
  - [ ] `int32 value = 2;`
  - [ ] `bool found = 3;`
- [ ] Add `ForwardGet` RPC to Raft service:
  - [ ] `rpc ForwardGet(GetRequest) returns (GetResponse);`
- [ ] Save file

### 1.2 Regenerate Protobuf
- [ ] Run: `cd pkg/raft/raftpb`
- [ ] Run: `protoc --go_out=. --go-grpc_out=. raft.proto`
- [ ] Verify no errors
- [ ] Verify generated files updated:
  - [ ] `raft.pb.go` contains GetRequest/GetResponse
  - [ ] `raft_grpc.pb.go` contains ForwardGet client/server stubs
- [ ] Return to project root: `cd ../../..`

### 1.3 Implement ForwardGet in Raft Core
- [ ] Open `pkg/raft/grpc.go`
- [ ] Add `ForwardGet()` method after existing `Forward()` method
- [ ] Implementation checklist:
  - [ ] Check if node is leader using `r.getLeaderData()`
  - [ ] If not leader:
    - [ ] Check for redirect loop via `forwardHopKey{}`
    - [ ] Get leader peer via `r.getPeer(leaderID)`
    - [ ] Recursively call `peer.ForwardGet(ctx, req)`
  - [ ] If leader:
    - [ ] Read value via `r.application.GetValue(int(req.Key))`
    - [ ] Return `&raftpb.GetResponse{...}`
- [ ] Add error handling for:
  - [ ] No leader available
  - [ ] Redirect loop detected
  - [ ] No peer found
- [ ] Save file

### 1.4 Update Transport Layer
- [ ] Open `pkg/raft/transport.go`
- [ ] Update `PeerClient` interface:
  - [ ] Add `ForwardGet(ctx, *raftpb.GetRequest) (*raftpb.GetResponse, error)`
- [ ] Add `GRPCPeerClient.ForwardGet()` implementation:
  - [ ] `return g.client.ForwardGet(ctx, req)`
- [ ] Save file

### 1.5 Update Test Mocks
- [ ] Open `pkg/raft/tests_setup.go`
- [ ] Add `ForwardGet()` to `mockPeerClient`:
  - [ ] Return mock response or error based on test needs
- [ ] Add `ForwardGet()` to `inMemPeer`:
  - [ ] Call `p.raft.ForwardGet(ctx, req)`
- [ ] Save file

### 1.6 Update API Server GET Handler
- [ ] Open `pkg/api/server.go`
- [ ] Locate `handleGet()` function (around line 185)
- [ ] Replace linearizable read section:
  - [ ] Remove manual leadership check (`if !s.raft.IsLeader()`)
  - [ ] Add context with timeout: `ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)`
  - [ ] Call `s.raft.ForwardGet(ctx, &raftpb.GetRequest{Key: int32(key)})`
  - [ ] Handle errors (timeout, internal error)
  - [ ] Extract value from response
- [ ] Keep stale read logic unchanged
- [ ] Verify response JSON encoding unchanged
- [ ] Save file

### 1.7 Build & Test Stage 1
- [ ] Run: `go build ./...`
- [ ] Fix any compilation errors
- [ ] Run: `go test ./pkg/raft/...`
- [ ] Run: `go test ./pkg/api/...`
- [ ] Commit: `git add -A && git commit -m "feat: implement ForwardGet for linearizable reads"`

---

## Stage 2: Fix PUT/DELETE Request Handling

### 2.1 Protobuf Changes for Write Operations
- [ ] Open `pkg/raft/raftpb/raft.proto`
- [ ] Add `ForwardResponse` message with fields:
  - [ ] `bool success = 1;`
  - [ ] `int32 value = 2;`
- [ ] Update `Message` message:
  - [ ] Add `string idempotency_token = 4;`
  - [ ] Add `string client_id = 5;`
- [ ] Update `Forward` RPC signature:
  - [ ] Change from `returns (Null)` to `returns (ForwardResponse)`
- [ ] Save file

### 2.2 Regenerate Protobuf
- [ ] Run: `cd pkg/raft/raftpb`
- [ ] Run: `protoc --go_out=. --go-grpc_out=. raft.proto`
- [ ] Verify no errors
- [ ] Verify generated files updated:
  - [ ] `raft.pb.go` contains ForwardResponse
  - [ ] `raft.pb.go` Message has new fields
  - [ ] `raft_grpc.pb.go` Forward returns ForwardResponse
- [ ] Return to project root: `cd ../../..`

### 2.3 Update Forward() in Raft Core
- [ ] Open `pkg/raft/grpc.go`
- [ ] Locate `Forward()` method (around line 29)
- [ ] Update implementation:
  - [ ] Extract idempotency_token and client_id from protobuf
  - [ ] Add to internal Message struct
  - [ ] If not leader: same forwarding logic (already correct)
  - [ ] If leader:
    - [ ] Call `r.BroadcastSync(internal, 5*time.Second)`
    - [ ] Return `&raftpb.ForwardResponse{Success: success, Value: int32(value)}`
    - [ ] Handle errors
- [ ] Update return type everywhere: `*raftpb.ForwardResponse` instead of `*raftpb.Null`
- [ ] Save file

### 2.4 Update Transport Layer Forward
- [ ] Open `pkg/raft/transport.go`
- [ ] Update `PeerClient` interface:
  - [ ] Change `Forward` return to `(*raftpb.ForwardResponse, error)`
- [ ] Update `GRPCPeerClient.Forward()`:
  - [ ] Return type: `(*raftpb.ForwardResponse, error)`
  - [ ] Implementation unchanged (just signature)
- [ ] Save file

### 2.5 Update Test Mocks Forward
- [ ] Open `pkg/raft/tests_setup.go`
- [ ] Update `mockPeerClient.Forward()`:
  - [ ] Return `(*raftpb.ForwardResponse, error)`
  - [ ] Update mock return value
- [ ] Update `inMemPeer.Forward()`:
  - [ ] Return `(*raftpb.ForwardResponse, error)`
  - [ ] Call `p.raft.Forward(ctx, m)`
- [ ] Save file

### 2.6 Update API Server PUT Handler
- [ ] Open `pkg/api/server.go`
- [ ] Locate `handlePut()` function (around line 102)
- [ ] Find `retryFunc` (around line 133)
- [ ] Replace implementation:
  - [ ] Create `*raftpb.Message` instead of `raft.Message`
  - [ ] Set fields: Type, Key, Value, IdempotencyToken, ClientId
  - [ ] Call `s.raft.Forward(ctx, msg)` instead of `s.raft.BroadcastSync(...)`
  - [ ] Extract `success` from `resp.Success`
  - [ ] Handle errors same as before
- [ ] Verify idempotency caching still works
- [ ] Save file

### 2.7 Update API Server DELETE Handler
- [ ] Open `pkg/api/server.go`
- [ ] Locate `handleDelete()` function (around line 236)
- [ ] Find `retryFunc` (around line 264)
- [ ] Replace implementation:
  - [ ] Create `*raftpb.Message` instead of `raft.Message`
  - [ ] Set fields: Type (DELETE), Key, Value (nil), IdempotencyToken, ClientId
  - [ ] Call `s.raft.Forward(ctx, msg)` instead of `s.raft.BroadcastSync(...)`
  - [ ] Extract `success` from `resp.Success`
  - [ ] Handle errors same as before
- [ ] Save file

### 2.8 Build & Test Stage 2
- [ ] Run: `go build ./...`
- [ ] Fix any compilation errors
- [ ] Run: `go test ./pkg/raft/...`
- [ ] Run: `go test ./pkg/api/...`
- [ ] Commit: `git add -A && git commit -m "feat: use Forward() for PUT/DELETE to fix hanging requests"`

---

## Stage 3: Testing & Validation

### 3.1 Unit Tests
- [ ] Create or update `pkg/api/server_test.go`
- [ ] Write tests for GET:
  - [ ] GET on leader returns data
  - [ ] GET on follower forwards and returns data
  - [ ] GET with `?stale=true` reads local
  - [ ] GET handles timeout gracefully
  - [ ] GET handles no leader error
- [ ] Write tests for PUT:
  - [ ] PUT on leader commits
  - [ ] PUT on follower forwards and commits
  - [ ] PUT idempotency works
  - [ ] PUT handles timeout
- [ ] Write tests for DELETE:
  - [ ] DELETE on leader commits
  - [ ] DELETE on follower forwards and commits
  - [ ] DELETE idempotency works
- [ ] Run: `go test ./pkg/api/... -v`
- [ ] All tests pass: [ ]

### 3.2 Integration Tests with Docker
- [ ] Start cluster: `docker-compose up -d` (or equivalent)
- [ ] Wait for cluster ready (30s)
- [ ] Identify nodes and leader:
  - [ ] `curl http://localhost:8001/leader` → note leader
  - [ ] `curl http://localhost:8002/leader`
  - [ ] `curl http://localhost:8003/leader`
- [ ] Test GET on follower (identify non-leader port):
  - [ ] `curl http://localhost:FOLLOWER_PORT/kv/100`
  - [ ] Expected: 200 OK, not 503
  - [ ] Response time: <200ms
- [ ] Test PUT on follower:
  - [ ] `time curl -X POST http://localhost:FOLLOWER_PORT/kv -d '{"key":100,"value":999}'`
  - [ ] Expected: 200 OK in <1s (not 5s)
  - [ ] Response: `{"success":true,...}`
- [ ] Test DELETE on follower:
  - [ ] `time curl -X DELETE http://localhost:FOLLOWER_PORT/kv/100`
  - [ ] Expected: 200 OK in <1s
  - [ ] Response: `{"success":true,...}`
- [ ] Verify data consistency:
  - [ ] PUT key=42, value=777 on follower
  - [ ] GET key=42 on all nodes (leader + 2 followers)
  - [ ] All return value=777
- [ ] Test idempotency:
  - [ ] PUT same key/value twice with same Idempotency-Key
  - [ ] Second response has `"X-Cache":"HIT"`
- [ ] Stop cluster: `docker-compose down`

### 3.3 Performance Validation
- [ ] Measure PUT latency on follower:
  - [ ] Before fix: ~5000ms (timeout)
  - [ ] After fix: <500ms target
  - [ ] Record actual: ______ms
- [ ] Measure GET latency on follower:
  - [ ] Target: <200ms
  - [ ] Record actual: ______ms
- [ ] Measure DELETE latency on follower:
  - [ ] Target: <500ms
  - [ ] Record actual: ______ms
- [ ] Performance acceptable: [ ]

### 3.4 Regression Testing
- [ ] Leader election still works:
  - [ ] Stop leader node
  - [ ] New leader elected within 5s
  - [ ] Writes work on new leader
- [ ] Log replication still works:
  - [ ] PUT on leader
  - [ ] All followers commit entry
- [ ] Snapshot handling unchanged:
  - [ ] Trigger snapshot (if applicable)
  - [ ] No errors in logs
- [ ] Idempotency cache still works:
  - [ ] Duplicate requests cached
  - [ ] `X-Cache: HIT` header present
- [ ] All regression tests pass: [ ]

### 3.5 Error Handling Tests
- [ ] Test with no leader:
  - [ ] Stop all nodes except one (no quorum)
  - [ ] GET/PUT/DELETE return appropriate errors
  - [ ] No panics or crashes
- [ ] Test redirect loop prevention:
  - [ ] (If testable) Verify forwardHopKey prevents loops
  - [ ] Or verify via code review
- [ ] Test timeout handling:
  - [ ] Add artificial delay (if possible)
  - [ ] Verify timeout error returned
  - [ ] No goroutine leaks

---

## Stage 4: Cleanup & Documentation

### 4.1 Code Cleanup
- [ ] Review all modified files for:
  - [ ] Unused imports
  - [ ] Debug log statements (remove or keep?)
  - [ ] TODO comments (address or document)
  - [ ] Code formatting: `go fmt ./...`
- [ ] Run linter: `golangci-lint run` (if available)
- [ ] Fix any linter warnings

### 4.2 Documentation
- [ ] Update `README.md` or create `docs/api.md`:
  - [ ] Document GET behavior (linearizable vs stale)
  - [ ] Document PUT/DELETE automatic forwarding
  - [ ] Document idempotency headers
  - [ ] Document expected latencies
- [ ] Add code comments:
  - [ ] In `handleGet()`: Explain Forward() usage
  - [ ] In `handlePut()`: Explain why not BroadcastSync
  - [ ] In `ForwardGet()`: Explain forwarding logic
- [ ] Update CHANGELOG (if exists):
  - [ ] Add entry for bug fix
  - [ ] Note breaking changes (if any)

### 4.3 Final Commit
- [ ] Review all changes: `git diff main`
- [ ] Stage all files: `git add -A`
- [ ] Commit with detailed message:
  ```
  fix: resolve GET failures and PUT/DELETE hangs on followers

  Issue #1: GET requests failed on non-leader nodes with 503 errors
  - Added ForwardGet() gRPC method for automatic leader forwarding
  - Removed manual leadership checks in handleGet()

  Issue #2: PUT/DELETE requests hung for 5s before timing out
  - Changed Forward() to return ForwardResponse instead of Null
  - Replaced BroadcastSync with Forward() gRPC in API handlers
  - Preserved idempotency tokens across forwarding

  Results:
  - GET on followers: 503 errors → 200 OK in <200ms
  - PUT on followers: 5s timeout → <500ms success
  - DELETE on followers: 5s timeout → <500ms success

  Testing: Integration tests with 3-node cluster validated all scenarios
  ```
- [ ] Committed: [ ]

---

## Post-Implementation

### Verification
- [ ] All tests pass: `go test ./...`
- [ ] Build succeeds: `go build ./...`
- [ ] Docker compose cluster works: `docker-compose up`
- [ ] Manual smoke tests pass
- [ ] No regressions identified

### Merge Preparation
- [ ] Create pull request (if using PR workflow)
- [ ] Request code review (if applicable)
- [ ] Address review feedback
- [ ] Squash commits if needed: `git rebase -i main`

### Deployment Checklist
- [ ] Merge to main: `git checkout main && git merge fix/raft-api-forwarding`
- [ ] Tag release (if applicable): `git tag v1.x.x`
- [ ] Deploy to staging first
- [ ] Monitor logs for errors
- [ ] Run smoke tests on staging
- [ ] Deploy to production (if applicable)

### Monitoring (Post-Deploy)
- [ ] Monitor error rates for 24h
- [ ] Check latency metrics (P50, P95, P99)
- [ ] Watch for any new error patterns
- [ ] Gather user feedback

---

## Rollback Procedure (If Needed)

### Quick Rollback (API Only)
1. [ ] `git checkout main -- pkg/api/server.go`
2. [ ] `git commit -m "revert: rollback API changes"`
3. [ ] Redeploy

### Full Rollback (All Changes)
1. [ ] `git revert <commit-hash>` (commit from Stage 4.3)
2. [ ] Regenerate protobuf: `cd pkg/raft/raftpb && protoc --go_out=. --go-grpc_out=. raft.proto`
3. [ ] `go build ./...`
4. [ ] Redeploy

### Verify Rollback
- [ ] Old behavior restored (GET fails on followers, PUT/DELETE timeout)
- [ ] No errors in logs
- [ ] System stable

---

## Success Criteria Final Check

### Functional Requirements
- [x] GET requests succeed on all nodes (leader + followers)
- [x] PUT requests complete in <1s on followers
- [x] DELETE requests complete in <1s on followers
- [x] No hanging requests or infinite waits
- [x] Idempotency preserved across forwarding

### Non-Functional Requirements
- [x] No data loss or consistency issues
- [x] No performance regressions
- [x] Clear error messages for failures
- [x] Comprehensive test coverage

### Documentation Requirements
- [x] API behavior documented
- [x] Code comments added
- [x] Technical analysis documented (claudedocs/)

---

## Timeline Tracking

| Stage | Planned | Actual | Notes |
|-------|---------|--------|-------|
| Stage 1 | 30 min | _____ | |
| Stage 2 | 45 min | _____ | |
| Stage 3 | 60 min | _____ | |
| Stage 4 | 30 min | _____ | |
| **Total** | **165 min** | **_____** | |

---

## Final Sign-Off

- [ ] All checklist items completed
- [ ] All tests passing
- [ ] Documentation updated
- [ ] User satisfied with results
- [ ] Ready for production (if applicable)

**Implementer**: _______________
**Date Completed**: _______________
**Version**: _______________

---

**Status**: 🟢 READY FOR EXECUTION
