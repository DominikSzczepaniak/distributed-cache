Here is the detailed execution plan for **Stage 2**.

In this stage, we build the "Muscle" of the system. We move away from Raft for data storage and build a standalone, high-performance Data Node.

**Crucially**, this is where we implement the **Lease Mechanism**. Without this stage being implemented correctly, the system cannot guarantee Strong Consistency in the event of a network partition.

---

## File: Stage2.md

```markdown
# Stage 2: The Data Plane & Lease Safety

**Objective:** Build the `datanode` binary. This node stores user Key-Value data in memory. Unlike the Controller, it does **not** run Raft. Instead, it ensures consistency by strictly enforcing **Leases** and **Epochs**.

**The Golden Rule of Stage 2:** "If I cannot talk to the Controller, I must assume I am dead and refuse all writes."

---

### **Substage 2.1: Skeleton & Configuration**

We need a separate entry point. This node acts as a worker.

*   **Goal:** Create `cmd/datanode/main.go`.
*   **Requirements:**
    *   Must **not** import `pkg/raft`.
    *   Must accept configuration for:
        *   `CONTROLLER_URL` (e.g., `http://raft-node-0:8080`)
        *   `NODE_ID` (e.g., `10.0.1.5:9000`)
        *   `LEASE_DURATION` (default: 5 seconds)

**Action Plan:**

1.  Create `cmd/datanode/main.go`.
2.  Define `DataNodeConfig` struct.
3.  Implement basic HTTP server startup (without handlers yet).

-----

### **Substage 2.2: The Lease Monitor (Heartbeat Loop)**

This is the most critical safety mechanism in the entire distributed system.

*   **Goal:** Implement a background routine that renews the node's "License to Write."
*   **User Story:** As a Data Node, I want to ping the Controller every 1 second. If I succeed, I extend my lease for 5 seconds. If I fail, I let my lease expire.

**Action Plan:**

1.  Create `pkg/datanode/lease.go`.
2.  Implement `LeaseManager`.

**Proposed Code (`pkg/datanode/lease.go`):**

```go
type LeaseManager struct {
    mu           sync.RWMutex
    validUntil   time.Time
    duration     time.Duration
    controllerURL string
    nodeID       string
}

// Start runs in a goroutine
func (l *LeaseManager) Start() {
    ticker := time.NewTicker(l.duration / 3) // e.g., every 1.5s if lease is 5s
    for range ticker.C {
        if l.renew() {
            l.mu.Lock()
            l.validUntil = time.Now().Add(l.duration)
            l.mu.Unlock()
        }
        // If renew fails, we do NOT update validUntil. 
        // Eventually Now() > validUntil, and IsActive() returns false.
    }
}

func (l *LeaseManager) renew() bool {
    // POST /cluster/heartbeat { "node_id": l.nodeID }
    // Return true if status == 200 OK
}

func (l *LeaseManager) IsActive() bool {
    l.mu.RLock()
    defer l.mu.RUnlock()
    return time.Now().Before(l.validUntil)
}
```

**Acceptance Criteria:**
*   [ ] Start Controller. Start DataNode.
*   [ ] Logs show "Lease renewed".
*   [ ] Stop Controller.
*   [ ] Logs show "Heartbeat failed".
*   [ ] After 5 seconds, `IsActive()` returns `false`.

-----

### **Substage 2.3: State Management (Epochs & Map)**

The Data Node needs to know "Who owns what?" and "What time is it?".

*   **Goal:** Maintain a local copy of `metadata.ClusterConfig`.
*   **Logic:**
    1.  The `renew()` heartbeat response from the Controller should include the current `Epoch`.
    2.  If `Response.Epoch > Local.Epoch`:
        *   Fetch full topology via `GET /topology`.
        *   Update local config.
        *   Log: "Updated to Epoch X".

**Action Plan:**

1.  Create `pkg/datanode/state.go`.
2.  Add a `ClusterConfig` field to the DataNode.
3.  Update the `LeaseManager` to handle the Epoch check.

-----

### **Substage 2.4: The Guarded Storage Engine**

Now we expose the actual Data API. We wrap your existing `pkg/cache` (ConcurrentMapCache) with the safety checks.

*   **Goal:** Implement HTTP handlers for `PUT /data` and `GET /data`.

**Action Plan:**

1.  Create `pkg/datanode/server.go`.
2.  Implement `handlePut`.

**Crucial Logic (`handlePut`):**

```go
type WriteRequest struct {
    Key   string `json:"key"`
    Value string `json:"value"`
    Epoch uint64 `json:"epoch"` // Client MUST provide this
}

func (s *Server) handlePut(w http.ResponseWriter, r *http.Request) {
    // 1. LEASE CHECK (Safety against Split Brain)
    if !s.lease.IsActive() {
        http.Error(w, "Node is Fenced (Lease Expired)", http.StatusServiceUnavailable) // 503
        return
    }

    // 2. EPOCH CHECK (Safety against Stale Clients)
    if req.Epoch < s.state.Epoch {
        http.Error(w, "Client Epoch Stale", http.StatusPreconditionFailed) // 412
        return
    }
    
    // 3. OWNERSHIP CHECK (Am I the primary?)
    // (We will implement the strict check in Stage 3, but for now, 
    // if the client sent it here, we assume we are the primary if Epoch matches).

    // 4. WRITE
    s.cache.Put(req.Key, req.Value)
    w.WriteHeader(http.StatusOK)
}
```

**Acceptance Criteria:**
*   [ ] `curl -X POST /data` with valid Epoch -> 200 OK.
*   [ ] `curl -X POST /data` with old Epoch -> 412 Error.
*   [ ] Stop Controller -> Wait 5s -> `curl -X POST /data` -> 503 Error.

-----

### **Substage 2.5: Docker Integration**

We need to run this alongside the Controller.

*   **Goal:** Update `docker-compose.yml`.

**Action Plan:**

1.  Add `datanode` service.
2.  Ensure it depends on `controller`.

```yaml
  datanode-1:
    build:
      context: .
      dockerfile: deploy/Dockerfile.datanode
    environment:
      - CONTROLLER_URL=http://controller-1:8080
      - NODE_ID=datanode-1:9000
    depends_on:
      - controller-1
```

---

### **Deliverable Checklist for Stage 2**

At the end of this stage, you have:
1.  [ ] A Data Node that automatically registers with the Controller (via Heartbeat).
2.  [ ] **Strong Consistency Guarantee:** The node refuses writes if partitioned from the Controller.
3.  [ ] **Stale Client Protection:** The node refuses writes if the client is outdated.
4.  [ ] A functional (non-replicated) KV store.
```