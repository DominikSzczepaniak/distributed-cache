Here is the detailed execution plan for **Stage 4**.

In this stage, we secure the data. We move from a "Safe but Fragile" system (where a node crash loses data, though safely) to a "Durable" system (where data survives a single node failure).

We will implement **Synchronous Primary-Copy Replication**.

---

## File: Stage4.md

```markdown
# Stage 4: Synchronous Replication (Data Safety)

**Objective:** Modify the Data Node to replicate writes to a secondary node *before* acknowledging success to the Client. This ensures that if the Primary crashes immediately after responding, the data is preserved on the Replica.

**Consistency Rule:** Reads are served **only** by the Primary. The Replica is a "Hot Standby" and does not serve read traffic (to avoid serving stale data).

---

### **Substage 4.1: Replica Resolution**

The Primary Data Node needs to know "Who is my backup?" based on the current configuration.

*   **Goal:** Add helper methods to resolve Replica URLs.
*   **Logic:**
    1.  Request comes for Key `K`.
    2.  Hash `K` -> Shard `S`.
    3.  Lookup Shard `S` in local `ClusterConfig`.
    4.  If `I_AM_PRIMARY`:
        *   Get `ReplicaIDs` list.
        *   Convert `ReplicaID` -> `ReplicaURL` (from the Nodes map).

**Action Plan:**

1.  Modify `pkg/datanode/state.go`.
2.  Add function `GetReplicaURL(shardID int) (string, bool)`.

-----

### **Substage 4.2: The Internal Replication Endpoint**

We need a private channel for nodes to talk to each other. We shouldn't use the public `PUT /data` because replication traffic implies "I am the Primary commanding you to write," not "I am a Client asking you to write."

*   **Goal:** Implement `POST /internal/replicate`.
*   **User Story:** As a Replica, I accept writes from the Primary. I must still validate the Epoch to ensure the Primary isn't a zombie.

**Proposed Code (`pkg/datanode/server.go`):**

```go
func (s *Server) handleReplicate(w http.ResponseWriter, r *http.Request) {
    var req WriteRequest
    // Decode...

    // 1. LEASE CHECK (Ideally, even Replicas should have leases, 
    // but primarily we check Epoch here).
    
    // 2. EPOCH CHECK (Critical Fencing)
    // If Primary thinks it's Epoch 10, but I know it's Epoch 11,
    // I reject the replication. The Primary has been demoted and doesn't know it yet.
    if req.Epoch < s.state.Epoch {
        http.Error(w, "Primary is Stale", http.StatusPreconditionFailed)
        return
    }

    // 3. WRITE
    // Direct write to cache. 
    s.cache.Put(req.Key, req.Value)
    w.WriteHeader(http.StatusOK)
}
```

**Acceptance Criteria:**
*   [ ] `curl -X POST /internal/replicate` writes data.
*   [ ] Access logs distinguish between Client writes and Replication writes.

-----

### **Substage 4.3: The Synchronous Write Path**

This is the critical path where we trade latency for durability.

*   **Goal:** Update the Primary's `handlePut` logic.
*   **Logic:**
    1.  **Safety Checks:** Lease Valid? Epoch Valid? Am I Primary?
    2.  **Replication Step:**
        *   Identify Replica.
        *   Send HTTP POST to Replica.
        *   **CRITICAL:** If Replica fails (500, Timeout, Network Error), **ABORT THE WRITE**.
        *   Return `500 Internal Server Error` to Client.
        *   *Why?* If we wrote locally but failed to replicate, we violated the durability guarantee.
    3.  **Local Write:** Only if Replica returned 200 OK.
    4.  **Ack:** Return 200 OK to Client.

**Proposed Logic:**

```go
func (s *Server) handlePut(w http.ResponseWriter, r *http.Request) {
    // ... Safety Checks ...

    // 1. Identify Replica
    replicaURL, hasReplica := s.state.GetReplicaURL(shardID)

    // 2. Synchronous Replication
    if hasReplica {
        err := s.sendReplication(replicaURL, req)
        if err != nil {
            slog.Error("Replication failed", "err", err)
            // Fail safe: Do not write locally if we can't replicate.
            http.Error(w, "Replication Failed", http.StatusInternalServerError)
            return
        }
    }

    // 3. Local Commit
    s.cache.Put(req.Key, req.Value)
    w.WriteHeader(http.StatusOK)
}
```

-----

### **Substage 4.4: Integration Verification (Data Safety)**

We validate that data survives a node crash.

*   **Goal:** Verify failover preserves data.
*   **Setup:**
    *   Controller + Node A (Primary) + Node B (Replica).
*   **Test Steps:**
    1.  Client writes Key "100" to Node A.
    2.  Node A replicates to Node B. Both ACK. Success.
    3.  **Crash:** `docker stop` Node A.
    4.  **Failover:** Controller detects death, promotes Node B to Primary.
    5.  **Recovery:** Client fetches map (sees B is Primary).
    6.  **Read:** Client `GET 100` from Node B.
    7.  **Result:** Value "100" MUST be returned.

**Acceptance Criteria:**
*   [ ] Positive Case: Data exists on Node B after Node A dies.
*   [ ] Negative Case (Consistency): If you stop Node B (Replica), writes to Node A MUST fail (500 Error). This proves we are not accepting "unsafe" single-node writes.

---

### **Deliverable Checklist for Stage 4**

At the end of this stage, you have:
1.  [ ] **Durability:** Writes are stored on N+1 nodes.
2.  [ ] **Consistency:** Writes fail if the cluster cannot guarantee redundancy.
3.  [ ] **Epoch Guarding:** Replication traffic is fenced just like Client traffic.
```