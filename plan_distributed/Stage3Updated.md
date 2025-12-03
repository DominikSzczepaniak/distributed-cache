Here is the detailed execution plan for **Stage 3**.

In this stage, we implement the "Brain" of the operation. Now that Data Nodes automatically "suicide" (fence themselves) via Leases (Stage 2), the Controller needs the logic to detect this and officially update the world map to restore service availability.

---

## File: Stage3.md

```markdown
# Stage 3: High Availability (Failover & Healing)

**Objective:** Implement the Failure Detection ("The Reaper") and Topology Recovery ("The Rebalancer") logic in the Controller.

**The Logic Chain:**
1.  Node A stops heartbeating (crash or partition).
2.  **Reaper:** Controller waits `GracePeriod` (e.g., 5s). Marks Node A as `DEAD`.
3.  **Rebalancer:** Controller updates Shard Map (Promotes Node B to Primary for A's shards).
4.  **Consensus:** Controller commits new Map to Raft (`Epoch` increments).
5.  **Propagation:** Surviving nodes see new Epoch, update their internal state.

---

### **Substage 3.1: The Reaper (Liveness Tracking)**

The Controller needs to track the last time it heard from every node. This state is **volatile** (in-memory only), as it is rebuilt continuously by heartbeats.

*   **Goal:** Create `pkg/controller/reaper.go`.
*   **Logic:**
    1.  Maintain `lastSeen map[string]time.Time`.
    2.  Update this map whenever `POST /cluster/heartbeat` is called.
    3.  Run a background loop checking for expiration.

**Proposed Code (`pkg/controller/reaper.go`):**

```go
type Reaper struct {
    mu          sync.RWMutex
    lastSeen    map[string]time.Time
    gracePeriod time.Duration
    onDeath     func(nodeID string) // Callback to trigger failover
}

func (r *Reaper) Track(nodeID string) {
    r.mu.Lock()
    defer r.mu.Unlock()
    r.lastSeen[nodeID] = time.Now()
}

func (r *Reaper) Run() {
    ticker := time.NewTicker(1 * time.Second)
    for range ticker.C {
        r.checkLiveness()
    }
}

func (r *Reaper) checkLiveness() {
    r.mu.Lock()
    defer r.mu.Unlock()
    
    for id, seen := range r.lastSeen {
        if time.Since(seen) > r.gracePeriod {
            // Node is DEAD.
            // Critical: Remove from map so we don't trigger this twice.
            delete(r.lastSeen, id)
            go r.onDeath(id) 
        }
    }
}
```

---

### **Substage 3.2: The Rebalancer (Topology Updates)**

When a node dies, we must modify the `ClusterConfig` via Raft.

*   **Goal:** Implement `HandleNodeFailure` in the Controller.
*   **Logic:**
    1.  Load current Config.
    2.  Iterate Shards.
    3.  If `Shard.Primary == DeadNode`:
        *   **Scenario A (Has Replicas):** Promote `ReplicaIDs[0]` to Primary.
        *   **Scenario B (No Replicas):** Mark Shard as `UNAVAILABLE` (AP vs CP choice: we choose CP, so write availability is lost until node returns).
    4.  Broadcast `UPDATE_TOPOLOGY` command to Raft.

**Action Plan:**

1.  Update `pkg/metadata/store.go` to support `Rebalance` logic.
2.  Wire `Reaper.onDeath` to call this logic.

---

### **Substage 3.3: Data Node Topology Awareness**

The Data Node currently fetches the Epoch, but it doesn't strictly *obey* the shard assignment yet. It needs to check if it actually owns the key it receives.

*   **Goal:** Update `pkg/datanode/server.go`.
*   **Logic:**
    *   In `handlePut`, after checking Lease and Epoch:
    *   `shardID := hash(key) % totalShards`
    *   `if config.Shards[shardID].Primary != myNodeID { return 400 Bad Request ("Not Primary") }`

**Why is this needed?**
If a node was partitioned (Lease expired) and then reconnects, it might have been demoted by the Controller while it was away. When it downloads the new Map (Epoch N+1), it must realize: "Oh, I am no longer the Primary for Shard 5," and stop accepting writes for it.

---

### **Substage 3.4: The "Split Brain" Integration Test**

This validates the core architectural promise of the system.

*   **Goal:** Prove that consistency is maintained during a network partition.
*   **Setup:**
    *   1 Controller (Leader).
    *   2 DataNodes (A and B).
    *   Config: Shard 1 -> Primary: A, Replica: B.

**Test Script (Bash/Go):**

1.  **Initial State:**
    *   Write Key "1" to Node A. **Success**.
2.  **Partition:**
    *   `docker network disconnect` Node A from Controller.
3.  **The "Zombie" Phase (0s - 5s):**
    *   Client tries Write to Node A.
    *   **Result:** Node A *might* accept for a few seconds (until local lease expires). *Note: In extremely strict systems, we'd use smaller leases, but this window is the "Availability" trade-off.*
4.  **The "Fenced" Phase (5s+):**
    *   Node A Lease expires.
    *   Client tries Write to Node A.
    *   **Result:** **503 Service Unavailable**. (Consistency Saved! Node A refuses to be a split-brain leader).
5.  **The Failover (Controller side):**
    *   Controller Reaper sees A missing.
    *   Promotes B to Primary. Epoch increments.
6.  **Recovery:**
    *   Client fetches new Topology (Sees Primary is B).
    *   Client writes to Node B. **Success**.

**Acceptance Criteria:**
*   [ ] The test script passes.
*   [ ] Logs confirm Node A specifically logged "Lease Expired: Fencing self".
*   [ ] Logs confirm Controller logged "Node A Dead: Promoting B".

---

### **Deliverable Checklist for Stage 3**

At the end of this stage, you have:
1.  [ ] **Self-Healing:** The system detects failures and reconfigures itself.
2.  [ ] **Strict Consistency:** Stale nodes self-fence via Leases.
3.  [ ] **Dynamic Authority:** Shard ownership can move from node to node programmatically.
```