Here is the detailed execution plan for **Stage 6**.

This stage is the "Advanced" phase. Now that we have a stable, consistent system, we implement the ability to **scale out** (add nodes) and redistribute data without taking the whole system offline.

**Crucial Architecture Decision:** To maintain Strong Consistency without extreme complexity (like double-writes), we will implement a **"Freeze & Switch"** migration strategy. This ensures that at no point in time do two nodes think they are the Primary for the same shard.

---

## File: Stage6.md

```markdown
# Stage 6: Dynamic Scaling (Data Rebalancing)

**Objective:** Allow the Cluster to add new Data Nodes and move Shards from overloaded nodes to empty nodes programmatically.

**The Migration Strategy:**
1.  **Snapshot:** The Target Node pulls bulk data from the Source Node.
2.  **Freeze:** The Source Node temporarily blocks writes (returns `423 Locked`).
3.  **Catchup:** The Target Node pulls any final updates that happened during step 1.
4.  **Switch:** The Controller updates the Topology (Epoch++) to make the Target Node the new Primary.

---

### **Substage 6.1: Bulk Data Transport**

Data Nodes need a "Backdoor" to move massive amounts of data efficiently, bypassing the standard key-by-key API.

*   **Goal:** Implement Import/Export endpoints on the Data Node.
*   **User Story:** As a Data Node, I need to stream all keys belonging to Shard X to another node.

**Action Plan:**

1.  **Update `pkg/datanode/store.go`:** Implement `ExportShard(shardID int) (map[string]string)`.
    *   *Note:* Since we use a simple map, we must iterate the whole map and filter by `hash(key) % totalShards == shardID`.
2.  **Implement HTTP Handlers:**
    *   `GET /internal/export?shard=1`: Stream JSON K/V pairs.
    *   `POST /internal/import`: Accept JSON stream and insert into cache.
    *   **Safety:** The Import handler must verify it is authorized (e.g., via a token from the Controller) but *ignore* the standard Lease/Epoch check since it is not serving traffic yet.

-----

### **Substage 6.2: The Migration State Machine (Controller)**

The Controller orchestrates the move. It effectively acts as a "Saga" manager.

*   **Goal:** Implement `MoveShard(shardID, sourceNode, targetNode)`.
*   **Logic:**

    1.  **Phase 1: Copy (Background)**
        *   Controller tells Target: `PullData(SourceURL, ShardID)`.
        *   Target calls Source's `/internal/export`.
        *   Source continues accepting Client writes.
        *   Target responds "Copy Complete".

    2.  **Phase 2: Freeze (The Critical Section)**
        *   Controller updates Topology: Mark Shard X as `LOCKED` (or `MIGRATING`).
        *   Commit to Raft -> Epoch increments.
        *   **Effect:** Source Node sees the new Epoch/Status. It begins rejecting writes for Shard X with `423 Locked`.

    3.  **Phase 3: Catchup & Switch**
        *   Controller tells Target: `PullData` again (to get the diffs from Phase 1).
        *   Controller updates Topology: Set Shard X Primary = Target. Status = `ACTIVE`.
        *   Commit to Raft -> Epoch increments.

    4.  **Phase 4: Cleanup**
        *   Controller tells Source: `DeleteShard(ShardID)`.

-----

### **Substage 6.3: Client Handling of Migration**

The Client needs to handle the brief moment where the Shard is locked.

*   **Goal:** Update `pkg/client` Retry Logic.
*   **Logic:**
    *   Current Logic: `503` -> Sleep/Retry.
    *   **New Logic:** Handle `423 Locked` (or `ErrShardMigrating`).
    *   **Action:** If Client receives `423`, it means "You are talking to the right node, but it's currently handing off ownership."
    *   **Strategy:** Spin-wait. Sleep 100ms, then call `FetchTopology()` to see if the Primary has changed.

-----

### **Substage 6.4: Automated Rebalancing**

Finally, we make the system "Autonomic."

*   **Goal:** A background process in the Controller that balances load.
*   **Logic:**
    1.  Listen for `REGISTER_NODE` events.
    2.  Calculate `TargetShardsPerNode = TotalShards / TotalNodes`.
    3.  Identify "Rich" nodes (count > Target) and "Poor" nodes (count < Target).
    4.  Queue `MoveShard` jobs to move shards from Rich to Poor.

**Acceptance Criteria:**

*   [ ] **Setup:** Start 2 Data Nodes. Fill with 10,000 keys.
*   [ ] **Scale:** Start 3rd Data Node.
*   [ ] **Observe:**
    *   Controller logs "Triggering Rebalance for Shard X".
    *   3rd Node logs "Importing Shard X".
    *   Controller logs "Shard X moved to Node 3".
*   [ ] **Verify:**
    *   Stop the original 2 nodes.
    *   Client `GET` keys belonging to Shard X.
    *   **Result:** Success (served by Node 3).

---

### **Project Completion**

Congratulations! You have designed a distributed system that features:
1.  **Raft Consensus** for Metadata.
2.  **Lease-Based Fencing** for Strong Consistency.
3.  **Synchronous Replication** for Durability.
4.  **Smart Clients** for Routing.
5.  **Dynamic Sharding** for Scalability.
```