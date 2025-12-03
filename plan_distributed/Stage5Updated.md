Here is the detailed execution plan for **Stage 5**.

In this stage, we build the interface that connects the application to our distributed system. The Client is "Smart" because it doesn't just blindly send requests; it understands the Cluster Topology, calculates sharding locally, and navigates the consistency errors (Epoch mismatches, Fencing) that we implemented in previous stages.

---

## File: Stage5.md

```markdown
# Stage 5: The Smart Client (Routing & Recovery)

**Objective:** Build a Client SDK (and update the CLI) that abstracts the complexity of the distributed system. The client handles **Topology Discovery**, **Local Hashing**, and **Automatic Retries** when the cluster reconfigures itself.

**Consistency Rule:** To maintain Strong Consistency, the Client must **only** Read/Write to the **Primary** node. Reading from Replicas is forbidden in this version to prevents stale reads.

---

### **Substage 5.1: Client Core & Topology Caching**

The client cannot be stateless. It must hold the current "World View" to know where keys belong.

*   **Goal:** Create `pkg/client/smart_client.go`.
*   **State:**
    *   `ClusterConfig`: The cached map of Shards -> Nodes.
    *   `ControllerURLs`: Seed list to find the topology.
*   **Logic:**
    *   On `NewSmartClient()`, call `FetchTopology()` from the Controller.
    *   Store the result (specifically the `Epoch` and `Shards`).

**Proposed Code (`pkg/client/smart_client.go`):**

```go
type SmartClient struct {
    mu          sync.RWMutex
    config      *metadata.ClusterConfig
    controllers []string
    httpClient  *http.Client
}

func (c *SmartClient) FetchTopology() error {
    // Round-robin through controllers until one responds with 200 OK
    // Update c.config
    // Log: "Client updated to Epoch X"
}
```

-----

### **Substage 5.2: Client-Side Routing**

The core logic: `Key -> Hash -> Shard -> Node URL`.

*   **Goal:** Implement `Route(key)`.
*   **Logic:**
    1.  `ShardID = hash(key) % TotalShards` (Must match DataNode logic exactly).
    2.  Lookup `ShardID` in `config.Shards`.
    3.  Extract `PrimaryID`.
    4.  Resolve `PrimaryID` to `Address` (from `config.Nodes`).
    5.  Return `TargetURL` and `CurrentEpoch`.

-----

### **Substage 5.3: The Retry Loop (Handling Consistency Errors)**

This is where the "Strong Consistency" guarantee is operationalized on the user side. The client must interpret specific HTTP error codes as signals to refresh its state.

*   **Goal:** Implement `Put(key, val)` and `Get(key)`.
*   **The Retry State Machine:**
    1.  **Attempt 1:** Route to cached Primary. Send Request (with Epoch).
    2.  **Error Handling:**
        *   **200 OK:** Success. Return.
        *   **412 Precondition Failed (Stale Epoch):**
            *   *Meaning:* "I am talking to the right node, but my config is old (failover happened)."
            *   *Action:* Call `FetchTopology()`. Retry immediately.
        *   **503 Service Unavailable (Lease Expired):**
            *   *Meaning:* "The Primary is isolated/fenced. It cannot accept writes."
            *   *Action:* Sleep (Backoff). Retry. (Eventually, the Controller will elect a new Primary, and we will get a 412 or success on the new node).
        *   **Connection Refused / Timeout:**
            *   *Meaning:* Node is crashed.
            *   *Action:* Call `FetchTopology()`. Retry.

**Proposed Code (`pkg/client/request.go`):**

```go
func (c *SmartClient) Put(key string, value string) error {
    for attempt := 0; attempt < 5; attempt++ {
        targetURL, epoch := c.Route(key)
        
        resp, err := c.sendPost(targetURL, key, value, epoch)
        
        if err != nil {
            // Network error (Node down)
            c.FetchTopology()
            continue
        }
        
        switch resp.StatusCode {
        case http.StatusOK:
            return nil
            
        case http.StatusPreconditionFailed: // 412
            // We have old map
            c.FetchTopology()
            continue
            
        case http.StatusServiceUnavailable: // 503
            // Node is fenced. Wait for failover.
            time.Sleep(200 * time.Millisecond)
            continue
            
        default:
            return fmt.Errorf("unexpected error: %d", resp.StatusCode)
        }
    }
    return fmt.Errorf("max retries exceeded")
}
```

-----

### **Substage 5.4: The CLI Tool**

Refactor the existing `raftcli` to use this new library.

*   **Goal:** `cmd/raftcli/main.go`.
*   **Updates:**
    *   Remove direct HTTP calls.
    *   Initialize `client.NewSmartClient(controllerAddr)`.
    *   Commands `put`, `get` delegates to the library.

-----

### **Substage 5.5: End-to-End Consistency Verification**

We validate the entire system stack.

*   **Scenario:** Write while killing nodes.
*   **Steps:**
    1.  Start Cluster (Controller, Node A, Node B).
    2.  Loop: `raftcli put key-$i val-$i` (infinite loop).
    3.  **Disruption:** Kill Node A (Primary).
    4.  **Observation:**
        *   Client logs: "503 Fenced" or "Connection Refused".
        *   Client logs: "Fetching Topology... Updated to Epoch N+1".
        *   Client logs: "Retrying on Node B... Success".
    5.  **Validation:** Stop loop. Run `raftcli get key-$i` for the keys written during the crash. They **must** exist on Node B.

**Acceptance Criteria:**
*   [ ] The client automatically recovers from a Primary failure without user intervention.
*   [ ] No data is lost (acknowledged writes are persistent).
*   [ ] No "Split Brain" writes were accepted (verified by logs).

---

### **Deliverable Checklist for Stage 5**

At the end of this stage, you have a **Complete System**:
1.  [ ] **Control Plane:** Manages truth.
2.  [ ] **Data Plane:** Enforces safety (Leases).
3.  [ ] **Replication:** Ensures durability.
4.  [ ] **Client:** Handles discovery and failover transparently.
```