Here is the detailed execution plan for **Stage 1.1**.

**Objective:** Define the "Source of Truth" for the entire cluster.
**Consultant's Note:** You are now defining the brain of the system. This data structure (`ClusterConfig`) will be replicated via Raft logs. If this structure is flawed (e.g., ambiguous shard ownership), the whole system will suffer from Split Brain later. We must be precise.

---

# Stage 1.1: The Metadata Domain Model

**Goal:** Implement the Go structs that represent the Cluster Topology (Nodes, Shards, and Epochs) and the Command payloads used to mutate them.

### Substage 1.1.1: Define the Cluster State

We need a package to hold these definitions. This package will be imported by the Controller (to write), the DataNode (to read/obey), and the Client (to route).

*   **Action:** Create `pkg/metadata/model.go`.

**Proposed Code (`pkg/metadata/model.go`):**

```go
package metadata

import (
	"time"
)

// NodeStatus represents the lifecycle state of a DataNode
type NodeStatus string

const (
	StatusActive  NodeStatus = "ACTIVE"  // Healthy and participating
	StatusDead    NodeStatus = "DEAD"    // Missed heartbeats (Reaper detected)
	StatusJoining NodeStatus = "JOINING" // Registered but not yet assigned shards
)

// ShardMetadata defines who owns a specific hash slot
type ShardMetadata struct {
	ID         int      `json:"id"`
	PrimaryID  string   `json:"primary_id"`  // NodeID (e.g., "node-1")
	ReplicaIDs []string `json:"replica_ids"` // List of Replica NodeIDs
}

// NodeMetadata holds connection info about a DataNode
type NodeMetadata struct {
	ID            string     `json:"id"`
	Address       string     `json:"address"` // HTTP API address (e.g. "10.0.1.5:8080")
	Status        NodeStatus `json:"status"`
	LastHeartbeat time.Time  `json:"last_heartbeat"`
}

// ClusterConfig is the State Machine state.
// This is what gets serialized to JSON and served via GET /topology
type ClusterConfig struct {
	// Epoch is the consistency guard.
	// It MUST increment on EVERY change to Shards or Nodes status.
	// DataNodes use this to fence off stale writes.
	Epoch uint64 `json:"epoch"`

	// TotalShards is fixed at startup (e.g., 1024)
	TotalShards int `json:"total_shards"`

	// Nodes: Map of NodeID -> Metadata
	Nodes map[string]NodeMetadata `json:"nodes"`

	// Shards: Map of ShardID -> Metadata
	Shards map[int]ShardMetadata `json:"shards"`
}

// NewClusterConfig creates an empty config with a defined shard count
func NewClusterConfig(numShards int) *ClusterConfig {
	config := &ClusterConfig{
		Epoch:       0,
		TotalShards: numShards,
		Nodes:       make(map[string]NodeMetadata),
		Shards:      make(map[int]ShardMetadata),
	}

	// Initialize empty shards (unassigned)
	for i := 0; i < numShards; i++ {
		config.Shards[i] = ShardMetadata{
			ID:         i,
			PrimaryID:  "",
			ReplicaIDs: []string{},
		}
	}
	return config
}
```

---

### Substage 1.1.2: Define Command Payloads

When the Controller wants to change the state (e.g., "Register Node A"), it must propose a command to Raft. Since we made the Raft payload generic (`bytes command_payload`) in Stage 1.0, we need specific structs to marshal into those bytes.

*   **Action:** Create `pkg/metadata/commands.go`.

**Proposed Code (`pkg/metadata/commands.go`):**

```go
package metadata

// CommandType helps the FSM know which struct to unmarshal
type CommandType string

const (
	CmdRegisterNode   CommandType = "REGISTER_NODE"
	CmdUpdateTopology CommandType = "UPDATE_TOPOLOGY"
)

// BaseCommand is a wrapper to identify the type of payload
type BaseCommand struct {
	Type    CommandType     `json:"type"`
	Payload []byte          `json:"payload"` // Nested JSON
}

// RegisterNodePayload is sent when a DataNode first joins
type RegisterNodePayload struct {
	NodeID  string `json:"node_id"`
	Address string `json:"address"`
}

// UpdateTopologyPayload is sent when the Controller moves shards or marks nodes dead
type UpdateTopologyPayload struct {
	// We send the FULL shard map or a diff. 
	// For simplicity in this project, sending the full map ensures safety.
	NewShards map[int]ShardMetadata `json:"new_shards"`
	
	// Optional: If we just want to update node status
	NodeUpdates map[string]NodeStatus `json:"node_updates,omitempty"`
}
```

---

### Substage 1.1.3: Serialization Helpers

We need helper functions to make it easy for the Controller (Stage 1.3) to work with these structs without cluttering the main logic with JSON code.

*   **Action:** Add helper methods to `pkg/metadata/commands.go`.

**Proposed Code (`pkg/metadata/commands.go` - append):**

```go
import "encoding/json"

// Helper to create a raft message payload
func NewRegisterNodeCommand(nodeID, address string) ([]byte, error) {
	payload := RegisterNodePayload{
		NodeID:  nodeID,
		Address: address,
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	
	cmd := BaseCommand{
		Type:    CmdRegisterNode,
		Payload: data,
	}
	return json.Marshal(cmd)
}

func NewUpdateTopologyCommand(shards map[int]ShardMetadata) ([]byte, error) {
	payload := UpdateTopologyPayload{
		NewShards: shards,
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}

	cmd := BaseCommand{
		Type:    CmdUpdateTopology,
		Payload: data,
	}
	return json.Marshal(cmd)
}
```

---

### Substage 1.1.4: Unit Testing the Model

We must verify that our serialization works perfectly. If JSON unmarshalling fails inside the Raft State Machine, the node will panic or diverge state, which is catastrophic.

*   **Action:** Create `pkg/metadata/model_test.go`.

**Proposed Code (`pkg/metadata/model_test.go`):**

```go
package metadata

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClusterConfig_Serialization(t *testing.T) {
	cfg := NewClusterConfig(10)
	cfg.Epoch = 5
	cfg.Nodes["node-1"] = NodeMetadata{ID: "node-1", Status: StatusActive}
	cfg.Shards[0] = ShardMetadata{ID: 0, PrimaryID: "node-1"}

	data, err := json.Marshal(cfg)
	require.NoError(t, err)

	var loaded ClusterConfig
	err = json.Unmarshal(data, &loaded)
	require.NoError(t, err)

	assert.Equal(t, uint64(5), loaded.Epoch)
	assert.Equal(t, 10, loaded.TotalShards)
	assert.Equal(t, "node-1", loaded.Shards[0].PrimaryID)
}

func TestCommand_Helpers(t *testing.T) {
	// 1. Create Command Bytes
	cmdBytes, err := NewRegisterNodeCommand("node-alpha", "10.0.0.1:9000")
	require.NoError(t, err)

	// 2. Unmarshal Base
	var base BaseCommand
	err = json.Unmarshal(cmdBytes, &base)
	require.NoError(t, err)
	assert.Equal(t, CmdRegisterNode, base.Type)

	// 3. Unmarshal Specific Payload
	var payload RegisterNodePayload
	err = json.Unmarshal(base.Payload, &payload)
	require.NoError(t, err)
	assert.Equal(t, "node-alpha", payload.NodeID)
}
```

---

### Deliverable Checklist for Stage 1.1

1.  [ ] `pkg/metadata` package created.
2.  [ ] `ClusterConfig` struct defined with **Epoch**.
3.  [ ] Command structs (`RegisterNodePayload`, etc.) defined.
4.  [ ] Serialization helpers implemented.
5.  [ ] Unit tests pass (`go test ./pkg/metadata/...`).

**Why this matters:**
You have now defined the language that the Controller, DataNodes, and Clients will speak. The **Epoch** field in `ClusterConfig` is the cornerstone of your "Strong Consistency" requirement. Without it, you cannot implement fencing in Stage 2.