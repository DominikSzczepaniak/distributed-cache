package metadata

import "encoding/json"

// CommandType represents the classification of a metadata change command.
type CommandType string

const (
	// CmdRegisterNode is used to announce a new DataNode to the cluster.
	CmdRegisterNode CommandType = "REGISTER_NODE"
	// CmdUpdateTopology is used to broadcast a new cluster configuration.
	CmdUpdateTopology CommandType = "UPDATE_TOPOLOGY"
)

// BaseCommand is the shell for all Raft commands affecting cluster state.
type BaseCommand struct {
	Type    CommandType `json:"type"`
	Payload []byte      `json:"payload"`
}

// RegisterNodePayload contains the details for a node registration command.
type RegisterNodePayload struct {
	NodeID  string `json:"node_id"`
	Address string `json:"address"`
}

// UpdateTopologyPayload contains the new cluster configuration to be applied.
type UpdateTopologyPayload struct {
	NewShards   map[int]ShardMetadata `json:"new_shards"`
	NodeUpdates map[string]NodeStatus `json:"node_updates,omitempty"`
}

// NewRegisterNodeCommand creates a serialized BaseCommand for node registration.
func NewRegisterNodeCommand(nodeID, address string) ([]byte, error) {
	payload := RegisterNodePayload{
		NodeID:  nodeID,
		Address: address,
	}
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}

	base := BaseCommand{
		Type:    CmdRegisterNode,
		Payload: payloadBytes,
	}
	return json.Marshal(base)
}
