package metadata

import "encoding/json"

type CommandType string

const (
	CmdRegisterNode   CommandType = "REGISTER_NODE"
	CmdUpdateTopology CommandType = "UPDATE_TOPOLOGY"
)

type BaseCommand struct {
	Type    CommandType `json:"type"`
	Payload []byte      `json:"payload"`
}

type RegisterNodePayload struct {
	NodeID  string `json:"node_id"`
	Address string `json:"address"`
}

type UpdateTopologyPayload struct {
	NewShards   map[int]ShardMetadata `json:"new_shards"`
	NodeUpdates map[string]NodeStatus `json:"node_updates,omitempty"`
}

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
