package controller

import (
	"encoding/json"
	"log/slog"

	"github.com/dominikszczepaniak/distributed-cache/pkg/metadata"
	"github.com/dominikszczepaniak/distributed-cache/pkg/raft"
)

// CommandType represents the type of operation to be applied to the Controller's state.
type CommandType string

const (
	CmdRegisterNode   CommandType = "RegisterNode"
	CmdUpdateTopology CommandType = "UpdateTopology"
	CmdNoOp           CommandType = "NoOp"
)

// Command encodes a metadata change for Raft replication.
type Command struct {
	Type    CommandType     `json:"type"`
	Payload json.RawMessage `json:"payload"`
}

// RegisterNodePayload carries node details for the registration command.
type RegisterNodePayload struct {
	NodeID  string `json:"node_id"`
	Address string `json:"address"`
}

// UpdateTopologyPayload carries the new configuration for the topology change command.
type UpdateTopologyPayload struct {
	Config metadata.ClusterConfig `json:"config"`
}

// ControllerApp implements the raft.Application interface for the Control Plane.
type ControllerApp struct {
	controller *Controller
}

// NewControllerApp initializes the state machine for the cluster controller.
func NewControllerApp() *ControllerApp {
	return &ControllerApp{}
}

// SetController injects the controller reference into the application (circular dependency).
func (a *ControllerApp) SetController(c *Controller) {
	a.controller = c
}

// AppendMessage is the core application logic for the Raft state machine.
// It receives messages committed by Raft and applies them to the Controller's local state.
// Supported command types:
// - CmdRegisterNode: Adds a node to the cluster.
// - CmdUpdateTopology: Forcefully updates the cluster configuration (e.g., after rebalance).
// - CmdNoOp: A heartbeat-like command to ensure consensus.
// AppendMessage applies a serialized command to the cluster configuration.
func (a *ControllerApp) AppendMessage(msg raft.Message) (bool, int) {
	if msg.MsgType != raft.CommandMsg {
		slog.Warn("ControllerApp received non-command message", "type", msg.MsgType)
		return false, 0
	}

	var cmd Command
	if err := json.Unmarshal(msg.Data, &cmd); err != nil {
		slog.Error("Failed to unmarshal command", "error", err)
		return false, 0
	}

	slog.Info("Applying Raft command", "type", cmd.Type)

	switch cmd.Type {
	case CmdRegisterNode:
		var payload RegisterNodePayload
		if err := json.Unmarshal(cmd.Payload, &payload); err != nil {
			slog.Error("Failed to unmarshal RegisterNode payload", "error", err)
			return false, 0
		}
		a.controller.applyRegisterNode(payload.NodeID, payload.Address)
		return true, 0

	case CmdUpdateTopology:
		var payload UpdateTopologyPayload
		if err := json.Unmarshal(cmd.Payload, &payload); err != nil {
			slog.Error("Failed to unmarshal UpdateTopology payload", "error", err)
			return false, 0
		}
		a.controller.applyUpdateTopology(&payload.Config)
		return true, 0

	case CmdNoOp:
		return true, 0

	default:
		slog.Warn("Unknown command type", "type", cmd.Type)
		return false, 0
	}
}

// GetSnapshot serializes the current cluster configuration into a byte array.
// This is used by Raft to create snapshots for log compaction.
// GetSnapshot serializes the current cluster configuration for log compaction.
func (a *ControllerApp) GetSnapshot() ([]byte, error) {
	if a.controller == nil {
		return []byte("{}"), nil
	}
	config := a.controller.GetConfig()
	return json.Marshal(config)
}

// RestoreFromSnapshot deserializes a cluster configuration and updates the Controller's state.
// This is used by Raft when a node needs to catch up via a snapshot rather than individual log entries.
// RestoreFromSnapshot deserializes and applies a previously saved cluster configuration.
func (a *ControllerApp) RestoreFromSnapshot(data []byte) (int, error) {
	if a.controller == nil {
		return 0, nil
	}
	var config metadata.ClusterConfig
	if err := json.Unmarshal(data, &config); err != nil {
		return 0, err
	}
	a.controller.applyUpdateTopology(&config)
	return 0, nil
}

// GetValue is part of the raft.Application interface but is not used by the Controller.
func (a *ControllerApp) GetValue(key int) int {
	return 0
}
