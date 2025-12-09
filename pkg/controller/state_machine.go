package controller

import (
	"encoding/json"
	"log/slog"

	"github.com/dominikszczepaniak/distributed-cache/pkg/metadata"
	"github.com/dominikszczepaniak/distributed-cache/pkg/raft"
)

type CommandType string

const (
	CmdRegisterNode   CommandType = "RegisterNode"
	CmdUpdateTopology CommandType = "UpdateTopology"
)

type Command struct {
	Type    CommandType     `json:"type"`
	Payload json.RawMessage `json:"payload"`
}

type RegisterNodePayload struct {
	NodeID  string `json:"node_id"`
	Address string `json:"address"`
}

type UpdateTopologyPayload struct {
	Config metadata.ClusterConfig `json:"config"`
}

type ControllerApp struct {
	controller *Controller
}

func NewControllerApp() *ControllerApp {
	return &ControllerApp{}
}

func (a *ControllerApp) SetController(c *Controller) {
	a.controller = c
}

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

	default:
		slog.Warn("Unknown command type", "type", cmd.Type)
		return false, 0
	}
}

func (a *ControllerApp) GetSnapshot() ([]byte, error) {
	if a.controller == nil {
		return []byte("{}"), nil
	}
	config := a.controller.GetConfig()
	return json.Marshal(config)
}

func (a *ControllerApp) RestoreFromSnapshot(data []byte) (error, int) {
	if a.controller == nil {
		return nil, 0
	}
	var config metadata.ClusterConfig
	if err := json.Unmarshal(data, &config); err != nil {
		return err, 0
	}
	a.controller.applyUpdateTopology(&config)
	return nil, 0
}

func (a *ControllerApp) GetValue(key int) int {
	return 0
}
