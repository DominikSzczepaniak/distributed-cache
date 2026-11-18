package controller

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/dominikszczepaniak/distributed-cache/pkg/metadata"
	"github.com/dominikszczepaniak/distributed-cache/pkg/raft"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func cmdMsg(t *testing.T, typ CommandType, payload any) raft.Message {
	t.Helper()
	var raw json.RawMessage
	if payload != nil {
		b, err := json.Marshal(payload)
		require.NoError(t, err)
		raw = b
	}
	data, err := json.Marshal(Command{Type: typ, Payload: raw})
	require.NoError(t, err)
	return raft.Message{MsgType: raft.CommandMsg, Data: data}
}

func newApp() (*ControllerApp, *Controller) {
	c := NewController(&raft.Raft{}, time.Hour)
	app := NewControllerApp()
	app.SetController(c)
	return app, c
}

func TestControllerApp_RegisterNode(t *testing.T) {
	app, c := newApp()

	ok, _ := app.AppendMessage(cmdMsg(t, CmdRegisterNode, RegisterNodePayload{NodeID: "n1", Address: "h:1"}))
	require.True(t, ok)

	cfg := c.GetConfig()
	require.Contains(t, cfg.Nodes, "n1")
	assert.Equal(t, "h:1", cfg.Nodes["n1"].Address)
	assert.Equal(t, metadata.StatusActive, cfg.Nodes["n1"].Status)
	assert.Equal(t, uint64(1), cfg.Epoch)
}

func TestControllerApp_RegisterNode_DuplicateIsIdempotent(t *testing.T) {
	app, c := newApp()
	msg := cmdMsg(t, CmdRegisterNode, RegisterNodePayload{NodeID: "n1", Address: "h:1"})

	app.AppendMessage(msg)
	epoch := c.GetConfig().Epoch
	ok, _ := app.AppendMessage(msg) // log replay / duplicate commit

	assert.True(t, ok)
	assert.Equal(t, epoch, c.GetConfig().Epoch, "re-applying a registration must not bump the epoch")
	assert.Len(t, c.GetConfig().Nodes, 1)
}

func TestControllerApp_RegisterNode_DoesNotMutatePublishedConfig(t *testing.T) {
	app, c := newApp()
	before := c.GetConfig()
	app.AppendMessage(cmdMsg(t, CmdRegisterNode, RegisterNodePayload{NodeID: "n1", Address: "h:1"}))
	assert.Empty(t, before.Nodes, "copy-on-write: earlier snapshot unchanged")
	assert.Equal(t, uint64(0), before.Epoch)
}

func TestControllerApp_UpdateTopologyReplacesConfig(t *testing.T) {
	app, c := newApp()
	newCfg := metadata.NewClusterConfig(2)
	newCfg.Epoch = 42
	newCfg.Nodes["x"] = metadata.NodeMetadata{ID: "x", Address: "x:1", Status: metadata.StatusActive}

	ok, _ := app.AppendMessage(cmdMsg(t, CmdUpdateTopology, UpdateTopologyPayload{Config: *newCfg}))

	require.True(t, ok)
	got := c.GetConfig()
	assert.Equal(t, uint64(42), got.Epoch)
	assert.Equal(t, 2, got.TotalShards)
	assert.Contains(t, got.Nodes, "x")
}

func TestControllerApp_NoOpAndUnknownAndMalformed(t *testing.T) {
	app, c := newApp()
	before := c.GetConfig()

	ok, _ := app.AppendMessage(cmdMsg(t, CmdNoOp, nil))
	assert.True(t, ok)

	ok, _ = app.AppendMessage(cmdMsg(t, CommandType("Bogus"), nil))
	assert.False(t, ok, "unknown command rejected")

	ok, _ = app.AppendMessage(raft.Message{MsgType: raft.CommandMsg, Data: []byte("not json")})
	assert.False(t, ok, "malformed envelope rejected")

	ok, _ = app.AppendMessage(raft.Message{MsgType: raft.CommandMsg, Data: []byte(`{"type":"RegisterNode","payload":"oops"}`)})
	assert.False(t, ok, "malformed RegisterNode payload rejected")

	ok, _ = app.AppendMessage(raft.Message{MsgType: raft.CommandMsg, Data: []byte(`{"type":"UpdateTopology","payload":[1]}`)})
	assert.False(t, ok, "malformed UpdateTopology payload rejected")

	assert.Same(t, before, c.GetConfig(), "rejected commands never touch state")
}

func TestControllerApp_SnapshotRoundTrip(t *testing.T) {
	app, c := newApp()
	app.AppendMessage(cmdMsg(t, CmdRegisterNode, RegisterNodePayload{NodeID: "n1", Address: "h:1"}))
	app.AppendMessage(cmdMsg(t, CmdRegisterNode, RegisterNodePayload{NodeID: "n2", Address: "h:2"}))
	snap, err := app.GetSnapshot()
	require.NoError(t, err)

	app2, c2 := newApp()
	err, _ = app2.RestoreFromSnapshot(snap)
	require.NoError(t, err)

	assert.Equal(t, c.GetConfig().Epoch, c2.GetConfig().Epoch)
	assert.Equal(t, len(c.GetConfig().Nodes), len(c2.GetConfig().Nodes))
	assert.Equal(t, c.GetConfig().TotalShards, c2.GetConfig().TotalShards)

	err, _ = app2.RestoreFromSnapshot([]byte("garbage"))
	assert.Error(t, err)
	assert.Equal(t, uint64(2), c2.GetConfig().Epoch, "failed restore leaves state intact")
}

func TestControllerApp_NoControllerAttached(t *testing.T) {
	app := NewControllerApp()
	snap, err := app.GetSnapshot()
	require.NoError(t, err)
	assert.JSONEq(t, "{}", string(snap))
	err, _ = app.RestoreFromSnapshot([]byte("{}"))
	assert.NoError(t, err)
	assert.Equal(t, 0, app.GetValue(1))
}

func TestControllerApp_NonCommandMessageRejected(t *testing.T) {
	app, c := newApp()
	before := c.GetConfig()
	ok, _ := app.AppendMessage(raft.Message{MsgType: raft.MessageType("PUT"), Data: []byte("{}")})
	assert.False(t, ok)
	assert.Same(t, before, c.GetConfig())
}

// Two replicas applying the same log must end with the same topology.
func TestControllerApp_DeterministicAcrossReplicas(t *testing.T) {
	msgs := []raft.Message{
		cmdMsg(t, CmdRegisterNode, RegisterNodePayload{NodeID: "a", Address: "a:1"}),
		cmdMsg(t, CmdRegisterNode, RegisterNodePayload{NodeID: "b", Address: "b:1"}),
		cmdMsg(t, CmdRegisterNode, RegisterNodePayload{NodeID: "a", Address: "a:1"}),
		cmdMsg(t, CmdNoOp, nil),
	}
	app1, c1 := newApp()
	app2, c2 := newApp()
	for _, m := range msgs {
		app1.AppendMessage(m)
		app2.AppendMessage(m)
	}
	g1, g2 := c1.GetConfig(), c2.GetConfig()
	assert.Equal(t, g1.Epoch, g2.Epoch)
	assert.Equal(t, len(g1.Nodes), len(g2.Nodes))
	for id, n1 := range g1.Nodes {
		assert.Equal(t, n1.Address, g2.Nodes[id].Address)
		assert.Equal(t, n1.Status, g2.Nodes[id].Status)
	}
}
