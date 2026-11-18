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

func topologyCommand(t *testing.T, epoch uint64, clientID, token string) raft.Message {
	t.Helper()
	cfg := metadata.NewClusterConfig(1)
	cfg.Epoch = epoch
	payload, err := json.Marshal(UpdateTopologyPayload{Config: *cfg})
	require.NoError(t, err)
	data, err := json.Marshal(Command{Type: CmdUpdateTopology, Payload: payload})
	require.NoError(t, err)
	return raft.Message{MsgType: raft.CommandMsg, Data: data, ClientID: clientID, IdempotencyToken: token}
}

// Review of split/03: IdempotencyToken is copied into every Raft log entry but
// no state machine reads it, so a command that the API layer re-proposes after
// a commit-ambiguous timeout is applied twice. Here the duplicate of an OLD
// topology update arrives after a newer one and rolls the topology back.
func TestKnownBug_ControllerAppAppliesEachClientTokenOnce(t *testing.T) {
	c := NewController(nil, time.Second)
	app := NewControllerApp()
	app.SetController(c)

	ok, _ := app.AppendMessage(topologyCommand(t, 5, "api-1", "tok-A"))
	require.True(t, ok)
	ok, _ = app.AppendMessage(topologyCommand(t, 6, "api-1", "tok-B"))
	require.True(t, ok)
	require.EqualValues(t, 6, c.GetConfig().Epoch)

	// duplicate proposal of the first command (same client, same token)
	app.AppendMessage(topologyCommand(t, 5, "api-1", "tok-A"))

	assert.EqualValues(t, 6, c.GetConfig().Epoch, "a duplicate (client, token) must be ignored by the state machine")
}

// Commands without a token (internal proposals) must keep working: they are not deduplicated.
func TestControllerAppAppliesCommandsWithoutTokenEachTime(t *testing.T) {
	c := NewController(nil, time.Second)
	app := NewControllerApp()
	app.SetController(c)
	app.AppendMessage(topologyCommand(t, 5, "", ""))
	app.AppendMessage(topologyCommand(t, 7, "", ""))
	assert.EqualValues(t, 7, c.GetConfig().Epoch)
}
