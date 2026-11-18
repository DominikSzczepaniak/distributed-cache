package metadata

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewRegisterNodeCommand_RoundTripAndDeterministic(t *testing.T) {
	a, err := NewRegisterNodeCommand("n1", "h:1")
	require.NoError(t, err)
	b, err := NewRegisterNodeCommand("n1", "h:1")
	require.NoError(t, err)
	assert.Equal(t, a, b, "same command encodes to identical bytes (replicas can compare/dedupe)")

	var base BaseCommand
	require.NoError(t, json.Unmarshal(a, &base))
	assert.Equal(t, CmdRegisterNode, base.Type)
	var p RegisterNodePayload
	require.NoError(t, json.Unmarshal(base.Payload, &p))
	assert.Equal(t, RegisterNodePayload{NodeID: "n1", Address: "h:1"}, p)

	c, _ := NewRegisterNodeCommand("n2", "h:1")
	assert.NotEqual(t, a, c)
}

func TestClusterConfig_NewHasAllShardsUnassigned(t *testing.T) {
	cfg := NewClusterConfig(5)
	assert.Len(t, cfg.Shards, 5)
	assert.Equal(t, 5, cfg.TotalShards)
	assert.Equal(t, uint64(0), cfg.Epoch)
	for i, s := range cfg.Shards {
		assert.Equal(t, i, s.ID)
		assert.Empty(t, s.PrimaryID)
		assert.NotNil(t, s.ReplicaIDs)
		assert.Equal(t, ShardStatusActive, s.Status)
	}
	assert.Empty(t, NewClusterConfig(0).Shards)
}
