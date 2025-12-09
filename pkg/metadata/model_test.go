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
	cmdBytes, err := NewRegisterNodeCommand("node-alpha", "10.0.0.1:9000")
	require.NoError(t, err)

	var base BaseCommand
	err = json.Unmarshal(cmdBytes, &base)
	require.NoError(t, err)
	assert.Equal(t, CmdRegisterNode, base.Type)

	var payload RegisterNodePayload
	err = json.Unmarshal(base.Payload, &payload)
	require.NoError(t, err)
	assert.Equal(t, "node-alpha", payload.NodeID)
	assert.Equal(t, "10.0.0.1:9000", payload.Address)
}
