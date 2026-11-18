package controller

import (
	"net/http"
	"testing"
	"time"

	"github.com/dominikszczepaniak/distributed-cache/pkg/metadata"
	"github.com/dominikszczepaniak/distributed-cache/pkg/raft"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type moveFixture struct {
	c        *Controller
	src, dst *mockDatanode
}

func newMoveFixture(t *testing.T) *moveFixture {
	t.Helper()
	f := &moveFixture{c: NewController(&raft.Raft{}, time.Hour), src: newMockDatanode(t), dst: newMockDatanode(t)}
	addNode(f.c, "A", f.src, metadata.StatusActive)
	addNode(f.c, "B", f.dst, metadata.StatusActive)
	f.c.config.Shards[0] = metadata.ShardMetadata{ID: 0, PrimaryID: "A", Status: metadata.ShardStatusActive}
	f.c.config.Epoch = 10
	return f
}

// assertUntouched checks the shard is still served by the original primary.
func (f *moveFixture) assertUntouched(t *testing.T) {
	t.Helper()
	cfg := f.c.GetConfig()
	assert.Equal(t, "A", cfg.Shards[0].PrimaryID, "primary must be unchanged after failed move")
	assert.Equal(t, metadata.ShardStatusActive, cfg.Shards[0].Status, "shard must not be left locked/migrating")
}

func TestMoveShard_Phase1PullFails_StateUntouched(t *testing.T) {
	f := newMoveFixture(t)
	f.dst.status.Store(http.StatusBadGateway) // real HandlePull answers 502 when export fails

	err := f.c.MoveShard(0, "B")

	require.Error(t, err)
	assert.Contains(t, err.Error(), "phase 1")
	f.assertUntouched(t)
	assert.Equal(t, uint64(10), f.c.GetConfig().Epoch, "no epoch bump before the lock phase")
	assert.Len(t, f.dst.pullCalls(), 1, "no catch-up attempted after phase 1 failure")
}

func TestMoveShard_TargetUnreachable_StateUntouched(t *testing.T) {
	f := newMoveFixture(t)
	f.dst.srv.Close()

	err := f.c.MoveShard(0, "B")

	require.Error(t, err)
	assert.Contains(t, err.Error(), "phase 1")
	f.assertUntouched(t)
	assert.Equal(t, uint64(10), f.c.GetConfig().Epoch)
}

// Phase 3 (catch-up under lock) fails: the source is still the only primary and
// holds all data, so the shard must be unlocked again instead of staying
// unavailable forever.
func TestMoveShard_Phase3CatchupFails_ShardNotLeftLocked(t *testing.T) {
	f := newMoveFixture(t)
	// First pull OK, second (catch-up) fails.
	f.dst.statusFn = func(n int) int {
		if n == 1 {
			return http.StatusOK
		}
		return http.StatusInternalServerError
	}

	err := f.c.MoveShard(0, "B")

	require.Error(t, err)
	assert.Contains(t, err.Error(), "phase 3")
	assert.Len(t, f.dst.pullCalls(), 2)
	f.assertUntouched(t)
	assert.Greater(t, f.c.GetConfig().Epoch, uint64(11), "rollback must publish a new epoch so datanodes see the unlock")
}

func TestMoveShard_UnknownShardOrNode(t *testing.T) {
	f := newMoveFixture(t)

	assert.Error(t, f.c.MoveShard(99, "B"), "unknown shard")
	assert.Error(t, f.c.MoveShard(0, "ghost"), "unknown target")

	f.c.config.Shards[1] = metadata.ShardMetadata{ID: 1, PrimaryID: "", Status: metadata.ShardStatusActive}
	assert.Error(t, f.c.MoveShard(1, "B"), "shard without primary has no source to copy from")

	assert.Equal(t, uint64(10), f.c.GetConfig().Epoch)
	assert.Empty(t, f.dst.pullCalls(), "no data movement on invalid requests")
}

func TestMoveShard_ToDeadNodeRejected(t *testing.T) {
	f := newMoveFixture(t)
	f.c.config.Nodes["B"] = metadata.NodeMetadata{ID: "B", Address: f.dst.addr(), Status: metadata.StatusDead}

	err := f.c.MoveShard(0, "B")

	require.Error(t, err, "a dead node must not become primary")
	f.assertUntouched(t)
	assert.Empty(t, f.dst.pullCalls())
}

func TestMoveShard_SuccessProtocol(t *testing.T) {
	f := newMoveFixture(t)

	require.NoError(t, f.c.MoveShard(0, "B"))

	calls := f.dst.pullCalls()
	require.Len(t, calls, 2, "bulk copy + catch-up under lock")
	for _, p := range calls {
		assert.Equal(t, pullCall{shard: 0, source: f.src.url()}, p, "target pulls shard 0 from the old primary")
	}
	cfg := f.c.GetConfig()
	assert.Equal(t, "B", cfg.Shards[0].PrimaryID)
	assert.Equal(t, metadata.ShardStatusActive, cfg.Shards[0].Status)
	assert.Equal(t, uint64(12), cfg.Epoch, "lock + switch = two epoch bumps")
	assert.Empty(t, f.src.pullCalls())
}

// After the move the new primary must not list itself as its own replica
// (it would replicate every write to itself), and the old primary should keep
// the data as a replica so redundancy is not silently lost.
func TestMoveShard_ReplicaListConsistentAfterMove(t *testing.T) {
	f := newMoveFixture(t)
	third := newMockDatanode(t)
	addNode(f.c, "C", third, metadata.StatusActive)
	f.c.config.Shards[0] = metadata.ShardMetadata{ID: 0, PrimaryID: "A", ReplicaIDs: []string{"B", "C"}, Status: metadata.ShardStatusActive}

	require.NoError(t, f.c.MoveShard(0, "B"))

	shard := f.c.GetConfig().Shards[0]
	assert.Equal(t, "B", shard.PrimaryID)
	assert.NotContains(t, shard.ReplicaIDs, "B", "primary must not be its own replica")
	assert.Contains(t, shard.ReplicaIDs, "C")
	assert.Contains(t, shard.ReplicaIDs, "A", "old primary keeps its copy as a replica")
}

func TestMoveShard_DoesNotMutatePreviouslyPublishedConfig(t *testing.T) {
	f := newMoveFixture(t)
	before := f.c.GetConfig() // readers (HTTP handlers) hold this pointer without a lock

	require.NoError(t, f.c.MoveShard(0, "B"))

	assert.Equal(t, "A", before.Shards[0].PrimaryID, "published snapshots are immutable (copy-on-write)")
	assert.Equal(t, uint64(10), before.Epoch)
}
