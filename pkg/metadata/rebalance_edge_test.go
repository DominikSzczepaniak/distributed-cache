package metadata

import (
	"fmt"
	"math/rand"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func threeNodeConfig(shards int) *ClusterConfig {
	cfg := NewClusterConfig(shards)
	ids := []string{"n1", "n2", "n3"}
	for _, id := range ids {
		cfg.Nodes[id] = NodeMetadata{ID: id, Address: id + ":1", Status: StatusActive}
	}
	for i := 0; i < shards; i++ {
		cfg.Shards[i] = ShardMetadata{
			ID:         i,
			PrimaryID:  ids[i%3],
			ReplicaIDs: []string{ids[(i+1)%3], ids[(i+2)%3]},
			Status:     ShardStatusActive,
		}
	}
	return cfg
}

func TestRebalance_AllNodesDeadLeavesNoPrimaries(t *testing.T) {
	cfg := threeNodeConfig(6)
	startEpoch := cfg.Epoch

	for _, id := range []string{"n1", "n2", "n3"} {
		cfg = Rebalance(cfg, id)
	}

	assert.Equal(t, startEpoch+3, cfg.Epoch)
	for id, n := range cfg.Nodes {
		assert.Equal(t, StatusDead, n.Status, id)
	}
	for _, s := range cfg.Shards {
		assert.Empty(t, s.PrimaryID, "shard %d has no live holder, so no primary", s.ID)
		assert.Empty(t, s.ReplicaIDs, "shard %d", s.ID)
	}
	assert.Len(t, cfg.Shards, 6, "shards are never dropped")
}

func TestRebalance_DoesNotMutateInput(t *testing.T) {
	cfg := threeNodeConfig(4)
	snapshot := deepCopyConfig(cfg)

	out := Rebalance(cfg, "n1")

	assert.True(t, reflect.DeepEqual(snapshot, cfg), "input config must be untouched (copy-on-write)")
	assert.NotSame(t, cfg, out)
	assert.Equal(t, cfg.Epoch+1, out.Epoch)
}

func TestRebalance_EpochStrictlyMonotonic(t *testing.T) {
	cfg := threeNodeConfig(3)
	prev := cfg.Epoch
	// includes repeated death of the same node and a node that never existed
	for _, id := range []string{"n1", "n1", "ghost", "n2", "n3", "n3"} {
		cfg = Rebalance(cfg, id)
		assert.Equal(t, prev+1, cfg.Epoch, "epoch must advance by exactly one per call (death of %s)", id)
		prev = cfg.Epoch
	}
}

func TestRebalance_RepeatedDeathIsStateIdempotent(t *testing.T) {
	once := Rebalance(threeNodeConfig(4), "n1")
	twice := Rebalance(once, "n1")

	assert.Equal(t, once.Epoch+1, twice.Epoch)
	once2, twice2 := *once, *twice
	once2.Epoch, twice2.Epoch = 0, 0
	assert.True(t, reflect.DeepEqual(once2.Shards, twice2.Shards), "topology identical apart from epoch")
	assert.Equal(t, StatusDead, twice.Nodes["n1"].Status)
}

func TestRebalance_UnknownNodeDoesNotCreateIt(t *testing.T) {
	out := Rebalance(threeNodeConfig(2), "ghost")
	assert.NotContains(t, out.Nodes, "ghost")
	assert.Len(t, out.Nodes, 3)
}

// Shard status (LOCKED during a migration) must survive an unrelated node failure,
// otherwise a failure elsewhere silently unlocks a shard that is mid-migration.
func TestRebalance_PreservesStatusOfUnaffectedShards(t *testing.T) {
	cfg := threeNodeConfig(3)
	s0 := cfg.Shards[0] // primary n1
	s1 := cfg.Shards[1] // primary n2
	s1.Status = ShardStatusLocked
	cfg.Shards[1] = s1
	s2 := cfg.Shards[2] // primary n3
	s2.Status = ShardStatusMigrating
	cfg.Shards[2] = s2
	_ = s0

	out := Rebalance(cfg, "n1")

	assert.Equal(t, ShardStatusLocked, out.Shards[1].Status, "locked shard (primary n2) must stay locked")
	assert.Equal(t, ShardStatusMigrating, out.Shards[2].Status)
	assert.Equal(t, ShardStatusActive, out.Shards[0].Status, "promoted shard is serviceable")
}

func TestRebalance_PreservesActiveStatusOnReplicaLoss(t *testing.T) {
	out := Rebalance(threeNodeConfig(3), "n3")
	for _, s := range out.Shards {
		if s.PrimaryID != "" {
			assert.Equal(t, ShardStatusActive, s.Status, "shard %d status must not be blanked", s.ID)
		}
	}
}

// Property test over random failure sequences.
func TestRebalance_RandomFailureSequencesKeepInvariants(t *testing.T) {
	for seed := int64(0); seed < 50; seed++ {
		t.Run(fmt.Sprint("seed", seed), func(t *testing.T) {
			rng := rand.New(rand.NewSource(seed))
			cfg := threeNodeConfig(7)
			dead := map[string]bool{}
			for step := 0; step < 5; step++ {
				victim := []string{"n1", "n2", "n3"}[rng.Intn(3)]
				prev := cfg
				cfg = Rebalance(cfg, victim)
				dead[victim] = true

				require.Equal(t, prev.Epoch+1, cfg.Epoch)
				for _, s := range cfg.Shards {
					assert.False(t, dead[s.PrimaryID], "dead node %s is primary of shard %d", s.PrimaryID, s.ID)
					seen := map[string]bool{s.PrimaryID: s.PrimaryID != ""}
					for _, r := range s.ReplicaIDs {
						assert.False(t, dead[r], "dead replica %s in shard %d", r, s.ID)
						assert.False(t, seen[r], "duplicate holder %s in shard %d", r, s.ID)
						seen[r] = true
					}
					old := prev.Shards[s.ID]
					if old.PrimaryID != "" && !dead[old.PrimaryID] {
						assert.Equal(t, old.PrimaryID, s.PrimaryID, "live primary must not change")
					}
					if old.PrimaryID != "" && old.PrimaryID != s.PrimaryID && s.PrimaryID != "" {
						assert.Contains(t, old.ReplicaIDs, s.PrimaryID, "only a former replica may be promoted")
					}
				}
			}
		})
	}
}
