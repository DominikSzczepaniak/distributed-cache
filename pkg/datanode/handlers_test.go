package datanode

import (
	"net/http"
	"testing"
	"time"

	"github.com/dominikszczepaniak/distributed-cache/pkg/metadata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------- GET ----------

func TestHandleGet(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		f := newFixture(t, singleShardCfg(1, metadata.ShardStatusActive))
		f.cache.Put("k", "v")
		w := postJSON(t, f.srv.HandleGet, "GET", "/data?key=k", nil)
		assert.Equal(t, http.StatusOK, w.Code)
		assert.Equal(t, "v", w.Body.String())
	})
	t.Run("not found", func(t *testing.T) {
		f := newFixture(t, singleShardCfg(1, metadata.ShardStatusActive))
		w := postJSON(t, f.srv.HandleGet, "GET", "/data?key=missing", nil)
		assert.Equal(t, http.StatusNotFound, w.Code)
	})
	t.Run("missing key parameter", func(t *testing.T) {
		f := newFixture(t, singleShardCfg(1, metadata.ShardStatusActive))
		w := postJSON(t, f.srv.HandleGet, "GET", "/data", nil)
		assert.Equal(t, http.StatusBadRequest, w.Code)
	})
	t.Run("fenced", func(t *testing.T) {
		f := newFixture(t, singleShardCfg(1, metadata.ShardStatusActive))
		f.cache.Put("k", "v")
		f.clock.Advance(testLease + time.Second)
		w := postJSON(t, f.srv.HandleGet, "GET", "/data?key=k", nil)
		assert.Equal(t, http.StatusServiceUnavailable, w.Code)
		assert.NotContains(t, w.Body.String(), "v\n", "fenced node must not leak data")
	})
	// Reads during migration are served by the (still authoritative) source primary.
	for _, st := range []metadata.ShardStatus{metadata.ShardStatusLocked, metadata.ShardStatusMigrating} {
		t.Run("shard "+string(st)+" still readable", func(t *testing.T) {
			f := newFixture(t, singleShardCfg(1, st))
			f.cache.Put("k", "v")
			w := postJSON(t, f.srv.HandleGet, "GET", "/data?key=k", nil)
			assert.Equal(t, http.StatusOK, w.Code)
			assert.Equal(t, "v", w.Body.String())
		})
	}
}

// ---------- DELETE ----------

func TestHandleDelete_SuccessReplicatesToAllReplicas(t *testing.T) {
	r1, r2 := newRecorder(t), newRecorder(t)
	f := newFixture(t, singleShardCfg(5, metadata.ShardStatusActive, r1, r2))
	f.cache.Put("k", "v")

	w := postJSON(t, f.srv.HandleDelete, "DELETE", "/data", DeleteRequest{Key: "k", Epoch: 5})

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Empty(t, f.cache.Get("k"))
	for i, r := range []*recorder{r1, r2} {
		require.Equal(t, 1, r.count(), "replica %d", i)
		assert.Equal(t, "/internal/replicate-delete", r.path(0))
		assert.Equal(t, "k", r.writeReq(0).Key)
		assert.Equal(t, uint64(5), r.writeReq(0).Epoch)
	}
}

func TestHandleDelete_RejectionsLeaveDataAndReplicasUntouched(t *testing.T) {
	cases := []struct {
		name   string
		setup  func(f *fixture)
		cfg    func(r *recorder) *metadata.ClusterConfig
		body   any
		status int
	}{
		{"fenced", func(f *fixture) { f.clock.Advance(testLease + time.Second) },
			func(r *recorder) *metadata.ClusterConfig { return singleShardCfg(5, metadata.ShardStatusActive, r) },
			DeleteRequest{Key: "k", Epoch: 5}, http.StatusServiceUnavailable},
		{"stale epoch", nil,
			func(r *recorder) *metadata.ClusterConfig { return singleShardCfg(5, metadata.ShardStatusActive, r) },
			DeleteRequest{Key: "k", Epoch: 4}, http.StatusPreconditionFailed},
		{"not primary", nil,
			func(r *recorder) *metadata.ClusterConfig {
				c := singleShardCfg(5, metadata.ShardStatusActive, r)
				s := c.Shards[0]
				s.PrimaryID = "node-2"
				c.Shards[0] = s
				return c
			},
			DeleteRequest{Key: "k", Epoch: 5}, http.StatusBadRequest},
		{"locked", nil,
			func(r *recorder) *metadata.ClusterConfig { return singleShardCfg(5, metadata.ShardStatusLocked, r) },
			DeleteRequest{Key: "k", Epoch: 5}, http.StatusLocked},
		{"migrating", nil,
			func(r *recorder) *metadata.ClusterConfig { return singleShardCfg(5, metadata.ShardStatusMigrating, r) },
			DeleteRequest{Key: "k", Epoch: 5}, http.StatusLocked},
		{"bad body", nil,
			func(r *recorder) *metadata.ClusterConfig { return singleShardCfg(5, metadata.ShardStatusActive, r) },
			"{nope", http.StatusBadRequest},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := newRecorder(t)
			f := newFixture(t, tc.cfg(r))
			f.cache.Put("k", "v")
			if tc.setup != nil {
				tc.setup(f)
			}
			w := postJSON(t, f.srv.HandleDelete, "DELETE", "/data", tc.body)
			assert.Equal(t, tc.status, w.Code)
			assert.Equal(t, "v", f.cache.Get("k"), "rejected delete must not remove data")
			assert.Zero(t, r.count(), "rejected delete must not be replicated")
		})
	}
}

func TestHandleDelete_ReplicationFailureKeepsLocalValue(t *testing.T) {
	good, bad := newRecorder(t), newRecorder(t)
	bad.status.Store(http.StatusInternalServerError)
	f := newFixture(t, singleShardCfg(5, metadata.ShardStatusActive, good, bad))
	f.cache.Put("k", "v")

	w := postJSON(t, f.srv.HandleDelete, "DELETE", "/data", DeleteRequest{Key: "k", Epoch: 5})

	assert.Equal(t, http.StatusInternalServerError, w.Code)
	assert.Equal(t, "v", f.cache.Get("k"), "client was told it failed, so the primary must keep the value")
}

func TestHandleDelete_ReplicaUnreachable(t *testing.T) {
	r := newRecorder(t)
	cfg := singleShardCfg(5, metadata.ShardStatusActive, r)
	r.srv.Close()
	f := newFixture(t, cfg)
	f.cache.Put("k", "v")

	w := postJSON(t, f.srv.HandleDelete, "DELETE", "/data", DeleteRequest{Key: "k", Epoch: 5})
	assert.Equal(t, http.StatusInternalServerError, w.Code)
	assert.Equal(t, "v", f.cache.Get("k"))
}

func TestHandleDelete_NonexistentKeyIsIdempotentOK(t *testing.T) {
	f := newFixture(t, singleShardCfg(5, metadata.ShardStatusActive))
	w := postJSON(t, f.srv.HandleDelete, "DELETE", "/data", DeleteRequest{Key: "never-existed", Epoch: 5})
	assert.Equal(t, http.StatusOK, w.Code)
}

func TestHandleDelete_NewerClientEpochAccepted(t *testing.T) {
	f := newFixture(t, singleShardCfg(5, metadata.ShardStatusActive))
	f.cache.Put("k", "v")
	w := postJSON(t, f.srv.HandleDelete, "DELETE", "/data", DeleteRequest{Key: "k", Epoch: 9})
	assert.Equal(t, http.StatusOK, w.Code)
	assert.Empty(t, f.cache.Get("k"))
}

// ---------- PUT extra paths ----------

func TestHandlePut_LockedAndMigratingRejected(t *testing.T) {
	for _, st := range []metadata.ShardStatus{metadata.ShardStatusLocked, metadata.ShardStatusMigrating} {
		t.Run(string(st), func(t *testing.T) {
			r := newRecorder(t)
			f := newFixture(t, singleShardCfg(5, st, r))
			w := postJSON(t, f.srv.HandlePut, "POST", "/data", WriteRequest{Key: "k", Value: "v", Epoch: 5})
			assert.Equal(t, http.StatusLocked, w.Code)
			assert.Empty(t, f.cache.Get("k"))
			assert.Zero(t, r.count())
		})
	}
}

func TestHandlePut_BadBody(t *testing.T) {
	f := newFixture(t, singleShardCfg(5, metadata.ShardStatusActive))
	w := postJSON(t, f.srv.HandlePut, "POST", "/data", "{nope")
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandlePut_ReplicatesWithPayloadAndEpoch(t *testing.T) {
	r1, r2 := newRecorder(t), newRecorder(t)
	f := newFixture(t, singleShardCfg(5, metadata.ShardStatusActive, r1, r2))

	w := postJSON(t, f.srv.HandlePut, "POST", "/data", WriteRequest{Key: "k", Value: "v", Epoch: 5})

	require.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "v", f.cache.Get("k"))
	for _, r := range []*recorder{r1, r2} {
		require.Equal(t, 1, r.count())
		assert.Equal(t, "/internal/replicate", r.path(0))
		assert.Equal(t, WriteRequest{Key: "k", Value: "v", Epoch: 5}, r.writeReq(0))
	}
}

// If any replica rejects, the client sees an error and the primary must not
// apply the write (replicas that already accepted are ahead; that is the
// documented trade-off of synchronous best-effort replication).
func TestHandlePut_SecondReplicaFails_PrimaryNotApplied(t *testing.T) {
	r1, r2 := newRecorder(t), newRecorder(t)
	r2.status.Store(http.StatusPreconditionFailed) // replica already on a newer epoch
	f := newFixture(t, singleShardCfg(5, metadata.ShardStatusActive, r1, r2))

	w := postJSON(t, f.srv.HandlePut, "POST", "/data", WriteRequest{Key: "k", Value: "v", Epoch: 5})

	assert.Equal(t, http.StatusInternalServerError, w.Code)
	assert.Empty(t, f.cache.Get("k"))
	assert.Equal(t, 1, r1.count())
	assert.Equal(t, 1, r2.count())
}

func TestHandlePut_ReplicaUnreachable(t *testing.T) {
	r := newRecorder(t)
	cfg := singleShardCfg(5, metadata.ShardStatusActive, r)
	r.srv.Close()
	f := newFixture(t, cfg)
	w := postJSON(t, f.srv.HandlePut, "POST", "/data", WriteRequest{Key: "k", Value: "v", Epoch: 5})
	assert.Equal(t, http.StatusInternalServerError, w.Code)
	assert.Empty(t, f.cache.Get("k"))
}

func TestHandlePut_ExactEpochAndNewerAccepted(t *testing.T) {
	f := newFixture(t, singleShardCfg(5, metadata.ShardStatusActive))
	for _, e := range []uint64{5, 6} {
		w := postJSON(t, f.srv.HandlePut, "POST", "/data", WriteRequest{Key: "k", Value: "v", Epoch: e})
		assert.Equal(t, http.StatusOK, w.Code, "epoch %d", e)
	}
}

// ---------- lease lost mid-request ----------

func TestHandlePut_LeaseLostDuringReplication_NotAppliedNorAcked(t *testing.T) {
	r := newRecorder(t)
	f := newFixture(t, singleShardCfg(5, metadata.ShardStatusActive, r))
	r.hook = func() { f.clock.Advance(testLease + time.Second) } // lease lapses while replicating

	w := postJSON(t, f.srv.HandlePut, "POST", "/data", WriteRequest{Key: "k", Value: "v", Epoch: 5})

	assert.Equal(t, http.StatusServiceUnavailable, w.Code, "a primary fenced mid-request must not acknowledge the write")
	assert.Empty(t, f.cache.Get("k"), "a fenced primary must not apply the write locally")
}

func TestHandleDelete_LeaseLostDuringReplication_NotAppliedNorAcked(t *testing.T) {
	r := newRecorder(t)
	f := newFixture(t, singleShardCfg(5, metadata.ShardStatusActive, r))
	f.cache.Put("k", "v")
	r.hook = func() { f.clock.Advance(testLease + time.Second) }

	w := postJSON(t, f.srv.HandleDelete, "DELETE", "/data", DeleteRequest{Key: "k", Epoch: 5})

	assert.Equal(t, http.StatusServiceUnavailable, w.Code)
	assert.Equal(t, "v", f.cache.Get("k"))
}

// ---------- replicate endpoints ----------

func TestHandleReplicate_And_DeleteReplicate(t *testing.T) {
	f := newFixture(t, &metadata.ClusterConfig{Epoch: 5})

	w := postJSON(t, f.srv.HandleReplicate, "POST", "/internal/replicate", WriteRequest{Key: "k", Value: "v", Epoch: 5})
	assert.Equal(t, http.StatusOK, w.Code, "equal epoch accepted")
	assert.Equal(t, "v", f.cache.Get("k"))

	w = postJSON(t, f.srv.HandleReplicate, "POST", "/internal/replicate", WriteRequest{Key: "k2", Value: "v2", Epoch: 9})
	assert.Equal(t, http.StatusOK, w.Code, "newer-epoch primary accepted")

	w = postJSON(t, f.srv.HandleDeleteReplicate, "POST", "/internal/replicate-delete", DeleteRequest{Key: "k", Epoch: 4})
	assert.Equal(t, http.StatusPreconditionFailed, w.Code, "stale primary's delete rejected")
	assert.Equal(t, "v", f.cache.Get("k"))

	w = postJSON(t, f.srv.HandleDeleteReplicate, "POST", "/internal/replicate-delete", DeleteRequest{Key: "k", Epoch: 5})
	assert.Equal(t, http.StatusOK, w.Code)
	assert.Empty(t, f.cache.Get("k"))

	for _, h := range []http.HandlerFunc{f.srv.HandleReplicate, f.srv.HandleDeleteReplicate} {
		assert.Equal(t, http.StatusBadRequest, postJSON(t, h, "POST", "/x", "{bad").Code)
	}
}

// ---------- put -> get -> delete round trip through real replicas ----------

func TestPrimaryReplica_EndToEndReplicationBetweenServers(t *testing.T) {
	// replica node is a real datanode Server behind httptest
	replica := newFixture(t, &metadata.ClusterConfig{Epoch: 5})
	mux := http.NewServeMux()
	mux.HandleFunc("/internal/replicate", replica.srv.HandleReplicate)
	mux.HandleFunc("/internal/replicate-delete", replica.srv.HandleDeleteReplicate)
	rs := newServerFromMux(t, mux)

	cfg := singleShardCfg(5, metadata.ShardStatusActive)
	cfg.Nodes["r"] = metadata.NodeMetadata{ID: "r", Address: rs, Status: metadata.StatusActive}
	s := cfg.Shards[0]
	s.ReplicaIDs = []string{"r"}
	cfg.Shards[0] = s
	primary := newFixture(t, cfg)

	require.Equal(t, http.StatusOK, postJSON(t, primary.srv.HandlePut, "POST", "/data", WriteRequest{Key: "k", Value: "v", Epoch: 5}).Code)
	assert.Equal(t, "v", primary.cache.Get("k"))
	assert.Equal(t, "v", replica.cache.Get("k"))

	require.Equal(t, http.StatusOK, postJSON(t, primary.srv.HandleDelete, "DELETE", "/data", DeleteRequest{Key: "k", Epoch: 5}).Code)
	assert.Empty(t, primary.cache.Get("k"))
	assert.Empty(t, replica.cache.Get("k"))
}
