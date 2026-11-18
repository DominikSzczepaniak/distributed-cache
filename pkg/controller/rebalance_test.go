package controller

import (
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dominikszczepaniak/distributed-cache/pkg/metadata"
	"github.com/dominikszczepaniak/distributed-cache/pkg/raft"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type pullCall struct {
	shard  int
	source string
}

// mockDatanode records /internal/pull requests it receives and answers with status.
type mockDatanode struct {
	srv    *httptest.Server
	status atomic.Int32
	// statusFn, if set, decides the status for the n-th (1-based) pull.
	statusFn func(n int) int
	mu       sync.Mutex
	pulls    []pullCall
}

func newMockDatanode(t *testing.T) *mockDatanode {
	t.Helper()
	m := &mockDatanode{}
	m.status.Store(http.StatusOK)
	m.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/internal/pull" || r.Method != http.MethodPost {
			http.NotFound(w, r)
			return
		}
		shard, _ := strconv.Atoi(r.URL.Query().Get("shard"))
		m.mu.Lock()
		m.pulls = append(m.pulls, pullCall{shard: shard, source: r.URL.Query().Get("source")})
		n := len(m.pulls)
		fn := m.statusFn
		m.mu.Unlock()
		if fn != nil {
			w.WriteHeader(fn(n))
			return
		}
		w.WriteHeader(int(m.status.Load()))
	}))
	t.Cleanup(m.srv.Close)
	return m
}

func (m *mockDatanode) addr() string { return m.srv.Listener.Addr().String() }
func (m *mockDatanode) url() string  { return "http://" + m.addr() }
func (m *mockDatanode) pullCalls() []pullCall {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]pullCall(nil), m.pulls...)
}

func addNode(c *Controller, id string, m *mockDatanode, st metadata.NodeStatus) {
	c.config.Nodes[id] = metadata.NodeMetadata{ID: id, Address: m.addr(), Status: st}
}

// Rebalance must assign primaries, add replicas, AND actually trigger a data
// pull on every new replica from that shard's primary (previously this test
// passed while every sync failed with "connection refused").
func TestController_Rebalance(t *testing.T) {
	c := NewController(&raft.Raft{}, 10*time.Second)
	a, b := newMockDatanode(t), newMockDatanode(t)
	addNode(c, "nodeA", a, metadata.StatusActive)
	addNode(c, "nodeB", b, metadata.StatusActive)
	for i := 0; i < 10; i++ {
		c.config.Shards[i] = metadata.ShardMetadata{ID: i, PrimaryID: "", Status: metadata.ShardStatusActive}
	}

	c.Rebalance()

	config := c.GetConfig()
	assignedShards := 0
	for _, shard := range config.Shards {
		if shard.PrimaryID != "" {
			assignedShards++
		}
		if len(shard.ReplicaIDs) != 1 {
			t.Errorf("Shard %d: expected 1 replica, got %d", shard.ID, len(shard.ReplicaIDs))
		}
		if len(shard.ReplicaIDs) > 0 && shard.ReplicaIDs[0] == shard.PrimaryID {
			t.Errorf("Shard %d: replica should be different from primary", shard.ID)
		}
	}
	if assignedShards != 10 {
		t.Errorf("Expected all 10 shards to be assigned, got %d", assignedShards)
	}
	if config.Epoch <= 0 {
		t.Errorf("Expected epoch > 0, got %d", config.Epoch)
	}

	// Every shard's replica must have been told to pull that shard from the primary.
	mocks := map[string]*mockDatanode{"nodeA": a, "nodeB": b}
	require.Eventually(t, func() bool { return len(a.pullCalls())+len(b.pullCalls()) == 10 },
		3*time.Second, 10*time.Millisecond, "expected one replica sync per shard, got A=%d B=%d", len(a.pullCalls()), len(b.pullCalls()))
	for _, shard := range config.Shards {
		replica := mocks[shard.ReplicaIDs[0]]
		primary := mocks[shard.PrimaryID]
		var found bool
		for _, p := range replica.pullCalls() {
			if p.shard == shard.ID {
				found = true
				assert.Equal(t, primary.url(), p.source, "shard %d must be pulled from its primary", shard.ID)
			}
		}
		assert.True(t, found, "shard %d: replica %s never asked to sync", shard.ID, shard.ReplicaIDs[0])
		for _, p := range primary.pullCalls() {
			assert.NotEqual(t, shard.ID, p.shard, "primary must not pull its own shard")
		}
	}
}

func TestController_Rebalance_ThreeNodesAssignsTwoDistinctReplicas(t *testing.T) {
	c := NewController(&raft.Raft{}, 10*time.Second)
	c.config = metadata.NewClusterConfig(6)
	ms := map[string]*mockDatanode{"n1": newMockDatanode(t), "n2": newMockDatanode(t), "n3": newMockDatanode(t)}
	for id, m := range ms {
		addNode(c, id, m, metadata.StatusActive)
	}

	c.Rebalance()

	total := 0
	for _, shard := range c.GetConfig().Shards {
		require.NotEmpty(t, shard.PrimaryID)
		require.Len(t, shard.ReplicaIDs, 2)
		seen := map[string]bool{shard.PrimaryID: true}
		for _, r := range shard.ReplicaIDs {
			assert.False(t, seen[r], "shard %d: node %s appears twice", shard.ID, r)
			seen[r] = true
		}
		total += 2
	}
	require.Eventually(t, func() bool {
		n := 0
		for _, m := range ms {
			n += len(m.pullCalls())
		}
		return n == total
	}, 3*time.Second, 10*time.Millisecond)
}

func TestController_Rebalance_SyncFailureDoesNotRollBackTopologyOrStopOtherSyncs(t *testing.T) {
	c := NewController(&raft.Raft{}, 10*time.Second)
	c.config = metadata.NewClusterConfig(4)
	bad, good := newMockDatanode(t), newMockDatanode(t)
	bad.status.Store(http.StatusInternalServerError)
	addNode(c, "bad", bad, metadata.StatusActive)
	addNode(c, "good", good, metadata.StatusActive)

	c.Rebalance()

	for _, shard := range c.GetConfig().Shards {
		assert.NotEmpty(t, shard.PrimaryID)
		assert.Len(t, shard.ReplicaIDs, 1)
	}
	// Both replicas get a pull attempt even though one side keeps failing.
	require.Eventually(t, func() bool { return len(bad.pullCalls())+len(good.pullCalls()) == 4 }, 3*time.Second, 10*time.Millisecond)
}

func TestController_Rebalance_UnreachableReplicaDoesNotHang(t *testing.T) {
	c := NewController(&raft.Raft{}, 10*time.Second)
	c.config = metadata.NewClusterConfig(2)
	live := newMockDatanode(t)
	dead := newMockDatanode(t)
	deadAddr := dead.addr()
	dead.srv.Close()
	c.config.Nodes["live"] = metadata.NodeMetadata{ID: "live", Address: live.addr(), Status: metadata.StatusActive}
	c.config.Nodes["gone"] = metadata.NodeMetadata{ID: "gone", Address: deadAddr, Status: metadata.StatusActive}

	done := make(chan struct{})
	go func() { c.Rebalance(); close(done) }()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Rebalance blocked on an unreachable node; sync must be asynchronous")
	}
	assert.Equal(t, 2, len(c.GetConfig().Shards))
}

func TestController_Rebalance_NoChangeWhenAlreadyBalanced(t *testing.T) {
	c := NewController(&raft.Raft{}, 10*time.Second)
	c.config = metadata.NewClusterConfig(3)
	a, b := newMockDatanode(t), newMockDatanode(t)
	addNode(c, "a", a, metadata.StatusActive)
	addNode(c, "b", b, metadata.StatusActive)
	for i := 0; i < 3; i++ {
		c.config.Shards[i] = metadata.ShardMetadata{ID: i, PrimaryID: "a", ReplicaIDs: []string{"b"}, Status: metadata.ShardStatusActive}
	}
	c.config.Epoch = 9

	c.Rebalance()

	assert.Equal(t, uint64(9), c.GetConfig().Epoch, "no-op rebalance must not bump epoch")
	time.Sleep(50 * time.Millisecond)
	assert.Empty(t, a.pullCalls())
	assert.Empty(t, b.pullCalls(), "no syncs when nothing changed")
}

func TestController_Rebalance_NoActiveNodes(t *testing.T) {
	c := NewController(&raft.Raft{}, 10*time.Second)
	c.config = metadata.NewClusterConfig(3)
	m := newMockDatanode(t)
	addNode(c, "dead", m, metadata.StatusDead)

	c.Rebalance()

	cfg := c.GetConfig()
	assert.Equal(t, uint64(0), cfg.Epoch)
	for _, s := range cfg.Shards {
		assert.Empty(t, s.PrimaryID, "no live node may be assigned a shard")
	}
}

func TestController_Rebalance_SkipsDeadNodesAsReplicaCandidates(t *testing.T) {
	c := NewController(&raft.Raft{}, 10*time.Second)
	c.config = metadata.NewClusterConfig(4)
	a, b, d := newMockDatanode(t), newMockDatanode(t), newMockDatanode(t)
	addNode(c, "a", a, metadata.StatusActive)
	addNode(c, "b", b, metadata.StatusActive)
	addNode(c, "dead", d, metadata.StatusDead)

	c.Rebalance()

	for _, s := range c.GetConfig().Shards {
		assert.NotEqual(t, "dead", s.PrimaryID)
		assert.NotContains(t, s.ReplicaIDs, "dead")
	}
	time.Sleep(100 * time.Millisecond)
	assert.Empty(t, d.pullCalls(), "dead node must never be asked to sync")
}

func TestController_Rebalance_ConcurrentCallsKeepInvariants(t *testing.T) {
	c := NewController(&raft.Raft{}, 10*time.Second)
	c.config = metadata.NewClusterConfig(8)
	for _, id := range []string{"n1", "n2", "n3"} {
		addNode(c, id, newMockDatanode(t), metadata.StatusActive)
	}

	var wg sync.WaitGroup
	for i := 0; i < 6; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); c.Rebalance() }()
	}
	wg.Wait()

	for _, s := range c.GetConfig().Shards {
		require.NotEmpty(t, s.PrimaryID)
		seen := map[string]bool{s.PrimaryID: true}
		for _, r := range s.ReplicaIDs {
			assert.False(t, seen[r], "shard %d duplicate holder %s", s.ID, r)
			seen[r] = true
		}
	}
}

// Node failure promotes the first replica, drops the dead node from every replica list,
// and rebalance then back-fills a replacement replica with a real sync.
func TestController_NodeFailure_PromotesReplicaAndBackfills(t *testing.T) {
	c := NewController(&raft.Raft{}, 10*time.Second)
	c.config = metadata.NewClusterConfig(1)
	a, b, cc, d := newMockDatanode(t), newMockDatanode(t), newMockDatanode(t), newMockDatanode(t)
	addNode(c, "A", a, metadata.StatusActive)
	addNode(c, "B", b, metadata.StatusActive)
	addNode(c, "C", cc, metadata.StatusActive)
	addNode(c, "D", d, metadata.StatusActive)
	c.config.Shards[0] = metadata.ShardMetadata{ID: 0, PrimaryID: "A", ReplicaIDs: []string{"B", "C"}, Status: metadata.ShardStatusActive}
	c.config.Epoch = 4

	c.HandleNodeFailure("A")

	require.Eventually(t, func() bool { return len(c.GetConfig().Shards[0].ReplicaIDs) == 2 }, 3*time.Second, 10*time.Millisecond,
		"rebalance must restore the replica count")
	cfg := c.GetConfig()
	shard := cfg.Shards[0]
	assert.Equal(t, "B", shard.PrimaryID, "first replica is promoted")
	assert.NotContains(t, shard.ReplicaIDs, "A")
	assert.NotContains(t, shard.ReplicaIDs, "B")
	assert.ElementsMatch(t, []string{"C", "D"}, shard.ReplicaIDs, "D back-fills the lost replica")
	assert.Equal(t, metadata.StatusDead, cfg.Nodes["A"].Status)
	assert.Greater(t, cfg.Epoch, uint64(4), "epoch must advance so clients/datanodes fence the old primary")

	require.Eventually(t, func() bool { return len(d.pullCalls()) == 1 }, 3*time.Second, 10*time.Millisecond)
	assert.Equal(t, pullCall{shard: 0, source: b.url()}, d.pullCalls()[0], "new replica syncs from the NEW primary")
	assert.Empty(t, a.pullCalls(), "dead node is never contacted")
}

func TestController_NodeFailure_ReplicaDeathKeepsPrimary(t *testing.T) {
	c := NewController(&raft.Raft{}, 10*time.Second)
	c.config = metadata.NewClusterConfig(1)
	a, b := newMockDatanode(t), newMockDatanode(t)
	addNode(c, "A", a, metadata.StatusActive)
	addNode(c, "B", b, metadata.StatusActive)
	c.config.Shards[0] = metadata.ShardMetadata{ID: 0, PrimaryID: "A", ReplicaIDs: []string{"B"}, Status: metadata.ShardStatusActive}

	c.HandleNodeFailure("B")

	shard := c.GetConfig().Shards[0]
	assert.Equal(t, "A", shard.PrimaryID)
	assert.Empty(t, shard.ReplicaIDs)
	assert.Equal(t, metadata.StatusDead, c.GetConfig().Nodes["B"].Status)
}
