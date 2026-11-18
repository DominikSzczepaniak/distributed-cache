package raft

// Fault-injecting transport for pkg/raft tests.
//
// FaultyNet sits between Raft nodes by wrapping the PeerClient interface. For
// every directed edge (from -> to) it can:
//   - drop the request (probability, seeded rand => reproducible),
//   - drop the response after the callee already processed the request,
//   - duplicate the request (callee sees it twice),
//   - delay the request (honours ctx cancellation),
//   - hold the request in a reorder buffer: the caller sees a timeout-like error
//     and the request is delivered later, on demand, via DeliverHeld (FIFO or
//     reversed). This is deterministic: nothing is delivered behind the test's
//     back,
//   - be cut by Partition/Cut and restored by Heal.
//
// Lock ordering: FaultyNet.mu is a leaf lock. It is never held while calling
// into a Raft node.

import (
	"context"
	"errors"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/dominikszczepaniak/distributed-cache/pkg/raft/raftpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

var (
	errNetPartitioned = errors.New("faultnet: edge partitioned")
	errNetDropped     = errors.New("faultnet: message dropped")
	errNetHeld        = errors.New("faultnet: message held in reorder buffer")
)

type edge struct{ from, to int }

type heldMsg struct {
	e       edge
	kind    string
	deliver func()
}

// EdgeStats counts what happened to messages on one directed edge.
type EdgeStats struct {
	Sent, Delivered, Dropped, Duplicated, Held, Blocked int
}

type FaultyNet struct {
	mu    sync.Mutex
	rnd   *rand.Rand
	n     int
	nodes []PeerClient // real targets, indexed by node id

	dropReq  map[edge]float64
	dropResp map[edge]float64
	dup      map[edge]float64
	delay    map[edge]time.Duration
	hold     map[edge]bool
	cut      map[edge]bool

	held   []heldMsg
	stats  map[edge]*EdgeStats
	byKind map[string]int
}

func NewFaultyNet(seed int64, targets []PeerClient) *FaultyNet {
	return &FaultyNet{
		rnd:      rand.New(rand.NewSource(seed)),
		n:        len(targets),
		nodes:    targets,
		dropReq:  map[edge]float64{},
		dropResp: map[edge]float64{},
		dup:      map[edge]float64{},
		delay:    map[edge]time.Duration{},
		hold:     map[edge]bool{},
		cut:      map[edge]bool{},
		stats:    map[edge]*EdgeStats{},
		byKind:   map[string]int{},
	}
}

// Peer returns the PeerClient that node `from` uses to talk to node `to`.
func (fn *FaultyNet) Peer(from, to int) PeerClient {
	return &FaultyPeer{net: fn, e: edge{from, to}}
}

func (fn *FaultyNet) forEachEdge(f func(e edge)) {
	for i := 0; i < fn.n; i++ {
		for j := 0; j < fn.n; j++ {
			if i != j {
				f(edge{i, j})
			}
		}
	}
}

// SetDrop sets the request-drop probability on from->to.
func (fn *FaultyNet) SetDrop(from, to int, p float64) {
	fn.mu.Lock()
	defer fn.mu.Unlock()
	fn.dropReq[edge{from, to}] = p
}

// SetDropAll sets the request-drop probability on every edge.
func (fn *FaultyNet) SetDropAll(p float64) {
	fn.mu.Lock()
	defer fn.mu.Unlock()
	fn.forEachEdge(func(e edge) { fn.dropReq[e] = p })
}

// SetDropResponse loses the *response* (callee still processed the request).
func (fn *FaultyNet) SetDropResponse(from, to int, p float64) {
	fn.mu.Lock()
	defer fn.mu.Unlock()
	fn.dropResp[edge{from, to}] = p
}

func (fn *FaultyNet) SetDuplicate(from, to int, p float64) {
	fn.mu.Lock()
	defer fn.mu.Unlock()
	fn.dup[edge{from, to}] = p
}

func (fn *FaultyNet) SetDuplicateAll(p float64) {
	fn.mu.Lock()
	defer fn.mu.Unlock()
	fn.forEachEdge(func(e edge) { fn.dup[e] = p })
}

func (fn *FaultyNet) SetDelay(from, to int, d time.Duration) {
	fn.mu.Lock()
	defer fn.mu.Unlock()
	fn.delay[edge{from, to}] = d
}

// Hold toggles the reorder buffer on from->to for LogRequest/VoteRequest/
// InstallSnapshot. While on, those calls fail at the caller and are buffered.
func (fn *FaultyNet) Hold(from, to int, on bool) {
	fn.mu.Lock()
	defer fn.mu.Unlock()
	fn.hold[edge{from, to}] = on
}

// Cut blocks one direction; Partition blocks both directions between groups.
func (fn *FaultyNet) Cut(from, to int) {
	fn.mu.Lock()
	defer fn.mu.Unlock()
	fn.cut[edge{from, to}] = true
}

func (fn *FaultyNet) Partition(groupA, groupB []int) {
	fn.mu.Lock()
	defer fn.mu.Unlock()
	for _, a := range groupA {
		for _, b := range groupB {
			fn.cut[edge{a, b}] = true
			fn.cut[edge{b, a}] = true
		}
	}
}

// Heal removes every partition/cut. Drop/dup/delay/hold settings are untouched.
func (fn *FaultyNet) Heal() {
	fn.mu.Lock()
	defer fn.mu.Unlock()
	fn.cut = map[edge]bool{}
}

// Reset clears every fault (but keeps held messages).
func (fn *FaultyNet) Reset() {
	fn.mu.Lock()
	defer fn.mu.Unlock()
	fn.dropReq = map[edge]float64{}
	fn.dropResp = map[edge]float64{}
	fn.dup = map[edge]float64{}
	fn.delay = map[edge]time.Duration{}
	fn.hold = map[edge]bool{}
	fn.cut = map[edge]bool{}
}

// HeldCount returns the number of buffered messages (optionally of one kind).
func (fn *FaultyNet) HeldCount(kind string) int {
	fn.mu.Lock()
	defer fn.mu.Unlock()
	c := 0
	for _, h := range fn.held {
		if kind == "" || h.kind == kind {
			c++
		}
	}
	return c
}

// DeliverHeld delivers every buffered message to its callee, ignoring the
// responses (the original callers already gave up). reverse=true delivers
// newest-first, which reorders them relative to send order. Returns how many
// were delivered.
func (fn *FaultyNet) DeliverHeld(reverse bool) int {
	fn.mu.Lock()
	batch := fn.held
	fn.held = nil
	fn.mu.Unlock()
	if reverse {
		for i := len(batch) - 1; i >= 0; i-- {
			batch[i].deliver()
		}
	} else {
		for _, h := range batch {
			h.deliver()
		}
	}
	return len(batch)
}

// DeliverAll is the "drain everything" helper: heal, then flush the buffer in
// send order.
func (fn *FaultyNet) DeliverAll() int {
	fn.mu.Lock()
	fn.hold = map[edge]bool{}
	fn.mu.Unlock()
	return fn.DeliverHeld(false)
}

func (fn *FaultyNet) Stats(from, to int) EdgeStats {
	fn.mu.Lock()
	defer fn.mu.Unlock()
	if s := fn.stats[edge{from, to}]; s != nil {
		return *s
	}
	return EdgeStats{}
}

// Calls returns how many calls of `kind` ("LogRequest", ...) were attempted.
func (fn *FaultyNet) Calls(kind string) int {
	fn.mu.Lock()
	defer fn.mu.Unlock()
	return fn.byKind[kind]
}

func (fn *FaultyNet) stat(e edge) *EdgeStats {
	s := fn.stats[e]
	if s == nil {
		s = &EdgeStats{}
		fn.stats[e] = s
	}
	return s
}

// faultyCall applies the configured faults to one RPC. `bufferable` kinds can
// be parked in the reorder buffer.
func faultyCall[T any](ctx context.Context, fn *FaultyNet, e edge, kind string, bufferable bool, f func() (T, error)) (T, error) {
	var zero T
	fn.mu.Lock()
	st := fn.stat(e)
	st.Sent++
	fn.byKind[kind]++
	if fn.cut[e] {
		st.Blocked++
		fn.mu.Unlock()
		return zero, errNetPartitioned
	}
	if p := fn.dropReq[e]; p > 0 && fn.rnd.Float64() < p {
		st.Dropped++
		fn.mu.Unlock()
		return zero, errNetDropped
	}
	if bufferable && fn.hold[e] {
		st.Held++
		fn.held = append(fn.held, heldMsg{e: e, kind: kind, deliver: func() { _, _ = f() }})
		fn.mu.Unlock()
		return zero, errNetHeld
	}
	dup := false
	if p := fn.dup[e]; p > 0 && fn.rnd.Float64() < p {
		dup = true
		st.Duplicated++
	}
	dropResp := false
	if p := fn.dropResp[e]; p > 0 && fn.rnd.Float64() < p {
		dropResp = true
	}
	d := fn.delay[e]
	fn.mu.Unlock()

	if d > 0 {
		t := time.NewTimer(d)
		select {
		case <-t.C:
		case <-ctx.Done():
			t.Stop()
			return zero, ctx.Err()
		}
	}
	if err := ctx.Err(); err != nil {
		return zero, err
	}
	resp, err := f()
	if dup {
		_, _ = f()
	}
	fn.mu.Lock()
	if dropResp {
		fn.stat(e).Dropped++
	} else {
		fn.stat(e).Delivered++
	}
	fn.mu.Unlock()
	if dropResp {
		return zero, errNetDropped
	}
	return resp, err
}

// FaultyPeer is the PeerClient handed to a Raft node for one directed edge.
type FaultyPeer struct {
	net *FaultyNet
	e   edge
}

func (p *FaultyPeer) target() PeerClient { return p.net.nodes[p.e.to] }

func (p *FaultyPeer) Forward(ctx context.Context, m *raftpb.Message) (*raftpb.ForwardResponse, error) {
	return faultyCall(ctx, p.net, p.e, "Forward", false, func() (*raftpb.ForwardResponse, error) {
		return p.target().Forward(ctx, m)
	})
}

func (p *FaultyPeer) ForwardGet(ctx context.Context, r *raftpb.GetRequest) (*raftpb.GetResponse, error) {
	return faultyCall(ctx, p.net, p.e, "ForwardGet", false, func() (*raftpb.GetResponse, error) {
		return p.target().ForwardGet(ctx, r)
	})
}

func (p *FaultyPeer) LogRequest(ctx context.Context, in *raftpb.LogRequestArgs) (*raftpb.LogResponse, error) {
	return faultyCall(ctx, p.net, p.e, "LogRequest", true, func() (*raftpb.LogResponse, error) {
		return p.target().LogRequest(context.WithoutCancel(ctx), in)
	})
}

func (p *FaultyPeer) VoteRequest(ctx context.Context, in *raftpb.VoteRequestArgs) (*raftpb.VoteResponse, error) {
	return faultyCall(ctx, p.net, p.e, "VoteRequest", true, func() (*raftpb.VoteResponse, error) {
		return p.target().VoteRequest(context.WithoutCancel(ctx), in)
	})
}

func (p *FaultyPeer) Heartbeat(ctx context.Context, in *emptypb.Empty) (*emptypb.Empty, error) {
	return faultyCall(ctx, p.net, p.e, "Heartbeat", false, func() (*emptypb.Empty, error) {
		return p.target().Heartbeat(ctx, in)
	})
}

func (p *FaultyPeer) InstallSnapshot(ctx context.Context, in *raftpb.InstallSnapshotRequest) (*raftpb.InstallSnapshotResponse, error) {
	return faultyCall(ctx, p.net, p.e, "InstallSnapshot", true, func() (*raftpb.InstallSnapshotResponse, error) {
		return p.target().InstallSnapshot(context.WithoutCancel(ctx), in)
	})
}

// ---------------------------------------------------------------------------
// Cluster helper
// ---------------------------------------------------------------------------

// TestCluster is N Raft nodes (no background election/heartbeat timers: tests
// drive elections explicitly) wired through a FaultyNet.
type TestCluster struct {
	Nodes []*Raft
	Net   *FaultyNet
}

// createClusterWithTransport builds n nodes whose peers all go through a
// seeded FaultyNet. Replicators started by becomeLeader are stopped on cleanup.
func createClusterWithTransport(t testing.TB, n int, seed int64) *TestCluster {
	t.Helper()
	c := &TestCluster{Nodes: make([]*Raft, n)}
	targets := make([]PeerClient, n)
	for i := 0; i < n; i++ {
		c.Nodes[i] = createTestRaft(t, i, n)
		targets[i] = &inMemPeer{r: c.Nodes[i]}
	}
	c.Net = NewFaultyNet(seed, targets)
	for i, node := range c.Nodes {
		peers := make([]PeerClient, n)
		for j := range peers {
			peers[j] = c.Net.Peer(i, j)
		}
		node.setPeers(peers)
	}
	t.Cleanup(func() {
		c.Net.Reset()
		for _, node := range c.Nodes {
			stopReplicators(node)
		}
		for _, node := range c.Nodes {
			stopSaver(node)
		}
	})
	return c
}

// ---- state inspection helpers (all take the node lock) ----

type nodeState struct {
	Term, VotedFor, Commit, LastIndex int
	Role                              Role
	Leader                            int
	Log                               []LogEntry
}

func stateOf(r *Raft) nodeState {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return nodeState{
		Term: r.currentTerm, VotedFor: r.votedFor, Commit: r.commitedLength,
		LastIndex: r.snapshotter.lastIndex + len(r.log),
		Role:      r.currentRole, Leader: r.currentLeaderId,
		Log: append([]LogEntry(nil), r.log...),
	}
}

func putEntry(term, key, val int) LogEntry {
	return LogEntry{Term: term, Message: Message{MsgType: PutMsg, Key: key, Value: intPtr(val)}}
}

// termsOf returns the terms of a log, handy for compact assertions.
func termsOf(l []LogEntry) []int {
	out := make([]int, len(l))
	for i, e := range l {
		out[i] = e.Term
	}
	return out
}

// electLeader makes node id a candidate and waits until it is leader.
func (c *TestCluster) electLeader(t testing.TB, id int) *Raft {
	t.Helper()
	c.Nodes[id].startElection()
	require.Eventually(t, func() bool { return c.Nodes[id].getCurrentRole() == Leader },
		5*time.Second, 5*time.Millisecond, "node %d did not become leader", id)
	return c.Nodes[id]
}

func (c *TestCluster) putOn(leader *Raft, key, val int) {
	leader.Broadcast(Message{MsgType: PutMsg, Key: key, Value: intPtr(val)})
}

func (c *TestCluster) waitCommitted(t testing.TB, ids []int, commit int) {
	t.Helper()
	require.Eventually(t, func() bool {
		for _, id := range ids {
			if stateOf(c.Nodes[id]).Commit < commit {
				return false
			}
		}
		return true
	}, 10*time.Second, 5*time.Millisecond, "nodes %v did not reach commit %d", ids, commit)
}

func allIDs(n int) []int {
	ids := make([]int, n)
	for i := range ids {
		ids[i] = i
	}
	return ids
}

// logRequestArgs builds a raw AppendEntries request for handler-level tests.
func logRequestArgs(leader, term, prevIdx, prevTerm, commit int, entries ...LogEntry) *raftpb.LogRequestArgs {
	pb := make([]*raftpb.LogEntry, len(entries))
	for i, e := range entries {
		pb[i] = &raftpb.LogEntry{Term: int32(e.Term), Message: &raftpb.Message{
			Type: toProtoMsgType(e.Message.MsgType), Key: int32(e.Message.Key),
			Value: wrapInt(e.Message.Value),
		}}
	}
	return &raftpb.LogRequestArgs{
		LeaderId: int32(leader), Term: int32(term),
		PrefixLen: int32(prevIdx), PrefixTerm: int32(prevTerm),
		CommitLength: int32(commit), Suffix: pb,
	}
}

func wrapInt(v *int) *wrapperspb.Int32Value {
	if v == nil {
		return nil
	}
	return wrapperspb.Int32(int32(*v))
}

// ---------------------------------------------------------------------------
// Self-tests for the transport (so a broken harness can't mask/produce bugs)
// ---------------------------------------------------------------------------

func TestFaultyNet_PartitionAndHeal(t *testing.T) {
	c := createClusterWithTransport(t, 3, 1)
	ctx := context.Background()
	req := logRequestArgs(0, 1, 0, 0, 0)

	resp, err := c.Net.Peer(0, 1).LogRequest(ctx, req)
	require.NoError(t, err)
	require.True(t, resp.Success)

	c.Net.Partition([]int{0}, []int{1, 2})
	for _, to := range []int{1, 2} {
		_, err = c.Net.Peer(0, to).LogRequest(ctx, req)
		require.ErrorIs(t, err, errNetPartitioned)
		_, err = c.Net.Peer(to, 0).LogRequest(ctx, req)
		require.ErrorIs(t, err, errNetPartitioned)
	}
	// 1<->2 is untouched.
	_, err = c.Net.Peer(1, 2).LogRequest(ctx, logRequestArgs(1, 1, 0, 0, 0))
	require.NoError(t, err)

	c.Net.Heal()
	_, err = c.Net.Peer(0, 1).LogRequest(ctx, req)
	require.NoError(t, err)
	assert.Equal(t, 2, c.Net.Stats(0, 1).Delivered)
	assert.Equal(t, 1, c.Net.Stats(0, 1).Blocked)
}

func TestFaultyNet_DropIsSeededAndDeterministic(t *testing.T) {
	run := func(seed int64) []bool {
		c := createClusterWithTransport(t, 2, seed)
		c.Net.SetDrop(0, 1, 0.5)
		out := make([]bool, 40)
		for i := range out {
			_, err := c.Net.Peer(0, 1).Heartbeat(context.Background(), &emptypb.Empty{})
			out[i] = err == nil
		}
		return out
	}
	a, b, other := run(42), run(42), run(7)
	assert.Equal(t, a, b, "same seed must give the same drop pattern")
	assert.NotEqual(t, a, other, "different seeds should differ")
	delivered := 0
	for _, ok := range a {
		if ok {
			delivered++
		}
	}
	assert.Greater(t, delivered, 5)
	assert.Less(t, delivered, 35)
}

func TestFaultyNet_HoldReorderAndDuplicate(t *testing.T) {
	c := createClusterWithTransport(t, 2, 1)
	ctx := context.Background()
	f := c.Nodes[1]

	// Two consecutive AppendEntries are buffered, then delivered newest-first.
	c.Net.Hold(0, 1, true)
	e1, e2 := putEntry(1, 1, 10), putEntry(1, 2, 20)
	_, err := c.Net.Peer(0, 1).LogRequest(ctx, logRequestArgs(0, 1, 0, 0, 0, e1))
	require.ErrorIs(t, err, errNetHeld)
	_, err = c.Net.Peer(0, 1).LogRequest(ctx, logRequestArgs(0, 1, 1, 1, 0, e2))
	require.ErrorIs(t, err, errNetHeld)
	assert.Equal(t, 2, c.Net.HeldCount("LogRequest"))
	assert.Equal(t, 0, stateOf(f).LastIndex, "nothing delivered while held")

	c.Net.Hold(0, 1, false)
	assert.Equal(t, 2, c.Net.DeliverHeld(true))
	// reversed: e2 (prev=1) is rejected, then e1 applies => only e1 present.
	assert.Equal(t, []int{1}, termsOf(stateOf(f).Log))

	// Duplicate delivery: callee sees the request twice, log is unchanged.
	c.Net.SetDuplicate(0, 1, 1.0)
	_, err = c.Net.Peer(0, 1).LogRequest(ctx, logRequestArgs(0, 1, 1, 1, 0, e2))
	require.NoError(t, err)
	assert.Equal(t, []int{1, 1}, termsOf(stateOf(f).Log))
	assert.Equal(t, 1, c.Net.Stats(0, 1).Duplicated)
}
