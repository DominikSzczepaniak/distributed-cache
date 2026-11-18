package raft

// Edge-case tests for log replication, leader safety, partitions and quorum
// behaviour. Handler-level tests call RPC handlers directly (fully
// deterministic); cluster-level tests go through FaultyNet.

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dominikszczepaniak/distributed-cache/pkg/raft/raftpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// countingApp counts how many times the state machine applied a command.
type countingApp struct {
	*testApp
	applied atomic.Int64
}

func (a *countingApp) AppendMessage(m Message) (bool, int) {
	a.applied.Add(1)
	return a.testApp.AppendMessage(m)
}

func newFollowerWithCountingApp(t testing.TB) (*Raft, *countingApp) {
	f := createTestRaft(t, 1, 3)
	app := &countingApp{testApp: newTestApp()}
	f.mu.Lock()
	f.application = app
	f.mu.Unlock()
	return f, app
}

func mustLogRequest(t testing.TB, f *Raft, args *raftpb.LogRequestArgs) *raftpb.LogResponse {
	t.Helper()
	resp, err := f.LogRequest(context.Background(), args)
	require.NoError(t, err)
	return resp
}

func setLog(r *Raft, term int, entries ...LogEntry) {
	r.mu.Lock()
	r.currentTerm = term
	r.log = append([]LogEntry(nil), entries...)
	r.mu.Unlock()
}

// ---------------------------------------------------------------------------
// AppendEntries handler
// ---------------------------------------------------------------------------

func TestLogRequest_DuplicateIsNoop(t *testing.T) {
	t.Parallel()
	f, app := newFollowerWithCountingApp(t)
	e1, e2 := putEntry(1, 1, 10), putEntry(1, 2, 20)
	req := logRequestArgs(0, 1, 0, 0, 2, e1, e2)

	r1 := mustLogRequest(t, f, req)
	require.True(t, r1.Success)
	require.EqualValues(t, 2, r1.Ack)
	require.EqualValues(t, 2, app.applied.Load())
	before := stateOf(f)

	for i := 0; i < 3; i++ { // replay the exact same RPC
		r := mustLogRequest(t, f, req)
		assert.True(t, r.Success)
		assert.EqualValues(t, 2, r.Ack, "ack must not move on a duplicate")
	}
	after := stateOf(f)
	assert.Equal(t, before.Log, after.Log)
	assert.Equal(t, before.Commit, after.Commit)
	assert.Equal(t, before.Term, after.Term)
	assert.EqualValues(t, 2, app.applied.Load(), "committed entries must be applied exactly once")
	assert.Equal(t, 10, app.getData()[1])
	assert.Equal(t, 20, app.getData()[2])
}

func TestLogRequest_OutOfOrderAndDelayed(t *testing.T) {
	t.Parallel()
	e1, e2, e3 := putEntry(1, 1, 10), putEntry(1, 2, 20), putEntry(1, 3, 30)

	t.Run("gap_is_rejected_then_converges", func(t *testing.T) {
		f := createTestRaft(t, 1, 3)
		// second batch (prev=1) overtakes the first
		r := mustLogRequest(t, f, logRequestArgs(0, 1, 1, 1, 0, e2))
		assert.False(t, r.Success)
		assert.Empty(t, stateOf(f).Log, "a rejected request must not touch the log")

		assert.True(t, mustLogRequest(t, f, logRequestArgs(0, 1, 0, 0, 0, e1)).Success)
		assert.True(t, mustLogRequest(t, f, logRequestArgs(0, 1, 1, 1, 0, e2)).Success)
		assert.Equal(t, []int{1, 1}, termsOf(stateOf(f).Log))
		assert.Equal(t, 20, *stateOf(f).Log[1].Message.Value)
	})

	t.Run("delayed_request_from_old_term_is_rejected", func(t *testing.T) {
		f := createTestRaft(t, 1, 3)
		setLog(f, 3, e1, e2, putEntry(3, 3, 30))
		r := mustLogRequest(t, f, logRequestArgs(0, 2, 0, 0, 0, putEntry(2, 9, 99)))
		assert.False(t, r.Success)
		assert.EqualValues(t, 3, r.CurrentTerm, "reply must carry the newer term so the old leader steps down")
		st := stateOf(f)
		assert.Equal(t, 3, st.Term, "term must never go backwards")
		assert.Equal(t, []int{1, 1, 3}, termsOf(st.Log))
	})

	t.Run("delayed_shorter_request_same_term_keeps_longer_log", func(t *testing.T) {
		f := createTestRaft(t, 1, 3)
		setLog(f, 1, e1, e2, e3)
		r := mustLogRequest(t, f, logRequestArgs(0, 1, 0, 0, 0, e1))
		assert.True(t, r.Success)
		assert.Equal(t, 3, stateOf(f).LastIndex, "stale shorter AppendEntries must not truncate acknowledged entries")
	})
}

func TestLogRequest_ConflictTruncatesSuffixThenConverges(t *testing.T) {
	t.Parallel()
	// follower holds uncommitted entries from a deposed leader (terms 1,1,2,2)
	base := []LogEntry{putEntry(1, 1, 1), putEntry(1, 2, 2), putEntry(2, 3, 3), putEntry(2, 4, 4)}
	cases := []struct {
		name      string
		prevIdx   int
		prevTerm  int
		entries   []LogEntry
		wantTerms []int
		wantKeys  []int
		wantAck   int32
	}{
		{"same_length_replacement", 2, 1, []LogEntry{putEntry(3, 30, 3), putEntry(3, 40, 4)}, []int{1, 1, 3, 3}, []int{1, 2, 30, 40}, 4},
		{"longer_leader_log", 2, 1, []LogEntry{putEntry(3, 30, 3), putEntry(3, 40, 4), putEntry(3, 50, 5)}, []int{1, 1, 3, 3, 3}, []int{1, 2, 30, 40, 50}, 5},
		{"shorter_leader_log", 2, 1, []LogEntry{putEntry(3, 30, 3)}, []int{1, 1, 3}, []int{1, 2, 30}, 3},
		{"conflict_not_at_first_new_entry", 1, 1, []LogEntry{putEntry(1, 2, 2), putEntry(3, 30, 3)}, []int{1, 1, 3}, []int{1, 2, 30}, 3},
		{"conflict_at_first_entry", 0, 0, []LogEntry{putEntry(3, 30, 3)}, []int{3}, []int{30}, 1},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f := createTestRaft(t, 1, 3)
			setLog(f, 2, base...)
			r := mustLogRequest(t, f, logRequestArgs(0, 3, tc.prevIdx, tc.prevTerm, 0, tc.entries...))
			require.True(t, r.Success)
			assert.Equal(t, tc.wantAck, r.Ack)
			st := stateOf(f)
			assert.Equal(t, tc.wantTerms, termsOf(st.Log), "follower log must match the leader's after the conflict")
			keys := make([]int, len(st.Log))
			for i, e := range st.Log {
				keys[i] = e.Message.Key
			}
			assert.Equal(t, tc.wantKeys, keys)
			assert.Equal(t, 3, st.Term)
		})
	}
}

func TestLogRequest_PrevLogMismatchRejectedWithoutMutation(t *testing.T) {
	t.Parallel()
	f := createTestRaft(t, 1, 3)
	log := []LogEntry{putEntry(1, 1, 1), putEntry(1, 2, 2), putEntry(2, 3, 3)}
	setLog(f, 2, log...)
	cases := []struct {
		name            string
		prevIdx, prevTm int
	}{
		{"prev_term_differs", 3, 1},
		{"prev_beyond_log", 7, 2},
	}
	for _, tc := range cases {
		r := mustLogRequest(t, f, logRequestArgs(0, 2, tc.prevIdx, tc.prevTm, 3, putEntry(2, 9, 9)))
		assert.False(t, r.Success, tc.name)
		assert.EqualValues(t, 0, r.Ack, tc.name)
		st := stateOf(f)
		assert.Equal(t, []int{1, 1, 2}, termsOf(st.Log), tc.name)
		assert.Equal(t, 0, st.Commit, "a rejected request must not advance commit: %s", tc.name)
	}
}

// ---------------------------------------------------------------------------
// Leader steps down on higher term
// ---------------------------------------------------------------------------

func makeLeader(t testing.TB, id, total, term int) *Raft {
	n := createTestRaft(t, id, total)
	n.mu.Lock()
	n.currentTerm = term
	n.votedFor = id
	n.currentRole = Leader
	n.currentLeaderId = id
	n.mu.Unlock()
	return n
}

func TestLeaderStepsDownOnHigherTermLogResponse(t *testing.T) {
	t.Parallel()
	l := makeLeader(t, 0, 3, 1)
	l.logResponse(1, 2, 0, false)
	st := stateOf(l)
	assert.Equal(t, Follower, st.Role)
	assert.Equal(t, 2, st.Term)
	assert.Equal(t, -1, st.VotedFor, "vote must be cleared on a new term")
}

func TestLeaderStepsDownOnHigherTermInReplicatorReply(t *testing.T) {
	t.Parallel()
	l := makeLeader(t, 0, 3, 1)
	m := &mockPeerClient{}
	m.On("LogRequest", mock.Anything, mock.Anything).
		Return(&raftpb.LogResponse{NodeId: 1, CurrentTerm: 5, Success: false}, nil).Once()
	l.setPeers([]PeerClient{nil, m, &mockPeerClient{}})
	l.mu.Lock()
	l.sentLengths[1] = 1
	rep := NewReplicator(l, 1)
	l.replicators = []*Replicator{nil, rep, nil}
	l.mu.Unlock()

	rep.replicate()

	st := stateOf(l)
	assert.Equal(t, Follower, st.Role)
	assert.Equal(t, 5, st.Term)
	l.mu.RLock()
	assert.Nil(t, l.replicators, "replicators must be torn down on step-down")
	l.mu.RUnlock()
	m.AssertExpectations(t)
}

func TestLeaderIgnoresStaleTermLogResponse(t *testing.T) {
	t.Parallel()
	l := makeLeader(t, 0, 3, 4)
	l.mu.Lock()
	l.log = []LogEntry{putEntry(4, 1, 1)}
	l.mu.Unlock()
	l.logResponse(1, 3, 1, true) // late reply from term 3
	st := stateOf(l)
	assert.Equal(t, Leader, st.Role)
	assert.Equal(t, 0, st.Commit, "an ack from an old term must not count toward commit")
}

// ---------------------------------------------------------------------------
// Partitions (5 nodes)
// ---------------------------------------------------------------------------

func TestPartition_MinorityCannotCommit_MajorityProgresses_HealConverges(t *testing.T) {
	c := createClusterWithTransport(t, 5, 11)
	old := c.electLeader(t, 0)
	c.putOn(old, 1, 10)
	c.waitCommitted(t, allIDs(5), 1)
	oldTerm := stateOf(old).Term

	// {0,1} is the minority, {2,3,4} the majority.
	c.Net.Partition([]int{0, 1}, []int{2, 3, 4})
	c.putOn(old, 2, 20) // accepted by the stale leader, can never reach quorum

	require.Never(t, func() bool {
		return stateOf(c.Nodes[0]).Commit > 1 || stateOf(c.Nodes[1]).Commit > 1
	}, 600*time.Millisecond, 20*time.Millisecond, "minority side committed without a quorum")
	assert.Equal(t, 0, c.Nodes[0].application.(*testApp).getData()[2], "uncommitted write must not be applied")

	// Majority side elects its own leader and keeps committing.
	nl := c.electLeader(t, 2)
	assert.Greater(t, stateOf(nl).Term, oldTerm)
	c.putOn(nl, 3, 30)
	c.waitCommitted(t, []int{2, 3, 4}, 2)

	// Heal: the old leader/follower converge and drop the uncommitted entry.
	c.Net.Heal()
	c.waitCommitted(t, allIDs(5), 2)
	require.Eventually(t, func() bool {
		ref := stateOf(c.Nodes[2])
		for _, n := range c.Nodes {
			st := stateOf(n)
			if len(st.Log) != len(ref.Log) {
				return false
			}
			for i := range st.Log {
				if st.Log[i].Term != ref.Log[i].Term || st.Log[i].Message.Key != ref.Log[i].Message.Key {
					return false
				}
			}
		}
		return true
	}, 10*time.Second, 10*time.Millisecond, "logs did not converge after heal")

	for i, n := range c.Nodes {
		st := stateOf(n)
		assert.Equal(t, []int{1, 2}, termsOf(st.Log), "node %d", i)
		data := n.application.(*testApp).getData()
		assert.Equal(t, 10, data[1], "node %d", i)
		assert.Equal(t, 30, data[3], "node %d", i)
		_, has := data[2]
		assert.False(t, has, "node %d must have lost the uncommitted entry", i)
	}
	assert.NotEqual(t, Leader, stateOf(c.Nodes[0]).Role, "deposed leader must be a follower")
}

func TestReplication_ConvergesUnderMessageDrops(t *testing.T) {
	for _, drop := range []float64{0.3, 0.5} {
		drop := drop
		t.Run(time.Duration(drop*100).String(), func(t *testing.T) {
			t.Parallel()
			c := createClusterWithTransport(t, 3, 21)
			l := c.electLeader(t, 0)
			c.Net.SetDropAll(drop)
			c.Net.SetDropResponse(0, 1, drop/2)
			const n = 15
			for i := 0; i < n; i++ {
				c.putOn(l, i, i*10)
			}
			c.waitCommitted(t, allIDs(3), n)
			c.Net.Reset()
			ref := stateOf(l)
			for i, node := range c.Nodes {
				st := stateOf(node)
				assert.Equal(t, termsOf(ref.Log), termsOf(st.Log), "node %d", i)
				data := node.application.(*testApp).getData()
				for k := 0; k < n; k++ {
					assert.Equal(t, k*10, data[k], "node %d key %d", i, k)
				}
			}
			assert.Greater(t, c.Net.Stats(0, 1).Dropped+c.Net.Stats(0, 2).Dropped, 0, "fault injection must actually have dropped something")
		})
	}
}

func TestReplication_ReorderedAndDuplicatedAppendEntriesConverge(t *testing.T) {
	c := createClusterWithTransport(t, 3, 5)
	l := c.electLeader(t, 0)

	c.Net.Hold(0, 1, true) // every AppendEntries to node 1 is parked
	for i := 0; i < 5; i++ {
		c.putOn(l, i, i+100)
	}
	c.waitCommitted(t, []int{0, 2}, 5) // quorum is {0,2}
	require.Eventually(t, func() bool { return c.Net.HeldCount("LogRequest") >= 3 }, 5*time.Second, 5*time.Millisecond)
	assert.Equal(t, 0, stateOf(c.Nodes[1]).LastIndex, "node 1 saw nothing while held")

	c.Net.SetDuplicate(0, 1, 0.5)
	c.Net.Hold(0, 1, false)
	c.Net.DeliverHeld(true) // newest-first: maximal reordering of stale requests
	c.waitCommitted(t, allIDs(3), 5)

	ref := stateOf(l)
	st := stateOf(c.Nodes[1])
	assert.Equal(t, termsOf(ref.Log), termsOf(st.Log))
	for k := 0; k < 5; k++ {
		assert.Equal(t, k+100, c.Nodes[1].application.(*testApp).getData()[k])
	}
}

// ---------------------------------------------------------------------------
// Quorum timeout
// ---------------------------------------------------------------------------

func TestLeaderWithoutQuorum_BroadcastTimesOutThenStepsDown(t *testing.T) {
	c := createClusterWithTransport(t, 3, 3)
	l := c.electLeader(t, 0)
	c.Net.Partition([]int{0}, []int{1, 2})

	// Replicators need 5 consecutive failures (>=250ms) before stepping down,
	// so a short BroadcastSync deadline expires first.
	ok, _, err := l.BroadcastSync(Message{MsgType: PutMsg, Key: 1, Value: intPtr(1)}, 60*time.Millisecond)
	assert.False(t, ok)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "timeout")
	assert.Equal(t, 0, stateOf(l).Commit, "no quorum => nothing committed")

	require.Eventually(t, func() bool { return l.getCurrentRole() != Leader },
		5*time.Second, 10*time.Millisecond, "isolated leader must step down after quorum loss")
	assert.Equal(t, 0, stateOf(l).Commit)

	// A node that is no longer leader and knows no leader refuses writes.
	ok, _, err = l.BroadcastSync(Message{MsgType: PutMsg, Key: 2, Value: intPtr(2)}, time.Second)
	assert.False(t, ok)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no leader known")
}

// ---------------------------------------------------------------------------
// Rewritten TestLogReplication: real assertions, success and failure paths
// ---------------------------------------------------------------------------

func TestLogReplication(t *testing.T) {
	t.Parallel()

	t.Run("success_all_followers_reachable", func(t *testing.T) {
		t.Parallel()
		c := createClusterWithTransport(t, 3, 1)
		l := c.electLeader(t, 0)
		c.putOn(l, 1, 42)
		c.waitCommitted(t, allIDs(3), 1)
		for i, n := range c.Nodes {
			st := stateOf(n)
			require.Len(t, st.Log, 1, "node %d", i)
			assert.Equal(t, 1, st.Log[0].Term)
			assert.Equal(t, 42, *st.Log[0].Message.Value)
			assert.Equal(t, 42, n.application.(*testApp).getData()[1], "node %d", i)
		}
	})

	t.Run("failure_one_follower_down_quorum_still_commits", func(t *testing.T) {
		t.Parallel()
		c := createClusterWithTransport(t, 3, 1)
		l := c.electLeader(t, 0)
		c.Net.Partition([]int{2}, []int{0, 1})
		c.putOn(l, 2, 24)
		c.waitCommitted(t, []int{0, 1}, 1)
		assert.Equal(t, 0, stateOf(c.Nodes[2]).LastIndex, "isolated follower must not have the entry")
		assert.Equal(t, 0, c.Nodes[2].application.(*testApp).getData()[2])

		c.Net.Heal() // it catches up afterwards
		c.waitCommitted(t, allIDs(3), 1)
		assert.Equal(t, 24, c.Nodes[2].application.(*testApp).getData()[2])
	})

	t.Run("failure_no_quorum_never_commits", func(t *testing.T) {
		t.Parallel()
		c := createClusterWithTransport(t, 3, 1)
		l := c.electLeader(t, 0)
		c.Net.Partition([]int{0}, []int{1, 2})
		c.putOn(l, 3, 7)
		require.Never(t, func() bool { return stateOf(l).Commit > 0 }, 400*time.Millisecond, 20*time.Millisecond)
		assert.Equal(t, 0, l.application.(*testApp).getData()[3])
	})

	t.Run("failure_follower_with_divergent_log_is_repaired", func(t *testing.T) {
		t.Parallel()
		c := createClusterWithTransport(t, 3, 1)
		// node 2 carries junk from a deposed term-0 era leader
		setLog(c.Nodes[2], 0, putEntry(0, 99, 99))
		l := c.electLeader(t, 0)
		c.putOn(l, 4, 44)
		c.waitCommitted(t, allIDs(3), 1)
		require.Eventually(t, func() bool {
			return len(stateOf(c.Nodes[2]).Log) == 1 && stateOf(c.Nodes[2]).Log[0].Message.Key == 4
		}, 5*time.Second, 10*time.Millisecond, "follower log must be replaced by leader's")
		assert.Equal(t, 0, c.Nodes[2].application.(*testApp).getData()[99])
	})
}

// Regression: LogRequest/VoteRequest used to flip currentRole to Follower on a
// higher term without stopping the ex-leader's Replicator goroutines (they
// kept running, and were duplicated if the node won another election).
func TestStepDownViaLogRequestOrVoteStopsReplicators(t *testing.T) {
	t.Parallel()
	c := createClusterWithTransport(t, 3, 2)
	l := c.electLeader(t, 0)
	c.Nodes[0].mu.RLock()
	require.NotNil(t, c.Nodes[0].replicators)
	c.Nodes[0].mu.RUnlock()

	mustLogRequest(t, l, logRequestArgs(1, stateOf(l).Term+1, 0, 0, 0))

	assert.Equal(t, Follower, stateOf(l).Role)
	l.mu.RLock()
	defer l.mu.RUnlock()
	assert.Nil(t, l.replicators, "replicator goroutines of a deposed leader must be stopped")
}

func TestStepDownViaVoteRequestStopsReplicators(t *testing.T) {
	t.Parallel()
	c := createClusterWithTransport(t, 3, 2)
	l := c.electLeader(t, 0)
	vote(t, l, 1, stateOf(l).Term+1, 0, 0) // granted or not, the higher term deposes the leader
	assert.Equal(t, Follower, stateOf(l).Role)
	l.mu.RLock()
	defer l.mu.RUnlock()
	assert.Nil(t, l.replicators)
}
