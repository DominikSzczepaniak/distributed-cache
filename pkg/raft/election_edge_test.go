package raft

import (
	"context"
	"testing"
	"time"

	"github.com/dominikszczepaniak/distributed-cache/pkg/raft/raftpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/emptypb"
)

func vote(t testing.TB, n *Raft, cand, term, logLen, logTerm int) *raftpb.VoteResponse {
	t.Helper()
	resp, err := n.VoteRequest(context.Background(), &raftpb.VoteRequestArgs{
		CandidateId: int32(cand), CandidateTerm: int32(term),
		CandidateLogLength: int32(logLen), CandidateLogTerm: int32(logTerm),
	})
	require.NoError(t, err)
	return resp
}

// The voter's log is [t1, t1, t2] (3 entries, last term 2) unless noted.
func TestVoteRequest_LogUpToDateTable(t *testing.T) {
	t.Parallel()
	voterLog := []LogEntry{putEntry(1, 1, 1), putEntry(1, 2, 2), putEntry(2, 3, 3)}
	cases := []struct {
		name             string
		candLen, candTrm int
		granted          bool
	}{
		{"candidate_shorter_same_last_term_rejected", 2, 2, false},
		{"candidate_equal_log_granted", 3, 2, true},
		{"candidate_longer_same_last_term_granted", 5, 2, true},
		{"candidate_higher_last_term_but_shorter_granted", 1, 3, true},
		{"candidate_lower_last_term_but_longer_rejected", 9, 1, false},
		{"candidate_empty_log_rejected", 0, 0, false},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			n := createTestRaft(t, 1, 3)
			setLog(n, 2, voterLog...)
			resp := vote(t, n, 0, 3, tc.candLen, tc.candTrm)
			assert.Equal(t, tc.granted, resp.Granted)
			assert.EqualValues(t, 3, resp.CurrentTerm, "term is adopted even when the vote is refused")
			st := stateOf(n)
			assert.Equal(t, 3, st.Term)
			if tc.granted {
				assert.Equal(t, 0, st.VotedFor)
			} else {
				assert.Equal(t, -1, st.VotedFor, "no vote may be recorded for a stale candidate")
			}
		})
	}

	t.Run("snapshot_only_log_uses_snapshot_last_term", func(t *testing.T) {
		t.Parallel()
		n := createTestRaft(t, 1, 3)
		n.mu.Lock()
		n.currentTerm = 2
		n.snapshotter.lastIndex = 10
		n.snapshotter.lastTerm = 2
		n.mu.Unlock()
		assert.False(t, vote(t, n, 0, 3, 9, 2).Granted, "shorter than the snapshot-covered log")
		n2 := createTestRaft(t, 1, 3)
		n2.mu.Lock()
		n2.currentTerm = 2
		n2.snapshotter.lastIndex = 10
		n2.snapshotter.lastTerm = 2
		n2.mu.Unlock()
		assert.True(t, vote(t, n2, 0, 3, 10, 2).Granted)
	})
}

func TestVoteRequest_OneVotePerTerm(t *testing.T) {
	t.Parallel()
	n := createTestRaft(t, 2, 5)
	require.True(t, vote(t, n, 0, 4, 0, 0).Granted)
	assert.False(t, vote(t, n, 1, 4, 0, 0).Granted, "second candidate in the same term must be refused")
	assert.True(t, vote(t, n, 0, 4, 0, 0).Granted, "retransmitted request from the same candidate is idempotent")
	assert.Equal(t, 0, stateOf(n).VotedFor)
	assert.True(t, vote(t, n, 1, 5, 0, 0).Granted, "a new term resets the vote")
	assert.False(t, vote(t, n, 0, 4, 0, 0).Granted, "stale-term request refused")
	assert.Equal(t, 1, stateOf(n).VotedFor)
	assert.Equal(t, 5, stateOf(n).Term)
}

// startElectionTimer attaches a real election timer (default 400-800ms) to a
// node built by createTestRaft, which normally has its timer cancelled.
func startElectionTimer(t testing.TB, n *Raft) {
	t.Helper()
	n.mu.Lock()
	n.raftElector = NewRaftElector(n)
	el := n.raftElector
	n.mu.Unlock()
	t.Cleanup(func() { close(el.cancelTimerCh) })
}

func rejectingPeers(n int) []PeerClient {
	peers := make([]PeerClient, n)
	for i := range peers {
		m := &mockPeerClient{}
		m.On("VoteRequest", mock.Anything, mock.Anything).
			Return(&raftpb.VoteResponse{NodeId: int32(i), CurrentTerm: 0, Granted: false}, nil).Maybe()
		m.On("Heartbeat", mock.Anything, mock.Anything).Return(&emptypb.Empty{}, nil).Maybe()
		peers[i] = m
	}
	return peers
}

// Rewritten TestElectionTimeout: the timer is really running, the peers
// refuse to vote, and the assertions are exact (no `role == X || role == Y`).
func TestElectionTimeout(t *testing.T) {
	t.Parallel()
	node := createTestRaft(t, 0, 3)
	node.setPeers(rejectingPeers(3))
	assert.Equal(t, Follower, stateOf(node).Role)
	assert.Equal(t, 0, stateOf(node).Term)

	startElectionTimer(t, node)

	// No heartbeat arrives => within one max timeout (800ms) it becomes candidate
	require.Eventually(t, func() bool { return stateOf(node).Role == Candidate },
		3*time.Second, 5*time.Millisecond, "follower must start an election on timeout")
	st := stateOf(node)
	assert.Equal(t, 1, st.Term, "first election increments the term exactly once")
	assert.Equal(t, 0, st.VotedFor, "candidate votes for itself")

	// Nobody grants: it stays candidate and retries in a higher term.
	require.Eventually(t, func() bool { return stateOf(node).Term >= 2 },
		3*time.Second, 5*time.Millisecond, "candidate must time out and re-run the election")
	assert.Equal(t, Candidate, stateOf(node).Role)
}

func TestElectionTimer_ResetByHeartbeatsThenFiresWhenTheyStop(t *testing.T) {
	t.Parallel()
	node := createTestRaft(t, 0, 3)
	node.setPeers(rejectingPeers(3))
	startElectionTimer(t, node)

	// Heartbeats every 100ms (<< 400ms minimum timeout) keep the node a follower.
	stop := time.After(1500 * time.Millisecond) // > 1 max election timeout
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
loop:
	for {
		select {
		case <-tick.C:
			_, err := node.Heartbeat(context.Background(), &emptypb.Empty{})
			require.NoError(t, err)
			require.Equal(t, Follower, stateOf(node).Role, "heartbeats must suppress elections")
			require.Equal(t, 0, stateOf(node).Term)
		case <-stop:
			break loop
		}
	}
	// Heartbeats stop => election timeout fires.
	require.Eventually(t, func() bool { return stateOf(node).Role == Candidate },
		3*time.Second, 5*time.Millisecond)
}

func TestElection_RealVotesThroughNetwork_LeaderHasMajority(t *testing.T) {
	c := createClusterWithTransport(t, 5, 1)
	c.Net.Partition([]int{4}, []int{0, 1, 2, 3}) // one node unreachable: 4/5 still a quorum
	l := c.electLeader(t, 0)
	st := stateOf(l)
	assert.Equal(t, 1, st.Term)
	assert.Equal(t, 0, st.Leader)
	// everyone reachable adopted the term and voted for node 0
	for _, id := range []int{1, 2, 3} {
		s := stateOf(c.Nodes[id])
		assert.Equal(t, 1, s.Term)
		assert.Equal(t, 0, s.VotedFor)
	}
}

func TestElection_NoMajority_NoLeader(t *testing.T) {
	c := createClusterWithTransport(t, 5, 1)
	c.Net.Partition([]int{0, 1}, []int{2, 3, 4})
	c.Nodes[0].startElection()
	require.Eventually(t, func() bool { return stateOf(c.Nodes[1]).VotedFor == 0 }, 5*time.Second, 5*time.Millisecond)
	require.Never(t, func() bool { return c.Nodes[0].getCurrentRole() == Leader },
		300*time.Millisecond, 20*time.Millisecond, "2 of 5 votes must not elect a leader")
	assert.Equal(t, Candidate, stateOf(c.Nodes[0]).Role)
}
