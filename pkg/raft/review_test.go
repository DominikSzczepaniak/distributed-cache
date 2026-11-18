package raft

// Tests derived from the code review of split/02-raft-fixes-edge-tests.
//
// Naming convention: tests that assert the CORRECT behaviour of a problem that
// is still present in the production code are named TestKnownBug_*. They fail
// today on purpose and are excluded from the required CI gate with
// `-skip TestKnownBug` (see docs/KNOWN_BUGS.md). Tests without that prefix
// pin down behaviour that is already correct (coverage the review found
// missing).
//
// Review item numbers in the comments refer to the review text.

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dominikszczepaniak/distributed-cache/pkg/raft/raftpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/emptypb"
)

// funcPeer is a scriptable PeerClient. Unset hooks answer with a harmless
// default; methods not listed here panic on the nil embedded interface.
type funcPeer struct {
	PeerClient
	logReq func(*raftpb.LogRequestArgs) (*raftpb.LogResponse, error)
	snap   func(*raftpb.InstallSnapshotRequest) (*raftpb.InstallSnapshotResponse, error)
}

func (p *funcPeer) LogRequest(_ context.Context, in *raftpb.LogRequestArgs) (*raftpb.LogResponse, error) {
	if p.logReq != nil {
		return p.logReq(in)
	}
	return &raftpb.LogResponse{}, nil
}

func (p *funcPeer) InstallSnapshot(_ context.Context, in *raftpb.InstallSnapshotRequest) (*raftpb.InstallSnapshotResponse, error) {
	if p.snap != nil {
		return p.snap(in)
	}
	return &raftpb.InstallSnapshotResponse{}, nil
}

func (p *funcPeer) Heartbeat(context.Context, *emptypb.Empty) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, nil
}

func (p *funcPeer) VoteRequest(context.Context, *raftpb.VoteRequestArgs) (*raftpb.VoteResponse, error) {
	return &raftpb.VoteResponse{}, nil
}

// leaderWithSnapshot builds a term-2 leader (node 0 of 3) whose snapshot file
// holds `keys` entries and whose snapshot covers [1..lastIdx].
func leaderWithSnapshot(t testing.TB, keys, lastIdx, lastTerm int) (*Raft, []byte) {
	t.Helper()
	l := makeLeader(t, 0, 3, 2)
	data := snapshotOf(t, manyKeys(keys))
	_, err := l.logSaver.WriteSnapshotData(data, 0)
	require.NoError(t, err)
	l.mu.Lock()
	l.snapshotter.lastIndex = lastIdx
	l.snapshotter.lastTerm = lastTerm
	l.commitedLength = lastIdx
	l.mu.Unlock()
	return l, data
}

// ---------------------------------------------------------------------------
// Review item 2: a delayed snapshot must not roll back the state machine
// ---------------------------------------------------------------------------

func TestKnownBug_StaleInstallSnapshotDoesNotRollBackStateMachine(t *testing.T) {
	t.Parallel()
	f := createTestRaft(t, 1, 3)
	r := mustLogRequest(t, f, logRequestArgs(0, 2, 0, 0, 4,
		putEntry(1, 1, 1), putEntry(1, 2, 2), putEntry(2, 3, 3), putEntry(2, 4, 4)))
	require.True(t, r.Success)
	require.Equal(t, 4, stateOf(f).Commit)
	require.Equal(t, 3, f.application.(*testApp).getData()[3])

	// delayed/duplicate snapshot that only covers entries 1..2
	for _, req := range chunks(snapshotOf(t, map[int]int{1: 1, 2: 2}), 1<<20, 2, 0, 2, 1) {
		install(t, f, req)
	}

	d := f.application.(*testApp).getData()
	assert.Equal(t, 3, d[3], "committed+applied entry 3 lost by a stale snapshot")
	assert.Equal(t, 4, d[4], "committed+applied entry 4 lost by a stale snapshot")
	assert.Equal(t, 4, stateOf(f).Commit, "commit index must never go backwards")
}

// Same scenario, other direction: a snapshot AT the commit index is also a no-op.
func TestKnownBug_InstallSnapshotAtCommitIndexIsIgnored(t *testing.T) {
	t.Parallel()
	f := createTestRaft(t, 1, 3)
	require.True(t, mustLogRequest(t, f, logRequestArgs(0, 2, 0, 0, 2,
		putEntry(1, 1, 1), putEntry(1, 2, 2))).Success)
	// same index, but a (bogus) different content: must not replace our state
	for _, req := range chunks(snapshotOf(t, map[int]int{1: 100, 2: 200}), 1<<20, 2, 0, 2, 1) {
		install(t, f, req)
	}
	assert.Equal(t, map[int]int{1: 1, 2: 2}, f.application.(*testApp).getData())
}

// ---------------------------------------------------------------------------
// Review item 3: truncation must reach the disk, and before the ack
// ---------------------------------------------------------------------------

func TestKnownBug_TruncatedEntriesArePersisted(t *testing.T) {
	t.Parallel()
	dir := testDataDir(t)
	n := createTestRaftIn(t, 1, 3, dir)
	require.True(t, mustLogRequest(t, n, logRequestArgs(0, 1, 0, 0, 0,
		putEntry(1, 1, 1), putEntry(1, 2, 2), putEntry(1, 3, 3))).Success)
	require.True(t, waitDurable(t, dir, 3*time.Second, func(_, _, _ int, l []LogEntry) bool { return len(l) == 3 }))

	// new leader (term 2) overwrites entries 2..3 with a same-length suffix
	require.True(t, mustLogRequest(t, n, logRequestArgs(0, 2, 1, 1, 0,
		putEntry(2, 20, 20), putEntry(2, 30, 30))).Success)
	require.Equal(t, []int{1, 2, 2}, termsOf(stateOf(n).Log))
	n.logSaver.SaveValues()
	_ = n.logSaver.WaitForPendingSaves()

	n2, err := restart(t, dir)
	require.NoError(t, err)
	assert.Equal(t, []int{1, 2, 2}, termsOf(stateOf(n2).Log), "acknowledged term-2 entries missing after restart")
}

func TestKnownBug_LongerReplacementAfterTruncationIsPersistedWithoutMixing(t *testing.T) {
	t.Parallel()
	dir := testDataDir(t)
	n := createTestRaftIn(t, 1, 3, dir)
	require.True(t, mustLogRequest(t, n, logRequestArgs(0, 1, 0, 0, 0,
		putEntry(1, 1, 1), putEntry(1, 2, 2))).Success)
	require.True(t, waitDurable(t, dir, 3*time.Second, func(_, _, _ int, l []LogEntry) bool { return len(l) == 2 }))

	// longer replacement from index 2 on
	require.True(t, mustLogRequest(t, n, logRequestArgs(0, 2, 1, 1, 0,
		putEntry(2, 20, 20), putEntry(2, 30, 30), putEntry(2, 40, 40))).Success)
	n.logSaver.SaveValues()
	_ = n.logSaver.WaitForPendingSaves()

	n2, err := restart(t, dir)
	require.NoError(t, err)
	st := stateOf(n2)
	assert.Equal(t, []int{1, 2, 2, 2}, termsOf(st.Log), "file must not mix old and new entries")
}

// The ack is only valid once the entries are durable: right after LogRequest
// returns, the files must already contain them (no waiting, no polling).
func TestKnownBug_LogRequestPersistsBeforeReply(t *testing.T) {
	t.Parallel()
	dir := testDataDir(t)
	n := createTestRaftIn(t, 1, 3, dir)
	r := mustLogRequest(t, n, logRequestArgs(0, 2, 0, 0, 0, putEntry(2, 1, 11), putEntry(2, 2, 22)))
	require.True(t, r.Success)

	probe := createTestRaftIn(t, 1, 3, dir)
	term, _, _, log, err := probe.logSaver.LoadValues()
	require.NoError(t, err)
	assert.Equal(t, 2, term, "term must be durable before replying")
	assert.Len(t, log, 2, "acknowledged entries must be durable before replying")
}

// ---------------------------------------------------------------------------
// Review item 4: every step-down path stops the replicators; re-election
// does not leak the old ones
// ---------------------------------------------------------------------------

func replicatorsOf(r *Raft) []*Replicator {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return append([]*Replicator(nil), r.replicators...)
}

func isStopped(rep *Replicator) bool {
	select {
	case <-rep.stopCh:
		return true
	default:
		return false
	}
}

func TestKnownBug_EveryStepDownPathStopsReplicators(t *testing.T) {
	t.Parallel()
	paths := []struct {
		name    string
		trigger func(t *testing.T, c *TestCluster, l *Raft, term int)
	}{
		{"LogRequest", func(t *testing.T, _ *TestCluster, l *Raft, term int) {
			mustLogRequest(t, l, logRequestArgs(1, term+1, 0, 0, 0))
		}},
		{"VoteRequest", func(t *testing.T, _ *TestCluster, l *Raft, term int) {
			vote(t, l, 1, term+1, 0, 0)
		}},
		{"late_vote_reply_with_higher_term", func(t *testing.T, _ *TestCluster, l *Raft, term int) {
			l.receiveVote(VoteResponse{nodeId: 2, currentTerm: term + 1})
		}},
		{"logResponse_with_higher_term", func(t *testing.T, _ *TestCluster, l *Raft, term int) {
			l.logResponse(1, term+1, 0, false)
		}},
		{"InstallSnapshot_from_higher_term", func(t *testing.T, _ *TestCluster, l *Raft, term int) {
			install(t, l, chunks(snapshotOf(t, map[int]int{}), 1<<20, term+1, 1, 0, 0)[0])
		}},
		{"sendInstallSnapshotRPC_reply_with_higher_term", func(t *testing.T, _ *TestCluster, l *Raft, term int) {
			_, err := l.logSaver.WriteSnapshotData(snapshotOf(t, manyKeys(3)), 0)
			require.NoError(t, err)
			l.mu.Lock()
			l.snapshotter.lastIndex = 5
			l.snapshotter.lastTerm = term
			peers := []PeerClient{nil, &funcPeer{snap: func(*raftpb.InstallSnapshotRequest) (*raftpb.InstallSnapshotResponse, error) {
				return &raftpb.InstallSnapshotResponse{Term: int32(term + 3)}, nil
			}}, nil}
			l.setPeers(peers)
			l.mu.Unlock()
			l.sendInstallSnapshotRPC(1)
		}},
	}
	for _, p := range paths {
		p := p
		t.Run(p.name, func(t *testing.T) {
			t.Parallel()
			c := createClusterWithTransport(t, 3, 2)
			l := c.electLeader(t, 0)
			old := replicatorsOf(l)
			require.NotEmpty(t, old)
			term := stateOf(l).Term

			p.trigger(t, c, l, term)

			require.Equal(t, Follower, l.getCurrentRole(), "trigger must depose the leader")
			l.mu.RLock()
			assert.Nil(t, l.replicators, "replicators still referenced after step-down")
			l.mu.RUnlock()
			for i, rep := range old {
				if rep != nil {
					assert.True(t, isStopped(rep), "replicator %d still running after step-down", i)
				}
			}
		})
	}
}

func TestKnownBug_ReElectionStopsPreviousReplicators(t *testing.T) {
	t.Parallel()
	c := createClusterWithTransport(t, 3, 2)
	l := c.electLeader(t, 0)
	old := replicatorsOf(l)
	term := stateOf(l).Term

	// step down through a path that (today) leaves replicators running ...
	l.receiveVote(VoteResponse{nodeId: 2, currentTerm: term + 1})
	require.Equal(t, Follower, l.getCurrentRole())
	// ... and win the next election
	l.mu.Lock()
	l.currentTerm = term + 2
	l.becomeLeader()
	l.mu.Unlock()

	for i, rep := range old {
		if rep != nil {
			assert.True(t, isStopped(rep), "replicator %d of the previous leadership still running after re-election", i)
		}
	}
}

func TestKnownBug_ReplicatorStopIsIdempotent(t *testing.T) {
	t.Parallel()
	l := makeLeader(t, 0, 3, 1)
	rep := NewReplicator(l, 1)
	assert.NotPanics(t, func() {
		rep.stop()
		rep.stop() // two step-down paths can race to stop the same replicator
	})
}

// ---------------------------------------------------------------------------
// Review item 5: check+append in LogRequest is not atomic
// (INFERRED by the reviewer; probabilistic stress test, may pass on a lucky run)
// ---------------------------------------------------------------------------

func TestKnownBug_ConcurrentLogRequestsAcrossTermsDoNotTruncateAckedEntries(t *testing.T) {
	f := createTestRaft(t, 1, 3)
	const iterations = 3000
	violations := 0
	for i := 0; i < iterations; i++ {
		stale := 10*i + 5 // the old leader's term; the follower is still in it
		setLog(f, stale)
		f.mu.Lock()
		f.votedFor = -1
		f.commitedLength = 0
		f.mu.Unlock()

		var wg sync.WaitGroup
		var newOK atomic.Bool
		start := make(chan struct{})
		wg.Add(2)
		go func() { // delayed request from the deposed leader
			defer wg.Done()
			<-start
			_, _ = f.LogRequest(context.Background(), logRequestArgs(0, stale, 0, 0, 0, putEntry(stale, 1, 1)))
		}()
		go func() { // the new leader's request
			defer wg.Done()
			<-start
			resp, err := f.LogRequest(context.Background(), logRequestArgs(2, stale+1, 0, 0, 0,
				putEntry(stale+1, 2, 2), putEntry(stale+1, 3, 3)))
			newOK.Store(err == nil && resp.Success)
		}()
		close(start)
		wg.Wait()

		if newOK.Load() {
			terms := termsOf(stateOf(f).Log)
			if len(terms) != 2 || terms[0] != stale+1 || terms[1] != stale+1 {
				violations++
			}
		}
	}
	assert.Zero(t, violations,
		"%d/%d times the new leader's acknowledged entries were truncated by a delayed old-term request", violations, iterations)
}

// ---------------------------------------------------------------------------
// Review item 6: snapshot transfers
// ---------------------------------------------------------------------------

// Only one InstallSnapshot transfer per follower may be in flight: each
// transfer's first chunk truncates the follower's snapshot file.
func TestKnownBug_OnlyOneSnapshotTransferPerFollowerAtATime(t *testing.T) {
	t.Parallel()
	l, _ := leaderWithSnapshot(t, 10, 10, 1)
	var mu sync.Mutex
	inflight, maxInflight := 0, 0
	release := make(chan struct{})
	l.setPeers([]PeerClient{nil, &funcPeer{snap: func(*raftpb.InstallSnapshotRequest) (*raftpb.InstallSnapshotResponse, error) {
		mu.Lock()
		inflight++
		if inflight > maxInflight {
			maxInflight = inflight
		}
		mu.Unlock()
		<-release
		mu.Lock()
		inflight--
		mu.Unlock()
		return &raftpb.InstallSnapshotResponse{Term: 2}, nil
	}}, nil})

	var wg sync.WaitGroup
	for i := 0; i < 2; i++ { // e.g. the replicator tick and prepareLogRequestArgs' goroutine
		wg.Add(1)
		go func() { defer wg.Done(); l.sendInstallSnapshotRPC(1) }()
	}
	require.Eventually(t, func() bool { mu.Lock(); defer mu.Unlock(); return inflight >= 1 }, 3*time.Second, time.Millisecond)
	time.Sleep(200 * time.Millisecond) // give a second transfer the chance to start
	mu.Lock()
	got := maxInflight
	mu.Unlock()
	close(release)
	wg.Wait()
	assert.Equal(t, 1, got, "two snapshot transfers ran concurrently to the same follower")
}

// prepareLogRequestArgs must not start a second transfer in the background
// (the replicator already handles the needsSnapshot case synchronously) and
// must not hand back an empty AppendEntries that the follower acks at index 0.
func TestKnownBug_PrepareLogRequestDoesNotSpawnSnapshotTransfer(t *testing.T) {
	t.Parallel()
	l, _ := leaderWithSnapshot(t, 10, 10, 1)
	var calls atomic.Int32
	l.setPeers([]PeerClient{nil, &funcPeer{snap: func(*raftpb.InstallSnapshotRequest) (*raftpb.InstallSnapshotResponse, error) {
		calls.Add(1)
		return &raftpb.InstallSnapshotResponse{Term: 2}, nil
	}}, nil})
	l.mu.Lock()
	l.sentLengths[1] = 5 // <= lastIndex => needs a snapshot
	l.mu.Unlock()

	args := l.prepareLogRequestArgs(1)

	assert.Never(t, func() bool { return calls.Load() > 0 }, 300*time.Millisecond, 10*time.Millisecond,
		"prepareLogRequestArgs started a background snapshot transfer")
	assert.NotNil(t, args)
}

// The leader taking a new snapshot (file truncated and rewritten in place)
// while a transfer of the old one is in progress must not make the follower
// receive a mix of both.
func TestKnownBug_SnapshotTransferIsConsistentWhileLeaderSnapshotsAgain(t *testing.T) {
	t.Parallel()
	l, oldSnapshot := leaderWithSnapshot(t, 40_000, 10, 1)
	require.Greater(t, len(oldSnapshot), 3*128*1024, "needs a multi-chunk transfer")

	var mu sync.Mutex
	var received []byte
	var lastIdx int32
	first := true
	l.setPeers([]PeerClient{nil, &funcPeer{snap: func(req *raftpb.InstallSnapshotRequest) (*raftpb.InstallSnapshotResponse, error) {
		mu.Lock()
		defer mu.Unlock()
		if first {
			first = false
			// while chunk 0 is "on the wire" the leader snapshots again
			l.mu.Lock()
			app := l.application.(*testApp)
			app.mu.Lock()
			app.data = make(map[int]int, 40_000)
			for k := 1; k <= 40_000; k++ {
				app.data[k] = k*7 + 1
			}
			app.mu.Unlock()
			l.snapshotter.snapshotThreshold = 1
			l.log = []LogEntry{putEntry(2, 1, 1), putEntry(2, 2, 2), putEntry(2, 3, 3)}
			l.commitedLength = l.snapshotter.lastIndex + 3
			require.NoError(t, l.decideRunSnapshot())
			l.mu.Unlock()
		}
		if int(req.Offset) != len(received) {
			t.Errorf("unexpected offset %d, have %d bytes", req.Offset, len(received))
		}
		received = append(received, req.Data...)
		lastIdx = req.LastIncludedIndex
		return &raftpb.InstallSnapshotResponse{Term: 2}, nil
	}}, nil})

	l.sendInstallSnapshotRPC(1)

	mu.Lock()
	defer mu.Unlock()
	require.EqualValues(t, 10, lastIdx, "transfer was announced for the old snapshot (index 10)")
	assert.Equal(t, oldSnapshot, received,
		"follower received bytes that are not the snapshot announced by LastIncludedIndex")
}

// The rejection handler must not decrement blindly.
func TestKnownBug_RejectionHandlerDoesNotOvershootNextIndex(t *testing.T) {
	t.Parallel()
	build := func(t *testing.T, sentLength int, onRequest func(l *Raft)) (*Raft, *Replicator) {
		l := makeLeader(t, 0, 3, 2)
		l.mu.Lock()
		for i := 1; i <= 8; i++ {
			l.log = append(l.log, putEntry(2, i, i))
		}
		l.sentLengths[1] = sentLength
		l.mu.Unlock()
		l.setPeers([]PeerClient{nil, &funcPeer{logReq: func(*raftpb.LogRequestArgs) (*raftpb.LogResponse, error) {
			if onRequest != nil {
				onRequest(l)
			}
			return &raftpb.LogResponse{NodeId: 1, CurrentTerm: 2, Success: false}, nil
		}}, nil})
		return l, NewReplicator(l, 1)
	}

	t.Run("never_reaches_zero_which_means_uninitialised", func(t *testing.T) {
		l, rep := build(t, 1, nil)
		rep.replicate()
		l.mu.RLock()
		defer l.mu.RUnlock()
		assert.GreaterOrEqual(t, l.sentLengths[1], 1,
			"sentLengths==0 is the 'uninitialised' marker and makes the leader jump to the end of its log")
	})

	t.Run("rejection_of_an_older_request_does_not_undo_progress", func(t *testing.T) {
		// request was built for nextIndex 5; meanwhile the follower was pushed back to 3
		l, rep := build(t, 5, func(l *Raft) {
			l.mu.Lock()
			l.sentLengths[1] = 3
			l.mu.Unlock()
		})
		rep.replicate()
		l.mu.RLock()
		defer l.mu.RUnlock()
		assert.Equal(t, 3, l.sentLengths[1], "must become min(current, requestNextIndex-1), not current-1")
	})
}

// ---------------------------------------------------------------------------
// Review item 6 (medium): term/vote change in InstallSnapshot must be durable
// ---------------------------------------------------------------------------

func TestKnownBug_InstallSnapshotHigherTermIsDurable(t *testing.T) {
	t.Parallel()
	dir := testDataDir(t)
	n := createTestRaftIn(t, 1, 3, dir)
	setLog(n, 1, putEntry(1, 1, 1))
	n.mu.Lock()
	n.votedFor = 2
	n.mu.Unlock()
	n.logSaver.SaveValues()
	require.NoError(t, n.logSaver.WaitForPendingSaves())
	require.True(t, waitDurable(t, dir, 3*time.Second, func(term, vote, _ int, _ []LogEntry) bool { return term == 1 && vote == 2 }))

	for _, r := range chunks(snapshotOf(t, manyKeys(3)), 1<<20, 3, 0, 5, 3) {
		install(t, n, r)
	}

	assert.True(t, waitDurable(t, dir, time.Second, func(term, vote, _ int, _ []LogEntry) bool {
		return term == 3 && vote == -1
	}), "term 3 / cleared vote from InstallSnapshot was never persisted: a restart could vote twice in term 3")
}

// ---------------------------------------------------------------------------
// Review: low severity
// ---------------------------------------------------------------------------

// Raft: leaderCommit is capped at the index of the LAST NEW ENTRY of this
// request, not at the end of the follower's log (which may still hold
// unverified entries from a deposed leader).
func TestKnownBug_CommitIsCappedAtLastNewEntryOfTheRequest(t *testing.T) {
	t.Parallel()
	f := createTestRaft(t, 1, 3)
	setLog(f, 1, putEntry(1, 1, 1), putEntry(1, 2, 2), putEntry(1, 3, 3))

	// the leader's request only vouches for entry 1
	r := mustLogRequest(t, f, logRequestArgs(0, 1, 0, 0, 3, putEntry(1, 1, 1)))
	require.True(t, r.Success)

	assert.LessOrEqual(t, stateOf(f).Commit, 1, "entries 2..3 were never verified against this leader")
	data := f.application.(*testApp).getData()
	_, has2 := data[2]
	_, has3 := data[3]
	assert.False(t, has2 || has3, "unverified entries must not be applied to the state machine")
}

func TestKnownBug_InstallSnapshotInSameTermMakesCandidateStepDown(t *testing.T) {
	t.Parallel()
	n := createTestRaft(t, 1, 3)
	n.mu.Lock()
	n.currentTerm = 3
	n.currentRole = Candidate
	n.votedFor = 1
	n.mu.Unlock()

	install(t, n, chunks(snapshotOf(t, manyKeys(2)), 1<<20, 3, 0, 2, 3)[0])

	assert.Equal(t, Follower, stateOf(n).Role, "a leader exists for this term, the candidate must step down")
}

func TestKnownBug_StaleInstallSnapshotDoesNotResetElectionTimer(t *testing.T) {
	t.Parallel()
	node := createTestRaft(t, 0, 3)
	node.setPeers(rejectingPeers(3))
	node.mu.Lock()
	node.currentTerm = 5
	node.mu.Unlock()
	startElectionTimer(t, node)

	stop := make(chan struct{})
	done := make(chan struct{})
	go func() { // an outdated leader (term 3) keeps sending snapshots
		defer close(done)
		tick := time.NewTicker(100 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-stop:
				return
			case <-tick.C:
				_, _ = node.InstallSnapshot(context.Background(), &raftpb.InstallSnapshotRequest{LeaderTerm: 3, LeaderId: 2})
			}
		}
	}()
	defer func() { close(stop); <-done }()

	assert.Eventually(t, func() bool { return stateOf(node).Role == Candidate }, 3*time.Second, 10*time.Millisecond,
		"a stale leader must not be able to postpone elections")
}

// ---------------------------------------------------------------------------
// Review: performance
// ---------------------------------------------------------------------------

func TestKnownBug_LeaderCapsEntriesPerLogRequest(t *testing.T) {
	t.Parallel()
	const maxPerRequest = 1000 // bound chosen by this test; adjust together with the production constant
	l := makeLeader(t, 0, 3, 2)
	l.mu.Lock()
	for i := 1; i <= 5000; i++ {
		l.log = append(l.log, putEntry(2, i, i))
	}
	l.sentLengths[1] = 1
	l.mu.Unlock()

	args := l.prepareLogRequestArgs(1)
	assert.LessOrEqual(t, len(args.Suffix), maxPerRequest, "one AppendEntries must not carry the whole log")
}

// Repairing a follower whose log diverges from index 1 must not cost one round
// trip per diverging entry (needs a conflict hint in LogResponse, i.e. a proto
// change - backward compatible if the fields are optional).
func TestKnownBug_DivergentFollowerRepairNeedsFewRoundTrips(t *testing.T) {
	t.Parallel()
	const k = 60
	l := makeLeader(t, 0, 3, 2)
	f := createTestRaft(t, 1, 3)
	var leaderLog, junk []LogEntry
	for i := 1; i <= k; i++ {
		leaderLog = append(leaderLog, putEntry(2, i, i))
		junk = append(junk, putEntry(1, 1000+i, i))
	}
	l.mu.Lock()
	l.log = leaderLog
	l.sentLengths[1] = k + 1
	l.mu.Unlock()
	setLog(f, 1, junk...)

	var rpcs int
	l.setPeers([]PeerClient{nil, &funcPeer{logReq: func(in *raftpb.LogRequestArgs) (*raftpb.LogResponse, error) {
		rpcs++
		return f.LogRequest(context.Background(), in)
	}}, nil})
	rep := NewReplicator(l, 1)
	for i := 0; i < k+5; i++ {
		rep.replicate()
		if got := stateOf(f).Log; len(got) == k && got[0].Term == 2 {
			break
		}
	}

	require.Equal(t, termsOf(leaderLog), termsOf(stateOf(f).Log), "follower must eventually be repaired")
	assert.LessOrEqual(t, rpcs, 10, "repair took %d round trips for %d diverging entries", rpcs, k)
}

// InstallSnapshot must not hold the node lock while it reads the snapshot file
// and restores the state machine: LogRequest/VoteRequest stall behind it.
func TestKnownBug_InstallSnapshotDoesNotBlockLogRequestDuringRestore(t *testing.T) {
	f := createTestRaft(t, 1, 3)
	reqs := chunks(snapshotOf(t, manyKeys(500_000)), 128*1024, 2, 0, 100, 2)
	for _, r := range reqs[:len(reqs)-1] {
		install(t, f, r)
	}
	last := reqs[len(reqs)-1]

	var installDur atomic.Int64
	done := make(chan struct{})
	go func() {
		defer close(done)
		start := time.Now()
		_, _ = f.InstallSnapshot(context.Background(), last)
		installDur.Store(int64(time.Since(start)))
	}()

	var maxLatency time.Duration
	for {
		select {
		case <-done:
			dur := time.Duration(installDur.Load())
			require.Greater(t, dur, 20*time.Millisecond, "restore too fast to measure on this machine")
			assert.Less(t, maxLatency, dur/4,
				"LogRequest waited %v while the final InstallSnapshot chunk took %v", maxLatency, dur)
			return
		default:
		}
		start := time.Now()
		_, _ = f.LogRequest(context.Background(), logRequestArgs(0, 1, 0, 0, 0)) // stale term: rejected immediately
		if d := time.Since(start); d > maxLatency {
			maxLatency = d
		}
		time.Sleep(200 * time.Microsecond)
	}
}

// ---------------------------------------------------------------------------
// Review: missing tests that pin down behaviour which is already correct
// ---------------------------------------------------------------------------

// A non-empty follower whose log diverges right after the leader's snapshot
// point is repaired through the normal replicator path.
func TestFollowerDivergingRightAfterLeaderSnapshotPointIsRepaired(t *testing.T) {
	t.Parallel()
	l := makeLeader(t, 0, 3, 2)
	f := createTestRaft(t, 1, 3)

	l.mu.Lock()
	l.snapshotter.lastIndex = 3
	l.snapshotter.lastTerm = 1
	l.log = []LogEntry{putEntry(2, 4, 4), putEntry(2, 5, 5)}
	l.commitedLength = 3
	l.sentLengths[1] = 6
	l.mu.Unlock()
	// follower agrees up to index 3, then holds uncommitted junk of a deposed term-1 leader
	setLog(f, 1, putEntry(1, 1, 1), putEntry(1, 2, 2), putEntry(1, 3, 3), putEntry(1, 40, 40), putEntry(1, 50, 50))
	l.setPeers([]PeerClient{nil, &inMemPeer{f}, nil})

	rep := NewReplicator(l, 1)
	for i := 0; i < 8; i++ {
		rep.replicate()
	}

	st := stateOf(f)
	assert.Equal(t, []int{1, 1, 1, 2, 2}, termsOf(st.Log))
	assert.Equal(t, 4, st.Log[3].Message.Key)
	assert.Equal(t, 5, st.Log[4].Message.Key)
	assert.Equal(t, 5, stateOf(l).Commit, "leader commits once the follower acknowledged 4..5")
}

// A follower that has a snapshot and receives a request whose prevIndex lies
// inside that snapshot (rel < 0 in appendEntries). Those entries are committed
// and identical on every correct node, so the request can be acknowledged by
// skipping the covered part instead of being rejected (and leaving the leader
// to back off towards a stale InstallSnapshot).
func TestKnownBug_LogRequestWithPrevIndexInsideFollowerSnapshotIsAccepted(t *testing.T) {
	t.Parallel()
	f := createTestRaft(t, 1, 3)
	f.mu.Lock()
	f.currentTerm = 2
	f.snapshotter.lastIndex = 5
	f.snapshotter.lastTerm = 1
	f.commitedLength = 5
	f.log = []LogEntry{putEntry(2, 6, 6)}
	f.mu.Unlock()

	r := mustLogRequest(t, f, logRequestArgs(0, 2, 2, 1, 6,
		putEntry(1, 3, 3), putEntry(1, 4, 4), putEntry(1, 5, 5), putEntry(2, 6, 6)))

	assert.True(t, r.Success, "entries covered by the snapshot are already committed here")
	assert.EqualValues(t, 6, r.Ack)
	assert.Equal(t, []int{2}, termsOf(stateOf(f).Log), "log after the snapshot is unchanged")
	assert.Equal(t, 6, stateOf(f).LastIndex)
}

// ---------------------------------------------------------------------------
// Review: logging cost on hot paths (benchmark, not a test: allocations are
// machine/Go-version dependent so there is no stable threshold to assert)
// ---------------------------------------------------------------------------

func BenchmarkLogRequestHeartbeat(b *testing.B) {
	f := createTestRaft(b, 1, 3)
	req := logRequestArgs(0, 1, 0, 0, 0)
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := f.LogRequest(ctx, req); err != nil {
			b.Fatal(err)
		}
	}
}
