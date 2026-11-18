package raft

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// restart simulates a crash + restart: a new Raft over the same files that
// loads (term, votedFor, commit, log) the same way NewRaft does.
func restart(t testing.TB, dir string) (*Raft, error) {
	t.Helper()
	n := createTestRaftIn(t, 1, 3, dir)
	term, votedFor, commit, log, err := n.logSaver.LoadValues()
	if err != nil {
		return n, err
	}
	n.mu.Lock()
	n.currentTerm, n.votedFor, n.commitedLength, n.log = term, votedFor, commit, log
	n.mu.Unlock()
	return n, nil
}

// waitDurable polls the files on disk (the RPC handlers persist asynchronously
// via `go logSaver.SaveValues()`, so there is no handle to wait on) until pred
// holds for the loaded state, or fails the test after timeout.
func waitDurable(t testing.TB, dir string, timeout time.Duration, pred func(term, votedFor, commit int, log []LogEntry) bool) bool {
	t.Helper()
	probe := createTestRaftIn(t, 1, 3, dir)
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		term, votedFor, commit, log, err := probe.logSaver.LoadValues()
		if err == nil && pred(term, votedFor, commit, log) {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return false
}

// Election safety across a restart: once a vote has been granted in a term it
// is durable, so the restarted node cannot vote for someone else in that term.
func TestRestart_NoDoubleVoteInSameTerm(t *testing.T) {
	t.Parallel()
	dir := testDataDir(t)
	n := createTestRaftIn(t, 1, 3, dir)
	setLog(n, 2, putEntry(2, 1, 1)) // non-empty log: the one case persistence handles
	require.True(t, vote(t, n, 0, 3, 1, 2).Granted)
	require.True(t, waitDurable(t, dir, 3*time.Second, func(term, votedFor, _ int, log []LogEntry) bool {
		// metadata and log are separate files written one after the other,
		// so wait for both halves
		return term == 3 && votedFor == 0 && len(log) == 1
	}), "granted vote never became durable")

	n2, err := restart(t, dir)
	require.NoError(t, err)
	st := stateOf(n2)
	assert.Equal(t, 3, st.Term)
	assert.Equal(t, 0, st.VotedFor)
	assert.Equal(t, []int{2}, termsOf(st.Log))

	assert.False(t, vote(t, n2, 2, 3, 1, 2).Granted, "must not vote twice in term 3 after restart")
	assert.True(t, vote(t, n2, 0, 3, 1, 2).Granted, "same candidate is still fine")
	assert.True(t, vote(t, n2, 2, 4, 1, 2).Granted, "a later term can vote again")
}

func TestRestart_FollowerKeepsAcknowledgedEntries(t *testing.T) {
	t.Parallel()
	dir := testDataDir(t)
	n := createTestRaftIn(t, 1, 3, dir)
	r := mustLogRequest(t, n, logRequestArgs(0, 2, 0, 0, 1, putEntry(2, 1, 11), putEntry(2, 2, 22)))
	require.True(t, r.Success)
	require.True(t, waitDurable(t, dir, 3*time.Second, func(_, _, commit int, log []LogEntry) bool {
		return commit == 1 && len(log) == 2
	}), "acknowledged entries never became durable")

	n2, err := restart(t, dir)
	require.NoError(t, err)
	st := stateOf(n2)
	assert.Equal(t, 2, st.Term)
	assert.Equal(t, 1, st.Commit)
	assert.Equal(t, []int{2, 2}, termsOf(st.Log), "entries acked to the leader must survive a crash")
}

// KNOWN BUG (see report): vote/term changes that do not append a log entry are
// never written (doSaveValues returns early when the log has nothing new, and
// when the log is empty no file is ever created), so a restarted node forgets
// its vote and can vote twice in one term.
func TestKnownBug_Restart_VoteWithoutNewLogEntriesIsLost(t *testing.T) {
	t.Parallel()
	dir := testDataDir(t)
	n := createTestRaftIn(t, 1, 3, dir)
	require.True(t, vote(t, n, 0, 3, 0, 0).Granted)
	durable := waitDurable(t, dir, 500*time.Millisecond, func(term, votedFor, _ int, _ []LogEntry) bool {
		return term == 3 && votedFor == 0
	})
	require.True(t, durable, "a node that granted a vote must have durable state")

	n2, err := restart(t, dir)
	require.NoError(t, err)
	assert.Equal(t, 3, stateOf(n2).Term)
	assert.False(t, vote(t, n2, 2, 3, 0, 0).Granted, "double vote in term 3 after restart")
}

// KNOWN BUG (see report): every save appends a *new* gob stream (new Encoder,
// O_APPEND) to the log file, so a file written by two saves cannot be decoded
// ("gob: duplicate type received") and the node cannot restart.
func TestKnownBug_Restart_AfterTwoIncrementalSaves(t *testing.T) {
	t.Parallel()
	dir := testDataDir(t)
	n := createTestRaftIn(t, 1, 3, dir)
	setLog(n, 1, putEntry(1, 1, 1), putEntry(1, 2, 2))
	n.logSaver.SaveValues()
	require.NoError(t, n.logSaver.WaitForPendingSaves())
	n.mu.Lock()
	n.log = append(n.log, putEntry(1, 3, 3))
	n.mu.Unlock()
	n.logSaver.SaveValues()
	require.NoError(t, n.logSaver.WaitForPendingSaves())

	n2, err := restart(t, dir)
	require.NoError(t, err, "log written by two saves must load")
	assert.Equal(t, []int{1, 1, 1}, termsOf(stateOf(n2).Log))
}
