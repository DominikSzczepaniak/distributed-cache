package raft

import (
	"context"
	"testing"
	"time"

	"github.com/dominikszczepaniak/distributed-cache/pkg/raft/raftpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// snapshotOf serialises a key/value map with the test state machine's format.
func snapshotOf(t testing.TB, kv map[int]int) []byte {
	t.Helper()
	app := newTestApp()
	for k, v := range kv {
		app.data[k] = v
	}
	b, err := app.GetSnapshot()
	require.NoError(t, err)
	return b
}

func manyKeys(n int) map[int]int {
	m := make(map[int]int, n)
	for i := 1; i <= n; i++ {
		m[i] = i * 7
	}
	return m
}

// chunks splits data into size-byte InstallSnapshot requests (Done on the last).
func chunks(data []byte, size, term, leader, lastIdx, lastTerm int) []*raftpb.InstallSnapshotRequest {
	var out []*raftpb.InstallSnapshotRequest
	for off := 0; off < len(data); off += size {
		end := off + size
		if end > len(data) {
			end = len(data)
		}
		out = append(out, &raftpb.InstallSnapshotRequest{
			LeaderTerm: int32(term), LeaderId: int32(leader),
			LastIncludedIndex: int32(lastIdx), LastIncludedTerm: int32(lastTerm),
			Offset: int32(off), Data: append([]byte(nil), data[off:end]...), Done: end == len(data),
		})
	}
	return out
}

func install(t testing.TB, n *Raft, req *raftpb.InstallSnapshotRequest) *raftpb.InstallSnapshotResponse {
	t.Helper()
	resp, err := n.InstallSnapshot(context.Background(), req)
	require.NoError(t, err)
	return resp
}

func TestInstallSnapshot_StaleTermRejected(t *testing.T) {
	t.Parallel()
	f := createTestRaft(t, 1, 3)
	f.application.(*testApp).data[1] = 111
	setLog(f, 5, putEntry(5, 1, 111))

	req := chunks(snapshotOf(t, map[int]int{9: 999}), 1<<20, 3, 0, 20, 3)[0]
	resp := install(t, f, req)

	assert.EqualValues(t, 5, resp.Term, "reply carries the follower's newer term")
	assert.Equal(t, map[int]int{1: 111}, f.application.(*testApp).getData(), "state machine untouched")
	st := stateOf(f)
	assert.Equal(t, 5, st.Term)
	assert.Equal(t, 1, len(st.Log), "log untouched")
	assert.Equal(t, 0, st.LastIndex-len(st.Log), "snapshot index untouched")
	data, err := f.logSaver.ReadSnapshotData()
	require.NoError(t, err)
	assert.Empty(t, data, "a rejected snapshot must not be written to disk")
}

func TestInstallSnapshot_MultiChunk(t *testing.T) {
	t.Parallel()
	f := createTestRaft(t, 1, 3)
	// stale local state that the snapshot must replace
	f.application.(*testApp).data[500] = 5
	setLog(f, 1, putEntry(1, 500, 5), putEntry(1, 501, 6))

	want := manyKeys(40)
	reqs := chunks(snapshotOf(t, want), 100, 4, 0, 30, 3)
	require.GreaterOrEqual(t, len(reqs), 4, "test needs a genuinely multi-chunk transfer")

	for i, req := range reqs[:len(reqs)-1] {
		install(t, f, req)
		assert.Equal(t, map[int]int{500: 5}, f.application.(*testApp).getData(), "chunk %d (Done=false) must not restore state", i)
		assert.Equal(t, 0, f.snapshotter.lastIndex, "chunk %d must not advance the snapshot index", i)
	}
	install(t, f, reqs[len(reqs)-1])

	assert.Equal(t, want, f.application.(*testApp).getData(), "state machine == snapshot content")
	st := stateOf(f)
	assert.Equal(t, 4, st.Term)
	assert.Equal(t, 0, st.Leader)
	assert.Equal(t, 30, st.Commit, "commit index jumps to lastIncludedIndex")
	assert.Equal(t, 30, f.snapshotter.lastIndex)
	assert.Equal(t, 3, f.snapshotter.lastTerm)
	assert.Empty(t, st.Log, "log not extending the snapshot is discarded")
	assert.Equal(t, 30, st.LastIndex)
}

func TestInstallSnapshot_DuplicateNonZeroChunkIsIdempotent(t *testing.T) {
	t.Parallel()
	f := createTestRaft(t, 1, 3)
	want := manyKeys(40)
	reqs := chunks(snapshotOf(t, want), 100, 2, 0, 12, 2)
	require.GreaterOrEqual(t, len(reqs), 4)
	for _, i := range []int{0, 1, 1, 2, 2, 3} { // chunk 1 and 2 delivered twice
		install(t, f, reqs[i])
	}
	for _, r := range reqs[4:] {
		install(t, f, r)
	}
	assert.Equal(t, want, f.application.(*testApp).getData())
}

// KNOWN BUG (see report): a delayed duplicate of chunk 0 truncates the
// partially-received snapshot file (WriteSnapshotData O_TRUNC at offset 0) and
// the follower never notices; the final restore reads a file with a hole and
// silently installs corrupted state.
func TestKnownBug_InstallSnapshot_DelayedDuplicateOfFirstChunkCorruptsState(t *testing.T) {
	t.Parallel()
	f := createTestRaft(t, 1, 3)
	want := manyKeys(40)
	reqs := chunks(snapshotOf(t, want), 100, 2, 0, 12, 2)
	require.GreaterOrEqual(t, len(reqs), 4)
	install(t, f, reqs[0])
	install(t, f, reqs[1])
	install(t, f, reqs[0]) // delayed duplicate
	for _, r := range reqs[2:] {
		_, _ = f.InstallSnapshot(context.Background(), r)
	}
	got := f.application.(*testApp).getData()
	if len(got) != 0 { // refusing the snapshot altogether would also be acceptable
		assert.Equal(t, want, got, "follower installed a corrupted snapshot")
	}
}

func TestInstallSnapshot_HigherTermClearsVote(t *testing.T) {
	t.Parallel()
	f := createTestRaft(t, 1, 3)
	f.mu.Lock()
	f.currentTerm = 1
	f.votedFor = 2
	f.mu.Unlock()
	for _, r := range chunks(snapshotOf(t, manyKeys(3)), 1<<20, 3, 0, 5, 3) {
		install(t, f, r)
	}
	st := stateOf(f)
	assert.Equal(t, 3, st.Term)
	assert.Equal(t, -1, st.VotedFor, "entering a new term must forget the old term's vote")
}

func TestInstallSnapshot_KeepsLogSuffixThatExtendsSnapshot(t *testing.T) {
	t.Parallel()
	f := createTestRaft(t, 1, 3)
	log := []LogEntry{putEntry(1, 1, 1), putEntry(1, 2, 2), putEntry(2, 3, 3), putEntry(2, 4, 4)}
	setLog(f, 2, log...)
	// snapshot covers entries 1..2 (lastIncluded term 1) and matches our log
	for _, r := range chunks(snapshotOf(t, map[int]int{1: 1, 2: 2}), 1<<20, 2, 0, 2, 1) {
		install(t, f, r)
	}
	st := stateOf(f)
	assert.Equal(t, 2, f.snapshotter.lastIndex)
	assert.Equal(t, []int{2, 2}, termsOf(st.Log), "entries after the snapshot point are retained")
	assert.Equal(t, 4, st.LastIndex)
}

// A follower that missed everything catches up from a leader whose log was
// compacted into a snapshot (leader must switch from AppendEntries to
// InstallSnapshot).
func TestLaggingFollowerCatchesUpViaSnapshot(t *testing.T) {
	c := createClusterWithTransport(t, 3, 9)
	for _, n := range c.Nodes {
		n.mu.Lock()
		n.snapshotter.snapshotThreshold = 5
		n.mu.Unlock()
	}
	c.Net.Partition([]int{2}, []int{0, 1})
	l0 := c.electLeader(t, 0)
	const n = 12
	for i := 1; i <= n; i++ {
		c.putOn(l0, i, i*10)
	}
	c.waitCommitted(t, []int{0, 1}, n)
	require.Eventually(t, func() bool {
		c.Nodes[1].mu.RLock()
		defer c.Nodes[1].mu.RUnlock()
		return c.Nodes[1].snapshotter.lastIndex > 0
	}, 5*time.Second, 10*time.Millisecond, "follower should have compacted its log")

	// The node holding the snapshot becomes leader: its log no longer contains
	// the early entries, so node 2 can only be served by InstallSnapshot.
	l1 := c.electLeader(t, 1)
	c.Net.Heal()
	c.putOn(l1, 100, 1000)
	c.waitCommitted(t, allIDs(3), n+1)

	require.Eventually(t, func() bool {
		d := c.Nodes[2].application.(*testApp).getData()
		for i := 1; i <= n; i++ {
			if d[i] != i*10 {
				return false
			}
		}
		return d[100] == 1000
	}, 8*time.Second, 10*time.Millisecond, "lagging follower did not converge")
	c.Nodes[2].mu.RLock()
	assert.Greater(t, c.Nodes[2].snapshotter.lastIndex, 0, "node 2 must have received a snapshot")
	c.Nodes[2].mu.RUnlock()
	assert.Equal(t, c.Nodes[1].application.(*testApp).getData(), c.Nodes[2].application.(*testApp).getData())
}
