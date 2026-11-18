package raft

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// waitForStableLeader waits until exactly one node is leader and every other
// node knows who it is.
func waitForStableLeader(t testing.TB, nodes []*Raft) *Raft {
	t.Helper()
	var leader *Raft
	require.Eventually(t, func() bool {
		leader = nil
		for _, n := range nodes {
			if n.getCurrentRole() == Leader {
				if leader != nil {
					return false
				}
				leader = n
			}
		}
		if leader == nil {
			return false
		}
		for _, n := range nodes {
			if n != leader && stateOf(n).Leader != leader.id {
				return false
			}
		}
		return true
	}, 15*time.Second, 10*time.Millisecond, "no stable leader elected")
	return leader
}

func TestFullRaftIntegration(t *testing.T) {
	t.Run("election_and_replication", func(t *testing.T) {
		nodes := createCluster(t, 3)
		leader := waitForStableLeader(t, nodes)
		val := 100
		leader.Broadcast(Message{MsgType: PutMsg, Key: 9, Value: &val})
		require.Eventually(t, func() bool {
			for _, n := range nodes {
				if n.application.(*testApp).getData()[9] != val {
					return false
				}
			}
			return true
		}, 10*time.Second, 10*time.Millisecond, "value not applied on all nodes")
		for i, n := range nodes {
			st := stateOf(n)
			assert.Greater(t, len(st.Log), 0, "node %d", i)
			assert.Greater(t, st.Commit, 0, "node %d", i)
		}
	})
	t.Run("forwarding", func(t *testing.T) {
		nodes := createCluster(t, 3)
		leader := waitForStableLeader(t, nodes)
		var follower *Raft
		for _, n := range nodes {
			if n != leader {
				follower = n
				break
			}
		}
		require.NotNil(t, follower)
		val := 55
		ok, _, err := follower.BroadcastSync(Message{MsgType: PutMsg, Key: 5, Value: &val}, 10*time.Second)
		require.NoError(t, err)
		require.True(t, ok)
		require.Eventually(t, func() bool {
			for _, n := range nodes {
				if n.application.(*testApp).getData()[5] != val {
					return false
				}
			}
			return true
		}, 10*time.Second, 10*time.Millisecond, "forwarded value not applied on all nodes")
	})
}

func TestRaftRecovery(t *testing.T) {
	dir := t.TempDir()
	initLogs := []LogEntry{
		{Term: 1, Message: Message{MsgType: PutMsg, Key: 1, Value: intPtr(42)}},
		{Term: 1, Message: Message{MsgType: PutMsg, Key: 2, Value: intPtr(24)}},
	}

	// "Before the crash": a node with durable state.
	node1 := createTestRaftIn(t, 0, 3, dir)
	node1.mu.Lock()
	node1.currentTerm = 2
	node1.votedFor = 1
	node1.commitedLength = 2
	node1.log = append([]LogEntry(nil), initLogs...)
	node1.mu.Unlock()
	ok, err := node1.logSaver.SaveValues()
	require.NoError(t, err)
	require.True(t, ok)
	require.NoError(t, node1.logSaver.WaitForPendingSaves())

	// "After the restart": a fresh node over the same files loads the state
	// (NewRaft does exactly this via LoadValues but also binds a gRPC port,
	// which needs real addresses, so the LoadValues path is exercised directly).
	node2 := createTestRaftIn(t, 0, 3, dir)
	term, votedFor, commit, savedLog, err := node2.logSaver.LoadValues()
	require.NoError(t, err)
	assert.Equal(t, 2, term)
	assert.Equal(t, 1, votedFor)
	assert.Equal(t, 2, commit)
	require.Len(t, savedLog, 2)
	for i, exp := range initLogs {
		got := savedLog[i]
		assert.Equal(t, exp.Term, got.Term)
		assert.Equal(t, exp.Message.MsgType, got.Message.MsgType)
		assert.Equal(t, exp.Message.Key, got.Message.Key)
		require.NotNil(t, got.Message.Value)
		assert.Equal(t, *exp.Message.Value, *got.Message.Value)
	}
}

// TestConcurrentOperations hammers the leader from many goroutines and checks
// that every replica converges to the same state.
//
// Size: 5k messages under -short, 200k otherwise. Raw throughput at scale
// (e.g. 25M proposals) is measured by BenchmarkReplicateAndCommitParallel:
//
//	go test ./pkg/raft -run xxx -bench ReplicateAndCommitParallel -benchtime=25000000x
func TestConcurrentOperations(t *testing.T) {
	nmsgs, numWorkers, waitFor := 5_000, 32, 30*time.Second
	if !testing.Short() {
		nmsgs, numWorkers, waitFor = 200_000, 128, 3*time.Minute
	}
	const numKeys = 1000

	nodes := createCluster(t, 3)
	leader := waitForStableLeader(t, nodes)

	jobs := make(chan int, 256)
	var wg sync.WaitGroup
	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for key := range jobs {
				v := key * 2
				leader.Broadcast(Message{MsgType: PutMsg, Key: key, Value: &v})
			}
		}()
	}
	for i := 0; i < nmsgs; i++ {
		jobs <- i % numKeys // every key is written, value is a pure function of the key
	}
	close(jobs)
	wg.Wait()

	// Broadcast is asynchronous: wait for the commit index, not for a sleep.
	require.Eventually(t, func() bool {
		for _, n := range nodes {
			if stateOf(n).Commit < nmsgs {
				return false
			}
		}
		return true
	}, waitFor, 50*time.Millisecond, "replicas did not commit all %d entries", nmsgs)

	for i, n := range nodes {
		for k := 0; k < numKeys; k++ {
			if !assert.Equal(t, k*2, n.application.GetValue(k), "node %d key %d", i, k) {
				return
			}
		}
	}
}
