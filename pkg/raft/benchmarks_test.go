package raft

import (
	"context"
	"runtime"
	"testing"
	"time"

	"github.com/dominikszczepaniak/distributed-cache/pkg/raft/raftpb"
	"github.com/stretchr/testify/require"
)

func waitAllCommit(b testing.TB, nodes []*Raft, n int) {
	b.Helper()
	require.Eventually(b, func() bool {
		for _, nd := range nodes {
			if stateOf(nd).Commit < n {
				return false
			}
		}
		return true
	}, 5*time.Minute, time.Millisecond)
}

// BenchmarkReplicateAndCommit: proposals through an in-memory 3-node cluster,
// timed until every replica has committed (and applied) all of them.
func BenchmarkReplicateAndCommit(b *testing.B) {
	nodes := createCluster(b, 3)
	leader := waitForStableLeader(b, nodes)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		v := i
		leader.Broadcast(Message{MsgType: PutMsg, Key: i % 1024, Value: &v})
	}
	waitAllCommit(b, nodes, b.N)
	b.StopTimer()
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "commits/s")
}

// BenchmarkReplicateAndCommitParallel: same, with concurrent proposers.
func BenchmarkReplicateAndCommitParallel(b *testing.B) {
	nodes := createCluster(b, 3)
	leader := waitForStableLeader(b, nodes)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			v := i
			leader.Broadcast(Message{MsgType: PutMsg, Key: i % 1024, Value: &v})
			i++
		}
	})
	waitAllCommit(b, nodes, b.N)
}

// BenchmarkLogRequestAppend measures the follower's AppendEntries handler
// (10 entries per RPC), without any network.
func BenchmarkLogRequestAppend(b *testing.B) {
	f := createTestRaft(b, 1, 3)
	const batch = 10
	entries := make([]LogEntry, batch)
	for i := range entries {
		entries[i] = putEntry(1, i, i)
	}
	reqs := make([]*raftpb.LogRequestArgs, b.N)
	for i := range reqs {
		prevTerm := 1
		if i == 0 {
			prevTerm = 0
		}
		reqs[i] = logRequestArgs(0, 1, i*batch, prevTerm, i*batch, entries...)
	}
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := f.LogRequest(ctx, reqs[i]); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkVoteRequest(b *testing.B) {
	n := createTestRaft(b, 1, 3)
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := n.VoteRequest(ctx, &raftpb.VoteRequestArgs{CandidateId: 0, CandidateTerm: int32(i + 1)})
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkLeaderBroadcastMemory replaces the old TestPausedConcurrentOperations
// (an 16M-message memory soak with no assertion value). It reports the live
// heap per proposal after the cluster has fully committed and a GC.
func BenchmarkLeaderBroadcastMemory(b *testing.B) {
	nodes := createCluster(b, 3)
	leader := waitForStableLeader(b, nodes)
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		v := i
		leader.Broadcast(Message{MsgType: PutMsg, Key: i % 10000, Value: &v})
	}
	waitAllCommit(b, nodes, b.N)
	b.StopTimer()
	runtime.GC()
	runtime.ReadMemStats(&after)
	b.ReportMetric(float64(int64(after.HeapAlloc)-int64(before.HeapAlloc))/float64(b.N), "heap-B/entry")
}
