// raft/raft_test.go
// Unit tests specifically for the raft package (not for the API of Raft)
package raft

import (
	"context"
	"fmt"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	mapset "github.com/deckarep/golang-set/v2"
	"github.com/dominikszczepaniak/distributed-cache/pkg/raft/raftpb"
)

type testApp struct {
	mu   sync.Mutex
	data map[int]int
}

func newTestApp() *testApp {
	return &testApp{data: make(map[int]int)}
}

func (a *testApp) AppendMessage(msg Message) (bool, int) {
	a.mu.Lock()
	defer a.mu.Unlock()

	switch msg.msgType {
	case put:
		a.data[msg.key] = *msg.value
		return true, 0
	case get:
		return true, a.data[msg.key]
	case delete:
		rv := reflect.ValueOf(a.data)
		rv.SetMapIndex(reflect.ValueOf(msg.key), reflect.Value{})
		return true, 0
	default:
		return false, 0
	}
}

type inMemPeer struct{ r *Raft }

func (p *inMemPeer) Forward(
	ctx context.Context, m *raftpb.Message,
) (*raftpb.Null, error) {
	return p.r.Forward(ctx, m)
}

func (p *inMemPeer) VoteRequest(
	ctx context.Context, in *raftpb.VoteRequestArgs,
) (*raftpb.VoteResponse, error) {
	return p.r.VoteRequest(ctx, in)
}

func (p *inMemPeer) LogRequest(
	ctx context.Context, in *raftpb.LogRequestArgs,
) (*raftpb.LogResponse, error) {
	return p.r.LogRequest(ctx, in)
}

func createCluster(t *testing.T, n int) []*Raft {
	t.Helper()
	dir := t.TempDir()

	apps := make([]*testApp, n)
	for i := range apps {
		apps[i] = newTestApp()
	}

	nodes := make([]*Raft, n)
	for id := 0; id < n; id++ {
		fn := filepath.Join(dir, fmt.Sprintf("node-%d.data", id))

		cfg := &Config{
			valuesFilename: fn,
			totalNodes:     n,
			raftId:         id,
			raftAddrs:      nil, // unused in‐proc
		}

		r := &Raft{
			id:             id,
			totalNodes:     n,
			currentTerm:    0,
			votedFor:       -1,
			log:            *new([]LogEntry),
			commitedLength: 0,

			currentRole:     Follower,
			currentLeaderId: -1,
			votesReceived:   mapset.NewSet[int](),
			sentLengths:     make([]int, n),
			ackedLenghts:    make([]int, n),

			application: apps[id],
		}

		r.logSaver = NewRaftDataSaver(r, cfg)
		r.raftElector = NewRaftElector(r)
		r.logReplicator = NewRaftLogReplicator(r)

		r.log = []LogEntry{{term: 0, message: Message{msgType: get, key: 0}}}

		nodes[id] = r
	}

	peers := make([]PeerClient, n)
	for i := 0; i < n; i++ {
		peers[i] = &inMemPeer{r: nodes[i]}
	}
	for _, r := range nodes {
		r.peers = peers
	}

	return nodes
}

func TestLeaderElection(t *testing.T) {
	nodes := createCluster(t, 3)
	nodes[0].StartElection()
	time.Sleep(50 * time.Millisecond)

	if nodes[0].currentRole != Leader {
		t.Fatalf("node0 expected Leader, got %s", nodes[0].currentRole)
	}
	for i := 1; i < 3; i++ {
		if nodes[i].currentRole != Follower {
			t.Errorf("node%d expected Follower, got %s", i, nodes[i].currentRole)
		}
	}
}

func TestLogReplication(t *testing.T) {
	nodes := createCluster(t, 3)

	time.Sleep(200 * time.Millisecond)
	var leaderNode *Raft
	for i := range nodes {
		if nodes[i].currentRole == Leader {
			leaderNode = nodes[i]
		}
	}
	leaderNode.logReplicator.logReplicateCh <- struct{}{}

	v := 42
	msg := Message{msgType: put, key: 1, value: &v}
	leaderNode.Broadcast(msg)

	time.Sleep(500 * time.Millisecond)

	for i, r := range nodes {
		app := r.application.(*testApp)
		app.mu.Lock()
		got := app.data[1]
		app.mu.Unlock()
		if got != v {
			t.Errorf("node%d expected data[1]=%d; got %d", i, v, got)
		}
	}
}
