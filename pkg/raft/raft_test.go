// // raft/raft_test.go
// // Unit tests specifically for the raft package (not for the API of Raft)
package raft

//
//import (
//	"context"
//	"fmt"
//	"path/filepath"
//	"reflect"
//	"sync"
//	"testing"
//	"time"
//
//	mapset "github.com/deckarep/golang-set/v2"
//	"github.com/dominikszczepaniak/distributed-cache/pkg/raft/raftpb"
//)
//
//func logEntryNotEqual(a, b LogEntry) bool {
//	if a.term != b.term {
//		return true
//	}
//	if a.message.msgType != b.message.msgType {
//		return true
//	}
//	if a.message.key != b.message.key {
//		return true
//	}
//	if a.message.value == nil && b.message.value != nil {
//		return true
//	}
//	if a.message.value != nil && b.message.value == nil {
//		return true
//	}
//	if a.message.value != nil && b.message.value != nil && *a.message.value != *b.message.value {
//		return true
//	}
//	return false
//}
//
//func logEntriesNotEqual(a, b []LogEntry) bool {
//	if len(a) != len(b) {
//		return true
//	}
//	for i := range a {
//		if logEntryNotEqual(a[i], b[i]) {
//			return true
//		}
//	}
//	return false
//}
//
//type testApp struct {
//	mu   sync.Mutex
//	data map[int]int
//}
//
//func newTestApp() *testApp {
//	return &testApp{data: make(map[int]int)}
//}
//
//func (a *testApp) AppendMessage(msg Message) (bool, int) {
//	a.mu.Lock()
//	defer a.mu.Unlock()
//
//	switch msg.msgType {
//	case put:
//		a.data[msg.key] = *msg.value
//		return true, 0
//	case get:
//		return true, a.data[msg.key]
//	case delete:
//		rv := reflect.ValueOf(a.data)
//		rv.SetMapIndex(reflect.ValueOf(msg.key), reflect.Value{})
//		return true, 0
//	default:
//		return false, 0
//	}
//}
//
//type inMemPeer struct{ r *Raft }
//
//func (p *inMemPeer) Forward(
//	ctx context.Context, m *raftpb.Message,
//) (*raftpb.Null, error) {
//	return p.r.Forward(ctx, m)
//}
//
//func (p *inMemPeer) VoteRequest(
//	ctx context.Context, in *raftpb.VoteRequestArgs,
//) (*raftpb.VoteResponse, error) {
//	return p.r.VoteRequest(ctx, in)
//}
//
//func (p *inMemPeer) LogRequest(
//	ctx context.Context, in *raftpb.LogRequestArgs,
//) (*raftpb.LogResponse, error) {
//	return p.r.LogRequest(ctx, in)
//}
//
//func createCluster(t *testing.T, n int) []*Raft {
//	t.Helper()
//	dir := t.TempDir()
//
//	apps := make([]*testApp, n)
//	for i := range apps {
//		apps[i] = newTestApp()
//	}
//
//	nodes := make([]*Raft, n)
//	for id := 0; id < n; id++ {
//		fn := filepath.Join(dir, fmt.Sprintf("node-%d.data", id))
//
//		cfg := &Config{
//			valuesFilename: fn,
//			totalNodes:     n,
//			raftId:         id,
//			raftAddrs:      nil, // unused in‐proc
//		}
//
//		r := &Raft{
//			id:             id,
//			totalNodes:     n,
//			currentTerm:    0,
//			votedFor:       -1,
//			log:            make([]LogEntry, 0),
//			commitedLength: 0,
//
//			currentRole:     Follower,
//			currentLeaderId: -1,
//			votesReceived:   mapset.NewSet[int](),
//			sentLengths:     make([]int, n),
//			ackedLengths:    make([]int, n),
//
//			application: apps[id],
//		}
//
//		r.logSaver = NewRaftDataSaver(r, cfg)
//		r.raftElector = NewRaftElector(r)
//		r.logReplicator = NewRaftLogReplicator(r)
//
//		nodes[id] = r
//	}
//
//	peers := make([]PeerClient, n)
//	for i := 0; i < n; i++ {
//		nodes[i].mu.Lock()
//		peers[i] = &inMemPeer{r: nodes[i]}
//		nodes[i].mu.Unlock()
//	}
//	for _, r := range nodes {
//		r.mu.Lock()
//		r.peers = peers
//		r.mu.Unlock()
//	}
//
//	return nodes
//}
//
//func TestLeaderElection(t *testing.T) {
//	nodes := createCluster(t, 3)
//	nodes[0].startElection()
//	time.Sleep(50 * time.Millisecond)
//	nodes[0].mu.RLock()
//	role0 := nodes[0].currentRole
//	nodes[0].mu.RUnlock()
//	if role0 != Leader {
//		t.Fatalf("node0 expected Leader, got %s", nodes[0].currentRole)
//	}
//	for i := 1; i < 3; i++ {
//		nodes[i].mu.RLock()
//		role := nodes[i].currentRole
//		nodes[i].mu.RUnlock()
//		if role != Follower {
//			t.Errorf("node%d expected Follower, got %s", i, role)
//		}
//	}
//}
//
//func TestLogReplication(t *testing.T) {
//	nodes := createCluster(t, 3)
//
//	time.Sleep(200 * time.Millisecond)
//	var leaderNode *Raft
//	outBreak := false
//	for {
//		if outBreak {
//			break
//		}
//		for i := range nodes {
//			nodes[i].mu.RLock()
//			role := nodes[i].currentRole
//			nodes[i].mu.RUnlock()
//			if role == Leader {
//				leaderNode = nodes[i]
//				outBreak = true
//				break
//			}
//		}
//	}
//
//	v := 42
//	msg := Message{msgType: put, key: 1, value: &v}
//	leaderNode.Broadcast(msg)
//
//	time.Sleep(2 * time.Second)
//
//	for i, r := range nodes {
//		app := r.application.(*testApp)
//		app.mu.Lock()
//		got := app.data[1]
//		app.mu.Unlock()
//		if got != v {
//			t.Errorf("node%d expected data[1]=%d; got %d", i, v, got)
//		}
//	}
//}
//
//func TestLogReplicationWithInitialLog(t *testing.T) {
//	v1 := 3
//	v2 := 5
//	initLog := []LogEntry{
//		{
//			term: 1,
//			message: Message{
//				msgType: get,
//				key:     2,
//				value:   &v1,
//			},
//		},
//		{
//			term: 2,
//			message: Message{
//				msgType: put,
//				key:     2,
//				value:   &v2,
//			},
//		},
//	}
//	nodes := createCluster(t, 3)
//
//	nodes[0].mu.Lock()
//	nodes[0].log = initLog
//	nodes[0].mu.Unlock()
//
//	time.Sleep(1000 * time.Millisecond) //one second to replicate log to everyone
//	nodes[0].mu.Lock()
//	log1 := nodes[0].log
//	nodes[0].mu.Unlock()
//
//	nodes[1].mu.Lock()
//	log2 := nodes[1].log
//	nodes[1].mu.Unlock()
//
//	nodes[2].mu.Lock()
//	log3 := nodes[2].log
//	nodes[2].mu.Unlock()
//
//	if !logEntriesNotEqual(log1, initLog) && logEntriesNotEqual(log1, log2) && logEntriesNotEqual(log2, log3) {
//		t.Errorf("Logs arent equal")
//	}
//}
//
//func TestLogReplicationWithForward(t *testing.T) {
//	nodes := createCluster(t, 3)
//
//	time.Sleep(200 * time.Millisecond)
//	var leaderIdx int
//outer:
//	for {
//		for i, n := range nodes {
//			n.mu.RLock()
//			isLeader := n.currentRole == Leader
//			n.mu.RUnlock()
//			if isLeader {
//				leaderIdx = i
//				break outer
//			}
//		}
//		time.Sleep(10 * time.Millisecond)
//	}
//
//	v := 42
//	msg := Message{msgType: put, key: 1, value: &v}
//	if leaderIdx == 1 {
//		nodes[0].Broadcast(msg)
//	} else {
//		nodes[1].Broadcast(msg)
//	}
//
//	time.Sleep(2 * time.Second)
//
//	for i, r := range nodes {
//		app := r.application.(*testApp)
//		app.mu.Lock()
//		got := app.data[1]
//		app.mu.Unlock()
//		if got != v {
//			t.Errorf("node%d expected data[1]=%d; got %d", i, v, got)
//		}
//	}
//}
//
//func TestThroughput(t *testing.T) {
//	//generate a lot of get and put requests - around 100k
//	//put some custom requests into that and make sure that random won't touch those
//	//assert custom requests are good
//}
