package raft

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"path/filepath"
	"reflect"
	"sync"
	"testing"

	mapset "github.com/deckarep/golang-set/v2"
	"github.com/dominikszczepaniak/distributed-cache/pkg/raft/raftpb"
	"github.com/stretchr/testify/mock"
	"google.golang.org/protobuf/types/known/emptypb"
)

type testApp struct {
	mu   sync.RWMutex
	data map[int]int
}

func newTestApp() *testApp {
	return &testApp{data: make(map[int]int)}
}

func (a *testApp) AppendMessage(msg Message) (bool, int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	switch msg.MsgType {
	case put:
		if msg.Value == nil {
			return false, 0
		}
		a.data[msg.Key] = *msg.Value
		return true, 0
	case get:
		return true, a.data[msg.Key]
	case delete:
		rv := reflect.ValueOf(a.data)
		rv.SetMapIndex(reflect.ValueOf(msg.Key), reflect.Value{})
		return true, 0
	default:
		return false, 0
	}
}

func (a *testApp) getData() map[int]int {
	a.mu.RLock()
	defer a.mu.RUnlock()
	m := make(map[int]int, len(a.data))
	for k, v := range a.data {
		m[k] = v
	}
	return m
}

func (a *testApp) GetSnapshot() ([]byte, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	var buf bytes.Buffer
	byteOrder := binary.LittleEndian

	mapLen := int32(len(a.data))
	if err := binary.Write(&buf, byteOrder, mapLen); err != nil {
		return nil, err
	}

	for k, v := range a.data {
		if err := binary.Write(&buf, byteOrder, int64(k)); err != nil {
			return nil, err
		}
		if err := binary.Write(&buf, byteOrder, int64(v)); err != nil {
			return nil, err
		}
	}
	return buf.Bytes(), nil
}

func (a *testApp) RestoreFromSnapshot(data []byte) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	reader := bytes.NewReader(data)
	byteOrder := binary.LittleEndian

	var mapLen int32
	if err := binary.Read(reader, byteOrder, mapLen); err != nil {
		return err
	}
	newMap := make(map[int]int)

	for i := 0; i < int(mapLen); i++ {
		var k64, v64 int64
		if err := binary.Read(reader, byteOrder, k64); err != nil {
			return err
		}
		if err := binary.Read(reader, byteOrder, v64); err != nil {
			return err
		}
		newMap[int(k64)] = int(v64)
	}
	a.data = newMap
	return nil
}

func intPtr(i int) *int { return &i }

type mockPeerClient struct{ mock.Mock }

func (m *mockPeerClient) Forward(ctx context.Context, msg *raftpb.Message) (*raftpb.Null, error) {
	args := m.Called(ctx, msg)
	return args.Get(0).(*raftpb.Null), args.Error(1)
}
func (m *mockPeerClient) VoteRequest(ctx context.Context, in *raftpb.VoteRequestArgs) (*raftpb.VoteResponse, error) {
	args := m.Called(ctx, in)
	return args.Get(0).(*raftpb.VoteResponse), args.Error(1)
}
func (m *mockPeerClient) LogRequest(ctx context.Context, in *raftpb.LogRequestArgs) (*raftpb.LogResponse, error) {
	args := m.Called(ctx, in)
	return args.Get(0).(*raftpb.LogResponse), args.Error(1)
}
func (m *mockPeerClient) Heartbeat(ctx context.Context, in *emptypb.Empty) (*emptypb.Empty, error) {
	args := m.Called(ctx, in)
	return args.Get(0).(*emptypb.Empty), args.Error(1)
}
func (m *mockPeerClient) InstallSnapshot(ctx context.Context, in *raftpb.InstallSnapshotRequest) (*raftpb.InstallSnapshotResponse, error) {
	args := m.Called(ctx, in)
	return args.Get(0).(*raftpb.InstallSnapshotResponse), args.Error(1)
}

func createTestRaft(t testing.TB, id, totalNodes int) *Raft {
	t.Helper()
	app := newTestApp()
	tmpDir := t.TempDir()
	fn := filepath.Join(tmpDir, fmt.Sprintf("node-%d", id))
	cfg := &Config{
		logsFilename:      fn + ".logs",
		metadataFilename:  fn + ".meta",
		snapshotFilename:  fn + ".snap",
		totalNodes:        totalNodes,
		raftId:            id,
		raftAddrs:         make([]string, totalNodes),
		snapshotThreshold: 1e7,
	}
	r := &Raft{
		id:              id,
		totalNodes:      totalNodes,
		currentTerm:     0,
		votedFor:        -1,
		log:             []LogEntry{},
		commitedLength:  0,
		currentRole:     Follower,
		currentLeaderId: -1,
		votesReceived:   mapset.NewSet[int](),
		sentLengths:     make([]int, totalNodes),
		ackedLengths:    make([]int, totalNodes),
		application:     app,
	}
	r.logSaver = NewRaftDataSaver(r, cfg)
	r.raftElector = NewRaftElector(r)
	close(r.raftElector.cancelTimerCh)
	r.logReplicator = NewRaftLogReplicator(r)
	close(r.logReplicator.cancelLogReplicateCh)
	r.snapshotter = newSnapshotter(cfg)

	initial := make([]PeerClient, totalNodes)
	r.setPeers(initial)

	return r
}

func createClusterMocks(t *testing.T, size int) ([]*Raft, []*mockPeerClient) {
	t.Helper()
	nodes := make([]*Raft, size)
	mocks := make([]*mockPeerClient, size)
	for i := 0; i < size; i++ {
		nodes[i] = createTestRaft(t, i, size)
		mocks[i] = &mockPeerClient{}
		mocks[i].
			On("LogRequest", mock.Anything, mock.Anything).
			Return(&raftpb.LogResponse{
				NodeId:      int32(i),
				CurrentTerm: int32(nodes[i].currentTerm),
				Ack:         0,
				Success:     false,
			}, nil).
			Maybe()
	}
	peers := make([]PeerClient, size)
	for i, m := range mocks {
		peers[i] = m
	}
	for _, node := range nodes {
		node.setPeers(peers)
	}
	return nodes, mocks
}

type inMemPeer struct{ r *Raft }

func (p *inMemPeer) Forward(ctx context.Context, m *raftpb.Message) (*raftpb.Null, error) {
	return p.r.Forward(ctx, m)
}
func (p *inMemPeer) VoteRequest(ctx context.Context, in *raftpb.VoteRequestArgs) (*raftpb.VoteResponse, error) {
	return p.r.VoteRequest(ctx, in)
}
func (p *inMemPeer) LogRequest(ctx context.Context, in *raftpb.LogRequestArgs) (*raftpb.LogResponse, error) {
	return p.r.LogRequest(ctx, in)
}
func (p *inMemPeer) Heartbeat(ctx context.Context, in *emptypb.Empty) (*emptypb.Empty, error) {
	return p.r.Heartbeat(ctx, in)
}
func (p *inMemPeer) InstallSnapshot(ctx context.Context, in *raftpb.InstallSnapshotRequest) (*raftpb.InstallSnapshotResponse, error) {
	return p.r.InstallSnapshot(ctx, in)
}

func createCluster(t testing.TB, n int) []*Raft {
	t.Helper()
	apps := make([]*testApp, n)
	for i := range apps {
		apps[i] = newTestApp()
	}
	nodes := make([]*Raft, n)
	for id := 0; id < n; id++ {
		tmp := t.TempDir()
		fn := filepath.Join(tmp, fmt.Sprintf("node-%d", id))
		cfg := &Config{
			logsFilename:      fn + ".logs",
			metadataFilename:  fn + ".meta",
			snapshotFilename:  fn + ".snap",
			totalNodes:        n,
			raftId:            id,
			raftAddrs:         make([]string, n),
			snapshotThreshold: 1e7,
		}
		r := &Raft{
			id:              id,
			totalNodes:      n,
			currentTerm:     0,
			votedFor:        -1,
			log:             []LogEntry{},
			commitedLength:  0,
			currentRole:     Follower,
			currentLeaderId: -1,
			votesReceived:   mapset.NewSet[int](),
			sentLengths:     make([]int, n),
			ackedLengths:    make([]int, n),
			application:     apps[id],
		}
		r.logSaver = NewRaftDataSaver(r, cfg)
		r.raftElector = NewRaftElector(r)
		r.logReplicator = NewRaftLogReplicator(r)
		r.snapshotter = newSnapshotter(cfg)
		nodes[id] = r
	}
	peers := make([]PeerClient, n)
	for i := range peers {
		peers[i] = &inMemPeer{r: nodes[i]}
	}
	for _, node := range nodes {
		node.setPeers(peers)
	}
	return nodes
}
