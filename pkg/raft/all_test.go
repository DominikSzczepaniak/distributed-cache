// raft/all_tests_test.go
package raft

import (
	"context"
	"fmt"
	"google.golang.org/protobuf/types/known/wrapperspb"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	mapset "github.com/deckarep/golang-set/v2"
	"github.com/dominikszczepaniak/distributed-cache/pkg/raft/raftpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// testApp implements the Application interface for testing.
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
	switch msg.msgType {
	case put:
		if msg.value == nil {
			return false, 0
		}
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

func (a *testApp) getData() map[int]int {
	a.mu.RLock()
	defer a.mu.RUnlock()
	m := make(map[int]int, len(a.data))
	for k, v := range a.data {
		m[k] = v
	}
	return m
}

// intPtr is a helper to create *int.
func intPtr(i int) *int { return &i }

// mockPeerClient mocks the PeerClient interface.
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

// createTestRaft sets up a Raft instance with persistence and elector, replicator.
func createTestRaft(t testing.TB, id, totalNodes int) *Raft {
	t.Helper()
	app := newTestApp()
	tmpDir := t.TempDir()
	cfg := &Config{
		valuesFilename: filepath.Join(tmpDir, fmt.Sprintf("node-%d.data", id)),
		totalNodes:     totalNodes,
		raftId:         id,
		raftAddrs:      make([]string, totalNodes),
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
		peers:           make([]PeerClient, totalNodes),
	}
	r.logSaver = NewRaftDataSaver(r, cfg)
	r.raftElector = NewRaftElector(r)
	close(r.raftElector.cancelTimerCh)
	r.logReplicator = NewRaftLogReplicator(r)
	close(r.logReplicator.cancelLogReplicateCh)
	return r
}

// createClusterMocks builds a cluster of Raft nodes with mock peers.
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
	for i := 0; i < size; i++ {
		for j := 0; j < size; j++ {
			nodes[i].peers[j] = mocks[j]
		}
	}
	return nodes, mocks
}

// inMemPeer implements PeerClient for in-memory integration tests.
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

// createCluster builds an in-memory cluster for integration tests.
func createCluster(t *testing.T, n int) []*Raft {
	t.Helper()
	apps := make([]*testApp, n)
	for i := range apps {
		apps[i] = newTestApp()
	}
	nodes := make([]*Raft, n)
	for id := 0; id < n; id++ {
		tmp := t.TempDir()
		fn := filepath.Join(tmp, fmt.Sprintf("node-%d.data", id))
		cfg := &Config{
			valuesFilename: fn,
			totalNodes:     n,
			raftId:         id,
			raftAddrs:      make([]string, n),
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
		nodes[id] = r
	}
	peers := make([]PeerClient, n)
	for i := range peers {
		peers[i] = &inMemPeer{r: nodes[i]}
	}
	for _, r := range nodes {
		r.mu.Lock()
		r.peers = peers
		r.mu.Unlock()
	}
	return nodes
}

// --- Message/Transport Tests ---

func TestMessageTypeConversion(t *testing.T) {
	t.Parallel()
	tests := []struct {
		msgType       MessageType
		expectedProto raftpb.Message_Type
		shouldPanic   bool
	}{
		{get, raftpb.Message_GET, false},
		{put, raftpb.Message_PUT, false},
		{delete, raftpb.Message_DELETE, false},
		{MessageType("X"), 0, true},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(string(tt.msgType), func(t *testing.T) {
			t.Parallel()
			if tt.shouldPanic {
				assert.Panics(t, func() { toProtoMsgType(tt.msgType) })
			} else {
				assert.Equal(t, tt.expectedProto, toProtoMsgType(tt.msgType))
			}
		})
	}
}

func TestMessageCreation(t *testing.T) {
	t.Parallel()
	v := 7
	msgs := []Message{
		{msgType: put, key: 1, value: &v},
		{msgType: get, key: 2, value: nil},
		{msgType: delete, key: 3, value: nil},
	}
	for _, msg := range msgs {
		m := msg
		t.Run(fmt.Sprintf("%s-%d", m.msgType, m.key), func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, m.msgType, m.msgType)
			assert.Equal(t, m.key, m.key)
			assert.True(t, reflect.DeepEqual(m.value, m.value))
		})
	}
}

// --- Log Replication Tests ---

func TestLogReplication(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name      string
		leaderLog []LogEntry
		setupMock func(*mockPeerClient)
	}{
		{
			"success",
			[]LogEntry{{term: 1, message: Message{msgType: put, key: 1, value: intPtr(42)}}},
			func(m *mockPeerClient) {
				m.On("LogRequest", mock.Anything, mock.MatchedBy(func(req *raftpb.LogRequestArgs) bool {
					return req.LeaderId == 0 && req.Term == 1
				})).Return(&raftpb.LogResponse{NodeId: 1, CurrentTerm: 1, Ack: 1, Success: true}, nil).Once()
			},
		},
		{
			"failure",
			[]LogEntry{{term: 1, message: Message{msgType: put, key: 2, value: intPtr(24)}}},
			func(m *mockPeerClient) {
				m.On("LogRequest", mock.Anything, mock.Anything).
					Return(&raftpb.LogResponse{NodeId: 1, CurrentTerm: 1, Ack: 0, Success: false}, nil).Once()
			},
		},
	}
	for _, tt := range cases {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			leader := createTestRaft(t, 0, 2)
			leader.currentRole = Leader
			leader.currentTerm = 1
			leader.log = append([]LogEntry{}, tt.leaderLog...)
			mockPeer := &mockPeerClient{}
			leader.peers[1] = mockPeer
			tt.setupMock(mockPeer)
			leader.replicateLog(0, 1)
			time.Sleep(100 * time.Millisecond)
			mockPeer.AssertExpectations(t)
		})
	}
}

func TestLogRequest(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name        string
		fTerm       int
		fLog        []LogEntry
		leaderTerm  int
		prefixLen   int
		prefixTerm  int
		entries     []LogEntry
		commitLen   int
		wantSuccess bool
		wantAck     int32
	}{
		{"accept", 1, nil, 1, 0, 0, []LogEntry{{term: 1, message: Message{msgType: put, key: 1, value: intPtr(42)}}}, 0, true, 1},
		{"stale_term", 2, nil, 1, 0, 0, nil, 0, false, 0},
		{"inconsistency", 1, []LogEntry{{term: 1, message: Message{msgType: put, key: 1, value: intPtr(1)}}}, 1, 1, 2, []LogEntry{{term: 1, message: Message{msgType: put, key: 2, value: intPtr(2)}}}, 1, false, 0},
	}
	for _, tt := range cases {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			f := createTestRaft(t, 1, 2)
			f.currentTerm = tt.fTerm
			f.log = append([]LogEntry{}, tt.fLog...)
			pb := make([]*raftpb.LogEntry, len(tt.entries))
			for i, e := range tt.entries {
				var val *wrapperspb.Int32Value
				if e.message.value != nil {
					val = wrapperspb.Int32(int32(*e.message.value))
				}
				pb[i] = &raftpb.LogEntry{
					Term:    int32(e.term),
					Message: &raftpb.Message{Type: toProtoMsgType(e.message.msgType), Key: int32(e.message.key), Value: val},
				}
			}
			req := &raftpb.LogRequestArgs{
				LeaderId:     0,
				Term:         int32(tt.leaderTerm),
				PrefixLen:    int32(tt.prefixLen),
				PrefixTerm:   int32(tt.prefixTerm),
				CommitLength: int32(tt.commitLen),
				Suffix:       pb,
			}
			resp, err := f.LogRequest(context.Background(), req)
			require.NoError(t, err)
			assert.Equal(t, tt.wantSuccess, resp.Success)
			assert.Equal(t, tt.wantAck, resp.Ack)
		})
	}
}

func TestLogCommitment(t *testing.T) {
	t.Parallel()
	leader := createTestRaft(t, 0, 3)
	leader.currentRole = Leader
	leader.currentTerm = 1
	msg := Message{msgType: put, key: 1, value: intPtr(42)}
	leader.log = append(leader.log, LogEntry{term: 1, message: msg})
	leader.ackedLengths[0] = 1
	leader.ackedLengths[1] = 1
	leader.mu.Lock()
	leader.commitLogEntries()
	leader.mu.Unlock()
	assert.Equal(t, 1, leader.commitedLength)
	data := leader.application.(*testApp).getData()
	assert.Equal(t, 42, data[1])
}

// --- Persistence Tests ---

func TestDataSaver(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		currTerm  int32
		votedFor  int32
		commitLen int32
		logs      []LogEntry
	}{
		{"empty", 0, -1, 0, nil},
		{"with_data", 2, 1, 2, []LogEntry{
			{term: 1, message: Message{msgType: put, key: 1, value: intPtr(42)}},
			{term: 2, message: Message{msgType: get, key: 2, value: nil}},
			{term: 2, message: Message{msgType: delete, key: 3, value: nil}},
		}},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			fn := filepath.Join(dir, "s.data")
			node := createTestRaft(t, 0, 3)
			cfg := &Config{valuesFilename: fn, totalNodes: 3, raftId: 0}
			s := NewRaftDataSaver(node, cfg)
			ok, err := s.SaveValues(tt.currTerm, tt.votedFor, tt.commitLen, tt.logs)
			require.NoError(t, err)
			assert.True(t, ok)
			term, vf, cl, loaded, err := s.LoadValues()
			require.NoError(t, err)
			assert.Equal(t, int(tt.currTerm), term)
			assert.Equal(t, int(tt.votedFor), vf)
			assert.Equal(t, int(tt.commitLen), cl)
			assert.Equal(t, len(tt.logs), len(loaded))
			for i := range tt.logs {
				exp, got := tt.logs[i], loaded[i]
				assert.Equal(t, exp.term, got.term)
				assert.Equal(t, exp.message.msgType, got.message.msgType)
				assert.Equal(t, exp.message.key, got.message.key)
				if exp.message.value == nil {
					assert.Nil(t, got.message.value)
				} else {
					assert.Equal(t, *exp.message.value, *got.message.value)
				}
			}
		})
	}
}

func TestDataSaverInvalidPath(t *testing.T) {
	node := createTestRaft(t, 0, 3)
	cfg := &Config{valuesFilename: "/invalid/path", totalNodes: 3, raftId: 0}
	defer func() {
		if r := recover(); r != nil {
			assert.Contains(t, fmt.Sprint(r), "not correct")
		}
	}()
	s := NewRaftDataSaver(node, cfg)
	_, err := s.SaveValues(1, 0, 0, nil)
	require.Error(t, err)
}

func TestLogFileParsing(t *testing.T) {
	t.Parallel()
	cases := []struct {
		content       string
		expectError   bool
		expectedCount int
	}{
		{"1 0 2\n1 PUT 1 42\n1 GET 2\n1 DELETE 3\n", false, 3},
		{"bad header\n", true, 0},
		{"1 0 0\n1 PUT 1\n", true, 0},
	}
	for _, tt := range cases {
		tt := tt
		t.Run("", func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			fn := filepath.Join(dir, "f.data")
			require.NoError(t, os.WriteFile(fn, []byte(tt.content), 0644))
			node := createTestRaft(t, 0, 3)
			cfg := &Config{valuesFilename: fn, totalNodes: 3, raftId: 0}
			s := NewRaftDataSaver(node, cfg)
			_, _, _, logs, err := s.LoadValues()
			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expectedCount, len(logs))
			}
		})
	}
}

// --- Integration Tests ---

func TestFullRaftIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("short")
	}
	t.Run("election_and_replication", func(t *testing.T) {
		nodes := createCluster(t, 3)
		time.Sleep(500 * time.Millisecond)
		var leader *Raft
	outer:
		for {
			for _, n := range nodes {
				n.mu.RLock()
				currentRole := n.currentRole
				n.mu.RUnlock()
				if currentRole == Leader {
					leader = n
					break outer
				}
			}
		}
		require.NotNil(t, leader)
		val := 100
		leader.Broadcast(Message{msgType: put, key: 9, value: &val})
		time.Sleep(1 * time.Second)
		for i, n := range nodes {
			n.mu.RLock()
			assert.Greater(t, len(n.log), 0, "node %d", i)
			assert.Greater(t, n.commitedLength, 0, "node %d", i)
			n.mu.RUnlock()
			data := n.application.(*testApp).getData()
			assert.Equal(t, val, data[9], "node %d", i)
		}
	})
	t.Run("forwarding", func(t *testing.T) {
		nodes := createCluster(t, 3)
		time.Sleep(500 * time.Millisecond)
		var leader, follower *Raft
		for _, n := range nodes {
			n.mu.RLock()
			if n.currentRole == Leader {
				leader = n
			} else if follower == nil {
				follower = n
			}
			n.mu.RUnlock()
		}
		require.NotNil(t, leader)
		require.NotNil(t, follower)
		val := 55
		follower.Broadcast(Message{msgType: put, key: 5, value: &val})
		time.Sleep(1 * time.Second)
		for i, n := range nodes {
			data := n.application.(*testApp).getData()
			assert.Equal(t, val, data[5], "node %d", i)
		}
	})
}

func TestRaftRecovery(t *testing.T) {
	if testing.Short() {
		t.Skip("short")
	}
	tmp := t.TempDir()
	fn := filepath.Join(tmp, "r.data")
	app1 := newTestApp()
	initLogs := []LogEntry{
		{term: 1, message: Message{msgType: put, key: 1, value: intPtr(42)}},
		{term: 1, message: Message{msgType: put, key: 2, value: intPtr(24)}},
	}
	node1 := &Raft{id: 0, totalNodes: 3, currentTerm: 2, votedFor: 1,
		log: initLogs, commitedLength: 2, application: app1}
	s := NewRaftDataSaver(node1, &Config{valuesFilename: fn, totalNodes: 3, raftId: 0})
	ok, err := s.SaveValues(2, 1, 2, initLogs)
	require.NoError(t, err)
	require.True(t, ok)
	app2 := newTestApp()
	node2 := NewRaft(app2, &Config{valuesFilename: fn, totalNodes: 3, raftId: 0, raftAddrs: make([]string, 3)})
	assert.Equal(t, 2, node2.currentTerm)
	assert.Equal(t, 1, node2.votedFor)
	assert.Equal(t, 2, node2.commitedLength)
	require.Len(t, node2.log, 2)
	for i, exp := range initLogs {
		got := node2.log[i]
		assert.Equal(t, exp.term, got.term)
		assert.Equal(t, exp.message.msgType, got.message.msgType)
		assert.Equal(t, exp.message.key, got.message.key)
		if exp.message.value != nil {
			assert.Equal(t, *exp.message.value, *got.message.value)
		}
	}
}

func TestConcurrentOperations(t *testing.T) {
	if testing.Short() {
		t.Skip("short")
	}
	nodes := createCluster(t, 3)
	time.Sleep(500 * time.Millisecond)
	var leader *Raft
	for _, n := range nodes {
		n.mu.RLock()
		if n.currentRole == Leader {
			leader = n
		}
		n.mu.RUnlock()
	}
	require.NotNil(t, leader)
	const nmsgs = 10
	done := make(chan struct{}, nmsgs)
	for i := 0; i < nmsgs; i++ {
		go func(key int) {
			val := key * 2
			leader.Broadcast(Message{msgType: put, key: key, value: &val})
			done <- struct{}{}
		}(i)
	}
	for i := 0; i < nmsgs; i++ {
		<-done
	}
	time.Sleep(2 * time.Second)
	for _, n := range nodes {
		data := n.application.(*testApp).getData()
		for i := 0; i < nmsgs; i++ {
			assert.Equal(t, i*2, data[i])
		}
	}
}

// ---------------

// // --- Election Tests ---
func TestRaftElection(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name          string
		totalNodes    int
		nodeId        int
		setupMocks    func([]*mockPeerClient)
		wantRole      Role
		wantVoteCount int
	}{
		{
			"3nodes_success",
			3, 0,
			func(mocks []*mockPeerClient) {
				for i := 1; i < 3; i++ {
					mocks[i].On("VoteRequest", mock.Anything, mock.Anything).
						Return(&raftpb.VoteResponse{NodeId: int32(i), CurrentTerm: 1, Granted: true}, nil).
						Once()
				}
			},
			Leader, 2,
		},
		{
			"5nodes_split",
			5, 0,
			func(mocks []*mockPeerClient) {
				mocks[1].On("VoteRequest", mock.Anything, mock.Anything).
					Return(&raftpb.VoteResponse{NodeId: 1, CurrentTerm: 1, Granted: true}, nil).Once()
				for _, i := range []int{2, 3, 4} {
					mocks[i].On("VoteRequest", mock.Anything, mock.Anything).
						Return(&raftpb.VoteResponse{NodeId: int32(i), CurrentTerm: 1, Granted: false}, nil).Once()
				}
			},
			Candidate, 2,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			nodes, mocks := createClusterMocks(t, tt.totalNodes)
			n := nodes[tt.nodeId]
			tt.setupMocks(mocks)
			n.startElection()
			time.Sleep(100 * time.Millisecond)
			n.mu.RLock()
			gotRole := n.currentRole
			gotVotes := n.votesReceived.Cardinality()
			n.mu.RUnlock()
			assert.Equal(t, tt.wantRole, gotRole)
			assert.GreaterOrEqual(t, tt.wantVoteCount, gotVotes)
			for _, m := range mocks {
				m.AssertExpectations(t)
			}
		})
	}
}

func TestVoteRequest(t *testing.T) {
	t.Parallel()
	type args struct {
		nodeTerm    int
		nodeVoted   int
		logLen      int
		logTerm     int
		candTerm    int
		candLogLen  int
		candLogTerm int
	}
	cases := []struct {
		name            string
		args            args
		wantGranted     bool
		wantCurrentTerm int
	}{
		{"higher_term", args{1, -1, 0, 0, 2, 0, 0}, true, 2},
		{"lower_term", args{2, -1, 0, 0, 1, 0, 0}, false, 2},
		{"already_voted", args{1, 1, 0, 0, 1, 0, 0}, false, 1},
	}
	for _, tt := range cases {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			n := createTestRaft(t, 1, 3)
			n.currentTerm = tt.args.nodeTerm
			n.votedFor = tt.args.nodeVoted
			if tt.args.logLen > 0 {
				n.log = append(n.log, LogEntry{term: tt.args.logTerm,
					message: Message{msgType: put, key: 1, value: intPtr(1)}})
			}
			req := &raftpb.VoteRequestArgs{
				CandidateId:        0,
				CandidateTerm:      int32(tt.args.candTerm),
				CandidateLogLength: int32(tt.args.candLogLen),
				CandidateLogTerm:   int32(tt.args.candLogTerm),
			}
			resp, err := n.VoteRequest(context.Background(), req)
			require.NoError(t, err)
			assert.Equal(t, tt.wantGranted, resp.Granted)
			assert.Equal(t, int32(tt.wantCurrentTerm), resp.CurrentTerm)
		})
	}
}

func TestElectionTimeout(t *testing.T) {
	if testing.Short() {
		t.Skip("short mode")
	}
	node := createTestRaft(t, 0, 3)
	mocks := make([]*mockPeerClient, 3)
	for i := range mocks {
		mocks[i] = &mockPeerClient{}
		node.peers[i] = mocks[i]
		mocks[i].On("VoteRequest", mock.Anything, mock.Anything).
			Return(&raftpb.VoteResponse{NodeId: int32(i), CurrentTerm: 1, Granted: false}, nil).
			Maybe()
	}
	time.Sleep(400 * time.Millisecond)
	node.mu.RLock()
	role, term := node.currentRole, node.currentTerm
	node.mu.RUnlock()
	assert.True(t, role == Candidate || role == Follower)
	if role == Candidate {
		assert.Greater(t, term, 0)
	}
	assert.NotEqual(t, Leader, role)
}
