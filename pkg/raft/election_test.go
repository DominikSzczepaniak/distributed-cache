package raft

import (
	"context"
	"testing"
	"time"

	"github.com/dominikszczepaniak/distributed-cache/pkg/raft/raftpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

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
				n.log = append(n.log, LogEntry{Term: tt.args.logTerm,
					Message: Message{MsgType: put, Key: 1, Value: intPtr(1)}})
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
		mocks[i].
			On("VoteRequest", mock.Anything, mock.Anything).
			Return(&raftpb.VoteResponse{
				NodeId:      int32(i),
				CurrentTerm: 1,
				Granted:     false,
			}, nil).
			Maybe()
	}
	peers := make([]PeerClient, len(mocks))
	for i, m := range mocks {
		peers[i] = m
	}
	node.setPeers(peers)
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
