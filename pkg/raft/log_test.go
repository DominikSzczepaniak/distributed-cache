package raft

import (
	"context"
	"github.com/dominikszczepaniak/distributed-cache/pkg/raft/raftpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/wrapperspb"
	"testing"
	"time"
)

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
