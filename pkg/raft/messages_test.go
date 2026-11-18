package raft

import (
	"testing"

	"github.com/dominikszczepaniak/distributed-cache/pkg/raft/raftpb"
	"github.com/stretchr/testify/assert"
)

func TestMessageTypeConversion(t *testing.T) {
	t.Parallel()
	tests := []struct {
		MsgType       MessageType
		expectedProto raftpb.Message_Type
		shouldPanic   bool
	}{
		{GetMsg, raftpb.Message_GET, false},
		{PutMsg, raftpb.Message_PUT, false},
		{DeleteMsg, raftpb.Message_DELETE, false},
		{MessageType("X"), 0, true},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(string(tt.MsgType), func(t *testing.T) {
			t.Parallel()
			if tt.shouldPanic {
				assert.Panics(t, func() { toProtoMsgType(tt.MsgType) })
			} else {
				assert.Equal(t, tt.expectedProto, toProtoMsgType(tt.MsgType))
			}
		})
	}
}
