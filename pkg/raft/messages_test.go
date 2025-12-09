package raft

import (
	"fmt"
	"reflect"
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

func TestMessageCreation(t *testing.T) {
	t.Parallel()
	v := 7
	msgs := []Message{
		{MsgType: PutMsg, Key: 1, Value: &v},
		{MsgType: GetMsg, Key: 2, Value: nil},
		{MsgType: DeleteMsg, Key: 3, Value: nil},
	}
	for _, msg := range msgs {
		m := msg
		t.Run(fmt.Sprintf("%s-%d", m.MsgType, m.Key), func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, m.MsgType, m.MsgType)
			assert.Equal(t, m.Key, m.Key)
			assert.True(t, reflect.DeepEqual(m.Value, m.Value))
		})
	}
}
