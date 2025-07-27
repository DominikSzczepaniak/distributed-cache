package raft

import (
	"fmt"
	"github.com/dominikszczepaniak/distributed-cache/pkg/raft/raftpb"
	"github.com/stretchr/testify/assert"
	"reflect"
	"testing"
)

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
