package raft

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDataSaver_SaveLoad(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		currTerm  int
		votedFor  int
		commitLen int
		logs      []LogEntry
	}{
		{
			name:      "empty",
			currTerm:  0,
			votedFor:  -1,
			commitLen: 0,
			logs:      nil,
		},
		{
			name:      "with_data",
			currTerm:  2,
			votedFor:  1,
			commitLen: 0,
			logs: []LogEntry{
				{Term: 1, Message: Message{MsgType: put, Key: 1, Value: intPtr(42)}},
				{Term: 2, Message: Message{MsgType: get, Key: 2, Value: nil}},
				{Term: 2, Message: Message{MsgType: delete, Key: 3, Value: nil}},
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			fn := filepath.Join(dir, "s.data")

			node := createTestRaft(t, 0, 3)
			cfg := &Config{
				logsFilename: fn, metadataFilename: fn + ".meta", snapshotFilename: fn + ".snap",
				totalNodes: 3,
				raftId:     0,
			}
			s := NewRaftDataSaver(node, cfg)

			node.mu.Lock()
			node.currentTerm = tt.currTerm
			node.votedFor = tt.votedFor
			node.commitedLength = tt.commitLen
			node.log = make([]LogEntry, len(tt.logs))
			copy(node.log, tt.logs)
			node.mu.Unlock()

			// save & load
			ok, err := s.SaveValues()
			require.NoError(t, err)
			assert.True(t, ok, "SaveValues should succeed")

			term, vf, cl, loaded, err := s.LoadValues()
			require.NoError(t, err)

			assert.Equal(t, int(tt.currTerm), term)
			assert.Equal(t, int(tt.votedFor), vf)
			assert.Equal(t, int(tt.commitLen), cl)
			assert.Equal(t, len(tt.logs), len(loaded))

			for i := range tt.logs {
				exp := tt.logs[i]
				got := loaded[i]
				assert.Equal(t, exp.Term, got.Term)
				assert.Equal(t, exp.Message.MsgType, got.Message.MsgType)
				assert.Equal(t, exp.Message.Key, got.Message.Key)
				if exp.Message.Value == nil {
					assert.Nil(t, got.Message.Value)
				} else {
					assert.Equal(t, *exp.Message.Value, *got.Message.Value)
				}
			}
		})
	}
}

func TestDataSaver_InvalidPath(t *testing.T) {
	t.Parallel()
	node := createTestRaft(t, 0, 3)
	fn := "/invalid/path/nope.data"
	cfg := &Config{
		logsFilename: fn, metadataFilename: fn + ".meta", snapshotFilename: fn + ".snap",
		totalNodes: 3,
		raftId:     0,
	}
	s := NewRaftDataSaver(node, cfg)

	// set minimal valid state
	node.mu.Lock()
	node.currentTerm = 1
	node.votedFor = 0
	node.commitedLength = 0
	node.log = nil
	node.mu.Unlock()

	ok, err := s.SaveValues()
	assert.False(t, ok, "SaveValues should return ok=false on invalid path")
	require.Error(t, err)
}

func TestDataSaver_CorruptedFile(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	fn := filepath.Join(dir, "broken.data")

	// write some non‐gob data
	require.NoError(t, os.WriteFile(fn, []byte("this is not gob"), 0o644))

	node := createTestRaft(t, 0, 3)
	cfg := &Config{
		logsFilename: fn, metadataFilename: fn + ".meta", snapshotFilename: fn + ".snap",
		totalNodes: 3,
		raftId:     0,
	}
	s := NewRaftDataSaver(node, cfg)

	_, _, _, _, err := s.LoadValues()
	assert.Error(t, err, "LoadValues should fail on corrupted file")
}
