package raft

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"os"
	"path/filepath"
	"testing"
)

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
