package raft

import (
	"math/rand"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
		leader.Broadcast(Message{MsgType: put, Key: 9, Value: &val})
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
		follower.Broadcast(Message{MsgType: put, Key: 5, Value: &val})
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
		{Term: 1, Message: Message{MsgType: put, Key: 1, Value: intPtr(42)}},
		{Term: 1, Message: Message{MsgType: put, Key: 2, Value: intPtr(24)}},
	}
	node1 := &Raft{id: 0, totalNodes: 3, currentTerm: 2, votedFor: 1,
		log: initLogs, commitedLength: 2, application: app1}
	s := NewRaftDataSaver(node1, &Config{logsFilename: fn, metadataFilename: fn + ".meta", snapshotFilename: fn + ".snap", totalNodes: 3, raftId: 0})

	node1.mu.Lock()
	node1.currentTerm = 2
	node1.votedFor = 1
	node1.commitedLength = 2
	node1.log = initLogs
	node1.mu.Unlock()

	ok, err := s.SaveValues()
	require.NoError(t, err)
	require.True(t, ok)
	app2 := newTestApp()
	node2 := NewRaft(app2, &Config{logsFilename: fn, metadataFilename: fn + ".meta", snapshotFilename: fn + ".snap", totalNodes: 3, raftId: 0, raftAddrs: make([]string, 3)})
	assert.Equal(t, 2, node2.currentTerm)
	assert.Equal(t, 1, node2.votedFor)
	assert.Equal(t, 2, node2.commitedLength)
	require.Len(t, node2.log, 2)
	for i, exp := range initLogs {
		got := node2.log[i]
		assert.Equal(t, exp.Term, got.Term)
		assert.Equal(t, exp.Message.MsgType, got.Message.MsgType)
		assert.Equal(t, exp.Message.Key, got.Message.Key)
		if exp.Message.Value != nil {
			assert.Equal(t, *exp.Message.Value, *got.Message.Value)
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
	const nmsgs = 10_000_000
	const num_keys = 10_000
	done := make(chan struct{}, nmsgs)
	sem := make(chan int, 256)
	for i := 0; i < nmsgs; i++ {
		sem <- 1
		go func(key int) {
			val := key * 2
			leader.Broadcast(Message{MsgType: put, Key: key, Value: &val})
			done <- struct{}{}
			<-sem
		}(rand.Intn(num_keys))
	}
	for i := 0; i < nmsgs; i++ {
		<-done
	}
	time.Sleep(10 * time.Second)
	for _, n := range nodes {
		//data := n.application.(*testApp).getData()
		for i := 0; i < num_keys; i++ {
			assert.Equal(t, i*2, n.application.GetValue(i))
		}
	}
}
