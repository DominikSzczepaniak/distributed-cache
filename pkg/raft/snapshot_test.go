package raft

import (
	"context"
	"github.com/dominikszczepaniak/distributed-cache/pkg/raft/raftpb"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWriteAndReadSnapshotData(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	fn := filepath.Join(dir, "snapshot.data")

	node := createTestRaft(t, 0, 3)
	cfg := &Config{
		logsFilename:     fn + ".logs",
		metadataFilename: fn + ".meta",
		snapshotFilename: fn,
		totalNodes:       3,
		raftId:           0,
	}
	saver := NewRaftDataSaver(node, cfg)

	snapshotData := []byte("test snapshot data content")
	bytesWritten, err := saver.WriteSnapshotData(snapshotData, 0)
	require.NoError(t, err)
	assert.Equal(t, len(snapshotData), bytesWritten)

	readData, err := saver.ReadSnapshotData()
	require.NoError(t, err)
	assert.Equal(t, snapshotData, readData)
}

func TestSnapshotCreationOnThreshold(t *testing.T) {
	if testing.Short() {
		t.Skip("short")
	}
	t.Parallel()
	node := createTestRaft(t, 0, 3)

	node.snapshotter.snapshotThreshold = 5

	node.mu.Lock()
	for i := 0; i < 10; i++ {
		node.log = append(node.log, LogEntry{
			Term: 1,
			Message: Message{
				MsgType: PutMsg,
				Key:     i,
				Value:   intPtr(i * 10),
			},
		})
	}

	node.commitedLength = 8
	logLenBefore := len(node.log)

	err := node.decideRunSnapshot()
	require.NoError(t, err)

	node.mu.Unlock()

	assert.Less(t, len(node.log), logLenBefore,
		"log should be truncated after snapshot")
	assert.Greater(t, node.snapshotter.lastIndex, 0,
		"snapshot lastIndex should be set")
}

func TestSnapshotWithApplicationState(t *testing.T) {
	if testing.Short() {
		t.Skip("short")
	}
	t.Parallel()
	node := createTestRaft(t, 0, 3)
	app := node.application.(*testApp)

	app.data[1] = 100
	app.data[2] = 200
	app.data[3] = 300

	snapshot, err := app.GetSnapshot()
	require.NoError(t, err)
	assert.Greater(t, len(snapshot), 0)

	newApp := newTestApp()
	err, lastKey := newApp.RestoreFromSnapshot(snapshot)
	require.NoError(t, err)

	assert.Equal(t, 100, newApp.GetValue(1))
	assert.Equal(t, 200, newApp.GetValue(2))
	assert.Equal(t, 300, newApp.GetValue(3))
	assert.Greater(t, lastKey, 0)
}

func TestInstallSnapshotRPC(t *testing.T) {
	if testing.Short() {
		t.Skip("short")
	}
	t.Parallel()
	nodes := createCluster(t, 3)
	leader := nodes[0]
	follower := nodes[1]

	leader.mu.Lock()
	leader.currentRole = Leader
	leader.currentTerm = 5

	leader.application.(*testApp).data[10] = 1000
	leader.application.(*testApp).data[20] = 2000
	snapshotData, err := leader.application.GetSnapshot()
	require.NoError(t, err)

	leader.snapshotter.lastIndex = 50
	leader.snapshotter.lastTerm = 4
	leader.mu.Unlock()

	_, err = leader.logSaver.WriteSnapshotData(snapshotData, 0)
	require.NoError(t, err)

	req := &raftpb.InstallSnapshotRequest{
		LeaderTerm:        int32(5),
		LeaderId:          int32(0),
		LastIncludedIndex: int32(0),
		LastIncludedTerm:  int32(4),
		Offset:            0,
		Data:              snapshotData,
		Done:              true,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	resp, err := follower.InstallSnapshot(ctx, req)
	require.NoError(t, err)
	require.NotNil(t, resp)

	time.Sleep(100 * time.Millisecond)

	data := follower.application.(*testApp).getData()
	assert.Equal(t, 1000, data[10], "follower should have snapshot data")
	assert.Equal(t, 2000, data[20], "follower should have snapshot data")
}

func TestSnapshotPersistence(t *testing.T) {
	if testing.Short() {
		t.Skip("short")
	}
	t.Parallel()
	tmpDir := t.TempDir()
	fn := filepath.Join(tmpDir, "snapshot.persist")

	app1 := newTestApp()
	app1.data[5] = 500
	snap1, err := app1.GetSnapshot()
	require.NoError(t, err)

	cfg1 := &Config{
		logsFilename:     fn + ".logs",
		metadataFilename: fn + ".meta",
		snapshotFilename: fn,
		totalNodes:       1,
		raftId:           0,
	}

	node1 := &Raft{
		id:          0,
		totalNodes:  1,
		application: app1,
		snapshotter: newSnapshotter(cfg1),
	}
	saver1 := NewRaftDataSaver(node1, cfg1)

	_, err = saver1.WriteSnapshotData(snap1, 0)
	require.NoError(t, err)

	app2 := newTestApp()
	node2 := &Raft{
		id:          0,
		totalNodes:  1,
		application: app2,
		snapshotter: newSnapshotter(cfg1),
	}
	saver2 := NewRaftDataSaver(node2, cfg1)

	readSnap, err := saver2.ReadSnapshotData()
	require.NoError(t, err)

	err, _ = app2.RestoreFromSnapshot(readSnap)
	require.NoError(t, err)

	assert.Equal(t, 500, app2.GetValue(5))
}

func TestSnapshotNoOpWhenBelowThreshold(t *testing.T) {
	t.Parallel()
	node := createTestRaft(t, 0, 3)

	node.snapshotter.snapshotThreshold = 1000

	node.mu.Lock()

	for i := 0; i < 5; i++ {
		node.log = append(node.log, LogEntry{
			Term:    1,
			Message: Message{MsgType: PutMsg, Key: i, Value: intPtr(i)},
		})
	}
	node.commitedLength = 3

	logLenBefore := len(node.log)
	lastIndexBefore := node.snapshotter.lastIndex

	err := node.decideRunSnapshot()
	require.NoError(t, err)

	node.mu.Unlock()

	assert.Equal(t, logLenBefore, len(node.log),
		"log should not be truncated when below threshold")
	assert.Equal(t, lastIndexBefore, node.snapshotter.lastIndex,
		"snapshot lastIndex should not change")
}

func TestSnapshotWithEmptyLog(t *testing.T) {
	t.Parallel()
	node := createTestRaft(t, 0, 3)

	node.snapshotter.snapshotThreshold = 10
	node.mu.Lock()

	node.log = []LogEntry{}
	node.commitedLength = 0

	err := node.decideRunSnapshot()
	require.NoError(t, err)

	node.mu.Unlock()

	assert.Equal(t, 0, len(node.log))
	assert.Equal(t, 0, node.snapshotter.lastIndex)
}

func TestSnapshotFileReplacement(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	fn := filepath.Join(dir, "snapshot.data")

	node := createTestRaft(t, 0, 3)
	cfg := &Config{
		logsFilename:     fn + ".logs",
		metadataFilename: fn + ".meta",
		snapshotFilename: fn,
		totalNodes:       3,
		raftId:           0,
	}
	saver := NewRaftDataSaver(node, cfg)

	snapshot1 := []byte("first snapshot")
	_, err := saver.WriteSnapshotData(snapshot1, 0)
	require.NoError(t, err)

	snapshot2 := []byte("second snapshot that is longer")
	_, err = saver.WriteSnapshotData(snapshot2, 0)
	require.NoError(t, err)

	readData, err := saver.ReadSnapshotData()
	require.NoError(t, err)
	assert.Equal(t, snapshot2, readData)
}

func TestSnapshotNonExistentFile(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	fn := filepath.Join(dir, "nonexistent", "snapshot.data")

	node := createTestRaft(t, 0, 3)
	cfg := &Config{
		logsFilename:     fn + ".logs",
		metadataFilename: fn + ".meta",
		snapshotFilename: fn,
		totalNodes:       3,
		raftId:           0,
	}
	saver := NewRaftDataSaver(node, cfg)

	readData, err := saver.ReadSnapshotData()
	assert.Nil(t, readData)
	assert.NoError(t, err)
}
