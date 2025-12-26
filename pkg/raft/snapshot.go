package raft

import (
	"fmt"
	"io"
	"log/slog"
	"os"
)

// Snapshot maintains metadata about the last compacted log entry.
// It tracks the index and term of the last entry included in the snapshot.
type Snapshot struct {
	lastIndex int
	lastTerm  int

	snapshotThreshold  int
	installingSnapshot bool
}

func newSnapshotter(cfg *Config) *Snapshot {
	return &Snapshot{
		lastIndex:         0,
		lastTerm:          0,
		snapshotThreshold: cfg.snapshotThreshold,
	}
}

func (rds *DataSaver) openFileForWriting(path string, flags int) (*os.File, error) {
	return os.OpenFile(path, flags, 0644)
}

// WriteSnapshotData writes a chunk of snapshot data to disk.
// It is used by Raft when receiving snapshots from a leader.
func (rds *DataSaver) WriteSnapshotData(data []byte, offset int) (int, error) {
	var f *os.File
	var err error

	if offset == 0 {
		flags := os.O_WRONLY | os.O_CREATE | os.O_TRUNC
		f, err = rds.openFileForWriting(rds.snapshotFilename, flags)
	} else {
		flags := os.O_WRONLY
		f, err = rds.openFileForWriting(rds.snapshotFilename, flags)
	}
	if err != nil {
		slog.Error(fmt.Sprintf("Opening snapshot file failed %s", err.Error()))
		return 0, err
	}
	defer f.Close()

	bytesWritten, err := f.WriteAt(data, int64(offset))
	if err != nil {
		slog.Error(fmt.Sprintf("Writing snapshot failed %s", err.Error()))
		return bytesWritten, err
	}

	return bytesWritten, nil
}

// decideRunSnapshot checks if the log has exceeded the snapshot threshold.
// If it has, it triggers the creation of a new snapshot from the application state
// and compacts the Raft log by removing entries included in the snapshot.
func (r *Raft) decideRunSnapshot() error {
	//this is run inside mutex, acquiring new one is not needed
	if len(r.log) < r.snapshotter.snapshotThreshold {
		return nil
	}
	comittedCount := r.commitedLength - r.snapshotter.lastIndex
	if comittedCount <= 0 {
		return nil
	}
	relIdx := comittedCount - 1
	lastTerm := r.snapshotter.lastTerm
	if relIdx >= 0 && relIdx < len(r.log) {
		lastTerm = r.log[relIdx].Term
	}
	slog.Info(fmt.Sprintf(
		"Taking snapshot at committed index=%d, dropping %d committed log entries (logLen=%d, threshold=%d)",
		r.commitedLength, comittedCount, len(r.log), r.snapshotter.snapshotThreshold,
	))
	snpsht, err := r.application.GetSnapshot()
	if err != nil {
		slog.Error(fmt.Sprintf("Getting application snapshot failed %s", err.Error()))
		return err
	}
	if _, err := r.logSaver.WriteSnapshotData(snpsht, 0); err != nil {
		return err
	}
	r.log = r.log[comittedCount:]
	r.snapshotter.lastIndex = r.commitedLength
	r.snapshotter.lastTerm = lastTerm

	return nil
}

// ReadSnapshotData loads the combined snapshot data from disk.
func (rds *DataSaver) ReadSnapshotData() ([]byte, error) {
	f, err := os.Open(rds.snapshotFilename)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		slog.Error("Opening snapshot file for reading failed", "error", err)
		return nil, err
	}
	defer f.Close()

	data, err := io.ReadAll(f)
	if err != nil {
		slog.Error("Reading snapshot data failed", "error", err)
		return nil, err
	}

	return data, nil
}
