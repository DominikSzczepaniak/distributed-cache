package raft

import (
	"fmt"
	"io"
	"log/slog"
	"os"
	"time"
)

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

//TODO
//on new snapshot do
//1. delete all previous logs until lastIndex - TODO in some higher logic func - DONE in decideRunSnapshot()
//2. delete all previous snapshots - DONE with O_CREATE on offset 0

func (rds *DataSaver) openFileForWriting(path string, flags int) (*os.File, error) {
	return os.OpenFile(path, flags, 0644)
}

func (rds *DataSaver) WriteSnapshotData(data []byte, offset int) (int, error) {
	//TODO if new snapshot enters this function with offset = 0, abort all the currently running ones
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

func (r *Raft) decideRunSnapshot() error {
	//this is run inside mutex, acquiring new one is not needed
	if len(r.log) >= r.snapshotter.snapshotThreshold {
		slog.Info(fmt.Sprintf("We decided to run snapshot, because log length is %d, while threshold is %d. Time: %d:%d:%d", len(r.log), r.snapshotter.snapshotThreshold, time.Now().Minute(), time.Now().Second(), time.Now().Nanosecond()))
		snpsht, err := r.application.GetSnapshot()
		logLenInSnapshot := len(r.log)
		lastTerm := r.log[len(r.log)-1].Term
		slog.Info(fmt.Sprintf("Got data for snapshot. Length of logs: %d, Last term: %d", logLenInSnapshot, lastTerm))
		if err != nil {
			slog.Error(fmt.Sprintf("Getting application snapshot failed %s", err.Error()))
			return err
		}
		_, err = r.logSaver.WriteSnapshotData(snpsht, 0)
		if err != nil {
			return err
		}
		slog.Info(fmt.Sprintf("Correctly wrote snapshot data. Cutting logs of length %d to %d - end", len(r.log), logLenInSnapshot-1))
		newLastIndex := r.snapshotter.lastIndex + logLenInSnapshot

		r.log = []LogEntry{}
		r.snapshotter.lastIndex = newLastIndex
		r.snapshotter.lastTerm = lastTerm
		r.commitedLength = newLastIndex
		r.application.RestoreFromSnapshot(snpsht)
	}
	return nil
}

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
