package raft

import (
	"fmt"
	"log/slog"
	"os"
)

type Snapshot struct {
	lastIndex int
	lastTerm  int
	state     map[int]int

	snapshotThreshold int
}

func newSnapshotter(cfg *Config) *Snapshot {
	return &Snapshot{
		lastIndex:         0,
		lastTerm:          0,
		state:             map[int]int{},
		snapshotThreshold: cfg.snapshotThreshold,
	}
}

//TODO
//on new snapshot do
//1. delete all previous logs until lastIndex - TODO in some higher logic func
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
		snpsht, err := r.application.GetSnapshot()
		if err != nil {
			slog.Error(fmt.Sprintf("Getting application snapshot failed %s", err.Error()))
			return err
		}
		_, err = r.logSaver.WriteSnapshotData(snpsht, 0)
		if err != nil {
			return err
		}
	}
	return nil
}
