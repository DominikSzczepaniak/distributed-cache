package raft

import (
	"os"
)

type Snapshot struct {
	lastIndex int
	lastTerm  int
	state     map[int]int
}

//TODO
//on new snapshot do
//1. delete all previous logs until lastIndex - TODO in some higher logic func
//2. delete all previous snapshots - DONE with O_CREATE on offset 0

func (rds *DataSaver) openFileForWriting(path string, flags int) (*os.File, error) {
	return os.OpenFile(path, flags, 0644)
}

func (rds *DataSaver) WriteSnapshotData(data []byte, offset int) (int, error) {
	var f *os.File
	var err error

	if offset == 0 {
		flags := os.O_WRONLY | os.O_CREATE | os.O_APPEND
		f, err = rds.openFileForWriting(rds.snapshotFilename, flags)
	} else {
		flags := os.O_WRONLY | os.O_APPEND
		f, err = rds.openFileForWriting(rds.snapshotFilename, flags)
	}
	if err != nil {
		return 0, err
	}
	defer f.Close()

	bytesWritten, err := f.Write(data)
	if err != nil {
		return bytesWritten, err
	}

	return bytesWritten, nil
}
