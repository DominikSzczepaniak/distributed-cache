// Functions for reading and writing Raft's persistent state (current term, voted for, log entries) to disk.
// Logic for recovering state on restart.
package raft

import "os"

type RaftDataSaver struct {
	parent *Raft 
	valuesFilename string 
	
	RaftDataSaverFunctions
}

type RaftDataSaverFunctions interface{
	SaveValues(currentTerm, votedFor, commitedLength int32, logs []LogEntry) (bool, error)
	LoadValues() (int, int, int, []LogEntry, error)

	saveLog(logs []LogEntry, file *os.File) (bool, error)
	loadLog() ([]LogEntry, error)
}

func NewRaftDataSaver(r *Raft) *RaftDataSaver{
	valuesFilename := os.Getenv("VALUES_FILENAME")
	if valuesFilename == ""{
		panic("Specify the name of VALUES_FILENAME environment variable")
	}
	return &RaftDataSaver{
		parent: r,
		valuesFilename: valuesFilename,
	}
}

func (rds *RaftDataSaver) saveLog(logs []LogEntry, file *os.File) (bool, error){
	//TODO
	return true, nil
}

func (rds *RaftDataSaver) loadLog() ([]LogEntry, error){
	//TODO
	return []LogEntry{}, nil
}

func (rds *RaftDataSaver) SaveValues(currentTerm, votedFor, commitedLength int32, logs []LogEntry) (bool, error){
	//TODO
	return true, nil
}

func (rds *RaftDataSaver) LoadValues() (int, int, int, []LogEntry, error){
	//TODO
	return 0,0,0, []LogEntry{}, nil
}