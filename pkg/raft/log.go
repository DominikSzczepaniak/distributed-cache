// Functions related to the Raft log: appending entries, committing entries, applying entries to the state machine, log truncation.
// Log entry struct definitions

package raft

type LogEntry struct {
	Term int 
	Command interface{}
}

