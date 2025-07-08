// All shared struct definitions for RPC arguments/replies, log entries, configuration entries, etc.
// Enums for Raft states.
// This keeps all data model definitions in one place

package raft 

type Role struct {
	name string
}

type VoteRequest struct {
	candidateId int 
	candidateTerm int 
	candidateLogLength int 
	candidateLogTerm int 
}

type VoteResponse struct {
	nodeId int 
	currentTerm int 
	granted bool 
}