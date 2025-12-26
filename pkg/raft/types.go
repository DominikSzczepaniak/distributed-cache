package raft

// Role represents the current state of a Raft node (Follower, Candidate, or Leader).
type Role string

const (
	Follower  Role = "follower"
	Candidate Role = "candidate"
	Leader    Role = "leader"
)

// VoteRequestData contains the information sent by a Candidate when requesting votes from peers.
type VoteRequestData struct {
	candidateId        int
	candidateTerm      int
	candidateLogLength int
	candidateLogTerm   int
}

// VoteResponse contains the result of a vote request.
type VoteResponse struct {
	nodeId      int
	currentTerm int
	granted     bool
}

// BroadcastResponse is returned after a message is proposed to the cluster via BroadcastSync.
type BroadcastResponse struct {
	// Success indicates if the message was successfully committed to the log.
	Success bool
	// Value is the result returned by the application state machine (if any).
	Value int
	// Error contains the reason for failure if Success is false.
	Error error
}
