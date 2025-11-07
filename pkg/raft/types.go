package raft

type Role string

const (
	Follower  Role = "follower"
	Candidate Role = "candidate"
	Leader    Role = "leader"
)

type VoteRequestData struct {
	candidateId        int
	candidateTerm      int
	candidateLogLength int
	candidateLogTerm   int
}

type VoteResponse struct {
	nodeId      int
	currentTerm int
	granted     bool
}

type BroadcastResponse struct {
	Success bool
	Value   int
	Error   error
}
