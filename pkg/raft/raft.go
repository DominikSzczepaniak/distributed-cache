// The main Raft struct definition.
// The NewRaft constructor function.
// Perhaps the main Run loop or core goroutine management.
// State transitions (becomeFollower, becomeCandidate, becomeLeader - or these could be in state.go).
// Public methods for interaction (e.g., ApplyCommand).
package raft

import mapset "github.com/deckarep/golang-set/v2"

type Raft struct {
	id int
	// Stable storage variables
	currentTerm int 
	votedFor   int 
	log      []LogEntry 
	commitedLength int

	// Volatile state on all servers 
	currentRole Role 
	currentLeader *Raft // or int?
	votesReceived mapset.Set[int]
	sentLengths []int 
	ackedLenghts []int 

	timeout float32
}

func NewRaft() *Raft {
	return &Raft{
		currentTerm: 0,
		votedFor: -1,
		log: []LogEntry{},
		commitedLength: 0,

		currentRole: Role{name: "follower"}, 
		currentLeader: nil, 
		votesReceived: mapset.NewSet[int](),
		sentLengths: []int{},
		ackedLenghts: []int{},
	}
}

func (r Raft) Receiving(voteRequest VoteRequest) (VoteResponse, error) {
	if voteRequest.candidateTerm > r.currentTerm {
		r.currentTerm = voteRequest.candidateTerm 
		r.currentRole = Role{name: "follower"}
		r.votedFor = -1
	}
	lastTerm := 0 
	if len(r.log) > 0 {
		lastTerm = r.log[len(r.log)-1].Term
	}
	logOk := (voteRequest.candidateTerm > lastTerm) || (voteRequest.candidateLogTerm == lastTerm && voteRequest.candidateLogLength >= len(r.log))

	if voteRequest.candidateTerm == r.currentTerm && logOk && (r.votedFor == -1 || r.votedFor == voteRequest.candidateId) {
		r.votedFor = voteRequest.candidateId
		return VoteResponse{nodeId: r.id, currentTerm: r.currentTerm, granted: true}, nil
	} else{
		return VoteResponse{nodeId: r.id, currentTerm: r.currentTerm, granted: false}, nil
	}
}
