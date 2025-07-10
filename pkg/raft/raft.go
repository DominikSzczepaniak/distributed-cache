// The main Raft struct definition.
// The NewRaft constructor function.
// Perhaps the main Run loop or core goroutine management.
// State transitions (becomeFollower, becomeCandidate, becomeLeader - or these could be in state.go).
// Public methods for interaction (e.g., ApplyCommand).
package raft

import (
	"math"
	"os"
	"strconv"
	"time"

	mapset "github.com/deckarep/golang-set/v2"
)

type Raft struct {
	id         int
	totalNodes int
	// Stable storage variables
	currentTerm    int
	votedFor       int
	log            []LogEntry
	commitedLength int

	// Volatile state on all servers
	currentRole   Role
	currentLeader *Raft // or int?
	votesReceived mapset.Set[int]
	sentLengths   []int
	ackedLenghts  []int

	RaftElection
}

func NewRaft() *Raft {
	totalNodesStr, exists := os.LookupEnv("TOTAL_NODES")
	if !exists {
		panic("TOTAL NODES NOT DEFINED!")
	}
	totalNodes, err := strconv.Atoi(totalNodesStr)
	if err != nil {
		panic("TOTAL NODES IS NOT A NUMBER!")
	}
	r := &Raft{
		id:             0, //we only keep one raft on each machine so lets always assume that 0 is us. We need to somehow tell other Raft nodes what ID are we (who tf are we?)
		totalNodes:     totalNodes,
		currentTerm:    0,
		votedFor:       -1,
		log:            []LogEntry{},
		commitedLength: 0,

		currentRole:   "follower",
		currentLeader: nil,
		votesReceived: mapset.NewSet[int](),
		sentLengths:   make([]int, totalNodes),
		ackedLenghts:  make([]int, totalNodes),
	}

	r.RaftElection = RaftElection{
		parent: r,

		minElectionTimeout: 100 * time.Millisecond, //change if needed
		maxElectionTimeout: 300 * time.Millisecond,

		resetTimerCh:  make(chan struct{}, 1),
		cancelTimerCh: make(chan struct{}),
	}

	go r.electionTimerLoop()
	return r
}

func (r *Raft) ProposeLeader(voteRequest VoteRequest) (VoteResponse, error) {
	if voteRequest.candidateTerm > r.currentTerm {
		r.currentTerm = voteRequest.candidateTerm
		r.currentRole = "follower"
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
	} else {
		return VoteResponse{nodeId: r.id, currentTerm: r.currentTerm, granted: false}, nil
	}
}
func (r *Raft) ReceiveVote(vote VoteResponse) {
	if r.currentTerm < vote.currentTerm {
		r.currentTerm = vote.currentTerm
		r.currentRole = "follower"
		select {
		case r.resetTimerCh <- struct{}{}:
		default:
		}
	} else if vote.granted && r.currentTerm == vote.currentTerm && r.currentRole == "candidate" {
		r.votesReceived.Add(vote.nodeId)
		totalNodes, err := strconv.Atoi(os.Getenv("TOTAL_NODES"))
		if err != nil {
			panic("TOTAL_NODES not defined in environment")
		}
		if r.votesReceived.Cardinality() >= int(math.Ceil(float64(totalNodes+1)/2)) {
			r.currentRole = "leader"
			r.currentLeader = r
			select {
			case r.resetTimerCh <- struct{}{}:
			default:
			}
			for followerId := 1; followerId < r.totalNodes; followerId++ {
				r.sentLengths[followerId] = len(r.log)
				r.ackedLenghts[followerId] = 0
				r.ReplicateLog(r.id, followerId)
			}

		}
	}
}

func (r *Raft) ReplicateLog(id int, followerId int) {
	panic("unimplemented")
}
