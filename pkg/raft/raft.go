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
	RaftFunctions
	id         int 
	totalNodes int
	// Stable storage variables
	currentTerm    int
	votedFor       int
	log            []LogEntry
	commitedLength int

	// Volatile state on all servers
	currentRole   Role
	currentLeader *Raft // or int - nodeid of leader?
	votesReceived mapset.Set[int]
	sentLengths   []int
	ackedLenghts  []int

	RaftElection
	RaftLogReplicator
}

type RaftFunctions interface{
	NewRaft() *Raft 
	ProposeLeader(voteRequest VoteRequest) (VoteResponse, error)
	ReceiveVote(vote VoteResponse)
	ReplicateLog(id int, followerId int) 
	Broadcast(message Message) 
	Receive(message Message)
	AppendEntries(prefixLen, leaderCommit, suffix int)
	LogRequest(leaderId, currentTerm, prefixLen, prefixTerm, commitLength int, suffix []LogEntry)
	LogResponse(followerId, term, ack int, success bool) 
	CommitLogEntries() //commits all messages to the application - cache
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

	id, err := strconv.Atoi(os.Getenv("RAFT_ID"))
	if err != nil {
		panic("RAFT_ID not defined in environment or is not a number")
	}

	r := &Raft{
		id:             id, 
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
		lastTerm = r.log[len(r.log)-1].term
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

func (r *Raft) ForwardMessage(message Message, nodeId int){
	panic("unimplemented")
}

func (r *Raft) Broadcast(message Message) { 
	if r.currentRole != "leader"{
		r.ForwardMessage(message, r.currentLeader.id)
	}
	r.log = append(r.log, LogEntry{
		message: message,
		term: r.currentTerm,
	})
	r.ackedLenghts[r.id] = len(r.log)
	for i:=0; i<r.totalNodes; i++{
		if i == r.id{
			continue
		}
		r.ReplicateLog(r.id, i)
	}
}

func (r *Raft) Receive(message Message) {
	panic("unimplemented")
}

func (r *Raft) AppendEntries(prefixLen, leaderCommit, suffix int) {
	panic("unimplemented")
}

func (r *Raft) LogRequest(leaderId, currentTerm, prefixLen, prefixTerm, commitLength int, suffix []LogEntry) {
	panic("unimplemented")
}

func (r *Raft) LogResponse(followerId, term, ack int, success bool){
	panic("unimplemented")
}

func (r *Raft) CommitLogEntries(){
	panic("unimplemented")
}