// The main Raft struct definition.
// The NewRaft constructor function.
// Perhaps the main Run loop or core goroutine management.
// State transitions (becomeFollower, becomeCandidate, becomeLeader - or these could be in state.go).
// Public methods for interaction (e.g., ApplyCommand).
package raft

import (
	"context"
	"math"
	"time"

	mapset "github.com/deckarep/golang-set/v2"
	"github.com/dominikszczepaniak/distributed-cache/pkg/raft/raftpb"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/wrapperspb"
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
	currentRole     Role
	currentLeaderId int
	votesReceived   mapset.Set[int]
	sentLengths     []int
	ackedLenghts    []int

	application Application

	peers []PeerClient
	conns []*grpc.ClientConn

	raftElector   *RaftElector       //takes care of elections
	logReplicator *RaftLogReplicator //sends logs to other servers
	logSaver      *RaftDataSaver     //takes care of saving data to persistent storage

	raftpb.UnimplementedRaftServer
}

type RaftFunctions interface {
	NewRaft() *Raft
	ReceiveVote(vote VoteResponse)
	ReplicateLog(id int, followerId int)
	Broadcast(message Message)
	AppendEntries(prefixLen, leaderCommit, suffix int)
	LogRequest(leaderId, currentTerm, prefixLen, prefixTerm, commitLength int, suffix []LogEntry)
	LogResponse(followerId, term, ack int, success bool)
	CommitLogEntries() //commits all messages to the application - cache
}

func NewRaft(application Application) *Raft {
	cfg := LoadConfig()

	r := &Raft{
		id:             cfg.raftId,
		totalNodes:     cfg.totalNodes,
		currentTerm:    0,
		votedFor:       -1,
		log:            []LogEntry{},
		commitedLength: 0,

		currentRole:     "follower",
		currentLeaderId: -1, //unknown
		votesReceived:   mapset.NewSet[int](),
		sentLengths:     make([]int, cfg.totalNodes),
		ackedLenghts:    make([]int, cfg.totalNodes),

		application: application,
	}
	r.logSaver = NewRaftDataSaver(r)
	r.raftElector = NewRaftElector(r)
	r.logReplicator = NewRaftLogReplicator(r)

	term, votedFor, commited, savedLog, err := r.logSaver.LoadValues()
	if err == nil {
		r.currentTerm = term
		r.votedFor = votedFor
		r.commitedLength = commited
		r.log = savedLog
	}

	r.initGRPC()
	return r
}

func (r *Raft) ReceiveVote(vote VoteResponse) {
	if r.currentTerm < vote.currentTerm {
		r.currentTerm = vote.currentTerm
		r.currentRole = "follower"
		select {
		case r.raftElector.resetTimerCh <- struct{}{}:
		default:
		}
	} else if vote.granted && r.currentTerm == vote.currentTerm && r.currentRole == "candidate" {
		r.votesReceived.Add(vote.nodeId)
		if r.votesReceived.Cardinality() >= int(math.Ceil(float64(r.totalNodes+1)/2)) {
			r.currentRole = "leader"
			r.currentLeaderId = r.id
			r.raftElector.ResetTimer()
			for followerId := 1; followerId < r.totalNodes; followerId++ {
				r.sentLengths[followerId] = len(r.log)
				r.ackedLenghts[followerId] = 0
				r.ReplicateLog(r.id, followerId)
			}

		}
	}
}

func (r *Raft) forwardToLeader(message Message, nodeId int) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	msg := &raftpb.Message{
		Type: raftpb.Message_Type(toProtoMsgType(message.msgType)),
		Key:  int32(message.key),
		Value: &wrapperspb.Int32Value{
			Value: int32(*message.value),
		},
	}
	if message.value != nil {
	}
	_, err := r.peers[nodeId].Forward(ctx, msg)

	if err != nil {
		panic("Network error") //TODO WHAT TO DO HERE?
	}
}

func (r *Raft) Broadcast(message Message) {
	if r.currentRole != "leader" {
		r.forwardToLeader(message, r.currentLeaderId) //response with Redirect http if not a leader?
	}
	r.log = append(r.log, LogEntry{
		message: message,
		term:    r.currentTerm,
	})
	go r.logSaver.SaveValues(int32(r.currentTerm), int32(r.votedFor), int32(len(r.log)), r.log)
	r.ackedLenghts[r.id] = len(r.log)
	for i := range r.totalNodes {
		if i == r.id {
			continue
		}
		r.ReplicateLog(r.id, i)
	}
}

func (r *Raft) DelieverToApplication(message Message) (success bool, value int) { //applies message to cache
	return r.application.AppendMessage(message)
}

func (r *Raft) AppendEntries(prefixLen, leaderCommit int, suffix []LogEntry) {
	if len(suffix) > 0 && len(r.log) > prefixLen {
		index := int(math.Min(float64(len(r.log)), float64(prefixLen+len(suffix))) - 1)
		if r.log[index].term != suffix[index-prefixLen].term {
			r.log = r.log[:prefixLen]
		}
	}
	if prefixLen+len(suffix) > len(r.log) {
		for i := len(r.log) - prefixLen; i < len(suffix); i++ {
			r.log = append(r.log, suffix[i])
		}
	}
	if leaderCommit > r.commitedLength {
		for i := r.commitedLength; i < leaderCommit; i++ {
			r.DelieverToApplication(r.log[i].message)
		}
		r.commitedLength = leaderCommit
	}
	go r.logSaver.SaveValues(int32(r.currentTerm), int32(r.votedFor), int32(len(r.log)), r.log)
}

func (r *Raft) LogResponse(followerId, term, ack int, success bool) { //its received on Leader - followers sent this as GRPC
	if r.currentTerm < term {
		r.currentTerm = term
		r.currentRole = "follower"
		r.votedFor = -1
		r.raftElector.ResetTimer()
	}
	if r.currentTerm == term && r.currentRole == "leader" {
		if success && ack >= r.ackedLenghts[followerId] {
			r.ackedLenghts[followerId] = ack
			r.sentLengths[followerId] = ack
			r.CommitLogEntries()
		} else if r.sentLengths[followerId] > 0 {
			r.sentLengths[followerId]--
			r.ReplicateLog(r.id, followerId)
		}
	}
}

func (r *Raft) CommitLogEntries() {
	majority := int(math.Ceil(float64(r.totalNodes+1) / 2))
	for r.commitedLength < len(r.log) {
		acks := 1
		for j := range r.totalNodes {
			if j == r.id {
				continue
			}
			if r.ackedLenghts[j] > r.commitedLength {
				acks++
			}
		}
		if acks >= majority {
			r.DelieverToApplication(r.log[r.commitedLength].message)
			r.commitedLength++
		} else {
			break
		}

	}
}
