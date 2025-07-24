// The main Raft struct definition.
// The NewRaft constructor function.
// Perhaps the main Run loop or core goroutine management.
// State transitions (becomeFollower, becomeCandidate, becomeLeader - or these could be in state.go).
// Public methods for interaction (e.g., ApplyCommand).
package raft

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"os"
	"sync"
	"time"

	mapset "github.com/deckarep/golang-set/v2"
	"github.com/dominikszczepaniak/distributed-cache/pkg/raft/raftpb"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

type Raft struct {
	mu sync.RWMutex

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

	logger *slog.Logger

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

func NewRaft(application Application, cfg *Config) *Raft {
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
		logger:      slog.New(slog.NewTextHandler(os.Stdout, nil)),
	}
	r.logSaver = NewRaftDataSaver(r, cfg)
	r.raftElector = NewRaftElector(r)
	r.logReplicator = NewRaftLogReplicator(r)

	term, votedFor, commited, savedLog, err := r.logSaver.LoadValues()
	if err == nil {
		r.currentTerm = term
		r.votedFor = votedFor
		r.commitedLength = commited
		r.log = savedLog
	}

	r.initGRPC(cfg)
	return r
}

func (r *Raft) ReceiveVote(vote VoteResponse) {
	r.mu.Lock()
	defer r.mu.Unlock()
	slog.Info(fmt.Sprintf("Received vote response from %d on node %d, granted: %t, node on term: %d, node from term: %d, node on role: %s",
		vote.nodeId,
		r.id,
		vote.granted,
		r.currentTerm,
		vote.currentTerm,
		r.currentRole))
	if r.currentTerm < vote.currentTerm {
		slog.Info("Stepping down due to higher term in vote response",
			slog.Int("nodeId", r.id),
			slog.Int("oldTerm", r.currentTerm),
			slog.Int("newTerm", vote.currentTerm))
		r.currentTerm = vote.currentTerm
		r.currentRole = Follower
		//r.mu.Unlock()
		go r.raftElector.ResetTimer()
		return
	}
	if vote.granted && r.currentTerm == vote.currentTerm && r.currentRole == Candidate {
		r.votesReceived.Add(vote.nodeId)
		slog.Info("Vote counted",
			slog.Int("for node", r.id),
			slog.Int("current count", r.votesReceived.Cardinality()),
			slog.Int("total nodes", r.totalNodes))

		majority := int(math.Ceil(float64(r.totalNodes+1) / 2))

		if r.votesReceived.Cardinality() >= majority {
			slog.Info("Node became leader",
				slog.Int("nodeId", r.id),
				slog.Int("term", r.currentTerm))

			r.currentRole = Leader
			r.currentLeaderId = r.id

			for followerId := 0; followerId < r.totalNodes; followerId++ {
				if followerId == r.id {
					continue
				}
				r.sentLengths[followerId] = len(r.log)
				r.ackedLenghts[followerId] = 0
				go r.ReplicateLog(r.id, followerId)
			}
			go r.raftElector.ResetTimer()
		}
	}
}

func (r *Raft) forwardToLeader(message Message, nodeId int) {
	slog.Info(fmt.Sprintf("Forwarding message {msgType %s, key %d, value %d} to leader %d", message.msgType, message.key, message.value, nodeId))
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
	r.mu.Lock()
	isLeader := r.currentRole == Leader
	leaderID := r.currentLeaderId
	r.mu.Unlock()

	if !isLeader {
		r.mu.Lock()
		peer := r.peers[leaderID]
		defer r.mu.Unlock()
		if peer == nil {
			slog.Warn("leader unknown – dropping client message")
			return
		}
		r.forwardToLeader(message, leaderID)
		return
	}
	r.mu.Lock()
	slog.Info(fmt.Sprintf("Appending message {msgType %s, key %d, value %d} to node %d logs", message.msgType, message.key, *message.value, r.id))
	r.log = append(r.log, LogEntry{
		message: message,
		term:    r.currentTerm,
	})
	r.ackedLenghts[r.id] = len(r.log)

	currentTerm := r.currentTerm
	votedFor := r.votedFor
	logLen := len(r.log)
	logCopy := make([]LogEntry, len(r.log))
	copy(logCopy, r.log)
	totalNodes := r.totalNodes
	nodeId := r.id
	r.mu.Unlock()

	go r.logSaver.SaveValues(int32(currentTerm), int32(votedFor), int32(logLen), logCopy)

	for i := range totalNodes {
		if i == r.id {
			continue
		}
		go r.ReplicateLog(nodeId, i)
	}
}

func (r *Raft) DelieverToApplication(message Message) (success bool, value int) { //applies message to cache
	return r.application.AppendMessage(message)
}

func (r *Raft) AppendEntries(prefixLen, leaderCommit int, suffix []LogEntry) {
	r.mu.Lock()
	defer r.mu.Unlock()

	slog.Info(fmt.Sprintf("Appending entries on node %d, current role %s", r.id, r.currentRole))
	for i, e := range suffix {
		slog.Info(fmt.Sprintf("Entry %d: term %d, msgType %s, key %d, value %d", i, e.term, e.message.msgType, e.message.key, e.message.value))
	}
	if len(suffix) > 0 && len(r.log) > prefixLen {
		slog.Info(fmt.Sprintf("Went into 1st loop"))
		index := int(math.Min(float64(len(r.log)), float64(prefixLen+len(suffix))) - 1)
		if r.log[index].term != suffix[index-prefixLen].term {
			r.log = r.log[:prefixLen]
		}
	}
	if prefixLen+len(suffix) > len(r.log) {
		slog.Info(fmt.Sprintf("Went into 2nd loop"))
		for i := len(r.log) - prefixLen; i < len(suffix); i++ {
			r.log = append(r.log, suffix[i])
		}
	}
	if leaderCommit > r.commitedLength {
		messagesToApply := make([]Message, 0, leaderCommit-r.commitedLength)
		for i := r.commitedLength; i < leaderCommit; i++ {
			messagesToApply = append(messagesToApply, r.log[i].message)
		}
		r.commitedLength = leaderCommit
		r.mu.Unlock()

		for _, msg := range messagesToApply {
			r.DelieverToApplication(msg)
		}

		r.mu.Lock()
	}
}

func (r *Raft) LogResponse(followerId, term, ack int, success bool) { //its received on Leader - followers sent this as GRPC
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.currentTerm < term {
		r.currentTerm = term
		r.currentRole = Follower
		r.votedFor = -1
		r.raftElector.ResetTimer()
	}
	if r.currentTerm == term && r.currentRole == Leader {
		if success && ack >= r.ackedLenghts[followerId] {
			r.ackedLenghts[followerId] = ack
			r.sentLengths[followerId] = ack
			r.CommitLogEntries()
		} else if r.sentLengths[followerId] > 0 {
			r.sentLengths[followerId]--
			go r.ReplicateLog(r.id, followerId)
		}
	}
}

func (r *Raft) CommitLogEntries() {
	majority := int(math.Ceil(float64(r.totalNodes+1) / 2))
	messagesToApply := make([]Message, 0)

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
			messagesToApply = append(messagesToApply, r.log[r.commitedLength].message)
			r.commitedLength++
		} else {
			break
		}
	}

	if len(messagesToApply) > 0 {
		r.mu.Unlock()
		for _, msg := range messagesToApply {
			r.DelieverToApplication(msg)
		}
		r.mu.Lock()
	}
}
