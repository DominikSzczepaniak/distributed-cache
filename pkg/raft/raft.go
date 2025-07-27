// TODO
// 2. Change message type, we dont really care about the actual message, we just care about the term from the message, so we can accept whatever
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
	ackedLengths    []int

	application Application

	peers []PeerClient
	conns []*grpc.ClientConn

	raftElector   *Elector       //takes care of elections
	logReplicator *LogReplicator //sends logs to other servers
	logSaver      *DataSaver     //takes care of saving data to persistent storage

	logger *slog.Logger

	raftpb.UnimplementedRaftServer
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
		ackedLengths:    make([]int, cfg.totalNodes),

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

func (r *Raft) receiveVote(vote VoteResponse) {
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
				r.ackedLengths[followerId] = 0
				go r.replicateLog(r.id, followerId)
			}
			go r.raftElector.ResetTimer()
		}
	}
}

func (r *Raft) forwardToLeader(message Message, leader PeerClient) {
	slog.Info(fmt.Sprintf("Forwarding message {msgType %s, key %d, value %d} to leader", message.msgType, message.key, message.value))
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	msg := &raftpb.Message{
		Type: toProtoMsgType(message.msgType),
		Key:  int32(message.key),
		Value: &wrapperspb.Int32Value{
			Value: int32(*message.value),
		},
	}
	if message.value != nil {
	}
	_, err := leader.Forward(ctx, msg)

	if err != nil {
		_, err = leader.Forward(ctx, msg) //retry once
		if err != nil {
			panic("Network error")
		}
	}
}

func (r *Raft) Broadcast(message Message) {
	r.mu.RLock()
	isLeader := r.currentRole == Leader
	leaderID := r.currentLeaderId
	r.mu.RUnlock()

	if !isLeader {
		r.mu.RLock()
		peer := r.peers[leaderID]
		r.mu.RUnlock()
		if peer == nil {
			slog.Warn("leader unknown – dropping client message")
			return
		}
		r.forwardToLeader(message, peer)
		return
	}
	r.mu.Lock()
	slog.Info(fmt.Sprintf("Appending message {msgType %s, key %d, value %d} to node %d logs", message.msgType, message.key, *message.value, r.id))
	r.log = append(r.log, LogEntry{
		message: message,
		term:    r.currentTerm,
	})
	r.ackedLengths[r.id] = len(r.log)

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
		go r.replicateLog(nodeId, i)
	}
}

func (r *Raft) deliverToApplication(message Message) (success bool, value int) {
	return r.application.AppendMessage(message)
}

func (r *Raft) appendEntries(prefixLen, leaderCommit int, suffix []LogEntry) {
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
			r.deliverToApplication(msg)
		}

		r.mu.Lock()
	}
}

func (r *Raft) logResponse(followerId, term, ack int, success bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.currentTerm < term {
		r.currentTerm = term
		r.currentRole = Follower
		r.votedFor = -1
		r.raftElector.ResetTimer()
	}
	if r.currentTerm == term && r.currentRole == Leader {
		if success && ack >= r.ackedLengths[followerId] {
			r.ackedLengths[followerId] = ack
			r.sentLengths[followerId] = ack
			r.commitLogEntries()
		} else if r.sentLengths[followerId] > 0 {
			r.sentLengths[followerId]--
			go r.replicateLog(r.id, followerId)
		}
	}
}

func (r *Raft) commitLogEntries() {
	majority := int(math.Ceil(float64(r.totalNodes+1) / 2))
	messagesToApply := make([]Message, 0)

	for r.commitedLength < len(r.log) {
		acks := 1
		for j := range r.totalNodes {
			if j == r.id {
				continue
			}
			if r.ackedLengths[j] > r.commitedLength {
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
			r.deliverToApplication(msg)
		}
		r.mu.Lock()
	}
}
