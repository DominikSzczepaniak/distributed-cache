// TODO
// 2. Change message type, we dont really care about the actual message, we just care about the term from the message, so we can accept whatever
package raft

import (
	"context"
	"fmt"

	//"log/slog"
	"math"
	"sync"
	"sync/atomic"
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
	//snapshots
	lastSnapshotIndex int
	lastSnapshotTerm  int
	snapshotThreshold int

	// Volatile state on all servers
	currentRole     Role
	currentLeaderId int
	votesReceived   mapset.Set[int]
	sentLengths     []int
	ackedLengths    []int

	application Application

	peers atomic.Value
	conns []*grpc.ClientConn

	raftElector   *Elector       //takes care of elections
	logReplicator *LogReplicator //sends logs to other servers
	logSaver      *DataSaver     //takes care of saving data to persistent storage
	heartbeat     *Heartbeat
	replicators   []*Replicator

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

		currentRole:       "follower",
		currentLeaderId:   -1, //unknown
		votesReceived:     mapset.NewSet[int](),
		sentLengths:       make([]int, cfg.totalNodes),
		ackedLengths:      make([]int, cfg.totalNodes),
		lastSnapshotIndex: 0,
		lastSnapshotTerm:  0,
		snapshotThreshold: 10000,

		application: application,
	}
	r.logSaver = NewRaftDataSaver(r, cfg)
	r.raftElector = NewRaftElector(r)
	r.logReplicator = NewRaftLogReplicator(r)
	r.heartbeat = newHeartbeat(r)

	term, votedFor, commited, savedLog, snapshot, err := r.logSaver.LoadValues()
	if err == nil {
		fmt.Printf("Loaded values from file: %d %d %d, log length: %d\n", term, votedFor, commited, len(savedLog))
		r.currentTerm = term
		r.votedFor = votedFor
		r.commitedLength = commited
		r.log = savedLog
		if snapshot != nil {
			r.lastSnapshotIndex = snapshot.LastIncludedIndex
			r.lastSnapshotTerm = snapshot.LastIncludedTerm
			if err := r.application.RestoreFromSnapshot(snapshot.Data); err != nil {
				panic(fmt.Sprintf("failed to restore snapshot on startup: %v", err))
			}
			r.commitedLength = r.lastSnapshotIndex
		}
	}

	r.initGRPC(cfg)
	return r
}

func (r *Raft) receiveVote(vote VoteResponse) {
	r.mu.Lock()
	defer r.mu.Unlock()
	//slog.Info(fmt.Sprintf("Received vote response from %d on node %d, granted: %t, node on term: %d, node from term: %d, node on role: %s",
	//	vote.nodeId,
	//	r.id,
	//	vote.granted,
	//	r.currentTerm,
	//	vote.currentTerm,
	//	r.currentRole))
	if r.currentTerm < vote.currentTerm {
		//slog.Info("Stepping down due to higher term in vote response",
		//	slog.Int("nodeId", r.id),
		//	slog.Int("oldTerm", r.currentTerm),
		//	slog.Int("newTerm", vote.currentTerm))
		r.currentTerm = vote.currentTerm
		r.currentRole = Follower
		go r.raftElector.ResetTimer()
		return
	}
	if vote.granted && r.currentTerm == vote.currentTerm && r.currentRole == Candidate {
		r.votesReceived.Add(vote.nodeId)
		//slog.Info("Vote counted",
		//	slog.Int("for node", r.id),
		//	slog.Int("current count", r.votesReceived.Cardinality()),
		//	slog.Int("total nodes", r.totalNodes))

		majority := int(math.Ceil(float64(r.totalNodes+1) / 2))

		if r.votesReceived.Cardinality() >= majority {
			//slog.Info("Node became leader",
			//	slog.Int("nodeId", r.id),
			//	slog.Int("term", r.currentTerm))

			r.becomeLeader()
		}
	}
}

func (r *Raft) forwardToLeader(message Message, leader PeerClient) {
	//slog.Info(fmt.Sprintf("Forwarding message {msgType %s, key %d, value %d} to leader", message.msgType, message.key, message.value))
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	msg := &raftpb.Message{
		Type: toProtoMsgType(message.MsgType),
		Key:  int32(message.Key),
		Value: &wrapperspb.Int32Value{
			Value: int32(*message.Value),
		},
	}
	if message.Value != nil {
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
		peer := r.getPeer(leaderID)
		if peer == nil {
			//slog.Warn("leader unknown – dropping client message")
			return
		}
		r.forwardToLeader(message, peer)
		return
	}
	r.mu.Lock()
	//slog.Info(fmt.Sprintf("Appending message {msgType %s, key %d, value %d} to node %d logs", message.msgType, message.key, *message.value, r.id))
	r.log = append(r.log, LogEntry{
		Message: message,
		Term:    r.currentTerm,
	})
	r.ackedLengths[r.id] = len(r.log)

	totalNodes := r.totalNodes

	replicatorsToSignal := make([]*Replicator, len(r.replicators))
	copy(replicatorsToSignal, r.replicators)
	r.mu.Unlock()

	go r.logSaver.SaveValues()

	for i := range totalNodes {
		if i == r.id {
			continue
		}
		if r.replicators[i] != nil {
			replicatorsToSignal[i].signal()
		}
	}
}

func (r *Raft) deliverToApplication(message Message) (success bool, value int) {
	return r.application.AppendMessage(message)
}

func (r *Raft) appendEntries(prefixLen, leaderCommit int, suffix []LogEntry) {
	r.mu.Lock()
	defer r.mu.Unlock()

	//slog.Info(fmt.Sprintf("Appending entries on node %d, current role %s", r.id, r.currentRole)) //TODO spamming too much and possibly slowing entire thing, consider batching
	//for i, e := range suffix {
	//	slog.Info(fmt.Sprintf("Entry %d: term %d, msgType %s, key %d, value %d", i, e.term, e.message.msgType, e.message.key, e.message.value))
	//}
	if len(suffix) > 0 && len(r.log) > prefixLen {
		//slog.Info(fmt.Sprintf("Went into 1st loop"))
		index := int(math.Min(float64(len(r.log)), float64(prefixLen+len(suffix))) - 1)
		if r.log[index].Term != suffix[index-prefixLen].Term {
			r.log = r.log[:prefixLen]
		}
	}
	if prefixLen+len(suffix) > len(r.log) {
		//slog.Info(fmt.Sprintf("Went into 2nd loop"))
		for i := len(r.log) - prefixLen; i < len(suffix); i++ {
			r.log = append(r.log, suffix[i])
		}
	}
	if leaderCommit > r.commitedLength {
		messagesToApply := make([]Message, 0, leaderCommit-r.commitedLength)
		for i := r.commitedLength; i < leaderCommit; i++ {
			messagesToApply = append(messagesToApply, r.log[i].Message)
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
			messagesToApply = append(messagesToApply, r.log[r.commitedLength].Message)
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
	if r.currentRole == Leader && r.getLastLogIndex()-r.lastSnapshotIndex > r.snapshotThreshold {
		r.takeSnapshot()
	}
}

func (r *Raft) getLogIndex(i int) int {
	return r.lastSnapshotIndex + i + 1
}

func (r *Raft) getTermForIndex(logIndex int) int {
	if logIndex < r.lastSnapshotIndex {
		// This should not happen if logic is correct
		return 0
	}
	if logIndex == r.lastSnapshotIndex {
		return r.lastSnapshotTerm
	}
	return r.log[logIndex-r.lastSnapshotIndex-1].Term
}

func (r *Raft) getLastLogIndex() int {
	return r.lastSnapshotIndex + len(r.log)
}

func (r *Raft) takeSnapshot() {
	snapshotData, err := r.application.GetSnapshot()
	if err != nil {
		fmt.Printf("Failed to get snapshot from application: %v\n", err)
		return
	}

	lastIndex := r.commitedLength
	lastTerm := r.getTermForIndex(lastIndex)

	snapshot := &Snapshot{
		LastIncludedIndex: lastIndex,
		LastIncludedTerm:  lastTerm,
		Data:              snapshotData,
	}

	if err := r.logSaver.SaveSnapshot(snapshot); err != nil {
		fmt.Printf("Failed to save snapshot: %v\n", err)
		return
	}

	// Compact the log
	newLog := make([]LogEntry, 0)
	for i := range r.log {
		if r.getLogIndex(i) > lastIndex {
			newLog = append(newLog, r.log[i])
		}
	}
	r.log = newLog
	r.lastSnapshotIndex = lastIndex
	r.lastSnapshotTerm = lastTerm

	// Persist the compacted log and updated metadata
	go r.logSaver.SaveValues()
}
