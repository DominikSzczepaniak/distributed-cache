package raft

import (
	"context"
	"fmt"
	"log/slog"
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

	// Volatile state on all servers
	currentRole     Role
	currentLeaderId int
	votesReceived   mapset.Set[int]
	sentLengths     []int
	ackedLengths    []int

	application Application

	peers atomic.Value
	conns []*grpc.ClientConn

	raftElector   *Elector
	logReplicator *LogReplicator
	logSaver      *DataSaver
	heartbeat     *Heartbeat
	replicators   []*Replicator
	snapshotter   *Snapshot

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
	}
	r.logSaver = NewRaftDataSaver(r, cfg)
	r.snapshotter = newSnapshotter(cfg)

	term, votedFor, commited, savedLog, err := r.logSaver.LoadValues()
	if err == nil {
		fmt.Printf("Loaded values from file: %d %d %d, log length: %d\n", term, votedFor, commited, len(savedLog))
		r.currentTerm = term
		r.votedFor = votedFor
		r.commitedLength = commited
		r.log = savedLog
	}
	snapshotData, err := r.logSaver.ReadSnapshotData()
	if err != nil {
		r.application.RestoreFromSnapshot(snapshotData)
	}

	r.raftElector = NewRaftElector(r)
	r.logReplicator = NewRaftLogReplicator(r)
	r.heartbeat = newHeartbeat(r)

	r.initGRPC(cfg)
	return r
}

func (r *Raft) receiveVote(vote VoteResponse) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.currentTerm < vote.currentTerm {
		r.currentTerm = vote.currentTerm
		r.currentRole = Follower
		go r.raftElector.ResetTimer()
		return
	}
	if vote.granted && r.currentTerm == vote.currentTerm && r.currentRole == Candidate {
		r.votesReceived.Add(vote.nodeId)

		majority := int(math.Ceil(float64(r.totalNodes+1) / 2))

		if r.votesReceived.Cardinality() >= majority {
			r.becomeLeader()
		}
	}
}

func (r *Raft) forwardToLeader(message Message, leader PeerClient) {
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
	isLeader, leaderID := r.getLeaderData()

	if !isLeader {
		peer := r.getPeer(leaderID)
		if peer == nil {
			return
		}
		r.forwardToLeader(message, peer)
		return
	}
	r.mu.Lock()
	slog.Info(fmt.Sprintf("Received new log call, current length: %d", len(r.log)))
	r.log = append(r.log, LogEntry{
		Message: message,
		Term:    r.currentTerm,
	})
	r.ackedLengths[r.id] = len(r.log) + r.snapshotter.lastIndex

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

	absoluteLogLength := len(r.log) + r.snapshotter.lastIndex
	if len(suffix) > 0 && absoluteLogLength > prefixLen {
		checkIndex := prefixLen
		relativeCheckIndex := checkIndex - r.snapshotter.lastIndex
		suffixCheckIndex := 0

		if relativeCheckIndex >= 0 && relativeCheckIndex < len(r.log) {
			if r.log[relativeCheckIndex].Term != suffix[suffixCheckIndex].Term {
				r.log = r.log[:relativeCheckIndex]
			}
		}
	}
	if prefixLen+len(suffix) > absoluteLogLength {
		startAppendingIndex := absoluteLogLength - prefixLen
		if startAppendingIndex < 0 {
			startAppendingIndex = 0
		}
		if startAppendingIndex < len(suffix) {
			r.log = append(r.log, suffix[startAppendingIndex:]...)
		}
	}
	if leaderCommit > r.commitedLength {
		//TODO check correctness
		maxCommit := len(r.log) + r.snapshotter.lastIndex
		if leaderCommit > maxCommit {
			leaderCommit = maxCommit
		}

		messagesToApply := make([]Message, 0, leaderCommit-r.commitedLength)
		for i := r.commitedLength; i < leaderCommit; i++ {
			relativeIndex := i - r.snapshotter.lastIndex
			messagesToApply = append(messagesToApply, r.log[relativeIndex].Message) //throws error sometimes
		}
		r.commitedLength = leaderCommit

		for _, msg := range messagesToApply {
			r.deliverToApplication(msg)
		}
	}
	err := r.decideRunSnapshot()
	if err != nil {
		return
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
			messagesToApply = append(messagesToApply, r.log[r.commitedLength-r.snapshotter.lastIndex].Message)
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

func (r *Raft) sendInstallSnapshotRPC(followerId int) {
	slog.Info(fmt.Sprintf("Follower %d is too far behind. Sending snapshot.", followerId))

	peer := r.getPeer(followerId)

	r.mu.RLock()
	lastIndex := r.snapshotter.lastIndex
	lastTerm := r.snapshotter.lastTerm
	currentTerm := r.currentTerm
	leaderId := r.id

	snapshotData, err := r.logSaver.ReadSnapshotData()
	if err != nil {
		r.mu.RUnlock()
		slog.Error(fmt.Sprintf("Failed to read snapshot data for follower %d: %v", followerId, err))
		return
	}
	r.mu.RUnlock()

	request := &raftpb.InstallSnapshotRequest{
		LeaderTerm:        int32(currentTerm),
		LeaderId:          int32(leaderId),
		LastIncludedIndex: int32(lastIndex),
		LastIncludedTerm:  int32(lastTerm),
		Offset:            0,
		Data:              snapshotData,
		Done:              true,
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*200)
	defer cancel()
	resp, err := peer.InstallSnapshot(ctx, request)
	if err != nil {
		slog.Warn(fmt.Sprintf("InstallSnapshot RPC to follower %d failed: %v", followerId, err))
		return
	}

	// --- Step 4: Process the response ---
	r.mu.Lock()
	defer r.mu.Unlock()

	// If the follower's term is higher, we are an outdated leader. Revert to follower.
	if int(resp.Term) > r.currentTerm {
		r.currentTerm = int(resp.Term)
		r.currentRole = Follower
		r.votedFor = -1
		// It's a good practice to reset the election timer here as well.
		go r.raftElector.ResetTimer()
		return
	}

	// If the RPC was successful, update the leader's view of the follower's progress.
	// The follower is now caught up to the snapshot's index.
	// The next log entry to send will be at lastIndex + 1.
	r.sentLengths[followerId] = lastIndex + 1
	r.ackedLengths[followerId] = lastIndex // The follower has acknowledged all entries up to the snapshot.
	slog.Info(fmt.Sprintf("Successfully installed snapshot on follower %d. Next index is now %d.", followerId, r.sentLengths[followerId]))
}
