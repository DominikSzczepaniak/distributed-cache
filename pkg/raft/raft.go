package raft

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"math"
	"os"
	"sync"
	"time"

	mapset "github.com/deckarep/golang-set/v2"
	"github.com/dominikszczepaniak/distributed-cache/pkg/raft/raftpb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// Raft implements the Raft consensus algorithm.
// It manages a replicated log, maintains consistent state across a cluster,
// and handles leader election and log compaction.
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

	connMgr   *ConnectionManager
	testPeers []PeerClient

	raftElector *Elector
	logSaver    *DataSaver
	heartbeat   *Heartbeat
	replicators []*Replicator
	snapshotter *Snapshot

	artificialDelay time.Duration

	raftpb.UnimplementedRaftServer
}

// NewRaft initializes a new Raft node with the provided application and configuration.
// It starts the background processes for leader election, log replication, and persistence.
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

		artificialDelay: cfg.artificialDelay,

		application: application,
	}
	r.logSaver = NewRaftDataSaver(r, cfg)
	r.snapshotter = newSnapshotter(cfg)

	term, votedFor, commited, savedLog, err := r.logSaver.LoadValues()
	if err == nil {
		slog.Info(fmt.Sprintf("Loaded values from file: term=%d votedFor=%d committed=%d logLength=%d", term, votedFor, commited, len(savedLog)))
		r.currentTerm = term
		r.votedFor = votedFor
		r.commitedLength = commited
		r.log = savedLog
	}
	snapshotData, err := r.logSaver.ReadSnapshotData()
	if err == nil {
		key, _ := r.application.RestoreFromSnapshot(snapshotData)
		slog.Info(fmt.Sprintf("When exited restore from snapshot, key value is %d", r.application.GetValue(key)))
	}

	r.heartbeat = newHeartbeat(r)
	r.raftElector = NewRaftElector(r)

	r.connMgr = NewConnectionManager(r.id, r.totalNodes, cfg.raftAddrs, cfg)

	go r.serveGRPC(cfg.raftAddrs[r.id])

	return r
}

func (r *Raft) receiveVote(vote VoteResponse) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.currentTerm < vote.currentTerm {
		r.currentTerm = vote.currentTerm
		r.currentRole = Follower
		r.raftElector.ResetTimer()
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

func (r *Raft) forwardToLeader(message Message, leader PeerClient) (bool, int, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	ctx = context.WithValue(ctx, forwardHopKey{}, true)

	msg := &raftpb.Message{
		Type:             toProtoMsgType(message.MsgType),
		Key:              int32(message.Key),
		IdempotencyToken: message.IdempotencyToken,
		ClientId:         message.ClientID,
		CommandPayload:   message.Data,
	}
	if message.Value != nil {
		msg.Value = wrapperspb.Int32(int32(*message.Value))
	}
	resp, err := leader.Forward(ctx, msg)
	if err != nil {
		resp, err = leader.Forward(ctx, msg) // Retry once
		if err != nil {
			slog.Warn("forwardToLeader failed", "from", r.id, "error", err)
			return false, 0, err
		}
	}
	return resp.Success, int(resp.Value), nil
}

// Broadcast asynchronously proposes a message to the cluster.
// It returns immediately after handing the message to the internal Raft loop.
func (r *Raft) Broadcast(message Message) {
	if r.artificialDelay > 0 {
		time.Sleep(r.artificialDelay)
	}
	isLeader, leaderID := r.getLeaderData()

	if !isLeader {
		peer := r.getPeer(leaderID)
		if peer == nil {
			if message.ResponseChan != nil {
				message.ResponseChan <- BroadcastResponse{Success: false, Error: fmt.Errorf("no leader known")}
			}
			return
		}

		go func() {
			success, value, err := r.forwardToLeader(message, peer)
			if message.ResponseChan != nil {
				message.ResponseChan <- BroadcastResponse{
					Success: success,
					Value:   value,
					Error:   err,
				}
			}
		}()
		return
	}
	r.mu.Lock()
	r.log = append(r.log, LogEntry{
		Message: message,
		Term:    r.currentTerm,
	})
	r.ackedLengths[r.id] = len(r.log) + r.snapshotter.lastIndex

	totalNodes := r.totalNodes

	reps := r.replicators
	r.mu.Unlock()

	go r.logSaver.SaveValues()

	for i := 0; i < totalNodes; i++ {
		if i == r.id {
			continue
		}
		if reps != nil && reps[i] != nil {
			reps[i].signal()
		}
	}
}

// BroadcastSync proposes a message to the cluster and waits for it to be committed.
// It returns true if the message was successfully replicated to a majority of nodes
// within the specified timeout.
func (r *Raft) BroadcastSync(message Message, timeout time.Duration) (bool, int, error) {
	responseChan := make(chan BroadcastResponse, 1)
	message.ResponseChan = responseChan

	r.Broadcast(message)

	select {
	case resp := <-responseChan:
		return resp.Success, resp.Value, resp.Error
	case <-time.After(timeout):
		return false, 0, fmt.Errorf("timeout waiting for response after %v", timeout)
	}
}

func (r *Raft) deliverToApplication(message Message) (success bool, value int) {
	success, value = r.application.AppendMessage(message)

	if message.ResponseChan != nil {
		select {
		case message.ResponseChan <- BroadcastResponse{
			Success: success,
			Value:   value,
			Error:   nil,
		}:
		default:
		}
	}

	return success, value
}

func (r *Raft) appendEntries(prefixLen, leaderCommit int, suffix []LogEntry) {
	r.mu.Lock()
	defer r.mu.Unlock()

	slog.Info(fmt.Sprintf("Node %d: appendEntries called (prefixLen=%d leaderCommit=%d suffixLen=%d myLogLen=%d myCommit=%d)",
		r.id, prefixLen, leaderCommit, len(suffix), len(r.log)+r.snapshotter.lastIndex, r.commitedLength))

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
		maxCommit := len(r.log) + r.snapshotter.lastIndex
		if leaderCommit > maxCommit {
			leaderCommit = maxCommit
		}

		slog.Info(fmt.Sprintf("Node %d: Committing entries from %d to %d (leaderCommit=%d)",
			r.id, r.commitedLength, leaderCommit, leaderCommit))

		for i := r.commitedLength; i < leaderCommit; i++ {
			relativeIndex := i - r.snapshotter.lastIndex
			msg := r.log[relativeIndex].Message
			slog.Info(fmt.Sprintf("Node %d: Delivering entry %d to application (key=%d value=%v type=%s)",
				r.id, i, msg.Key, msg.Value, msg.MsgType))
			r.deliverToApplication(msg)
		}
		r.commitedLength = leaderCommit
		slog.Info(fmt.Sprintf("Node %d: Commit complete, commitedLength now %d", r.id, r.commitedLength))
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
			r.sentLengths[followerId] = ack + 1
			r.commitLogEntries()
		} else if r.sentLengths[followerId] > r.snapshotter.lastIndex+1 {
			r.sentLengths[followerId]--
			if r.replicators != nil && r.replicators[followerId] != nil {
				r.replicators[followerId].signal()
			}
		}
	}
}

func (r *Raft) commitLogEntries() {
	majority := int(math.Ceil(float64(r.totalNodes+1) / 2))
	messagesToApply := make([]Message, 0)

	for r.commitedLength < r.snapshotter.lastIndex+len(r.log) {
		acks := 1
		for j := 0; j < r.totalNodes; j++ {
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
	const chunkSize = 128 * 1024

	r.mu.Lock()
	slog.Info(fmt.Sprintf("Follower %d is too far behind. Sending snapshot in blocks.", followerId))

	peer := r.getPeer(followerId)
	lastIndex := r.snapshotter.lastIndex
	lastTerm := r.snapshotter.lastTerm
	currentTerm := r.currentTerm
	leaderId := r.id
	snapPath := r.logSaver.snapshotFilename
	r.mu.Unlock()

	f, err := os.Open(snapPath)
	if err != nil {
		slog.Error(fmt.Sprintf("Failed to open snapshot file for follower %d: %v", followerId, err))
		return
	}
	defer f.Close()

	stat, err := f.Stat()
	if err != nil {
		slog.Error(fmt.Sprintf("Failed to stat snapshot file for follower %d: %v", followerId, err))
		return
	}
	totalSize := int(stat.Size())

	buf := make([]byte, chunkSize)
	offset := 0

	for {
		n, readErr := f.Read(buf)
		if n == 0 && readErr == io.EOF {
			break
		}
		if readErr != nil && readErr != io.EOF {
			slog.Error(fmt.Sprintf("Error reading snapshot file for follower %d: %v", followerId, readErr))
			return
		}

		done := (offset + n) >= totalSize
		dataChunk := append([]byte(nil), buf[:n]...)

		req := &raftpb.InstallSnapshotRequest{
			LeaderTerm:        int32(currentTerm),
			LeaderId:          int32(leaderId),
			LastIncludedIndex: int32(lastIndex),
			LastIncludedTerm:  int32(lastTerm),
			Offset:            int32(offset),
			Data:              dataChunk,
			Done:              done,
		}

		ctx, cancel := context.WithTimeout(context.Background(), 600*time.Millisecond)
		resp, err := peer.InstallSnapshot(ctx, req)
		cancel()
		if err != nil {
			slog.Warn(fmt.Sprintf("InstallSnapshot RPC to follower %d failed at offset %d: %v", followerId, offset, err))
			return
		}

		if int(resp.Term) > r.currentTerm {
			r.mu.Lock()
			if int(resp.Term) > r.currentTerm {
				r.currentTerm = int(resp.Term)
				r.currentRole = Follower
				r.votedFor = -1
				r.raftElector.ResetTimer()
			}
			r.mu.Unlock()
			return
		}

		offset += n

		if done {
			break
		}
	}

	r.mu.Lock()
	r.sentLengths[followerId] = lastIndex + 1
	r.ackedLengths[followerId] = lastIndex
	slog.Info(fmt.Sprintf("Successfully installed snapshot on follower %d (%d bytes in blocks). Next index is now %d.",
		followerId, totalSize, r.sentLengths[followerId]))
	r.mu.Unlock()
}
