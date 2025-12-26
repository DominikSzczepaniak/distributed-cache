package raft

import (
	"context"
	"fmt"
	"log/slog"

	"math/rand"
	"time"

	mapset "github.com/deckarep/golang-set/v2"
	"github.com/dominikszczepaniak/distributed-cache/pkg/raft/raftpb"
)

// Elector manages the randomization and timing of leader elections.
// It ensures that if a leader is not heard from within a timeout period,
// the node will transition to the Candidate role and start a new election.
type Elector struct {
	parent *Raft
	rnd    *rand.Rand

	minElectionTimeout time.Duration
	maxElectionTimeout time.Duration
	resetTimerCh       chan struct{}
	cancelTimerCh      chan struct{}
}

// NewRaftElector initializes the election logic for a Raft node.
func NewRaftElector(r *Raft) *Elector {
	src := rand.NewSource(time.Now().UnixNano() + int64(r.id))
	re := &Elector{
		parent: r,
		rnd:    rand.New(src),

		minElectionTimeout: 400 * time.Millisecond,
		maxElectionTimeout: 800 * time.Millisecond,

		resetTimerCh:  make(chan struct{}, 1),
		cancelTimerCh: make(chan struct{}),
	}
	go re.electionTimerLoop()
	return re
}

func (re *Elector) nextTimeout() time.Duration {
	return re.minElectionTimeout +
		time.Duration(re.rnd.Int63n(int64(re.maxElectionTimeout-re.minElectionTimeout)))
}

// electionTimerLoop is the background process that resets the election timer
// and triggers an election if the timeout is reached without a heartbeat.
func (re *Elector) electionTimerLoop() {
	timer := time.NewTimer(re.nextTimeout())
	defer timer.Stop()

	slog.Info(fmt.Sprintf("Node %d: Election timer loop started", re.parent.id))

	for {
		select {
		case <-re.resetTimerCh:
			slog.Debug(fmt.Sprintf("Node %d: Election timer reset", re.parent.id))
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(re.nextTimeout())

		case <-timer.C:
			slog.Debug(fmt.Sprintf("Node %d: Election timer fired, checking if should start election", re.parent.id))
			re.parent.mu.RLock()
			isLeader := re.parent.currentRole == Leader
			isSnapshotWriting := re.parent.snapshotter.installingSnapshot
			role := re.parent.currentRole
			re.parent.mu.RUnlock()
			slog.Debug(fmt.Sprintf("Node %d: Election timer check complete (role=%s, isLeader=%t, isSnapshotWriting=%t)", re.parent.id, role, isLeader, isSnapshotWriting))
			if !isLeader && !isSnapshotWriting {
				slog.Info(fmt.Sprintf("Starting election for node %d, because isLeader is %t and snapshot is writing is %t", re.parent.id, isLeader, isSnapshotWriting))
				re.parent.startElection()
			}
			timer.Reset(re.nextTimeout())

		case <-re.cancelTimerCh:
			slog.Info(fmt.Sprintf("Node %d: Election timer loop cancelled", re.parent.id))
			return
		}
	}
}

// ResetTimer pushes a reset signal to the background election timer loop.
func (re *Elector) ResetTimer() {
	select {
	case re.resetTimerCh <- struct{}{}:
	default:
	}
}

func (r *Raft) startElection() {
	r.mu.Lock()

	r.raftElector.ResetTimer()
	slog.Debug(fmt.Sprintf("Election started for node %d", r.id))
	r.currentRole = Candidate
	r.currentTerm++
	r.votedFor = r.id
	r.votesReceived = mapset.NewSet[int]()
	r.votesReceived.Add(r.id)
	r.currentLeaderId = -1

	logTerm := r.getLastLogTerm()

	voteData := VoteRequestData{r.id, r.currentTerm, len(r.log) + r.snapshotter.lastIndex, logTerm}
	totalNodes := r.totalNodes

	r.mu.Unlock()

	for i := 0; i < totalNodes; i++ {
		if i == r.id {
			continue
		}
		slog.Debug(fmt.Sprintf("Sending vote request to node %d from %d", i, r.id))
		go r.sendVoteRequest(voteData, i)
	}
}

func (r *Raft) sendVoteRequest(data VoteRequestData, nodeId int) {
	if !r.isPeerAvailable(nodeId) {
		slog.Debug(fmt.Sprintf("Node %d: Skipping vote request to unavailable peer %d", r.id, nodeId))
		return
	}

	peer := r.getPeer(nodeId)
	if peer == nil {
		slog.Debug(fmt.Sprintf("Node %d: No peer client for node %d", r.id, nodeId))
		return
	}

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		resp, err := peer.VoteRequest(ctx, &raftpb.VoteRequestArgs{
			CandidateId:        int32(data.candidateId),
			CandidateTerm:      int32(data.candidateTerm),
			CandidateLogLength: int32(data.candidateLogLength),
			CandidateLogTerm:   int32(data.candidateLogTerm),
		})
		if err != nil {
			slog.Warn(fmt.Sprintf("Node %d: VoteRequest to peer %d failed: %v", r.id, nodeId, err))
			return
		}

		r.receiveVote(VoteResponse{int(resp.NodeId), int(resp.CurrentTerm), resp.Granted})
	}()
}
