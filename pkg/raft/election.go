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

type Elector struct {
	parent *Raft
	rnd    *rand.Rand

	minElectionTimeout time.Duration
	maxElectionTimeout time.Duration
	resetTimerCh       chan struct{}
	cancelTimerCh      chan struct{}
}

func NewRaftElector(r *Raft) *Elector {
	src := rand.NewSource(time.Now().UnixNano() + int64(r.id))
	re := &Elector{
		parent: r,
		rnd:    rand.New(src),

		minElectionTimeout: 150 * time.Millisecond,
		maxElectionTimeout: 300 * time.Millisecond,

		resetTimerCh:  make(chan struct{}, 1),
		cancelTimerCh: make(chan struct{}),
	}
	go re.electionTimerLoop()
	return re
}

func (re *Elector) nextTimeout() time.Duration {
	return re.minElectionTimeout +
		time.Duration(rand.Int63n(int64(re.maxElectionTimeout-re.minElectionTimeout)))
}

func (re *Elector) electionTimerLoop() {
	timer := time.NewTimer(re.nextTimeout())
	defer timer.Stop()

	for {
		select {
		case <-re.resetTimerCh:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(re.nextTimeout())

		case <-timer.C:
			re.parent.mu.RLock()
			isLeader := re.parent.currentRole == Leader
			isSnapshotWriting := re.parent.snapshotter.installingSnapshot
			re.parent.mu.RUnlock()
			if !isLeader && !isSnapshotWriting {
				slog.Info(fmt.Sprintf("Starting election for node %d, because isLeader is %t and snapshot is writing is %t", re.parent.id, isLeader, isSnapshotWriting))
				re.parent.startElection()
			}
			timer.Reset(re.nextTimeout())

		case <-re.cancelTimerCh:
			return
		}
	}
}

func (re *Elector) ResetTimer() {
	select {
	case re.resetTimerCh <- struct{}{}:
		slog.Info(fmt.Sprintf("ResetTimer started for node %d", re.parent.id))
	default:
	}
}

func (r *Raft) startElection() {
	r.mu.Lock()

	r.raftElector.ResetTimer()
	fmt.Printf("Election started for node %d\n", r.id)
	r.currentRole = Candidate
	r.currentTerm++
	r.votedFor = r.id
	r.votesReceived = mapset.NewSet[int]()
	r.votesReceived.Add(r.id)

	logTerm := r.getLastLogTerm()

	voteData := VoteRequestData{r.id, r.currentTerm, len(r.log) + r.snapshotter.lastIndex, logTerm}
	totalNodes := r.totalNodes

	r.mu.Unlock()

	for i := 0; i < totalNodes; i++ {
		if i == r.id {
			continue
		}
		fmt.Printf("Sending vote request to node %d from %d\n", i, r.id)
		go r.sendVoteRequest(voteData, i)
	}
}

func (r *Raft) sendVoteRequest(data VoteRequestData, nodeId int) {
	peer := r.getPeer(nodeId)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		defer cancel()

		resp, err := peer.VoteRequest(ctx, &raftpb.VoteRequestArgs{
			CandidateId:        int32(data.candidateId),
			CandidateTerm:      int32(data.candidateTerm),
			CandidateLogLength: int32(data.candidateLogLength),
			CandidateLogTerm:   int32(data.candidateLogTerm),
		})
		if err != nil {
			return
		}

		r.receiveVote(VoteResponse{int(resp.NodeId), int(resp.CurrentTerm), resp.Granted})
	}()
}
