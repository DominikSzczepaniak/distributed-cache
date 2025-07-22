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

type RaftElector struct {
	parent             *Raft
	minElectionTimeout time.Duration
	maxElectionTimeout time.Duration
	resetTimerCh       chan struct{}
	cancelTimerCh      chan struct{}
}

func NewRaftElector(r *Raft) *RaftElector {
	re := &RaftElector{
		parent: r,

		minElectionTimeout: 100 * time.Millisecond, //change if needed
		maxElectionTimeout: 300 * time.Millisecond,

		resetTimerCh:  make(chan struct{}, 1),
		cancelTimerCh: make(chan struct{}),
	}
	go re.electionTimerLoop()
	return re
}

func (re *RaftElector) nextTimeout() time.Duration {
	return re.minElectionTimeout +
		time.Duration(rand.Int63n(int64(re.maxElectionTimeout-re.minElectionTimeout)))
}

func (re *RaftElector) electionTimerLoop() {
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
			if re.parent.currentRole != Leader {
				re.parent.StartElection()
			}
			timer.Reset(re.nextTimeout())

		case <-re.cancelTimerCh:
			return
		}
	}
}

func (r *RaftElector) ResetTimer() {
	select {
	case r.resetTimerCh <- struct{}{}:
		slog.Info(fmt.Sprintf("Restarted timer for %d", r.parent.id))
	default:
	}
}

func (r *Raft) StartElection() {
	r.raftElector.ResetTimer()
	fmt.Printf("Election started for node %d\n", r.id)
	r.currentRole = Candidate
	r.currentTerm++
	r.votedFor = r.id
	r.votesReceived = mapset.NewSet[int]()
	r.votesReceived.Add(r.id)
	logTerm := 0
	if len(r.log) > 0 {
		logTerm = r.log[len(r.log)-1].term
	}
	for i := 0; i < r.totalNodes; i++ {
		if i == r.id {
			continue
		}
		fmt.Printf("Sending vote request to node %d from %d\n", i, r.id)
		r.sendVoteRequest(VoteRequestData{r.id, r.currentTerm, len(r.log), logTerm}, i)
	}
}

func (r *Raft) sendVoteRequest(data VoteRequestData, nodeId int) {
	go func() {
		if r.currentRole != Candidate {
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		resp, err := r.peers[nodeId].VoteRequest(ctx, &raftpb.VoteRequestArgs{
			CandidateId:        int32(data.candidateId),
			CandidateTerm:      int32(data.candidateTerm),
			CandidateLogLength: int32(data.candidateLogLength),
			CandidateLogTerm:   int32(data.candidateLogTerm),
		})

		slog.Info(fmt.Sprintf("Got vote response on node %d\n", nodeId))
		if err != nil {
			panic("Network error") //TODO WHAT TO DO HERE?
		}
		r.ReceiveVote(VoteResponse{int(resp.NodeId), int(resp.CurrentTerm), resp.Granted})
	}()
}
