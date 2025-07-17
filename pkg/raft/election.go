package raft

import (
	"context"
	"math/rand"
	"time"

	mapset "github.com/deckarep/golang-set/v2"
	"github.com/dominikszczepaniak/distributed-cache/pkg/raft/raftpb"
)

type RaftElection struct {
	parent             *Raft
	minElectionTimeout time.Duration
	maxElectionTimeout time.Duration
	resetTimerCh       chan struct{}
	cancelTimerCh      chan struct{}
}

func (re *RaftElection) nextTimeout() time.Duration {
	return re.minElectionTimeout +
		time.Duration(rand.Int63n(int64(re.maxElectionTimeout-re.minElectionTimeout)))
}

func (re *RaftElection) electionTimerLoop() {
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
			re.parent.StartElection()
			timer.Reset(re.nextTimeout())

		case <-re.cancelTimerCh:
			return
		}
	}
}

func (r *RaftElection) ResetTimer() {
	select {
	case <-r.resetTimerCh:
		r.resetTimerCh <- struct{}{}
	default:
	}
}

func (r *Raft) StartElection() {
	r.currentRole = "candidate"
	r.currentTerm++
	r.votedFor = r.id
	r.votesReceived = mapset.NewSet[int]()
	r.votesReceived.Add(r.id)
	for i := 0; i < r.totalNodes; i++ {
		if i == r.id {
			continue
		}
		r.sendVoteRequest(VoteRequestData{r.id, r.currentTerm, len(r.log), r.log[len(r.log)-1].term}, i)
	}
}

func (r *Raft) sendVoteRequest(data VoteRequestData, nodeId int) {
	go func() {
		if r.currentRole != "candidate" {
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
		if err != nil {
			panic("Network error") //TODO WHAT TO DO HERE?
		}
		r.ReceiveVote(VoteResponse{int(resp.NodeId), int(resp.CurrentTerm), resp.Granted})

	}()
}
