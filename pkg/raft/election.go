package raft

import (
	"math/rand"
	"time"

	mapset "github.com/deckarep/golang-set/v2"
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


func (r *RaftElection) ResetTimer(){
	select{
	case <-r.resetTimerCh:
		r.resetTimerCh <- struct{}{}
	default:
	}
}

func (r *Raft) StartElection() {
	//send rpc request for ProposeLeader to every node
	r.currentRole = "candidate"
	r.currentTerm++
	r.votedFor = r.id
	r.votesReceived = mapset.NewSet[int]()
	r.votesReceived.Add(r.id)
	for i := 0; i < r.totalNodes; i++ {
		if i == r.id{
			continue
		}
		//send via grpc to node i:
		r.ProposeLeader(VoteRequest{r.id, r.currentTerm, len(r.log), r.log[len(r.log)-1].term})
	}


	panic("unimplemented")
}
