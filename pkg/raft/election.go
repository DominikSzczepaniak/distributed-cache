package raft

import (
	"math/rand"
	"time"
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

func (r *Raft) StartElection() {
	//send rpc request for ProposeLeader to every node
	panic("unimplemented")
}
