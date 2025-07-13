// Functions related to the Raft log: appending entries, committing entries, applying entries to the state machine, log truncation.
// Log entry struct definitions

package raft

import (
	"math/rand"
	"time"
)

type LogEntry struct {
	term int 
	message Message
}

type RaftLogReplicator struct {
	parent *Raft

	logReplicateCh chan struct{}
	cancelLogReplicateCh       chan struct{}
	minLogReplicateTimeout time.Duration
	maxLogReplicateTimeout time.Duration
}

func (rl *RaftLogReplicator) NewRaftLogReplicator(r Raft){
	rl.parent = &r
	rl.logReplicateCh = make(chan struct{}, 1)
	rl.cancelLogReplicateCh = make(chan struct{})

	rl.minLogReplicateTimeout = 100 * time.Millisecond
	rl.maxLogReplicateTimeout = 300 * time.Millisecond

	go rl.logReplicateLoop()
}

func (rl *RaftLogReplicator) nextTimeout() time.Duration {
	return rl.minLogReplicateTimeout + time.Duration(rand.Int63n(int64(rl.maxLogReplicateTimeout-rl.minLogReplicateTimeout)))
}

func (rl *RaftLogReplicator) logReplicateLoop() {
	timer := time.NewTimer(rl.nextTimeout())
	defer timer.Stop()

	for {
		select {
		case <-rl.logReplicateCh:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(rl.nextTimeout())

		case <-timer.C:
			for i:=0; i<rl.parent.totalNodes; i++{
				if i == rl.parent.id{
					continue
				}
				rl.parent.ReplicateLog(rl.parent.id, i)
			}
			timer.Reset(rl.nextTimeout())

		case <-rl.cancelLogReplicateCh:
			return
		}
	}
}


func (r *Raft) ReplicateLog(id int, followerId int) {
	if r.currentRole != "leader"{
		return
	}
	prefixLen := r.sentLengths[followerId]
	suffix := r.log[prefixLen:]
	prefixTerm := 0
	if prefixLen > 0{
		prefixTerm = r.log[prefixLen-1].term
	}
	r.LogRequest(id, r.currentTerm, prefixLen, prefixTerm, r.commitedLength, suffix) //we send this to followerId
}
