// Functions related to the Raft log: appending entries, committing entries, applying entries to the state machine, log truncation.
// Log entry struct definitions

package raft

import (
	"context"
	"math/rand"
	"time"

	"github.com/dominikszczepaniak/distributed-cache/pkg/raft/raftpb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

type LogEntry struct {
	term    int
	message Message
}

type RaftLogReplicator struct {
	parent *Raft

	logReplicateCh         chan struct{}
	cancelLogReplicateCh   chan struct{}
	minLogReplicateTimeout time.Duration
	maxLogReplicateTimeout time.Duration
}

func NewRaftLogReplicator(r *Raft) *RaftLogReplicator {
	rl := &RaftLogReplicator{
		parent:                 r,
		logReplicateCh:         make(chan struct{}, 1),
		cancelLogReplicateCh:   make(chan struct{}),
		minLogReplicateTimeout: 100 * time.Millisecond,
		maxLogReplicateTimeout: 300 * time.Millisecond,
	}
	go rl.logReplicateLoop()
	return rl
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
			for i := 0; i < rl.parent.totalNodes; i++ {
				if i == rl.parent.id {
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

func (r *Raft) prepareLogRequestArgs(id, followerId int) *raftpb.LogRequestArgs {
	prefixLen := r.sentLengths[followerId]
	suffix := r.log[prefixLen:]
	prefixTerm := 0
	if prefixLen > 0 {
		prefixTerm = r.log[prefixLen-1].term
	}

	pbSuffix := make([]*raftpb.LogEntry, len(suffix))
	for i, e := range suffix {
		var val *wrapperspb.Int32Value
		if e.message.value != nil {
			val = wrapperspb.Int32(int32(*e.message.value))
		}
		pbSuffix[i] = &raftpb.LogEntry{
			Term: int32(e.term),
			Message: &raftpb.Message{
				Type:  toProtoMsgType(e.message.msgType),
				Key:   int32(e.message.key),
				Value: val,
			},
		}
	}

	return &raftpb.LogRequestArgs{
		LeaderId:     int32(r.id),
		Term:         int32(r.currentTerm),
		PrefixLen:    int32(prefixLen),
		PrefixTerm:   int32(prefixTerm),
		CommitLength: int32(r.commitedLength),
		Suffix:       pbSuffix,
	}
}

func (r *Raft) ReplicateLog(id, followerId int) {
	if r.currentRole != "leader" {
		return
	}
	args := r.prepareLogRequestArgs(id, followerId)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		resp, err := r.peers[followerId].LogRequest(ctx, args)
		if err != nil {
			r.LogResponse(followerId, r.currentTerm, 0, false)
			return
		}
		r.LogResponse(
			followerId,
			int(resp.CurrentTerm),
			int(resp.Ack),
			resp.Success,
		)
	}()
}
