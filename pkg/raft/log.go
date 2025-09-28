package raft

import (
	"context"
	"math/rand"
	"time"

	"github.com/dominikszczepaniak/distributed-cache/pkg/raft/raftpb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

type LogEntry struct {
	Term    int
	Message Message
}

type LogReplicator struct {
	parent *Raft

	logReplicateCh         chan struct{}
	cancelLogReplicateCh   chan struct{}
	minLogReplicateTimeout time.Duration
	maxLogReplicateTimeout time.Duration
}

func NewRaftLogReplicator(r *Raft) *LogReplicator {
	rl := &LogReplicator{
		parent:                 r,
		logReplicateCh:         make(chan struct{}, 1),
		cancelLogReplicateCh:   make(chan struct{}),
		minLogReplicateTimeout: 50 * time.Millisecond,
		maxLogReplicateTimeout: 100 * time.Millisecond,
	}
	go rl.logReplicateLoop()
	return rl
}

func (rl *LogReplicator) nextTimeout() time.Duration {
	return rl.minLogReplicateTimeout + time.Duration(rand.Int63n(int64(rl.maxLogReplicateTimeout-rl.minLogReplicateTimeout)))
}

func (rl *LogReplicator) logReplicateLoop() {
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
			rl.parent.mu.RLock()
			isLeader := rl.parent.currentRole == Leader
			totalNodes := rl.parent.totalNodes
			nodeId := rl.parent.id
			rl.parent.mu.RUnlock()

			if isLeader {
				for i := 0; i < totalNodes; i++ {
					if i == nodeId {
						continue
					}
					go rl.parent.replicateLog(nodeId, i)
				}
			}
			timer.Reset(rl.nextTimeout())

		case <-rl.cancelLogReplicateCh:
			return
		}
	}
}

func (r *Raft) prepareLogRequestArgs(followerId int) *raftpb.LogRequestArgs {
	r.mu.RLock()
	defer r.mu.RUnlock()

	prefixLen := r.sentLengths[followerId]
	suffixStartIndex := prefixLen - r.snapshotter.lastIndex
	suffix := r.log[suffixStartIndex:]
	prefixTerm := 0
	if prefixLen > 0 {
		prefixTerm = r.log[prefixLen-1].Term
	}

	pbSuffix := make([]*raftpb.LogEntry, len(suffix))
	for i, e := range suffix {
		var val *wrapperspb.Int32Value
		if e.Message.Value != nil {
			val = wrapperspb.Int32(int32(*e.Message.Value))
		}
		pbSuffix[i] = &raftpb.LogEntry{
			Term: int32(e.Term),
			Message: &raftpb.Message{
				Type:  toProtoMsgType(e.Message.MsgType),
				Key:   int32(e.Message.Key),
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

func (r *Raft) replicateLog(id, followerId int) {
	retryCh := make(chan int)
	done := make(chan struct{})
	go r.retryAppendEntries(retryCh, done)

	isLeader, _ := r.getLeaderData()

	if isLeader {
		r.handleReplicateLog(followerId, retryCh, done)
	}

	for {
		select {
		case peerId := <-retryCh:
			isLeader, _ := r.getLeaderData()

			if !isLeader {
				close(done)
				return
			}
			r.handleReplicateLog(peerId, retryCh, done)

		case <-done:
			return
		}
	}
}

func (r *Raft) handleReplicateLog(followerId int, retryCh chan<- int, done chan struct{}) {
	isLeader, _ := r.getLeaderData()

	if !isLeader {
		return
	}
	args := r.prepareLogRequestArgs(followerId)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		defer cancel()

		peer := r.getPeer(followerId)
		resp, err := peer.LogRequest(ctx, args)
		if err != nil {
			retryCh <- followerId
			return
		}
		r.logResponse(
			followerId,
			int(resp.CurrentTerm),
			int(resp.Ack),
			resp.Success,
		)
		done <- struct{}{}
	}()
}

func (r *Raft) retryAppendEntries(retryCh chan int, done <-chan struct{}) {
	for {
		select {
		case peerId := <-retryCh:
			go r.replicateLog(r.id, peerId)
		case <-done:
			return
		}
	}
}
