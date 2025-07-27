// Functions related to the Raft log: appending entries, committing entries, applying entries to the state machine, log truncation.
// Log entry struct definitions

package raft

import (
	"context"
	"fmt"
	"log/slog"
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
	slog.Info(fmt.Sprintf("Preparing log request for leader %d", r.id))
	r.mu.RLock()
	defer r.mu.RUnlock()

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
	slog.Info(fmt.Sprintf("Preparing log request for follower %d finished", followerId))
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
	r.mu.RLock()
	currentRole := r.currentRole
	r.mu.RUnlock()

	slog.Info(fmt.Sprintf("Replicating log from %d to %d, node role: %s", id, followerId, currentRole))
	retryCh := make(chan int)
	done := make(chan struct{})
	go r.retryAppendEntries(retryCh, done)

	r.mu.RLock()
	isLeader := r.currentRole == Leader
	r.mu.RUnlock()

	if isLeader {
		r.handleReplicateLog(followerId, retryCh, done)
	}

	for {
		select {
		case peerId := <-retryCh:
			r.mu.RLock()
			isLeader := r.currentRole == Leader
			r.mu.RUnlock()

			if isLeader {
				slog.Info("lost leadership, stopping replicateLog")
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
	r.mu.RLock()
	isLeader := r.currentRole == Leader
	r.mu.RUnlock()

	if !isLeader {
		slog.Info("Node is not a leader, returning")
		return
	}
	args := r.prepareLogRequestArgs(followerId)
	slog.Info(fmt.Sprintf("Prepared log for replication from %d to %d", r.id, followerId))
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		slog.Info(fmt.Sprintf("Logs for leader %d", r.id))
		for _, log := range r.log {
			slog.Info(fmt.Sprintf("term: %d, msgType: %s, key: %d, value: %d", log.term, log.message.msgType, log.message.key, *log.message.value))
		}
		r.mu.RLock()
		peer := r.peers[followerId]
		r.mu.RUnlock()
		resp, err := peer.LogRequest(ctx, args)
		if err != nil {
			slog.Error(err.Error())
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
