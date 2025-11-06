package raft

import (
	"context"
	"fmt"
	"github.com/dominikszczepaniak/distributed-cache/pkg/raft/raftpb"
	"google.golang.org/protobuf/types/known/wrapperspb"
	"log/slog"
	"time"
)

type LogEntry struct {
	Term    int
	Message Message
}

type Replicator struct {
	parent     *Raft
	followerId int
	signalCh   chan struct{}
	stopCh     chan struct{}
}

func NewReplicator(parent *Raft, followerId int) *Replicator {
	return &Replicator{
		parent:     parent,
		followerId: followerId,
		signalCh:   make(chan struct{}, 1),
		stopCh:     make(chan struct{}),
	}
}

func (rep *Replicator) start() {
	go rep.run()
}

func (rep *Replicator) stop() {
	close(rep.stopCh)
}

func (rep *Replicator) run() {
	heartbeat := time.NewTicker(50 * time.Millisecond)
	defer heartbeat.Stop()

	for {
		select {
		case <-rep.stopCh:
			return

		case <-rep.signalCh:
			rep.replicate()
		case <-heartbeat.C:
			rep.replicate()
		}
	}
}

func (rep *Replicator) signal() {
	select {
	case rep.signalCh <- struct{}{}:
	default:
	}
}

func (rep *Replicator) replicate() {
	if !rep.parent.isPeerAvailable(rep.followerId) {
		slog.Debug(fmt.Sprintf("Node %d: Skipping replication to unavailable peer %d",
			rep.parent.id, rep.followerId))
		return
	}

	rep.parent.mu.RLock()
	nextIndex := rep.parent.sentLengths[rep.followerId]
	if nextIndex == 0 {
		nextIndex = rep.parent.snapshotter.lastIndex + len(rep.parent.log) + 1
	}
	needsSnapshot := nextIndex <= rep.parent.snapshotter.lastIndex
	rep.parent.mu.RUnlock()

	if needsSnapshot {
		rep.parent.sendInstallSnapshotRPC(rep.followerId)
		return
	}

	peer := rep.parent.getPeer(rep.followerId)
	if peer == nil {
		slog.Debug(fmt.Sprintf("Node %d: No peer client for follower %d",
			rep.parent.id, rep.followerId))
		return
	}

	args := rep.parent.prepareLogRequestArgs(rep.followerId)

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	resp, err := peer.LogRequest(ctx, args)

	if err != nil {
		slog.Warn(fmt.Sprintf("Node %d: LogRequest to follower %d failed: %v",
			rep.parent.id, rep.followerId, err))
		return
	}

	rep.parent.mu.Lock()
	defer rep.parent.mu.Unlock()

	if resp.CurrentTerm > int32(rep.parent.currentTerm) {
		rep.parent.becomeFollower(int(resp.CurrentTerm))
		return
	}

	if rep.parent.currentRole == Leader && int(resp.CurrentTerm) == rep.parent.currentTerm {
		if resp.Success {
			match := int(resp.Ack)
			rep.parent.sentLengths[rep.followerId] = match + 1
			rep.parent.ackedLengths[rep.followerId] = match
			rep.parent.commitLogEntries()
		} else {
			floor := rep.parent.snapshotter.lastIndex + 1
			if rep.parent.sentLengths[rep.followerId] > floor {
				rep.parent.sentLengths[rep.followerId]--
			}
		}
	}
}

func (r *Raft) prepareLogRequestArgs(followerId int) *raftpb.LogRequestArgs {
	r.mu.RLock()
	defer r.mu.RUnlock()
	nextIndex := r.sentLengths[followerId]
	if nextIndex == 0 {
		nextIndex = r.snapshotter.lastIndex + len(r.log) + 1
	}
	if nextIndex <= r.snapshotter.lastIndex {
		slog.Info(fmt.Sprintf(
			"InstallSnapshot from %d to %d: nextIndex=%d, snapshot.lastIndex=%d",
			r.id, followerId, nextIndex, r.snapshotter.lastIndex,
		))
		go r.sendInstallSnapshotRPC(followerId)
		return &raftpb.LogRequestArgs{
			LeaderId: int32(r.id),
			Term:     int32(r.currentTerm),
		}
	}
	prevIndex := nextIndex - 1
	var prevTerm int
	switch {
	case prevIndex == 0:
		prevTerm = 0
	case prevIndex == r.snapshotter.lastIndex:
		prevTerm = r.snapshotter.lastTerm
	default:
		rel := prevIndex - r.snapshotter.lastIndex - 1
		if rel < 0 || rel >= len(r.log) {
			return &raftpb.LogRequestArgs{
				LeaderId: int32(r.id),
				Term:     int32(r.currentTerm),
			}
		}
		prevTerm = r.log[rel].Term
	}
	start := nextIndex - r.snapshotter.lastIndex - 1
	if start < 0 {
		start = 0
	}
	if start > len(r.log) {
		start = len(r.log)
	}
	suffix := r.log[start:]
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
		PrefixLen:    int32(prevIndex),
		PrefixTerm:   int32(prevTerm),
		CommitLength: int32(r.commitedLength),
		Suffix:       pbSuffix,
	}
}
