package raft

import (
	"context"
	"log/slog"
	"time"
)

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
	args := rep.parent.prepareLogRequestArgs(rep.followerId)

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	peer := rep.parent.getPeer(rep.followerId)
	resp, err := peer.LogRequest(ctx, args)

	if err != nil {
		slog.Error("LogRequest RPC failed", "followerId", rep.followerId, "error", err)
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
			newAckedLength := int(args.PrefixLen) + len(args.Suffix)
			rep.parent.sentLengths[rep.followerId] = newAckedLength
			rep.parent.ackedLengths[rep.followerId] = newAckedLength
			rep.parent.commitLogEntries()
		} else {
			if rep.parent.sentLengths[rep.followerId] > 0 {
				rep.parent.sentLengths[rep.followerId]--
			}
		}
	}
}
