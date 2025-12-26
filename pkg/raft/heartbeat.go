package raft

import (
	"context"
	"time"

	"google.golang.org/protobuf/types/known/emptypb"
)

// Heartbeat manages the periodic broadcast of heartbeats (empty AppendEntries) by the leader.
type Heartbeat struct {
	parent        *Raft
	resetTimerCh  chan struct{}
	cancelTimerCh chan struct{}
	timeout       time.Duration
}

func newHeartbeat(parent *Raft) *Heartbeat {
	heartbeat := &Heartbeat{
		parent:        parent,
		resetTimerCh:  make(chan struct{}, 1),
		cancelTimerCh: make(chan struct{}, 1),
		timeout:       100 * time.Millisecond,
	}
	go heartbeat.heartbeatLoop()
	return heartbeat
}

func (h *Heartbeat) sendHeartbeat() {
	for i := 0; i < h.parent.totalNodes; i++ {
		if i == h.parent.id {
			continue
		}

		peer := h.parent.getPeer(i)
		if peer == nil {
			continue
		}
		go func(p PeerClient) {
			ctx, cancel := context.WithTimeout(context.Background(), h.timeout)
			defer cancel()
			args := &emptypb.Empty{}

			_, _ = p.Heartbeat(ctx, args)
		}(peer)
	}
}

func (h *Heartbeat) receiveHeartbeat() {
	h.parent.raftElector.ResetTimer()
}

func (h *Heartbeat) heartbeatLoop() {
	timer := time.NewTimer(h.timeout)
	defer timer.Stop()

	for {
		select {
		case <-h.resetTimerCh:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(h.timeout)

		case <-timer.C:
			h.parent.mu.RLock()
			isLeader := h.parent.currentRole == Leader
			h.parent.mu.RUnlock()
			if isLeader {
				h.sendHeartbeat()
			}
			timer.Reset(h.timeout)

		case <-h.cancelTimerCh:
			return
		}
	}
}
