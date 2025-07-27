package raft

import (
	"context"
	"google.golang.org/protobuf/types/known/emptypb"
	"time"
)

type Heartbeat struct {
	parent        *Raft
	resetTimerCh  chan struct{}
	cancelTimerCh chan struct{}
	timeout       time.Duration
}

func newHeartbeat(parent *Raft) *Heartbeat {
	return &Heartbeat{
		parent:        parent,
		resetTimerCh:  make(chan struct{}, 1),
		cancelTimerCh: make(chan struct{}, 1),
		timeout:       20 * time.Millisecond, //TODO might be too aggressive for network
	}
}

func (h *Heartbeat) sendHeartbeat() {
	for i := range h.parent.totalNodes {
		if i == h.parent.id {
			continue
		}

		peer := h.parent.getPeer(i)

		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), h.timeout)
			defer cancel()
			args := &emptypb.Empty{}

			_, _ = peer.Heartbeat(ctx, args)

		}()
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
