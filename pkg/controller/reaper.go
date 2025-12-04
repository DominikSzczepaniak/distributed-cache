package controller

import (
	"sync"
	"time"
)

type Reaper struct {
	mu          sync.RWMutex
	lastSeen    map[string]time.Time
	gracePeriod time.Duration
	onDeath     func(nodeID string) // Callback to trigger failover
}

func NewReaper(gracePeriod time.Duration, onDeath func(nodeID string)) *Reaper {
	return &Reaper{
		lastSeen:    make(map[string]time.Time),
		gracePeriod: gracePeriod,
		onDeath:     onDeath,
	}
}

func (r *Reaper) Track(nodeID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.lastSeen[nodeID] = time.Now()
}

func (r *Reaper) Run() {
	ticker := time.NewTicker(1 * time.Second)
	for range ticker.C {
		r.checkLiveness()
	}
}

func (r *Reaper) checkLiveness() {
	r.mu.Lock()
	defer r.mu.Unlock()

	for id, seen := range r.lastSeen {
		if time.Since(seen) > r.gracePeriod {
			// Node is DEAD.
			// Critical: Remove from map so we don't trigger this twice.
			delete(r.lastSeen, id)
			go r.onDeath(id)
		}
	}
}
