package controller

import (
	"sync"
	"time"
)

// Reaper monitors node health by tracking heartbeats and declaring nodes dead if they timeout.
type Reaper struct {
	mu          sync.RWMutex
	lastSeen    map[string]time.Time
	gracePeriod time.Duration
	onDeath     func(nodeID string)
}

// NewReaper initializes a heartbeat monitor with the specified grace period and death callback.
func NewReaper(gracePeriod time.Duration, onDeath func(nodeID string)) *Reaper {
	return &Reaper{
		lastSeen:    make(map[string]time.Time),
		gracePeriod: gracePeriod,
		onDeath:     onDeath,
	}
}

// Track updates the last seen time for a specific node.
func (r *Reaper) Track(nodeID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.lastSeen[nodeID] = time.Now()
}

// Run starts the background monitoring loop.
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
			delete(r.lastSeen, id)
			go r.onDeath(id)
		}
	}
}
