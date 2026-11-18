package controller

import (
	"sync"
	"time"
)

type Reaper struct {
	mu          sync.RWMutex
	lastSeen    map[string]time.Time
	gracePeriod time.Duration
	onDeath     func(nodeID string)
	now         func() time.Time // injectable clock for tests
}

func NewReaper(gracePeriod time.Duration, onDeath func(nodeID string)) *Reaper {
	return newReaperWithClock(gracePeriod, onDeath, time.Now)
}

func newReaperWithClock(gracePeriod time.Duration, onDeath func(nodeID string), now func() time.Time) *Reaper {
	return &Reaper{
		now:         now,
		lastSeen:    make(map[string]time.Time),
		gracePeriod: gracePeriod,
		onDeath:     onDeath,
	}
}

func (r *Reaper) Track(nodeID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.lastSeen[nodeID] = r.now()
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
		if r.now().Sub(seen) > r.gracePeriod {
			delete(r.lastSeen, id)
			go r.onDeath(id)
		}
	}
}
