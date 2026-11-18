package api

import (
	"sync"
	"time"
)

type LeaderCache struct {
	mu         sync.RWMutex
	leaderID   int
	leaderAddr string
	lastUpdate time.Time
	ttl        time.Duration
	now        func() time.Time // injectable clock for tests
}

func NewLeaderCache(ttl time.Duration) *LeaderCache {
	return &LeaderCache{
		leaderID:   -1,
		ttl:        ttl,
		now:        time.Now,
		lastUpdate: time.Time{},
	}
}

func (lc *LeaderCache) Get() (int, string, bool) {
	lc.mu.RLock()
	defer lc.mu.RUnlock()

	if lc.now().Sub(lc.lastUpdate) > lc.ttl {
		return -1, "", false // Expired
	}

	return lc.leaderID, lc.leaderAddr, true
}

func (lc *LeaderCache) Set(leaderID int, leaderAddr string) {
	lc.mu.Lock()
	defer lc.mu.Unlock()

	lc.leaderID = leaderID
	lc.leaderAddr = leaderAddr
	lc.lastUpdate = lc.now()
}

func (lc *LeaderCache) Invalidate() {
	lc.mu.Lock()
	defer lc.mu.Unlock()

	lc.lastUpdate = time.Time{}
}
