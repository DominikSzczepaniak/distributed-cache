package api

import (
	"sync"
	"time"
)

// LeaderCache maintains a local view of the current cluster leader's address.
// This allows the API server to quickly forward requests without querying Raft every time.
type LeaderCache struct {
	mu         sync.RWMutex
	leaderID   int
	leaderAddr string
	lastUpdate time.Time
	ttl        time.Duration
}

// NewLeaderCache initializes a new leader address cache with the specified TTL.
func NewLeaderCache(ttl time.Duration) *LeaderCache {
	return &LeaderCache{
		leaderID:   -1,
		ttl:        ttl,
		lastUpdate: time.Time{},
	}
}

// Get returns the cached leader ID and address if valid and not expired.
func (lc *LeaderCache) Get() (int, string, bool) {
	lc.mu.RLock()
	defer lc.mu.RUnlock()

	if time.Since(lc.lastUpdate) > lc.ttl {
		return -1, "", false // Expired
	}

	return lc.leaderID, lc.leaderAddr, true
}

// Set updates the cached leader information.
func (lc *LeaderCache) Set(leaderID int, leaderAddr string) {
	lc.mu.Lock()
	defer lc.mu.Unlock()

	lc.leaderID = leaderID
	lc.leaderAddr = leaderAddr
	lc.lastUpdate = time.Now()
}

// Invalidate clears the cached leader information, typically called after a failed forward.
func (lc *LeaderCache) Invalidate() {
	lc.mu.Lock()
	defer lc.mu.Unlock()

	lc.lastUpdate = time.Time{}
}
