package api

import (
	"fmt"
	"sync"
	"time"
)

// LeaderCache caches leader information to avoid repeated lookups
type LeaderCache struct {
	mu         sync.RWMutex
	leaderID   int
	leaderAddr string
	lastUpdate time.Time
	ttl        time.Duration
}

// NewLeaderCache creates a new leader cache with specified TTL
func NewLeaderCache(ttl time.Duration) *LeaderCache {
	return &LeaderCache{
		leaderID:   -1,
		ttl:        ttl,
		lastUpdate: time.Time{},
	}
}

// Get returns cached leader info if not expired
func (lc *LeaderCache) Get() (int, string, bool) {
	lc.mu.RLock()
	fmt.Printf("[LOCK] Acquired RLock for LeaderCache in Get\n")
	defer func() {
		lc.mu.RUnlock()
		fmt.Printf("[LOCK] Released RLock for LeaderCache in Get\n")
	}()

	if time.Since(lc.lastUpdate) > lc.ttl {
		return -1, "", false // Expired
	}

	return lc.leaderID, lc.leaderAddr, true
}

// Set updates the leader cache
func (lc *LeaderCache) Set(leaderID int, leaderAddr string) {
	lc.mu.Lock()
	fmt.Printf("[LOCK] Acquired Lock for LeaderCache in Set\n")
	defer func() {
		lc.mu.Unlock()
		fmt.Printf("[LOCK] Released Lock for LeaderCache in Set\n")
	}()

	lc.leaderID = leaderID
	lc.leaderAddr = leaderAddr
	lc.lastUpdate = time.Now()
}

// Invalidate clears the cache
func (lc *LeaderCache) Invalidate() {
	lc.mu.Lock()
	fmt.Printf("[LOCK] Acquired Lock for LeaderCache in Invalidate\n")
	defer func() {
		lc.mu.Unlock()
		fmt.Printf("[LOCK] Released Lock for LeaderCache in Invalidate\n")
	}()

	lc.lastUpdate = time.Time{}
}
