package api

import (
	"sync"
	"time"

	"github.com/dominikszczepaniak/distributed-cache/pkg/raft"
)

// IdempotencyCache provides a mechanism to deduplicate requests by storing
// their results for a specific duration (TTL).
type IdempotencyCache struct {
	mu      sync.RWMutex
	entries map[string]*IdempotencyCacheEntry
	ttl     time.Duration
}

// IdempotencyCacheEntry holds the result and completion time of a processed request.
type IdempotencyCacheEntry struct {
	Response    raft.BroadcastResponse
	CompletedAt time.Time
}

// NewIdempotencyCache initializes a new cache for request de-duplication.
func NewIdempotencyCache(ttl time.Duration) *IdempotencyCache {
	cache := &IdempotencyCache{
		entries: make(map[string]*IdempotencyCacheEntry),
		ttl:     ttl,
	}

	go cache.cleanupExpired()

	return cache
}

// Get retrieves a cached response for a given token, if it exists and hasn't expired.
func (c *IdempotencyCache) Get(token string) (*raft.BroadcastResponse, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	entry, exists := c.entries[token]
	if !exists {
		return nil, false
	}

	if time.Since(entry.CompletedAt) > c.ttl {
		return nil, false
	}

	return &entry.Response, true
}

// Set stores a response in the cache, associated with the provided token.
func (c *IdempotencyCache) Set(token string, response raft.BroadcastResponse) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.entries[token] = &IdempotencyCacheEntry{
		Response:    response,
		CompletedAt: time.Now(),
	}
}

func (c *IdempotencyCache) cleanupExpired() {
	ticker := time.NewTicker(c.ttl / 2)
	defer ticker.Stop()

	for range ticker.C {
		c.mu.Lock()
		now := time.Now()
		for token, entry := range c.entries {
			if now.Sub(entry.CompletedAt) > c.ttl {
				delete(c.entries, token)
			}
		}
		c.mu.Unlock()
	}
}
