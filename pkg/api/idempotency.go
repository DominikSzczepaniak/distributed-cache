package api

import (
	"sync"
	"time"

	"github.com/dominikszczepaniak/distributed-cache/pkg/raft"
)

type IdempotencyCache struct {
	mu      sync.RWMutex
	entries map[string]*IdempotencyCacheEntry
	ttl     time.Duration
	now     func() time.Time // injectable clock for tests
}

type IdempotencyCacheEntry struct {
	Response    raft.BroadcastResponse
	CompletedAt time.Time
}

func NewIdempotencyCache(ttl time.Duration) *IdempotencyCache {
	return newIdempotencyCacheWithClock(ttl, time.Now)
}

func newIdempotencyCacheWithClock(ttl time.Duration, now func() time.Time) *IdempotencyCache {
	cache := &IdempotencyCache{
		entries: make(map[string]*IdempotencyCacheEntry),
		ttl:     ttl,
		now:     now,
	}

	go cache.cleanupExpired()

	return cache
}

func (c *IdempotencyCache) Get(token string) (*raft.BroadcastResponse, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	entry, exists := c.entries[token]
	if !exists {
		return nil, false
	}

	if c.now().Sub(entry.CompletedAt) > c.ttl {
		return nil, false
	}

	return &entry.Response, true
}

func (c *IdempotencyCache) Set(token string, response raft.BroadcastResponse) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.entries[token] = &IdempotencyCacheEntry{
		Response:    response,
		CompletedAt: c.now(),
	}
}

func (c *IdempotencyCache) cleanupExpired() {
	ticker := time.NewTicker(c.ttl / 2)
	defer ticker.Stop()

	for range ticker.C {
		c.mu.Lock()
		now := c.now()
		for token, entry := range c.entries {
			if now.Sub(entry.CompletedAt) > c.ttl {
				delete(c.entries, token)
			}
		}
		c.mu.Unlock()
	}
}
