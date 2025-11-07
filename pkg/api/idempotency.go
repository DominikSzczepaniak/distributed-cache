package api

import (
	"sync"
	"time"

	"github.com/dominikszczepaniak/distributed-cache/pkg/raft"
)

// IdempotencyCache caches responses for write operations to support idempotent retries
type IdempotencyCache struct {
	mu      sync.RWMutex
	entries map[string]*IdempotencyCacheEntry
	ttl     time.Duration
}

// IdempotencyCacheEntry stores a cached response
type IdempotencyCacheEntry struct {
	Response    raft.BroadcastResponse
	CompletedAt time.Time
}

// NewIdempotencyCache creates a new idempotency cache with specified TTL
func NewIdempotencyCache(ttl time.Duration) *IdempotencyCache {
	cache := &IdempotencyCache{
		entries: make(map[string]*IdempotencyCacheEntry),
		ttl:     ttl,
	}

	// Start cleanup goroutine
	go cache.cleanupExpired()

	return cache
}

// Get retrieves cached response if available and not expired
func (c *IdempotencyCache) Get(token string) (*raft.BroadcastResponse, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	entry, exists := c.entries[token]
	if !exists {
		return nil, false
	}

	// Check if expired
	if time.Since(entry.CompletedAt) > c.ttl {
		return nil, false
	}

	return &entry.Response, true
}

// Set caches a response
func (c *IdempotencyCache) Set(token string, response raft.BroadcastResponse) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.entries[token] = &IdempotencyCacheEntry{
		Response:    response,
		CompletedAt: time.Now(),
	}
}

// cleanupExpired removes expired entries periodically
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
