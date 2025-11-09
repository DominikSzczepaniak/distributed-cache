package api

import (
	"fmt"
	"sync"
	"time"

	"github.com/dominikszczepaniak/distributed-cache/pkg/raft"
)

type IdempotencyCache struct {
	mu      sync.RWMutex
	entries map[string]*IdempotencyCacheEntry
	ttl     time.Duration
}

type IdempotencyCacheEntry struct {
	Response    raft.BroadcastResponse
	CompletedAt time.Time
}

func NewIdempotencyCache(ttl time.Duration) *IdempotencyCache {
	cache := &IdempotencyCache{
		entries: make(map[string]*IdempotencyCacheEntry),
		ttl:     ttl,
	}

	go cache.cleanupExpired()

	return cache
}

func (c *IdempotencyCache) Get(token string) (*raft.BroadcastResponse, bool) {
	c.mu.RLock()
	fmt.Printf("[LOCK] Acquired RLock for IdempotencyCache in Get\n")
	defer func() {
		c.mu.RUnlock()
		fmt.Printf("[LOCK] Released RLock for IdempotencyCache in Get\n")
	}()

	entry, exists := c.entries[token]
	if !exists {
		return nil, false
	}

	if time.Since(entry.CompletedAt) > c.ttl {
		return nil, false
	}

	return &entry.Response, true
}

func (c *IdempotencyCache) Set(token string, response raft.BroadcastResponse) {
	c.mu.Lock()
	fmt.Printf("[LOCK] Acquired Lock for IdempotencyCache in Set\n")
	defer func() {
		c.mu.Unlock()
		fmt.Printf("[LOCK] Released Lock for IdempotencyCache in Set\n")
	}()

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
		fmt.Printf("[LOCK] Acquired Lock for IdempotencyCache in cleanupExpired\n")
		now := time.Now()
		for token, entry := range c.entries {
			if now.Sub(entry.CompletedAt) > c.ttl {
				delete(c.entries, token)
			}
		}
		c.mu.Unlock()
		fmt.Printf("[LOCK] Released Lock for IdempotencyCache in cleanupExpired\n")
	}
}
