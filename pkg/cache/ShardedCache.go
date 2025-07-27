package cache

import (
	"sync"
)

// --- ShardedCache: An efficient concurrent cache implementation ---
const defaultNumShards = 16 // A common choice, power of 2 for bitwise ops

type cacheShard struct {
	data map[int]int
	mu   sync.RWMutex // Each shard gets its own RWMutex
}

type ShardedCache struct {
	shards    []*cacheShard
	numShards int
	shardMask int // For efficient modulo using bitwise AND (if numShards is power of 2)
}

// NewShardedCache creates a new ShardedCache with the specified number of shards.
// It's recommended to use a power of 2 for numShards for optimal performance.
func NewShardedCache(numShards int) *ShardedCache {
	// Ensure numShards is a power of 2 for efficient bitwise operations
	if numShards <= 0 || (numShards&(numShards-1)) != 0 {
		// If not a power of 2, default to a sensible power of 2
		numShards = defaultNumShards
	}

	shards := make([]*cacheShard, numShards)
	for i := 0; i < numShards; i++ {
		shards[i] = &cacheShard{
			data: make(map[int]int),
		}
	}
	return &ShardedCache{
		shards:    shards,
		numShards: numShards,
		shardMask: numShards - 1, // e.g., if numShards=16 (0b10000), shardMask=15 (0b01111)
	}
}

// getShard determines which shard a key belongs to.
// Using bitwise AND is faster than modulo (%) for powers of 2.
func (sc *ShardedCache) getShard(key int) *cacheShard {
	// We use `uint(key)` to handle potential negative keys gracefully for the bitwise operation,
	// though `int` keys are usually positive in cache contexts.
	return sc.shards[uint(key)&uint(sc.shardMask)]
}

// Get retrieves a value from the cache.
func (sc *ShardedCache) Get(key int) int {
	shard := sc.getShard(key)
	shard.mu.RLock()
	val := shard.data[key]
	shard.mu.RUnlock()
	return val
}

// Delete removes a key-value pair from the cache.
func (sc *ShardedCache) Delete(key int) {
	shard := sc.getShard(key)
	shard.mu.Lock() // Acquire write lock for delete
	delete(shard.data, key)
	shard.mu.Unlock()
}

// Put adds or updates a key-value pair in the cache.
func (sc *ShardedCache) Put(key, value int) {
	shard := sc.getShard(key)
	shard.mu.Lock() // Acquire write lock for put
	shard.data[key] = value
	shard.mu.Unlock()
}
