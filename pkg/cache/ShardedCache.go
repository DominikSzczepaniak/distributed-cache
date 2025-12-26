package cache

import (
	"hash/fnv"
	"sync"
)

const defaultNumShards = 16

type cacheShard struct {
	data map[string]string
	mu   sync.RWMutex
}

// ShardedCache partitions data into multiple segments to reduce lock contention.
type ShardedCache struct {
	shards    []*cacheShard
	numShards int
	shardMask int
}

// NewShardedCache initializes a new ShardedCache with the specified number of segments.
func NewShardedCache(numShards int) *ShardedCache {
	if numShards <= 0 || (numShards&(numShards-1)) != 0 {
		numShards = defaultNumShards
	}

	shards := make([]*cacheShard, numShards)
	for i := 0; i < numShards; i++ {
		shards[i] = &cacheShard{
			data: make(map[string]string),
		}
	}
	return &ShardedCache{
		shards:    shards,
		numShards: numShards,
		shardMask: numShards - 1,
	}
}

func (sc *ShardedCache) getShard(key string) *cacheShard {
	h := fnv.New32a()
	h.Write([]byte(key))
	return sc.shards[uint(h.Sum32())&uint(sc.shardMask)]
}

// Get retrieves a value from the appropriate cache shard.
func (sc *ShardedCache) Get(key string) string {
	shard := sc.getShard(key)
	shard.mu.RLock()
	val := shard.data[key]
	shard.mu.RUnlock()
	return val
}

// Delete removes a key from the appropriate cache shard.
func (sc *ShardedCache) Delete(key string) {
	shard := sc.getShard(key)
	shard.mu.Lock()
	delete(shard.data, key)
	shard.mu.Unlock()
}

// Put stores a value in the appropriate cache shard.
func (sc *ShardedCache) Put(key, value string) {
	shard := sc.getShard(key)
	shard.mu.Lock()
	shard.data[key] = value
	shard.mu.Unlock()
}
