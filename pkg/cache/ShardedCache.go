package cache

import (
	"sync"
)

const defaultNumShards = 16

type cacheShard struct {
	data map[int]int
	mu   sync.RWMutex
}

type ShardedCache struct {
	shards    []*cacheShard
	numShards int
	shardMask int
}

func NewShardedCache(numShards int) *ShardedCache {
	if numShards <= 0 || (numShards&(numShards-1)) != 0 {
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
		shardMask: numShards - 1,
	}
}

func (sc *ShardedCache) getShard(key int) *cacheShard {
	return sc.shards[uint(key)&uint(sc.shardMask)]
}

func (sc *ShardedCache) Get(key int) int {
	shard := sc.getShard(key)
	shard.mu.RLock()
	val := shard.data[key]
	shard.mu.RUnlock()
	return val
}

func (sc *ShardedCache) Delete(key int) {
	shard := sc.getShard(key)
	shard.mu.Lock()
	delete(shard.data, key)
	shard.mu.Unlock()
}

func (sc *ShardedCache) Put(key, value int) {
	shard := sc.getShard(key)
	shard.mu.Lock()
	shard.data[key] = value
	shard.mu.Unlock()
}
