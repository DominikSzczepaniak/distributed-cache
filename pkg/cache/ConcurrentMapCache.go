package cache

import (
	"hash/fnv"
	"sync"
)

// ConcurrentMapCache is a thread-safe cache implementation using a RWMutex and a standard map.
type ConcurrentMapCache struct {
	data map[string]string
	mu   sync.RWMutex
}

// NewConcurrentMapCache initializes a new ConcurrentMapCache.
func NewConcurrentMapCache() *ConcurrentMapCache {
	return &ConcurrentMapCache{
		data: make(map[string]string),
	}
}

// Get retrieves a value from the cache.
func (c *ConcurrentMapCache) Get(key string) string {
	c.mu.RLock()
	val := c.data[key]
	c.mu.RUnlock()
	return val
}

// Delete removes a key from the cache.
func (c *ConcurrentMapCache) Delete(key string) {
	c.mu.Lock()
	delete(c.data, key)
	c.mu.Unlock()
}

// Put stores a value in the cache.
func (c *ConcurrentMapCache) Put(key, value string) {
	c.mu.Lock()
	c.data[key] = value
	c.mu.Unlock()
}

// ExportShard serializes keys that belong to a specific shard.
func (c *ConcurrentMapCache) ExportShard(shardID int, totalShards int) map[string]string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	result := make(map[string]string)
	for k, v := range c.data {
		h := fnv.New32a()
		h.Write([]byte(k))
		keyShardID := int(h.Sum32()) % totalShards
		if keyShardID < 0 {
			keyShardID = -keyShardID
		}

		if keyShardID == shardID {
			result[k] = v
		}
	}
	return result
}

// Import merges external data into the local cache.
func (c *ConcurrentMapCache) Import(data map[string]string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for k, v := range data {
		c.data[k] = v
	}
}
