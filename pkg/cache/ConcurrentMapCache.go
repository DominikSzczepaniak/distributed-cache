package cache

import (
	"hash/fnv"
	"sync"
)

type ConcurrentMapCache struct {
	data map[string]string
	mu   sync.RWMutex
}

func NewConcurrentMapCache() *ConcurrentMapCache {
	return &ConcurrentMapCache{
		data: make(map[string]string),
	}
}

func (c *ConcurrentMapCache) Get(key string) string {
	c.mu.RLock()
	val := c.data[key]
	c.mu.RUnlock()
	return val
}

func (c *ConcurrentMapCache) Delete(key string) {
	c.mu.Lock()
	delete(c.data, key)
	c.mu.Unlock()
}

func (c *ConcurrentMapCache) Put(key, value string) {
	c.mu.Lock()
	c.data[key] = value
	c.mu.Unlock()
}

// ExportShard returns all key-value pairs that belong to the specified shardID
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

// Import bulk inserts key-value pairs into the cache
func (c *ConcurrentMapCache) Import(data map[string]string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for k, v := range data {
		c.data[k] = v
	}
}
