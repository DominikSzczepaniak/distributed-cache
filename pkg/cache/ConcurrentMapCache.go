package cache

import "sync"

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
