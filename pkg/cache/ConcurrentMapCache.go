package cache

import "sync"

type ConcurrentMapCache struct {
	data map[int]int
	mu   sync.RWMutex
}

func NewConcurrentMapCache() *ConcurrentMapCache {
	return &ConcurrentMapCache{
		data: make(map[int]int),
	}
}

func (c *ConcurrentMapCache) Get(key int) int {
	c.mu.RLock()
	val := c.data[key]
	c.mu.RUnlock()
	return val
}

func (c *ConcurrentMapCache) Delete(key int) {
	c.mu.Lock()
	delete(c.data, key)
	c.mu.Unlock()
}

func (c *ConcurrentMapCache) Put(key, value int) {
	c.mu.Lock()
	c.data[key] = value
	c.mu.Unlock()
}
