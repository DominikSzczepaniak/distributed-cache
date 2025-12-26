package cache

import "sync"

// SyncMapCache is a simple wrapper around sync.Map for concurrent access.
type SyncMapCache struct {
	data sync.Map
}

// NewSyncMapCache initializes a new SyncMapCache.
func NewSyncMapCache() *SyncMapCache {
	return &SyncMapCache{}
}

// Get retrieves a value from the cache.
func (c *SyncMapCache) Get(key string) string {
	val, ok := c.data.Load(key)
	if !ok {
		return ""
	}
	return val.(string)
}

// Delete removes a key from the cache.
func (c *SyncMapCache) Delete(key string) {
	c.data.Delete(key)
}

// Put stores a value in the cache.
func (c *SyncMapCache) Put(key, value string) {
	c.data.Store(key, value)
}
