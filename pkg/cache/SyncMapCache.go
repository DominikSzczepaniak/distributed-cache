package cache

import "sync"

type SyncMapCache struct {
	data sync.Map
}

func NewSyncMapCache() *SyncMapCache {
	return &SyncMapCache{}
}

func (c *SyncMapCache) Get(key string) string {
	val, ok := c.data.Load(key)
	if !ok {
		return ""
	}
	return val.(string)
}

func (c *SyncMapCache) Delete(key string) {
	c.data.Delete(key)
}

func (c *SyncMapCache) Put(key, value string) {
	c.data.Store(key, value)
}
