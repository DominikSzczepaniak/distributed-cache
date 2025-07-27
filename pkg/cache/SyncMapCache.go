package cache

import "sync"

type SyncMapCache struct {
	data sync.Map
}

func NewSyncMapCache() *SyncMapCache {
	return &SyncMapCache{}
}

func (smc *SyncMapCache) Get(key int) int {
	val, _ := smc.data.Load(key)
	if v, ok := val.(int); ok {
		return v
	}
	return 0
}

func (smc *SyncMapCache) Delete(key int) {
	smc.data.Delete(key)
}

func (smc *SyncMapCache) Put(key, value int) {
	smc.data.Store(key, value)
}
