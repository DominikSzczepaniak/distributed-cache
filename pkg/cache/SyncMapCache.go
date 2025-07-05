package cache

import "sync"

// --- SyncMapCache: A wrapper around Go's built-in sync.Map ---
type SyncMapCache struct {
	data sync.Map // sync.Map stores interface{}, interface{}
}

func NewSyncMapCache() *SyncMapCache {
	return &SyncMapCache{}
}

// Get retrieves a value from the cache.
func (smc *SyncMapCache) Get(key int) int {
	// Load returns (value interface{}, ok bool)
	val, _ := smc.data.Load(key)
	if v, ok := val.(int); ok { // Type assertion is necessary
		return v
	}
	return 0 // Return zero value if key not found or not int
}

// Delete removes a key-value pair from the cache.
func (smc *SyncMapCache) Delete(key int) {
	smc.data.Delete(key)
}

// Put adds or updates a key-value pair in the cache.
func (smc *SyncMapCache) Put(key, value int) {
	smc.data.Store(key, value) // Store accepts interface{}, interface{}
}
