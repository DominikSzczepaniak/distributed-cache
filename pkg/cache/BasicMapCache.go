package cache

// BasicMapCache is a non-thread-safe cache implementation using a standard map.
// It is intended for use in environments where external synchronization is provided.
type BasicMapCache struct {
	data map[string]string
}

// NewBasicMapCache initializes a new BasicMapCache.
func NewBasicMapCache() *BasicMapCache {
	return &BasicMapCache{
		data: make(map[string]string),
	}
}

// Get retrieves a value from the cache.
func (c *BasicMapCache) Get(key string) string {
	return c.data[key]
}

// Delete removes a key from the cache.
func (c *BasicMapCache) Delete(key string) {
	delete(c.data, key)
}

// Put stores a value in the cache.
func (c *BasicMapCache) Put(key, value string) {
	c.data[key] = value
}
