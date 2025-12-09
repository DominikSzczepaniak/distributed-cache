package cache

type BasicMapCache struct {
	data map[string]string
}

func NewBasicMapCache() *BasicMapCache {
	return &BasicMapCache{
		data: make(map[string]string),
	}
}

func (c *BasicMapCache) Get(key string) string {
	return c.data[key]
}

func (c *BasicMapCache) Delete(key string) {
	delete(c.data, key)
}

func (c *BasicMapCache) Put(key, value string) {
	c.data[key] = value
}
