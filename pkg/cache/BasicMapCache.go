package cache

type BasicMapCache struct {
	data map[int]int
}

func NewBasicMapCache() *BasicMapCache {
	return &BasicMapCache{
		data: make(map[int]int),
	}
}

func (c *BasicMapCache) Get(key int) int {
	return c.data[key]
}

func (c *BasicMapCache) Delete(key int) {
	delete(c.data, key)
}

func (c *BasicMapCache) Put(key, value int) {
	c.data[key] = value
}
