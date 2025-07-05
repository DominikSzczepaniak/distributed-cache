package cachemodel

type Cache interface {
	Get(key int) int
	Delete(key int)
	Put(key, value int)
}
