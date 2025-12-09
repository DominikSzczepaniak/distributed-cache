package cachemodel

type Cache interface {
	Get(key string) string
	Put(key, value string)
	Delete(key string)
}
