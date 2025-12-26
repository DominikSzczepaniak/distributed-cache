package cachemodel

// Cache is the fundamental interface for all key-value storage implementations.
type Cache interface {
	Get(key string) string
	Put(key, value string)
	Delete(key string)
}
