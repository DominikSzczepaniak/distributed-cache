package tests

import (
	"math/rand"
	"testing"
	"time"

	"github.com/dominikszczepaniak/distributed-cache/pkg/cache"
	"github.com/dominikszczepaniak/distributed-cache/pkg/cachemodel"
)

func runCacheOperations(b *testing.B, cache cachemodel.Cache, numKeys int, rng *rand.Rand) {
	for i := 0; i < numKeys; i++ {
		cache.Put(i, i*10)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		opChoice := rng.Intn(100)

		key := rng.Intn(numKeys)
		value := rng.Intn(1000000)

		if opChoice < 70 {
			_ = cache.Get(key)
		} else if opChoice < 90 {
			cache.Put(key, value)
		} else {
			cache.Delete(key)
		}
	}
}

func BenchmarkCacheComparison(b *testing.B) {
	const (
		numKeys         = 1000000 // number of distinct keys the cache will operate on
		rngSeed   int64 = 42
		numShards       = 32 // number of shards for ShardedCache
	)

	b.Run("BasicMapCache_SingleThreaded", func(b *testing.B) {
		cache := cache.NewBasicMapCache()
		rng := rand.New(rand.NewSource(rngSeed))
		runCacheOperations(b, cache, numKeys, rng)
	})

	b.Run("ConcurrentMapCache_Concurrent", func(b *testing.B) {
		cache := cache.NewConcurrentMapCache()
		for i := 0; i < numKeys; i++ {
			cache.Put(i, i*10)
		}
		b.ResetTimer()
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			goroutineRng := rand.New(rand.NewSource(time.Now().UnixNano()))
			for pb.Next() {
				opChoice := goroutineRng.Intn(100)
				key := goroutineRng.Intn(numKeys)
				value := goroutineRng.Intn(1000000)
				if opChoice < 70 {
					_ = cache.Get(key)
				} else if opChoice < 90 {
					cache.Put(key, value)
				} else {
					cache.Delete(key)
				}
			}
		})
	})

	b.Run("ShardedCache_Concurrent", func(b *testing.B) {
		cache := cache.NewShardedCache(numShards)
		for i := 0; i < numKeys; i++ {
			cache.Put(i, i*10)
		}
		b.ResetTimer()
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			goroutineRng := rand.New(rand.NewSource(time.Now().UnixNano()))
			for pb.Next() {
				opChoice := goroutineRng.Intn(100)
				key := goroutineRng.Intn(numKeys)
				value := goroutineRng.Intn(1000000)
				if opChoice < 70 {
					_ = cache.Get(key)
				} else if opChoice < 90 {
					cache.Put(key, value)
				} else {
					cache.Delete(key)
				}
			}
		})
	})

	b.Run("SyncMapCache_Concurrent", func(b *testing.B) {
		cache := cache.NewSyncMapCache()
		for i := 0; i < numKeys; i++ {
			cache.Put(i, i*10)
		}
		b.ResetTimer()
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			goroutineRng := rand.New(rand.NewSource(time.Now().UnixNano()))
			for pb.Next() {
				opChoice := goroutineRng.Intn(100)
				key := goroutineRng.Intn(numKeys)
				value := goroutineRng.Intn(1000000)
				if opChoice < 70 {
					_ = cache.Get(key)
				} else if opChoice < 90 {
					cache.Put(key, value)
				} else {
					cache.Delete(key)
				}
			}
		})
	})
}
