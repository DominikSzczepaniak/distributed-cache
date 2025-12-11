//go:build ignore

package main

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	redisAddr = "localhost:6379"
	duration  = 10 * time.Second
	workers   = 100
)

func main() {
	fmt.Println("=== Redis RPS Benchmark (in-memory cache) ===")
	fmt.Printf("Workers: %d, Duration: %v\n\n", workers, duration)

	fmt.Println("Connecting to Redis...")
	client := redis.NewClient(&redis.Options{
		Addr:     redisAddr,
		PoolSize: 200,
	})
	defer client.Close()

	ctx := context.Background()

	if err := client.Ping(ctx).Err(); err != nil {
		fmt.Printf("Redis not ready: %v\n", err)
		return
	}
	fmt.Println("Redis ready!")

	fmt.Println("Warmup...")
	for i := 0; i < 100; i++ {
		client.Set(ctx, fmt.Sprintf("warmup-%d", i), "val", 0)
	}

	var count int64
	var stop int32
	var wg sync.WaitGroup

	fmt.Printf("Running for %v at MAXIMUM power...\n\n", duration)

	start := time.Now()

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			localCount := 0
			for atomic.LoadInt32(&stop) == 0 {
				key := fmt.Sprintf("bench-%d-%d", id, localCount)
				if err := client.Set(ctx, key, "value", 0).Err(); err == nil {
					localCount++
				}
			}
			atomic.AddInt64(&count, int64(localCount))
		}(w)
	}

	time.Sleep(duration)
	atomic.StoreInt32(&stop, 1)
	wg.Wait()

	elapsed := time.Since(start)
	rps := float64(count) / elapsed.Seconds()

	fmt.Println("===============================")
	fmt.Printf("  System: Redis (in-memory)\n")
	fmt.Printf("  Workers: %d\n", workers)
	fmt.Printf("  Total requests: %d\n", count)
	fmt.Printf("  Duration: %.2fs\n", elapsed.Seconds())
	fmt.Printf("  RPS: %.0f requests/second\n", rps)
	fmt.Println("===============================")
}
