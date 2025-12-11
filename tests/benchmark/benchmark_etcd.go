//go:build ignore

package main

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	etcdEndpoint = "localhost:12379"
	duration     = 10 * time.Second
	workers      = 100
)

func main() {
	fmt.Println("=== etcd RPS Benchmark (for comparison) ===")
	fmt.Printf("Workers: %d, Duration: %v\n\n", workers, duration)

	// Connect to etcd
	fmt.Println("Connecting to etcd...")
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{etcdEndpoint},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		fmt.Printf("Failed to connect: %v\n", err)
		return
	}
	defer client.Close()

	// Health check
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	_, err = client.Status(ctx, etcdEndpoint)
	cancel()
	if err != nil {
		fmt.Printf("etcd not healthy: %v\n", err)
		return
	}
	fmt.Println("etcd ready!")

	// Warmup
	fmt.Println("Warmup...")
	for i := 0; i < 100; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		client.Put(ctx, fmt.Sprintf("warmup-%d", i), "val")
		cancel()
	}

	var count int64
	var stop int32
	var wg sync.WaitGroup

	fmt.Printf("Running for %v at MAXIMUM power...\n\n", duration)

	start := time.Now()

	// Start workers
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			localCount := 0
			for atomic.LoadInt32(&stop) == 0 {
				key := fmt.Sprintf("bench-%d-%d", id, localCount)
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				_, err := client.Put(ctx, key, "value")
				cancel()
				if err == nil {
					localCount++
				}
			}
			atomic.AddInt64(&count, int64(localCount))
		}(w)
	}

	// Run for duration
	time.Sleep(duration)
	atomic.StoreInt32(&stop, 1)
	wg.Wait()

	elapsed := time.Since(start)
	rps := float64(count) / elapsed.Seconds()

	fmt.Println("===============================")
	fmt.Printf("  System: etcd (Raft-based)\n")
	fmt.Printf("  Workers: %d\n", workers)
	fmt.Printf("  Total requests: %d\n", count)
	fmt.Printf("  Duration: %.2fs\n", elapsed.Seconds())
	fmt.Printf("  RPS: %.0f requests/second\n", rps)
	fmt.Println("===============================")
}
