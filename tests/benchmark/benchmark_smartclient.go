//go:build ignore

package main

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dominikszczepaniak/distributed-cache/pkg/client"
)

const (
	duration = 10 * time.Second
)

func main() {
	workerCounts := []int{1, 10, 50, 100}

	fmt.Println("╔═══════════════════════════════════════════════════════════════╗")
	fmt.Println("║       Smart Client Scaling Benchmark                          ║")
	fmt.Println("╚═══════════════════════════════════════════════════════════════╝")
	fmt.Println()

	controllers := []string{"http://localhost:8080", "http://localhost:8081", "http://localhost:8082"}

	fmt.Println("Connecting to controllers...")
	sc, err := client.NewSmartClient(controllers)
	if err != nil {
		fmt.Printf("Failed to create smart client: %v\n", err)
		return
	}
	fmt.Printf("Connected! Topology epoch: %d\n", sc.GetEpoch())

	config := sc.GetConfig()
	fmt.Printf("Active nodes: %d\n", len(config.Nodes))
	for nodeID := range config.Nodes {
		fmt.Printf("  - %s\n", nodeID)
	}

	fmt.Println("\nWarmup...")
	for i := 0; i < 100; i++ {
		sc.Put(fmt.Sprintf("warmup-%d", i), "val")
	}

	fmt.Println()
	fmt.Printf("Running %v benchmark for each worker count...\n\n", duration)

	results := make(map[int]float64)

	for _, workers := range workerCounts {
		rps := runBenchmark(sc, workers)
		results[workers] = rps
		fmt.Printf("Workers: %3d  →  RPS: %.0f\n", workers, rps)
	}

	fmt.Println()
	fmt.Println("╔═══════════════════════════════════════════════════════════════╗")
	fmt.Println("║                    SCALING RESULTS                            ║")
	fmt.Println("╠═══════════════════════════════════════════════════════════════╣")
	for _, workers := range workerCounts {
		rps := results[workers]
		fmt.Printf("║  Workers: %3d                    RPS: %8.0f              ║\n", workers, rps)
	}
	fmt.Println("╚═══════════════════════════════════════════════════════════════╝")
}

func runBenchmark(sc *client.SmartClient, workers int) float64 {
	var count int64
	var stop int32
	var wg sync.WaitGroup

	start := time.Now()

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			localCount := 0
			for atomic.LoadInt32(&stop) == 0 {
				key := fmt.Sprintf("bench-%d-%d", id, localCount)
				if err := sc.Put(key, "value"); err == nil {
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
	return float64(count) / elapsed.Seconds()
}
