//go:build ignore

package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

const (
	controllerURL = "http://localhost:8080"
	datanodeURL   = "http://localhost:9010"
	duration      = 10 * time.Second
	epoch         = 999999
)

type writeRequest struct {
	Key   string `json:"key"`
	Value string `json:"value"`
	Epoch int    `json:"epoch"`
}

func main() {
	workerCounts := []int{1, 10, 50, 100, 200}

	fmt.Println("╔═══════════════════════════════════════════════════════════════╗")
	fmt.Println("║           Client Scaling Benchmark                            ║")
	fmt.Println("╚═══════════════════════════════════════════════════════════════╝")
	fmt.Println()

	client := &http.Client{
		Transport: &http.Transport{
			MaxIdleConns:        500,
			MaxIdleConnsPerHost: 500,
			IdleConnTimeout:     30 * time.Second,
		},
		Timeout: 5 * time.Second,
	}

	fmt.Println("Waiting for datanode...")
	for i := 0; i < 30; i++ {
		resp, err := client.Get(datanodeURL + "/health")
		if err == nil && resp.StatusCode == 200 {
			resp.Body.Close()
			fmt.Println("Datanode ready!")
			break
		}
		time.Sleep(time.Second)
	}

	fmt.Println("Warmup...")
	for i := 0; i < 100; i++ {
		doPut(client, fmt.Sprintf("warmup-%d", i), "val")
	}

	fmt.Println()
	fmt.Printf("Running %v benchmark for each worker count...\n\n", duration)

	results := make(map[int]float64)

	for _, workers := range workerCounts {
		rps := runBenchmark(client, workers)
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

func runBenchmark(client *http.Client, workers int) float64 {
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
				if doPut(client, key, "value") {
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

func doPut(client *http.Client, key, value string) bool {
	req := writeRequest{Key: key, Value: value, Epoch: epoch}
	body, _ := json.Marshal(req)

	resp, err := client.Post(datanodeURL+"/data", "application/json", bytes.NewBuffer(body))
	if err != nil {
		return false
	}
	resp.Body.Close()
	return resp.StatusCode == 200
}
