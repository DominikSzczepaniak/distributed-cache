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
	datanodeURL = "http://localhost:9010"
	epoch       = 999999
	duration    = 10 * time.Second
	workers     = 100
)

type writeRequest struct {
	Key   string `json:"key"`
	Value string `json:"value"`
	Epoch int    `json:"epoch"`
}

func main() {
	fmt.Println("=== Go RPS Benchmark (connection pooling) ===")
	fmt.Printf("Workers: %d, Duration: %v\n\n", workers, duration)

	client := &http.Client{
		Transport: &http.Transport{
			MaxIdleConns:        200,
			MaxIdleConnsPerHost: 200,
			IdleConnTimeout:     30 * time.Second,
		},
		Timeout: 5 * time.Second,
	}

	fmt.Println("Waiting for datanode...")
	for i := 0; i < 30; i++ {
		resp, err := client.Get(datanodeURL + "/health")
		if err == nil && resp.StatusCode == 200 {
			resp.Body.Close()
			break
		}
		time.Sleep(time.Second)
	}
	fmt.Println("Datanode ready!")

	fmt.Println("Warmup...")
	for i := 0; i < 100; i++ {
		doPut(client, fmt.Sprintf("warmup-%d", i), "val")
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
	rps := float64(count) / elapsed.Seconds()

	fmt.Println("===============================")
	fmt.Printf("  Workers: %d\n", workers)
	fmt.Printf("  Total requests: %d\n", count)
	fmt.Printf("  Duration: %.2fs\n", elapsed.Seconds())
	fmt.Printf("  RPS: %.0f requests/second\n", rps)
	fmt.Println("===============================")
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
