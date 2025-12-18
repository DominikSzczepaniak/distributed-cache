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
	duration = 10 * time.Second
	workers  = 100
	epoch    = 999999
)

type writeRequest struct {
	Key   string `json:"key"`
	Value string `json:"value"`
	Epoch int    `json:"epoch"`
}

func main() {
	// Get datanode URLs from args or default
	datanodeURLs := []string{"http://localhost:9010"}
	if len(datanodeURLs) == 0 {
		fmt.Println("No datanodes specified")
		return
	}

	fmt.Printf("Testing with %d datanode(s), %d workers, %v duration\n", len(datanodeURLs), workers, duration)

	client := &http.Client{
		Transport: &http.Transport{
			MaxIdleConns:        500,
			MaxIdleConnsPerHost: 500,
			IdleConnTimeout:     30 * time.Second,
		},
		Timeout: 5 * time.Second,
	}

	// Wait for datanodes
	fmt.Println("Checking datanodes...")
	for _, url := range datanodeURLs {
		for i := 0; i < 30; i++ {
			resp, err := client.Get(url + "/health")
			if err == nil && resp.StatusCode == 200 {
				resp.Body.Close()
				fmt.Printf("  %s ready\n", url)
				break
			}
			time.Sleep(time.Second)
		}
	}

	// Warmup
	fmt.Println("Warmup...")
	for i := 0; i < 100; i++ {
		doPut(client, datanodeURLs[0], fmt.Sprintf("warmup-%d", i), "val")
	}

	fmt.Printf("\nRunning benchmark...\n")

	var count int64
	var stop int32
	var wg sync.WaitGroup

	start := time.Now()

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			localCount := 0
			targetURL := datanodeURLs[id%len(datanodeURLs)]
			for atomic.LoadInt32(&stop) == 0 {
				key := fmt.Sprintf("bench-%d-%d", id, localCount)
				if doPut(client, targetURL, key, "value") {
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

	fmt.Printf("\nRPS: %.0f\n", rps)
}

func doPut(client *http.Client, baseURL, key, value string) bool {
	req := writeRequest{Key: key, Value: value, Epoch: epoch}
	body, _ := json.Marshal(req)

	resp, err := client.Post(baseURL+"/data", "application/json", bytes.NewBuffer(body))
	if err != nil {
		return false
	}
	resp.Body.Close()
	return resp.StatusCode == 200
}
