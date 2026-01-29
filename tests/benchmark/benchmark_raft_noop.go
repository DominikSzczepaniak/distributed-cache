//go:build ignore

package main

import (
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

const (
	controllerURL = "http://localhost:8080"
	duration      = 5 * time.Second
	workers       = 10
)

func main() {
	fmt.Println("=== Raft NoOp Benchmark ===")
	fmt.Printf("Workers: %d, Duration: %v\n\n", workers, duration)

	client := &http.Client{
		Transport: &http.Transport{
			MaxIdleConns:        100,
			MaxIdleConnsPerHost: 100,
		},
		Timeout: 10 * time.Second,
	}

	fmt.Println("Waiting for controller...")
	for i := 0; i < 30; i++ {
		resp, err := client.Get(controllerURL + "/topology")
		if err == nil && resp.StatusCode == 200 {
			resp.Body.Close()
			break
		}
		time.Sleep(time.Second)
	}
	fmt.Println("Controller ready!")

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
				resp, err := client.Get(controllerURL + "/debug/raft_noop")
				if err == nil && resp.StatusCode == 200 {
					localCount++
					resp.Body.Close()
				} else if err != nil {
					// fmt.Printf("Error: %v\n", err)
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

	fmt.Printf("RPS: %.2f\n", rps)
}
