// Package benchmark contains the load generator, latency statistics and the Go
// benchmarks that replace the old benchmark_*.sh / `//go:build ignore` mains.
//
//	go test ./tests/benchmark -run xxx -bench . -benchtime=1x   # one pass of every benchmark
//	go test ./tests/benchmark -run xxx -bench Scaling -benchtime=20000x
//
// Every benchmark executes b.N operations in total and reports p50_ms, p95_ms,
// p99_ms, max_ms, ops/s and err_rate via b.ReportMetric.
package benchmark

import (
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Result summarizes one load run.
type Result struct {
	Ops     int // operations attempted (successful + failed)
	Errors  int
	Elapsed time.Duration
	P50     time.Duration
	P95     time.Duration
	P99     time.Duration
	Max     time.Duration
}

func (r Result) ErrRate() float64 {
	if r.Ops == 0 {
		return 0
	}
	return float64(r.Errors) / float64(r.Ops)
}

func (r Result) OpsPerSec() float64 {
	if r.Elapsed <= 0 {
		return 0
	}
	return float64(r.Ops) / r.Elapsed.Seconds()
}

// percentile returns the nearest-rank percentile (0 < p <= 100) of an already
// sorted sample set; p<=0 yields the minimum.
func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	rank := int((p/100)*float64(len(sorted)) + 0.999999999)
	if rank < 1 {
		rank = 1
	}
	if rank > len(sorted) {
		rank = len(sorted)
	}
	return sorted[rank-1]
}

// summarize sorts the latencies of the successful operations and computes the
// percentiles; failed counts operations that returned an error.
func summarize(latencies []time.Duration, failed int, elapsed time.Duration) Result {
	s := append([]time.Duration(nil), latencies...)
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	r := Result{
		Ops:     len(s) + failed,
		Errors:  failed,
		Elapsed: elapsed,
		P50:     percentile(s, 50),
		P95:     percentile(s, 95),
		P99:     percentile(s, 99),
	}
	if len(s) > 0 {
		r.Max = s[len(s)-1]
	}
	return r
}

// runLoad performs total operations spread over `workers` goroutines. op gets
// the worker index and the global operation index. Latency is measured per
// operation; only successful operations contribute latency samples.
func runLoad(total, workers int, op func(worker, i int) error) Result {
	if workers < 1 {
		workers = 1
	}
	var (
		next   atomic.Int64
		failed atomic.Int64
		mu     sync.Mutex
		all    = make([]time.Duration, 0, total)
		wg     sync.WaitGroup
	)
	start := time.Now()
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			local := make([]time.Duration, 0, total/workers+1)
			for {
				i := int(next.Add(1) - 1)
				if i >= total {
					break
				}
				t0 := time.Now()
				if err := op(w, i); err != nil {
					failed.Add(1)
					continue
				}
				local = append(local, time.Since(t0))
			}
			mu.Lock()
			all = append(all, local...)
			mu.Unlock()
		}(w)
	}
	wg.Wait()
	return summarize(all, int(failed.Load()), time.Since(start))
}

// report publishes r through b.ReportMetric.
func report(b *testing.B, r Result) {
	b.Helper()
	ms := func(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }
	b.ReportMetric(ms(r.P50), "p50_ms")
	b.ReportMetric(ms(r.P95), "p95_ms")
	b.ReportMetric(ms(r.P99), "p99_ms")
	b.ReportMetric(ms(r.Max), "max_ms")
	b.ReportMetric(r.OpsPerSec(), "ops/s")
	b.ReportMetric(r.ErrRate(), "err_rate")
}
