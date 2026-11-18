package benchmark

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/dominikszczepaniak/distributed-cache/pkg/client"
	"github.com/dominikszczepaniak/distributed-cache/pkg/metadata"
)

func TestPercentile(t *testing.T) {
	ms := func(n int) time.Duration { return time.Duration(n) * time.Millisecond }
	var samples []time.Duration
	for i := 1; i <= 100; i++ {
		samples = append(samples, ms(i))
	}
	for _, tc := range []struct {
		p    float64
		want time.Duration
	}{{50, ms(50)}, {95, ms(95)}, {99, ms(99)}, {100, ms(100)}, {0, ms(1)}} {
		if got := percentile(samples, tc.p); got != tc.want {
			t.Errorf("p%v = %v; want %v", tc.p, got, tc.want)
		}
	}
	if got := percentile([]time.Duration{ms(7)}, 99); got != ms(7) {
		t.Errorf("single sample p99 = %v; want 7ms", got)
	}
	if got := percentile(nil, 50); got != 0 {
		t.Errorf("empty p50 = %v; want 0", got)
	}
}

func TestSummarize_SortsAndCounts(t *testing.T) {
	r := summarize([]time.Duration{30 * time.Millisecond, 10 * time.Millisecond, 20 * time.Millisecond}, 1, time.Second)
	if r.P50 != 20*time.Millisecond || r.P99 != 30*time.Millisecond {
		t.Fatalf("unexpected percentiles: %+v", r)
	}
	// 3 successful samples + 1 failed operation = 4 attempted operations.
	if r.Ops != 4 || r.Errors != 1 {
		t.Fatalf("ops/errors = %d/%d; want 4/1", r.Ops, r.Errors)
	}
	if got, want := r.ErrRate(), 0.25; got != want {
		t.Fatalf("err rate = %v; want %v", got, want)
	}
	if got := r.OpsPerSec(); got != 4 {
		t.Fatalf("ops/s = %v; want 4", got)
	}
}

// stubCluster is a minimal in-process controller+datanode (one shard) so the
// load generator can be smoke-tested without subprocesses, i.e. under -short.
func stubCluster(t *testing.T) *client.SmartClient {
	t.Helper()
	var mu sync.Mutex
	data := map[string]string{}

	var srv *httptest.Server
	mux := http.NewServeMux()
	mux.HandleFunc("/topology", func(w http.ResponseWriter, r *http.Request) {
		cfg := metadata.ClusterConfig{
			Epoch:       1,
			TotalShards: 1,
			Nodes:       map[string]metadata.NodeMetadata{"n": {ID: "n", Address: srv.Listener.Addr().String(), Status: metadata.StatusActive}},
			Shards:      map[int]metadata.ShardMetadata{0: {ID: 0, PrimaryID: "n", Status: metadata.ShardStatusActive}},
		}
		json.NewEncoder(w).Encode(cfg)
	})
	mux.HandleFunc("/data", func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		switch r.Method {
		case http.MethodPost:
			var req struct {
				Key   string `json:"key"`
				Value string `json:"value"`
			}
			b, _ := io.ReadAll(r.Body)
			json.Unmarshal(b, &req)
			data[req.Key] = req.Value
		case http.MethodGet:
			v, ok := data[r.URL.Query().Get("key")]
			if !ok {
				http.Error(w, "Key not found", http.StatusNotFound)
				return
			}
			w.Write([]byte(v))
		}
	})
	srv = httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	cl, err := client.NewSmartClient([]string{srv.URL})
	if err != nil {
		t.Fatal(err)
	}
	return cl
}

// Smoke test of the whole load path with a tiny workload: percentiles are
// computed (non-zero, ordered) and nothing failed.
func TestLoadSmoke_Stub(t *testing.T) {
	cl := stubCluster(t)
	const ops = 200
	res := runLoad(ops, 4, func(w, i int) error {
		return cl.Put(fmt.Sprintf("k-%d", i), "value")
	})
	assertSane(t, res, ops)

	res = runLoad(ops, 4, func(w, i int) error {
		_, err := cl.Get(fmt.Sprintf("k-%d", i))
		return err
	})
	assertSane(t, res, ops)
}

func assertSane(t testing.TB, r Result, wantOps int) {
	t.Helper()
	if r.Ops != wantOps {
		t.Fatalf("ops = %d; want %d", r.Ops, wantOps)
	}
	if r.Errors != 0 || r.ErrRate() != 0 {
		t.Fatalf("errors = %d (rate %v); want 0", r.Errors, r.ErrRate())
	}
	if r.P50 <= 0 || r.P95 <= 0 || r.P99 <= 0 {
		t.Fatalf("percentiles not computed: %+v", r)
	}
	if !(r.P50 <= r.P95 && r.P95 <= r.P99 && r.P99 <= r.Max) {
		t.Fatalf("percentiles out of order: %+v", r)
	}
	if r.OpsPerSec() <= 0 {
		t.Fatalf("throughput not computed: %+v", r)
	}
}
