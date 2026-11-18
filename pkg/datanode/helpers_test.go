package datanode

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dominikszczepaniak/distributed-cache/pkg/cache"
	"github.com/dominikszczepaniak/distributed-cache/pkg/metadata"
)

// testClock is a goroutine-safe manual clock.
type testClock struct {
	mu sync.Mutex
	t  time.Time
}

func newTestClock() *testClock { return &testClock{t: time.Unix(1_700_000_000, 0)} }
func (c *testClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}
func (c *testClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

// recorder is a replica / peer mock that records request bodies per path.
type recorder struct {
	srv    *httptest.Server
	status atomic.Int32
	mu     sync.Mutex
	paths  []string
	bodies [][]byte
	hook   func() // optional, runs on every request before responding
}

func newRecorder(t *testing.T) *recorder {
	t.Helper()
	r := &recorder{}
	r.status.Store(http.StatusOK)
	r.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		var buf bytes.Buffer
		buf.ReadFrom(req.Body)
		r.mu.Lock()
		r.paths = append(r.paths, req.URL.Path)
		r.bodies = append(r.bodies, buf.Bytes())
		hook := r.hook
		r.mu.Unlock()
		if hook != nil {
			hook()
		}
		w.WriteHeader(int(r.status.Load()))
	}))
	t.Cleanup(r.srv.Close)
	return r
}

func (r *recorder) addr() string { return r.srv.Listener.Addr().String() }
func (r *recorder) count() int   { r.mu.Lock(); defer r.mu.Unlock(); return len(r.paths) }
func (r *recorder) path(i int) string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.paths[i]
}
func (r *recorder) writeReq(i int) WriteRequest {
	r.mu.Lock()
	defer r.mu.Unlock()
	var w WriteRequest
	json.Unmarshal(r.bodies[i], &w)
	return w
}

// fixture is a datanode "node-1" with an active (fake-clock) lease.
type fixture struct {
	srv   *Server
	cache *cache.ConcurrentMapCache
	state *StateManager
	lease *LeaseManager
	clock *testClock
}

const testLease = time.Minute

func newFixture(t *testing.T, cfg *metadata.ClusterConfig) *fixture {
	t.Helper()
	return newFixtureB(cfg)
}

// newFixtureB is newFixture without testing.T so benchmarks can use it.
func newFixtureB(cfg *metadata.ClusterConfig) *fixture {
	clk := newTestClock()
	c := cache.NewConcurrentMapCache()
	sm := NewStateManager()
	if cfg != nil {
		sm.Update(cfg)
	}
	lm := NewLeaseManager("http://127.0.0.1:1", "node-1", testLease, sm)
	lm.now = clk.Now
	lm.extendLease()
	return &fixture{srv: NewServer(c, lm, sm, "node-1"), cache: c, state: sm, lease: lm, clock: clk}
}

// singleShardCfg: node-1 is primary of the only shard, replicas as given.
func singleShardCfg(epoch uint64, status metadata.ShardStatus, replicas ...*recorder) *metadata.ClusterConfig {
	cfg := &metadata.ClusterConfig{
		Epoch:       epoch,
		TotalShards: 1,
		Nodes:       map[string]metadata.NodeMetadata{"node-1": {ID: "node-1", Address: "node-1:1", Status: metadata.StatusActive}},
		Shards:      map[int]metadata.ShardMetadata{},
	}
	ids := []string{}
	for i, r := range replicas {
		id := string(rune('a'+i)) + "-replica"
		cfg.Nodes[id] = metadata.NodeMetadata{ID: id, Address: r.addr(), Status: metadata.StatusActive}
		ids = append(ids, id)
	}
	cfg.Shards[0] = metadata.ShardMetadata{ID: 0, PrimaryID: "node-1", ReplicaIDs: ids, Status: status}
	return cfg
}

func postJSON(t *testing.T, h http.HandlerFunc, method, url string, body any) *httptest.ResponseRecorder {
	t.Helper()
	var buf bytes.Buffer
	switch b := body.(type) {
	case string:
		buf.WriteString(b)
	case nil:
	default:
		if err := json.NewEncoder(&buf).Encode(b); err != nil {
			t.Fatal(err)
		}
	}
	req := httptest.NewRequest(method, url, &buf)
	w := httptest.NewRecorder()
	h(w, req)
	return w
}

func newServerFromMux(t *testing.T, mux *http.ServeMux) string {
	t.Helper()
	s := httptest.NewServer(mux)
	t.Cleanup(s.Close)
	return s.Listener.Addr().String()
}
