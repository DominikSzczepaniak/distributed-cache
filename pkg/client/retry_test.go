package client

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dominikszczepaniak/distributed-cache/pkg/metadata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// controllerMock serves a mutable topology and counts fetches.
type controllerMock struct {
	srv    *httptest.Server
	cfg    atomic.Pointer[metadata.ClusterConfig]
	fetchN atomic.Int32
}

func newControllerMock(t *testing.T, cfg *metadata.ClusterConfig) *controllerMock {
	t.Helper()
	m := &controllerMock{}
	m.cfg.Store(cfg)
	m.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		m.fetchN.Add(1)
		json.NewEncoder(w).Encode(m.cfg.Load())
	}))
	t.Cleanup(m.srv.Close)
	return m
}

func oneShardCfg(epoch uint64, nodeID string, srv *httptest.Server) *metadata.ClusterConfig {
	return &metadata.ClusterConfig{
		Epoch:       epoch,
		TotalShards: 1,
		Nodes: map[string]metadata.NodeMetadata{
			nodeID: {ID: nodeID, Address: srv.Listener.Addr().String(), Status: metadata.StatusActive},
		},
		Shards: map[int]metadata.ShardMetadata{0: {ID: 0, PrimaryID: nodeID, Status: metadata.ShardStatusActive}},
	}
}

func newClient(t *testing.T, ctrl ...string) *SmartClient {
	t.Helper()
	c, err := NewSmartClient(ctrl)
	require.NoError(t, err)
	return c
}

// statusSeq replies with the given statuses in order, then the last forever.
type statusSeq struct {
	mu       sync.Mutex
	statuses []int
	calls    int
	epochs   []uint64
	methods  []string
}

func (s *statusSeq) handler(okBody string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var req struct {
			Epoch uint64 `json:"epoch"`
		}
		json.Unmarshal(body, &req)
		s.mu.Lock()
		idx := s.calls
		if idx >= len(s.statuses) {
			idx = len(s.statuses) - 1
		}
		s.calls++
		s.epochs = append(s.epochs, req.Epoch)
		s.methods = append(s.methods, r.Method)
		code := s.statuses[idx]
		s.mu.Unlock()
		w.WriteHeader(code)
		if code == 200 {
			w.Write([]byte(okBody))
		}
	}
}

func (s *statusSeq) count() int { s.mu.Lock(); defer s.mu.Unlock(); return s.calls }

func TestSmartClient_Put_StaleEpoch412RefreshesTopologyAndRetries(t *testing.T) {
	// datanode only accepts epoch 2; controller initially advertises epoch 1 then 2.
	var gotEpochs []uint64
	var mu sync.Mutex
	var ctrl *controllerMock
	dn := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct{ Epoch uint64 }
		json.NewDecoder(r.Body).Decode(&req)
		mu.Lock()
		gotEpochs = append(gotEpochs, req.Epoch)
		mu.Unlock()
		if req.Epoch < 2 {
			w.WriteHeader(http.StatusPreconditionFailed)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer dn.Close()
	ctrl = newControllerMock(t, oneShardCfg(1, "n1", dn))
	c := newClient(t, ctrl.srv.URL)
	require.Equal(t, uint64(1), c.GetEpoch())

	ctrl.cfg.Store(oneShardCfg(2, "n1", dn)) // topology advances behind the client's back

	require.NoError(t, c.Put("k", "v"))
	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, []uint64{1, 2}, gotEpochs, "first try stale, retry must carry refreshed epoch")
	assert.Equal(t, uint64(2), c.GetEpoch())
	assert.EqualValues(t, 2, ctrl.fetchN.Load(), "initial fetch + exactly one refresh")
}

func TestSmartClient_Delete_StaleEpoch412RefreshesTopologyAndRetries(t *testing.T) {
	var mu sync.Mutex
	var gotEpochs []uint64
	dn := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct{ Epoch uint64 }
		json.NewDecoder(r.Body).Decode(&req)
		mu.Lock()
		gotEpochs = append(gotEpochs, req.Epoch)
		mu.Unlock()
		if r.Method != http.MethodDelete {
			t.Errorf("method = %s", r.Method)
		}
		if req.Epoch < 2 {
			w.WriteHeader(http.StatusPreconditionFailed)
		}
	}))
	defer dn.Close()
	ctrl := newControllerMock(t, oneShardCfg(1, "n1", dn))
	c := newClient(t, ctrl.srv.URL)
	ctrl.cfg.Store(oneShardCfg(2, "n1", dn))

	require.NoError(t, c.Delete("k"))
	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, []uint64{1, 2}, gotEpochs)
}

func TestSmartClient_Put_NotPrimary400RefreshesAndFollowsNewPrimary(t *testing.T) {
	var oldHits, newHits atomic.Int32
	oldDN := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		oldHits.Add(1)
		w.WriteHeader(http.StatusBadRequest) // Not Primary for Shard
	}))
	defer oldDN.Close()
	newDN := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		newHits.Add(1)
	}))
	defer newDN.Close()

	ctrl := newControllerMock(t, oneShardCfg(1, "old", oldDN))
	c := newClient(t, ctrl.srv.URL)
	ctrl.cfg.Store(oneShardCfg(2, "new", newDN))

	require.NoError(t, c.Put("k", "v"))
	assert.EqualValues(t, 1, oldHits.Load())
	assert.EqualValues(t, 1, newHits.Load(), "write must land on the new primary")
}

func TestSmartClient_Put_Fenced503ThenFailover(t *testing.T) {
	var newHits atomic.Int32
	oldDN := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer oldDN.Close()
	newDN := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { newHits.Add(1) }))
	defer newDN.Close()

	ctrl := newControllerMock(t, oneShardCfg(1, "old", oldDN))
	c := newClient(t, ctrl.srv.URL)
	ctrl.cfg.Store(oneShardCfg(2, "new", newDN))

	require.NoError(t, c.Put("k", "v"))
	assert.EqualValues(t, 1, newHits.Load())
}

// A fenced primary never recovers on its own; the client must re-read
// topology to find the promoted replica, for every operation type.
func TestSmartClient_Get_Fenced503RefreshesTopologyAndFailsOver(t *testing.T) {
	oldDN := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer oldDN.Close()
	newDN := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("val")) }))
	defer newDN.Close()

	ctrl := newControllerMock(t, oneShardCfg(1, "old", oldDN))
	c := newClient(t, ctrl.srv.URL)
	ctrl.cfg.Store(oneShardCfg(2, "new", newDN))

	v, err := c.Get("k")
	require.NoError(t, err)
	assert.Equal(t, "val", v)
}

func TestSmartClient_Delete_Fenced503RefreshesTopologyAndFailsOver(t *testing.T) {
	oldDN := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer oldDN.Close()
	var newHits atomic.Int32
	newDN := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { newHits.Add(1) }))
	defer newDN.Close()

	ctrl := newControllerMock(t, oneShardCfg(1, "old", oldDN))
	c := newClient(t, ctrl.srv.URL)
	ctrl.cfg.Store(oneShardCfg(2, "new", newDN))

	require.NoError(t, c.Delete("k"))
	assert.EqualValues(t, 1, newHits.Load())
}

func TestSmartClient_Delete_NotPrimary400RefreshesAndFollowsNewPrimary(t *testing.T) {
	oldDN := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer oldDN.Close()
	var newHits atomic.Int32
	newDN := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { newHits.Add(1) }))
	defer newDN.Close()

	ctrl := newControllerMock(t, oneShardCfg(1, "old", oldDN))
	c := newClient(t, ctrl.srv.URL)
	ctrl.cfg.Store(oneShardCfg(2, "new", newDN))

	require.NoError(t, c.Delete("k"))
	assert.EqualValues(t, 1, newHits.Load())
}

func TestSmartClient_Locked423RetriesForAllOps(t *testing.T) {
	cases := []struct {
		name string
		op   func(c *SmartClient) error
	}{
		{"put", func(c *SmartClient) error { return c.Put("k", "v") }},
		{"get", func(c *SmartClient) error { _, err := c.Get("k"); return err }},
		{"delete", func(c *SmartClient) error { return c.Delete("k") }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			seq := &statusSeq{statuses: []int{423, 423, 200}}
			dn := httptest.NewServer(seq.handler("ok"))
			defer dn.Close()
			ctrl := newControllerMock(t, oneShardCfg(1, "n1", dn))
			c := newClient(t, ctrl.srv.URL)

			require.NoError(t, tc.op(c))
			assert.Equal(t, 3, seq.count())
			assert.GreaterOrEqual(t, ctrl.fetchN.Load(), int32(3), "locked path refreshes topology (migration may have finished)")
		})
	}
}

func TestSmartClient_ReplicationFailure500RetriesPutAndDelete(t *testing.T) {
	for name, op := range map[string]func(c *SmartClient) error{
		"put":    func(c *SmartClient) error { return c.Put("k", "v") },
		"delete": func(c *SmartClient) error { return c.Delete("k") },
	} {
		t.Run(name, func(t *testing.T) {
			seq := &statusSeq{statuses: []int{500, 500, 200}}
			dn := httptest.NewServer(seq.handler(""))
			defer dn.Close()
			ctrl := newControllerMock(t, oneShardCfg(1, "n1", dn))
			c := newClient(t, ctrl.srv.URL)

			require.NoError(t, op(c))
			assert.Equal(t, 3, seq.count())
		})
	}
}

func TestSmartClient_MaxRetriesExceeded(t *testing.T) {
	for _, status := range []int{412, 400, 423, 503, 500} {
		for name, op := range map[string]func(c *SmartClient) error{
			"put":    func(c *SmartClient) error { return c.Put("k", "v") },
			"delete": func(c *SmartClient) error { return c.Delete("k") },
		} {
			if name == "delete" && status == 400 {
				continue // see TestSmartClient_Delete_NotPrimary400...
			}
			t.Run(name+"_"+http.StatusText(status), func(t *testing.T) {
				seq := &statusSeq{statuses: []int{status}}
				dn := httptest.NewServer(seq.handler(""))
				defer dn.Close()
				ctrl := newControllerMock(t, oneShardCfg(1, "n1", dn))
				c := newClient(t, ctrl.srv.URL)

				err := op(c)
				require.Error(t, err)
				assert.Contains(t, err.Error(), "max retries exceeded")
				assert.Equal(t, c.maxRetries, seq.count(), "exactly maxRetries attempts, no more")
			})
		}
	}
}

func TestSmartClient_Get_MaxRetriesExceededOnLocked(t *testing.T) {
	seq := &statusSeq{statuses: []int{423}}
	dn := httptest.NewServer(seq.handler(""))
	defer dn.Close()
	ctrl := newControllerMock(t, oneShardCfg(1, "n1", dn))
	c := newClient(t, ctrl.srv.URL)
	_, err := c.Get("k")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "max retries exceeded")
	assert.Equal(t, c.maxRetries, seq.count())
}

func TestSmartClient_NonRetryableStatusesReturnImmediately(t *testing.T) {
	seq := &statusSeq{statuses: []int{418}}
	dn := httptest.NewServer(seq.handler(""))
	defer dn.Close()
	ctrl := newControllerMock(t, oneShardCfg(1, "n1", dn))
	c := newClient(t, ctrl.srv.URL)

	err := c.Put("k", "v")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "418")
	assert.Equal(t, 1, seq.count())

	err = c.Delete("k")
	require.Error(t, err)
	assert.Equal(t, 2, seq.count())

	_, err = c.Get("k")
	require.Error(t, err)
	assert.Equal(t, 3, seq.count())
}

func TestSmartClient_Get_NotFound(t *testing.T) {
	seq := &statusSeq{statuses: []int{404}}
	dn := httptest.NewServer(seq.handler(""))
	defer dn.Close()
	ctrl := newControllerMock(t, oneShardCfg(1, "n1", dn))
	c := newClient(t, ctrl.srv.URL)
	_, err := c.Get("missing")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "key not found")
	assert.Equal(t, 1, seq.count(), "404 is definitive, not retried")
}

func TestSmartClient_DatanodeDown_RefreshesAndFailsOver(t *testing.T) {
	dead := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	deadCfg := oneShardCfg(1, "old", dead)
	dead.Close() // connection refused from now on

	var newHits atomic.Int32
	newDN := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { newHits.Add(1) }))
	defer newDN.Close()

	ctrl := newControllerMock(t, deadCfg)
	c := newClient(t, ctrl.srv.URL)
	ctrl.cfg.Store(oneShardCfg(2, "new", newDN))

	require.NoError(t, c.Put("k", "v"))
	assert.EqualValues(t, 1, newHits.Load())
}

func TestSmartClient_DatanodeDown_NoFailoverExhaustsRetries(t *testing.T) {
	dead := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	cfg := oneShardCfg(1, "old", dead)
	dead.Close()
	ctrl := newControllerMock(t, cfg)
	c := newClient(t, ctrl.srv.URL)

	assert.Error(t, c.Put("k", "v"))
	_, err := c.Get("k")
	assert.Error(t, err)
	assert.Error(t, c.Delete("k"))
}

func TestSmartClient_AllControllersDown(t *testing.T) {
	a := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	b := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	urls := []string{a.URL, b.URL}
	a.Close()
	b.Close()

	_, err := NewSmartClient(urls)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to fetch initial topology")
}

func TestSmartClient_ControllersDownAfterStart_OperationsFailWithoutPanic(t *testing.T) {
	// Topology is stale (n1 returns 400 forever) and controllers vanish.
	dn := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer dn.Close()
	ctrl := newControllerMock(t, oneShardCfg(1, "n1", dn))
	c := newClient(t, ctrl.srv.URL)
	ctrl.srv.Close()

	err := c.Put("k", "v")
	require.Error(t, err)
	assert.Equal(t, uint64(1), c.GetEpoch(), "last known topology is retained when refresh fails")
	assert.Error(t, c.FetchTopology())
}

func TestSmartClient_TopologyRefreshFailoverBetweenControllers(t *testing.T) {
	dn := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer dn.Close()

	var primaryUp atomic.Bool
	primaryUp.Store(true)
	var primaryHits atomic.Int32
	primary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		primaryHits.Add(1)
		if !primaryUp.Load() {
			http.Error(w, "not leader", http.StatusServiceUnavailable)
			return
		}
		json.NewEncoder(w).Encode(oneShardCfg(1, "n1", dn))
	}))
	defer primary.Close()
	secondary := newControllerMock(t, oneShardCfg(7, "n1", dn))

	c := newClient(t, primary.URL, secondary.srv.URL)
	require.Equal(t, uint64(1), c.GetEpoch())

	primaryUp.Store(false)
	require.NoError(t, c.FetchTopology())
	assert.Equal(t, uint64(7), c.GetEpoch(), "refresh must fall through to the second controller")
	assert.EqualValues(t, 2, primaryHits.Load())
}

func TestSmartClient_FetchTopology_BadPayloadsFallThrough(t *testing.T) {
	garbage := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("{not json")) }))
	defer garbage.Close()
	good := newControllerMock(t, &metadata.ClusterConfig{Epoch: 3})
	c := newClient(t, garbage.URL, good.srv.URL)
	assert.Equal(t, uint64(3), c.GetEpoch())
}

func TestSmartClient_RequestTimeoutOnSlowServer(t *testing.T) {
	release := make(chan struct{})
	dn := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	defer dn.Close()
	defer close(release)
	ctrl := newControllerMock(t, oneShardCfg(1, "n1", dn))
	c := newClient(t, ctrl.srv.URL)
	c.httpClient.Timeout = 50 * time.Millisecond
	c.maxRetries = 2

	start := time.Now()
	err := c.Put("k", "v")
	elapsed := time.Since(start)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "max retries exceeded")
	assert.Less(t, elapsed, 2*time.Second, "each attempt bounded by client timeout")
	assert.GreaterOrEqual(t, elapsed, 100*time.Millisecond, "both attempts actually waited for the timeout")
}

func TestSmartClient_RouteErrors(t *testing.T) {
	c := &SmartClient{}
	_, err := c.Route("k")
	assert.Error(t, err, "no topology")

	c.config = &metadata.ClusterConfig{TotalShards: 0}
	_, err = c.Route("k")
	assert.Error(t, err, "no shards")

	c.config = &metadata.ClusterConfig{TotalShards: 1, Shards: map[int]metadata.ShardMetadata{}}
	_, err = c.Route("k")
	assert.Error(t, err, "shard missing")

	c.config = &metadata.ClusterConfig{
		TotalShards: 1,
		Shards:      map[int]metadata.ShardMetadata{0: {ID: 0, PrimaryID: "ghost"}},
		Nodes:       map[string]metadata.NodeMetadata{},
	}
	_, err = c.Route("k")
	assert.Error(t, err, "primary missing")

	c.config = &metadata.ClusterConfig{
		TotalShards: 1,
		Shards:      map[int]metadata.ShardMetadata{0: {ID: 0, PrimaryID: ""}},
		Nodes:       map[string]metadata.NodeMetadata{},
	}
	_, err = c.Route("k")
	assert.Error(t, err, "shard with no primary (all nodes dead) is not routable")
}

func TestSmartClient_Route_DeterministicAndSpreads(t *testing.T) {
	cfg := &metadata.ClusterConfig{Epoch: 1, TotalShards: 4,
		Nodes:  map[string]metadata.NodeMetadata{},
		Shards: map[int]metadata.ShardMetadata{},
	}
	for i := 0; i < 4; i++ {
		id := string(rune('a' + i))
		cfg.Nodes[id] = metadata.NodeMetadata{ID: id, Address: id + ":1"}
		cfg.Shards[i] = metadata.ShardMetadata{ID: i, PrimaryID: id}
	}
	c := &SmartClient{config: cfg}

	seen := map[int]bool{}
	for i := 0; i < 200; i++ {
		k := "key-" + strings.Repeat("x", i%7) + string(rune('0'+i%10)) + string(rune('A'+i%26))
		r1, err := c.Route(k)
		require.NoError(t, err)
		r2, _ := c.Route(k)
		assert.Equal(t, r1, r2, "routing is deterministic for %q", k)
		assert.Equal(t, "http://"+string(rune('a'+r1.ShardID))+":1", r1.TargetURL)
		seen[r1.ShardID] = true
	}
	assert.Greater(t, len(seen), 1, "keys should spread over shards")
}

func TestSmartClient_ConcurrentCallsAndRefreshRace(t *testing.T) {
	var puts, gets, dels atomic.Int32
	dn := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			puts.Add(1)
		case http.MethodDelete:
			dels.Add(1)
		case http.MethodGet:
			gets.Add(1)
			w.Write([]byte("v"))
		}
	}))
	defer dn.Close()
	ctrl := newControllerMock(t, oneShardCfg(1, "n1", dn))
	c := newClient(t, ctrl.srv.URL)

	var wg sync.WaitGroup
	const workers, iters = 16, 25
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < iters; i++ {
				k := "k" + string(rune('a'+w))
				assert.NoError(t, c.Put(k, "v"))
				v, err := c.Get(k)
				assert.NoError(t, err)
				assert.Equal(t, "v", v)
				assert.NoError(t, c.Delete(k))
				if i%5 == 0 {
					assert.NoError(t, c.FetchTopology())
					_ = c.GetConfig()
					_ = c.GetEpoch()
				}
			}
		}(w)
	}
	wg.Wait()
	assert.EqualValues(t, workers*iters, puts.Load())
	assert.EqualValues(t, workers*iters, gets.Load())
	assert.EqualValues(t, workers*iters, dels.Load())
}
