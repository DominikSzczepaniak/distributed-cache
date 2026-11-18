package datanode

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dominikszczepaniak/distributed-cache/pkg/metadata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// controllerStub answers /cluster/heartbeat and /topology with switchable health.
type controllerStub struct {
	srv        *httptest.Server
	healthy    atomic.Bool
	heartbeats atomic.Int32
	epoch      atomic.Uint64
	topoStatus atomic.Int32
}

func newControllerStub(t *testing.T) *controllerStub {
	t.Helper()
	c := &controllerStub{}
	c.healthy.Store(true)
	c.topoStatus.Store(http.StatusOK)
	c.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/cluster/heartbeat":
			c.heartbeats.Add(1)
			if !c.healthy.Load() {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			json.NewEncoder(w).Encode(map[string]uint64{"epoch": c.epoch.Load()})
		case "/topology":
			if s := int(c.topoStatus.Load()); s != http.StatusOK {
				w.WriteHeader(s)
				return
			}
			json.NewEncoder(w).Encode(metadata.ClusterConfig{Epoch: c.epoch.Load(), TotalShards: 3})
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(c.srv.Close)
	return c
}

func TestLeaseManager_StartRenewLoop_RenewsLosesAndRecovers(t *testing.T) {
	ctrl := newControllerStub(t)
	lm := NewLeaseManager(ctrl.srv.URL, "node-1", 150*time.Millisecond, NewStateManager()) // ticks every 50ms
	lm.Start()
	defer lm.Stop()

	require.True(t, lm.IsActive(), "Start performs a synchronous first renewal")

	// Keeps renewing well beyond a single lease duration.
	start := ctrl.heartbeats.Load()
	require.Eventually(t, func() bool { return ctrl.heartbeats.Load() >= start+4 }, 3*time.Second, 10*time.Millisecond)
	assert.True(t, lm.IsActive(), "lease stays valid while heartbeats succeed")

	// Controller starts failing: lease must lapse on its own (node fences itself).
	ctrl.healthy.Store(false)
	require.Eventually(t, func() bool { return !lm.IsActive() }, 3*time.Second, 10*time.Millisecond, "node must self-fence when it cannot renew")

	// Controller recovers: loop re-acquires the lease without a restart.
	ctrl.healthy.Store(true)
	require.Eventually(t, lm.IsActive, 3*time.Second, 10*time.Millisecond, "lease must be re-acquired after recovery")
}

func TestLeaseManager_Start_ControllerDownAtBoot_NotActiveUntilItComesUp(t *testing.T) {
	ctrl := newControllerStub(t)
	ctrl.healthy.Store(false)
	lm := NewLeaseManager(ctrl.srv.URL, "node-1", 150*time.Millisecond, NewStateManager())
	lm.Start()
	defer lm.Stop()

	assert.False(t, lm.IsActive(), "a node that never heard from the controller must not serve")
	ctrl.healthy.Store(true)
	require.Eventually(t, lm.IsActive, 3*time.Second, 10*time.Millisecond)
}

func TestLeaseManager_Stop_HaltsHeartbeats(t *testing.T) {
	ctrl := newControllerStub(t)
	lm := NewLeaseManager(ctrl.srv.URL, "node-1", 90*time.Millisecond, NewStateManager())
	lm.Start()
	require.Eventually(t, func() bool { return ctrl.heartbeats.Load() >= 3 }, 3*time.Second, 10*time.Millisecond)

	lm.Stop()
	lm.Stop()                         // idempotent
	time.Sleep(60 * time.Millisecond) // let any in-flight tick drain
	n := ctrl.heartbeats.Load()
	time.Sleep(200 * time.Millisecond)
	assert.Equal(t, n, ctrl.heartbeats.Load(), "no heartbeats after Stop")
}

func TestLeaseManager_Renew_TopologyRefreshOnlyWhenRemoteEpochNewer(t *testing.T) {
	ctrl := newControllerStub(t)
	sm := NewStateManager()
	lm := NewLeaseManager(ctrl.srv.URL, "node-1", time.Second, sm)

	ctrl.epoch.Store(0)
	assert.True(t, lm.renew())
	assert.Equal(t, 0, sm.Get().TotalShards, "same epoch: no topology fetch")

	ctrl.epoch.Store(3)
	assert.True(t, lm.renew())
	assert.Equal(t, uint64(3), sm.GetEpoch())
	assert.Equal(t, 3, sm.Get().TotalShards)
}

func TestLeaseManager_Renew_TopologyFetchFailureKeepsOldStateButLeaseStillRenews(t *testing.T) {
	ctrl := newControllerStub(t)
	sm := NewStateManager()
	sm.Update(&metadata.ClusterConfig{Epoch: 1, TotalShards: 1})
	ctrl.epoch.Store(5)
	ctrl.topoStatus.Store(http.StatusInternalServerError)
	lm := NewLeaseManager(ctrl.srv.URL, "node-1", time.Second, sm)

	assert.True(t, lm.renew(), "heartbeat itself succeeded")
	assert.Equal(t, uint64(1), sm.GetEpoch(), "failed topology fetch must not corrupt local state")
}

func TestLeaseManager_Renew_UndecodableHeartbeatBodyStillCountsAsSuccess(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("not json")) }))
	defer srv.Close()
	lm := NewLeaseManager(srv.URL, "node-1", time.Second, NewStateManager())
	assert.True(t, lm.renew())
}

func TestLeaseManager_Renew_ControllerUnreachable(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	url := srv.URL
	srv.Close()
	lm := NewLeaseManager(url, "node-1", time.Second, NewStateManager())
	assert.False(t, lm.renew())
	assert.False(t, lm.IsActive())
}

func TestLeaseManager_ConcurrentIsActiveAndExtend(t *testing.T) {
	lm := NewLeaseManager("http://127.0.0.1:1", "n", time.Minute, NewStateManager())
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			for j := 0; j < 500; j++ {
				lm.extendLease()
			}
		}()
		go func() {
			defer wg.Done()
			for j := 0; j < 500; j++ {
				lm.IsActive()
			}
		}()
	}
	wg.Wait()
	assert.True(t, lm.IsActive())
}
