package datanode

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/dominikszczepaniak/distributed-cache/pkg/metadata"
	"github.com/stretchr/testify/assert"
)

func TestLeaseManager_RenewSuccess(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/cluster/heartbeat", r.URL.Path)
		assert.Equal(t, "POST", r.Method)
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"epoch": 0}`))
	}))
	defer server.Close()

	leaseDuration := 100 * time.Millisecond
	stateMgr := NewStateManager()
	lm := NewLeaseManager(server.URL, "node-1", leaseDuration, stateMgr)

	success := lm.renew()
	assert.True(t, success)

	lm.extendLease()
	assert.True(t, lm.IsActive())
}

func TestLeaseManager_RenewFailure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	leaseDuration := 50 * time.Millisecond
	stateMgr := NewStateManager()
	lm := NewLeaseManager(server.URL, "node-1", leaseDuration, stateMgr)

	success := lm.renew()
	assert.False(t, success)

	assert.False(t, lm.IsActive())
}

func TestLeaseManager_Expiration(t *testing.T) {
	leaseDuration := 50 * time.Millisecond
	stateMgr := NewStateManager()
	lm := NewLeaseManager("http://invalid-url", "node-1", leaseDuration, stateMgr)

	lm.extendLease()
	assert.True(t, lm.IsActive())

	time.Sleep(2 * leaseDuration)
	assert.False(t, lm.IsActive())
}

func TestLeaseManager_EpochUpdate(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/cluster/heartbeat" {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"epoch": 5}`))
			return
		}
		if r.URL.Path == "/topology" {
			w.WriteHeader(http.StatusOK)
			config := metadata.ClusterConfig{
				Epoch:       5,
				TotalShards: 10,
			}
			json.NewEncoder(w).Encode(config)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	leaseDuration := 100 * time.Millisecond
	stateMgr := NewStateManager()

	assert.Equal(t, uint64(0), stateMgr.GetEpoch())

	lm := NewLeaseManager(server.URL, "node-1", leaseDuration, stateMgr)

	success := lm.renew()
	assert.True(t, success)

	assert.Equal(t, uint64(5), stateMgr.GetEpoch())
	assert.Equal(t, 10, stateMgr.Get().TotalShards)
}
