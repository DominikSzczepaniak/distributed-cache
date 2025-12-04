package datanode

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/dominikszczepaniak/distributed-cache/pkg/cache"
	"github.com/dominikszczepaniak/distributed-cache/pkg/metadata"
	"github.com/stretchr/testify/assert"
)

func TestServer_HandlePut_Success(t *testing.T) {
	// Setup
	c := cache.NewConcurrentMapCache()
	stateMgr := NewStateManager()
	stateMgr.Update(&metadata.ClusterConfig{Epoch: 5})

	// Mock LeaseManager (active)
	// We need a real LeaseManager but we can trick it or just use it with a mock controller
	// that keeps it active.
	// Easier: Just manually set validUntil in LeaseManager using reflection or just use a long duration
	// and ensure it starts active.
	// Since we can't easily access private fields, we'll use a mock controller that returns success once
	// to activate the lease.
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"epoch": 5}`))
	}))
	defer server.Close()

	lm := NewLeaseManager(server.URL, "node-1", 1*time.Hour, stateMgr)
	lm.renew() // Activate lease
	lm.extendLease()

	srv := NewServer(c, lm, stateMgr)

	// Request
	reqBody := WriteRequest{Key: "foo", Value: "bar", Epoch: 5}
	body, _ := json.Marshal(reqBody)
	req := httptest.NewRequest("POST", "/data", bytes.NewBuffer(body))
	w := httptest.NewRecorder()

	srv.HandlePut(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "bar", c.Get("foo"))
}

func TestServer_HandlePut_Fenced(t *testing.T) {
	c := cache.NewConcurrentMapCache()
	stateMgr := NewStateManager()

	// Mock LeaseManager (inactive)
	lm := NewLeaseManager("http://invalid", "node-1", 1*time.Millisecond, stateMgr)
	// Don't activate it

	srv := NewServer(c, lm, stateMgr)

	reqBody := WriteRequest{Key: "foo", Value: "bar", Epoch: 5}
	body, _ := json.Marshal(reqBody)
	req := httptest.NewRequest("POST", "/data", bytes.NewBuffer(body))
	w := httptest.NewRecorder()

	srv.HandlePut(w, req)

	assert.Equal(t, http.StatusServiceUnavailable, w.Code)
}

func TestServer_HandlePut_StaleEpoch(t *testing.T) {
	c := cache.NewConcurrentMapCache()
	stateMgr := NewStateManager()
	stateMgr.Update(&metadata.ClusterConfig{Epoch: 10})

	// Mock LeaseManager (active)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"epoch": 10}`))
	}))
	defer server.Close()

	lm := NewLeaseManager(server.URL, "node-1", 1*time.Hour, stateMgr)
	lm.renew()
	lm.extendLease()

	srv := NewServer(c, lm, stateMgr)

	// Request with old epoch
	reqBody := WriteRequest{Key: "foo", Value: "bar", Epoch: 5}
	body, _ := json.Marshal(reqBody)
	req := httptest.NewRequest("POST", "/data", bytes.NewBuffer(body))
	w := httptest.NewRecorder()

	srv.HandlePut(w, req)

	assert.Equal(t, http.StatusPreconditionFailed, w.Code)
}
