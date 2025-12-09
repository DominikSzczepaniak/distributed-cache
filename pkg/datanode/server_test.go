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
	c := cache.NewConcurrentMapCache()
	stateMgr := NewStateManager()
	stateMgr.Update(&metadata.ClusterConfig{Epoch: 5})

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"epoch": 5}`))
	}))
	defer server.Close()

	lm := NewLeaseManager(server.URL, "node-1", 1*time.Hour, stateMgr)
	lm.renew()
	lm.extendLease()

	srv := NewServer(c, lm, stateMgr, "node-1")

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

	lm := NewLeaseManager("http://invalid", "node-1", 1*time.Millisecond, stateMgr)

	srv := NewServer(c, lm, stateMgr, "node-1")

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

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"epoch": 10}`))
	}))
	defer server.Close()

	lm := NewLeaseManager(server.URL, "node-1", 1*time.Hour, stateMgr)
	lm.renew()
	lm.extendLease()

	srv := NewServer(c, lm, stateMgr, "node-1")

	reqBody := WriteRequest{Key: "foo", Value: "bar", Epoch: 5}
	body, _ := json.Marshal(reqBody)
	req := httptest.NewRequest("POST", "/data", bytes.NewBuffer(body))
	w := httptest.NewRecorder()

	srv.HandlePut(w, req)

	assert.Equal(t, http.StatusPreconditionFailed, w.Code)
}

func TestServer_HandlePut_NotPrimary(t *testing.T) {
	c := cache.NewConcurrentMapCache()
	stateMgr := NewStateManager()

	config := &metadata.ClusterConfig{
		Epoch:       5,
		TotalShards: 1,
		Shards: map[int]metadata.ShardMetadata{
			0: {ID: 0, PrimaryID: "node-2", ReplicaIDs: []string{}},
		},
	}
	stateMgr.Update(config)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"epoch": 5}`))
	}))
	defer server.Close()

	lm := NewLeaseManager(server.URL, "node-1", 1*time.Hour, stateMgr)
	lm.renew()
	lm.extendLease()

	srv := NewServer(c, lm, stateMgr, "node-1")

	reqBody := WriteRequest{Key: "foo", Value: "bar", Epoch: 5}
	body, _ := json.Marshal(reqBody)
	req := httptest.NewRequest("POST", "/data", bytes.NewBuffer(body))
	w := httptest.NewRecorder()

	srv.HandlePut(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
	assert.Contains(t, w.Body.String(), "Not Primary")
}

func TestServer_HandleReplicate_Success(t *testing.T) {
	c := cache.NewConcurrentMapCache()
	stateMgr := NewStateManager()

	config := &metadata.ClusterConfig{Epoch: 5}
	stateMgr.Update(config)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"epoch": 5}`))
	}))
	defer server.Close()

	lm := NewLeaseManager(server.URL, "node-1", 1*time.Hour, stateMgr)
	srv := NewServer(c, lm, stateMgr, "node-1")

	reqBody := WriteRequest{Key: "foo", Value: "replicated", Epoch: 5}
	body, _ := json.Marshal(reqBody)
	req := httptest.NewRequest("POST", "/internal/replicate", bytes.NewBuffer(body))
	w := httptest.NewRecorder()

	srv.HandleReplicate(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "replicated", c.Get("foo"))
}

func TestServer_HandleReplicate_StalePrimary(t *testing.T) {
	c := cache.NewConcurrentMapCache()
	stateMgr := NewStateManager()

	config := &metadata.ClusterConfig{Epoch: 10}
	stateMgr.Update(config)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"epoch": 10}`))
	}))
	defer server.Close()

	lm := NewLeaseManager(server.URL, "node-1", 1*time.Hour, stateMgr)
	srv := NewServer(c, lm, stateMgr, "node-1")

	reqBody := WriteRequest{Key: "foo", Value: "stale", Epoch: 5}
	body, _ := json.Marshal(reqBody)
	req := httptest.NewRequest("POST", "/internal/replicate", bytes.NewBuffer(body))
	w := httptest.NewRecorder()

	srv.HandleReplicate(w, req)

	assert.Equal(t, http.StatusPreconditionFailed, w.Code)
	assert.Contains(t, w.Body.String(), "Primary is Stale")
}

func TestServer_HandlePut_ReplicationSuccess(t *testing.T) {
	c := cache.NewConcurrentMapCache()
	stateMgr := NewStateManager()

	replicaMock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer replicaMock.Close()

	replicaAddr := replicaMock.URL[7:]

	config := &metadata.ClusterConfig{
		Epoch:       5,
		TotalShards: 1,
		Nodes: map[string]metadata.NodeMetadata{
			"node-1":    {ID: "node-1", Address: "node-1:9000", Status: metadata.StatusActive},
			"replica-1": {ID: "replica-1", Address: replicaAddr, Status: metadata.StatusActive},
		},
		Shards: map[int]metadata.ShardMetadata{
			0: {ID: 0, PrimaryID: "node-1", ReplicaIDs: []string{"replica-1"}},
		},
	}
	stateMgr.Update(config)

	controller := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"epoch": 5}`))
	}))
	defer controller.Close()

	lm := NewLeaseManager(controller.URL, "node-1", 1*time.Hour, stateMgr)
	lm.renew()
	lm.extendLease()

	srv := NewServer(c, lm, stateMgr, "node-1")

	reqBody := WriteRequest{Key: "foo", Value: "bar", Epoch: 5}
	body, _ := json.Marshal(reqBody)
	req := httptest.NewRequest("POST", "/data", bytes.NewBuffer(body))
	w := httptest.NewRecorder()

	srv.HandlePut(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "bar", c.Get("foo"))
}

func TestServer_HandlePut_ReplicationFailure(t *testing.T) {
	c := cache.NewConcurrentMapCache()
	stateMgr := NewStateManager()

	replicaMock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "Internal Error", http.StatusInternalServerError)
	}))
	defer replicaMock.Close()

	replicaAddr := replicaMock.URL[7:]

	config := &metadata.ClusterConfig{
		Epoch:       5,
		TotalShards: 1,
		Nodes: map[string]metadata.NodeMetadata{
			"node-1":    {ID: "node-1", Address: "node-1:9000", Status: metadata.StatusActive},
			"replica-1": {ID: "replica-1", Address: replicaAddr, Status: metadata.StatusActive},
		},
		Shards: map[int]metadata.ShardMetadata{
			0: {ID: 0, PrimaryID: "node-1", ReplicaIDs: []string{"replica-1"}},
		},
	}
	stateMgr.Update(config)

	controller := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"epoch": 5}`))
	}))
	defer controller.Close()

	lm := NewLeaseManager(controller.URL, "node-1", 1*time.Hour, stateMgr)
	lm.renew()
	lm.extendLease()

	srv := NewServer(c, lm, stateMgr, "node-1")

	reqBody := WriteRequest{Key: "foo", Value: "bar", Epoch: 5}
	body, _ := json.Marshal(reqBody)
	req := httptest.NewRequest("POST", "/data", bytes.NewBuffer(body))
	w := httptest.NewRecorder()

	srv.HandlePut(w, req)

	assert.Equal(t, http.StatusInternalServerError, w.Code)
	assert.Contains(t, w.Body.String(), "Replication Failed")
	assert.Empty(t, c.Get("foo"))
}
