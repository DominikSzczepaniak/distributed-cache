package client

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClient_Put_Success(t *testing.T) {
	// Mock server that responds with success
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodPost, r.Method)
		assert.Equal(t, "/kv", r.URL.Path)
		assert.Equal(t, "application/json", r.Header.Get("Content-Type"))

		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(PutResponse{Success: true, Message: "OK"})
	}))
	defer server.Close()

	client := NewClient()
	err := client.Put(server.URL, 42, 100)
	require.NoError(t, err)
}

func TestClient_Put_WithRedirect(t *testing.T) {
	redirectCount := 0
	targetServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(PutResponse{Success: true})
	}))
	defer targetServer.Close()

	sourceServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		redirectCount++
		w.WriteHeader(307)
		json.NewEncoder(w).Encode(RedirectResponse{
			Error:   "MOVED",
			Address: targetServer.URL,
			NodeID:  "2",
		})
	}))
	defer sourceServer.Close()

	client := NewClient()
	err := client.Put(sourceServer.URL, 42, 100)
	require.NoError(t, err)
	assert.Equal(t, 1, redirectCount, "Should have received one redirect")
}

func TestClient_Put_TooManyRedirects(t *testing.T) {
	redirectCount := 0

	// Create a server that always redirects to itself
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		redirectCount++
		w.WriteHeader(307)
		// Redirect back to same server to create loop
		json.NewEncoder(w).Encode(RedirectResponse{
			Error:   "MOVED",
			Address: server.URL,
			NodeID:  "2",
		})
	}))
	defer server.Close()

	client := NewClient()
	err := client.Put(server.URL, 42, 100)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "too many redirects")
	assert.Equal(t, 3, redirectCount, "Should have tried max redirects")
}

func TestClient_Get_Success(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodGet, r.Method)
		assert.Equal(t, "/kv/42", r.URL.Path)

		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(GetResponse{Key: 42, Value: 100})
	}))
	defer server.Close()

	client := NewClient()
	value, err := client.Get(server.URL, 42)
	require.NoError(t, err)
	assert.Equal(t, 100, value)
}

func TestClient_Get_WithRedirect(t *testing.T) {
	targetServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(GetResponse{Key: 42, Value: 200})
	}))
	defer targetServer.Close()

	sourceServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(307)
		json.NewEncoder(w).Encode(RedirectResponse{
			Error:   "MOVED",
			Address: targetServer.URL,
		})
	}))
	defer sourceServer.Close()

	client := NewClient()
	value, err := client.Get(sourceServer.URL, 42)
	require.NoError(t, err)
	assert.Equal(t, 200, value)
}

func TestClient_Delete_Success(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodDelete, r.Method)
		assert.Equal(t, "/kv/42", r.URL.Path)

		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client := NewClient()
	err := client.Delete(server.URL, 42)
	require.NoError(t, err)
}

func TestClient_Delete_WithRedirect(t *testing.T) {
	targetServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer targetServer.Close()

	sourceServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(307)
		json.NewEncoder(w).Encode(RedirectResponse{
			Error:   "MOVED",
			Address: targetServer.URL,
		})
	}))
	defer sourceServer.Close()

	client := NewClient()
	err := client.Delete(sourceServer.URL, 42)
	require.NoError(t, err)
}

func TestClient_Health(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/health", r.URL.Path)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client := NewClient()
	err := client.Health(server.URL)
	require.NoError(t, err)
}

func TestClient_Health_Unhealthy(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()

	client := NewClient()
	err := client.Health(server.URL)
	require.Error(t, err)
}

func TestClient_GetStatus(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/status", r.URL.Path)
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"role":         "leader",
			"term":         5,
			"commit_index": 100,
		})
	}))
	defer server.Close()

	client := NewClient()
	status, err := client.GetStatus(server.URL)
	require.NoError(t, err)
	assert.Equal(t, "leader", status["role"])
	assert.Equal(t, float64(5), status["term"])
}

func TestClient_GetLeader(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/leader", r.URL.Path)
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"id":      1,
			"address": "http://leader-node:8080",
		})
	}))
	defer server.Close()

	client := NewClient()
	leaderAddr, err := client.GetLeader(server.URL)
	require.NoError(t, err)
	assert.Equal(t, "http://leader-node:8080", leaderAddr)
}

func TestClient_CustomOptions(t *testing.T) {
	client := NewClientWithOptions(5*time.Second, 5)
	assert.Equal(t, 5, client.maxRedirects)
	assert.Equal(t, 5*time.Second, client.httpClient.Timeout)
}

func TestClient_Put_InvalidResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("invalid key"))
	}))
	defer server.Close()

	client := NewClient()
	err := client.Put(server.URL, 42, 100)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unexpected status")
}

func TestClient_Get_KeyNotFound(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte("key not found"))
	}))
	defer server.Close()

	client := NewClient()
	_, err := client.Get(server.URL, 999)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "404")
}

func TestClient_RedirectWithoutAddress(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(307)
		json.NewEncoder(w).Encode(RedirectResponse{
			Error:   "MOVED",
			Address: "", // Missing address
		})
	}))
	defer server.Close()

	client := NewClient()
	err := client.Put(server.URL, 42, 100)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "without valid address")
}
