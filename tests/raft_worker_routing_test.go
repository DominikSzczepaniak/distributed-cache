package tests

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRaftWorkerRouting_RedirectToPrimary tests that Raft nodes return 307 redirects to workers
func TestRaftWorkerRouting_RedirectToPrimary(t *testing.T) {
	t.Log("Testing Raft → Worker routing with HTTP 307 redirects")

	// NOTE: This test requires:
	// 1. At least 1 Raft node running (e.g., http://localhost:8080)
	// 2. At least 1 Worker node registered
	// 3. Partition table initialized with worker assignments

	// Test configuration
	raftNodeAddr := "http://localhost:8080"
	testKey := 123
	testValue := 456

	// Create HTTP client that does NOT follow redirects automatically
	client := &http.Client{
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse // Don't follow redirects
		},
		Timeout: 5 * time.Second,
	}

	t.Run("PUT returns 307 redirect", func(t *testing.T) {
		// Send PUT request to Raft node
		putReq := map[string]int{"key": testKey, "value": testValue}
		body, err := json.Marshal(putReq)
		require.NoError(t, err)

		resp, err := client.Post(fmt.Sprintf("%s/kv", raftNodeAddr), "application/json", bytes.NewBuffer(body))
		require.NoError(t, err)
		defer resp.Body.Close()

		// Verify HTTP 307 Temporary Redirect
		assert.Equal(t, http.StatusTemporaryRedirect, resp.StatusCode, "Expected 307 redirect from Raft node")

		// Verify Location header is set
		location := resp.Header.Get("Location")
		assert.NotEmpty(t, location, "Location header should be set")
		t.Logf("Redirect Location: %s", location)

		// Parse response body
		var redirectResp map[string]interface{}
		err = json.NewDecoder(resp.Body).Decode(&redirectResp)
		require.NoError(t, err)

		// Verify response contains worker information
		assert.Equal(t, "Request should be sent to worker node", redirectResp["message"])
		assert.NotNil(t, redirectResp["worker_id"], "Response should include worker_id")
		assert.NotNil(t, redirectResp["worker_address"], "Response should include worker_address")
		assert.NotNil(t, redirectResp["partition_id"], "Response should include partition_id")
		assert.NotNil(t, redirectResp["redirect_url"], "Response should include redirect_url")

		t.Logf("Worker ID: %v", redirectResp["worker_id"])
		t.Logf("Worker Address: %v", redirectResp["worker_address"])
		t.Logf("Partition ID: %v", redirectResp["partition_id"])
	})

	t.Run("GET returns 307 redirect", func(t *testing.T) {
		// Send GET request to Raft node
		resp, err := client.Get(fmt.Sprintf("%s/kv/%d", raftNodeAddr, testKey))
		require.NoError(t, err)
		defer resp.Body.Close()

		// Verify HTTP 307 Temporary Redirect
		assert.Equal(t, http.StatusTemporaryRedirect, resp.StatusCode, "Expected 307 redirect from Raft node")

		// Verify Location header is set
		location := resp.Header.Get("Location")
		assert.NotEmpty(t, location, "Location header should be set")
		t.Logf("Redirect Location: %s", location)

		// Parse response body
		var redirectResp map[string]interface{}
		err = json.NewDecoder(resp.Body).Decode(&redirectResp)
		require.NoError(t, err)

		// Verify response contains worker information
		assert.Equal(t, "Request should be sent to worker node", redirectResp["message"])
		assert.NotNil(t, redirectResp["worker_id"])
		assert.NotNil(t, redirectResp["worker_address"])
	})

	t.Run("DELETE returns 307 redirect", func(t *testing.T) {
		// Send DELETE request to Raft node
		req, err := http.NewRequest(http.MethodDelete, fmt.Sprintf("%s/kv/%d", raftNodeAddr, testKey), nil)
		require.NoError(t, err)

		resp, err := client.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		// Verify HTTP 307 Temporary Redirect
		assert.Equal(t, http.StatusTemporaryRedirect, resp.StatusCode, "Expected 307 redirect from Raft node")

		// Verify Location header is set
		location := resp.Header.Get("Location")
		assert.NotEmpty(t, location, "Location header should be set")
		t.Logf("Redirect Location: %s", location)
	})
}

// TestRaftWorkerRouting_FollowRedirect tests following redirects to workers
func TestRaftWorkerRouting_FollowRedirect(t *testing.T) {
	t.Log("Testing redirect following from Raft to Worker")

	raftNodeAddr := "http://localhost:8080"
	testKey := 789
	testValue := 999

	// Create HTTP client that FOLLOWS redirects automatically
	client := &http.Client{
		Timeout: 5 * time.Second,
	}

	t.Run("PUT with redirect following", func(t *testing.T) {
		// Send PUT request to Raft node (will follow redirect automatically)
		putReq := map[string]int{"key": testKey, "value": testValue}
		body, err := json.Marshal(putReq)
		require.NoError(t, err)

		resp, err := client.Post(fmt.Sprintf("%s/kv", raftNodeAddr), "application/json", bytes.NewBuffer(body))
		require.NoError(t, err)
		defer resp.Body.Close()

		// After following redirect, should get 200 OK from worker
		assert.Equal(t, http.StatusOK, resp.StatusCode, "Expected 200 OK from worker after redirect")

		// Parse success response
		var putResp map[string]interface{}
		err = json.NewDecoder(resp.Body).Decode(&putResp)
		require.NoError(t, err)

		assert.True(t, putResp["success"].(bool), "PUT should succeed on worker")
		t.Logf("PUT succeeded on worker: %v", putResp)
	})

	t.Run("GET with redirect following", func(t *testing.T) {
		// Send GET request to Raft node (will follow redirect automatically)
		resp, err := client.Get(fmt.Sprintf("%s/kv/%d", raftNodeAddr, testKey))
		require.NoError(t, err)
		defer resp.Body.Close()

		// After following redirect, should get 200 OK from worker
		assert.Equal(t, http.StatusOK, resp.StatusCode, "Expected 200 OK from worker after redirect")

		// Parse response
		var getResp map[string]interface{}
		err = json.NewDecoder(resp.Body).Decode(&getResp)
		require.NoError(t, err)

		assert.Equal(t, float64(testKey), getResp["key"].(float64))
		assert.Equal(t, float64(testValue), getResp["value"].(float64))
		t.Logf("GET succeeded on worker: %v", getResp)
	})
}

// TestRaftWorkerRouting_MultipleKeys tests that different keys route to correct workers
func TestRaftWorkerRouting_MultipleKeys(t *testing.T) {
	t.Log("Testing multiple keys route to correct workers based on partition table")

	raftNodeAddr := "http://localhost:8080"

	// Create HTTP client that does NOT follow redirects
	client := &http.Client{
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
		Timeout: 5 * time.Second,
	}

	// Test multiple keys and track which workers they map to
	workerAssignments := make(map[int][]int) // worker_id -> []keys

	testKeys := []int{100, 200, 300, 400, 500, 600, 700, 800, 900, 1000}

	for _, key := range testKeys {
		putReq := map[string]int{"key": key, "value": key * 10}
		body, err := json.Marshal(putReq)
		require.NoError(t, err)

		resp, err := client.Post(fmt.Sprintf("%s/kv", raftNodeAddr), "application/json", bytes.NewBuffer(body))
		require.NoError(t, err)

		// Should get 307 redirect
		assert.Equal(t, http.StatusTemporaryRedirect, resp.StatusCode)

		// Parse redirect response
		var redirectResp map[string]interface{}
		bodyBytes, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		json.Unmarshal(bodyBytes, &redirectResp)

		workerID := int(redirectResp["worker_id"].(float64))
		workerAssignments[workerID] = append(workerAssignments[workerID], key)

		t.Logf("Key %d → Worker %d (Partition %v)", key, workerID, redirectResp["partition_id"])
	}

	// Verify that keys are distributed across workers
	t.Logf("Worker assignments:")
	for workerID, keys := range workerAssignments {
		t.Logf("  Worker %d: %d keys %v", workerID, len(keys), keys)
	}

	// With multiple workers, we should see distribution
	// (this assumes test environment has 2+ workers registered)
	if len(workerAssignments) > 1 {
		t.Log("✓ Keys distributed across multiple workers")
	} else {
		t.Log("Note: Only 1 worker detected, may be expected for single-worker test")
	}
}

// TestRaftWorkerRouting_WorkerUnavailable tests error handling when worker is unavailable
func TestRaftWorkerRouting_WorkerUnavailable(t *testing.T) {
	t.Skip("Skipping unavailable worker test - requires controlled worker failure")

	// This test would require:
	// 1. Registering a worker
	// 2. Stopping the worker
	// 3. Attempting to route to that worker
	// 4. Verifying 503 Service Unavailable response

	// Implementation would be similar to TestRaftWorkerRouting_RedirectToPrimary
	// but with controlled worker shutdown
}

// TestRaftWorkerRouting_NoWorkersRegistered tests behavior when no workers are registered
func TestRaftWorkerRouting_NoWorkersRegistered(t *testing.T) {
	t.Skip("Skipping no-workers test - requires fresh cluster without workers")

	// This test would require starting Raft cluster without any workers registered
	// Expected behavior: 503 Service Unavailable when trying to route requests
}

// TestEndToEnd_RaftWorkerFlow tests complete flow: Client → Raft → Worker → Response
func TestEndToEnd_RaftWorkerFlow(t *testing.T) {
	t.Log("Testing complete end-to-end flow: Client → Raft → Worker → Response")

	raftNodeAddr := "http://localhost:8080"

	// Use client that follows redirects
	client := &http.Client{Timeout: 5 * time.Second}

	testCases := []struct {
		key   int
		value int
	}{
		{1111, 2222},
		{3333, 4444},
		{5555, 6666},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("Key%d", tc.key), func(t *testing.T) {
			// 1. PUT via Raft (redirects to worker)
			putReq := map[string]int{"key": tc.key, "value": tc.value}
			body, err := json.Marshal(putReq)
			require.NoError(t, err)

			putResp, err := client.Post(fmt.Sprintf("%s/kv", raftNodeAddr), "application/json", bytes.NewBuffer(body))
			require.NoError(t, err)
			defer putResp.Body.Close()

			assert.Equal(t, http.StatusOK, putResp.StatusCode, "PUT should succeed after redirect")

			// 2. GET via Raft (redirects to worker)
			getResp, err := client.Get(fmt.Sprintf("%s/kv/%d", raftNodeAddr, tc.key))
			require.NoError(t, err)
			defer getResp.Body.Close()

			assert.Equal(t, http.StatusOK, getResp.StatusCode, "GET should succeed after redirect")

			// 3. Verify value
			var getResult map[string]interface{}
			err = json.NewDecoder(getResp.Body).Decode(&getResult)
			require.NoError(t, err)

			assert.Equal(t, float64(tc.value), getResult["value"].(float64), "Value should match")

			// 4. DELETE via Raft (redirects to worker)
			delReq, err := http.NewRequest(http.MethodDelete, fmt.Sprintf("%s/kv/%d", raftNodeAddr, tc.key), nil)
			require.NoError(t, err)

			delResp, err := client.Do(delReq)
			require.NoError(t, err)
			defer delResp.Body.Close()

			// DELETE returns 204 No Content or 200 OK
			assert.True(t, delResp.StatusCode == http.StatusOK || delResp.StatusCode == http.StatusNoContent,
				"DELETE should succeed")

			// 5. Verify deletion - GET should return 404
			getResp2, err := client.Get(fmt.Sprintf("%s/kv/%d", raftNodeAddr, tc.key))
			require.NoError(t, err)
			defer getResp2.Body.Close()

			assert.Equal(t, http.StatusNotFound, getResp2.StatusCode, "Key should be deleted")

			t.Logf("✓ Complete flow verified for key=%d", tc.key)
		})
	}
}
