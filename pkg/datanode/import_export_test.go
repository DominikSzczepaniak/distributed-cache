package datanode

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/dominikszczepaniak/distributed-cache/pkg/cache"
	"github.com/dominikszczepaniak/distributed-cache/pkg/metadata"
)

func TestServer_ImportExport(t *testing.T) {
	c := cache.NewConcurrentMapCache()
	stateMgr := NewStateManager()
	leaseMgr := NewLeaseManager("http://controller", "node1", 5*time.Second, stateMgr)
	server := NewServer(c, leaseMgr, stateMgr, "node1")

	config := metadata.NewClusterConfig(10)
	stateMgr.Update(config)

	t.Run("HandleImport", func(t *testing.T) {
		data := map[string]string{
			"importKey": "importVal",
		}
		body, _ := json.Marshal(data)
		req := httptest.NewRequest("POST", "/internal/import", bytes.NewBuffer(body))
		w := httptest.NewRecorder()

		server.HandleImport(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("Expected 200 OK, got %d", w.Code)
		}

		if c.Get("importKey") != "importVal" {
			t.Errorf("Import failed, key not found")
		}
	})

	t.Run("HandleExport", func(t *testing.T) {
		found := false
		for i := 0; i < 10; i++ {
			url := fmt.Sprintf("/internal/export?shard=%d", i)
			req := httptest.NewRequest("GET", url, nil)
			w := httptest.NewRecorder()

			server.HandleExport(w, req)

			if w.Code != http.StatusOK {
				t.Errorf("Expected 200 OK for shard %d, got %d", i, w.Code)
				continue
			}

			var data map[string]string
			if err := json.NewDecoder(w.Body).Decode(&data); err != nil {
				t.Errorf("Failed to decode response for shard %d: %v", i, err)
				continue
			}

			if val, ok := data["importKey"]; ok {
				if val == "importVal" {
					found = true
					break
				}
			}
		}

		if !found {
			t.Errorf("Failed to find 'importKey' in any of the 10 shards export")
		}
	})
}
