// Package e2e holds end-to-end tests that run real controller and datanode
// processes via tests/harness. They replace tests/e2e/basic_e2e_test.sh and the
// data-path half of the old strong-consistency script.
//
// Heavy tests skip under -short; run them with `go test -race ./tests/...`.
package e2e

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dominikszczepaniak/distributed-cache/pkg/metadata"
	"github.com/dominikszczepaniak/distributed-cache/tests/harness"
)

func TestMain(m *testing.M) { harness.RunMain(m) }

func newDataCluster(t *testing.T) *harness.Cluster {
	t.Helper()
	return harness.NewCluster(t, harness.Options{Controllers: 3, DataNodes: 2})
}

func TestE2E_PutGetDelete(t *testing.T) {
	c := newDataCluster(t)
	cl := c.Client()

	if err := cl.Put("e2e-test-key", "e2e-test-value"); err != nil {
		t.Fatalf("put: %v", err)
	}
	got, err := cl.Get("e2e-test-key")
	if err != nil || got != "e2e-test-value" {
		t.Fatalf("get = %q, %v; want e2e-test-value", got, err)
	}
	if err := cl.Delete("e2e-test-key"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if got, err := cl.Get("e2e-test-key"); err == nil || !strings.Contains(err.Error(), "key not found") {
		t.Fatalf("get after delete = %q, %v; want 'key not found' error", got, err)
	}
}

func TestE2E_GetMissingKey(t *testing.T) {
	c := newDataCluster(t)
	cl := c.Client()
	got, err := cl.Get("never-written")
	if err == nil || !strings.Contains(err.Error(), "key not found") {
		t.Fatalf("get missing = %q, %v; want 'key not found' error", got, err)
	}
}

func TestE2E_OverwriteKey(t *testing.T) {
	c := newDataCluster(t)
	cl := c.Client()
	for i := 1; i <= 5; i++ {
		v := fmt.Sprintf("value-%d", i)
		if err := cl.Put("sequence-key", v); err != nil {
			t.Fatalf("put %s: %v", v, err)
		}
		if got, err := cl.Get("sequence-key"); err != nil || got != v {
			t.Fatalf("read-after-write: got %q, %v; want %q", got, err, v)
		}
	}
}

// Many keys written sequentially must each be readable immediately afterwards
// and still be readable (with the right value) once all writes are done.
func TestE2E_MultiKeyReadAfterWrite(t *testing.T) {
	c := newDataCluster(t)
	cl := c.Client()
	const n = 50
	for i := 0; i < n; i++ {
		k, v := fmt.Sprintf("multi-key-%d", i), fmt.Sprintf("multi-value-%d", i)
		if err := cl.Put(k, v); err != nil {
			t.Fatalf("put %s: %v", k, err)
		}
		if got, err := cl.Get(k); err != nil || got != v {
			t.Fatalf("read-after-write %s: got %q, %v", k, got, err)
		}
	}
	for i := 0; i < n; i++ {
		k, v := fmt.Sprintf("multi-key-%d", i), fmt.Sprintf("multi-value-%d", i)
		if got, err := cl.Get(k); err != nil || got != v {
			t.Fatalf("final read %s: got %q, %v", k, got, err)
		}
	}
}

func TestE2E_ConcurrentClients(t *testing.T) {
	c := newDataCluster(t)
	const workers, perWorker = 4, 25
	var wg sync.WaitGroup
	errs := make(chan error, workers*perWorker)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			cl := c.Client()
			for i := 0; i < perWorker; i++ {
				k, v := fmt.Sprintf("w%d-k%d", w, i), fmt.Sprintf("w%d-v%d", w, i)
				if err := cl.Put(k, v); err != nil {
					errs <- fmt.Errorf("put %s: %w", k, err)
					return
				}
				if got, err := cl.Get(k); err != nil || got != v {
					errs <- fmt.Errorf("get %s = %q, %v", k, got, err)
					return
				}
			}
		}(w)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

// Registering a node through a *follower* is forwarded to the leader, committed
// by a quorum, and becomes visible on every controller's /topology as JSON.
func TestE2E_RegisterNodeAppearsInTopology(t *testing.T) {
	c := harness.NewCluster(t, harness.Options{Controllers: 3})
	leader := c.WaitForLeader()
	follower := (leader + 1) % 3
	before := c.Topology().Epoch

	status, err := c.Register(follower, "node-x:9000", "10.9.9.9:9000")
	if err != nil || status != http.StatusOK {
		t.Fatalf("register via follower = %d, %v; want 200", status, err)
	}

	c.WaitControllersConverged()
	for i := 0; i < c.NumControllers(); i++ {
		// Decode as raw JSON so the wire format (field names, status string) is asserted.
		resp, err := http.Get(c.ControllerURL(i) + "/topology")
		if err != nil {
			t.Fatalf("controller %d topology: %v", i, err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		var raw struct {
			Epoch uint64                     `json:"epoch"`
			Nodes map[string]json.RawMessage `json:"nodes"`
		}
		if err := json.Unmarshal(body, &raw); err != nil {
			t.Fatalf("controller %d: bad JSON %s: %v", i, body, err)
		}
		nodeJSON, ok := raw.Nodes["node-x:9000"]
		if !ok {
			t.Fatalf("controller %d: node-x:9000 missing in %s", i, body)
		}
		var n struct {
			ID      string `json:"id"`
			Address string `json:"address"`
			Status  string `json:"status"`
		}
		if err := json.Unmarshal(nodeJSON, &n); err != nil {
			t.Fatal(err)
		}
		if n.ID != "node-x:9000" || n.Address != "10.9.9.9:9000" || n.Status != string(metadata.StatusActive) {
			t.Fatalf("controller %d: unexpected node %+v", i, n)
		}
		if raw.Epoch <= before {
			t.Fatalf("controller %d: epoch %d did not advance past %d", i, raw.Epoch, before)
		}
	}

	// Registering the same node again is idempotent: still OK, no duplicate.
	if status, err := c.Register(leader, "node-x:9000", "10.9.9.9:9000"); err != nil || status != http.StatusOK {
		t.Fatalf("re-register = %d, %v; want 200", status, err)
	}
	if got := len(c.Topology().Nodes); got != 1 {
		t.Fatalf("want 1 node after idempotent re-register, got %d", got)
	}

	// Invalid input is rejected.
	if status, _ := c.Register(leader, "", "x"); status != http.StatusBadRequest {
		t.Fatalf("register without node_id = %d; want 400", status)
	}
}

// Current implementation: the datanode cache is memory-only, so a restarted
// datanode comes back EMPTY (no persistence, no pull from its replica), keeps
// its identity and re-joins with an active lease, and accepts new writes. The
// surviving replica still holds the pre-crash data.
func TestE2E_DataNodeRestartBehavior(t *testing.T) {
	c := newDataCluster(t)
	cl := c.Client()

	if err := cl.Put("durable-key", "durable-value"); err != nil {
		t.Fatalf("put: %v", err)
	}
	route, err := cl.Route("durable-key")
	if err != nil {
		t.Fatal(err)
	}
	primary := c.DataNodeIndexFor(route.TargetURL)
	if primary < 0 {
		t.Fatalf("route %s matches no datanode", route.TargetURL)
	}
	replica := 1 - primary
	epochBefore := c.Topology().Epoch

	c.KillDataNode(primary)
	c.RestartDataNode(primary)
	c.WaitDataNodesSynced()

	// The restarted primary lost its in-memory data.
	st, _, err := harness.RawGet(c.DataNodeURL(primary), "durable-key")
	if err != nil {
		t.Fatal(err)
	}
	if st != http.StatusNotFound {
		t.Fatalf("restarted datanode returned status %d for pre-crash key; want 404 (memory-only cache)", st)
	}
	// The replica still has it.
	if st, body, err := harness.RawGet(c.DataNodeURL(replica), "durable-key"); err != nil || st != http.StatusOK || body != "durable-value" {
		t.Fatalf("replica read = %d %q %v; want 200 durable-value", st, body, err)
	}
	// Topology is unchanged by a quick restart (node was already registered).
	if got := c.Topology().Epoch; got != epochBefore {
		t.Fatalf("epoch changed across datanode restart: %d -> %d", epochBefore, got)
	}
	// The cluster accepts writes again through a fresh client.
	cl2 := c.Client()
	if err := cl2.Put("after-restart", "ok"); err != nil {
		t.Fatalf("put after restart: %v", err)
	}
	if got, err := cl2.Get("after-restart"); err != nil || got != "ok" {
		t.Fatalf("get after restart = %q, %v", got, err)
	}
}

// A write is acknowledged only after the primary replicated it synchronously,
// so a read directly on the replica must see it (and the delete) immediately.
func TestE2E_ReplicaReadAfterReplication(t *testing.T) {
	c := newDataCluster(t)
	cl := c.Client()

	for i := 0; i < 20; i++ {
		k, v := fmt.Sprintf("cross-node-%d", i), fmt.Sprintf("cross-value-%d", i)
		if err := cl.Put(k, v); err != nil {
			t.Fatalf("put: %v", err)
		}
		for d := 0; d < 2; d++ {
			st, body, err := harness.RawGet(c.DataNodeURL(d), k)
			if err != nil || st != http.StatusOK || body != v {
				t.Fatalf("datanode %d read %s = %d %q %v; want 200 %q", d, k, st, body, err, v)
			}
		}
	}

	if err := cl.Delete("cross-node-0"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	for d := 0; d < 2; d++ {
		if st, _, err := harness.RawGet(c.DataNodeURL(d), "cross-node-0"); err != nil || st != http.StatusNotFound {
			t.Fatalf("datanode %d after delete = %d, %v; want 404", d, st, err)
		}
	}
}

// Writes carrying an epoch older than the datanode's are rejected (fencing).
func TestE2E_StaleEpochWriteRejected(t *testing.T) {
	c := newDataCluster(t)
	epoch := c.Topology().Epoch
	if epoch == 0 {
		t.Fatal("expected non-zero epoch")
	}
	primaryURL := ""
	{
		cl := c.Client()
		r, err := cl.Route("stale-key")
		if err != nil {
			t.Fatal(err)
		}
		primaryURL = r.TargetURL
	}
	body := fmt.Sprintf(`{"key":"stale-key","value":"v","epoch":%d}`, epoch-1)
	hc := &http.Client{Timeout: 3 * time.Second}
	resp, err := hc.Post(primaryURL+"/data", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusPreconditionFailed {
		t.Fatalf("stale-epoch write status = %d; want 412", resp.StatusCode)
	}
	if st, _, _ := harness.RawGet(primaryURL, "stale-key"); st != http.StatusNotFound {
		t.Fatalf("rejected write must not be stored, read status %d", st)
	}
}
