// Package fault_injection holds crash, partition and message-fault tests that
// run real controller/datanode processes behind the harness' per-edge Raft
// proxies. They replace the docker-based scripts in this directory.
//
// Heavy tests skip under -short; run them with `go test -race ./tests/...`.
package fault_injection

import (
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/dominikszczepaniak/distributed-cache/pkg/metadata"
	"github.com/dominikszczepaniak/distributed-cache/tests/harness"
)

func TestMain(m *testing.M) { harness.RunMain(m) }

func others(n, i int) []int {
	var r []int
	for j := 0; j < n; j++ {
		if j != i {
			r = append(r, j)
		}
	}
	return r
}

func hasNode(t *testing.T, c *harness.Cluster, ctrl int, id string) bool {
	t.Helper()
	cfg, err := c.TopologyOf(ctrl)
	if err != nil {
		t.Fatalf("topology of controller %d: %v", ctrl, err)
	}
	_, ok := cfg.Nodes[id]
	return ok
}

// waitNode waits (bounded) until controller ctrl has applied node id: a
// follower learns the commit index on the next heartbeat, so it may lag the
// acknowledgement by a few milliseconds.
func waitNode(t *testing.T, c *harness.Cluster, ctrl int, id string) {
	t.Helper()
	c.WaitFor(fmt.Sprintf("controller %d to apply %s", ctrl, id), 10*time.Second, func() bool {
		cfg, err := c.TopologyOf(ctrl)
		if err != nil {
			return false
		}
		_, ok := cfg.Nodes[id]
		return ok
	})
}

// Killing the Raft leader: the survivors elect a new one, the topology is
// intact on them, and the data plane keeps serving (data written before the
// crash is still readable, new writes succeed).
func TestFault_ControllerLeaderKill(t *testing.T) {
	c := harness.NewCluster(t, harness.Options{Controllers: 3, DataNodes: 2})
	cl := c.Client()
	if err := cl.Put("before-kill", "v1"); err != nil {
		t.Fatalf("put: %v", err)
	}
	before := c.Topology()

	old := c.WaitForLeader()
	c.KillController(old)
	survivors := others(3, old)
	newLeader := c.WaitForLeaderAmong(survivors)
	if newLeader == old {
		t.Fatalf("new leader equals killed leader %d", old)
	}

	for _, i := range survivors {
		cfg, err := c.TopologyOf(i)
		if err != nil {
			t.Fatal(err)
		}
		if cfg.Epoch != before.Epoch || len(cfg.Nodes) != len(before.Nodes) {
			t.Fatalf("controller %d topology changed: epoch %d->%d nodes %d->%d",
				i, before.Epoch, cfg.Epoch, len(before.Nodes), len(cfg.Nodes))
		}
		for id, s := range before.Shards {
			if cfg.Shards[id].PrimaryID != s.PrimaryID {
				t.Fatalf("controller %d: shard %d primary changed %q -> %q", i, id, s.PrimaryID, cfg.Shards[id].PrimaryID)
			}
		}
	}

	// A fresh client (killed controller is simply skipped) still serves.
	cl2 := c.Client()
	if got, err := cl2.Get("before-kill"); err != nil || got != "v1" {
		t.Fatalf("get after leader kill = %q, %v", got, err)
	}
	if err := cl2.Put("after-kill", "v2"); err != nil {
		t.Fatalf("put after leader kill: %v", err)
	}
	if got, err := cl2.Get("after-kill"); err != nil || got != "v2" {
		t.Fatalf("get after-kill = %q, %v", got, err)
	}

	// The control plane still commits with 2 of 3 controllers.
	if st, err := c.NoOp(newLeader); err != nil || st != http.StatusOK {
		t.Fatalf("noop on new leader = %d, %v", st, err)
	}

	// The old leader rejoins as follower and converges.
	c.RestartController(old)
	c.WaitControllersConverged()
	if st, err := c.ControllerStatus(old); err != nil || st.Role == "leader" {
		t.Fatalf("restarted old leader status = %+v, %v; want follower", st, err)
	}
}

// Killing the leader while a registration is pending on the surviving majority:
// registration works on the new leader and the restarted node catches up.
func TestFault_RegisterAfterLeaderKillAndCatchUp(t *testing.T) {
	c := harness.NewCluster(t, harness.Options{Controllers: 3})
	old := c.WaitForLeader()
	c.KillController(old)
	survivors := others(3, old)
	nl := c.WaitForLeaderAmong(survivors)

	if st, err := c.Register(nl, "catchup-node:9000", "10.1.1.1:9000"); err != nil || st != http.StatusOK {
		t.Fatalf("register on new leader = %d, %v", st, err)
	}
	c.RestartController(old)
	c.WaitControllersConverged()
	if !hasNode(t, c, old, "catchup-node:9000") {
		t.Fatal("restarted controller did not catch up with the registration it missed")
	}
}

// Partition the leader away: the majority elects a new leader and keeps
// accepting registrations; the minority (old) leader cannot commit anything;
// after healing the old leader converges and the uncommitted write is gone.
func TestFault_PartitionLeader(t *testing.T) {
	c := harness.NewCluster(t, harness.Options{Controllers: 3})
	old := c.WaitForLeader()
	majority := others(3, old)

	c.Isolate(old)

	nl := c.WaitForLeaderAmong(majority)
	if nl == old {
		t.Fatalf("majority re-elected isolated node %d", old)
	}

	// Majority side commits.
	if st, err := c.Register(nl, "majority-node:9000", "10.2.2.2:9000"); err != nil || st != http.StatusOK {
		t.Fatalf("register on majority leader = %d, %v; want 200", st, err)
	}
	for _, i := range majority {
		waitNode(t, c, i, "majority-node:9000")
	}

	// Minority leader cannot reach consensus: the write must NOT be acknowledged.
	st, err := c.Register(old, "minority-node:9000", "10.3.3.3:9000")
	if err == nil && st == http.StatusOK {
		t.Fatal("isolated minority leader acknowledged a register; it must not commit")
	}
	if hasNode(t, c, old, "majority-node:9000") {
		t.Fatal("isolated node saw the majority's write through a cut link")
	}
	if hasNode(t, c, old, "minority-node:9000") {
		t.Fatal("isolated node applied an uncommitted entry")
	}

	// Heal: old leader steps down, converges on the majority's state, and its
	// uncommitted entry is discarded everywhere.
	c.Heal()
	c.WaitControllersConverged()
	for i := 0; i < 3; i++ {
		if !hasNode(t, c, i, "majority-node:9000") {
			t.Errorf("controller %d missing majority-node after heal", i)
		}
		if hasNode(t, c, i, "minority-node:9000") {
			t.Errorf("controller %d contains the never-committed minority-node", i)
		}
	}
	if st, err := c.ControllerStatus(old); err != nil || st.Role == "leader" {
		t.Fatalf("old leader status after heal = %+v, %v; want it to have stepped down", st, err)
	}
}

// A single follower cut off must not disturb the leader or the other follower;
// it catches up after healing.
func TestFault_PartitionFollower(t *testing.T) {
	c := harness.NewCluster(t, harness.Options{Controllers: 3})
	leader := c.WaitForLeader()
	follower := (leader + 1) % 3
	c.Isolate(follower)

	if st, err := c.Register(leader, "while-cut:9000", "10.4.4.4:9000"); err != nil || st != http.StatusOK {
		t.Fatalf("register with one follower cut = %d, %v; want 200", st, err)
	}
	if hasNode(t, c, follower, "while-cut:9000") {
		t.Fatal("cut follower should not have the entry yet")
	}
	c.Heal()
	c.WaitControllersConverged()
	if !hasNode(t, c, follower, "while-cut:9000") {
		t.Fatal("follower did not catch up after heal")
	}
}

// One-way link loss (leader can send but not receive from a follower) must not
// lose committed data.
func TestFault_AsymmetricLinkCut(t *testing.T) {
	c := harness.NewCluster(t, harness.Options{Controllers: 3})
	leader := c.WaitForLeader()
	follower := (leader + 1) % 3
	c.CutEdge(follower, leader) // follower cannot reach leader; leader can still reach follower

	// Whatever the leadership does, the group must keep committing with 2 of 3
	// reachable paths; retry until a leader accepts.
	var ok bool
	deadline := time.Now().Add(30 * time.Second)
	for i := 0; i < 3 && !ok && time.Now().Before(deadline); i++ {
		for j := 0; j < 3; j++ {
			if st, err := c.Register(j, "asym-node:9000", "10.5.5.5:9000"); err == nil && st == http.StatusOK {
				ok = true
				break
			}
		}
	}
	if !ok {
		t.Fatal("no controller could commit a register with one asymmetric link cut")
	}
	c.Heal()
	c.WaitControllersConverged()
	for i := 0; i < 3; i++ {
		if !hasNode(t, c, i, "asym-node:9000") {
			t.Errorf("controller %d lost acknowledged registration", i)
		}
	}
}

// Delayed Raft messages (100ms per chunk on every edge) slow things down but
// every acknowledged registration is eventually on every controller.
func TestFault_DelayedMessagesConverge(t *testing.T) {
	c := harness.NewCluster(t, harness.Options{Controllers: 3})
	c.WaitForLeader()
	c.Delay(100 * time.Millisecond)

	const n = 5
	acked := registerAll(t, c, "delay", n, 40*time.Second)
	c.Delay(0)
	c.Heal()
	c.WaitControllersConverged()
	assertAllHave(t, c, acked)
	if len(acked) != n {
		t.Fatalf("only %d/%d registrations acknowledged under delay", len(acked), n)
	}
}

// Lost messages are modelled as broken connections (1% of chunks kill their
// TCP stream). Acknowledged writes must survive, and the group must converge
// once the network heals.
func TestFault_DroppedMessagesConverge(t *testing.T) {
	c := harness.NewCluster(t, harness.Options{Controllers: 3})
	c.WaitForLeader()
	c.Drop(0.01)

	acked := registerAll(t, c, "drop", 5, 60*time.Second)
	c.Heal()
	c.WaitControllersConverged()
	assertAllHave(t, c, acked)
	if len(acked) == 0 {
		t.Fatal("no registration could be committed under 1% connection loss")
	}
}

// registerAll registers n nodes, retrying each (on any controller) until it is
// acknowledged or the overall budget is exhausted. Returns acknowledged IDs.
func registerAll(t *testing.T, c *harness.Cluster, prefix string, n int, budget time.Duration) []string {
	t.Helper()
	deadline := time.Now().Add(budget)
	var acked []string
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("%s-%d:9000", prefix, i)
		for attempt := 0; time.Now().Before(deadline); attempt++ {
			st, err := c.Register(attempt%c.NumControllers(), id, fmt.Sprintf("10.6.6.%d:9000", i))
			if err == nil && st == http.StatusOK {
				acked = append(acked, id)
				// Let the leader's Rebalance broadcast land before the next
				// registration: back-to-back registrations hit a lost-update
				// race in pkg/controller (see report), which is not what these
				// tests are about.
				c.WaitControllersConverged()
				break
			}
		}
	}
	return acked
}

func assertAllHave(t *testing.T, c *harness.Cluster, ids []string) {
	t.Helper()
	for i := 0; i < c.NumControllers(); i++ {
		cfg, err := c.TopologyOf(i)
		if err != nil {
			t.Fatalf("controller %d topology: %v", i, err)
		}
		for _, id := range ids {
			if _, ok := cfg.Nodes[id]; !ok {
				t.Errorf("controller %d lost acknowledged registration %s", i, id)
			}
		}
	}
}

// ---- client behaviour against a slow / cut / hanging datanode ----

// A slow datanode link only adds latency: the write still succeeds, and takes
// at least the injected delay.
func TestFault_ClientSlowDataNode(t *testing.T) {
	c := harness.NewCluster(t, harness.Options{Controllers: 3, DataNodes: 2})
	cl0 := c.Client()
	r, err := cl0.Route("slow-key")
	if err != nil {
		t.Fatal(err)
	}
	primary := c.DataNodeIndexFor(r.TargetURL)
	px := c.ProxyDataNode(primary)
	cl := c.ClientVia(map[int]*harness.Proxy{primary: px})

	px.SetDelay(300 * time.Millisecond)
	start := time.Now()
	if err := cl.Put("slow-key", "v"); err != nil {
		t.Fatalf("put through slow link: %v", err)
	}
	if el := time.Since(start); el < 300*time.Millisecond {
		t.Fatalf("put took %v; expected at least the injected 300ms delay", el)
	}
	if got, err := cl.Get("slow-key"); err != nil || got != "v" {
		t.Fatalf("get through slow link = %q, %v", got, err)
	}
}

// With the datanode link cut, the client retries a bounded number of times and
// returns an error (no hang, no false success); after healing the same client
// works again.
func TestFault_ClientCutDataNode(t *testing.T) {
	c := harness.NewCluster(t, harness.Options{Controllers: 3, DataNodes: 2})
	cl0 := c.Client()
	r, err := cl0.Route("cut-key")
	if err != nil {
		t.Fatal(err)
	}
	primary := c.DataNodeIndexFor(r.TargetURL)
	px := c.ProxyDataNode(primary)
	cl := c.ClientVia(map[int]*harness.Proxy{primary: px})

	px.SetCut(true)
	start := time.Now()
	err = cl.Put("cut-key", "v")
	if err == nil {
		t.Fatal("put succeeded although the datanode link is cut")
	}
	if !strings.Contains(err.Error(), "max retries exceeded") {
		t.Fatalf("unexpected error: %v", err)
	}
	if el := time.Since(start); el > 20*time.Second {
		t.Fatalf("client took %v to give up; retries are not bounded", el)
	}
	if _, err := cl.Get("cut-key"); err == nil {
		t.Fatal("get succeeded although the datanode link is cut")
	}

	px.Heal()
	if err := cl.Put("cut-key", "v"); err != nil {
		t.Fatalf("put after heal: %v", err)
	}
	if got, err := cl.Get("cut-key"); err != nil || got != "v" {
		t.Fatalf("get after heal = %q, %v", got, err)
	}
}

// A hanging datanode (requests swallowed) trips the client's 5s HTTP timeout;
// the client then retries and succeeds once the link works again.
func TestFault_ClientTimeoutThenRetrySucceeds(t *testing.T) {
	c := harness.NewCluster(t, harness.Options{Controllers: 3, DataNodes: 2})
	cl0 := c.Client()
	r, err := cl0.Route("hang-key")
	if err != nil {
		t.Fatal(err)
	}
	primary := c.DataNodeIndexFor(r.TargetURL)
	px := c.ProxyDataNode(primary)
	cl := c.ClientVia(map[int]*harness.Proxy{primary: px})

	px.SetBlackhole(true)
	time.AfterFunc(time.Second, px.Heal)
	start := time.Now()
	if err := cl.Put("hang-key", "v"); err != nil {
		t.Fatalf("put should succeed on retry after the first attempt times out: %v", err)
	}
	el := time.Since(start)
	if el < 4500*time.Millisecond {
		t.Fatalf("put returned after %v; the first attempt should have hit the 5s client timeout", el)
	}
	if el > 20*time.Second {
		t.Fatalf("put took %v; retry did not recover promptly", el)
	}
	if got, err := cl.Get("hang-key"); err != nil || got != "v" {
		t.Fatalf("get = %q, %v", got, err)
	}
}

var _ = metadata.StatusActive
