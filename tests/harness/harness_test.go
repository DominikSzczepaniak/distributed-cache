package harness_test

import (
	"fmt"
	"net"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/dominikszczepaniak/distributed-cache/tests/harness"
)

func TestMain(m *testing.M) { harness.RunMain(m) }

func pidAlive(pid int) bool {
	return syscall.Kill(pid, 0) == nil
}

// psHas reports whether `ps` lists any of the given pids.
func psHas(t *testing.T, pids []int) bool {
	t.Helper()
	for _, pid := range pids {
		out, _ := exec.Command("ps", "-o", "pid=", "-p", fmt.Sprint(pid)).Output()
		if strings.TrimSpace(string(out)) != "" {
			return true
		}
	}
	return false
}

// TestHarness_StartStop starts a 3-controller + 2-datanode cluster, checks it is
// really ready (leader, registered nodes, shards assigned, datanodes in sync) and
// verifies that Close leaves no processes behind.
func TestHarness_StartStop(t *testing.T) {
	harness.RequireHeavy(t)

	c := harness.NewCluster(t, harness.Options{Controllers: 3, DataNodes: 2})
	leader := c.WaitForLeader()
	if leader < 0 || leader > 2 {
		t.Fatalf("bad leader index %d", leader)
	}

	topo := c.Topology()
	if len(topo.Nodes) != 2 {
		t.Fatalf("want 2 registered datanodes, got %d: %+v", len(topo.Nodes), topo.Nodes)
	}
	for id, s := range topo.Shards {
		if s.PrimaryID == "" {
			t.Fatalf("shard %d has no primary", id)
		}
	}

	cl := c.Client()
	if err := cl.Put("k", "v"); err != nil {
		t.Fatalf("put: %v", err)
	}
	if v, err := cl.Get("k"); err != nil || v != "v" {
		t.Fatalf("get = %q, %v", v, err)
	}

	pids := c.Pids()
	if len(pids) != 5 {
		t.Fatalf("want 5 pids, got %v", pids)
	}
	for _, p := range pids {
		if !pidAlive(p) {
			t.Fatalf("pid %d should be alive", p)
		}
	}

	c.Close()

	for _, p := range pids {
		if pidAlive(p) {
			t.Errorf("pid %d leaked after Close", p)
		}
	}
	if psHas(t, pids) {
		t.Errorf("ps still lists processes of the cluster after Close")
	}
}

// TestHarness_KillRestart verifies Kill/Restart keep the state dir and the same
// ports, and that a restarted controller rejoins the group.
func TestHarness_KillRestart(t *testing.T) {
	harness.RequireHeavy(t)

	c := harness.NewCluster(t, harness.Options{Controllers: 3, DataNodes: 1})
	leader := c.WaitForLeader()
	follower := (leader + 1) % 3

	addr := c.ControllerURL(follower)
	c.KillController(follower)
	if c.ControllerRunning(follower) {
		t.Fatal("controller still reported running after Kill")
	}
	if _, err := net.DialTimeout("tcp", strings.TrimPrefix(addr, "http://"), 300*time.Millisecond); err == nil {
		t.Fatal("API port still accepts connections after Kill")
	}

	c.RestartController(follower)
	if c.ControllerURL(follower) != addr {
		t.Fatalf("API address changed across restart")
	}
	c.WaitControllersConverged()
	if _, err := os.Stat(c.ControllerStateFile(follower)); err != nil {
		t.Fatalf("state file missing after restart: %v", err)
	}
}

// TestHarness_ProxyCutHeal verifies the per-edge proxy: cutting a node off lets
// the majority elect a new leader, and healing lets everything converge again.
func TestHarness_ProxyCutHeal(t *testing.T) {
	harness.RequireHeavy(t)

	c := harness.NewCluster(t, harness.Options{Controllers: 3, DataNodes: 1})
	old := c.WaitForLeader()

	c.Isolate(old)
	nl := c.WaitForLeaderAmong(others(old))
	if nl == old {
		t.Fatalf("new leader must differ from isolated one")
	}
	c.Heal()
	c.WaitControllersConverged()
}

func others(i int) []int {
	var r []int
	for j := 0; j < 3; j++ {
		if j != i {
			r = append(r, j)
		}
	}
	return r
}
