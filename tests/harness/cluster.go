package harness

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dominikszczepaniak/distributed-cache/pkg/client"
	"github.com/dominikszczepaniak/distributed-cache/pkg/metadata"
)

// Options configures a Cluster.
type Options struct {
	Controllers       int           // size of the Raft group (default 3)
	DataNodes         int           // number of datanodes (default 0)
	ArtificialDelay   time.Duration // RAFT_ARTIFICIAL_DELAY for every controller
	LeaseDuration     time.Duration // datanode lease (default 3s)
	SnapshotThreshold int           // SNAPSHOT_THRESHOLD (default 1000, must be >= 10)
	// NoWait skips the readiness wait in NewCluster (the caller waits itself).
	NoWait bool
}

// CtrlStatus mirrors the controller's GET /status.
type CtrlStatus struct {
	NodeID     int    `json:"node_id"`
	Role       string `json:"role"`
	Term       int    `json:"term"`
	LeaderID   int    `json:"leader_id"`
	TotalNodes int    `json:"total_nodes"`
}

// DNStatus mirrors the datanode's GET /status.
type DNStatus struct {
	NodeID      string `json:"node_id"`
	Epoch       uint64 `json:"epoch"`
	LeaseActive bool   `json:"lease_active"`
}

type ctrlNode struct {
	*proc
	idx      int
	apiPort  int
	raftPort int
	stateDir string
}

type dnNode struct {
	*proc
	idx  int
	port int
}

type edge struct{ from, to int }

// Cluster is a running set of controller and datanode subprocesses plus the
// per-edge Raft proxies. Create it with NewCluster; it is torn down by
// t.Cleanup (or earlier via Close).
type Cluster struct {
	t    testing.TB
	opts Options
	dir  string

	ctrls   []*ctrlNode
	dns     []*dnNode
	proxies map[edge]*Proxy
	extra   []*Proxy // datanode proxies created by tests
	front   *front

	http      *http.Client
	closeOnce sync.Once
}

// NewCluster builds and starts a cluster and (unless Options.NoWait) waits until
// it is ready: a leader is elected, all datanodes are registered, shards have
// primaries/replicas, and every datanode has the controllers' topology epoch with
// an active lease. Readiness is polled, never slept.
func NewCluster(t testing.TB, opts Options) *Cluster {
	t.Helper()
	RequireHeavy(t)
	if opts.Controllers == 0 {
		opts.Controllers = 3
	}
	if opts.LeaseDuration == 0 {
		opts.LeaseDuration = 3 * time.Second
	}
	if opts.SnapshotThreshold == 0 {
		opts.SnapshotThreshold = 1000
	}

	c := &Cluster{
		t:       t,
		opts:    opts,
		dir:     t.TempDir(),
		proxies: make(map[edge]*Proxy),
		http:    &http.Client{Timeout: 2 * time.Second},
	}
	t.Cleanup(func() {
		if t.Failed() {
			c.DumpLogs()
		}
		c.Close()
	})

	n := opts.Controllers
	ports, err := freePorts(2*n + opts.DataNodes)
	if err != nil {
		t.Fatalf("harness: allocating ports: %v", err)
	}
	for i := 0; i < n; i++ {
		c.ctrls = append(c.ctrls, &ctrlNode{
			idx:      i,
			apiPort:  ports[2*i],
			raftPort: ports[2*i+1],
			stateDir: filepath.Join(c.dir, fmt.Sprintf("ctrl%d", i)),
		})
	}
	for i := 0; i < opts.DataNodes; i++ {
		c.dns = append(c.dns, &dnNode{idx: i, port: ports[2*n+i]})
	}

	// One proxy per directed Raft edge: controller i dials controller j via it.
	for i := 0; i < n; i++ {
		for j := 0; j < n; j++ {
			if i == j {
				continue
			}
			p, err := newProxy(fmt.Sprintf("raft %d->%d", i, j), fmt.Sprintf("127.0.0.1:%d", c.ctrls[j].raftPort))
			if err != nil {
				t.Fatalf("harness: proxy %d->%d: %v", i, j, err)
			}
			c.proxies[edge{i, j}] = p
		}
	}

	for _, cn := range c.ctrls {
		if err := os.MkdirAll(cn.stateDir, 0o755); err != nil {
			t.Fatalf("harness: %v", err)
		}
		cn.proc = newProc(fmt.Sprintf("controller-%d", cn.idx), controllerBin, c.controllerEnv(cn))
		if err := cn.start(); err != nil {
			t.Fatalf("harness: start %s: %v", cn.name, err)
		}
	}

	c.front, err = newFront(c.ControllerURLs())
	if err != nil {
		t.Fatalf("harness: front: %v", err)
	}

	for _, dn := range c.dns {
		dn.proc = newProc(fmt.Sprintf("datanode-%d", dn.idx), datanodeBin, c.datanodeEnv(dn))
		if err := dn.start(); err != nil {
			t.Fatalf("harness: start %s: %v", dn.name, err)
		}
		if !opts.NoWait {
			// Register datanodes one at a time and let the controllers settle in
			// between: the controller currently loses a RegisterNode that is
			// applied between the leader's Rebalance snapshot and the
			// UpdateTopology that carries it (lost update in pkg/controller), and
			// datanodes never re-register after a 200.
			id := c.DataNodeID(dn.idx)
			c.WaitFor(id+" registered", 60*time.Second, func() bool {
				l, ok := c.leaderAmong(c.runningControllers())
				if !ok {
					return false
				}
				cfg, err := c.TopologyOf(l)
				if err != nil {
					return false
				}
				_, reg := cfg.Nodes[id]
				return reg
			})
			c.WaitControllersConverged()
		}
	}

	if !opts.NoWait {
		c.WaitReady()
	}
	return c
}

func (c *Cluster) controllerEnv(cn *ctrlNode) []string {
	n := c.opts.Controllers
	addrs := make([]string, n)
	for j := 0; j < n; j++ {
		if j == cn.idx {
			addrs[j] = fmt.Sprintf("127.0.0.1:%d", cn.raftPort)
		} else {
			addrs[j] = c.proxies[edge{cn.idx, j}].Addr()
		}
	}
	return []string{
		fmt.Sprintf("RAFT_ID=%d", cn.idx),
		fmt.Sprintf("TOTAL_NODES=%d", n),
		"RAFT_ADDRS=" + strings.Join(addrs, ","),
		"FILENAME=" + filepath.Join(cn.stateDir, "state"),
		fmt.Sprintf("SNAPSHOT_THRESHOLD=%d", c.opts.SnapshotThreshold),
		fmt.Sprintf("API_ADDR=127.0.0.1:%d", cn.apiPort),
		fmt.Sprintf("RAFT_ARTIFICIAL_DELAY=%s", c.opts.ArtificialDelay),
		"RAFT_INITIAL_BACKOFF=100ms",
		"RAFT_MAX_BACKOFF=1s",
		"RAFT_BACKOFF_MULTIPLIER=2.0",
		"RAFT_MAX_RETRIES=-1",
		"RAFT_CONN_TIMEOUT=1s",
		"RAFT_HEALTH_CHECK_INTERVAL=500ms",
	}
}

func (c *Cluster) datanodeEnv(dn *dnNode) []string {
	return []string{
		fmt.Sprintf("NODE_ID=127.0.0.1:%d", dn.port),
		"CONTROLLER_URL=" + c.front.URL(),
		fmt.Sprintf("LEASE_DURATION=%s", c.opts.LeaseDuration),
	}
}

// ---------------------------------------------------------------- addressing

func (c *Cluster) NumControllers() int { return len(c.ctrls) }
func (c *Cluster) NumDataNodes() int   { return len(c.dns) }

func (c *Cluster) ControllerURL(i int) string {
	return fmt.Sprintf("http://127.0.0.1:%d", c.ctrls[i].apiPort)
}

func (c *Cluster) ControllerURLs() []string {
	urls := make([]string, len(c.ctrls))
	for i := range c.ctrls {
		urls[i] = c.ControllerURL(i)
	}
	return urls
}

func (c *Cluster) DataNodeURL(i int) string {
	return fmt.Sprintf("http://127.0.0.1:%d", c.dns[i].port)
}

// DataNodeID is the node ID/address the datanode registers with.
func (c *Cluster) DataNodeID(i int) string {
	return fmt.Sprintf("127.0.0.1:%d", c.dns[i].port)
}

// ControllerStateFile is the Raft metadata file of controller i.
func (c *Cluster) ControllerStateFile(i int) string {
	return filepath.Join(c.ctrls[i].stateDir, "state.meta")
}

// Client returns a SmartClient that knows every controller.
func (c *Cluster) Client() *client.SmartClient {
	c.t.Helper()
	cl, err := client.NewSmartClient(c.ControllerURLs())
	if err != nil {
		c.t.Fatalf("harness: new smart client: %v", err)
	}
	return cl
}

// Pids returns the pids of all currently running processes.
func (c *Cluster) Pids() []int {
	var pids []int
	for _, cn := range c.ctrls {
		if cn.proc != nil && cn.running() {
			pids = append(pids, cn.cmd.Process.Pid)
		}
	}
	for _, dn := range c.dns {
		if dn.proc != nil && dn.running() {
			pids = append(pids, dn.cmd.Process.Pid)
		}
	}
	return pids
}

// ---------------------------------------------------------------- lifecycle

func (c *Cluster) KillController(i int)         { c.ctrls[i].kill() }
func (c *Cluster) ControllerRunning(i int) bool { return c.ctrls[i].running() }
func (c *Cluster) KillDataNode(i int)           { c.dns[i].kill() }
func (c *Cluster) DataNodeRunning(i int) bool   { return c.dns[i].running() }

// RestartController restarts a killed controller with the same ports and state
// directory, and waits until its HTTP API answers.
func (c *Cluster) RestartController(i int) {
	c.t.Helper()
	cn := c.ctrls[i]
	if err := cn.start(); err != nil {
		c.t.Fatalf("harness: restart %s: %v", cn.name, err)
	}
	c.WaitFor(fmt.Sprintf("controller %d /health", i), 30*time.Second, func() bool {
		return c.healthy(c.ControllerURL(i))
	})
}

// RestartDataNode restarts a killed datanode (same port; its in-memory cache is
// empty again) and waits until /health answers.
func (c *Cluster) RestartDataNode(i int) {
	c.t.Helper()
	dn := c.dns[i]
	if err := dn.start(); err != nil {
		c.t.Fatalf("harness: restart %s: %v", dn.name, err)
	}
	c.WaitFor(fmt.Sprintf("datanode %d /health", i), 30*time.Second, func() bool {
		return c.healthy(c.DataNodeURL(i))
	})
}

// Close kills every process (by process group) and closes proxies. Idempotent.
func (c *Cluster) Close() {
	c.closeOnce.Do(func() {
		for _, dn := range c.dns {
			if dn.proc != nil {
				dn.kill()
			}
		}
		for _, cn := range c.ctrls {
			if cn.proc != nil {
				cn.kill()
			}
		}
		for _, p := range c.proxies {
			p.Close()
		}
		for _, p := range c.extra {
			p.Close()
		}
		if c.front != nil {
			c.front.Close()
		}
	})
}

// ---------------------------------------------------------------- logs

func (c *Cluster) allProcs() []*proc {
	var ps []*proc
	for _, cn := range c.ctrls {
		if cn.proc != nil {
			ps = append(ps, cn.proc)
		}
	}
	for _, dn := range c.dns {
		if dn.proc != nil {
			ps = append(ps, dn.proc)
		}
	}
	return ps
}

// Logs returns the captured output of a process, e.g. "controller-1".
func (c *Cluster) Logs(name string) string {
	for _, p := range c.allProcs() {
		if p.name == name {
			return p.logs.String()
		}
	}
	return ""
}

// DumpLogs writes the tail of every process log to t.Log.
func (c *Cluster) DumpLogs() {
	for _, p := range c.allProcs() {
		c.t.Logf("---- %s (last 150 lines) ----\n%s", p.name, p.logs.tail(150))
	}
}

// ---------------------------------------------------------------- waiting

// WaitFor polls cond every 50ms until it returns true or the timeout elapses
// (then the test fails). It also fails fast if a process died on its own.
func (c *Cluster) WaitFor(desc string, timeout time.Duration, cond func() bool) {
	c.t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if cond() {
			return
		}
		for _, p := range c.allProcs() {
			if p.exitedUnexpectedly() {
				c.t.Fatalf("harness: %s exited unexpectedly while waiting for %s", p.name, desc)
			}
		}
		if time.Now().After(deadline) {
			c.t.Fatalf("harness: timed out after %v waiting for %s\n%s", timeout, desc, c.describe())
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func (c *Cluster) describe() string {
	var b strings.Builder
	for i := range c.ctrls {
		if !c.ctrls[i].running() {
			fmt.Fprintf(&b, "  controller %d: not running\n", i)
			continue
		}
		st, err := c.ControllerStatus(i)
		fmt.Fprintf(&b, "  controller %d: %+v err=%v\n", i, st, err)
	}
	return b.String()
}

func (c *Cluster) runningControllers() []int {
	var r []int
	for i, cn := range c.ctrls {
		if cn.running() {
			r = append(r, i)
		}
	}
	return r
}

// leaderAmong returns the leader as seen by the controllers in set: the node
// with role "leader" and the highest term, provided a majority of the whole
// group responds and every responder in set agrees on it.
func (c *Cluster) leaderAmong(set []int) (int, bool) {
	leader, leaderTerm := -1, -1
	statuses := map[int]CtrlStatus{}
	for _, i := range set {
		st, err := c.ControllerStatus(i)
		if err != nil {
			continue
		}
		statuses[i] = st
		if st.Role == "leader" && st.Term > leaderTerm {
			leader, leaderTerm = i, st.Term
		}
	}
	if leader < 0 || len(statuses) < len(c.ctrls)/2+1 {
		return -1, false
	}
	for i, st := range statuses {
		if i != leader && (st.LeaderID != leader || st.Term != leaderTerm) {
			return -1, false
		}
	}
	return leader, true
}

// WaitForLeader waits until all running controllers agree on a single leader
// and returns its index.
func (c *Cluster) WaitForLeader() int {
	c.t.Helper()
	return c.WaitForLeaderAmong(c.runningControllers())
}

// WaitForLeaderAmong waits for the controllers in set to agree on a leader
// (used while the rest of the group is partitioned away).
func (c *Cluster) WaitForLeaderAmong(set []int) int {
	c.t.Helper()
	leader := -1
	c.WaitFor(fmt.Sprintf("leader among %v", set), 30*time.Second, func() bool {
		l, ok := c.leaderAmong(set)
		leader = l
		return ok
	})
	return leader
}

// WaitControllersConverged waits until all running controllers agree on one
// leader/term and hold identical topologies (same epoch, nodes and shards).
func (c *Cluster) WaitControllersConverged() {
	c.t.Helper()
	// Require the converged state to hold across two polls 250ms apart so a
	// pending Rebalance broadcast is not mistaken for quiescence.
	var firstOK time.Time
	c.WaitFor("controllers converged", 45*time.Second, func() bool {
		if !c.convergedNow() {
			firstOK = time.Time{}
			return false
		}
		if firstOK.IsZero() {
			firstOK = time.Now()
		}
		return time.Since(firstOK) >= 250*time.Millisecond
	})
}

func (c *Cluster) convergedNow() bool {
	{
		set := c.runningControllers()
		if _, ok := c.leaderAmong(set); !ok {
			return false
		}
		var ref string
		for k, i := range set {
			cfg, err := c.TopologyOf(i)
			if err != nil {
				return false
			}
			fp := fingerprint(cfg)
			if k == 0 {
				ref = fp
			} else if fp != ref {
				return false
			}
		}
		return true
	}
}

// fingerprint ignores per-replica fields (LastHeartbeat is stamped locally on apply).
func fingerprint(cfg *metadata.ClusterConfig) string {
	nodes := map[string]string{}
	for id, n := range cfg.Nodes {
		nodes[id] = n.Address + "/" + string(n.Status)
	}
	shards, _ := json.Marshal(cfg.Shards)
	nb, _ := json.Marshal(nodes)
	return fmt.Sprintf("%d|%d|%s|%s", cfg.Epoch, cfg.TotalShards, nb, shards)
}

// WaitReady waits for the full cluster to be usable (see NewCluster).
func (c *Cluster) WaitReady() {
	c.t.Helper()
	c.WaitForLeader()
	if len(c.dns) == 0 {
		return
	}
	wantReplicas := len(c.dns) - 1
	if wantReplicas > 3 {
		wantReplicas = 3
	}
	c.WaitFor("datanodes registered, shards assigned and datanodes in sync", 60*time.Second, func() bool {
		set := c.runningControllers()
		leader, ok := c.leaderAmong(set)
		if !ok {
			return false
		}
		cfg, err := c.TopologyOf(leader)
		if err != nil || len(cfg.Nodes) != len(c.dns) || cfg.TotalShards == 0 {
			return false
		}
		for _, s := range cfg.Shards {
			if s.PrimaryID == "" || len(s.ReplicaIDs) != wantReplicas || s.Status != metadata.ShardStatusActive {
				return false
			}
		}
		for _, i := range set {
			other, err := c.TopologyOf(i)
			if err != nil || other.Epoch != cfg.Epoch {
				return false
			}
		}
		for i := range c.dns {
			st, err := c.DataNodeStatus(i)
			if err != nil || st.Epoch != cfg.Epoch || !st.LeaseActive {
				return false
			}
		}
		return true
	})
}

// WaitDataNodesSynced waits until every running datanode has the epoch of the
// leader's topology and an active lease.
func (c *Cluster) WaitDataNodesSynced() {
	c.t.Helper()
	c.WaitFor("datanodes synced to controller epoch", 30*time.Second, func() bool {
		leader, ok := c.leaderAmong(c.runningControllers())
		if !ok {
			return false
		}
		cfg, err := c.TopologyOf(leader)
		if err != nil {
			return false
		}
		for i := range c.dns {
			if !c.dns[i].running() {
				continue
			}
			st, err := c.DataNodeStatus(i)
			if err != nil || st.Epoch != cfg.Epoch || !st.LeaseActive {
				return false
			}
		}
		return true
	})
}

// ---------------------------------------------------------------- queries

func (c *Cluster) getJSON(url string, out interface{}) error {
	resp, err := c.http.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("GET %s: status %d", url, resp.StatusCode)
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

func (c *Cluster) healthy(base string) bool {
	resp, err := c.http.Get(base + "/health")
	if err != nil {
		return false
	}
	resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

func (c *Cluster) ControllerStatus(i int) (CtrlStatus, error) {
	var st CtrlStatus
	err := c.getJSON(c.ControllerURL(i)+"/status", &st)
	return st, err
}

func (c *Cluster) DataNodeStatus(i int) (DNStatus, error) {
	var st DNStatus
	err := c.getJSON(c.DataNodeURL(i)+"/status", &st)
	return st, err
}

// TopologyOf returns controller i's local view of the topology.
func (c *Cluster) TopologyOf(i int) (*metadata.ClusterConfig, error) {
	var cfg metadata.ClusterConfig
	if err := c.getJSON(c.ControllerURL(i)+"/topology", &cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}

// Topology returns the topology of the current leader.
func (c *Cluster) Topology() *metadata.ClusterConfig {
	c.t.Helper()
	cfg, err := c.TopologyOf(c.WaitForLeader())
	if err != nil {
		c.t.Fatalf("harness: topology: %v", err)
	}
	return cfg
}

// Register POSTs /cluster/register to controller i and returns the HTTP status.
func (c *Cluster) Register(i int, nodeID, addr string) (int, error) {
	body, _ := json.Marshal(map[string]string{"node_id": nodeID, "address": addr})
	hc := &http.Client{Timeout: 10 * time.Second} // BroadcastSync waits up to 5s
	resp, err := hc.Post(c.ControllerURL(i)+"/cluster/register", "application/json", bytes.NewReader(body))
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)
	return resp.StatusCode, nil
}

// NoOp POSTs /debug/raft_noop to controller i (a committed no-op entry) and
// returns the HTTP status.
func (c *Cluster) NoOp(i int) (int, error) {
	hc := &http.Client{Timeout: 10 * time.Second}
	resp, err := hc.Post(c.ControllerURL(i)+"/debug/raft_noop", "application/json", nil)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)
	return resp.StatusCode, nil
}

// ---------------------------------------------------------------- faults

func (c *Cluster) proxy(from, to int) *Proxy {
	p := c.proxies[edge{from, to}]
	if p == nil {
		c.t.Fatalf("harness: no raft edge %d->%d", from, to)
	}
	return p
}

// CutEdge cuts the directed Raft link from -> to (one direction only).
func (c *Cluster) CutEdge(from, to int) { c.proxy(from, to).SetCut(true) }

// Cut cuts the link between a and b in both directions.
func (c *Cluster) Cut(a, b int) { c.CutEdge(a, b); c.CutEdge(b, a) }

// Isolate cuts every Raft link of node i (its HTTP API stays reachable).
func (c *Cluster) Isolate(i int) {
	for j := range c.ctrls {
		if j != i {
			c.Cut(i, j)
		}
	}
}

// BlackholeEdge silently discards traffic from -> to without closing connections.
func (c *Cluster) BlackholeEdge(from, to int) { c.proxy(from, to).SetBlackhole(true) }

// Delay adds latency d to every forwarded chunk on all edges.
func (c *Cluster) Delay(d time.Duration) {
	for _, p := range c.proxies {
		p.SetDelay(d)
	}
}

// DelayEdge adds latency d on one directed edge.
func (c *Cluster) DelayEdge(from, to int, d time.Duration) { c.proxy(from, to).SetDelay(d) }

// Drop makes every forwarded chunk on all edges kill its connection with probability rate.
func (c *Cluster) Drop(rate float64) {
	for _, p := range c.proxies {
		p.SetDrop(rate)
	}
}

// Heal removes every injected fault on every edge.
func (c *Cluster) Heal() {
	for _, p := range c.proxies {
		p.Heal()
	}
}

// ---------------------------------------------------------------- misc

// freePorts reserves n distinct free TCP ports (all listeners are held open
// together so the same port cannot be returned twice).
func freePorts(n int) ([]int, error) {
	lns := make([]net.Listener, 0, n)
	ports := make([]int, 0, n)
	defer func() {
		for _, l := range lns {
			l.Close()
		}
	}()
	for i := 0; i < n; i++ {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			return nil, err
		}
		lns = append(lns, l)
		ports = append(ports, l.Addr().(*net.TCPAddr).Port)
	}
	return ports, nil
}

// front is an HTTP reverse proxy in front of the controllers that sends each
// request to the current controller, failing over to the next one on a
// connection error. Datanodes use
// it as CONTROLLER_URL because they only accept a single controller URL; it
// models a load balancer / VIP so that datanodes survive a controller crash.
type front struct {
	srv *http.Server
	ln  net.Listener
}

func newFront(backends []string) (*front, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	tr := &failoverTransport{
		backends: backends,
		rt: &http.Transport{
			DialContext:         (&net.Dialer{Timeout: 300 * time.Millisecond}).DialContext,
			MaxIdleConnsPerHost: 4,
		},
	}
	rp := &httputil.ReverseProxy{
		Director:  func(r *http.Request) { r.URL.Scheme = "http"; r.URL.Host = "front.invalid" },
		Transport: tr,
		ErrorLog:  log.New(io.Discard, "", 0),
	}
	f := &front{ln: ln, srv: &http.Server{Handler: rp}}
	go f.srv.Serve(ln)
	return f, nil
}

func (f *front) URL() string { return "http://" + f.ln.Addr().String() }
func (f *front) Close()      { f.srv.Close() }

// failoverTransport is sticky: it keeps using the last backend that worked and
// only moves on when it fails, so heartbeats do not flap back to a restarted
// controller (each controller's reaper tracks heartbeats locally).
type failoverTransport struct {
	backends []string
	rt       *http.Transport

	mu     sync.Mutex
	cursor int
}

func (t *failoverTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	var body []byte
	if req.Body != nil {
		body, _ = io.ReadAll(req.Body)
		req.Body.Close()
	}
	var lastErr error
	t.mu.Lock()
	start := t.cursor
	t.mu.Unlock()
	for k := 0; k < len(t.backends); k++ {
		idx := (start + k) % len(t.backends)
		u, _ := url.Parse(t.backends[idx])
		r2 := req.Clone(req.Context())
		r2.URL.Scheme, r2.URL.Host = u.Scheme, u.Host
		r2.Host = u.Host
		if body != nil {
			r2.Body = io.NopCloser(bytes.NewReader(body))
			r2.ContentLength = int64(len(body))
		}
		resp, err := t.rt.RoundTrip(r2)
		if err == nil {
			t.mu.Lock()
			t.cursor = idx
			t.mu.Unlock()
			return resp, nil
		}
		lastErr = err
	}
	return nil, lastErr
}

// ProxyDataNode returns a TCP proxy in front of datanode i for client-side fault
// injection (cut/delay/blackhole). It is closed with the cluster.
func (c *Cluster) ProxyDataNode(i int) *Proxy {
	c.t.Helper()
	p, err := newProxy(fmt.Sprintf("datanode %d", i), fmt.Sprintf("127.0.0.1:%d", c.dns[i].port))
	if err != nil {
		c.t.Fatalf("harness: datanode proxy: %v", err)
	}
	c.extra = append(c.extra, p)
	return p
}

// ClientVia returns a SmartClient whose view of the topology has the addresses
// of the given datanodes (index -> proxy) replaced by their proxies, so all
// client traffic to those datanodes flows through the proxy. The topology is
// the leader's real one, served by a private in-test server.
func (c *Cluster) ClientVia(proxies map[int]*Proxy) *client.SmartClient {
	c.t.Helper()
	cfg := c.Topology()
	nodes := make(map[string]metadata.NodeMetadata, len(cfg.Nodes))
	for id, n := range cfg.Nodes {
		nodes[id] = n
	}
	for i, p := range proxies {
		id := c.DataNodeID(i)
		n := nodes[id]
		n.Address = p.Addr()
		nodes[id] = n
	}
	view := *cfg
	view.Nodes = nodes
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(&view)
	}))
	c.t.Cleanup(srv.Close)
	cl, err := client.NewSmartClient([]string{srv.URL})
	if err != nil {
		c.t.Fatalf("harness: client via proxies: %v", err)
	}
	return cl
}
