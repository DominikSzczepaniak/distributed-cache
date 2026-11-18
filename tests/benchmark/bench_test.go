package benchmark

import (
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/dominikszczepaniak/distributed-cache/pkg/client"
	"github.com/dominikszczepaniak/distributed-cache/pkg/metadata"
	"github.com/dominikszczepaniak/distributed-cache/tests/harness"
)

func TestMain(m *testing.M) { harness.RunMain(m) }

var value64 = strings.Repeat("v", 64)

// clients creates n SmartClients (one per worker, like independent callers).
func clients(tb testing.TB, c *harness.Cluster, n int) []*client.SmartClient {
	tb.Helper()
	cls := make([]*client.SmartClient, n)
	for i := range cls {
		cls[i] = c.Client()
	}
	return cls
}

func putOp(cls []*client.SmartClient, prefix string) func(w, i int) error {
	return func(w, i int) error {
		return cls[w].Put(fmt.Sprintf("%s-%d", prefix, i), value64)
	}
}

// preload writes n keys and returns a GET op that cycles through them.
func preload(tb testing.TB, cls []*client.SmartClient, n int) func(w, i int) error {
	tb.Helper()
	for i := 0; i < n; i++ {
		if err := cls[0].Put(fmt.Sprintf("pre-%d", i), value64); err != nil {
			tb.Fatalf("preload: %v", err)
		}
	}
	return func(w, i int) error {
		_, err := cls[w].Get(fmt.Sprintf("pre-%d", i%n))
		return err
	}
}

// TestBench_ClusterSmoke is the quick variant of the benchmarks against a real
// cluster (heavy: skipped under -short; the -short smoke is TestLoadSmoke_Stub).
func TestBench_ClusterSmoke(t *testing.T) {
	c := harness.NewCluster(t, harness.Options{Controllers: 3, DataNodes: 2})
	cls := clients(t, c, 2)
	const ops = 40
	assertSane(t, runLoad(ops, 2, putOp(cls, "smoke")), ops)
	get := preload(t, cls, 10)
	assertSane(t, runLoad(ops, 2, get), ops)
}

// BenchmarkPutGet: PUT and GET latency/throughput on a 3-controller,
// 2-datanode (replicated) cluster with 8 clients.
func BenchmarkPutGet(b *testing.B) {
	c := harness.NewCluster(b, harness.Options{Controllers: 3, DataNodes: 2})
	const workers = 8
	cls := clients(b, c, workers)
	get := preload(b, cls, 1000)

	b.Run("PUT", func(b *testing.B) {
		b.ResetTimer()
		report(b, runLoad(b.N, workers, putOp(cls, "put")))
	})
	b.Run("GET", func(b *testing.B) {
		b.ResetTimer()
		report(b, runLoad(b.N, workers, get))
	})
	b.Run("MIXED_80R_20W", func(b *testing.B) {
		b.ResetTimer()
		report(b, runLoad(b.N, workers, func(w, i int) error {
			if i%5 == 0 {
				return putOp(cls, "mix")(w, i)
			}
			return get(w, i)
		}))
	})
}

// BenchmarkClientScaling: PUT throughput/latency as the number of concurrent
// clients grows.
func BenchmarkClientScaling(b *testing.B) {
	c := harness.NewCluster(b, harness.Options{Controllers: 3, DataNodes: 2})
	for _, n := range []int{1, 4, 16, 64} {
		cls := clients(b, c, n)
		b.Run(fmt.Sprintf("clients=%d", n), func(b *testing.B) {
			b.ResetTimer()
			report(b, runLoad(b.N, n, putOp(cls, fmt.Sprintf("scale%d", n))))
		})
	}
}

// BenchmarkReplication compares the three datanode layouts the controller can
// produce or be configured with:
//
//	1dn_no_replication   one datanode, nothing to replicate to
//	2dn_replicated       Rebalance output: one primary for all shards + 1 replica,
//	                     every PUT is synchronously replicated before the ack
//	2dn_sharded_no_repl  primaries spread over both datanodes, no replicas
//	                     (pushed through /debug/config, as the old script did)
func BenchmarkReplication(b *testing.B) {
	const workers = 16
	run := func(b *testing.B, name string, c *harness.Cluster) {
		cls := clients(b, c, workers)
		b.Run(name, func(b *testing.B) {
			b.ResetTimer()
			report(b, runLoad(b.N, workers, putOp(cls, "repl-"+name)))
		})
	}

	one := harness.NewCluster(b, harness.Options{Controllers: 3, DataNodes: 1})
	run(b, "1dn_no_replication", one)

	two := harness.NewCluster(b, harness.Options{Controllers: 3, DataNodes: 2})
	run(b, "2dn_replicated", two)

	sharded := harness.NewCluster(b, harness.Options{Controllers: 3, DataNodes: 2})
	cfg := sharded.Topology()
	cfg.Epoch += 1000
	ids := []string{sharded.DataNodeID(0), sharded.DataNodeID(1)}
	for id := range cfg.Shards {
		s := cfg.Shards[id]
		s.PrimaryID = ids[id%2]
		s.ReplicaIDs = []string{}
		s.Status = metadata.ShardStatusActive
		cfg.Shards[id] = s
	}
	sharded.SetTopology(cfg)
	run(b, "2dn_sharded_no_repl", sharded)
}

// BenchmarkRaftNoop measures committed Raft commands per second (and latency)
// through the leader with RAFT_ARTIFICIAL_DELAY swept over several values.
func BenchmarkRaftNoop(b *testing.B) {
	const workers = 10
	delays := []time.Duration{0, 5 * time.Millisecond, 10 * time.Millisecond, 25 * time.Millisecond, 50 * time.Millisecond}
	type env struct {
		c      *harness.Cluster
		leader int
	}
	envs := make([]env, len(delays))
	for i, d := range delays {
		c := harness.NewCluster(b, harness.Options{Controllers: 3, ArtificialDelay: d})
		envs[i] = env{c, c.WaitForLeader()}
	}
	for i, d := range delays {
		e := envs[i]
		b.Run(fmt.Sprintf("delay=%s", d), func(b *testing.B) {
			b.ResetTimer()
			report(b, runLoad(b.N, workers, func(w, _ int) error {
				st, err := e.c.NoOp(e.leader)
				if err != nil {
					return err
				}
				if st != http.StatusOK {
					return fmt.Errorf("noop status %d", st)
				}
				return nil
			}))
		})
	}
}
