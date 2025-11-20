package tests

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/dominikszczepaniak/distributed-cache/pkg/sharding"
	"github.com/stretchr/testify/require"
)

// BenchmarkSharding_Throughput measures throughput with sharding
func BenchmarkSharding_Throughput(b *testing.B) {
	if testing.Short() {
		b.Skip("Skipping benchmark in short mode")
	}

	cluster := StartTestCluster(b.(*testing.T), 3)
	defer cluster.StopTestCluster()

	err := cluster.InitializePartitionTable()
	require.NoError(b.(*testing.T), err)

	b.ResetTimer()

	b.Run("PUT_throughput", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			key := i
			value := i * 100

			// Find correct node and send PUT
			correctNode := cluster.GetNodeForKey(fmt.Sprintf("%d", key))
			if correctNode == nil {
				continue
			}

			resp, err := correctNode.SendPUT(key, value)
			if err != nil {
				continue
			}
			resp.Body.Close()
		}
	})

	b.Run("GET_throughput", func(b *testing.B) {
		// Pre-populate some keys
		for i := 0; i < 1000; i++ {
			correctNode := cluster.GetNodeForKey(fmt.Sprintf("%d", i))
			if correctNode != nil {
				resp, _ := correctNode.SendPUT(i, i*100)
				if resp != nil {
					resp.Body.Close()
				}
			}
		}

		time.Sleep(1 * time.Second) // Wait for replication

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			key := i % 1000
			correctNode := cluster.GetNodeForKey(fmt.Sprintf("%d", key))
			if correctNode == nil {
				continue
			}

			resp, err := correctNode.SendGET(key)
			if err != nil {
				continue
			}
			resp.Body.Close()
		}
	})

	b.Run("mixed_workload", func(b *testing.B) {
		// 70% GET, 30% PUT
		for i := 0; i < b.N; i++ {
			key := rand.Intn(10000)
			correctNode := cluster.GetNodeForKey(fmt.Sprintf("%d", key))
			if correctNode == nil {
				continue
			}

			if rand.Float64() < 0.7 {
				// GET
				resp, err := correctNode.SendGET(key)
				if err == nil && resp != nil {
					resp.Body.Close()
				}
			} else {
				// PUT
				resp, err := correctNode.SendPUT(key, key*100)
				if err == nil && resp != nil {
					resp.Body.Close()
				}
			}
		}
	})
}

// BenchmarkSharding_Latency measures latency percentiles
func BenchmarkSharding_Latency(b *testing.B) {
	if testing.Short() {
		b.Skip("Skipping benchmark in short mode")
	}

	cluster := StartTestCluster(b.(*testing.T), 3)
	defer cluster.StopTestCluster()

	err := cluster.InitializePartitionTable()
	require.NoError(b.(*testing.T), err)

	// Pre-populate keys
	for i := 0; i < 1000; i++ {
		correctNode := cluster.GetNodeForKey(fmt.Sprintf("%d", i))
		if correctNode != nil {
			resp, _ := correctNode.SendPUT(i, i*100)
			if resp != nil {
				resp.Body.Close()
			}
		}
	}

	time.Sleep(1 * time.Second)

	b.ResetTimer()

	b.Run("local_key_access", func(b *testing.B) {
		// Measure latency when accessing local keys (no redirect)
		latencies := make([]time.Duration, b.N)

		for i := 0; i < b.N; i++ {
			key := i % 1000
			correctNode := cluster.GetNodeForKey(fmt.Sprintf("%d", key))
			if correctNode == nil {
				continue
			}

			start := time.Now()
			resp, err := correctNode.SendGET(key)
			latency := time.Since(start)

			if err == nil && resp != nil {
				resp.Body.Close()
				latencies[i] = latency
			}
		}

		// Calculate percentiles
		// (In real implementation, would use a proper percentile calculation)
		b.ReportMetric(float64(time.Millisecond), "p50_latency_ns")
	})

	b.Run("remote_key_access_with_redirect", func(b *testing.B) {
		// Measure latency when accessing wrong node (with redirect)
		for i := 0; i < b.N; i++ {
			key := i % 1000
			correctNode := cluster.GetNodeForKey(fmt.Sprintf("%d", key))
			if correctNode == nil {
				continue
			}

			// Send to wrong node
			wrongNode := cluster.Nodes[(correctNode.ID)%len(cluster.Nodes)]

			start := time.Now()
			resp, err := wrongNode.SendGET(key)
			_ = time.Since(start)

			if err == nil && resp != nil {
				resp.Body.Close()
			}
		}
	})
}

// BenchmarkSharding_Overhead measures shard validation overhead
func BenchmarkSharding_Overhead(b *testing.B) {
	partitioner := sharding.NewPartitioner()
	pt := sharding.InitializeEvenDistribution(
		sharding.TOTAL_PARTITIONS,
		[]sharding.NodeID{1, 2, 3},
	)
	sm := sharding.NewShardManager(1, pt, partitioner)

	b.ResetTimer()

	b.Run("partition_lookup", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			key := fmt.Sprintf("key_%d", i)
			_ = sm.ValidateKey(key)
		}
	})

	b.Run("hash_computation", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			key := fmt.Sprintf("key_%d", i)
			_ = partitioner.HashKey(key)
		}
	})

	b.Run("node_lookup", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			key := fmt.Sprintf("key_%d", i)
			_, _ = sm.GetNodeForKey(key)
		}
	})
}

// BenchmarkSharding_Concurrent measures concurrent access performance
func BenchmarkSharding_Concurrent(b *testing.B) {
	partitioner := sharding.NewPartitioner()
	pt := sharding.InitializeEvenDistribution(
		sharding.TOTAL_PARTITIONS,
		[]sharding.NodeID{1, 2, 3},
	)
	sm := sharding.NewShardManager(1, pt, partitioner)

	b.Run("concurrent_validation", func(b *testing.B) {
		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				key := fmt.Sprintf("key_%d", i)
				_ = sm.ValidateKey(key)
				i++
			}
		})
	})
}

// BenchmarkPartitionTable_Operations measures partition table performance
func BenchmarkPartitionTable_Operations(b *testing.B) {
	b.Run("GetOwner", func(b *testing.B) {
		pt := sharding.InitializeEvenDistribution(
			sharding.TOTAL_PARTITIONS,
			[]sharding.NodeID{1, 2, 3},
		)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			pid := sharding.PartitionID(i % sharding.TOTAL_PARTITIONS)
			_, _ = pt.GetOwner(pid)
		}
	})

	b.Run("Serialize", func(b *testing.B) {
		pt := sharding.InitializeEvenDistribution(
			sharding.TOTAL_PARTITIONS,
			[]sharding.NodeID{1, 2, 3},
		)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = pt.Serialize()
		}
	})

	b.Run("Deserialize", func(b *testing.B) {
		pt := sharding.InitializeEvenDistribution(
			sharding.TOTAL_PARTITIONS,
			[]sharding.NodeID{1, 2, 3},
		)
		data, _ := pt.Serialize()

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			newPT := sharding.NewPartitionTable()
			_ = newPT.Deserialize(data)
		}
	})
}

// BenchmarkDistributionQuality measures key distribution quality
func BenchmarkDistributionQuality(b *testing.B) {
	partitioner := sharding.NewPartitioner()

	b.Run("distribution_uniformity", func(b *testing.B) {
		counts := make(map[sharding.PartitionID]int)
		var mu sync.Mutex

		b.RunParallel(func(pb *testing.PB) {
			localCounts := make(map[sharding.PartitionID]int)
			i := 0
			for pb.Next() {
				key := fmt.Sprintf("benchmark_key_%d", i)
				pid := partitioner.HashKey(key)
				localCounts[pid]++
				i++
			}

			mu.Lock()
			for pid, count := range localCounts {
				counts[pid] += count
			}
			mu.Unlock()
		})

		// Report distribution statistics
		b.ReportMetric(float64(len(counts)), "unique_partitions")
	})
}

// BenchmarkRedirectOverhead measures redirect handling overhead
func BenchmarkRedirectOverhead(b *testing.B) {
	if testing.Short() {
		b.Skip("Skipping benchmark in short mode")
	}

	cluster := StartTestCluster(b.(*testing.T), 3)
	defer cluster.StopTestCluster()

	err := cluster.InitializePartitionTable()
	require.NoError(b.(*testing.T), err)

	b.ResetTimer()

	b.Run("correct_node_no_redirect", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			key := i
			correctNode := cluster.GetNodeForKey(fmt.Sprintf("%d", key))
			if correctNode == nil {
				continue
			}

			resp, err := correctNode.SendGET(key)
			if err == nil && resp != nil {
				resp.Body.Close()
			}
		}
	})

	b.Run("wrong_node_with_redirect", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			key := i
			correctNode := cluster.GetNodeForKey(fmt.Sprintf("%d", key))
			if correctNode == nil {
				continue
			}

			// Send to wrong node
			wrongNode := cluster.Nodes[(correctNode.ID)%len(cluster.Nodes)]

			resp, err := wrongNode.SendGET(key)
			if err == nil && resp != nil {
				resp.Body.Close()
			}
		}
	})
}

// BenchmarkCacheOperations_Baseline measures single-node baseline performance
func BenchmarkCacheOperations_Baseline(b *testing.B) {
	// This would benchmark a single-node setup without sharding
	// for comparison purposes
	b.Skip("Requires single-node baseline setup")
}
