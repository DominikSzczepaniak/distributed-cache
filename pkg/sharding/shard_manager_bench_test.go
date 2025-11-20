package sharding

import (
	"fmt"
	"testing"
)

// BenchmarkValidateKey_LocalHit measures performance of local key validation
func BenchmarkValidateKey_LocalHit(b *testing.B) {
	pt := NewPartitionTable()
	partitioner := NewPartitioner()
	nodeID := NodeID(1)

	// Assign first 1000 partitions to node 1
	for i := 0; i < 1000; i++ {
		pt.Assign(PartitionID(i), nodeID)
	}

	sm := NewShardManager(nodeID, pt, partitioner)

	// Find a key that maps to partition 0 (owned by node 1)
	testKey := "benchmark_key"
	for i := 0; i < 100000; i++ {
		testKey = fmt.Sprintf("key%d", i)
		if partitioner.HashKey(testKey) < 1000 {
			break
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = sm.ValidateKey(testKey)
	}
}

// BenchmarkValidateKey_Redirect measures performance of redirect case
func BenchmarkValidateKey_Redirect(b *testing.B) {
	pt := NewPartitionTable()
	partitioner := NewPartitioner()
	currentNode := NodeID(1)
	otherNode := NodeID(2)

	// Assign first 1000 partitions to node 2 (not current node)
	for i := 0; i < 1000; i++ {
		pt.Assign(PartitionID(i), otherNode)
	}

	sm := NewShardManager(currentNode, pt, partitioner)
	sm.UpdatePeerAddress(otherNode, "http://localhost:10001")

	// Find a key that maps to partition 0 (owned by node 2)
	testKey := "benchmark_key"
	for i := 0; i < 100000; i++ {
		testKey = fmt.Sprintf("key%d", i)
		if partitioner.HashKey(testKey) < 1000 {
			break
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = sm.ValidateKey(testKey)
	}
}

// BenchmarkValidateKey_Concurrent measures concurrent validation performance
func BenchmarkValidateKey_Concurrent(b *testing.B) {
	pt := NewPartitionTable()
	partitioner := NewPartitioner()
	nodeID := NodeID(1)

	// Assign partitions to 3 nodes
	for i := 0; i < TOTAL_PARTITIONS; i++ {
		pt.Assign(PartitionID(i), NodeID(i%3))
	}

	sm := NewShardManager(nodeID, pt, partitioner)
	sm.UpdatePeerAddress(NodeID(0), "http://localhost:10000")
	sm.UpdatePeerAddress(NodeID(1), "http://localhost:10001")
	sm.UpdatePeerAddress(NodeID(2), "http://localhost:10002")

	// Generate test keys
	testKeys := make([]string, 1000)
	for i := range testKeys {
		testKeys[i] = fmt.Sprintf("concurrent_key_%d", i)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			_ = sm.ValidateKey(testKeys[i%len(testKeys)])
			i++
		}
	})
}

// BenchmarkGetNodeForKey measures GetNodeForKey performance
func BenchmarkGetNodeForKey(b *testing.B) {
	pt := NewPartitionTable()
	partitioner := NewPartitioner()
	nodeID := NodeID(1)

	// Assign all partitions
	for i := 0; i < TOTAL_PARTITIONS; i++ {
		pt.Assign(PartitionID(i), NodeID(i%10))
	}

	sm := NewShardManager(nodeID, pt, partitioner)
	testKey := "benchmark_key_123"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = sm.GetNodeForKey(testKey)
	}
}

// BenchmarkIsLocalKey measures IsLocalKey boolean check performance
func BenchmarkIsLocalKey(b *testing.B) {
	pt := NewPartitionTable()
	partitioner := NewPartitioner()
	nodeID := NodeID(1)

	// Assign first half to node 1
	for i := 0; i < TOTAL_PARTITIONS/2; i++ {
		pt.Assign(PartitionID(i), nodeID)
	}

	sm := NewShardManager(nodeID, pt, partitioner)
	testKey := "benchmark_key_123"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = sm.IsLocalKey(testKey)
	}
}

// BenchmarkUpdatePeerAddress measures peer address update performance
func BenchmarkUpdatePeerAddress(b *testing.B) {
	pt := NewPartitionTable()
	partitioner := NewPartitioner()
	nodeID := NodeID(1)

	sm := NewShardManager(nodeID, pt, partitioner)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		peerID := NodeID(i % 100)
		addr := fmt.Sprintf("http://localhost:%d", 10000+i%100)
		sm.UpdatePeerAddress(peerID, addr)
	}
}

// BenchmarkGetMetrics measures metrics retrieval performance
func BenchmarkGetMetrics(b *testing.B) {
	pt := NewPartitionTable()
	partitioner := NewPartitioner()
	nodeID := NodeID(1)

	sm := NewShardManager(nodeID, pt, partitioner)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = sm.GetMetrics()
	}
}

// BenchmarkPartitionLookup measures just the partition table lookup cost
func BenchmarkPartitionLookup(b *testing.B) {
	pt := NewPartitionTable()
	partitioner := NewPartitioner()

	// Assign all partitions
	for i := 0; i < TOTAL_PARTITIONS; i++ {
		pt.Assign(PartitionID(i), NodeID(i%10))
	}

	testKey := "benchmark_key_123"
	partitionID := partitioner.HashKey(testKey)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = pt.GetOwner(partitionID)
	}
}

// BenchmarkEndToEnd measures complete request validation flow
func BenchmarkEndToEnd(b *testing.B) {
	pt := NewPartitionTable()
	partitioner := NewPartitioner()
	nodeID := NodeID(1)

	// Realistic 3-node setup
	pt = InitializeEvenDistribution(TOTAL_PARTITIONS, []NodeID{0, 1, 2})

	sm := NewShardManager(nodeID, pt, partitioner)
	sm.UpdatePeerAddress(NodeID(0), "http://localhost:10000")
	sm.UpdatePeerAddress(NodeID(1), "http://localhost:10001")
	sm.UpdatePeerAddress(NodeID(2), "http://localhost:10002")

	// Generate realistic keys
	testKeys := make([]string, 1000)
	for i := range testKeys {
		testKeys[i] = fmt.Sprintf("user:%d:session", i)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := testKeys[i%len(testKeys)]
		_ = sm.ValidateKey(key)
	}
}

// BenchmarkMemoryAllocation measures memory allocation per operation
func BenchmarkMemoryAllocation(b *testing.B) {
	pt := NewPartitionTable()
	partitioner := NewPartitioner()
	nodeID := NodeID(1)

	pt.Assign(0, NodeID(2))
	sm := NewShardManager(nodeID, pt, partitioner)
	sm.UpdatePeerAddress(NodeID(2), "http://localhost:10001")

	// Find a key that requires redirect
	testKey := "alloc_test"
	for i := 0; i < 100000; i++ {
		testKey = fmt.Sprintf("key%d", i)
		if partitioner.HashKey(testKey) == 0 {
			break
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = sm.ValidateKey(testKey)
	}
}
