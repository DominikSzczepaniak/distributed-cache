package sharding

import (
	"fmt"
	"math"
	"sync"
	"testing"
)

func TestNewPartitioner(t *testing.T) {
	p := NewPartitioner()

	if p == nil {
		t.Fatal("NewPartitioner returned nil")
	}

	if p.totalPartitions != TOTAL_PARTITIONS {
		t.Errorf("Expected %d partitions, got %d", TOTAL_PARTITIONS, p.totalPartitions)
	}

	if p.hashFunc == nil {
		t.Error("Hash function is nil")
	}
}

func TestHashKeyDeterminism(t *testing.T) {
	p := NewPartitioner()

	testKeys := []string{
		"user:123",
		"session:abc",
		"cache:item:456",
		"",
		"a",
		"こんにちは",
	}

	for _, key := range testKeys {
		partition1 := p.HashKey(key)
		partition2 := p.HashKey(key)
		partition3 := p.HashKey(key)

		if partition1 != partition2 || partition2 != partition3 {
			t.Errorf("HashKey not deterministic for key %q: got %d, %d, %d",
				key, partition1, partition2, partition3)
		}

		// Verify partition is in valid range
		if partition1 >= PartitionID(TOTAL_PARTITIONS) {
			t.Errorf("Partition %d is out of range [0, %d)", partition1, TOTAL_PARTITIONS)
		}
	}
}

func TestHashKeyIntDeterminism(t *testing.T) {
	p := NewPartitioner()

	testKeys := []int{0, 1, 42, 123, 999, -1, -999}

	for _, key := range testKeys {
		partition1 := p.HashKeyInt(key)
		partition2 := p.HashKeyInt(key)

		if partition1 != partition2 {
			t.Errorf("HashKeyInt not deterministic for key %d: got %d, %d",
				key, partition1, partition2)
		}

		// Verify partition is in valid range
		if partition1 >= PartitionID(TOTAL_PARTITIONS) {
			t.Errorf("Partition %d is out of range [0, %d)", partition1, TOTAL_PARTITIONS)
		}
	}
}

func TestHashDistribution(t *testing.T) {
	p := NewPartitioner()

	// Use more keys to get better distribution statistics
	numKeys := 100000
	partitionCounts := make(map[PartitionID]int)

	// Generate keys and count partition assignments
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key:%d", i)
		partition := p.HashKey(key)
		partitionCounts[partition]++
	}

	// Calculate statistics
	totalAssigned := 0
	minCount := numKeys
	maxCount := 0

	for _, count := range partitionCounts {
		totalAssigned += count
		if count < minCount {
			minCount = count
		}
		if count > maxCount {
			maxCount = count
		}
	}

	// Verify all keys were counted
	if totalAssigned != numKeys {
		t.Errorf("Expected %d total assignments, got %d", numKeys, totalAssigned)
	}

	numPartitionsUsed := len(partitionCounts)

	// Calculate expected count per partition (across ALL partitions, not just used ones)
	expectedPerPartition := float64(numKeys) / float64(TOTAL_PARTITIONS)

	// Calculate variance across ALL partitions (including empty ones)
	var sumSquaredDiff float64
	for i := PartitionID(0); i < TOTAL_PARTITIONS; i++ {
		count := partitionCounts[i] // defaults to 0 if not in map
		diff := float64(count) - expectedPerPartition
		sumSquaredDiff += diff * diff
	}
	variance := sumSquaredDiff / float64(TOTAL_PARTITIONS)
	stdDev := math.Sqrt(variance)

	// Calculate percentage variance instead of CV (since expected is small)
	percentVariance := (stdDev / expectedPerPartition) * 100

	t.Logf("Distribution statistics:")
	t.Logf("  Total keys: %d", numKeys)
	t.Logf("  Partitions used: %d / %d (%.1f%%)", numPartitionsUsed, TOTAL_PARTITIONS,
		100.0*float64(numPartitionsUsed)/float64(TOTAL_PARTITIONS))
	t.Logf("  Min count: %d", minCount)
	t.Logf("  Max count: %d", maxCount)
	t.Logf("  Expected per partition: %.2f", expectedPerPartition)
	t.Logf("  Standard deviation: %.2f", stdDev)
	t.Logf("  Percent variance: %.1f%%", percentVariance)

	// Acceptance criteria: with 100K keys across 16K partitions,
	// we expect good utilization and reasonable distribution

	// 1. Should use a significant portion of partitions (>90% for 100K keys)
	utilizationPercent := 100.0 * float64(numPartitionsUsed) / float64(TOTAL_PARTITIONS)
	if utilizationPercent < 90.0 {
		t.Errorf("Low partition utilization: %.1f%% (expected >90%%)", utilizationPercent)
	}

	// 2. No partition should have extremely high count (>20 for expected ~6.1)
	maxReasonable := int(expectedPerPartition * 3.5)
	if maxReasonable < 20 {
		maxReasonable = 20
	}
	if maxCount > maxReasonable {
		t.Errorf("Max partition count %d is too high (expected <%d)", maxCount, maxReasonable)
	}

	// 3. Percent variance should be reasonable (<50% for good hash functions)
	if percentVariance > 50.0 {
		t.Errorf("Percent variance %.1f%% is too high (expected <50%%)", percentVariance)
	}
}

func TestHashDistributionWithMurmur(t *testing.T) {
	p := NewPartitionerWithHash(MurmurHash3)

	numKeys := 100000
	partitionCounts := make(map[PartitionID]int)

	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key:%d", i)
		partition := p.HashKey(key)
		partitionCounts[partition]++
	}

	numPartitionsUsed := len(partitionCounts)
	expectedPerPartition := float64(numKeys) / float64(TOTAL_PARTITIONS)

	var sumSquaredDiff float64
	for i := PartitionID(0); i < TOTAL_PARTITIONS; i++ {
		count := partitionCounts[i]
		diff := float64(count) - expectedPerPartition
		sumSquaredDiff += diff * diff
	}
	variance := sumSquaredDiff / float64(TOTAL_PARTITIONS)
	stdDev := math.Sqrt(variance)
	percentVariance := (stdDev / expectedPerPartition) * 100

	t.Logf("MurmurHash3 distribution:")
	t.Logf("  Partitions used: %d / %d", numPartitionsUsed, TOTAL_PARTITIONS)
	t.Logf("  Percent variance: %.1f%%", percentVariance)

	if percentVariance > 50.0 {
		t.Errorf("MurmurHash3 distribution too uneven: %.1f%% > 50%%", percentVariance)
	}
}

func TestConcurrentHashKey(t *testing.T) {
	p := NewPartitioner()

	const numGoroutines = 100
	const numOpsPerGoroutine = 1000

	results := make([][]PartitionID, numGoroutines)
	var wg sync.WaitGroup

	// Start concurrent goroutines
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			results[goroutineID] = make([]PartitionID, numOpsPerGoroutine)

			for j := 0; j < numOpsPerGoroutine; j++ {
				key := fmt.Sprintf("key:%d:%d", goroutineID, j)
				results[goroutineID][j] = p.HashKey(key)
			}
		}(i)
	}

	wg.Wait()

	// Verify determinism: run same keys again and check results match
	for i := 0; i < numGoroutines; i++ {
		for j := 0; j < numOpsPerGoroutine; j++ {
			key := fmt.Sprintf("key:%d:%d", i, j)
			partition := p.HashKey(key)

			if partition != results[i][j] {
				t.Errorf("Concurrent result mismatch for key %q: expected %d, got %d",
					key, results[i][j], partition)
			}
		}
	}

	t.Logf("Successfully completed %d concurrent hash operations", numGoroutines*numOpsPerGoroutine)
}

func TestValidatePartitionID(t *testing.T) {
	p := NewPartitioner()

	tests := []struct {
		name    string
		pid     PartitionID
		wantErr bool
	}{
		{"valid zero", 0, false},
		{"valid middle", TOTAL_PARTITIONS / 2, false},
		{"valid max", TOTAL_PARTITIONS - 1, false},
		{"invalid at boundary", TOTAL_PARTITIONS, true},
		{"invalid large", TOTAL_PARTITIONS + 100, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := p.ValidatePartitionID(tt.pid)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidatePartitionID(%d) error = %v, wantErr %v",
					tt.pid, err, tt.wantErr)
			}
		})
	}
}

func TestSetHashFunc(t *testing.T) {
	p := NewPartitioner()

	// Get partition with default hash
	key := "test_key"
	partition1 := p.HashKey(key)

	// Change hash function
	p.SetHashFunc(MurmurHash3)

	// Get partition with new hash
	partition2 := p.HashKey(key)

	// Verify hash function actually changed
	// (partitions may coincidentally be the same, but unlikely)
	t.Logf("Partition with CRC16: %d, with MurmurHash3: %d", partition1, partition2)

	// Verify new hash is deterministic
	partition3 := p.HashKey(key)
	if partition2 != partition3 {
		t.Error("Hash function not deterministic after change")
	}
}

func TestGetTotalPartitions(t *testing.T) {
	p := NewPartitioner()

	total := p.GetTotalPartitions()
	if total != TOTAL_PARTITIONS {
		t.Errorf("GetTotalPartitions() = %d, want %d", total, TOTAL_PARTITIONS)
	}
}

func BenchmarkHashKey(b *testing.B) {
	p := NewPartitioner()
	key := "benchmark:key:12345"

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = p.HashKey(key)
	}
}

func BenchmarkHashKeyInt(b *testing.B) {
	p := NewPartitioner()
	key := 12345

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = p.HashKeyInt(key)
	}
}

func BenchmarkHashKeyConcurrent(b *testing.B) {
	p := NewPartitioner()
	key := "benchmark:key:12345"

	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = p.HashKey(key)
		}
	})
}
