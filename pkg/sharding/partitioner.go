package sharding

import (
	"fmt"
	"strconv"
	"sync"
)

// Partitioner handles key-to-partition mapping using consistent hashing
type Partitioner struct {
	mu              sync.RWMutex
	totalPartitions uint16
	hashFunc        HashFunc
}

// NewPartitioner creates a new partitioner with the default configuration
func NewPartitioner() *Partitioner {
	return &Partitioner{
		totalPartitions: TOTAL_PARTITIONS,
		hashFunc:        DefaultHashFunc,
	}
}

// NewPartitionerWithHash creates a partitioner with a custom hash function
func NewPartitionerWithHash(hashFunc HashFunc) *Partitioner {
	return &Partitioner{
		totalPartitions: TOTAL_PARTITIONS,
		hashFunc:        hashFunc,
	}
}

// HashKey computes the partition ID for a given string key
// This function is thread-safe and deterministic
func (p *Partitioner) HashKey(key string) PartitionID {
	p.mu.RLock()
	hashFunc := p.hashFunc
	totalPartitions := p.totalPartitions
	p.mu.RUnlock()

	hashValue := hashFunc(key)
	return PartitionID(hashValue % totalPartitions)
}

// HashKeyInt computes the partition ID for an integer key
// This is a convenience method for compatibility with the existing int-based cache
func (p *Partitioner) HashKeyInt(key int) PartitionID {
	return p.HashKey(strconv.Itoa(key))
}

// SetHashFunc updates the hash function used by the partitioner
// This should only be called before the partitioner is used
func (p *Partitioner) SetHashFunc(hashFunc HashFunc) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.hashFunc = hashFunc
}

// GetTotalPartitions returns the total number of partitions
func (p *Partitioner) GetTotalPartitions() uint16 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.totalPartitions
}

// ValidatePartitionID checks if a partition ID is valid
func (p *Partitioner) ValidatePartitionID(pid PartitionID) error {
	p.mu.RLock()
	totalPartitions := p.totalPartitions
	p.mu.RUnlock()

	if pid >= PartitionID(totalPartitions) {
		return fmt.Errorf("invalid partition ID %d: must be < %d", pid, totalPartitions)
	}
	return nil
}
