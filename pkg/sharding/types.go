package sharding

const (
	// TOTAL_PARTITIONS defines the total number of hash slots.
	// Using 16384 (same as Redis Cluster) provides:
	// - Fine-grained distribution for rebalancing
	// - Power of 2 for efficient modulo operations
	// - Small memory footprint (~64KB for partition table)
	TOTAL_PARTITIONS = 16384
)

// PartitionID represents a hash slot ID (0 to 16383)
type PartitionID uint16

// NodeID represents a Raft node identifier
type NodeID int
